/*-------------------------------------------------------------------------
 *
 * parse_expr.c
 *	  handle expressions in parser
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/parser/parse_expr.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "catalog/pg_synonym.h"
#include "catalog/pg_type.h"
#include "catalog/pg_type_object.h"
#include "commands/dbcommands.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "optimizer/tlist.h"
#include "optimizer/var.h"
#include "parser/analyze.h"
#include "parser/parse_clause.h"
#include "parser/parse_coerce.h"
#include "parser/parse_collate.h"
#include "parser/parse_expr.h"
#include "parser/parse_func.h"
#include "parser/parse_oper.h"
#include "parser/parse_relation.h"
#include "parser/parse_target.h"
#include "parser/parse_type.h"
#include "parser/parse_agg.h"
#include "parser/parsetree.h"
#include "utils/builtins.h"
#include "utils/date.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"
#include "utils/orcl_datetime.h"
#include "utils/timestamp.h"
#include "utils/xml.h"

#ifdef _PG_ORCL_
#include "parser/parser.h"
#include "catalog/pg_class.h"
#include "catalog/pg_package.h"
#include "utils/guc.h"
#include "opentenbase_ora/opentenbase_ora.h"
#include "utils/syscache.h"
#include "access/htup_details.h"
#endif

#define MAXINT64LEN 20

/* GUC parameters */
bool		operator_precedence_warning = false;
bool		Transform_null_equals = false;
bool        enable_opentenbase_ora_return_type = false;
/*
 * Node-type groups for operator precedence warnings
 * We use zero for everything not otherwise classified
 */
#define PREC_GROUP_POSTFIX_IS	1	/* postfix IS tests (NullTest, etc) */
#define PREC_GROUP_INFIX_IS		2	/* infix IS (IS DISTINCT FROM, etc) */
#define PREC_GROUP_LESS			3	/* < > */
#define PREC_GROUP_EQUAL		4	/* = */
#define PREC_GROUP_LESS_EQUAL	5	/* <= >= <> */
#define PREC_GROUP_LIKE			6	/* LIKE ILIKE SIMILAR */
#define PREC_GROUP_BETWEEN		7	/* BETWEEN */
#define PREC_GROUP_IN			8	/* IN */
#define PREC_GROUP_NOT_LIKE		9	/* NOT LIKE/ILIKE/SIMILAR */
#define PREC_GROUP_NOT_BETWEEN	10	/* NOT BETWEEN */
#define PREC_GROUP_NOT_IN		11	/* NOT IN */
#define PREC_GROUP_POSTFIX_OP	12	/* generic postfix operators */
#define PREC_GROUP_INFIX_OP		13	/* generic infix operators */
#define PREC_GROUP_PREFIX_OP	14	/* generic prefix operators */

/*
 * Map precedence groupings to old precedence ordering
 *
 * Old precedence order:
 * 1. NOT
 * 2. =
 * 3. < >
 * 4. LIKE ILIKE SIMILAR
 * 5. BETWEEN
 * 6. IN
 * 7. generic postfix Op
 * 8. generic Op, including <= => <>
 * 9. generic prefix Op
 * 10. IS tests (NullTest, BooleanTest, etc)
 *
 * NOT BETWEEN etc map to BETWEEN etc when considered as being on the left,
 * but to NOT when considered as being on the right, because of the buggy
 * precedence handling of those productions in the old grammar.
 */
static const int oldprecedence_l[] = {
        0, 10, 10, 3, 2, 8, 4, 5, 6, 4, 5, 6, 7, 8, 9
};
static const int oldprecedence_r[] = {
        0, 10, 10, 3, 2, 8, 4, 5, 6, 1, 1, 1, 7, 8, 9
};

static Node *transformExprRecurse(ParseState *pstate, Node *expr);
static Node *transformParamRef(ParseState *pstate, ParamRef *pref);
static Node *transformAExprOp(ParseState *pstate, A_Expr *a);
static Node *transformAExprOpAny(ParseState *pstate, A_Expr *a);
static Node *transformAExprOpAll(ParseState *pstate, A_Expr *a);
static Node *transformAExprDistinct(ParseState *pstate, A_Expr *a);
static Node *transformAExprNullIf(ParseState *pstate, A_Expr *a);
static Node *transformAExprOf(ParseState *pstate, A_Expr *a);
static Node *transformAExprIn(ParseState *pstate, A_Expr *a);
static Node *transformAExprBetween(ParseState *pstate, A_Expr *a);
static Node *transformBoolExpr(ParseState *pstate, BoolExpr *a);
static Node *transformOraIndirection(ParseState *pstate, FuncCall *fn);
static Node *transformOraTypConstructor(ParseState *pstate, FuncCall *fn);
static Node *transformFuncCall(ParseState *pstate, FuncCall *fn);
static Node *transformMultiAssignRef(ParseState *pstate, MultiAssignRef *maref);
static Node *transformCaseExpr(ParseState *pstate, CaseExpr *c);
static Node *transformSubLink(ParseState *pstate, SubLink *sublink);
static Node *transformArrayExpr(ParseState *pstate, A_ArrayExpr *a,
                                Oid array_type, Oid element_type, int32 typmod);
static Node *transformRowExpr(ParseState *pstate, RowExpr *r, bool allowDefault);
static Node *transformCoalesceExpr(ParseState *pstate, CoalesceExpr *c);
static Node *transformMinMaxExpr(ParseState *pstate, MinMaxExpr *m);
static Node *transformSQLValueFunction(ParseState *pstate,
                                       SQLValueFunction *svf);
static Node *transformXmlExpr(ParseState *pstate, XmlExpr *x);
static Node *transformXmlSerialize(ParseState *pstate, XmlSerialize *xs);
static Node *transformBooleanTest(ParseState *pstate, BooleanTest *b);
static Node *transformCurrentOfExpr(ParseState *pstate, CurrentOfExpr *cexpr);
static Node *transformColumnRef(ParseState *pstate, ColumnRef *cref, bool try_columnref);
static Node *transformWholeRowRef(ParseState *pstate, RangeTblEntry *rte,
                                  int location);
static Node *transformIndirection(ParseState *pstate, A_Indirection *ind);
static Node *transformTypeCast(ParseState *pstate, TypeCast *tc);
static Node *transformCollateClause(ParseState *pstate, CollateClause *c);
static Node *make_row_comparison_op(ParseState *pstate, List *opname,
                                    List *largs, List *rargs, int location);
static Node *make_row_distinct_op(ParseState *pstate, List *opname,
                                  RowExpr *lrow, RowExpr *rrow, int location);
static Expr *make_distinct_op(ParseState *pstate, List *opname,
                              Node *ltree, Node *rtree, int location);
static Node *make_nulltest_from_distinct(ParseState *pstate,
                                         A_Expr *distincta, Node *arg);
static int	operator_precedence_group(Node *node, const char **nodename);
static void emit_precedence_warnings(ParseState *pstate,
                                     int opgroup, const char *opname,
                                     Node *lchild, Node *rchild,
                                     int location);
#ifdef _PG_ORCL_
static Node *transformSequenceColumn(ParseState *pstate, ColumnRef *cref);
static void convert_to_opentenbase_ora_func(FuncCall *fn, List *targs);
static void ora_select_common_type(ParseState *pstate, List *exprs, Oid *typoid, int32 *typmod);
static Node *transformDecodeExpr(ParseState *pstate, Node *warg, Node *expr, Oid stype, Oid stypemod);
static Node *transformMultiSetExpr(ParseState *pstate, MultiSetExpr *mse);
static Node* transformRowNumberResultType(ParseState *pstate, Node *node);
#endif
extern bool skip_check_same_relname;

/*
 * transformExpr -
 *	  Analyze and transform expressions. Type checking and type casting is
 *	  done here.  This processing converts the raw grammar output into
 *	  expression trees with fully determined semantics.
 */
Node *
transformExpr(ParseState *pstate, Node *expr, ParseExprKind exprKind)
{
    Node	   *result;
    ParseExprKind sv_expr_kind;

    /* Save and restore identity of expression type we're parsing */
    Assert(exprKind != EXPR_KIND_NONE);
    sv_expr_kind = pstate->p_expr_kind;
    pstate->p_expr_kind = exprKind;

    result = transformExprRecurse(pstate, expr);

    pstate->p_expr_kind = sv_expr_kind;

    return result;
}


static Node *
transformExprRecurse(ParseState *pstate, Node *expr)
{
    Node	   *result = NULL;

    if (expr == NULL)
        return NULL;

    /* Guard against stack overflow due to overly complex expressions */
    check_stack_depth();

    switch (nodeTag(expr))
    {
        case T_ColumnRef:
            result = transformColumnRef(pstate, (ColumnRef *) expr, false);
            break;

        case T_ParamRef:
            result = transformParamRef(pstate, (ParamRef *) expr);
            break;

        case T_A_Const:
        {
            A_Const    *con = (A_Const *) expr;
            Value	   *val = &con->val;

            result = (Node *) make_const(pstate, val, con->location);
            break;
        }

        case T_A_Indirection:
            result = transformIndirection(pstate, (A_Indirection *) expr);
            break;

        case T_A_ArrayExpr:
            result = transformArrayExpr(pstate, (A_ArrayExpr *) expr,
                                        InvalidOid, InvalidOid, -1);
            break;

        case T_TypeCast:
            result = transformTypeCast(pstate, (TypeCast *) expr);
            break;

        case T_CollateClause:
            result = transformCollateClause(pstate, (CollateClause *) expr);
            break;

        case T_A_Expr:
        {
            A_Expr	   *a = (A_Expr *) expr;

            switch (a->kind)
            {
                case AEXPR_OP:
                    result = transformAExprOp(pstate, a);
                    break;
                case AEXPR_OP_ANY:
                    result = transformAExprOpAny(pstate, a);
                    break;
                case AEXPR_OP_ALL:
                    result = transformAExprOpAll(pstate, a);
                    break;
                case AEXPR_DISTINCT:
                case AEXPR_NOT_DISTINCT:
                    result = transformAExprDistinct(pstate, a);
                    break;
                case AEXPR_NULLIF:
                    result = transformAExprNullIf(pstate, a);
                    break;
                case AEXPR_OF:
                    result = transformAExprOf(pstate, a);
                    break;
                case AEXPR_IN:
                    result = transformAExprIn(pstate, a);
                    break;
                case AEXPR_LIKE:
                case AEXPR_ILIKE:
                case AEXPR_SIMILAR:
                    /* we can transform these just like AEXPR_OP */
                    result = transformAExprOp(pstate, a);
                    break;
                case AEXPR_BETWEEN:
                case AEXPR_NOT_BETWEEN:
                case AEXPR_BETWEEN_SYM:
                case AEXPR_NOT_BETWEEN_SYM:
                    result = transformAExprBetween(pstate, a);
                    break;
                case AEXPR_PAREN:
                    result = transformExprRecurse(pstate, a->lexpr);
                    break;
                default:
                elog(ERROR, "unrecognized A_Expr kind: %d", a->kind);
                    result = NULL;	/* keep compiler quiet */
                    break;
            }
            break;
        }

        case T_BoolExpr:
            result = transformBoolExpr(pstate, (BoolExpr *) expr);
            break;

        case T_FuncCall:
        {
            if (ORA_MODE)
            {
                /* Try matching collect_variable(index1)(index2)... */
                if (InPlpgsqlFunc())
                    result = transformOraIndirection(pstate, (FuncCall *) expr);

                /* Try converting collectType(xxx) to ARRAY[xxx]::castType */
                if (result == NULL)
                    result = transformOraTypConstructor(pstate, (FuncCall *) expr);
            }

            if (result == NULL)
                result = transformFuncCall(pstate, (FuncCall *) expr);
            break;
        }

        case T_MultiAssignRef:
            result = transformMultiAssignRef(pstate, (MultiAssignRef *) expr);
            break;

        case T_GroupingFunc:
            result = transformGroupingFunc(pstate, (GroupingFunc *) expr);
            break;

        case T_NamedArgExpr:
        {
            NamedArgExpr *na = (NamedArgExpr *) expr;

            na->arg = (Expr *) transformExprRecurse(pstate, (Node *) na->arg);
            result = expr;
            break;
        }

        case T_SubLink:
            result = transformSubLink(pstate, (SubLink *) expr);
            break;

        case T_CaseExpr:
            result = transformCaseExpr(pstate, (CaseExpr *) expr);
            break;

        case T_RowExpr:
            result = transformRowExpr(pstate, (RowExpr *) expr, false);
            break;

        case T_CoalesceExpr:
            result = transformCoalesceExpr(pstate, (CoalesceExpr *) expr);
            break;

        case T_MinMaxExpr:
            result = transformMinMaxExpr(pstate, (MinMaxExpr *) expr);
            break;

        case T_SQLValueFunction:
            result = transformSQLValueFunction(pstate,
                                               (SQLValueFunction *) expr);
            break;

        case T_XmlExpr:
            result = transformXmlExpr(pstate, (XmlExpr *) expr);
            break;

        case T_XmlSerialize:
            result = transformXmlSerialize(pstate, (XmlSerialize *) expr);
            break;

        case T_NullTest:
        {
            NullTest   *n = (NullTest *) expr;

            if (operator_precedence_warning)
                emit_precedence_warnings(pstate, PREC_GROUP_POSTFIX_IS, "IS",
                                         (Node *) n->arg, NULL,
                                         n->location);

            n->arg = (Expr *) transformExprRecurse(pstate, (Node *) n->arg);
            if (ORA_MODE && IsA((Node *) n->arg, Param) &&
                exprType((Node *) n->arg) == UNKNOWNOID)
			    n->arg = (Expr *) coerce_to_common_type(pstate, (Node *) n->arg, 
                                                        TEXTOID, "IS NULL");
            /* the argument can be any type, so don't coerce it */
            n->argisrow = type_is_rowtype(exprType((Node *) n->arg));
            result = expr;
            break;
        }

        case T_BooleanTest:
            result = transformBooleanTest(pstate, (BooleanTest *) expr);
            break;

        case T_CurrentOfExpr:
            result = transformCurrentOfExpr(pstate, (CurrentOfExpr *) expr);
            break;

            /*
             * In all places where DEFAULT is legal, the caller should have
             * processed it rather than passing it to transformExpr().
             */
        case T_SetToDefault:
            ereport(ERROR,
                    (errcode(ERRCODE_SYNTAX_ERROR),
                            errmsg("DEFAULT is not allowed in this context"),
                            parser_errposition(pstate,
                                               ((SetToDefault *) expr)->location)));
            break;

            /*
             * CaseTestExpr doesn't require any processing; it is only
             * injected into parse trees in a fully-formed state.
             *
             * Ordinarily we should not see a Var here, but it is convenient
             * for transformJoinUsingClause() to create untransformed operator
             * trees containing already-transformed Vars.  The best
             * alternative would be to deconstruct and reconstruct column
             * references, which seems expensively pointless.  So allow it.
             */
        case T_CaseTestExpr:
        case T_Var:
        {
            result = (Node *) expr;
            break;
        }

#ifdef _PG_ORCL_
		case T_RownumExpr:
			{
				if (pstate->p_expr_kind == EXPR_KIND_RETURNING||
					pstate->p_expr_kind == EXPR_KIND_VALUES ||
					pstate->p_expr_kind == EXPR_KIND_VALUES_SINGLE ||
					pstate->p_expr_kind == EXPR_KIND_INSERT_TARGET)
					ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR)
						,errmsg("Specified pseudocolumn or operator not allowed here.")
						,parser_errposition(pstate, exprLocation(expr))));
				else if (pstate->isMergeStmt && (pstate->p_expr_kind == EXPR_KIND_MERGE_WHEN ||
						pstate->p_expr_kind == EXPR_KIND_UPDATE_SOURCE))
				{
					Const *con = makeConst(INT4OID, -1, InvalidOid, sizeof(int32),
					          DatumGetInt32(0), false, true);
					con->location = -1;
					result = (Node *)con;
				}
				else
				{
					pstate->p_hasRownum = true;
					result = (Node *) expr;
				}
			}
			break;
		case T_PriorExpr:
			{
				PriorExpr *pexpr = (PriorExpr *) expr;
	
				if (!pstate->p_hasConnectBy)
				{
					ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR)
						,errmsg("CONNECT BY clause is required in this query block")
						,parser_errposition(pstate, exprLocation(expr))));
				}
	
				if (pstate->p_is_start_with || pstate->p_is_prior)
				{
					/* nested prior or prior inside start with qual are not allowed */
					ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR)
						,errmsg("Specified pseudocolumn or operator not allowed here.")
						,parser_errposition(pstate, exprLocation(expr))));
				}
	
				pstate->p_is_prior = true;
				pexpr->expr = transformExprRecurse(pstate, pexpr->expr);
				pstate->p_is_prior = false;
				result = expr;
			}
			break;
		case T_CBIsLeafExpr:
		case T_CBIsCycleExpr:
			{
				if (pstate->p_is_connect_by || pstate->p_is_start_with)
				{
					ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR)
						,errmsg("CONNECT_BY_ISLEAF/CONNECT_BY_ISCYCLE operator is not supported in the"
								" START WITH or in the CONNECT BY condition")
						,parser_errposition(pstate, exprLocation(expr))));
				}
				else if (pstate->p_expr_kind == EXPR_KIND_INSERT_TARGET)
					ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR)
						  ,errmsg("Specified pseudocolumn or operator not allowed here.")
						  ,parser_errposition(pstate, exprLocation(expr))));
				else if (!pstate->p_has_nocycle && nodeTag(expr) == T_CBIsCycleExpr)
				{
					ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR),
						errmsg("NOCYCLE keyword is required with CONNECT_BY_ISCYCLE pseudocolumn"),
						errdetail("CONNECT_BY_ISCYCLE was specifed in a query which does not have the NOCYCLE keyword."),
						errhint("Remove CONNECT_BY_ISCYCLE or add NOCYCLE."),
						parser_errposition(pstate, exprLocation(expr))));
				}
			}
			/* fallthrough */
		case T_LevelExpr:
			{
				if (!pstate->p_hasConnectBy)
				{
					ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR)
						,errmsg("CONNECT BY clause is required in this query block")
						,parser_errposition(pstate, exprLocation(expr))));
				}
				else if (pstate->p_expr_kind == EXPR_KIND_INSERT_TARGET)
				ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR)
					  ,errmsg("Specified pseudocolumn or operator not allowed here.")
					  ,parser_errposition(pstate, exprLocation(expr))));
				result = (Node *) expr;
			}
			break;
		case T_CBRootExpr:
			{
				CBRootExpr *root = (CBRootExpr *) expr;
	
				if (pstate->p_is_connect_by || pstate->p_is_start_with)
				{
					ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR)
						,errmsg("CONNECT_BY_ROOR operator is not supported in the"
								" START WITH or in the CONNECT BY condition")
						,parser_errposition(pstate, exprLocation(expr))));
				}
	
				if (!pstate->p_hasConnectBy)
				{
					ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR)
						,errmsg("CONNECT BY clause is required in this query block")
						,parser_errposition(pstate, exprLocation(expr))));
				}
	
				root->expr = transformExprRecurse(pstate, root->expr);
				result = expr;
			}
			break;
		case T_CBPathExpr:
			{
				CBPathExpr *pe = (CBPathExpr *) expr;
	
				if (pstate->p_expr_kind == EXPR_KIND_WHERE)
				{
					ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR)
						,errmsg("SYS_CONNECT_BY_PATH function is not allowed here")
						,parser_errposition(pstate, exprLocation(expr))));
				}
	
				if (!pstate->p_hasConnectBy)
				{
					ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR)
						,errmsg("CONNECT BY clause is required in this query block")
						,parser_errposition(pstate, exprLocation(expr))));
				}
	
				pe->expr = transformExprRecurse(pstate, pe->expr);
				result = expr;
			}
			break;
		case T_ColumnRefJoin:
			/* ColumnRefJoin: syntax 'columnref(+)', '(+)' is opentenbase_ora's old-style join word */
			if (pstate->p_expr_kind != EXPR_KIND_WHERE && pstate->p_expr_kind != EXPR_KIND_HAVING
														&& pstate->p_expr_kind != EXPR_KIND_JOIN_ON
														&& pstate->p_expr_kind != EXPR_KIND_MERGE_WHEN)
			{
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
							errmsg("syntax error"),
							parser_errposition(pstate, ((ColumnRefJoin*)expr)->location)));
			}
			else
			{
				ColumnRefJoin	*crj = (ColumnRefJoin*)expr;
				int				varno;

				result = expr;
				Assert(crj->column && IsA(crj->column,ColumnRef));

				StaticAssertExpr(offsetof(ColumnRefJoin, column) == offsetof(ColumnRefJoin, var), 
					"column and var must union");

				/* Check and transform the ColumnRef conisted of column to Var */
				crj->var = (Var*)transformColumnRef(pstate, crj->column, false);
				if (crj->var == NULL || !IsA(crj->column, Var) || crj->var->varattno == 0)
				{
					ereport(ERROR,
							(errcode(ERRCODE_SYNTAX_ERROR),
								errmsg("syntax error"),
								parser_errposition(pstate, crj->location)));
				}

				if (!pstate->has_outer_join_operator && (pstate->p_expr_kind == EXPR_KIND_WHERE
													|| pstate->p_expr_kind == EXPR_KIND_MERGE_WHEN))
					pstate->has_outer_join_operator = true;

				if (!pstate->need_ignore_outer_join && pstate->p_expr_kind == EXPR_KIND_HAVING)
					pstate->need_ignore_outer_join = true;

				if (pstate->has_ansi_join)
					elog(ERROR, "old style outer join (+) cannot be used with ANSI joins");

				if (pstate->p_expr_kind == EXPR_KIND_JOIN_ON)
				{
					varno = crj->var->varno;
					pstate->has_outer_join_on = true;

					if (crj->var->varlevelsup != 0)
						elog(ERROR, "an outer join cannot be specified on a correlation column");

					if (pstate->outer_join_varno == 0)
						pstate->outer_join_varno = varno;
					else if (pstate->outer_join_varno != varno)
						elog(ERROR, "a predicate may reference only one outer-joined table");
				}
			}
			break;
		case T_MultiSetExpr:
			{
				if (ORA_MODE)
					result = transformMultiSetExpr(pstate, (MultiSetExpr *) expr);
				else
					elog(ERROR, "multiset expr not support in pg mode.");
			}
			break;
#endif /* _PG_ORCL_ */
        default:
            /* should not reach here */
        elog(ERROR, "unrecognized node type: %d", (int) nodeTag(expr));
            result = NULL;		/* keep compiler quiet */
            break;
    }


    return result;
}

static Node *
transformMultiSetExpr(ParseState *pstate, MultiSetExpr *mse)
{
	MultiSetExpr	*newMse;
	Oid				loid, roid;
	Node			*le, *re;

	if (mse->lexpr == NULL || mse->rexpr == NULL)
		elog(ERROR, "multiset operator must have two operands.");
	
	le = mse->lexpr;
	re = mse->rexpr;



	if ((IsA(le, A_Const) && ((A_Const *)le)->val.type ==  T_Null) ||
			(IsA(re, A_Const) && ((A_Const *)re)->val.type == T_Null))
		elog(ERROR, "Reference to uninitialized collection");

	newMse = makeNode(MultiSetExpr);
	newMse->lexpr = transformExprRecurse(pstate, le);
	newMse->rexpr = transformExprRecurse(pstate, re);
	newMse->setop = mse->setop;
	newMse->all = mse->all;
	
	Assert(newMse->lexpr != NULL && newMse->rexpr != NULL);


	loid = exprType(newMse->lexpr);
	roid = exprType(newMse->rexpr);
	
	if (loid != roid)
		elog(ERROR, "multiset operator operands type not match");
	else if(!OidIsValid(loid) || !OidIsValid(roid))
		elog(ERROR, "multiset operator operands type not match");
	else
	{
		if (type_is_nestedtable(loid))
		{
			List		*targs;
			Value		*valtyp;
			Value		*valall;
			Node		*func;

			targs = NIL;
			targs = lappend(targs, newMse->lexpr);
			targs = lappend(targs, newMse->rexpr);

			valtyp = makeNode(Value);
			valall = makeNode(Value);
			valtyp->type = T_Integer;
			valtyp->val.ival = (int) (newMse->setop);
			valall->type = T_Integer;
			valall->val.ival = (int) (newMse->all);

			targs = lappend(targs, make_const(pstate, valtyp, -1));
			targs = lappend(targs, make_const(pstate, valall, -1));

			func = ParseFuncOrColumn(pstate,
										list_make2(makeString("opentenbase_ora"), makeString("MULTISET_FN")),
										targs,
										pstate->p_last_srf,
										NULL,
										false,
										-1);
			
			if (func != NULL)
				return func;
		}
		else
			elog(ERROR, "multiset operator oprands is not nested table.");
	}

	elog(ERROR, "failed to resolve multiset op.");
	
	return (Node *) newMse;
}
/*
 * helper routine for delivering "column does not exist" error message
 *
 * (Usually we don't have to work this hard, but the general case of field
 * selection from an arbitrary node needs it.)
 */
static void
unknown_attribute(ParseState *pstate, Node *relref, char *attname,
                  int location)
{
    RangeTblEntry *rte;

    if (IsA(relref, Var) &&
        ((Var *) relref)->varattno == InvalidAttrNumber)
    {
        /* Reference the RTE by alias not by actual table name */
        rte = GetRTEByRangeTablePosn(pstate,
                                     ((Var *) relref)->varno,
                                     ((Var *) relref)->varlevelsup);
        ereport(ERROR,
                (errcode(ERRCODE_UNDEFINED_COLUMN),
                        errmsg("column %s.%s does not exist",
                               rte->eref->aliasname, attname),
                        parser_errposition(pstate, location)));
    }
    else
    {
        /* Have to do it by reference to the type of the expression */
        Oid			relTypeId = exprType(relref);

        if (ISCOMPLEX(relTypeId))
            ereport(ERROR,
                    (errcode(ERRCODE_UNDEFINED_COLUMN),
                            errmsg("column \"%s\" not found in data type %s",
                                   attname, format_type_be(relTypeId)),
                            parser_errposition(pstate, location)));
        else if (relTypeId == RECORDOID)
            ereport(ERROR,
                    (errcode(ERRCODE_UNDEFINED_COLUMN),
                            errmsg("could not identify column \"%s\" in record data type",
                                   attname),
                            parser_errposition(pstate, location)));
        else
            ereport(ERROR,
                    (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                            errmsg("column notation .%s applied to type %s, "
                                   "which is not a composite type",
                                   attname, format_type_be(relTypeId)),
                            parser_errposition(pstate, location)));
    }
}

static Node *
transformIndirection(ParseState *pstate, A_Indirection *ind)
{
    Node	   *last_srf = pstate->p_last_srf;
    Node	   *result = transformExprRecurse(pstate, ind->arg);
    List	   *subscripts = NIL;
    bool        is_ora_collect = false;
    int			location = exprLocation(result);
    ListCell   *i;
    Oid indexTypOid = InvalidOid;
    Oid arrayType =  exprType(result);
    int32 arrayTypMod = exprTypmod(result);	

    if (ORA_MODE &&
        pstate->p_collectref_hook &&
        IsA(ind->arg, ColumnRef) &&
        list_length(ind->indirection) != 0 &&
        result && IsA(result, Param))
    {
        /*
         * Get the index_type corresponding to the collection type variable.
         */
        Param          *param = (Param *) result;
        ParseCollect    parsecollect;

        parsecollect = pstate->p_collectref_hook(pstate, param->paramid, &indexTypOid);
        if (parsecollect != COLLECT_NONE)
            is_ora_collect = true;
    }

    /*
     * We have to split any field-selection operations apart from
     * subscripting.  Adjacent A_Indices nodes have to be treated as a single
     * multidimensional subscript operation.
     */
    foreach(i, ind->indirection)
    {
        Node	   *n = lfirst(i);

        if (IsA(n, A_Indices))
        {
            subscripts = lappend(subscripts, n);

            if (is_ora_collect && ((A_Indices *) n)->is_slice)
                ereport(ERROR,
                        (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                         errmsg("collection slicing operations are not supported here"),
                         parser_errposition(pstate, location)));
        }
        else if (IsA(n, A_Star))
        {
            ereport(ERROR,
                    (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                            errmsg("row expansion via \"*\" is not supported here"),
                            parser_errposition(pstate, location)));
        }
        else
        {
            Node	   *newresult;

            Assert(IsA(n, String));

            if (PLUDTOID == arrayType && IsA(result, Param))
            {
                arrayType = get_package_udt_typeoid((Param *) result, pstate->p_pl_state);
                getBaseTypeAndTypmod(arrayType, &arrayTypMod);
            }
            else
            {
                arrayType =  exprType(result);
                arrayTypMod = exprTypmod(result);
            }

            /* process subscripts before this field selection */
            if (subscripts)
                result = (Node *) transformArraySubscripts(pstate,
                                                           result,
                                                           arrayType,
                                                           InvalidOid,
                                                           arrayTypMod,
                                                           subscripts,
                                                           NULL,
                                                           InvalidOid);
            subscripts = NIL;

            newresult = ParseFuncOrColumn(pstate,
                                          list_make1(n),
                                          list_make1(result),
                                          last_srf,
                                          NULL,
                                          false,
                                          location);
            if (newresult == NULL)
                unknown_attribute(pstate, result, strVal(n), location);
            result = newresult;

            arrayType =  exprType(result);
            arrayTypMod = exprTypmod(result);	
#ifdef _PG_ORCL_
            if (PLUDTOID == arrayType && IsA(result, Param))
            {
                arrayType = get_package_udt_typeoid((Param *) result, pstate->p_pl_state);
                getBaseTypeAndTypmod(arrayType, &arrayTypMod);		
            }
#endif
        }
    }
    /* process trailing subscripts, if any */
    if (subscripts)
    {
        if (PLUDTOID == arrayType && IsA(result, Param))
        {
            arrayType = get_package_udt_typeoid((Param *) result, pstate->p_pl_state);
            getBaseTypeAndTypmod(arrayType, &arrayTypMod);
        }
        else
        {
            arrayType =  exprType(result);
            arrayTypMod = exprTypmod(result);
        }

        result = (Node *) transformArraySubscripts(pstate,
                                                   result,
                                                   arrayType,
                                                   InvalidOid,
                                                   arrayTypMod,
                                                   subscripts,
                                                   NULL,
                                                   indexTypOid);
    }

    return result;
}

void
mockTypObjConsSelfParam(List **fparams, Oid typoid)
{
	Const *param;
	int16 typLen;
	bool typByVal;

	if (fparams == NULL)
		return;

	get_typlenbyval(typoid, &typLen, &typByVal);
	param = makeConst(typoid,
					-1,
					0,
					(int) typLen,
					 (Datum) 0,
					 true,
					 typByVal);

	*fparams = *fparams == NIL ? list_make1((Node *) param) : 
								lcons((Node *) param, *fparams);
}

static Node *
transformColRefAsFunction(ParseState *pstate, ColumnRef *cref)
{
	Node	*node = NULL;
	char	*sch_name = NULL;

	switch (list_length(cref->fields))
	{
		case 1:
		{
			Node	*field1 = (Node *) linitial(cref->fields);
			char	*funname = NULL;

			Assert(IsA(field1, String));
			funname = strVal(field1);

			/* Set the currently visible with functions and local functions */
			set_with_local_funcs(pstate);

			node = ParseFuncOrColumn(pstate, list_make1(makeString(funname)),
												NULL, pstate->p_last_srf, NULL,
												false, cref->location);
			break;
		}
		case 2:
		{
			Node	*field1 = (Node *) linitial(cref->fields);
			Node	*field2 = (Node *) lsecond(cref->fields);

			Assert(IsA(field1, String));
			sch_name = strVal(field1);

			if (ORA_MODE && node == NULL && !IsA(field2, A_Star) &&
							get_namespace_oid(sch_name, true) != InvalidOid)
			{
				/*
				 * 'field1' maybe a namespace and field2 is a function.
				 */
				node = ParseFuncOrColumn(pstate, list_make2(field1, field2),
											NULL, pstate->p_last_srf, NULL,
											false, cref->location);
			}
			break;
		}
		case 3:
		{
			Node	*field1 = (Node *) linitial(cref->fields);
			Node	*field2 = (Node *) lsecond(cref->fields);
			Node	*field3 = (Node *) lthird(cref->fields);
			char	*catname = NULL;

			Assert(IsA(field1, String));
			catname = strVal(field1);
			Assert(IsA(field2, String));
			sch_name = strVal(field2);

			if (ORA_MODE && !IsA(field3, A_Star) &&
						strcmp(catname, get_database_name(MyDatabaseId)) == 0 &&
						get_namespace_oid(sch_name, true) != InvalidOid)
			{
				/*
				 * 'field1' maybe a database name and field2 is a schema and field3
				 * is a function.
				 */
				node = ParseFuncOrColumn(pstate, list_make2(field2, field3),
											NULL, pstate->p_last_srf, NULL,
											false, cref->location);
			}
			break;
		}
		default:
			return NULL;
	}

	return node;
}

static Node *
transformCompositeColumn(ParseState *pstate, ColumnRef *cref)
{
	Node			*colnode = NULL;
	char			*catname = NULL;
	char			*nspname = NULL;
	char			*relname = NULL;
	char			*colname = NULL;
	char			*attstr = NULL;
	int				levels_up;
	RangeTblEntry	*rte;

	/*----------
	 * The alowed syntaxes are:
	 *
	 *  A.B        A is a column name and its type is a composite type,
	 *             B is field name of the composite type;
	 *  A.B.C      table A, column B, field C;
	 *  A.B.C.D    schema A, table B, column C, field D;
	 *  A.B.C.D.E  database A, schema B, table C, column D, field E.
	 */
	switch (list_length(cref->fields))
	{
		case 1:
			return NULL;
			break;
		case 2:
			{
				Node	*field1 = (Node *) linitial(cref->fields);
				Node	*field2 = (Node *) lsecond(cref->fields);

				Assert(IsA(field1, String));
				colname = strVal(field1);

				/* Try to identify as an unqualified column */
				colnode = colNameToVar(pstate, colname, false, cref->location);

				if (colnode != NULL && IsA(field2, String))
					attstr = strVal(field2);
				else
					return NULL;

				break;
			}
		case 3:
			{
				Node	*field1 = (Node *) linitial(cref->fields);
				Node	*field2 = (Node *) lsecond(cref->fields);
				Node	*field3 = (Node *) lthird(cref->fields);

				Assert(IsA(field1, String));
				relname = strVal(field1);
				Assert(IsA(field2, String));
				colname = strVal(field2);

				/* Locate the referenced RTE */
				rte = refnameRangeTblEntry(pstate, nspname, relname,
										   cref->location,
										   &levels_up);

				if (rte != NULL)
				{
					colnode = scanRTEForColumn(pstate, rte, colname, cref->location,
											   0, NULL);
					if (colnode != NULL && IsA(field3, String))
						attstr = strVal(field3);
					else
						return NULL;
				}
				else
					return NULL;

				break;
			}
		case 4:
			{
				Node	*field1 = (Node *) linitial(cref->fields);
				Node	*field2 = (Node *) lsecond(cref->fields);
				Node	*field3 = (Node *) lthird(cref->fields);
				Node	*field4 = (Node *) lfourth(cref->fields);

				Assert(IsA(field1, String));
				nspname = strVal(field1);
				Assert(IsA(field2, String));
				relname = strVal(field2);
				Assert(IsA(field3, String));
				colname = strVal(field3);

				/* Locate the referenced RTE */
				rte = refnameRangeTblEntry(pstate, nspname, relname,
										   cref->location,
										   &levels_up);

				if (rte != NULL)
				{
					colnode = scanRTEForColumn(pstate, rte, colname, cref->location,
											   0, NULL);
					if (colnode != NULL && IsA(field4, String))
						attstr = strVal(field4);
					else
						return NULL;
				}
				else
					return NULL;

				break;
			}
		case 5:
			{
				Node	*field1 = (Node *) linitial(cref->fields);
				Node	*field2 = (Node *) lsecond(cref->fields);
				Node	*field3 = (Node *) lthird(cref->fields);
				Node	*field4 = (Node *) lfourth(cref->fields);
				Node	*field5 = (Node *) llast(cref->fields);

				Assert(IsA(field1, String));
				catname = strVal(field1);
				Assert(IsA(field2, String));
				nspname = strVal(field2);
				Assert(IsA(field3, String));
				relname = strVal(field3);
				Assert(IsA(field4, String));
				colname = strVal(field4);

				/* check the catalog name */
				if (pg_strcasecmp(catname, get_database_name(MyDatabaseId)) != 0)
					return NULL;

				/* Locate the referenced RTE */
				rte = refnameRangeTblEntry(pstate, nspname, relname,
										cref->location,
										&levels_up);

				if (rte != NULL)
				{
					colnode = scanRTEForColumn(pstate, rte, colname, cref->location,
											   0, NULL);
					if (colnode != NULL && IsA(field5, String))
						attstr = strVal(field5);
					else
						return NULL;
				}
				else
					return NULL;

				break;
			}
		default:
			return NULL;
			break;
	}

	if (colnode != NULL && IsA(colnode, Var) && attstr != NULL)
	{
		Oid		coloid = ((Var *) colnode)->vartype;

		if (type_is_rowtype(coloid))
		{
			int			i = 0;
			int			natts;
			Oid			typrelid;
			HeapTuple	tuple;

			/* Look up composite types in pg_type */
			tuple = SearchSysCache1(TYPEOID, ObjectIdGetDatum(coloid));
			if (!HeapTupleIsValid(tuple))
				elog(ERROR, "cache lookup failed for type %u", coloid);
			typrelid = ((Form_pg_type) GETSTRUCT(tuple))->typrelid;
			ReleaseSysCache(tuple);

			/*
			 * Find the number of attributes corresponding to the
			 * composite types in pg_class.
			 */
			tuple = SearchSysCache1(RELOID, ObjectIdGetDatum(typrelid));
			if (!HeapTupleIsValid(tuple))
				elog(ERROR, "cache lookup failed for the number of attributes "
							"corresponding to the composite type %u", coloid);
			natts = ((Form_pg_class) GETSTRUCT(tuple))->relnatts;
			ReleaseSysCache(tuple);

			for(; i < natts; i++)
			{
				Form_pg_attribute	att;

				tuple = SearchSysCache2(ATTNUM,
										ObjectIdGetDatum(typrelid),
										Int16GetDatum(i + 1));
				if (!HeapTupleIsValid(tuple))
					elog(ERROR, "cache lookup failed for composite type %u attributes", coloid);

				att = (Form_pg_attribute) GETSTRUCT(tuple);
				if (pg_strcasecmp(NameStr(att->attname), attstr) == 0)
				{
					FieldSelect *fselect = makeNode(FieldSelect);

					fselect->arg = (Expr *) colnode;
					fselect->fieldnum = i + 1;
					fselect->resulttype = att->atttypid;
					fselect->resulttypmod = att->atttypmod;
					fselect->resultcollid = att->attcollation;
					ReleaseSysCache(tuple);

					return (Node *) fselect;
				}

				ReleaseSysCache(tuple);
			}
		}
	}

	return NULL;
}

/*
 * Transform a ColumnRef.
 *
 * If you find yourself changing this code, see also ExpandColumnRefStar.
 *
 * try_columnref: Try to convert columnref, strict conversion is required,
 * and no error will be reported if failed.
 */
static Node *
transformColumnRef(ParseState *pstate, ColumnRef *cref, bool try_columnref)
{
    Node	   *node = NULL;
    char	   *nspname = NULL;
    char	   *relname = NULL;
    char	   *colname = NULL;
    RangeTblEntry *rte;
    int			levels_up;
    enum
    {
        CRERR_NO_COLUMN,
        CRERR_NO_RTE,
        CRERR_WRONG_DB,
        CRERR_TOO_MANY
    }			crerr = CRERR_NO_COLUMN;

    /*
     * Give the PreParseColumnRefHook, if any, first shot.  If it returns
     * non-null then that's all, folks.
     */
    if (pstate->p_pre_columnref_hook != NULL)
    {
    	node = pstate->p_pre_columnref_hook(pstate, cref);
        if (node != NULL)
            return node;
    }

    /*----------
     * The allowed syntaxes are:
     *
     * A		First try to resolve as unqualified column name;
     *			if no luck, try to resolve as unqualified table name (A.*).
     * A.B		A is an unqualified table name; B is either a
     *			column or function name (trying column name first).
     * A.B.C	schema A, table B, col or func name C.
     * A.B.C.D	catalog A, schema B, table C, col or func D.
     * A.*		A is an unqualified table name; means whole-row value.
     * A.B.*	whole-row value of table B in schema A.
     * A.B.C.*	whole-row value of table C in schema B in catalog A.
     *
     * We do not need to cope with bare "*"; that will only be accepted by
     * the grammar at the top level of a SELECT list, and transformTargetList
     * will take care of it before it ever gets here.  Also, "A.*" etc will
     * be expanded by transformTargetList if they appear at SELECT top level,
     * so here we are only going to see them as function or operator inputs.
     *
     * Currently, if a catalog name is given then it must equal the current
     * database name; we check it here and then discard it.
     *----------
     */
    switch (list_length(cref->fields))
    {
        case 1:
        {
            Node	   *field1 = (Node *) linitial(cref->fields);

            Assert(IsA(field1, String));
            colname = strVal(field1);

            /* Try to identify as an unqualified column */
            node = colNameToVar(pstate, colname, false, cref->location);

            if (node == NULL)
            {
                /*
                 * Not known as a column of any range-table entry.
                 *
                 * Try to find the name as a relation.  Note that only
                 * relations already entered into the rangetable will be
                 * recognized.
                 *
                 * This is a hack for backwards compatibility with
                 * PostQUEL-inspired syntax.  The preferred form now is
                 * "rel.*".
                 */
                rte = refnameRangeTblEntry(pstate, NULL, colname,
                                           cref->location,
                                           &levels_up);
                if (rte)
                    node = transformWholeRowRef(pstate, rte,
                                                cref->location);
				if (ORA_MODE && node == NULL && pstate->order_list)
				{
					ListCell *lc;
					foreach(lc, *pstate->order_list)
					{
						TargetEntry *tle = lfirst(lc);

						if (!tle->resjunk && tle->resname &&
								strcmp(tle->resname, colname) == 0)
						{
							if (node)
								ereport(ERROR,
										(errcode(ERRCODE_AMBIGUOUS_COLUMN),
										errmsg("column reference \"%s\" is ambiguous",
										colname),
										parser_errposition(pstate, cref->location)));
							node = (Node*)(tle->expr);
						}
					}
				}
            }
            break;
        }
        case 2:
        {
            Node	   *field1 = (Node *) linitial(cref->fields);
            Node	   *field2 = (Node *) lsecond(cref->fields);

            Assert(IsA(field1, String));
            relname = strVal(field1);

			node = NULL;
			if (ORA_MODE && nspname == NULL && skip_check_same_relname &&
				!IsA(field2, A_Star))
			{
				List       *relslist = NIL;
				ListCell   *l;
				bool            found_colname = false;

				colname = strVal(field2);
				relslist = refnameRangeTblEntries(pstate, relname, cref->location);

				/* if only one candidate relation, then enter the normal logic */
				if (list_length(relslist) > 1 ||
					(list_head(relslist) != NULL && list_length(linitial(relslist)) > 1))
				{
					foreach(l, relslist)
					{
						List        *rels = NIL;
						ListCell    *lc;

						rels = lfirst(l);

						foreach(lc, rels)
						{
							Node   *tmp_node;
							tmp_node = scanRTEForColumn(pstate, lfirst(lc), colname,
														cref->location, 0, NULL);
							if (tmp_node != NULL)
							{
								if (found_colname)
									ereport(ERROR,
											(errcode(ERRCODE_AMBIGUOUS_COLUMN),
											errmsg("column reference \"%s\" is ambiguous",
													colname),
											parser_errposition(pstate, cref->location)));
								found_colname = true;
								node = tmp_node;
							}
						}

						if (node != NULL)
							break;
					}

					if (node != NULL)
						break;
				}
			}
			/* Locate the referenced RTE */
			rte = refnameRangeTblEntry(pstate, nspname, relname,
									   cref->location,
									   &levels_up);
			if (rte == NULL)
			{
				crerr = CRERR_NO_RTE;
				break;
			}

			/* Whole-row reference? */
			if (IsA(field2, A_Star))
			{
				node = transformWholeRowRef(pstate, rte, cref->location);
				break;
			}

			Assert(IsA(field2, String));
			colname = strVal(field2);

			/* Try to identify as a column of the RTE */
			node = scanRTEForColumn(pstate, rte, colname, cref->location,
									0, NULL);

            if (node == NULL)
            {
                /* Try it as a function call on the whole row */
                node = transformWholeRowRef(pstate, rte, cref->location);
                node = ParseFuncOrColumn(pstate,
                                         list_make1(makeString(colname)),
                                         list_make1(node),
                                         pstate->p_last_srf,
                                         NULL,
                                         false,
                                         cref->location);
            }
            break;
        }
        case 3:
        {
            Node	   *field1 = (Node *) linitial(cref->fields);
            Node	   *field2 = (Node *) lsecond(cref->fields);
            Node	   *field3 = (Node *) lthird(cref->fields);

            Assert(IsA(field1, String));
            nspname = strVal(field1);
            Assert(IsA(field2, String));
            relname = strVal(field2);

            /* Locate the referenced RTE */
            rte = refnameRangeTblEntry(pstate, nspname, relname,
                                       cref->location,
                                       &levels_up);
            if (rte == NULL)
            {
                crerr = CRERR_NO_RTE;
                break;
            }

            /* Whole-row reference? */
            if (IsA(field3, A_Star))
            {
                node = transformWholeRowRef(pstate, rte, cref->location);
                break;
            }

            Assert(IsA(field3, String));
            colname = strVal(field3);

            /* Try to identify as a column of the RTE */
            node = scanRTEForColumn(pstate, rte, colname, cref->location,
                                    0, NULL);
            if (node == NULL)
            {
                /* Try it as a function call on the whole row */
                node = transformWholeRowRef(pstate, rte, cref->location);
                node = ParseFuncOrColumn(pstate,
                                         list_make1(makeString(colname)),
                                         list_make1(node),
                                         pstate->p_last_srf,
                                         NULL,
                                         false,
                                         cref->location);
            }
            break;
        }
        case 4:
        {
            Node	   *field1 = (Node *) linitial(cref->fields);
            Node	   *field2 = (Node *) lsecond(cref->fields);
            Node	   *field3 = (Node *) lthird(cref->fields);
            Node	   *field4 = (Node *) lfourth(cref->fields);
            char	   *catname;

            Assert(IsA(field1, String));
            catname = strVal(field1);
            Assert(IsA(field2, String));
            nspname = strVal(field2);
            Assert(IsA(field3, String));
            relname = strVal(field3);

            /*
             * We check the catalog name and then ignore it.
             */
            if (pg_strcasecmp(catname, get_database_name(MyDatabaseId)) != 0)
            {
                crerr = CRERR_WRONG_DB;
                break;
            }

            /* Locate the referenced RTE */
            rte = refnameRangeTblEntry(pstate, nspname, relname,
                                       cref->location,
                                       &levels_up);
            if (rte == NULL)
            {
                crerr = CRERR_NO_RTE;
                break;
            }

            /* Whole-row reference? */
            if (IsA(field4, A_Star))
            {
                node = transformWholeRowRef(pstate, rte, cref->location);
                break;
            }

            Assert(IsA(field4, String));
            colname = strVal(field4);

            /* Try to identify as a column of the RTE */
            node = scanRTEForColumn(pstate, rte, colname, cref->location,
                                    0, NULL);
            if (node == NULL)
            {
                /* Try it as a function call on the whole row */
                node = transformWholeRowRef(pstate, rte, cref->location);
                node = ParseFuncOrColumn(pstate,
                                         list_make1(makeString(colname)),
                                         list_make1(node),
                                         pstate->p_last_srf,
                                         NULL,
                                         false,
                                         cref->location);
            }
            break;
        }
        default:
            crerr = CRERR_TOO_MANY; /* too many dotted names */
            break;
    }

    /*
     * Now give the PostParseColumnRefHook, if any, a chance.  We pass the
     * translation-so-far so that it can throw an error if it wishes in the
     * case that it has a conflicting interpretation of the ColumnRef. (If it
     * just translates anyway, we'll throw an error, because we can't undo
     * whatever effects the preceding steps may have had on the pstate.) If it
     * returns NULL, use the standard translation, or throw a suitable error
     * if there is none.
     */
    if (pstate->p_post_columnref_hook != NULL)
    {
        Node	   *hookresult;

        hookresult = pstate->p_post_columnref_hook(pstate, cref, node);
        if (node == NULL)
            node = hookresult;
        else if (hookresult != NULL)
            ereport(ERROR,
                    (errcode(ERRCODE_AMBIGUOUS_COLUMN),
                            errmsg("column reference \"%s\" is ambiguous",
                                   NameListToString(cref->fields)),
                            parser_errposition(pstate, cref->location)));
    }

#ifdef _PG_ORCL_
    /*
	 * select postgres.public.test_tc1_seq.nextval;
	 * select postgres.public.test_tc1_seq.currval;
	 */
	if (NULL == node && crerr != CRERR_WRONG_DB)
	{
		if (ORA_MODE || enable_lightweight_ora_syntax)
			node = transformSequenceColumn(pstate, cref);
	}

	if (node == NULL && ORA_MODE && !try_columnref)
		node = transformColRefAsFunction(pstate, cref);

	if (node == NULL && ORA_MODE)
		node = transformCompositeColumn(pstate, cref);
#endif

    /*
     * Throw error if no translation found.
     */
    if (node == NULL && !try_columnref)
    {
        switch (crerr)
        {
            case CRERR_NO_COLUMN:
                errorMissingColumn(pstate, relname, colname, cref->location);
                break;
            case CRERR_NO_RTE:
                errorMissingRTE(pstate, makeRangeVar(nspname, relname,
                                                     cref->location));
                break;
            case CRERR_WRONG_DB:
                ereport(ERROR,
                        (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                                errmsg("cross-database references are not implemented: %s",
                                       NameListToString(cref->fields)),
                                parser_errposition(pstate, cref->location)));
                break;
            case CRERR_TOO_MANY:
                ereport(ERROR,
                        (errcode(ERRCODE_SYNTAX_ERROR),
                                errmsg("improper qualified name (too many dotted names): %s",
                                       NameListToString(cref->fields)),
                                parser_errposition(pstate, cref->location)));
                break;
        }
    }

    return node;
}

static Node *
transformParamRef(ParseState *pstate, ParamRef *pref)
{
    Node	   *result;

    /*
     * The core parser knows nothing about Params.  If a hook is supplied,
     * call it.  If not, or if the hook returns NULL, throw a generic error.
     */
    if (pstate->p_paramref_hook != NULL)
        result = pstate->p_paramref_hook(pstate, pref);
    else
        result = NULL;

    if (result == NULL)
        ereport(ERROR,
                (errcode(ERRCODE_UNDEFINED_PARAMETER),
                        errmsg("there is no parameter $%d", pref->number),
                        parser_errposition(pstate, pref->location)));

    return result;
}

/* Test whether an a_expr is a plain NULL constant or not */
static bool
exprIsNullConstant(Node *arg)
{
    if (arg && IsA(arg, A_Const))
    {
        A_Const    *con = (A_Const *) arg;

        if (con->val.type == T_Null)
            return true;
    }
    return false;
}

static Node *
transformAExprOp(ParseState *pstate, A_Expr *a)
{
    Node	   *lexpr = a->lexpr;
    Node	   *rexpr = a->rexpr;
    Node	   *result;

    if (operator_precedence_warning)
    {
        int			opgroup;
        const char *opname;

        opgroup = operator_precedence_group((Node *) a, &opname);
        if (opgroup > 0)
            emit_precedence_warnings(pstate, opgroup, opname,
                                     lexpr, rexpr,
                                     a->location);

        /* Look through AEXPR_PAREN nodes so they don't affect tests below */
        while (lexpr && IsA(lexpr, A_Expr) &&
               ((A_Expr *) lexpr)->kind == AEXPR_PAREN)
            lexpr = ((A_Expr *) lexpr)->lexpr;
        while (rexpr && IsA(rexpr, A_Expr) &&
               ((A_Expr *) rexpr)->kind == AEXPR_PAREN)
            rexpr = ((A_Expr *) rexpr)->lexpr;
    }

    /*
     * Special-case "foo = NULL" and "NULL = foo" for compatibility with
     * standards-broken products (like Microsoft's).  Turn these into IS NULL
     * exprs. (If either side is a CaseTestExpr, then the expression was
     * generated internally from a CASE-WHEN expression, and
     * transform_null_equals does not apply.)
     */
    if (Transform_null_equals &&
        list_length(a->name) == 1 &&
        strcmp(strVal(linitial(a->name)), "=") == 0 &&
        (exprIsNullConstant(lexpr) || exprIsNullConstant(rexpr)) &&
        (!IsA(lexpr, CaseTestExpr) &&!IsA(rexpr, CaseTestExpr)))
    {
        NullTest   *n = makeNode(NullTest);

        n->nulltesttype = IS_NULL;
        n->location = a->location;

        if (exprIsNullConstant(lexpr))
            n->arg = (Expr *) rexpr;
        else
            n->arg = (Expr *) lexpr;

        result = transformExprRecurse(pstate, (Node *) n);
    }
    else if (lexpr && IsA(lexpr, RowExpr) &&
             rexpr && IsA(rexpr, SubLink) &&
             ((SubLink *) rexpr)->subLinkType == EXPR_SUBLINK)
    {
        /*
         * Convert "row op subselect" into a ROWCOMPARE sublink. Formerly the
         * grammar did this, but now that a row construct is allowed anywhere
         * in expressions, it's easier to do it here.
         */
        SubLink    *s = (SubLink *) rexpr;

        s->subLinkType = ROWCOMPARE_SUBLINK;
        s->testexpr = lexpr;
        s->operName = a->name;
        s->location = a->location;
        result = transformExprRecurse(pstate, (Node *) s);
    }
    else if (lexpr && IsA(lexpr, RowExpr) &&
             rexpr && IsA(rexpr, RowExpr))
    {
        /* ROW() op ROW() is handled specially */
        lexpr = transformExprRecurse(pstate, lexpr);
        rexpr = transformExprRecurse(pstate, rexpr);

        result = make_row_comparison_op(pstate,
                                        a->name,
                                        castNode(RowExpr, lexpr)->args,
                                        castNode(RowExpr, rexpr)->args,
                                        a->location);
    }
    else
    {
        /* Ordinary scalar operator */
        Node	   *last_srf = pstate->p_last_srf;

        lexpr = transformExprRecurse(pstate, lexpr);
        rexpr = transformExprRecurse(pstate, rexpr);

        result = (Node *) make_op(pstate,
                                  a->name,
                                  lexpr,
                                  rexpr,
                                  last_srf,
                                  a->location);
    }

    return result;
}

static Node *
transformAExprOpAny(ParseState *pstate, A_Expr *a)
{
    Node	   *lexpr = a->lexpr;
    Node	   *rexpr = a->rexpr;

    if (operator_precedence_warning)
        emit_precedence_warnings(pstate, PREC_GROUP_POSTFIX_OP,
                                 strVal(llast(a->name)),
                                 lexpr, NULL,
                                 a->location);

    lexpr = transformExprRecurse(pstate, lexpr);
    rexpr = transformExprRecurse(pstate, rexpr);

    return (Node *) make_scalar_array_op(pstate,
                                         a->name,
                                         true,
                                         lexpr,
                                         rexpr,
                                         a->location);
}

static Node *
transformAExprOpAll(ParseState *pstate, A_Expr *a)
{
    Node	   *lexpr = a->lexpr;
    Node	   *rexpr = a->rexpr;

    if (operator_precedence_warning)
        emit_precedence_warnings(pstate, PREC_GROUP_POSTFIX_OP,
                                 strVal(llast(a->name)),
                                 lexpr, NULL,
                                 a->location);

    lexpr = transformExprRecurse(pstate, lexpr);
    rexpr = transformExprRecurse(pstate, rexpr);

    return (Node *) make_scalar_array_op(pstate,
                                         a->name,
                                         false,
                                         lexpr,
                                         rexpr,
                                         a->location);
}

static Node *
transformAExprDistinct(ParseState *pstate, A_Expr *a)
{
    Node	   *lexpr = a->lexpr;
    Node	   *rexpr = a->rexpr;
    Node	   *result;

    if (operator_precedence_warning)
        emit_precedence_warnings(pstate, PREC_GROUP_INFIX_IS, "IS",
                                 lexpr, rexpr,
                                 a->location);

    /*
     * If either input is an undecorated NULL literal, transform to a NullTest
     * on the other input. That's simpler to process than a full DistinctExpr,
     * and it avoids needing to require that the datatype have an = operator.
     */
    if (exprIsNullConstant(rexpr))
        return make_nulltest_from_distinct(pstate, a, lexpr);
    if (exprIsNullConstant(lexpr))
        return make_nulltest_from_distinct(pstate, a, rexpr);

    lexpr = transformExprRecurse(pstate, lexpr);
    rexpr = transformExprRecurse(pstate, rexpr);

    if (lexpr && IsA(lexpr, RowExpr) &&
        rexpr && IsA(rexpr, RowExpr))
    {
        /* ROW() op ROW() is handled specially */
        result = make_row_distinct_op(pstate, a->name,
                                      (RowExpr *) lexpr,
                                      (RowExpr *) rexpr,
                                      a->location);
    }
    else
    {
        /* Ordinary scalar operator */
        result = (Node *) make_distinct_op(pstate,
                                           a->name,
                                           lexpr,
                                           rexpr,
                                           a->location);
    }

    /*
     * If it's NOT DISTINCT, we first build a DistinctExpr and then stick a
     * NOT on top.
     */
    if (a->kind == AEXPR_NOT_DISTINCT)
        result = (Node *) makeBoolExpr(NOT_EXPR,
                                       list_make1(result),
                                       a->location);

    return result;
}

static void
ora_nullif_args_special_compare(Node *lexpr, Node *rexpr)
{
	/* nullif(expr, udf()) or nullif(udf(), udf()) or nullif(udf(), expr) */
	if (IsA(lexpr, FuncExpr))
	{
		FuncExpr *lexpr_func = (FuncExpr*) lexpr;

		if (lexpr_func->funcid > FirstNormalObjectId)
			elog(ERROR, "inconsistent datatypes");
	}

	if (IsA(rexpr, FuncExpr))
	{
		FuncExpr *rexpr_func = (FuncExpr*)rexpr;

		if (rexpr_func->funcid > FirstNormalObjectId)
			elog(ERROR, "inconsistent datatypes");
	}

	/* second arg is to_char */
	if (IsA(rexpr, FuncExpr))
	{
		FuncExpr *rexpr_func = (FuncExpr *)rexpr;
		Oid funcid = rexpr_func->funcid;

		if (funcid == F_ORCL_TEXT_TOCHAR ||
            funcid == F_ORCL_INT4_TOCHAR ||
            funcid == F_ORCL_FLOAT4_TOCHAR ||
            funcid == F_ORCL_FLOAT8_TOCHAR ||
            funcid == F_ORCL_NUMERIC_TOCHAR ||
            funcid == F_ORCL_INTERVAL_TOCHAR ||
			funcid == F_ORCL_INTERVAL_TYPMODE_TOCHAR ||
			funcid == F_ORCL_TO_CHAR)
			elog(ERROR, "inconsistent datatypes");
	}

	/* arg is column field, nullif(col, col), nullif(expr, col) */
	if ((IsA(lexpr, Var) && IsA(rexpr, Var)) || IsA(rexpr, Var))
		elog(ERROR, "inconsistent datatypes");
}

/*
 * https://docs.opentenbase_ora.com/en/database/opentenbase_ora/opentenbase-ora-database/19/sqlrf/NULLIF.html#GUID-445FC268-7FFA-4850-98C9-D53D88AB2405
 *
 * NULLIF compares expr1 and expr2. If they are equal, then the function returns
 * null. If they are not equal, then the function returns expr1. You cannot
 * specify the literal NULL for expr1.
 *
 * If both arguments are numeric data types, then opentenbase_ora Database determines the
 * argument with the higher numeric precedence, implicitly converts the other
 * argument to that data type, and returns that data type. If the arguments are
 * not numeric, then they must be of the same data type, or opentenbase_ora returns an
 * error.
 */
static void
ora_check_nullif_args(Node *lexpr, Node *rexpr)
{
	if (lexpr == NULL)
		elog(ERROR, "inconsistent datatypes");
	else if (rexpr == NULL)
	{
		/* do nothing */
	}
	else
	{
		Oid ltypId;
		Oid rtypId;
		TYPCATEGORY lctg;
		TYPCATEGORY rctg;

		ltypId = exprType(lexpr);
		lctg = TypeCategory(ltypId);
		rtypId = exprType(rexpr);
		rctg = TypeCategory(rtypId);

		if (lctg != rctg)
		{
			if (!(lctg == TYPCATEGORY_UNKNOWN || rctg == TYPCATEGORY_UNKNOWN))
				ora_nullif_args_special_compare(lexpr, rexpr);

			/* first arg is '', second is not str const. */
			if (IsA(lexpr, Const) && IsA(rexpr, Const))
			{
				Const *lexpr_const = (Const *) lexpr;

				if (lexpr_const->constlen == NULL_TYPE_STR_LEN &&
						(rctg != TYPCATEGORY_UNKNOWN || rctg != TYPCATEGORY_STRING))
					elog(ERROR, "inconsistent datatypes");
			}
			if (lctg == TYPCATEGORY_NUMERIC ||
				lctg == TYPCATEGORY_DATETIME ||
				lctg == TYPCATEGORY_TIMESPAN)
			{
				if (IsA(rexpr, Const))
				{
					Const *rexpr_const =(Const *) rexpr;

					if (!rexpr_const->constisnull || rexpr_const->constlen == NULL_TYPE_STR_LEN)
						elog(ERROR, "inconsistent datatypes");
				}
			}
			if (rctg == TYPCATEGORY_NUMERIC ||
				rctg == TYPCATEGORY_DATETIME ||
				rctg == TYPCATEGORY_TIMESPAN)
			{
				if (IsA(lexpr, Const))
				{
					Const *lexpr_const =(Const *) lexpr;

					if (!lexpr_const->constisnull)
						elog(ERROR, "inconsistent datatypes");
				}
			}
		}
	}
}

static Node *
transformAExprNullIf(ParseState *pstate, A_Expr *a)
{
    Node	   *lexpr = transformExprRecurse(pstate, a->lexpr);
    Node	   *rexpr = transformExprRecurse(pstate, a->rexpr);
    OpExpr	   *result;

	if (ORA_MODE)
		ora_check_nullif_args(lexpr, rexpr);

    result = (OpExpr *) make_op(pstate,
                                a->name,
                                lexpr,
                                rexpr,
                                pstate->p_last_srf,
                                a->location);

    /*
     * The comparison operator itself should yield boolean ...
     */
    if (result->opresulttype != BOOLOID)
        ereport(ERROR,
                (errcode(ERRCODE_DATATYPE_MISMATCH),
                        errmsg("NULLIF requires = operator to yield boolean"),
                        parser_errposition(pstate, a->location)));
    if (result->opretset)
        ereport(ERROR,
                (errcode(ERRCODE_DATATYPE_MISMATCH),
                        /* translator: %s is name of a SQL construct, eg NULLIF */
                        errmsg("%s must not return a set", "NULLIF"),
                        parser_errposition(pstate, a->location)));

    /*
     * ... but the NullIfExpr will yield the first operand's type.
     */
    result->opresulttype = exprType((Node *) linitial(result->args));
	/* left '' should be treated specially */
	if (ORA_MODE && IsA(lexpr, Const))
	{
		Const *lexpr_const = (Const *)lexpr;
		Node *left = linitial(result->args);

		if (lexpr_const->constlen == NULL_TYPE_STR_LEN && IsA(left, Const))
		{
			Const *left_const = (Const *) left;

			if (left_const->constisnull)
				left_const->constlen = NULLIF_FLAG_LEN;
		}
	}

    /*
     * We rely on NullIfExpr and OpExpr being the same struct
     */
    NodeSetTag(result, T_NullIfExpr);

    return (Node *) result;
}

/*
 * Checking an expression for match to a list of type names. Will result
 * in a boolean constant node.
 */
static Node *
transformAExprOf(ParseState *pstate, A_Expr *a)
{
    Node	   *lexpr = a->lexpr;
    Const	   *result;
    ListCell   *telem;
    Oid			ltype,
            rtype;
    bool		matched = false;

    if (operator_precedence_warning)
        emit_precedence_warnings(pstate, PREC_GROUP_POSTFIX_IS, "IS",
                                 lexpr, NULL,
                                 a->location);

    lexpr = transformExprRecurse(pstate, lexpr);

    ltype = exprType(lexpr);
    foreach(telem, (List *) a->rexpr)
    {
        rtype = typenameTypeId(pstate, lfirst(telem));
        matched = (rtype == ltype);
        if (matched)
            break;
    }

    /*
     * We have two forms: equals or not equals. Flip the sense of the result
     * for not equals.
     */
    if (strcmp(strVal(linitial(a->name)), "<>") == 0)
        matched = (!matched);

    result = (Const *) makeBoolConst(matched, false);

    /* Make the result have the original input's parse location */
    result->location = exprLocation((Node *) a);

    return (Node *) result;
}

static Node *
transformAExprIn(ParseState *pstate, A_Expr *a)
{
    Node	   *result = NULL;
    Node	   *lexpr;
    List	   *rexprs;
    List	   *rvars;
    List	   *rnonvars;
    bool		useOr;
    ListCell   *l;

    /*
     * If the operator is <>, combine with AND not OR.
     */
    if (strcmp(strVal(linitial(a->name)), "<>") == 0)
        useOr = false;
    else
        useOr = true;

    if (operator_precedence_warning)
        emit_precedence_warnings(pstate,
                                 useOr ? PREC_GROUP_IN : PREC_GROUP_NOT_IN,
                                 "IN",
                                 a->lexpr, NULL,
                                 a->location);

    /*
     * We try to generate a ScalarArrayOpExpr from IN/NOT IN, but this is only
     * possible if there is a suitable array type available.  If not, we fall
     * back to a boolean condition tree with multiple copies of the lefthand
     * expression.  Also, any IN-list items that contain Vars are handled as
     * separate boolean conditions, because that gives the planner more scope
     * for optimization on such clauses.
     *
     * First step: transform all the inputs, and detect whether any contain
     * Vars.
     */
    lexpr = transformExprRecurse(pstate, a->lexpr);
    rexprs = rvars = rnonvars = NIL;
    foreach(l, (List *) a->rexpr)
    {
        Node	   *rexpr = transformExprRecurse(pstate, lfirst(l));

        rexprs = lappend(rexprs, rexpr);
        if (contain_vars_of_level(rexpr, 0))
            rvars = lappend(rvars, rexpr);
        else
            rnonvars = lappend(rnonvars, rexpr);
    }

    /*
     * ScalarArrayOpExpr is only going to be useful if there's more than one
     * non-Var righthand item.
     */
    if (list_length(rnonvars) > 1)
    {
        List	   *allexprs;
        Oid			scalar_type;
        Oid			array_type;

        /*
         * Try to select a common type for the array elements.  Note that
         * since the LHS' type is first in the list, it will be preferred when
         * there is doubt (eg, when all the RHS items are unknown literals).
         *
         * Note: use list_concat here not lcons, to avoid damaging rnonvars.
         */
        allexprs = list_concat(list_make1(lexpr), rnonvars);
        scalar_type = select_common_type(pstate, allexprs, NULL, NULL);

        /*
         * Do we have an array type to use?  Aside from the case where there
         * isn't one, we don't risk using ScalarArrayOpExpr when the common
         * type is RECORD, because the RowExpr comparison logic below can cope
         * with some cases of non-identical row types.
         */
        if (OidIsValid(scalar_type) && scalar_type != RECORDOID)
            array_type = get_array_type(scalar_type);
        else
            array_type = InvalidOid;
        if (array_type != InvalidOid)
        {
            /*
             * OK: coerce all the right-hand non-Var inputs to the common type
             * and build an ArrayExpr for them.
             */
            List	   *aexprs;
            ArrayExpr  *newa;

            aexprs = NIL;
            foreach(l, rnonvars)
            {
                Node	   *rexpr = (Node *) lfirst(l);

                rexpr = coerce_to_common_type(pstate, rexpr,
                                              scalar_type,
                                              "IN");
                aexprs = lappend(aexprs, rexpr);
            }
            newa = makeNode(ArrayExpr);
            newa->array_typeid = array_type;
            /* array_collid will be set by parse_collate.c */
            newa->element_typeid = scalar_type;
            newa->elements = aexprs;
            newa->multidims = false;
            newa->location = -1;

            result = (Node *) make_scalar_array_op(pstate,
                                                   a->name,
                                                   useOr,
                                                   lexpr,
                                                   (Node *) newa,
                                                   a->location);

            /* Consider only the Vars (if any) in the loop below */
            rexprs = rvars;
        }
    }

    /*
     * Must do it the hard way, ie, with a boolean expression tree.
     */
    foreach(l, rexprs)
    {
        Node	   *rexpr = (Node *) lfirst(l);
        Node	   *cmp;

        if (IsA(lexpr, RowExpr) &&
            IsA(rexpr, RowExpr))
        {
            /* ROW() op ROW() is handled specially */
            cmp = make_row_comparison_op(pstate,
                                         a->name,
                                         copyObject(((RowExpr *) lexpr)->args),
                                         ((RowExpr *) rexpr)->args,
                                         a->location);
        }
        else
        {
            /* Ordinary scalar operator */
            cmp = (Node *) make_op(pstate,
                                   a->name,
                                   copyObject(lexpr),
                                   rexpr,
                                   pstate->p_last_srf,
                                   a->location);
        }

        cmp = coerce_to_boolean(pstate, cmp, "IN");
        if (result == NULL)
            result = cmp;
        else
            result = (Node *) makeBoolExpr(useOr ? OR_EXPR : AND_EXPR,
                                           list_make2(result, cmp),
                                           a->location);
    }

    return result;
}

static Node *
transformAExprBetween(ParseState *pstate, A_Expr *a)
{
    Node	   *aexpr;
    Node	   *bexpr;
    Node	   *cexpr;
    Node	   *result;
    Node	   *sub1;
    Node	   *sub2;
    List	   *args;

    /* Deconstruct A_Expr into three subexprs */
    aexpr = a->lexpr;
    args = castNode(List, a->rexpr);
    Assert(list_length(args) == 2);
    bexpr = (Node *) linitial(args);
    cexpr = (Node *) lsecond(args);

    if (operator_precedence_warning)
    {
        int			opgroup;
        const char *opname;

        opgroup = operator_precedence_group((Node *) a, &opname);
        emit_precedence_warnings(pstate, opgroup, opname,
                                 aexpr, cexpr,
                                 a->location);
        /* We can ignore bexpr thanks to syntactic restrictions */
        /* Wrap subexpressions to prevent extra warnings */
        aexpr = (Node *) makeA_Expr(AEXPR_PAREN, NIL, aexpr, NULL, -1);
        bexpr = (Node *) makeA_Expr(AEXPR_PAREN, NIL, bexpr, NULL, -1);
        cexpr = (Node *) makeA_Expr(AEXPR_PAREN, NIL, cexpr, NULL, -1);
    }

    /*
     * Build the equivalent comparison expression.  Make copies of
     * multiply-referenced subexpressions for safety.  (XXX this is really
     * wrong since it results in multiple runtime evaluations of what may be
     * volatile expressions ...)
     *
     * Ideally we would not use hard-wired operators here but instead use
     * opclasses.  However, mixed data types and other issues make this
     * difficult:
     * http://archives.postgresql.org/pgsql-hackers/2008-08/msg01142.php
     */
    switch (a->kind)
    {
        case AEXPR_BETWEEN:
            args = list_make2(makeSimpleA_Expr(AEXPR_OP, ">=",
                                               aexpr, bexpr,
                                               a->location),
                              makeSimpleA_Expr(AEXPR_OP, "<=",
                                               copyObject(aexpr), cexpr,
                                               a->location));
            result = (Node *) makeBoolExpr(AND_EXPR, args, a->location);
            break;
        case AEXPR_NOT_BETWEEN:
            args = list_make2(makeSimpleA_Expr(AEXPR_OP, "<",
                                               aexpr, bexpr,
                                               a->location),
                              makeSimpleA_Expr(AEXPR_OP, ">",
                                               copyObject(aexpr), cexpr,
                                               a->location));
            result = (Node *) makeBoolExpr(OR_EXPR, args, a->location);
            break;
        case AEXPR_BETWEEN_SYM:
            args = list_make2(makeSimpleA_Expr(AEXPR_OP, ">=",
                                               aexpr, bexpr,
                                               a->location),
                              makeSimpleA_Expr(AEXPR_OP, "<=",
                                               copyObject(aexpr), cexpr,
                                               a->location));
            sub1 = (Node *) makeBoolExpr(AND_EXPR, args, a->location);
            args = list_make2(makeSimpleA_Expr(AEXPR_OP, ">=",
                                               copyObject(aexpr), copyObject(cexpr),
                                               a->location),
                              makeSimpleA_Expr(AEXPR_OP, "<=",
                                               copyObject(aexpr), copyObject(bexpr),
                                               a->location));
            sub2 = (Node *) makeBoolExpr(AND_EXPR, args, a->location);
            args = list_make2(sub1, sub2);
            result = (Node *) makeBoolExpr(OR_EXPR, args, a->location);
            break;
        case AEXPR_NOT_BETWEEN_SYM:
            args = list_make2(makeSimpleA_Expr(AEXPR_OP, "<",
                                               aexpr, bexpr,
                                               a->location),
                              makeSimpleA_Expr(AEXPR_OP, ">",
                                               copyObject(aexpr), cexpr,
                                               a->location));
            sub1 = (Node *) makeBoolExpr(OR_EXPR, args, a->location);
            args = list_make2(makeSimpleA_Expr(AEXPR_OP, "<",
                                               copyObject(aexpr), copyObject(cexpr),
                                               a->location),
                              makeSimpleA_Expr(AEXPR_OP, ">",
                                               copyObject(aexpr), copyObject(bexpr),
                                               a->location));
            sub2 = (Node *) makeBoolExpr(OR_EXPR, args, a->location);
            args = list_make2(sub1, sub2);
            result = (Node *) makeBoolExpr(AND_EXPR, args, a->location);
            break;
        default:
        elog(ERROR, "unrecognized A_Expr kind: %d", a->kind);
            result = NULL;		/* keep compiler quiet */
            break;
    }

    return transformExprRecurse(pstate, result);
}

static Node *
transformBoolExpr(ParseState *pstate, BoolExpr *a)
{
    List	   *args = NIL;
    const char *opname;
    ListCell   *lc;

    switch (a->boolop)
    {
        case AND_EXPR:
            opname = "AND";
            break;
        case OR_EXPR:
            opname = "OR";
            break;
        case NOT_EXPR:
            opname = "NOT";
            break;
        default:
        elog(ERROR, "unrecognized boolop: %d", (int) a->boolop);
            opname = NULL;		/* keep compiler quiet */
            break;
    }

    foreach(lc, a->args)
    {
        Node	   *arg = (Node *) lfirst(lc);

        arg = transformExprRecurse(pstate, arg);
        arg = coerce_to_boolean(pstate, arg, opname);
        args = lappend(args, arg);
    }

    return (Node *) makeBoolExpr(a->boolop, args, a->location);
}

static void
convert_to_opentenbase_ora_func(FuncCall *fn, List *targs)
{
	char	*schemaname = NULL;
	char	*funcname = NULL;

	/* deconstruct the name list */
	DeconstructQualifiedName(fn->funcname, &schemaname, &funcname);

	if (schemaname != NULL)
		return;

	/* convert system funcCall to opentenbase_ora funcCall */
	if (
        pg_strcasecmp(funcname, "sys_guid") == 0 ||
		pg_strcasecmp(funcname, "regexp_replace") == 0||
        pg_strcasecmp(funcname, "nextval") == 0 ||
        pg_strcasecmp(funcname, "currval") == 0 ||
        pg_strcasecmp(funcname, "setval") == 0 ||
        pg_strcasecmp(funcname, "lastval") == 0
        )
	{
		fn->funcname = OpenTenBaseOraFuncName(funcname);
	}

	if (strcasecmp(funcname, "approx_count_distinct") == 0)
	{
		fn->funcname = list_make1(makeString("COUNT"));
		fn->agg_distinct = true;
	}

}

static void
process_opentenbase_ora_within_group(FuncCall *fn)
{
	char	*schemaname = NULL;
	char	*funcname = NULL;

	/* deconstruct the name list */
	DeconstructQualifiedName(fn->funcname, &schemaname, &funcname);

	if (schemaname != NULL)
		return;

	/* select listagg(col, ',') within group (order by colxxx) from table_name
	 to
	  select listagg(col, ',' order by colxxx) from table_name
	 by set agg_within_group = 0
	*/
	if (pg_strcasecmp(funcname, "listagg") == 0 ||
		pg_strcasecmp(funcname, "list_agg") == 0)
	{
		fn->agg_within_group = 0;
	}
}

static int
GetOraPrecedence(Oid typoid)
{
    int i;
    for (i = 0; i < NumOraPrecedenceMap; i++)
    {
        if (typoid == OraPrecedenceMap[i].typoid)
            return OraPrecedenceMap[i].level;
    }
    return LV_LEAST;
}

static int
OraTypePerferred(Oid typoid1, Oid typoid2)
{
    return GetOraPrecedence(typoid1) - GetOraPrecedence(typoid2);
}

static List *
transformNvlArgs(ParseState *pstate, List *args)
{
	List *result = NIL;
	Node *larg = NULL;
	Node *rarg = NULL;
	Node *cnode = NULL;
	Oid   ltypid = InvalidOid;
	Oid   rtypid = InvalidOid;
	int32 ltypmod = -1;
	int32 rtypmod = -1;
	Oid   default_type = TEXTOID;

    if (pstate == NULL || args == NULL || list_length(args) != 2)
    {
        return args;
    }

    larg = (Node *)linitial(args);
    rarg = (Node *)lsecond(args);
    ltypid = exprType(larg);
    rtypid = exprType(rarg);

    /*
     * Ensure that the conversion to text type is possible to prevent issues with incompatible
     * types and enable proper output.
     * eg. SELECT nvl('Hello World' ,1);
     */
    if ((ltypid != UNKNOWNOID &&
		 can_coerce_type(1, &ltypid, &default_type, COERCION_IMPLICIT, false) &&
         rtypid == UNKNOWNOID) ||
        (ltypid == UNKNOWNOID &&
         rtypid != UNKNOWNOID &&
		 can_coerce_type(1, &rtypid, &default_type, COERCION_IMPLICIT, false)))
    {
        return args;
    }

	if (ltypid == UNKNOWNOID)
		larg = coerce_to_common_type(pstate, larg, default_type, "NVL");
    if (rtypid == UNKNOWNOID)
		rarg = coerce_to_common_type(pstate, rarg, default_type, "NVL");
    
    /* update args type */
    ltypid = exprType(larg);
    rtypid = exprType(rarg);
    ltypmod = exprTypmod(larg);
    rtypmod = exprTypmod(rarg);

    if (ltypid == rtypid)
    {
        return list_make2(larg, rarg);
    }

    /*
    * If expr1 is character data, then opentenbase_ora Database converts expr2 to the
    * data type of expr1 before comparing them and returns VARCHAR2 in the
    * character set of expr1.
    *
    * If expr1 is numeric, then opentenbase_ora Database determines which argument has
    * the highest numeric precedence, implicitly converts the other argument
    * to that data type, and returns that data type
    */
    if (TypeCategory(ltypid) == TYPCATEGORY_STRING ||
            OraTypePerferred(ltypid, rtypid) > 0)
    {
		/*
		 * We will return text type to avoid length problem
		 * while expr1 is character data and can be implicit coerced to
		 * text.
		 */
		if (TypeCategory(ltypid) == TYPCATEGORY_STRING)
		{
			Node *tmp_node = NULL;

			/* Try to implicit coerce to text */
			tmp_node = coerce_to_target_type(pstate,
											 larg,
											 ltypid,
											 TEXTOID,
											 -1,
											 COERCION_IMPLICIT,
											 COERCE_IMPLICIT_CAST,
											 -1);
			if (tmp_node)
			{
				larg = tmp_node;
				ltypid = exprType(larg);
				ltypmod = exprTypmod(larg);
			}
		}

	if (can_coerce_type(1, &rtypid, &ltypid, COERCION_EXPLICIT, false))
        {
            /* Ensure the length remains unchanged. */
            cnode = coerce_to_target_type(pstate,
                                          rarg,
                                          rtypid,
                                          ltypid,
                                          enable_right_nvl_typmod? ltypmod : rtypmod,
                                          COERCION_EXPLICIT,
                                          COERCE_EXPLICIT_CAST,
                                          -1);
            if (!cnode)
                cnode = rarg;
            result = lappend(result, larg);
            result = lappend(result, cnode);

            return result;
        }
        return list_make2(larg, rarg);
    }

    /*
    * right expr has highest numeric precedence
    */
    if (OraTypePerferred(ltypid, rtypid) < 0)
    {
	if (can_coerce_type(1, &ltypid, &rtypid, COERCION_EXPLICIT, false))
        {
            /* Ensure the length remains unchanged. */
            cnode = coerce_to_target_type(pstate,
                                          larg,
                                          ltypid,
                                          rtypid,
                                          enable_right_nvl_typmod? rtypmod : ltypmod,
                                          COERCION_EXPLICIT,
                                          COERCE_EXPLICIT_CAST,
                                          -1);
            if (!cnode)
                cnode = larg;
            result = lappend(result, cnode);
            result = lappend(result, rarg);

            return result;
        }
        return list_make2(larg, rarg);
    }

    /*
    * left expr and right expr have the same numeric precedence
    * try to implicitly converts to the data type of the right expr.
    */
	if (can_coerce_type(1, &ltypid, &rtypid, COERCION_IMPLICIT, false))
    {
        /* Ensure the length remains unchanged. */
        cnode = coerce_to_target_type(pstate,
                                      larg,
                                      ltypid,
                                      rtypid,
                                      enable_right_nvl_typmod? rtypmod : ltypmod,
                                      COERCION_IMPLICIT,
                                      COERCE_IMPLICIT_CAST,
                                      -1);
        if (!cnode)
            cnode = larg;
        result = lappend(result, cnode);
        result = lappend(result, rarg);

        return result;
    }

    /*
    * left expr and right expr have the same numeric precedence
    * try to implicitly converts to the data type of the left expr.
    */
	if (can_coerce_type(1, &rtypid, &ltypid, COERCION_EXPLICIT, false))
    {
        /* Ensure the length remains unchanged. */
        cnode = coerce_to_target_type(pstate,
                                      rarg,
                                      rtypid,
                                      ltypid,
                                      enable_right_nvl_typmod? ltypmod : rtypmod,
                                      COERCION_EXPLICIT,
                                      COERCE_EXPLICIT_CALL,
                                      -1);
        if (!cnode)
            cnode = rarg;
        result = lappend(result, larg);
        result = lappend(result, cnode);

        return result;
    }

    return list_make2(larg, rarg);
}

static List *
transformNvl2Args(ParseState *pstate, List *args)
{
    List *result = NIL;
    void *arg1 = NULL,
         *arg2 = NULL,
         *arg3 = NULL;

    if (pstate == NULL || args == NULL || list_length(args) != 3) {
        return args;
    }

    arg1 = linitial(args);
    arg2 = lsecond(args);
    arg3 = lthird(args);

    if (exprType(arg2) == UNKNOWNOID)
        arg2 = coerce_to_common_type(pstate, (Node *)arg2, TEXTOID, "NVL2");
    if (exprType(arg3) == UNKNOWNOID)
        arg3 = coerce_to_common_type(pstate, (Node *)arg3, TEXTOID, "NVL2");

    result = lcons(arg1, transformNvlArgs(pstate, list_make2(arg2, arg3)));

    return result;
}

static List *
transformReplaceArgs(ParseState *pstate, List *args)
{
	Node *arg1 = NULL;
	Node *arg2 = NULL;
	Node *arg3 = NULL;
	Node *carg1 = NULL;
	Oid   typid1 = InvalidOid;
	int32 typmod1 = -1;
	Oid   default_type = TEXTOID;

	if (!enable_lightweight_ora_syntax || pstate == NULL || args == NULL || list_length(args) != 3)
	{
		return args;
	}

	arg1 = (Node *) linitial(args);
	arg2 = (Node *) lsecond(args);
	arg3 = (Node *) lthird(args);
	typid1 = exprType(arg1);

	/*
	 * When the input parameter is of time type, it is automatically converted to text type by default.
	 * eg. select replace((date '20221010'+1)::date,'-','');
	 */
	if (!((typid1 == DATEOID ||
           typid1 == TIMESTAMPOID ||
           typid1 == TIMESTAMPTZOID) &&
	can_coerce_type(1, &typid1, &default_type, COERCION_EXPLICIT, false)))
	{
		return args;
	}

	carg1 = coerce_to_target_type(pstate,
	                              arg1,
	                              typid1,
	                              default_type,
	                              typmod1,
	                              COERCION_EXPLICIT,
	                              COERCE_EXPLICIT_CAST,
	                              -1);
	if (!carg1)
		carg1 = arg1;

	return list_make3(carg1, arg2, arg3);
}

static List *
transformSubstrArgs(ParseState *pstate, List *args)
{
	Node *arg1 = NULL;
	Node *arg2 = NULL;
	Node *arg3 = NULL;
	Node *carg1 = NULL;
	Oid   typid1 = InvalidOid;
	int32 default_typmod = -1;
	Oid   default_type = TEXTOID;

	if (!enable_lightweight_ora_syntax ||
	    pstate == NULL ||
	    args == NULL ||
	    (list_length(args) != 2 && list_length(args) != 3))
	{
		return args;
	}

	arg1 = (Node *) linitial(args);
	typid1 = exprType(arg1);
	arg2 = (Node *) lsecond(args);

	if (list_length(args) == 3)
		arg3 = (Node *) lthird(args);

	/*
	 * When the input parameter is of time type, it is automatically converted to text type by
	 * default. eg. select substr(current_date, 2);
	 */
	if (!((typid1 == DATEOID ||
	       typid1 == TIMESTAMPOID ||
	       typid1 == TIMESTAMPTZOID) &&
	      can_coerce_type(1, &typid1, &default_type, COERCION_EXPLICIT, false)))
	{
		return args;
	}

	carg1 = coerce_to_target_type(pstate,
	                              arg1,
	                              typid1,
	                              default_type,
	                              default_typmod,
	                              COERCION_EXPLICIT,
	                              COERCE_EXPLICIT_CAST,
	                              -1);
	if (!carg1)
		carg1 = arg1;

	if (list_length(args) == 3)
		return list_make3(carg1, arg2, arg3);
	else
		return list_make2(carg1, arg2);
}

static List *
transformStringAggArgs(ParseState *pstate, List *args)
{
	Node *arg1 = NULL;
	Node *arg2 = NULL;
	Node *carg1 = NULL;
	Node *carg2 = NULL;
	Oid   typid1 = InvalidOid;
	Oid   typid2 = InvalidOid;
	int32 default_typmod = -1;
	Oid   default_type = TEXTOID;

	if (!enable_lightweight_ora_syntax || pstate == NULL || args == NULL || list_length(args) != 2)
	{
		return args;
	}

	arg1 = (Node *) linitial(args);
	typid1 = exprType(arg1);
	arg2 = (Node *) lsecond(args);
	typid2 = exprType(arg2);

	/*
	 * When the input parameter is of time type, it is automatically converted to text type by
	 * default. eg. select string_agg(current_date, null)||null from dual;
	 */
	if (!((typid1 == DATEOID ||
	       typid1 == TIMESTAMPOID ||
	       typid1 == TIMESTAMPTZOID) &&
	      can_coerce_type(1, &typid1, &default_type, COERCION_EXPLICIT, false)))
	{
		carg1 = arg1;
	}
	else
	{
		carg1 = coerce_to_target_type(pstate,
		                              arg1,
		                              typid1,
		                              default_type,
		                              default_typmod,
		                              COERCION_EXPLICIT,
		                              COERCE_EXPLICIT_CAST,
		                              -1);
		if (!carg1)
			carg1 = arg1;
	}

	if (!((typid2 == DATEOID ||
	       typid2 == TIMESTAMPOID ||
	       typid2 == TIMESTAMPTZOID) &&
	      can_coerce_type(1, &typid2, &default_type, COERCION_EXPLICIT, false)))
	{
		carg2 = arg2;
	}
	else
	{
		carg2 = coerce_to_target_type(pstate,
		                              arg2,
		                              typid2,
		                              default_type,
		                              default_typmod,
		                              COERCION_EXPLICIT,
		                              COERCE_EXPLICIT_CAST,
		                              -1);
		if (!carg2)
			carg2 = arg2;
	}

	if (carg1 == arg1 && carg2 == arg2)
		return args;
	else
		return list_make2(carg1, carg2);
}

static List *
transformDumpArgs(ParseState *pstate, List *args)
{
    List *result = NIL;
    void *arg1 = NULL;

    if (pstate == NULL || args == NULL || list_length(args) < 1) {
        return args;
    }

    arg1 = linitial(args);

    if (exprType(arg1) == UNKNOWNOID)
        arg1 = coerce_to_common_type(pstate, (Node *)arg1, TEXTOID, "orcl_dump");

    result = lcons(arg1, list_delete_first(args));

    return result;
}

/*
 * lpad(expr1, int32, expr2) or rpad(arg1, int32, arg3).
 * Both expr1 and expr2 can be any of character data types
 * and numeric data types that can be converted to TEXT.
 */
static List *
transformPadArgs(ParseState *pstate, List *args)
{
	List   *result = NIL;
	void   *arg1 = NULL;
	void   *arg2 = NULL;
	void   *arg3 = NULL;
	Oid     type1 = InvalidOid;
	Oid     type2 = InvalidOid;
	Oid     type3 = InvalidOid;
	Oid     target1TypeOid = TEXTOID;
	Oid     target2TypeOid = NUMERICOID;

	if (pstate == NULL || args == NULL || list_length(args) < 2)
	{
		return args;
	}

	arg1  = linitial(args);
	type1 = exprType(arg1);
	arg2  = lsecond(args);
	type2  = exprType(arg2);
	if (list_length(args) == 3)
	{
		arg3  = lthird(args);
		type3 = exprType(arg3);
	}

	/* convert arg1 to text. */
	if (OidIsValid(type1) && type1 != target1TypeOid)
	{
		if (can_coerce_type(1, &type1, &target1TypeOid, COERCION_EXPLICIT, false))
		{
			arg1 = coerce_to_target_type(pstate,
										(Node *) arg1,
										type1,
										target1TypeOid,
										-1,
										COERCION_EXPLICIT,
										COERCE_EXPLICIT_CAST,
										-1);
		}
	}

	result = list_make1(arg1);

	/* convert arg2 to numeric. */
	if (OidIsValid(type2) && type2 != target2TypeOid)
	{
		if (can_coerce_type(1, &type2, &target2TypeOid, COERCION_EXPLICIT, false))
		{
			arg2 = coerce_to_target_type(pstate,
										(Node *) arg2,
										type2,
										target2TypeOid,
										-1,
										COERCION_EXPLICIT,
										COERCE_EXPLICIT_CAST,
										-1);
		}
	}
	result = lappend(result, arg2);

	/* convert arg3 to text if exists. */
	if (OidIsValid(type3) && type3 != target1TypeOid)
	{
		if (can_coerce_type(1, &type3, &target1TypeOid, COERCION_EXPLICIT, false))
		{
			arg3 = coerce_to_target_type(pstate,
										(Node *) arg3,
										type3,
										target1TypeOid,
										-1,
										COERCION_EXPLICIT,
										COERCE_EXPLICIT_CAST,
										-1);
		}
	}

	if (arg3)
	{
		result = lappend(result, arg3);
	}

	return result;
}

/*
 * instr(str, substr, start, num).
 * Both str and substr can be any of character data types
 * and numeric data types that can be converted to TEXT.
 * And Both start and num can be any of character data types
 * and numeric data types that can be converted to int4 explicitly.
 */
static List *
transformInstrArgs(ParseState *pstate, List *args)
{
	List   *result = NIL;
	void   *arg1 = NULL;
	void   *arg2 = NULL;
	void   *arg3 = NULL;
	void   *arg4 = NULL;
	Oid     type1 = InvalidOid;
	Oid     type2 = InvalidOid;
	Oid     type3 = InvalidOid;
	Oid     type4 = InvalidOid;
	Oid     target1TypeOid = TEXTOID;
	Oid     target2TypeOid = NUMERICOID;

	if (pstate == NULL || args == NULL || list_length(args) < 2)
	{
		return args;
	}

	arg1   = linitial(args);
	type1  = exprType(arg1);
	arg2   = lsecond(args);
	type2  = exprType(arg2);
	if (list_length(args) > 2)
	{
		arg3   = lthird(args);
		type3  = exprType(arg3);
	}
	if (list_length(args) > 3)
	{
		arg4  = lfourth(args);
		type4 = exprType(arg4);
	}

	/* convert arg1 to text. */
	if (OidIsValid(type1) && type1 != target1TypeOid)
	{
		if (can_coerce_type(1, &type1, &target1TypeOid, COERCION_EXPLICIT, false))
		{
			arg1 = coerce_to_target_type(pstate,
										(Node *) arg1,
										type1,
										target1TypeOid,
										-1,
										COERCION_EXPLICIT,
										COERCE_EXPLICIT_CAST,
										-1);
		}
	}
	result = list_make1(arg1);

	/* convert arg2 to text. */
	if (OidIsValid(type2) && type2 != target1TypeOid)
	{
		if (can_coerce_type(1, &type2, &target1TypeOid, COERCION_EXPLICIT, false))
		{
			arg2 = coerce_to_target_type(pstate,
										(Node *) arg2,
										type2,
										target1TypeOid,
										-1,
										COERCION_EXPLICIT,
										COERCE_EXPLICIT_CAST,
										-1);
		}
	}
	result = lappend(result, arg2);

	/* convert arg3 to numeric. */
	if (OidIsValid(type3) && type3 != target2TypeOid)
	{
		if (can_coerce_type(1, &type3, &target2TypeOid, COERCION_EXPLICIT, false))
		{
			arg3 = coerce_to_target_type(pstate,
										(Node *) arg3,
										type3,
										target2TypeOid,
										-1,
										COERCION_EXPLICIT,
										COERCE_EXPLICIT_CAST,
										-1);
		}
	}
	if (arg3)
	{
		result = lappend(result, arg3);
	}

	/* convert arg4 to numeric if exists. */
	if (OidIsValid(type4) && type4 != target2TypeOid)
	{
		if (can_coerce_type(1, &type4, &target2TypeOid, COERCION_EXPLICIT, false))
		{
			arg4 = coerce_to_target_type(pstate,
										(Node *) arg4,
										type4,
										target2TypeOid,
										-1,
										COERCION_EXPLICIT,
										COERCE_EXPLICIT_CAST,
										-1);
		}
	}

	if (arg4)
	{
		result = lappend(result, arg4);
	}

	return result;
}

static List *
transformConcatArgs(ParseState *pstate, List *args)
{
    List        *result = NIL;
    ListCell    *lc = NULL;
    
    if (pstate == NULL || args == NIL)
    {
        return args;
    }

    foreach(lc, args)
    {
        Node    *arg = lfirst(lc);
        Oid     type = exprType(arg);
        Oid     targetType = TEXTOID;
	if (type == INTERVALOID && can_coerce_type(1, &type, &targetType, COERCION_EXPLICIT, false))
        {
            arg = coerce_to_target_type(pstate,
                                        arg,
                                        type,
                                        targetType,
                                        -1,
                                        COERCION_EXPLICIT,
                                        COERCE_EXPLICIT_CAST,
                                        -1);
        }
        result = lappend(result, arg);
    }
    return result;
}

static List *
transformLowerArgs(ParseState *pstate, List *args, List	**orig_types)
{
	List       *result = NIL;
	ListCell   *lc = NULL;

	if (pstate == NULL || args == NIL)
	{
		return args;
	}

	foreach(lc, args)
	{
		Node   *arg = lfirst(lc);
		Oid     type = exprType(arg);
		Oid     targetType = TEXTOID;
		arg = coerce_to_target_type(pstate,
									arg,
									type,
									targetType,
									-1,
									COERCION_EXPLICIT,
									COERCE_EXPLICIT_CAST,
									-1);
		result = lappend(result, arg);

		if (orig_types)
			*orig_types = lappend_oid(*orig_types, type);
	}
	return result;
}

static List *
transformUpperArgs(ParseState *pstate, List *args, List	**orig_types)
{
	List       *result = NIL;
	ListCell   *lc = NULL;

	if (pstate == NULL || args == NIL)
	{
		return args;
	}

	foreach(lc, args)
	{
		Node   *arg = lfirst(lc);
		Oid     type = exprType(arg);
		Oid     targetType = TEXTOID;
		arg = coerce_to_target_type(pstate,
									arg,
									type,
									targetType,
									-1,
									COERCION_EXPLICIT,
									COERCE_EXPLICIT_CAST,
									-1);
		result = lappend(result, arg);

		if (orig_types)
			*orig_types = lappend_oid(*orig_types, type);
	}
	return result;
}

static List *
transformOraHashArgs(ParseState *pstate, List *args)
{
	List *result = NIL;
	void *arg1 = NULL;

	if (pstate == NULL || args == NULL || list_length(args) < 1)
	{
		return args;
	}

	arg1 = linitial(args);

	if (exprType(arg1) == UNKNOWNOID)
	{
		arg1 = coerce_to_common_type(pstate, (Node *)arg1, TEXTOID, "ora_hash");
	}

	result = lcons(arg1, list_delete_first(args));

	return result;
}

/*
 * Transform mod args to numeric
 */
static List *
transformModArgs(ParseState *pstate, List *args)
{
	List        *result = NIL;
	ListCell    *lc = NULL;

	if (pstate == NULL || args == NIL)
		return args;

	foreach(lc, args)
	{
		Node    *arg = lfirst(lc);
		Oid     type = exprType(arg);
		TYPCATEGORY ctg = TypeCategory(type);

		if (ctg != TYPCATEGORY_NUMERIC)
		{
			arg = coerce_to_target_type(pstate,
										arg,
										type,
										NUMERICOID,
										-1,
										COERCION_EXPLICIT,
										COERCE_EXPLICIT_CAST,
										-1);
			if (arg == NULL)
			{
				ereport(ERROR,
						(errcode(ERRCODE_CANNOT_COERCE),
						 errmsg("cannot cast %s to %s",
							 format_type_be(type), format_type_be(NUMERICOID))));
			}
		}

		result = lappend(result, arg);
	}

	return result;
}

static List *
transformStringToArrayArgs(ParseState *pstate, List *args)
{
	List        *result = NIL;
	ListCell    *lc = NULL;

	if (pstate == NULL || args == NIL)
		return args;

	foreach(lc, args)
	{
		Node    *arg = lfirst(lc);

		if (IsA(arg, Const))
		{
			Const *con = (Const *) arg;
			if (con->constisnull && con->constlen == NULL_TYPE_STR_LEN)
				arg = (Node*)make_const(pstate, makeString(pstrdup("")), con->location);
		}

		result = lappend(result, arg);
	}

	return result;
}

/*
 * FunctionName:  TransformUpdateXmlArgs
 *
 * Description:   Transform the argument list for the UPDATEXML function,
 *                ensuring that all arguments are of the correct type.
 *
 * Inputs:
 *     pstate:    The parse state, or NULL if not available
 *     args:      The original argument list for the UPDATEXML function
 *
 * Outputs:
 *     Returns a new argument list that can be passed to the UPDATEXML
 *     function, with each parameter either preserving its original XML
 *     type or being explicitly cast to TEXT.
 */
static List *
TransformUpdateXmlArgs(ParseState *pstate, List *args)
{
	List *    result = NIL;
	ListCell *lc = NULL;
	int       i = 0;

	if (pstate == NULL || args == NIL)
	{
		return args;
	}

	if ((list_length(args) - 1) % 2 != 0)
		elog(ERROR, "Args error for updatexml");

	foreach (lc, args)
	{
		Node *arg = lfirst(lc);
		Oid   type = exprType(arg);
		Oid   targetType = TEXTOID;
		bool  isOpenTenBaseOraXMLType = false;

		isOpenTenBaseOraXMLType = is_opentenbase_ora_xmltype(type);

		if (i == 0 || isOpenTenBaseOraXMLType)
		{
			targetType = XMLTYPEOID;
		}

		if ((i - 1) % 2 == 1 && type == XMLTYPEOID)
		{
			result = lappend(result, arg);
			i++;
			continue;
		}

		if (can_coerce_type(1, &type, &targetType, COERCION_EXPLICIT, false))
		{
			arg = coerce_to_target_type(pstate,
			                            arg,
			                            type,
			                            targetType,
			                            -1,
			                            COERCION_EXPLICIT,
			                            COERCE_EXPLICIT_CAST,
			                            -1);
		}
		result = lappend(result, arg);
		i++;
	}
	return result;
}

static void
checkLnnvlArgIsRelationalOp(ParseState *pstate, List *args)
{
	Node *arg = NULL;

	if (pstate == NULL || args == NIL)
		return ;

	arg = (Node *) linitial(args);

	if (IsA(arg, NullTest))
		return ;

	if (IsA(arg, OpExpr))
	{
		OpExpr *exp = (OpExpr *) arg;

		if (exp->opresulttype == BOOLOID)
			return ;
	}

	elog(ERROR, "invalid relational operator");
}

static List *
transformFuncArgsToNumberArgs(ParseState *pstate, List *args)
{
	List       *result = NIL;
	ListCell   *lc = NULL;

	if (pstate == NULL || args == NIL)
		return args;

	foreach(lc, args)
	{
		Node   *arg = lfirst(lc);
		Oid     type = exprType(arg);
		Oid     targetType = NUMERICOID;
		arg = coerce_to_target_type(pstate,
									arg,
									type,
									targetType,
									-1,
									COERCION_EXPLICIT,
									COERCE_EXPLICIT_CAST,
									-1);
		result = lappend(result, arg);
	}
	return result;
}



static List *
transformRatioArgs(ParseState *pstate, List *args)
{
	List       *result = NIL;
	ListCell   *lc = NULL;

	if (pstate == NULL || args == NIL)
		return args;

	foreach(lc, args)
	{
		Node   *arg = lfirst(lc);
		Oid     type = exprType(arg);
		Oid     targetType = NUMERICOID;
		arg = coerce_to_target_type(pstate,
									arg,
									type,
									targetType,
									-1,
									COERCION_EXPLICIT,
									COERCE_EXPLICIT_CAST,
									-1);
		result = lappend(result, arg);
	}
	return result;
}

/*
 * Try converting FuncCall to Indirection.
 * Compatible with opentenbase_ora array parentheses to access array elements.
 */
static Node *
transformOraIndirection(ParseState *pstate, FuncCall *fn)
{
	Node		   *collectvar = NULL;
	ColumnRef	   *columnref;
	ParseCollect	parsecollect = COLLECT_NONE;

	if (!pstate->p_collectref_hook)
		return NULL;

	if (fn->agg_order || fn->agg_filter || fn->agg_within_group ||
		fn->agg_star || fn->agg_distinct || fn->func_variadic ||
		fn->over || fn->keep || fn->return_columnref || fn->obj_cons ||
		OidIsValid(fn->type_oid) || list_length(fn->args) != 1 ||
		IsA(linitial(fn->args), NamedArgExpr))
		return NULL;

	columnref = makeNode(ColumnRef);
	columnref->fields = fn->funcname;
	columnref->location = fn->location;

	if (pstate->p_post_columnref_hook != NULL)
	{
		/* Find collection type variables defined in PL/SQL */
		collectvar = pstate->p_post_columnref_hook(pstate, columnref, NULL);
	}

	if (collectvar && IsA(collectvar, Param))
	{
		A_Indices		*indices;
		A_Indirection	*indirection;

		indices = makeNode(A_Indices);
		indices->is_slice = false;
		indices->lidx = NULL;
		indices->uidx = linitial(fn->args);

		indirection = makeNode(A_Indirection);
		indirection->indirection = list_make1(indices);
		indirection->arg = (Node *) columnref;

		if (list_length(fn->indirection) != 0)
			indirection->indirection = list_concat_copy(indirection->indirection, fn->indirection);

		parsecollect = pstate->p_collectref_hook(pstate, ((Param *) collectvar)->paramid, NULL);
		if (parsecollect != COLLECT_NONE)
			return transformIndirection(pstate, indirection);
	}

	return NULL;
}

/*
 * Try converting FuncCall to TypeConstructor.
 *
 * The collection type constructor can be replaced by Array[elementList]::TypeCast.
 */
static Node *
transformOraTypConstructor(ParseState *pstate, FuncCall *fn)
{
	Oid					elemtypid = InvalidOid;
	Oid					arrayid = InvalidOid;
	Oid					typrelid = InvalidOid;
	Type				tup;
	A_ArrayExpr			*a_arrayexpr;
	ParseCallbackState	pcbstate;

	if (fn->agg_order || fn->agg_filter || fn->agg_within_group ||
		fn->agg_star || fn->agg_distinct || fn->func_variadic ||
		fn->over || fn->keep || fn->return_columnref || fn->obj_cons ||
		OidIsValid(fn->type_oid) || fn->indirection ||
		list_length(fn->funcname) > 2)
		return NULL;

	/* Get the pg array type corresponding to the nested table type */
	setup_parser_errposition_callback(&pcbstate, pstate, fn->location);
	tup = LookupTypeName(pstate, makeTypeNameFromNameList(fn->funcname), NULL, true);
	cancel_parser_errposition_callback(&pcbstate);
	if (HeapTupleIsValid(tup))
	{
		typrelid = ((Form_pg_type) GETSTRUCT(tup))->typrelid;
		if (OidIsValid(typrelid) &&
			get_rel_relkind(typrelid) == RELKIND_NESTED_TABLE_TYPE)
			elemtypid = get_nesttable_elemtype((Form_pg_type) GETSTRUCT(tup), NULL);

		ReleaseSysCache(tup);
	}

	if (OidIsValid(elemtypid))
	{
		tup = typeidType(elemtypid);
		arrayid = ((Form_pg_type) GETSTRUCT(tup))->typarray;
		ReleaseSysCache(tup);
	}

	if (OidIsValid(arrayid))
	{
		char			*namespace;
		char			*arrayname;
		Node			*typcast;
		TypeName		*typname;
		Form_pg_type	typeForm;

		/* constuct ARRAY[xxx]::castType */
		a_arrayexpr = makeNode(A_ArrayExpr);
		a_arrayexpr->elements = fn->args;
		a_arrayexpr->location = fn->location;

		tup = typeidType(arrayid);
		typeForm = (Form_pg_type) GETSTRUCT(tup);
		arrayname = pstrdup(NameStr(typeForm->typname));
		namespace = get_namespace_name(typeForm->typnamespace);
		ReleaseSysCache(tup);

		typname = makeTypeNameFromNameList(list_make2(makeString(namespace),
										  makeString(arrayname)));

		typcast = makeTypeCast((Node *) a_arrayexpr, typname, -1);

		return transformTypeCast(pstate, (TypeCast*) typcast);
	}

	return NULL;
}

static bool
is_column_value_of_xmltable(ParseState *pstate, List *names, Node *arg)
{
	ColumnRef *cref = NULL;
	Node *field1 = NULL;
	Node *field2 = NULL;
	char *relname = NULL;
	char *colname = NULL;

	RangeTblEntry *rte;
	int	levels_up;
	ListCell   *c;
	Oid			vartypeid;
	int32		type_mod;
	Oid			varcollid;

	char	*schemaname = NULL;
	char	*funcname = NULL;
	int attrno = 0;
	bool is_found = false;

	if (!arg || nodeTag(arg) != T_ColumnRef)
		return false;

	cref = (ColumnRef *) arg;
	if (list_length(cref->fields) != 2)
		return false;

	field1 = (Node *) linitial(cref->fields);
	field2 = (Node *) lsecond(cref->fields);
	if (IsA(field2, A_Star))
		return false;
	
	relname = strVal(field1);
	colname = strVal(field2);
	if (strcmp(colname, "COLUMN_VALUE") != 0)
		return false;

	/* Locate the referenced RTE */
	rte = refnameRangeTblEntry(pstate, NULL, relname, cref->location, &levels_up);
	if (rte == NULL || rte->rtekind != RTE_FUNCTION || list_length(rte->functions) != 1)
		return false;

	/* deconstruct the name list */
	DeconstructQualifiedName(names, &schemaname, &funcname);
	if (strcasecmp(funcname, "extract") == 0)
		return false;

	/* get attrno by scan rte */
	foreach(c, rte->eref->colnames)
	{
		const char *attcolname = strVal(lfirst(c));

		attrno++;
		if (strcmp(attcolname, colname) == 0)
		{
			is_found = true;
			break;
		}
	}

	if (!is_found)
		return false;

	get_rte_attribute_type(rte, attrno, &vartypeid, &type_mod, &varcollid);
	if (strcasecmp(get_typ_name(vartypeid), "XML") != 0)
		return false;

	return true;
}


static Node*
transformRowNumberResultType(ParseState *pstate, Node *node)
{
    if (IsA(node, WindowFunc))
    {
        WindowFunc *func = (WindowFunc *)node;


        if (func->winfnoid == F_WINDOW_ROW_NUMBER)
        {
            Oid type = exprType(node);

            node = coerce_to_target_type(pstate, node, type, NUMERICOID, 
                                           -1, COERCION_EXPLICIT, COERCE_EXPLICIT_CAST, -1);
        }
    }
   return node; 
}

static Node *
transformFuncCall(ParseState *pstate, FuncCall *fn)
{
	Node       *last_srf = pstate->p_last_srf;
	Node       *result = NULL;
	List       *targs;
	List       *orig_types = NIL;
	ListCell   *args;
	ParseExprItem   old_item;
	Node		*iarg;
    char	*schemaname = NULL;
	char	*funcname = NULL;

	/* Transform the list of arguments ... */
	old_item = pstate->p_expr_item;
	pstate->p_expr_item = EXPR_ITEM_FN_ARGS;
	targs = NIL;
	foreach(args, fn->args)
	{
		iarg = (Node *) lfirst(args);
		if (ORA_MODE && is_column_value_of_xmltable(pstate, fn->funcname, iarg))
			iarg = makeTypeCast(iarg, makeTypeName("XMLTYPE"), -1);

		targs = lappend(targs, transformExprRecurse(pstate, iarg));
	}
	pstate->p_expr_item = old_item;

	/* handle type object self param */
	if (fn->obj_cons)
		mockTypObjConsSelfParam(&targs, fn->type_oid);

	if (ORA_MODE)
		process_opentenbase_ora_within_group(fn);

	/*
	 * When WITHIN GROUP is used, we treat its ORDER BY expressions as
	 * additional arguments to the function, for purposes of function lookup
	 * and argument type coercion.  So, transform each such expression and add
	 * them to the targs list.  We don't explicitly mark where each argument
	 * came from, but ParseFuncOrColumn can tell what's what by reference to
	 * list_length(fn->agg_order).
	 */
	if (fn->agg_within_group)
	{
		Assert(fn->agg_order != NIL);
		foreach(args, fn->agg_order)
		{
			SortBy	   *arg = (SortBy *) lfirst(args);

			targs = lappend(targs, transformExpr(pstate, arg->node,
												EXPR_KIND_ORDER_BY));
		}
	}

	if (ORA_MODE)
	{
		convert_to_opentenbase_ora_func(fn, targs);

		/* deconstruct the name list */
		DeconstructQualifiedName(fn->funcname, &schemaname, &funcname);

		if (schemaname == NULL || strcasecmp(schemaname, "opentenbase_ora") == 0)
		{
			/*
			 * transform ratio_to_report args to numeric type
			 */
			if (funcname && strcasecmp(funcname, "ratio_to_report") == 0)
				targs = transformRatioArgs(pstate, targs);
			/*
			 * transform arguments of opentenbase_ora's function nvl before
			 * transforming the nvl function.
			 */
			if (funcname && strcasecmp(funcname, "nvl") == 0)
				targs = transformNvlArgs(pstate, targs);
			/*
			 * transform arguments of opentenbase_ora's function nvl2 before
			 * transforming the nvl2 function.
			 */
			else if (funcname && strcasecmp(funcname, "nvl2") == 0)
				targs = transformNvl2Args(pstate, targs);
			/* Check arg of lnnvl is valid. */
			else if (funcname && strcasecmp(funcname, "lnnvl") == 0)
				checkLnnvlArgIsRelationalOp(pstate, targs);
			/*
			 * transform arguments of opentenbase_ora's function nanvl or atan2 before
			 * transforming the nanvl or atan2 function.
			 */
			else if (funcname && (strcasecmp(funcname, "nanvl") == 0 ||
						strcasecmp(funcname, "atan2") == 0))
				targs = transformFuncArgsToNumberArgs(pstate, targs);
			/*
			 * transform arguments of opentenbase_ora's function dump before
			 * transforming the dump function.
			 */
			else if (funcname && strcasecmp(funcname, "dump") == 0)
				targs = transformDumpArgs(pstate, targs);
			/*
			 * transform arguments of opentenbase_ora's function lpad or rpad before
			 * transforming the lpad or rpad function.
			 */
			else if (funcname && (strcasecmp(funcname, "lpad") == 0 ||
									strcasecmp(funcname, "rpad") == 0))
				targs = transformPadArgs(pstate, targs);
			/*
			 * transform arguments of opentenbase_ora's function instr before
			 * transforming the instr function.
			 */
			else if (funcname && strcasecmp(funcname, "instr") == 0)
				targs = transformInstrArgs(pstate, targs);
			/*
			 * transform arguments of opentenbase_ora's function ora_hash before
			 * transforming the ora_hash function.
			 */
			else if (funcname && strcasecmp(funcname, "ora_hash") == 0)
				targs = transformOraHashArgs(pstate, targs);
			/*
			 * transform arguments of opentenbase_ora's function lower before
			 * transforming the lower function.
			 */
			else if (funcname && strcasecmp(funcname, "lower") == 0)
				targs = transformLowerArgs(pstate, targs, &orig_types);
			/*
			 * transform arguments of opentenbase_ora's function upper before
			 * transforming the upper function.
			 */
			else if (funcname && strcasecmp(funcname, "upper") == 0)
				targs = transformUpperArgs(pstate, targs, &orig_types);
			/*
			 * Record the original type before conversion.
			 */
			else if (funcname && (strcasecmp(funcname, "nls_lower") == 0 ||
					 strcasecmp(funcname, "nls_upper") == 0) &&
					 list_length(targs) == 1)
				orig_types = list_make1_oid(exprType((Node *) linitial(targs)));
			/*
			 * transform arguments of opentenbase_ora's function updatexml before
			 * transforming the updatexml function.
			 */
			else if (funcname && strcasecmp(funcname, "updatexml") == 0)
				targs = TransformUpdateXmlArgs(pstate, targs);
			/*
			 * transform arguments of opentenbase_ora's function mod before transforming
			 * the mod function.
			 */
			else if (funcname && strcasecmp(funcname, "mod") == 0)
				targs = transformModArgs(pstate, targs);
			/*
			 * transform arguments of string_to_array to handle empty string as
			 * null in opentenbase_ora mode
			 */
			else if (funcname && strcasecmp(funcname, "STRING_TO_ARRAY") == 0)
				targs = transformStringToArrayArgs(pstate, targs);
			/*
			 * check argument of value in opentenbase_ora mode
			 */
			else if (funcname && strcasecmp(funcname, "value") == 0)
			{
				Node *n;

				if (list_length(targs) != 1)
					elog(ERROR, "invalid user.table.column, table.column, or column specification");

				n = (Node *) linitial(targs);
				if (n && !IsA(n, Var))
					elog(ERROR, "invalid user.table.column, table.column, or column specification");
			}
			/*
			 * others cases
			 */
			else
			{
				/* do nothing, keep compiler quiet. */
			}
		}

		if ((funcname != NULL && strcasecmp(funcname, "concat") == 0) &&
			(schemaname == NULL || strcasecmp(schemaname, "pg_catalog") == 0))
		{
			targs = transformConcatArgs(pstate, targs);
		}
	}
    else
    {
        /* deconstruct the name list */
        DeconstructQualifiedName(fn->funcname, &schemaname, &funcname);

        if (enable_lightweight_ora_syntax && funcname && strcasecmp(funcname, "nvl") == 0)
            targs = transformNvlArgs(pstate, targs);
        else if (funcname && strcasecmp(funcname, "replace") == 0)
            targs = transformReplaceArgs(pstate, targs);
        else if (funcname && strcasecmp(funcname, "substr") == 0)
            targs = transformSubstrArgs(pstate, targs);
        else if (funcname && strcasecmp(funcname, "string_agg") == 0)
            targs = transformStringAggArgs(pstate, targs);
    }

	/* Set the currently visible with functions and local functions */
	set_with_local_funcs(pstate);

	/* ... and hand off to ParseFuncOrColumn */
	result = ParseFuncOrColumn(pstate,
							  fn->funcname,
							  targs,
							  last_srf,
							  fn,
							  false,
							  fn->location);

	if (ORA_MODE && result && list_length(orig_types) == 1)
	{
		Oid	orig_type;
		Oid	type;

		type = exprType(result);
		orig_type = linitial_oid(orig_types);
		if (IS_CHAR_TYPE(orig_type))
		{
			/*
			 * If the original parameter type is not CHAR_TYPE,
			 * convert the type back to CHAR_TYPE after executing
			 * the function, otherwise there will be problems with
			 * equality comparison.
			 */
			result = coerce_to_target_type(pstate,
										   result,
										   type,
										   orig_type,
										   -1,
										   COERCION_EXPLICIT,
										   COERCE_EXPLICIT_CAST,
										   -1);
		}
	}

	if (orig_types)
		pfree(orig_types);

    if (ORA_MODE && enable_opentenbase_ora_return_type)
    {
        if (schemaname == NULL || strcasecmp(schemaname, "pg_catalog") == 0)
        {
            if (funcname && strcasecmp(funcname, "row_number") == 0)
                result = transformRowNumberResultType(pstate, result);
        }
    }
	return result;
}


static Node *
transformMultiAssignRef(ParseState *pstate, MultiAssignRef *maref)
{
    SubLink    *sublink;
    RowExpr    *rexpr;
    Query	   *qtree;
    TargetEntry *tle;

_begin:
    /* We should only see this in first-stage processing of UPDATE tlists */
    Assert(pstate->p_expr_kind == EXPR_KIND_UPDATE_SOURCE);

    /* We only need to transform the source if this is the first column */
    if (maref->colno == 1)
    {
        /*
         * For now, we only allow EXPR SubLinks and RowExprs as the source of
         * an UPDATE multiassignment.  This is sufficient to cover interesting
         * cases; at worst, someone would have to write (SELECT * FROM expr)
         * to expand a composite-returning expression of another form.
         */
        if (IsA(maref->source, SubLink) &&
            ((SubLink *) maref->source)->subLinkType == EXPR_SUBLINK)
        {
            /* Relabel it as a MULTIEXPR_SUBLINK */
            sublink = (SubLink *) maref->source;
            sublink->subLinkType = MULTIEXPR_SUBLINK;
            /* And transform it */
            sublink = (SubLink *) transformExprRecurse(pstate,
                                                       (Node *) sublink);

            qtree = castNode(Query, sublink->subselect);

            /* Check subquery returns required number of columns */
            if (count_nonjunk_tlist_entries(qtree->targetList) != maref->ncolumns)
                ereport(ERROR,
                        (errcode(ERRCODE_SYNTAX_ERROR),
                                errmsg("number of columns does not match number of values"),
                                parser_errposition(pstate, sublink->location)));

            /*
             * Build a resjunk tlist item containing the MULTIEXPR SubLink,
             * and add it to pstate->p_multiassign_exprs, whence it will later
             * get appended to the completed targetlist.  We needn't worry
             * about selecting a resno for it; transformUpdateStmt will do
             * that.
             */
            tle = makeTargetEntry((Expr *) sublink, 0, NULL, true);
            pstate->p_multiassign_exprs = lappend(pstate->p_multiassign_exprs,
                                                  tle);

            /*
             * Assign a unique-within-this-targetlist ID to the MULTIEXPR
             * SubLink.  We can just use its position in the
             * p_multiassign_exprs list.
             */
            sublink->subLinkId = list_length(pstate->p_multiassign_exprs);
        }
        else if (IsA(maref->source, RowExpr))
        {
            /* Transform the RowExpr, allowing SetToDefault items */
            rexpr = (RowExpr *) transformRowExpr(pstate,
                                                 (RowExpr *) maref->source,
                                                 true);

            /* Check it returns required number of columns */
            if (list_length(rexpr->args) != maref->ncolumns)
                ereport(ERROR,
                        (errcode(ERRCODE_SYNTAX_ERROR),
                                errmsg("number of columns does not match number of values"),
                                parser_errposition(pstate, rexpr->location)));

            /*
             * Temporarily append it to p_multiassign_exprs, so we can get it
             * back when we come back here for additional columns.
             */
            tle = makeTargetEntry((Expr *) rexpr, 0, NULL, true);
            pstate->p_multiassign_exprs = lappend(pstate->p_multiassign_exprs,
                                                  tle);
        }
        else
		{
			if (ORA_MODE)
			{
				MemoryContext olctx, origctx;
				RowExpr *re = NULL;

				origctx = GetMemoryChunkContext(maref->source);
				olctx = MemoryContextSwitchTo(origctx);

				re = makeNode(RowExpr);

				re->row_format = COERCE_EXPLICIT_CALL;
				re->args = list_make1(copyObject(maref->source));
				maref->source = (Node *) re;

				MemoryContextSwitchTo(olctx);

				goto _begin;
			}
            ereport(ERROR,
                    (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                            errmsg("source for a multiple-column UPDATE item must be a sub-SELECT or ROW() expression"),
                            parser_errposition(pstate, exprLocation(maref->source))));
		}
    }
    else
    {
        /*
         * Second or later column in a multiassignment.  Re-fetch the
         * transformed SubLink or RowExpr, which we assume is still the last
         * entry in p_multiassign_exprs.
         */
        Assert(pstate->p_multiassign_exprs != NIL);
        tle = (TargetEntry *) llast(pstate->p_multiassign_exprs);
    }

    /*
     * Emit the appropriate output expression for the current column
     */
    if (IsA(tle->expr, SubLink))
    {
        Param	   *param;

        sublink = (SubLink *) tle->expr;
        Assert(sublink->subLinkType == MULTIEXPR_SUBLINK);
        qtree = castNode(Query, sublink->subselect);

        /* Build a Param representing the current subquery output column */
        tle = (TargetEntry *) list_nth(qtree->targetList, maref->colno - 1);
        Assert(!tle->resjunk);

        param = makeNode(Param);
        param->paramkind = PARAM_MULTIEXPR;
        param->paramid = (sublink->subLinkId << 16) | maref->colno;
        param->paramtype = exprType((Node *) tle->expr);
        param->paramtypmod = exprTypmod((Node *) tle->expr);
        param->paramcollid = exprCollation((Node *) tle->expr);
        param->location = exprLocation((Node *) tle->expr);

        return (Node *) param;
    }

    if (IsA(tle->expr, RowExpr))
    {
        Node	   *result;

        rexpr = (RowExpr *) tle->expr;

        /* Just extract and return the next element of the RowExpr */
        result = (Node *) list_nth(rexpr->args, maref->colno - 1);

        /*
         * If we're at the last column, delete the RowExpr from
         * p_multiassign_exprs; we don't need it anymore, and don't want it in
         * the finished UPDATE tlist.
         */
        if (maref->colno == maref->ncolumns)
            pstate->p_multiassign_exprs =
                    list_delete_ptr(pstate->p_multiassign_exprs, tle);

        return result;
    }

    elog(ERROR, "unexpected expr type in multiassign list");
    return NULL;				/* keep compiler quiet */
}

static Node *
transformCaseExpr(ParseState *pstate, CaseExpr *c)
{
    CaseExpr   *newc = makeNode(CaseExpr);
    Node	   *last_srf = pstate->p_last_srf;
    Node	   *arg;
    CaseTestExpr *placeholder;
    List	   *newargs;
    List	   *resultexprs;
    ListCell   *l;
    Node	   *defresult;
    Oid			ptype;
	/* for opentenbase_ora DECODE's first arg */
	Node	   *first_expr = NULL;
	Oid			stype = InvalidOid;
	int32		stypemod = -1;

	if (c->isdecode)
		c = copyObject(c);

    /* transform the test expression, if any */
    arg = transformExprRecurse(pstate, (Node *) c->arg);

    /* generate placeholder for test expression */
    if (arg)
    {
        /*
         * If test expression is an untyped literal, force it to text. We have
         * to do something now because we won't be able to do this coercion on
         * the placeholder.  This is not as flexible as what was done in 7.4
         * and before, but it's good enough to handle the sort of silly coding
         * commonly seen.
         */
        if (exprType(arg) == UNKNOWNOID)
            arg = coerce_to_common_type(pstate, arg, TEXTOID, "CASE");

        /*
         * Run collation assignment on the test expression so that we know
         * what collation to mark the placeholder with.  In principle we could
         * leave it to parse_collate.c to do that later, but propagating the
         * result to the CaseTestExpr would be unnecessarily complicated.
         */
        assign_expr_collations(pstate, arg);

        placeholder = makeNode(CaseTestExpr);
        placeholder->typeId = exprType(arg);
        placeholder->typeMod = exprTypmod(arg);
        placeholder->collation = exprCollation(arg);
    }
    else
        placeholder = NULL;

    newc->arg = (Expr *) arg;

	if (c->isdecode)
	{
		A_Expr   *aexpr;
		Node     *sexpr; /* The fisrt search value */
		CaseWhen *cw = (CaseWhen *) linitial(c->args);

		if (IsA(cw->expr, NullTest))
		{
			NullTest *nt = (NullTest *) (cw->expr);
			stype = TEXTOID;

			Assert(nt->nulltesttype == IS_NULL);
			first_expr = transformExprRecurse(pstate, (Node *) copyObject(nt->arg));
		}
		else
		{
			Assert(IsA(cw->expr, BoolExpr) || IsA(cw->expr, A_Expr));
			if (IsA(cw->expr, BoolExpr))
			{
				Assert(((BoolExpr *)cw->expr)->boolop == OR_EXPR);
				aexpr = (A_Expr *) linitial(((BoolExpr *)cw->expr)->args);
			}
			else
				aexpr = (A_Expr *) cw->expr;
			sexpr    = transformExprRecurse(pstate, copyObject(aexpr->rexpr));
			stype    = exprType(sexpr);
			stypemod = exprTypmod(sexpr);

			if (TypeCategory(stype) == TYPCATEGORY_NUMERIC)
			{
				stype    = NUMERICOID;
				stypemod = -1;
			}
			else if (stype == UNKNOWNOID || TypeCategory(stype) == TYPCATEGORY_STRING)
			{
				stype    = TEXTOID;
				stypemod = -1;
			}
			first_expr = transformExprRecurse(pstate, copyObject(aexpr->lexpr));
		}

		/* Convert expr to the data type of the first search, if necessary */
		if (OidIsValid(stype))
			first_expr = coerce_to_target_type(pstate,
											   first_expr,
											   exprType(first_expr),
											   stype,
											   stypemod,
											   COERCION_EXPLICIT,
											   COERCE_EXPLICIT_CAST,
											   -1);
	}

    /* transform the list of arguments */
    newargs = NIL;
    resultexprs = NIL;

	foreach(l, c->args)
	{
		CaseWhen   *w = lfirst_node(CaseWhen, l);
		CaseWhen   *neww = makeNode(CaseWhen);
		Node	   *warg;

		warg = (Node *) w->expr;
		if (placeholder)
		{
			/* shorthand form was specified, so expand... */
			warg = (Node *) makeSimpleA_Expr(AEXPR_OP, "=",
											 (Node *) placeholder,
											 warg,
											 w->location);
		}

		if (c->isdecode && first_expr != NULL)
			neww->expr = (Expr *) transformDecodeExpr(pstate, warg, first_expr,
													  stype, stypemod);
		else
			neww->expr = (Expr *) transformExprRecurse(pstate, warg);

		neww->expr = (Expr *) coerce_to_boolean(pstate,
												(Node *) neww->expr,
												"CASE/WHEN");

		warg = (Node *) w->result;
		neww->result = (Expr *) transformExprRecurse(pstate, warg);
		neww->location = w->location;

		newargs = lappend(newargs, neww);
		resultexprs = lappend(resultexprs, neww->result);
	}

    newc->args = newargs;

    /* transform the default clause */
    defresult = (Node *) c->defresult;
    if (defresult == NULL)
    {
        A_Const    *n = makeNode(A_Const);

        n->val.type = T_Null;
        n->location = -1;
        defresult = (Node *) n;
    }
    newc->defresult = (Expr *) transformExprRecurse(pstate, defresult);

    /*
     * Note: default result is considered the most significant type in
     * determining preferred type. This is how the code worked before, but it
     * seems a little bogus to me --- tgl
     */

    if (c->isdecode)
    {
        Oid   typoid    = InvalidOid;
        Oid   ntype     = InvalidOid;
        int32 typmod    = -1;
        int   nlocation = -1;
        Node  *cnode    = NULL;

        /* opentenbase_ora converts all results to the data type of the first result expr. */
        resultexprs = lappend(resultexprs, newc->defresult);
        ora_select_common_type(pstate, resultexprs, &typoid, &typmod);
        if (!OidIsValid(typoid))
        {
            typoid = select_common_type(pstate, resultexprs, "CASE", NULL);
        }

        Assert(OidIsValid(typoid));
        ptype = typoid;
        newc->casetype = ptype;
        nlocation = exprLocation((const Node *) newc->defresult);

        /* convert unkown to text, to postpone type_cast operation till execute time */
        if (exprType((const Node *) newc->defresult) == UNKNOWNOID)
        {
            newc->defresult = (Expr *) coerce_to_common_type(pstate,
                                                             (Node *) newc->defresult,
                                                             TEXTOID,
                                                             "CASE");
        }

        ntype = exprType((const Node *) newc->defresult);
        
        /* Convert default result clause, if necessary */
        if (OidIsValid(ntype))
            cnode = coerce_to_target_type(pstate,
                                          (Node *) newc->defresult,
                                          ntype,
                                          ptype,
                                          typmod,
                                          COERCION_EXPLICIT,
                                          COERCE_EXPLICIT_CAST,
                                          nlocation);

        if (cnode == NULL)
            ereport(ERROR,
                    (errcode(ERRCODE_DATATYPE_MISMATCH),
                            errmsg("CASE/ELSE could not convert type %s to %s",
                                   format_type_be(ntype),
                                   format_type_be(ptype)),
                            parser_errposition(pstate, nlocation)));
        newc->defresult = (Expr *) cnode;

        /* Convert results in when-clause, if necessary */
        foreach(l, newc->args)
        {
            CaseWhen *w = (CaseWhen *) lfirst(l);
            nlocation = exprLocation((const Node *) w->result);
            
            /* convert unkown to text, to postpone type_cast operation till execute time */
            if (exprType((const Node *) w->result) == UNKNOWNOID)
            {
                w->result = (Expr *) coerce_to_common_type(pstate,
                                                           (Node *) w->result,
                                                           TEXTOID,
                                                           "CASE");
            }

            ntype = exprType((const Node *) w->result);

            if (OidIsValid(ntype))
                cnode = coerce_to_target_type(pstate,
                                              (Node *) w->result,
                                              ntype,
                                              ptype,
                                              typmod,
                                              COERCION_EXPLICIT,
                                              COERCE_EXPLICIT_CAST,
                                              nlocation);

            if (cnode == NULL)
                ereport(ERROR,
                        (errcode(ERRCODE_DATATYPE_MISMATCH),
                                errmsg("CASE/WHEN could not convert type %s to %s",
                                       format_type_be(ntype),
                                       format_type_be(ptype)),
                                parser_errposition(pstate, nlocation)));

            w->result      = (Expr *) cnode;
            newc->location = c->location;
        }

        return (Node *) newc;
    }

    resultexprs = lcons(newc->defresult, resultexprs);

    ptype = select_common_type(pstate, resultexprs, "CASE", NULL);
    Assert(OidIsValid(ptype));
    newc->casetype = ptype;
    /* casecollid will be set by parse_collate.c */

    /* Convert default result clause, if necessary */
    newc->defresult = (Expr *)
            coerce_to_common_type(pstate,
                                  (Node *) newc->defresult,
                                  ptype,
                                  "CASE/ELSE");

    /* Convert when-clause results, if necessary */
    foreach(l, newc->args)
    {
        CaseWhen   *w = (CaseWhen *) lfirst(l);

        w->result = (Expr *)
                coerce_to_common_type(pstate,
                                      (Node *) w->result,
                                      ptype,
                                      "CASE/WHEN");
    }

    /* if any subexpression contained a SRF, complain */
    if (pstate->p_last_srf != last_srf)
        ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                        /* translator: %s is name of a SQL construct, eg GROUP BY */
                        errmsg("set-returning functions are not allowed in %s",
                               "CASE"),
                        errhint("You might be able to move the set-returning function into a LATERAL FROM item."),
                        parser_errposition(pstate,
                                           exprLocation(pstate->p_last_srf))));

    newc->location = c->location;

    return (Node *) newc;
}

static Node *
transformSubLink(ParseState *pstate, SubLink *sublink)
{
    Node	   *result = (Node *) sublink;
    Query	   *qtree;
    const char *err;

    /*
     * Check to see if the sublink is in an invalid place within the query. We
     * allow sublinks everywhere in SELECT/INSERT/UPDATE/DELETE, but generally
     * not in utility statements.
     */
    err = NULL;
    switch (pstate->p_expr_kind)
    {
        case EXPR_KIND_NONE:
            Assert(false);		/* can't happen */
            break;
        case EXPR_KIND_OTHER:
            /* Accept sublink here; caller must throw error if wanted */
            break;
        case EXPR_KIND_JOIN_ON:
        case EXPR_KIND_JOIN_USING:
        case EXPR_KIND_FROM_SUBSELECT:
        case EXPR_KIND_FROM_FUNCTION:
        case EXPR_KIND_WHERE:
        case EXPR_KIND_POLICY:
        case EXPR_KIND_HAVING:
        case EXPR_KIND_FILTER:
        case EXPR_KIND_WINDOW_PARTITION:
        case EXPR_KIND_WINDOW_ORDER:
        case EXPR_KIND_WINDOW_FRAME_RANGE:
        case EXPR_KIND_WINDOW_FRAME_ROWS:
        case EXPR_KIND_SELECT_TARGET:
        case EXPR_KIND_INSERT_TARGET:
        case EXPR_KIND_UPDATE_SOURCE:
        case EXPR_KIND_UPDATE_TARGET:
		case EXPR_KIND_MERGE_WHEN:
		case EXPR_KIND_GROUP_BY:
        case EXPR_KIND_ORDER_BY:
        case EXPR_KIND_DISTINCT_ON:
        case EXPR_KIND_LIMIT:
        case EXPR_KIND_OFFSET:
        case EXPR_KIND_RETURNING:
        case EXPR_KIND_VALUES:
        case EXPR_KIND_VALUES_SINGLE:
            /* okay */
            break;
        case EXPR_KIND_CHECK_CONSTRAINT:
        case EXPR_KIND_DOMAIN_CHECK:
            err = _("cannot use subquery in check constraint");
            break;
        case EXPR_KIND_COLUMN_DEFAULT:
        case EXPR_KIND_FUNCTION_DEFAULT:
            err = _("cannot use subquery in DEFAULT expression");
            break;
        case EXPR_KIND_INDEX_EXPRESSION:
            err = _("cannot use subquery in index expression");
            break;
        case EXPR_KIND_INDEX_PREDICATE:
            err = _("cannot use subquery in index predicate");
            break;
        case EXPR_KIND_ALTER_COL_TRANSFORM:
            err = _("cannot use subquery in transform expression");
            break;
        case EXPR_KIND_EXECUTE_PARAMETER:
            err = _("cannot use subquery in EXECUTE parameter");
            break;
        case EXPR_KIND_TRIGGER_WHEN:
            err = _("cannot use subquery in trigger WHEN condition");
            break;
        case EXPR_KIND_PARTITION_EXPRESSION:
            err = _("cannot use subquery in partition key expression");
            break;
        case EXPR_KIND_CALL_ARGUMENT:
            err = _("cannot use subquery in CALL argument");
            break;

            /*
             * There is intentionally no default: case here, so that the
             * compiler will warn if we add a new ParseExprKind without
             * extending this switch.  If we do see an unrecognized value at
             * runtime, the behavior will be the same as for EXPR_KIND_OTHER,
             * which is sane anyway.
             */
    }
    if (err)
        ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                        errmsg_internal("%s", err),
                        parser_errposition(pstate, sublink->location)));

    pstate->p_hasSubLinks = true;

	pstate->p_sublink = sublink;

    /*
     * OK, let's transform the sub-SELECT.
     */
    qtree = parse_sub_analyze(sublink->subselect, pstate, NULL, false, true);
	pstate->p_sublink = NULL;

    /*
     * Check that we got a SELECT.  Anything else should be impossible given
     * restrictions of the grammar, but check anyway.
     */
    if (!IsA(qtree, Query) ||
        qtree->commandType != CMD_SELECT)
    elog(ERROR, "unexpected non-SELECT command in SubLink");

    sublink->subselect = (Node *) qtree;

    if (sublink->subLinkType == EXISTS_SUBLINK)
    {
        /*
         * EXISTS needs no test expression or combining operator. These fields
         * should be null already, but make sure.
         */
        sublink->testexpr = NULL;
        sublink->operName = NIL;
    }
    else if (sublink->subLinkType == EXPR_SUBLINK ||
             sublink->subLinkType == ARRAY_SUBLINK)
    {
        /*
         * Make sure the subselect delivers a single column (ignoring resjunk
         * targets).
         */
        if (count_nonjunk_tlist_entries(qtree->targetList) != 1)
            ereport(ERROR,
                    (errcode(ERRCODE_SYNTAX_ERROR),
                            errmsg("subquery must return only one column"),
                            parser_errposition(pstate, sublink->location)));

        /*
         * EXPR and ARRAY need no test expression or combining operator. These
         * fields should be null already, but make sure.
         */
        sublink->testexpr = NULL;
        sublink->operName = NIL;
    }
    else if (sublink->subLinkType == MULTIEXPR_SUBLINK)
    {
        /* Same as EXPR case, except no restriction on number of columns */
        sublink->testexpr = NULL;
        sublink->operName = NIL;
    }
    else
    {
        /* ALL, ANY, or ROWCOMPARE: generate row-comparing expression */
        Node	   *lefthand;
        List	   *left_list;
        List	   *right_list;
        ListCell   *l;

        if (operator_precedence_warning)
        {
            if (sublink->operName == NIL)
                emit_precedence_warnings(pstate, PREC_GROUP_IN, "IN",
                                         sublink->testexpr, NULL,
                                         sublink->location);
            else
                emit_precedence_warnings(pstate, PREC_GROUP_POSTFIX_OP,
                                         strVal(llast(sublink->operName)),
                                         sublink->testexpr, NULL,
                                         sublink->location);
        }

        /*
         * If the source was "x IN (select)", convert to "x = ANY (select)".
         */
        if (sublink->operName == NIL)
            sublink->operName = list_make1(makeString("="));

        /*
         * Transform lefthand expression, and convert to a list
         */
        lefthand = transformExprRecurse(pstate, sublink->testexpr);
        if (lefthand && IsA(lefthand, RowExpr))
            left_list = ((RowExpr *) lefthand)->args;
        else
            left_list = list_make1(lefthand);

        /*
         * Build a list of PARAM_SUBLINK nodes representing the output columns
         * of the subquery.
         */
        right_list = NIL;
        foreach(l, qtree->targetList)
        {
            TargetEntry *tent = (TargetEntry *) lfirst(l);
            Param	   *param;

            if (tent->resjunk)
                continue;

            param = makeNode(Param);
            param->paramkind = PARAM_SUBLINK;
            param->paramid = tent->resno;
            param->paramtype = exprType((Node *) tent->expr);
            param->paramtypmod = exprTypmod((Node *) tent->expr);
            param->paramcollid = exprCollation((Node *) tent->expr);
            param->location = -1;

            right_list = lappend(right_list, param);
        }

        /*
         * We could rely on make_row_comparison_op to complain if the list
         * lengths differ, but we prefer to generate a more specific error
         * message.
         */
        if (list_length(left_list) < list_length(right_list))
            ereport(ERROR,
                    (errcode(ERRCODE_SYNTAX_ERROR),
                            errmsg("subquery has too many columns"),
                            parser_errposition(pstate, sublink->location)));
        if (list_length(left_list) > list_length(right_list))
            ereport(ERROR,
                    (errcode(ERRCODE_SYNTAX_ERROR),
                            errmsg("subquery has too few columns"),
                            parser_errposition(pstate, sublink->location)));

        /*
         * Identify the combining operator(s) and generate a suitable
         * row-comparison expression.
         */
        sublink->testexpr = make_row_comparison_op(pstate,
                                                   sublink->operName,
                                                   left_list,
                                                   right_list,
                                                   sublink->location);
    }

    return result;
}

/*
 * transformArrayExpr
 *
 * If the caller specifies the target type, the resulting array will
 * be of exactly that type.  Otherwise we try to infer a common type
 * for the elements using select_common_type().
 */
static Node *
transformArrayExpr(ParseState *pstate, A_ArrayExpr *a,
                   Oid array_type, Oid element_type, int32 typmod)
{
    ArrayExpr  *newa = makeNode(ArrayExpr);
    List	   *newelems = NIL;
    List	   *newcoercedelems = NIL;
    ListCell   *element;
    Oid			coerce_type;
    bool		coerce_hard;

    /*
     * Transform the element expressions
     *
     * Assume that the array is one-dimensional unless we find an array-type
     * element expression.
     */
    newa->multidims = false;
    foreach(element, a->elements)
    {
        Node	   *e = (Node *) lfirst(element);
        Node	   *newe;

        /* Look through AEXPR_PAREN nodes so they don't affect test below */
        while (e && IsA(e, A_Expr) &&
               ((A_Expr *) e)->kind == AEXPR_PAREN)
            e = ((A_Expr *) e)->lexpr;

        /*
         * If an element is itself an A_ArrayExpr, recurse directly so that we
         * can pass down any target type we were given.
         */
        if (IsA(e, A_ArrayExpr))
        {
            newe = transformArrayExpr(pstate,
                                      (A_ArrayExpr *) e,
                                      array_type,
                                      element_type,
                                      typmod);
            /* we certainly have an array here */
            Assert(array_type == InvalidOid || array_type == exprType(newe));
            newa->multidims = true;
        }
        else
        {
            newe = transformExprRecurse(pstate, e);

            /*
             * Check for sub-array expressions, if we haven't already found
             * one.
             */
            if (!newa->multidims && type_is_array(exprType(newe)))
                newa->multidims = true;
        }

        newelems = lappend(newelems, newe);
    }

    /*
     * Select a target type for the elements.
     *
     * If we haven't been given a target array type, we must try to deduce a
     * common type based on the types of the individual elements present.
     */
    if (OidIsValid(array_type))
    {
        /* Caller must ensure array_type matches element_type */
        Assert(OidIsValid(element_type));
        coerce_type = (newa->multidims ? array_type : element_type);
        coerce_hard = true;
    }
    else
    {
        /* Can't handle an empty array without a target type */
        if (newelems == NIL)
            ereport(ERROR,
                    (errcode(ERRCODE_INDETERMINATE_DATATYPE),
                            errmsg("cannot determine type of empty array"),
                            errhint("Explicitly cast to the desired type, "
                                    "for example ARRAY[]::integer[]."),
                            parser_errposition(pstate, a->location)));

        /* Select a common type for the elements */
        coerce_type = select_common_type(pstate, newelems, "ARRAY", NULL);

        if (newa->multidims)
        {
            array_type = coerce_type;
            element_type = get_element_type(array_type);
            if (!OidIsValid(element_type))
                ereport(ERROR,
                        (errcode(ERRCODE_UNDEFINED_OBJECT),
                                errmsg("could not find element type for data type %s",
                                       format_type_be(array_type)),
                                parser_errposition(pstate, a->location)));
        }
        else
        {
            element_type = coerce_type;
            array_type = get_array_type(element_type);
            if (!OidIsValid(array_type))
                ereport(ERROR,
                        (errcode(ERRCODE_UNDEFINED_OBJECT),
                                errmsg("could not find array type for data type %s",
                                       format_type_be(element_type)),
                                parser_errposition(pstate, a->location)));
        }
        coerce_hard = false;
    }

    /*
     * Coerce elements to target type
     *
     * If the array has been explicitly cast, then the elements are in turn
     * explicitly coerced.
     *
     * If the array's type was merely derived from the common type of its
     * elements, then the elements are implicitly coerced to the common type.
     * This is consistent with other uses of select_common_type().
     */
    foreach(element, newelems)
    {
        Node	   *e = (Node *) lfirst(element);
        Node	   *newe;

        if (coerce_hard)
        {
            newe = coerce_to_target_type(pstate, e,
                                         exprType(e),
                                         coerce_type,
                                         typmod,
                                         COERCION_EXPLICIT,
                                         COERCE_EXPLICIT_CAST,
                                         -1);
            if (newe == NULL)
                ereport(ERROR,
                        (errcode(ERRCODE_CANNOT_COERCE),
                                errmsg("cannot cast type %s to %s",
                                       format_type_be(exprType(e)),
                                       format_type_be(coerce_type)),
                                parser_errposition(pstate, exprLocation(e))));
        }
        else
            newe = coerce_to_common_type(pstate, e,
                                         coerce_type,
                                         "ARRAY");
        newcoercedelems = lappend(newcoercedelems, newe);
    }

    newa->array_typeid = array_type;
    /* array_collid will be set by parse_collate.c */
    newa->element_typeid = element_type;
    newa->elements = newcoercedelems;
    newa->location = a->location;

    return (Node *) newa;
}

static Node *
transformRowExpr(ParseState *pstate, RowExpr *r, bool allowDefault)
{
    RowExpr    *newr;
    char		fname[16];
    int			fnum;
    ListCell   *lc;

    newr = makeNode(RowExpr);

    /* Transform the field expressions */
    newr->args = transformExpressionList(pstate, r->args,
                                         pstate->p_expr_kind, allowDefault);

    /* Barring later casting, we consider the type RECORD */
    newr->row_typeid = RECORDOID;
    newr->row_format = COERCE_IMPLICIT_CAST;

    /* ROW() has anonymous columns, so invent some field names */
    newr->colnames = NIL;
    fnum = 1;
    foreach(lc, newr->args)
    {
        if (ORA_MODE)
            snprintf(fname, sizeof(fname), "F%d", fnum++);
        else
            snprintf(fname, sizeof(fname), "f%d", fnum++);
        newr->colnames = lappend(newr->colnames, makeString(pstrdup(fname)));
    }

    newr->location = r->location;

    return (Node *) newr;
}

static Node *
transformCoalesceExpr(ParseState *pstate, CoalesceExpr *c)
{
    CoalesceExpr *newc = makeNode(CoalesceExpr);
    Node	   *last_srf = pstate->p_last_srf;
    List	   *newargs = NIL;
    List	   *newcoercedargs = NIL;
    ListCell   *args;
	List		*newargswithnull = NIL;
	bool		is_str_null_args = false;

    /* opentenbase_ora function bug: the number of parameters less than 2 */
    if (ORA_MODE && list_length(c->args) < 2)
    	ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
    		errmsg("not enough arguments for function")));

    foreach(args, c->args)
    {
        Node	   *e = (Node *) lfirst(args);
        Node	   *newe;
		bool		is_null_const = false;

        newe = transformExprRecurse(pstate, e);

		/*
		 * Because '' is treated as null in opentenbase_ora mode, and for the Coalesce
		 * function, '' and null are different types, we need to restore null to
		 * the parameter of '' to select type.
		 */
		if ((ORA_MODE || enable_lightweight_ora_syntax) && newe && IsA(newe, Const))
		{
			Const *c = (Const*) newe;
			if (c->constisnull && c->constlen == NULL_TYPE_STR_LEN)
			{
				is_null_const = true;
				is_str_null_args = true;

				newargswithnull = lappend(newargswithnull, newe);
				newe = (Node *) makeConst(TEXTOID, -1, InvalidOid, -1,
								PointerGetDatum(cstring_to_text("")), false, false);
			}
		}

		if (!is_null_const)
			newargswithnull = lappend(newargswithnull, newe);

		newargs = lappend(newargs, newe);
    }

    newc->coalescetype = select_common_type(pstate, newargs, "COALESCE", NULL);
    /* coalescecollid will be set by parse_collate.c */

	if ((ORA_MODE || enable_lightweight_ora_syntax) && is_str_null_args)
	{
		list_free(newargs);
		newargs = newargswithnull;
	}
	else
	{
		list_free(newargswithnull);
	}

    /* Convert arguments if necessary */
    foreach(args, newargs)
    {
        Node	   *e = (Node *) lfirst(args);
        Node	   *newe;

        newe = coerce_to_common_type(pstate, e,
                                     newc->coalescetype,
                                     "COALESCE");
        newcoercedargs = lappend(newcoercedargs, newe);
    }

    /* if any subexpression contained a SRF, complain */
    if (pstate->p_last_srf != last_srf)
        ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                        /* translator: %s is name of a SQL construct, eg GROUP BY */
                        errmsg("set-returning functions are not allowed in %s",
                               "COALESCE"),
                        errhint("You might be able to move the set-returning function into a LATERAL FROM item."),
                        parser_errposition(pstate,
                                           exprLocation(pstate->p_last_srf))));

    newc->args = newcoercedargs;
    newc->location = c->location;
    return (Node *) newc;
}

static Node *
transformMinMaxExpr(ParseState *pstate, MinMaxExpr *m)
{
    MinMaxExpr *newm = makeNode(MinMaxExpr);
    List	   *newargs = NIL;
    List	   *newcoercedargs = NIL;
    const char *funcname = (m->op == IS_GREATEST) ? "GREATEST" : "LEAST";
    ListCell   *args;

    newm->op = m->op;
    foreach(args, m->args)
    {
        Node	   *e = (Node *) lfirst(args);
        Node	   *newe;

        newe = transformExprRecurse(pstate, e);
        newargs = lappend(newargs, newe);
    }

	if (ORA_MODE)
		ora_select_common_type(pstate, newargs, &newm->minmaxtype, NULL);
	else
		newm->minmaxtype = select_common_type(pstate, newargs, funcname, NULL);
    /* minmaxcollid and inputcollid will be set by parse_collate.c */

    /* Convert arguments if necessary */
    foreach(args, newargs)
    {
        Node	   *e = (Node *) lfirst(args);
        Node	   *newe;

		if (ORA_MODE)
			newe = ora_coerce_to_common_type(pstate, e,
											newm->minmaxtype,
											funcname);
		else
			newe = coerce_to_common_type(pstate, e,
											newm->minmaxtype,
											funcname);
        newcoercedargs = lappend(newcoercedargs, newe);
    }

    newm->args = newcoercedargs;
    newm->location = m->location;
    return (Node *) newm;
}

static Node *
transformSQLValueFunction(ParseState *pstate, SQLValueFunction *svf)
{
    /*
     * All we need to do is insert the correct result type and (where needed)
     * validate the typmod, so we just modify the node in-place.
     */
    switch (svf->op)
    {
        case SVFOP_CURRENT_DATE:
            svf->type = DATEOID;
            break;
        case SVFOP_CURRENT_TIME:
            svf->type = TIMETZOID;
            break;
        case SVFOP_CURRENT_TIME_N:
            svf->type = TIMETZOID;
            svf->typmod = anytime_typmod_check(true, svf->typmod);
            break;
        case SVFOP_CURRENT_TIMESTAMP:
            svf->type = TIMESTAMPTZOID;
            break;
        case SVFOP_CURRENT_TIMESTAMP_N:
            svf->type = TIMESTAMPTZOID;
            svf->typmod = anytimestamp_typmod_check(true, svf->typmod);
            break;
        case SVFOP_LOCALTIME:
            svf->type = TIMEOID;
            break;
        case SVFOP_LOCALTIME_N:
            svf->type = TIMEOID;
            svf->typmod = anytime_typmod_check(false, svf->typmod);
            break;
        case SVFOP_LOCALTIMESTAMP:
            svf->type = TIMESTAMPOID;
            break;
        case SVFOP_LOCALTIMESTAMP_N:
            svf->type = TIMESTAMPOID;
            svf->typmod = anytimestamp_typmod_check(false, svf->typmod);
            break;
        case SVFOP_CURRENT_ROLE:
        case SVFOP_CURRENT_USER:
        case SVFOP_SESSION_USER:
        case SVFOP_CURRENT_CATALOG:
        case SVFOP_CURRENT_SCHEMA:
            svf->type = NAMEOID;
            break;
        case SVFOP_USER:
            if (ORA_MODE)
                svf->type = VARCHAR2OID;
            else
                svf->type = NAMEOID;
            break;
        case SVFOP_USER_ID:
			svf->type = INT8OID;
            break;
    }

    return (Node *) svf;
}

static Node *
transformXmlExpr(ParseState *pstate, XmlExpr *x)
{
    XmlExpr    *newx;
    ListCell   *lc;
    int			i;

    if (operator_precedence_warning && x->op == IS_DOCUMENT)
        emit_precedence_warnings(pstate, PREC_GROUP_POSTFIX_IS, "IS",
                                 (Node *) linitial(x->args), NULL,
                                 x->location);

    newx = makeNode(XmlExpr);
    newx->op = x->op;
    /* opentenbase_ora will not convert xml name in xmlelement */
    if (!(ORA_MODE && x->op == IS_XMLELEMENT) && x->name)
        newx->name = map_sql_identifier_to_xml_name(x->name, false, false);
    else
        newx->name = x->name ? pstrdup(x->name) : NULL;
    newx->xmloption = x->xmloption;
    newx->type = XMLOID;		/* this just marks the node as transformed */
    newx->typmod = -1;
    newx->location = x->location;

    /*
     * gram.y built the named args as a list of ResTarget.  Transform each,
     * and break the names out as a separate list.
     */
    newx->named_args = NIL;
    newx->arg_names = NIL;

    foreach(lc, x->named_args)
    {
        ResTarget  *r = lfirst_node(ResTarget, lc);
        Node	   *expr;
        char	   *argname;

        expr = transformExprRecurse(pstate, r->val);

        if (r->name)
            argname = map_sql_identifier_to_xml_name(r->name, false, false);
        else if (IsA(r->val, ColumnRef))
            argname = map_sql_identifier_to_xml_name(FigureColname(r->val, NULL),
                                                     true, false);
        else
        {
            ereport(ERROR,
                    (errcode(ERRCODE_SYNTAX_ERROR),
                            x->op == IS_XMLELEMENT
                            ? errmsg("unnamed XML attribute value must be a column reference")
                            : errmsg("unnamed XML element value must be a column reference"),
                            parser_errposition(pstate, r->location)));
            argname = NULL;		/* keep compiler quiet */
        }

        /* reject duplicate argnames in XMLELEMENT only */
        if (x->op == IS_XMLELEMENT)
        {
            ListCell   *lc2;

            foreach(lc2, newx->arg_names)
            {
                if (strcmp(argname, strVal(lfirst(lc2))) == 0)
                    ereport(ERROR,
                            (errcode(ERRCODE_SYNTAX_ERROR),
                                    errmsg("XML attribute name \"%s\" appears more than once",
                                           argname),
                                    parser_errposition(pstate, r->location)));
            }
        }

        newx->named_args = lappend(newx->named_args, expr);
        newx->arg_names = lappend(newx->arg_names, makeString(argname));
    }

    /* The other arguments are of varying types depending on the function */
    newx->args = NIL;
    i = 0;
    foreach(lc, x->args)
    {
        Node	   *e = (Node *) lfirst(lc);
        Node	   *newe;

        newe = transformExprRecurse(pstate, e);
        switch (x->op)
        {
            case IS_XMLCONCAT:
                newe = coerce_to_specific_type(pstate, newe, XMLOID,
                                               "XMLCONCAT");
                break;
            case IS_XMLELEMENT:
                /* no coercion necessary */
                break;
            case IS_XMLFOREST:
                newe = coerce_to_specific_type(pstate, newe, XMLOID,
                                               "XMLFOREST");
                break;
            case IS_XMLPARSE:
                if (i == 0)
                    newe = coerce_to_specific_type(pstate, newe, TEXTOID,
                                                   "XMLPARSE");
                else
				{
					if (ORA_MODE)
						newe = coerce_to_specific_type(pstate, newe, INT4OID, "XMLPARSE");
					else
						newe = coerce_to_boolean(pstate, newe, "XMLPARSE");
				}
                break;
            case IS_XMLPI:
                newe = coerce_to_specific_type(pstate, newe, TEXTOID,
                                               "XMLPI");
                break;
            case IS_XMLROOT:
                if (i == 0)
                    newe = coerce_to_specific_type(pstate, newe, XMLOID,
                                                   "XMLROOT");
                else if (i == 1)
                    newe = coerce_to_specific_type(pstate, newe, TEXTOID,
                                                   "XMLROOT");
                else
                    newe = coerce_to_specific_type(pstate, newe, INT4OID,
                                                   "XMLROOT");
                break;
            case IS_XMLSERIALIZE:
                /* not handled here */
                Assert(false);
                break;
            case IS_DOCUMENT:
                newe = coerce_to_specific_type(pstate, newe, XMLOID,
                                               "IS DOCUMENT");
                break;
        }
        newx->args = lappend(newx->args, newe);
        i++;
    }

    return (Node *) newx;
}

static Node *
transformXmlSerialize(ParseState *pstate, XmlSerialize *xs)
{
    Node	   *result;
    XmlExpr    *xexpr;
    Oid			targetType;
    int32		targetTypmod;

    xexpr = makeNode(XmlExpr);
    xexpr->op = IS_XMLSERIALIZE;
    xexpr->args = list_make1(coerce_to_specific_type(pstate,
                                                     transformExprRecurse(pstate, xs->expr),
                                                     XMLOID,
                                                     "XMLSERIALIZE"));

    typenameTypeIdAndMod(pstate, xs->typeName, &targetType, &targetTypmod, false);

    xexpr->xmloption = xs->xmloption;
    xexpr->location = xs->location;
    /* We actually only need these to be able to parse back the expression. */
    xexpr->type = targetType;
    xexpr->typmod = targetTypmod;

    /*
     * The actual target type is determined this way.  SQL allows char and
     * varchar as target types.  We allow anything that can be cast implicitly
     * from text.  This way, user-defined text-like data types automatically
     * fit in.
     */
    result = coerce_to_target_type(pstate, (Node *) xexpr,
                                   TEXTOID, targetType, targetTypmod,
                                   COERCION_IMPLICIT,
                                   COERCE_IMPLICIT_CAST,
                                   -1);
    if (result == NULL)
        ereport(ERROR,
                (errcode(ERRCODE_CANNOT_COERCE),
                        errmsg("cannot cast XMLSERIALIZE result to %s",
                               format_type_be(targetType)),
                        parser_errposition(pstate, xexpr->location)));
    return result;
}

static Node *
transformBooleanTest(ParseState *pstate, BooleanTest *b)
{
    const char *clausename;

    if (operator_precedence_warning)
        emit_precedence_warnings(pstate, PREC_GROUP_POSTFIX_IS, "IS",
                                 (Node *) b->arg, NULL,
                                 b->location);

    switch (b->booltesttype)
    {
        case IS_TRUE:
            clausename = "IS TRUE";
            break;
        case IS_NOT_TRUE:
            clausename = "IS NOT TRUE";
            break;
        case IS_FALSE:
            clausename = "IS FALSE";
            break;
        case IS_NOT_FALSE:
            clausename = "IS NOT FALSE";
            break;
        case IS_UNKNOWN:
            clausename = "IS UNKNOWN";
            break;
        case IS_NOT_UNKNOWN:
            clausename = "IS NOT UNKNOWN";
            break;
        default:
        elog(ERROR, "unrecognized booltesttype: %d",
             (int) b->booltesttype);
            clausename = NULL;	/* keep compiler quiet */
    }

    b->arg = (Expr *) transformExprRecurse(pstate, (Node *) b->arg);

    b->arg = (Expr *) coerce_to_boolean(pstate,
                                        (Node *) b->arg,
                                        clausename);

    return (Node *) b;
}

static Node *
transformCurrentOfExpr(ParseState *pstate, CurrentOfExpr *cexpr)
{
    int			sublevels_up;

#ifdef PGXC
    ereport(ERROR,
			(errcode(ERRCODE_STATEMENT_TOO_COMPLEX),
				(errmsg("WHERE CURRENT OF clause not yet supported"))));
#endif

    /* CURRENT OF can only appear at top level of UPDATE/DELETE */
    Assert(pstate->p_target_rangetblentry != NULL);
    cexpr->cvarno = RTERangeTablePosn(pstate,
                                      pstate->p_target_rangetblentry,
                                      &sublevels_up);
    Assert(sublevels_up == 0);

    /*
     * Check to see if the cursor name matches a parameter of type REFCURSOR.
     * If so, replace the raw name reference with a parameter reference. (This
     * is a hack for the convenience of plpgsql.)
     */
    if (cexpr->cursor_name != NULL) /* in case already transformed */
    {
        ColumnRef  *cref = makeNode(ColumnRef);
        Node	   *node = NULL;

        /* Build an unqualified ColumnRef with the given name */
        cref->fields = list_make1(makeString(cexpr->cursor_name));
        cref->location = -1;

        /* See if there is a translation available from a parser hook */
        if (pstate->p_pre_columnref_hook != NULL)
        	node = pstate->p_pre_columnref_hook(pstate, cref);
        if (node == NULL && pstate->p_post_columnref_hook != NULL)
            node = pstate->p_post_columnref_hook(pstate, cref, NULL);

        /*
         * XXX Should we throw an error if we get a translation that isn't a
         * refcursor Param?  For now it seems best to silently ignore false
         * matches.
         */
        if (node != NULL && IsA(node, Param))
        {
            Param	   *p = (Param *) node;

            if (p->paramkind == PARAM_EXTERN &&
                p->paramtype == REFCURSOROID)
            {
                /* Matches, so convert CURRENT OF to a param reference */
                cexpr->cursor_name = NULL;
                cexpr->cursor_param = p->paramid;
            }
        }
    }

    return (Node *) cexpr;
}

/*
 * Construct a whole-row reference to represent the notation "relation.*".
 */
static Node *
transformWholeRowRef(ParseState *pstate, RangeTblEntry *rte, int location)
{
    Var		   *result;
    int			vnum;
    int			sublevels_up;

    /* Find the RTE's rangetable location */
    vnum = RTERangeTablePosn(pstate, rte, &sublevels_up);

    /*
     * Build the appropriate referencing node.  Note that if the RTE is a
     * function returning scalar, we create just a plain reference to the
     * function value, not a composite containing a single column.  This is
     * pretty inconsistent at first sight, but it's what we've done
     * historically.  One argument for it is that "rel" and "rel.*" mean the
     * same thing for composite relations, so why not for scalar functions...
     */
    result = makeWholeRowVar(rte, vnum, sublevels_up, true);

    /* location is not filled in by makeWholeRowVar */
    result->location = location;

    /* mark relation as requiring whole-row SELECT access */
    markVarForSelectPriv(pstate, result, rte);

    return (Node *) result;
}

/*
 * Handle an explicit CAST construct.
 *
 * Transform the argument, look up the type name, and apply any necessary
 * coercion function(s).
 */
static Node *
transformTypeCast(ParseState *pstate, TypeCast *tc)
{
    Node	   *result;
    Node	   *arg = tc->arg;
    Node	   *expr;
    Oid			inputType;
    Oid			targetType;
    int32		targetTypmod;
    int			location;

    /* Look up the type name first */
    typenameTypeIdAndMod(pstate, tc->typeName, &targetType, &targetTypmod, false);


    /*
     * Look through any AEXPR_PAREN nodes that may have been inserted thanks
     * to operator_precedence_warning.  Otherwise, ARRAY[]::foo[] behaves
     * differently from (ARRAY[])::foo[].
     */
    while (arg && IsA(arg, A_Expr) &&
           ((A_Expr *) arg)->kind == AEXPR_PAREN)
        arg = ((A_Expr *) arg)->lexpr;

    /*
     * If the subject of the typecast is an ARRAY[] construct and the target
     * type is an array type, we invoke transformArrayExpr() directly so that
     * we can pass down the type information.  This avoids some cases where
     * transformArrayExpr() might not infer the correct type.  Otherwise, just
     * transform the argument normally.
     */
    if (IsA(arg, A_ArrayExpr))
    {
        Oid			targetBaseType;
        int32		targetBaseTypmod;
        Oid			elementType;

        /*
         * If target is a domain over array, work with the base array type
         * here.  Below, we'll cast the array type to the domain.  In the
         * usual case that the target is not a domain, the remaining steps
         * will be a no-op.
         */
        targetBaseTypmod = targetTypmod;
        targetBaseType = getBaseTypeAndTypmod(targetType, &targetBaseTypmod);
        elementType = get_element_type(targetBaseType);
        if (OidIsValid(elementType))
        {
            expr = transformArrayExpr(pstate,
                                      (A_ArrayExpr *) arg,
                                      targetBaseType,
                                      elementType,
                                      targetBaseTypmod);
        }
        else
            expr = transformExprRecurse(pstate, arg);
    }
    else
        expr = transformExprRecurse(pstate, arg);

    inputType = exprType(expr);
    if (inputType == InvalidOid)
        return expr;			/* do nothing if NULL input */

    /*
     * Location of the coercion is preferentially the location of the :: or
     * CAST symbol, but if there is none then use the location of the type
     * name (this can happen in TypeName 'string' syntax, for instance).
     */
    location = tc->location;
    if (location < 0)
        location = tc->typeName->location;

    result = coerce_to_target_type(pstate, expr, inputType,
                                   OidIsValid(pstate->multisetRetOid)?pstate->multisetRetOid: targetType, targetTypmod,
                                   COERCION_EXPLICIT,
                                   COERCE_EXPLICIT_CAST,
                                   location);
    if (result == NULL)
        ereport(ERROR,
                (errcode(ERRCODE_CANNOT_COERCE),
                        errmsg("cannot cast type %s to %s",
                               format_type_be(inputType),
                               format_type_be(targetType)),
                        parser_coercion_errposition(pstate, location, expr)));

    return result;
}

/*
 * Handle an explicit COLLATE clause.
 *
 * Transform the argument, and look up the collation name.
 */
static Node *
transformCollateClause(ParseState *pstate, CollateClause *c)
{
    CollateExpr *newc;
    Oid			argtype;

    newc = makeNode(CollateExpr);
    newc->arg = (Expr *) transformExprRecurse(pstate, c->arg);

    argtype = exprType((Node *) newc->arg);

    /*
     * The unknown type is not collatable, but coerce_type() takes care of it
     * separately, so we'll let it go here.
     */
    if (!type_is_collatable(argtype) && argtype != UNKNOWNOID)
        ereport(ERROR,
                (errcode(ERRCODE_DATATYPE_MISMATCH),
                        errmsg("collations are not supported by type %s",
                               format_type_be(argtype)),
                        parser_errposition(pstate, c->location)));

    newc->collOid = LookupCollation(pstate, c->collname, c->location);
    newc->location = c->location;

    return (Node *) newc;
}

/*
 * Transform a "row compare-op row" construct
 *
 * The inputs are lists of already-transformed expressions.
 * As with coerce_type, pstate may be NULL if no special unknown-Param
 * processing is wanted.
 *
 * The output may be a single OpExpr, an AND or OR combination of OpExprs,
 * or a RowCompareExpr.  In all cases it is guaranteed to return boolean.
 * The AND, OR, and RowCompareExpr cases further imply things about the
 * behavior of the operators (ie, they behave as =, <>, or < <= > >=).
 */
static Node *
make_row_comparison_op(ParseState *pstate, List *opname,
                       List *largs, List *rargs, int location)
{
    RowCompareExpr *rcexpr;
    RowCompareType rctype;
    List	   *opexprs;
    List	   *opnos;
    List	   *opfamilies;
    ListCell   *l,
            *r;
    List	  **opinfo_lists;
    Bitmapset  *strats;
    int			nopers;
    int			i;

    nopers = list_length(largs);
    if (nopers != list_length(rargs))
        ereport(ERROR,
                (errcode(ERRCODE_SYNTAX_ERROR),
                        errmsg("unequal number of entries in row expressions"),
                        parser_errposition(pstate, location)));

    /*
     * We can't compare zero-length rows because there is no principled basis
     * for figuring out what the operator is.
     */
    if (nopers == 0)
        ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                        errmsg("cannot compare rows of zero length"),
                        parser_errposition(pstate, location)));

    /*
     * Identify all the pairwise operators, using make_op so that behavior is
     * the same as in the simple scalar case.
     */
    opexprs = NIL;
    forboth(l, largs, r, rargs)
    {
        Node	   *larg = (Node *) lfirst(l);
        Node	   *rarg = (Node *) lfirst(r);
        OpExpr	   *cmp;

        cmp = castNode(OpExpr, make_op(pstate, opname, larg, rarg,
                                       pstate->p_last_srf, location));

        /*
         * We don't use coerce_to_boolean here because we insist on the
         * operator yielding boolean directly, not via coercion.  If it
         * doesn't yield bool it won't be in any index opfamilies...
         */
        if (cmp->opresulttype != BOOLOID)
            ereport(ERROR,
                    (errcode(ERRCODE_DATATYPE_MISMATCH),
                            errmsg("row comparison operator must yield type boolean, "
                                   "not type %s",
                                   format_type_be(cmp->opresulttype)),
                            parser_errposition(pstate, location)));
        if (expression_returns_set((Node *) cmp))
            ereport(ERROR,
                    (errcode(ERRCODE_DATATYPE_MISMATCH),
                            errmsg("row comparison operator must not return a set"),
                            parser_errposition(pstate, location)));
        opexprs = lappend(opexprs, cmp);
    }

    /*
     * If rows are length 1, just return the single operator.  In this case we
     * don't insist on identifying btree semantics for the operator (but we
     * still require it to return boolean).
     */
    if (nopers == 1)
        return (Node *) linitial(opexprs);

    /*
     * Now we must determine which row comparison semantics (= <> < <= > >=)
     * apply to this set of operators.  We look for btree opfamilies
     * containing the operators, and see which interpretations (strategy
     * numbers) exist for each operator.
     */
    opinfo_lists = (List **) palloc(nopers * sizeof(List *));
    strats = NULL;
    i = 0;
    foreach(l, opexprs)
    {
        Oid			opno = ((OpExpr *) lfirst(l))->opno;
        Bitmapset  *this_strats;
        ListCell   *j;

        opinfo_lists[i] = get_op_btree_interpretation(opno);

        /*
         * convert strategy numbers into a Bitmapset to make the intersection
         * calculation easy.
         */
        this_strats = NULL;
        foreach(j, opinfo_lists[i])
        {
            OpBtreeInterpretation *opinfo = lfirst(j);

            this_strats = bms_add_member(this_strats, opinfo->strategy);
        }
        if (i == 0)
            strats = this_strats;
        else
            strats = bms_int_members(strats, this_strats);
        i++;
    }

    /*
     * If there are multiple common interpretations, we may use any one of
     * them ... this coding arbitrarily picks the lowest btree strategy
     * number.
     */
    i = bms_first_member(strats);
    if (i < 0)
    {
        /* No common interpretation, so fail */
        ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                        errmsg("could not determine interpretation of row comparison operator %s",
                               strVal(llast(opname))),
                        errhint("Row comparison operators must be associated with btree operator families."),
                        parser_errposition(pstate, location)));
    }
    rctype = (RowCompareType) i;

    /*
     * For = and <> cases, we just combine the pairwise operators with AND or
     * OR respectively.
     */
    if (rctype == ROWCOMPARE_EQ)
        return (Node *) makeBoolExpr(AND_EXPR, opexprs, location);
    if (rctype == ROWCOMPARE_NE)
        return (Node *) makeBoolExpr(OR_EXPR, opexprs, location);

    /*
     * Otherwise we need to choose exactly which opfamily to associate with
     * each operator.
     */
    opfamilies = NIL;
    for (i = 0; i < nopers; i++)
    {
        Oid			opfamily = InvalidOid;
        ListCell   *j;

        foreach(j, opinfo_lists[i])
        {
            OpBtreeInterpretation *opinfo = lfirst(j);

            if (opinfo->strategy == rctype)
            {
                opfamily = opinfo->opfamily_id;
                break;
            }
        }
        if (OidIsValid(opfamily))
            opfamilies = lappend_oid(opfamilies, opfamily);
        else					/* should not happen */
            ereport(ERROR,
                    (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                            errmsg("could not determine interpretation of row comparison operator %s",
                                   strVal(llast(opname))),
                            errdetail("There are multiple equally-plausible candidates."),
                            parser_errposition(pstate, location)));
    }

    /*
     * Now deconstruct the OpExprs and create a RowCompareExpr.
     *
     * Note: can't just reuse the passed largs/rargs lists, because of
     * possibility that make_op inserted coercion operations.
     */
    opnos = NIL;
    largs = NIL;
    rargs = NIL;
    foreach(l, opexprs)
    {
        OpExpr	   *cmp = (OpExpr *) lfirst(l);

        opnos = lappend_oid(opnos, cmp->opno);
        largs = lappend(largs, linitial(cmp->args));
        rargs = lappend(rargs, lsecond(cmp->args));
    }

    rcexpr = makeNode(RowCompareExpr);
    rcexpr->rctype = rctype;
    rcexpr->opnos = opnos;
    rcexpr->opfamilies = opfamilies;
    rcexpr->inputcollids = NIL; /* assign_expr_collations will fix this */
    rcexpr->largs = largs;
    rcexpr->rargs = rargs;

    return (Node *) rcexpr;
}

/*
 * Transform a "row IS DISTINCT FROM row" construct
 *
 * The input RowExprs are already transformed
 */
static Node *
make_row_distinct_op(ParseState *pstate, List *opname,
                     RowExpr *lrow, RowExpr *rrow,
                     int location)
{
    Node	   *result = NULL;
    List	   *largs = lrow->args;
    List	   *rargs = rrow->args;
    ListCell   *l,
            *r;

    if (list_length(largs) != list_length(rargs))
        ereport(ERROR,
                (errcode(ERRCODE_SYNTAX_ERROR),
                        errmsg("unequal number of entries in row expressions"),
                        parser_errposition(pstate, location)));

    forboth(l, largs, r, rargs)
    {
        Node	   *larg = (Node *) lfirst(l);
        Node	   *rarg = (Node *) lfirst(r);
        Node	   *cmp;

        cmp = (Node *) make_distinct_op(pstate, opname, larg, rarg, location);
        if (result == NULL)
            result = cmp;
        else
            result = (Node *) makeBoolExpr(OR_EXPR,
                                           list_make2(result, cmp),
                                           location);
    }

    if (result == NULL)
    {
        /* zero-length rows?  Generate constant FALSE */
        result = makeBoolConst(false, false);
    }

    return result;
}

/*
 * make the node for an IS DISTINCT FROM operator
 */
static Expr *
make_distinct_op(ParseState *pstate, List *opname, Node *ltree, Node *rtree,
                 int location)
{
    Expr	   *result;

    result = make_op(pstate, opname, ltree, rtree,
                     pstate->p_last_srf, location);
    if (((OpExpr *) result)->opresulttype != BOOLOID)
        ereport(ERROR,
                (errcode(ERRCODE_DATATYPE_MISMATCH),
                        errmsg("IS DISTINCT FROM requires = operator to yield boolean"),
                        parser_errposition(pstate, location)));
    if (((OpExpr *) result)->opretset)
        ereport(ERROR,
                (errcode(ERRCODE_DATATYPE_MISMATCH),
                        /* translator: %s is name of a SQL construct, eg NULLIF */
                        errmsg("%s must not return a set", "IS DISTINCT FROM"),
                        parser_errposition(pstate, location)));

    /*
     * We rely on DistinctExpr and OpExpr being same struct
     */
    NodeSetTag(result, T_DistinctExpr);

    return result;
}

/*
 * Produce a NullTest node from an IS [NOT] DISTINCT FROM NULL construct
 *
 * "arg" is the untransformed other argument
 */
static Node *
make_nulltest_from_distinct(ParseState *pstate, A_Expr *distincta, Node *arg)
{
    NullTest   *nt = makeNode(NullTest);

    nt->arg = (Expr *) transformExprRecurse(pstate, arg);
    /* the argument can be any type, so don't coerce it */
    if (distincta->kind == AEXPR_NOT_DISTINCT)
        nt->nulltesttype = IS_NULL;
    else
        nt->nulltesttype = IS_NOT_NULL;
    /* argisrow = false is correct whether or not arg is composite */
    nt->argisrow = false;
    nt->location = distincta->location;
    return (Node *) nt;
}

/*
 * Identify node's group for operator precedence warnings
 *
 * For items in nonzero groups, also return a suitable node name into *nodename
 *
 * Note: group zero is used for nodes that are higher or lower precedence
 * than everything that changed precedence; we need never issue warnings
 * related to such nodes.
 */
static int
operator_precedence_group(Node *node, const char **nodename)
{
    int			group = 0;

    *nodename = NULL;
    if (node == NULL)
        return 0;

    if (IsA(node, A_Expr))
    {
        A_Expr	   *aexpr = (A_Expr *) node;

        if (aexpr->kind == AEXPR_OP &&
            aexpr->lexpr != NULL &&
            aexpr->rexpr != NULL)
        {
            /* binary operator */
            if (list_length(aexpr->name) == 1)
            {
                *nodename = strVal(linitial(aexpr->name));
                /* Ignore if op was always higher priority than IS-tests */
                if (strcmp(*nodename, "+") == 0 ||
                    strcmp(*nodename, "-") == 0 ||
                    strcmp(*nodename, "*") == 0 ||
                    strcmp(*nodename, "/") == 0 ||
                    strcmp(*nodename, "%") == 0 ||
                    strcmp(*nodename, "^") == 0)
                    group = 0;
                else if (strcmp(*nodename, "<") == 0 ||
                         strcmp(*nodename, ">") == 0)
                    group = PREC_GROUP_LESS;
                else if (strcmp(*nodename, "=") == 0)
                    group = PREC_GROUP_EQUAL;
                else if (strcmp(*nodename, "<=") == 0 ||
                         strcmp(*nodename, ">=") == 0 ||
                         strcmp(*nodename, "<>") == 0)
                    group = PREC_GROUP_LESS_EQUAL;
                else
                    group = PREC_GROUP_INFIX_OP;
            }
            else
            {
                /* schema-qualified operator syntax */
                *nodename = "OPERATOR()";
                group = PREC_GROUP_INFIX_OP;
            }
        }
        else if (aexpr->kind == AEXPR_OP &&
                 aexpr->lexpr == NULL &&
                 aexpr->rexpr != NULL)
        {
            /* prefix operator */
            if (list_length(aexpr->name) == 1)
            {
                *nodename = strVal(linitial(aexpr->name));
                /* Ignore if op was always higher priority than IS-tests */
                if (strcmp(*nodename, "+") == 0 ||
                    strcmp(*nodename, "-"))
                    group = 0;
                else
                    group = PREC_GROUP_PREFIX_OP;
            }
            else
            {
                /* schema-qualified operator syntax */
                *nodename = "OPERATOR()";
                group = PREC_GROUP_PREFIX_OP;
            }
        }
        else if (aexpr->kind == AEXPR_OP &&
                 aexpr->lexpr != NULL &&
                 aexpr->rexpr == NULL)
        {
            /* postfix operator */
            if (list_length(aexpr->name) == 1)
            {
                *nodename = strVal(linitial(aexpr->name));
                group = PREC_GROUP_POSTFIX_OP;
            }
            else
            {
                /* schema-qualified operator syntax */
                *nodename = "OPERATOR()";
                group = PREC_GROUP_POSTFIX_OP;
            }
        }
        else if (aexpr->kind == AEXPR_OP_ANY ||
                 aexpr->kind == AEXPR_OP_ALL)
        {
            *nodename = strVal(llast(aexpr->name));
            group = PREC_GROUP_POSTFIX_OP;
        }
        else if (aexpr->kind == AEXPR_DISTINCT ||
                 aexpr->kind == AEXPR_NOT_DISTINCT)
        {
            *nodename = "IS";
            group = PREC_GROUP_INFIX_IS;
        }
        else if (aexpr->kind == AEXPR_OF)
        {
            *nodename = "IS";
            group = PREC_GROUP_POSTFIX_IS;
        }
        else if (aexpr->kind == AEXPR_IN)
        {
            *nodename = "IN";
            if (strcmp(strVal(linitial(aexpr->name)), "=") == 0)
                group = PREC_GROUP_IN;
            else
                group = PREC_GROUP_NOT_IN;
        }
        else if (aexpr->kind == AEXPR_LIKE)
        {
            *nodename = "LIKE";
            if (strcmp(strVal(linitial(aexpr->name)), "~~") == 0)
                group = PREC_GROUP_LIKE;
            else
                group = PREC_GROUP_NOT_LIKE;
        }
        else if (aexpr->kind == AEXPR_ILIKE)
        {
            *nodename = "ILIKE";
            if (strcmp(strVal(linitial(aexpr->name)), "~~*") == 0)
                group = PREC_GROUP_LIKE;
            else
                group = PREC_GROUP_NOT_LIKE;
        }
        else if (aexpr->kind == AEXPR_SIMILAR)
        {
            *nodename = "SIMILAR";
            if (strcmp(strVal(linitial(aexpr->name)), "~") == 0)
                group = PREC_GROUP_LIKE;
            else
                group = PREC_GROUP_NOT_LIKE;
        }
        else if (aexpr->kind == AEXPR_BETWEEN ||
                 aexpr->kind == AEXPR_BETWEEN_SYM)
        {
            Assert(list_length(aexpr->name) == 1);
            *nodename = strVal(linitial(aexpr->name));
            group = PREC_GROUP_BETWEEN;
        }
        else if (aexpr->kind == AEXPR_NOT_BETWEEN ||
                 aexpr->kind == AEXPR_NOT_BETWEEN_SYM)
        {
            Assert(list_length(aexpr->name) == 1);
            *nodename = strVal(linitial(aexpr->name));
            group = PREC_GROUP_NOT_BETWEEN;
        }
    }
    else if (IsA(node, NullTest) ||
             IsA(node, BooleanTest))
    {
        *nodename = "IS";
        group = PREC_GROUP_POSTFIX_IS;
    }
    else if (IsA(node, XmlExpr))
    {
        XmlExpr    *x = (XmlExpr *) node;

        if (x->op == IS_DOCUMENT)
        {
            *nodename = "IS";
            group = PREC_GROUP_POSTFIX_IS;
        }
    }
    else if (IsA(node, SubLink))
    {
        SubLink    *s = (SubLink *) node;

        if (s->subLinkType == ANY_SUBLINK ||
            s->subLinkType == ALL_SUBLINK)
        {
            if (s->operName == NIL)
            {
                *nodename = "IN";
                group = PREC_GROUP_IN;
            }
            else
            {
                *nodename = strVal(llast(s->operName));
                group = PREC_GROUP_POSTFIX_OP;
            }
        }
    }
    else if (IsA(node, BoolExpr))
    {
        /*
         * Must dig into NOTs to see if it's IS NOT DOCUMENT or NOT IN.  This
         * opens us to possibly misrecognizing, eg, NOT (x IS DOCUMENT) as a
         * problematic construct.  We can tell the difference by checking
         * whether the parse locations of the two nodes are identical.
         *
         * Note that when we are comparing the child node to its own children,
         * we will not know that it was a NOT.  Fortunately, that doesn't
         * matter for these cases.
         */
        BoolExpr   *b = (BoolExpr *) node;

        if (b->boolop == NOT_EXPR)
        {
            Node	   *child = (Node *) linitial(b->args);

            if (IsA(child, XmlExpr))
            {
                XmlExpr    *x = (XmlExpr *) child;

                if (x->op == IS_DOCUMENT &&
                    x->location == b->location)
                {
                    *nodename = "IS";
                    group = PREC_GROUP_POSTFIX_IS;
                }
            }
            else if (IsA(child, SubLink))
            {
                SubLink    *s = (SubLink *) child;

                if (s->subLinkType == ANY_SUBLINK && s->operName == NIL &&
                    s->location == b->location)
                {
                    *nodename = "IN";
                    group = PREC_GROUP_NOT_IN;
                }
            }
        }
    }
    return group;
}

/*
 * helper routine for delivering 9.4-to-9.5 operator precedence warnings
 *
 * opgroup/opname/location represent some parent node
 * lchild, rchild are its left and right children (either could be NULL)
 *
 * This should be called before transforming the child nodes, since if a
 * precedence-driven parsing change has occurred in a query that used to work,
 * it's quite possible that we'll get a semantic failure while analyzing the
 * child expression.  We want to produce the warning before that happens.
 * In any case, operator_precedence_group() expects untransformed input.
 */
static void
emit_precedence_warnings(ParseState *pstate,
                         int opgroup, const char *opname,
                         Node *lchild, Node *rchild,
                         int location)
{
    int			cgroup;
    const char *copname;

    Assert(opgroup > 0);

    /*
     * Complain if left child, which should be same or higher precedence
     * according to current rules, used to be lower precedence.
     *
     * Exception to precedence rules: if left child is IN or NOT IN or a
     * postfix operator, the grouping is syntactically forced regardless of
     * precedence.
     */
    cgroup = operator_precedence_group(lchild, &copname);
    if (cgroup > 0)
    {
        if (oldprecedence_l[cgroup] < oldprecedence_r[opgroup] &&
            cgroup != PREC_GROUP_IN &&
            cgroup != PREC_GROUP_NOT_IN &&
            cgroup != PREC_GROUP_POSTFIX_OP &&
            cgroup != PREC_GROUP_POSTFIX_IS)
            ereport(WARNING,
                    (errmsg("operator precedence change: %s is now lower precedence than %s",
                            opname, copname),
                            parser_errposition(pstate, location)));
    }

    /*
     * Complain if right child, which should be higher precedence according to
     * current rules, used to be same or lower precedence.
     *
     * Exception to precedence rules: if right child is a prefix operator, the
     * grouping is syntactically forced regardless of precedence.
     */
    cgroup = operator_precedence_group(rchild, &copname);
    if (cgroup > 0)
    {
        if (oldprecedence_r[cgroup] <= oldprecedence_l[opgroup] &&
            cgroup != PREC_GROUP_PREFIX_OP)
            ereport(WARNING,
                    (errmsg("operator precedence change: %s is now lower precedence than %s",
                            opname, copname),
                            parser_errposition(pstate, location)));
    }
}

/*
 * Produce a string identifying an expression by kind.
 *
 * Note: when practical, use a simple SQL keyword for the result.  If that
 * doesn't work well, check call sites to see whether custom error message
 * strings are required.
 */
const char *
ParseExprKindName(ParseExprKind exprKind)
{
    switch (exprKind)
    {
        case EXPR_KIND_NONE:
            return "invalid expression context";
        case EXPR_KIND_OTHER:
            return "extension expression";
        case EXPR_KIND_JOIN_ON:
            return "JOIN/ON";
        case EXPR_KIND_JOIN_USING:
            return "JOIN/USING";
        case EXPR_KIND_FROM_SUBSELECT:
            return "sub-SELECT in FROM";
        case EXPR_KIND_FROM_FUNCTION:
            return "function in FROM";
        case EXPR_KIND_WHERE:
            return "WHERE";
        case EXPR_KIND_POLICY:
            return "POLICY";
        case EXPR_KIND_HAVING:
            return "HAVING";
        case EXPR_KIND_FILTER:
            return "FILTER";
        case EXPR_KIND_WINDOW_PARTITION:
            return "window PARTITION BY";
        case EXPR_KIND_WINDOW_ORDER:
            return "window ORDER BY";
        case EXPR_KIND_WINDOW_FRAME_RANGE:
            return "window RANGE";
        case EXPR_KIND_WINDOW_FRAME_ROWS:
            return "window ROWS";
        case EXPR_KIND_SELECT_TARGET:
            return "SELECT";
        case EXPR_KIND_INSERT_TARGET:
            return "INSERT";
        case EXPR_KIND_UPDATE_SOURCE:
        case EXPR_KIND_UPDATE_TARGET:
            return "UPDATE";
		case EXPR_KIND_MERGE_WHEN:
			return "MERGE WHEN";
		case EXPR_KIND_GROUP_BY:
            return "GROUP BY";
        case EXPR_KIND_ORDER_BY:
            return "ORDER BY";
        case EXPR_KIND_DISTINCT_ON:
            return "DISTINCT ON";
        case EXPR_KIND_LIMIT:
            return "LIMIT";
        case EXPR_KIND_OFFSET:
            return "OFFSET";
        case EXPR_KIND_RETURNING:
            return "RETURNING";
        case EXPR_KIND_VALUES:
        case EXPR_KIND_VALUES_SINGLE:
            return "VALUES";
        case EXPR_KIND_CHECK_CONSTRAINT:
        case EXPR_KIND_DOMAIN_CHECK:
            return "CHECK";
        case EXPR_KIND_COLUMN_DEFAULT:
        case EXPR_KIND_FUNCTION_DEFAULT:
            return "DEFAULT";
        case EXPR_KIND_INDEX_EXPRESSION:
            return "index expression";
        case EXPR_KIND_INDEX_PREDICATE:
            return "index predicate";
        case EXPR_KIND_ALTER_COL_TRANSFORM:
            return "USING";
        case EXPR_KIND_EXECUTE_PARAMETER:
            return "EXECUTE";
        case EXPR_KIND_TRIGGER_WHEN:
            return "WHEN";
        case EXPR_KIND_PARTITION_EXPRESSION:
            return "PARTITION BY";
        case EXPR_KIND_CALL_ARGUMENT:
            return "CALL";

            /*
             * There is intentionally no default: case here, so that the
             * compiler will warn if we add a new ParseExprKind without
             * extending this switch.  If we do see an unrecognized value at
             * runtime, we'll fall through to the "unrecognized" return.
             */
    }
    return "unrecognized expression kind";
}

#ifdef _PG_ORCL_
/*
 * Look up SequenceId and resolve synonym if opentenbase_ora compatibility enabled
 */
static Oid
lookupSequenceId(char **nspname, char **seqname)
{
	Oid	nspId,
		seqId = InvalidOid;
	bool	try_syn = false;

_rechck:
	if (*nspname != NULL)
	{
		nspId = LookupNamespaceNoError(*nspname);
		if (OidIsValid(nspId))
		{
			seqId = get_relname_relid(*seqname, nspId);
			if (OidIsValid(seqId))
				return seqId;
		}
		/* Not found, try synonym */
	}
	else
	{
		seqId = RelnameGetRelid(*seqname);
		if (OidIsValid(seqId))
		{
			*nspname = get_namespace_name(get_rel_namespace(seqId));
			Assert(*nspname != NULL);
			return seqId;
		}
	}

	if (!ORA_MODE)
		return seqId;

	if (ORA_MODE && !try_syn)
	{
		char	*dblink;

		try_syn = true;
		resolve_synonym(*nspname, *seqname, nspname, seqname, &dblink);

		/* Only consider the local object for sequence */
		if (*seqname != NULL && dblink == NULL)
			goto _rechck;
	}

	return seqId;
}

static void
seq_nextcall_init(ParseState *pstate)
{
	HASHCTL hinfo;
	int hflags = 0;

	if (pstate->hash_seqnextval_calls)
		return;

	/* Init node hashtable */
	MemSet(&hinfo, 0, sizeof(hinfo));

	hinfo.keysize = sizeof(Oid);
	hinfo.entrysize = sizeof(SeqNextCallItem);
	hflags |= HASH_ELEM;
	hinfo.hcxt = CurrentMemoryContext;
	hflags |= HASH_CONTEXT;
	hinfo.hash = oid_hash;
	hflags |= HASH_FUNCTION;

	pstate->hash_seqnextval_calls = hash_create("seqnext_calls_info",
										64,
										&hinfo,
										hflags);
}

/* opentenbase_ora behavior checks that the same SQL statement does not contain more than one seq.nextVal */
static bool
seq_nextcall_push_with_check(ParseState *pstate, Oid seqid)
{
	SeqNextCallItem *cur_item = NULL;
	const int initial_call_num = 1;

	if (pstate->hash_seqnextval_calls == NULL)
		seq_nextcall_init(pstate);

	cur_item = (SeqNextCallItem *) hash_search(pstate->hash_seqnextval_calls, &seqid, HASH_FIND, NULL);
	if (cur_item)
	{
		/* The record can be found and must have been called twice, so an error is returned */
		return false;
	}
	else
	{
		/* First occurrence, insert this record, return normal */
		cur_item = (SeqNextCallItem *) hash_search(pstate->hash_seqnextval_calls, &seqid, HASH_ENTER, NULL);
		if (cur_item == NULL)
			elog(ERROR, "seq_nextcall_push_with_check: hash_enter failed seqid[%u].", seqid);

		cur_item->seqnext_calls_num = initial_call_num;
	}
	return true;
}

/*
 * Try to transform sequence function call such as
 * my_seq.nextval or my_seq.currval
 */
static Node *
transformSequenceColumn(ParseState *pstate, ColumnRef *cref)
{
	Node	   *node = NULL;
	char	   *catname = NULL;
	char	   *nspname = NULL;
	char	   *seqname = NULL;
	char	   *valname = NULL;
	List	   *funcname = NULL;
	StringInfoData	funcargs;

	Oid			seqId = InvalidOid;

	initStringInfo(&funcargs);
	switch (list_length(cref->fields))
	{
		case 1:
			break;
		case 2:
			{
				Node	   *field1 = (Node *) linitial(cref->fields);
				Node	   *field2 = (Node *) lsecond(cref->fields);

				Assert(IsA(field1, String));
				seqname = strVal(field1);

				/* Look up SequenceId and resolve synonym */
				seqId = lookupSequenceId(&nspname, &seqname);

				if (!IsA(field2, String))
				{
					break;
				}
				valname = strVal(field2);

				break;
			}
		case 3:
			{
				Node	   *field1 = (Node *) linitial(cref->fields);
				Node	   *field2 = (Node *) lsecond(cref->fields);
				Node	   *field3 = (Node *) lthird(cref->fields);

				Assert(IsA(field1, String));
				nspname = strVal(field1);
				Assert(IsA(field2, String));
				seqname = strVal(field2);

				/* Look up SequenceId and resolve synonym */
				seqId = lookupSequenceId(&nspname, &seqname);

				if (!IsA(field3, String))
				{
					break;
				}
				valname = strVal(field3);

				break;
			}
		case 4:
			{
				Node	   *field1 = (Node *) linitial(cref->fields);
				Node	   *field2 = (Node *) lsecond(cref->fields);
				Node	   *field3 = (Node *) lthird(cref->fields);
				Node	   *field4 = (Node *) lfourth(cref->fields);

				Assert(IsA(field1, String));
				catname = strVal(field1);
				Assert(pg_strcasecmp(catname, get_database_name(MyDatabaseId)) == 0);

				Assert(IsA(field2, String));
				nspname = strVal(field2);
				Assert(IsA(field3, String));
				seqname = strVal(field3);

				/* Look up SequenceId and resolve synonym */
				seqId = lookupSequenceId(&nspname, &seqname);

				if (!IsA(field4, String))
				{
					break;
				}
				valname = strVal(field4);

				break;
			}
		default:
			break;
	}

	if (nspname != NULL)
	{
		if (catname != NULL)
			appendStringInfo(&funcargs, "\"%s\".\"%s\"", catname, nspname);
		else
			appendStringInfo(&funcargs, "\"%s\"", nspname);
	}
	if (seqname != NULL)
		appendStringInfo(&funcargs, ".\"%s\"", seqname);

	/* if is not sequence operator, do nothing but return */
	if (valname == NULL)
		return NULL;
	else if (pg_strcasecmp(valname, "nextval") == 0)
        if (enable_lightweight_ora_syntax)
            funcname = list_make2(makeString("opentenbase_ora"), makeString("nextval"));
        else
		funcname = list_make2(makeString("opentenbase_ora"), makeString("NEXTVAL"));
	else if (pg_strcasecmp(valname, "currval") == 0)
        if (enable_lightweight_ora_syntax)
            funcname = list_make2(makeString("opentenbase_ora"), makeString("currval"));
        else
		funcname = list_make2(makeString("opentenbase_ora"), makeString("CURRVAL"));
	else
		return NULL;

	if (!OidIsValid(seqId))
		return NULL;

	if (get_rel_relkind(seqId) == RELKIND_SEQUENCE)
	{
		List *args = NULL;

		if (ORA_MODE &&
			pg_strcasecmp(valname, "nextval") == 0 &&
			(seq_nextcall_push_with_check(pstate, seqId) == false))
			elog(ERROR, "The same SQL command does not support multiple seq.nextval");

		args = (List *) list_make1(make_const(pstate, makeString(funcargs.data), -1));
		node = ParseFuncOrColumn(pstate,
								 funcname,
							 	 args,
							 	 pstate->p_last_srf,
							 	 NULL,
								 false,
							 	 cref->location);
	}

	return node;
}

static void
ora_select_common_type(ParseState *pstate,
                       List *exprs,
                       Oid *typoid,
                       int32 *typmod)
{
    Node  *pexpr  = NULL;
    Oid   ptype   = InvalidOid;
    int32 ptypmod = -1;
	ListCell	*lc;
    
    Assert(pstate && exprs);
    
    pexpr = (Node *) linitial(exprs);
    ptype = exprType((const Node *) pexpr);

	if (ptype != UNKNOWNOID)
	{
		lc = lnext(list_head(exprs));

		for_each_cell(lc, lc)
		{
			Node	*nexpr = (Node *) lfirst(lc);
			Oid		ntype = exprType(nexpr);

			if (ntype != ptype)
				break;
		}
		if (lc == NULL)			/* got to the end of the list? */
		{
			if (typoid)
			{
				*typoid = ptype;
			}
			if (typmod)
			{
				*typmod = ptypmod;
			}
			return;
		}
	}

    if (ptype == UNKNOWNOID ||
        TypeCategory(ptype) == TYPCATEGORY_STRING)
    {
        ptype = TEXTOID;
    }
    
    if (TypeCategory(ptype) == TYPCATEGORY_NUMERIC)
    {
        ptype = NUMERICOID;
    }
    
    if (typoid)
    {
        *typoid = ptype;
    }
    if (typmod)
    {
        *typmod = ptypmod;
    }
    return;
}

static Node *
transformDecodeExpr(ParseState *pstate, Node *warg, Node *expr, Oid stype, Oid stypemod)
{
    A_Expr *aexpr;
    Node   *sexpr;
    Node   *result;
    Node   *nullTest = NULL;

    Assert(pstate && warg && expr);
    if (!OidIsValid(stype) || IsA(warg, NullTest))
    {
        return transformExprRecurse(pstate, warg);
    }

    Assert(IsA(warg, BoolExpr) || IsA(warg, A_Expr));
    if (IsA(warg, BoolExpr))
    {
        Assert(((BoolExpr *)warg)->boolop == OR_EXPR);
        aexpr = (A_Expr *) linitial(((BoolExpr *)warg)->args);

        nullTest = lsecond(((BoolExpr *)warg)->args);
        Assert(IsA(nullTest, BoolExpr));
        nullTest = transformExprRecurse(pstate, nullTest);
    }
    else
    {
        aexpr = (A_Expr *) warg;
    }

    sexpr = transformExprRecurse(pstate, aexpr->rexpr);

    /* convert unkown to text, to postpone type_cast operation till execute time */
    if (exprType(sexpr) == UNKNOWNOID)
    {
        sexpr = coerce_to_common_type(pstate,
                                      sexpr,
                                      TEXTOID,
                                      "CASE/WHEN");
    }
    
    sexpr = coerce_to_target_type(pstate, sexpr, exprType(sexpr),
                                  stype, stypemod,
                                  COERCION_EXPLICIT,
                                  COERCE_EXPLICIT_CAST,
                                  -1);
    
    result = (Node *) make_op(pstate,
                              aexpr->name,
                              expr,
                              sexpr,
                              pstate->p_last_srf,
                              aexpr->location);

    if (IsA(warg, BoolExpr))
    {
        /* return (OpExpr || NullTest). */
        Assert(nullTest);
        return (Node *) makeBoolExpr(((BoolExpr *) warg)->boolop,
                                     list_make2(result, nullTest),
                                     ((BoolExpr *) warg)->location);
    }

    return result;
}

#endif
