/*-------------------------------------------------------------------------
 *
 * queryjumble.c
 *	 Query normalization and fingerprinting.
 *
 * Normalization is a process whereby similar queries, typically differing only
 * in their constants (though the exact rules are somewhat more subtle than
 * that) are recognized as equivalent, and are tracked as a single entry.  This
 * is particularly useful for non-prepared queries.
 *
 * Normalization is implemented by fingerprinting queries, selectively
 * serializing those fields of each query tree's nodes that are judged to be
 * essential to the query.  This is referred to as a query jumble.  This is
 * distinct from a regular serialization in that various extraneous
 * information is ignored as irrelevant or not essential to the query, such
 * as the collations of Vars and, most notably, the values of constants.
 *
 * This jumble is acquired at the end of parse analysis of each query, and
 * a 64-bit hash of it is stored into the query's Query.queryId field.
 * The server then copies this value around, making it available in plan
 * tree(s) generated from the query.  The executor can then use this value
 * to blame query costs on the proper queryId.
 *
 * Portions Copyright (c) 1996-2021, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/utils/misc/queryjumble.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/hash.h"
#include "miscadmin.h"
#include "parser/scansup.h"
#include "parser/scanner.h"
#include "utils/queryjumble.h"
#include "utils/lsyscache.h"
#include "catalog/namespace.h"
#include "commands/defrem.h"
#include "utils/builtins.h"
#include "optimizer/spm.h"

#define JUMBLE_SIZE 1024 /* query serialization buffer size */

#define NSP_NAME(oid) isTempNamespace(oid) ? "pg_temp" : get_namespace_name(oid)

static uint64 compute_utility_queryid(const char *str, int query_len);
static void JumbleQueryInternal(JumbleState *jstate, Query *query);
static void JumbleRangeTable(JumbleState *jstate, List *rtable);
static void JumbleRowMarks(JumbleState *jstate, List *rowMarks);
static void JumbleExpr(JumbleState *jstate, Node *node);
static void RecordConstLocation(JumbleState *jstate, int location);
static void fill_in_constant_lengths(JumbleState *jstate, const char *query,
									 int query_loc);
static int	comp_location(const void *a, const void *b);

/*
 * Given a possibly multi-statement source string, confine our attention to the
 * relevant part of the string.
 */
const char *
CleanQuerytext(const char *query, int *location, int *len)
{
	int query_location = *location;
	int query_len = *len;

	/* First apply starting offset, unless it's -1 (unknown). */
	if (query_location >= 0)
	{
		Assert(query_location <= strlen(query));
		query += query_location;
		/* Length of 0 (or -1) means "rest of string" */
		if (query_len <= 0)
			query_len = strlen(query);
		else
			Assert(query_len <= strlen(query));
	}
	else
	{
		/* If query location is unknown, distrust query_len as well */
		query_location = 0;
		query_len = strlen(query);
	}

	/*
	 * Discard leading and trailing whitespace, too.  Use scanner_isspace()
	 * not libc's isspace(), because we want to match the lexer's behavior.
	 */
	while (query_len > 0 && scanner_isspace(query[0]))
		query++, query_location++, query_len--;
	while (query_len > 0 && scanner_isspace(query[query_len - 1]))
		query_len--;

	*location = query_location;
	*len = query_len;

	return query;
}

JumbleState *
JumbleQuery(Query *query, const char *querytext)
{
	JumbleState *jstate = NULL;
	if (query->utilityStmt)
	{
		const char *sql;
		int query_location = query->stmt_location;
		int query_len = query->stmt_len;

		/*
		 * Confine our attention to the relevant part of the string, if the
		 * query is a portion of a multi-statement source string.
		 */
		sql = CleanQuerytext(querytext, &query_location, &query_len);

		query->queryId = compute_utility_queryid(sql, query_len);
	}
	else
	{
		jstate = (JumbleState *) palloc(sizeof(JumbleState));

		/* Set up workspace for query jumbling */
		jstate->jumble = (unsigned char *) palloc(JUMBLE_SIZE);
		jstate->jumble_len = 0;
		jstate->clocations_buf_size = 32;
		jstate->clocations = (LocationLen *)
			palloc(jstate->clocations_buf_size * sizeof(LocationLen));
		jstate->clocations_count = 0;
		jstate->highest_extern_param_id = 0;
		jstate->param_list = NIL;
		jstate->paramids = NULL;

		/* Compute query ID and mark the Query node with it */
		JumbleQueryInternal(jstate, query);
		query->queryId = hash_any(jstate->jumble, jstate->jumble_len);

		if (SPMTrackSqlId_hook)
			SPMTrackSqlId_hook(query);

		SPMFillParamList(jstate);

		/*
		 * If we are unlucky enough to get a hash of zero, use 1 instead, to
		 * prevent confusion with the utility-statement case.
		 */
		if (query->queryId == 0)
			query->queryId = 1;
	}

	return jstate;
}

/*
 * Compute a query identifier for the given utility query string.
 */
static uint64
compute_utility_queryid(const char *str, int query_len)
{
	uint64 queryId;

	queryId = hash_any((const unsigned char *) str, query_len);

	/*
	 * If we are unlucky enough to get a hash of zero(invalid), use
	 * queryID as 2 instead, queryID 1 is already in use for normal
	 * statements.
	 */
	if (queryId == 0)
		queryId = 2;

	return queryId;
}

/*
 * AppendJumble: Append a value that is substantive in a given query to
 * the current jumble.
 */
void
AppendJumble(JumbleState *jstate, const unsigned char *item, Size size)
{
	unsigned char *jumble = jstate->jumble;
	Size		jumble_len = jstate->jumble_len;

	/*
	 * Whenever the jumble buffer is full, we hash the current contents and
	 * reset the buffer to contain just that hash value, thus relying on the
	 * hash to summarize everything so far.
	 */
	while (size > 0)
	{
		Size		part_size;

		if (jumble_len >= JUMBLE_SIZE)
		{
			uint32 start_hash = hash_any(jumble, JUMBLE_SIZE);
			
			memcpy(jumble, &start_hash, sizeof(start_hash));
			jumble_len = sizeof(start_hash);
		}
		part_size = Min(size, JUMBLE_SIZE - jumble_len);
		memcpy(jumble + jumble_len, item, part_size);
		jumble_len += part_size;
		item += part_size;
		size -= part_size;
	}
	jstate->jumble_len = jumble_len;
}

#define MANE_MISSERR(oid, namefunc, type)                                                          \
	({                                                                                             \
		char *res = NULL;                                                                          \
		res = namefunc(oid);                                                                       \
		if (res == NULL)                                                                           \
			elog(ERROR, "cache lookup failed for %s %u", type, oid);                               \
		(res);                                                                                     \
	})

#define NSP_NAME_MISSERR(oid, type)                                                                \
	({                                                                                             \
		char *res = isTempNamespace(oid) ? "pg_temp" : get_namespace_name(oid);                    \
		if (res == NULL)                                                                           \
			elog(ERROR, "cache lookup failed for %s %u", type, oid);                               \
		(res);                                                                                     \
	})

/*
 * Wrappers around AppendJumble to encapsulate details of serialization
 * of individual local variable elements.
 */
#define APP_JUMB(item) \
	AppendJumble(jstate, (const unsigned char *) &(item), sizeof(item))
#define APP_JUMB_STRING(str)                                                 \
	do                                                                       \
	{                                                                        \
		Assert(str);                                                         \
		AppendJumble(jstate, (const unsigned char *)(str), strlen(str) + 1); \
	} while (0)

#define APP_JUMB_TYPE(typ)                                                                         \
	do                                                                                             \
	{                                                                                              \
		if (OidIsValid(typ) && !IsSystemObjOid(typ))                                               \
		{                                                                                          \
			char *name = NULL;                                                                     \
			name = NSP_NAME_MISSERR(get_typ_namespace(typ), "type");                               \
			APP_JUMB_STRING(name);                                                                 \
			APP_JUMB_STRING(".");                                                                  \
			name = MANE_MISSERR(typ, get_typ_name, "type");                                        \
			APP_JUMB_STRING(name);                                                                 \
		}                                                                                          \
		else                                                                                       \
			APP_JUMB(typ);                                                                         \
	} while (0)

#define APP_JUMB_FUNC(func)                                                                        \
	do                                                                                             \
	{                                                                                              \
		if (OidIsValid(func) && !IsSystemObjOid(func))                                             \
		{                                                                                          \
			char *name = NULL;                                                                     \
			name = NSP_NAME_MISSERR(get_func_namespace(func), "function");                         \
			APP_JUMB_STRING(name);                                                                 \
			APP_JUMB_STRING(".");                                                                  \
			name = MANE_MISSERR(func, get_func_name, "function");                                  \
			APP_JUMB_STRING(name);                                                                 \
		}                                                                                          \
		else                                                                                       \
			APP_JUMB(func);                                                                        \
	} while (0)

#define APP_JUMB_OP(op)                                                                            \
	do                                                                                             \
	{                                                                                              \
		if (OidIsValid(op) && !IsSystemObjOid(op))                                                 \
		{                                                                                          \
			char *name = NULL;                                                                     \
			name = NSP_NAME_MISSERR(get_opnamespace(op), "operator");                              \
			APP_JUMB_STRING(name);                                                                 \
			APP_JUMB_STRING(".");                                                                  \
			name = MANE_MISSERR(op, get_opname, "operator");                                       \
			APP_JUMB_STRING(name);                                                                 \
		}                                                                                          \
		else                                                                                       \
			APP_JUMB(op);                                                                          \
	} while (0)

#define APP_JUMB_COLL(coll)                                                                        \
	do                                                                                             \
	{                                                                                              \
		if (OidIsValid(coll) && !IsSystemObjOid(coll))                                             \
		{                                                                                          \
			char *name = NULL;                                                                     \
			name = NSP_NAME_MISSERR(get_collation_namespace(coll), "collation");                   \
			APP_JUMB_STRING(name);                                                                 \
			APP_JUMB_STRING(".");                                                                  \
			name = MANE_MISSERR(coll, get_collation_name, "collation");                            \
			APP_JUMB_STRING(name);                                                                 \
		}                                                                                          \
		else                                                                                       \
			APP_JUMB(coll);                                                                        \
	} while (0)

#define APP_JUMB_REL(rel)                                                                          \
	do                                                                                             \
	{                                                                                              \
		if (OidIsValid(rel) && !IsSystemObjOid(rel))                                               \
		{                                                                                          \
			char *name = NULL;                                                                     \
			name = NSP_NAME_MISSERR(get_rel_namespace(rel), "relation");                           \
			APP_JUMB_STRING(name);                                                                 \
			APP_JUMB_STRING(".");                                                                  \
			name = MANE_MISSERR(rel, get_rel_name, "relation");                                    \
			APP_JUMB_STRING(name);                                                                 \
		}                                                                                          \
		else                                                                                       \
			APP_JUMB(rel);                                                                         \
	} while (0)

#define APP_JUMB_OPCLASS(opc)                                                                      \
	do                                                                                             \
	{                                                                                              \
		if (OidIsValid(opc) && !IsSystemObjOid(opc))                                               \
		{                                                                                          \
			char *name;                                                                            \
			char *nspace;                                                                          \
			get_opclass_name_namespace(opc, &name, &nspace);                                       \
			APP_JUMB_STRING(nspace);                                                               \
			APP_JUMB_STRING(".");                                                                  \
			APP_JUMB_STRING(name);                                                                 \
		}                                                                                          \
		else                                                                                       \
			APP_JUMB(opc);                                                                         \
	} while (0)

#define APP_JUMB_CONSTRAINT(constraint)                                                            \
	do                                                                                             \
	{                                                                                              \
		if (OidIsValid(constraint) && !IsSystemObjOid(constraint))                                 \
		{                                                                                          \
			char *name;                                                                            \
			Oid   relid;                                                                           \
			name = get_constraint_name_relid(constraint, &relid);                                  \
			APP_JUMB_REL(relid);                                                                   \
			APP_JUMB_STRING(".[");                                                                 \
			APP_JUMB_STRING(name);                                                                 \
			APP_JUMB_STRING("]");                                                                  \
		}                                                                                          \
		else                                                                                       \
			APP_JUMB(constraint);                                                                  \
	} while (0)

/*
 * JumbleQueryInternal: Selectively serialize the query tree, appending
 * significant data to the "query jumble" while ignoring nonsignificant data.
 *
 * Rule of thumb for what to include is that we should ignore anything not
 * semantically significant (such as alias names) as well as anything that can
 * be deduced from child nodes (else we'd just be double-hashing that piece
 * of information).
 */
static void
JumbleQueryInternal(JumbleState *jstate, Query *query)
{
	Assert(IsA(query, Query));
	Assert(query->utilityStmt == NULL);

	APP_JUMB(query->commandType);
	/* resultRelation is usually predictable from commandType */
	JumbleExpr(jstate, (Node *) query->cteList);
	JumbleRangeTable(jstate, query->rtable);
	JumbleExpr(jstate, (Node *) query->jointree);
	JumbleExpr(jstate, (Node *) query->mergeActionList);
	JumbleExpr(jstate, (Node *) query->targetList);
	JumbleExpr(jstate, (Node *) query->onConflict);
	JumbleExpr(jstate, (Node *) query->returningList);
	JumbleExpr(jstate, (Node *) query->groupClause);
	JumbleExpr(jstate, (Node *) query->groupingSets);
	JumbleExpr(jstate, query->havingQual);
	JumbleExpr(jstate, (Node *) query->windowClause);
	JumbleExpr(jstate, (Node *) query->distinctClause);
	JumbleExpr(jstate, (Node *) query->sortClause);
	JumbleExpr(jstate, query->limitOffset);
	JumbleExpr(jstate, query->limitCount);
	JumbleRowMarks(jstate, query->rowMarks);
	JumbleExpr(jstate, query->setOperations);
}

/*
 * Jumble a range table
 */
static void
JumbleRangeTable(JumbleState *jstate, List *rtable)
{
	ListCell   *lc;

	foreach(lc, rtable)
	{
		RangeTblEntry *rte = lfirst_node(RangeTblEntry, lc);

		APP_JUMB(rte->rtekind);
		switch (rte->rtekind)
		{
			case RTE_RELATION:
				APP_JUMB_REL(rte->relid);

				JumbleExpr(jstate, (Node *) rte->tablesample);
				break;
			case RTE_SUBQUERY:
				JumbleQueryInternal(jstate, rte->subquery);
				break;
			case RTE_JOIN:
				APP_JUMB(rte->jointype);
				break;
			case RTE_FUNCTION:
				JumbleExpr(jstate, (Node *) rte->functions);
				break;
			case RTE_TABLEFUNC:
				JumbleExpr(jstate, (Node *) rte->tablefunc);
				break;
			case RTE_VALUES:
				JumbleExpr(jstate, (Node *) rte->values_lists);
				break;
			case RTE_CTE:

				/*
				 * Depending on the CTE name here isn't ideal, but it's the
				 * only info we have to identify the referenced WITH item.
				 */
				APP_JUMB_STRING(rte->ctename);
				APP_JUMB(rte->ctelevelsup);
				break;
			case RTE_NAMEDTUPLESTORE:
				APP_JUMB_STRING(rte->enrname);
				break;
			default:
				elog(ERROR, "unrecognized RTE kind: %d", (int) rte->rtekind);
				break;
		}
	}
}

/*
 * Jumble a rowMarks list
 */
static void
JumbleRowMarks(JumbleState *jstate, List *rowMarks)
{
	ListCell   *lc;

	foreach(lc, rowMarks)
	{
		RowMarkClause *rowmark = lfirst_node(RowMarkClause, lc);

		if (!rowmark->pushedDown)
		{
			APP_JUMB(rowmark->rti);
			APP_JUMB(rowmark->strength);
			APP_JUMB(rowmark->waitPolicy);
		}
	}
}

/*
 * Jumble an expression tree
 *
 * In general this function should handle all the same node types that
 * expression_tree_walker() does, and therefore it's coded to be as parallel
 * to that function as possible.  However, since we are only invoked on
 * queries immediately post-parse-analysis, we need not handle node types
 * that only appear in planning.
 *
 * Note: the reason we don't simply use expression_tree_walker() is that the
 * point of that function is to support tree walkers that don't care about
 * most tree node types, but here we care about all types.  We should complain
 * about any unrecognized node type.
 */
static void
JumbleExpr(JumbleState *jstate, Node *node)
{
	ListCell   *temp;

	if (node == NULL)
		return;

	/* Guard against stack overflow due to overly complex expressions */
	check_stack_depth();

	/*
	 * We always emit the node's NodeTag, then any additional fields that are
	 * considered significant, and then we recurse to any child nodes.
	 */
	APP_JUMB(node->type);

	switch (nodeTag(node))
	{
		case T_Var:
			{
				Var		   *var = (Var *) node;

				APP_JUMB(var->varno);
				APP_JUMB(var->varattno);
				APP_JUMB(var->varlevelsup);
			}
			break;
		case T_Const:
			{
				Const	   *c = (Const *) node;

				/* We jumble only the constant's type, not its value */
				APP_JUMB_TYPE(c->consttype);
				/* Also, record its parse location for query normalization */
				RecordConstLocation(jstate, c->location);
			}
			break;
		case T_Param:
			{
				Param	   *p = (Param *) node;

				APP_JUMB(p->paramkind);
				APP_JUMB(p->paramid);

				APP_JUMB_TYPE(p->paramtype);

				/* Also, track the highest external Param id */
				if (p->paramkind == PARAM_EXTERN &&
					p->paramid > jstate->highest_extern_param_id)
					jstate->highest_extern_param_id = p->paramid;
				if (p->paramkind == PARAM_EXTERN && !bms_is_member(p->paramid, jstate->paramids))
				{
					Param *spm_param = copyObject(p);
					spm_param->location = 0;
					jstate->param_list = lappend(jstate->param_list, p);
					jstate->paramids = bms_add_member(jstate->paramids, p->paramid);
				}
			}
			break;
		case T_Aggref:
			{
				Aggref	   *expr = (Aggref *) node;

				APP_JUMB_FUNC(expr->aggfnoid);

				JumbleExpr(jstate, (Node *) expr->aggdirectargs);
				JumbleExpr(jstate, (Node *) expr->args);
				JumbleExpr(jstate, (Node *) expr->aggorder);
				JumbleExpr(jstate, (Node *) expr->aggdistinct);
				JumbleExpr(jstate, (Node *) expr->aggfilter);
			}
			break;
		case T_GroupingFunc:
			{
				GroupingFunc *grpnode = (GroupingFunc *) node;

				JumbleExpr(jstate, (Node *) grpnode->refs);
			}
			break;
		case T_WindowFunc:
			{
				WindowFunc *expr = (WindowFunc *) node;

				APP_JUMB_FUNC(expr->winfnoid);

				APP_JUMB(expr->winref);
				JumbleExpr(jstate, (Node *) expr->args);
				JumbleExpr(jstate, (Node *) expr->aggfilter);
			}
			break;
		case T_ArrayRef:
			{
				ArrayRef   *aref = (ArrayRef *) node;

				JumbleExpr(jstate, (Node *) aref->refupperindexpr);
				JumbleExpr(jstate, (Node *) aref->reflowerindexpr);
				JumbleExpr(jstate, (Node *) aref->refexpr);
				JumbleExpr(jstate, (Node *) aref->refassgnexpr);
			}
			break;
		case T_FuncExpr:
			{
				FuncExpr   *expr = (FuncExpr *) node;

				APP_JUMB_FUNC(expr->funcid);

				JumbleExpr(jstate, (Node *) expr->args);
			}
			break;
		case T_NamedArgExpr:
			{
				NamedArgExpr *nae = (NamedArgExpr *) node;

				APP_JUMB(nae->argnumber);
				JumbleExpr(jstate, (Node *) nae->arg);
			}
			break;
		case T_OpExpr:
		case T_DistinctExpr:	/* struct-equivalent to OpExpr */
		case T_NullIfExpr:		/* struct-equivalent to OpExpr */
			{
				OpExpr	   *expr = (OpExpr *) node;

				APP_JUMB_OP(expr->opno);

				JumbleExpr(jstate, (Node *) expr->args);
			}
			break;
		case T_ScalarArrayOpExpr:
			{
				ScalarArrayOpExpr *expr = (ScalarArrayOpExpr *) node;

				APP_JUMB_OP(expr->opno);

				APP_JUMB(expr->useOr);
				JumbleExpr(jstate, (Node *) expr->args);
			}
			break;
		case T_BoolExpr:
			{
				BoolExpr   *expr = (BoolExpr *) node;

				APP_JUMB(expr->boolop);
				JumbleExpr(jstate, (Node *) expr->args);
			}
			break;
		case T_SubLink:
			{
				SubLink    *sublink = (SubLink *) node;

				APP_JUMB(sublink->subLinkType);
				APP_JUMB(sublink->subLinkId);
				JumbleExpr(jstate, (Node *) sublink->testexpr);
				JumbleQueryInternal(jstate, castNode(Query, sublink->subselect));
			}
			break;
		case T_FieldSelect:
			{
				FieldSelect *fs = (FieldSelect *) node;

				APP_JUMB(fs->fieldnum);
				JumbleExpr(jstate, (Node *) fs->arg);
			}
			break;
		case T_FieldStore:
			{
				FieldStore *fstore = (FieldStore *) node;

				JumbleExpr(jstate, (Node *) fstore->arg);
				JumbleExpr(jstate, (Node *) fstore->newvals);
			}
			break;
		case T_RelabelType:
			{
				RelabelType *rt = (RelabelType *) node;

				APP_JUMB_TYPE(rt->resulttype);

				JumbleExpr(jstate, (Node *) rt->arg);
			}
			break;
		case T_CoerceViaIO:
			{
				CoerceViaIO *cio = (CoerceViaIO *) node;

				APP_JUMB_TYPE(cio->resulttype);

				JumbleExpr(jstate, (Node *) cio->arg);
			}
			break;
		case T_ArrayCoerceExpr:
			{
				ArrayCoerceExpr *acexpr = (ArrayCoerceExpr *) node;

				APP_JUMB_TYPE(acexpr->resulttype);

				JumbleExpr(jstate, (Node *) acexpr->arg);
				JumbleExpr(jstate, (Node *) acexpr->elemexpr);
			}
			break;
		case T_ConvertRowtypeExpr:
			{
				ConvertRowtypeExpr *crexpr = (ConvertRowtypeExpr *) node;

				APP_JUMB_TYPE(crexpr->resulttype);

				JumbleExpr(jstate, (Node *) crexpr->arg);
			}
			break;
		case T_CollateExpr:
			{
				CollateExpr *ce = (CollateExpr *) node;

				APP_JUMB_COLL(ce->collOid);

				JumbleExpr(jstate, (Node *) ce->arg);
			}
			break;
		case T_CaseExpr:
			{
				CaseExpr   *caseexpr = (CaseExpr *) node;

				JumbleExpr(jstate, (Node *) caseexpr->arg);
				foreach(temp, caseexpr->args)
				{
					CaseWhen   *when = lfirst_node(CaseWhen, temp);

					JumbleExpr(jstate, (Node *) when->expr);
					JumbleExpr(jstate, (Node *) when->result);
				}
				JumbleExpr(jstate, (Node *) caseexpr->defresult);
			}
			break;
		case T_CaseTestExpr:
			{
				CaseTestExpr *ct = (CaseTestExpr *) node;

				APP_JUMB_TYPE(ct->typeId);
			}
			break;
		case T_ArrayExpr:
			JumbleExpr(jstate, (Node *) ((ArrayExpr *) node)->elements);
			break;
		case T_RowExpr:
			JumbleExpr(jstate, (Node *) ((RowExpr *) node)->args);
			break;
		case T_RowCompareExpr:
			{
				RowCompareExpr *rcexpr = (RowCompareExpr *) node;

				APP_JUMB(rcexpr->rctype);
				JumbleExpr(jstate, (Node *) rcexpr->largs);
				JumbleExpr(jstate, (Node *) rcexpr->rargs);
			}
			break;
		case T_CoalesceExpr:
			JumbleExpr(jstate, (Node *) ((CoalesceExpr *) node)->args);
			break;
		case T_MinMaxExpr:
			{
				MinMaxExpr *mmexpr = (MinMaxExpr *) node;

				APP_JUMB(mmexpr->op);
				JumbleExpr(jstate, (Node *) mmexpr->args);
			}
			break;
		case T_SQLValueFunction:
			{
				SQLValueFunction *svf = (SQLValueFunction *) node;

				APP_JUMB(svf->op);
				/* type is fully determined by op */
				APP_JUMB(svf->typmod);
			}
			break;
		case T_XmlExpr:
			{
				XmlExpr    *xexpr = (XmlExpr *) node;

				APP_JUMB(xexpr->op);
				JumbleExpr(jstate, (Node *) xexpr->named_args);
				JumbleExpr(jstate, (Node *) xexpr->args);
			}
			break;
		case T_NullTest:
			{
				NullTest   *nt = (NullTest *) node;

				APP_JUMB(nt->nulltesttype);
				JumbleExpr(jstate, (Node *) nt->arg);
			}
			break;
		case T_BooleanTest:
			{
				BooleanTest *bt = (BooleanTest *) node;

				APP_JUMB(bt->booltesttype);
				JumbleExpr(jstate, (Node *) bt->arg);
			}
			break;
		case T_CoerceToDomain:
			{
				CoerceToDomain *cd = (CoerceToDomain *) node;

				APP_JUMB_TYPE(cd->resulttype);

				JumbleExpr(jstate, (Node *) cd->arg);
			}
			break;
		case T_CoerceToDomainValue:
			{
				CoerceToDomainValue *cdv = (CoerceToDomainValue *) node;

				APP_JUMB_TYPE(cdv->typeId);
			}
			break;
		case T_SetToDefault:
			{
				SetToDefault *sd = (SetToDefault *) node;

				APP_JUMB_TYPE(sd->typeId);
			}
			break;
		case T_CurrentOfExpr:
			{
				CurrentOfExpr *ce = (CurrentOfExpr *) node;

				APP_JUMB(ce->cvarno);
				if (ce->cursor_name)
					APP_JUMB_STRING(ce->cursor_name);
				APP_JUMB(ce->cursor_param);
			}
			break;
		case T_NextValueExpr:
			{
				NextValueExpr *nve = (NextValueExpr *) node;

				APP_JUMB_REL(nve->seqid);
				APP_JUMB_TYPE(nve->typeId);
			}
			break;
		case T_InferenceElem:
			{
				InferenceElem *ie = (InferenceElem *) node;

				APP_JUMB_COLL(ie->infercollid);
				APP_JUMB_OPCLASS(ie->inferopclass);

				JumbleExpr(jstate, ie->expr);
			}
			break;
		case T_TargetEntry:
			{
				TargetEntry *tle = (TargetEntry *) node;

				APP_JUMB(tle->resno);
				APP_JUMB(tle->ressortgroupref);
				JumbleExpr(jstate, (Node *) tle->expr);
			}
			break;
		case T_RangeTblRef:
			{
				RangeTblRef *rtr = (RangeTblRef *) node;

				APP_JUMB(rtr->rtindex);
			}
			break;
		case T_JoinExpr:
			{
				JoinExpr   *join = (JoinExpr *) node;

				APP_JUMB(join->jointype);
				APP_JUMB(join->isNatural);
				APP_JUMB(join->rtindex);
				JumbleExpr(jstate, join->larg);
				JumbleExpr(jstate, join->rarg);
				JumbleExpr(jstate, join->quals);
			}
			break;
		case T_FromExpr:
			{
				FromExpr   *from = (FromExpr *) node;

				JumbleExpr(jstate, (Node *) from->fromlist);
				JumbleExpr(jstate, from->quals);
			}
			break;
		case T_OnConflictExpr:
			{
				OnConflictExpr *conf = (OnConflictExpr *) node;

				APP_JUMB(conf->action);
				JumbleExpr(jstate, (Node *) conf->arbiterElems);
				JumbleExpr(jstate, conf->arbiterWhere);
				JumbleExpr(jstate, (Node *) conf->onConflictSet);
				JumbleExpr(jstate, conf->onConflictWhere);

				APP_JUMB_CONSTRAINT(conf->constraint);

				APP_JUMB(conf->exclRelIndex);
				JumbleExpr(jstate, (Node *) conf->exclRelTlist);
			}
			break;
		case T_MergeAction:
			{
				MergeAction *mergeaction = (MergeAction *) node;

				APP_JUMB(mergeaction->matched);
				APP_JUMB(mergeaction->commandType);
				JumbleExpr(jstate, mergeaction->qual);
				JumbleExpr(jstate, (Node *) mergeaction->targetList);
				JumbleExpr(jstate, (Node *) mergeaction->deleteAction);
			}
			break;
		case T_ConnectByExpr:
			{
				ConnectByExpr *cb = (ConnectByExpr *) node;

				APP_JUMB(cb->nocycle);
				JumbleExpr(jstate, cb->expr);
				JumbleExpr(jstate, cb->start);
				JumbleExpr(jstate, (Node *) cb->sort);
			}
			break;
		case T_LevelExpr:
		case T_CBIsLeafExpr:
		case T_CBIsCycleExpr:
			break;
		case T_PriorExpr:
			JumbleExpr(jstate, ((PriorExpr *) node)->expr);
			APP_JUMB(((PriorExpr *) node)->for_path);
			break;
		case T_CBRootExpr:
			JumbleExpr(jstate, ((CBRootExpr *) node)->expr);
			break;
		case T_CBPathExpr:
			JumbleExpr(jstate, ((CBPathExpr *) node)->expr);
			break;
		case T_List:
			foreach(temp, (List *) node)
			{
				JumbleExpr(jstate, (Node *) lfirst(temp));
			}
			break;
		case T_IntList:
			foreach(temp, (List *) node)
			{
				APP_JUMB(lfirst_int(temp));
			}
			break;
		case T_SortGroupClause:
			{
				SortGroupClause *sgc = (SortGroupClause *) node;

				APP_JUMB(sgc->tleSortGroupRef);
				APP_JUMB_OP(sgc->eqop);
				APP_JUMB_OP(sgc->sortop);
				APP_JUMB(sgc->nulls_first);
			}
			break;
		case T_GroupingSet:
			{
				GroupingSet *gsnode = (GroupingSet *) node;

				JumbleExpr(jstate, (Node *) gsnode->content);
			}
			break;
		case T_WindowClause:
			{
				WindowClause *wc = (WindowClause *) node;

				APP_JUMB(wc->winref);
				APP_JUMB(wc->frameOptions);
				JumbleExpr(jstate, (Node *) wc->partitionClause);
				JumbleExpr(jstate, (Node *) wc->orderClause);
				JumbleExpr(jstate, wc->startOffset);
				JumbleExpr(jstate, wc->endOffset);
			}
			break;
		case T_CommonTableExpr:
			{
				CommonTableExpr *cte = (CommonTableExpr *) node;

				/* we store the string name because RTE_CTE RTEs need it */
				APP_JUMB_STRING(cte->ctename);
				APP_JUMB(cte->ctematerialized);
				JumbleQueryInternal(jstate, castNode(Query, cte->ctequery));
			}
			break;
		case T_SetOperationStmt:
			{
				SetOperationStmt *setop = (SetOperationStmt *) node;

				APP_JUMB(setop->op);
				APP_JUMB(setop->all);
				JumbleExpr(jstate, setop->larg);
				JumbleExpr(jstate, setop->rarg);
			}
			break;
		case T_RangeTblFunction:
			{
				RangeTblFunction *rtfunc = (RangeTblFunction *) node;

				JumbleExpr(jstate, rtfunc->funcexpr);
			}
			break;
		case T_TableFunc:
			{
				TableFunc  *tablefunc = (TableFunc *) node;

				JumbleExpr(jstate, tablefunc->docexpr);
				JumbleExpr(jstate, tablefunc->rowexpr);
				JumbleExpr(jstate, (Node *) tablefunc->colexprs);
			}
			break;
		case T_TableSampleClause:
			{
				TableSampleClause *tsc = (TableSampleClause *) node;

				APP_JUMB_FUNC(tsc->tsmhandler);

				JumbleExpr(jstate, (Node *) tsc->args);
				JumbleExpr(jstate, (Node *) tsc->repeatable);
			}
			break;
		case T_IntoClause:
		case T_OidList:
			/* Avoid warning throw from utility */
			break;
#ifdef _PG_ORCL_
		case T_RownumExpr:
			{
				RecordConstLocation(jstate, ((RownumExpr*)node)->location);
			}
			break;
#endif
		default:
			/* Only a warning, since we can stumble along anyway */
			elog(WARNING, "unrecognized node type: %d",
				 (int) nodeTag(node));
			break;
	}
}

/*
 * Record location of constant within query string of query tree
 * that is currently being walked.
 */
static void
RecordConstLocation(JumbleState *jstate, int location)
{
	/* -1 indicates unknown or undefined location */
	if (location >= 0)
	{
		/* enlarge array if needed */
		if (jstate->clocations_count >= jstate->clocations_buf_size)
		{
			jstate->clocations_buf_size *= 2;
			jstate->clocations = (LocationLen *)
				repalloc(jstate->clocations,
						 jstate->clocations_buf_size *
						 sizeof(LocationLen));
		}
		jstate->clocations[jstate->clocations_count].location = location;
		/* initialize lengths to -1 to simplify third-party module usage */
		jstate->clocations[jstate->clocations_count].length = -1;
		jstate->clocations_count++;
	}
}

/*
 * Generate a normalized version of the query string that will be used to
 * represent all similar queries.
 *
 * Note that the normalized representation may well vary depending on
 * just which "equivalent" query is used to create the hashtable entry.
 * We assume this is OK.
 *
 * If query_loc > 0, then "query" has been advanced by that much compared to
 * the original string start, so we need to translate the provided locations
 * to compensate.  (This lets us avoid re-scanning statements before the one
 * of interest, so it's worth doing.)
 *
 * *query_len_p contains the input string length, and is updated with
 * the result string length on exit.  The resulting string might be longer
 * or shorter depending on what happens with replacement of constants.
 *
 * Returns a palloc'd string.
 */
char *
generate_normalized_query(JumbleState *jstate, const char *query,
						  int query_loc, int *query_len_p, bool use_param_no)
{
	char	   *norm_query;
	int			query_len = *query_len_p;
	int			i,
				norm_query_buflen,	/* Space allowed for norm_query */
				len_to_wrt,		/* Length (in bytes) to write */
				quer_loc = 0,	/* Source query byte location */
				n_quer_loc = 0, /* Normalized query byte location */
				last_off = 0,	/* Offset from start for previous tok */
				last_tok_len = 0;	/* Length (in bytes) of that tok */

	/*
	 * Get constants' lengths (core system only gives us locations).  Note
	 * this also ensures the items are sorted by location.
	 */
	fill_in_constant_lengths(jstate, query, query_loc);

	/*
	 * Allow for $n symbols to be longer than the constants they replace.
	 * Constants must take at least one byte in text form, while a $n symbol
	 * certainly isn't more than 11 bytes, even if n reaches INT_MAX.  We
	 * could refine that limit based on the max value of n for the current
	 * query, but it hardly seems worth any extra effort to do so.
	 */
	norm_query_buflen = query_len + jstate->clocations_count * 10;

	/* Allocate result buffer */
	norm_query = palloc(norm_query_buflen + 1);

	for (i = 0; i < jstate->clocations_count; i++)
	{
		int			off,		/* Offset from start for cur tok */
					tok_len;	/* Length (in bytes) of that tok */

		off = jstate->clocations[i].location;
		/* Adjust recorded location if we're dealing with partial string */
		off -= query_loc;

		tok_len = jstate->clocations[i].length;

		if (tok_len < 0)
			continue;			/* ignore any duplicates */

		/* Copy next chunk (what precedes the next constant) */
		len_to_wrt = off - last_off;
		len_to_wrt -= last_tok_len;

		/* 
		 *	If the parser could not recognize tokons correctly,
		 *	return original query directly.
		 */
		if (len_to_wrt <= 0)
		{
			memcpy(norm_query, query, query_len);
			norm_query[query_len] = '\0';
			return norm_query;
		}

		memcpy(norm_query + n_quer_loc, query + quer_loc, len_to_wrt);
		n_quer_loc += len_to_wrt;

		/* And insert a param symbol in place of the constant token */
		if (use_param_no)
			norm_query[n_quer_loc++] = '?';
		else
			n_quer_loc +=
				sprintf(norm_query + n_quer_loc, "$%d", i + 1 + jstate->highest_extern_param_id);

		quer_loc = off + tok_len;
		last_off = off;
		last_tok_len = tok_len;
	}

	/*
	 * We've copied up until the last ignorable constant.  Copy over the
	 * remaining bytes of the original query string.
	 */
	len_to_wrt = query_len - quer_loc;

	Assert(len_to_wrt >= 0);
	memcpy(norm_query + n_quer_loc, query + quer_loc, len_to_wrt);
	n_quer_loc += len_to_wrt;

	Assert(n_quer_loc <= norm_query_buflen);
	norm_query[n_quer_loc] = '\0';

	*query_len_p = n_quer_loc;
	return norm_query;
}

/*
 * Given a valid SQL string and an array of constant-location records,
 * fill in the textual lengths of those constants.
 *
 * The constants may use any allowed constant syntax, such as float literals,
 * bit-strings, single-quoted strings and dollar-quoted strings.  This is
 * accomplished by using the public API for the core scanner.
 *
 * It is the caller's job to ensure that the string is a valid SQL statement
 * with constants at the indicated locations.  Since in practice the string
 * has already been parsed, and the locations that the caller provides will
 * have originated from within the authoritative parser, this should not be
 * a problem.
 *
 * Duplicate constant pointers are possible, and will have their lengths
 * marked as '-1', so that they are later ignored.  (Actually, we assume the
 * lengths were initialized as -1 to start with, and don't change them here.)
 *
 * If query_loc > 0, then "query" has been advanced by that much compared to
 * the original string start, so we need to translate the provided locations
 * to compensate.  (This lets us avoid re-scanning statements before the one
 * of interest, so it's worth doing.)
 *
 * N.B. There is an assumption that a '-' character at a Const location begins
 * a negative numeric constant.  This precludes there ever being another
 * reason for a constant to start with a '-'.
 */
static void
fill_in_constant_lengths(JumbleState *jstate, const char *query,
						 int query_loc)
{
	LocationLen *locs;
	core_yyscan_t yyscanner;
	core_yy_extra_type yyextra;
	core_YYSTYPE yylval;
	YYLTYPE		yylloc;
	int			last_loc = -1;
	int			i;

	/*
	 * Sort the records by location so that we can process them in order while
	 * scanning the query text.
	 */
	if (jstate->clocations_count > 1)
		qsort(jstate->clocations, jstate->clocations_count,
			  sizeof(LocationLen), comp_location);
	locs = jstate->clocations;

	/* initialize the flex scanner --- should match raw_parser() */
	yyscanner = scanner_init(query,
							 &yyextra,
							 ScanKeywords,
							 NumScanKeywords);

	/* we don't want to re-emit any escape string warnings */
	yyextra.escape_string_warning = false;

	/* Search for each constant, in sequence */
	for (i = 0; i < jstate->clocations_count; i++)
	{
		int			loc = locs[i].location;
		int			tok;

		/* Adjust recorded location if we're dealing with partial string */
		loc -= query_loc;

		Assert(loc >= 0);

		if (loc <= last_loc)
			continue;			/* Duplicate constant, ignore */

		/* Lex tokens until we find the desired constant */
		for (;;)
		{
			tok = core_yylex(&yylval, &yylloc, yyscanner);

			/* We should not hit end-of-string, but if we do, behave sanely */
			if (tok == 0)
				break;			/* out of inner for-loop */

			/*
			 * We should find the token position exactly, but if we somehow
			 * run past it, work with that.
			 */
			if (yylloc >= loc)
			{
				if (query[loc] == '-')
				{
					/*
					 * It's a negative value - this is the one and only case
					 * where we replace more than a single token.
					 *
					 * Do not compensate for the core system's special-case
					 * adjustment of location to that of the leading '-'
					 * operator in the event of a negative constant.  It is
					 * also useful for our purposes to start from the minus
					 * symbol.  In this way, queries like "select * from foo
					 * where bar = 1" and "select * from foo where bar = -2"
					 * will have identical normalized query strings.
					 */
					tok = core_yylex(&yylval, &yylloc, yyscanner);
					if (tok == 0)
						break;	/* out of inner for-loop */
				}

				/*
				 * We now rely on the assumption that flex has placed a zero
				 * byte after the text of the current token in scanbuf.
				 */
				locs[i].length = strlen(yyextra.scanbuf + loc);
				break;			/* out of inner for-loop */
			}
		}

		/* If we hit end-of-string, give up, leaving remaining lengths -1 */
		if (tok == 0)
			break;

		last_loc = loc;
	}

	scanner_finish(yyscanner);
}

/*
 * comp_location: comparator for qsorting LocationLen structs by location
 */
static int
comp_location(const void *a, const void *b)
{
	int			l = ((const LocationLen *) a)->location;
	int			r = ((const LocationLen *) b)->location;

	if (l < r)
		return -1;
	else if (l > r)
		return +1;
	else
		return 0;
}
