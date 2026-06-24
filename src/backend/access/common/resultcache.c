/*-------------------------------------------------------------------------
 *
 * resultcache.c
 *	  result cache managment code
 *
 * Portions Copyright (c) 2018, Tencent OpenTenBase-C Group
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/access/common/resultcache.c
 *
 */

#include "postgres.h"
#include "access/result_cache.h"
#include "storage/spin.h"
#include "storage/shmem.h"
#include "pgxc/pgxcnode.h"
#include "utils/memutils.h"
#include "optimizer/clauses.h"
#include "executor/executor.h"
#include "access/heapam.h"
#include "access/hash.h"
#include "storage/lockdefs.h"
#include "utils/rel.h"
#include "utils/builtins.h"
#include "pgxc/nodemgr.h"
#include "nodes/parsenodes.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"

bool		enable_result_cache;
int			result_cache_partition_number;
int			result_cache_number_per_partition;
int			max_size_per_cached_result;


static HTAB *ResultCacheHash;
ResultCachePartition *ResultCachePartitions;

#define result_cache_hash_code(query) get_hash_value(ResultCacheHash, (void *)query)
#define result_cache_partition_id(hashcode) hashcode % CACHE_PARTITION_NUMBER
#define result_cache_partition_ctl(partid) (ResultCachePartitions[partid])

#define ENCODE_ITEM(item) \
	EncodeItem(jstate, (const unsigned char *) &(item), sizeof(item))
#define ENCODE_STRING(str) \
	EncodeItem(jstate, (const unsigned char *) (str), strlen(str) + 1)


typedef struct LocationLen
{
	int			location;		/* start offset in query text */
	int			length;			/* length in bytes, or -1 to ignore */
}			LocationLen;

typedef struct QueryEncoder
{
	unsigned char *encoded_result;
	Size		encoded_len;
	int			highest_extern_param_id;
} QueryEncoder;

static void EncodeItem(QueryEncoder *jstate, const unsigned char *item, Size size);
static void EncodeExpr(QueryEncoder *jstate, Node *node);
static void EncodeRangeTable(QueryEncoder *jstate, List *rtable);
static void EncodeQuery(QueryEncoder *jstate, Query *query);


static void
EncodeItem(QueryEncoder *jstate, const unsigned char *item, Size size)
{
	unsigned char *result = jstate->encoded_result;
	Size		len = jstate->encoded_len;

	while (size > 0)
	{
		Size		part_size;

		if (len >= MAX_QUERY_LENGTH)
		{
			uint32		start_hash = hash_any(result, MAX_QUERY_LENGTH);

			memcpy(result, &start_hash, sizeof(start_hash));
			len = sizeof(start_hash);
		}
		part_size = Min(size, MAX_QUERY_LENGTH - len);
		memcpy(result + len, item, part_size);
		len += part_size;
		item += part_size;
		size -= part_size;
	}
	jstate->encoded_len = len;
}

static void
EncodeExpr(QueryEncoder *jstate, Node *node)
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
	ENCODE_ITEM(node->type);

	switch (nodeTag(node))
	{
		case T_Var:
			{
				Var		   *var = (Var *) node;

				ENCODE_ITEM(var->varno);
				ENCODE_ITEM(var->varattno);
				ENCODE_ITEM(var->varlevelsup);
			}
			break;
		case T_Const:
			{
				Const	   *c = (Const *) node;
				int			data_length;

				ENCODE_ITEM(c->consttype);
				ENCODE_ITEM(c->constisnull);
				if (!c->constisnull)
				{
					if (c->constlen >= 0 && c->constlen < 16)
					{
						if (c->constbyval)
							ENCODE_ITEM(c->constvalue);
						else
							EncodeItem(jstate, (const unsigned char *) DatumGetPointer(c->constvalue), c->constlen);
					}
					else if (c->constlen == -1)
					{
						/* varlena */
						Pointer		val = DatumGetPointer(c->constvalue);
						char	   *resultptr;

						if (VARATT_IS_EXTERNAL(val))
						{
							if (VARATT_IS_EXTERNAL_EXPANDED(val))
							{
								/*
								 * we want to flatten the expanded value so
								 * that the constructed tuple doesn't depend
								 * on it
								 */
								ExpandedObjectHeader *eoh = DatumGetEOHP(c->constvalue);

								data_length = EOH_get_flat_size(eoh);
								resultptr = palloc0(data_length);
								EOH_flatten_into(eoh, (void *) resultptr, data_length);
								EncodeItem(jstate, (const unsigned char *) resultptr, data_length);
								pfree(resultptr);
							}
							else
							{
								/*
								 * no alignment, since it's short by
								 * definition
								 */
								data_length = VARSIZE_EXTERNAL(val);
								EncodeItem(jstate, (const unsigned char *) val, data_length);
							}
						}
						else if (VARATT_IS_SHORT(val))
						{
							/* no alignment for short varlenas */
							data_length = VARSIZE_SHORT(val);
							EncodeItem(jstate, (const unsigned char *) val, data_length);
						}
						else
						{
							/* full 4-byte header varlena */
							data_length = VARSIZE(val);
							EncodeItem(jstate, (const unsigned char *) val, data_length);
						}
					}
					else if (c->constlen == -2)
					{
						/* cstring ... never needs alignment */

						data_length = strlen(DatumGetCString(c->constvalue)) + 1;
						EncodeItem(jstate, (const unsigned char *) DatumGetPointer(c->constvalue), data_length);
					}
					else
					{
						data_length = c->constlen;
						EncodeItem(jstate, (const unsigned char *) DatumGetPointer(c->constvalue), data_length);
					}

				}
			}
			break;
		case T_Param:
			{
				Param	   *p = (Param *) node;

				ENCODE_ITEM(p->paramkind);
				ENCODE_ITEM(p->paramid);
				ENCODE_ITEM(p->paramtype);
				/* Also, track the highest external Param id */
				if (p->paramkind == PARAM_EXTERN &&
					p->paramid > jstate->highest_extern_param_id)
					jstate->highest_extern_param_id = p->paramid;
			}
			break;
		case T_Aggref:
			{
				Aggref	   *expr = (Aggref *) node;

				ENCODE_ITEM(expr->aggfnoid);
				EncodeExpr(jstate, (Node *) expr->aggdirectargs);
				EncodeExpr(jstate, (Node *) expr->args);
				EncodeExpr(jstate, (Node *) expr->aggorder);
				EncodeExpr(jstate, (Node *) expr->aggdistinct);
				EncodeExpr(jstate, (Node *) expr->aggfilter);
			}
			break;
		case T_GroupingFunc:
			{
				GroupingFunc *grpnode = (GroupingFunc *) node;

				EncodeExpr(jstate, (Node *) grpnode->refs);
			}
			break;
		case T_WindowFunc:
			{
				WindowFunc *expr = (WindowFunc *) node;

				ENCODE_ITEM(expr->winfnoid);
				ENCODE_ITEM(expr->winref);
				EncodeExpr(jstate, (Node *) expr->args);
				EncodeExpr(jstate, (Node *) expr->aggfilter);
			}
			break;
		case T_ArrayRef:
			{
				ArrayRef   *aref = (ArrayRef *) node;

				EncodeExpr(jstate, (Node *) aref->refupperindexpr);
				EncodeExpr(jstate, (Node *) aref->reflowerindexpr);
				EncodeExpr(jstate, (Node *) aref->refexpr);
				EncodeExpr(jstate, (Node *) aref->refassgnexpr);
			}
			break;
		case T_FuncExpr:
			{
				FuncExpr   *expr = (FuncExpr *) node;

				ENCODE_ITEM(expr->funcid);
				EncodeExpr(jstate, (Node *) expr->args);
			}
			break;
		case T_NamedArgExpr:
			{
				NamedArgExpr *nae = (NamedArgExpr *) node;

				ENCODE_ITEM(nae->argnumber);
				EncodeExpr(jstate, (Node *) nae->arg);
			}
			break;
		case T_OpExpr:
		case T_DistinctExpr:	/* struct-equivalent to OpExpr */
		case T_NullIfExpr:		/* struct-equivalent to OpExpr */
			{
				OpExpr	   *expr = (OpExpr *) node;

				ENCODE_ITEM(expr->opno);
				EncodeExpr(jstate, (Node *) expr->args);
			}
			break;
		case T_ScalarArrayOpExpr:
			{
				ScalarArrayOpExpr *expr = (ScalarArrayOpExpr *) node;

				ENCODE_ITEM(expr->opno);
				ENCODE_ITEM(expr->useOr);
				EncodeExpr(jstate, (Node *) expr->args);
			}
			break;
		case T_BoolExpr:
			{
				BoolExpr   *expr = (BoolExpr *) node;

				ENCODE_ITEM(expr->boolop);
				EncodeExpr(jstate, (Node *) expr->args);
			}
			break;
		case T_SubLink:
			{
				SubLink    *sublink = (SubLink *) node;

				ENCODE_ITEM(sublink->subLinkType);
				ENCODE_ITEM(sublink->subLinkId);
				EncodeExpr(jstate, (Node *) sublink->testexpr);
				EncodeQuery(jstate, castNode(Query, sublink->subselect));
			}
			break;
		case T_FieldSelect:
			{
				FieldSelect *fs = (FieldSelect *) node;

				ENCODE_ITEM(fs->fieldnum);
				EncodeExpr(jstate, (Node *) fs->arg);
			}
			break;
		case T_FieldStore:
			{
				FieldStore *fstore = (FieldStore *) node;

				EncodeExpr(jstate, (Node *) fstore->arg);
				EncodeExpr(jstate, (Node *) fstore->newvals);
			}
			break;
		case T_RelabelType:
			{
				RelabelType *rt = (RelabelType *) node;

				ENCODE_ITEM(rt->resulttype);
				EncodeExpr(jstate, (Node *) rt->arg);
			}
			break;
		case T_CoerceViaIO:
			{
				CoerceViaIO *cio = (CoerceViaIO *) node;

				ENCODE_ITEM(cio->resulttype);
				EncodeExpr(jstate, (Node *) cio->arg);
			}
			break;
		case T_ArrayCoerceExpr:
			{
				ArrayCoerceExpr *acexpr = (ArrayCoerceExpr *) node;

				ENCODE_ITEM(acexpr->resulttype);
				EncodeExpr(jstate, (Node *) acexpr->arg);
			}
			break;
		case T_ConvertRowtypeExpr:
			{
				ConvertRowtypeExpr *crexpr = (ConvertRowtypeExpr *) node;

				ENCODE_ITEM(crexpr->resulttype);
				EncodeExpr(jstate, (Node *) crexpr->arg);
			}
			break;
		case T_CollateExpr:
			{
				CollateExpr *ce = (CollateExpr *) node;

				ENCODE_ITEM(ce->collOid);
				EncodeExpr(jstate, (Node *) ce->arg);
			}
			break;
		case T_CaseExpr:
			{
				CaseExpr   *caseexpr = (CaseExpr *) node;

				EncodeExpr(jstate, (Node *) caseexpr->arg);
				foreach(temp, caseexpr->args)
				{
					CaseWhen   *when = lfirst_node(CaseWhen, temp);

					EncodeExpr(jstate, (Node *) when->expr);
					EncodeExpr(jstate, (Node *) when->result);
				}
				EncodeExpr(jstate, (Node *) caseexpr->defresult);
			}
			break;
		case T_CaseTestExpr:
			{
				CaseTestExpr *ct = (CaseTestExpr *) node;

				ENCODE_ITEM(ct->typeId);
			}
			break;
		case T_ArrayExpr:
			EncodeExpr(jstate, (Node *) ((ArrayExpr *) node)->elements);
			break;
		case T_RowExpr:
			EncodeExpr(jstate, (Node *) ((RowExpr *) node)->args);
			break;
		case T_RowCompareExpr:
			{
				RowCompareExpr *rcexpr = (RowCompareExpr *) node;

				ENCODE_ITEM(rcexpr->rctype);
				EncodeExpr(jstate, (Node *) rcexpr->largs);
				EncodeExpr(jstate, (Node *) rcexpr->rargs);
			}
			break;
		case T_CoalesceExpr:
			EncodeExpr(jstate, (Node *) ((CoalesceExpr *) node)->args);
			break;
		case T_MinMaxExpr:
			{
				MinMaxExpr *mmexpr = (MinMaxExpr *) node;

				ENCODE_ITEM(mmexpr->op);
				EncodeExpr(jstate, (Node *) mmexpr->args);
			}
			break;
		case T_SQLValueFunction:
			{
				SQLValueFunction *svf = (SQLValueFunction *) node;

				ENCODE_ITEM(svf->op);
				/* type is fully determined by op */
				ENCODE_ITEM(svf->typmod);
			}
			break;
		case T_XmlExpr:
			{
				XmlExpr    *xexpr = (XmlExpr *) node;

				ENCODE_ITEM(xexpr->op);
				EncodeExpr(jstate, (Node *) xexpr->named_args);
				EncodeExpr(jstate, (Node *) xexpr->args);
			}
			break;
		case T_NullTest:
			{
				NullTest   *nt = (NullTest *) node;

				ENCODE_ITEM(nt->nulltesttype);
				EncodeExpr(jstate, (Node *) nt->arg);
			}
			break;
		case T_BooleanTest:
			{
				BooleanTest *bt = (BooleanTest *) node;

				ENCODE_ITEM(bt->booltesttype);
				EncodeExpr(jstate, (Node *) bt->arg);
			}
			break;
		case T_CoerceToDomain:
			{
				CoerceToDomain *cd = (CoerceToDomain *) node;

				ENCODE_ITEM(cd->resulttype);
				EncodeExpr(jstate, (Node *) cd->arg);
			}
			break;
		case T_CoerceToDomainValue:
			{
				CoerceToDomainValue *cdv = (CoerceToDomainValue *) node;

				ENCODE_ITEM(cdv->typeId);
			}
			break;
		case T_SetToDefault:
			{
				SetToDefault *sd = (SetToDefault *) node;

				ENCODE_ITEM(sd->typeId);
			}
			break;
		case T_CurrentOfExpr:
			{
				CurrentOfExpr *ce = (CurrentOfExpr *) node;

				ENCODE_ITEM(ce->cvarno);
				if (ce->cursor_name)
					ENCODE_STRING(ce->cursor_name);
				ENCODE_ITEM(ce->cursor_param);
			}
			break;
		case T_NextValueExpr:
			{
				NextValueExpr *nve = (NextValueExpr *) node;

				ENCODE_ITEM(nve->seqid);
				ENCODE_ITEM(nve->typeId);
				ENCODE_ITEM(nve->typmod);
			}
			break;
		case T_InferenceElem:
			{
				InferenceElem *ie = (InferenceElem *) node;

				ENCODE_ITEM(ie->infercollid);
				ENCODE_ITEM(ie->inferopclass);
				EncodeExpr(jstate, ie->expr);
			}
			break;
		case T_TargetEntry:
			{
				TargetEntry *tle = (TargetEntry *) node;

				ENCODE_ITEM(tle->resno);
				ENCODE_ITEM(tle->ressortgroupref);
				EncodeExpr(jstate, (Node *) tle->expr);
			}
			break;
		case T_RangeTblRef:
			{
				RangeTblRef *rtr = (RangeTblRef *) node;

				ENCODE_ITEM(rtr->rtindex);
			}
			break;
		case T_JoinExpr:
			{
				JoinExpr   *join = (JoinExpr *) node;

				ENCODE_ITEM(join->jointype);
				ENCODE_ITEM(join->isNatural);
				ENCODE_ITEM(join->rtindex);
				EncodeExpr(jstate, join->larg);
				EncodeExpr(jstate, join->rarg);
				EncodeExpr(jstate, join->quals);
			}
			break;
		case T_FromExpr:
			{
				FromExpr   *from = (FromExpr *) node;

				EncodeExpr(jstate, (Node *) from->fromlist);
				EncodeExpr(jstate, from->quals);
			}
			break;
		case T_OnConflictExpr:
			{
				OnConflictExpr *conf = (OnConflictExpr *) node;

				ENCODE_ITEM(conf->action);
				EncodeExpr(jstate, (Node *) conf->arbiterElems);
				EncodeExpr(jstate, conf->arbiterWhere);
				EncodeExpr(jstate, (Node *) conf->onConflictSet);
				EncodeExpr(jstate, conf->onConflictWhere);
				ENCODE_ITEM(conf->constraint);
				ENCODE_ITEM(conf->exclRelIndex);
				EncodeExpr(jstate, (Node *) conf->exclRelTlist);
			}
			break;
		case T_ConnectByExpr:
			{
				ConnectByExpr *cb = (ConnectByExpr *) node;

				ENCODE_ITEM(cb->nocycle);
				EncodeExpr(jstate, cb->expr);
				EncodeExpr(jstate, cb->start);
				EncodeExpr(jstate, (Node *) cb->sort);
			}
			break;
		case T_LevelExpr:
		case T_CBIsLeafExpr:
		case T_CBIsCycleExpr:
			break;
		case T_PriorExpr:
			EncodeExpr(jstate, ((PriorExpr *)node)->expr);
			ENCODE_ITEM(((PriorExpr *)node)->for_path);
		case T_CBRootExpr:
			EncodeExpr(jstate, ((CBRootExpr *)node)->expr);
			break;
		case T_CBPathExpr:
			EncodeExpr(jstate, ((CBPathExpr *)node)->expr);
			break;
		case T_List:
			foreach(temp, (List *) node)
			{
				EncodeExpr(jstate, (Node *) lfirst(temp));
			}
			break;
		case T_IntList:
			foreach(temp, (List *) node)
			{
				ENCODE_ITEM(lfirst_int(temp));
			}
			break;
		case T_SortGroupClause:
			{
				SortGroupClause *sgc = (SortGroupClause *) node;

				ENCODE_ITEM(sgc->tleSortGroupRef);
				ENCODE_ITEM(sgc->eqop);
				ENCODE_ITEM(sgc->sortop);
				ENCODE_ITEM(sgc->nulls_first);
			}
			break;
		case T_GroupingSet:
			{
				GroupingSet *gsnode = (GroupingSet *) node;

				EncodeExpr(jstate, (Node *) gsnode->content);
			}
			break;
		case T_WindowClause:
			{
				WindowClause *wc = (WindowClause *) node;

				ENCODE_ITEM(wc->winref);
				ENCODE_ITEM(wc->frameOptions);
				EncodeExpr(jstate, (Node *) wc->partitionClause);
				EncodeExpr(jstate, (Node *) wc->orderClause);
				EncodeExpr(jstate, wc->startOffset);
				EncodeExpr(jstate, wc->endOffset);
			}
			break;
		case T_CommonTableExpr:
			{
				CommonTableExpr *cte = (CommonTableExpr *) node;

				/* we store the string name because RTE_CTE RTEs need it */
				ENCODE_STRING(cte->ctename);
				EncodeQuery(jstate, castNode(Query, cte->ctequery));
			}
			break;
		case T_SetOperationStmt:
			{
				SetOperationStmt *setop = (SetOperationStmt *) node;

				ENCODE_ITEM(setop->op);
				ENCODE_ITEM(setop->all);
				EncodeExpr(jstate, setop->larg);
				EncodeExpr(jstate, setop->rarg);
			}
			break;
		case T_RangeTblFunction:
			{
				RangeTblFunction *rtfunc = (RangeTblFunction *) node;

				EncodeExpr(jstate, rtfunc->funcexpr);
			}
			break;
		case T_TableFunc:
			{
				TableFunc  *tablefunc = (TableFunc *) node;

				EncodeExpr(jstate, tablefunc->docexpr);
				EncodeExpr(jstate, tablefunc->rowexpr);
				EncodeExpr(jstate, (Node *) tablefunc->colexprs);
			}
			break;
		case T_TableSampleClause:
			{
				TableSampleClause *tsc = (TableSampleClause *) node;

				ENCODE_ITEM(tsc->tsmhandler);
				EncodeExpr(jstate, (Node *) tsc->args);
				EncodeExpr(jstate, (Node *) tsc->repeatable);
			}
			break;
		default:
			/* Only a warning, since we can stumble along anyway */
			elog(WARNING, "unrecognized node type: %d",
				 (int) nodeTag(node));
			break;
	}
}

static void
EncodeRangeTable(QueryEncoder *jstate, List *rtable)
{
	ListCell   *lc;

	foreach(lc, rtable)
	{
		RangeTblEntry *rte = lfirst_node(RangeTblEntry, lc);

		ENCODE_ITEM(rte->rtekind);
		switch (rte->rtekind)
		{
			case RTE_RELATION:
				ENCODE_ITEM(rte->relid);
				EncodeExpr(jstate, (Node *) rte->tablesample);
				break;
			case RTE_SUBQUERY:
				EncodeQuery(jstate, rte->subquery);
				break;
			case RTE_JOIN:
				ENCODE_ITEM(rte->jointype);
				break;
			case RTE_FUNCTION:
				EncodeExpr(jstate, (Node *) rte->functions);
				break;
			case RTE_TABLEFUNC:
				EncodeExpr(jstate, (Node *) rte->tablefunc);
				break;
			case RTE_VALUES:
				EncodeExpr(jstate, (Node *) rte->values_lists);
				break;
			case RTE_CTE:

				/*
				 * Depending on the CTE name here isn't ideal, but it's the
				 * only info we have to identify the referenced WITH item.
				 */
				ENCODE_STRING(rte->ctename);
				ENCODE_ITEM(rte->ctelevelsup);
				break;
			case RTE_NAMEDTUPLESTORE:
				ENCODE_STRING(rte->enrname);
				break;
			default:
				elog(ERROR, "unrecognized RTE kind: %d", (int) rte->rtekind);
				break;
		}
	}
}


static void
EncodeQuery(QueryEncoder *jstate, Query *query)
{
	Assert(IsA(query, Query));
	Assert(query->utilityStmt == NULL);

	ENCODE_ITEM(query->commandType);
	/* resultRelation is usually predictable from commandType */
	EncodeExpr(jstate, (Node *) query->cteList);
	EncodeRangeTable(jstate, query->rtable);
	EncodeExpr(jstate, (Node *) query->jointree);
	EncodeExpr(jstate, (Node *) query->targetList);
	EncodeExpr(jstate, (Node *) query->onConflict);
	EncodeExpr(jstate, (Node *) query->returningList);
	EncodeExpr(jstate, (Node *) query->groupClause);
	EncodeExpr(jstate, (Node *) query->groupingSets);
	EncodeExpr(jstate, query->havingQual);
	EncodeExpr(jstate, (Node *) query->windowClause);
	EncodeExpr(jstate, (Node *) query->distinctClause);
	EncodeExpr(jstate, (Node *) query->sortClause);
	EncodeExpr(jstate, query->limitOffset);
	EncodeExpr(jstate, query->limitCount);
	/* we ignore rowMarks */
	EncodeExpr(jstate, query->setOperations);
}

static inline int16
RemoveFromPartition(ResultCachePartition partition, ResultCacheEntry entry)
{
	int16		index;

	if (entry->prev != -1)
	{
		index = partition->partition_queue[entry->prev]->next;
		partition->partition_queue[entry->prev]->next = entry->next;
	}
	else
	{
		index = partition->partition_head;
		partition->partition_head = entry->next;
	}

	if (entry->next != -1)
	{
		partition->partition_queue[entry->next]->prev = entry->prev;
	}
	else
	{
		partition->partition_tail = entry->prev;
	}
	partition->partition_size--;
	partition->partition_queue[index] = NULL;
	return index;
}

static inline void
InsertIntoPartition(ResultCachePartition partition, ResultCacheEntry entry, int16 index)
{
	if (partition->partition_tail == -1)
	{
		partition->partition_tail = index;
		partition->partition_head = index;
		entry->prev = -1;
	}
	else
	{
		partition->partition_queue[partition->partition_tail]->next = index;
		entry->prev = partition->partition_tail;
		partition->partition_tail = index;
	}
	entry->next = -1;
	partition->partition_size++;
	partition->partition_queue[index] = entry;
}

/* Get max commit global timestamp of each relation from datanodes */
static TimestampTz
GetMaxCommitTs(Oid *relids)
{
	Oid		   *coOids,
			   *dnOids;
	int			numdnodes,
				numcoords;
	char	   *relname = NULL;
	StringInfoData buf;
	Relation	rel;
	int			i;
	TimestampTz result;

	if (relids == NULL)
	{
		return PG_INT64_MAX;
	}
	initStringInfo(&buf);
	appendStringInfo(&buf, "SELECT pg_catalog.pg_relations_max_gts('{");
	for (i = 0; i < MAX_RELATION_PER_QUERY && relids[i] != InvalidOid; i++)
	{
		Oid			relid = relids[i];

		rel = relation_open(relid, NoLock);
		relname = quote_qualified_identifier(get_namespace_name(rel->rd_rel->relnamespace),
											 RelationGetRelationName(rel));
		if (i == 0)
		{
			appendStringInfo(&buf, "%s", relname);
		}
		else
		{
			appendStringInfo(&buf, ", %s", relname);
		}
		relation_close(rel, NoLock);
	}

	appendStringInfo(&buf, "}');");
	PgxcNodeGetOids(&coOids, &dnOids, &numcoords, &numdnodes, false);

	result = DatumGetTimestampTz(pgxc_execute_on_nodes_for_max(numdnodes, dnOids, buf.data));

	/*
	 * if result == PG_INT64_MIN, means no nodes return valid value, set
	 * result to error code:  PG_INT64_MAX
	 */
	if (result == PG_INT64_MIN)
	{
		return DatumGetTimestampTz(PG_INT64_MAX);
	}
	return result;
}

/*
 * Shared memory sizing for ResultCache
 */
Size
ResultCacheShmemSize(void)
{
    return (sizeof(ResultCachePartition) + sizeof(ResultCachePartitionData) + sizeof(ResultCacheEntry) 
            * MAX_RESULT_CACHE_PER_PARTITION) * CACHE_PARTITION_NUMBER 
            + MAXALIGN(CACHE_PARTITION_NUMBER * sizeof(LWLockPadded))
            + hash_estimate_size(INIT_RESULT_CACHE_SIZE, sizeof(ResultCacheEntryData) + MAX_RESULT_CACHE_LENGTH);
}

/*
 * Init Result Cache Share Memory
 */
void
InitResultCache(void)
{
	HASHCTL		info;
	bool		found;

	if (!IS_PGXC_COORDINATOR || MAX_RESULT_CACHE_LENGTH <= 0)
		return;

	info.keysize = sizeof(uint32);
	info.entrysize = sizeof(ResultCacheEntryData) + MAX_RESULT_CACHE_LENGTH;
	info.num_partitions = CACHE_PARTITION_NUMBER;

	/* Init hash table for result cache */
	ResultCacheHash = ShmemInitHash("Result Cache Hash Table",
									INIT_RESULT_CACHE_SIZE, INIT_RESULT_CACHE_SIZE,
									&info,
									HASH_ELEM | HASH_PARTITION | HASH_BLOBS);

	/* Init partition control structure for result cache */
	ResultCachePartitions = (ResultCachePartition *) ShmemInitStruct("Result Cache Partition Queue",
																	 (sizeof(ResultCachePartition) + sizeof(ResultCachePartitionData)
																	  + sizeof(ResultCacheEntry) * MAX_RESULT_CACHE_PER_PARTITION)
																	 * CACHE_PARTITION_NUMBER,
																	 &found);

	if (!found)
	{
		int			i,
					j;

		for (i = 0; i < CACHE_PARTITION_NUMBER; i++)
		{
			/* Init every partition control data */
			ResultCachePartitions[i] = (ResultCachePartition) ((char *) (&ResultCachePartitions[CACHE_PARTITION_NUMBER])
															   + i * (sizeof(ResultCachePartitionData)
																	  + sizeof(ResultCacheEntry) * MAX_RESULT_CACHE_PER_PARTITION));
			ResultCachePartitions[i]->partition_queue = (ResultCacheEntry *)
				(((char *) &ResultCachePartitions[i]->partition_queue)
				 + sizeof(ResultCacheEntry *));
			LWLockInitialize(&ResultCachePartitions[i]->partition_lock, LWTRANCHE_RESULT_CACHE);
			ResultCachePartitions[i]->partition_head = -1;
			ResultCachePartitions[i]->partition_tail = -1;
			ResultCachePartitions[i]->partition_size = 0;
			for (j = 0; j < MAX_RESULT_CACHE_PER_PARTITION; j++)
			{
				ResultCachePartitions[i]->partition_queue[j] = NULL;
			}
		}
	}
}

/*
 * Remove result cache related to a relation, used for TRUNCATE table
 */
void
RemoveResultCacheForRelation(Relation rel)
{
	int			i,
				j,
				k;
	bool		found;

	if (!IS_PGXC_COORDINATOR ||
		MAX_RESULT_CACHE_LENGTH <= 0 ||
		RelIsRowStore(RelationGetRelStore(rel)))
		return;

	for (i = 0; i < CACHE_PARTITION_NUMBER; i++)
	{
		ResultCachePartition partition = result_cache_partition_ctl(i);

		LWLockAcquire(&partition->partition_lock, LW_EXCLUSIVE);
		j = partition->partition_head;
		while (j != -1)
		{
			ResultCacheEntry entry = partition->partition_queue[j];
			Oid		   *relids = entry->relids;

			j = entry->next;
			for (k = 0; k < MAX_RELATION_PER_QUERY && relids[k] != InvalidOid; k++)
			{
				if (relids[k] == rel->rd_id)
				{
					RemoveFromPartition(partition, entry);
					hash_search(ResultCacheHash, (void *) &entry->queryid, HASH_REMOVE, &found);
					break;
				}
			}
		}
		LWLockRelease(&partition->partition_lock);
	}
}


/*
 * Encoded a query tree and get a hashcode
 */
uint32
QueryTreeGetEncodedQueryId(Query *query)
{
	QueryEncoder encoder;
	uint32		hashcode;

	encoder.encoded_result = (unsigned char *) palloc0(MAX_QUERY_LENGTH);
	encoder.encoded_len = 0;
	encoder.highest_extern_param_id = 0;

	EncodeQuery(&encoder, query);
	hashcode = hash_any(encoder.encoded_result, encoder.encoded_len);
	pfree(encoder.encoded_result);

	return hashcode;
}

/*
 * Check if a list of querytree satisfy result cache
 */
bool
CheckQuerySatisfyCache(List *querytree)
{
	Query	   *query;
	ListCell   *lc;
	int			count = 0;

	/*
	 * We only allow result cache on coordinator with only one query
	 */
	if (!enable_result_cache ||
		!IS_PGXC_COORDINATOR ||
		MAX_RESULT_CACHE_LENGTH <= 0 ||
		list_length(querytree) != 1)
		return false;

	query = linitial_node(Query, querytree);

	/*
	 * 1. Query type must be select 2. Query cannot contain mutable function,
	 * such as now(), random() 3. Current user should have permission on
	 * related relation 4. Query contains more than one and less than
	 * MAX_RELATION_PER_QUERY colstore relations
	 */
	if (query->commandType != CMD_SELECT ||
		contain_mutable_functions((Node *) query) ||
		!ExecCheckRTPerms(query->rtable, true) ||
		list_length(query->rtable) == 0 ||
		list_length(query->rtable) > MAX_RELATION_PER_QUERY)
		return false;

	foreach(lc, query->rtable)
	{
		Relation	relation = NULL;
		RangeTblEntry *rte = (RangeTblEntry *) lfirst(lc);

		if (rte->rtekind == RTE_RELATION)
		{
			relation = relation_open(rte->relid, NoLock);

			if (relation == NULL)
				return false;
			if (RelIsRowStore(RelationGetRelStore(relation)))
			{
				relation_close(relation, NoLock);
				return false;
			}
			count++;
			relation_close(relation, NoLock);
		}
	}

	if (count == 0)
		return false;

	return true;
}

/*
 * Get all colstore relation ids from a query tree
 */
Oid *
QueryTreeGetRelids(Query *query)
{
	ListCell   *lc;
	Oid		   *relids;
	int			relnum = 0;

	Assert(query->commandType == CMD_SELECT);

	relids = palloc0(MAX_RELATION_PER_QUERY * sizeof(Oid));

	foreach(lc, query->rtable)
	{
		RangeTblEntry *rte = (RangeTblEntry *) lfirst(lc);
		Relation	relation = NULL;

		if (rte->rtekind != RTE_RELATION)
		{
			continue;
		}
		relation = relation_open(rte->relid, NoLock);
		if (relation == NULL)
		{
			pfree(relids);
			return NULL;
		}
		if (RelIsRowStore(RelationGetRelStore(relation)))
		{
			relation_close(relation, NoLock);
			pfree(relids);
			return NULL;
		}
		relids[relnum++] = relation->rd_id;
		relation_close(relation, NoLock);
	}

	if (relnum == 0)
		return NULL;

	return relids;
}

/*
 * Search for same query result from result cache hash table
 */
char *
SearchQueryResultCache(uint32 queryid)
{
	char	   *result = NULL;
	bool		found;
	int16		index;
	ResultCacheEntry entry;
	uint32		hashcode = result_cache_hash_code(&queryid);
	ResultCachePartition partition = result_cache_partition_ctl(result_cache_partition_id(hashcode));

	LWLockAcquire(&partition->partition_lock, LW_EXCLUSIVE);
	entry = (ResultCacheEntry) hash_search_with_hash_value(ResultCacheHash,
														   (void *) &queryid,
														   hashcode, HASH_FIND, &found);

	/*
	 * If found, check related relations do not modify. Before return, move
	 * entry to lru list tail
	 */
	if (found && entry->gts > GetMaxCommitTs(entry->relids))
	{
		result = palloc0(MAX_RESULT_CACHE_LENGTH);
		memcpy(result, entry->result, MAX_RESULT_CACHE_LENGTH);

		if (partition->partition_size > 1)
		{
			index = RemoveFromPartition(partition, entry);
			InsertIntoPartition(partition, entry, index);
		}
	}

	LWLockRelease(&partition->partition_lock);
	return result;
}

/*
 * Insert a new result to result cache hash table
 */
void
InsertQueryResultCache(uint32 queryid, char *result, int size, Oid *relids)
{
	bool		found;
	int			i;
	ResultCacheEntry entry;
	uint32		hashcode = result_cache_hash_code(&queryid);
	ResultCachePartition partition = result_cache_partition_ctl(result_cache_partition_id(hashcode));
	TimestampTz time;

	LWLockAcquire(&partition->partition_lock, LW_EXCLUSIVE);
	entry = (ResultCacheEntry) hash_search_with_hash_value(ResultCacheHash,
														   (void *) &queryid,
														   hashcode, HASH_FIND, &found);
	if (found)
	{
		/*
		 * If old result is out of date, remove it from result cache hash
		 * table
		 */
		time = GetMaxCommitTs(entry->relids);
		if (entry->gts <= time)
		{
			RemoveFromPartition(partition, entry);
			hash_search_with_hash_value(ResultCacheHash,
										(void *) &queryid,
										hashcode, HASH_REMOVE, &found);
		}
		else
		{
			LWLockRelease(&partition->partition_lock);
			return;
		}
		if (time == PG_INT64_MAX)
		{
			LWLockRelease(&partition->partition_lock);
			return;
		}
	}

	/* If current partition is full, victim partition_head entry */
	if (partition->partition_size == MAX_RESULT_CACHE_PER_PARTITION)
	{
		entry = partition->partition_queue[partition->partition_head];
		RemoveFromPartition(partition, entry);
		hash_search(ResultCacheHash, (void *) &entry->queryid, HASH_REMOVE, &found);
	}

	/* Find a empty spot for new result, add entry to partition lru tail */
	for (i = 0; i < MAX_RESULT_CACHE_PER_PARTITION; i++)
	{
		if (partition->partition_queue[i] == NULL)
		{
			Snapshot	snapshot = GetActiveSnapshot();

			entry = (ResultCacheEntry) hash_search_with_hash_value(ResultCacheHash,
																   (void *) &queryid,
																   hashcode,
																   HASH_ENTER,
																   &found);
			entry->gts = snapshot->start_ts;
			entry->result = ((char *) &(entry->result)) + sizeof(char *);
			memset(entry->result, 0, MAX_RESULT_CACHE_LENGTH);
			memset(entry->relids, 0, MAX_RELATION_PER_QUERY * sizeof(Oid));
			memcpy(entry->result, result, size);
			memcpy(entry->relids, relids, MAX_RELATION_PER_QUERY * sizeof(Oid));
			InsertIntoPartition(partition, entry, i);
			break;
		}
	}

	LWLockRelease(&partition->partition_lock);
}
