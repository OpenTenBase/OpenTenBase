/*-------------------------------------------------------------------------
 *
 * locator.c
 *		Functions that help manage table location information such as
 * partitioning and replication information.
 *
 *
 *
 * Portions Copyright (c) 2012-2014, TransLattice, Inc.
 * Portions Copyright (c) 1996-2009, PostgreSQL Global Development Group
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 *
 *
 * IDENTIFICATION
 *		$$
 *
 *-------------------------------------------------------------------------
 */

#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <time.h>

#include "postgres.h"
#include "access/skey.h"
#include "access/gtm.h"
#include "access/relscan.h"
#include "catalog/indexing.h"
#include "catalog/pg_am.h"
#include "catalog/pg_type.h"
#include "nodes/pg_list.h"
#include "nodes/nodeFuncs.h"
#include "utils/builtins.h"
#include "utils/catcache.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/relcache.h"
#include "utils/tqual.h"
#include "utils/syscache.h"
#include "nodes/nodes.h"
#include "optimizer/clauses.h"
#include "parser/parse_coerce.h"
#include "parser/parsetree.h"
#include "pgxc/nodemgr.h"
#include "pgxc/locator.h"
#include "pgxc/pgxc.h"
#include "pgxc/pgxcnode.h"
#include "catalog/index.h"
#include "catalog/pgxc_class.h"
#include "catalog/pgxc_node.h"
#include "catalog/pgxc_group.h"
#include "catalog/namespace.h"
#include "access/hash.h"
#include "executor/executor.h"

#ifdef XCP
#include "utils/date.h"
#include "utils/memutils.h"
#ifdef __OPENTENBASE__
#include "pgxc/shardmap.h"
#endif
/*
 * Locator details are private
 */
struct _Locator
{
	/*
	 * Determine target nodes for value.
	 * Resulting nodes are stored to the results array.
	 * Function returns number of node references written to the array.
	 */
	int			(*locatefunc) (Locator *self,
#ifdef __OPENTENBASE_C__
							   Datum *disValues, bool *disIsNulls, int ndiscols,
#endif
							   uint32 *hashvalue);
	LocatorListType listType;
#ifdef _MIGRATE_
	Oid 		groupid;		/* only used by LOCATOR_TYPE_SHARD */
	char        locatorType;    /* locator type */
	int  	    nodeindexMap[OPENTENBASE_MAX_DATANODE_NUMBER];   /* map for global node index to local */
#endif
#ifdef __OPENTENBASE_C__
	AttrNumber  *discolnums;
	Oid         *discoltypes;
	int         ndiscols;
#endif
	/* locator-specific data */
	/* XXX: move them into union ? */
	int			roundRobinNode; /* for LOCATOR_TYPE_RROBIN */
	LocatorHashFunc	hashfunc; /* for LOCATOR_TYPE_HASH */
	int 		valuelen; /* 1, 2 or 4 for LOCATOR_TYPE_MODULO */

	int			nodeCount; /* How many nodes are in the map */
	void	   *nodeMap; /* map index to node reference according to listType */
	void	   *results; /* array to output results */
	/* begin opentenbase_ora */
	void		   *locator_extra;
	TupleTableSlot *inputslot;
	/* end opentenbase_ora */
};
#endif

int		num_preferred_data_nodes = 0;
Oid		preferred_data_node[MAX_PREFERRED_NODES];
extern  bool enable_global_indexscan;

#ifdef XCP
static int modulo_value_len(Oid dataType);
static int locate_static(Locator *self,
#ifdef __OPENTENBASE_C__
						 Datum *disValues, bool *disIsNulls, int ndiscols,
#endif
						 uint32 *hashvalue);
static int locate_roundrobin(Locator *self,
#ifdef __OPENTENBASE_C__
							 Datum *disValues, bool *disIsNulls, int ndiscols,
#endif
							 uint32 *hashvalue);
static int locate_modulo_random(Locator *self,
#ifdef __OPENTENBASE_C__
								Datum *disValues, bool *disIsNulls, int ndiscols,
#endif
								uint32 *hashvalue);
static int locate_hash_insert(Locator *self,
#ifdef __OPENTENBASE_C__
							  Datum *disValues, bool *disIsNulls, int ndiscols,
#endif
							  uint32 *hashvalue);
static int locate_hash_select(Locator *self,
#ifdef __OPENTENBASE_C__
							  Datum *disValues, bool *disIsNulls, int ndiscols,
#endif
							  uint32 *hashvalue);
#ifdef _MIGRATE_
static int locate_shard_insert(Locator *self,
#ifdef __OPENTENBASE_C__
							   Datum *disValues, bool *disIsNulls, int ndiscols,
#endif
							   uint32 *hashvalue);
static int locate_shard_select(Locator *self,
#ifdef __OPENTENBASE_C__
							   Datum *disValues, bool *disIsNulls, int ndiscols,
#endif
							   uint32 *hashvalue);
#endif
static int locate_modulo_insert(Locator *self,
#ifdef __OPENTENBASE_C__
								Datum *disValues, bool *disIsNulls, int ndiscols,
#endif
								uint32 *hashvalue);
static int locate_modulo_select(Locator *self,
#ifdef __OPENTENBASE_C__
								Datum *disValues, bool *disIsNulls, int ndiscols,
#endif
								uint32 *hashvalue);

static Expr *get_attr_expression(Oid reloid, Index varno, Node *quals, List *rtable,
                                 AttrNumber attrNum, ParamListInfo boundParams);
#endif

static bool DatanodeInGroup(Oid* nodeoids, int nodenums, Oid nodeoid);

/* opentenbase_ora */
static int locate_expr_insert(Locator *top_loc,
							  Datum *values,
							  bool *nulls,
							  int ndiscols,
							  uint32 *hashvalue);

/*
 * GetPreferredReplicationNode
 * Pick any Datanode from given list, however fetch a preferred node first.
 */
List *
GetPreferredReplicationNode(List *relNodes)
{
	ListCell	*item;
	int			nodeid = -1;

	if (list_length(relNodes) <= 0)
		elog(ERROR, "a list of nodes should have at least one node");

	foreach(item, relNodes)
	{
		int cnt_nodes;
		char nodetype = PGXC_NODE_DATANODE;
		for (cnt_nodes = 0;
				cnt_nodes < num_preferred_data_nodes && nodeid < 0;
				cnt_nodes++)
		{
			if (PGXCNodeGetNodeId(preferred_data_node[cnt_nodes],
								  &nodetype) == lfirst_int(item))
				nodeid = lfirst_int(item);
		}
		if (nodeid >= 0)
			break;
	}
	if (nodeid < 0)
		return list_make1_int(list_nth_int(relNodes,
					((unsigned int) random()) % list_length(relNodes)));

	return list_make1_int(nodeid);
}

/*
 * GetAnyDataNode
 * Pick any data node from given set, but try a preferred node
 */
int
GetAnyDataNode(Bitmapset *nodes)
{
	Bitmapset  *preferred = NULL;
	int			i, nodeid;
	int			nmembers = 0;
	int			members[NumDataNodes];
	Bitmapset  *org_nodes = nodes;

	/* use all datanodes */
	if (nodes == NULL)
	{
		for (i = 0; i < NumDataNodes; i++)
			nodes = bms_add_member(nodes, i);
	}

	for (i = 0; i < num_preferred_data_nodes; i++)
	{
		char ntype = PGXC_NODE_DATANODE;
		nodeid = PGXCNodeGetNodeId(preferred_data_node[i], &ntype);

		/* OK, found one */
		if (bms_is_member(nodeid, nodes))
			preferred = bms_add_member(preferred, nodeid);
	}

	/*
	 * If no preferred data nodes or they are not in the desired set, pick up
	 * from the original set.
	 */
	if (bms_is_empty(preferred))
		preferred = bms_copy(nodes);

	/*
	 * Load balance.
	 * We can not get item from the set, convert it to array
	 */
	while ((nodeid = bms_first_member(preferred)) >= 0)
		members[nmembers++] = nodeid;
	bms_free(preferred);

	if (org_nodes != nodes)
		bms_free(nodes);

	/* If there is a single member nothing to balance */
	if (nmembers == 1)
		return members[0];

	/*
	 * In general, the set may contain any number of nodes, and if we save
	 * previous returned index for load balancing the distribution won't be
	 * flat, because small set will probably reset saved value, and lower
	 * indexes will be picked up more often.
	 * So we just get a random value from 0..nmembers-1.
	 */
	return members[((unsigned int) random()) % nmembers];
}

/*
 * compute_modulo
 *	Computes modulo of two 64-bit unsigned values.
 */
static int
compute_modulo(uint64 numerator, uint64 denominator)
{
	Assert(denominator > 0);

	return numerator % denominator;
}

/*
 * GetRelationDistColumn - Returns the name of the hash or modulo distribution column
 * First hash distribution is checked
 * Retuens NULL if the table is neither hash nor modulo distributed
 */
char *
GetRelationDistColumn(RelationLocInfo * rel_loc_info)
{
char *pColName;

	pColName = NULL;

	pColName = GetRelationHashColumn(rel_loc_info);
	if (pColName == NULL)
		pColName = GetRelationModuloColumn(rel_loc_info);

	return pColName;
}

#ifdef _MIGRATE_
/*
 * IsTypeDistributable
 * Returns whether the data type is distributable using a column value.
 */
bool
IsTypeDistributable(Oid col_type)
{
	if(col_type == INT8OID
	|| col_type == INT2OID
	|| col_type == OIDOID
	|| col_type == INT4OID
	|| col_type == BOOLOID
	|| col_type == CHAROID
	|| col_type == NAMEOID
	|| col_type == INT2VECTOROID
	|| col_type == TEXTOID
	|| col_type == OIDVECTOROID
	|| col_type == FLOAT4OID
	|| col_type == FLOAT8OID
	|| col_type == ABSTIMEOID
	|| col_type == RELTIMEOID
	|| col_type == CASHOID
	|| col_type == BPCHAROID
	|| col_type == BYTEAOID
	|| col_type == RAWOID 
	|| col_type == VARCHAROID
	|| col_type == DATEOID
	|| col_type == TIMEOID
	|| col_type == TIMESTAMPOID
	|| col_type == TIMESTAMPTZOID
	|| col_type == INTERVALOID
	|| col_type == TIMETZOID
	|| col_type == NUMERICOID
	|| col_type == UUIDOID
	|| col_type == VARCHAR2OID
	|| col_type == NVARCHAR2OID
	|| col_type == NUMERICDOID
	|| col_type == NUMERICFOID
	)
		return true;

	return false;
}
#endif

/*
 * Returns whether or not the data type is hash distributable with PG-XC
 * PGXCTODO - expand support for other data types!
 */
bool
IsTypeHashDistributable(Oid col_type)
{
	return (hash_func_ptr(col_type) != NULL);
}

/*
 * GetRelationHashColumn - return hash column for relation.
 *
 * Returns NULL if the relation is not hash partitioned.
 */
char *
GetRelationHashColumn(RelationLocInfo * rel_loc_info)
{
	char	   *column_str = NULL;

	if (rel_loc_info == NULL)
		column_str = NULL;
	else if (rel_loc_info->locatorType != LOCATOR_TYPE_HASH)
		column_str = NULL;
	else
	{
		int len = 0;
		char *colname = rel_loc_info->nDisAttrs > 0 ?
				get_attname(rel_loc_info->relid, rel_loc_info->disAttrNums[0]) : NULL;
		len = strlen(colname);

		column_str = (char *) palloc(len + 1);
		strncpy(column_str, colname, len + 1);
	}

	return column_str;
}

#if 0
/*
 * IsDistColumnForRelId - return whether or not column for relation is used for hash or modulo distribution
 *
 */
bool
IsDistColumnForRelId(Oid relid, char *part_col_name)
{
	RelationLocInfo *rel_loc_info;

	/* if no column is specified, we're done */
	if (!part_col_name)
		return false;

	/* if no locator, we're done too */
	if (!(rel_loc_info = GetRelationLocInfo(relid)))
		return false;

	/* is the table distributed by column value */
	if (!IsRelationDistributedByValue(rel_loc_info))
		return false;

	/* does the column name match the distribution column */
	return !strcmp(part_col_name, rel_loc_info->partAttrName);
}
#endif


/*
 * Returns whether or not the data type is modulo distributable with PG-XC
 * PGXCTODO - expand support for other data types!
 */
bool
IsTypeModuloDistributable(Oid col_type)
{
	return (modulo_value_len(col_type) != -1);
}

/*
 * GetRelationModuloColumn - return modulo column for relation.
 *
 * Returns NULL if the relation is not modulo partitioned.
 */
char *
GetRelationModuloColumn(RelationLocInfo * rel_loc_info)
{
	char	   *column_str = NULL;

	if (rel_loc_info == NULL)
		column_str = NULL;
	else if (rel_loc_info->locatorType != LOCATOR_TYPE_MODULO)
		column_str = NULL;
	else
	{
		int len = 0;
		char *colname = rel_loc_info->nDisAttrs > 0 ?
		                get_attname(rel_loc_info->relid, rel_loc_info->disAttrNums[0]) : NULL;
		
		len = strlen(colname);
		
		column_str = (char *) palloc(len + 1);
		strncpy(column_str, colname, len + 1);
	}

	return column_str;
}

/*
 * Update the round robin node for the relation
 *
 * PGXCTODO - may not want to bother with locking here, we could track
 * these in the session memory context instead...
 */
int
GetRoundRobinNode(Oid relid)
{
	int			ret_node;
	Relation	rel = relation_open(relid, AccessShareLock);

    Assert (IsLocatorReplicated(rel->rd_locator_info->locatorType) ||
			rel->rd_locator_info->locatorType == LOCATOR_TYPE_RROBIN);

	ret_node = lfirst_int(rel->rd_locator_info->roundRobinNode);

	/* Move round robin indicator to next node */
	if (rel->rd_locator_info->roundRobinNode->next != NULL)
		rel->rd_locator_info->roundRobinNode = rel->rd_locator_info->roundRobinNode->next;
	else
		/* reset to first one */
		rel->rd_locator_info->roundRobinNode = rel->rd_locator_info->rl_nodeList->head;

	relation_close(rel, AccessShareLock);

	return ret_node;
}

/*
 * IsLocatorInfoEqual
 * Check equality of given locator information
 */
bool
IsLocatorInfoEqual(RelationLocInfo *rel_loc_info1, RelationLocInfo *rel_loc_info2)
{
	List *nodeList1, *nodeList2;
	int i = 0;
	Assert(rel_loc_info1 && rel_loc_info2);

	nodeList1 = rel_loc_info1->rl_nodeList;
	nodeList2 = rel_loc_info2->rl_nodeList;

	/* Same relation? */
	if (rel_loc_info1->relid != rel_loc_info2->relid)
		return false;

	/* Same locator type? */
	if (rel_loc_info1->locatorType != rel_loc_info2->locatorType)
		return false;

	/* Same attribute number? */
	if (rel_loc_info1->nDisAttrs != rel_loc_info2->nDisAttrs)
		return false;
	
	for (i = 0; i < rel_loc_info1->nDisAttrs; ++i)
	{
		if (rel_loc_info1->disAttrNums[i] != rel_loc_info2->disAttrNums[i])
			return false;
	}

	/* Same node list? */
	if (list_difference_int(nodeList1, nodeList2) != NIL ||
		list_difference_int(nodeList2, nodeList1) != NIL)
		return false;

	/* Everything is equal */
	return true;
}

/*
 * ConvertToLocatorType
 *		get locator distribution type
 * We really should just have pgxc_class use disttype instead...
 */
char
ConvertToLocatorType(int disttype)
{
	char		loctype = LOCATOR_TYPE_NONE;

	switch (disttype)
	{
		case DISTTYPE_HASH:
			loctype = LOCATOR_TYPE_HASH;
			break;
		case DISTTYPE_ROUNDROBIN:
			loctype = LOCATOR_TYPE_RROBIN;
			break;
		case DISTTYPE_REPLICATION:
			loctype = LOCATOR_TYPE_REPLICATED;
			break;
		case DISTTYPE_MODULO:
			loctype = LOCATOR_TYPE_MODULO;
			break;
#ifdef _SHARDING_
		case DISTTYPE_SHARD:
			loctype = LOCATOR_TYPE_SHARD;
			break;
#endif
		default:
			ereport(ERROR,
					(errcode(ERRCODE_WRONG_OBJECT_TYPE),
					 errmsg("Invalid distribution type")));
			break;
	}

	return loctype;
}

/*
 * GetLocatorType - Returns the locator type of the table
 *
 */
char
GetLocatorType(Oid relid)
{
	char		ret = '\0';

	RelationLocInfo *ret_loc_info = GetRelationLocInfo(relid);

	if (ret_loc_info != NULL)
		ret = ret_loc_info->locatorType;

	return ret;
}

/*
 * Return a list of all Datanodes.
 * We assume all tables use all nodes in the prototype, so just return a list
 * from first one.
 */
List *
GetAllDataNodes(void)
{
	int			i;
	List	   *nodeList = NIL;

	for (i = 0; i < NumDataNodes; i++)
		nodeList = lappend_int(nodeList, i);

	return nodeList;
}

/*
 * Return a list of all Coordinators
 * This is used to send DDL to all nodes and to clean up pooler connections.
 * Do not put in the list the local Coordinator where this function is launched.
 */
List *
GetAllCoordNodes(bool include_myself)
{
	int			i;
	List	   *nodeList = NIL;

	for (i = 0; i < NumCoords; i++)
	{
		/*
		 * Do not put in list the Coordinator we are on,
		 * it doesn't make sense to connect to the local Coordinator.
		 */

		if (i != PGXCNodeId - 1)
			nodeList = lappend_int(nodeList, i);
		else if (include_myself)
            nodeList = lappend_int(nodeList, i);
	}

	return nodeList;
}

static bool
DatanodeInGroup(Oid* nodeoids, int nodenums, Oid nodeoid)
{
	int j = 0;
	bool found = false;

	for (j = 0; j < nodenums; j++)
	{
		if (nodeoids[j] == nodeoid)
		{
			found = true;
			break;
		}
	}

	return found;
}

/*
 * Build locator information associated with the specified relation.
 */
void
RelationBuildLocator(Relation rel)
{
	Relation	pcrel;
	ScanKeyData	skey;
	SysScanDesc	pcscan;
	HeapTuple	htup = NULL;
	HeapTuple   gtup = NULL;
	MemoryContext	oldContext;
	RelationLocInfo	*relationLocInfo;
	int		j;
	Form_pgxc_class	pgxc_class;
	Form_pgxc_group	pgxc_group;
	bool			node_in_group =  false;
	Oid				curr_nodeoid;
	int				numnodes;
	Oid			*nodeoids;
#ifdef __OPENTENBASE_C__
	bool        isNull;
	Datum       discolsDatum;
	int2vector  *discolnums;
#endif

	ScanKeyInit(&skey,
				Anum_pgxc_class_pcrelid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(RelationGetRelid(rel)));

	pcrel = heap_open(PgxcClassRelationId, AccessShareLock);
	pcscan = systable_beginscan(pcrel, PgxcClassPgxcRelIdIndexId, true,
								SnapshotSelf, 1, &skey);
	htup = systable_getnext(pcscan);

	if (!HeapTupleIsValid(htup))
	{
		/* Assume local relation only */
		rel->rd_locator_info = NULL;
		systable_endscan(pcscan);
		heap_close(pcrel, AccessShareLock);
		return;
	}

	pgxc_class = (Form_pgxc_class) GETSTRUCT(htup);
	if (OidIsValid(pgxc_class->pgroup))
	{
		gtup = SearchSysCache1(PGXCGROUPOID, ObjectIdGetDatum(pgxc_class->pgroup));
		if (!HeapTupleIsValid(gtup))
			elog(ERROR, "cache lookup failed for group %u", pgxc_class->pgroup);
		pgxc_group = (Form_pgxc_group) GETSTRUCT(gtup);
		numnodes = pgxc_group->group_members.dim1;
		nodeoids = pgxc_group->group_members.values;
	}
	else
	{
		PgxcNodeGetOids(NULL, &nodeoids, NULL, &numnodes, false);
	}

	oldContext = MemoryContextSwitchTo(CacheMemoryContext);

	relationLocInfo = (RelationLocInfo *) palloc(sizeof(RelationLocInfo));
	rel->rd_locator_info = relationLocInfo;

	relationLocInfo->relid = RelationGetRelid(rel);
	relationLocInfo->locatorType = pgxc_class->pclocatortype;
#ifdef _MIGRATE_
	relationLocInfo->groupId = pgxc_class->pgroup;
#endif
	relationLocInfo->rl_nodeList = NIL;
#ifdef __OPENTENBASE_C__
	relationLocInfo->disAttrNums = NULL;
	relationLocInfo->disAttrTypes = NULL;
	relationLocInfo->disAttrTypMods = NULL;
	discolsDatum = heap_getattr(htup, Anum_pgxc_class_discolnums, RelationGetDescr(pcrel), &isNull);
	if (isNull)
	{
		relationLocInfo->nDisAttrs = 0;
	}
	else
	{
		discolnums = (int2vector *) DatumGetPointer(discolsDatum);
		relationLocInfo->nDisAttrs = discolnums->dim1;
	}
	
	if (relationLocInfo->nDisAttrs > 0)
	{
		relationLocInfo->disAttrNums = (AttrNumber *)palloc(sizeof(AttrNumber) * relationLocInfo->nDisAttrs);
		memcpy(relationLocInfo->disAttrNums, discolnums->values, sizeof(AttrNumber) * relationLocInfo->nDisAttrs);
		relationLocInfo->disAttrTypes = (Oid *)palloc(sizeof(Oid) * relationLocInfo->nDisAttrs);
		relationLocInfo->disAttrTypMods = (int32 *)palloc(sizeof(int32) * relationLocInfo->nDisAttrs);
		for (j = 0; j < relationLocInfo->nDisAttrs; j++)
		{
			AttrNumber column = relationLocInfo->disAttrNums[j];
			relationLocInfo->disAttrTypes[j] = TupleDescAttr(rel->rd_att, column - 1)->atttypid;
			relationLocInfo->disAttrTypMods[j] = TupleDescAttr(rel->rd_att, column - 1)->atttypmod;
		}
	}
#endif

#ifdef _MIGRATE_
	SyncShardMapList(false);

	curr_nodeoid = get_pgxc_nodeoid_extend(PGXCNodeName, pgxc_plane_name(PGXCMainPlaneNameID));
	if (InvalidOid == curr_nodeoid)
	{
		elog(ERROR, "no such node:%s on PGXCMainPlaneName %s PGXCPlaneName %s",
				PGXCNodeName,
				pgxc_plane_name(PGXCMainPlaneNameID),
				pgxc_plane_name(PGXCPlaneNameID));
	}
	node_in_group = DatanodeInGroup(nodeoids, numnodes, curr_nodeoid);

	if (IS_PGXC_COORDINATOR || (IS_PGXC_DATANODE && node_in_group))
	{
		if(relationLocInfo->locatorType == LOCATOR_TYPE_SHARD)
		{
			int32	 dn_num;
			int32  *datanodes;

			/* major map */
			GetShardNodes(pgxc_class->pgroup, &datanodes, &dn_num, NULL);
			for(j = 0; j < dn_num; j++)
			{							
				relationLocInfo->rl_nodeList = list_append_unique_int(relationLocInfo->rl_nodeList, datanodes[j]);
			}
			pfree(datanodes);
		}
		else
		{
#endif
			for (j = 0; j < numnodes; j++)
			{
				char ntype = PGXC_NODE_DATANODE;
				int nid = PGXCNodeGetNodeId(nodeoids[j], &ntype);
				relationLocInfo->rl_nodeList = lappend_int(relationLocInfo->rl_nodeList, nid);
			}
#ifdef _MIGRATE_
		}
#endif
		/*
		 * If the locator type is round robin, we set a node to
		 * use next time. In addition, if it is replicated,
		 * we choose a node to use for balancing reads.
		 */
		if (relationLocInfo->locatorType == LOCATOR_TYPE_RROBIN ||
			IsLocatorReplicated(relationLocInfo->locatorType))
		{
			int offset;
			/*
			 * pick a random one to start with,
			 * since each process will do this independently
			 */
			offset = compute_modulo(abs(rand()), list_length(relationLocInfo->rl_nodeList));

			relationLocInfo->roundRobinNode = relationLocInfo->rl_nodeList->head; /* initialize */
			for (j = 0; j < offset && relationLocInfo->roundRobinNode->next != NULL; j++)
				relationLocInfo->roundRobinNode = relationLocInfo->roundRobinNode->next;
		}
#ifdef _MIGRATE_
	}
#endif

	MemoryContextSwitchTo(oldContext);

	if (OidIsValid(pgxc_class->pgroup))
	{
		ReleaseSysCache(gtup);
	}
	else
	{
		pfree(nodeoids);
	}

	systable_endscan(pcscan);
	heap_close(pcrel, AccessShareLock);

#ifdef _PG_ORCL_
	/* locator created here */
	rel->rd_att->tdincl_nodeid = !RelationIsReplication(rel);
#endif
}

/*
 * GetLocatorRelationInfo - Returns the locator information for relation,
 * in a copy of the RelationLocatorInfo struct in relcache
 */
RelationLocInfo *
GetRelationLocInfo(Oid relid)
{
	RelationLocInfo *ret_loc_info = NULL;
	Relation	rel = relation_open(relid, AccessShareLock);

	/* Relation needs to be valid */
	Assert(rel->rd_isvalid);

	if (rel->rd_locator_info)
		ret_loc_info = CopyRelationLocInfo(rel->rd_locator_info);

	relation_close(rel, AccessShareLock);

	return ret_loc_info;
}

/*
 * Get the distribution type of relation.
 */
char
GetRelationLocType(Oid relid)
{
	RelationLocInfo *locinfo = GetRelationLocInfo(relid);
	if (!locinfo)
		return LOCATOR_TYPE_NONE;

	return locinfo->locatorType;
}

/*
 * Copy the RelationLocInfo struct
 */
RelationLocInfo *
CopyRelationLocInfo(RelationLocInfo * src_info)
{
	RelationLocInfo *dest_info;

	Assert(src_info);

	dest_info = (RelationLocInfo *) palloc0(sizeof(RelationLocInfo));

	dest_info->relid = src_info->relid;
	dest_info->locatorType = src_info->locatorType;
#ifdef _MIGRATE_
	dest_info->groupId     = src_info->groupId;
#endif
#ifdef __OPENTENBASE_C__
	dest_info->nDisAttrs = src_info->nDisAttrs;
	if (dest_info->nDisAttrs > 0)
	{
		dest_info->disAttrNums = (AttrNumber *) palloc0(sizeof(AttrNumber) * dest_info->nDisAttrs);
		memcpy(dest_info->disAttrNums, src_info->disAttrNums, sizeof(AttrNumber) * dest_info->nDisAttrs);
		dest_info->disAttrTypes = (Oid *) palloc0(sizeof(Oid) * dest_info->nDisAttrs);
		memcpy(dest_info->disAttrTypes, src_info->disAttrTypes, sizeof(Oid) * dest_info->nDisAttrs);
		dest_info->disAttrTypMods = (int32 *) palloc0(sizeof(int32) * dest_info->nDisAttrs);
		memcpy(dest_info->disAttrTypMods, src_info->disAttrTypMods, sizeof(Oid) * dest_info->nDisAttrs);
	}
#endif
	if (src_info->rl_nodeList)
		dest_info->rl_nodeList = list_copy(src_info->rl_nodeList);
	/* Note, for round robin, we use the relcache entry */

	return dest_info;
}

/*
 * Free RelationLocInfo struct
 */
void
FreeRelationLocInfo(RelationLocInfo *relationLocInfo)
{
	if (relationLocInfo)
	{
		if (relationLocInfo->disAttrNums)
			pfree(relationLocInfo->disAttrNums);
		if (relationLocInfo->disAttrTypes)
			pfree(relationLocInfo->disAttrTypes);
		if (relationLocInfo->disAttrTypMods)
			pfree(relationLocInfo->disAttrTypMods);
		pfree(relationLocInfo);
	}
}

/*
 * Free the contents of the ExecNodes expression */
void
FreeExecNodes(ExecNodes **exec_nodes)
{
	ExecNodes *tmp_en = *exec_nodes;

	/* Nothing to do */
	if (!tmp_en)
		return;
	list_free(tmp_en->primarynodelist);
	list_free(tmp_en->nodeList);
	pfree_ext(tmp_en->g_index_table_name);
	pfree(tmp_en);
	*exec_nodes = NULL;
}


#ifdef XCP
/*
 * Determine value length in bytes for specified type for a module locator.
 * Return -1 if module locator is not supported for the type.
 */
static int
modulo_value_len(Oid dataType)
{
	switch (dataType)
	{
		case BOOLOID:
		case CHAROID:
			return 1;
		case INT2OID:
			return 2;
		case INT4OID:
		case ABSTIMEOID:
		case RELTIMEOID:
		case DATEOID:
			return 4;
		case INT8OID:
			return 8;
		default:
			return -1;
	}
}

LocatorHashFunc
hash_func_ptr(Oid dataType)
{
	switch (dataType)
	{
		case INT8OID:
		case CASHOID:
			return hashint8;
		case INT2OID:
			return hashint2;
		case OIDOID:
			return hashoid;
		case INT4OID:
		case ABSTIMEOID:
		case RELTIMEOID:
		case DATEOID:
		case XIDOID:
			return hashint4;
#ifdef __OPENTENBASE__
		case FLOAT4OID:
			return hashfloat4;
		case FLOAT8OID:
			return hashfloat8;
		case JSONBOID:
			return jsonb_hash;
#endif
	    case RIDOID:
	        return rowid_hash;
		case BOOLOID:
		case CHAROID:
			return hashchar;
		case NAMEOID:
			return hashname;
		case VARCHAROID:
		case TEXTOID:
		case VARCHAR2OID:
		case NVARCHAR2OID:
			return hashtext;
		case OIDVECTOROID:
			return hashoidvector;
		case BPCHAROID:
			return hashbpchar;
		case RAWOID:
		case BYTEAOID:
			return hashvarlena;
		case TIMEOID:
			return time_hash;
		case TIMESTAMPOID:
		case TIMESTAMPTZOID:
			return timestamp_hash;
		case INTERVALOID:
			return interval_hash;
		case TIMETZOID:
			return timetz_hash;
		case NUMERICOID:
			return hash_numeric;
		case UUIDOID:
			return uuid_hash;
		case VARBITOID:
		case BITOID:
			return bithash;
		default:
			return NULL;
	}
}

#ifdef _MIGRATE_
Locator *
createLocator(char locatorType, RelationAccessType accessType,
			  LocatorListType listType, int nodeCount,
			  void *nodeList, void **result, bool primary, Oid groupid,
			  Oid *discoltypes, AttrNumber *discolnums, int ndiscols,
			  void *locator_extra)
#else
Locator *
createLocator(char locatorType, RelationAccessType accessType,
			  Oid dataType, LocatorListType listType, int nodeCount,
			  void *nodeList, void **result, bool primary,
			  void *locator_extra)
#endif
{
	Locator    *locator;
	ListCell   *lc;
	void 	   *nodeMap = NULL;
	int 		i;
	Oid dataType = InvalidOid;
	
	if (ndiscols > 0)
		dataType = discoltypes[0];

	locator = (Locator *) palloc(sizeof(Locator));
	locator->listType = listType;
	locator->nodeCount = nodeCount;
#ifdef _MIGRATE_
	locator->groupid  = InvalidOid;
	locator->locatorType = locatorType;
	locator->ndiscols = ndiscols;
	locator->discolnums = discolnums;
	locator->discoltypes = discoltypes;
#endif

	/* opentenbase_ora */
	locator->locator_extra = NULL;

	/* Create node map */
	switch (listType)
	{
		case LOCATOR_LIST_NONE:
			/* No map, return indexes */
			break;
		case LOCATOR_LIST_INT:
			/* Copy integer array */
			nodeMap = palloc(nodeCount * sizeof(int));
			memcpy(nodeMap, nodeList, nodeCount * sizeof(int));
			break;
		case LOCATOR_LIST_OID:
			/* Copy array of Oids */
			nodeMap = palloc(nodeCount * sizeof(Oid));
			memcpy(nodeMap, nodeList, nodeCount * sizeof(Oid));
			break;
		case LOCATOR_LIST_POINTER:
			/* Copy array of Oids */
			nodeMap = palloc(nodeCount * sizeof(void *));
			memcpy(nodeMap, nodeList, nodeCount * sizeof(void *));
			break;
		case LOCATOR_LIST_LIST:
			/* Create map from list */
		{
			List *l = (List *) nodeList;
			locator->nodeCount = list_length(l);
			if (IsA(l, IntList))
			{
				int *intptr;
				nodeMap = palloc(locator->nodeCount * sizeof(int));
				intptr = (int *) nodeMap;
				foreach(lc, l)
					*intptr++ = lfirst_int(lc);
				locator->listType = LOCATOR_LIST_INT;
			}
			else if (IsA(l, OidList))
			{
				Oid *oidptr;
				nodeMap = palloc(locator->nodeCount * sizeof(Oid));
				oidptr = (Oid *) nodeMap;
				foreach(lc, l)
					*oidptr++ = lfirst_oid(lc);
				locator->listType = LOCATOR_LIST_OID;
			}
			else if (IsA(l, List))
			{
				void **voidptr;
				nodeMap = palloc(locator->nodeCount * sizeof(void *));
				voidptr = (void **) nodeMap;
				foreach(lc, l)
					*voidptr++ = lfirst(lc);
				locator->listType = LOCATOR_LIST_POINTER;
			}
			else
			{
				/* can not get here */
				Assert(false);
			}
			break;
		}
	}
	/*
	 * Determine locatefunc, allocate results, set up parameters
	 * specific to locator type
	 */
	switch (locatorType)
	{
#ifdef _MIGRATE_
		case LOCATOR_TYPE_SHARD:
			if (RELATION_ACCESS_INSERT == accessType)
			{
				locator->locatefunc = locate_shard_insert;
				locator->nodeMap = nodeMap;
				locator->groupid = groupid;;
				switch (locator->listType)
				{
					case LOCATOR_LIST_NONE:
					case LOCATOR_LIST_INT:
						locator->results = palloc(sizeof(int));
						break;
					case LOCATOR_LIST_OID:
						locator->results = palloc(sizeof(Oid));
						break;
					case LOCATOR_LIST_POINTER:
						GetGroupNodeIndexMap(locator->groupid, locator->nodeindexMap);
						locator->results = palloc(sizeof(void *));
						break;
					case LOCATOR_LIST_LIST:
						/* Should never happen */
						Assert(false);
						break;
				}
			}
			else
			{
				locator->locatefunc = locate_shard_select;
				locator->nodeMap = nodeMap;
				locator->groupid = groupid;
				switch (locator->listType)
				{
					case LOCATOR_LIST_NONE:
					case LOCATOR_LIST_INT:
						locator->results = palloc(locator->nodeCount * sizeof(int));
						break;
					case LOCATOR_LIST_OID:
						locator->results = palloc(locator->nodeCount * sizeof(Oid));
						break;
					case LOCATOR_LIST_POINTER:
						GetGroupNodeIndexMap(locator->groupid, locator->nodeindexMap);
						locator->results = palloc(locator->nodeCount * sizeof(void *));
						break;
					case LOCATOR_LIST_LIST:
						/* Should never happen */
						Assert(false);
						break;
				}
			}
			break;
#endif
		case LOCATOR_TYPE_REPLICATED:
		case LOCATOR_TYPE_FOREIGN:
			if (accessType == RELATION_ACCESS_INSERT ||
				accessType == RELATION_ACCESS_UPDATE ||
				accessType == RELATION_ACCESS_READ_FQS ||
				accessType == RELATION_ACCESS_READ_FOR_UPDATE)
			{
				locator->locatefunc = locate_static;
				if (nodeMap == NULL)
				{
					/* no map, prepare array with indexes */
					int *intptr;
					nodeMap = palloc(locator->nodeCount * sizeof(int));
					intptr = (int *) nodeMap;
					for (i = 0; i < locator->nodeCount; i++)
						*intptr++ = i;
				}
				locator->nodeMap = nodeMap;
				locator->results = nodeMap;
			}
			else
			{
				/* SELECT, use random node.. */
				locator->locatefunc = locate_modulo_random;
				locator->nodeMap = nodeMap;
				switch (locator->listType)
				{
					case LOCATOR_LIST_NONE:
					case LOCATOR_LIST_INT:
						locator->results = palloc(sizeof(int));
						break;
					case LOCATOR_LIST_OID:
						locator->results = palloc(sizeof(Oid));
						break;
					case LOCATOR_LIST_POINTER:
						locator->results = palloc(sizeof(void *));
						break;
					case LOCATOR_LIST_LIST:
						/* Should never happen */
						Assert(false);
						break;
				}
				locator->roundRobinNode = -1;
			}
			break;
		case LOCATOR_TYPE_RROBIN:
			if (accessType == RELATION_ACCESS_INSERT)
			{
				locator->locatefunc = locate_roundrobin;
				locator->nodeMap = nodeMap;
				switch (locator->listType)
				{
					case LOCATOR_LIST_NONE:
					case LOCATOR_LIST_INT:
						locator->results = palloc(sizeof(int));
						break;
					case LOCATOR_LIST_OID:
						locator->results = palloc(sizeof(Oid));
						break;
					case LOCATOR_LIST_POINTER:
						locator->results = palloc(sizeof(void *));
						break;
					case LOCATOR_LIST_LIST:
						/* Should never happen */
						Assert(false);
						break;
				}
				/* randomize choice of the initial node */
				locator->roundRobinNode = (abs(rand()) % locator->nodeCount) - 1;
			}
			else
			{
				locator->locatefunc = locate_static;
				if (nodeMap == NULL)
				{
					/* no map, prepare array with indexes */
					int *intptr;
					nodeMap = palloc(locator->nodeCount * sizeof(int));
					intptr = (int *) nodeMap;
					for (i = 0; i < locator->nodeCount; i++)
						*intptr++ = i;
				}
				locator->nodeMap = nodeMap;
				locator->results = nodeMap;
			}
			break;
		case LOCATOR_TYPE_HASH:
			if (accessType == RELATION_ACCESS_INSERT)
			{
				locator->locatefunc = locate_hash_insert;
				locator->nodeMap = nodeMap;
				switch (locator->listType)
				{
					case LOCATOR_LIST_NONE:
					case LOCATOR_LIST_INT:
						locator->results = palloc(sizeof(int));
						break;
					case LOCATOR_LIST_OID:
						locator->results = palloc(sizeof(Oid));
						break;
					case LOCATOR_LIST_POINTER:
						locator->results = palloc(sizeof(void *));
						break;
					case LOCATOR_LIST_LIST:
						/* Should never happen */
						Assert(false);
						break;
				}
			}
			else
			{
				locator->locatefunc = locate_hash_select;
				locator->nodeMap = nodeMap;
				switch (locator->listType)
				{
					case LOCATOR_LIST_NONE:
					case LOCATOR_LIST_INT:
						locator->results = palloc(locator->nodeCount * sizeof(int));
						break;
					case LOCATOR_LIST_OID:
						locator->results = palloc(locator->nodeCount * sizeof(Oid));
						break;
					case LOCATOR_LIST_POINTER:
						locator->results = palloc(locator->nodeCount * sizeof(void *));
						break;
					case LOCATOR_LIST_LIST:
						/* Should never happen */
						Assert(false);
						break;
				}
			}

			locator->hashfunc = hash_func_ptr(dataType);
			if (locator->hashfunc == NULL)
				ereport(ERROR, (errmsg("Error: unsupported data type for HASH locator: %d\n",
								   dataType)));
			break;
		case LOCATOR_TYPE_MODULO:
			if (accessType == RELATION_ACCESS_INSERT)
			{
				locator->locatefunc = locate_modulo_insert;
				locator->nodeMap = nodeMap;
				switch (locator->listType)
				{
					case LOCATOR_LIST_NONE:
					case LOCATOR_LIST_INT:
						locator->results = palloc(sizeof(int));
						break;
					case LOCATOR_LIST_OID:
						locator->results = palloc(sizeof(Oid));
						break;
					case LOCATOR_LIST_POINTER:
						locator->results = palloc(sizeof(void *));
						break;
					case LOCATOR_LIST_LIST:
						/* Should never happen */
						Assert(false);
						break;
				}
			}
			else
			{
				locator->locatefunc = locate_modulo_select;
				locator->nodeMap = nodeMap;
				switch (locator->listType)
				{
					case LOCATOR_LIST_NONE:
					case LOCATOR_LIST_INT:
						locator->results = palloc(locator->nodeCount * sizeof(int));
						break;
					case LOCATOR_LIST_OID:
						locator->results = palloc(locator->nodeCount * sizeof(Oid));
						break;
					case LOCATOR_LIST_POINTER:
						locator->results = palloc(locator->nodeCount * sizeof(void *));
						break;
					case LOCATOR_LIST_LIST:
						/* Should never happen */
						Assert(false);
						break;
				}
			}

			locator->valuelen = modulo_value_len(dataType);
			if (locator->valuelen == -1)
				ereport(ERROR, (errmsg("Error: unsupported data type for MODULO locator: %d\n",
								   dataType)));
			break;
		/* begin opentenbase_ora */
		case LOCATOR_TYPE_MIXED:
			{
				multi_distribution  *edist;

				if (locator_extra == NULL)
					ereport(ERROR,
							(errmsg("multiple distributions not found for distribution: %c\n",
									locatorType)));

				edist = (multi_distribution *) locator_extra;
				edist->nnodes = list_length(edist->locator);
				edist->dist_nodes = palloc0(edist->nnodes * sizeof(List *));
				edist->resultslot = MakeTupleTableSlot(NULL);

				locator->nodeMap = nodeMap;
				locator->groupid = groupid;
				locator->locator_extra = locator_extra;

				if (accessType == RELATION_ACCESS_INSERT)
					locator->locatefunc = locate_expr_insert;
				else
					ereport(ERROR,
							(errmsg("unsupported access type %d for distribution: %c\n",
									accessType, locatorType)));
				switch (locator->listType)
				{
					case LOCATOR_LIST_NONE:
					case LOCATOR_LIST_INT:
						locator->results = palloc(sizeof(int));
						break;
					case LOCATOR_LIST_OID:
					case LOCATOR_LIST_POINTER:
					case LOCATOR_LIST_LIST:
						ereport(ERROR,
								(errmsg("unsupported list type %d for distribution: %c\n",
										locator->listType, locatorType)));
						break;
				}

			}
			break;
		/* end opentenbase_ora */
		default:
			ereport(ERROR, (errmsg("Error: no such supported locator type: %c\n",
								   locatorType)));
	}

	if (result)
		*result = locator->results;

	return locator;
}

void
freeLocator(Locator *locator)
{
	/* begin opentenbase_ora */
	if (locator->locator_extra != NULL)
	{
		multi_distribution *edist = (multi_distribution *) locator->locator_extra;
		ListCell *lc;

		foreach(lc, edist->locator)
		{
			Locator	*sub_locator;

			sub_locator = (Locator*)(lfirst(lc));
			freeLocator(sub_locator);
		}

		if (edist->resultslot != NULL)
			ExecDropSingleTupleTableSlot(edist->resultslot);
	}
	/* end opentenbase_ora */

	pfree(locator->nodeMap);
	/*
	 * locator->nodeMap and locator->results may point to the same memory,
	 * do not free it twice
	 */
	if (locator->results != locator->nodeMap)
		pfree(locator->results);
	pfree(locator);
}

/*
 * Each time return the same predefined results
 */
static int
locate_static(Locator *self,
#ifdef __OPENTENBASE_C__
			  Datum *disValues, bool *disIsNulls, int ndiscols,
#endif
			  uint32 *hashvalue)
{
	/* TODO */
	if (hashvalue)
		*hashvalue = 0;
	return self->nodeCount;
}

/*
 * Each time return one next node, in round robin manner
 */
static int
locate_roundrobin(Locator *self,
#ifdef __OPENTENBASE_C__
				  Datum *disValues, bool *disIsNulls, int ndiscols,
#endif
				  uint32 *hashvalue)
{
	/* TODO */
	if (hashvalue)
		*hashvalue = 0;
	if (++self->roundRobinNode >= self->nodeCount)
		self->roundRobinNode = 0;
	switch (self->listType)
	{
		case LOCATOR_LIST_NONE:
			((int *) self->results)[0] = self->roundRobinNode;
			break;
		case LOCATOR_LIST_INT:
			((int *) self->results)[0] =
					((int *) self->nodeMap)[self->roundRobinNode];
			break;
		case LOCATOR_LIST_OID:
			((Oid *) self->results)[0] =
					((Oid *) self->nodeMap)[self->roundRobinNode];
			break;
		case LOCATOR_LIST_POINTER:
			((void **) self->results)[0] =
					((void **) self->nodeMap)[self->roundRobinNode];
			break;
		case LOCATOR_LIST_LIST:
			/* Should never happen */
			Assert(false);
			break;
	}
	return 1;
}

/*
 * Each time return one node, in a random manner
 * This is similar to locate_modulo_select, but that
 * function does not use a random modulo..
 */
static int
locate_modulo_random(Locator *self,
#ifdef __OPENTENBASE_C__
					 Datum *disValues, bool *disIsNulls, int ndiscols,
#endif
					 uint32 *hashvalue)
{
	int offset;

	if (hashvalue)
		*hashvalue = 0;
	Assert(self->nodeCount > 0);
	offset = compute_modulo(abs(rand()), self->nodeCount);
	switch (self->listType)
	{
		case LOCATOR_LIST_NONE:
			((int *) self->results)[0] = offset;
			break;
		case LOCATOR_LIST_INT:
			((int *) self->results)[0] =
					((int *) self->nodeMap)[offset];
			break;
		case LOCATOR_LIST_OID:
			((Oid *) self->results)[0] =
					((Oid *) self->nodeMap)[offset];
			break;
		case LOCATOR_LIST_POINTER:
			((void **) self->results)[0] =
					((void **) self->nodeMap)[offset];
			break;
		case LOCATOR_LIST_LIST:
			/* Should never happen */
			Assert(false);
			break;
	}
	return 1;
}

/*
 * Calculate hash from supplied value and use modulo by nodeCount as an index
 */
static int
locate_hash_insert(Locator *self,
#ifdef __OPENTENBASE_C__
				   Datum *disValues, bool *disIsNulls, int ndiscols,
#endif
				   uint32 *hashvalue)
{
	int index;
	bool isnull = false;
	Datum value = 0;
	unsigned int hash32 = 0;

	if (ndiscols > 0)
	{
		isnull = disIsNulls[0];
		value = disValues[0];
	}
	if (isnull || ndiscols == 0)
		index = 0;
	else
	{
		if (self->hashfunc == NULL)
			ereport(ERROR, (errmsg("Error: HASH locator can only handle NULL value as unknown data type")));

		hash32 = (unsigned int) DatumGetInt32(DirectFunctionCall1(self->hashfunc, value));
		index = compute_modulo(hash32, self->nodeCount);
	}

	if (hashvalue)
		*hashvalue = hash32;

	switch (self->listType)
	{
		case LOCATOR_LIST_NONE:
			((int *) self->results)[0] = index;
			break;
		case LOCATOR_LIST_INT:
			((int *) self->results)[0] = ((int *) self->nodeMap)[index];
			break;
		case LOCATOR_LIST_OID:
			((Oid *) self->results)[0] = ((Oid *) self->nodeMap)[index];
			break;
		case LOCATOR_LIST_POINTER:
			((void **) self->results)[0] = ((void **) self->nodeMap)[index];
			break;
		case LOCATOR_LIST_LIST:
			/* Should never happen */
			Assert(false);
			break;
	}
	return 1;
}

#ifdef _MIGRATE_
uint32
EvaluateHashkey(Oid *type, bool *isNull, Datum *dvalue, int nAttr)
{
	int i = 0;
	uint32	hkey;
	uint32 hashkey = 0;
	
	for (i = 0; i < nAttr; i++)
	{
		hashkey = (hashkey << 1) | ((hashkey & 0x80000000) ? 1 : 0);
		if (!isNull[i])
		{
			hkey = DatumGetUInt32(compute_hash(type[i], dvalue[i], LOCATOR_TYPE_SHARD));
			hashkey ^= hkey;
		}
	}

	return  hashkey;
}

static int
locate_shard_insert(Locator *self,
#ifdef __OPENTENBASE_C__
					Datum *disValues, bool *disIsNulls, int ndiscols,
#endif
					uint32 *hashvalue)
{
	int global_index = 0;
	int local_index  = 0;
	uint32 hashkey = 0;

	hashkey = EvaluateHashkey(self->discoltypes, disIsNulls, disValues, ndiscols);
	global_index = GetNodeIndexByHashValue(self->groupid, hashkey);

	if (hashvalue)
		*hashvalue = hashkey;

	switch (self->listType)
	{
		case LOCATOR_LIST_NONE:
			((int *) self->results)[0] = global_index;
			break;
		case LOCATOR_LIST_INT:
			((int *) self->results)[0] = global_index;
			break;
		case LOCATOR_LIST_OID:
			((Oid *) self->results)[0] = ((Oid *) self->nodeMap)[global_index];
			break;
		case LOCATOR_LIST_POINTER:
			{
				Assert(global_index >= 0);
				local_index = self->nodeindexMap[global_index];
				((void **) self->results)[0] = ((void **) self->nodeMap)[local_index];
			}
			break;
		case LOCATOR_LIST_LIST:
			/* Should never happen */
			Assert(false);
			break;
	}
	/* set to 1, since will route to only one node. */
	return 1;	
}

static int
locate_shard_select(Locator *self,
#ifdef __OPENTENBASE_C__
					Datum *disValues, bool *disIsNulls, int ndiscols,
#endif
					uint32 *hashvalue)
{
	int local_index  = 0;
	int	global_index = 0;
	bool has_null = false;
	uint32 hashkey = 0;
	
	if (ndiscols > 0)
	{
		int i = 0;
		for (i = 0; i < ndiscols; i++)
		{
			if (disIsNulls[i])
			{
				has_null = true;
				break;
			}
		}
	}
	else
	{
		has_null = true;
	}
	
	if (has_null)
	{
		int i;
		if (hashvalue)
			*hashvalue = hashkey;
		switch (self->listType)
		{
			case LOCATOR_LIST_NONE:
				for (i = 0; i < self->nodeCount; i++)
					((int *) self->results)[i] = i;
				break;
			case LOCATOR_LIST_INT:
				memcpy(self->results, self->nodeMap,
					   self->nodeCount * sizeof(int));
				break;
			case LOCATOR_LIST_OID:
				memcpy(self->results, self->nodeMap,
					   self->nodeCount * sizeof(Oid));
				break;
			case LOCATOR_LIST_POINTER:
				memcpy(self->results, self->nodeMap,
					   self->nodeCount * sizeof(void *));
				break;
			case LOCATOR_LIST_LIST:
				/* Should never happen */
				Assert(false);
				break;
		}
		return self->nodeCount;
	}
	else
	{
		hashkey = EvaluateHashkey(self->discoltypes, disIsNulls, disValues, ndiscols);
		global_index = GetNodeIndexByHashValue(self->groupid, hashkey);
		Assert(global_index >= 0);
		if (hashvalue)
			*hashvalue = hashkey;
		switch (self->listType)
		{
			case LOCATOR_LIST_NONE:
				((int *) self->results)[0] = global_index;
				break;
			case LOCATOR_LIST_INT:
				((int *) self->results)[0] = global_index;
				break;
			case LOCATOR_LIST_OID:
				((Oid *) self->results)[0] = ((Oid *) self->nodeMap)[global_index];
				break;
			case LOCATOR_LIST_POINTER:
				local_index        = self->nodeindexMap[global_index];
				((void **) self->results)[0] = ((void **) self->nodeMap)[local_index];
				break;
			case LOCATOR_LIST_LIST:
				/* Should never happen */
				Assert(false);
				break;
		}
		/* set to 1, since will route to only one node. */
		return 1;
	}
}
#endif

/*
 * Calculate hash from supplied value and use modulo by nodeCount as an index
 * if value is NULL assume no hint and return all the nodes.
 */
static int
locate_hash_select(Locator *self,
#ifdef __OPENTENBASE_C__
				   Datum *disValues, bool *disIsNulls, int ndiscols,
#endif
				   uint32 *hashvalue)
{
	Datum value = 0;
	bool isnull = false;
	unsigned int hash32 = 0;
	
	if (ndiscols > 0)
	{
		value = disValues[0];
		isnull = disIsNulls[0];
	}
	
	if (isnull || ndiscols == 0)
	{
		int i;
		if (hashvalue)
			*hashvalue = hash32;
		switch (self->listType)
		{
			case LOCATOR_LIST_NONE:
				for (i = 0; i < self->nodeCount; i++)
					((int *) self->results)[i] = i;
				break;
			case LOCATOR_LIST_INT:
				memcpy(self->results, self->nodeMap,
					   self->nodeCount * sizeof(int));
				break;
			case LOCATOR_LIST_OID:
				memcpy(self->results, self->nodeMap,
					   self->nodeCount * sizeof(Oid));
				break;
			case LOCATOR_LIST_POINTER:
				memcpy(self->results, self->nodeMap,
					   self->nodeCount * sizeof(void *));
				break;
			case LOCATOR_LIST_LIST:
				/* Should never happen */
				Assert(false);
				break;
		}
		return self->nodeCount;
	}
	else
	{
		int 		 index;

		if (self->hashfunc == NULL)
			ereport(ERROR, (errmsg("Error: HASH locator can only handle NULL value as unknown data type")));
		hash32 = (unsigned int) DatumGetInt32(DirectFunctionCall1(self->hashfunc, value));
		index = compute_modulo(hash32, self->nodeCount);
		if (hashvalue)
			*hashvalue = hash32;
		switch (self->listType)
		{
			case LOCATOR_LIST_NONE:
				((int *) self->results)[0] = index;
				break;
			case LOCATOR_LIST_INT:
				((int *) self->results)[0] = ((int *) self->nodeMap)[index];
				break;
			case LOCATOR_LIST_OID:
				((Oid *) self->results)[0] = ((Oid *) self->nodeMap)[index];
				break;
			case LOCATOR_LIST_POINTER:
				((void **) self->results)[0] = ((void **) self->nodeMap)[index];
				break;
			case LOCATOR_LIST_LIST:
				/* Should never happen */
				Assert(false);
				break;
		}
		return 1;
	}
}

/*
 * locate_expr_insert
 *    Construct a list array of distributed nodes for all sub-distribution. And
 *  return the max consumer ID. Later for each of consumer ID form a new tuple
 *  to deliver, see locate_get_distributed_tuple().
 */
static int
locate_expr_insert(Locator *top_loc,
				   Datum *values,
				   bool *nulls,
				   int ndiscols,
				   uint32 *hashvalue)
{
	TupleTableSlot	*slot;
	multi_distribution	*edist = (multi_distribution *) top_loc->locator_extra;
	ListCell	*lc;
	int		i = 0;
	int		subdist_id = 0;
	int		max_consid = 0;

	/* Reset distributed node results. Memory leak? */
	memset(edist->dist_nodes, 0, sizeof(List *) * edist->nnodes);

	slot = (TupleTableSlot *) top_loc->inputslot;

	foreach(lc, edist->locator)
	{
		Locator	*sub_loc = (Locator *) lfirst(lc);
		int		ncount;
		Datum	*value = NULL;
		bool	*isnull = NULL;

		/* get dist keys */
		if (sub_loc->ndiscols > 0)
		{
			value = palloc0(sizeof(Datum) * sub_loc->ndiscols);
			isnull = palloc0(sizeof(bool) * sub_loc->ndiscols);

			for (i = 0; i < sub_loc->ndiscols; i++)
				value[i] = slot_getattr(slot, sub_loc->discolnums[i], &isnull[i]);
		}

		ncount = GET_NODES(sub_loc, value, isnull, sub_loc->ndiscols, NULL);

		for (i = 0; i < ncount; i++)
		{
			List	*subms = edist->dist_nodes[subdist_id];
			int		consumerIdx;

			consumerIdx = ((int *) sub_loc->results)[i];
			subms = lappend_int(subms, consumerIdx);
			edist->dist_nodes[subdist_id] = subms;

			if (consumerIdx > max_consid)
				max_consid = consumerIdx;
		}

		subdist_id++;
	}

	return max_consid + 1;
}

/*
 * Use modulo of supplied value by nodeCount as an index
 */
static int
locate_modulo_insert(Locator *self,
#ifdef __OPENTENBASE_C__
					 Datum *disValues, bool *disIsNulls, int ndiscols,
#endif 
					 uint32 *hashvalue)
{
	int index;
	bool isnull = false;
	Datum value = 0;
	
	if (ndiscols > 0)
	{
		isnull = disIsNulls[0];
		value = disValues[0];
	}
	
	if (isnull || ndiscols == 0)
		index = 0;
	else
	{
		uint64 val;

		if (self->valuelen == -1)
			ereport(ERROR, (errmsg("Error: MODULO locator can only handle NULL value as unknown data type")));

		if (self->valuelen == 8)
			val = (uint64) (GET_8_BYTES(value));
		else if (self->valuelen == 4)
			val = (uint64) (GET_4_BYTES(value));
		else if (self->valuelen == 2)
			val = (uint64) (GET_2_BYTES(value));
		else if (self->valuelen == 1)
			val = (uint64) (GET_1_BYTE(value));
		else
			val = 0;

		index = compute_modulo(val, self->nodeCount);
	}
	if (hashvalue)
		*hashvalue = 0;
	switch (self->listType)
	{
		case LOCATOR_LIST_NONE:
			((int *) self->results)[0] = index;
			break;
		case LOCATOR_LIST_INT:
			((int *) self->results)[0] = ((int *) self->nodeMap)[index];
			break;
		case LOCATOR_LIST_OID:
			((Oid *) self->results)[0] = ((Oid *) self->nodeMap)[index];
			break;
		case LOCATOR_LIST_POINTER:
			((void **) self->results)[0] = ((void **) self->nodeMap)[index];
			break;
		case LOCATOR_LIST_LIST:
			/* Should never happen */
			Assert(false);
			break;
	}
	return 1;
}

/*
 * Use modulo of supplied value by nodeCount as an index
 * if value is NULL assume no hint and return all the nodes.
 */
static int
locate_modulo_select(Locator *self,
#ifdef __OPENTENBASE_C__
					 Datum *disValues, bool *disIsNulls, int ndiscols,
#endif
					 uint32 *hashvalue)
{
	bool isnull = false;
	Datum value = 0;
	
	if (ndiscols > 0)
	{
		isnull = disIsNulls[0];
		value = disValues[0];
	}
	
	if (hashvalue)
		*hashvalue = 0;
	if (isnull || ndiscols == 0)
	{
		int i;
		switch (self->listType)
		{
			case LOCATOR_LIST_NONE:
				for (i = 0; i < self->nodeCount; i++)
					((int *) self->results)[i] = i;
				break;
			case LOCATOR_LIST_INT:
				memcpy(self->results, self->nodeMap,
					   self->nodeCount * sizeof(int));
				break;
			case LOCATOR_LIST_OID:
				memcpy(self->results, self->nodeMap,
					   self->nodeCount * sizeof(Oid));
				break;
			case LOCATOR_LIST_POINTER:
				memcpy(self->results, self->nodeMap,
					   self->nodeCount * sizeof(void *));
				break;
			case LOCATOR_LIST_LIST:
				/* Should never happen */
				Assert(false);
				break;
		}
		return self->nodeCount;
	}
	else
	{
		uint64	val;
		int 	index;

		if (self->valuelen == -1)
			ereport(ERROR, (errmsg("Error: MODULO locator can only handle NULL value as unknown data type")));

		if (self->valuelen == 8)
			val = (uint64) (GET_8_BYTES(value));
		else if (self->valuelen == 4)
			val = (unsigned int) (GET_4_BYTES(value));
		else if (self->valuelen == 2)
			val = (unsigned int) (GET_2_BYTES(value));
		else if (self->valuelen == 1)
			val = (unsigned int) (GET_1_BYTE(value));
		else
			val = 0;

		index = compute_modulo(val, self->nodeCount);

		switch (self->listType)
		{
			case LOCATOR_LIST_NONE:
				((int *) self->results)[0] = index;
				break;
			case LOCATOR_LIST_INT:
				((int *) self->results)[0] = ((int *) self->nodeMap)[index];
				break;
			case LOCATOR_LIST_OID:
				((Oid *) self->results)[0] = ((Oid *) self->nodeMap)[index];
				break;
			case LOCATOR_LIST_POINTER:
				((void **) self->results)[0] = ((void **) self->nodeMap)[index];
				break;
			case LOCATOR_LIST_LIST:
				/* Should never happen */
				Assert(false);
				break;
		}
		return 1;
	}
}

int
GET_NODES(Locator *self,
		  Datum *disValues, bool *disIsNulls, int ndiscols,
		  uint32 *hashvalue)
{
	return (*self->locatefunc) (self, disValues, disIsNulls, ndiscols, hashvalue);
}

#ifdef __OPENTENBASE__
char
getLocatorDisType(Locator *self)
{
	return self->locatorType;
}

bool
IsDistributedColumn(AttrNumber attr, RelationLocInfo *relation_loc_info)
{
	bool result = false;
	
	if (relation_loc_info && IsLocatorDistributedByValue(relation_loc_info->locatorType))
	{
		int i = 0;
		for (i = 0; i < relation_loc_info->nDisAttrs; i++)
		{
			if (attr == relation_loc_info->disAttrNums[i])
			{
				result = true;
				break;
			}
		}
	}

	return result;
}
#endif

void *
getLocatorResults(Locator *self)
{
	return self->results;
}

void *
getLocatorNodeMap(Locator *self)
{
	return self->nodeMap;
}

int
getLocatorNodeCount(Locator *self)
{
	return self->nodeCount;
}
#endif

/*
 * GetRelationNodes
 *
 * Get list of relation nodes
 * If the table is replicated and we are reading, we can just pick one.
 * If the table is partitioned, we apply partitioning column value, if possible.
 *
 * If the relation is partitioned, partValue will be applied if present
 * (indicating a value appears for partitioning column), otherwise it
 * is ignored.
 *
 * preferredNodes is only used when for replicated tables. If set, it will
 * use one of the nodes specified if the table is replicated on it.
 * This helps optimize for avoiding introducing additional nodes into the
 * transaction.
 *
 * The returned List is a copy, so it should be freed when finished.
 */
ExecNodes *
GetRelationNodes(RelationLocInfo *rel_loc_info,
				 Datum *disValues, bool *disIsNulls, int ndiscols,
				 RelationAccessType accessType)
{
	ExecNodes	*exec_nodes;
	int			*nodenums;
	int			i, count;
	Locator		*locator;

	if (rel_loc_info == NULL)
		return NULL;

	exec_nodes = makeNode(ExecNodes);
	exec_nodes->baselocatortype = rel_loc_info->locatorType;
	exec_nodes->accesstype = accessType;
	exec_nodes->g_index_table_name = NULL;
#ifdef  _MIGRATE_
	locator = createLocator(rel_loc_info->locatorType,
							accessType,
							LOCATOR_LIST_LIST,
							0,
							(void *)rel_loc_info->rl_nodeList,
							(void **)&nodenums,
							false,
							rel_loc_info->groupId,
							rel_loc_info->disAttrTypes,
							rel_loc_info->disAttrNums,
							rel_loc_info->nDisAttrs, NULL);
#else
	locator = createLocator(rel_loc_info->locatorType,
							accessType,
							typeOfValueForDistCol,
							LOCATOR_LIST_LIST,
							0,
							(void *)rel_loc_info->rl_nodeList,
							(void **)&nodenums,
							false, NULL);
#endif
	count = GET_NODES(locator, disValues, disIsNulls, ndiscols, NULL);
	for (i = 0; i < count; i++)
		exec_nodes->nodeList = lappend_int(exec_nodes->nodeList, nodenums[i]);

	freeLocator(locator);
	return exec_nodes;
}

/*
 * GetRelationNodesForExplain
 * This is just for explain statement, just pick one datanode.
 * The returned List is a copy, so it should be freed when finished.
 */
ExecNodes *
GetRelationNodesForExplain(RelationLocInfo *rel_loc_info,
						   RelationAccessType accessType)
{
	ExecNodes	*exec_nodes;
	exec_nodes = makeNode(ExecNodes);
	exec_nodes->baselocatortype = rel_loc_info->locatorType;
	exec_nodes->accesstype = accessType;
	exec_nodes->nodeList = lappend_int(exec_nodes->nodeList, 1);
	return exec_nodes;
}

ExecNodes *
GetSimpleNodesByQuals(RelationLocInfo *rel_loc_info, Index varno,
                      Node *quals, List *rtable, RelationAccessType relaccess,
                      ParamListInfo boundParams)
{
	ExecNodes		*exec_nodes;
	int             ndiscols = 0;
	Datum           *disvalues = NULL;
	bool            *disisnulls = NULL;
	
	if (!rel_loc_info)
		return NULL;
	/*
	 * If the table distributed by value, check if we can reduce the Datanodes
	 * by looking at the qualifiers for this relation
	 */
	if (IsRelationDistributedByValue(rel_loc_info) && rel_loc_info->nDisAttrs > 0)
	{
		int             i = 0;

		/* for multi-distribution-columns */
		disvalues = (Datum *) palloc(sizeof(Datum) * rel_loc_info->nDisAttrs);
		disisnulls = (bool *) palloc(sizeof(bool) * rel_loc_info->nDisAttrs);
		ndiscols = rel_loc_info->nDisAttrs;

		for (i = 0; i < rel_loc_info->nDisAttrs; i++)
		{
			AttrNumber	attrNum = rel_loc_info->disAttrNums[i];
			Oid			disttype = rel_loc_info->disAttrTypes[i];
			int32		disttypmod = rel_loc_info->disAttrTypMods[i];
			Expr		*distcol_expr = NULL;

			distcol_expr = pgxc_find_distcol_expr(varno, attrNum, quals, rtable);
			/*
			 * If the type of expression used to find the Datanode, is not same as
			 * the distribution column type, try casting it. This is same as what
			 * will happen in case of inserting that type of expression value as the
			 * distribution column value.
			 * 
			 * XXX: Not consider ArrayExpr now, may be fix later.
			 */
			if (distcol_expr)
			{
				distcol_expr = (Expr *) coerce_to_target_type(NULL,
		                                         (Node *) distcol_expr,
		                                         exprType((Node *) distcol_expr),
		                                         disttype, disttypmod,
		                                         COERCION_ASSIGNMENT,
		                                         COERCE_IMPLICIT_CAST, -1);

				distcol_expr = (Expr *) eval_const_expressions_with_params(boundParams,
																	(Node *) distcol_expr);
			}

			if (distcol_expr && IsA(distcol_expr, Const))
			{
				disvalues[i] = ((Const *) distcol_expr)->constvalue;
				disisnulls[i] = ((Const *)distcol_expr)->constisnull;
			}
			else
			{
				disisnulls[i] = true;
				disvalues[i] = 0;
			}
		}
	}

	exec_nodes = GetRelationNodes(rel_loc_info,
#ifdef __OPENTENBASE_C__
								  disvalues, disisnulls, ndiscols,
#endif
								  relaccess);

	if (disvalues)
		pfree(disvalues);
	if (disisnulls)
		pfree(disisnulls);

	return exec_nodes;
}

/*
 * GetRelationNodesByQuals
 * A wrapper around GetRelationNodes to reduce the node list by looking at the
 * quals. varno is assumed to be the varno of reloid inside the quals. No check
 * is made to see if that's correct.
 */
ExecNodes *
GetRelationNodesByQuals(Oid reloid,
                        RelationLocInfo *rel_loc_info,
                        Index varno, Node *quals, List *rtable,
                        RelationAccessType relaccess,
                        Node **dis_qual, ParamListInfo boundParams)
{
#define ONE_SECOND_DATUM 1000000
	Expr			*distcol_expr = NULL;
	ExecNodes		*exec_nodes;
#ifdef __OPENTENBASE_C__
	int             i = 0;
	int             ndiscols = 0;
	Datum           *disvalues = NULL;
	bool            *disisnulls = NULL;
	AttrNumber		*disAttrNums = NULL;

	if (dis_qual)
		*dis_qual = NULL;
#endif
	if (!rel_loc_info)
		return NULL;

	/*
	 * If the table distributed by value, check if we can reduce the Datanodes
	 * by looking at the qualifiers for this relation
	 */
	if (IsRelationDistributedByValue(rel_loc_info))
	{
		/* for multi-distribution-columns */
		if (rel_loc_info->nDisAttrs > 0)
		{
			Expr *con_expr = NULL;
			
			disvalues = (Datum *) palloc(sizeof(Datum) * rel_loc_info->nDisAttrs);
			disisnulls = (bool *) palloc(sizeof(bool) * rel_loc_info->nDisAttrs);
			ndiscols = rel_loc_info->nDisAttrs;
			disAttrNums = rel_loc_info->disAttrNums;
			if (RelationOidIsCrossNodeIndex(reloid))
			{
				disAttrNums = cross_node_index_get_distribute_attnum_map(rel_loc_info);
			}
			for (i = 0; i < rel_loc_info->nDisAttrs; i++)
			{
				if (RelationOidIsCrossNodeIndex(reloid))
					con_expr = get_attr_expression(IndexGetRelation(reloid, true),
												   varno, quals, rtable,
												   disAttrNums[i], boundParams);
				else
					con_expr = get_attr_expression(reloid, varno, quals, rtable,
												   disAttrNums[i], boundParams);
				if (con_expr && IsA(con_expr, Const))
				{
					disvalues[i] = ((Const *) con_expr)->constvalue;
					disisnulls[i] = ((Const *)con_expr)->constisnull;
				}
				else
				{
					disisnulls[i] = true;
					disvalues[i] = 0;
				}
				
				/* Store the first distribution key. */
				if (i == 0)
					distcol_expr = (Expr *) con_expr;
			}
			if (RelationOidIsCrossNodeIndex(reloid))
			{
				pfree_ext(disAttrNums);
			}
		}
	}

	if (rel_loc_info->nDisAttrs == 1)
	{
		if (distcol_expr && IsA(distcol_expr, Const))
		{
			if (dis_qual)
				*dis_qual = (Node *) distcol_expr;
		}
		else
		{
			ndiscols = 1;
			disvalues[0] = (Datum) 0;
			disisnulls[0] = true;
		}
	}

	exec_nodes = GetRelationNodes(rel_loc_info,
#ifdef __OPENTENBASE_C__
								  disvalues, disisnulls, ndiscols,
#endif
								  relaccess);
	if (disvalues)
		pfree(disvalues);
	if (disisnulls)
		pfree(disisnulls);
	
    if (enable_global_indexscan)
	    RestrictNodesByGlobalIndex(reloid, varno, rtable, quals, relaccess,
	                               dis_qual, &exec_nodes, boundParams);

	return exec_nodes;
}

bool
IsRelationDistribColumn(RelationLocInfo *locInfo, const char *attname)
{
	int i = 0;
	
	/* No relation, so simply leave */
	if (!locInfo)
		return false;
	
	if (locInfo->nDisAttrs == 0)
		return false;
	
	/* No distribution column if relation is not distributed with a key */
	if (!IsRelationDistributedByValue(locInfo))
		return false;
	
	/* Return true if column name equals to any distributed-column name*/
	for(i = 0; i < locInfo->nDisAttrs; i++)
	{
		return (strcmp(attname, get_attname(locInfo->relid, locInfo->disAttrNums[i])) == 0);
	}
	return false;
}

/*
 * pgxc_find_distcol_expr
 * Search through the quals provided and find out an expression which will give
 * us value of distribution column if exists in the quals. Say for a table
 * tab1 (val int, val2 int) distributed by hash(val), a query "SELECT * FROM
 * tab1 WHERE val = fn(x, y, z) and val2 = 3", fn(x,y,z) is the expression which
 * decides the distribution column value in the rows qualified by this query.
 * Hence return fn(x, y, z). But for a query "SELECT * FROM tab1 WHERE val =
 * fn(x, y, z) || val2 = 3", there is no expression which decides the values
 * distribution column val can take in the qualified rows. So, in such cases
 * this function returns NULL.
 */
Expr *
pgxc_find_distcol_expr(Index varno, AttrNumber attrNum,
					   Node *quals, List *rtable)
{
	List *lquals;
	ListCell *qual_cell;

	/* If no quals, no distribution column expression */
	if (!quals)
		return NULL;

	/* Convert the qualification into List if it's not already so */
	if (!IsA(quals, List))
		lquals = make_ands_implicit((Expr *)quals);
	else
		lquals = (List *)quals;

	/*
	 * For every ANDed expression, check if that expression is of the form
	 * <distribution_col> = <expr>. If so return expr.
	 */
	foreach(qual_cell, lquals)
	{
		Expr *qual_expr = (Expr *)lfirst(qual_cell);
		OpExpr *op;
		Expr *lexpr;
		Expr *rexpr;
		Var *var_expr;
		Expr *distcol_expr;

		if (!IsA(qual_expr, OpExpr))
			continue;
		op = (OpExpr *)qual_expr;
		/* If not a binary operator, it can not be '='. */
		if (list_length(op->args) != 2)
			continue;

		lexpr = linitial(op->args);
		rexpr = lsecond(op->args);

		/*
		 * If either of the operands is a RelabelType, extract the Var in the RelabelType.
		 * A RelabelType represents a "dummy" type coercion between two binary compatible datatypes.
		 * If we do not handle these then our optimization does not work in case of varchar
		 * For example if col is of type varchar and is the dist key then
		 * select * from vc_tab where col = 'abcdefghijklmnopqrstuvwxyz';
		 * should be shipped to one of the nodes only
		 */
		if (IsA(lexpr, RelabelType))
			lexpr = ((RelabelType*)lexpr)->arg;
		if (IsA(rexpr, RelabelType))
			rexpr = ((RelabelType*)rexpr)->arg;

		/*
		 * If either of the operands is a Var expression, assume the other
		 * one is distribution column expression. If none is Var check next
		 * qual.
		 */
		if (IsA(lexpr, Var))
		{
			var_expr = (Var *)lexpr;
			distcol_expr = rexpr;
		}
		else if (IsA(rexpr, Var))
		{
			var_expr = (Var *)rexpr;
			distcol_expr = lexpr;
		}
		else
			continue;
		
		/* outer reference variable, skip */
		if (var_expr->varlevelsup != 0)
			continue;
		
		/*
		 * If Var found is not the distribution column of required relation,
		 * check next qual
		 */
		if (var_expr->varno != varno || var_expr->varattno != attrNum || var_expr->varlevelsup != 0)
		{
			/*
			 * Consider if this is a var of joinrel,
			 * should look for it's baserel.
			 */
			if (rtable != NULL)
			{
				RangeTblEntry   *rte = rt_fetch(var_expr->varno, rtable);
				
				if (rte->rtekind == RTE_JOIN)
				{
					Var *var = (Var *) list_nth(rte->joinaliasvars,
					                            var_expr->varattno - 1);
					if (var->varno != varno || var->varattno != attrNum)
						continue;
				}
				else
					continue;
			}
			else /* normal case, fail */
				continue;
		}
		/*
		 * If the operator is not an assignment operator, check next
		 * constraint. An operator is an assignment operator if it's
		 * mergejoinable or hashjoinable. Beware that not every assignment
		 * operator is mergejoinable or hashjoinable, so we might leave some
		 * oportunity. But then we have to rely on the opname which may not
		 * be something we know to be equality operator as well.
		 */
		if (!op_mergejoinable(op->opno, exprType((Node *)lexpr)) &&
			!op_hashjoinable(op->opno, exprType((Node *)lexpr)))
			continue;
		/* Found the distribution column expression return it */
		return distcol_expr;
	}
	/* Exhausted all quals, but no distribution column expression */
	return NULL;
}

static Expr *
get_attr_expression(Oid reloid, Index varno, Node *quals, List *rtable,
					AttrNumber attrNum, ParamListInfo boundParams)
{
	Oid disttype = get_atttype(reloid, attrNum);
	int32 disttypmod = get_atttypmod(reloid, attrNum);
	Expr *colexpr = pgxc_find_distcol_expr(varno, attrNum, quals, rtable);
	
	/*
	 * If the type of expression used to find the Datanode, is not same as
	 * the distribution column type, try casting it. This is same as what
	 * will happen in case of inserting that type of expression value as the
	 * distribution column value.
	 */
	if (colexpr)
	{
		colexpr = (Expr *) coerce_to_target_type(NULL,
		                                         (Node *) colexpr,
		                                         exprType((Node *) colexpr),
		                                         disttype, disttypmod,
		                                         COERCION_ASSIGNMENT,
		                                         COERCE_IMPLICIT_CAST, -1);

		colexpr = (Expr *) eval_const_expressions_with_params(boundParams,
															  (Node *) colexpr);
	}
	
	return colexpr;
}

#ifdef _MLS_
extern char* g_default_locator_type;
char
get_default_locator_type(void)
{
    if (strlen(g_default_locator_type) == 0)
    {
#ifdef ENABLE_ALL_TABLE_TYPE
        return LOCATOR_TYPE_HASH;
#else
        return LOCATOR_TYPE_SHARD;
#endif
    }

    if (strcmp(g_default_locator_type, "shard") == 0)
    {
        return LOCATOR_TYPE_SHARD;
    }
    else if (strcmp(g_default_locator_type, "hash") == 0)
    {
        return LOCATOR_TYPE_HASH;
    }
    else if (strcmp(g_default_locator_type, "replication") == 0)
    {
        return LOCATOR_TYPE_REPLICATED; 
    }

    elog(ERROR, "unknown locator type:%s", g_default_locator_type);
    /* keep compiler slience */
    return LOCATOR_TYPE_HASH;

}

int
get_default_distype(void)
{
    if (strlen(g_default_locator_type) == 0)
    {
#ifdef ENABLE_ALL_TABLE_TYPE
        return DISTTYPE_HASH;
#else
        return DISTTYPE_SHARD;
#endif
    }

    if (strcmp(g_default_locator_type, "shard") == 0)
    {
        return DISTTYPE_SHARD;
    }
    else if (strcmp(g_default_locator_type, "hash") == 0)
    {
        return DISTTYPE_HASH;
    }
    else if (strcmp(g_default_locator_type, "replication") == 0)
    {
        return DISTTYPE_REPLICATION; 
    }

    elog(ERROR, "unknown locator type:%s", g_default_locator_type);
    /* keep compiler slience */
    return DISTTYPE_HASH;
}

/* check if attnumsB is subset of attnumsA */
static bool
attnum_arr_contains(AttrNumber *attnumsA, int nattnumsA, 
					AttrNumber *attnumsB,int nattnumsB)
{
	bool flag = true;
	int iA, iB;
	for (iB = 0; iB < nattnumsB; iB ++)
	{
		flag = false;
		for (iA = 0; iA < nattnumsA; iA ++)
		{
			if (attnumsA[iA] == attnumsB[iB])
			{
				flag = true;
				break;
			}
		}
		if (flag == false)
			break;
	}
	return flag;
}

/*
 * RestrictNodesByGlobalIndex
 * check is it possible to restrict datanode use global index
 */
void
RestrictNodesByGlobalIndex(Oid reloid, Index varno,
                           List *rtable, Node *quals,
						   RelationAccessType relaccess,
						   Node **dis_qual,
						   ExecNodes **nodes, ParamListInfo boundParams)
{
    ListCell *l = NULL;
    List *index_info_list = NIL;
    IndexInfo *index_info = NULL;
    RelationLocInfo *rel_loc_info = NULL;
    ExecNodes		*tmp_exec_nodes;
    Relation        rel;
    bool            index_on_distkey = false;

	if (!OidIsValid(reloid))
		return;

	rel = relation_open(reloid, NoLock);
	/* get index list info */
	index_info_list = RelationGetIndexInfoList(rel);

	if (list_length((*nodes)->nodeList) == 1)
	{
		AttrNumber *discolnums = NULL;
		int ndiscols = 0;
		/* get normal index list info */
		ndiscols = RelationGetNumDisKeys(rel);
		discolnums = RelationGetDisKeys(rel);
		foreach(l, index_info_list)
		{
			index_info = lfirst_node(IndexInfo, l);
			
			if (index_info->ii_Am == BTREE_AM_OID &&
				attnum_arr_contains(index_info->ii_KeyAttrNumbers, 
					index_info->ii_NumIndexAttrs, 
					discolnums, 
					ndiscols))
			{
				index_on_distkey = true;
				break;
			}
		}
	}
	if (index_on_distkey)
	{
		relation_close(rel, NoLock);
		return;
	}

	index_info_list = RelationGetGlobalIndexInfoList(rel);
	foreach(l, index_info_list)
	{
		index_info = lfirst_node(IndexInfo, l);
		if (!index_info->ii_Unique)
			continue;
		rel_loc_info = GetRelationLocInfo(index_info->gindex_oid);
		tmp_exec_nodes = GetRelationNodesByQuals(index_info->gindex_oid,
		                                         rel_loc_info,
		                                         varno,
		                                         quals,
		                                         rtable,
		                                         relaccess,
		                                         dis_qual,
		                                         boundParams);
		if (list_length((*nodes)->nodeList) > list_length((tmp_exec_nodes)->nodeList) && 
			list_length((tmp_exec_nodes)->nodeList) > 0)
		{
			tmp_exec_nodes->g_index_table_name = get_rel_name(index_info->gindex_oid);
			FreeExecNodes(nodes);
			*nodes = tmp_exec_nodes;
			
		}
		else
		{
			FreeExecNodes(&tmp_exec_nodes);
		}
	}
	relation_close(rel, NoLock);
}

/* begin opentenbase_ora */
multi_distribution *
get_locator_extradata(Locator *l)
{
	return l->locator_extra;
}

void
set_locator_inputslot(Locator *l, void *slot)
{
	l->inputslot = (TupleTableSlot *) slot;
}
/* end opentenbase_ora */
#endif
