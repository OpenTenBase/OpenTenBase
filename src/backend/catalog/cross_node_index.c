/*-------------------------------------------------------------------------
 *
 * cross_node_index.c
 *	  code to create and destroy POSTGRES cross node index relations and it`s index
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/catalog/cross_node_index.c
 *
 *
 * INTERFACE ROUTINES
 *		cross_node_index_create()	- Create a cataloged cross node index relation
 *		cross_node_index_drop()		- Removes cross node index relation from catalogs
 *		BuildCrossNodeIndexInfo()	- Prepare to insert cross node  index tuples
 *		FormCrossNodeIndexDatum()	- Construct cross node datum vector for one index tuple
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <unistd.h>
#include "access/skey.h"
#include "access/xact.h"
#include "access/htup_details.h"
#include "catalog/objectaccess.h"
#include "catalog/index.h"
#include "catalog/indexing.h"
#include "catalog/heap.h"
#include "catalog/pg_attribute.h"
#include "catalog/pg_inherits_fn.h"
#include "catalog/pg_type.h"
#include "commands/defrem.h"
#include "commands/tablecmds.h"
#include "commands/tablespace.h"
#include "nodes/pg_list.h"
#include "nodes/makefuncs.h"
#include "parser/parser.h"
#include "parser/parse_utilcmd.h"
#include "utils/inval.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/syscache.h"
#include "utils/relcache.h"

/* ----------------------------------------------------------------
 *		index_table_create
 *
 * Create index table and related index on the index table.
 * ----------------------------------------------------------------
 */
void
index_table_create(Relation heapRelation,
							TupleDesc index_key_desc,
							char *indexRelationName,
							bool unique,
							Oid tableSpaceId,
							Oid nameSpaceId,
							ObjectAddress *relation,
							ObjectAddress *index,
							IndexStmt *originIndexStmt)
{
	List	    *table_elts = NULL;
	IndexStmt   *indexStmt;
	CreateStmt  *createStmt;
	ColumnDef   *coldef;
	int i;
	Form_pg_attribute att;
	List *index_table_diskey = NULL;
	if (RelationIsPartition(heapRelation->rd_rel))
		ereport(ERROR,
		(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
		 errmsg("global index does not support partition table yet")));
	if (heapRelation->rd_locator_info->locatorType != LOCATOR_TYPE_SHARD)
		ereport(ERROR,
		(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
		 errmsg("global index does not support no shard table yet")));
	/* Start building a CreateStmt for creating the index table */
	createStmt = makeNode(CreateStmt);
	createStmt->relation = makeRangeVar(get_namespace_name(nameSpaceId),
										indexRelationName, 
										0);
	createStmt->relation->relpersistence = RELPERSISTENCE_PERMANENT;
	createStmt->options = list_make1(makeDefElem("fillfactor", (Node *) makeInteger(70), 0));;
	createStmt->oncommit = ONCOMMIT_NOOP;
	createStmt->tablespacename = get_tablespace_name(tableSpaceId);
	createStmt->islocal = true;
	createStmt->relkind = RELKIND_RELATION;
	createStmt->distributeby = makeNode(DistributeBy);
	createStmt->distributeby->disttype = DISTTYPE_SHARD;
	createStmt->subcluster = makeShardSubCluster(heapRelation->rd_locator_info->groupId);

	coldef = makeNode(ColumnDef);
	coldef->is_local = true;

	for (i = 0; i < index_key_desc->natts; i++)
	{
		att = &index_key_desc->attrs[i];
		coldef->colname = att->attname.data;
		coldef->typeName = makeTypeNameFromOid(att->atttypid, att->atttypmod);
		table_elts = lappend(table_elts, copyObject(coldef));

		/* we donot support set distribute key for index table independently */
		index_table_diskey = lappend(index_table_diskey, makeString(att->attname.data));
	}

	createStmt->distributeby->colname = index_table_diskey;
	coldef->colname = CNI_SHARD_ID;
	coldef->typeName = makeTypeNameFromOid(INT8OID, -1);
	table_elts = lappend(table_elts, copyObject(coldef));

	coldef->colname = CNI_TABLE_OID;
	coldef->typeName = makeTypeNameFromOid(INT8OID, -1);
	table_elts = lappend(table_elts, copyObject(coldef));

	coldef->colname = CNI_CTID;
	coldef->typeName = makeTypeNameFromOid(TIDOID, -1);
	table_elts = lappend(table_elts, copyObject(coldef));
	
	coldef->colname = CNI_EXTRA;
	coldef->typeName = makeTypeNameFromOid(TEXTOID, -1);
	table_elts = lappend(table_elts, copyObject(coldef));
	createStmt->tableElts = table_elts;
	
	*relation = DefineRelation(createStmt,
								RELKIND_GLOBAL_INDEX,
								InvalidOid, NULL,
								NULL,
								InvalidOid);

	CommandCounterIncrement();

	/* Start building a IndexStmt for creating the index */
	indexStmt = makeNode(IndexStmt);
	indexStmt->unique = unique;
	indexStmt->cross_node = false;
	indexStmt->concurrent = false;
	indexStmt->relation = makeRangeVar(get_namespace_name(nameSpaceId),
										ChooseRelationName(indexRelationName, NULL, "idt", nameSpaceId), 
										0);
	indexStmt->relationId = InvalidOid;
	indexStmt->accessMethod = DEFAULT_INDEX_TYPE;

	indexStmt->indexParams = originIndexStmt->indexParams;
	indexStmt->options = NIL;

	*index = DefineIndex(relation->objectId, indexStmt,
						InvalidOid, InvalidOid, InvalidOid,
						false, false, false, false, true, NULL, NULL);

	CommandCounterIncrement();

	list_free_deep(index_table_diskey);
	index_table_diskey = NULL;
	list_free_deep(table_elts);
	table_elts = NULL;
}

void
cross_node_index_drop(Oid indexId, bool concurrent)
{
	char	*relname;
	char	*spcname;
	List	*q_name;
	Oid		heapId;
	Relation	userHeapRelation;
	Relation	indexRelation;
	HeapTuple	tuple;
	DropStmt *dstmt;
	
	heapId = IndexGetRelation(indexId, false);
	userHeapRelation = heap_open(heapId, AccessExclusiveLock);
	spcname = get_namespace_name(get_rel_namespace(indexId));
	relname = get_rel_name(indexId);
	
	dstmt = makeNode(DropStmt);
	dstmt = makeNode(DropStmt);
	dstmt->removeType = OBJECT_INDEX;
	dstmt->missing_ok = false;
	dstmt->behavior = DROP_CASCADE;
	dstmt->concurrent = concurrent;
	q_name = list_make2(makeString(spcname), makeString(relname));
	dstmt->objects = lappend(dstmt->objects, q_name);
	RemoveRelations(dstmt, NULL);

	RelationForgetRelation(indexId);
	indexRelation = heap_open(IndexRelationId, RowExclusiveLock);
	tuple = SearchSysCache1(INDEXRELID, ObjectIdGetDatum(indexId));
	CatalogTupleDelete(indexRelation, &tuple->t_self);
	ReleaseSysCache(tuple);
	heap_close(indexRelation, RowExclusiveLock);
	/*
	 * fix ATTRIBUTE relation
	 */
	DeleteAttributeTuples(indexId);

	/*
	 * fix RELATION relation
	 */
	DeleteRelationTuple(indexId);

	/*
	 * fix INHERITS relation
	 */
	DeleteInheritsTuple(indexId, InvalidOid);
	CacheInvalidateRelcache(userHeapRelation);
	heap_close(userHeapRelation, NoLock);
}


/*
 * relid_has_globalindex
 *    whether relation has cross node index.
 */
bool
relid_has_cross_node_index(Oid relid)
{
	Relation rel;
	bool result = false;
	rel = relation_open(relid, NoLock);
	result = relation_has_cross_node_index(rel);
	heap_close(rel, NoLock);
	return result;
}

bool
relation_has_cross_node_index(Relation rel)
{
	bool result = false;
	result = (bool)(RelationBuildCrossNodeIndexList(rel, false) != NIL);
	return result;
}

List * 
RelationBuildCrossNodeIndexList(Relation relation, bool copy_data)
{
	Relation	indrel;
	SysScanDesc indscan;
	ScanKeyData skey;
	HeapTuple	htup;
	List	   *result = NIL;
	MemoryContext oldcxt;
	
	/* Quick exit if we already computed the list. */
	if (relation->rd_cross_node_index_list_valid)
	{
		if (copy_data)
			return list_copy(relation->rd_cross_node_index_list);
		else
			return relation->rd_cross_node_index_list;
	}
		

	/* Prepare to scan pg_index for entries having indrelid = this rel. */
	ScanKeyInit(&skey,
				Anum_pg_index_indrelid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(RelationGetRelid(relation)));
	
	indrel = heap_open(IndexRelationId, AccessShareLock);
	indscan = systable_beginscan(indrel, IndexIndrelidIndexId, true,
								NULL, 1, &skey);
	
	while (HeapTupleIsValid(htup = systable_getnext(indscan)))
	{
		Relation	index_relation;
		Form_pg_index index = (Form_pg_index) GETSTRUCT(htup);

		if (!IndexIsLive(index))
			continue;
		index_relation = relation_open(index->indexrelid, NoLock);
		if (RelationIsCrossNodeIndex(index_relation->rd_rel))
		{
			result = insert_ordered_oid(result, index->indexrelid);
		}
		heap_close(index_relation, NoLock);
	}
	systable_endscan(indscan);
	heap_close(indrel, AccessShareLock);
	
	/* Now save a copy of the completed list in the relcache entry. */
	oldcxt = MemoryContextSwitchTo(CacheMemoryContext);
	relation->rd_cross_node_index_list = list_copy(result);
	relation->rd_cross_node_index_list_valid = true;
	MemoryContextSwitchTo(oldcxt);

	if (!copy_data)
	{
		list_free(result);
		result = relation->rd_cross_node_index_list;
	}
	return result;
}

bool 
RelationOidIsCrossNodeIndex(Oid oid)
{
	Relation rel;
	bool res;

	if (!OidIsValid(oid))
		return false;

	rel = relation_open(oid, NoLock);
	res = RelationIsCrossNodeIndex(rel->rd_rel);
	relation_close(rel, NoLock);
	return res;
}

/* 
	get distribute colum as table`s index
	assume distribute colum is same as index key
*/
AttrNumber*
cross_node_index_get_distribute_attnum_map(RelationLocInfo *rel_loc_info)
{
	Relation	 index_table_relation;
	int i_distri = 0;
	AttrNumber* result = (AttrNumber *)palloc0(rel_loc_info->nDisAttrs * sizeof(AttrNumber));
	index_table_relation = relation_open(rel_loc_info->relid, NoLock);
	for (i_distri = 0; i_distri < rel_loc_info->nDisAttrs; i_distri++)
	{
		result[i_distri] = index_table_relation->rd_index->indkey.values[i_distri];
	}
	relation_close(index_table_relation, NoLock);
	return result;
	
}
