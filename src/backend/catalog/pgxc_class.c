/*-------------------------------------------------------------------------
 *
 * pgxc_class.c
 *	routines to support manipulation of the pgxc_class relation
 *
 * Copyright (c) 1996-2010, PostgreSQL Global Development Group
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 *
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/heapam.h"
#include "access/htup_details.h"
#include "catalog/dependency.h"
#include "catalog/indexing.h"
#include "catalog/namespace.h"
#include "catalog/pg_type.h"
#include "catalog/pg_shdepend.h"
#include "catalog/pgxc_class.h"
#include "utils/builtins.h"
#include "utils/rel.h"
#include "utils/syscache.h"
#ifdef _MIGRATE_
#include "utils/lsyscache.h"
#include "utils/fmgroids.h"
#endif
#include "pgxc/locator.h"
#include "utils/array.h"

/*
 * PgxcClassCreate
 *		Create a pgxc_class entry
 */
#ifdef _MIGRATE_
void
PgxcClassCreate(Oid pcrelid,
				char pclocatortype,
#ifdef __OPENTENBASE_C__
				int ndiscols,
				int *discolnums,
#endif
				int pchashalgorithm,
				int pchashbuckets,
				int numnodes,
				Oid *nodes,
				Oid group)
#else
void
PgxcClassCreate(Oid pcrelid,
				char pclocatortype,
				int pcattnum,
				int pchashalgorithm,
				int pchashbuckets,
				int numnodes,
				Oid *nodes)
#endif
{
	Relation	pgxcclassrel;
	HeapTuple	htup;
	bool		nulls[Natts_pgxc_class];
	Datum		values[Natts_pgxc_class];
	int		i;
	oidvector	*nodes_array;
#ifdef __OPENTENBASE_C__
	int2vector  *discols_array = NULL;
#endif

	/* Build array of Oids to be inserted */
	nodes_array = buildoidvector(nodes, numnodes);

#ifdef __OPENTENBASE_C__
	/* build array of distributed columns from third */
	if (ndiscols > 0)
	{
		discols_array = buildint2vector((int16 *)discolnums, ndiscols);
	}
	else
	{
		discols_array = buildint2vector(NULL, 0);
	}
#endif

	/* Iterate through attributes initializing nulls and values */
	for (i = 0; i < Natts_pgxc_class; i++)
	{
		nulls[i]  = false;
		values[i] = (Datum) 0;
	}

	/* should not happen */
	if (pcrelid == InvalidOid)
	{
		elog(ERROR,"pgxc class relid invalid.");
		return;
	}
	
#ifdef _MIGRATE_
	values[Anum_pgxc_class_distribute_group - 1] = ObjectIdGetDatum(group);
#endif
	values[Anum_pgxc_class_pcrelid - 1]   = ObjectIdGetDatum(pcrelid);
	values[Anum_pgxc_class_pclocatortype - 1] = CharGetDatum(pclocatortype);

	if (pclocatortype == LOCATOR_TYPE_HASH || pclocatortype == LOCATOR_TYPE_MODULO)
	{
		values[Anum_pgxc_class_pchashalgorithm - 1] = UInt16GetDatum(pchashalgorithm);
		values[Anum_pgxc_class_pchashbuckets - 1] = UInt16GetDatum(pchashbuckets);
	}
#ifdef _MIGRATE_
	else if (LOCATOR_TYPE_SHARD == pclocatortype)
	{
		values[Anum_pgxc_class_pchashalgorithm - 1] = UInt16GetDatum(pchashalgorithm);
		values[Anum_pgxc_class_pchashbuckets - 1] = UInt16GetDatum(pchashbuckets);
	}	
#endif

	/* Node information */
	values[Anum_pgxc_class_nodes - 1] = PointerGetDatum(nodes_array);
#ifdef __OPENTENBASE_C__
	if (discols_array)
	{
		values[Anum_pgxc_class_discolnums - 1] = PointerGetDatum(discols_array);
	}
	else
	{
		nulls[Anum_pgxc_class_discolnums - 1]  = true;
	}
#endif
	values[Anum_pgxc_class_second_distribute - 1] = 0;
	values[Anum_pgxc_class_cold_distribute_group - 1] = 0;
	nulls[Anum_pgxc_class_cold_nodes - 1] = true;

	/* Open the relation for insertion */
	pgxcclassrel = heap_open(PgxcClassRelationId, RowExclusiveLock);

	htup = heap_form_tuple(pgxcclassrel->rd_att, values, nulls);

	CatalogTupleInsert(pgxcclassrel, htup);

	heap_close(pgxcclassrel, RowExclusiveLock);
}


/*
 * PgxcClassAlter
 *		Modify a pgxc_class entry with given data
 */
void
PgxcClassAlter(Oid pcrelid,
			   char pclocatortype,
	           int ndiscols,
	           AttrNumber *discolnums,
			   int pchashalgorithm,
			   int pchashbuckets,
			   int numnodes,
			   Oid *nodes,
			   PgxcClassAlterType type)
{
	Relation	rel;
	HeapTuple	oldtup, newtup;
	oidvector  *nodes_array = NULL;
	Datum		new_record[Natts_pgxc_class];
	bool		new_record_nulls[Natts_pgxc_class];
	bool		new_record_repl[Natts_pgxc_class];

	Assert(OidIsValid(pcrelid));

	rel = heap_open(PgxcClassRelationId, RowExclusiveLock);
	oldtup = SearchSysCacheCopy1(PGXCCLASSRELID,
								 ObjectIdGetDatum(pcrelid));

	if (!HeapTupleIsValid(oldtup)) /* should not happen */
		elog(ERROR, "cache lookup failed for pgxc_class %u", pcrelid);

	/* Initialize fields */
	MemSet(new_record, 0, sizeof(new_record));
	MemSet(new_record_nulls, false, sizeof(new_record_nulls));
	MemSet(new_record_repl, false, sizeof(new_record_repl));

	/* Fields are updated depending on operation type */
	switch (type)
	{
		case PGXC_CLASS_ALTER_DISTRIBUTION:
			new_record_repl[Anum_pgxc_class_pclocatortype - 1] = true;
			new_record_repl[Anum_pgxc_class_pchashalgorithm - 1] = true;
			new_record_repl[Anum_pgxc_class_pchashbuckets - 1] = true;
			new_record_repl[Anum_pgxc_class_discolnums - 1] = true;
			break;
		case PGXC_CLASS_ALTER_NODES:
			new_record_repl[Anum_pgxc_class_nodes - 1] = true;

			/* Build array of Oids to be inserted */
			nodes_array = buildoidvector(nodes, numnodes);
			break;
		case PGXC_CLASS_ALTER_ALL:
		default:
			new_record_repl[Anum_pgxc_class_pcrelid - 1] = true;
			new_record_repl[Anum_pgxc_class_pclocatortype - 1] = true;
			new_record_repl[Anum_pgxc_class_pchashalgorithm - 1] = true;
			new_record_repl[Anum_pgxc_class_pchashbuckets - 1] = true;
			new_record_repl[Anum_pgxc_class_nodes - 1] = true;
			new_record_repl[Anum_pgxc_class_discolnums - 1] = true;

			/* Build array of Oids to be inserted */
			nodes_array = buildoidvector(nodes, numnodes);
	}

	/* Set up new fields */
	/* Relation Oid */
	if (new_record_repl[Anum_pgxc_class_pcrelid - 1])
		new_record[Anum_pgxc_class_pcrelid - 1] = ObjectIdGetDatum(pcrelid);

	/* Locator type */
	if (new_record_repl[Anum_pgxc_class_pclocatortype - 1])
		new_record[Anum_pgxc_class_pclocatortype - 1] = CharGetDatum(pclocatortype);

	/* Attribute number of distribution column */
	if (new_record_repl[Anum_pgxc_class_discolnums - 1])
	{
		if (ndiscols > 0)
		{
			int2vector *discols_array = buildint2vector((int16 *) discolnums, ndiscols);
			new_record[Anum_pgxc_class_discolnums - 1] = PointerGetDatum(discols_array);
		}
		else
		{
			new_record_nulls[Anum_pgxc_class_discolnums - 1] = true;
		}
	}

	/* Hash algorithm type */
	if (new_record_repl[Anum_pgxc_class_pchashalgorithm - 1])
		new_record[Anum_pgxc_class_pchashalgorithm - 1] = UInt16GetDatum(pchashalgorithm);

	/* Hash buckets */
	if (new_record_repl[Anum_pgxc_class_pchashbuckets - 1])
		new_record[Anum_pgxc_class_pchashbuckets - 1] = UInt16GetDatum(pchashbuckets);

	/* Node information */
	if (new_record_repl[Anum_pgxc_class_nodes - 1])
		new_record[Anum_pgxc_class_nodes - 1] = PointerGetDatum(nodes_array);

	new_record_nulls[Anum_pgxc_class_cold_distribute_group - 1] = true;
	new_record_nulls[Anum_pgxc_class_second_distribute - 1] = true;
	new_record_nulls[Anum_pgxc_class_cold_nodes - 1] = true;

	/* Update relation */
	newtup = heap_modify_tuple(oldtup, RelationGetDescr(rel),
							   new_record,
							   new_record_nulls, new_record_repl);
	CatalogTupleUpdate(rel, &oldtup->t_self, newtup);

	heap_close(rel, RowExclusiveLock);
}

/*
 * RemovePGXCClass():
 *		Remove extended PGXC information
 */
void
RemovePgxcClass(Oid pcrelid)
{
	Relation  relation;
	HeapTuple tup;
	Form_pgxc_class classtuple;

	/*
	 * Delete the pgxc_class tuple.
	 */
	relation = heap_open(PgxcClassRelationId, RowExclusiveLock);
	tup = SearchSysCache(PGXCCLASSRELID,
						 ObjectIdGetDatum(pcrelid),
						 0, 0, 0);

	if (!HeapTupleIsValid(tup)) /* should not happen */
		elog(ERROR, "cache lookup failed for pgxc_class %u", pcrelid);

	classtuple = (Form_pgxc_class) GETSTRUCT(tup);

	if (classtuple->pclocatortype == LOCATOR_TYPE_SHARD)
	{
		ClearGroupDependency(pcrelid, classtuple->pgroup);
	}
	simple_heap_delete(relation, &tup->t_self);

	ReleaseSysCache(tup);

	heap_close(relation, RowExclusiveLock);
}


#ifdef _MIGRATE_
/*
  * logic on datanode.
  * Distribute key must registered in datanode, it will be used to filter when data node would be splited.
  */
void
RegisterDistributeKey(Oid pcrelid,
				char pclocatortype,
				int ndiscols,
				AttrNumber *discolnums,
				int pchashalgorithm,
				int pchashbuckets)
{
	Relation	pgxcclassrel;
	HeapTuple	htup;
	bool		nulls[Natts_pgxc_class];
	Datum		values[Natts_pgxc_class];
	int		i;
	//oidvector  *nodes_array;
	

	/* Iterate through attributes initializing nulls and values */
	for (i = 0; i < Natts_pgxc_class; i++)
	{
		nulls[i]  = false;
		values[i] = (Datum) 0;
	}

	/* should not happen */
	if (pcrelid == InvalidOid)
	{
		elog(ERROR,"pgxc class relid invalid.");
		return;
	}

	values[Anum_pgxc_class_pcrelid - 1]   = ObjectIdGetDatum(pcrelid);
	values[Anum_pgxc_class_pclocatortype - 1] = CharGetDatum(pclocatortype);

	if (pclocatortype == LOCATOR_TYPE_HASH || pclocatortype == LOCATOR_TYPE_MODULO)
	{
		if (ndiscols > 0)
		{
			int2vector *discols_array = buildint2vector((int16 *) discolnums, ndiscols);
			values[Anum_pgxc_class_discolnums - 1] = PointerGetDatum(discols_array);
		}
		else
		{
			nulls[Anum_pgxc_class_discolnums - 1] = true;
		}
		values[Anum_pgxc_class_pchashalgorithm - 1] = UInt16GetDatum(pchashalgorithm);
		values[Anum_pgxc_class_pchashbuckets - 1] = UInt16GetDatum(pchashbuckets);
	}
	else if (LOCATOR_TYPE_SHARD == pclocatortype)
	{
		if (ndiscols > 0)
		{
			int2vector *discols_array = buildint2vector((int16 *) discolnums, ndiscols);
			values[Anum_pgxc_class_discolnums - 1] = PointerGetDatum(discols_array);
		}
		else
		{
			nulls[Anum_pgxc_class_discolnums - 1] = true;
		}
		values[Anum_pgxc_class_pchashalgorithm - 1] = UInt16GetDatum(pchashalgorithm);
		values[Anum_pgxc_class_pchashbuckets - 1] = UInt16GetDatum(pchashbuckets);
		
	}

	/* Node information */
	nulls[Anum_pgxc_class_nodes - 1] = true; 
	values[Anum_pgxc_class_second_distribute - 1] = 0;

	/* Open the relation for insertion */
	pgxcclassrel = heap_open(PgxcClassRelationId, RowExclusiveLock);

	htup = heap_form_tuple(pgxcclassrel->rd_att, values, nulls);

	CatalogTupleInsert(pgxcclassrel, htup);

	heap_close(pgxcclassrel, RowExclusiveLock);
}

char GetRelLocatorType(Oid reloid)
{
	char		locatortype = LOCATOR_TYPE_NONE;
	HeapTuple	 tuple; 
	Form_pgxc_class classtuple   = NULL;

	tuple = SearchSysCache(PGXCCLASSRELID,
						 	ObjectIdGetDatum(reloid),
						 	0, 0, 0);

	if (HeapTupleIsValid(tuple)) 
	{
		classtuple = (Form_pgxc_class)GETSTRUCT(tuple);
		locatortype = classtuple->pclocatortype;
		ReleaseSysCache(tuple);
		return locatortype;
	}
	else
	{
		elog(DEBUG1, "cache lookup failed for pgxc_class %u", reloid);
	}
    
	return locatortype;
}

Oid GetRelGroup(Oid reloid)
{
    Oid          group_oid  = InvalidOid;
	HeapTuple	 tuple; 
	Form_pgxc_class classtuple   = NULL;

	tuple = SearchSysCache(PGXCCLASSRELID,
						 	ObjectIdGetDatum(reloid),
						 	0, 0, 0);

	if (HeapTupleIsValid(tuple)) 
	{
		classtuple = (Form_pgxc_class)GETSTRUCT(tuple);
		group_oid = classtuple->pgroup;
			
		ReleaseSysCache(tuple);
		return group_oid;
	}
	else
	{
		elog(DEBUG1, "cache lookup failed for pgxc_class %u", reloid);
	}
    
    return InvalidOid;
}

void GetNotShardRelations(bool is_contain_replic, List **rellist, List **nslist)
{
	HeapScanDesc scan;
	HeapTuple 	tup;
	Form_pgxc_class pgxc_class;
	Relation rel;

	rel = heap_open(PgxcClassRelationId, AccessShareLock);	
	scan = heap_beginscan_catalog(rel,0,NULL);
	tup = heap_getnext(scan,ForwardScanDirection);

	while(HeapTupleIsValid(tup))
	{
		char *relname;
		char *nsname;
		Oid	  nsoid;
		Datum discolsDatum;
		bool  isNull;
		int2vector *discolnums;
		pgxc_class = (Form_pgxc_class)GETSTRUCT(tup);
		
		discolsDatum = heap_getattr(tup, Anum_pgxc_class_discolnums, RelationGetDescr(rel), &isNull);
		Assert (!isNull);
		
		discolnums = (int2vector *) DatumGetPointer(discolsDatum);
		if (discolnums->dim1 > 0)
		{
			if(discolnums->values[0] == 0)
			{
				tup = heap_getnext(scan,ForwardScanDirection);
				continue;
			}
		}

		if(is_contain_replic)
		{
			if(pgxc_class->pclocatortype != LOCATOR_TYPE_SHARD)
			{
				relname = get_rel_name(pgxc_class->pcrelid);
				if(rellist)
				{
					*rellist = lappend(*rellist, relname);
				}
				nsoid = get_rel_namespace(pgxc_class->pcrelid);
				nsname = get_namespace_name(nsoid);
				if(nslist)
				{
					*nslist = lappend(*nslist, nsname);
				}
			}
		}
		else
		{
			if(pgxc_class->pclocatortype != LOCATOR_TYPE_SHARD
				&& pgxc_class->pclocatortype != LOCATOR_TYPE_REPLICATED)
			{
				relname = get_rel_name(pgxc_class->pcrelid);
				if(rellist)
				{
					*rellist = lappend(*rellist, relname);
				}
				nsoid = get_rel_namespace(pgxc_class->pcrelid);
				nsname = get_namespace_name(nsoid);
				if(nslist)
				{
					*nslist = lappend(*nslist, nsname);
				}
			}
		}
		
		tup = heap_getnext(scan,ForwardScanDirection);
	}

	heap_endscan(scan);
	heap_close(rel,AccessShareLock);
}

List * GetShardRelations(bool is_contain_replic)
{
	HeapScanDesc scan;
	HeapTuple 	tup;
	Form_pgxc_class pgxc_class;
	List *result;
	Relation rel;

	rel = heap_open(PgxcClassRelationId, AccessShareLock);	
	scan = heap_beginscan_catalog(rel,0,NULL);
	tup = heap_getnext(scan,ForwardScanDirection);
	result = NULL;

	while(HeapTupleIsValid(tup))
	{
		pgxc_class = (Form_pgxc_class)GETSTRUCT(tup);

		if(pgxc_class->pclocatortype == LOCATOR_TYPE_SHARD
			|| (is_contain_replic && pgxc_class->pclocatortype == LOCATOR_TYPE_REPLICATED))
		{
			result = lappend_oid(result,pgxc_class->pcrelid);
		}

		tup = heap_getnext(scan,ForwardScanDirection);
	}

	heap_endscan(scan);
	heap_close(rel,AccessShareLock);

	return result;	
}


typedef enum PgxcClassRelationType
{
	PGXC_CLASS_REPLICATION_ALL,
	PGXC_CLASS_REPLICATION_GROUP,
	PGXC_CLASS_NONE
} PgxcClassRelationType;

/* modify pgxc_class with different types */
void
ModifyPgxcClass(PgxcClassModifyType type, PgxcClassModifyData *data)
{
	PgxcClassRelationType relation_type = PGXC_CLASS_REPLICATION_ALL;
	
	switch(type)
	{
		case PGXC_CLASS_ADD_NODE:
		case PGXC_CLASS_DROP_NODE:
			{
				HeapScanDesc scan;
				HeapTuple 	tup;
				Form_pgxc_class pgxc_class;
				Relation rel;
				oidvector *nodelist;
				int nkeys = 2;
				
				if (!OidIsValid(data->node))
					return;

				/*
				  * we need modify shard tables in given group, add node to nodelist,
				  * or remove node from nodelist.
				  */
				/* init scan key */
				while(relation_type < PGXC_CLASS_NONE)
				{
					ScanKeyData key[2];

					switch(relation_type)
					{					
						case PGXC_CLASS_REPLICATION_ALL:
						{
							ScanKeyInit(&key[0],
										Anum_pgxc_class_pclocatortype,
										BTEqualStrategyNumber, F_CHAREQ,
										CharGetDatum('R'));
							ScanKeyInit(&key[1],
										Anum_pgxc_class_distribute_group,
										BTEqualStrategyNumber, F_OIDEQ,
										ObjectIdGetDatum(0));
							break;
						}
						case PGXC_CLASS_REPLICATION_GROUP:
						{
							ScanKeyInit(&key[0],
										Anum_pgxc_class_pclocatortype,
										BTEqualStrategyNumber, F_CHAREQ,
										CharGetDatum('R'));
							ScanKeyInit(&key[1],
										Anum_pgxc_class_distribute_group,
										BTEqualStrategyNumber, F_OIDEQ,
										ObjectIdGetDatum(data->group));
							break;
						}
						default:
							break;
					}

					rel = heap_open(PgxcClassRelationId, AccessExclusiveLock);	

					scan = heap_beginscan_catalog(rel, nkeys, key);

					tup = heap_getnext(scan, ForwardScanDirection);

					while(HeapTupleIsValid(tup))
					{
						int num = 0;
						oidvector *oldoids = NULL;
						
						pgxc_class = (Form_pgxc_class)GETSTRUCT(tup);

						oldoids = &pgxc_class->nodeoids;
						num = Anum_pgxc_class_nodes;

						if (oidvector_member(oldoids, data->node))
						{
							if (type == PGXC_CLASS_DROP_NODE)
							{
								HeapTuple	newtup;
								Datum *values;
								bool  *isnull;
								bool  *replace;
								
								nodelist = oidvector_remove(oldoids, data->node);

								values = (Datum*)palloc0(Natts_pgxc_class * sizeof(Datum));
								isnull = (bool *)palloc0(Natts_pgxc_class * sizeof(bool));
								replace = (bool *)palloc0(Natts_pgxc_class * sizeof(bool));

								replace[num - 1] = true;
								values[num - 1] = PointerGetDatum(nodelist);

								newtup = heap_modify_tuple(tup, RelationGetDescr(rel), values, isnull, replace);		
								CatalogTupleUpdate(rel, &newtup->t_self, newtup);

								pfree(values);
								pfree(isnull);
								pfree(replace);
								pfree(nodelist);
								pfree(newtup);
							}
							else if (type == PGXC_CLASS_ADD_NODE)
							{
								if (relation_type == PGXC_CLASS_REPLICATION_ALL)
								{
									elog(LOG, "node %d already in nodelist of table %d with group %d in pgxc_class.",
										         data->node, pgxc_class->pcrelid, pgxc_class->pgroup);
								}
								else
								{
									heap_endscan(scan);
									heap_close(rel,AccessExclusiveLock);
									elog(ERROR, "node %d already in nodelist of table %d with group %d in pgxc_class.",
										         data->node, pgxc_class->pcrelid, pgxc_class->pgroup);
								}
							}
							else
							{
								heap_endscan(scan);
								heap_close(rel,AccessExclusiveLock);
								elog(ERROR, "unknow PgxcClassModifyType %d.", type);
							}
						}
						else
						{
							if (type == PGXC_CLASS_DROP_NODE)
							{
								elog(LOG, "node %d not in nodelist of table %d with group %d in pgxc_class.",
											data->node, pgxc_class->pcrelid, pgxc_class->pgroup);
							}
							else if (type == PGXC_CLASS_ADD_NODE)
							{
								HeapTuple	newtup;
								Datum *values;
								bool  *isnull;
								bool  *replace;
								
								nodelist = oidvector_append(oldoids, data->node);

								values = (Datum*)palloc0(Natts_pgxc_class * sizeof(Datum));
								isnull = (bool *)palloc0(Natts_pgxc_class * sizeof(bool));
								replace = (bool *)palloc0(Natts_pgxc_class * sizeof(bool));

								replace[num - 1] = true;
								values[num - 1] = PointerGetDatum(nodelist);

								newtup = heap_modify_tuple(tup, RelationGetDescr(rel), values, isnull, replace);		
								CatalogTupleUpdate(rel, &newtup->t_self, newtup);

								pfree(values);
								pfree(isnull);
								pfree(replace);
								pfree(nodelist);
								pfree(newtup);
							}
						}
						
						tup = heap_getnext(scan, ForwardScanDirection);
					}

					heap_endscan(scan);
					heap_close(rel,AccessExclusiveLock);
					if (OidIsValid(data->group))
					{
						relation_type++;
					}
					else
					{
						relation_type = PGXC_CLASS_NONE;
					}
				}
			}
		default:
			break;
	}
}

/*
 * Ensure key value hot group does not conflict table's hot group
 */
void CheckPgxcClassGroupConfilct(Oid keyvaluehot)	
{
	HeapScanDesc scan;
	HeapTuple	tup;
	Form_pgxc_class pgxc_class;	
	Relation rel;

	if (!OidIsValid(keyvaluehot))
	{
		return;
	}
	
	rel = heap_open(PgxcClassRelationId, AccessShareLock);	
	scan = heap_beginscan_catalog(rel, 0, NULL);
	tup = heap_getnext(scan,ForwardScanDirection);

	while(HeapTupleIsValid(tup))
	{
		pgxc_class = (Form_pgxc_class)GETSTRUCT(tup);
		
		if (OidIsValid(pgxc_class->pgroup))
		{
			if (keyvaluehot == pgxc_class->pgroup)
			{
				 elog(ERROR, "key value hot group %u conflict exist table:%u hot group %u", keyvaluehot, pgxc_class->pcrelid, pgxc_class->pgroup);
			}
		}
		tup = heap_getnext(scan,ForwardScanDirection);
	}
	
	heap_endscan(scan);
	heap_close(rel,AccessShareLock);
}

#endif
