/*-------------------------------------------------------------------------
 *
 * partcache.c
 *		Support routines for manipulating partition information cached in
 *		relcache
 *
 * Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *		  src/backend/utils/cache/partcache.c
 *
 *-------------------------------------------------------------------------
*/
#include "postgres.h"

#include "access/hash.h"
#include "access/heapam.h"
#include "access/htup_details.h"
#include "access/nbtree.h"
#include "catalog/partition.h"
#include "catalog/pg_inherits.h"
#include "catalog/pg_inherits_fn.h"
#include "catalog/pg_opclass.h"
#include "catalog/pg_partitioned_table.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "optimizer/clauses.h"
#include "optimizer/planner.h"
#include "partitioning/partbounds.h"
#include "utils/builtins.h"
#include "utils/datum.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/partcache.h"
#include "utils/rel.h"
#include "utils/syscache.h"
#ifdef _PG_ORCL_
#include "catalog/index.h"
#include "optimizer/predtest.h"
#endif

static void RelationBuildPartitionDesc(Relation rel);
static void RelationBuildPartitionKey(Relation relation);
static List *generate_partition_qual(Relation rel);
static int32 qsort_partition_hbound_cmp(const void *a, const void *b);
static int32 qsort_partition_list_value_cmp(const void *a, const void *b,
							   void *arg);
static int32 qsort_partition_rbound_cmp(const void *a, const void *b,
						   void *arg);

/*
 * RelationGetPartitionKey -- get partition key, if relation is partitioned
 *
 * Note: partition keys are not allowed to change after the partitioned rel
 * is created.  RelationClearRelation knows this and preserves rd_partkey
 * across relcache rebuilds, as long as the relation is open.  Therefore,
 * even though we hand back a direct pointer into the relcache entry, it's
 * safe for callers to continue to use that pointer as long as they hold
 * the relation open.
 */
PartitionKey
RelationGetPartitionKey(Relation rel)
{
	if (rel->rd_rel->relkind != RELKIND_PARTITIONED_TABLE)
		return NULL;

	if (unlikely(rel->rd_partkey == NULL))
		RelationBuildPartitionKey(rel);

	return rel->rd_partkey;

}

/*
 * RelationBuildPartitionKey
 *		Build and attach to relcache partition key data of relation
 *
 * Partitioning key data is a complex structure; to avoid complicated logic to
 * free individual elements whenever the relcache entry is flushed, we give it
 * its own memory context, child of CacheMemoryContext, which can easily be
 * deleted on its own.  To avoid leaking memory in that context in case of an
 * error partway through this function, the context is initially created as a
 * child of CurTransactionContext and only re-parented to CacheMemoryContext
 * at the end, when no further errors are possible.  Also, we don't make this
 * context the current context except in very brief code sections, out of fear
 * that some of our callees allocate memory on their own which would be leaked
 * permanently.
 */
static void
RelationBuildPartitionKey(Relation relation)
{
	Form_pg_partitioned_table form;
	HeapTuple	tuple;
	bool		isnull;
	int			i;
	PartitionKey key;
	AttrNumber *attrs;
	oidvector  *opclass;
	oidvector  *collation;
	ListCell   *partexprs_item;
	Datum		datum;
	MemoryContext partkeycxt,
				oldcxt;
	int16		procnum;

	tuple = SearchSysCache1(PARTRELID,
							ObjectIdGetDatum(RelationGetRelid(relation)));

	if (!HeapTupleIsValid(tuple))
		elog(ERROR, "cache lookup failed for partition key of relation %u",
			 RelationGetRelid(relation));

	partkeycxt = AllocSetContextCreate(CurTransactionContext,
									   "partition key",
									   ALLOCSET_SMALL_SIZES);
	MemoryContextCopyAndSetIdentifier(partkeycxt,
									  RelationGetRelationName(relation));

	key = (PartitionKey) MemoryContextAllocZero(partkeycxt,
												sizeof(PartitionKeyData));

	/* Fixed-length attributes */
	form = (Form_pg_partitioned_table) GETSTRUCT(tuple);
	key->strategy = form->partstrat;
	key->partnatts = form->partnatts;

	/*
	 * We can rely on the first variable-length attribute being mapped to the
	 * relevant field of the catalog's C struct, because all previous
	 * attributes are non-nullable and fixed-length.
	 */
	attrs = form->partattrs.values;

	/* But use the hard way to retrieve further variable-length attributes */
	/* Operator class */
	datum = SysCacheGetAttr(PARTRELID, tuple,
							Anum_pg_partitioned_table_partclass, &isnull);
	Assert(!isnull);
	opclass = (oidvector *) DatumGetPointer(datum);

	/* Collation */
	datum = SysCacheGetAttr(PARTRELID, tuple,
							Anum_pg_partitioned_table_partcollation, &isnull);
	Assert(!isnull);
	collation = (oidvector *) DatumGetPointer(datum);

	/* Expressions */
	datum = SysCacheGetAttr(PARTRELID, tuple,
							Anum_pg_partitioned_table_partexprs, &isnull);
	if (!isnull)
	{
		char	   *exprString;
		Node	   *expr;

		exprString = TextDatumGetCString(datum);
		expr = stringToNode(exprString);
		pfree(exprString);

		/*
		 * Run the expressions through const-simplification since the planner
		 * will be comparing them to similarly-processed qual clause operands,
		 * and may fail to detect valid matches without this step; fix
		 * opfuncids while at it.  We don't need to bother with
		 * canonicalize_qual() though, because partition expressions should be
		 * in canonical form already (ie, no need for OR-merging or constant
		 * elimination).
		 */
		expr = eval_const_expressions(NULL, expr);
		fix_opfuncids(expr);

		oldcxt = MemoryContextSwitchTo(partkeycxt);
		key->partexprs = (List *) copyObject(expr);
		MemoryContextSwitchTo(oldcxt);
	}

	oldcxt = MemoryContextSwitchTo(partkeycxt);
	key->partattrs = (AttrNumber *) palloc0(key->partnatts * sizeof(AttrNumber));
	key->partopfamily = (Oid *) palloc0(key->partnatts * sizeof(Oid));
	key->partopcintype = (Oid *) palloc0(key->partnatts * sizeof(Oid));
	key->partsupfunc = (FmgrInfo *) palloc0(key->partnatts * sizeof(FmgrInfo));

	key->partcollation = (Oid *) palloc0(key->partnatts * sizeof(Oid));

	/* Gather type and collation info as well */
	key->parttypid = (Oid *) palloc0(key->partnatts * sizeof(Oid));
	key->parttypmod = (int32 *) palloc0(key->partnatts * sizeof(int32));
	key->parttyplen = (int16 *) palloc0(key->partnatts * sizeof(int16));
	key->parttypbyval = (bool *) palloc0(key->partnatts * sizeof(bool));
	key->parttypalign = (char *) palloc0(key->partnatts * sizeof(char));
	key->parttypcoll = (Oid *) palloc0(key->partnatts * sizeof(Oid));
	MemoryContextSwitchTo(oldcxt);

	/* determine support function number to search for */
	procnum = (key->strategy == PARTITION_STRATEGY_HASH) ?
		HASHEXTENDED_PROC : BTORDER_PROC;

	/* Copy partattrs and fill other per-attribute info */
	memcpy(key->partattrs, attrs, key->partnatts * sizeof(int16));
	partexprs_item = list_head(key->partexprs);
	for (i = 0; i < key->partnatts; i++)
	{
		AttrNumber	attno = key->partattrs[i];
		HeapTuple	opclasstup;
		Form_pg_opclass opclassform;
		Oid			funcid;

		/* Collect opfamily information */
		opclasstup = SearchSysCache1(CLAOID,
									 ObjectIdGetDatum(opclass->values[i]));
		if (!HeapTupleIsValid(opclasstup))
			elog(ERROR, "cache lookup failed for opclass %u", opclass->values[i]);

		opclassform = (Form_pg_opclass) GETSTRUCT(opclasstup);
		key->partopfamily[i] = opclassform->opcfamily;
		key->partopcintype[i] = opclassform->opcintype;

		/* Get a support function for the specified opfamily and datatypes */
		funcid = get_opfamily_proc(opclassform->opcfamily,
								   opclassform->opcintype,
								   opclassform->opcintype,
								   procnum);
		if (!OidIsValid(funcid))
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
					 errmsg("operator class \"%s\" of access method %s is missing support function %d for type %s",
							NameStr(opclassform->opcname),
							(key->strategy == PARTITION_STRATEGY_HASH) ?
							"hash" : "btree",
							procnum,
							format_type_be(opclassform->opcintype))));

		fmgr_info_cxt(funcid, &key->partsupfunc[i], partkeycxt);

		/* Collation */
		key->partcollation[i] = collation->values[i];

		/* Collect type information */
		if (attno != 0)
		{
			Form_pg_attribute att = TupleDescAttr(relation->rd_att, attno - 1);

			key->parttypid[i] = att->atttypid;
			key->parttypmod[i] = att->atttypmod;
			key->parttypcoll[i] = att->attcollation;
		}
		else
		{
			if (partexprs_item == NULL)
				elog(ERROR, "wrong number of partition key expressions");

			key->parttypid[i] = exprType(lfirst(partexprs_item));
			key->parttypmod[i] = exprTypmod(lfirst(partexprs_item));
			key->parttypcoll[i] = exprCollation(lfirst(partexprs_item));

			partexprs_item = lnext(partexprs_item);
		}
		get_typlenbyvalalign(key->parttypid[i],
							 &key->parttyplen[i],
							 &key->parttypbyval[i],
							 &key->parttypalign[i]);

		ReleaseSysCache(opclasstup);
	}

	ReleaseSysCache(tuple);

	/* Assert that we're not leaking any old data during assignments below */
	Assert(relation->rd_partkeycxt == NULL);
	Assert(relation->rd_partkey == NULL);

	/*
	 * Success --- reparent our context and make the relcache point to the
	 * newly constructed key
	 */
	MemoryContextSetParent(partkeycxt, CacheMemoryContext);
	relation->rd_partkeycxt = partkeycxt;
	relation->rd_partkey = key;
}

/*
 * RelationGetPartitionDesc -- get partition descriptor, if relation is partitioned
 *
 * Note: we arrange for partition descriptors to not get freed until the
 * relcache entry's refcount goes to zero (see hacks in RelationClose,
 * RelationClearRelation, and RelationBuildPartitionDesc).  Therefore, even
 * though we hand back a direct pointer into the relcache entry, it's safe
 * for callers to continue to use that pointer as long as (a) they hold the
 * relation open, and (b) they hold a relation lock strong enough to ensure
 * that the data doesn't become stale.
 */
PartitionDesc
RelationGetPartitionDesc(Relation rel)
{
	if (rel->rd_rel->relkind != RELKIND_PARTITIONED_TABLE)
		return NULL;

	if (unlikely(rel->rd_partdesc == NULL))
		RelationBuildPartitionDesc(rel);

	return rel->rd_partdesc;
}

/*
 * RelationBuildPartitionDesc
 *		Form rel's partition descriptor
 *
 * Not flushed from the cache by RelationClearRelation() unless changed because
 * of addition or removal of partition.
 */
static void
RelationBuildPartitionDesc(Relation rel)
{
	List	   *inhoids,
			   *partoids;
	Oid		   *oids = NULL;
	List	   *boundspecs = NIL;
	ListCell   *cell;
	int			i,
				nparts;
	PartitionKey key = RelationGetPartitionKey(rel);
	PartitionDesc result;
	MemoryContext new_pdcxt;
	MemoryContext oldcxt;

	int			ndatums = 0;
	int			default_index = -1;

	/* Hash partitioning specific */
	PartitionHashBound **hbounds = NULL;

	/* List partitioning specific */
	PartitionListValue **all_values = NULL;
	int			null_index = -1;

	/* Range partitioning specific */
	PartitionRangeBound **rbounds = NULL;
#ifdef __OPENTENBASE__
	bool old_portable_output = false;
#endif

	/* Get partition oids from pg_inherits */
	inhoids = find_inheritance_children(RelationGetRelid(rel), NoLock);

	/* Collect bound spec nodes in a list */
	i = 0;
	partoids = NIL;
	foreach(cell, inhoids)
	{
		Oid			inhrelid = lfirst_oid(cell);
		HeapTuple	tuple;
		Datum		datum;
		bool		isnull;
		Node	   *boundspec;

		tuple = SearchSysCache1(RELOID, inhrelid);
		if (!HeapTupleIsValid(tuple))
			elog(ERROR, "cache lookup failed for relation %u", inhrelid);

		/*
		 * It is possible that the pg_class tuple of a partition has not been
		 * updated yet to set its relpartbound field.  The only case where
		 * this happens is when we open the parent relation to check using its
		 * partition descriptor that a new partition's bound does not overlap
		 * some existing partition.
		 */
		if (!((Form_pg_class) GETSTRUCT(tuple))->relispartition)
		{
			ReleaseSysCache(tuple);
			continue;
		}

		datum = SysCacheGetAttr(RELOID, tuple,
								Anum_pg_class_relpartbound,
								&isnull);
		Assert(!isnull);
#ifdef __OPENTENBASE__
		/*
		 * partition bound stored as string transformed without portable_output
		 * in catalog, so we need to read bound without portable_input.
		 */
		old_portable_output = set_portable_input(false);
#endif
		boundspec = (Node *) stringToNode(TextDatumGetCString(datum));

		/*
		 * Sanity check: If the PartitionBoundSpec says this is the default
		 * partition, its OID should correspond to whatever's stored in
		 * pg_partitioned_table.partdefid; if not, the catalog is corrupt.
		 */
		if (castNode(PartitionBoundSpec, boundspec)->is_default)
		{
			Oid			partdefid;

			partdefid = get_default_partition_oid(RelationGetRelid(rel));
			if (partdefid != inhrelid)
				elog(ERROR, "expected partdefid %u, but got %u",
					 inhrelid, partdefid);
		}
#ifdef __OPENTENBASE__
		set_portable_input(old_portable_output);
#endif
		boundspecs = lappend(boundspecs, boundspec);
		partoids = lappend_oid(partoids, inhrelid);
		ReleaseSysCache(tuple);
	}

	nparts = list_length(partoids);

	if (nparts > 0)
	{
		oids = (Oid *) palloc(nparts * sizeof(Oid));
		i = 0;
		foreach(cell, partoids)
			oids[i++] = lfirst_oid(cell);

		/* Convert from node to the internal representation */
		if (key->strategy == PARTITION_STRATEGY_HASH)
		{
			ndatums = nparts;
			hbounds = (PartitionHashBound **)
				palloc(nparts * sizeof(PartitionHashBound *));

			i = 0;
			foreach(cell, boundspecs)
			{
				PartitionBoundSpec *spec = castNode(PartitionBoundSpec,
													lfirst(cell));

				if (spec->strategy != PARTITION_STRATEGY_HASH)
					elog(ERROR, "invalid strategy in partition bound spec");

				hbounds[i] = (PartitionHashBound *)
					palloc(sizeof(PartitionHashBound));

				hbounds[i]->modulus = spec->modulus;
				hbounds[i]->remainder = spec->remainder;
				hbounds[i]->index = i;
				i++;
			}

			/* Sort all the bounds in ascending order */
			qsort(hbounds, nparts, sizeof(PartitionHashBound *),
				  qsort_partition_hbound_cmp);
		}
		else if (key->strategy == PARTITION_STRATEGY_LIST)
		{
			List	   *non_null_values = NIL;

			/*
			 * Create a unified list of non-null values across all partitions.
			 */
			i = 0;
			null_index = -1;
			foreach(cell, boundspecs)
			{
				PartitionBoundSpec *spec = castNode(PartitionBoundSpec,
													lfirst(cell));
				ListCell   *c;

				if (spec->strategy != PARTITION_STRATEGY_LIST)
					elog(ERROR, "invalid strategy in partition bound spec");

				/*
				 * Note the index of the partition bound spec for the default
				 * partition. There's no datum to add to the list of non-null
				 * datums for this partition.
				 */
				if (spec->is_default)
				{
					default_index = i;
					i++;
					continue;
				}

				foreach(c, spec->listdatums)
				{
					Const	   *val = castNode(Const, lfirst(c));
					PartitionListValue *list_value = NULL;

					if (!val->constisnull)
					{
						list_value = (PartitionListValue *)
							palloc0(sizeof(PartitionListValue));
						list_value->index = i;
						list_value->value = val->constvalue;
					}
					else
					{
						/*
						 * Never put a null into the values array, flag
						 * instead for the code further down below where we
						 * construct the actual relcache struct.
						 */
						if (null_index != -1)
							elog(ERROR, "found null more than once");
						null_index = i;
					}

					if (list_value)
						non_null_values = lappend(non_null_values,
												  list_value);
				}

				i++;
			}

			ndatums = list_length(non_null_values);

			/*
			 * Collect all list values in one array. Alongside the value, we
			 * also save the index of partition the value comes from.
			 */
			all_values = (PartitionListValue **) palloc(ndatums *
														sizeof(PartitionListValue *));
			i = 0;
			foreach(cell, non_null_values)
			{
				PartitionListValue *src = lfirst(cell);

				all_values[i] = (PartitionListValue *)
					palloc(sizeof(PartitionListValue));
				all_values[i]->value = src->value;
				all_values[i]->index = src->index;
				i++;
			}

			qsort_arg(all_values, ndatums, sizeof(PartitionListValue *),
					  qsort_partition_list_value_cmp, (void *) key);
		}
		else if (key->strategy == PARTITION_STRATEGY_RANGE)
		{
			int			k;
			PartitionRangeBound **all_bounds,
					   *prev;

			all_bounds = (PartitionRangeBound **) palloc0(2 * nparts *
														  sizeof(PartitionRangeBound *));

			/*
			 * Create a unified list of range bounds across all the
			 * partitions.
			 */
			i = ndatums = 0;
			foreach(cell, boundspecs)
			{
				PartitionBoundSpec *spec = castNode(PartitionBoundSpec,
													lfirst(cell));
				PartitionRangeBound *lower,
						   *upper;

				if (spec->strategy != PARTITION_STRATEGY_RANGE)
					elog(ERROR, "invalid strategy in partition bound spec");

				/*
				 * Note the index of the partition bound spec for the default
				 * partition. There's no datum to add to the allbounds array
				 * for this partition.
				 */
				if (spec->is_default)
				{
					default_index = i++;
					continue;
				}

				lower = make_one_partition_rbound(key, i, spec->lowerdatums,
												  true);
				upper = make_one_partition_rbound(key, i, spec->upperdatums,
												  false);
				all_bounds[ndatums++] = lower;
				all_bounds[ndatums++] = upper;
				i++;
			}

			Assert(ndatums == nparts * 2 ||
				   (default_index != -1 && ndatums == (nparts - 1) * 2));

			/* Sort all the bounds in ascending order */
			qsort_arg(all_bounds, ndatums,
					  sizeof(PartitionRangeBound *),
					  qsort_partition_rbound_cmp,
					  (void *) key);

			/* Save distinct bounds from all_bounds into rbounds. */
			rbounds = (PartitionRangeBound **)
				palloc(ndatums * sizeof(PartitionRangeBound *));
			k = 0;
			prev = NULL;
			for (i = 0; i < ndatums; i++)
			{
				PartitionRangeBound *cur = all_bounds[i];
				bool		is_distinct = false;
				int			j;

				/* Is the current bound distinct from the previous one? */
				for (j = 0; j < key->partnatts; j++)
				{
					Datum		cmpval;

					if (prev == NULL || cur->kind[j] != prev->kind[j])
					{
						is_distinct = true;
						break;
					}

					/*
					 * If the bounds are both MINVALUE or MAXVALUE, stop now
					 * and treat them as equal, since any values after this
					 * point must be ignored.
					 */
					if (cur->kind[j] != PARTITION_RANGE_DATUM_VALUE)
						break;

					cmpval = FunctionCall2Coll(&key->partsupfunc[j],
											   key->partcollation[j],
											   cur->datums[j],
											   prev->datums[j]);
					if (DatumGetInt32(cmpval) != 0)
					{
						is_distinct = true;
						break;
					}
				}

				/*
				 * Only if the bound is distinct save it into a temporary
				 * array i.e. rbounds which is later copied into boundinfo
				 * datums array.
				 */
				if (is_distinct)
					rbounds[k++] = all_bounds[i];

				prev = cur;
			}

			/* Update ndatums to hold the count of distinct datums. */
			ndatums = k;
		}
		else
			elog(ERROR, "unexpected partition strategy: %d",
				 (int) key->strategy);
	}

	/* Now build the actual relcache partition descriptor */
	new_pdcxt = AllocSetContextCreate(CurTransactionContext,
										  "partition descriptor",
										  ALLOCSET_DEFAULT_SIZES);
	MemoryContextCopyAndSetIdentifier(new_pdcxt, RelationGetRelationName(rel));

	oldcxt = MemoryContextSwitchTo(new_pdcxt);

	result = (PartitionDescData *) palloc0(sizeof(PartitionDescData));
	result->nparts = nparts;
	if (nparts > 0)
	{
		PartitionBoundInfo boundinfo;
		int		   *mapping;
		int			next_index = 0;

		result->oids = (Oid *) palloc0(nparts * sizeof(Oid));

		boundinfo = (PartitionBoundInfoData *)
			palloc0(sizeof(PartitionBoundInfoData));
		boundinfo->strategy = key->strategy;
		boundinfo->default_index = -1;
		boundinfo->ndatums = ndatums;
		boundinfo->null_index = -1;
		boundinfo->datums = (Datum **) palloc0(ndatums * sizeof(Datum *));

		/* Initialize mapping array with invalid values */
		mapping = (int *) palloc(sizeof(int) * nparts);
		for (i = 0; i < nparts; i++)
			mapping[i] = -1;

		switch (key->strategy)
		{
			case PARTITION_STRATEGY_HASH:
				{
					/* Modulus are stored in ascending order */
					int			greatest_modulus = hbounds[ndatums - 1]->modulus;

					boundinfo->nindexes = greatest_modulus;
					boundinfo->indexes = (int *) palloc(greatest_modulus *
														sizeof(int));

					for (i = 0; i < greatest_modulus; i++)
						boundinfo->indexes[i] = -1;

					for (i = 0; i < nparts; i++)
					{
						int			modulus = hbounds[i]->modulus;
						int			remainder = hbounds[i]->remainder;

						boundinfo->datums[i] = (Datum *) palloc(2 *
																sizeof(Datum));
						boundinfo->datums[i][0] = Int32GetDatum(modulus);
						boundinfo->datums[i][1] = Int32GetDatum(remainder);

						while (remainder < greatest_modulus)
						{
							/* overlap? */
							Assert(boundinfo->indexes[remainder] == -1);
							boundinfo->indexes[remainder] = i;
							remainder += modulus;
						}

						mapping[hbounds[i]->index] = i;
						pfree(hbounds[i]);
					}
					pfree(hbounds);
					break;
				}

			case PARTITION_STRATEGY_LIST:
				{
					boundinfo->nindexes = ndatums;
					boundinfo->indexes = (int *) palloc(ndatums * sizeof(int));

					/*
					 * Copy values.  Indexes of individual values are mapped
					 * to canonical values so that they match for any two list
					 * partitioned tables with same number of partitions and
					 * same lists per partition.  One way to canonicalize is
					 * to assign the index in all_values[] of the smallest
					 * value of each partition, as the index of all of the
					 * partition's values.
					 */
					for (i = 0; i < ndatums; i++)
					{
						boundinfo->datums[i] = (Datum *) palloc(sizeof(Datum));
						boundinfo->datums[i][0] = datumCopy(all_values[i]->value,
															key->parttypbyval[0],
															key->parttyplen[0]);

						/* If the old index has no mapping, assign one */
						if (mapping[all_values[i]->index] == -1)
							mapping[all_values[i]->index] = next_index++;

						boundinfo->indexes[i] = mapping[all_values[i]->index];
					}

					/*
					 * If null-accepting partition has no mapped index yet,
					 * assign one.  This could happen if such partition
					 * accepts only null and hence not covered in the above
					 * loop which only handled non-null values.
					 */
					if (null_index != -1)
					{
						Assert(null_index >= 0);
						if (mapping[null_index] == -1)
							mapping[null_index] = next_index++;
						boundinfo->null_index = mapping[null_index];
					}

					/* Assign mapped index for the default partition. */
					if (default_index != -1)
					{
						/*
						 * The default partition accepts any value not
						 * specified in the lists of other partitions, hence
						 * it should not get mapped index while assigning
						 * those for non-null datums.
						 */
						Assert(default_index >= 0 &&
							   mapping[default_index] == -1);
						mapping[default_index] = next_index++;
						boundinfo->default_index = mapping[default_index];
					}

					/* All partition must now have a valid mapping */
					Assert(next_index == nparts);
					break;
				}

			case PARTITION_STRATEGY_RANGE:
				{
					boundinfo->kind = (PartitionRangeDatumKind **)
						palloc(ndatums *
							   sizeof(PartitionRangeDatumKind *));
					boundinfo->nindexes = ndatums + 1;
					boundinfo->indexes = (int *) palloc((ndatums + 1) *
														sizeof(int));

					for (i = 0; i < ndatums; i++)
					{
						int			j;

						boundinfo->datums[i] = (Datum *) palloc(key->partnatts *
																sizeof(Datum));
						boundinfo->kind[i] = (PartitionRangeDatumKind *)
							palloc(key->partnatts *
								   sizeof(PartitionRangeDatumKind));
						for (j = 0; j < key->partnatts; j++)
						{
							if (rbounds[i]->kind[j] == PARTITION_RANGE_DATUM_VALUE)
								boundinfo->datums[i][j] =
									datumCopy(rbounds[i]->datums[j],
											  key->parttypbyval[j],
											  key->parttyplen[j]);
							boundinfo->kind[i][j] = rbounds[i]->kind[j];
						}

						/*
						 * There is no mapping for invalid indexes.
						 *
						 * Any lower bounds in the rbounds array have invalid
						 * indexes assigned, because the values between the
						 * previous bound (if there is one) and this (lower)
						 * bound are not part of the range of any existing
						 * partition.
						 */
						if (rbounds[i]->lower)
							boundinfo->indexes[i] = -1;
						else
						{
							int			orig_index = rbounds[i]->index;

							/* If the old index has no mapping, assign one */
							if (mapping[orig_index] == -1)
								mapping[orig_index] = next_index++;

							boundinfo->indexes[i] = mapping[orig_index];
						}
					}

					/* Assign mapped index for the default partition. */
					if (default_index != -1)
					{
						Assert(default_index >= 0 && mapping[default_index] == -1);
						mapping[default_index] = next_index++;
						boundinfo->default_index = mapping[default_index];
					}
					boundinfo->indexes[i] = -1;
					break;
				}

			default:
				elog(ERROR, "unexpected partition strategy: %d",
					 (int) key->strategy);
		}

		result->boundinfo = boundinfo;

		/*
		 * Now assign OIDs from the original array into mapped indexes of the
		 * result array.  Order of OIDs in the former is defined by the
		 * catalog scan that retrieved them, whereas that in the latter is
		 * defined by canonicalized representation of the partition bounds.
		 */
		for (i = 0; i < nparts; i++)
			result->oids[mapping[i]] = oids[i];
		pfree(mapping);
	}

	MemoryContextSwitchTo(oldcxt);
	
	/*
	 * We have a fully valid partdesc ready to store into the relcache.
	 * Reparent it so it has the right lifespan.
	 */
	MemoryContextSetParent(new_pdcxt, CacheMemoryContext);

	/*
	 * But first, a kluge: if there's an old rd_pdcxt, it contains an old
	 * partition descriptor that may still be referenced somewhere.  Preserve
	 * it, while not leaking it, by reattaching it as a child context of the
	 * new rd_pdcxt.  Eventually it will get dropped by either RelationClose
	 * or RelationClearRelation.
	 */
	if (rel->rd_pdcxt != NULL)
		MemoryContextSetParent(rel->rd_pdcxt, new_pdcxt);
	rel->rd_pdcxt = new_pdcxt;
	rel->rd_partdesc = result;
}

/*
 * RelationGetPartitionQual
 *
 * Returns a list of partition quals
 */
List *
RelationGetPartitionQual(Relation rel)
{
	/* Quick exit */
	if (!rel->rd_rel->relispartition)
		return NIL;

	return generate_partition_qual(rel);
}

/*
 * get_partition_qual_relid
 *
 * Returns an expression tree describing the passed-in relation's partition
 * constraint.
 *
 * If the relation is not found, or is not a partition, or there is no
 * partition constraint, return NULL.  We must guard against the first two
 * cases because this supports a SQL function that could be passed any OID.
 * The last case can happen even if relispartition is true, when a default
 * partition is the only partition.
 */
Expr *
get_partition_qual_relid(Oid relid)
{
	Expr	   *result = NULL;

	/* Do the work only if this relation exists and is a partition. */
	if (get_rel_relispartition(relid))
	{
		Relation	rel = relation_open(relid, AccessShareLock);
		List	   *and_args;

		and_args = generate_partition_qual(rel);

		/* Convert implicit-AND list format to boolean expression */
		if (and_args == NIL)
			result = NULL;
		else if (list_length(and_args) > 1)
			result = makeBoolExpr(AND_EXPR, and_args, -1);
		else
			result = linitial(and_args);

		/* Keep the lock, to allow safe deparsing against the rel by caller. */
		relation_close(rel, NoLock);
	}

	return result;
}

/*
 * generate_partition_qual
 *
 * Generate partition predicate from rel's partition bound expression. The
 * function returns a NIL list if there is no predicate.
 *
 * Result expression tree is stored CacheMemoryContext to ensure it survives
 * as long as the relcache entry. But we should be running in a less long-lived
 * working context. To avoid leaking cache memory if this routine fails partway
 * through, we build in working memory and then copy the completed structure
 * into cache memory.
 */
static List *
generate_partition_qual(Relation rel)
{
	HeapTuple	tuple;
	MemoryContext oldcxt;
	Datum		boundDatum;
	bool		isnull;
	List	   *my_qual = NIL,
			   *result = NIL;
	Relation	parent;
	bool		found_whole_row;

	/* Guard against stack overflow due to overly deep partition tree */
	check_stack_depth();

	/* Quick copy */
	if (rel->rd_partcheck != NIL)
		return copyObject(rel->rd_partcheck);

	/* Grab at least an AccessShareLock on the parent table */
	parent = relation_open(get_partition_parent(RelationGetRelid(rel)),
					   AccessShareLock);

	/* Get pg_class.relpartbound */
	tuple = SearchSysCache1(RELOID, RelationGetRelid(rel));
	if (!HeapTupleIsValid(tuple))
		elog(ERROR, "cache lookup failed for relation %u",
			 RelationGetRelid(rel));

	boundDatum = SysCacheGetAttr(RELOID, tuple,
								 Anum_pg_class_relpartbound,
								 &isnull);
	if (!isnull)
	{
		PartitionBoundSpec *bound;

		bound = castNode(PartitionBoundSpec,
						 stringToNode(TextDatumGetCString(boundDatum)));

		my_qual = get_qual_from_partbound(rel, parent, bound);
	}

	ReleaseSysCache(tuple);

	/* Add the parent's quals to the list (if any) */
	if (parent->rd_rel->relispartition)
		result = list_concat(generate_partition_qual(parent), my_qual);
	else
		result = my_qual;

	/*
	 * Change Vars to have partition's attnos instead of the parent's. We do
	 * this after we concatenate the parent's quals, because we want every Var
	 * in it to bear this relation's attnos. It's safe to assume varno = 1
	 * here.
	 */
	result = map_partition_varattnos(result, 1, rel, parent,
									 &found_whole_row);
	/* There can never be a whole-row reference here */
	if (found_whole_row)
		elog(ERROR, "unexpected whole-row reference found in partition key");

	/* Save a copy in the relcache */
	oldcxt = MemoryContextSwitchTo(CacheMemoryContext);
	rel->rd_partcheck = copyObject(result);
	MemoryContextSwitchTo(oldcxt);

	/* Keep the parent locked until commit */
	relation_close(parent, NoLock);

	return result;
}

/*
 * qsort_partition_hbound_cmp
 *
 * We sort hash bounds by modulus, then by remainder.
 */
static int32
qsort_partition_hbound_cmp(const void *a, const void *b)
{
	PartitionHashBound *h1 = (*(PartitionHashBound *const *) a);
	PartitionHashBound *h2 = (*(PartitionHashBound *const *) b);

	return partition_hbound_cmp(h1->modulus, h1->remainder,
								h2->modulus, h2->remainder);
}

/*
 * qsort_partition_list_value_cmp
 *
 * Compare two list partition bound datums
 */
static int32
qsort_partition_list_value_cmp(const void *a, const void *b, void *arg)
{
	Datum		val1 = (*(const PartitionListValue **) a)->value,
				val2 = (*(const PartitionListValue **) b)->value;
	PartitionKey key = (PartitionKey) arg;

	return DatumGetInt32(FunctionCall2Coll(&key->partsupfunc[0],
										   key->partcollation[0],
										   val1, val2));
}

/* Used when sorting range bounds across all range partitions */
static int32
qsort_partition_rbound_cmp(const void *a, const void *b, void *arg)
{
	PartitionRangeBound *b1 = (*(PartitionRangeBound *const *) a);
	PartitionRangeBound *b2 = (*(PartitionRangeBound *const *) b);
	PartitionKey key = (PartitionKey) arg;

	return partition_rbound_cmp(key->partnatts, key->partsupfunc,
								key->partcollation, b1->datums, b1->kind,
								b1->lower, b2);
}


Datum get_relation_bound_datum(Oid relid)
{
	HeapTuple	tuple;
	Datum		datum;
	bool		isnull;

	tuple = SearchSysCache1(RELOID, relid);
	if (!HeapTupleIsValid(tuple))
	{
		elog(ERROR, "failed to lookup cache for relation: %u", relid);
	}

	datum = SysCacheGetAttr(RELOID, tuple, Anum_pg_class_relpartbound,
							&isnull);
	ReleaseSysCache(tuple);
	return datum;
}

#ifdef _PG_ORCL_
PartitionBoundSpec *
get_relation_bound(Oid relid)
{
	HeapTuple	tuple;
	Datum	datum;
	bool	isnull;
	PartitionBoundSpec	*boundspec;

	tuple = SearchSysCache1(RELOID, relid);
	if (!HeapTupleIsValid(tuple))
	{
		elog(ERROR, "failed to lookup cache for relation: %u", relid);
	}

	datum = SysCacheGetAttr(RELOID, tuple, Anum_pg_class_relpartbound,
							&isnull);
	Assert(!isnull);
	boundspec = (PartitionBoundSpec *) stringToNode(TextDatumGetCString(datum));

	ReleaseSysCache(tuple);

	return boundspec;
}

List *
get_partitions_bound(List *relids, bool only_one_def, List **copied_relids)
{
	ListCell	*l;
	List		*bounds = NIL;
	bool	has_def = false;

	if (copied_relids)
		*copied_relids = NIL;

	foreach(l, relids)
	{
		Oid	relid = lfirst_oid(l);
		PartitionBoundSpec	*boundspec;

		boundspec = get_relation_bound(relid);
		if (boundspec->is_default)
		{
			if (only_one_def && !has_def)
				has_def = true;
			else
				continue;
		}

		bounds = lappend(bounds, boundspec);
		if (copied_relids)
			*copied_relids = lappend_oid(*copied_relids, relid);
	}

	return bounds;
}

/*
 * Remove duplicate values for LIST partition.
 */
static List *
remove_duplicate_values(PartitionKey key, List *values)
{
	List	*new_list = NIL;
	Node	*null_node = NULL;
	ListCell	*l;

	foreach(l, values)
	{
		ListCell	*lc;
		Const		*value = castNode(Const, lfirst(l));

		if (value->constisnull)
		{
			if (null_node == NULL)
				null_node = (Node *) value;
			continue;
		}

		foreach(lc, new_list)
		{
			Const	*node = castNode(Const, lfirst(lc));

			Assert(!node->constisnull);

			if (datumIsEqual(node->constvalue, value->constvalue,
								key->parttypbyval[0], key->parttyplen[0]))
				break;
		}

		if (lc == NULL)
			new_list = lappend(new_list, value);
	}

	if (null_node != NULL)
		new_list = lappend(new_list, null_node);

	return new_list;
}

static bool
partition_bound_isoverlapped(PartitionBoundSpec *b, PartitionBoundSpec *cur,
								PartitionKey key)
{
	bool	overlap = false;

	if (key->strategy == PARTITION_STRATEGY_LIST)
	{
		ListCell	*c;
		Const	*tmp = NULL;
		List	*non_null_values = NIL;
		List	*b_list;
		List	*cur_list;
		List	*c_values = NIL;
		int		n_nulls = 0;

		b_list = remove_duplicate_values(key, b->listdatums);
		cur_list = remove_duplicate_values(key, cur->listdatums);

		c_values = list_concat(b_list, cur_list);
		foreach(c, c_values)
		{
			Const	   *val = castNode(Const, lfirst(c));
			PartitionListValue *list_value = NULL;

			/* Assume all list values has same types, typmod... */
			if (tmp == NULL)
				tmp = val;

			if (!val->constisnull)
			{
				list_value = (PartitionListValue *)
									palloc0(sizeof(PartitionListValue));
				list_value->index = -1; /* regardless */
				list_value->value = val->constvalue;
			}
			else
			{
				n_nulls++;
				continue;
			}

			if (list_value)
				non_null_values = lappend(non_null_values, list_value);
		}

		if (n_nulls > 1)
			overlap = true;
		else
		{
			/*
			 * Check if there are duplicate datums in list. If found, make them
			 * be merged bounds.
			 */
			PartitionListValue	**all_values;
			PartitionListValue	*last_val = NULL;
			ListCell	*cell;
			int		i = 0;
			int		ndatums;

			ndatums = list_length(non_null_values);
			all_values
				= (PartitionListValue **) palloc(ndatums *
												sizeof(PartitionListValue *));
			foreach(cell, non_null_values)
			{
				PartitionListValue *src = lfirst(cell);

				all_values[i] = (PartitionListValue *)
									palloc(sizeof(PartitionListValue));
				all_values[i]->value = src->value;
				all_values[i]->index = src->index;
				i++;
			}

			qsort_arg(all_values, ndatums, sizeof(PartitionListValue *),
						qsort_partition_list_value_cmp, (void *) key);

			/* Check if has duplicated values */
			for (i = 0; i < ndatums; i++)
			{
				if (last_val == NULL)
				{
					last_val = all_values[i];
					continue;
				}

				if (datumIsEqual(all_values[i]->value, last_val->value,
									key->parttypbyval[0], key->parttyplen[0]))
				{
					overlap = true;
					break;
				}

				last_val = all_values[i];
			}
		}
	}
	else if (key->strategy == PARTITION_STRATEGY_RANGE)
	{
		PartitionRangeBound *cur_lower;
		PartitionRangeBound	*cur_upper;
		PartitionRangeBound	*b_lower;
		PartitionRangeBound	*b_upper;

		cur_lower = make_one_partition_rbound(key, -1, cur->lowerdatums, true);
		cur_upper = make_one_partition_rbound(key, -1, cur->upperdatums, false);

		b_lower = make_one_partition_rbound(key, -1, b->lowerdatums, true);
		b_upper = make_one_partition_rbound(key, -1, b->upperdatums, false);

		if (partition_rbound_cmp(key->partnatts, key->partsupfunc,
								 key->partcollation, b_upper->datums,
								 b_upper->kind, false, cur_lower) > 0)
		{
			if (partition_rbound_cmp(key->partnatts, key->partsupfunc,
								 key->partcollation, b_lower->datums,
								 b_lower->kind, true, cur_upper) < 0)
				overlap = true;

		}
	}
	else
		elog(ERROR, "unexpected supported merge partition strategy: %d",
									(int) key->strategy);

	return overlap;
}

/*
 * get_overlapped_bounds
 *    Loop over bounds to return the overlapped bounds with 'b'.
 */
static List *
get_overlapped_bounds(List *bounds, List **d_list, PartitionBoundSpec *b,
						PartitionKey key)
{
	ListCell	*l;
	ListCell	*prev = NULL;

	*d_list = NIL;

	if (list_length(bounds) == 0)
		return bounds;

	l = list_head(bounds);
	while (l != NULL)
	{
		PartitionBoundSpec	*cur = lfirst(l);
		bool	overlap = false;

		/* Default partition already trimed by the caller */
		Assert(!cur->is_default);
		overlap = partition_bound_isoverlapped(b, cur, key);
		if (overlap)
		{
			*d_list = lappend(*d_list, cur);
			bounds = list_delete_cell(bounds, l, prev);

			if (prev == NULL)
				l = list_head(bounds);
			else
				l = lnext(prev);
		}
		else
		{
			prev = l;
			l = lnext(l);
		}
	}

	return bounds;
}

/*
 * get_overlap_partition_bound
 *    Merge those overlapping bound into one. Assume partition key are same
 *  among those partitions.
 */
List *
get_overlap_partition_bound(List *bounds, PartitionKey key)
{
	ListCell	*l;
	ListCell	*prev = NULL;
	List	*new_bounds = NIL;
	PartitionBoundSpec	*def_boundspec = NULL;

	/* First loop the bounds check and remove the default partitons */
	l = list_head(bounds);
	while (l != NULL)
	{
		PartitionBoundSpec	*b;

		b = (PartitionBoundSpec *) lfirst(l);
		if (b->is_default)
		{
			/*
			 * Should not happen, get_partitions_bound() has already picked up
			 * only one default partition. But still check and do that maybe
			 * bounds is not from get_partitions_bound() in some days.
			 */
			if (def_boundspec != NULL)
				elog(ERROR, "multiple default partition to create for merge/split partition");

			def_boundspec = b;
			bounds = list_delete_cell(bounds, l, prev);
			if (prev == NULL)
				l = list_head(bounds);
			else
				l = lnext(prev);
		}
		else
		{
			prev = l;
			l = lnext(l);
		}
	}

	l = list_head(bounds);
	while (l != NULL)
	{
		PartitionBoundSpec	*b;
		List	*subbounds = NULL;
		ListCell	*ll;

		b = (PartitionBoundSpec *) lfirst(l);
		subbounds = lappend(subbounds, b);

		/* Delete from bounds */
		bounds = list_delete_cell(bounds, l, NULL);
		ll = list_head(subbounds);
		while (true)
		{
			List	*d_list = NIL;

			if (list_length(bounds) == 0 || ll == NULL)
				break;

			b = (PartitionBoundSpec *) lfirst(ll);
			bounds = get_overlapped_bounds(bounds, &d_list, b, key);
			subbounds = list_concat(subbounds, d_list);

			ll = lnext(ll);
		}

		new_bounds = lappend(new_bounds, subbounds);
		l = list_head(bounds);
	}

	/* Only one default for merged partition */
	if (def_boundspec)
		new_bounds = lappend(new_bounds, list_make1(def_boundspec));

	return new_bounds;
}

/*
 * merge_partition_bound
 *   Merge 'bounds' into one PartitionBoundSpec. Also check if the bounds is
 *  adjacent and is the same partition specification.
 */
PartitionBoundSpec *
merge_partition_bound(List *bounds, PartitionKey key)
{
	int		nparts = list_length(bounds);
	PartitionBoundSpec	*nbound;

	Assert(key != NULL);

	nbound = makeNode(PartitionBoundSpec);
	nbound->is_default = false;
	nbound->strategy = key->strategy;
	nbound->location = -1;
	if (key->strategy == PARTITION_STRATEGY_LIST)
	{
		ListCell	*cell;

		/*
		 * Create a unified list of non-null values across all partitions.
		 */
		foreach(cell, bounds)
		{
			PartitionBoundSpec *spec = castNode(PartitionBoundSpec, lfirst(cell));

			if (spec->strategy != PARTITION_STRATEGY_LIST)
				elog(ERROR, "invalid strategy in partition bound spec");

			if (spec->is_default)
			{
				/* Merge into default */
				return spec;
			}

			/*
			 * Merge all lists togather, and there may be some duplicated values
			 * in list (from subpartition of merged partition), create table
			 * logic can handle this case - remove duplicated values. Therefore,
			 * here is no any extra-efforts.
			 */
			nbound->listdatums = list_concat(nbound->listdatums, spec->listdatums);
		}

		nbound->listdatums = remove_duplicate_values(key, nbound->listdatums);
		return nbound;
	}
	else if (key->strategy == PARTITION_STRATEGY_RANGE)
	{
		int		i = 0;
		int		ndatums = 0;
		ListCell	*cell;
		List		*overlap_part = NIL;
		PartitionRangeBound **all_bounds,
							*prev_bound = NULL;
		PartitionBoundSpec	*tmp;

		all_bounds
			= (PartitionRangeBound **) palloc0(2 * nparts *
												sizeof(PartitionRangeBound *));
		foreach(cell, bounds)
		{
			PartitionBoundSpec *spec = castNode(PartitionBoundSpec, lfirst(cell));
			PartitionRangeBound	*lower,
								*upper;

			if (spec->strategy != PARTITION_STRATEGY_RANGE)
				elog(ERROR, "invalid strategy in partition bound spec");

			/* Default partiton as the final PartitionBoundSpec */
			if (spec->is_default)
				return spec;

			lower = make_one_partition_rbound(key, i, spec->lowerdatums, true);
			upper = make_one_partition_rbound(key, i, spec->upperdatums, false);
			all_bounds[ndatums++] = lower;
			all_bounds[ndatums++] = upper;
			i++;
		}

		Assert(ndatums == nparts * 2);

		/* Sort all the bounds in ascending order */
		qsort_arg(all_bounds, ndatums, sizeof(PartitionRangeBound *),
						qsort_partition_rbound_cmp, (void *) key);

		prev_bound = NULL;
		/* Check if the bounds are adjacent and overlap */
		for (i = 0; i < ndatums; i++)
		{
			PartitionRangeBound	*cur = all_bounds[i];

			if (i == 0)
			{
				/* append the first partition */
				overlap_part = lappend_int(overlap_part, cur->index);
				continue;
			}

			if (list_length(overlap_part) == 0)
			{
				Assert(cur->lower && prev_bound != NULL);

				/*
				 * Check if current bound is adjacent with the latest upper bound.
				 * If so, we continue out loop.
				 */
				if (partition_rbound_cmp(key->partnatts, key->partsupfunc,
										key->partcollation, prev_bound->datums,
										prev_bound->kind, true, cur) == 0)
				{
					/*
					 * Current bound is adjacent with the lastest removed upper,
					 * consider a continuious range.
					 */
					overlap_part = lappend_int(overlap_part, cur->index);
					continue;
				}
				break;
			}

			if (cur->lower)
				overlap_part = lappend_int(overlap_part, cur->index);
			else
			{
				/* current range closed, remove it from list */
				ListCell	*l;
				ListCell	*prev = NULL;

				foreach(l, overlap_part)
				{
					int	c = lfirst_int(l);

					if (c == cur->index)
						break;
					prev = l;
				}

				overlap_part = list_delete_cell(overlap_part, l, prev);

				/* Save the latest upper bound */
				prev_bound = cur;
			}
		}

		if (i != ndatums)
			elog(ERROR, "bounds of merged partitions are not adjacent");

		/*
		 * Make a new bound using the low of the 1st bound and the high of the
		 * last bound
		 */
		tmp = list_nth(bounds, all_bounds[0]->index);
		nbound->lowerdatums = tmp->lowerdatums;
		tmp = list_nth(bounds, all_bounds[ndatums - 1]->index);
		nbound->upperdatums = tmp->upperdatums;

		return nbound;
	}
	else
		elog(ERROR, "unexpected supported merge partition strategy: %d",
								(int) key->strategy);
}

static Oid
get_partrelid_from_partbound(Relation parent, PartitionBoundSpec *check_vals)
{
	PartitionKey	key = RelationGetPartitionKey(parent);
	ListCell	*lc;
	Oid		defRelid = InvalidOid;
	List	*inhreids;

	if (key == NULL)
		elog(ERROR, "could not get partition relation from non-partition table");

	if (check_vals->strategy != key->strategy)
		elog(ERROR, "could not get split/merge partition relation from other "
						"partition specification");

	inhreids = find_inheritance_children(RelationGetRelid(parent),
										AccessExclusiveLock);
	foreach(lc, inhreids)
	{
		Oid	relid = lfirst_oid(lc);
		PartitionBoundSpec	*bound;

		/*
		 * First ignore default partition, if not found among non-default
		 * partition relations, return that default partition.
		 */
		bound = get_relation_bound(relid);
		if (bound->is_default)
		{
			defRelid = relid;
			continue;
		}

		if (partition_bound_isoverlapped(check_vals, bound, key))
			return relid;
	}

	return defRelid;
}

/*
 * Get a qualified relation Oid from input boundspc.
 */
Oid
get_relid_from_partbound(Relation parent, List *bounds, LOCKMODE mode,
								bool ispartition)
{
	PartitionBoundSpec	*check_vals;
	Relation	partrel;
	Oid		partrelid;

	check_vals = linitial(bounds);

	partrelid = get_partrelid_from_partbound(parent, check_vals);
	if (!OidIsValid(partrelid) || list_length(bounds) == 1)
		return partrelid;

	/* Qualify the 2nd level */
	if (ispartition)
		elog(ERROR, "can not get subpartition for PARTITION");

	check_vals = lsecond(bounds);
	partrel = heap_open(partrelid, AccessExclusiveLock);
	partrelid = get_partrelid_from_partbound(partrel, check_vals);
	heap_close(partrel, NoLock);

	return partrelid;
}

/*
 * Get the max bound of current level of `partrel'.
 */
static PartitionBoundSpec *
get_partitions_maxbound(PartitionKey key, Oid partrel)
{
	Oid		parentId;
	List	*rels = NIL;
	ListCell	*l;
	List		*bounds;
	PartitionRangeBound **all_bounds;
	int		nbound = 0;
	int		i = 0;

	parentId = get_partition_parent(partrel);
	Assert(parentId != InvalidOid);
	rels = find_inheritance_children(parentId, AccessExclusiveLock);
	Assert(list_length(rels) > 0);

	/* Don't pick up a default partition */
	bounds = get_partitions_bound(rels, false, NULL);
	nbound = list_length(bounds) * 2;
	Assert(nbound > 0);
	all_bounds = palloc(sizeof(PartitionRangeBound *) * nbound);

	foreach(l, bounds)
	{
		PartitionBoundSpec	*spec = castNode(PartitionBoundSpec, lfirst(l));
		PartitionRangeBound	*lower,
							*upper;

		if (spec->strategy != PARTITION_STRATEGY_RANGE)
			elog(ERROR, "only range partition may have a maxbound value");

		Assert(!spec->is_default);

		/* Index as index of `bounds` list */
		lower = make_one_partition_rbound(key, i / 2, spec->lowerdatums, true);
		upper = make_one_partition_rbound(key, i / 2, spec->upperdatums, false);
		all_bounds[i++] = lower;
		all_bounds[i++] = upper;
	}

	qsort_arg(all_bounds, nbound, sizeof(PartitionRangeBound *),
						qsort_partition_rbound_cmp, (void *) key);

	/* The last element is the max bound */
	Assert(list_length(bounds) > all_bounds[nbound - 1]->index);
	return list_nth(bounds, all_bounds[nbound - 1]->index);
}

/*
 * split_partition_bound
 *   partrel: a partiton should be split
 *   split_bound: a cut off point or list value to split 'partrel'
 */
List *
split_partition_bound(Relation rel, Relation parentrel, Oid partrel,
									PartitionBoundSpec *split_bound)
{
	PartitionBoundSpec	*bound;
	PartitionKey	key = RelationGetPartitionKey(parentrel);

	Assert(get_partition_parent(partrel) == RelationGetRelid(parentrel));

	bound = get_relation_bound(partrel);
	Assert(key->strategy == bound->strategy);
	if (bound->strategy == PARTITION_STRATEGY_LIST)
	{
		PartitionBoundSpec	*new_bound1;
		PartitionBoundSpec	*new_bound2;

		/* tansformSplitAt already removed dup values */
		new_bound1 = copyObject(bound);
		new_bound1->listdatums = split_bound->listdatums;
		new_bound1->is_default = false;
		if (bound->is_default)
			new_bound2 = bound;
		else
		{
			/* Remove split values from bound->listdatums */
			ListCell	*l;
			bool	new_bound1_hasnull = false;
			bool	bound_hasnull = false;
			List	*new_bound1_notnull = NIL;
			List	*bound_notnull = NIL;
			Node	*null_node = NULL;

			new_bound2 = copyObject(bound);
			new_bound2->listdatums = NIL;
			foreach(l, new_bound1->listdatums)
			{
				Const	*c = castNode(Const, lfirst(l));

				if (c->constisnull)
				{
					new_bound1_hasnull = true;
					continue;
				}

				new_bound1_notnull = lappend(new_bound1_notnull, c);
			}

			foreach(l, bound->listdatums)
			{
				Const	*c = castNode(Const, lfirst(l));

				if (c->constisnull)
				{
					bound_hasnull = true;
					null_node = (Node *) c;
					continue;
				}

				bound_notnull = lappend(bound_notnull, c);
			}

			/* Should split NULL? */
			if (new_bound1_hasnull && !bound_hasnull)
				elog(ERROR, "NULL const does not exist in split partition value list");
			else if (!new_bound1_hasnull && bound_hasnull)
			{
				/* NULL is not splitted */
				new_bound2->listdatums = lappend(new_bound2->listdatums, null_node);
			}

			/* `new_bound1_notnull' must be a subset of `bound_notnull' */
			foreach(l, new_bound1_notnull)
			{
				Const		*n = castNode(Const, lfirst(l));
				ListCell	*ll;
				ListCell	*prev = NULL;

				Assert(!n->constisnull);
				foreach(ll, bound_notnull)
				{
					Const	*b = castNode(Const, lfirst(ll));

					Assert(!n->constisnull);

					/* If the two datum is equal? */
					if (datumIsEqual(n->constvalue, b->constvalue,
									key->parttypbyval[0], key->parttyplen[0]))
						break;
					prev = ll;
				}

				if (ll == NULL)
					elog(ERROR, "splitting value not found in splitted partition");
				else
				{
					/* Remove the found values in bound_notnull */
					bound_notnull = list_delete_cell(bound_notnull, ll, prev);
				}
			}

			new_bound2->listdatums = list_concat(new_bound2->listdatums,
													bound_notnull);
			if (list_length(new_bound2->listdatums) == 0)
				elog(ERROR, "resulting partition has at least one value for LIST partition");
		}

		return list_make2(new_bound1, new_bound2);
	}
	else if (bound->strategy == PARTITION_STRATEGY_RANGE)
	{
		/* Make sure split point between upper and lower of `bound' */
		PartitionRangeBound	*rel_lower;
		PartitionRangeBound	*rel_upper;
		PartitionRangeBound	*split_rb;

		if (bound->is_default)
		{
			PartitionBoundSpec	*last_bound;
			PartitionBoundSpec	*bound1;
			ListCell	*lc;

			last_bound = get_partitions_maxbound(key, partrel);
			bound1 = copyObject(bound);

			foreach(lc, last_bound->upperdatums)
			{
				PartitionRangeDatum *prd
						= castNode(PartitionRangeDatum, lfirst(lc));

				/*
				 * There is no way to split a partition if the last max bound
				 * is MAXVALUE or MINVALUE.
				 */
				if (prd->kind == PARTITION_RANGE_DATUM_MAXVALUE)
					ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR),
						errmsg("can not split default range partition"),
						errdetail("range partition already contains MAX_VALUES")));
			}

			/*
			 * If qualified partition is default, we use max_bound as the lower
			 * bound for new splited partition. And 'max_bound' must be less
			 * than split_bound, even if this is not true, define relation logic
			 * should will reject it.
			 */
			bound1->lowerdatums = last_bound->upperdatums;
			bound1->upperdatums = split_bound->upperdatums;
			bound1->is_default = false;

			return list_make2(bound1, bound);
		}

		if (list_length(bound->lowerdatums)
						!= list_length(split_bound->lowerdatums))
			elog(ERROR, "number of expressions is not equal to "
							"the number of partition columns");

		rel_lower = make_one_partition_rbound(key, -1, bound->lowerdatums, true);
		rel_upper = make_one_partition_rbound(key, -1, bound->upperdatums, false);

		split_rb = make_one_partition_rbound(key, -1, split_bound->lowerdatums, true);

		/*
		 * transformMergeSplitFor() checked no NULL const in split_bound for
		 * range partition. Earlier check for range point, it is cheaper than
		 * checking in check_new_partition_bound().
		 */
		if (partition_rbound_cmp(key->partnatts, key->partsupfunc,
								 key->partcollation, split_rb->datums,
								 split_rb->kind, true, rel_lower) > 0)
		{
			if (partition_rbound_cmp(key->partnatts, key->partsupfunc,
									key->partcollation, split_rb->datums,
									split_rb->kind, false, rel_upper) < 0)
			{
				/* Valid split point, generate two split bounds */
				PartitionBoundSpec	*bound1 = copyObject(bound);
				PartitionBoundSpec	*bound2 = copyObject(bound);

				bound1->upperdatums = split_bound->lowerdatums;
				bound2->lowerdatums = split_bound->lowerdatums;

				return list_make2(bound1, bound2);
			}
		}
		elog(ERROR, "split point is out of partition range");
	}
	else
		elog(ERROR, "only can split RANGE/LIST partition");
}
#endif
