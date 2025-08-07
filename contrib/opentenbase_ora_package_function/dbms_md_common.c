/*-------------------------------------------------------------------------
 *
 * common.c
 *	Catalog routines used by pg_dump; long ago these were shared
 *	by another dump tool, but not anymore.
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/bin/pg_dump/common.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <ctype.h>
#include "utils/memutils.h"

#include "catalog/pg_class.h"
#include "dbms_md_string_utils.h"
#include "dbms_md_dumputils.h"
#include "dbms_md_backup_archiver.h"
#include "dbms_md_dump.h"

static const char* modulename = "common";

void init_md_common_gvar(DBMS_MD_DUMP_COMMON_ST* gvar)
{
	gvar->dumpIdMap = NULL;
	gvar->allocedDumpIds = 0;
	gvar->lastDumpId = 0;

	gvar->catalogIdMapValid = false;
	gvar->catalogIdMap = NULL;
	gvar->numCatalogIds = 0;

	gvar->tblinfoindex = NULL;
	gvar->typinfoindex = NULL;
	gvar->funinfoindex = NULL;
	gvar->oprinfoindex = NULL;
	gvar->collinfoindex = NULL;
	gvar->nspinfoindex = NULL;
	gvar->extinfoindex = NULL;
	gvar->numTables = 0;
	gvar->numTypes = 0;
	gvar->numFuncs = 0;
	gvar->numOperators = 0;
	gvar->numCollations = 0;
	gvar->numNamespaces = 0;
	gvar->numExtensions = 0;

	gvar->extmembers = NULL;
	gvar->numextmembers = 0;
}

void clean_md_common_gvar(DBMS_MD_DUMP_COMMON_ST* gvar)
{
	if (NULL != gvar->dumpIdMap)
		dbms_md_free(gvar->dumpIdMap);

	gvar->dumpIdMap = NULL;
	gvar->allocedDumpIds = 0;
	gvar->lastDumpId = 0;
	gvar->catalogIdMapValid = false;

	if (NULL != gvar->catalogIdMap)
		dbms_md_free(gvar->catalogIdMap);
	gvar->catalogIdMap = NULL;

	gvar->numCatalogIds = 0;

	if (NULL != gvar->tblinfoindex)
		dbms_md_free(gvar->tblinfoindex);
	
	gvar->tblinfoindex = NULL;

	if (NULL != gvar->typinfoindex)
		dbms_md_free(gvar->typinfoindex);

	gvar->typinfoindex = NULL;

	if (NULL != gvar->funinfoindex)
		dbms_md_free(gvar->funinfoindex);
	
	gvar->funinfoindex = NULL;

	if (NULL != gvar->oprinfoindex)
		dbms_md_free(gvar->oprinfoindex);
		
	gvar->oprinfoindex = NULL;

	if (NULL != gvar->collinfoindex)
		dbms_md_free(gvar->collinfoindex);
	
	gvar->collinfoindex = NULL;

	if (NULL != gvar->nspinfoindex)
		dbms_md_free(gvar->nspinfoindex);
	
	gvar->nspinfoindex = NULL;

	if (NULL != gvar->extinfoindex)
		dbms_md_free(gvar->extinfoindex);

	gvar->extinfoindex = NULL;

	gvar->numTables = 0;
	gvar->numTypes = 0;
	gvar->numFuncs = 0;
	gvar->numOperators = 0;
	gvar->numCollations = 0;
	gvar->numNamespaces = 0;
	gvar->numExtensions = 0;

	if (NULL != gvar->extmembers)
		dbms_md_free(gvar->extmembers);
	
	gvar->extmembers = NULL;
	gvar->numextmembers = 0;
}

static void flagInhTables(Archive *fout, TableInfo *tbinfo, int numTables,
			  InhInfo *inhinfo, int numInherits);
static void flagInhIndexes(Archive *fout, TableInfo *tblinfo, int numTables);
static void flagInhAttrs(Archive *fout, TableInfo *tblinfo, int numTables);
static DumpableObject **
buildIndexArray(Archive *fout, void *objArray, int numObjs, Size objSize);

static int	DOCatalogIdCompare(const void *p1, const void *p2);
static int	ExtensionMemberIdCompare(const void *p1, const void *p2);
static void findParentsByOid(Archive *fout, TableInfo *self,
				 InhInfo *inhinfo, int numInherits);
static int	strInArray(const char *pattern, char **arr, int arr_size);
static IndxInfo *findIndexByOid(Oid oid, DumpableObject **idxinfoindex,
			   int numIndexes);


/*
 * dbms_md_get_schema_data
 *	  Collect information about all potentially dumpable objects
 */
TableInfo *
dbms_md_get_schema_data(Archive *fout, int *numTablesPtr)
{
	TableInfo  *tblinfo;
	TypeInfo   *typinfo;
	FuncInfo   *funinfo;
	OprInfo    *oprinfo;
	CollInfo   *collinfo;
	NamespaceInfo *nspinfo;
	ExtensionInfo *extinfo;
	InhInfo    *inhinfo;
	int			numAggregates;
	int			numInherits;
	//int			numRules;
	int			numProcLangs;
	int			numCasts;
	int			numTransforms;
	int			numAccessMethods;
	int			numOpclasses;
	int			numOpfamilies;
	int			numConversions;
	int			numTSParsers;
	int			numTSTemplates;
	int			numTSDicts;
	int			numTSConfigs;
	int			numForeignDataWrappers;
	int			numForeignServers;
	int			numDefaultACLs;
	int			numEventTriggers;

	DBMS_MD_DUMP_COMMON_ST *gvar = &(fout->dopt->comm_gvar);
	/*
	 * We must read extensions and extension membership info first, because
	 * extension membership needs to be consultable during decisions about
	 * whether other objects are to be dumped.
	 */
	write_msg(modulename, "reading extensions\n");
	extinfo = getExtensions(fout, &(gvar->numExtensions));
	gvar->extinfoindex = buildIndexArray(fout, extinfo, gvar->numExtensions, sizeof(ExtensionInfo));

	write_msg(modulename, "identifying extension members\n");
	getExtensionMembership(fout, extinfo, gvar->numExtensions);
	
	write_msg(modulename, "reading schemas\n");
	nspinfo = getNamespaces(fout, &(gvar->numNamespaces));
	gvar->nspinfoindex = buildIndexArray(fout, nspinfo, gvar->numNamespaces, sizeof(NamespaceInfo));

	/*
	 * getTables should be done as soon as possible, so as to minimize the
	 * window between starting our transaction and acquiring per-table locks.
	 * However, we have to do getNamespaces first because the tables get
	 * linked to their containing namespaces during getTables.
	 */
	write_msg(modulename, "reading user-defined tables\n");
	tblinfo = getTables(fout, &(gvar->numTables));
	gvar->tblinfoindex = buildIndexArray(fout, tblinfo, gvar->numTables, sizeof(TableInfo));

	/* Do this after we've built tblinfoindex */
	getOwnedSeqs(fout, tblinfo, gvar->numTables);

	write_msg(modulename, "reading user-defined functions\n");
	funinfo = getFuncs(fout, &(gvar->numFuncs));
	gvar->funinfoindex = buildIndexArray(fout, funinfo, gvar->numFuncs, sizeof(FuncInfo));

	/* this must be after getTables and getFuncs */
	write_msg(modulename, "reading user-defined types\n");
	typinfo = getTypes(fout, &(gvar->numTypes));
	gvar->typinfoindex = buildIndexArray(fout, typinfo, gvar->numTypes, sizeof(TypeInfo));

	/* this must be after getFuncs, too */
	write_msg(modulename, "reading procedural languages\n");
	getProcLangs(fout, &numProcLangs);

	write_msg(modulename, "reading user-defined aggregate functions\n");
	getAggregates(fout, &numAggregates);

	write_msg(modulename, "reading user-defined operators\n");
	oprinfo = getOperators(fout, &(gvar->numOperators));
	gvar->oprinfoindex = buildIndexArray(fout, oprinfo, gvar->numOperators, sizeof(OprInfo));

	write_msg(modulename, "reading user-defined access methods\n");
	getAccessMethods(fout, &numAccessMethods);

	write_msg(modulename, "reading user-defined operator classes\n");
	getOpclasses(fout, &numOpclasses);

	write_msg(modulename, "reading user-defined operator families\n");
	getOpfamilies(fout, &numOpfamilies);

	write_msg(modulename, "reading user-defined text search parsers\n");
	getTSParsers(fout, &numTSParsers);

	write_msg(modulename, "reading user-defined text search templates\n");
	getTSTemplates(fout, &numTSTemplates);

	write_msg(modulename, "reading user-defined text search dictionaries\n");
	getTSDictionaries(fout, &numTSDicts);

	write_msg(modulename, "reading user-defined text search configurations\n");
	getTSConfigurations(fout, &numTSConfigs);

	write_msg(modulename, "reading user-defined foreign-data wrappers\n");
	getForeignDataWrappers(fout, &numForeignDataWrappers);

	write_msg(modulename, "reading user-defined foreign servers\n");
	getForeignServers(fout, &numForeignServers);

	write_msg(modulename, "reading default privileges\n");
	getDefaultACLs(fout, &numDefaultACLs);

	write_msg(modulename, "reading user-defined collations\n");
	collinfo = getCollations(fout, &(gvar->numCollations));
	gvar->collinfoindex = buildIndexArray(fout, collinfo, gvar->numCollations, sizeof(CollInfo));

	write_msg(modulename, "reading user-defined conversions\n");
	getConversions(fout, &numConversions);

	write_msg(modulename, "reading type casts\n");
	getCasts(fout, &numCasts);

	write_msg(modulename, "reading transforms\n");
	getTransforms(fout, &numTransforms);

	write_msg(modulename, "reading table inheritance information\n");
	inhinfo = getInherits(fout, &numInherits);

	write_msg(modulename, "reading event triggers\n");
	getEventTriggers(fout, &numEventTriggers);

	/* Identify extension configuration tables that should be dumped */
	write_msg(modulename, "finding extension tables\n");
	processExtensionTables(fout, extinfo, gvar->numExtensions);

	/* Link tables to parents, mark parents of target tables interesting */
	write_msg(modulename, "finding inheritance relationships\n");
	flagInhTables(fout, tblinfo, gvar->numTables, inhinfo, numInherits);

	write_msg(modulename, "reading column info for interesting tables\n");
	getTableAttrs(fout, tblinfo, gvar->numTables);

	write_msg(modulename, "flagging inherited columns in subtables\n");
	flagInhAttrs(fout, tblinfo, gvar->numTables);

	write_msg(modulename, "reading indexes\n");
	getIndexes(fout, tblinfo, gvar->numTables);

	write_msg(modulename, "flagging indexes in partitioned tables\n");
	flagInhIndexes(fout, tblinfo, gvar->numTables);
#if 0
	write_msg(modulename, "reading extended statistics\n");
	getExtendedStatistics(fout, tblinfo, numTables);

	write_msg(modulename, "reading constraints\n");
	getConstraints(fout, tblinfo, numTables);

	write_msg(modulename, "reading triggers\n");
	getTriggers(fout, tblinfo, numTables);

	write_msg(modulename, "reading rewrite rules\n");
	getRules(fout, &numRules);

	write_msg(modulename, "reading policies\n");
	getPolicies(fout, tblinfo, numTables);

	write_msg(modulename, "reading publications\n");
	getPublications(fout);

	write_msg(modulename, "reading publication membership\n");
	getPublicationTables(fout, tblinfo, numTables);

	write_msg(modulename, "reading subscriptions\n");
	getSubscriptions(fout);
#endif 
	*numTablesPtr = gvar->numTables;
	return tblinfo;
}

/* flagInhTables -
 *	 Fill in parent link fields of every target table, and mark
 *	 parents of target tables as interesting
 *
 * Note that only direct ancestors of targets are marked interesting.
 * This is sufficient; we don't much care whether they inherited their
 * attributes or not.
 *
 * modifies tblinfo
 */
static void
flagInhTables(Archive *fout, TableInfo *tblinfo, int numTables,
			  InhInfo *inhinfo, int numInherits)
{
	int			i,
				j;
	int			numParents;
	TableInfo **parents;

	for (i = 0; i < numTables; i++)
	{
		/* Some kinds never have parents */
		if (tblinfo[i].relkind == RELKIND_SEQUENCE ||
			tblinfo[i].relkind == RELKIND_VIEW ||
			tblinfo[i].relkind == RELKIND_MATVIEW)
			continue;

		/* Don't bother computing anything for non-target tables, either */
		if (!tblinfo[i].dobj.dump)
			continue;

		/* Find all the immediate parent tables */
		findParentsByOid(fout, &tblinfo[i], inhinfo, numInherits);

		/* Mark the parents as interesting for getTableAttrs */
		numParents = tblinfo[i].numParents;
		parents = tblinfo[i].parents;
		for (j = 0; j < numParents; j++)
			parents[j]->interesting = true;
	}
}

/*
 * flagInhIndexes -
 *	 Create AttachIndexInfo objects for partitioned indexes, and add
 *	 appropriate dependency links.
 */
static void
flagInhIndexes(Archive *fout, TableInfo tblinfo[], int numTables)
{
	int		i,
			j,
			k;
	DumpableObject ***parentIndexArray;

	parentIndexArray = (DumpableObject ***)	dbms_md_malloc0(fout->memory_ctx, 
									getMaxDumpId(fout) * sizeof(DumpableObject **));

	for (i = 0; i < numTables; i++)
	{
		TableInfo	   *parenttbl;
		IndexAttachInfo *attachinfo;

		if (!tblinfo[i].ispartition || tblinfo[i].numParents == 0)
			continue;

		Assert(tblinfo[i].numParents == 1);
		parenttbl = tblinfo[i].parents[0];

		/*
		 * We need access to each parent table's index list, but there is no
		 * index to cover them outside of this function.  To avoid having to
		 * sort every parent table's indexes each time we come across each of
		 * its partitions, create an indexed array for each parent the first
		 * time it is required.
		 */
		if (parentIndexArray[parenttbl->dobj.dumpId] == NULL)
			parentIndexArray[parenttbl->dobj.dumpId] =
				buildIndexArray(fout, parenttbl->indexes,
								parenttbl->numIndexes,
								sizeof(IndxInfo));

		attachinfo = (IndexAttachInfo *)	dbms_md_malloc0(fout->memory_ctx, 
										tblinfo[i].numIndexes * sizeof(IndexAttachInfo));
		for (j = 0, k = 0; j < tblinfo[i].numIndexes; j++)
		{
			IndxInfo   *index = &(tblinfo[i].indexes[j]);
			IndxInfo   *parentidx;

			if (index->parentidx == 0)
				continue;

			parentidx = findIndexByOid(index->parentidx,
									   parentIndexArray[parenttbl->dobj.dumpId],
									   parenttbl->numIndexes);
			if (parentidx == NULL)
				continue;

			attachinfo[k].dobj.objType = DO_INDEX_ATTACH;
			attachinfo[k].dobj.catId.tableoid = 0;
			attachinfo[k].dobj.catId.oid = 0;
			AssignDumpId(fout, &attachinfo[k].dobj);
			attachinfo[k].dobj.name = dbms_md_strdup(index->dobj.name);
			attachinfo[k].parentIdx = parentidx;
			attachinfo[k].partitionIdx = index;

			/*
			 * We want dependencies from parent to partition (so that the
			 * partition index is created first), and another one from
			 * attach object to parent (so that the partition index is
			 * attached once the parent index has been created).
			 */
			addObjectDependency(fout, &parentidx->dobj, index->dobj.dumpId);
			addObjectDependency(fout, &attachinfo[k].dobj, parentidx->dobj.dumpId);

			k++;
		}
	}

	for (i = 0; i < numTables; i++)
		if (parentIndexArray[i])
			dbms_md_free(parentIndexArray[i]);
	dbms_md_free(parentIndexArray);
}

/* flagInhAttrs -
 *	 for each dumpable table in tblinfo, flag its inherited attributes
 *
 * What we need to do here is detect child columns that inherit NOT NULL
 * bits from their parents (so that we needn't specify that again for the
 * child) and child columns that have DEFAULT NULL when their parents had
 * some non-null default.  In the latter case, we make up a dummy AttrDefInfo
 * object so that we'll correctly emit the necessary DEFAULT NULL clause;
 * otherwise the backend will apply an inherited default to the column.
 *
 * modifies tblinfo
 */
static void
flagInhAttrs(Archive *fout, TableInfo *tblinfo, int numTables)
{
	int			i,
				j,
				k;

	DumpOptions *dopt = fout->dopt;
	
	for (i = 0; i < numTables; i++)
	{
		TableInfo  *tbinfo = &(tblinfo[i]);
		int			numParents;
		TableInfo **parents;

		/* Some kinds never have parents */
		if (tbinfo->relkind == RELKIND_SEQUENCE ||
			tbinfo->relkind == RELKIND_VIEW ||
			tbinfo->relkind == RELKIND_MATVIEW)
			continue;

		/* Don't bother computing anything for non-target tables, either */
		if (!tbinfo->dobj.dump)
			continue;

		numParents = tbinfo->numParents;
		parents = tbinfo->parents;

		if (numParents == 0)
			continue;			/* nothing to see here, move along */

		/* For each column, search for matching column names in parent(s) */
		for (j = 0; j < tbinfo->numatts; j++)
		{
			bool		foundNotNull;	/* Attr was NOT NULL in a parent */
			bool		foundDefault;	/* Found a default in a parent */

			/* no point in examining dropped columns */
			if (tbinfo->attisdropped[j])
				continue;

			foundNotNull = false;
			foundDefault = false;
			for (k = 0; k < numParents; k++)
			{
				TableInfo  *parent = parents[k];
				int			inhAttrInd;

				inhAttrInd = strInArray(tbinfo->attnames[j],
										parent->attnames,
										parent->numatts);
				if (inhAttrInd >= 0)
				{
					foundNotNull |= parent->notnull[inhAttrInd];
					foundDefault |= (parent->attrdefs[inhAttrInd] != NULL);
				}
			}

			/* Remember if we found inherited NOT NULL */
			tbinfo->inhNotNull[j] = foundNotNull;

			/* Manufacture a DEFAULT NULL clause if necessary */
			if (foundDefault && tbinfo->attrdefs[j] == NULL)
			{
				AttrDefInfo *attrDef;

				attrDef = (AttrDefInfo *) dbms_md_malloc0(fout->memory_ctx, sizeof(AttrDefInfo));
				attrDef->dobj.objType = DO_ATTRDEF;
				attrDef->dobj.catId.tableoid = 0;
				attrDef->dobj.catId.oid = 0;
				AssignDumpId(fout, &attrDef->dobj);
				attrDef->dobj.name = dbms_md_strdup(tbinfo->dobj.name);
				attrDef->dobj.namespace = tbinfo->dobj.namespace;
				attrDef->dobj.dump = tbinfo->dobj.dump;

				attrDef->adtable = tbinfo;
				attrDef->adnum = j + 1;
				attrDef->adef_expr = dbms_md_strdup("NULL");

				/* Will column be dumped explicitly? */
				if (shouldPrintColumn(dopt, tbinfo, j))
				{
					attrDef->separate = false;
					/* No dependency needed: NULL cannot have dependencies */
				}
				else
				{
					/* column will be suppressed, print default separately */
					attrDef->separate = true;
					/* ensure it comes out after the table */
					addObjectDependency(fout, &attrDef->dobj,
										tbinfo->dobj.dumpId);
				}

				tbinfo->attrdefs[j] = attrDef;
			}
		}
	}
}

/*
 * AssignDumpId
 *		Given a newly-created dumpable object, assign a dump ID,
 *		and enter the object into the lookup table.
 *
 * The caller is expected to have filled in objType and catId,
 * but not any of the other standard fields of a DumpableObject.
 */
void
AssignDumpId(Archive *fout, DumpableObject *dobj)
{
	DBMS_MD_DUMP_COMMON_ST *gvar = &(fout->dopt->comm_gvar);
	dobj->dumpId = ++gvar->lastDumpId;
	dobj->name = NULL;			/* must be set later */
	dobj->namespace = NULL;		/* may be set later */
	dobj->dump = DUMP_COMPONENT_ALL;	/* default assumption */
	dobj->ext_member = false;	/* default assumption */
	dobj->dependencies = NULL;
	dobj->nDeps = 0;
	dobj->allocDeps = 0;

	while (dobj->dumpId >= gvar->allocedDumpIds)
	{
		int			newAlloc;

		if (gvar->allocedDumpIds <= 0)
		{
			newAlloc = 256;
			gvar->dumpIdMap = (DumpableObject **)
				dbms_md_malloc0(fout->memory_ctx, newAlloc * sizeof(DumpableObject *));
		}
		else
		{
			newAlloc = gvar->allocedDumpIds * 2;
			gvar->dumpIdMap = (DumpableObject **)
				dbms_md_realloc(gvar->dumpIdMap, newAlloc * sizeof(DumpableObject *));
		}
		memset(gvar->dumpIdMap + gvar->allocedDumpIds, 0,
			   (newAlloc - gvar->allocedDumpIds) * sizeof(DumpableObject *));
		gvar->allocedDumpIds = newAlloc;
	}
	gvar->dumpIdMap[dobj->dumpId] = dobj;

	/* mark catalogIdMap invalid, but don't rebuild it yet */
	gvar->catalogIdMapValid = false;
}

/*
 * Assign a DumpId that's not tied to a DumpableObject.
 *
 * This is used when creating a "fixed" ArchiveEntry that doesn't need to
 * participate in the sorting logic.
 */
DumpId
createDumpId(Archive *fout)
{
	DBMS_MD_DUMP_COMMON_ST *gvar = &(fout->dopt->comm_gvar);
	return ++gvar->lastDumpId;
}

/*
 * Return the largest DumpId so far assigned
 */
DumpId
getMaxDumpId(Archive *fout)
{
	DBMS_MD_DUMP_COMMON_ST *gvar = &(fout->dopt->comm_gvar);
	return gvar->lastDumpId;
}

/*
 * Find a DumpableObject by dump ID
 *
 * Returns NULL for invalid ID
 */
DumpableObject *
findObjectByDumpId(Archive *fout, DumpId dumpId)
{
	DBMS_MD_DUMP_COMMON_ST *gvar = &(fout->dopt->comm_gvar);
	if (dumpId <= 0 || dumpId >= gvar->allocedDumpIds)
		return NULL;			/* out of range? */
	return gvar->dumpIdMap[dumpId];
}

/*
 * Find a DumpableObject by catalog ID
 *
 * Returns NULL for unknown ID
 *
 * We use binary search in a sorted list that is built on first call.
 * If AssignDumpId() and findObjectByCatalogId() calls were freely intermixed,
 * the code would work, but possibly be very slow.  In the current usage
 * pattern that does not happen, indeed we build the list at most twice.
 */
DumpableObject *
findObjectByCatalogId(Archive *fout, CatalogId catalogId)
{
	DumpableObject **low;
	DumpableObject **high;
	DBMS_MD_DUMP_COMMON_ST *gvar = &(fout->dopt->comm_gvar);

	if (!gvar->catalogIdMapValid)
	{
		if (gvar->catalogIdMap)
			dbms_md_free(gvar->catalogIdMap);
		getDumpableObjects(fout, &gvar->catalogIdMap, &(gvar->numCatalogIds));
		if (gvar->numCatalogIds > 1)
			qsort((void *) (gvar->catalogIdMap), gvar->numCatalogIds,
				  sizeof(DumpableObject *), DOCatalogIdCompare);
		gvar->catalogIdMapValid = true;
	}

	/*
	 * We could use bsearch() here, but the notational cruft of calling
	 * bsearch is nearly as bad as doing it ourselves; and the generalized
	 * bsearch function is noticeably slower as well.
	 */
	if (gvar->numCatalogIds <= 0)
		return NULL;
	low = gvar->catalogIdMap;
	high = gvar->catalogIdMap + (gvar->numCatalogIds - 1);
	while (low <= high)
	{
		DumpableObject **middle;
		int			difference;

		middle = low + (high - low) / 2;
		/* comparison must match DOCatalogIdCompare, below */
		difference = oidcmp((*middle)->catId.oid, catalogId.oid);
		if (difference == 0)
			difference = oidcmp((*middle)->catId.tableoid, catalogId.tableoid);
		if (difference == 0)
			return *middle;
		else if (difference < 0)
			low = middle + 1;
		else
			high = middle - 1;
	}
	return NULL;
}

/*
 * Find a DumpableObject by OID, in a pre-sorted array of one type of object
 *
 * Returns NULL for unknown OID
 */
static DumpableObject *
findObjectByOid(Oid oid, DumpableObject **indexArray, int numObjs)
{
	DumpableObject **low;
	DumpableObject **high;

	/*
	 * This is the same as findObjectByCatalogId except we assume we need not
	 * look at table OID because the objects are all the same type.
	 *
	 * We could use bsearch() here, but the notational cruft of calling
	 * bsearch is nearly as bad as doing it ourselves; and the generalized
	 * bsearch function is noticeably slower as well.
	 */
	if (numObjs <= 0)
		return NULL;
	low = indexArray;
	high = indexArray + (numObjs - 1);
	while (low <= high)
	{
		DumpableObject **middle;
		int			difference;

		middle = low + (high - low) / 2;
		difference = oidcmp((*middle)->catId.oid, oid);
		if (difference == 0)
			return *middle;
		else if (difference < 0)
			low = middle + 1;
		else
			high = middle - 1;
	}
	return NULL;
}


/*
 * Build an index array of DumpableObject pointers, sorted by OID
 */
static DumpableObject **
buildIndexArray(Archive *fout, void *objArray, int numObjs, Size objSize)
{
	DumpableObject **ptrs;
	int			i;

	ptrs = (DumpableObject **) dbms_md_malloc0(fout->memory_ctx, 
											  numObjs * sizeof(DumpableObject *));
	for (i = 0; i < numObjs; i++)
		ptrs[i] = (DumpableObject *) ((char *) objArray + i * objSize);

	/* We can use DOCatalogIdCompare to sort since its first key is OID */
	if (numObjs > 1)
		qsort((void *) ptrs, numObjs, sizeof(DumpableObject *),
			  DOCatalogIdCompare);

	return ptrs;
}

/*
 * qsort comparator for pointers to DumpableObjects
 */
static int
DOCatalogIdCompare(const void *p1, const void *p2)
{
	const DumpableObject *obj1 = *(DumpableObject *const *) p1;
	const DumpableObject *obj2 = *(DumpableObject *const *) p2;
	int			cmpval;

	/*
	 * Compare OID first since it's usually unique, whereas there will only be
	 * a few distinct values of tableoid.
	 */
	cmpval = oidcmp(obj1->catId.oid, obj2->catId.oid);
	if (cmpval == 0)
		cmpval = oidcmp(obj1->catId.tableoid, obj2->catId.tableoid);
	return cmpval;
}

/*
 * Build an array of pointers to all known dumpable objects
 *
 * This simply creates a modifiable copy of the internal map.
 */
void
getDumpableObjects(Archive *fout, DumpableObject ***objs, int *numObjs)
{
	DBMS_MD_DUMP_COMMON_ST *gvar = &(fout->dopt->comm_gvar);
	int			i,
				j;

	*objs = (DumpableObject **)
		dbms_md_malloc0(fout->memory_ctx, gvar->allocedDumpIds * sizeof(DumpableObject *));
	j = 0;
	for (i = 1; i < gvar->allocedDumpIds; i++)
	{
		if (gvar->dumpIdMap[i])
			(*objs)[j++] = gvar->dumpIdMap[i];
	}
	*numObjs = j;
}

/*
 * Add a dependency link to a DumpableObject
 *
 * Note: duplicate dependencies are currently not eliminated
 */
void
addObjectDependency(Archive *fout, DumpableObject *dobj, DumpId refId)
{
	if (dobj->nDeps >= dobj->allocDeps)
	{
		if (dobj->allocDeps <= 0)
		{
			dobj->allocDeps = 16;
			dobj->dependencies = (DumpId *)
				dbms_md_malloc0(fout->memory_ctx, dobj->allocDeps * sizeof(DumpId));
		}
		else
		{
			dobj->allocDeps *= 2;
			dobj->dependencies = (DumpId *)dbms_md_realloc(dobj->dependencies,
						   dobj->allocDeps * sizeof(DumpId));
		}
	}
	dobj->dependencies[dobj->nDeps++] = refId;
}

/*
 * Remove a dependency link from a DumpableObject
 *
 * If there are multiple links, all are removed
 */
void
removeObjectDependency(DumpableObject *dobj, DumpId refId)
{
	int			i;
	int			j = 0;

	for (i = 0; i < dobj->nDeps; i++)
	{
		if (dobj->dependencies[i] != refId)
			dobj->dependencies[j++] = dobj->dependencies[i];
	}
	dobj->nDeps = j;
}


/*
 * findTableByOid
 *	  finds the entry (in tblinfo) of the table with the given oid
 *	  returns NULL if not found
 */
TableInfo *
findTableByOid(Archive *fout, Oid oid)
{
	DBMS_MD_DUMP_COMMON_ST *gvar = &(fout->dopt->comm_gvar);
	return (TableInfo *) findObjectByOid(oid, gvar->tblinfoindex, gvar->numTables);
}

/*
 * findTypeByOid
 *	  finds the entry (in typinfo) of the type with the given oid
 *	  returns NULL if not found
 */
TypeInfo *
findTypeByOid(Archive *fout, Oid oid)
{
	DBMS_MD_DUMP_COMMON_ST *gvar = &(fout->dopt->comm_gvar);
	return (TypeInfo *) findObjectByOid(oid, gvar->typinfoindex, gvar->numTypes);
}

/*
 * findFuncByOid
 *	  finds the entry (in funinfo) of the function with the given oid
 *	  returns NULL if not found
 */
FuncInfo *
findFuncByOid(Archive *fout, Oid oid)
{
	DBMS_MD_DUMP_COMMON_ST *gvar =&(fout->dopt->comm_gvar);
	return (FuncInfo *) findObjectByOid(oid, gvar->funinfoindex, gvar->numFuncs);
}

/*
 * findOprByOid
 *	  finds the entry (in oprinfo) of the operator with the given oid
 *	  returns NULL if not found
 */
OprInfo *
findOprByOid(Archive *fout, Oid oid)
{
	DBMS_MD_DUMP_COMMON_ST *gvar = &(fout->dopt->comm_gvar);
	return (OprInfo *) findObjectByOid(oid, gvar->oprinfoindex, gvar->numOperators);
}

/*
 * findCollationByOid
 *	  finds the entry (in collinfo) of the collation with the given oid
 *	  returns NULL if not found
 */
CollInfo *
findCollationByOid(Archive *fout, Oid oid)
{
	DBMS_MD_DUMP_COMMON_ST *gvar = &(fout->dopt->comm_gvar);
	return (CollInfo *) findObjectByOid(oid, gvar->collinfoindex, gvar->numCollations);
}

/*
 * findNamespaceByOid
 *	  finds the entry (in nspinfo) of the namespace with the given oid
 *	  returns NULL if not found
 */
NamespaceInfo *
findNamespaceByOid(Archive *fout, Oid oid)
{
	DBMS_MD_DUMP_COMMON_ST *gvar = &(fout->dopt->comm_gvar);
	return (NamespaceInfo *) findObjectByOid(oid, gvar->nspinfoindex, gvar->numNamespaces);
}

/*
 * findExtensionByOid
 *	  finds the entry (in extinfo) of the extension with the given oid
 *	  returns NULL if not found
 */
ExtensionInfo *
findExtensionByOid(Archive *fout, Oid oid)
{
	DBMS_MD_DUMP_COMMON_ST *gvar = &(fout->dopt->comm_gvar);
	return (ExtensionInfo *) findObjectByOid(oid, gvar->extinfoindex, gvar->numExtensions);
}

/*
 * findIndexByOid
 *		find the entry of the index with the given oid
 *
 * This one's signature is different from the previous ones because we lack a
 * global array of all indexes, so caller must pass their array as argument.
 */
static IndxInfo *
findIndexByOid(Oid oid, DumpableObject **idxinfoindex, int numIndexes)
{
	return (IndxInfo *) findObjectByOid(oid, idxinfoindex, numIndexes);
}

/*
 * setExtensionMembership
 *	  accept and save data about which objects belong to extensions
 */
void
setExtensionMembership(Archive *fout, ExtensionMemberId *extmems, int nextmems)
{
	DBMS_MD_DUMP_COMMON_ST *gvar = &(fout->dopt->comm_gvar);
	
	/* Sort array in preparation for binary searches */
	if (nextmems > 1)
		qsort((void *) extmems, nextmems, sizeof(ExtensionMemberId),
			  ExtensionMemberIdCompare);
	/* And save */
	gvar->extmembers = extmems;
	gvar->numextmembers = nextmems;
}

/*
 * findOwningExtension
 *	  return owning extension for specified catalog ID, or NULL if none
 */
ExtensionInfo *
findOwningExtension(Archive *fout, CatalogId catalogId)
{
	ExtensionMemberId *low;
	ExtensionMemberId *high;
	DBMS_MD_DUMP_COMMON_ST *gvar = &(fout->dopt->comm_gvar);

	/*
	 * We could use bsearch() here, but the notational cruft of calling
	 * bsearch is nearly as bad as doing it ourselves; and the generalized
	 * bsearch function is noticeably slower as well.
	 */
	if (gvar->numextmembers <= 0)
		return NULL;
	low = gvar->extmembers;
	high = gvar->extmembers + (gvar->numextmembers - 1);
	while (low <= high)
	{
		ExtensionMemberId *middle;
		int			difference;

		middle = low + (high - low) / 2;
		/* comparison must match ExtensionMemberIdCompare, below */
		difference = oidcmp(middle->catId.oid, catalogId.oid);
		if (difference == 0)
			difference = oidcmp(middle->catId.tableoid, catalogId.tableoid);
		if (difference == 0)
			return middle->ext;
		else if (difference < 0)
			low = middle + 1;
		else
			high = middle - 1;
	}
	return NULL;
}

/*
 * qsort comparator for ExtensionMemberIds
 */
static int
ExtensionMemberIdCompare(const void *p1, const void *p2)
{
	const ExtensionMemberId *obj1 = (const ExtensionMemberId *) p1;
	const ExtensionMemberId *obj2 = (const ExtensionMemberId *) p2;
	int			cmpval;

	/*
	 * Compare OID first since it's usually unique, whereas there will only be
	 * a few distinct values of tableoid.
	 */
	cmpval = oidcmp(obj1->catId.oid, obj2->catId.oid);
	if (cmpval == 0)
		cmpval = oidcmp(obj1->catId.tableoid, obj2->catId.tableoid);
	return cmpval;
}


/*
 * findParentsByOid
 *	  find a table's parents in tblinfo[]
 */
static void
findParentsByOid(Archive *fout, TableInfo *self,
				 InhInfo *inhinfo, int numInherits)
{
	Oid			oid = self->dobj.catId.oid;
	int			i,
				j;
	int			numParents;

	numParents = 0;
	for (i = 0; i < numInherits; i++)
	{
		if (inhinfo[i].inhrelid == oid)
			numParents++;
	}

	self->numParents = numParents;

	if (numParents > 0)
	{
		self->parents = (TableInfo **)
			dbms_md_malloc0(fout->memory_ctx, sizeof(TableInfo *) * numParents);
		j = 0;
		for (i = 0; i < numInherits; i++)
		{
			if (inhinfo[i].inhrelid == oid)
			{
				TableInfo  *parent;

				parent = findTableByOid(fout, inhinfo[i].inhparent);
				if (parent == NULL)
				{
					elog(ERROR, "failed sanity check, parent OID %u of table \"%s\" (OID %u) not found\n",
							  inhinfo[i].inhparent,
							  self->dobj.name,
							  oid);
					return ;
				}
				self->parents[j++] = parent;
			}
		}
	}
	else
		self->parents = NULL;
}

/*
 * parseOidArray
 *	  parse a string of numbers delimited by spaces into a character array
 *
 * Note: actually this is used for both Oids and potentially-signed
 * attribute numbers.  This should cause no trouble, but we could split
 * the function into two functions with different argument types if it does.
 */

void
parseOidArray(const char *str, Oid *array, int arraysize)
{
	int			j,
				argNum;
	char		temp[100];
	char		s;

	argNum = 0;
	j = 0;
	for (;;)
	{
		s = *str++;
		if (s == ' ' || s == '\0')
		{
			if (j > 0)
			{
				if (argNum >= arraysize)
				{
					elog(ERROR, "could not parse numeric array \"%s\": too many numbers\n", str);
					return ;
				}
				temp[j] = '\0';
				array[argNum++] = dbms_md_str_to_oid(temp);
				j = 0;
			}
			if (s == '\0')
				break;
		}
		else
		{
			if (!(isdigit((unsigned char) s) || s == '-') ||
				j >= sizeof(temp) - 1)
			{
				elog(ERROR, "could not parse numeric array \"%s\": invalid character in number\n", str);
				return ;
			}
			temp[j++] = s;
		}
	}

	while (argNum < arraysize)
		array[argNum++] = InvalidOid;
}


/*
 * strInArray:
 *	  takes in a string and a string array and the number of elements in the
 * string array.
 *	  returns the index if the string is somewhere in the array, -1 otherwise
 */

static int
strInArray(const char *pattern, char **arr, int arr_size)
{
	int			i;

	for (i = 0; i < arr_size; i++)
	{
		if (strcmp(pattern, arr[i]) == 0)
			return i;
	}
	return -1;
}


bool dbms_md_check_dump_obj_valid(DBMS_MD_DUMPOBJ_BI oti)
{
	return (oti <= DBMS_MD_DBOJ_TI_MAX) && (oti >= DBMS_MD_DBOJ_TI_MIN);
}

bool dbms_md_is_dump_this_obj(DumpOptions *dopt, DBMS_MD_DUMPOBJ_BI oti)
{
	uint64 bm = dopt->dump_obj_bm;
	uint64 st = 0;
	
	if (!dbms_md_check_dump_obj_valid(oti))
		return false;
	
	st = (1<<oti);
	return (st == (bm&st));
}


