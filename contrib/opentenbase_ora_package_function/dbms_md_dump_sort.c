/*-------------------------------------------------------------------------
 *
 * pg_dump_sort.c
 *	  Sort the items of a dump into a safe order for dumping
 *
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/bin/pg_dump/pg_dump_sort.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "dbms_md_dumputils.h"
#include "dbms_md_backup_archiver.h"
#include "dbms_md_dump.h"

#include "catalog/pg_class.h"

/* translator: this is a module name */
static const char *modulename = gettext_noop("sorter");

/*
 * Sort priority for database object types.
 * Objects are sorted by type, and within a type by name.
 *
 * Because materialized views can potentially reference system views,
 * DO_REFRESH_MATVIEW should always be the last thing on the list.
 *
 * NOTE: object-type priorities must match the section assignments made in
 * pg_dump.c; that is, PRE_DATA objects must sort before DO_PRE_DATA_BOUNDARY,
 * POST_DATA objects must sort after DO_POST_DATA_BOUNDARY, and DATA objects
 * must sort between them.
 *
 * Note: sortDataAndIndexObjectsBySize wants to have all DO_TABLE_DATA and
 * DO_INDEX objects in contiguous chunks, so do not reuse the values for those
 * for other object types.
 */
static const int dbObjectTypePriority[] =
{
	1,							/* DO_NAMESPACE */
	4,							/* DO_EXTENSION */
	5,							/* DO_TYPE */
	5,							/* DO_SHELL_TYPE */
	6,							/* DO_FUNC */
	7,							/* DO_AGG */
	8,							/* DO_OPERATOR */
	8,							/* DO_ACCESS_METHOD */
	9,							/* DO_OPCLASS */
	9,							/* DO_OPFAMILY */
	3,							/* DO_COLLATION */
	11,							/* DO_CONVERSION */
	18,							/* DO_TABLE */
	20,							/* DO_ATTRDEF */
	28,							/* DO_INDEX */
	29,							/* DO_INDEX_ATTACH */
	30,							/* DO_STATSEXT */
	31,							/* DO_RULE */
	32,							/* DO_TRIGGER */
	27,							/* DO_CONSTRAINT */
	33,							/* DO_FK_CONSTRAINT */
	2,							/* DO_PROCLANG */
	10,							/* DO_CAST */
	23,							/* DO_TABLE_DATA */
	24,							/* DO_SEQUENCE_SET */
	19,							/* DO_DUMMY_TYPE */
	12,							/* DO_TSPARSER */
	14,							/* DO_TSDICT */
	13,							/* DO_TSTEMPLATE */
	15,							/* DO_TSCONFIG */
	16,							/* DO_FDW */
	17,							/* DO_FOREIGN_SERVER */
	33,							/* DO_DEFAULT_ACL */
	3,							/* DO_TRANSFORM */
	21,							/* DO_BLOB */
	25,							/* DO_BLOB_DATA */
	22,							/* DO_PRE_DATA_BOUNDARY */
	26,							/* DO_POST_DATA_BOUNDARY */
	34,							/* DO_EVENT_TRIGGER */
	39,							/* DO_REFRESH_MATVIEW */
	35,							/* DO_POLICY */
	36,							/* DO_PUBLICATION */
	37,							/* DO_PUBLICATION_REL */
	38							/* DO_SUBSCRIPTION */
};

static int	md_do_type_name_compare(void *AH, const void *p1, const void *p2);
static bool md_topo_sort(Archive *fout, DumpableObject **objs,
		 int numObjs,
		 DumpableObject **ordering,
		 int *nOrdering);
static void addHeapElement(int val, int *heap, int heapLength);
static int	removeHeapElement(int *heap, int heapLength);
static void findDependencyLoops(Archive *fout, DumpableObject **objs, int nObjs, int totObjs);
static int findLoop(Archive *fout, 
		 DumpableObject *obj,
		 DumpId startPoint,
		 bool *processed,
		 DumpId *searchFailed,
		 DumpableObject **workspace,
		 int depth);
static void repairDependencyLoop(Archive *fout, DumpableObject **loop,
					 int nLoop);
static void describeDumpableObject(DumpableObject *obj,
					   char *buf, int bufsize);

static int	md_do_size_compare(const void *p1, const void *p2);

static char *dbms_md_med3(void *AH, char *a, char *b, char *c,
	 int (*cmp) (void *AH, const void *, const void *));
static void dbms_md_swapfunc(char *, char *, size_t, int);
static void
dbms_md_qsort(void *AH, void *a, size_t n, size_t es, int (*cmp) (void *AH, const void *, const void *));

/*
 * Qsort routine based on J. L. Bentley and M. D. McIlroy,
 * "Engineering a sort function",
 * Software--Practice and Experience 23 (1993) 1249-1265.
 *
 * We have modified their original by adding a check for already-sorted input,
 * which seems to be a win per discussions on pgsql-hackers around 2006-03-21.
 *
 * Also, we recurse on the smaller partition and iterate on the larger one,
 * which ensures we cannot recurse more than log(N) levels (since the
 * partition recursed to is surely no more than half of the input).  Bentley
 * and McIlroy explicitly rejected doing this on the grounds that it's "not
 * worth the effort", but we have seen crashes in the field due to stack
 * overrun, so that judgment seems wrong.
 */

#define dbms_md_swapcode(TYPE, parmi, parmj, n) \
do {		\
	size_t i = (n) / sizeof (TYPE);			\
	TYPE *pi = (TYPE *)(void *)(parmi);			\
	TYPE *pj = (TYPE *)(void *)(parmj);			\
	do {						\
		TYPE	t = *pi;			\
		*pi++ = *pj;				\
		*pj++ = t;				\
		} while (--i > 0);				\
} while (0)

#define SWAPINIT(a, es) swaptype = ((char *)(a) - (char *)0) % sizeof(long) || \
	(es) % sizeof(long) ? 2 : (es) == sizeof(long)? 0 : 1;

static void
dbms_md_swapfunc(char *a, char *b, size_t n, int swaptype)
{
	if (swaptype <= 1)
		dbms_md_swapcode(long, a, b, n);
	else
		dbms_md_swapcode(char, a, b, n);
}

#define dbms_md_swap(a, b)						\
	if (swaptype == 0) {					\
		long t = *(long *)(void *)(a);			\
		*(long *)(void *)(a) = *(long *)(void *)(b);	\
		*(long *)(void *)(b) = t;			\
	} else							\
		dbms_md_swapfunc(a, b, es, swaptype)

#define vecswap(a, b, n) if ((n) > 0) dbms_md_swapfunc(a, b, n, swaptype)

static char *
dbms_md_med3(void *AH, char *a, char *b, char *c, int (*cmp) (void *AH, const void *, const void *))
{
	return cmp(AH, a, b) < 0 ?
		(cmp(AH, b, c) < 0 ? b : (cmp(AH, a, c) < 0 ? c : a))
		: (cmp(AH, b, c) > 0 ? b : (cmp(AH, a, c) < 0 ? a : c));
}

static void
dbms_md_qsort(void *AH, void *a, size_t n, size_t es, int (*cmp) (void *AH, const void *, const void *))
{
	char	   *pa,
			   *pb,
			   *pc,
			   *pd,
			   *pl,
			   *pm,
			   *pn;
	size_t		d1,
				d2;
	int			r,
				swaptype,
				presorted;

loop:SWAPINIT(a, es);
	if (n < 7)
	{
		for (pm = (char *) a + es; pm < (char *) a + n * es; pm += es)
			for (pl = pm; pl > (char *) a && cmp(AH, pl - es, pl) > 0;
				 pl -= es)
				dbms_md_swap(pl, pl - es);
		return;
	}
	presorted = 1;
	for (pm = (char *) a + es; pm < (char *) a + n * es; pm += es)
	{
		if (cmp(AH, pm - es, pm) > 0)
		{
			presorted = 0;
			break;
		}
	}
	if (presorted)
		return;
	pm = (char *) a + (n / 2) * es;
	if (n > 7)
	{
		pl = (char *) a;
		pn = (char *) a + (n - 1) * es;
		if (n > 40)
		{
			size_t		d = (n / 8) * es;

			pl = dbms_md_med3(AH, pl, pl + d, pl + 2 * d, cmp);
			pm = dbms_md_med3(AH, pm - d, pm, pm + d, cmp);
			pn = dbms_md_med3(AH, pn - 2 * d, pn - d, pn, cmp);
		}
		pm = dbms_md_med3(AH, pl, pm, pn, cmp);
	}
	dbms_md_swap(a, pm);
	pa = pb = (char *) a + es;
	pc = pd = (char *) a + (n - 1) * es;
	for (;;)
	{
		while (pb <= pc && (r = cmp(AH, pb, a)) <= 0)
		{
			if (r == 0)
			{
				dbms_md_swap(pa, pb);
				pa += es;
			}
			pb += es;
		}
		while (pb <= pc && (r = cmp(AH, pc, a)) >= 0)
		{
			if (r == 0)
			{
				dbms_md_swap(pc, pd);
				pd -= es;
			}
			pc -= es;
		}
		if (pb > pc)
			break;
		dbms_md_swap(pb, pc);
		pb += es;
		pc -= es;
	}
	pn = (char *) a + n * es;
	d1 = Min(pa - (char *) a, pb - pa);
	vecswap(a, pb - d1, d1);
	d1 = Min(pd - pc, pn - pd - es);
	vecswap(pb, pn - d1, d1);
	d1 = pb - pa;
	d2 = pd - pc;
	if (d1 <= d2)
	{
		/* Recurse on left partition, then iterate on right partition */
		if (d1 > es)
			dbms_md_qsort(AH, a, d1 / es, es, cmp);
		if (d2 > es)
		{
			/* Iterate rather than recurse to save stack space */
			/* pg_qsort(pn - d2, d2 / es, es, cmp); */
			a = pn - d2;
			n = d2 / es;
			goto loop;
		}
	}
	else
	{
		/* Recurse on right partition, then iterate on left partition */
		if (d2 > es)
			dbms_md_qsort(AH, pn - d2, d2 / es, es, cmp);
		if (d1 > es)
		{
			/* Iterate rather than recurse to save stack space */
			/* pg_qsort(a, d1 / es, es, cmp); */
			n = d1 / es;
			goto loop;
		}
	}
}

static int
findFirstEqualType(DumpableObjectType type, DumpableObject **objs, int numObjs)
{
	int			i;

	for (i = 0; i < numObjs; i++)
		if (objs[i]->objType == type)
			return i;
	return -1;
}

static int
findFirstDifferentType(DumpableObjectType type, DumpableObject **objs, int numObjs, int start)
{
	int			i;

	for (i = start; i < numObjs; i++)
		if (objs[i]->objType != type)
			return i;
	return numObjs - 1;
}

/*
 * When we do a parallel dump, we want to start with the largest items first.
 *
 * Say we have the objects in this order:
 * ....DDDDD....III....
 *
 * with D = Table data, I = Index, . = other object
 *
 * This sorting function now takes each of the D or I blocks and sorts them
 * according to their size.
 */
void
sortDataAndIndexObjectsBySize(DumpableObject **objs, int numObjs)
{
	int			startIdx,
				endIdx;
	void	   *startPtr;

	if (numObjs <= 1)
		return;

	startIdx = findFirstEqualType(DO_TABLE_DATA, objs, numObjs);
	if (startIdx >= 0)
	{
		endIdx = findFirstDifferentType(DO_TABLE_DATA, objs, numObjs, startIdx);
		startPtr = objs + startIdx;
		qsort(startPtr, endIdx - startIdx, sizeof(DumpableObject *),
			  md_do_size_compare);
	}

	startIdx = findFirstEqualType(DO_INDEX, objs, numObjs);
	if (startIdx >= 0)
	{
		endIdx = findFirstDifferentType(DO_INDEX, objs, numObjs, startIdx);
		startPtr = objs + startIdx;
		qsort(startPtr, endIdx - startIdx, sizeof(DumpableObject *),
			  md_do_size_compare);
	}
}

static int
md_do_size_compare(const void *p1, const void *p2)
{
	DumpableObject *obj1 = *(DumpableObject **) p1;
	DumpableObject *obj2 = *(DumpableObject **) p2;
	int			obj1_size = 0;
	int			obj2_size = 0;

	if (obj1->objType == DO_TABLE_DATA)
		obj1_size = ((TableDataInfo *) obj1)->tdtable->relpages;
	if (obj1->objType == DO_INDEX)
		obj1_size = ((IndxInfo *) obj1)->relpages;

	if (obj2->objType == DO_TABLE_DATA)
		obj2_size = ((TableDataInfo *) obj2)->tdtable->relpages;
	if (obj2->objType == DO_INDEX)
		obj2_size = ((IndxInfo *) obj2)->relpages;

	/* we want to see the biggest item go first */
	if (obj1_size > obj2_size)
		return -1;
	if (obj2_size > obj1_size)
		return 1;

	return 0;
}

/*
 * Sort the given objects into a type/name-based ordering
 *
 * Normally this is just the starting point for the dependency-based
 * ordering.
 */
void
sortDumpableObjectsByTypeName(Archive *fout, DumpableObject **objs, int numObjs)
{
	if (numObjs > 1)
		dbms_md_qsort((void*)fout, 
						(void *) objs, 
						numObjs, 
						sizeof(DumpableObject *),
						md_do_type_name_compare);
}

static int
md_do_type_name_compare(void *AH, const void *p1, const void *p2)
{
	DumpableObject *obj1 = *(DumpableObject *const *) p1;
	DumpableObject *obj2 = *(DumpableObject *const *) p2;
	Archive *fout = (Archive*)AH;
	int			cmpval;

	/* Sort by type */
	cmpval = dbObjectTypePriority[obj1->objType] -
		dbObjectTypePriority[obj2->objType];

	if (cmpval != 0)
		return cmpval;

	/*
	 * Sort by namespace.  Note that all objects of the same type should
	 * either have or not have a namespace link, so we needn't be fancy about
	 * cases where one link is null and the other not.
	 */
	if (obj1->namespace && obj2->namespace)
	{
		cmpval = strcmp(obj1->namespace->dobj.name,
						obj2->namespace->dobj.name);
		if (cmpval != 0)
			return cmpval;
	}

	/* Sort by name */
	cmpval = strcmp(obj1->name, obj2->name);
	if (cmpval != 0)
		return cmpval;

	/* To have a stable sort order, break ties for some object types */
	if (obj1->objType == DO_FUNC || obj1->objType == DO_AGG)
	{
		FuncInfo   *fobj1 = *(FuncInfo *const *) p1;
		FuncInfo   *fobj2 = *(FuncInfo *const *) p2;
		int			i;

		cmpval = fobj1->nargs - fobj2->nargs;
		if (cmpval != 0)
			return cmpval;
		for (i = 0; i < fobj1->nargs; i++)
		{
			TypeInfo   *argtype1 = findTypeByOid(fout, fobj1->argtypes[i]);
			TypeInfo   *argtype2 = findTypeByOid(fout, fobj2->argtypes[i]);

			if (argtype1 && argtype2)
			{
				if (argtype1->dobj.namespace && argtype2->dobj.namespace)
				{
					cmpval = strcmp(argtype1->dobj.namespace->dobj.name,
									argtype2->dobj.namespace->dobj.name);
					if (cmpval != 0)
						return cmpval;
				}
				cmpval = strcmp(argtype1->dobj.name, argtype2->dobj.name);
				if (cmpval != 0)
					return cmpval;
			}
		}
	}
	else if (obj1->objType == DO_OPERATOR)
	{
		OprInfo    *oobj1 = *(OprInfo *const *) p1;
		OprInfo    *oobj2 = *(OprInfo *const *) p2;

		/* oprkind is 'l', 'r', or 'b'; this sorts prefix, postfix, infix */
		cmpval = (oobj2->oprkind - oobj1->oprkind);
		if (cmpval != 0)
			return cmpval;
	}
	else if (obj1->objType == DO_ATTRDEF)
	{
		AttrDefInfo *adobj1 = *(AttrDefInfo *const *) p1;
		AttrDefInfo *adobj2 = *(AttrDefInfo *const *) p2;

		cmpval = (adobj1->adnum - adobj2->adnum);
		if (cmpval != 0)
			return cmpval;
	}

	/* Usually shouldn't get here, but if we do, sort by OID */
	return oidcmp(obj1->catId.oid, obj2->catId.oid);
}


/*
 * Sort the given objects into a safe dump order using dependency
 * information (to the extent we have it available).
 *
 * The DumpIds of the PRE_DATA_BOUNDARY and POST_DATA_BOUNDARY objects are
 * passed in separately, in case we need them during dependency loop repair.
 */
void
sortDumpableObjects(Archive *fout, DumpableObject **objs, int numObjs,
					DumpId preBoundaryId, DumpId postBoundaryId)
{
	DumpableObject **ordering;
	int			nOrdering;

	if (numObjs <= 0)			/* can't happen anymore ... */
		return;

	/*
	 * Saving the boundary IDs in static variables is a bit grotty, but seems
	 * better than adding them to parameter lists of subsidiary functions.
	 */
	fout->preDataBoundId = preBoundaryId;
	fout->postDataBoundId = postBoundaryId;

	ordering = (DumpableObject **) dbms_md_malloc0(fout->memory_ctx, numObjs * sizeof(DumpableObject *));
	while (!md_topo_sort(fout, objs, numObjs, ordering, &nOrdering))
		findDependencyLoops(fout, ordering, nOrdering, numObjs);

	memcpy(objs, ordering, numObjs * sizeof(DumpableObject *));

	pfree(ordering);
}

/*
 * md_topo_sort -- topological sort of a dump list
 *
 * Generate a re-ordering of the dump list that satisfies all the dependency
 * constraints shown in the dump list.  (Each such constraint is a fact of a
 * partial ordering.)  Minimize rearrangement of the list not needed to
 * achieve the partial ordering.
 *
 * The input is the list of numObjs objects in objs[].  This list is not
 * modified.
 *
 * Returns TRUE if able to build an ordering that satisfies all the
 * constraints, FALSE if not (there are contradictory constraints).
 *
 * On success (TRUE result), ordering[] is filled with a sorted array of
 * DumpableObject pointers, of length equal to the input list length.
 *
 * On failure (FALSE result), ordering[] is filled with an unsorted array of
 * DumpableObject pointers of length *nOrdering, listing the objects that
 * prevented the sort from being completed.  In general, these objects either
 * participate directly in a dependency cycle, or are depended on by objects
 * that are in a cycle.  (The latter objects are not actually problematic,
 * but it takes further analysis to identify which are which.)
 *
 * The caller is responsible for allocating sufficient space at *ordering.
 */
static bool
md_topo_sort(Archive *fout, DumpableObject **objs,
		 int numObjs,
		 DumpableObject **ordering, /* output argument */
		 int *nOrdering)		/* output argument */
{
	DumpId		maxDumpId = getMaxDumpId(fout);
	int		   *pendingHeap;
	int		   *beforeConstraints;
	int		   *idMap;
	DumpableObject *obj;
	int			heapLength;
	int			i,
				j,
				k;

	/*
	 * This is basically the same algorithm shown for topological sorting in
	 * Knuth's Volume 1.  However, we would like to minimize unnecessary
	 * rearrangement of the input ordering; that is, when we have a choice of
	 * which item to output next, we always want to take the one highest in
	 * the original list.  Therefore, instead of maintaining an unordered
	 * linked list of items-ready-to-output as Knuth does, we maintain a heap
	 * of their item numbers, which we can use as a priority queue.  This
	 * turns the algorithm from O(N) to O(N log N) because each insertion or
	 * removal of a heap item takes O(log N) time.  However, that's still
	 * plenty fast enough for this application.
	 */

	*nOrdering = numObjs;		/* for success return */

	/* Eliminate the null case */
	if (numObjs <= 0)
		return true;

	/* Create workspace for the above-described heap */
	pendingHeap = (int *) dbms_md_malloc0(fout->memory_ctx, numObjs * sizeof(int));

	/*
	 * Scan the constraints, and for each item in the input, generate a count
	 * of the number of constraints that say it must be before something else.
	 * The count for the item with dumpId j is stored in beforeConstraints[j].
	 * We also make a map showing the input-order index of the item with
	 * dumpId j.
	 */
	beforeConstraints = (int *) dbms_md_malloc0(fout->memory_ctx, (maxDumpId + 1) * sizeof(int));
	memset(beforeConstraints, 0, (maxDumpId + 1) * sizeof(int));
	idMap = (int *) dbms_md_malloc0(fout->memory_ctx, (maxDumpId + 1) * sizeof(int));
	for (i = 0; i < numObjs; i++)
	{
		obj = objs[i];
		j = obj->dumpId;
		if (j <= 0 || j > maxDumpId)
			elog(ERROR, "invalid dumpId %d\n", j);
		idMap[j] = i;
		for (j = 0; j < obj->nDeps; j++)
		{
			k = obj->dependencies[j];
			if (k <= 0 || k > maxDumpId)
				elog(ERROR, "invalid dependency %d\n", k);
			beforeConstraints[k]++;
		}
	}

	/*
	 * Now initialize the heap of items-ready-to-output by filling it with the
	 * indexes of items that already have beforeConstraints[id] == 0.
	 *
	 * The essential property of a heap is heap[(j-1)/2] >= heap[j] for each j
	 * in the range 1..heapLength-1 (note we are using 0-based subscripts
	 * here, while the discussion in Knuth assumes 1-based subscripts). So, if
	 * we simply enter the indexes into pendingHeap[] in decreasing order, we
	 * a-fortiori have the heap invariant satisfied at completion of this
	 * loop, and don't need to do any sift-up comparisons.
	 */
	heapLength = 0;
	for (i = numObjs; --i >= 0;)
	{
		if (beforeConstraints[objs[i]->dumpId] == 0)
			pendingHeap[heapLength++] = i;
	}

	/*--------------------
	 * Now emit objects, working backwards in the output list.  At each step,
	 * we use the priority heap to select the last item that has no remaining
	 * before-constraints.  We remove that item from the heap, output it to
	 * ordering[], and decrease the beforeConstraints count of each of the
	 * items it was constrained against.  Whenever an item's beforeConstraints
	 * count is thereby decreased to zero, we insert it into the priority heap
	 * to show that it is a candidate to output.  We are done when the heap
	 * becomes empty; if we have output every element then we succeeded,
	 * otherwise we failed.
	 * i = number of ordering[] entries left to output
	 * j = objs[] index of item we are outputting
	 * k = temp for scanning constraint list for item j
	 *--------------------
	 */
	i = numObjs;
	while (heapLength > 0)
	{
		/* Select object to output by removing largest heap member */
		j = removeHeapElement(pendingHeap, heapLength--);
		obj = objs[j];
		/* Output candidate to ordering[] */
		ordering[--i] = obj;
		/* Update beforeConstraints counts of its predecessors */
		for (k = 0; k < obj->nDeps; k++)
		{
			int			id = obj->dependencies[k];

			if ((--beforeConstraints[id]) == 0)
				addHeapElement(idMap[id], pendingHeap, heapLength++);
		}
	}

	/*
	 * If we failed, report the objects that couldn't be output; these are the
	 * ones with beforeConstraints[] still nonzero.
	 */
	if (i != 0)
	{
		k = 0;
		for (j = 1; j <= maxDumpId; j++)
		{
			if (beforeConstraints[j] != 0)
				ordering[k++] = objs[idMap[j]];
		}
		*nOrdering = k;
	}

	/* Done */
	pfree(pendingHeap);
	pfree(beforeConstraints);
	pfree(idMap);

	return (i == 0);
}

/*
 * Add an item to a heap (priority queue)
 *
 * heapLength is the current heap size; caller is responsible for increasing
 * its value after the call.  There must be sufficient storage at *heap.
 */
static void
addHeapElement(int val, int *heap, int heapLength)
{
	int			j;

	/*
	 * Sift-up the new entry, per Knuth 5.2.3 exercise 16. Note that Knuth is
	 * using 1-based array indexes, not 0-based.
	 */
	j = heapLength;
	while (j > 0)
	{
		int			i = (j - 1) >> 1;

		if (val <= heap[i])
			break;
		heap[j] = heap[i];
		j = i;
	}
	heap[j] = val;
}

/*
 * Remove the largest item present in a heap (priority queue)
 *
 * heapLength is the current heap size; caller is responsible for decreasing
 * its value after the call.
 *
 * We remove and return heap[0], which is always the largest element of
 * the heap, and then "sift up" to maintain the heap invariant.
 */
static int
removeHeapElement(int *heap, int heapLength)
{
	int			result = heap[0];
	int			val;
	int			i;

	if (--heapLength <= 0)
		return result;
	val = heap[heapLength];		/* value that must be reinserted */
	i = 0;						/* i is where the "hole" is */
	for (;;)
	{
		int			j = 2 * i + 1;

		if (j >= heapLength)
			break;
		if (j + 1 < heapLength &&
			heap[j] < heap[j + 1])
			j++;
		if (val >= heap[j])
			break;
		heap[i] = heap[j];
		i = j;
	}
	heap[i] = val;
	return result;
}

/*
 * findDependencyLoops - identify loops in md_topo_sort's failure output,
 *		and pass each such loop to repairDependencyLoop() for action
 *
 * In general there may be many loops in the set of objects returned by
 * md_topo_sort; for speed we should try to repair as many loops as we can
 * before trying md_topo_sort again.  We can safely repair loops that are
 * disjoint (have no members in common); if we find overlapping loops
 * then we repair only the first one found, because the action taken to
 * repair the first might have repaired the other as well.  (If not,
 * we'll fix it on the next go-round.)
 *
 * objs[] lists the objects md_topo_sort couldn't sort
 * nObjs is the number of such objects
 * totObjs is the total number of objects in the universe
 */
static void
findDependencyLoops(Archive *fout, DumpableObject **objs, int nObjs, int totObjs)
{
	/*
	 * We use three data structures here:
	 *
	 * processed[] is a bool array indexed by dump ID, marking the objects
	 * already processed during this invocation of findDependencyLoops().
	 *
	 * searchFailed[] is another array indexed by dump ID.  searchFailed[j] is
	 * set to dump ID k if we have proven that there is no dependency path
	 * leading from object j back to start point k.  This allows us to skip
	 * useless searching when there are multiple dependency paths from k to j,
	 * which is a common situation.  We could use a simple bool array for
	 * this, but then we'd need to re-zero it for each start point, resulting
	 * in O(N^2) zeroing work.  Using the start point's dump ID as the "true"
	 * value lets us skip clearing the array before we consider the next start
	 * point.
	 *
	 * workspace[] is an array of DumpableObject pointers, in which we try to
	 * build lists of objects constituting loops.  We make workspace[] large
	 * enough to hold all the objects in md_topo_sort's output, which is huge
	 * overkill in most cases but could theoretically be necessary if there is
	 * a single dependency chain linking all the objects.
	 */
	bool	   *processed;
	DumpId	   *searchFailed;
	DumpableObject **workspace;
	bool		fixedloop;
	int			i;

	processed = (bool *) dbms_md_malloc0(fout->memory_ctx, 
										(getMaxDumpId(fout) + 1) * sizeof(bool));
	searchFailed = (DumpId *) dbms_md_malloc0(fout->memory_ctx, 
										(getMaxDumpId(fout) + 1) * sizeof(DumpId));
	workspace = (DumpableObject **) dbms_md_malloc0(fout->memory_ctx, 
										totObjs * sizeof(DumpableObject *));
	fixedloop = false;

	for (i = 0; i < nObjs; i++)
	{
		DumpableObject *obj = objs[i];
		int			looplen;
		int			j;

		looplen = findLoop(fout, 
						   obj,
						   obj->dumpId,
						   processed,
						   searchFailed,
						   workspace,
						   0);

		if (looplen > 0)
		{
			/* Found a loop, repair it */
			repairDependencyLoop(fout, workspace, looplen);
			fixedloop = true;
			/* Mark loop members as processed */
			for (j = 0; j < looplen; j++)
				processed[workspace[j]->dumpId] = true;
		}
		else
		{
			/*
			 * There's no loop starting at this object, but mark it processed
			 * anyway.  This is not necessary for correctness, but saves later
			 * invocations of findLoop() from uselessly chasing references to
			 * such an object.
			 */
			processed[obj->dumpId] = true;
		}
	}

	/* We'd better have fixed at least one loop */
	if (!fixedloop)
		elog(ERROR, "could not identify dependency loop\n");

	dbms_md_free(workspace);
	dbms_md_free(searchFailed);
	dbms_md_free(processed);
}

/*
 * Recursively search for a circular dependency loop that doesn't include
 * any already-processed objects.
 *
 *	obj: object we are examining now
 *	startPoint: dumpId of starting object for the hoped-for circular loop
 *	processed[]: flag array marking already-processed objects
 *	searchFailed[]: flag array marking already-unsuccessfully-visited objects
 *	workspace[]: work array in which we are building list of loop members
 *	depth: number of valid entries in workspace[] at call
 *
 * On success, the length of the loop is returned, and workspace[] is filled
 * with pointers to the members of the loop.  On failure, we return 0.
 *
 * Note: it is possible that the given starting object is a member of more
 * than one cycle; if so, we will find an arbitrary one of the cycles.
 */
static int
findLoop(Archive *fout,
		 DumpableObject *obj,
		 DumpId startPoint,
		 bool *processed,
		 DumpId *searchFailed,
		 DumpableObject **workspace,
		 int depth)
{
	int			i;

	/*
	 * Reject if obj is already processed.  This test prevents us from finding
	 * loops that overlap previously-processed loops.
	 */
	if (processed[obj->dumpId])
		return 0;

	/*
	 * If we've already proven there is no path from this object back to the
	 * startPoint, forget it.
	 */
	if (searchFailed[obj->dumpId] == startPoint)
		return 0;

	/*
	 * Reject if obj is already present in workspace.  This test prevents us
	 * from going into infinite recursion if we are given a startPoint object
	 * that links to a cycle it's not a member of, and it guarantees that we
	 * can't overflow the allocated size of workspace[].
	 */
	for (i = 0; i < depth; i++)
	{
		if (workspace[i] == obj)
			return 0;
	}

	/*
	 * Okay, tentatively add obj to workspace
	 */
	workspace[depth++] = obj;

	/*
	 * See if we've found a loop back to the desired startPoint; if so, done
	 */
	for (i = 0; i < obj->nDeps; i++)
	{
		if (obj->dependencies[i] == startPoint)
			return depth;
	}

	/*
	 * Recurse down each outgoing branch
	 */
	for (i = 0; i < obj->nDeps; i++)
	{
		DumpableObject *nextobj = findObjectByDumpId(fout, obj->dependencies[i]);
		int			newDepth;

		if (!nextobj)
			continue;			/* ignore dependencies on undumped objects */
		newDepth = findLoop(fout, nextobj,
							startPoint,
							processed,
							searchFailed,
							workspace,
							depth);
		if (newDepth > 0)
			return newDepth;
	}

	/*
	 * Remember there is no path from here back to startPoint
	 */
	searchFailed[obj->dumpId] = startPoint;

	return 0;
}

/*
 * A user-defined datatype will have a dependency loop with each of its
 * I/O functions (since those have the datatype as input or output).
 * Similarly, a range type will have a loop with its canonicalize function,
 * if any.  Break the loop by making the function depend on the associated
 * shell type, instead.
 */
static void
repairTypeFuncLoop(Archive*fout, DumpableObject *typeobj, DumpableObject *funcobj)
{
	TypeInfo   *typeInfo = (TypeInfo *) typeobj;

	/* remove function's dependency on type */
	removeObjectDependency(funcobj, typeobj->dumpId);

	/* add function's dependency on shell type, instead */
	if (typeInfo->shellType)
	{
		addObjectDependency(fout, funcobj, typeInfo->shellType->dobj.dumpId);

		/*
		 * Mark shell type (always including the definition, as we need the
		 * shell type defined to identify the function fully) as to be dumped
		 * if any such function is
		 */
		if (funcobj->dump)
			typeInfo->shellType->dobj.dump = funcobj->dump |
				DUMP_COMPONENT_DEFINITION;
	}
}

/*
 * Because we force a view to depend on its ON SELECT rule, while there
 * will be an implicit dependency in the other direction, we need to break
 * the loop.  If there are no other objects in the loop then we can remove
 * the implicit dependency and leave the ON SELECT rule non-separate.
 * This applies to matviews, as well.
 */
static void
repairViewRuleLoop(Archive*fout, DumpableObject *viewobj,
				   DumpableObject *ruleobj)
{
	/* remove rule's dependency on view */
	removeObjectDependency(ruleobj, viewobj->dumpId);
	/* flags on the two objects are already set correctly for this case */
}

/*
 * However, if there are other objects in the loop, we must break the loop
 * by making the ON SELECT rule a separately-dumped object.
 *
 * Because findLoop() finds shorter cycles before longer ones, it's likely
 * that we will have previously fired repairViewRuleLoop() and removed the
 * rule's dependency on the view.  Put it back to ensure the rule won't be
 * emitted before the view.
 *
 * Note: this approach does *not* work for matviews, at the moment.
 */
static void
repairViewRuleMultiLoop(Archive *fout, DumpableObject *viewobj,
						DumpableObject *ruleobj)
{
	TableInfo  *viewinfo = (TableInfo *) viewobj;
	RuleInfo   *ruleinfo = (RuleInfo *) ruleobj;

	/* remove view's dependency on rule */
	removeObjectDependency(viewobj, ruleobj->dumpId);
	/* mark view to be printed with a dummy definition */
	viewinfo->dummy_view = true;
	/* mark rule as needing its own dump */
	ruleinfo->separate = true;
	/* put back rule's dependency on view */
	addObjectDependency(fout, ruleobj, viewobj->dumpId);
	/* now that rule is separate, it must be post-data */
	addObjectDependency(fout, ruleobj, fout->postDataBoundId);
}

/*
 * If a matview is involved in a multi-object loop, we can't currently fix
 * that by splitting off the rule.  As a stopgap, we try to fix it by
 * dropping the constraint that the matview be dumped in the pre-data section.
 * This is sufficient to handle cases where a matview depends on some unique
 * index, as can happen if it has a GROUP BY for example.
 *
 * Note that the "next object" is not necessarily the matview itself;
 * it could be the matview's rowtype, for example.  We may come through here
 * several times while removing all the pre-data linkages.
 */
static void
repairMatViewBoundaryMultiLoop(Archive*fout, 
								DumpableObject *matviewobj,
								DumpableObject *boundaryobj,
								DumpableObject *nextobj)
{
	TableInfo  *matviewinfo = (TableInfo *) matviewobj;

	/* remove boundary's dependency on object after it in loop */
	removeObjectDependency(boundaryobj, nextobj->dumpId);
	/* mark matview as postponed into post-data section */
	matviewinfo->postponed_def = true;
}

/*
 * Because we make tables depend on their CHECK constraints, while there
 * will be an automatic dependency in the other direction, we need to break
 * the loop.  If there are no other objects in the loop then we can remove
 * the automatic dependency and leave the CHECK constraint non-separate.
 */
static void
repairTableConstraintLoop(Archive*fout, DumpableObject *tableobj,
						  DumpableObject *constraintobj)
{
	/* remove constraint's dependency on table */
	removeObjectDependency(constraintobj, tableobj->dumpId);
}

/*
 * However, if there are other objects in the loop, we must break the loop
 * by making the CHECK constraint a separately-dumped object.
 *
 * Because findLoop() finds shorter cycles before longer ones, it's likely
 * that we will have previously fired repairTableConstraintLoop() and
 * removed the constraint's dependency on the table.  Put it back to ensure
 * the constraint won't be emitted before the table...
 */
static void
repairTableConstraintMultiLoop(Archive *fout, DumpableObject *tableobj,
							   DumpableObject *constraintobj)
{
	/* remove table's dependency on constraint */
	removeObjectDependency(tableobj, constraintobj->dumpId);
	/* mark constraint as needing its own dump */
	((ConstraintInfo *) constraintobj)->separate = true;
	/* put back constraint's dependency on table */
	addObjectDependency(fout, constraintobj, tableobj->dumpId);
	/* now that constraint is separate, it must be post-data */
	addObjectDependency(fout, constraintobj, fout->postDataBoundId);
}

/*
 * Attribute defaults behave exactly the same as CHECK constraints...
 */
static void
repairTableAttrDefLoop(Archive*fout, DumpableObject *tableobj,
					   DumpableObject *attrdefobj)
{
	/* remove attrdef's dependency on table */
	removeObjectDependency(attrdefobj, tableobj->dumpId);
}

static void
repairTableAttrDefMultiLoop(Archive *fout, DumpableObject *tableobj,
							DumpableObject *attrdefobj)
{
	/* remove table's dependency on attrdef */
	removeObjectDependency(tableobj, attrdefobj->dumpId);
	/* mark attrdef as needing its own dump */
	((AttrDefInfo *) attrdefobj)->separate = true;
	/* put back attrdef's dependency on table */
	addObjectDependency(fout, attrdefobj, tableobj->dumpId);
}

/*
 * CHECK constraints on domains work just like those on tables ...
 */
static void
repairDomainConstraintLoop(Archive*fout, DumpableObject *domainobj,
						   DumpableObject *constraintobj)
{
	/* remove constraint's dependency on domain */
	removeObjectDependency(constraintobj, domainobj->dumpId);
}

static void
repairDomainConstraintMultiLoop(Archive *fout, DumpableObject *domainobj,
								DumpableObject *constraintobj)
{
	/* remove domain's dependency on constraint */
	removeObjectDependency(domainobj, constraintobj->dumpId);
	/* mark constraint as needing its own dump */
	((ConstraintInfo *) constraintobj)->separate = true;
	/* put back constraint's dependency on domain */
	addObjectDependency(fout, constraintobj, domainobj->dumpId);
	/* now that constraint is separate, it must be post-data */
	addObjectDependency(fout, constraintobj, fout->postDataBoundId);
}

static void
repairIndexLoop(Archive*fout, DumpableObject *partedindex,
				DumpableObject *partindex)
{
	removeObjectDependency(partedindex, partindex->dumpId);
}

/*
 * Fix a dependency loop, or die trying ...
 *
 * This routine is mainly concerned with reducing the multiple ways that
 * a loop might appear to common cases, which it passes off to the
 * "fixer" routines above.
 */
static void
repairDependencyLoop(Archive *fout, DumpableObject **loop,
					 int nLoop)
{
	int			i,
				j;

	/* Datatype and one of its I/O or canonicalize functions */
	if (nLoop == 2 &&
		loop[0]->objType == DO_TYPE &&
		loop[1]->objType == DO_FUNC)
	{
		repairTypeFuncLoop(fout, loop[0], loop[1]);
		return;
	}
	if (nLoop == 2 &&
		loop[1]->objType == DO_TYPE &&
		loop[0]->objType == DO_FUNC)
	{
		repairTypeFuncLoop(fout, loop[1], loop[0]);
		return;
	}

	/* View (including matview) and its ON SELECT rule */
	if (nLoop == 2 &&
		loop[0]->objType == DO_TABLE &&
		loop[1]->objType == DO_RULE &&
		(((TableInfo *) loop[0])->relkind == RELKIND_VIEW ||
		 ((TableInfo *) loop[0])->relkind == RELKIND_MATVIEW) &&
		((RuleInfo *) loop[1])->ev_type == '1' &&
		((RuleInfo *) loop[1])->is_instead &&
		((RuleInfo *) loop[1])->ruletable == (TableInfo *) loop[0])
	{
		repairViewRuleLoop(fout, loop[0], loop[1]);
		return;
	}
	if (nLoop == 2 &&
		loop[1]->objType == DO_TABLE &&
		loop[0]->objType == DO_RULE &&
		(((TableInfo *) loop[1])->relkind == RELKIND_VIEW ||
		 ((TableInfo *) loop[1])->relkind == RELKIND_MATVIEW) &&
		((RuleInfo *) loop[0])->ev_type == '1' &&
		((RuleInfo *) loop[0])->is_instead &&
		((RuleInfo *) loop[0])->ruletable == (TableInfo *) loop[1])
	{
		repairViewRuleLoop(fout, loop[1], loop[0]);
		return;
	}

	/* Indirect loop involving view (but not matview) and ON SELECT rule */
	if (nLoop > 2)
	{
		for (i = 0; i < nLoop; i++)
		{
			if (loop[i]->objType == DO_TABLE &&
				((TableInfo *) loop[i])->relkind == RELKIND_VIEW)
			{
				for (j = 0; j < nLoop; j++)
				{
					if (loop[j]->objType == DO_RULE &&
						((RuleInfo *) loop[j])->ev_type == '1' &&
						((RuleInfo *) loop[j])->is_instead &&
						((RuleInfo *) loop[j])->ruletable == (TableInfo *) loop[i])
					{
						repairViewRuleMultiLoop(fout, loop[i], loop[j]);
						return;
					}
				}
			}
		}
	}

	/* Indirect loop involving matview and data boundary */
	if (nLoop > 2)
	{
		for (i = 0; i < nLoop; i++)
		{
			if (loop[i]->objType == DO_TABLE &&
				((TableInfo *) loop[i])->relkind == RELKIND_MATVIEW)
			{
				for (j = 0; j < nLoop; j++)
				{
					if (loop[j]->objType == DO_PRE_DATA_BOUNDARY)
					{
						DumpableObject *nextobj;

						nextobj = (j < nLoop - 1) ? loop[j + 1] : loop[0];
						repairMatViewBoundaryMultiLoop(fout, loop[i], loop[j],
													   nextobj);
						return;
					}
				}
			}
		}
	}

	/* Table and CHECK constraint */
	if (nLoop == 2 &&
		loop[0]->objType == DO_TABLE &&
		loop[1]->objType == DO_CONSTRAINT &&
		((ConstraintInfo *) loop[1])->contype == 'c' &&
		((ConstraintInfo *) loop[1])->contable == (TableInfo *) loop[0])
	{
		repairTableConstraintLoop(fout, loop[0], loop[1]);
		return;
	}
	if (nLoop == 2 &&
		loop[1]->objType == DO_TABLE &&
		loop[0]->objType == DO_CONSTRAINT &&
		((ConstraintInfo *) loop[0])->contype == 'c' &&
		((ConstraintInfo *) loop[0])->contable == (TableInfo *) loop[1])
	{
		repairTableConstraintLoop(fout, loop[1], loop[0]);
		return;
	}

	/* Indirect loop involving table and CHECK constraint */
	if (nLoop > 2)
	{
		for (i = 0; i < nLoop; i++)
		{
			if (loop[i]->objType == DO_TABLE)
			{
				for (j = 0; j < nLoop; j++)
				{
					if (loop[j]->objType == DO_CONSTRAINT &&
						((ConstraintInfo *) loop[j])->contype == 'c' &&
						((ConstraintInfo *) loop[j])->contable == (TableInfo *) loop[i])
					{
						repairTableConstraintMultiLoop(fout, loop[i], loop[j]);
						return;
					}
				}
			}
		}
	}

	/* Table and attribute default */
	if (nLoop == 2 &&
		loop[0]->objType == DO_TABLE &&
		loop[1]->objType == DO_ATTRDEF &&
		((AttrDefInfo *) loop[1])->adtable == (TableInfo *) loop[0])
	{
		repairTableAttrDefLoop(fout, loop[0], loop[1]);
		return;
	}
	if (nLoop == 2 &&
		loop[1]->objType == DO_TABLE &&
		loop[0]->objType == DO_ATTRDEF &&
		((AttrDefInfo *) loop[0])->adtable == (TableInfo *) loop[1])
	{
		repairTableAttrDefLoop(fout, loop[1], loop[0]);
		return;
	}

	/* index on partitioned table and corresponding index on partition */
	if (nLoop == 2 &&
		loop[0]->objType == DO_INDEX &&
		loop[1]->objType == DO_INDEX)
	{
		if (((IndxInfo *) loop[0])->parentidx == loop[1]->catId.oid)
		{
			repairIndexLoop(fout, loop[0], loop[1]);
			return;
		}
		else if (((IndxInfo *) loop[1])->parentidx == loop[0]->catId.oid)
		{
			repairIndexLoop(fout, loop[1], loop[0]);
			return;
		}
	}

	/* Indirect loop involving table and attribute default */
	if (nLoop > 2)
	{
		for (i = 0; i < nLoop; i++)
		{
			if (loop[i]->objType == DO_TABLE)
			{
				for (j = 0; j < nLoop; j++)
				{
					if (loop[j]->objType == DO_ATTRDEF &&
						((AttrDefInfo *) loop[j])->adtable == (TableInfo *) loop[i])
					{
						repairTableAttrDefMultiLoop(fout, loop[i], loop[j]);
						return;
					}
				}
			}
		}
	}

	/* Domain and CHECK constraint */
	if (nLoop == 2 &&
		loop[0]->objType == DO_TYPE &&
		loop[1]->objType == DO_CONSTRAINT &&
		((ConstraintInfo *) loop[1])->contype == 'c' &&
		((ConstraintInfo *) loop[1])->condomain == (TypeInfo *) loop[0])
	{
		repairDomainConstraintLoop(fout, loop[0], loop[1]);
		return;
	}
	if (nLoop == 2 &&
		loop[1]->objType == DO_TYPE &&
		loop[0]->objType == DO_CONSTRAINT &&
		((ConstraintInfo *) loop[0])->contype == 'c' &&
		((ConstraintInfo *) loop[0])->condomain == (TypeInfo *) loop[1])
	{
		repairDomainConstraintLoop(fout, loop[1], loop[0]);
		return;
	}

	/* Indirect loop involving domain and CHECK constraint */
	if (nLoop > 2)
	{
		for (i = 0; i < nLoop; i++)
		{
			if (loop[i]->objType == DO_TYPE)
			{
				for (j = 0; j < nLoop; j++)
				{
					if (loop[j]->objType == DO_CONSTRAINT &&
						((ConstraintInfo *) loop[j])->contype == 'c' &&
						((ConstraintInfo *) loop[j])->condomain == (TypeInfo *) loop[i])
					{
						repairDomainConstraintMultiLoop(fout, loop[i], loop[j]);
						return;
					}
				}
			}
		}
	}

	/*
	 * If all the objects are TABLE_DATA items, what we must have is a
	 * circular set of foreign key constraints (or a single self-referential
	 * table).  Print an appropriate complaint and break the loop arbitrarily.
	 */
	for (i = 0; i < nLoop; i++)
	{
		if (loop[i]->objType != DO_TABLE_DATA)
			break;
	}
	if (i >= nLoop)
	{
		write_msg(NULL, ngettext("NOTICE: there are circular foreign-key constraints on this table:\n",
								 "NOTICE: there are circular foreign-key constraints among these tables:\n",
								 nLoop));
		for (i = 0; i < nLoop; i++)
			write_msg(NULL, "  %s\n", loop[i]->name);
		write_msg(NULL, "You might not be able to restore the dump without using --disable-triggers or temporarily dropping the constraints.\n");
		write_msg(NULL, "Consider using a full dump instead of a --data-only dump to avoid this problem.\n");
		if (nLoop > 1)
			removeObjectDependency(loop[0], loop[1]->dumpId);
		else					/* must be a self-dependency */
			removeObjectDependency(loop[0], loop[0]->dumpId);
		return;
	}

	/*
	 * If we can't find a principled way to break the loop, complain and break
	 * it in an arbitrary fashion.
	 */
	write_msg(modulename, "WARNING: could not resolve dependency loop among these items:\n");
	for (i = 0; i < nLoop; i++)
	{
		char		buf[1024];

		describeDumpableObject(loop[i], buf, sizeof(buf));
		write_msg(modulename, "  %s\n", buf);
	}

	if (nLoop > 1)
		removeObjectDependency(loop[0], loop[1]->dumpId);
	else						/* must be a self-dependency */
		removeObjectDependency(loop[0], loop[0]->dumpId);
}

/*
 * Describe a dumpable object usefully for errors
 *
 * This should probably go somewhere else...
 */
static void
describeDumpableObject(DumpableObject *obj, char *buf, int bufsize)
{
	switch (obj->objType)
	{
		case DO_NAMESPACE:
			snprintf(buf, bufsize,
					 "SCHEMA %s  (ID %d OID %u)",
					 obj->name, obj->dumpId, obj->catId.oid);
			return;
		case DO_EXTENSION:
			snprintf(buf, bufsize,
					 "EXTENSION %s  (ID %d OID %u)",
					 obj->name, obj->dumpId, obj->catId.oid);
			return;
		case DO_TYPE:
			snprintf(buf, bufsize,
					 "TYPE %s  (ID %d OID %u)",
					 obj->name, obj->dumpId, obj->catId.oid);
			return;
		case DO_SHELL_TYPE:
			snprintf(buf, bufsize,
					 "SHELL TYPE %s  (ID %d OID %u)",
					 obj->name, obj->dumpId, obj->catId.oid);
			return;
		case DO_FUNC:
			snprintf(buf, bufsize,
					 "FUNCTION %s  (ID %d OID %u)",
					 obj->name, obj->dumpId, obj->catId.oid);
			return;
		case DO_AGG:
			snprintf(buf, bufsize,
					 "AGGREGATE %s  (ID %d OID %u)",
					 obj->name, obj->dumpId, obj->catId.oid);
			return;
		case DO_OPERATOR:
			snprintf(buf, bufsize,
					 "OPERATOR %s  (ID %d OID %u)",
					 obj->name, obj->dumpId, obj->catId.oid);
			return;
		case DO_ACCESS_METHOD:
			snprintf(buf, bufsize,
					 "ACCESS METHOD %s  (ID %d OID %u)",
					 obj->name, obj->dumpId, obj->catId.oid);
			return;
		case DO_OPCLASS:
			snprintf(buf, bufsize,
					 "OPERATOR CLASS %s  (ID %d OID %u)",
					 obj->name, obj->dumpId, obj->catId.oid);
			return;
		case DO_OPFAMILY:
			snprintf(buf, bufsize,
					 "OPERATOR FAMILY %s  (ID %d OID %u)",
					 obj->name, obj->dumpId, obj->catId.oid);
			return;
		case DO_COLLATION:
			snprintf(buf, bufsize,
					 "COLLATION %s  (ID %d OID %u)",
					 obj->name, obj->dumpId, obj->catId.oid);
			return;
		case DO_CONVERSION:
			snprintf(buf, bufsize,
					 "CONVERSION %s  (ID %d OID %u)",
					 obj->name, obj->dumpId, obj->catId.oid);
			return;
		case DO_TABLE:
			snprintf(buf, bufsize,
					 "TABLE %s  (ID %d OID %u)",
					 obj->name, obj->dumpId, obj->catId.oid);
			return;
		case DO_ATTRDEF:
			snprintf(buf, bufsize,
					 "ATTRDEF %s.%s  (ID %d OID %u)",
					 ((AttrDefInfo *) obj)->adtable->dobj.name,
					 ((AttrDefInfo *) obj)->adtable->attnames[((AttrDefInfo *) obj)->adnum - 1],
					 obj->dumpId, obj->catId.oid);
			return;
		case DO_INDEX:
			snprintf(buf, bufsize,
					 "INDEX %s  (ID %d OID %u)",
					 obj->name, obj->dumpId, obj->catId.oid);
			return;
		case DO_INDEX_ATTACH:
			snprintf(buf, bufsize,
					 "INDEX ATTACH %s  (ID %d)",
					 obj->name, obj->dumpId);
			return;
		case DO_STATSEXT:
			snprintf(buf, bufsize,
					 "STATISTICS %s  (ID %d OID %u)",
					 obj->name, obj->dumpId, obj->catId.oid);
			return;
		case DO_REFRESH_MATVIEW:
			snprintf(buf, bufsize,
					 "REFRESH MATERIALIZED VIEW %s  (ID %d OID %u)",
					 obj->name, obj->dumpId, obj->catId.oid);
			return;
		case DO_RULE:
			snprintf(buf, bufsize,
					 "RULE %s  (ID %d OID %u)",
					 obj->name, obj->dumpId, obj->catId.oid);
			return;
		case DO_TRIGGER:
			snprintf(buf, bufsize,
					 "TRIGGER %s  (ID %d OID %u)",
					 obj->name, obj->dumpId, obj->catId.oid);
			return;
		case DO_EVENT_TRIGGER:
			snprintf(buf, bufsize,
					 "EVENT TRIGGER %s (ID %d OID %u)",
					 obj->name, obj->dumpId, obj->catId.oid);
			return;
		case DO_CONSTRAINT:
			snprintf(buf, bufsize,
					 "CONSTRAINT %s  (ID %d OID %u)",
					 obj->name, obj->dumpId, obj->catId.oid);
			return;
		case DO_FK_CONSTRAINT:
			snprintf(buf, bufsize,
					 "FK CONSTRAINT %s  (ID %d OID %u)",
					 obj->name, obj->dumpId, obj->catId.oid);
			return;
		case DO_PROCLANG:
			snprintf(buf, bufsize,
					 "PROCEDURAL LANGUAGE %s  (ID %d OID %u)",
					 obj->name, obj->dumpId, obj->catId.oid);
			return;
		case DO_CAST:
			snprintf(buf, bufsize,
					 "CAST %u to %u  (ID %d OID %u)",
					 ((CastInfo *) obj)->castsource,
					 ((CastInfo *) obj)->casttarget,
					 obj->dumpId, obj->catId.oid);
			return;
		case DO_TRANSFORM:
			snprintf(buf, bufsize,
					 "TRANSFORM %u lang %u  (ID %d OID %u)",
					 ((TransformInfo *) obj)->trftype,
					 ((TransformInfo *) obj)->trflang,
					 obj->dumpId, obj->catId.oid);
			return;
		case DO_TABLE_DATA:
			snprintf(buf, bufsize,
					 "TABLE DATA %s  (ID %d OID %u)",
					 obj->name, obj->dumpId, obj->catId.oid);
			return;
		case DO_SEQUENCE_SET:
			snprintf(buf, bufsize,
					 "SEQUENCE SET %s  (ID %d OID %u)",
					 obj->name, obj->dumpId, obj->catId.oid);
			return;
		case DO_DUMMY_TYPE:
			snprintf(buf, bufsize,
					 "DUMMY TYPE %s  (ID %d OID %u)",
					 obj->name, obj->dumpId, obj->catId.oid);
			return;
		case DO_TSPARSER:
			snprintf(buf, bufsize,
					 "TEXT SEARCH PARSER %s  (ID %d OID %u)",
					 obj->name, obj->dumpId, obj->catId.oid);
			return;
		case DO_TSDICT:
			snprintf(buf, bufsize,
					 "TEXT SEARCH DICTIONARY %s  (ID %d OID %u)",
					 obj->name, obj->dumpId, obj->catId.oid);
			return;
		case DO_TSTEMPLATE:
			snprintf(buf, bufsize,
					 "TEXT SEARCH TEMPLATE %s  (ID %d OID %u)",
					 obj->name, obj->dumpId, obj->catId.oid);
			return;
		case DO_TSCONFIG:
			snprintf(buf, bufsize,
					 "TEXT SEARCH CONFIGURATION %s  (ID %d OID %u)",
					 obj->name, obj->dumpId, obj->catId.oid);
			return;
		case DO_FDW:
			snprintf(buf, bufsize,
					 "FOREIGN DATA WRAPPER %s  (ID %d OID %u)",
					 obj->name, obj->dumpId, obj->catId.oid);
			return;
		case DO_FOREIGN_SERVER:
			snprintf(buf, bufsize,
					 "FOREIGN SERVER %s  (ID %d OID %u)",
					 obj->name, obj->dumpId, obj->catId.oid);
			return;
		case DO_DEFAULT_ACL:
			snprintf(buf, bufsize,
					 "DEFAULT ACL %s  (ID %d OID %u)",
					 obj->name, obj->dumpId, obj->catId.oid);
			return;
		case DO_BLOB:
			snprintf(buf, bufsize,
					 "BLOB  (ID %d OID %u)",
					 obj->dumpId, obj->catId.oid);
			return;
		case DO_BLOB_DATA:
			snprintf(buf, bufsize,
					 "BLOB DATA  (ID %d)",
					 obj->dumpId);
			return;
		case DO_POLICY:
			snprintf(buf, bufsize,
					 "POLICY (ID %d OID %u)",
					 obj->dumpId, obj->catId.oid);
			return;
		case DO_PUBLICATION:
			snprintf(buf, bufsize,
					 "PUBLICATION (ID %d OID %u)",
					 obj->dumpId, obj->catId.oid);
			return;
		case DO_PUBLICATION_REL:
			snprintf(buf, bufsize,
					 "PUBLICATION TABLE (ID %d OID %u)",
					 obj->dumpId, obj->catId.oid);
			return;
		case DO_SUBSCRIPTION:
			snprintf(buf, bufsize,
					 "SUBSCRIPTION (ID %d OID %u)",
					 obj->dumpId, obj->catId.oid);
			return;
		case DO_PRE_DATA_BOUNDARY:
			snprintf(buf, bufsize,
					 "PRE-DATA BOUNDARY  (ID %d)",
					 obj->dumpId);
			return;
		case DO_POST_DATA_BOUNDARY:
			snprintf(buf, bufsize,
					 "POST-DATA BOUNDARY  (ID %d)",
					 obj->dumpId);
			return;
	}
	/* shouldn't get here */
	snprintf(buf, bufsize,
			 "object type %d  (ID %d OID %u)",
			 (int) obj->objType,
			 obj->dumpId, obj->catId.oid);
}
