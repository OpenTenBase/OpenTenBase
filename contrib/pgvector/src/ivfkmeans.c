#include "postgres.h"

#include <float.h>
#include <limits.h>
#include <math.h>

#include "access/genam.h"
#include "fmgr.h"
#include "ivfflat.h"
#include "miscadmin.h"
#include "utils/memutils.h"
#include "utils/relcache.h"

#if PG_VERSION_NUM >= 160000
#include "varatt.h"
#endif

typedef struct IvfflatKmeansMemoryEstimate
{
	int			numSamples;
	int			numCenters;
	int			dimensions;
	int			numGroups;
	Size		memoryUsed;
	Size		initTotalSize;
	Size		mainTotalSize;
	Size		totalSize;
	Size		initWeightSize;
	Size		newCentersSize;
	Size		aggSize;
	Size		centerCountsSize;
	Size		closestCentersSize;
	Size		lowerBoundSize;
	Size		upperBoundSize;
	Size		sSize;
	Size		halfcdistSize;
	Size		newcdistSize;
	Size		groupLowerBoundSize;
	Size		centerGroupSize;
	Size		groupOffsetsSize;
	Size		groupMembersSize;
	Size		groupCountsSize;
	Size		centerDriftSize;
	Size		groupMaxDriftSize;
	Size		tmpGroupFloatSize;
	Size		tmpGroupIntSize;
	Size		tmpGroupBoolSize;
	Size		groupCentersSize;
	Size		groupAggSize;
	Size		groupBuildMembersPosSize;
}			IvfflatKmeansMemoryEstimate;

typedef enum IvfflatKmeansAlgorithm
{
	IVFFLAT_KMEANS_ELKAN,
	IVFFLAT_KMEANS_YINYANG
}			IvfflatKmeansAlgorithm;

typedef struct IvfflatKmeansSelection
{
	IvfflatKmeansAlgorithm algorithm;
	IvfflatKmeansMemoryEstimate elkanEstimate;
	IvfflatKmeansMemoryEstimate yinyangEstimate;
}			IvfflatKmeansSelection;

/*
 * Convert to the same kB unit used by IvfflatCheckMemoryUsage.
 */
static Size
KmeansMemorySizeToKB(Size totalSize)
{
	return totalSize / 1024;
}

/*
 * Convert kB to a ceiling MB value for human-readable diagnostics.
 */
static Size
KmeansMemoryKBToMB(Size totalKB)
{
	return totalKB / 1024 + (totalKB % 1024 != 0);
}

/*
 * Convert bytes to a fit-semantics-aligned MB display value.
 */
static Size
KmeansMemorySizeToMB(Size totalSize)
{
	return KmeansMemoryKBToMB(KmeansMemorySizeToKB(totalSize));
}

/*
 * Check memory against maintenance_work_mem with existing pgvector semantics.
 */
static bool
IvfflatKmeansMemoryFits(Size totalSize)
{
	return totalSize / 1024 <= (Size) maintenance_work_mem;
}

/*
 * Estimate memory for Elkan k-means
 */
static void
EstimateElkanKmeansMemory(int numSamples, int numCenters, int dimensions, Size itemsize, Size memoryUsed, IvfflatKmeansMemoryEstimate * estimate)
{
	Size		totalSize = memoryUsed;

	memset(estimate, 0, sizeof(IvfflatKmeansMemoryEstimate));
	estimate->numSamples = numSamples;
	estimate->numCenters = numCenters;
	estimate->dimensions = dimensions;
	estimate->memoryUsed = memoryUsed;

	estimate->initWeightSize = mul_size(sizeof(float), numSamples);
	estimate->newCentersSize = VECTOR_ARRAY_SIZE(numCenters, itemsize);
	estimate->aggSize = mul_size(sizeof(float), mul_size(numCenters, dimensions));
	estimate->centerCountsSize = mul_size(sizeof(int), numCenters);
	estimate->closestCentersSize = mul_size(sizeof(int), numSamples);
	estimate->lowerBoundSize = mul_size(sizeof(float), mul_size(numSamples, numCenters));
	estimate->upperBoundSize = mul_size(sizeof(float), numSamples);
	estimate->sSize = mul_size(sizeof(float), numCenters);
	estimate->halfcdistSize = mul_size(sizeof(float), mul_size(numCenters, numCenters));
	estimate->newcdistSize = mul_size(sizeof(float), numCenters);

	totalSize = add_size(totalSize, estimate->newCentersSize);
	totalSize = add_size(totalSize, estimate->aggSize);
	totalSize = add_size(totalSize, estimate->centerCountsSize);
	totalSize = add_size(totalSize, estimate->closestCentersSize);
	totalSize = add_size(totalSize, estimate->lowerBoundSize);
	totalSize = add_size(totalSize, estimate->upperBoundSize);
	totalSize = add_size(totalSize, estimate->sSize);
	totalSize = add_size(totalSize, estimate->halfcdistSize);
	totalSize = add_size(totalSize, estimate->newcdistSize);

	estimate->mainTotalSize = totalSize;
	estimate->initTotalSize = add_size(totalSize, estimate->initWeightSize);
	estimate->totalSize = estimate->initTotalSize;
}

/*
 * Estimate memory for Yinyang k-means
 */
static void
EstimateYinyangKmeansMemory(int numSamples, int numCenters, int dimensions, Size itemsize, Size memoryUsed, IvfflatKmeansMemoryEstimate * estimate)
{
	Size		mainTotalSize = memoryUsed;

	memset(estimate, 0, sizeof(IvfflatKmeansMemoryEstimate));
	estimate->numSamples = numSamples;
	estimate->numCenters = numCenters;
	estimate->dimensions = dimensions;
	estimate->numGroups = Max(numCenters / 10, 1);
	estimate->memoryUsed = memoryUsed;

	estimate->initWeightSize = mul_size(sizeof(float), numSamples);
	estimate->newCentersSize = VECTOR_ARRAY_SIZE(numCenters, itemsize);
	estimate->aggSize = mul_size(sizeof(float), mul_size(numCenters, dimensions));
	estimate->centerCountsSize = mul_size(sizeof(int), numCenters);
	estimate->closestCentersSize = mul_size(sizeof(int), numSamples);
	estimate->upperBoundSize = mul_size(sizeof(float), numSamples);
	estimate->groupLowerBoundSize = mul_size(sizeof(float), mul_size(numSamples, estimate->numGroups));
	estimate->centerGroupSize = mul_size(sizeof(int), numCenters);
	estimate->groupOffsetsSize = mul_size(sizeof(int), estimate->numGroups + 1);
	estimate->groupMembersSize = mul_size(sizeof(int), numCenters);
	estimate->groupCountsSize = mul_size(sizeof(int), estimate->numGroups);
	estimate->centerDriftSize = mul_size(sizeof(float), numCenters);
	estimate->groupMaxDriftSize = mul_size(sizeof(float), estimate->numGroups);
	estimate->tmpGroupFloatSize = mul_size(sizeof(float), estimate->numGroups);
	estimate->tmpGroupIntSize = mul_size(sizeof(int), estimate->numGroups);
	estimate->tmpGroupBoolSize = mul_size(sizeof(bool), estimate->numGroups);
	estimate->groupCentersSize = VECTOR_ARRAY_SIZE(estimate->numGroups, itemsize);
	estimate->groupAggSize = mul_size(sizeof(float), mul_size(estimate->numGroups, dimensions));
	estimate->groupBuildMembersPosSize = mul_size(sizeof(int), estimate->numGroups);

	mainTotalSize = add_size(mainTotalSize, estimate->newCentersSize);
	mainTotalSize = add_size(mainTotalSize, estimate->aggSize);
	mainTotalSize = add_size(mainTotalSize, estimate->centerCountsSize);
	mainTotalSize = add_size(mainTotalSize, estimate->closestCentersSize);
	mainTotalSize = add_size(mainTotalSize, estimate->upperBoundSize);
	mainTotalSize = add_size(mainTotalSize, estimate->groupLowerBoundSize);
	mainTotalSize = add_size(mainTotalSize, estimate->centerGroupSize);
	mainTotalSize = add_size(mainTotalSize, estimate->groupOffsetsSize);
	mainTotalSize = add_size(mainTotalSize, estimate->groupMembersSize);
	mainTotalSize = add_size(mainTotalSize, estimate->groupCountsSize);
	mainTotalSize = add_size(mainTotalSize, estimate->centerDriftSize);
	mainTotalSize = add_size(mainTotalSize, estimate->groupMaxDriftSize);
	mainTotalSize = add_size(mainTotalSize, estimate->tmpGroupFloatSize);
	mainTotalSize = add_size(mainTotalSize, estimate->tmpGroupFloatSize);
	mainTotalSize = add_size(mainTotalSize, estimate->tmpGroupIntSize);
	mainTotalSize = add_size(mainTotalSize, estimate->tmpGroupBoolSize);
	mainTotalSize = add_size(mainTotalSize, estimate->groupCentersSize);
	mainTotalSize = add_size(mainTotalSize, estimate->groupAggSize);
	mainTotalSize = add_size(mainTotalSize, estimate->groupBuildMembersPosSize);

	estimate->initTotalSize = add_size(memoryUsed, estimate->initWeightSize);
	estimate->mainTotalSize = mainTotalSize;
	estimate->totalSize = Max(estimate->initTotalSize, estimate->mainTotalSize);
}

/*
 * Report when no k-means implementation fits the memory budget
 */
static void
ReportKmeansMemoryError(const IvfflatKmeansSelection * selection)
{
	const IvfflatKmeansMemoryEstimate *elkan = &selection->elkanEstimate;
	const IvfflatKmeansMemoryEstimate *yinyang = &selection->yinyangEstimate;
	Size		requiredSize = Min(elkan->totalSize, yinyang->totalSize);

	ereport(ERROR,
			(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
			 errmsg("memory required is %zu kB (%zu MB), maintenance_work_mem is %d kB (%zu MB)",
					KmeansMemorySizeToKB(requiredSize), KmeansMemorySizeToMB(requiredSize),
					maintenance_work_mem, KmeansMemoryKBToMB((Size) maintenance_work_mem)),
			 errdetail("IVFFlat k-means memory estimates: samples=%d, lists=%d, dimensions=%d, yinyang_groups=%d, maintenance_work_mem=%d kB (%zu MB), Elkan=%zu kB (%zu MB), Yinyang=%zu kB (%zu MB); Elkan lowerBound=%zu kB (%zu MB), Yinyang groupLowerBound=%zu kB (%zu MB).",
					   yinyang->numSamples, yinyang->numCenters, yinyang->dimensions, yinyang->numGroups,
					   maintenance_work_mem, KmeansMemoryKBToMB((Size) maintenance_work_mem),
					   KmeansMemorySizeToKB(elkan->totalSize), KmeansMemorySizeToMB(elkan->totalSize),
					   KmeansMemorySizeToKB(yinyang->totalSize), KmeansMemorySizeToMB(yinyang->totalSize),
					   KmeansMemorySizeToKB(elkan->lowerBoundSize), KmeansMemorySizeToMB(elkan->lowerBoundSize),
					   KmeansMemorySizeToKB(yinyang->groupLowerBoundSize), KmeansMemorySizeToMB(yinyang->groupLowerBoundSize)),
			 errhint("Increase maintenance_work_mem above the smaller estimated requirement, or reduce lists.")));
}

/*
 * Select k-means algorithm based only on memory feasibility.
 */
static IvfflatKmeansSelection
SelectKmeansAlgorithm(VectorArray samples, VectorArray centers, Size memoryUsed)
{
	IvfflatKmeansSelection selection;
	bool		elkanFits;
	bool		yinyangFits;

	memset(&selection, 0, sizeof(IvfflatKmeansSelection));

	EstimateElkanKmeansMemory(samples->length, centers->maxlen, centers->dim, centers->itemsize, memoryUsed, &selection.elkanEstimate);
	EstimateYinyangKmeansMemory(samples->length, centers->maxlen, centers->dim, centers->itemsize, memoryUsed, &selection.yinyangEstimate);

	elkanFits = IvfflatKmeansMemoryFits(selection.elkanEstimate.totalSize);
	yinyangFits = IvfflatKmeansMemoryFits(selection.yinyangEstimate.totalSize);

	if (elkanFits)
	{
		selection.algorithm = IVFFLAT_KMEANS_ELKAN;
		ereport(DEBUG1,
				(errmsg("using Elkan k-means for IVFFlat build: Elkan fits maintenance_work_mem (Elkan=%zu kB/%zu MB, Yinyang=%zu kB/%zu MB, maintenance_work_mem=%d kB/%zu MB)",
						KmeansMemorySizeToKB(selection.elkanEstimate.totalSize),
						KmeansMemorySizeToMB(selection.elkanEstimate.totalSize),
						KmeansMemorySizeToKB(selection.yinyangEstimate.totalSize),
						KmeansMemorySizeToMB(selection.yinyangEstimate.totalSize),
						maintenance_work_mem, KmeansMemoryKBToMB((Size) maintenance_work_mem))));
	}
	else if (yinyangFits)
	{
		selection.algorithm = IVFFLAT_KMEANS_YINYANG;
		ereport(DEBUG1,
				(errmsg("using Yinyang k-means for IVFFlat build: Elkan exceeds maintenance_work_mem and Yinyang fits (Elkan=%zu kB/%zu MB, Yinyang=%zu kB/%zu MB, maintenance_work_mem=%d kB/%zu MB)",
						KmeansMemorySizeToKB(selection.elkanEstimate.totalSize),
						KmeansMemorySizeToMB(selection.elkanEstimate.totalSize),
						KmeansMemorySizeToKB(selection.yinyangEstimate.totalSize),
						KmeansMemorySizeToMB(selection.yinyangEstimate.totalSize),
						maintenance_work_mem, KmeansMemoryKBToMB((Size) maintenance_work_mem))));
	}
	else
	{
		selection.algorithm = IVFFLAT_KMEANS_YINYANG;
		ReportKmeansMemoryError(&selection);
	}

	return selection;
}

/*
 * Initialize with kmeans++
 *
 * https://theory.stanford.edu/~sergei/papers/kMeansPP-soda.pdf
 */
static void
InitCenters(Relation index, VectorArray samples, VectorArray centers, float *lowerBound, Size weightSize)
{
	FmgrInfo   *procinfo;
	Oid			collation;
	float	   *weight = palloc(weightSize);
	int			numCenters = centers->maxlen;
	int			numSamples = samples->length;

	procinfo = index_getprocinfo(index, 1, IVFFLAT_KMEANS_DISTANCE_PROC);
	collation = index->rd_indcollation[0];

	/* Choose an initial center uniformly at random */
	VectorArraySet(centers, 0, VectorArrayGet(samples, RandomInt() % samples->length));
	centers->length++;

	for (int i = 0; i < numSamples; i++)
		weight[i] = FLT_MAX;

	for (int i = 0; i < numCenters; i++)
	{
		int			j;
		double		sum;
		double		choice;

		CHECK_FOR_INTERRUPTS();

		sum = 0.0;

		for (j = 0; j < numSamples; j++)
		{
			Datum		vec = PointerGetDatum(VectorArrayGet(samples, j));
			double		distance;

			/* Only need to compute distance for new center */
			/* TODO Use triangle inequality to reduce distance calculations */
			distance = DatumGetFloat8(FunctionCall2Coll(procinfo, collation, vec, PointerGetDatum(VectorArrayGet(centers, i))));

			/* Set lower bound */
			lowerBound[(Size) j * numCenters + i] = distance;

			/* Use distance squared for weighted probability distribution */
			distance *= distance;

			if (distance < weight[j])
				weight[j] = distance;

			sum += weight[j];
		}

		/* Only compute lower bound on last iteration */
		if (i + 1 == numCenters)
			break;

		/* Choose new center using weighted probability distribution. */
		choice = sum * RandomDouble();
		for (j = 0; j < numSamples - 1; j++)
		{
			choice -= weight[j];
			if (choice <= 0)
				break;
		}

		VectorArraySet(centers, i + 1, VectorArrayGet(samples, j));
		centers->length++;
	}

	pfree(weight);
}

/*
 * Initialize with kmeans++ without storing per-sample-per-center bounds.
 */
static void
InitCentersLowMemory(Relation index, VectorArray samples, VectorArray centers, Size weightSize)
{
	FmgrInfo   *procinfo;
	Oid			collation;
	float	   *weight = palloc(weightSize);
	int			numCenters = centers->maxlen;
	int			numSamples = samples->length;

	procinfo = index_getprocinfo(index, 1, IVFFLAT_KMEANS_DISTANCE_PROC);
	collation = index->rd_indcollation[0];

	/* Choose an initial center uniformly at random */
	VectorArraySet(centers, 0, VectorArrayGet(samples, RandomInt() % samples->length));
	centers->length++;

	for (int i = 0; i < numSamples; i++)
		weight[i] = FLT_MAX;

	for (int i = 0; i < numCenters; i++)
	{
		int			j;
		double		sum;
		double		choice;

		CHECK_FOR_INTERRUPTS();

		sum = 0.0;

		for (j = 0; j < numSamples; j++)
		{
			Datum		vec = PointerGetDatum(VectorArrayGet(samples, j));
			double		distance;

			distance = DatumGetFloat8(FunctionCall2Coll(procinfo, collation, vec, PointerGetDatum(VectorArrayGet(centers, i))));

			/* Use distance squared for weighted probability distribution */
			distance *= distance;

			if (distance < weight[j])
				weight[j] = distance;

			sum += weight[j];
		}

		if (i + 1 == numCenters)
			break;

		choice = sum * RandomDouble();
		for (j = 0; j < numSamples - 1; j++)
		{
			choice -= weight[j];
			if (choice <= 0)
				break;
		}

		VectorArraySet(centers, i + 1, VectorArrayGet(samples, j));
		centers->length++;
	}

	pfree(weight);
}

/*
 * Norm centers
 */
static void
NormCenters(const IvfflatTypeInfo * typeInfo, Oid collation, VectorArray centers)
{
	MemoryContext normCtx = AllocSetContextCreate(CurrentMemoryContext,
												  "Ivfflat norm temporary context",
												  ALLOCSET_DEFAULT_SIZES);

	IvfflatNormVectors(typeInfo, collation, centers, normCtx);
	MemoryContextDelete(normCtx);
}

/*
 * Quick approach if we have no data
 */
static void
RandomCenters(Relation index, VectorArray centers, const IvfflatTypeInfo * typeInfo)
{
	int			dimensions = centers->dim;
	FmgrInfo   *normprocinfo = IvfflatOptionalProcInfo(index, IVFFLAT_KMEANS_NORM_PROC);
	Oid			collation = index->rd_indcollation[0];
	float	   *x = palloc_array_checked(float, dimensions);

	/* Fill with random data */
	while (centers->length < centers->maxlen)
	{
		Pointer		center = VectorArrayGet(centers, centers->length);

		for (int i = 0; i < dimensions; i++)
			x[i] = (float) RandomDouble();

		typeInfo->updateCenter(center, dimensions, x);

		centers->length++;
	}

	if (normprocinfo != NULL)
		NormCenters(typeInfo, collation, centers);
}

#ifdef IVFFLAT_MEMORY
/*
 * Show memory usage
 */
static void
ShowMemoryUsage(MemoryContext context, Size estimatedSize)
{
	elog(INFO, "total memory: %zu MB",
		 MemoryContextMemAllocated(context, true) / (1024 * 1024));
	elog(INFO, "estimated memory: %zu MB", estimatedSize / (1024 * 1024));
}
#endif

/*
 * Sum centers
 */
static void
SumCenters(VectorArray samples, float *agg, int *closestCenters, const IvfflatTypeInfo * typeInfo)
{
	for (int i = 0; i < samples->length; i++)
	{
		float	   *x = agg + ((Size) closestCenters[i] * samples->dim);

		typeInfo->sumCenter(VectorArrayGet(samples, i), x);
	}
}

/*
 * Update centers
 */
static void
UpdateCenters(float *agg, VectorArray centers, const IvfflatTypeInfo * typeInfo)
{
	for (int i = 0; i < centers->length; i++)
	{
		float	   *x = agg + ((Size) i * centers->dim);

		typeInfo->updateCenter(VectorArrayGet(centers, i), centers->dim, x);
	}
}

/*
 * Compute new centers
 */
static void
ComputeNewCenters(VectorArray samples, float *agg, VectorArray newCenters, int *centerCounts, int *closestCenters, FmgrInfo *normprocinfo, Oid collation, const IvfflatTypeInfo * typeInfo)
{
	int			dimensions = newCenters->dim;
	int			numCenters = newCenters->length;
	int			numSamples = samples->length;

	/* Reset sum and count */
	for (int i = 0; i < numCenters; i++)
	{
		float	   *x = agg + ((Size) i * dimensions);

		for (int j = 0; j < dimensions; j++)
			x[j] = 0.0;

		centerCounts[i] = 0;
	}

	/* Increment sum of closest center */
	SumCenters(samples, agg, closestCenters, typeInfo);

	/* Increment count of closest center */
	for (int i = 0; i < numSamples; i++)
		centerCounts[closestCenters[i]] += 1;

	/* Divide sum by count */
	for (int i = 0; i < numCenters; i++)
	{
		float	   *x = agg + ((Size) i * dimensions);

		if (centerCounts[i] > 0)
		{
			/* Double avoids overflow, but requires more memory */
			/* TODO Update bounds */
			for (int j = 0; j < dimensions; j++)
			{
				if (isinf(x[j]))
					x[j] = x[j] > 0 ? FLT_MAX : -FLT_MAX;
			}

			for (int j = 0; j < dimensions; j++)
				x[j] /= centerCounts[i];
		}
		else
		{
			/* TODO Handle empty centers properly */
			for (int j = 0; j < dimensions; j++)
				x[j] = RandomDouble();
		}
	}

	/* Set new centers */
	UpdateCenters(agg, newCenters, typeInfo);

	/* Normalize if needed */
	if (normprocinfo != NULL)
		NormCenters(typeInfo, collation, newCenters);
}

static void
YinyangBuildMembers(int numCenters, int numGroups, int *centerGroup, int *groupOffsets, int *groupMembers, int *groupCounts)
{
	int		   *pos = palloc0_array(int, numGroups);

	memset(groupCounts, 0, sizeof(int) * numGroups);
	for (int i = 0; i < numCenters; i++)
		groupCounts[centerGroup[i]]++;

	groupOffsets[0] = 0;
	for (int g = 0; g < numGroups; g++)
	{
		groupOffsets[g + 1] = groupOffsets[g] + groupCounts[g];
		pos[g] = groupOffsets[g];
	}

	for (int i = 0; i < numCenters; i++)
		groupMembers[pos[centerGroup[i]]++] = i;

	pfree(pos);
}

static void
YinyangGroupInitialCenters(Relation index, VectorArray centers, const IvfflatTypeInfo * typeInfo,
						   int numGroups, int *centerGroup, int *groupOffsets, int *groupMembers,
						   int *groupCounts)
{
	FmgrInfo   *procinfo = index_getprocinfo(index, 1, IVFFLAT_KMEANS_DISTANCE_PROC);
	FmgrInfo   *normprocinfo = IvfflatOptionalProcInfo(index, IVFFLAT_KMEANS_NORM_PROC);
	Oid			collation = index->rd_indcollation[0];
	int			numCenters = centers->length;
	int			dimensions = centers->dim;
	VectorArray groupCenters = VectorArrayInit(numGroups, dimensions, centers->itemsize);
	float	   *agg = palloc0_array(float, (Size) numGroups * dimensions);

	groupCenters->length = numGroups;
	for (int g = 0; g < numGroups; g++)
	{
		int			center = (int) (((int64) g * numCenters) / numGroups);

		VectorArraySet(groupCenters, g, VectorArrayGet(centers, center));
	}

	for (int iter = 0; iter < 5; iter++)
	{
		memset(groupCounts, 0, sizeof(int) * numGroups);

		for (int c = 0; c < numCenters; c++)
		{
			Datum		vec = PointerGetDatum(VectorArrayGet(centers, c));
			float		bestDistance = FLT_MAX;
			int			bestGroup = 0;

			for (int g = 0; g < numGroups; g++)
			{
				float		distance = DatumGetFloat8(FunctionCall2Coll(procinfo, collation, vec, PointerGetDatum(VectorArrayGet(groupCenters, g))));

				if (distance < bestDistance)
				{
					bestDistance = distance;
					bestGroup = g;
				}
			}

			centerGroup[c] = bestGroup;
			groupCounts[bestGroup]++;
		}

		memset(agg, 0, sizeof(float) * (Size) numGroups * dimensions);
		for (int c = 0; c < numCenters; c++)
			typeInfo->sumCenter(VectorArrayGet(centers, c), agg + (Size) centerGroup[c] * dimensions);

		for (int g = 0; g < numGroups; g++)
		{
			float	   *x = agg + (Size) g * dimensions;

			if (groupCounts[g] == 0)
				continue;

			for (int d = 0; d < dimensions; d++)
				x[d] /= groupCounts[g];

			typeInfo->updateCenter(VectorArrayGet(groupCenters, g), dimensions, x);
		}

		if (normprocinfo != NULL)
			NormCenters(typeInfo, collation, groupCenters);
	}

	YinyangBuildMembers(numCenters, numGroups, centerGroup, groupOffsets, groupMembers, groupCounts);

	for (int g = 0; g < numGroups; g++)
	{
		if (groupCounts[g] == 0)
		{
			int			center = g % numCenters;
			int			oldGroup = centerGroup[center];

			centerGroup[center] = g;
			groupCounts[oldGroup]--;
			groupCounts[g]++;
		}
	}

	YinyangBuildMembers(numCenters, numGroups, centerGroup, groupOffsets, groupMembers, groupCounts);

	VectorArrayFree(groupCenters);
	pfree(agg);
}

/*
 * Yinyang k-means variant for IVFFlat using Global and Group filters.
 *
 * Based on the Global Filter + Group Filter idea from Ding et al.,
 * "Yinyang K-Means: A Drop-In Replacement of the Classic K-Means in
 * Classification and Clustering".
 */
static void
YinyangKmeans(Relation index, VectorArray samples, VectorArray centers, const IvfflatTypeInfo * typeInfo, const IvfflatKmeansMemoryEstimate * memoryEstimate)
{
	FmgrInfo   *procinfo;
	FmgrInfo   *normprocinfo;
	Oid			collation;
	int			dimensions = centers->dim;
	int			numCenters = centers->maxlen;
	int			numSamples = samples->length;
	int			numGroups = Max(numCenters / 10, 1);
	VectorArray newCenters;
	float	   *agg;
	int		   *centerCounts;
	int		   *closestCenters;
	float	   *upperBound;
	float	   *groupLowerBound;
	int		   *centerGroup;
	int		   *groupOffsets;
	int		   *groupMembers;
	int		   *groupCounts;
	float	   *centerDrift;
	float	   *groupMaxDrift;
	float	   *scanMin1;
	float	   *scanMin2;
	int		   *scanMinCenter;
	bool	   *groupScanned;

	IvfflatCheckMemoryUsage(memoryEstimate->totalSize);

	InitCentersLowMemory(index, samples, centers, memoryEstimate->initWeightSize);

	numSamples = samples->length;
	numCenters = centers->length;
	numGroups = memoryEstimate->numGroups;

	procinfo = index_getprocinfo(index, 1, IVFFLAT_KMEANS_DISTANCE_PROC);
	normprocinfo = IvfflatOptionalProcInfo(index, IVFFLAT_KMEANS_NORM_PROC);
	collation = index->rd_indcollation[0];

	agg = palloc(memoryEstimate->aggSize);
	centerCounts = palloc(memoryEstimate->centerCountsSize);
	closestCenters = palloc(memoryEstimate->closestCentersSize);
	upperBound = palloc(memoryEstimate->upperBoundSize);
	groupLowerBound = palloc_extended(memoryEstimate->groupLowerBoundSize, MCXT_ALLOC_HUGE);
	centerGroup = palloc(memoryEstimate->centerGroupSize);
	groupOffsets = palloc(memoryEstimate->groupOffsetsSize);
	groupMembers = palloc(memoryEstimate->groupMembersSize);
	groupCounts = palloc(memoryEstimate->groupCountsSize);
	centerDrift = palloc0(memoryEstimate->centerDriftSize);
	groupMaxDrift = palloc0(memoryEstimate->groupMaxDriftSize);
	scanMin1 = palloc(memoryEstimate->tmpGroupFloatSize);
	scanMin2 = palloc(memoryEstimate->tmpGroupFloatSize);
	scanMinCenter = palloc(memoryEstimate->tmpGroupIntSize);
	groupScanned = palloc(memoryEstimate->tmpGroupBoolSize);

	newCenters = VectorArrayInit(numCenters, dimensions, centers->itemsize);
	newCenters->length = numCenters;

	YinyangGroupInitialCenters(index, centers, typeInfo, numGroups, centerGroup, groupOffsets, groupMembers, groupCounts);

	/* Initialize assignments and exact per-group lower bounds from frozen centers. */
	for (int j = 0; j < numSamples; j++)
	{
		Datum		vec = PointerGetDatum(VectorArrayGet(samples, j));
		float		bestDistance = FLT_MAX;
		int			bestCenter = 0;

		for (int g = 0; g < numGroups; g++)
		{
			scanMin1[g] = FLT_MAX;
			scanMin2[g] = FLT_MAX;
			scanMinCenter[g] = -1;
		}

		for (int c = 0; c < numCenters; c++)
		{
			int			g = centerGroup[c];
			float		distance = DatumGetFloat8(FunctionCall2Coll(procinfo, collation, vec, PointerGetDatum(VectorArrayGet(centers, c))));

			if (distance < scanMin1[g])
			{
				scanMin2[g] = scanMin1[g];
				scanMin1[g] = distance;
				scanMinCenter[g] = c;
			}
			else if (distance < scanMin2[g])
				scanMin2[g] = distance;

			if (distance < bestDistance)
			{
				bestDistance = distance;
				bestCenter = c;
			}
		}

		closestCenters[j] = bestCenter;
		upperBound[j] = bestDistance;
		for (int g = 0; g < numGroups; g++)
			groupLowerBound[(Size) j * numGroups + g] = scanMinCenter[g] == bestCenter ? scanMin2[g] : scanMin1[g];
	}

	for (int iteration = 0; iteration < 500; iteration++)
	{
		int			changes = 0;

		CHECK_FOR_INTERRUPTS();
		for (int j = 0; j < numSamples; j++)
		{
			int			oldCenter = closestCenters[j];
			int			oldGroup = centerGroup[oldCenter];
			int			bestCenter = oldCenter;
			float		bestDistance = upperBound[j];
			float		oldAssignedDistance = upperBound[j];
			bool		globallyPruned = true;
			bool		assignedRecomputed = false;

			for (int g = 0; g < numGroups; g++)
			{
				float		effective = groupLowerBound[(Size) j * numGroups + g] - groupMaxDrift[g];

				if (effective < 0)
					effective = 0;
				if (bestDistance > effective)
				{
					globallyPruned = false;
					break;
				}
			}

			if (globallyPruned)
				continue;

			for (int g = 0; g < numGroups; g++)
			{
				groupScanned[g] = false;
				scanMin1[g] = FLT_MAX;
				scanMin2[g] = FLT_MAX;
				scanMinCenter[g] = -1;
			}

			/* Recompute the assigned-center distance once before exact group scans. */
			oldAssignedDistance = DatumGetFloat8(FunctionCall2Coll(procinfo, collation, PointerGetDatum(VectorArrayGet(samples, j)), PointerGetDatum(VectorArrayGet(centers, oldCenter))));
			bestDistance = oldAssignedDistance;
			upperBound[j] = oldAssignedDistance;
			assignedRecomputed = true;

			for (int g = 0; g < numGroups; g++)
			{
				float		effective = groupLowerBound[(Size) j * numGroups + g] - groupMaxDrift[g];

				if (effective < 0)
					effective = 0;

				if (bestDistance <= effective)
				{
					groupLowerBound[(Size) j * numGroups + g] = effective;
					continue;
				}

				groupScanned[g] = true;

				for (int p = groupOffsets[g]; p < groupOffsets[g + 1]; p++)
				{
					int			c = groupMembers[p];
					float		distance;

					if (c == oldCenter)
						distance = oldAssignedDistance;
					else
						distance = DatumGetFloat8(FunctionCall2Coll(procinfo, collation, PointerGetDatum(VectorArrayGet(samples, j)), PointerGetDatum(VectorArrayGet(centers, c))));

					if (distance < scanMin1[g])
					{
						scanMin2[g] = scanMin1[g];
						scanMin1[g] = distance;
						scanMinCenter[g] = c;
					}
					else if (distance < scanMin2[g])
						scanMin2[g] = distance;

					if (distance < bestDistance)
					{
						bestDistance = distance;
						bestCenter = c;
					}
				}
			}

			if (bestCenter != oldCenter)
			{
				closestCenters[j] = bestCenter;
				changes++;
			}
			upperBound[j] = bestDistance;

			for (int g = 0; g < numGroups; g++)
			{
				float		bound;

				if (groupScanned[g])
					bound = scanMinCenter[g] == bestCenter ? scanMin2[g] : scanMin1[g];
				else
				{
					bound = groupLowerBound[(Size) j * numGroups + g];
					if (g == oldGroup && bestCenter != oldCenter && assignedRecomputed && oldAssignedDistance < bound)
						bound = oldAssignedDistance;
				}

				groupLowerBound[(Size) j * numGroups + g] = bound;
			}
		}
		ComputeNewCenters(samples, agg, newCenters, centerCounts, closestCenters, normprocinfo, collation, typeInfo);

		memset(groupMaxDrift, 0, memoryEstimate->groupMaxDriftSize);
		for (int c = 0; c < numCenters; c++)
		{
			float		drift = DatumGetFloat8(FunctionCall2Coll(procinfo, collation, PointerGetDatum(VectorArrayGet(centers, c)), PointerGetDatum(VectorArrayGet(newCenters, c))));

			centerDrift[c] = drift;
			if (drift > groupMaxDrift[centerGroup[c]])
				groupMaxDrift[centerGroup[c]] = drift;
		}

		for (int j = 0; j < numSamples; j++)
			upperBound[j] += centerDrift[closestCenters[j]];

		for (int c = 0; c < numCenters; c++)
			VectorArraySet(centers, c, VectorArrayGet(newCenters, c));

		if (changes == 0 && iteration != 0)
			break;
	}
}

/*
 * Use Elkan for performance. This requires distance function to satisfy triangle inequality.
 *
 * We use L2 distance for L2 (not L2 squared like index scan)
 * and angular distance for inner product and cosine distance
 *
 * https://www.aaai.org/Papers/ICML/2003/ICML03-022.pdf
 */
static void
ElkanKmeans(Relation index, VectorArray samples, VectorArray centers, const IvfflatTypeInfo * typeInfo, const IvfflatKmeansMemoryEstimate * memoryEstimate)
{
	FmgrInfo   *procinfo;
	FmgrInfo   *normprocinfo;
	Oid			collation;
	int			dimensions = centers->dim;
	int			numCenters = centers->maxlen;
	int			numSamples = samples->length;
	VectorArray newCenters;
	float	   *agg;
	int		   *centerCounts;
	int		   *closestCenters;
	float	   *lowerBound;
	float	   *upperBound;
	float	   *s;
	float	   *halfcdist;
	float	   *newcdist;

	/* Check memory requirements */
	IvfflatCheckMemoryUsage(memoryEstimate->totalSize);

	/* Ensure indexing does not overflow */
	if (numCenters > INT_MAX / numCenters)
		elog(ERROR, "Indexing overflow detected. Please report a bug.");

	/* Set support functions */
	procinfo = index_getprocinfo(index, 1, IVFFLAT_KMEANS_DISTANCE_PROC);
	normprocinfo = IvfflatOptionalProcInfo(index, IVFFLAT_KMEANS_NORM_PROC);
	collation = index->rd_indcollation[0];

	/* Allocate space */
	/* Use float instead of double to save memory */
	agg = palloc(memoryEstimate->aggSize);
	centerCounts = palloc(memoryEstimate->centerCountsSize);
	closestCenters = palloc(memoryEstimate->closestCentersSize);
	lowerBound = palloc_extended(memoryEstimate->lowerBoundSize, MCXT_ALLOC_HUGE);
	upperBound = palloc(memoryEstimate->upperBoundSize);
	s = palloc(memoryEstimate->sSize);
	halfcdist = palloc_extended(memoryEstimate->halfcdistSize, MCXT_ALLOC_HUGE);
	newcdist = palloc(memoryEstimate->newcdistSize);

	/* Initialize new centers */
	newCenters = VectorArrayInit(numCenters, dimensions, centers->itemsize);
	newCenters->length = numCenters;

#ifdef IVFFLAT_MEMORY
	ShowMemoryUsage(MemoryContextGetParent(CurrentMemoryContext), memoryEstimate->totalSize);
#endif

	/* Pick initial centers */
	InitCenters(index, samples, centers, lowerBound, memoryEstimate->initWeightSize);

	/* Assign each x to its closest initial center c(x) = argmin d(x,c) */
	for (int j = 0; j < numSamples; j++)
	{
		float		minDistance = FLT_MAX;
		int			closestCenter = 0;

		/* Find closest center */
		for (int k = 0; k < numCenters; k++)
		{
			/* TODO Use Lemma 1 in k-means++ initialization */
			float		distance = lowerBound[(Size) j * numCenters + k];

			if (distance < minDistance)
			{
				minDistance = distance;
				closestCenter = k;
			}
		}

		upperBound[j] = minDistance;
		closestCenters[j] = closestCenter;
	}

	/* Give 500 iterations to converge */
	for (int iteration = 0; iteration < 500; iteration++)
	{
		int			changes = 0;
		bool		rjreset;

		/* Can take a while, so ensure we can interrupt */
		CHECK_FOR_INTERRUPTS();

		/* Step 1: For all centers, compute distance */
		for (int j = 0; j < numCenters; j++)
		{
			Datum		vec = PointerGetDatum(VectorArrayGet(centers, j));

			for (int k = j + 1; k < numCenters; k++)
			{
				float		distance = 0.5 * DatumGetFloat8(FunctionCall2Coll(procinfo, collation, vec, PointerGetDatum(VectorArrayGet(centers, k))));

				halfcdist[(Size) j * numCenters + k] = distance;
				halfcdist[(Size) k * numCenters + j] = distance;
			}
		}

		/* For all centers c, compute s(c) */
		for (int j = 0; j < numCenters; j++)
		{
			float		minDistance = FLT_MAX;

			for (int k = 0; k < numCenters; k++)
			{
				float		distance;

				if (j == k)
					continue;

				distance = halfcdist[(Size) j * numCenters + k];
				if (distance < minDistance)
					minDistance = distance;
			}

			s[j] = minDistance;
		}

		rjreset = iteration != 0;

		for (int j = 0; j < numSamples; j++)
		{
			bool		rj;

			/* Step 2: Identify all points x such that u(x) <= s(c(x)) */
			if (upperBound[j] <= s[closestCenters[j]])
				continue;

			rj = rjreset;

			for (int k = 0; k < numCenters; k++)
			{
				Datum		vec;
				float		dxcx;

				/* Step 3: For all remaining points x and centers c */
				if (k == closestCenters[j])
					continue;

				if (upperBound[j] <= lowerBound[(Size) j * numCenters + k])
					continue;

				if (upperBound[j] <= halfcdist[(Size) closestCenters[j] * numCenters + k])
					continue;

				vec = PointerGetDatum(VectorArrayGet(samples, j));

				/* Step 3a */
				if (rj)
				{
					dxcx = DatumGetFloat8(FunctionCall2Coll(procinfo, collation, vec, PointerGetDatum(VectorArrayGet(centers, closestCenters[j]))));

					/* d(x,c(x)) computed, which is a form of d(x,c) */
					lowerBound[(Size) j * numCenters + closestCenters[j]] = dxcx;
					upperBound[j] = dxcx;

					rj = false;
				}
				else
					dxcx = upperBound[j];

				/* Step 3b */
				if (dxcx > lowerBound[(Size) j * numCenters + k] || dxcx > halfcdist[(Size) closestCenters[j] * numCenters + k])
				{
					float		dxc = DatumGetFloat8(FunctionCall2Coll(procinfo, collation, vec, PointerGetDatum(VectorArrayGet(centers, k))));

					/* d(x,c) calculated */
					lowerBound[(Size) j * numCenters + k] = dxc;

					if (dxc < dxcx)
					{
						closestCenters[j] = k;

						/* c(x) changed */
						upperBound[j] = dxc;

						changes++;
					}
				}
			}
		}

		/* Step 4: For each center c, let m(c) be mean of all points assigned */
		ComputeNewCenters(samples, agg, newCenters, centerCounts, closestCenters, normprocinfo, collation, typeInfo);

		/* Step 5 */
		for (int j = 0; j < numCenters; j++)
			newcdist[j] = DatumGetFloat8(FunctionCall2Coll(procinfo, collation, PointerGetDatum(VectorArrayGet(centers, j)), PointerGetDatum(VectorArrayGet(newCenters, j))));

		for (int j = 0; j < numSamples; j++)
		{
			for (int k = 0; k < numCenters; k++)
			{
				float		distance = lowerBound[(Size) j * numCenters + k] - newcdist[k];

				if (distance < 0)
					distance = 0;

				lowerBound[(Size) j * numCenters + k] = distance;
			}
		}

		/* Step 6 */
		/* We reset r(x) before Step 3 in the next iteration */
		for (int j = 0; j < numSamples; j++)
			upperBound[j] += newcdist[closestCenters[j]];

		/* Step 7 */
		for (int j = 0; j < numCenters; j++)
			VectorArraySet(centers, j, VectorArrayGet(newCenters, j));

		if (changes == 0 && iteration != 0)
			break;
	}
}

/*
 * Ensure no NaN or infinite values
 */
static void
CheckElements(VectorArray centers, const IvfflatTypeInfo * typeInfo)
{
	float	   *scratch = palloc_array_checked(float, centers->dim);

	for (int i = 0; i < centers->length; i++)
	{
		for (int j = 0; j < centers->dim; j++)
			scratch[j] = 0;

		/* /fp:fast may not propagate NaN with MSVC, but that's alright */
		typeInfo->sumCenter(VectorArrayGet(centers, i), scratch);

		for (int j = 0; j < centers->dim; j++)
		{
			if (isnan(scratch[j]))
				elog(ERROR, "NaN detected. Please report a bug.");

			if (isinf(scratch[j]))
				elog(ERROR, "Infinite value detected. Please report a bug.");
		}
	}
}

/*
 * Ensure no zero vectors for cosine distance
 */
static void
CheckNorms(VectorArray centers, Relation index)
{
	/* Check NORM_PROC instead of KMEANS_NORM_PROC */
	FmgrInfo   *normprocinfo = IvfflatOptionalProcInfo(index, IVFFLAT_NORM_PROC);
	Oid			collation = index->rd_indcollation[0];

	if (normprocinfo == NULL)
		return;

	for (int i = 0; i < centers->length; i++)
	{
		double		norm = DatumGetFloat8(FunctionCall1Coll(normprocinfo, collation, PointerGetDatum(VectorArrayGet(centers, i))));

		if (norm == 0)
			elog(ERROR, "Zero norm detected. Please report a bug.");
	}
}

/*
 * Detect issues with centers
 */
static void
CheckCenters(Relation index, VectorArray centers, const IvfflatTypeInfo * typeInfo)
{
	if (centers->length != centers->maxlen)
		elog(ERROR, "Not enough centers. Please report a bug.");

	CheckElements(centers, typeInfo);
	CheckNorms(centers, index);
}

/*
 * Perform naive k-means centering
 * We use spherical k-means for inner product and cosine
 */
void
IvfflatKmeans(Relation index, VectorArray samples, VectorArray centers, const IvfflatTypeInfo * typeInfo, Size memoryUsed)
{
	MemoryContext kmeansCtx = AllocSetContextCreate(CurrentMemoryContext,
													"Ivfflat kmeans temporary context",
													ALLOCSET_DEFAULT_SIZES);
	MemoryContext oldCtx = MemoryContextSwitchTo(kmeansCtx);

	if (samples->length == 0)
		RandomCenters(index, centers, typeInfo);
	else
	{
		IvfflatKmeansSelection selection = SelectKmeansAlgorithm(samples, centers, memoryUsed);

		if (selection.algorithm == IVFFLAT_KMEANS_YINYANG)
			YinyangKmeans(index, samples, centers, typeInfo, &selection.yinyangEstimate);
		else
			ElkanKmeans(index, samples, centers, typeInfo, &selection.elkanEstimate);
	}

	CheckCenters(index, centers, typeInfo);

	MemoryContextSwitchTo(oldCtx);
	MemoryContextDelete(kmeansCtx);
}
