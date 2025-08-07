/*-------------------------------------------------------------------------
 *
 * gdd_detector.c
 *	  Global DeadLock Detector - Detector Algorithm
 *
 *
 * Copyright (c) 2018-2020 VMware, Inc. or its affiliates.
 * Copyright (c) 2021-Present OpenTenBase development team, Tencent
 *
 * IDENTIFICATION
 *	  src/backend/utils/gdd/gdd_detector.c
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "storage/procarray.h"

#include "utils/gdd_detector.h"
#include "utils/gdd_detectorpriv.h"

/***************************************************************************/

static GddGraph *gddCtxGetGraph(GddCtx *ctx, Oid node);
static GddStat *gddCtxGetGlobalStat(GddCtx *ctx, int vid);
static void gddCtxRemoveVid(GddCtx *ctx, int vid);
static int gddCtxGetMaxWaitVid(GddCtx *ctx);

static GddStat *gddStatNew(int vid);
static void gddStatInit(GddStat *stat, int vid);

static GddGraph *gddGraphNew(int id);
static GddVert *gddGraphGetVert(GddGraph *graph, int vid);
static GddEdge *gddGraphMakeEdge(GddGraph *graph, int from, int to, bool solid);
static void gddGraphRemoveVid(GddGraph *graph, int vid);

static GddVert *gddVertNew(int id);
static void gddVertBindStats(GddVert *vert, GddStat *global, GddStat *topstat);
static bool gddVertUnlinkAll(GddVert *vert);
static bool gddVertReduce(GddVert *vert);
static int gddVertGetInDegree(GddVert *vert);
static int gddVertGetOutDegree(GddVert *vert);

static GddEdge *gddEdgeNew(GddVert *from, GddVert *to, bool solid);
static void gddEdgeLink(GddEdge *edge);
static void gddEdgeUnlink(GddEdge *edge, GddListIter *fromiter, GddListIter *toiter);
static void gddEdgeUnlinkFrom(GddEdge *edge, GddListIter *fromiter);
static void gddEdgeUnlinkTo(GddEdge *edge, GddListIter *toiter);

static void gddMapInit(GddMap *map);
static void gddMapEnsureCapacity(GddMap *map, int capacity);
static GddPair *gddMapGetPair(GddMap *map, int key);
static void *gddMapGet(GddMap *map, int key);
static void gddMapSetUnsafe(GddMap *map, int key, void *ptr);

static bool need_reduce_original_indeg = false;

/***************************************************************************/

/*
 * Create a new Global DeadLock Detector context.
 */
GddCtx *
GddCtxNew(void)
{
	GddCtx		*ctx = palloc0(sizeof(*ctx));

	gddStatInit(&ctx->topstat, 0);
	gddMapInit(&ctx->globals);
	gddMapInit(&ctx->graphs);

	ctx->global_transaction = NULL;
	ctx->global_transaction_count = 0;
	ctx->global_transaction_size = 0;
    need_reduce_original_indeg = false;
	return ctx;
}

/*
 * init txn info by txn index
 */
void
InitGddCtxTransaction(GddCtx *ctx, int txn_index)
{
    TransactionInfo *txn = ctx->global_transaction;

    INIT(txn[txn_index].pid);
    INIT(txn[txn_index].node);
    txn[txn_index].query = NULL;
}

/*
 * init pid and node info
 */
void
InitGddTxnPidNodeInfo(GddCtx *ctx)
{
    GddMapIter	graphiter;
    GddMapIter	vertiter;
    GddVert     *vert = NULL;
    TransactionInfo *txn = ctx->global_transaction;

    /* init pid and node info */
    gdd_ctx_foreach_vert(graphiter, vertiter, ctx)
    {
        vert = gdd_map_iter_get_ptr(vertiter);
        PALLOC(txn[vert->id].pid, vert->pid);
        PALLOC(txn[vert->id].node, gdd_map_iter_get_key(graphiter));
    }
}

/*
 * Add an edge to one local graph.
 *
 * Return the edge.
 */
GddEdge *
GddCtxAddEdge(GddCtx *ctx, Oid node, int from, int to, bool solid)
{
	GddEdge		*edge;
	GddGraph	*graph;
	GddStat		*global;

	Assert(ctx != NULL);

	graph = gddCtxGetGraph(ctx, node);
	edge = gddGraphMakeEdge(graph, from, to, solid);

	global = gddCtxGetGlobalStat(ctx, from);
	gddVertBindStats(edge->from, global, &ctx->topstat);

	global = gddCtxGetGlobalStat(ctx, to);
	gddVertBindStats(edge->to, global, &ctx->topstat);

	/*
	 * Actually link the edge to its from/to verts,
	 * and update global in/out degrees.
	 */
	gddEdgeLink(edge);

	return edge;
}

/*
 * Reduce verts and edges looply until nothing can be deleted.
 */
void
GddCtxReduce(GddCtx *ctx)
{
	GddMapIter	graphiter;
	GddMapIter	vertiter;
	bool		dirty = true;

	Assert(ctx != NULL);

	while (dirty)
	{
		dirty = false;

		gdd_ctx_foreach_vert(graphiter, vertiter, ctx)
		{
			GddVert		*vert = gdd_map_iter_get_ptr(vertiter);

			if (gddVertReduce(vert))
				dirty = true;

			/*
			 * Remove vert from the graph if no edges left.
			 *
			 * We want to do this even if gddVertReduce() returns false
			 * as vert1's last edge might be removed in vert2's reduce call.
			 * However depending on the order of vert1 and vert2, we can not
			 * promise all the empty verts are removed, so do not make the
			 * assumption on this.  See gddCtxGetMaxWaitVid() for example.
			 */
			if (!gddVertGetInDegree(vert) && !gddVertGetOutDegree(vert))
				gdd_map_iter_delete(vertiter);
		}
	}
}

/*
 * Break deadlocks and return a list of vids to cancel.
 */
List *
GddCtxBreakDeadLock(GddCtx *ctx)
{
    List		*vids = NIL;
    int         vid = 0;
    Assert(ctx != NULL);

    need_reduce_original_indeg = true;

    while (!GddCtxEmpty(ctx))
    {
        /*
         * The only policy supported for now is to cancel the max wait txn vert
         */
        vid = gddCtxGetMaxWaitVid(ctx);

        vids = lappend_int(vids, vid);

        /*
         * Cancel this vert and reduce again to see if more deadlocks
         * are detected.
         */
        gddCtxRemoveVid(ctx, vid);
        GddCtxReduce(ctx);
    }

    need_reduce_original_indeg = false;

    return vids;
}

/*
 * Return true if no edges left.
 */
bool
GddCtxEmpty(GddCtx *ctx)
{
	Assert(ctx->topstat.indeg == ctx->topstat.outdeg);

	return ctx->topstat.indeg == 0;
}

/***************************************************************************/

/*
 * Get the graph with node id.
 *
 * A new one is created if it's first seen.
 */
static GddGraph *
gddCtxGetGraph(GddCtx *ctx, Oid node)
{
	GddGraph	*graph;

	Assert(ctx != NULL);

	graph = gddMapGet(&ctx->graphs, node);

	if (!graph)
	{
		graph = gddGraphNew(node);
		gddMapSetUnsafe(&ctx->graphs, node, graph);
	}

	return graph;
}

/*
 * Get the global with vert id.
 *
 * A new one is created if it's first seen.
 */
static GddStat *
gddCtxGetGlobalStat(GddCtx *ctx, int vid)
{
	GddStat		*global;

	Assert(ctx != NULL);

	global = gddMapGet(&ctx->globals, vid);

	if (!global)
	{
		global = gddStatNew(vid);
		gddMapSetUnsafe(&ctx->globals, vid, global);
	}

	return global;
}

/*
 * Remove verts whose vert id equal to vid on all local graphs.
 */
static void
gddCtxRemoveVid(GddCtx *ctx, int vid)
{
	GddMapIter	iter;

	Assert(ctx != NULL);

	/* Do not use gdd_ctx_foreach_vert() as break is used in the inner loop */
	gdd_ctx_foreach_graph(iter, ctx)
	{
		GddGraph	*graph = gdd_map_iter_get_ptr(iter);

		gddGraphRemoveVid(graph, vid);
	}
}

/*
 * Get the max vert id, return 0 if no vert left at all.
 */
static int
gddCtxGetMaxWaitVid(GddCtx *ctx)
{
	GddMapIter	graphiter;
	GddMapIter	vertiter;
	int max_original_indeg = -1;
    int maxWaitVid = 0;

	Assert(ctx != NULL);

	gdd_ctx_foreach_vert(graphiter, vertiter, ctx)
	{
		GddVert		*vert = gdd_map_iter_get_ptr(vertiter);

		/*
		 * AutoXact is suspending, ignore them.
		 */
		if (ctx->is_atxact_transaction &&
			ctx->is_atxact_transaction(ctx, vert->id))
			continue;

		/*
		 * It's possible for vert to have zero in/out edge,
		 * which means vert was already deleted in the
		 * algorithm conception so should be skipped.
		 */
		if (gddVertGetInDegree(vert) || gddVertGetOutDegree(vert))
        {
		    if (vert->global->original_indeg > max_original_indeg)
            {
                max_original_indeg = vert->global->original_indeg;
                maxWaitVid = vert->id;
            }
        }
	}

	return maxWaitVid;
}

/***************************************************************************/

/*
 * Create a new global struct.
 */
static GddStat *
gddStatNew(int vid)
{
	GddStat		*stat = palloc(sizeof(*stat));

	gddStatInit(stat, vid);

	return stat;
}

/*
 * Initialize a global struct.
 */
static void
gddStatInit(GddStat *stat, int vid)
{
	Assert(stat != NULL);

	stat->id = vid;
	stat->indeg = 0;
	stat->outdeg = 0;
	stat->original_indeg = 0;
}

/***************************************************************************/

/*
 * Create a new graph struct.
 */
static GddGraph *
gddGraphNew(int id)
{
	GddGraph *graph = palloc(sizeof(*graph));

	graph->id = id;

	gddMapInit(&graph->verts);

	return graph;
}

/*
 * Get the vert with vert id.
 *
 * A new one is created if it's first seen.
 */
static GddVert *
gddGraphGetVert(GddGraph *graph, int vid)
{
	GddVert		*vert;

	Assert(graph != NULL);

	vert = gddMapGet(&graph->verts, vid);

	if (!vert)
	{
		vert = gddVertNew(vid);
		gddMapSetUnsafe(&graph->verts, vid, vert);
	}

	return vert;
}

/*
 * Make an edge in graph.
 *
 * The edge is not linked yet.
 */
static GddEdge *
gddGraphMakeEdge(GddGraph *graph, int from, int to, bool solid)
{
	GddEdge		*edge;
	GddVert		*vfrom;
	GddVert		*vto;

	Assert(graph != NULL);

	vfrom = gddGraphGetVert(graph, from);
	vto = gddGraphGetVert(graph, to);

	edge = gddEdgeNew(vfrom, vto, solid);

	return edge;
}

/*
 * Remove the vert whose vert id is vid.
 */
static void
gddGraphRemoveVid(GddGraph *graph, int vid)
{
	GddMapIter	iter;

	Assert(graph != NULL);

	gdd_graph_foreach_vert(iter, graph)
	{
		GddVert		*vert = gdd_map_iter_get_ptr(iter);

		Assert(vert != NULL);

		if (vert->id != vid)
			continue;

		/* Remove all local in/out edges */
		gddVertUnlinkAll(vert);

		/* Finally remove vert from graph */
		gdd_map_iter_delete(iter);

		/* Only one vert could have vert id equals to vid */
		break;
	}
}

/***************************************************************************/

/*
 * Create a new vert.
 */
static GddVert *
gddVertNew(int id)
{
	GddVert		*vert = palloc(sizeof(*vert));

	vert->id = id;
	vert->edgesIn = NIL;
	vert->edgesOut = NIL;
	vert->pid = 0;

	return vert;
}

/*
 * Bind global and vert.
 */
static void
gddVertBindStats(GddVert *vert, GddStat *global, GddStat *topstat)
{
	Assert(vert != NULL);

	vert->global = global;
	vert->topstat = topstat;
}

/*
 * Unlink vert's all edges.
 */
static bool
gddVertUnlinkAll(GddVert *vert)
{
	GddListIter	edgeiter;
	bool		dirty = false;

	Assert(vert != NULL);

	gdd_vert_foreach_in_edge(edgeiter, vert)
	{
		GddEdge		*edge = gdd_list_iter_get_ptr(edgeiter);

		gddEdgeUnlink(edge, NULL, &edgeiter);
		dirty = true;
	}

	Assert(vert->edgesIn == NIL);

	gdd_vert_foreach_out_edge(edgeiter, vert)
	{
		GddEdge		*edge = gdd_list_iter_get_ptr(edgeiter);

		gddEdgeUnlink(edge, &edgeiter, NULL);
		dirty = true;
	}

	Assert(vert->edgesOut == NIL);

	return dirty;
}

/*
 * Reduce vert's edges.
 *
 * The algorithm is described at README.md.
 *
 * Return true if anything is deleted.
 */
static bool
gddVertReduce(GddVert *vert)
{
	GddStat		*global;
	bool		dirty = false;

	Assert(vert != NULL);
	Assert(vert->global != NULL);

	global = vert->global;

	/*
	 * Remove global end verts (verts with global 0 in/out degrees)
	 */
	if (global->indeg == 0 || global->outdeg == 0)
	{
		/* Remove all local in/out edges */
		if (gddVertUnlinkAll(vert))
			dirty = true;

		return dirty;
	}

	/*
	 * Remove a vert's dotted in edges if vert's local out degree is 0.
	 */
	if (gddVertGetOutDegree(vert) == 0)
	{
		GddListIter	edgeiter;

		gdd_vert_foreach_in_edge(edgeiter, vert)
		{
			GddEdge		*edge = gdd_list_iter_get_ptr(edgeiter);

			/* Do not remove solid edges */
			if (edge->solid)
				continue;

			gddEdgeUnlink(edge, NULL, &edgeiter);
			dirty = true;
		}

	}

	return dirty;
}

/*
 * Get vert's local in degree.
 */
static int
gddVertGetInDegree(GddVert *vert)
{
	Assert(vert != NULL);

	return list_length(vert->edgesIn);
}

/*
 * Get vert's local in degree.
 */
static int
gddVertGetOutDegree(GddVert *vert)
{
	Assert(vert != NULL);

	return list_length(vert->edgesOut);
}

/***************************************************************************/

/*
 * Create a new edge.
 */
static GddEdge *
gddEdgeNew(GddVert *from, GddVert *to, bool solid)
{
	GddEdge		*edge = palloc(sizeof(*edge));

	edge->from = from;
	edge->to = to;
	edge->solid = solid;

	return edge;
}

/*
 * Link edge to its from/to verts and update global in/out degrees.
 */
static void
gddEdgeLink(GddEdge *edge)
{
	Assert(edge != NULL);
	Assert(edge->from != NULL);
	Assert(edge->from->global != NULL);
	Assert(edge->to != NULL);
	Assert(edge->to->global != NULL);
	Assert(edge->to->topstat != NULL);
	Assert(edge->to->topstat == edge->from->topstat);

	edge->from->topstat->outdeg++;
	edge->to->topstat->indeg++;
	Assert(edge->from->topstat->outdeg == edge->to->topstat->indeg);

	/* Update global in/out degrees */
	edge->from->global->outdeg++;
	edge->to->global->indeg++;
    edge->to->global->original_indeg++;

	edge->from->edgesOut = lappend(edge->from->edgesOut, edge);
	edge->to->edgesIn = lappend(edge->to->edgesIn, edge);
}

/*
 * Unlink edge from its from/to verts and update global in/out degrees.
 */
static void
gddEdgeUnlink(GddEdge *edge, GddListIter *fromiter, GddListIter *toiter)
{
	Assert(edge != NULL);

	gddEdgeUnlinkFrom(edge, fromiter);
	gddEdgeUnlinkTo(edge, toiter);

	Assert(edge->from->topstat->outdeg == edge->to->topstat->indeg);
}

/*
 * Unlink edge from its from vert and update global out degree.
 */
static void
gddEdgeUnlinkFrom(GddEdge *edge, GddListIter *fromiter)
{
	Assert(edge != NULL);
	Assert(edge->from != NULL);
	Assert(edge->from->global != NULL);
	Assert(edge->from->topstat != NULL);

	edge->from->topstat->outdeg--;
	Assert(edge->from->topstat->outdeg >= 0);

	edge->from->global->outdeg--;
	Assert(edge->from->global->outdeg >= 0);

	/*
	 * If the from vert's iter is provided then delete it in-place,
	 * otherwise delete it with a scan.
	 */
	if (fromiter)
	{
		gdd_list_iter_delete(*fromiter);
		edge->from->edgesOut = gdd_list_iter_get_list(*fromiter);
	}
	else
		edge->from->edgesOut = list_delete_ptr(edge->from->edgesOut, edge);
}

/*
 * Unlink edge from its to vert and update global in degree.
 */
static void
gddEdgeUnlinkTo(GddEdge *edge, GddListIter *toiter)
{
	Assert(edge != NULL);
	Assert(edge->to != NULL);
	Assert(edge->to->global != NULL);
	Assert(edge->to->topstat != NULL);

	edge->to->topstat->indeg--;
	Assert(edge->to->topstat->indeg >= 0);

	edge->to->global->indeg--;
	Assert(edge->to->global->indeg >= 0);

	if (need_reduce_original_indeg)
    {
        edge->to->global->original_indeg--;
        Assert(edge->to->global->indeg >= 0);
    }

	/*
	 * If the to vert's iter is provided then delete it in-place,
	 * otherwise delete it with a scan.
	 */
	if (toiter)
	{
		gdd_list_iter_delete(*toiter);
		edge->to->edgesIn = gdd_list_iter_get_list(*toiter);
	}
	else
		edge->to->edgesIn = list_delete_ptr(edge->to->edgesIn, edge);
}

/***************************************************************************/

/*
 * Initialize a map.
 */
static void
gddMapInit(GddMap *map)
{
	Assert(map != NULL);

	map->length = 0;
	map->capacity = 4;
	map->pairs = palloc(map->capacity * sizeof(GddPair));
}

/*
 * Ensure map has enough capacity.
 */
static void
gddMapEnsureCapacity(GddMap *map, int capacity)
{
	Assert(map != NULL);

	if (capacity <= map->capacity)
		return;

	/* To prevent frequently resizing we prepare double capacity as required */
	map->capacity = capacity << 1;

	map->pairs = repalloc(map->pairs,
						  map->capacity * sizeof(GddPair));
}

/*
 * Lookup the pair struct by key.
 *
 * Return NULL if key is not found.
 */
static GddPair *
gddMapGetPair(GddMap *map, int key)
{
	int			i;

	Assert(map != NULL);

	for (i = 0; i < map->length; i++)
	{
		GddPair		*pair = &map->pairs[i];

		if (pair->key == key)
			return pair;
	}

	return NULL;
}

/*
 * Lookup the pointer by key.
 *
 * Return NULL if key is not found.  If NULL is valid value for the pointer
 * then use gddMapGetPair() instead of this one.
 */
static void *
gddMapGet(GddMap *map, int key)
{
	GddPair		*pair = gddMapGetPair(map, key);

	return pair ? pair->ptr : NULL;
}

/*
 * Append a new <k,v> pair without checking for existence.
 *
 * Only use this if you have already checked with the gddMapGet*() functions
 * that k does not exist.
 */
static void
gddMapSetUnsafe(GddMap *map, int key, void *ptr)
{
	GddPair		*pair;

	gddMapEnsureCapacity(map, map->length + 1);

	pair = &map->pairs[map->length++];

	pair->key = key;
	pair->ptr = ptr;
}
