/*-------------------------------------------------------------------------
 *
 * remoteDataAccess.c
 *
 *	  Functions to re-distrubution data to remote Datanodes
 *
 *
 * Copyright (c) 2019-2024 TeleDB Development Group
 * 
 *
 * IDENTIFICATION
 *	  src/backend/pgxc/pool/remoteDataAccess.c
 *
 *-------------------------------------------------------------------------
 */

#include <time.h>
#include <openssl/aes.h>
#include <openssl/rand.h>
#include "postgres.h"
#include "access/twophase.h"
#include "access/gtm.h"
#include "access/sysattr.h"
#include "access/transam.h"
#include "access/xact.h"
#include "access/relscan.h"
#include "catalog/pg_type.h"
#include "catalog/pgxc_node.h"
#include "commands/prepare.h"
#include "executor/executor.h"
#include "gtm/gtm_c.h"
#include "libpq/libpq.h"
#include "miscadmin.h"
#include "pgxc/execRemote.h"
#include "pgxc/remoteDataAccess.h"
#include "tcop/tcopprot.h"
#include "executor/nodeSubplan.h"
#include "nodes/nodeFuncs.h"
#include "pgstat.h"
#include "nodes/nodes.h"
#include "nodes/nodeFuncs.h"
#include "optimizer/var.h"
#include "pgxc/copyops.h"
#include "pgxc/nodemgr.h"
#include "pgxc/poolmgr.h"
#include "storage/ipc.h"
#include "storage/proc.h"
#include "utils/datum.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/pg_rusage.h"
#include "utils/tuplesort.h"
#include "utils/snapmgr.h"
#include "utils/builtins.h"
#include "tcop/utility.h"
#include "pgxc/locator.h"
#include "pgxc/pgxc.h"
#include "parser/parse_type.h"
#include "parser/parsetree.h"
#include "pgxc/xc_maintenance_mode.h"
#include "catalog/pgxc_class.h"
#include "common/md5.h"
#ifdef __OPENTENBASE__
#include "commands/explain_dist.h"
#include "pgxc/squeue.h"
#include "executor/execParallel.h"
#include "postmaster/postmaster.h"
#include "executor/nodeModifyTable.h"
#include "utils/syscache.h"
#include "nodes/print.h"
#include "pgxc/rda_frame_impl.h"
#include "pgxc/forwarder.h"
#endif

#define PASSWORD_LEN 512
#define PRINT_INTERVAL_SEC  (1000 * 1000L)
#define RETRY_NUM  3

extern int DataCheckTimeout;

static void close_rda_shmem_frames(RemoteDataAccessState *node);
static void init_rda_frame_entry(rda_frame_entry * entry,
                                         int nodeid, Oid nodeOid);
static void init_rda_shmem_frames(RemoteDataAccessState *node);
static int push_cached_slot_to_out_frames(RDAResponseCombiner *combiner);
static TupleTableSlot * redistrute_outer_slot(RemoteDataAccessState *node,
                                                 TupleTableSlot *slot);
static bool save_local_slot_to_store(RDAResponseCombiner *combiner,
                                                 TupleTableSlot *slot);
static bool send_slot_to_rda_frame(rda_frame_entry *frame_entry,
                                             TupleTableSlot *slot);
static bool send_tuplestore_to_rda_frame(rda_frame_entry *frame_entry);
static bool send_datarow_to_rda_frame(rda_frame_entry *frame_entry,
                                             RemoteDataRow datarow);
static bool send_close_to_rda_frame(rda_frame_entry *frame_entry);
static bool send_rescan_to_rda_frame(rda_frame_entry *frame_entry);
static void send_close_to_all_rda_frames(RDAResponseCombiner *combiner);
static void force_send_rescan_to_all_rda_frames(RDAResponseCombiner *combiner);
static void force_push_cached_slot_out(RDAResponseCombiner *combiner,
                                                        bool discard_in);
static TupleTableSlot *extract_slot_from_remote_frame(
        RDAResponseCombiner *combiner, rda_frame_entry *frame_entry);
static bool recevice_slot_from_rda_frame(rda_frame_entry *frame_entry,
                                             MemoryContext result_mcxt);
static bool recevice_rescan_from_in_frame(rda_frame_entry *frame_entry);
static void wait_rescan_in_frames(RDAResponseCombiner *combiner);
static void force_discard_all_slot_in_frames(RDAResponseCombiner *combiner);
static void recevice_and_discard_slot_in_frames(RDAResponseCombiner *combiner);
static rda_frame_entry *recevice_slot_from_any_frame(
                            RDAResponseCombiner *combiner, bool force,
                            MemoryContext result_mcxt);
static void rda_connections_cleanup(RemoteDataAccessState *node);

static void
InitRDAResponseCombiner(RDAResponseCombiner *combiner, int node_count,
                        CombineType combine_type)
{
    combiner->node_count = node_count;
    combiner->combine_type = combine_type;
    combiner->current_conn = 0;
    combiner->complete_count = 0;
    combiner->local_fetch_finish = false;

    combiner->errorMessage = NULL;
    combiner->errorDetail = NULL;
    combiner->errorHint = NULL;

    combiner->rda_ctx = NULL;
    combiner->tmp_ctx = NULL;

#ifdef __OPENTENBASE__
    combiner->recv_tuples = 0;
    combiner->send_slots = 0;
    combiner->local_processed = 0;
    combiner->print_stats_tz = 0;
#endif

    combiner->merge_sort = false;
    combiner->local_store_empty = true;
    combiner->tuplesortstate = NULL;

    combiner->tuplestorestate = NULL;
    combiner->local_slot = NULL;
    combiner->rda_shmem_ptr = NULL;
    combiner->rda_shmem_size = 0;
    combiner->out_frames = NULL;
    combiner->in_frames = NULL;
    combiner->in_frames_array = NULL;
}

RemoteDataAccessState *
ExecInitRemoteDataAccess(RemoteDataAccess *node, EState *estate, int eflags)
{
    RemoteDataAccessState   *remotestate;
    RDAResponseCombiner     *combiner;
    struct rusage           start_r;
    struct timeval          start_t;

#ifdef _MIGRATE_
    Oid groupid = InvalidOid;
    Oid reloid = InvalidOid;
    ListCell *table;
#endif

    if (log_remote_data_access_stats)
        ResetUsageCommon(&start_r, &start_t);

    remotestate = makeNode(RemoteDataAccessState);
    remotestate->bound = false;
    remotestate->eflags = eflags;
    combiner = (RDAResponseCombiner *)remotestate;

    /* parse rda id from cursor string */
    if (node->cursor)
    {
        char *end = NULL;
        remotestate->rda_id = strtoll(node->cursor, &end, 10);
    }
    else
        elog(ERROR, "Fail to get rda sequence id.");

    elog(LOG, "in ExecInitRemoteDataAccess with rda id: %ld.", remotestate->rda_id);

    /* init remote nodes expect myself */
    if (node->nodeList)
    {
        ListCell *lc;
        foreach(lc, node->nodeList)
        {
            int nodeid = lfirst_int(lc);
            if (nodeid != (PGXCNodeId - 1))
                remotestate->execNodes = lappend_int(remotestate->execNodes, nodeid);
        }
    }
    remotestate->execOnAll = node->execOnAll;
    InitRDAResponseCombiner(combiner, 0, COMBINE_TYPE_NONE);

    combiner->ss.ps.plan = (Plan *)node;
    combiner->ss.ps.state = estate;
    combiner->ss.ps.ExecProcNode = ExecRemoteDataAccess;
    combiner->ss.ps.qual = NULL;
    combiner->node_count = list_length(remotestate->execNodes);

    ExecInitResultTupleSlot(estate, &combiner->ss.ps);
    ExecAssignResultTypeFromTL((PlanState *)remotestate);

    /* get this cluster dataNode map */
    PgxcNodeGetOidsExtend(NULL, &remotestate->dnOids,
                          NULL, NULL, &remotestate->numDns, NULL, true);

    /*
     * If we are going to execute subplan locally or doing explain initialize
     * the subplan. Otherwise have remote node doing that.
     */

    outerPlanState(remotestate) = ExecInitNode(outerPlan(node), estate, eflags);

    if (node->distributionNodes)
    {
        Oid distributionType = InvalidOid;
        TupleDesc typeInfo;

        typeInfo = combiner->ss.ps.ps_ResultTupleSlot->tts_tupleDescriptor;
        if (node->distributionKey != InvalidAttrNumber)
        {
            Form_pg_attribute attr;
            attr = typeInfo->attrs[node->distributionKey - 1];
            distributionType = attr->atttypid;
        }
        /* Set up locator */
#ifdef _MIGRATE_
        foreach (table, estate->es_range_table)
        {
            RangeTblEntry *tbl_entry = (RangeTblEntry *)lfirst(table);
            if (tbl_entry->rtekind == RTE_RELATION)
            {
                reloid = tbl_entry->relid;
                elog(DEBUG5, "[ExecInitRemoteDataAccess]reloid=%d", reloid);
                break;
            }
        }
        groupid = GetRelGroup(reloid);
        elog(DEBUG5, "[ExecInitRemoteDataAccess]groupid=%d", groupid);

        remotestate->locator = createLocator(node->distributionType,
                                             RELATION_ACCESS_INSERT,
                                             distributionType,
                                             LOCATOR_LIST_LIST,
                                             0,
                                             (void *)node->distributionNodes,
                                             (void **)&remotestate->dest_nodes,
                                             false,
                                             groupid, InvalidOid, InvalidOid, InvalidAttrNumber,
                                             InvalidOid);
#else
        remotestate->locator = createLocator(node->distributionType,
                                             RELATION_ACCESS_INSERT,
                                             distributionType,
                                             LOCATOR_LIST_LIST,
                                             0,
                                             (void *)node->distributionNodes,
                                             (void **)&remotestate->dest_nodes,
                                             false);
#endif
    }
    else
    {
        /* case for all re-distr, make distr-out node id map */
        ListCell *lc;
        int idx = 0;

        remotestate->dest_nodes = (int *)palloc((combiner->node_count + 1) * sizeof(int));
        foreach(lc, remotestate->execNodes)
        {
            remotestate->dest_nodes[idx++] = lfirst_int(lc);
            elog(LOG, "rda:  remotestate->dest_nodes[%d] :%d", idx - 1, remotestate->dest_nodes[idx]);
        }
        remotestate->dest_nodes[idx] = PGXCNodeId - 1;

        elog(LOG, "rda: %ld re-distr the whole slots out to every node. remotestate->dest_nodes[%d] is %d  eflags is %d", remotestate->rda_id, idx, remotestate->dest_nodes[idx], eflags);
    }

    /*
     * It does not makes sense to merge sort if there is only one tuple source.
     * By the contract it is already sorted
     */
    if (node->sort && node->execOnAll && list_length(node->nodeList) > 1)
    {
        combiner->merge_sort = true;
        elog(LOG, "rda: %ld enable merge sort", remotestate->rda_id);
    }

    /*
     * Reference to RemoteSubplan, we don't execute the body(subplan on remote)
     *  in EXPLAIN_ONLY mode
     */
    if (remotestate->execNodes && !(eflags & EXEC_FLAG_EXPLAIN_ONLY))
    {
        /* Connect to remote nodes and send down subplan */
        if (!(eflags & EXEC_FLAG_SUBPLAN))
        {
            remotestate->eflags = eflags;

            /* In parallel worker, no init, do it until we start to run. */
            if (!IsParallelWorker())
            {
                /* Not parallel aware, build connections. */
                if (!node->scan.plan.parallel_aware)
                {
                    ExecFinishInitRemoteDataAccess(remotestate);
                }
                else
                {
                    /* In session process, if we are under gather, do nothing. */
                }
            }
        }
    }

    if (log_remote_data_access_stats)
        ShowUsageCommon("ExecInitRemoteDataAccess", &start_r, &start_t);

    return remotestate;
}

static void
reset_combiner_for_rescan(RDAResponseCombiner *combiner)
{
    HASH_SEQ_STATUS         seq;
    rda_frame_entry         *frame_entry;

    combiner->complete_count = 0;
    combiner->local_fetch_finish = false;
    combiner->send_slots = 0;
    combiner->recv_tuples = 0;
    combiner->local_processed = 0;

    if (combiner->merge_sort)
    {
        tuplesort_end((Tuplesortstate *) combiner->tuplesortstate);
        combiner->tuplesortstate = NULL;

        tuplestore_clear(combiner->tuplestorestate);
    }
    ExecClearTuple(combiner->ss.ps.ps_ResultTupleSlot);

    hash_seq_init(&seq, combiner->out_frames);
    while ((frame_entry = hash_seq_search(&seq)) != NULL)
    {
        frame_entry->eof = false;
        frame_entry->close_pending = false;
        frame_entry->rescan_state = false;
        frame_entry->store_empty = true;

        tuplestore_clear(frame_entry->tuplestore);
        ExecClearTuple(frame_entry->tmpslot);
    }

    hash_seq_init(&seq, combiner->in_frames);
    while ((frame_entry = hash_seq_search(&seq)) != NULL)
    {
        frame_entry->eof = false;
        frame_entry->rescan_state = false;
        frame_entry->recv_datarow = NULL;
    }
}

static void
print_io_stats(RDAResponseCombiner *combiner)
{
    HASH_SEQ_STATUS         seq;
    StringInfoData          str;
    rda_frame_entry         *frame_entry;
    uint64                  out_total = 0;
    uint64                  in_total = 0;

    initStringInfo(&str);
    appendStringInfo(&str, "! RDA %ld stats:\n", combiner->rda_id);

    hash_seq_init(&seq, combiner->out_frames);
    while ((frame_entry = hash_seq_search(&seq)) != NULL)
    {
        rda_dsm_frame *frame = frame_entry->frame;

        out_total += frame_entry->io_bytes;

        appendStringInfo(&str, "\tout-frame nodeid: %d head:0x%X tail:0x%X "
                  "free_space: %u, io_bytes: %lu\n",
                  frame->dest_nodeid, frame->head,
                  frame->tail, rda_frame_free_space(frame),
                  frame_entry->io_bytes);
    }

    hash_seq_init(&seq, combiner->in_frames);
    while ((frame_entry = hash_seq_search(&seq)) != NULL)
    {
        rda_dsm_frame *frame = frame_entry->frame;

        in_total += frame_entry->io_bytes;

        appendStringInfo(&str, "\tin-frame nodeid: %d head:0x%X tail:0x%X "
                  "data_space: %u, io_bytes: %lu\n",
                  frame->dest_nodeid, frame->head,
                  frame->tail, rda_frame_data_size(frame),
                  frame_entry->io_bytes);
    }

    appendStringInfo(&str, "\tout buffer bytes: %lu, in buffer bytes: %lu "
              "send slots: %lu, recv tuples: %lu, process_count: %lu "
              "local_finish: %d",
              out_total, in_total, combiner->send_slots, combiner->recv_tuples,
              combiner->local_processed, combiner->local_fetch_finish);

    ereport(LOG,
            (errmsg_internal("%s", "RemoteDataAccess"),
             errdetail_internal("%s", str.data)));

    pfree(str.data);
}

static void
close_rda_shmem_frames(RemoteDataAccessState *node)
{
    HASH_SEQ_STATUS         seq;
    rda_frame_entry         *frame_entry;
    RDAResponseCombiner     *combiner = (RDAResponseCombiner *)node;

    /* free tuplestore in out-frames */
    hash_seq_init(&seq, combiner->out_frames);
    while ((frame_entry = hash_seq_search(&seq)) != NULL)
    {
        if (frame_entry->tuplestore)
            tuplestore_end(frame_entry->tuplestore);

        if (frame_entry->tmpslot)
            ExecDropSingleTupleTableSlot(frame_entry->tmpslot);

        frame_entry->tuplestore = NULL;
        frame_entry->tmpslot = NULL;
        debug_out_file_close(frame_entry->debug_out_fp);
    }

    /* free datarow which may be remain in in-frames */
    hash_seq_init(&seq, combiner->in_frames);
    while ((frame_entry = hash_seq_search(&seq)) != NULL)
    {
        if (frame_entry->recv_datarow)
            pfree(frame_entry->recv_datarow);
        frame_entry->recv_datarow = NULL;
        debug_out_file_close(frame_entry->debug_out_fp);
    }

    if (combiner->tuplesortstate)
        tuplesort_end((Tuplesortstate *) combiner->tuplesortstate);
    if (combiner->tuplestorestate)
        tuplestore_end(combiner->tuplestorestate);
    combiner->tuplesortstate = NULL;
    combiner->tuplestorestate = NULL;

    ExecClearTuple(combiner->ss.ps.ps_ResultTupleSlot);
 
    pfree(combiner->in_frames_array);
    hash_destroy(combiner->in_frames);
    hash_destroy(combiner->out_frames);

    combiner->local_slot = NULL;
    combiner->in_frames = NULL;
    combiner->out_frames = NULL;
    combiner->in_frames_array = NULL;

    if (combiner->tmp_ctx)
        MemoryContextDelete(combiner->tmp_ctx);
    if (combiner->rda_ctx)
        MemoryContextDelete(combiner->rda_ctx);
    combiner->tmp_ctx = NULL;
    combiner->rda_ctx = NULL;

    dsm_rda_detach(node->rda_id, &combiner->rda_shmem_ptr, &combiner->rda_shmem_size);
}

static void
init_rda_frame_entry(rda_frame_entry * entry, int nodeid, Oid nodeOid)
{
    entry->eof = false;
    entry->remote_nodeOid = nodeOid;
    entry->remote_nodeid = nodeid;
    entry->local_nodeid = PGXCNodeId - 1;
    entry->debug_out_fp = NULL;
    entry->io_bytes = 0;
    entry->seq_num = 0;
    entry->tuplestore = NULL;
    entry->tmpslot = NULL;
    entry->recv_datarow = NULL;
    entry->store_empty = true;
    entry->close_pending = false;
    entry->rescan_state = false;
    memset(&entry->tup, 0x0, sizeof(rda_dsm_tup));

    /* update nodeid in frame */
    entry->frame->src_nodeid = (uint32_t)(PGXCNodeId - 1);
    entry->frame->dest_nodeid = (uint32_t)nodeid;
}

static Tuplestorestate *
rda_alloc_tuplestore(int maxKBytes, char *name, MemoryContext ctx)
{
    int ptrno PG_USED_FOR_ASSERTS_ONLY;
    Tuplestorestate *tuplestore = tuplestore_begin_datarow(false, maxKBytes, ctx);

    tuplestore_collect_stat(tuplestore, name);
    /*
     * Allocate a second read pointer to read from the store. We know
     * it must have index 1, so needn't store that.
     */
    ptrno = tuplestore_alloc_read_pointer(tuplestore, 0);
    Assert(ptrno == 1);

    return tuplestore;
}

/*
 * init_rda_shmem_frames
 * create shared-memory for rda, and split into frames in hash table
 */
static void
init_rda_shmem_frames(RemoteDataAccessState *node)
{
    bool                found;
    bool                ret;
    int                 idx;
    int                 tuple_store_size = 0;
    HASHCTL             ctl;
    ListCell            *item;
    int32_t             request_size;
    rda_dsm_frame       **in_frames;
    rda_dsm_frame       **out_frames;
    char                storename[128];
    MemoryContext       old_ctx;
    RDAResponseCombiner *combiner = (RDAResponseCombiner *)node;
    EState              *estate = combiner->ss.ps.state;
    TupleTableSlot      *resultslot = combiner->ss.ps.ps_ResultTupleSlot;

    combiner->rda_id = node->rda_id;
    snprintf(storename, 128, "RDA %ld", node->rda_id);
    combiner->rda_ctx = AllocSetContextCreate(estate->es_query_cxt,
                                              storename,
                                              ALLOCSET_DEFAULT_SIZES);

    snprintf(storename, 128, "RDA %ld temp", node->rda_id);
    combiner->tmp_ctx = AllocSetContextCreate(combiner->rda_ctx,
                                                storename,
                                                ALLOCSET_DEFAULT_SIZES);
    request_size = RDA_FRAME_SIZE * (combiner->node_count * 2);

    /* create shared-memory and map it */
    ret = dms_rda_create_and_attach(node->rda_id, request_size,
                    &combiner->rda_shmem_ptr, &combiner->rda_shmem_size);
    if (!ret)
    {
        ereport(ERROR,
                (errcode(ERRCODE_OUT_OF_MEMORY),
                    errmsg("Failed to create shared-memory for RDA: %ld", node->rda_id)));
    }

    /* make hash table for rda shared-memory frame */
    memset(&ctl, 0, sizeof(HASHCTL));
    ctl.keysize = sizeof(Oid);
    ctl.entrysize = sizeof(rda_frame_entry);
    ctl.hash = oid_hash;
    ctl.hcxt = combiner->rda_ctx;
    combiner->out_frames = hash_create("RDA Frame Out Hash", 64, &ctl,
                                                    HASH_ELEM | HASH_FUNCTION | HASH_CONTEXT);

    memset(&ctl, 0, sizeof(HASHCTL));
    ctl.keysize = sizeof(Oid);
    ctl.entrysize = sizeof(rda_frame_entry);
    ctl.hash = oid_hash;
    ctl.hcxt = combiner->rda_ctx;
    combiner->in_frames = hash_create("RDA Frame In Hash", 64, &ctl,
                                                    HASH_ELEM | HASH_FUNCTION | HASH_CONTEXT);

    /* switch rda memory context for frames */
    old_ctx = MemoryContextSwitchTo(combiner->rda_ctx);

    /* get mmaped frames */
    in_frames = (rda_dsm_frame **)
                    palloc(combiner->node_count * sizeof(rda_dsm_frame *));

    out_frames = (rda_dsm_frame **)
                    palloc(combiner->node_count * sizeof(rda_dsm_frame *));

    dsm_map_rda_frames(combiner->rda_shmem_ptr, combiner->rda_shmem_size,
                       combiner->node_count, combiner->node_count,
                       in_frames, out_frames, true);

    /* build in-frames array at the same time */
    combiner->in_frames_array = (rda_frame_entry **)palloc(
                            combiner->node_count * sizeof(rda_frame_entry *));

    idx = 0;
    tuple_store_size = work_mem / combiner->node_count;
    tuple_store_size = tuple_store_size > 1024 ? tuple_store_size : 1024;
    foreach(item, node->execNodes)
    {
        rda_frame_entry *entry;
        int nodeid = lfirst_int(item);
        Oid nodeOid = node->dnOids[nodeid];

        /* build in-frames hash entry */
        entry = (rda_frame_entry *)hash_search(combiner->in_frames,
                                               &nodeOid, HASH_ENTER, &found);
        entry->frame = in_frames[idx];
        entry->combiner = combiner;
        init_rda_frame_entry(entry, nodeid, nodeOid);
        if (rda_io_buffer_debug_out_to_file)
        {
            entry->debug_out_fp = debug_out_file_open(combiner->rda_id, nodeid, OWNER_BACKEND_FRAME_DEQUEUE);
        }

        /* fill in-frames array */
        combiner->in_frames_array[idx] = entry;

        /* build out-frames hash entry */
        entry = (rda_frame_entry *)hash_search(combiner->out_frames,
                                               &nodeOid, HASH_ENTER, &found);
        entry->frame = out_frames[idx];
        entry->combiner = combiner;
        init_rda_frame_entry(entry, nodeid, nodeOid);
        if (rda_io_buffer_debug_out_to_file)
        {
            entry->debug_out_fp = debug_out_file_open(combiner->rda_id, nodeid, OWNER_BACKEND_FRAME_ENQUEUE);
        }

        entry->tmpslot = MakeSingleTupleTableSlot(resultslot->tts_tupleDescriptor);

        /* create tuplestore for out-frames */
        snprintf(storename, 128, "%ld node %d", node->rda_id, entry->remote_nodeid);
        entry->tuplestore = rda_alloc_tuplestore(tuple_store_size, storename, combiner->tmp_ctx);

        idx++;
    }
    if (combiner->merge_sort)
    {
        snprintf(storename, 128, "%ld node local", node->rda_id);
        combiner->tuplestorestate = rda_alloc_tuplestore(tuple_store_size, storename, combiner->tmp_ctx);
    }

    MemoryContextSwitchTo(old_ctx);
    pfree(in_frames);
    pfree(out_frames);
}

/*
 * push_cached_slot_to_out_frames
 * Push all cached slot in tuplestore out to rda-frame, return
 *  the number of frame which cached slot is not empty
 */
static int
push_cached_slot_to_out_frames(RDAResponseCombiner *combiner)
{
    bool                    ret;
    int                     remaining = 0;
    HASH_SEQ_STATUS         seq;
    rda_frame_entry         *frame_entry;

    hash_seq_init(&seq, combiner->out_frames);
    while ((frame_entry = hash_seq_search(&seq)) != NULL)
    {
        if (frame_entry->eof)
            continue;

        /* free space is too small to put one slot */
        if (rda_frame_free_space(frame_entry->frame) <= RDA_TUP_HEAD_SIZE)
        {
            if (!frame_entry->store_empty || frame_entry->close_pending)
                remaining ++;
            continue;
        }

        ret = send_tuplestore_to_rda_frame(frame_entry);
        if (!ret)
        {
            elog(DEBUG1, "rda: %ld couldn't send slot in tuplestore to "
                        "frame: %d head: 0x%X tail: 0x%X free: %u, total: %lu",
                    combiner->rda_id, frame_entry->remote_nodeid,
                    frame_entry->frame->head, frame_entry->frame->tail,
                    rda_frame_free_space(frame_entry->frame), frame_entry->io_bytes);
            remaining ++;
            continue;
        }

        if (!frame_entry->close_pending)
            continue;

        ret = send_close_to_rda_frame(frame_entry);
        if (!ret)
        {
            elog(DEBUG1, "rda: %ld couldn't send 'close' to frame: %d "
                         "head: 0x%X tail: 0x%X free: %u, total: %lu",
                         combiner->rda_id, frame_entry->remote_nodeid,
                         frame_entry->frame->head, frame_entry->frame->tail,
                         rda_frame_free_space(frame_entry->frame), frame_entry->io_bytes);
            remaining ++;
            continue;
        }

        elog(LOG, "send close to nodeid: %d on rda: %ld",
                        frame_entry->remote_nodeid, combiner->rda_id);
        frame_entry->eof = true;
        frame_entry->close_pending = false;
    }

    return remaining;
}

static TupleTableSlot *
redistrute_outer_slot(RemoteDataAccessState *node, TupleTableSlot *slot)
{
    int                     i;
    Datum                   value = (Datum)0;
    bool                    isnull = false;
    int                     numnodes;
    bool                    deliver_local = false;
    RDAResponseCombiner     *combiner = (RDAResponseCombiner *)node;
    RemoteDataAccess        *plan = (RemoteDataAccess *)combiner->ss.ps.plan;

    if (node->locator)
    {
        if (plan->distributionKey == InvalidAttrNumber)
            isnull = true;
        else
            value = slot_getattr(slot, plan->distributionKey, &isnull);
#ifdef __COLD_HOT__
        numnodes = GET_NODES(node->locator, value, isnull, 0, true, NULL);
#else
        numnodes = GET_NODES(node->locator, value, isnull, NULL);
#endif
    }
    else
    {
        /* we will distr-out slot to all nodes include myself */
        numnodes = combiner->node_count + 1;
    }

    for (i = 0; i < numnodes; i++)
    {
        int consumerIdx = node->dest_nodes[i];
        if (consumerIdx == PGXCNodeId - 1)
        {
            deliver_local = true;
            if (combiner->merge_sort)
                save_local_slot_to_store(combiner, slot);
        }
        else
        {
            Oid out_Oid = node->dnOids[consumerIdx];
            rda_frame_entry *frame_entry = (rda_frame_entry *)
                        hash_search(combiner->out_frames, &out_Oid, HASH_FIND, NULL);

            if (!send_slot_to_rda_frame(frame_entry, slot))
            {
                elog(WARNING, "fail to send slot for node: %d", frame_entry->remote_nodeid);
            }
        }
    }

    if (deliver_local && !combiner->merge_sort)
        return slot;

    return NULL;
}

static bool 
save_local_slot_to_store(RDAResponseCombiner *combiner, TupleTableSlot *slot)
{
    /*
     * in merge-sort mode, fetch all slots to re-distr, save local slot to tuplestore
     */
    tuplestore_puttupleslot(combiner->tuplestorestate, slot);
    combiner->local_store_empty = false;

    return true;
}

static bool
get_local_slot_from_store(RDAResponseCombiner *combiner)
{
    TupleTableSlot              *resultslot = combiner->ss.ps.ps_ResultTupleSlot;

    return tuplestore_gettupleslot(combiner->tuplestorestate,
                                            true, true, resultslot);
}

static bool
send_slot_to_rda_frame(rda_frame_entry *frame_entry, TupleTableSlot *slot)
{
    RemoteDataRow           datarow;
    bool                    free_datarow;
    MemoryContext           old_ctx;

    if (frame_entry == NULL || TupIsNull(slot))
        return true;

    if (frame_entry->close_pending || frame_entry->eof)
    {
        elog(ERROR, "rda: %ld node frame: %d is closing, err!!!",
                     frame_entry->combiner->rda_id, frame_entry->remote_nodeid);
        return false;
    }

    /* do our best to send cached slot to frame */
    if (send_tuplestore_to_rda_frame(frame_entry))
    {
        /* Get datarow from the tuple slot */
        if (slot->tts_datarow)
        {
            /*
            * The function ExecCopySlotDatarow always make a copy, but here we
            * can optimize and avoid copying the data, so we just get the reference
            */
            datarow = slot->tts_datarow;
            free_datarow = false;
        }
        else
        {
            /* switch to rda memory for slot copy */
            old_ctx = MemoryContextSwitchTo(frame_entry->combiner->rda_ctx);
            datarow = ExecCopySlotDatarow(slot, frame_entry->combiner->tmp_ctx);
            MemoryContextSwitchTo(old_ctx);
            free_datarow = true;
        }

        if (send_datarow_to_rda_frame(frame_entry, datarow))
        {
            frame_entry->combiner->send_slots ++;
            elog(DEBUG1, "send datarow : %d to frame:%d on rda: %ld",
                            datarow->msglen, frame_entry->remote_nodeid,
                            frame_entry->combiner->rda_id);
            /* return true if send slot to shared-memory directly */
            if (free_datarow)
                pfree(datarow);
            return true;
        }
        if (free_datarow)
            pfree(datarow);
    }

    /* save slot to tuplestore */
    tuplestore_puttupleslot(frame_entry->tuplestore, slot);
    frame_entry->store_empty = false;

    return true;
}

static bool
send_tuplestore_to_rda_frame(rda_frame_entry *frame_entry)
{
    bool            ret;
    bool            send_all = false;
    TupleTableSlot  *tmpslot = frame_entry->tmpslot;
    Tuplestorestate *tuplestore = frame_entry->tuplestore;

    if (frame_entry == NULL || frame_entry->store_empty)
        return true;

    //elog(DEBUG1, "try to send slot in tuplestore to frame");

    ExecClearTuple(tmpslot);

    /* Read from tuplestore and send the tuple.*/
    /*
     * Tuplestore does not clear eof flag on the active read pointer, causing
     * the store is always in EOF state once reached when there is a single
     * read pointer. We do not want behavior like this and workaround by using
     * secondary read pointer. Primary read pointer (0) is active when we are
     * writing to the tuple store, also it is used to bookmark current position
     * when reading to be able to roll back and return just read tuple back to
     * the store if we failed to write it out to the queue.
     * Secondary read pointer is for reading, and its eof flag is cleared if a
     * tuple is written to the store.
     */
    tuplestore_select_read_pointer(tuplestore, 1);

    while (!tuplestore_ateof(tuplestore))
    {
        /* save position */
        tuplestore_copy_read_pointer(tuplestore, 1, 0);

        /* Try to get next tuple to the temporary slot */
        if (!tuplestore_gettupleslot(tuplestore, true, false, tmpslot))
        {
            send_all = true;
            break;
        }

        /* The slot should contain a data row */
        Assert(tmpslot->tts_datarow);

        /* send slot in tuplestore to shared-memory */
        ret = send_datarow_to_rda_frame(frame_entry, tmpslot->tts_datarow);
        if (!ret)
        {
            /* Restore read position to get same tuple next time */
            tuplestore_copy_read_pointer(tuplestore, 0, 1);

            /* We might advance the mark, try to truncate */
            tuplestore_trim(tuplestore);

            /* Prepare for writing, set proper read pointer */
            tuplestore_select_read_pointer(tuplestore, 0);
            
            send_all = false;
            break;
        }
        frame_entry->combiner->send_slots ++;
        elog(DEBUG1, "send tuplestore datarow : %d to frame: %d",
                                tmpslot->tts_datarow->msglen, frame_entry->remote_nodeid);
    }

    /* Handle the latest row. */
    send_all = tuplestore_ateof(tuplestore);

    /* Remove rows we have just read */
    tuplestore_trim(tuplestore);

    /* prepare for writes, set read pointer 0 as active */
    tuplestore_select_read_pointer(tuplestore, 0);

    /* re-flag tuplestore empty */
    frame_entry->store_empty = send_all;

    return send_all;
}

static bool
send_datarow_to_rda_frame(rda_frame_entry *frame_entry, RemoteDataRow datarow)
{
    bool            ret;
    uint32_t        package_size;

    if (frame_entry == NULL || datarow == NULL)
        return true;

    /* check free space is enough or not */
    package_size = datarow->msglen + sizeof(rda_dsm_tup);
    if (rda_frame_free_space(frame_entry->frame) < package_size)
        return false;

    /* ready to send datarow */
    ret = rda_frame_enqueue_data(&frame_entry->debug_out_fp, frame_entry->seq_num++,
                                frame_entry->frame, 'c', datarow->msglen,
                                frame_entry->remote_nodeid, (uint8_t *)datarow->msg);
    if (ret)
        frame_entry->io_bytes += package_size;

    return ret;
}

/*
 * send_close_to_rda_frame
 * Send close-package to the out-frame.
 */
static bool
send_close_to_rda_frame(rda_frame_entry *frame_entry)
{
    /*
     * 1. check tuplestore is empty
     * 2. flags 'close_pending' if tuplestore is not empty or
     *     free space is not enough in frame
     * 3. write close-package to frame if free space is enough
     */

    bool        ret;
    uint16_t    type = 'd';
    uint8_t     data[4] = {'E', 'O', 'F', 0x0};

    if (frame_entry == NULL || frame_entry->eof)
        return true;

    /* pending 'close' if any slot in tuplestore */
    if (!frame_entry->store_empty)
    {
        frame_entry->close_pending = true;
        return false;
    }

    /* pending 'close' if shared-memory space is not enough */
    if (rda_frame_free_space(frame_entry->frame) < sizeof(rda_dsm_tup) + 4)
    {
        frame_entry->close_pending = true;
        return false;
    }

    ret = rda_frame_enqueue_data(&frame_entry->debug_out_fp, frame_entry->seq_num++,
                                frame_entry->frame, type, 4,
                                frame_entry->remote_nodeid, data);
    if (!ret)
        frame_entry->close_pending = true;
    else
    {
        frame_entry->io_bytes += (sizeof(rda_dsm_tup) + 4);
        elog(DEBUG1, "Send 'close' to nodeid: %d", frame_entry->remote_nodeid);
    }

    return ret;
}

/*
 * send_rescan_to_rda_frame
 * Send rescan-package out to rda-frame, write shared-memory directly as we
 *  clean cached slots in tuplestore before
 */
static bool
send_rescan_to_rda_frame(rda_frame_entry *frame_entry)
{
    bool        ret;
    uint16_t    type = 'r';
    uint8_t     data[7] = {'R', 'E', 'S', 'C', 'A', 'N', 0x0};

    Assert(frame_entry->eof);
    Assert(frame_entry->store_empty);

    /* frame free space is not enough for rescan package */
    if (rda_frame_free_space(frame_entry->frame) < sizeof(rda_dsm_tup) + 7)
        return false;

    ret = rda_frame_enqueue_data(&frame_entry->debug_out_fp, frame_entry->seq_num++,
                                frame_entry->frame, type, 7,
                                frame_entry->remote_nodeid, data);
    if (ret)
        frame_entry->io_bytes += (sizeof(rda_dsm_tup) + 7);

    return ret;
}

/*
 * send_close_to_all_rda_frames
 * Send 'close' package to all out-rda-frames when local slot is empty
 */
static void
send_close_to_all_rda_frames(RDAResponseCombiner *combiner)
{
    HASH_SEQ_STATUS         seq;
    rda_frame_entry         *frame_entry;

    hash_seq_init(&seq, combiner->out_frames);
    while ((frame_entry = hash_seq_search(&seq)) != NULL)
    {
        if (frame_entry->eof)
            continue;

        if (send_close_to_rda_frame(frame_entry))
        {
            elog(LOG, "send close to nodeid: %d rda: %ld ok",
                    frame_entry->remote_nodeid, combiner->rda_id);
            frame_entry->close_pending = false;
            frame_entry->eof = true;
        }
        else
            elog(WARNING, "close-package is pending to node: %d rda: %ld",
                             frame_entry->remote_nodeid, combiner->rda_id);
    }
}

/*
 * force_send_rescan_to_all_rda_frames
 * Force to send rescan-package to all out-frames
 */
static void
force_send_rescan_to_all_rda_frames(RDAResponseCombiner *combiner)
{
    int             debug_cnt = 0;
    int             remaining = 0;

    do {
        HASH_SEQ_STATUS         seq;
        rda_frame_entry         *frame_entry;

        remaining = 0;
        hash_seq_init(&seq, combiner->out_frames);
        while ((frame_entry = hash_seq_search(&seq)) != NULL)
        {
            if (frame_entry->rescan_state)
                continue;

            if (send_rescan_to_rda_frame(frame_entry))
            {
                frame_entry->rescan_state = true;
                elog(LOG, "send rescan to nodeid: %d rda: %ld ok",
                                frame_entry->remote_nodeid, combiner->rda_id);
            }
            else
                remaining ++;
        }

        if (remaining > 0 && (++debug_cnt % RDA_LOG_INTERVAL_TIMES) == 0)
        {
            elog(LOG, "couldn't send rescan-package out on rda: %ld", combiner->rda_id);
            if (debug_cnt > RDA_SEND_WAIT_TIMES)
            {
                print_io_stats(combiner);

                ereport(LOG,
                         (errcode(ERRCODE_DATA_EXCEPTION),
                            errmsg("Timeout to send rescan-package on rda: %ld",
                                                                 combiner->rda_id)));
            }
        }

        pg_usleep(RDA_LOG_INTERVAL * 1000L);
    } while(remaining > 0);
}

/*
 * force_push_cached_slot_out
 * Push all cached slots in tuplestore out to shared-memory frame
 * We must send all slots out at the end of RDA
 * And we will discard all slots receviced when Rescan process, we
 *  should clean the receviced frame memory quickly in case other
 *  node block to send.
 */
static void
force_push_cached_slot_out(RDAResponseCombiner *combiner, bool discard_in)
{
    int             remaining = 0;
    uint64          send_slots = 0;
    int             debug_cnt = 0;

    send_slots = combiner->send_slots;
    do {
        remaining = push_cached_slot_to_out_frames(combiner);

        /* reset counter if we send out same data */
        if (send_slots < combiner->send_slots)
        {
            debug_cnt = 0;
            send_slots = combiner->send_slots;
        }

        /* discard the in-slots during push out when rescan-mode */
        if (discard_in)
            recevice_and_discard_slot_in_frames(combiner);

        if (remaining > 0 && (++debug_cnt % RDA_LOG_INTERVAL_TIMES) == 0)
        {
            elog(LOG, "couldn't send cached slot out on rda: %ld", combiner->rda_id);
            if (debug_cnt > RDA_SEND_WAIT_TIMES)
            {
                print_io_stats(combiner);

                ereport(LOG,
                         (errcode(ERRCODE_DATA_EXCEPTION),
                            errmsg("Timeout to send cached slot on rda: %ld",
                                                                 combiner->rda_id)));
            }
        }

        pg_usleep(RDA_LOG_INTERVAL * 1000L);
    } while(remaining > 0);
}

static TupleTableSlot *
extract_slot_from_remote_frame(RDAResponseCombiner *combiner, rda_frame_entry *frame_entry)
{
    TupleTableSlot              *resultslot = combiner->ss.ps.ps_ResultTupleSlot;

	ExecStoreDataRowTuple((RemoteDataRow)frame_entry->recv_datarow, resultslot, true);

    /* just set pointer to NIL, the new slot will capture the memory */
    frame_entry->recv_datarow = NULL;

    /* try our best to get one remote slot from shared-memory when cache empty */
    if (recevice_slot_from_rda_frame(frame_entry, resultslot->tts_mcxt))
    {
        /* update flags if get 'close' here */
        if (frame_entry->eof)
        {
            elog(LOG, "recevice 'close' from node: %d on rda: %ld",
                            frame_entry->remote_nodeid, combiner->rda_id);
            combiner->complete_count ++;
        }
    }

    return resultslot;
}

/*
 * recevice_rescan_from_in_frame
 * read rescan-package from in-frame, call it after eof
 */
static bool
recevice_rescan_from_in_frame(rda_frame_entry *frame_entry)
{
    bool                    ret;
    rda_dsm_tup_datarow     *datarow;
    uint8_t                 *cal_md5 = NULL;

    if (frame_entry->rescan_state)
        return true;

    /* rescan-package is following eof */
    Assert(frame_entry->eof);

    /* rescan-package is not ready */
    if (rda_frame_data_size(frame_entry->frame) < sizeof(rda_dsm_tup) + 7)
        return false;

    if (frame_entry->tup.magic == 0)
    {
        ret = rda_frame_get_tup_head(&frame_entry->debug_out_fp, frame_entry->frame, &frame_entry->tup);
        if (!ret)
            return false;
    }

    /* only rescan-package is expected */
    Assert(frame_entry->tup.magic == PG_RDA_TUP_MAGIC);
    Assert(frame_entry->tup.type == 'r');

    datarow = (rda_dsm_tup_datarow *) palloc(sizeof(rda_dsm_tup_datarow)
                                            + frame_entry->tup.msglen);
    datarow->msgnode = frame_entry->tup.msgnode;
    datarow->msglen = frame_entry->tup.msglen;
    
#ifdef DATAROW_MD5_CHECK
    cal_md5 = (uint8_t *)frame_entry->tup.md5;
#endif 

    /* read the remaining package */
    ret = rda_frame_get_tup_data(&frame_entry->debug_out_fp, frame_entry->frame, datarow, cal_md5);
    if (ret)
    { 
        frame_entry->io_bytes += (sizeof(rda_dsm_tup) + datarow->msglen);
        frame_entry->rescan_state = true;
        memset(&frame_entry->tup, 0x0, sizeof(rda_dsm_tup));
    }
    pfree(datarow);

    return ret;
}

/*
 * wait_rescan_in_frames
 * wait all remote nodes is in rescan-mode state
 */
static void
wait_rescan_in_frames(RDAResponseCombiner *combiner)
{
    int                     debug_cnt = 0;
    int                     remaining;

    do {
        HASH_SEQ_STATUS         seq;
        rda_frame_entry         *frame_entry;

        remaining = 0;
        hash_seq_init(&seq, combiner->in_frames);
        while ((frame_entry = hash_seq_search(&seq)) != NULL)
        {
            Assert(frame_entry->eof);

            if (frame_entry->rescan_state)
                continue;

            if (recevice_rescan_from_in_frame(frame_entry))
            {
                elog(LOG, "recieve rescan to nodeid: %d rda: %ld ok",
                                frame_entry->remote_nodeid, combiner->rda_id);
                continue;
            }                

            remaining ++;
        }

        if ((++debug_cnt % RDA_LOG_INTERVAL_TIMES) == 0)
        {
            elog(LOG, "couldn't recv rescan on rda: %ld", combiner->rda_id);
            if (debug_cnt > RDA_WAIT_RESCAN_TIMES)
            {
                print_io_stats(combiner);

                ereport(LOG,
                         (errcode(ERRCODE_DATA_EXCEPTION),
                            errmsg("Timeout to recv rescan on rda: %ld",
                                                                 combiner->rda_id)));
            }
        }

        pg_usleep(RDA_LOG_INTERVAL * 1000L);
    } while(remaining > 0);
}

/*
 * force_discard_all_slot_in_frames
 * Fetch all slots from remote node until eof,  timeout error will happen if
 *  none slot receviced in gap 20s
 */
static void
force_discard_all_slot_in_frames(RDAResponseCombiner *combiner)
{
    uint64          recv_tuples;
    int             debug_cnt = 0;

    recv_tuples = combiner->recv_tuples;
    do {
        recevice_and_discard_slot_in_frames(combiner);

        /* reset counter if we recevice more data */
        if (recv_tuples < combiner->recv_tuples)
        {
            debug_cnt = 0;
            recv_tuples = combiner->recv_tuples;
        }

        if ((++debug_cnt % RDA_LOG_INTERVAL_TIMES) == 0)
        {
            elog(LOG, "couldn't discard slot on rda: %ld recv: %ld",
                                            combiner->rda_id, combiner->recv_tuples);
            if (debug_cnt > RDA_RECEIVE_WAIT_TIMES)
            {
                ereport(LOG,
                         (errcode(ERRCODE_DATA_EXCEPTION),
                            errmsg("Timeout to discard slot on rda: %ld",
                                                                 combiner->rda_id)));
            }
        }

        pg_usleep(RDA_LOG_INTERVAL * 1000L);
    } while(combiner->complete_count < combiner->node_count);
}

/*
 * recevice_and_discard_slot_in_frames
 * Drop all slots in rda-frames until eof or frame empty
 */
static void
recevice_and_discard_slot_in_frames(RDAResponseCombiner *combiner)
{
    HASH_SEQ_STATUS         seq;
    rda_frame_entry         *frame_entry;
    TupleTableSlot          *resultslot = combiner->ss.ps.ps_ResultTupleSlot;

    if (combiner->complete_count >= combiner->node_count)
        return;

    hash_seq_init(&seq, combiner->in_frames);
    while ((frame_entry = hash_seq_search(&seq)) != NULL)
    {
        if (frame_entry->eof)
            continue;

        /* read in all datarow and discard */
        while (recevice_slot_from_rda_frame(frame_entry, resultslot->tts_mcxt))
        {
            if (frame_entry->eof)
            {
                elog(LOG, "recevice 'close' from node: %d on rda: %ld",
                            frame_entry->remote_nodeid, combiner->rda_id);
                combiner->complete_count ++;
                break;
            }

            /* discard slot, and read again */
            pfree(frame_entry->recv_datarow);
            frame_entry->recv_datarow = NULL;
        }
    }
}

/*
 * recevice_slot_from_any_frame
 * Get a slot from remote node on rda-frame, when 'force' is setting, we must
 *  get one to return until it's timeout.
 */
static rda_frame_entry *
recevice_slot_from_any_frame(RDAResponseCombiner *combiner, bool force,
                                                MemoryContext result_mcxt)
{
    int             i;
    bool            ret;
    int             debug_cnt = 0;

    /* all in-frames channel are closed, no more slot in frames */
    if (combiner->complete_count >= combiner->node_count)
        return NULL;

    do 
	{
        if (combiner->current_conn < 0 ||
                            combiner->current_conn >= combiner->node_count)
        {
            combiner->current_conn = 0;
        }

        for (i = 0; i < combiner->node_count; i++)
        {
            rda_frame_entry *frame_entry = combiner->in_frames_array[(combiner->current_conn + i) % combiner->node_count];
            if (frame_entry->eof)
            {
                continue;
            }
                

            ret = recevice_slot_from_rda_frame(frame_entry, result_mcxt);
            if (!ret)
            {
                continue;
            }

            if (frame_entry->eof)
            {
                elog(LOG, "recevice 'close' from node: %d on rda: %ld",
                                frame_entry->remote_nodeid, combiner->rda_id);
                combiner->complete_count ++;
            }
            else
            {
                if (debug_cnt > 0)
                {
                    elog(LOG, "recevice a tuple from node: %d on rda: %ld, wait time %d ms", frame_entry->remote_nodeid, combiner->rda_id, debug_cnt * RDA_LOG_INTERVAL);
                }
                
                combiner->current_conn++; 
                return frame_entry;
            }
        }

        if (combiner->complete_count >= combiner->node_count)
        {
            return NULL;
        }    

        /* add debug to wait too long */
        if ((++debug_cnt % RDA_LOG_INTERVAL_TIMES) == 0)
        {
            elog(LOG, "could not get any slot on rda: %ld, complete_cnt: %d",
                      combiner->rda_id, combiner->complete_count);

            for (i = 0; i < combiner->node_count; i++)
            {
                rda_frame_entry *frame_entry = combiner->in_frames_array[i];
                if (!frame_entry->eof)
                    elog(DEBUG1, "rda: %ld wait slot on frame: %d "
                                 "head: 0x%X tail: 0x%X content: %u, total: %lu",
                                 combiner->rda_id, frame_entry->remote_nodeid,
                                 frame_entry->frame->head, frame_entry->frame->tail,
                                 rda_frame_data_size(frame_entry->frame), frame_entry->io_bytes);
            }

            if (debug_cnt > RDA_RECEIVE_WAIT_TIMES)
            {
                print_io_stats(combiner);

                ereport(LOG,
                         (errcode(ERRCODE_DATA_EXCEPTION),
                            errmsg("Timeout to get one remote slot on rda: %ld", combiner->rda_id)));
            }
        }

        if ((debug_cnt % RDA_PUSH_INTERVAL_TIMES) == 0)
        {
            /* try to send cached slots during wait coming slots */
            push_cached_slot_to_out_frames(combiner);
        }

        /* if we force to get a slot from shmem, wait F-process a while */
        if (force)
        {
            pg_usleep(RDA_LOG_INTERVAL * 1000L);
        }
    } while(force);

	elog(LOG, "recevice_slot_from_any_frame exit, rda: %ld", combiner->rda_id);
    return NULL;
}

/*
 * Fetch one slot from rda channel
 * get one slot from the special channel, is used in tuplesort for merge-sort.
 */
TupleTableSlot *
fetch_slot_from_rda(RDAResponseCombiner *combiner, int tapenum)
{
    bool                        ret;
    int                         debug_cnt = 0;
    rda_frame_entry             *frame_entry;
    TupleTableSlot              *resultslot = combiner->ss.ps.ps_ResultTupleSlot;

    if (tapenum >= combiner->node_count)
    {
        if (combiner->local_store_empty)
            return NULL;

        if (get_local_slot_from_store(combiner))
            return resultslot;

        combiner->local_store_empty = true;
        return NULL;
    }

    /* now, get one slot from rda-frame */
    frame_entry = combiner->in_frames_array[tapenum];
    if (frame_entry == NULL || frame_entry->eof)
        return NULL;

    do 
    {
        ret = recevice_slot_from_rda_frame(frame_entry, resultslot->tts_mcxt);
        if (ret)
        {
            // got one slot here
            if (frame_entry->eof)
            {
                elog(LOG, "recevice 'close' from node: %d on rda: %ld",
                             frame_entry->remote_nodeid, combiner->rda_id);
                combiner->complete_count ++;

                return NULL;
            }

            return extract_slot_from_remote_frame(combiner, frame_entry);
        }

        /* add debug to wait too long */
        if ((++debug_cnt % RDA_LOG_INTERVAL_TIMES) == 0)
        {
            elog(LOG, "could not get one slot rda: %ld, frame: %d "
                    "head:0x%X tail:0x%X io_bytes: %lu",
                 combiner->rda_id, frame_entry->remote_nodeid,
                 frame_entry->frame->head, frame_entry->frame->tail,
                 frame_entry->io_bytes);
            if (debug_cnt > RDA_RECEIVE_WAIT_TIMES)
            {
                print_io_stats(combiner);

                ereport(LOG,
                         (errcode(ERRCODE_DATA_EXCEPTION),
                            errmsg("Timeout to get one remote slot, rda: %ld, frame: %d",
                                            combiner->rda_id, frame_entry->remote_nodeid)));
            }
        }

        if ((debug_cnt % RDA_PUSH_INTERVAL_TIMES) == 0)
        {
            /* try to send cached slots during wait coming slots */
            push_cached_slot_to_out_frames(combiner);
        }

        /* wait to get one slot from this rda-frame */
        pg_usleep(RDA_LOG_INTERVAL * 1000L);
    } while(true);

    return NULL;
}

/*
 * recevice_slot_from_rda_frame
 * Get one slot from in-frame, return true if get a whole tup.
 */
static bool
recevice_slot_from_rda_frame(rda_frame_entry *frame_entry, MemoryContext result_mcxt)
{
    bool                    ret;
    uint16_t                type;
    rda_dsm_tup_datarow     *datarow;
    MemoryContext           oldcontext;
    uint8_t                 *cal_md5 = NULL;

    if (frame_entry == NULL || frame_entry->eof)
        return false;

    /* one slot is cached in frame */
    if (frame_entry->recv_datarow)
        return true;

    /* get tup struct body at first if cached one is empty */
    if (frame_entry->tup.magic != PG_RDA_TUP_MAGIC)
    {
        ret = rda_frame_get_tup_head(&frame_entry->debug_out_fp, frame_entry->frame, &frame_entry->tup);
        if (!ret)
            return ret;
    }

    /* tup struct body is available now */
    Assert(frame_entry->tup.magic == PG_RDA_TUP_MAGIC);

    /* data is not ready in frame */
    if (rda_frame_data_size(frame_entry->frame) < frame_entry->tup.msglen)
        return false;

    /* here we use datarow as rda tup, be careful of memory */
    oldcontext = MemoryContextSwitchTo(result_mcxt);
    datarow = (rda_dsm_tup_datarow *) palloc(sizeof(rda_dsm_tup_datarow)
                                            + frame_entry->tup.msglen);
    datarow->msgnode = frame_entry->tup.msgnode;
    datarow->msglen = frame_entry->tup.msglen;

#ifdef DATAROW_MD5_CHECK
    cal_md5 = (uint8_t *)frame_entry->tup.md5;
#endif  

    /* ready to get tup struct message content */
    /* cal_md5 length is 32 */
    ret = rda_frame_get_tup_data(&frame_entry->debug_out_fp, frame_entry->frame,
                                 datarow, cal_md5);
    if (!ret)
    {
        /* fail to get content */
        pfree(datarow);
        MemoryContextSwitchTo(oldcontext);
        return ret;
    }

    /* clean the cached tup here as we get a whole one */
    type = frame_entry->tup.type;
    frame_entry->io_bytes += (sizeof(rda_dsm_tup) + datarow->msglen);
    memset(&frame_entry->tup, 0x0, sizeof(rda_dsm_tup));

    /* here we get a whole tup message */
    switch (type)
    {
   		case 'd':
    	{
        	elog(DEBUG1, "recevice 'close' in raw");
        	/* close package */
        	frame_entry->eof = true;
        	pfree(datarow);
        	break;
    	}
    	case 'c':
    	{
        	elog(DEBUG1, "recevice raw:%p 0:%x, 1:%x", datarow->msg,
            					                        datarow->msg[0], datarow->msg[1]);
        	/* get rowdata */
        	frame_entry->combiner->recv_tuples ++;
        	frame_entry->recv_datarow = datarow;
        	break;
    	}
    	default:
        	ret = false;
        	pfree(datarow);
        	elog(ERROR, "nuknown tup message type: %c", (char)type);
    }

    MemoryContextSwitchTo(oldcontext);
    return ret;
}

/*
 * recevice_slot_from_fwd_local_frame
 * Get one slot from fwd local in-frame, return true if get a whole tup.
 */
bool
recevice_slot_from_fwd_local_frame(FWDLocalRDAFrameEntry *frame_entry)
{
    bool                    ret  = false;
    uint16_t                type = 0;
    rda_dsm_tup_datarow     *datarow = NULL;
    FILE                    *file    = NULL;

    if (frame_entry == NULL || frame_entry->eof)
    {
        return false;
    }

    /* one slot is cached in frame */
    if (frame_entry->recv_datarow)
    {
        return true;
    }

    /* get tup struct body at first if cached one is empty */
    if (frame_entry->tup.magic != PG_RDA_TUP_MAGIC)
    {
        ret = rda_frame_get_tup_head(&file, frame_entry->frame, &frame_entry->tup);
        if (!ret)
        {
            return ret;
        }
    }

    /* tup struct body is available now */
    Assert(frame_entry->tup.magic == PG_RDA_TUP_MAGIC);

    /* data is not ready in frame */
    if (rda_frame_data_size(frame_entry->frame) < frame_entry->tup.msglen)
    {
        return false;
    }
    
    datarow = (rda_dsm_tup_datarow *) malloc(sizeof(rda_dsm_tup_datarow) + frame_entry->tup.msglen);
    if (NULL == datarow)
    {
        return false;
    }
    datarow->msgnode = frame_entry->tup.msgnode;
    datarow->msglen = frame_entry->tup.msglen;

    /* ready to get tup struct message content */
    /* cal_md5 length is 32 */
    ret = rda_frame_get_tup_data(&file, frame_entry->frame, datarow, NULL);
    if (!ret)
    {
        /* fail to get content */
        free(datarow);
        return ret;
    }

    /* clean the cached tup here as we get a whole one */
    type = frame_entry->tup.type;
    frame_entry->io_bytes += (sizeof(rda_dsm_tup) + datarow->msglen);
    memset(&frame_entry->tup, 0x0, sizeof(rda_dsm_tup));

    /* here we get a whole tup message */
    switch (type)
    {
   		case 'd':
    	{
        	/* close package */
        	frame_entry->eof = true;
        	free(datarow);
        	break;
    	}
    	case 'c':
    	{
        	/* get rowdata */
        	frame_entry->recv_datarow = datarow;
        	break;
    	}
    	default:
        	ret = false;
        	free(datarow);
    }
    return ret;
}


void ExecFinishInitRemoteDataAccess(RemoteDataAccessState *node)
{
    int                 ret;
    int                 i = 0;
    int                 *nodes_idx;
    ListCell            *node_list_item;
    RDAResponseCombiner *combiner = (RDAResponseCombiner *)node;
    int retry_idx = 0;
    bool register_success_flag = false;

    if(node->finish_init)
        return;

    elog(LOG, "in ExecFinishRemoteDataAccess(%ld).", node->rda_id);

    /* init and map shared-memory */
    init_rda_shmem_frames(node);

    ConnectForwarderRDA();

    /* prepare args for forward and shared-memory */
    i = 0;
    nodes_idx = palloc(combiner->node_count * sizeof(int));
    foreach (node_list_item, node->execNodes)
    {
        int nodeid = lfirst_int(node_list_item);
        nodes_idx[i++] = nodeid;
    }

    while (retry_idx < RETRY_NUM)
    {
        ret = ForwarderRegisterRDA(node->rda_id, nodes_idx, combiner->node_count,
                                nodes_idx, combiner->node_count);
        if (ret == 0)
        {
            register_success_flag = true;
            break;;
        }
        /* error type is timeout */
        else if  (ret == 1008)
        {
            retry_idx += 1;
        }
        /* other error */
        else
        {
            break;
        }
    }
    
    if (!register_success_flag)
    {
        elog(ERROR, "Fail to register RDA: %ld service: %d", node->rda_id, ret);
    }

	elog(LOG, "register RDA: %ld succeed", node->rda_id);
	
    pfree(nodes_idx);
    ForwarderRDADisconnect();
    node->finish_init = true;
    return;
}

TupleTableSlot *
ExecRemoteDataAccess(PlanState *pstate)
{
    RemoteDataAccessState       *node = castNode(RemoteDataAccessState, pstate);
    RDAResponseCombiner         *combiner = (RDAResponseCombiner *)node;
    PlanState                   *outerPlan = outerPlanState(node);
    RemoteDataAccess            *plan = (RemoteDataAccess *) combiner->ss.ps.plan;
    TupleTableSlot              *resultslot = combiner->ss.ps.ps_ResultTupleSlot;
    struct rusage               start_r;
    struct timeval              start_t;
#ifdef __OPENTENBASE__
    TimestampTz                 currentTz = 0;
#endif

	if ((node->eflags & EXEC_FLAG_EXPLAIN_ONLY) != 0)
		return NULL;

    if (!node->finish_init)
        ExecFinishInitRemoteDataAccess(node);
    node->bound = true;

    if (log_remote_data_access_stats)
        ResetUsageCommon(&start_r, &start_t);

#ifdef __OPENTENBASE__
    if (enable_statistic)
    {
        currentTz = GetCurrentTimestamp();
        if (combiner->print_stats_tz == 0 ||
                (currentTz - combiner->print_stats_tz) > (5 * PRINT_INTERVAL_SEC))
        {
            combiner->print_stats_tz = currentTz;
            print_io_stats(combiner);
        }
    }
#endif

    /* 
     * 1. send cached slot(in tuplestore) to out-frames
     */
    push_cached_slot_to_out_frames(combiner);

    /*
     * 2. get local slot until to re-distr one for node-self
     *   If merge-sort is set, get all local slots to re-distr
     */
    while (!combiner->local_fetch_finish && combiner->local_slot == NULL)
    {
        TupleTableSlot *slot = ExecProcNode(outerPlan);
        if (TupIsNull(slot))
        {
            combiner->local_fetch_finish = true;
            send_close_to_all_rda_frames(combiner);

            elog(LOG, "ExecRemoteDataAccess(%ld): local finished, "
                            "process_count:%lu, send_tuples:%lu",
                combiner->rda_id, combiner->local_processed, combiner->send_slots);
            break;
        }
        combiner->local_processed ++;

        /* here, get one slot, ready to re-distr */
        combiner->local_slot = redistrute_outer_slot(node, slot);
    }

    /*
     * 3. get one slot for up-level plan-node
     */
    if (!combiner->merge_sort)
    {
        rda_frame_entry *frame_entry;

        /* return local slot at first */
        if (combiner->local_slot)
        {
            TupleTableSlot *slot = combiner->local_slot;

            combiner->local_slot = NULL;

            if (log_remote_data_access_stats)
                ShowUsageCommon("ExecRemoteDataAccess", &start_r, &start_t);
            return slot;
        }

        /* get a remote slot */
        frame_entry = recevice_slot_from_any_frame(combiner, true, resultslot->tts_mcxt);
        if (frame_entry)
        {
            TupleTableSlot *slot = extract_slot_from_remote_frame(combiner, frame_entry);

            if (log_remote_data_access_stats)
                ShowUsageCommon("ExecRemoteDataAccess", &start_r, &start_t);
            return slot;
        }
    }
    else
    {
        /* merge-sort case */
        if (combiner->tuplesortstate == NULL)
            combiner->tuplesortstate = tuplesort_begin_merge_for_rda(
                                            resultslot->tts_tupleDescriptor,
                                            plan->sort->numCols,
                                            plan->sort->sortColIdx,
                                            plan->sort->sortOperators,
                                            plan->sort->sortCollations,
                                            plan->sort->nullsFirst,
                                            combiner,
                                            work_mem);

        /* get one sorted slot for up-level */
        if (tuplesort_gettupleslot((Tuplesortstate *) combiner->tuplesortstate,
                                    true, true, resultslot, NULL))
        {
            /* ready to return slot, try to send cached slot out again here */
            push_cached_slot_to_out_frames(combiner);

            if (log_remote_data_access_stats)
                ShowUsageCommon("ExecRemoteDataAccess", &start_r, &start_t);
            return resultslot;
        }
    }

    /* here will return NIL slot to up-level, make sure cached slot is clean */
    force_push_cached_slot_out(combiner, false);

    if (log_remote_data_access_stats)
        ShowUsageCommon("ExecRemoteDataAccess", &start_r, &start_t);

#ifdef __OPENTENBASE__
    if (enable_statistic)
    {
        elog(LOG, "ExecRemoteDataAccess(%ld): process_count:%lu, recv_tuples:%lu, send_tuples:%lu.",
            node->rda_id, combiner->local_processed, combiner->recv_tuples, combiner->send_slots);
        print_io_stats(combiner);
    }
#endif
    return NULL;
}

void
ExecEndRemoteDataAccess(RemoteDataAccessState *node)
{
    int                 i = 0;
    int                 res;
    struct rusage       start_r;
    struct timeval      start_t;
    int                 *nodes_idx;
    ListCell            *node_list_item;
    RDAResponseCombiner *combiner = (RDAResponseCombiner *)node;

    elog(LOG, "in ExecEndRemoteDataAccess(%ld).", node->rda_id);

    if (log_remote_data_access_stats)
        ResetUsageCommon(&start_r, &start_t);

    /* make sure rpc frame cleanup at the end */
    if (node->finish_init)
        rda_connections_cleanup(node);

    if (outerPlanState(node))
        ExecEndNode(outerPlanState(node));
    if (node->locator)
        freeLocator(node->locator);
    else
        pfree(node->dest_nodes);
    if (node->dnOids)
        pfree(node->dnOids);

    node->dest_nodes = NULL;
    node->locator = NULL;
    node->dnOids = NULL;

    /* EXPLAIN_ONLY case */
    if (!node->finish_init)
        return;

    close_rda_shmem_frames(node);

    nodes_idx = palloc(combiner->node_count * sizeof(int));
    foreach (node_list_item, node->execNodes)
    {
        nodes_idx[i++] = lfirst_int(node_list_item);
    }


    ConnectForwarderRDA();

    res = ForwarderUnRegisterRDA(node->rda_id, nodes_idx, combiner->node_count,
                                nodes_idx, combiner->node_count);
    if (res != 0)
    {
        elog(ERROR, "Fail to un-register RDA service: %d", res);
    }
	elog(LOG, "un-register RDA service: %ld succeed", node->rda_id);
	
    pfree(nodes_idx);
    ForwarderRDADisconnect();

    if (log_remote_data_access_stats)
        ShowUsageCommon("ExecEndRemoteDataAccess", &start_r, &start_t);
}

/*
 * rda_connections_cleanup
 * Cleanup rda-frame channel, send out all slots and discard all
 *  the remaining slots until EOF.
 */
static void
rda_connections_cleanup(RemoteDataAccessState *node)
{
    PlanState                   *outerPlan = outerPlanState(node);
    RDAResponseCombiner         *combiner = (RDAResponseCombiner *)node;

    /*
     * 1. fetch all local slots, push out with best
     */
    while(!combiner->local_fetch_finish)
    {
        TupleTableSlot *slot = ExecProcNode(outerPlan);
        if (TupIsNull(slot))
        {
            combiner->local_fetch_finish = true;
            send_close_to_all_rda_frames(combiner);
            break;
        }
        combiner->local_processed ++;

        /* discard slot for local node */
        redistrute_outer_slot(node, slot);
    }

    /*
     * 2. wait all cached slots send out, do recevice at the same time
     */
    force_push_cached_slot_out(combiner, true);

    /*
     * 4. receive and discard all the remaining remote slots
     */
    force_discard_all_slot_in_frames(combiner);
}

void
ExecReScanRemoteDataAccess(RemoteDataAccessState *node)
{
    PlanState                   *outerPlan = outerPlanState(node);
    RDAResponseCombiner         *combiner = (RDAResponseCombiner *)node;

    elog(LOG, "in ExecReScanRemoteDataAccess(%ld).", node->rda_id);

    /*
     * If we haven't queried remote nodes yet, just return. If outerplan'
     * chgParam is not NULL then it will be re-scanned by ExecProcNode,
     * else - no reason to re-scan it at all.
     */
    if (!node->finish_init)
        return;

    /*
     * 1. cleanup send and receice frame channel
     */
    if (node->bound)
        rda_connections_cleanup(node);

    /*
     * 2. send rescan-package out as all out cached slots are clean
     */
    force_send_rescan_to_all_rda_frames(combiner);

    /*
     * 3. time to wait all rescan-package arriving
     */
    wait_rescan_in_frames(combiner);

    elog(LOG, "ExecReScanRemoteDataAccess(%ld): process_count:%lu,"
                                "recv_tuples:%lu, send_tuples:%lu.",
                node->rda_id, combiner->local_processed,
                combiner->recv_tuples, combiner->send_slots);

    /*
     * 4. reset combiner state for next exec
     */
    reset_combiner_for_rescan(combiner);

    /*
     * now, rescan the under plan-tree at the end
     * if chgParam of subnode is not null then plan will be re-scanned by
     * first ExecProcNode.
     */
    if (node->bound && outerPlan->chgParam == NULL)
        ExecReScan(outerPlan);

    node->eflags &= ~(EXEC_FLAG_DISCONN);
    node->bound = false;
}

void
ExecDisconnectRemoteDataAccess(RemoteDataAccessState *node)
{
    /*
     * HashJoin case
     * InnerPlan is empty, they do not doing the left outerPlan
     * In Disconnect process:
     *  1. scan local subplan and re-distr out to remote nodes
     *  2. discard slots for me, include remote slots
     *  3. send close-package and wait to recevice all close-packages
     *  4. it's like 'rescan', but do not send rescan-package
     */

    elog(LOG, "in ExecDisconnectRemoteDataAccess(%ld).", node->rda_id);

    if (!node->finish_init)
        ExecFinishInitRemoteDataAccess(node);

    /* cleanup send and receice frame channel */
    rda_connections_cleanup(node);

    node->bound = true;
    node->eflags |= EXEC_FLAG_DISCONN;
}

void
ExecFinishRemoteDataAccess(RemoteDataAccessState *node)
{
    /*
     * quick finish the remain slots
     */

    if ((node->eflags & EXEC_FLAG_EXPLAIN_ONLY) != 0)
        return;

    elog(LOG, "in ExecFinishRemoteDataAccess(%ld).", node->rda_id);

    /* same as disconnect */
    ExecDisconnectRemoteDataAccess(node);
}
