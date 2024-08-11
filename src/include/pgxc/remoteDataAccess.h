/*-------------------------------------------------------------------------
 *
 * remoteDataAccess.h
 *
 *	  Functions to re-distrubution data to remote Datanodes
 *
 *
 * Copyright (c) 2019-2024 TeleDB Development Group
 *
 *
 * IDENTIFICATION
 *	  src/include/pgxc/remoteDataAccess.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef REMOTEDATAACCESS_H
#define REMOTEDATAACCESS_H

#include "locator.h"
#include "nodes/nodes.h"
#include "pgxcnode.h"
#include "planner.h"
#ifdef XCP
#include "squeue.h"
#include "remotecopy.h"
#endif
#include "access/tupdesc.h"
#include "executor/tuptable.h"
#include "nodes/execnodes.h"
#include "nodes/pg_list.h"
#include "tcop/dest.h"
#include "tcop/pquery.h"
#include "utils/snapshot.h"
#include "access/xact.h"
#include "pgxc/rda_frame_impl.h"
#include "pgxc/execRemote.h"

extern int rda_send_wait_time;
extern int rda_receive_wait_time;
extern int rda_wait_rescan_time;

#define RDA_SEND_WAIT_TIMES  (rda_send_wait_time  * 1000 / RDA_LOG_INTERVAL)
#define RDA_RECEIVE_WAIT_TIMES  (rda_receive_wait_time * 1000 / RDA_LOG_INTERVAL)
#define RDA_WAIT_RESCAN_TIMES  (rda_wait_rescan_time * 1000 / RDA_LOG_INTERVAL)

#define RDA_LOG_INTERVAL  2            /* try fetch tuple  RDA_LOG_INTERVAL mill second */
#define RDA_LOG_INTERVAL_TIMES  1000
#define RDA_PUSH_INTERVAL_TIMES 1      /* push cached slot during receving, 'times' of RDA_LOG_INTERVAL */

struct RDAResponseCombiner;
extern char *Authenticate_vector;

typedef struct rda_frame_entry
{
    Oid                 remote_nodeOid;     /* hash key of this entry */
    int                 remote_nodeid;      /* remote node id which this frame will send to or recevice from */
    int                 local_nodeid;       /* node id in local, this node */

    uint64              io_bytes;           /* send or receive bytes on this channel */
    FILE                *debug_out_fp;      /* buffer flying on this channel when debug enable */
    uint32_t            seq_num;            /* seqence number for this channel */

    bool                eof;                /* complete package is receviced from this remote node, means no more pack */
    bool                store_empty;        /* current state of tuplestore, empty or not */
    bool                close_pending;      /* pending to send close-package */
    bool                rescan_state;       /* recevice rescan-pakcage from remote note or send it out */
    rda_dsm_tup         tup;                /* temp cache tup header for recevicing, get a header but connext not ready */
    rda_dsm_frame       *frame;             /* shared-memory frame for one node to send or recevice buffer */
    Tuplestorestate     *tuplestore;        /* local slot cache for out-frame */
    rda_dsm_tup_datarow *recv_datarow;      /* cached datarow for recevice from remote node */
    TupleTableSlot      *tmpslot;           /* tmp use for tuplestore, send slot out */

    struct RDAResponseCombiner *combiner;
} rda_frame_entry;

/*
 * Common part for RemoteDataAccess plan state node needed to store remote datanodes tuples
 * RDAResponseCombiner must be the first field of the plan state node so we can
 * typecast
 */
typedef struct RDAResponseCombiner
{
    ScanState       ss;                 /* its first field is NodeTag */
    int             node_count;         /* total count of participating nodes */
    int             current_conn;       /* used to balance load when reading from connections */
    CombineType     combine_type;       /* see CombineType enum */
    int64           rda_id;
    int             complete_count;     /* count of received Complete messages */
    bool            local_fetch_finish; /* local tupleSlot is empty to fetch */

    char            *errorMessage;      /* error message to send back to client */
    char            *errorDetail;       /* error detail to send back to client */
    char            *errorHint;         /* error hint to send back to client */

    MemoryContext   rda_ctx;            /* separate context for rda */
    MemoryContext   tmp_ctx;            /* separate context for rda */

#ifdef __OPENTENBASE__
    /* statistic information for debug */
    uint64          send_slots;           /* count of slots which send to rda-frame */
    uint64          recv_tuples;          /* number of recv tuples */
    uint64          local_processed;      /* count of slots fetch from local */
    TimestampTz     print_stats_tz;       /* print stats timeTz */
#endif

    bool            merge_sort;           /* perform mergesort of node tuples */
    bool            local_store_empty;    /* tuplestore is emtpy for local slot in merge-sort */
    void            *tuplesortstate;      /* for merge sort */
    Tuplestorestate *tuplestorestate;     /* tmp store for local slot in merge-sort */

    /* rda shmem */
    void            *rda_shmem_ptr;       /* pointer to rda shared-memory */
    uint32_t        rda_shmem_size;       /* size of rda shared-memory */
    HTAB            *out_frames;          /* buffer queue to send out, hash by Oid */
    HTAB            *in_frames;           /* incoming tuple buffer for me from other Nodes, by Oid */
    rda_frame_entry         **in_frames_array;   /* collect all incoming rda frames in array to speedup read */

    TupleTableSlot *local_slot;
} RDAResponseCombiner;

/*
 * Execution state of a RemoteDataAccess node
 */
typedef struct RemoteDataAccessState
{
    RDAResponseCombiner combiner;

    int64               rda_id;                 /* rda plan node sequence id */
    bool		        bound;					/* subplan is sent down to the nodes */
    Locator             *locator;               /* determine destination of tuples of */
    int                 nParamRemote;           /* number of params sent from the master node */
    RemoteParam         *remoteparams;          /* parameter descriptors */

    bool                execOnAll;
    List                *execNodes;             /* where to execute subplan */

    Oid                 *dnOids;                /* for all dataNode Oids array */
    int                 numDns;                 /* number of custer master dataNodes */
    int                 *dest_nodes;            /* allocate once */

    bool                finish_init;
    int32               eflags;                 /* estate flag. */
} RemoteDataAccessState;

extern RemoteDataAccessState *ExecInitRemoteDataAccess(RemoteDataAccess *node, EState *estate, int eflags);
extern void ExecFinishInitRemoteDataAccess(RemoteDataAccessState *node);
extern TupleTableSlot *ExecRemoteDataAccess(PlanState *pstate);
extern void ExecReScanRemoteDataAccess(RemoteDataAccessState *node);
extern void ExecEndRemoteDataAccess(RemoteDataAccessState *node);
extern void ExecDisconnectRemoteDataAccess(RemoteDataAccessState *node);
extern void ExecFinishRemoteDataAccess(RemoteDataAccessState *node);

/* for merge-sort */
extern TupleTableSlot * fetch_slot_from_rda(RDAResponseCombiner *combiner, int tapenum);
#endif
