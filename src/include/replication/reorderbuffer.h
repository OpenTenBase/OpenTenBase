/*
 * reorderbuffer.h
 *      PostgreSQL logical replay/reorder buffer management.
 *
 * Copyright (c) 2012-2017, PostgreSQL Global Development Group
 *
 * This source code file contains modifications made by THL A29 Limited ("Tencent Modifications").
 * All Tencent Modifications are Copyright (C) 2023 THL A29 Limited.
 *
 * src/include/replication/reorderbuffer.h
 */
#ifndef REORDERBUFFER_H
#define REORDERBUFFER_H

#include "access/htup_details.h"
#include "lib/ilist.h"
#include "storage/sinval.h"
#include "utils/hsearch.h"
#include "utils/relcache.h"
#include "utils/snapshot.h"
#include "utils/timestamp.h"

/* an individual tuple, stored in one chunk of memory */
typedef struct ReorderBufferTupleBuf
{
    /* position in preallocated list */
    slist_node    node;

    /* tuple header, the interesting bit for users of logical decoding */
    HeapTupleData tuple;

    /* pre-allocated size of tuple buffer, different from tuple size */
    Size        alloc_tuple_size;

    /* actual tuple data follows */
} ReorderBufferTupleBuf;

/* pointer to the data stored in a TupleBuf */
#define ReorderBufferTupleBufData(p) \
    ((HeapTupleHeader) MAXALIGN(((char *) p) + sizeof(ReorderBufferTupleBuf)))

/*
 * Types of the change passed to a 'change' callback.
 *
 * For efficiency and simplicity reasons we want to keep Snapshots, CommandIds
 * and ComboCids in the same list with the user visible INSERT/UPDATE/DELETE
 * changes. Users of the decoding facilities will never see changes with
 * *_INTERNAL_* actions.
 *
 * The INTERNAL_SPEC_INSERT and INTERNAL_SPEC_CONFIRM changes concern
 * "speculative insertions", and their confirmation respectively.  They're
 * used by INSERT .. ON CONFLICT .. UPDATE.  Users of logical decoding don't
 * have to care about these.
 */
enum ReorderBufferChangeType
{
    REORDER_BUFFER_CHANGE_INSERT,
    REORDER_BUFFER_CHANGE_UPDATE,
    REORDER_BUFFER_CHANGE_DELETE,
    REORDER_BUFFER_CHANGE_MESSAGE,
    REORDER_BUFFER_CHANGE_INTERNAL_SNAPSHOT,
    REORDER_BUFFER_CHANGE_INTERNAL_COMMAND_ID,
    REORDER_BUFFER_CHANGE_INTERNAL_TUPLECID,
    REORDER_BUFFER_CHANGE_INTERNAL_SPEC_INSERT,
    REORDER_BUFFER_CHANGE_INTERNAL_SPEC_CONFIRM
};

/*
 * a single 'change', can be an insert (with one tuple), an update (old, new),
 * or a delete (old).
 *
 * The same struct is also used internally for other purposes but that should
 * never be visible outside reorderbuffer.c.
 */
typedef struct ReorderBufferChange
{
    XLogRecPtr    lsn;

    /* The type of change. */
    enum ReorderBufferChangeType action;

    RepOriginId origin_id;

    /*
     * Context data for the change. Which part of the union is valid depends
     * on action.
     */
    union
    {
        /* Old, new tuples when action == *_INSERT|UPDATE|DELETE */
        struct
        {
            /* relation that has been changed */
            RelFileNode relnode;

            /* no previously reassembled toast chunks are necessary anymore */
            bool        clear_toast_afterwards;

            /* valid for DELETE || UPDATE */
            ReorderBufferTupleBuf *oldtuple;
            /* valid for INSERT || UPDATE */
            ReorderBufferTupleBuf *newtuple;
        }            tp;

        /* Message with arbitrary data. */
        struct
        {
            char       *prefix;
            Size        message_size;
            char       *message;
        }            msg;

        /* New snapshot, set when action == *_INTERNAL_SNAPSHOT */
        Snapshot    snapshot;

        /*
         * New command id for existing snapshot in a catalog changing tx. Set
         * when action == *_INTERNAL_COMMAND_ID.
         */
        CommandId    command_id;

        /*
         * New cid mapping for catalog changing transaction, set when action
         * == *_INTERNAL_TUPLECID.
         */
        struct
        {
            RelFileNode node;
            ItemPointerData tid;
            CommandId    cmin;
            CommandId    cmax;
            CommandId    combocid;
        }            tuplecid;
    }            data;

    /*
     * While in use this is how a change is linked into a transactions,
     * otherwise it's the preallocated list.
     */
    dlist_node    node;
} ReorderBufferChange;

typedef struct ReorderBufferTXN
{
    /*
     * The transactions transaction id, can be a toplevel or sub xid.
     */
    TransactionId xid;

    /* did the TX have catalog changes */
    bool        has_catalog_changes;

    /*
     * Do we know this is a subxact?
     */
    bool        is_known_as_subxact;

    /*
     * LSN of the first data carrying, WAL record with knowledge about this
     * xid. This is allowed to *not* be first record adorned with this xid, if
     * the previous records aren't relevant for logical decoding.
     */
    XLogRecPtr    first_lsn;

    /* ----
     * LSN of the record that lead to this xact to be committed or
     * aborted. This can be a
     * * plain commit record
     * * plain commit record, of a parent transaction
     * * prepared transaction commit
     * * plain abort record
     * * prepared transaction abort
     * * error during decoding
     * ----
     */
    XLogRecPtr    final_lsn;

    /*
     * LSN pointing to the end of the commit record + 1.
     */
    XLogRecPtr    end_lsn;

    /*
     * LSN of the last lsn at which snapshot information reside, so we can
     * restart decoding from there and fully recover this transaction from
     * WAL.
     */
    XLogRecPtr    restart_decoding_lsn;

    /* origin of the change that caused this transaction */
    RepOriginId origin_id;
    XLogRecPtr    origin_lsn;

    /*
     * Commit time, only known when we read the actual commit record.
     */
    TimestampTz commit_time;

    /*
     * Base snapshot or NULL.
     */
    Snapshot    base_snapshot;
    XLogRecPtr    base_snapshot_lsn;

    /*
     * How many ReorderBufferChange's do we have in this txn.
     *
     * Changes in subtransactions are *not* included but tracked separately.
     */
    uint64        nentries;

    /*
     * How many of the above entries are stored in memory in contrast to being
     * spilled to disk.
     */
    uint64        nentries_mem;

    /*
     * Has this transaction been spilled to disk?  It's not always possible to
     * deduce that fact by comparing nentries with nentries_mem, because e.g.
     * subtransactions of a large transaction might get serialized together
     * with the parent - if they're restored to memory they'd have
     * nentries_mem == nentries.
     */
    bool        serialized;

    /*
     * List of ReorderBufferChange structs, including new Snapshots and new
     * CommandIds
     */
    dlist_head    changes;

    /*
     * List of (relation, ctid) => (cmin, cmax) mappings for catalog tuples.
     * Those are always assigned to the toplevel transaction. (Keep track of
     * #entries to create a hash of the right size)
     */
    dlist_head    tuplecids;
    uint64        ntuplecids;

    /*
     * On-demand built hash for looking up the above values.
     */
    HTAB       *tuplecid_hash;

    /*
     * Hash containing (potentially partial) toast entries. NULL if no toast
     * tuples have been found for the current change.
     */
    HTAB       *toast_hash;

    /*
     * non-hierarchical list of subtransactions that are *not* aborted. Only
     * used in toplevel transactions.
     */
    dlist_head    subtxns;
    uint32        nsubtxns;

    /*
     * Stored cache invalidations. This is not a linked list because we get
     * all the invalidations at once.
     */
    uint32        ninvalidations;
    SharedInvalidationMessage *invalidations;

    /* ---
     * Position in one of three lists:
     * * list of subtransactions if we are *known* to be subxact
     * * list of toplevel xacts (can be an as-yet unknown subxact)
     * * list of preallocated ReorderBufferTXNs
     * ---
     */
    dlist_node    node;

} ReorderBufferTXN;

/* so we can define the callbacks used inside struct ReorderBuffer itself */
typedef struct ReorderBuffer ReorderBuffer;

/* change callback signature */
typedef void (*ReorderBufferApplyChangeCB) (
                                            ReorderBuffer *rb,
                                            ReorderBufferTXN *txn,
                                            Relation relation,
                                            ReorderBufferChange *change);

/* begin callback signature */
typedef void (*ReorderBufferBeginCB) (
                                      ReorderBuffer *rb,
                                      ReorderBufferTXN *txn);

/* commit callback signature */
typedef void (*ReorderBufferCommitCB) (
                                       ReorderBuffer *rb,
                                       ReorderBufferTXN *txn,
                                       XLogRecPtr commit_lsn);

/* message callback signature */
typedef void (*ReorderBufferMessageCB) (
                                        ReorderBuffer *rb,
                                        ReorderBufferTXN *txn,
                                        XLogRecPtr message_lsn,
                                        bool transactional,
                                        const char *prefix, Size sz,
                                        const char *message);

struct ReorderBuffer
{
    /*
     * xid => ReorderBufferTXN lookup table
     */
    HTAB       *by_txn;

    /*
     * Transactions that could be a toplevel xact, ordered by LSN of the first
     * record bearing that xid.
     */
    dlist_head    toplevel_by_lsn;

    /*
     * one-entry sized cache for by_txn. Very frequently the same txn gets
     * looked up over and over again.
     */
    TransactionId by_txn_last_xid;
    ReorderBufferTXN *by_txn_last_txn;

    /*
     * Callbacks to be called when a transactions commits.
     */
    ReorderBufferBeginCB begin;
    ReorderBufferApplyChangeCB apply_change;
    ReorderBufferCommitCB commit;
    ReorderBufferMessageCB message;

    /*
     * Pointer that will be passed untouched to the callbacks.
     */
    void       *private_data;

    /*
     * Private memory context.
     */
    MemoryContext context;

    /*
     * Memory contexts for specific types objects
     */
    MemoryContext change_context;
    MemoryContext txn_context;

    /*
     * Data structure slab cache.
     *
     * We allocate/deallocate some structures very frequently, to avoid bigger
     * overhead we cache some unused ones here.
     *
     * The maximum number of cached entries is controlled by const variables
     * on top of reorderbuffer.c
     */

    /* cached ReorderBufferTupleBufs */
    slist_head    cached_tuplebufs;
    Size        nr_cached_tuplebufs;

    XLogRecPtr    current_restart_decoding_lsn;

    /* buffer for disk<->memory conversions */
    char       *outbuf;
    Size        outbufsize;
};


ReorderBuffer *ReorderBufferAllocate(void);
void        ReorderBufferFree(ReorderBuffer *);

ReorderBufferTupleBuf *ReorderBufferGetTupleBuf(ReorderBuffer *, Size tuple_len);
void        ReorderBufferReturnTupleBuf(ReorderBuffer *, ReorderBufferTupleBuf *tuple);
ReorderBufferChange *ReorderBufferGetChange(ReorderBuffer *);
void        ReorderBufferReturnChange(ReorderBuffer *, ReorderBufferChange *);

void        ReorderBufferQueueChange(ReorderBuffer *, TransactionId, XLogRecPtr lsn, ReorderBufferChange *);
void ReorderBufferQueueMessage(ReorderBuffer *, TransactionId, Snapshot snapshot, XLogRecPtr lsn,
                          bool transactional, const char *prefix,
                          Size message_size, const char *message);
void ReorderBufferCommit(ReorderBuffer *, TransactionId,
                    XLogRecPtr commit_lsn, XLogRecPtr end_lsn,
                    TimestampTz commit_time, RepOriginId origin_id, XLogRecPtr origin_lsn);
void        ReorderBufferAssignChild(ReorderBuffer *, TransactionId, TransactionId, XLogRecPtr commit_lsn);
void ReorderBufferCommitChild(ReorderBuffer *, TransactionId, TransactionId,
                         XLogRecPtr commit_lsn, XLogRecPtr end_lsn);
void        ReorderBufferAbort(ReorderBuffer *, TransactionId, XLogRecPtr lsn);
void        ReorderBufferAbortOld(ReorderBuffer *, TransactionId xid);
void        ReorderBufferForget(ReorderBuffer *, TransactionId, XLogRecPtr lsn);

void        ReorderBufferSetBaseSnapshot(ReorderBuffer *, TransactionId, XLogRecPtr lsn, struct SnapshotData *snap);
void        ReorderBufferAddSnapshot(ReorderBuffer *, TransactionId, XLogRecPtr lsn, struct SnapshotData *snap);
void ReorderBufferAddNewCommandId(ReorderBuffer *, TransactionId, XLogRecPtr lsn,
                             CommandId cid);
void ReorderBufferAddNewTupleCids(ReorderBuffer *, TransactionId, XLogRecPtr lsn,
                             RelFileNode node, ItemPointerData pt,
                             CommandId cmin, CommandId cmax, CommandId combocid);
void ReorderBufferAddInvalidations(ReorderBuffer *, TransactionId, XLogRecPtr lsn,
                              Size nmsgs, SharedInvalidationMessage *msgs);
void ReorderBufferImmediateInvalidation(ReorderBuffer *, uint32 ninvalidations,
                                   SharedInvalidationMessage *invalidations);
void        ReorderBufferProcessXid(ReorderBuffer *, TransactionId xid, XLogRecPtr lsn);
void        ReorderBufferXidSetCatalogChanges(ReorderBuffer *, TransactionId xid, XLogRecPtr lsn);
bool        ReorderBufferXidHasCatalogChanges(ReorderBuffer *, TransactionId xid);
bool        ReorderBufferXidHasBaseSnapshot(ReorderBuffer *, TransactionId xid);

ReorderBufferTXN *ReorderBufferGetOldestTXN(ReorderBuffer *);

void        ReorderBufferSetRestartPoint(ReorderBuffer *, XLogRecPtr ptr);

void        StartupReorderBuffer(void);
void DeleteSpillToDiskSnap(TransactionId xid);

#endif
