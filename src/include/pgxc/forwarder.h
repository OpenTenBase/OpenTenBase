/*-------------------------------------------------------------------------
 *
 * forwarder.h
 *
 *          Definitions for forwarder porcess.
 *
 *
 * Copyright (c) 2019-2024 TeleDB Development Group
 * 
 *
 * src/include/pgxc/forwarder.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef FORWARDER_H
#define FORWARDER_H
#include <sys/time.h>
#include "nodes/nodes.h"
#include "pgxcnode.h"
#include "poolcomm.h"
#include "storage/pmsignal.h"
#include "utils/hsearch.h"
#include "squeue.h"
#include "utils/guc.h"
#include "storage/s_lock.h"
#include "pgxc/rda_frame_impl.h"
#include "common/md5.h"
#include "port/atomics.h"

/* reserved system rda ids */
#define FWD_DDS_RDA_ID                  0X0000000000000000

#define FWD_MAX_RESERVED_RDA_ID         0X0000000000000001
typedef struct FWDLocalRDAFrameEntry
{
    bool                eof;
    int64               seq_num;    
    uint64              io_bytes;           /* send or receive bytes on this channel */
    rda_dsm_frame       *frame;             /* shared-memory frame for one node to send or recevice buffer */
    rda_dsm_tup         tup;                /* temp cache tup header for recevicing, get a header but connext not ready */
    rda_dsm_tup_datarow *recv_datarow;      /* cached datarow for recevice from remote node */
} FWDLocalRDAFrameEntry;

typedef struct FWDLocalCombiner
{
    int32                       dn_frame_num;
    int32                       cn_frame_num;
    int32                       current_index;

    FWDLocalRDAFrameEntry       *in_frame_entries;
    FWDLocalRDAFrameEntry       *out_frame_entries;

    rda_dsm_frame               **in_frames;
    rda_dsm_frame               **out_frames;
} FWDLocalCombiner;


/* data queue status */
enum rda_status
{
    RDA_INIT,
    RDA_LOCAL_REGISTERED,   /* local register succeed */
    RDA_REQUEST_RDA_REMOTE, /* send remote rda status request */
    RDA_RUNNING,            /* rda both sides running well */
    RDA_BLOCKED,            /* server has no free space */
    RDA_UNREGISTERED,
    RDA_BAD_CONNECTION,     /* connection went wrong, we need reset the logical connection */
    RDA_STATUS_BUTTY
};
    
#define RDA_STATUS_NAME_LEN  64
extern const char * g_rda_status_names[];

typedef struct 
{
    int64           rda_id;                      /* rda id */
    int32           node_id;                     /* node id */
    bool            is_in_frame;                 /* true is in */
    uint32          frame_data_size;             /* frame data size */
    int             status;                      /* status */
    int64           status_duration;             /* timestamp length of cur_status */	
	
	uint64          cur_seq;                     /* data package sequence number */
    uint64          ack_seq; 			         /* ack seq number from server */
    uint64          peer_next_send;              /* next seq number from server */

    uint64          first_buffer_seq;
    uint64          last_buffer_seq;
    int             buffer_free_size;            /* buffer free size of buffer_queue */
    
    uint32_t        peer_free_slot_num;          /* receiver free package slots */
	int             controller_index;            /* controller idx used by sender|receiver */

    
	/* bytes track */
    uint64          send_offset;                 /* byte offset to send next */
	uint64          pkgs_sent;      	         /* number of pkgs  ssent */	
	uint64			pkgs_resent;		         /* number of pkgs  resent */
	uint64			pkgs_received;		         /* number of pkgs  resent */
	uint64			pkgs_dropped;		         /* number of pkgs  dropped */

    uint64          bytes_sent;                  /* number of bytes sent */
	uint64          bytes_resent;                /* number of bytes resent */
    uint64          bytes_received;              /* number of bytes received */
    uint64          bytes_dropped;               /* number of bytes dropped */
} rda_stat_record;


/* interfaces for others to call. */
extern void TeleDBForwarderIam(void);
extern bool IsForwarderProcess(void);
extern bool recevice_slot_from_fwd_local_frame(FWDLocalRDAFrameEntry *frame_entry);

/* Initialize internal structures */
extern int  ForwarderInit(void);
extern void ForwarderCleanupRDAStore(void);

/* Global settings */
extern int g_FwdServerPort;
extern int g_FwdWorkerNum;
extern int g_FwdIOThreadNum;
extern int g_rda_sender_socket_num_per_node;

extern bool g_fwd_dump_pkg_to_file;
extern bool g_fwd_enable_fwd_md5;

extern void ConnectForwarderRDA(void);
extern void ForwarderRDADisconnect(void);
extern int  ForwarderRegisterRDA(int64 rda_id, int *dest_node_ids, int dest_num,
                                         int *src_node_ids, int src_num);
extern int  ForwarderUnRegisterRDA(int64 rda_id, int *dest_node_ids, int dest_num,
                                            int *src_node_ids, int src_num);
extern int  ForwarderReload(void);
extern int  ForwarderReloadStatus(void);
extern rda_stat_record*  ForwarderStatRDA(int *number);

extern void TeleDBForwarderIam(void);
extern rda_stat_record* ForwarderGetStatusData(int *number);

#endif
