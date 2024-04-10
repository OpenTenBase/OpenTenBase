/*-------------------------------------------------------------------------
 *
 * rda_frame_impl.h
 *
 *	  Functions to re-distrubution data to remote Datanodes
 *
 *
 * Copyright (c) 2019-2024 TeleDB Development Group
 *
 *
 * IDENTIFICATION
 *    src/include/pgxc/rda_frame_impl.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef RDA_FRAME_IMPL_H
#define RDA_FRAME_IMPL_H

/* GUC */
extern int rda_mmap_frame_size_kb;

#define PG_RDA_DIR  "rda_storage"
#define PG_RDA_MMAP_FILE_PREFIX	"mmap."
#define PG_RDA_FRAME_MAGIC      (0x5A5A5A5A)
#define PG_RDA_TUP_MAGIC        (0x5A5A)
#define MD5_CHECK_LEN 32
// #define DATAROW_MD5_CHECK 0
#define RDA_FRAME_SIZE (rda_mmap_frame_size_kb * 1024)

#define RDA_FRAME_STATUS_OK               0
#define RDA_FRAME_STATUS_BAD_CONNECTION  -1
typedef slock_t pg_spin_lock;
/*
 * Remote-Data-Access Frame in shared-memory
 *  frame->data is the beginning of raw-buffer
 */
typedef struct rda_dsm_frame
{
    uint32_t                magic;     /* magic number */
	
	pg_spin_lock   			lock;
    uint32_t       			length;    /* total size of data buffer in frame */
    volatile uint32_t       head;      /* put data at the 'head' pos, then inc 'head' */
    volatile uint32_t       tail;      /* get data from the 'tail' pos, then inc 'tail' */

    int32_t                 status;
	uint32_t                src_nodeid; /* source nodeid of frame */
    uint32_t                dest_nodeid;/* destination nodeid of frame */
	
    uint32_t                _pending_1;
    uint32_t                _pending_2; /* 36 bytes header */
    uint8_t                 data[0];    /* available buffer area to in/out */
} rda_dsm_frame;

/*
 * rda-dsm-tup is the base package of backend to recevice
 *  from rda-dsm-frame or send to.
 * hack here: tup should aglin to 'RemoteDataRowData', it could
 * reduce memory-copy
 */
typedef struct rda_dsm_tup
{
    uint16_t    type;               /* tup message type */
    uint16_t    magic;              /* tup magic number */
    uint32_t    seq_num;            /* tup tag to distinct */
    uint32_t    checksum;           /* checksum of this tup header */
#ifdef DATAROW_MD5_CHECK
    uint32_t    md5[8];             /* md5 value of datarow msg*/
#endif 
    /* RemoteDataRowData start here */
    uint32_t    msgnode;
    uint32_t    msglen;
    uint8_t     msg[0];
} rda_dsm_tup;

/* instead of RemoteDataRowData in rda-frame
 */
typedef struct rda_dsm_tup_datarow
{
    uint32_t    msgnode;
    uint32_t    msglen;
    uint8_t     msg[0];
} rda_dsm_tup_datarow;

#define RDA_TUP_HEAD_SIZE (sizeof(rda_dsm_tup))
#define RDA_TUP_TO_DATAROW(tup)  ((void *)(&tup->msgnode))
#define DATAROW_TO_RDA_TUP(datarow)     ((rda_dsm_tup *) \
                        ((uint8_t *)(datarow) - ((size_t)&(((rda_dsm_tup *)0)->msgnode))))

#define OWNER_BACKEND_FRAME_ENQUEUE           1
#define OWNER_FORWARDER_FRAME_DEQUEUE         2
#define OWNER_FORWARDER_NET_READ              3
#define OWNER_FORWARDER_FRAME_ENQUEUE         4
#define OWNER_BACKEND_FRAME_DEQUEUE           5

#define OWNER_FORWARDER 1
#define OWNER_BACKEND   0
FILE * debug_out_file_open(int64 rda_id, int nodeid, int owner);
void debug_out_file_close(FILE *fp);
bool debug_out_file_write_buffer(FILE *fp, uint32_t len, uint8_t *data);

/* rda shmem frame ops */
extern bool dms_rda_create_and_attach(int64 rda_id, uint32_t request_size,
                                void **mapped_addr, uint32_t *mapped_size);
extern bool dms_rda_attach(int64 rda_id, void **mapped_addr,
                                                    uint32_t *mapped_size);
extern void dsm_map_rda_frames(void *mapped_addr, uint32_t mapped_size,
                               int in_nodes, int out_nodes,
                               rda_dsm_frame **in_frames,
                               rda_dsm_frame **out_frames, bool init);
extern bool dsm_rda_detach(int64 rda_id, void **mapped_addr,
                                                    uint32_t *mapped_size);
extern bool dsm_rda_detach_and_unlink(int64 rda_id, void **mapped_addr,
                                                    uint32_t *mapped_size);

/* rda frame ops */
extern uint32_t rda_frame_free_space(rda_dsm_frame *frame);
extern uint32_t rda_frame_data_size(rda_dsm_frame *frame);

/* forwarder raw-buffer in/out */
extern void     rda_frame_inc_write_off(rda_dsm_frame *frame, uint32_t uiLen);
extern void     rda_frame_inc_data_off(rda_dsm_frame *frame, uint32_t uiLen);
extern uint8_t *rda_frame_get_write_off(rda_dsm_frame *frame, uint32_t *uiLen);
extern uint8_t *rda_frame_get_data_off(rda_dsm_frame *frame, uint32_t *uiLen);
extern int32_t  rda_get_frame_status(rda_dsm_frame *frame);
extern void     rda_set_frame_status(rda_dsm_frame *frame, int32_t status);
extern void     rda_reset_frame(rda_dsm_frame *frame);

/* backend tup in/out */
extern bool rda_frame_enqueue_data(FILE **fpp, uint32_t seq_num, rda_dsm_frame *frame,
                                uint16_t type, uint32_t len, uint32_t node, uint8_t *data);
extern bool rda_frame_get_tup_head(FILE **fpp, rda_dsm_frame *frame, rda_dsm_tup *tup);
extern bool rda_frame_get_tup_data(FILE **fpp, rda_dsm_frame *frame,
                                rda_dsm_tup_datarow *datarow, uint8_t *md5);

#endif