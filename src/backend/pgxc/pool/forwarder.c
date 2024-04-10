/*-------------------------------------------------------------------------
 *
 * forwarder.c
 *
 *	  Data forwarder process for distributed query.
 *
 * Copyright (c) 2019-2024 TeleDB Development Group
 * 
 * src/backend/pgxc/pool/forwarder.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include <poll.h>
#include <signal.h>
#include <sys/mman.h>
#include <sys/file.h>
#include <sys/param.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <dirent.h>
#include "common/ip.h"
#include "libpq/pqsignal.h"
#include "miscadmin.h"
#include "access/xact.h"
#include "catalog/pgxc_node.h"
#include "commands/dbcommands.h"
#include "nodes/nodes.h"
#include "pgxc/forwarder.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/memutils.h"
#include "utils/lsyscache.h"
#include "utils/resowner.h"
#include "lib/stringinfo.h"
#include "libpq/libpq-be.h"
#include "libpq/pqformat.h"
#include "libpq/libpq.h"
#include "common/username.h"
#include "catalog/pg_authid.h"
#include "pgxc/locator.h"
#include "pgxc/pgxc.h"
#include "pgxc/nodemgr.h"
#include "pgxc/poolutils.h"
#include "pgxc/poolcomm.h"
#include "../interfaces/libpq/libpq-fe.h"
#include "../interfaces/libpq/libpq-int.h"
#include "postmaster/postmaster.h"		/* For Unix_socket_directories */
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <openssl/aes.h>
#include <openssl/rand.h>
#include <openssl/evp.h>
#include "utils/varlena.h"
#include "port.h"
#include <math.h>
#include "pgxc/pgxcnode.h"
#include "pgxc/map.h"
#include "port/atomics.h"
#include "storage/fd.h"
#include <getopt.h>
#include <stdlib.h>
#include <arpa/inet.h>
#include <stdio.h>
#include <unistd.h>
#include <string.h>
#include <pthread.h>
#include <errno.h>
#include <time.h>
#include <sys/epoll.h>


#define FWD_DISABLE_MEMORY_CHECK   
#define __ENABLE_RDA_MD5__

#define FORWARDER_PREFIX "Forwarder: "
#define FORMATTED_TS_LEN                (128)                                          /* format timestamp buf length */
#define FWD_WRITE_LOG_ONCE_LIMIT        (4096)                                         /* number of logs written at a time */
#define FWD_MAX_THREAD_LOG_PIPE_LEN     (256 * FWD_WRITE_LOG_ONCE_LIMIT)               /* length of thread log pipe */
#define FWD_DEFAULT_LOG_BUF_LEN         (2048)                                         /* length of thread log length */
#define FWD_MAX_DATA_PKG_SIZE           (16 * 1024)
#define FWD_MAX_CONNECTIONS_PER_THREAD  1024
#define FWD_OP_QUEUE_SIZE               (8192)
#define FWD_CN_NODEID_BASE              10000                                          /* first cn node id */
#define STR_MD5_LEN                     33
#define FWD_NODE_ID_ALL                 -1


/* debug feature for io transfer */
bool g_fwd_dump_pkg_to_file = false;
bool g_fwd_enable_fwd_md5   = false;


/* resource size configurations */
int g_FwdWorkerNum   = 2;
int g_FwdIOThreadNum = 2;
int g_rda_sender_socket_num_per_node = 3;
pthread_t g_fwd_main_thread_id = 0;

#define      RDA_STAT_ELEMENT_SIZE (g_total_node_num * sizeof(rda_stat_record) * 2)
#define      RDA_STAT_ELEMENT_NUM  (g_total_node_num * 2)
static HTAB *g_rda_stat_hash   = NULL;
static int32 g_rda_stat_number = 0;

#ifdef  FWD_DISABLE_MEMORY_CHECK
#define fwd_malloc(size) malloc(size)
#define fwd_free(ptr)    free(ptr)

inline static void fwd_init_memory_check()
{   
    return;
}

static inline void fwd_check_thread_id()
{
   return;
}
#else
static inline void fwd_check_thread_id();
/* Check if a thread is the main thread. */
static inline void fwd_check_thread_id()
{
    if (g_fwd_main_thread_id != pthread_self())
    {
        abort();
    }
    return;
}
/* For memory check , add local memory check for forwarder */
#define MALLOC_FUN_NAME_LENGTH  64
static void *fwd_malloc_imp(int32 size, const char *func, int32 lineno);
static void fwd_free_imp(void *ptr, const char *func, int32 lineno);
#define fwd_malloc(size) fwd_malloc_imp(size, __func__, __LINE__)
#define fwd_free(ptr)    fwd_free_imp(ptr, __func__, __LINE__)
typedef struct
{
    char malloc_func_name[MALLOC_FUN_NAME_LENGTH];
    int  malloc_line_number;
    char free_func_name[MALLOC_FUN_NAME_LENGTH];
    int  free_line_number;
} FWDMemoryCheck;

static void    *g_fwd_memory_check_map = NULL;
static int      g_fwd_memory_check_map_cnt = 0;
pthread_mutex_t g_fwd_memory_check_mutex;

inline static void fwd_init_memory_check()
{
    g_fwd_memory_check_map = fwd_memory_check_map_create();
    if (NULL == g_fwd_memory_check_map)
    {
        /* g_fwd_memory_check_map malloc failed */
        abort();
    }

    pthread_mutex_init(&g_fwd_memory_check_mutex, NULL);
}

/* For forwarder memory check */
static void fwd_memory_check_malloc(void* ptr, const char* func_name, int line_number)
{
    int             ret            = 0;
    FWDMemoryCheck *memory_context = NULL;

    pthread_mutex_lock(&g_fwd_memory_check_mutex);

    memory_context = (FWDMemoryCheck *)fwd_memory_check_map_get(g_fwd_memory_check_map, ptr);
    /* check if the address is reused */
    if (memory_context != NULL)
    {
        memset((char*)memory_context, 0X00, sizeof(FWDMemoryCheck));
        strncpy(memory_context->malloc_func_name, func_name, MALLOC_FUN_NAME_LENGTH);
        memory_context->malloc_line_number = line_number;
        pthread_mutex_unlock(&g_fwd_memory_check_mutex);
    }
    else
    {
        /* the address isn't reused */
        memory_context = (FWDMemoryCheck *)malloc(sizeof(FWDMemoryCheck));
        if (NULL == memory_context)
        {
            /* memory_context malloc failed */
            abort();
        }
        memset((char*)memory_context, 0X00, sizeof(FWDMemoryCheck));
        strncpy(memory_context->malloc_func_name, func_name, MALLOC_FUN_NAME_LENGTH);
        memory_context->malloc_line_number = line_number;
        ret = fwd_memory_check_map_put(g_fwd_memory_check_map, ptr, memory_context);
        if (ret == -1)
        {
            /* insert the map failed */
            abort();
        }
        g_fwd_memory_check_map_cnt++;
        pthread_mutex_unlock(&g_fwd_memory_check_mutex);
    }
}

/* For forwarder memory check */
static void fwd_memory_check_free(void* ptr, const char* func_name, int line_number)
{
    FWDMemoryCheck *memory_context = NULL;

    pthread_mutex_lock(&g_fwd_memory_check_mutex);
    memory_context = (FWDMemoryCheck *)fwd_memory_check_map_get(g_fwd_memory_check_map, ptr);
    if (NULL == memory_context)
    {
        /* impossible */
        abort();
    }
    
    if (memory_context->free_func_name != NULL && memory_context->free_line_number != 0)
    {
        /* double free */
        abort();
    }

    strncpy(memory_context->free_func_name, func_name, MALLOC_FUN_NAME_LENGTH);
    memory_context->free_line_number = line_number;

    pthread_mutex_unlock(&g_fwd_memory_check_mutex);
}

typedef struct 
{
	uint64          magic;     
	int32           line_no;
    int32           size;
}FWDMemCheckHead;
typedef struct 
{
    int32           line_no;
    int32           size;
	uint64          magic;  
}FWDMemCheckTail;
#define FWD_MEMORY_MAGIC 0X3F3F3F3F3F3F3F3F

static void *fwd_malloc_imp(int32 size, const char *func, int32 lineno)
{
    int32 alloc_size = 0;
    void *ptr = NULL;
    void *result = NULL;
    FWDMemCheckHead *header = NULL;
    FWDMemCheckTail *tail   = NULL;
    
    alloc_size = sizeof(FWDMemCheckHead) + size + sizeof(FWDMemCheckTail);
    ptr = malloc(alloc_size);
    if (ptr)
    {
        
        header = (FWDMemCheckHead*)ptr;
        header->line_no = lineno;
        header->size    = size;
        header->magic   = FWD_MEMORY_MAGIC;

        tail = (FWDMemCheckTail*)((char*)ptr + sizeof(FWDMemCheckHead) + size);
        tail->size    = size;
        tail->line_no = lineno;
        tail->magic   = FWD_MEMORY_MAGIC;

        result = (void*)((char*)ptr + sizeof(FWDMemCheckHead));
        fwd_memory_check_malloc(result, func, lineno);
        return result;
    }
    return NULL;
}

static void fwd_free_imp(void *ptr, const char *func, int32 lineno)
{
    FWDMemCheckHead *header = NULL;
    FWDMemCheckTail *tail   = NULL;

    fwd_memory_check_free(ptr, func, lineno);
    header = (FWDMemCheckHead*)((char*)ptr - sizeof(FWDMemCheckHead));
    if (header->magic != FWD_MEMORY_MAGIC)
    {
        abort();
    }

    tail   = (FWDMemCheckTail*)((char*)ptr + header->size);
    if (tail->magic != FWD_MEMORY_MAGIC || tail->size != header->size || tail->line_no != header->line_no)
    {
        abort();
    }
    free((void*)header);
}

#endif


/* RDA buffer queue for rpc package buffering. */
#define RDA_BUFFER_SND_QUEUE_LEN 16
#define RDA_BUFFER_RCV_QUEUE_LEN 128
#define RDA_FIRST_PKG_SEQ        1
#define RDA_INVALID_PKG_SEQ      0

typedef slock_t pg_spin_lock;


/* FWDOpQueue element for RDA queue. */
#define FWD_MAJOR_VERSION 0
#define FWD_MINOR_VERSION 0
#pragma pack(1)
typedef struct 
{
    /* user data start from below. */
    int8                  protocol;
    int8                  major_version;/* major version code, FWD_MAJOR_VERSION */
    int8                  minor_version;/* minor version code, FWD_MINOR_VERSION */                     
    uint32				  pkg_len;	    /* total package length, including sizeof(FWDPackage) + data length*/
    uint32				  pkg_left_len; /* package left len, used in sender and receiver to ensure data sending complete. */
    int64                 rda_id;       /* rda identiifer */
	uint64				  pkg_offset;   /* package offset */
	
	uint64                pkg_seq;      /* sequence number of the pkg */

    int32                 node_id;
    int32                 controller_idx;
    uint32                pending_1;
    uint32                pending_2;    	
	struct timeval        timestamp;/* last sent timestamp */	
#ifdef __ENABLE_RDA_MD5__
    char                  md5[STR_MD5_LEN];
#endif
    uint8_t               data[0];
}FWDPackage;
#pragma pack()
#define PkglenOffset (offsetof(FWDPackage,pkg_len))
#define MinSendNetLen (offsetof(FWDPackage,data)) /* mini size need hton transfer */
#define PayloadLen(send_pkg)    (send_pkg->pkg_len - sizeof(FWDPackage))
#define FWD_MAX_PKG_SIZE (FWD_MAX_DATA_PKG_SIZE + sizeof(FWDPackage))
typedef struct 
{
	FWDPackage          *package;     /* package data. */
	
	uint64               pkg_seq;     /* sequence number of the data package, used for deleted or null package */
}FWDQueueElement;


typedef struct 
{
	FWDQueueElement      *q_list; 	

	int32                q_length; 
	pg_spin_lock         q_lock;  
	volatile int32       q_head;   
	volatile int32       q_tail;  
    FILE 			    *debug_fp; /* for debug purpose */
}FWDQueue;

/* FwdOperation */
/* operation */
enum op_type
{
    FWD_OP_CREATE = 0,
    FWD_OP_DROP   = 1,
    FWD_OP_RESET  = 2,
    FWD_OP_Butty
};

static const char *const op_type_names[] = {"CREATE", "DROP", "RESET"};

typedef struct
{
    enum op_type type;
    int64   rda_id;
    int     node_id;
    void   *data;
} FwdOperation;
typedef struct FWDOpQueue
{
    
    int head;
    int tail;
    int size;
    pthread_mutex_t lock;
    uint64 seq;            /* latest sequence number */
    FwdOperation data[0];  /* FWD_OP_QUEUE_SIZE */
} FWDOpQueue;

const char * g_rda_status_names[] = {"RDA INIT", "RDA LOCAL REGISTERED", "RDA REQUEST REMOTE STATUS", "RDA RUNNING", "RDA BLOCKED", "RDA UNREGISTERED", "RDA BAD CONNECTION"};

typedef struct 
{
    enum   rda_status cur_status;
    struct timeval    timestamp; /* timestamp when cur_status set */	
}RDAStatusMgr;

typedef struct RDAController RDAController;
#define INVALID_RDA_CONTROLLER_ID  -1

/* data queue status */
enum rda_package_type
{
    RDA_PUSH_DATA           = 0,
    RDA_PUSH_DATA_ACK       = 1,
    RDA_INIT_STATUS         = 2,
    RDA_INIT_STATUS_ACK     = 3,
    RDA_SERVER_STATUS       = 4,
    RDA_SERVER_STATUS_ACK   = 5,
    RDA_PACKAGE_BUTTY
};
static const char *const g_rda_package_names[] = {"RDA_PUSH_DATA", "RDA_PUSH_DATA_ACK", "RDA_INIT_STATUS", "RDA_INIT_STATUS_ACK", "RDA_SERVER_STATUS", "RDA_SERVER_STATUS_ACK", "RDA_PACKAGE_BUTTY"};

typedef struct PushDataResponse
{
    uint32      free_space;
    uint64      ack_seq;
    uint64      next_send;
    uint32      pending_1;
    uint32      pending_2;
} PushDataResponse;

typedef struct RDAStatusResponse
{
    uint32      pending_1;
    uint32      pending_2;
} RDAStatusResponse;

#pragma pack(1)
typedef struct 
{
   FWDPackage header;
   union
   {
       PushDataResponse  datarsp;
       RDAStatusResponse rdastatrsp;
       PushDataResponse  rcvstatusrsp;
   }payload;
}ResponsePackage;
#pragma pack()
#define     FWD_PORT_SND_BUF_SIZE sizeof(ResponsePackage)

typedef struct data_queue
{
    int64           rda_id;             /* rda id */
    int             node_id;            /* node id */
	RDAStatusMgr    status;             /* for sender to confirm status of receiver. */
    rda_dsm_frame   *frame;             /* mmap */

	/* bytes track */
    uint64          ack_bytes;          /* number of date bytes sent */
    uint64          send_offset;        /* byte offset to send next */

	/* recevier free slots number */
    bool            resend_package_done;/* the resend package has been sent completely */
	uint64          ack_seq; 			/* ack seq number from server */
    uint64          next_send;
    uint32_t        peer_free_slot_num;      /* receiver free package slots */
	
    int             controller_idx;     /* controller idx used by sender */
    RDAController   *controller;        /* controller used by receiver */

	/* spinlock to protect buffer_queue, */ 
	FWDQueue        *buffer_queue;      /* data send buffer queuer for sending or receiving */

	uint64_t        seq;                /* data package sequence number */
    
    struct timeval  stat_timestamp;     /* last stat timestamp */	
    
	/* for statics purpose */
    uint64          bytes_sent;         /* number of bytes sent */
	uint64          bytes_resent;       /* number of bytes resent */
    uint64          bytes_received;     /* number of bytes received */
    uint64          bytes_dropped;      /* number of bytes dropped */

	uint64          pkgs_sent;      	/* number of pkgs sent */	
	uint64			pkgs_resent;		/* number of pkgs resent */
	uint64			pkgs_received;		/* number of pkgs resent */
	uint64			pkgs_dropped;		/* number of pkgs dropped */

	FILE 			*debug_fp;			/* for debug purpose */
} data_queue;

/* mmap info */
typedef struct mmap_info
{
    int64           rda_id;
    void            *mmap_addr;
    uint32_t        mmap_size;
    int             ref_cnt;
    int             src_node_num;
    int             dest_node_num;
    rda_dsm_frame **receive_frame;
    rda_dsm_frame **send_frame;
    data_queue     *receive_data_queue;
    data_queue     *send_data_queue;
} mmap_info;


/* network_ret_code */
#define    network_ret_ok       0
#define    network_bad_socket   -1
#define    network_decode_error -2

#define    network_need_retry    1

/* data struct of forwarder server connections */
/* The socket(s) we're listening to. */
#define     FWD_MAXLISTEN           64
static int	FWDListenSocket[FWD_MAXLISTEN];
#define     FWD_SND_BUFFER 32768
#define     FWD_RCV_BUFFER 32768

enum IOThreadStatus
{
    IOThreadStatus_Init           = 0,
    IOThreadStatus_Running        = 1,
    IOThreadStatus_Done           = 2,
    IOThreadStatus_Listen_Error   = 3,
    IOThreadStatus_Epoll_Error    = 4,
    IOThreadStatus_Conn_Error     = 5,
    IOThreadStatus_Butty
};
typedef struct FWDIOThreadCtl
{
    /* fields for epoll. */
	int					thr_efd;
	bool			    thr_epoll_ok;

    /* io thread status, as values of IOThreadStatus*/
    int                 thread_status;     

    /* exit signal */
    bool                need_exit;
} FWDIOThreadCtl;

typedef struct FWDListenThreadCtl
{
    /* io thread status, as values of IOThreadStatus*/
    int                 thread_status;             

    /* exit signal */
    bool                need_exit;
} FWDListenThreadCtl;

typedef struct FWDServerThreadCtl
{
    /* fields for threads load balance. */
	int					next_thread;

    /* listen thread controller */
    FWDListenThreadCtl  listen_thread;
    
    /* io thread controllers */
	FWDIOThreadCtl      *io_threads;
} FWDServerThreadCtl;

FWDServerThreadCtl g_fwdServerThreadCTL;


typedef struct FWDServersPort
{
	int			 sock;				/* File descriptor */
	SockAddr	 laddr;				/* local addr (postmaster) */
	SockAddr	 raddr;				/* remote addr (client) */

    /* recieve header */
    int32        header_rcv_len;
    char         header[MinSendNetLen];
    int32        package_rcv_len;
    char         *package; /* alloc when needed, and will be put into queue of rda controller. size should be sizeof (FWDPackage) + package data len*/

    /* send package buffer */
    int32        resp_buf_pkg_len; /* how many bytes resp_buf contains */
    int32        resp_buf_send_len;/* how many bytes has been sent */
    char         resp_buf[FWD_PORT_SND_BUF_SIZE];

	char		*remote_host;		/* name (or ip addr) of remote host */
	char		*remote_port;		/* text rep of remote port */
} FWDServersPort;

typedef struct FWDConnectionInfo
{
	/* Port contains all the vital information about this connection */
	FWDServersPort *con_port;

} FWDConnectionInfo;
typedef struct 
{
    int   status;
} ServerSocketData;

#define MAX_SOCKET_NUMBER 65536
typedef struct
{    
    ServerSocketData  socketmgr[MAX_SOCKET_NUMBER];/* each socket has an element, NULL tell us the socket has been closed. */
}ServerSocketMgr;

static ServerSocketMgr *g_server_sockets_data = NULL;

typedef struct 
{
    Oid             peernode; /* oid of peer node */
    int             socket;

    uint32          datalen;  /* total data length socket */
    uint32          sendlen;  /* data sent length out to socket */
    char            sendpkg[FWD_MAX_PKG_SIZE]; /* buffer last pkg here, at most FWD_MAX_PKG_SIZE*/

    uint32          readlen;  /* data read length from socket */
    char            respkg[FWD_PORT_SND_BUF_SIZE];
}ConnMgr;

FWDLocalCombiner *g_fwd_local_rda_combiner_array = NULL;
bool             g_fwd_local_rda_combiner_complete = false;

static void fwd_register_local_rda(void);
static void fwd_unregister_local_rda(void);
static bool fwd_local_rda_send(int64 rda_id, int32 node_id, uint8_t *data, int len);
static rda_dsm_tup_datarow* fwd_local_rda_receive(int64 rda_id);

/* number of data nodes, which is also row num of g_client_socket_array */
static    int       g_fwd_cn_num = 0;
static    int       g_fwd_dn_num = 0;
static    ConnMgr **g_client_socket_array = NULL;
static    int       g_total_node_num = 0;

/* used to indicate whether the overload period is currently in progress */
bool g_fwd_is_during_socket_reload = false;

/* size config of queues */

int g_FwdServerPort = 6680;
int fwd_sleep_inteval = 2;          /* thread sleep time in millisecond. */

static volatile sig_atomic_t got_SIGHUP = false;
static volatile sig_atomic_t shutdown_requested = false;

/* The root memory context */
static MemoryContext ForwarderMemoryContext = NULL;
PGPipe  *g_ForwarderLogQueue = NULL;
/* Flag to tell if we are TeleDBX forwarder process */
static bool am_forwarder = false;
/* rda status timeout array, in useconds */
#ifdef FWD_DISABLE_MEMORY_CHECK
static int64 rda_status_timeouts[RDA_STATUS_BUTTY] = {
                                                      0     /* RDA_INIT*/, 
                                                      0     /* RDA_LOCAL_REGISTERED*/, 
                                                      30000 /* RDA_REQUEST_RDA_REMOTE, can be big, not a big deal. */, 
                                                      1000  /* RDA_RUNNING*/, 
                                                      1000  /* RDA_BLOCKED*/, 
                                                      0     /*RDA_UNREGISTERED*/
                                                      };
#else
static int64 rda_status_timeouts[RDA_STATUS_BUTTY] = {
                                                      0 /* RDA_INIT*/, 
                                                      0 /* RDA_LOCAL_REGISTERED*/, 
                                                      5000000 /* RDA_REQUEST_RDA_REMOTE, can be big, not a big deal. */, 
                                                      5000000  /* RDA_RUNNING*/, 
                                                      5000000  /* RDA_BLOCKED*/, 
                                                      0 /*RDA_UNREGISTERED*/
                                                      };
#endif

/* Signal handlers */
static void forwarder_die(SIGNAL_ARGS);
static void forwarder_sig_hup_handler(SIGNAL_ARGS);
static void forwarder_quickdie(SIGNAL_ARGS);
static void ForwarderLoop(void);

static void forwarder_subthread_write_log(int elevel, int lineno, const char *filename, const char *funcname, const char *fmt, ...)__attribute__((format(printf, 5, 6)));

/* Sender thread resource */
static void       **g_fwd_data_chan_send_map      = NULL;
static FWDOpQueue **g_fwd_data_chan_send_op_queue = NULL;

/* Receiver thread resource */
static void       **g_fwd_data_chan_receive_map      = NULL;
static FWDOpQueue **g_fwd_data_chan_receive_op_queue = NULL;

/* Pipes for stat info */
#define             RDA_STAT_PIPE_LENGTH             2048
#define             RDA_STAT_INTERVAL                1000000
#define             RDA_STAT_LOOP_CNT                256


/* RDA_SLOTS_PER_GROUP defines the max number of RDA in the system. */
#define    RDA_SLOTS_PER_GROUP     (4999)
#define    RDA_MAX_DN_CONTROLLER_ID (TELEDBX_MAX_DATANODE_NUMBER * RDA_SLOTS_PER_GROUP)
#define    CN_RDA_CONTROLLER_ID_BASE RDA_MAX_DN_CONTROLLER_ID

typedef struct NodeRDASlots
{
    int              size;                   /* number of all rda controllers */
    pg_spin_lock     used_cnt_lock; 
    int              used_cnt;               /* number of occupied rda controllers */
    RDAController    *rda_slots;             /* an array of rda controllers */
} NodeRDASlots;

static PGPipe       **g_rda_stat_pipes             = NULL; 

static NodeRDASlots **g_fwd_server_rda_slots_group = NULL;
static bool           g_fwd_dn_rcv_rda_slots_group_ready = false;

/* cleanner thread resource */
static void       *g_fwd_cleanner_map;
static FWDOpQueue *g_fwd_cleanner_op_queue; /* queue for rda resource cleaning used in cleaning thread */

/* Forwarder control over the bunches of threads. */
typedef struct ServiceThreadControl ServiceThreadControl;
typedef struct DataFwdControl DataFwdControl;

static DataFwdControl *g_fwd_data_control;

/* src node id, passed by backend process */
static int my_node_id = -1;

/* FWDOpQueue functions */
static FWDOpQueue *fwd_new_op_queue(int size);
static void        fwd_delete_queue(FWDOpQueue *q);
static bool        fwd_pop_queue(FWDOpQueue *q, FwdOperation *result);
static int         fwd_push_queue(FWDOpQueue *queue, int type, int64 rda_id, int node_id, void *data);
static FWDPackage *fwd_make_package(int8 type, int32 nodeid, 
                                int32 ctrl_index, int64 rda_id, 
                                uint64 pkg_offset, uint32 pkg_len, 
                                void *data, uint64 seq);
static void drop_package_real(FWDPackage *pkg, char *file, int lineno);
#define  fwd_drop_package(pkg) drop_package_real(pkg, __FILE__, __LINE__)

inline void touch_package(FWDPackage *pkg);

/* New routing: FWDOpQueue Group functions */
static NodeRDASlots *fwd_new_rda_node_group(int groupid);
static void fwd_delete_rda_node_group(NodeRDASlots *q_group);
static int  fwd_init_rda_queue_groups(void);
static void fwd_delete_rda_node_groups(void);

static RDAController *fwd_alloc_rda_controller(int64 rda_id, int node_id);
static void fwd_free_rda_controller(int controller_id);

static RDAController *get_rda_controller_by_rda_id(int64 rda_id, int node);
static RDAController *get_rda_controller_by_id(int controller_id);
static inline int fwd_index_to_node_id(int index);
static inline int fwd_node_to_index(int node_id);


static bool copy_pkg_to_frame(FWDPackage *pkg, rda_dsm_frame *frame);

/* Resource management functions */
static int fwd_init_thread_resource(void);
static void free_thread_resource(void);

/* sockets manage funcitons */
static ConnMgr* fwd_init_sender_socket(NodeDefinition *node_def);
static int  fwd_init_sender_socket_array(void);
static void fwd_maintain_socket_array(void);
static void fwd_cleanup_socket_array(void);
static void fwd_socket_reload_impl(void);
static void fwd_init_rda_stat(void);
static void fwd_remove_rda_from_stat_hash(int64 rda_id);
static void fwd_add_rda_to_stat_hash(int64 rda_id);
static void fwd_add_rda_stat_to_hash(void);
static void fwd_stat_rda_queue(data_queue *d_queue, ServiceThreadControl *thread_ctl);



/* thread control */
enum rda_comm_type
{
    FWD_RDA_SENDER,
    FWD_RDA_RECEIVER,
    FWD_RDA_RECEIVER_Butty
};

static const char *const g_rda_comm_type_names[] = {"sender", "receiver", "none"};

static FWDQueue* fwd_create_queue(uint32 size);
static void      fwd_destory_queue(FWDQueue *queue);
static void      fwd_reset_queue(FWDQueue *queue);
static int       fwd_queue_push_pkg(FWDQueue *queue, FWDPackage *pkg, enum rda_comm_type);
static void      fwd_queue_drop_pkg(FWDQueue *queue, uint64 seq);
static FWDPackage* fwd_queue_remove_pkg(FWDQueue *queue, uint64 seq);
static bool      fwd_queue_is_full(FWDQueue *queue);
static bool      fwd_queue_is_empty(FWDQueue *queue);
static int 	     fwd_queue_data_length(FWDQueue *queue);
static FWDPackage *fwd_queue_get_package(FWDQueue *queue, uint64 seq);
static FWDPackage *fwd_queue_sniffer(FWDQueue *queue);
static int   fwd_queue_data_length(FWDQueue *queue);
static int   fwd_queue_free_length(FWDQueue *queue);
static void  fwd_queue_get_last_seq(FWDQueue *queue, uint64 *seq);
static void  fwd_queue_get_first_seq(FWDQueue *queue, uint64 *seq);


/* server process functions */
static int  fwd_accept_connection(int server_fd, FWDServersPort *port);
static void fwd_conn_free(FWDServersPort *conn);
static FWDServersPort *fwd_create_conn(int serverFd);
static FWDPackage *fwd_server_receive_a_package(FWDServersPort *port, bool *bad_port);
static int fwd_server_send_response(FWDServersPort *port, ResponsePackage *pkg, bool *ignored);
static int fwd_server_port_process(FWDServersPort *port);
static void fwd_set_nonblock_connection(int socket);

static int fwd_init_masks(fd_set *rmask);
static int fwd_init_server_threads(void);
static int fwd_stop_server_threads(void);
static void fwd_maintain_server_threads(void);
static void *fwd_listen_thread_main(void *arg);
static void *fwd_io_thread_main(void *argp);

/* client functions */
static int fwd_make_connect(ConnMgr *mgr);
static int fwd_retry_last_package(ConnMgr *curr_socket, data_queue *d_queue, ServiceThreadControl *control);
static inline void fwd_dump_pkg_to_file(FWDPackage *pkg, FILE *file, int saved_pkg_left_len);
static inline void fwd_check_pkg_sequence(FWDPackage *pkg, FILE *file);

static ResponsePackage* fwd_decode_response_pkg(char *network_header, int32 len);
static int fwd_encode_response_pkg(char *network_header, int32 len, ResponsePackage *rsp);
static int fwd_decode_pkg_header(char *network_header, int32 len, FWDPackage *request);
static void fwd_encode_pkg_header(char *network_header, int32 len, FWDPackage *request);
static int fwd_client_send_package(ConnMgr *mgr, FWDPackage *request);
static int fwd_client_send_pkg_header(ConnMgr *mgr, FWDPackage *request);
static int fwd_client_handle_responses(ConnMgr *mgr, ServiceThreadControl *control, bool *changed);
static int fwd_request_server_status(ConnMgr *mgr, int64 rda_id, char protocol, int32 controller_index, bool *force_drop) ;

/* Unit test function */
void RDAQueueTest(void);
void RDAControllerTest(void);
void fwd_local_rda_send_test(void);
void fwd_local_rda_receive_test(void);

/* thread control */
enum thread_type
{
    FWD_THREAD_SENDER,
    FWD_THREAD_RECEIVER,
    FWD_THREAD_CLEANER,
    FWD_THREAD_Butty
};

static const char *const g_fwd_thread_type_names[] = {"fwd_sender_thread", "fwd_receiver_thread", "fwd_cleaner_thread"};

struct ServiceThreadControl
{
    /* Resuorce of this thread. */
    void       *data_queue_map;        /* A map of channel to read or write. */
    FWDOpQueue *data_chan_op_queue;    /* 
                                            For sender and receiver thread, main thread dispatch rda data queue create into the queue for data sending and receiving.
                                            For cleaner thread, sender and receiver dispatch rda dataqueue drop message into the queue for rda cleaning.s
                                        */
                                  
    bool thread_need_quit;        /* quit flag */
    bool thread_need_drain;       /* drain flag */
    bool error;
    bool quit_status;             /* succeessful quit or not */

    bool thread_running;          /* running flag */
    bool thread_draining;         /* draining flag */
    ThreadSema quit_sem;          /* used to wait for thread quit */
    ThreadSema drain_sem;         /* used to wait for thread drain */

    PGPipe     *rda_dump_pipe;    /* rda dump pipe line, used when we need dump */
    enum thread_type type;        /* sender, receiver, cleanner */
    int id;                       /* thread id, for logging only. */
    FILE *log_fd;                 /* log file for debugging */
};

enum fwd_status
{
    FWD_INIT,
    FWD_RUNNING,
    FWD_DRAINING,
    FWD_QUIT,
};

static const char *const fwd_status_names[] = {"initiated", "running", "draining", "quit"};

struct DataFwdControl
{
    int32 sender_num;                          /* number of sender threads */
    ServiceThreadControl *sender_control;      /* control of sender threads */

    int32 receiver_num;                        /* number of receiver threads */
    ServiceThreadControl *receiver_control;    /* control of receiver threads */

    ServiceThreadControl *cleanner_control;    /* control of cleanner thread */

    enum fwd_status status;                    /* forwarder status */
};

/* forwarder unix sock channel */
typedef struct
{
    ForwarderPort port;
} ForwarderHandle;

typedef struct
{
    ForwarderPort   port;
    int             pid;
    char            *database;
    char            *user_name;

    int             agentindex;
} ForwarderAgent;

#define INIT_VERSION    0
#define FORWARDER_POLL_TIMEOUT_VAL  1      /* timeout ms for poll unix-sock */
typedef struct
{
    uint32  m_nobject;
    uint64  *m_fsm;
    uint32  m_fsmlen;
    uint32  m_version;
}BitmapMgr;

enum RDACtlStatus
{
    RDACtlStatus_Idle = 0,
    RDACtlStatus_Init,
    RDACtlStatus_Running,
    RDACtlStatus_Exiting,
    RDACtlStatus_butty
};


typedef struct RDAController
{
    int64   rda_id;                  /* rda id */
    int     controller_id;           /* global unique, groupid * RDA_SLOTS_PER_GROUP + index_of_bucket */
	int     group_id;
    int     node_id;

    int     socket;                  /* socket that transfering the rda data, -1 means no socket or bad socket*/
    int     ctl_status;              /* RDACtlStatus values */
	data_queue      *data_queue;
} RDAController;


static int              agentCount             = 0;
static int              forwarderAgentSize     = 0;
static BitmapMgr        *forwarderAgentMgr     = NULL;
static uint32           mgrVersion             = INIT_VERSION;
static int32            *agentIndexes          = NULL;
static int              usedAgentSize          = 0;

static int              g_fwd_unix_server_fd              = -1;
static ForwarderAgent   **forwarderAgents      = NULL;
static ForwarderHandle  *forwarderHandle       = NULL;

static void
init_data_thread_control(
                                ServiceThreadControl *control,
                                void *data_queue_map,
                                FWDOpQueue *data_chan_op_queue,
                                PGPipe     *rda_stat_pipe,
                                enum thread_type type,
                            int id);
static DataFwdControl *init_data_fwd_control(void);
static void            free_data_fwd_control(DataFwdControl *control);

static void  thread_sigmask(void);
static int32 create_thread(void *(*f)(void *), void *arg, int32 mode);
static bool  fwd_create_service_thread(void);
static void  fwd_clean_data_thread_main();
static void *fwd_data_sender_thread_main(void *arg);
static void *fwd_data_receiver_thread_main(void *arg);
static void *fwd_data_cleaner_thread_main(void *arg);
static bool  fwd_cleaner_handle_rda_change(ServiceThreadControl *control, bool *done);
static bool  fwd_data_thread_op_handler(ServiceThreadControl *control, bool *done);
static bool  fwd_send_data_loop(ServiceThreadControl *control);
static bool  fwd_send_pkgs(ServiceThreadControl *control, data_queue *d_queue, ConnMgr *curr_socket, bool *force_drop);
static bool  fwd_resend_timeout_pkg(ServiceThreadControl *control, data_queue *d_queue, ConnMgr *curr_socket, bool *force_drop);
static bool  fwd_handle_sender_response(ServiceThreadControl *control);
static bool  fwd_receive_data_loop(ServiceThreadControl *control);
static bool  fwd_data_thread_drain_loop(ServiceThreadControl *control);
static bool  fwd_cleaner_drain_data_loop(ServiceThreadControl *control);
static void  fwd_clean_data_thread_main(void);


/* clean up function. */
static void fwd_clean_fwd_resource(void);

/* request function */
static mmap_info *
create_mmap_info(int64 rda_id, void *mmap_addr, uint32_t mmap_size, int src_node_num, int dest_node_num,
                 rda_dsm_frame **receive_frame, rda_dsm_frame **send_frame,
                 data_queue *receive_data_queue, data_queue *send_data_queue);
static void delete_mmap_info(mmap_info *m_info);

/* register and unregister service implementations */
static int fwd_register_rda_impl(int64 rda_id, int *src_node_ids, int src_node_num,
                                 int *dest_node_ids, int dest_node_num);
static int fwd_unregister_rda_impl(int64 rda_id, int *src_node_ids, int src_node_num,
                                   int *dest_node_ids, int dest_node_num);
static bool get_reload_status_impl(void);


/* socket mgr infomation functions */
static int  fwd_server_socket_status_init(void);
static int  fwd_server_socket_get_status(int socket);
static void fwd_server_socket_set_status(int socket, int status);
static void fwd_server_socket_close(int socket);
static void fwd_server_socket_status_destory(void);


/* server and response callback defined here. */
typedef ResponsePackage* (*rda_server_call_back_ptr)(FWDPackage *);
typedef int (*rda_response_call_back_ptr)(ServiceThreadControl *, ResponsePackage *);

static ResponsePackage *rda_server_pushdata_callback(FWDPackage *request);
static ResponsePackage *rda_server_init_status_callback(FWDPackage *request);
static ResponsePackage *rda_server_receiver_status_callback(FWDPackage *request);

static int rda_client_pushdata_rsp_callback(ServiceThreadControl *control, ResponsePackage *rsp);
static int rda_client_rda_status_rsp_callback(ServiceThreadControl *control, ResponsePackage *rsp);
static int rda_client_server_status_rsp_callback(ServiceThreadControl *control, ResponsePackage *rsp);

rda_server_call_back_ptr   g_fwd_server_callback_array[RDA_PACKAGE_BUTTY]  = {rda_server_pushdata_callback, NULL, rda_server_init_status_callback, NULL, rda_server_receiver_status_callback, NULL};
rda_response_call_back_ptr g_fwd_client_rsp_callback_array[RDA_PACKAGE_BUTTY] = {NULL, rda_client_pushdata_rsp_callback, NULL, rda_client_rda_status_rsp_callback, NULL, rda_client_server_status_rsp_callback};


/* Use this macro when a sub thread needs to print logs */
#define forwarder_thread_logger(elevel, ...) \
    do{ \
		if (LOG == elevel)\
		{\
			forwarder_subthread_write_log(elevel, __LINE__, __FILE__, PG_FUNCNAME_MACRO, __VA_ARGS__); \
		}\
    } while(0)

/* hash function */
static int route_to_node(int node_id);
inline static void set_rda_status(RDAStatusMgr *mgr, enum rda_status status)
{
    mgr->cur_status = status;
    gettimeofday(&mgr->timestamp, NULL);
}

inline static void set_pkg_version(FWDPackage *request)
{
    request->major_version = FWD_MAJOR_VERSION;
    request->minor_version = FWD_MINOR_VERSION;
}

inline static bool check_pkg_version(FWDPackage *request)
{
    return request->major_version == FWD_MAJOR_VERSION && request->minor_version == FWD_MINOR_VERSION;
}

inline static enum rda_status get_rda_status(RDAStatusMgr *mgr)
{
    return mgr->cur_status;
}

inline static int64 timeval_useconds_diff(struct timeval  *difftime)
{
    struct timeval    current_time;
    gettimeofday(&current_time, NULL);
    return (int64)(current_time.tv_sec * 1000000 + current_time.tv_usec - difftime->tv_sec * 1000000 - difftime->tv_usec);
}

inline static bool rda_status_timeout(RDAStatusMgr *mgr)
{
    int64  timediff = 0;
    struct timeval    current_time;
    gettimeofday(&current_time, NULL);

    timediff = (int64)(current_time.tv_sec * 1000000 + current_time.tv_usec  - mgr->timestamp.tv_sec * 1000000 - mgr->timestamp.tv_usec);
        
    return timediff >= rda_status_timeouts[mgr->cur_status];
}

inline static void rda_record_status_duration(RDAStatusMgr *mgr, int64 rda_id)
{
    int64  timediff = 0;
    struct timeval    current_time;
    gettimeofday(&current_time, NULL);

    timediff = (current_time.tv_sec * 1000000 + current_time.tv_usec - mgr->timestamp.tv_sec * 1000000 - mgr->timestamp.tv_usec);
    if (timediff >= rda_status_timeouts[mgr->cur_status] && g_fwd_dump_pkg_to_file)
    {
        forwarder_thread_logger(LOG, FORWARDER_PREFIX "sender thread %ld rda_id %lu status[%s] timeout, timediff %ld us",
                                pthread_self(), rda_id, g_rda_status_names[mgr->cur_status], timediff);

        
    }
}

inline static void rda_record_server_package_duration(FWDPackage *pkg)
{
    int64  timediff = 0;
    struct timeval    current_time;
    gettimeofday(&current_time, NULL);

    timediff = (current_time.tv_sec * 1000 + current_time.tv_usec / 1000 - pkg->timestamp.tv_sec * 1000 - pkg->timestamp.tv_usec / 1000);
    if (timediff >= 0 && g_fwd_dump_pkg_to_file)
    {
        forwarder_thread_logger(LOG, FORWARDER_PREFIX "receiver thread %ld rda_id %lu pkg_seq %ld src_node_id %d timeout, timediff %ld us",
                                pthread_self(), pkg->rda_id, pkg->pkg_seq, pkg->node_id, timediff);


    }
}

inline static void rda_status_touch(RDAStatusMgr *mgr)
{
     gettimeofday(&mgr->timestamp, NULL);
}


/* Can only be called in main thread. */
static int fwd_server_socket_status_init(void)
{
    g_server_sockets_data = (ServerSocketMgr*)fwd_malloc(sizeof(ServerSocketMgr));
    if (NULL == g_server_sockets_data)
    {
        fwd_check_thread_id();
        elog(LOG, FORWARDER_PREFIX" init server socket data mgr failed");
        return -1;
    }

    memset(g_server_sockets_data, 0X00, sizeof(ServerSocketMgr));
    return 0;
}

static int fwd_server_socket_get_status(int socket)
{
    if (socket < MAX_SOCKET_NUMBER && socket >= 0)
    {
        return g_server_sockets_data->socketmgr[socket].status;
    }
    else
    {
        return network_bad_socket;
    }
}   

static void fwd_server_socket_set_status(int socket, int status)
{
    if (socket < MAX_SOCKET_NUMBER && socket >= 0)
    {
        g_server_sockets_data->socketmgr[socket].status = status;
    }
}   

static void fwd_server_socket_close(int socket)
{
    if (socket < MAX_SOCKET_NUMBER && socket >= 0)
    {
        g_server_sockets_data->socketmgr[socket].status = network_bad_socket;
    }
    else
    {
        abort();
    }
}  

static void fwd_server_socket_status_destory(void)
{
    fwd_free(g_server_sockets_data);
}

FWDQueue * fwd_create_queue(uint32 size)
{
	FWDQueue *queue = NULL;
	queue = (FWDQueue*)fwd_malloc(sizeof(FWDQueue));
	memset(queue, 0X00, sizeof(FWDQueue));
	
	queue->q_list = (FWDQueueElement*)fwd_malloc(sizeof(FWDQueueElement) * size);    
    if (NULL == queue->q_list)
    {
        return NULL;
    }
    
	/* all set zero */
    memset(queue->q_list, 0X00, sizeof(FWDQueueElement) * size);

	queue->q_length = size;            
    queue->q_head   = 0;
    queue->q_tail   = 0;
    queue->debug_fp = NULL;
    SpinLockInit(&(queue->q_lock));
	return queue;
}
/* destory the queue and drop all left over packages. */
void fwd_destory_queue(FWDQueue *queue)
{
	int32 slot;
	if (queue)
	{	
		if (queue->q_list)
		{
			if (queue->q_head <= queue->q_tail)
			{
				for (slot = queue->q_head; slot < queue->q_tail; slot++)
				{
					if (queue->q_list[slot].package)
					{
						fwd_drop_package(queue->q_list[slot].package);
					}
				}
				
			}
			else
			{
				for (slot = queue->q_head; slot < queue->q_length; slot++)
				{
					if (queue->q_list[slot].package)
					{
						fwd_drop_package(queue->q_list[slot].package);
					}
				}

				
				for (slot = 0; slot < queue->q_tail; slot++)
				{
					if (queue->q_list[slot].package)
					{
						fwd_drop_package(queue->q_list[slot].package);
					}
				}
			}
			fwd_free(queue->q_list);
            queue->q_head = -1;
            queue->q_tail = -1;
            queue->q_list = NULL;
		}
		fwd_free(queue);
	}
}

/* reset the queue and drop all left over packages. */
void fwd_reset_queue(FWDQueue *queue)
{
	int32 slot;
	if (queue)
	{	
		if (queue->q_list)
		{
			if (queue->q_head <= queue->q_tail)
			{
				for (slot = queue->q_head; slot < queue->q_tail; slot++)
				{
					if (queue->q_list[slot].package)
					{
						fwd_drop_package(queue->q_list[slot].package);
					}
				}
			}
			else
			{
				for (slot = queue->q_head; slot < queue->q_length; slot++)
				{
					if (queue->q_list[slot].package)
					{
						fwd_drop_package(queue->q_list[slot].package);
					}
				}
				
				for (slot = 0; slot < queue->q_tail; slot++)
				{
					if (queue->q_list[slot].package)
					{
						fwd_drop_package(queue->q_list[slot].package);
					}
				}
			}
            queue->q_head = 0;
            queue->q_tail = 0;
            memset(queue->q_list, 0X00, sizeof(FWDQueueElement) * queue->q_length);
		}
	}
}

/* Get the package with sequence number seq. */
FWDPackage *fwd_queue_get_package(FWDQueue *queue, uint64 seq)
{
    int32 distance = 0;
    int32 target = 0;
    int32 elemnum = 0;
    FWDPackage *pkg = NULL;
    SpinLockAcquire(&(queue->q_lock));
    if (queue->q_head == queue->q_tail)
    {
        SpinLockRelease(&(queue->q_lock));
        return NULL;
    }

    distance = seq - queue->q_list[queue->q_head].pkg_seq;
    elemnum = (queue->q_length - queue->q_head + queue->q_tail) % queue->q_length;
    if (distance >= elemnum)
    {
        SpinLockRelease(&(queue->q_lock));
        return NULL;
    }

    target = (queue->q_head + distance) % (queue->q_length);
    pkg = queue->q_list[target].package;
    SpinLockRelease(&(queue->q_lock));

    if (pkg->pkg_seq != seq)
    {
        abort();
    }
    return pkg;
}

/* Drop the package with sequence number seq, the dropped package must be at the beginning of the queue. */
void fwd_queue_drop_pkg(FWDQueue *queue, uint64 seq)
{
    FWDPackage *pkg = NULL;

    SpinLockAcquire(&(queue->q_lock));
    if (queue->q_head == queue->q_tail)
    {
        SpinLockRelease(&(queue->q_lock));
        return;                
    }

    if(seq != queue->q_list[queue->q_head].pkg_seq)
    {
        abort();
    }
    
	pkg = queue->q_list[queue->q_head].package;

	/* can not drop null package */
	if (NULL == pkg)
	{
        abort();
		SpinLockRelease(&(queue->q_lock));
        return;    
	}

    /* not complete sent, can't drop */
    if (pkg->pkg_left_len != 0 )
    {
        abort();
    }

	/* clear element */
	queue->q_list[queue->q_head].package = NULL;
	queue->q_head = (queue->q_head + 1) % queue->q_length;
	SpinLockRelease(&(queue->q_lock));
	
	/* drop the package */
	fwd_drop_package(pkg);
}

/* remove the package without drop. */
FWDPackage* fwd_queue_remove_pkg(FWDQueue *queue, uint64 seq)
{
	FWDPackage *pkg = NULL;

    SpinLockAcquire(&(queue->q_lock));
    if (queue->q_head == queue->q_tail)
    {
        SpinLockRelease(&(queue->q_lock));
        return NULL;                
    }

    if(seq != queue->q_list[queue->q_head].pkg_seq)
    {
        abort();
    }
    
	pkg = queue->q_list[queue->q_head].package;


	/* clear element */
	queue->q_list[queue->q_head].package = NULL;
	queue->q_head = (queue->q_head + 1) % queue->q_length;
	SpinLockRelease(&(queue->q_lock));

	return pkg;
}

/* Get the package with sequence number seq. */
void fwd_queue_get_first_seq(FWDQueue *queue, uint64 *seq)
{
	SpinLockAcquire(&(queue->q_lock));
    if (queue->q_head == queue->q_tail)
	{
        if (queue->q_head)
        {
            *seq = queue->q_list[queue->q_head - 1].pkg_seq;  
        }
        else
        {
		    *seq = queue->q_list[queue->q_length - 1].pkg_seq;  
        }
	}
    else
    {
        *seq = queue->q_list[queue->q_head].pkg_seq;  
    }
    SpinLockRelease(&(queue->q_lock));
}

/* Get the last package'sequence. */
void fwd_queue_get_last_seq(FWDQueue *queue, uint64 *seq)
{
	SpinLockAcquire(&(queue->q_lock));
	if (queue->q_tail)
	{
		*seq = queue->q_list[queue->q_tail - 1].pkg_seq;  
	}
	else
	{
		*seq = queue->q_list[queue->q_length - 1].pkg_seq;  
	}
    SpinLockRelease(&(queue->q_lock));
}

/* Just return the first element without advance the pointer. */
FWDPackage *fwd_queue_sniffer(FWDQueue *queue)
{
    FWDPackage *ptr = NULL;
	SpinLockAcquire(&(queue->q_lock));
    if (queue->q_head == queue->q_tail)
    {
        SpinLockRelease(&(queue->q_lock));
        return NULL;                
    }            
    ptr = queue->q_list[queue->q_head].package;
    SpinLockRelease(&(queue->q_lock));
    return ptr;
}

/* Push the element into the queue, if not enought space, return -1. */
int fwd_queue_push_pkg(FWDQueue *queue, FWDPackage *pkg, enum rda_comm_type type)
{
    FWDPackage *latest_pkg = NULL;
	if (pkg->pkg_seq < 0)
	{
		return -1;
	}

    if (g_fwd_dump_pkg_to_file)
    {
        forwarder_thread_logger(LOG, FORWARDER_PREFIX "%s fwd_queue_push_pkg enter sequence:%ld, thread:%ld", g_rda_comm_type_names[type], pkg->pkg_seq, pthread_self());
    }
	
	SpinLockAcquire(&(queue->q_lock));
  
    /* full queue, return immediately. */
    if ((queue->q_tail + 1) % queue->q_length == queue->q_head)
    {
        SpinLockRelease(&(queue->q_lock));
        return -1;
    }

    if (queue->q_tail != queue->q_head)
    {
        if (queue->q_tail != 0)
        {
            latest_pkg = queue->q_list[queue->q_tail - 1].package;
        }
        else
        {
            latest_pkg = queue->q_list[queue->q_length - 1].package;
        }
        
        if (latest_pkg->pkg_seq != pkg->pkg_seq - 1)
        {
            abort();
        }
    }

    /* set the package */
	queue->q_list[queue->q_tail].package = pkg;
	queue->q_list[queue->q_tail].pkg_seq = pkg->pkg_seq;

	queue->q_tail = (queue->q_tail + 1) % queue->q_length;
	SpinLockRelease(&(queue->q_lock));  
	return 0;
}

bool fwd_queue_is_full(FWDQueue *queue)
{
    SpinLockAcquire(&(queue->q_lock));
    if ((queue->q_tail + 1) % queue->q_length == queue->q_head)
    {
        SpinLockRelease(&(queue->q_lock));
        return true;
    }
    else
    {
        SpinLockRelease(&(queue->q_lock));
        return false;
    }
}
bool fwd_queue_is_empty(FWDQueue *queue)
{
    SpinLockAcquire(&(queue->q_lock));
    if (queue->q_tail == queue->q_head)
    {
        SpinLockRelease(&(queue->q_lock));
        return true;
    }
    else
    {
        SpinLockRelease(&(queue->q_lock));
        return false;
    }
}
int fwd_queue_data_length(FWDQueue *queue)
{
	int len = -1;
	SpinLockAcquire(&(queue->q_lock));
	if (queue->q_head != (queue->q_tail + 1) % queue->q_length)
    {    
        len = (queue->q_length - queue->q_head + queue->q_tail) % queue->q_length; 
    }
	else
	{
		len = queue->q_length - 1;
	}
	
	SpinLockRelease(&(queue->q_lock));
	return  len >= 0 ? len : 0;
}

int fwd_queue_free_length(FWDQueue *queue)
{
	int len = -1;
	SpinLockAcquire(&(queue->q_lock));
	if (queue->q_tail != queue->q_head)
    {    
        len = (queue->q_length - queue->q_tail + queue->q_head) % queue->q_length - 1;
    }
	else
	{
		len = queue->q_length - 1;
	}
	SpinLockRelease(&(queue->q_lock));
	return len >= 0 ? len : 0;
}

void TeleDBForwarderIam(void)
{
	am_forwarder = true;
}

bool
IsForwarderProcess(void)
{
    return am_forwarder;
}

/*
 *  Signal handler.
 */
static void
forwarder_die(SIGNAL_ARGS)
{
	shutdown_requested = true;
}


static void
forwarder_sig_hup_handler(SIGNAL_ARGS)
{
	got_SIGHUP = true;
}


static void
forwarder_quickdie(SIGNAL_ARGS)
{
	PG_SETMASK(&BlockSig);
	exit(2);
}

/*
 * Handle the asynchronous log print in forwarder.
 */
static void
setup_formatted_current_log_time(char* formatted_current_log_time)
{
    pg_time_t	stamp_time;
    char		msbuf[13];
    struct timeval timeval;

    gettimeofday(&timeval, NULL);
    stamp_time = (pg_time_t) timeval.tv_sec;

    /*
     * Note: we expect that guc.c will ensure that log_timezone is set up (at
     * least with a minimal GMT value) before Log_line_prefix can become
     * nonempty or CSV mode can be selected.
     */
    pg_strftime(formatted_current_log_time, FORMATTED_TS_LEN,
            /* leave room for milliseconds... */
                "%Y-%m-%d %H:%M:%S     %Z",
                pg_localtime(&stamp_time, log_timezone));

    /* 'paste' milliseconds into place... */
    sprintf(msbuf, ".%03d", (int) (timeval.tv_usec / 1000));
    memcpy(formatted_current_log_time + 19, msbuf, 4);
}

/*
 * write pooler's subthread log into thread log queue
 * only call by pooler's subthread in elog
 */
static void
forwarder_subthread_write_log(int elevel, int lineno, const char *filename, const char *funcname, const char *fmt, ...)
{
    char *buf = NULL;
    int buf_len = 0;
    int offset = 0;
    char formatted_current_log_time[FORMATTED_TS_LEN];
	
    if (PipeIsFull(g_ForwarderLogQueue))
    {
        return;
    }

    /* use malloc in sub thread */
    buf_len = strlen(filename) + strlen(funcname) + FWD_DEFAULT_LOG_BUF_LEN;
    buf = (char*)malloc(buf_len);
    if (buf == NULL)
    {
        /* no log */
        return;
    }

    /* construction log, format: elevel | lineno | filename | funcname | log content */
    *(int*)(buf + offset) = elevel;
    offset += sizeof(elevel);
    *(int*)(buf + offset) = lineno;
    offset += sizeof(lineno);
    memcpy(buf + offset, filename, strlen(filename) + 1);
    offset += (strlen(filename) + 1);
    memcpy(buf + offset, funcname, strlen(funcname) + 1);
    offset += (strlen(funcname) + 1);

    /*
     * because the main thread writes the log of the sub thread asynchronously,
     * record the actual log writing time here
     */
    setup_formatted_current_log_time(formatted_current_log_time);
    memcpy(buf + offset, formatted_current_log_time, strlen(formatted_current_log_time));
    offset += strlen(formatted_current_log_time);
    *(char*)(buf + offset) = ' ';
    offset += sizeof(char);

    /* Generate actual output --- have to use appendStringInfoVA */
    for (;;)
    {
        va_list		args;
        int			avail;
        int			nprinted;

        avail = buf_len - offset - 1;
        va_start(args, fmt);
        nprinted = vsnprintf(buf + offset, avail, fmt, args);
        va_end(args);
        if (nprinted >= 0 && nprinted < avail - 1)
        {
            offset += nprinted;
            *(char*)(buf + offset) = '\0';
			offset += sizeof(char);
            break;
        }

        buf_len = (buf_len * 2 > (int) MaxAllocSize) ? MaxAllocSize : buf_len * 2;
        buf = (char *) realloc(buf, buf_len);
        if (buf == NULL)
        {
            /* no log */
            return;
        }
    }

    /* put log into thread log queue, drop log if queue is full */
    if (-1 == PipePut(g_ForwarderLogQueue, buf))
    {
        free(buf);
    }
}

/*
 * write subthread log in main thread
 */
static void
forwarder_handle_subthread_log(bool is_pooler_exit)
{
    int write_log_cnt = 0;
    int offset = 0;
    int elevel = LOG;
    int lineno = 0;
    int pipe_len = 0;
    int total_thread_num = 0;
    char *log_buf = NULL;
    char *filename = NULL;
    char *funcname = NULL;
    char *log_content = NULL;

    total_thread_num = (g_FwdWorkerNum * 4 + g_FwdIOThreadNum * 2+ 2);/* sender/receiver + io thread + cleaner + listener */

    while ((log_buf = (char*)PipeGet(g_ForwarderLogQueue)) != NULL)
    {
        /* elevel | lineno | filename | funcname | log content */
        elevel = *(int*)log_buf;
        offset = sizeof(elevel);
        lineno = *(int*)(log_buf + offset);
        offset += sizeof(lineno);
        filename = log_buf + offset;
        offset += (strlen(filename) + 1);
        funcname = log_buf + offset;
        offset += (strlen(funcname) + 1);
        log_content = log_buf + offset;

        /* write log here */
        elog_start(filename, lineno,
#ifdef USE_MODULE_MSGIDS
                PGXL_MSG_MODULE, PGXL_MSG_FILEID, __COUNTER__,
#endif
                funcname);
        elog_finish(elevel, "%s", log_content);

        free(log_buf);

        /*
         * if the number of logs written at one time exceeds POOLER_WRITE_LOG_ONCE_LIMIT,
         * in order not to block the main thread, return here
         */
        if (write_log_cnt++ >= (FWD_WRITE_LOG_ONCE_LIMIT * total_thread_num) && !is_pooler_exit)
        {
            pipe_len = PipeLength(g_ForwarderLogQueue);
            fwd_check_thread_id();
            elog(LOG, FORWARDER_PREFIX"forwarder_handle_subthread_log write_log_cnt:%d statements printed, pipe_len %d", write_log_cnt, pipe_len);
            return;
        }
    }
    pipe_len = PipeLength(g_ForwarderLogQueue);
}

static ForwarderHandle *
GetForwarderRDAHandle(void)
{
    ForwarderHandle *handle;
    int             fdsock;

    /* Connect to the forwarder */
    fdsock = forwarder_connect(g_FwdServerPort, Unix_socket_directories);
    if (fdsock < 0)
    {
        int         saved_errno = errno;

        ereport(ERROR,
                (errcode(ERRCODE_CONNECTION_FAILURE),
                    errmsg(FORWARDER_PREFIX"failed to connect to forwarder manager: %m")));
        errno = saved_errno;
        return NULL;
    }

    /* Allocate handle */
    /*
     * XXX we may change malloc here to palloc but first ensure
     * the CurrentMemoryContext is properly set.
     * The handle allocated just before new session is forked off and
     * inherited by the session process. It should remain valid for all
     * the session lifetime.
     */
    handle = (ForwarderHandle *) malloc(sizeof(ForwarderHandle));
    if (!handle)
    {
        ereport(ERROR,
                (errcode(ERRCODE_OUT_OF_MEMORY),
                    errmsg(FORWARDER_PREFIX"out of memory")));
        return NULL;
    }

    handle->port.fdsock = fdsock;
    handle->port.RecvLength = 0;
    handle->port.RecvPointer = 0;
    handle->port.SendPointer = 0;

    return handle;
}

static void
ForwarderRDACloseHandle(ForwarderHandle *handle)
{
    close(Socket(handle->port));
    free(handle);
}

static void
ForwarderRDAConnect(ForwarderHandle *handle, const char *database, const char *user_name)
{
    int n32;
    char msgtype = 'c';

    Assert(handle);
    Assert(database);
    Assert(user_name);

    /* Save the handle */
    forwarderHandle = handle;

    /* Message type */
    forwarder_putbytes(&handle->port, &msgtype, 1);

    /* Message length */
    n32 = htonl(strlen(database) + strlen(user_name) + 18);
    forwarder_putbytes(&handle->port, (char *) &n32, 4);

    /* PID number */
    n32 = htonl(MyProcPid);
    forwarder_putbytes(&handle->port, (char *) &n32, 4);

    /* Length of Database string */
    n32 = htonl(strlen(database) + 1);
    forwarder_putbytes(&handle->port, (char *) &n32, 4);

    /* Send database name followed by \0 terminator */
    forwarder_putbytes(&handle->port, database, strlen(database) + 1);
    forwarder_flush(&handle->port);

    /* Length of user name string */
    n32 = htonl(strlen(user_name) + 1);
    forwarder_putbytes(&handle->port, (char *) &n32, 4);

    /* Send user name followed by \0 terminator */
    forwarder_putbytes(&handle->port, user_name, strlen(user_name) + 1);
    forwarder_flush(&handle->port);
}

int
ForwarderRegisterRDA(int64 rda_id, int *dest_node_ids, int dest_num,
                         int *src_node_ids, int src_num)
{
    uint32          n32;
    int             i;
    uint32          buf[4 + dest_num + src_num]; /*rda_id(2) + srcnum(1) + dstnum(1)*/

    if (forwarderHandle == NULL)
        return EOF;

    if (dest_num == 0 || src_num == 0)
        return EOF;

    if (dest_node_ids == NULL || src_node_ids == NULL)
        return EOF;

    /* insert rda_id */
    n32 = htonl((uint32)(0xFFFFFFFF & (rda_id>>32)));
    buf[0] = n32;
    n32 = htonl((uint32)(0xFFFFFFFF & (rda_id)));
    buf[1] = n32;

    /* insert src node length and node list */
    n32 = htonl((uint32) src_num);
    buf[2] = n32;
    for (i = 3; i < src_num + 3; i++)
    {
        n32 = htonl((uint32) src_node_ids[i - 3]);
        buf[i] = n32;
    }

    /* insert dest node length and node list */
    n32 = htonl((uint32) dest_num);
    buf[3 + src_num] = n32;
    for (i = (4 + src_num); i < dest_num + (4 + src_num); i++)
    {
        n32 = htonl((uint32) dest_node_ids[i - (4 + src_num)]);
        buf[i] = n32;
    }

    forwarder_putmessage(&forwarderHandle->port, 'b', (char *) buf,
                            (4 + dest_num + src_num)*sizeof(uint32));
    forwarder_flush(&forwarderHandle->port);

    /* Get result */
    return forwarder_recvres(&forwarderHandle->port, true);
}

int
ForwarderUnRegisterRDA(int64 rda_id, int *dest_node_ids, int dest_num,
                                             int *src_node_ids, int src_num)
{
    uint32          n32;
    int             i;
    uint32          buf[4 + dest_num + src_num];

    if (forwarderHandle == NULL)
        return EOF;

    if (dest_num == 0 || src_num == 0)
        return EOF;

    if (dest_node_ids == NULL || src_node_ids == NULL)
        return EOF;

    /* insert rda_id */
    n32 = htonl((uint32)(0xFFFFFFFF & (rda_id>>32)));
    buf[0] = n32;
    n32 = htonl((uint32)(0xFFFFFFFF & (rda_id)));
    buf[1] = n32;

    /* insert src node length and node list */
    n32 = htonl((uint32) src_num);
    buf[2] = n32;
    for (i = 3; i < src_num + 3; i++)
    {
        n32 = htonl((uint32) src_node_ids[i - 3]);
        buf[i] = n32;
    }

    /* insert dest node length and node list */
    n32 = htonl((uint32) dest_num);
    buf[3 + src_num] = n32;
    for (i = (4 + src_num); i < dest_num + (4 + src_num); i++)
    {
        n32 = htonl((uint32) dest_node_ids[i - (4 + src_num)]);
        buf[i] = n32;
    }

    forwarder_putmessage(&forwarderHandle->port, 'q', (char *) buf,
                            (4 + dest_num + src_num)*sizeof(uint32));
    forwarder_flush(&forwarderHandle->port);

    /* Get result */
    return forwarder_recvres(&forwarderHandle->port, true);
}

/*
 * Call the reload rda services
 * Then rebuild the socket connection at forwarder process
*/
int
ForwarderReload(void)
{
    if (forwarderHandle == NULL)
        return -1;

    forwarder_putmessage(&forwarderHandle->port, 'p', NULL, 0);
    forwarder_flush(&forwarderHandle->port);

    return 0;
}

/*
 * Get reload status.
 * -1: call reload service failed
 * 0: reload have finished
 * 1: reloading
 */
int
ForwarderReloadStatus(void)
{
    if (forwarderHandle == NULL)
        return -1;

    forwarder_putmessage(&forwarderHandle->port, 's', NULL, 0);
    forwarder_flush(&forwarderHandle->port);

    return forwarder_recvres(&forwarderHandle->port, true);
}

/*
 * Call the dump_rda_status rpc services
*/
rda_stat_record *
ForwarderStatRDA(int *number)
{
    if (forwarderHandle == NULL)
    {
        *number = 0;
        return NULL;    
    }

    forwarder_putmessage(&forwarderHandle->port, 'r', NULL, 0);
    forwarder_flush(&forwarderHandle->port);

    return ForwarderGetStatusData(number);
}


void
ConnectForwarderRDA(void)
{
    ForwarderHandle *handle = NULL;

    handle = GetForwarderRDAHandle();

    ForwarderRDAConnect(handle, get_database_name(MyDatabaseId), GetClusterUserName());
}

void
ForwarderRDADisconnect(void)
{
    if (forwarderHandle)
    {
        Assert(forwarderHandle);

        forwarder_putmessage(&forwarderHandle->port, 'd', NULL, 0);
        forwarder_flush(&forwarderHandle->port);

        ForwarderRDACloseHandle(forwarderHandle);
        forwarderHandle = NULL;
    }
}

/* bitmap index occupytation management */
static BitmapMgr *
BmpMgrCreate(uint32 objnum)
{
    BitmapMgr *mgr = NULL;

    mgr = (BitmapMgr*)palloc0(sizeof(BitmapMgr));

    mgr->m_fsmlen  = DIVIDE_UP(objnum, BITS_IN_LONGLONG);
    mgr->m_nobject = mgr->m_fsmlen * BITS_IN_LONGLONG;  /* align the object number to 64 */

    mgr->m_fsm = palloc(sizeof(uint64) * mgr->m_fsmlen);

    /* zero the FSM memory */
    memset(mgr->m_fsm, 0X00, sizeof(uint64) * mgr->m_fsmlen);

    mgr->m_version  = INIT_VERSION;
    return mgr;
};

/* find an unused index and return it */
static int
BmpMgrAlloc(BitmapMgr *mgr)
{
    uint32  i       = 0;
    uint32  j       = 0;
    uint32  k       = 0;
    int     offset  = 0;
    uint8   *ucaddr = NULL;
    uint32  *uiaddr = NULL;

    /* sequencial scan the FSM map */
    for (i = 0; i < mgr->m_fsmlen; i++)
    {
        /* got free space */
        if (mgr->m_fsm[i] < MAX_UINT64)
        {
            /* first word has free space */
            uiaddr = (uint32*)&(mgr->m_fsm[i]);
            if (uiaddr[0] < MAX_UINT32)
            {
                ucaddr = (uint8*)&uiaddr[0];
                offset = 0;
            }
            else
            {
                /* in second word */
                ucaddr = (uint8*)&uiaddr[1];
                offset = BITS_IN_WORD;
            }

            /* find free space */
            for (j = 0; j < sizeof(uint32); j++)
            {
                if (ucaddr[j] < MAX_UINT8)
                {
                    for (k = 0; k < BITS_IN_BYTE; k++)
                    {
                        if (BIT_CLEAR(ucaddr[j], k))
                        {
                            SET_BIT(ucaddr[j], k);
                            mgr->m_version++;
                            return offset + i * BITS_IN_LONGLONG + j * BITS_IN_BYTE + k;
                        }
                    }
                }
            }
        }
    }

    /* out of space */
    return -1;
}

/* free index */
static void
BmpMgrFree(BitmapMgr *mgr, int index)
{
    uint32  fsmidx = 0;
    uint32  offset = 0;
    uint8  *pAddr  = NULL;

    if (index < 0 || (uint32)index >= mgr->m_nobject)
    {
        fwd_check_thread_id();
        elog(PANIC, FORWARDER_PREFIX"invalid index:%d", index);
    }

    offset = index % BITS_IN_LONGLONG;
    fsmidx = index / BITS_IN_LONGLONG;

    pAddr  = (uint8*)&(mgr->m_fsm[fsmidx]);
    CLEAR_BIT(pAddr[offset / BITS_IN_BYTE], offset % BITS_IN_BYTE);
    mgr->m_version++;
}

static bool
BmpMgrHasIndexAndClear(BitmapMgr *mgr, int index)
{
    uint32  fsmidx = 0;
    uint32  offset = 0;
    uint8  *pAddr  = NULL;

    if (index < 0 || (uint32)index >= mgr->m_nobject)
    {
        fwd_check_thread_id();
        elog(PANIC, FORWARDER_PREFIX"invalid index:%d", index);
    }

    offset = index % BITS_IN_LONGLONG;
    fsmidx = index / BITS_IN_LONGLONG;

    pAddr  = (uint8*)&(mgr->m_fsm[fsmidx]);
    if( BIT_SET(pAddr[offset / BITS_IN_BYTE], offset % BITS_IN_BYTE))
    {
        CLEAR_BIT(pAddr[offset / BITS_IN_BYTE], offset % BITS_IN_BYTE);
        mgr->m_version++;
        return true;
    }
    return false;
}

static uint32
BmpMgrGetVersion(BitmapMgr *mgr)
{
    return mgr->m_version;
}

static int
BmpMgrGetUsed(BitmapMgr *mgr, int32 *indexes, int32 indexlen)
{
    uint32   u64num  = 0;
    uint32   u32num  = 0;
    uint32   u8num   = 0;
    uint32   ubitnum = 0;
    int32    number  = 0;
    uint8   *ucaddr  = NULL;
    uint32  *uiaddr  = NULL;

    if (indexlen != mgr->m_nobject)
    {
        return -1;
    }

    /* scan the array */
    for (u64num = 0; u64num < mgr->m_fsmlen; u64num++)
    {
        if (mgr->m_fsm[u64num])
        {
            uiaddr = (uint32*)&(mgr->m_fsm[u64num]);
            for (u32num = 0; u32num < sizeof(uint64)/sizeof(uint32); u32num++)
            {
                if (uiaddr[u32num])
                {
                    ucaddr = (uint8*)&uiaddr[u32num];

                    for (u8num = 0; u8num < sizeof(uint32); u8num++)
                    {
                        if (ucaddr[u8num])
                        {
                            for (ubitnum = 0; ubitnum < BITS_IN_BYTE; ubitnum++)
                            {
                                if (BIT_SET(ucaddr[u8num], ubitnum))
                                {
                                    indexes[number] = u64num * BITS_IN_LONGLONG + BITS_IN_WORD * u32num + u8num * BITS_IN_BYTE + ubitnum;
                                    number++;
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    return number;
}

static inline void
RebuildAgentIndex(void)
{
    if (mgrVersion != BmpMgrGetVersion(forwarderAgentMgr))
    {
        usedAgentSize= BmpMgrGetUsed(forwarderAgentMgr, agentIndexes, forwarderAgentSize);
        if (usedAgentSize != agentCount)
        {
            fwd_check_thread_id();
            elog(PANIC, FORWARDER_PREFIX"invalid BmpMgr status");
        }
        mgrVersion = BmpMgrGetVersion(forwarderAgentMgr);
    }
}

static void
agent_create(int new_fd)
{
    ForwarderAgent  *agent;
    int32           agentindex = 0;

    agentindex = BmpMgrAlloc(forwarderAgentMgr);
    if (-1 == agentindex)
    {
        ereport(PANIC,
                (errcode(ERRCODE_OUT_OF_MEMORY),
                    errmsg(FORWARDER_PREFIX"out of agent index")));
    }

    /* Allocate agent */
    agent = (ForwarderAgent *) palloc0(sizeof(ForwarderAgent));
    if (!agent)
    {
        close(new_fd);
        BmpMgrFree(forwarderAgentMgr, agentindex);
        ereport(ERROR,
                (errcode(ERRCODE_OUT_OF_MEMORY),
                    errmsg(FORWARDER_PREFIX"out of memory")));
        return;
    }

    agent->port.fdsock = new_fd;
    agent->port.RecvLength = 0;
    agent->port.RecvPointer = 0;
    agent->port.SendPointer = 0;

    agent->pid = 0;
    agent->agentindex = agentindex;
    /* Append new agent to the list */
    forwarderAgents[agentindex] = agent;

    agentCount++;

    forwarder_thread_logger(DEBUG1,
                            FORWARDER_PREFIX"agent_create end, agentCount:%d, agentIndex:%d, fd:%d",
                            agentCount, agentindex, new_fd);
}

static void
agent_destroy(ForwarderAgent *agent)
{
    int32  agentindex;
    int32  fd;
    Assert(agent);

    agentindex = agent->agentindex;
    fd         = Socket(agent->port);
    close(fd);

    forwarder_thread_logger(DEBUG1, FORWARDER_PREFIX"agent_destroy close fd:%d, pid:%d", fd, agent->pid);

    pfree(agent);
    /* Destory shadow */
    if (BmpMgrHasIndexAndClear(forwarderAgentMgr, agentindex))
    {
        --agentCount;
    }
    forwarderAgents[agentindex] = NULL;
}

static void
handle_connect(ForwarderAgent * agent, StringInfo s)
{
    int         len;
    const char  *database = NULL;
    const char  *user_name = NULL;

    forwarder_getmessage(&agent->port, s, 0);
    agent->pid = pq_getmsgint(s, 4);

    len = pq_getmsgint(s, 4);
    database = pq_getmsgbytes(s, len);

    len = pq_getmsgint(s, 4);
    user_name = pq_getmsgbytes(s, len);

    agent->database = pstrdup(database);
    agent->user_name = pstrdup(user_name);

    pq_getmsgend(s);
}

static void
handle_reload_status(ForwarderAgent *agent)
{
    bool ret = false;
    ret = get_reload_status_impl();
    if (ret)
    {
        forwarder_sendres(&agent->port, 0, NULL, 0, true);
    }
    else
    {
        forwarder_sendres(&agent->port, 1, NULL, 0, true);
    }
}

static void
handle_reload(ForwarderAgent *agent)
{
    fwd_socket_reload_impl();
}

static void
handle_dump_rda_status(ForwarderAgent *agent)
{
    char *tag      = "r";
    int32 ret      = 0;
    int32 n32      = 0;
    int32 data_len = 0;
    int32 index    = 0;
    rda_stat_record *result      = NULL;
    rda_stat_record *hash_record = NULL;
	HASH_SEQ_STATUS scan_status;

    data_len = g_rda_stat_number * RDA_STAT_ELEMENT_SIZE;
    /* tag + code + length + data */
    if (data_len)
    {
        result=  (rda_stat_record*)fwd_malloc(data_len);
        if (NULL == result)
        {
            /* command tag */
        	forwarder_putbytes(&agent->port, tag, 1);

            /* ret code */
            ret = -1;
        	n32 = htonl(ret);
        	forwarder_putbytes(&agent->port, (char *) &n32, 4);
        	
        	pool_flush(&agent->port);
            elog(LOG, FORWARDER_PREFIX "handle_dump_rda_status out of memory");
            return;
        }

        index = 0;
        hash_seq_init(&scan_status, g_rda_stat_hash);
		while ((hash_record = (rda_stat_record *) hash_seq_search(&scan_status)) != NULL && index < g_rda_stat_number)
		{
			memcpy((void*)(result + index * RDA_STAT_ELEMENT_NUM), (void*)hash_record, RDA_STAT_ELEMENT_SIZE);
            index++;
		}
        /* command tag */
        forwarder_putbytes(&agent->port, tag, 1);

        /* ret code */
        n32 = 0;
        forwarder_putbytes(&agent->port, (char *) &n32, 4);

        /* data length */
        n32 = htonl(data_len);
        forwarder_putbytes(&agent->port, (char *) &n32, 4);

        /* data */
        forwarder_putbytes(&agent->port, (char *) result, data_len);
        pool_flush(&agent->port);

        fwd_free(result);
    }    
    else /* no data */
    {
        /* command tag */
        forwarder_putbytes(&agent->port, tag, 1);

        /* ret code */
        n32 = 0;
        forwarder_putbytes(&agent->port, (char *) &n32, 4);

        /* data length */
        n32 = 0;
        forwarder_putbytes(&agent->port, (char *) &n32, 4);
        pool_flush(&agent->port);
    }
}

rda_stat_record* ForwarderGetStatusData(int *number)
{
    char  tag      = '\0';
    int32 ret      = 0;
    int32 n32      = 0;
    int32 data_len = 0;
    rda_stat_record *result = NULL;

    *number = 0;
    if (NULL == forwarderHandle)
    {
        return NULL;
    }

    /* tag + code + length + data */
    forwarder_getbytes(&forwarderHandle->port, (char*)&tag, 1);

    forwarder_getbytes(&forwarderHandle->port, (char*)&n32, 4);
    ret = ntohl(n32);
    if (ret)
    {
        return NULL;
    }

    forwarder_getbytes(&forwarderHandle->port, (char*)&n32, 4);
    data_len = ntohl(n32);
    if (0 == data_len)
    {   
        return NULL;
    }
    result = (rda_stat_record*)palloc0(data_len);
    forwarder_getbytes(&forwarderHandle->port, (char*)result, data_len);
    
    /* here we returnt the number of rda_stat_record instead of RDA number */
    *number = data_len / sizeof(rda_stat_record);
    return result;
}

static void
handle_unregister(ForwarderAgent *agent, StringInfo s)
{
    int         i         = 0;
    int         ret       = 0;
    int64       rda_id    = 0;
    int         dest_num  = 0;
    int         src_num   = 0;
    int         *dest_node_ids  = NULL;
    int         *src_node_ids   = NULL;

    forwarder_getmessage(&agent->port, s, 0);
    rda_id = pq_getmsgint64(s);

    src_num = pq_getmsgint(s, 4);
    src_node_ids = (int *)palloc(src_num * sizeof(int));
    for (i = 0; i < src_num; i++)
    {
        src_node_ids[i] = pq_getmsgint(s, 4);
        fwd_check_thread_id();
        elog(DEBUG5, FORWARDER_PREFIX "reg src idx:%d, nid:%d", i, src_node_ids[i]);
    }

    dest_num = pq_getmsgint(s, 4);
    dest_node_ids = (int *)palloc(dest_num * sizeof(int));
    for (i = 0; i < dest_num; i++)
    {
        dest_node_ids[i] = pq_getmsgint(s, 4);
        fwd_check_thread_id();
        elog(DEBUG5, FORWARDER_PREFIX "reg dest idx:%d, nid:%d", i, dest_node_ids[i]);
    }
    pq_getmsgend(s);

    ret = fwd_unregister_rda_impl(rda_id, src_node_ids, src_num, dest_node_ids, dest_num);

    fwd_check_thread_id();
    elog(LOG, FORWARDER_PREFIX "unregister: rda_id:%ld, src_num: %d, dest_num: %d, result: %d",
                            rda_id, src_num, dest_num, ret);

    forwarder_sendres(&agent->port, ret, NULL, 0, true);
}

/* Only called in main thread. */
static void
handle_register(ForwarderAgent *agent, StringInfo s)
{
    int         i;
	int         ret;
    int64       rda_id;
    int         dest_num, src_num;
    int         *dest_node_ids, *src_node_ids;

    forwarder_getmessage(&agent->port, s, 0);
    rda_id = pq_getmsgint64(s);

    src_num = pq_getmsgint(s, 4);
    src_node_ids = (int *)palloc(src_num * sizeof(int));
    for (i = 0; i < src_num; i++)
    {
        src_node_ids[i] = pq_getmsgint(s, 4);
        fwd_check_thread_id();
        elog(DEBUG5, FORWARDER_PREFIX "reg src idx:%d, nid:%d", i, src_node_ids[i]);
    }

    dest_num = pq_getmsgint(s, 4);
    dest_node_ids = (int *)palloc(dest_num * sizeof(int));
    for (i = 0; i < dest_num; i++)
    {
        dest_node_ids[i] = pq_getmsgint(s, 4);
        fwd_check_thread_id();
        elog(DEBUG5, FORWARDER_PREFIX "reg dest idx:%d, nid:%d", i, dest_node_ids[i]);
    }
    pq_getmsgend(s);
    
    ret = fwd_register_rda_impl(rda_id, src_node_ids, src_num, dest_node_ids, dest_num);
	
    fwd_check_thread_id();
    elog(LOG, FORWARDER_PREFIX "register: rda_id:%ld, my_id: %d, src_num: %d, dest_num: %d, result: %d",
                            rda_id, my_node_id, src_num, dest_num, ret);

    forwarder_sendres(&agent->port, ret, NULL, 0, true);
}

static void
agent_handle_input(ForwarderAgent * agent, StringInfo s)
{
    int         qtype;

    qtype = forwarder_getbyte(&agent->port);
    /*
     * We can have multiple messages, so handle them all
     */
    for (;;)
    {
        switch (qtype)
        {
            case 'c':           /* CONNECT */
                handle_connect(agent, s);
                break;
            case 'd':           /* DISCONNECT */
                forwarder_getmessage(&agent->port, s, 4);
                agent_destroy(agent);
                pq_getmsgend(s);
                break;
            case 'b':           /* register */
                handle_register(agent, s);
                break;
            case 'q':           /* unregister */
                handle_unregister(agent, s);
                break;
            case 'p':           /* reload */
                pool_getmessage(&agent->port, s, 4);
                handle_reload(agent);
                pq_getmsgend(s);
                break;
            case 's':           /* reload status */
                pool_getmessage(&agent->port, s, 4);
                handle_reload_status(agent);
                pq_getmsgend(s);
                break;
            case 'r':           /* get rda status */
                pool_getmessage(&agent->port, s, 4);
                handle_dump_rda_status(agent);
                pq_getmsgend(s);
                break;
            default:            /* EOF or protocol violation */
                fwd_check_thread_id();
                elog(WARNING, FORWARDER_PREFIX"invalid request tag:%c", qtype);
                agent_destroy(agent);
                return;
        }

        /* avoid reading from connection */
        if ((qtype = forwarder_pollbyte(&agent->port)) == EOF)
            break;
    }
}

static void
ForwarderAgentResInit(void)
{
    forwarderAgentSize = ALIGN_UP(MaxConnections, BITS_IN_LONGLONG);

    forwarderAgents = (ForwarderAgent **) palloc0(forwarderAgentSize * sizeof(ForwarderAgent *));
    if (forwarderAgents == NULL)
    {
        ereport(ERROR,
                (errcode(ERRCODE_OUT_OF_MEMORY),
                    errmsg(FORWARDER_PREFIX"out of memory")));
    }

    forwarderAgentMgr = BmpMgrCreate(forwarderAgentSize);
    if (forwarderAgentMgr == NULL)
    {
        ereport(ERROR,
                (errcode(ERRCODE_OUT_OF_MEMORY),
                    errmsg(FORWARDER_PREFIX"out of memory")));
    }

    agentIndexes = (int32*) palloc0(forwarderAgentSize * sizeof(int32));
    if (agentIndexes == NULL)
    {
        ereport(ERROR,
                (errcode(ERRCODE_OUT_OF_MEMORY),
                    errmsg(FORWARDER_PREFIX"out of memory")));
    }
}


/*
 * Initialize internal structures.
 */
int ForwarderInit()
{
    int ret = 0;
    g_fwd_main_thread_id = pthread_self();
    fwd_check_thread_id();
    elog(LOG, FORWARDER_PREFIX "Forwarder process is started: %d", getpid());

    fwd_init_memory_check();

    /*
     * Set up memory contexts for the forwarder objects
     */
    ForwarderMemoryContext = AllocSetContextCreate(TopMemoryContext,
                                                   "ForwarderMemoryContext",
                                                   ALLOCSET_DEFAULT_MINSIZE,
                                                   ALLOCSET_DEFAULT_INITSIZE,
                                                   ALLOCSET_DEFAULT_MAXSIZE);
	MemoryContextSwitchTo(ForwarderMemoryContext);

    ForgetLockFiles();

    /*
     * If possible, make this process a group leader, so that the postmaster
     * can signal any child processes too.	(pool manager probably never has any
     * child processes, but for consistency we make all postmaster child
     * processes do this.)
     */
#ifdef HAVE_SETSID
    if (setsid() < 0)
    {
        fwd_check_thread_id();
        elog(LOG, FORWARDER_PREFIX "setsid() failed: %m");
    }
#endif
    /*
     * Properly accept or ignore signals the postmaster might send us
     */
    pqsignal(SIGINT, forwarder_die);
    pqsignal(SIGTERM, forwarder_die);
    pqsignal(SIGQUIT, forwarder_quickdie);
    pqsignal(SIGHUP, forwarder_sig_hup_handler);


    /* We allow SIGQUIT (quickdie) at all times */
    sigdelset(&BlockSig, SIGQUIT);

    /*
     * Unblock signals (they were blocked when the postmaster forked us)
     */
    PG_SETMASK(&UnBlockSig);

    TeleDBForwarderIam();
    
    /* create log queue, so we can print log in mutiple threads. */
    g_ForwarderLogQueue = CreatePipe(FWD_MAX_THREAD_LOG_PIPE_LEN);

    /* Init sender, receiver and cleanner resource*/
    ret = fwd_init_thread_resource();
    if (ret)
    {
        fwd_clean_fwd_resource();
        fwd_check_thread_id();
        elog(LOG, FORWARDER_PREFIX "fwd_clean_fwd_resource failed");
        return -1;
    }
    fwd_check_thread_id();
    elog(LOG, FORWARDER_PREFIX "worker thread resource initiated");

    /* Init thread control */
    g_fwd_data_control = init_data_fwd_control();
    if (NULL == g_fwd_data_control)
    {
        fwd_clean_fwd_resource();
        fwd_check_thread_id();
        elog(LOG, FORWARDER_PREFIX "init_data_fwd_control failed");
        return -1;
    }
    fwd_check_thread_id();
    elog(LOG, FORWARDER_PREFIX "worker thread control initiated");

    /* init agent server for local connection */
    ForwarderAgentResInit();

    /* do the hard stuff. */
    ForwarderLoop();
    return 0;
}

/*
 * Main handling loop
 */
static void
ForwarderLoop(void)
{
    int             i           = 0;
    int             ret         = 0;    
    int             status      = 0;
    StringInfoData  input_message;
    int             maxfd       = MaxConnections + 1024;
    struct pollfd   *pool_fd    = NULL;
    int             timeout_val = FORWARDER_POLL_TIMEOUT_VAL;

#ifdef HAVE_UNIX_SOCKETS
    if (Unix_socket_directories)
    {
        char       *rawstring;
        List       *elemlist;
        ListCell   *l;

        int        success = 0;

        /* Need a modifiable copy of Unix_socket_directories */
        rawstring = pstrdup(Unix_socket_directories);

        /* Parse string into list of directories */
        if (!SplitDirectoriesString(rawstring, ',', &elemlist))
        {
            /* syntax error in list */
            ereport(FATAL,
                    (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("invalid list syntax in parameter \"%s\"",
                            "unix_socket_directories")));
        }

        foreach(l, elemlist)
        {
            char        *socketdir  = (char *) lfirst(l);
            int         saved_errno;

            /* Connect to the pooler */
            g_fwd_unix_server_fd = forwarder_listen(g_FwdServerPort, socketdir);
            if (g_fwd_unix_server_fd < 0)
            {
                saved_errno = errno;
                ereport(WARNING,
                        (errmsg(FORWARDER_PREFIX"could not create Unix-domain socket in directory \"%s\", errno %d, g_fwd_unix_server_fd %d",
                                socketdir, saved_errno, g_fwd_unix_server_fd)));
            }
            else
            {
                success++;
            }
        }

        if (!success && elemlist != NIL)
            ereport(ERROR,
                    (errmsg(FORWARDER_PREFIX"failed to start listening on Unix-domain socket for forwarder: %m")));

        list_free_deep(elemlist);
        pfree(rawstring);
    }
#endif

    fwd_check_thread_id();
    elog(LOG, FORWARDER_PREFIX"ForwarderLoop begin g_fwd_unix_server_fd:%d", g_fwd_unix_server_fd);

    pool_fd = (struct pollfd *) palloc(maxfd * sizeof(struct pollfd));
    pool_fd[0].fd     = g_fwd_unix_server_fd;
    pool_fd[0].events = POLLIN; //POLLRDNORM;

    for (i = 1; i < maxfd; i++)
    {
        pool_fd[i].fd = -1;
        pool_fd[i].events = POLLIN | POLLPRI | POLLRDNORM | POLLRDBAND;
    }
    initStringInfo(&input_message);

    /* open listen on fwd port */
    for (i = 0; i < FWD_MAXLISTEN; i++)
    {
        FWDListenSocket[i] = -1;
    }
    
    if (ListenAddresses)
    {
        if (strcmp(ListenAddresses, "*") == 0)
            status = StreamServerPort(AF_UNSPEC, NULL,
                                      (unsigned short) g_FwdServerPort,
                                      NULL,
                                      FWDListenSocket, FWD_MAXLISTEN);
        else
            status = StreamServerPort(AF_UNSPEC, ListenAddresses,
                                      (unsigned short) g_FwdServerPort,
                                      NULL,
                                      FWDListenSocket, FWD_MAXLISTEN);

        if (status != STATUS_OK)
            ereport(FATAL,
                    (errmsg(FORWARDER_PREFIX"could not create listen socket for \"%s\" port %d",
                            ListenAddresses, g_FwdServerPort)));
    }

    /* start server thread after we have prepared FWDListenSocket*/
    ret = fwd_init_server_threads();
    if (ret != 0)
    {
        ereport(FATAL,
                    (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("start server thread failed")));
    }

    /* Start threads */
    fwd_create_service_thread();
    fwd_check_thread_id();
    elog(LOG, FORWARDER_PREFIX "worker thread started");

    // RDAQueueTest();
    // SocketTest();
    // RDAControllerTest();
    //sleep(60);
    //ForwarderTest();
    for (;;)
    {
        int retval;
        int i;

        if (got_SIGHUP)
        {
            got_SIGHUP = false;
            ProcessConfigFile(PGC_SIGHUP);
        }

        /*
         * Emergency bailout if postmaster has died.  This is to avoid the
         * necessity for manual cleanup of all postmaster children.
         */
        if (!PostmasterIsAlive())
        {
            close(g_fwd_unix_server_fd);
            forwarder_handle_subthread_log(true);
            fwd_stop_server_threads();
            ret = 1;
            break;
        }

        RebuildAgentIndex();
        for (i = 0; i < agentCount; i++)
        {
            int32           index = 0;
            int             sockfd;
            ForwarderAgent  *agent = NULL;

            index  = agentIndexes[i];
            agent  = forwarderAgents[index];

            /* skip the agents in async deconstruct progress */
            sockfd = Socket(agent->port);
            pool_fd[i + 1].fd = sockfd;
        }

        if (shutdown_requested)
        {
            /*
             *  Just close the socket and exit. Linux will help to release the resouces.
             */
            close(g_fwd_unix_server_fd);
            forwarder_handle_subthread_log(true);
            fwd_stop_server_threads();
            break;
        }

        retval = poll(pool_fd, agentCount + 1, timeout_val * 1000);
        if (retval < 0)
        {
            if (errno == EINTR)
                continue;
            fwd_check_thread_id();
            elog(FATAL, FORWARDER_PREFIX"poll returned with error %d", retval);
        }
        else if (retval > 0)
        {
            /*
             * Agent may be removed from the array while processing
             * and trailing items are shifted, so scroll downward
             * to avoid problem
             */
            int32           index;
            ForwarderAgent  *agent;
            int 	        sockfd;
            for (i = agentCount - 1; i >= 0; i--)
            {
                index  = agentIndexes[i];
                agent  = forwarderAgents[index];

                sockfd = Socket(agent->port);
                if ((sockfd == pool_fd[i + 1].fd) && pool_fd[i + 1].revents)
                {
                    agent_handle_input(agent, &input_message);
                }
            }

            if (pool_fd[0].revents & POLLIN)
            {
                int new_fd = accept(g_fwd_unix_server_fd, NULL, NULL);

                if (new_fd < 0)
                {
                    int saved_errno = errno;
                    ereport(LOG,
                            (errcode(ERRCODE_CONNECTION_FAILURE), errmsg(FORWARDER_PREFIX"Forwarder manager failed to accept connection: %m")));
                    errno = saved_errno;
                }
                else
                {
                    agent_create(new_fd);
                }
            }
        }
        /* init socket array if needed */
        fwd_init_sender_socket_array();

        /* reconnet the bad sockets */
        fwd_maintain_socket_array();

        /* maintain server threads */
        fwd_maintain_server_threads();
        
        /* collect rda stat info */
        fwd_add_rda_stat_to_hash();

        /* handle sub thread's log */
        forwarder_handle_subthread_log(false);
    }

    fwd_clean_fwd_resource();
    exit(ret);
}

/* init resource used by the senders and receivers */
static int
fwd_init_thread_resource()
{
    int ret = 0;
    int i   = 0;
    
    /* free by free_thread_resource */
    g_fwd_data_chan_send_map = fwd_malloc(sizeof(void *) * g_FwdWorkerNum);
    if (g_fwd_data_chan_send_map == NULL)
    {
        return -1;
    }
    memset(g_fwd_data_chan_send_map, 0, sizeof(void *) * g_FwdWorkerNum);

    g_fwd_data_chan_send_op_queue = fwd_malloc(sizeof(FWDOpQueue *) * g_FwdWorkerNum);
    if (g_fwd_data_chan_send_op_queue == NULL)
    {
        return -1;
    }
    memset(g_fwd_data_chan_send_op_queue, 0, sizeof(FWDOpQueue *) * g_FwdWorkerNum);

    g_fwd_data_chan_receive_map = fwd_malloc(sizeof(void *) * g_FwdWorkerNum);
    if (g_fwd_data_chan_receive_map == NULL)
    {
        return -1;
    }
    memset(g_fwd_data_chan_receive_map, 0, sizeof(void *) * g_FwdWorkerNum);

    g_fwd_data_chan_receive_op_queue = fwd_malloc(sizeof(FWDOpQueue *) * g_FwdWorkerNum);
    if (g_fwd_data_chan_receive_op_queue == NULL)
    {
        return -1;
    }
    memset(g_fwd_data_chan_receive_op_queue, 0, sizeof(FWDOpQueue *) * g_FwdWorkerNum);

    for (i = 0; i < g_FwdWorkerNum; i++)
    {
        g_fwd_data_chan_send_map[i] = map_create();
        g_fwd_data_chan_send_op_queue[i] = fwd_new_op_queue(FWD_OP_QUEUE_SIZE);
        if (g_fwd_data_chan_send_op_queue[i] == NULL)
        {
            ret = -1;
            break;
        }

        g_fwd_data_chan_receive_map[i] = map_create();
        g_fwd_data_chan_receive_op_queue[i] = fwd_new_op_queue(FWD_OP_QUEUE_SIZE);
        if (g_fwd_data_chan_receive_op_queue[i] == NULL)
        {
            ret = -1;
            break;
        }
    }

    if (ret != 0)
    {
        return ret;
    }

    g_fwd_cleanner_map = map_create();
    g_fwd_cleanner_op_queue = fwd_new_op_queue(FWD_OP_QUEUE_SIZE * g_FwdWorkerNum);
    if (g_fwd_cleanner_op_queue == NULL)
    {
        ret = -1;
    }

    /* Init server sockets management. */
    ret = fwd_server_socket_status_init();
    if (ret != 0)
    {
        return -1;
    }
    return ret;
}

void free_thread_resource()
{
    int i;
    for (i = 0; i < g_FwdWorkerNum; i++)
    {
        if (g_fwd_data_chan_send_map != NULL && g_fwd_data_chan_send_map[i] != NULL)
        {
            map_delete(g_fwd_data_chan_send_map[i]);
        }
        if (g_fwd_data_chan_send_op_queue != NULL && g_fwd_data_chan_send_op_queue[i] != NULL)
        {
            fwd_delete_queue(g_fwd_data_chan_send_op_queue[i]);
        }
        if (g_fwd_data_chan_receive_map != NULL && g_fwd_data_chan_receive_map[i] != NULL)
        {
            map_delete(g_fwd_data_chan_receive_map[i]);
        }
        if (g_fwd_data_chan_receive_op_queue != NULL && g_fwd_data_chan_receive_op_queue[i] != NULL)
        {
            fwd_delete_queue(g_fwd_data_chan_receive_op_queue[i]);
        }
    }

    if (g_fwd_data_chan_send_map)
    {
        fwd_free(g_fwd_data_chan_send_map);
    }
	
    if (g_fwd_data_chan_send_op_queue)
    {
        fwd_free(g_fwd_data_chan_send_op_queue);
    }
	
    if (g_fwd_data_chan_receive_map)
    {
        fwd_free(g_fwd_data_chan_receive_map);
    }
	
    if (g_fwd_data_chan_receive_op_queue)
    {
        fwd_free(g_fwd_data_chan_receive_op_queue);
    }
	
    if (g_fwd_server_rda_slots_group)
    {
        fwd_delete_rda_node_groups();
        g_fwd_dn_rcv_rda_slots_group_ready = false;
    }

    if (g_fwd_cleanner_map)
    {
        map_delete(g_fwd_cleanner_map);
    }
	
    if (g_fwd_cleanner_op_queue)
    {
        fwd_delete_queue(g_fwd_cleanner_op_queue);
    }
}

/* clean up all forwarder resource if there is any. */
static void
fwd_clean_fwd_resource()
{
    fwd_unregister_local_rda();
    fwd_clean_data_thread_main();
    fwd_stop_server_threads();
    fwd_cleanup_socket_array();   
    free_data_fwd_control(g_fwd_data_control);
    free_thread_resource();
    fwd_server_socket_status_destory();
}

int
route_to_node(int node_id)
{
    return node_id % g_FwdWorkerNum;
}

mmap_info *
create_mmap_info(int64 rda_id,
                 void *mmap_addr,
                 uint32_t mmap_size,
                 int src_node_num,
                 int dest_node_num,
                 rda_dsm_frame **receive_frame,
                 rda_dsm_frame **send_frame,
                 data_queue *receive_data_queue,
                 data_queue *send_data_queue)
{
    mmap_info *m_info;

    /* free by delete_mmap_info */
    m_info = (mmap_info *)fwd_malloc(sizeof(mmap_info));
    if (NULL == m_info)
    {
        return NULL;
    }

    m_info->rda_id = rda_id;
    m_info->mmap_addr = mmap_addr;
    m_info->mmap_size = mmap_size;
    m_info->ref_cnt = src_node_num + dest_node_num;
    m_info->src_node_num = src_node_num;
    m_info->dest_node_num = dest_node_num;
    m_info->receive_frame = receive_frame;
    m_info->send_frame = send_frame;
    m_info->receive_data_queue = receive_data_queue;
    m_info->send_data_queue = send_data_queue;

    return m_info;
}
/* drop mmap info. */
void delete_mmap_info(mmap_info *m_info)
{
    int i = 0;
    if (m_info != NULL)
    {
        if (m_info->receive_frame != NULL)
        {
            fwd_free(m_info->receive_frame);
            m_info->receive_frame = NULL;
        }
        if (m_info->send_frame != NULL)
        {
            fwd_free(m_info->send_frame);
            m_info->send_frame = NULL;
        }
		
        if (m_info->receive_data_queue != NULL)
        {
            for (i = 0; i < m_info->src_node_num; i++)
            {
                if (m_info->receive_data_queue[i].controller)
                {
                    fwd_free_rda_controller(m_info->receive_data_queue[i].controller->controller_id);
                    m_info->receive_data_queue[i].controller = NULL;
                }

				/* drop receive buffer queue */
				if (m_info->receive_data_queue[i].buffer_queue)
                {
                    if (g_fwd_dump_pkg_to_file)
                    {
                        if (m_info->receive_data_queue[i].buffer_queue->debug_fp)
                        {
                            fclose(m_info->receive_data_queue[i].buffer_queue->debug_fp);
                            m_info->receive_data_queue[i].buffer_queue->debug_fp = NULL;
                        }
                    }

                    fwd_destory_queue(m_info->receive_data_queue[i].buffer_queue);
                    m_info->receive_data_queue[i].buffer_queue = NULL;
                }

				if (g_fwd_dump_pkg_to_file)
				{
					fclose(m_info->receive_data_queue[i].debug_fp);
                    m_info->receive_data_queue[i].debug_fp = NULL;
				}
            }
            fwd_free(m_info->receive_data_queue);
        }
		
        if (m_info->send_data_queue != NULL)
        {
            for (i = 0; i < m_info->dest_node_num; i++)
            {
				/* drop send buffer queue */
				if (m_info->send_data_queue[i].buffer_queue)
                {
                    if (g_fwd_dump_pkg_to_file)
                    {
                        if (m_info->send_data_queue[i].buffer_queue->debug_fp)
                        {
                            fclose(m_info->send_data_queue[i].buffer_queue->debug_fp);
                            m_info->send_data_queue[i].buffer_queue->debug_fp = NULL;
                        }
                    }
                    fwd_destory_queue(m_info->send_data_queue[i].buffer_queue);
                    m_info->send_data_queue[i].buffer_queue = NULL;
                }

				if (g_fwd_dump_pkg_to_file)
				{
					fclose(m_info->send_data_queue[i].debug_fp);
                    m_info->send_data_queue[i].debug_fp = NULL;
				}
            }
            fwd_free(m_info->send_data_queue);
            m_info->send_data_queue = NULL;
        }
        fwd_free(m_info);
    }
}

/* register_rda service implementation. */
int fwd_register_rda_impl(
                                int64 rda_id,
                                int *src_node_ids,
                                int src_node_num,
                                int *dest_node_ids,
                                int dest_node_num)
{

    bool failed = false;
    int i       = 0;
    int ret     = 0;
    int      thread_index   = 0;
    
    uint32_t  mmap_size = NULL;
    void     *mmap_addr = NULL;

    rda_dsm_frame *frame = NULL;
    rda_dsm_frame **receive_frame = NULL;
    rda_dsm_frame **send_frame = NULL;

    data_queue    *receive_data_queue = NULL;
    data_queue    *send_data_queue = NULL;
    mmap_info     *m_info = NULL;

    fwd_init_sender_socket_array();

    /* validate forwarder status */
    if (g_fwd_data_control->status != FWD_RUNNING)
    {
        return -1;
    }

    if (!dms_rda_attach(rda_id, &mmap_addr, &mmap_size))
    {
        dsm_rda_detach_and_unlink(rda_id, &mmap_addr, &mmap_size);
        return -1;
    }

    /* free by delete_mmap_info */
    receive_frame = fwd_malloc(sizeof(rda_dsm_frame *) * src_node_num);
    if (receive_frame == NULL)
    {
        goto cleanup;
    }
    memset(receive_frame, 0, sizeof(rda_dsm_frame *) * src_node_num);

    send_frame = fwd_malloc(sizeof(rda_dsm_frame *) * dest_node_num);
    if (send_frame == NULL)
    {
        goto cleanup;
    }
    memset(send_frame, 0, sizeof(rda_dsm_frame *) * dest_node_num);

    receive_data_queue = (data_queue *)fwd_malloc(sizeof(data_queue) * src_node_num);
    if (receive_data_queue == NULL)
    {
        goto cleanup;
    }
    memset(receive_data_queue, 0, sizeof(data_queue *) * src_node_num);

    send_data_queue = (data_queue *)fwd_malloc(sizeof(data_queue) * dest_node_num);
    if (send_data_queue == NULL)
    {
        goto cleanup;
    }
    memset(send_data_queue, 0, sizeof(data_queue *) * dest_node_num);

    dsm_map_rda_frames(mmap_addr, mmap_size, src_node_num, dest_node_num,
                       receive_frame, send_frame, false);

    /* init servers queues. */
    failed = false; 
    for (i = 0; i < src_node_num; i++)
    {
        frame = (rda_dsm_frame *)receive_frame[i];
        /* free by cleanner thread */
        receive_data_queue[i].rda_id = rda_id;
        receive_data_queue[i].node_id = src_node_ids[i];
        receive_data_queue[i].frame = frame;
        receive_data_queue[i].ack_bytes = 0;
        receive_data_queue[i].resend_package_done = true;
        receive_data_queue[i].send_offset    = 0;
        receive_data_queue[i].seq            = RDA_INVALID_PKG_SEQ;
        receive_data_queue[i].controller_idx = -1;
        set_rda_status(&receive_data_queue[i].status, RDA_INIT);

        gettimeofday(&receive_data_queue[i].stat_timestamp, NULL);
        receive_data_queue[i].bytes_sent     = 0;
        receive_data_queue[i].bytes_received = 0;
        receive_data_queue[i].bytes_dropped  = 0;
		receive_data_queue[i].bytes_resent   = 0;
		
		receive_data_queue[i].pkgs_sent      = 0;
        receive_data_queue[i].pkgs_received  = 0;
        receive_data_queue[i].pkgs_dropped   = 0;
		receive_data_queue[i].pkgs_resent    = 0;

		receive_data_queue[i].ack_seq	     = RDA_INVALID_PKG_SEQ;
		receive_data_queue[i].buffer_queue   = fwd_create_queue(RDA_BUFFER_RCV_QUEUE_LEN);
		if (NULL == receive_data_queue[i].buffer_queue)
        {
            fwd_check_thread_id();
        	elog(LOG, FORWARDER_PREFIX "create forwarder receiving queue length:%d failed", RDA_BUFFER_RCV_QUEUE_LEN);
            goto cleanup0;
        }

        /* set the buffer queue used by the rda receiver. */
		if (g_fwd_dump_pkg_to_file)
		{
			receive_data_queue[i].buffer_queue->debug_fp = debug_out_file_open(rda_id, src_node_ids[i], OWNER_FORWARDER_FRAME_ENQUEUE);
            if (NULL == receive_data_queue[i].buffer_queue->debug_fp)
            {
                failed = true;
                fwd_check_thread_id();
                elog(LOG, FORWARDER_PREFIX "open debug file of frame failed");
                goto cleanup0;
            }
            fwd_check_thread_id();
            elog(LOG, FORWARDER_PREFIX "receive_data_queue[%d].buffer_queue->debug_fp %p created", i, receive_data_queue[i].buffer_queue->debug_fp);
		}
        
        /* try to get a controller */
        receive_data_queue[i].controller = fwd_alloc_rda_controller(rda_id, src_node_ids[i]);
        if (NULL == receive_data_queue[i].controller)
        {
            failed = true;
            fwd_check_thread_id();
            elog(LOG, FORWARDER_PREFIX "not enough rda controller");
            goto cleanup0;
        }
        fwd_check_thread_id();
        elog(LOG, FORWARDER_PREFIX "receive node %d rda_id %ld get rda_controller %d", my_node_id, rda_id, receive_data_queue[i].controller->controller_id);
        receive_data_queue[i].controller->data_queue = &receive_data_queue[i];
        receive_data_queue[i].controller_idx = receive_data_queue[i].controller->controller_id;
        
		
		/* set the buffer queue used by the rda receiver. */
		if (g_fwd_dump_pkg_to_file)
		{
			receive_data_queue[i].debug_fp = debug_out_file_open(rda_id, src_node_ids[i], OWNER_FORWARDER_NET_READ);
            if (NULL == receive_data_queue[i].debug_fp)
            {
                failed = true;
                fwd_check_thread_id();
                elog(LOG, FORWARDER_PREFIX "open debug file failed");
                goto cleanup0;
            }
            fwd_check_thread_id();
            elog(LOG, FORWARDER_PREFIX "receive_data_queue[%d].debug_fp %p created", i, receive_data_queue[i].debug_fp);
		}
    }

    /* init clients queues. */
    for (i = 0; i < dest_node_num; i++)
    {
        frame = (rda_dsm_frame *)send_frame[i];
        /* free by cleanner thread */
        send_data_queue[i].rda_id = rda_id;
        send_data_queue[i].node_id = dest_node_ids[i];
        send_data_queue[i].frame = frame;
        send_data_queue[i].resend_package_done = true;
        send_data_queue[i].ack_bytes = 0;
        send_data_queue[i].send_offset = 0;
        send_data_queue[i].seq = RDA_FIRST_PKG_SEQ;
        send_data_queue[i].controller_idx = -1;
        send_data_queue[i].controller = NULL;
        set_rda_status(&send_data_queue[i].status, RDA_INIT);
        send_data_queue[i].bytes_sent     = 0;
        send_data_queue[i].bytes_received = 0;
        send_data_queue[i].bytes_dropped  = 0;
		send_data_queue[i].bytes_resent   = 0;

        gettimeofday(&send_data_queue[i].stat_timestamp, NULL);
		send_data_queue[i].pkgs_sent     = 0;
        send_data_queue[i].pkgs_received = 0;
        send_data_queue[i].pkgs_dropped  = 0;
		send_data_queue[i].pkgs_resent   = 0;

		send_data_queue[i].ack_seq	     = RDA_INVALID_PKG_SEQ;
		send_data_queue[i].buffer_queue  = fwd_create_queue(RDA_BUFFER_SND_QUEUE_LEN);
		if (NULL == send_data_queue[i].buffer_queue)
        {
            fwd_check_thread_id();
        	elog(LOG, FORWARDER_PREFIX "create forwarder sending queue length:%d failed", RDA_BUFFER_RCV_QUEUE_LEN);
            goto cleanup0;
        }

		if (g_fwd_dump_pkg_to_file)
		{
            send_data_queue[i].debug_fp = debug_out_file_open(rda_id, dest_node_ids[i], OWNER_FORWARDER_FRAME_DEQUEUE);
            if (NULL == send_data_queue[i].debug_fp)
            {
                fwd_check_thread_id();
                elog(LOG, FORWARDER_PREFIX "open debug file failed");
                goto cleanup0;
            }
            fwd_check_thread_id();
            elog(LOG, FORWARDER_PREFIX "send_data_queue[%d].debug_fp %p created", i, send_data_queue[i].debug_fp);
		}
    }


    /* register in the cleanner map. */
    m_info = create_mmap_info(rda_id, mmap_addr, mmap_size, src_node_num, dest_node_num,
                              receive_frame, send_frame, receive_data_queue, send_data_queue);
    if (NULL == m_info)
    {
        fwd_check_thread_id();
        elog(LOG, FORWARDER_PREFIX "create mmap failed");
        goto cleanup0;
    }

    /* here we create the mmap file */
    ret = fwd_push_queue(g_fwd_cleanner_op_queue, FWD_OP_CREATE, rda_id, FWD_NODE_ID_ALL, (void *)m_info);
    if (ret < 0)
    {
        delete_mmap_info(m_info);
        dsm_rda_detach_and_unlink(rda_id, &mmap_addr, &mmap_size);
        fwd_check_thread_id();
        elog(LOG, FORWARDER_PREFIX "fwd_push_queue of create mmap info failed");
        goto cleanup0;
    }

    /* register in the receiver map */
    failed  = false;
    for (i = 0; i < src_node_num; i++)
    {
        /* calculate group id*/
        thread_index = route_to_node(receive_data_queue[i].node_id);
        /* push request to receiver queue */
        if (fwd_push_queue(g_fwd_data_chan_receive_op_queue[thread_index], FWD_OP_CREATE, rda_id, receive_data_queue[i].node_id, (void *)&receive_data_queue[i]) < 0)
        {
            failed = true;
            fwd_check_thread_id();
            elog(LOG, FORWARDER_PREFIX "data receive queue creating src_node_ids request fwd_push_queue failed");
            break;
        }
    }
    /*
     * for those op request which has been pushed to queue successfully, try to create drop request first.
     * if still failed, then they will be added to receiver map, and then expire and removed from map.
     * finally all will be deleted by the cleanner.
     */
    if (failed)
    {
        while(--i >= 0)
        {

            thread_index = route_to_node(receive_data_queue[i].node_id);
            if (fwd_push_queue(g_fwd_data_chan_receive_op_queue[thread_index], FWD_OP_DROP, rda_id, receive_data_queue[i].node_id, NULL) < 0)
            {
                fwd_check_thread_id();
                elog(LOG, FORWARDER_PREFIX "data receive queue rollback creating request fwd_push_queue failed");
                break;
            }
        }
        goto cleanup0;
    }

    /* register in the sender map */
    failed  = false;
    for (i = 0; i < dest_node_num; i++)
    {
        /* calculate group id*/
        thread_index = route_to_node(send_data_queue[i].node_id);
        /* push request to sender queue */
        if (fwd_push_queue(g_fwd_data_chan_send_op_queue[thread_index], FWD_OP_CREATE, rda_id, send_data_queue[i].node_id, (void *)&send_data_queue[i]) < 0)
        {
            failed = true;
            fwd_check_thread_id();
            elog(LOG, FORWARDER_PREFIX "data sender queue creating request dest_node_ids fwd_push_queue failed");
            break;
        }
    }

    /* clean up on failure */
    if (failed)
    {
        while (--i >= 0)
        {
            thread_index = route_to_node(send_data_queue[i].node_id);
            if (fwd_push_queue(g_fwd_data_chan_send_op_queue[thread_index], FWD_OP_DROP, rda_id, send_data_queue[i].node_id, NULL) < 0)
            {
                fwd_check_thread_id();
                elog(LOG, FORWARDER_PREFIX "data sender queue rollback creating request dest_node_ids fwd_push_queue failed");
                break;
            }
        }

        for (i = 0; i < src_node_num; i++)
        {
            /* calculate group id*/
            thread_index = route_to_node(receive_data_queue[i].node_id);
            /* push request to receiver queue */
            if (fwd_push_queue(g_fwd_data_chan_receive_op_queue[thread_index], FWD_OP_DROP, rda_id, receive_data_queue[i].node_id, NULL) < 0)
            {
                fwd_check_thread_id();
                elog(LOG, FORWARDER_PREFIX "data sender queue rollback creating request src_node_ids fwd_push_queue failed");
                break;
            }
        }
        goto cleanup0;
    }

    fwd_add_rda_to_stat_hash(rda_id);
    return 0;

cleanup0:
    for (i = 0; i < dest_node_num; i++)
    {
        if (send_data_queue[i].buffer_queue)
        {
            if (send_data_queue[i].buffer_queue->debug_fp)
            {
                fclose(send_data_queue[i].debug_fp);
            }

            fwd_destory_queue(send_data_queue[i].buffer_queue);            
        }

        if (g_fwd_dump_pkg_to_file)
        {
            fclose(send_data_queue[i].debug_fp);
        }
    }

    for (i = 0; i < src_node_num; i++)
    {
        if (receive_data_queue[i].buffer_queue)
		{
            if (receive_data_queue[i].buffer_queue->debug_fp)
            {
                fclose(receive_data_queue[i].debug_fp);
            }
			fwd_destory_queue(receive_data_queue[i].buffer_queue);
		}

        if (receive_data_queue[i].controller)
        {
            fwd_free_rda_controller(receive_data_queue[i].controller->controller_id);
            if (g_fwd_dump_pkg_to_file)
            {
                fclose(receive_data_queue[i].debug_fp);
            }
        }
    }

cleanup:
    if (receive_frame != NULL)
    {
        fwd_free(receive_frame);
    }
    if (send_frame != NULL)
    {
        fwd_free(send_frame);
    }
    if (receive_data_queue != NULL)
    {
        fwd_free(receive_data_queue);
    }
    if (send_data_queue != NULL)
    {
        fwd_free(send_data_queue);
    }
    return -1;
}
/* send data to node_id and rda_id */
static bool fwd_local_rda_send(int64 rda_id, int32 node_id, uint8_t *data, int len)
{
    bool  succeed     = false;
    int   status      = 0;
    int   frame_index = 0;
    FILE *file        = NULL;
    FWDLocalRDAFrameEntry *dst_frame = NULL;
    if (rda_id < FWD_MAX_RESERVED_RDA_ID && node_id != my_node_id)
    {
        if (node_id >= FWD_CN_NODEID_BASE)
        {
            if (g_fwd_cn_num > 0)
            {
                frame_index = node_id - FWD_CN_NODEID_BASE + g_fwd_dn_num;/* dn first, then cns */
            }
            else
            {
                forwarder_thread_logger(LOG, FORWARDER_PREFIX "thread %ld, fwd_local_rda_send failed for non coordinator, rda_id %ld, src_node_id %d", pthread_self(), rda_id, node_id);
                return false;
            }
        }
        else
        {
            frame_index = node_id;/* dn first, then cns */
        }

        dst_frame = &g_fwd_local_rda_combiner_array[rda_id].out_frame_entries[frame_index];

        status = rda_get_frame_status(dst_frame->frame);
        if (RDA_FRAME_STATUS_OK == status)
        {
            /* enqueue data, use tuple type 'c' */
            succeed   = rda_frame_enqueue_data(&file, dst_frame->seq_num, dst_frame->frame, 'c', len, my_node_id, data);
            dst_frame->seq_num++;
            dst_frame->io_bytes += len;
            return succeed;
        }
        else
        {
            rda_reset_frame(dst_frame->frame);
            forwarder_thread_logger(LOG, FORWARDER_PREFIX "thread %ld, fwd_local_rda_send status invalid for rda_id %ld, src_node_id %d, status %d", pthread_self(), rda_id, node_id, status);
            return false;
        }
    }
    forwarder_thread_logger(LOG, FORWARDER_PREFIX "thread %ld, fwd_local_rda_send failed for too big rda_id %ld, src_node_id %d", pthread_self(), rda_id, node_id);
    return false;
}

/* recceivce data from rda_id */
static rda_dsm_tup_datarow* fwd_local_rda_receive(int64 rda_id)
{
    bool  succeed      = false;
    int   status       = 0;
    int   frame_index  = 0;
    int   total_number = 0;
    int   total_index  = 0; 
    FWDLocalRDAFrameEntry *src_frame = NULL;
    rda_dsm_tup_datarow   *result    = NULL;
    
    if (rda_id < FWD_MAX_RESERVED_RDA_ID)
    {
        total_number = g_fwd_local_rda_combiner_array[rda_id].dn_frame_num + g_fwd_local_rda_combiner_array[rda_id].cn_frame_num;
        total_index  = total_number +  g_fwd_local_rda_combiner_array[rda_id].current_index;
        for (frame_index = g_fwd_local_rda_combiner_array[rda_id].current_index; frame_index < total_index; frame_index++)
        {
            src_frame = &g_fwd_local_rda_combiner_array[rda_id].in_frame_entries[g_fwd_local_rda_combiner_array[rda_id].current_index];

            status = rda_get_frame_status(src_frame->frame);
            if (RDA_FRAME_STATUS_OK == status)
            {
                /* receive data */
                succeed = recevice_slot_from_fwd_local_frame(src_frame);
                if (succeed)
                {
                    result = src_frame->recv_datarow;
                    src_frame->recv_datarow = NULL;
                    g_fwd_local_rda_combiner_array[rda_id].current_index = (g_fwd_local_rda_combiner_array[rda_id].current_index + 1) % total_number;
                    return result;
                }
                else
                {
                    if (g_fwd_dump_pkg_to_file)
                    {
                        forwarder_thread_logger(LOG, FORWARDER_PREFIX "thread %ld, fwd_local_rda_receive failed, rda_id %ld", pthread_self(), rda_id);
                    }
                    g_fwd_local_rda_combiner_array[rda_id].current_index = (g_fwd_local_rda_combiner_array[rda_id].current_index + 1) % total_number;
                }
            }
            else
            {
                rda_reset_frame(src_frame->frame);
                forwarder_thread_logger(LOG, FORWARDER_PREFIX "thread %ld, fwd_local_rda_receive status invalid for rda_id %ld, frame_indexs %d, status %d", pthread_self(), rda_id, frame_index, status);
                g_fwd_local_rda_combiner_array[rda_id].current_index = (g_fwd_local_rda_combiner_array[rda_id].current_index + 1) % total_number;
                
            }
        }
    }
    return NULL;
}

/* register local rda, build local frames. */
static void fwd_register_local_rda(void)
{
    int64     rda_id = 0;
    int       index  = 0;
    int       ret    = 0;
    int       request_size   = 0;
    uint32_t  rda_shmem_size = 0;
    int      *node_ids       = NULL;
    void     *rda_shmem_ptr  = NULL;
    g_fwd_local_rda_combiner_array = palloc0(sizeof(FWDLocalCombiner) * FWD_MAX_RESERVED_RDA_ID);
    if (NULL == g_fwd_local_rda_combiner_array)
    {
        fwd_check_thread_id();
        elog(ERROR, FORWARDER_PREFIX "fwd_register_local_rda failed, out of memory");
    }

    for (rda_id = FWD_DDS_RDA_ID; rda_id < FWD_MAX_RESERVED_RDA_ID; rda_id++)
    {
        g_fwd_local_rda_combiner_array[rda_id].dn_frame_num = g_fwd_dn_num;
        g_fwd_local_rda_combiner_array[rda_id].cn_frame_num = g_fwd_cn_num;
        
        /* init rda frames */
        request_size = RDA_FRAME_SIZE * (RDA_STAT_ELEMENT_NUM);

        /* create shared-memory and map it */
        ret = dms_rda_create_and_attach(rda_id, 
                                        request_size,
                                        &rda_shmem_ptr, 
                                        &rda_shmem_size);
        if (!ret)
        {
            ereport(ERROR,
                    (errcode(ERRCODE_OUT_OF_MEMORY),
                        errmsg("Failed to create shared-memory for RDA: %ld, out of memory", rda_id)));
        }
        /* get mmaped frames */
        g_fwd_local_rda_combiner_array[rda_id].in_frames = (rda_dsm_frame **)palloc0(g_total_node_num * sizeof(rda_dsm_frame *));
        if (NULL ==  g_fwd_local_rda_combiner_array[rda_id].in_frames)
        {
            fwd_check_thread_id();
            elog(ERROR, FORWARDER_PREFIX "fwd_register_local_rda failed, out of memory");
        }

        g_fwd_local_rda_combiner_array[rda_id].out_frames = (rda_dsm_frame **)palloc0(g_total_node_num * sizeof(rda_dsm_frame *));
        if (NULL == g_fwd_local_rda_combiner_array[rda_id].out_frames)
        {
            fwd_check_thread_id();
            elog(ERROR, FORWARDER_PREFIX "fwd_register_local_rda failed, out of memory");
        }

        dsm_map_rda_frames(rda_shmem_ptr, rda_shmem_size,
                        g_total_node_num, g_total_node_num,
                        g_fwd_local_rda_combiner_array[rda_id].in_frames,
                        g_fwd_local_rda_combiner_array[rda_id].out_frames, true);

        /* get mmaped frames */
        g_fwd_local_rda_combiner_array[rda_id].in_frame_entries = (FWDLocalRDAFrameEntry *)palloc0(g_total_node_num * sizeof(FWDLocalRDAFrameEntry));
        if (NULL ==  g_fwd_local_rda_combiner_array[rda_id].in_frames)
        {
            fwd_check_thread_id();
            elog(ERROR, FORWARDER_PREFIX "fwd_register_local_rda failed, out of memory");
        }

        g_fwd_local_rda_combiner_array[rda_id].out_frame_entries = (FWDLocalRDAFrameEntry *)palloc0(g_total_node_num * sizeof(FWDLocalRDAFrameEntry));
        if (NULL == g_fwd_local_rda_combiner_array[rda_id].out_frames)
        {
            fwd_check_thread_id();
            elog(ERROR, FORWARDER_PREFIX "fwd_register_local_rda failed, out of memory");
        }

        /* fill frame entries */
        for (index = 0; index < g_total_node_num; index++)
        {
            g_fwd_local_rda_combiner_array[rda_id].in_frame_entries[index].frame = g_fwd_local_rda_combiner_array[rda_id].in_frames[index];
            g_fwd_local_rda_combiner_array[rda_id].out_frame_entries[index].frame = g_fwd_local_rda_combiner_array[rda_id].out_frames[index];
        }
        
        /* register rda */
        node_ids = (int*)palloc0(sizeof(int) * g_total_node_num);
        if (NULL == node_ids)
        {
            fwd_check_thread_id();
            elog(ERROR, FORWARDER_PREFIX "fwd_register_local_rda failed, out of memory");
        }

        /* fill node index, cn start at FWD_CN_NODEID_BASE*/
        for (index = 0; index < g_total_node_num; index++)
        {
            if (index >= g_fwd_dn_num)
            {
                node_ids[index] = index + FWD_CN_NODEID_BASE - g_fwd_dn_num;   
            }
            else
            {
                node_ids[index] = index;    
            }
        }

        ret = fwd_register_rda_impl(rda_id, node_ids, g_total_node_num, node_ids, g_total_node_num);
        if (ret)
        {
            fwd_check_thread_id();
            elog(ERROR, FORWARDER_PREFIX "fwd_register_local_rda fwd_register_rda_impl failed");
        }
        pfree(node_ids);
    }
    g_fwd_local_rda_combiner_complete = true;
}
/* unregister all local rdas */
static void fwd_unregister_local_rda(void)
{
    int     rda_id = 0;
    int     index  = 0;
    int     ret    = 0;
    int    *node_ids = NULL;
   
    for (rda_id = FWD_DDS_RDA_ID; rda_id < FWD_MAX_RESERVED_RDA_ID; rda_id++)
    {
        /* register rda */
        node_ids = (int*)palloc0(sizeof(int) * g_total_node_num);
        if (NULL == node_ids)
        {
            continue;
        }

         /* fill node index, cn start at FWD_CN_NODEID_BASE*/
        for (index = 0; index < g_total_node_num; index++)
        {
            if (index >= g_fwd_dn_num)
            {
                node_ids[index] = index + FWD_CN_NODEID_BASE - g_fwd_dn_num;   
            }
            else
            {
                node_ids[index] = index;    
            }
        }
        
        ret = fwd_unregister_rda_impl(rda_id, node_ids, g_total_node_num, node_ids, g_total_node_num);
        if (ret)
        {
            fwd_check_thread_id();
            elog(ERROR, FORWARDER_PREFIX "fwd_unregister_local_rda fwd_unregister_rda_impl failed");
        }

        pfree(g_fwd_local_rda_combiner_array[rda_id].in_frames);
        pfree(g_fwd_local_rda_combiner_array[rda_id].out_frames);
        pfree(g_fwd_local_rda_combiner_array[rda_id].in_frame_entries);
        pfree(g_fwd_local_rda_combiner_array[rda_id].out_frame_entries);
        pfree(node_ids);
    }
    pfree(g_fwd_local_rda_combiner_array);
    g_fwd_local_rda_combiner_array = NULL;
    g_fwd_local_rda_combiner_complete = false;
    /* wait thread to handle */
    sleep(5);
}

/* unregister_rda service implementation. */
int fwd_unregister_rda_impl(
                            int64 rda_id,
                            int *src_node_ids,
                            int src_node_num,
                            int *dest_node_ids,
                            int dest_node_num)
{
    int ret      = 0;
    int i        = 0;
    int group_id = 0;

    /* validate forwarder status */
    if (g_fwd_data_control->status != FWD_RUNNING)
    {
        return -1;
    }

    for (i = 0; i < src_node_num; i++)
    {
        /* calculate the group */
        group_id = route_to_node(src_node_ids[i]);

        /* push request to queue */
        ret = fwd_push_queue(g_fwd_data_chan_receive_op_queue[group_id], FWD_OP_DROP, rda_id, src_node_ids[i], NULL);
        if (ret)
        {
            fwd_check_thread_id();
            elog(LOG, FORWARDER_PREFIX "fwd_unregister_rda_impl fwd_push_queue for rda_id:%ld failed", rda_id);
            break;
        }
    }

    for (i = 0; i < dest_node_num; i++)
    {
        /* calculate the group */
        group_id = route_to_node(dest_node_ids[i]);

        /* push request to queue */
        ret = fwd_push_queue(g_fwd_data_chan_send_op_queue[group_id], FWD_OP_DROP, rda_id, dest_node_ids[i], NULL);
        if (ret)
        {
            fwd_check_thread_id();
            elog(LOG, FORWARDER_PREFIX "fwd_unregister_rda_impl fwd_push_queue for rda_id:%ld failed", rda_id);
            break;
        }
    }

    fwd_remove_rda_from_stat_hash(rda_id);
    return 0;
}

/* push data service implementation. */
ResponsePackage *rda_server_pushdata_callback(FWDPackage *request)
{
    int ret             = 0;
    int src_node_id     = 0;
    uint64            pkg_seq        = 0;
    uint64            latest_seq     = 0;
	FWDQueue         *fwd_q          = NULL;
    RDAController    *rda_controller = NULL;
    ResponsePackage  *response     = NULL;
    char cal_md5[STR_MD5_LEN];

    /* validate forwarder status */
    if (g_fwd_data_control->status != FWD_RUNNING)
    {
        forwarder_thread_logger(LOG,
	    						FORWARDER_PREFIX "thread %ld, receiver rda_server_pushdata_callback exit for not in FWD_RUNNING status, rda_id %ld, src_node_id %d, controller_idx %d, data len %d, data offset %ld, seq %ld",
	                            pthread_self(), request->rda_id, request->node_id, request->controller_idx, request->pkg_len, request->pkg_offset, request->pkg_seq);
        fwd_drop_package(request);
        return NULL;
    }
	
    
	if (g_fwd_dump_pkg_to_file)
	{
		forwarder_thread_logger(LOG,
	    						FORWARDER_PREFIX "thread %ld, receiver rda_server_pushdata_callback enter, rda_id %ld, src_node_id %d, controller_idx %d, data len %d, data offset %ld, seq %ld",
	                            pthread_self(), request->rda_id, request->node_id, request->controller_idx, request->pkg_len, request->pkg_offset, request->pkg_seq);
	}

    /* locate controller */
    rda_controller = get_rda_controller_by_id(request->controller_idx);
    if (rda_controller == NULL)
    {
    	forwarder_thread_logger(LOG,
	    						FORWARDER_PREFIX "receiver rda_server_pushdata_callback get_rda_controller_by_id failed, thread:%ld, rda_id %ld, src_node_id %d, controller_idx %d, data len %d, seq %ld ",
	                            pthread_self(), request->rda_id, request->node_id, request->controller_idx, request->pkg_len, request->pkg_seq);
        fwd_drop_package(request);
        return NULL;
    }

    fwd_q = (FWDQueue *)rda_controller->data_queue->buffer_queue;
    if (g_fwd_dump_pkg_to_file)
    {
        forwarder_thread_logger(LOG,
                            FORWARDER_PREFIX "receiver rda_server_pushdata_callback get rda_control, thread:%ld, rda_id %ld, src_node_id %d, controller_idx %d, data len %d, data offset %ld, seq %ld "
                                             "queue group %d, control idx %d, buf_queue %p",
                            pthread_self(), request->rda_id, request->node_id, request->controller_idx, request->pkg_len, request->pkg_offset, request->pkg_seq,
                            rda_controller->group_id, rda_controller->controller_id, fwd_q);
    }

#ifdef __ENABLE_RDA_MD5__
	if (g_fwd_enable_fwd_md5)
	{
	    /* validate md5sum */
	    memset(cal_md5, 0, STR_MD5_LEN);
	    if (!pg_md5_hash(request->data, request->pkg_len - sizeof(FWDPackage), cal_md5))
	    {
	        fwd_drop_package(request);
	        return NULL;
	    }

        if (strcmp(request->md5, cal_md5) != 0)
	    {
	        fwd_drop_package(request);
			abort();
	        return NULL;
	    }
	}
#endif
    /* update stat counter */
    rda_controller->data_queue->pkgs_received++;
    rda_controller->data_queue->bytes_received += request->pkg_len;   
    
    /* drop package not in sequence */
    src_node_id = request->node_id;
    pkg_seq     = request->pkg_seq;
    fwd_queue_get_last_seq(fwd_q, &latest_seq);
    if (latest_seq + 1 != request->pkg_seq)
    {
		rda_controller->data_queue->bytes_dropped += request->pkg_len;
		rda_controller->data_queue->pkgs_dropped++;

        forwarder_thread_logger(LOG,
                                FORWARDER_PREFIX "receiver rda_server_pushdata_callback package dropped for sequence out of order, thread:%ld, rda_id %ld, src_node_id %d, controller_idx %d, data len %d, data offset %ld, need seq %ld, pkg seq %ld "
                                                 "control idx %d", pthread_self(), request->rda_id, request->node_id, request->controller_idx, request->pkg_len, request->pkg_offset, latest_seq + 1, request->pkg_seq,
                                                 rda_controller->controller_id);
        fwd_drop_package(request);
    }
    else
    {
        ret = fwd_queue_push_pkg(fwd_q, request, FWD_RDA_RECEIVER);
        if (ret != 0)
        {	
            /* drop the package if push failed. */
            rda_controller->data_queue->bytes_dropped += request->pkg_len;
            rda_controller->data_queue->pkgs_dropped++;
            

            forwarder_thread_logger(LOG, FORWARDER_PREFIX "receiver rda_server_pushdata_callback fwd_queue_push_pkg failed, package dropped, thread:%ld, rda_id %ld, src_node_id %d, controller_idx %d, data len %d, data offset %ld, pkg_seq %ld "
                                                          "queue group %d, control idx %d, queue_len %d", pthread_self(), request->rda_id, request->node_id, request->controller_idx, request->pkg_len, request->pkg_offset, request->pkg_seq,
                                                          rda_controller->group_id, rda_controller->controller_id, fwd_queue_free_length(fwd_q));
            fwd_drop_package(request);
        }
        else
        {
            /* dump data if needed */
            if (g_fwd_dump_pkg_to_file)
            {
                debug_out_file_write_buffer(rda_controller->data_queue->debug_fp, PayloadLen(request), request->data);
            
                forwarder_thread_logger(LOG,
                                        FORWARDER_PREFIX "thread %ld, receiver rda_server_pushdata_callback fwd_queue_push_pkg succeed, rda_id %ld, src_node_id %d, controller_idx %d, seq %ld, queue_len %d",
                                        pthread_self(), rda_controller->rda_id, src_node_id, rda_controller->controller_id, pkg_seq, fwd_queue_free_length(fwd_q));
            }
        }
    }

    response = (ResponsePackage*)fwd_malloc(sizeof(ResponsePackage));
    if (response)
    {
        /* fill the header */
        response->header.protocol        = RDA_PUSH_DATA_ACK;
        response->header.pkg_len         = sizeof(ResponsePackage);
        response->header.rda_id          = rda_controller->rda_id;
        response->header.node_id         = my_node_id;
        response->header.pkg_seq         = pkg_seq;
        response->header.controller_idx  = rda_controller->controller_id;
        response->header.pkg_left_len    = 0;
        response->header.pkg_offset      = 0;
        response->header.pending_1       = 0;
        response->header.pending_2       = 0;

        set_pkg_version((FWDPackage*)response);

        /* fill response package */
        fwd_queue_get_last_seq(fwd_q, &response->payload.datarsp.ack_seq);
        response->payload.datarsp.free_space      = fwd_queue_free_length(fwd_q);
        response->payload.datarsp.next_send       = response->payload.datarsp.ack_seq + 1;

        if (g_fwd_dump_pkg_to_file)
        {
            forwarder_thread_logger(LOG, FORWARDER_PREFIX "receiver rda_server_pushdata_callback fwd_queue_push_pkg return package succeed, thread:%ld, rda_id %ld, src_node_id %d, controller_idx %d, pkg_seq %ld , ack_seq %ld, free_space %d, next_send %ld, queue_len %d", 
                                    pthread_self(), response->header.rda_id, 
                                    src_node_id, response->header.controller_idx, pkg_seq, 
                                    response->payload.datarsp.ack_seq, response->payload.datarsp.free_space, 
                                    response->payload.datarsp.next_send, fwd_queue_free_length(fwd_q));
        }

        rda_controller->data_queue->pkgs_sent++;
        rda_controller->data_queue->bytes_sent += response->header.pkg_len;   
        return response;
    }
    else
    {
        forwarder_thread_logger(LOG, FORWARDER_PREFIX "receiver rda_server_pushdata_callback make ResponsePackage failed, thread:%ld, rda_id %ld, src_node_id %d, controller_idx %d, seq %ld, queue_len %d",
                                pthread_self(), rda_controller->rda_id, src_node_id, rda_controller->controller_id, pkg_seq, fwd_queue_free_length(fwd_q));
        return NULL;
    }
}

/* get rda status service implementation. */
ResponsePackage* rda_server_init_status_callback(FWDPackage *request)
{
    RDAController    *rda_ctl        = NULL;
    ResponsePackage  *response       = NULL;
    /* validate forwarder status */
    if (g_fwd_data_control->status != FWD_RUNNING)
    {
        fwd_drop_package(request);
        return NULL;
    }

    rda_ctl = get_rda_controller_by_rda_id(request->rda_id, request->node_id);
    if (NULL == rda_ctl)
    {
        forwarder_thread_logger(LOG,
	    						FORWARDER_PREFIX "receiver rda_server_init_status_callback get_rda_controller_by_id failed, thread:%ld, rda_id %ld, src_node_id %d, controller_idx %d, data len %d, seq %ld ",
	                            pthread_self(), request->rda_id, request->node_id, request->controller_idx, request->pkg_len, request->pkg_seq);
        fwd_drop_package(request);
        return NULL;
    }

    response = (ResponsePackage*)fwd_malloc(sizeof(ResponsePackage));
    if (response)
    {
        /* file the header */
        response->header.protocol = RDA_INIT_STATUS_ACK;
        response->header.pkg_len  = sizeof(ResponsePackage);
        response->header.rda_id   = request->rda_id;
        response->header.node_id  = my_node_id;
        response->header.pkg_seq  = request->pkg_seq;
        response->header.controller_idx  = rda_ctl->controller_id;
        response->header.pkg_left_len    = 0;
        response->header.pkg_offset      = 0;
        response->header.pending_1       = 0;
        response->header.pending_2       = 0;

        set_pkg_version((FWDPackage*)response);
        rda_ctl->data_queue->pkgs_sent++;
        rda_ctl->data_queue->bytes_sent += response->header.pkg_len;
    }

    if (g_fwd_dump_pkg_to_file)
    {
        forwarder_thread_logger(LOG,
                            FORWARDER_PREFIX "receiver rda_server_init_status_callback succeed, thread:%ld, rda_id %ld, src_node_id %d, controller_idx %d, data len %d, seq %ld ",
                            pthread_self(), request->rda_id, request->node_id, rda_ctl->controller_id, request->pkg_len, request->pkg_seq);
    }
    
    fwd_drop_package(request);
    return response;
}

/* get receiver service implementation. */
ResponsePackage* rda_server_receiver_status_callback(FWDPackage *request)
{
    RDAController    *controller;
    ResponsePackage  *response       = NULL;

    /* validate forwarder status */
    if (g_fwd_data_control->status != FWD_RUNNING)
    {
        forwarder_thread_logger(LOG,
	    						FORWARDER_PREFIX "receiver rda_server_receiver_status_callback failed for not in FWD_RUNNING, thread:%ld, rda_id %ld, src_node_id %d, controller_idx %d, data len %d, seq %ld ",
	                            pthread_self(), request->rda_id, request->node_id, request->controller_idx, request->pkg_len, request->pkg_seq);
        fwd_drop_package(request);
        return NULL;
    }

    /* locate rda controller */
    controller = get_rda_controller_by_id(request->controller_idx);
    if (NULL == controller)
    {
        forwarder_thread_logger(LOG,
	    						FORWARDER_PREFIX "receiver rda_server_receiver_status_callback get_rda_controller_by_id failed, thread:%ld, rda_id %ld, src_node_id %d, controller_idx %d, data len %d, seq %ld ",
	                            pthread_self(), request->rda_id, request->node_id, request->controller_idx, request->pkg_len, request->pkg_seq);
        fwd_drop_package(request);
        return NULL;
    }

    response = (ResponsePackage*)fwd_malloc(sizeof(ResponsePackage));
    if (response)
    {
        /* file the header */
        response->header.protocol = RDA_SERVER_STATUS_ACK;
        response->header.pkg_len  = sizeof(ResponsePackage);
        response->header.rda_id   = request->rda_id;
        response->header.node_id  = my_node_id;
        response->header.pkg_seq  = request->pkg_seq;
        response->header.controller_idx  = controller->controller_id;
        response->header.pkg_left_len    = 0;
        response->header.pkg_offset      = 0;
        response->header.pending_1       = 0;
        response->header.pending_2       = 0;
        set_pkg_version((FWDPackage*)response);
        controller->data_queue->pkgs_sent++;
        controller->data_queue->bytes_sent += response->header.pkg_len;
    }

    fwd_queue_get_last_seq(controller->data_queue->buffer_queue, &response->payload.rcvstatusrsp.ack_seq);
    response->payload.rcvstatusrsp.free_space = fwd_queue_free_length(controller->data_queue->buffer_queue);
    response->payload.rcvstatusrsp.next_send  = response->payload.rcvstatusrsp.ack_seq + 1;

    if (g_fwd_dump_pkg_to_file)
    {
        forwarder_thread_logger(LOG,
							FORWARDER_PREFIX "receiver rda_server_receiver_status_callback, thread:%ld, rda_id %ld, src_node_id %d, controller_idx %d, free_space %d, next_send %ld, ack_seq %ld",
	                        pthread_self(), response->header.rda_id, response->header.node_id, response->header.controller_idx, 
                            response->payload.rcvstatusrsp.free_space, 
                            response->payload.rcvstatusrsp.next_send, 
                            response->payload.rcvstatusrsp.ack_seq);
    }
    fwd_drop_package(request);
    return response;
}


bool
get_reload_status_impl()
{
    int ret = 0;

    /* Init sender, receiver and cleanner resource*/
    ret = fwd_init_thread_resource();
    if (ret)
    {
        fwd_clean_fwd_resource();
        fwd_check_thread_id();
        elog(LOG, FORWARDER_PREFIX "fwd_clean_fwd_resource failed");
        return false;
    }
    fwd_check_thread_id();
    elog(LOG, FORWARDER_PREFIX "worker thread resource initiated");

    /* Init thread control */
    g_fwd_data_control = init_data_fwd_control();
    if (NULL == g_fwd_data_control)
    {
        fwd_clean_fwd_resource();
        fwd_check_thread_id();
        elog(LOG, FORWARDER_PREFIX "init_data_fwd_control failed");
        return false;
    }
    fwd_check_thread_id();
    elog(LOG, FORWARDER_PREFIX "worker thread control initiated");

    /* start server thread after we have prepared FWDListenSocket*/
    ret = fwd_init_server_threads();
    if (ret != 0)
    {
        ereport(FATAL,
                    (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("start server thread failed")));
    }

    /* Start threads */
    fwd_create_service_thread();
    fwd_check_thread_id();
    elog(LOG, FORWARDER_PREFIX "worker thread started");

    fwd_init_sender_socket_array();
    return true;
}

void ForwarderCleanupRDAStore(void)
{
	DIR		   *dir;
	struct dirent *dent;

	/* Open the directory; can't use AllocateDir in postmaster. */
	if ((dir = AllocateDir(PG_RDA_DIR)) == NULL)
	{
		ereport(LOG,
				(errcode_for_file_access(),
				 errmsg("could not open directory \"%s\": %m",
						PG_RDA_DIR)));
		return;
	}
	
	/* empty the dir. */
	while ((dent = ReadDir(dir, PG_RDA_DIR)) != NULL)
	{
		char		buf[MAXPGPATH + sizeof(PG_RDA_DIR)];
		
		/* skip system entries */
		if (0 == strncmp(dent->d_name, ".", 1) || 0 == strncmp(dent->d_name, "..", 2))
		{
			continue;
		}
		
		snprintf(buf, sizeof(buf), PG_RDA_DIR "/%s", dent->d_name);
		/* We found a matching file; so remove it. */
		if (unlink(buf) != 0)
		{
			int			save_errno;

			save_errno = errno;
			closedir(dir);
			errno = save_errno;

			ereport(LOG,
					(errcode_for_file_access(),
					 errmsg("could not remove file \"%s\": %m", buf)));
		}
	}

	/* Cleanup complete. */
	FreeDir(dir);
}


FWDOpQueue *
fwd_new_op_queue(int size)
{
    FWDOpQueue *queue = NULL;
    
    queue = (FWDOpQueue *)fwd_malloc(sizeof(FWDOpQueue) + sizeof(FwdOperation) * size);
    if (queue != NULL)
    {
        queue->head = 0;
        queue->tail = 0;
        queue->size = size;
        queue->seq  = 0;
        memset(queue->data, 0x00, sizeof(FwdOperation) * size);
        pthread_mutex_init(&queue->lock, NULL);
    }

    return queue;
}

void
fwd_delete_queue(FWDOpQueue *queue)
{
    pthread_mutex_destroy(&queue->lock);
    fwd_free(queue);
}

bool
fwd_pop_queue(FWDOpQueue *queue, FwdOperation *result)
{
    
    FwdOperation *header = NULL;
    if (NULL == result)
    {
        return false;
    }

    pthread_mutex_lock(&queue->lock);
    if (queue->head == queue->tail)
    {
        pthread_mutex_unlock(&queue->lock);
        return false;
    }

    header = &queue->data[queue->head % queue->size];
    queue->head = (queue->head + 1) % queue->size;

    memcpy((char *)result, (void *)header, sizeof(FwdOperation));

    pthread_mutex_unlock(&queue->lock);
    return true;
}

/*
 * push elem to queue
 * return remaining size on success, otherwise -1.
 */
int
fwd_push_queue(FWDOpQueue *queue, int type, int64 rda_id, int node_id, void *data)
{
    /* queue is full */
    pthread_mutex_lock(&queue->lock);
    if((queue->tail + 1) % queue->size == queue->head)
    {
        pthread_mutex_unlock(&queue->lock);
        return  -1;
    }
    
    queue->data[queue->tail % queue->size].data    = data;
    queue->data[queue->tail % queue->size].type    = type;
    queue->data[queue->tail % queue->size].rda_id  = rda_id;
    queue->data[queue->tail % queue->size].node_id = node_id;
    queue->tail = (queue->tail + 1) % queue->size;
    pthread_mutex_unlock(&queue->lock);
    return 0;
}

/*
 * create queue group
 */
NodeRDASlots*
fwd_new_rda_node_group(int groupid)
{
    int          idx = 0;
    NodeRDASlots *q_group;
    
	q_group = (NodeRDASlots *)fwd_malloc(sizeof(NodeRDASlots));
    if (q_group != NULL)
    {
        q_group->rda_slots = (RDAController *)fwd_malloc(sizeof(RDAController) * RDA_SLOTS_PER_GROUP);
        if (q_group->rda_slots != NULL)
        {
            memset(q_group->rda_slots, 0, sizeof(RDAController) * RDA_SLOTS_PER_GROUP);
            for (idx = 0; idx < RDA_SLOTS_PER_GROUP; idx++)
            {
               q_group->rda_slots[idx].group_id = groupid;
               q_group->rda_slots[idx].controller_id = groupid * RDA_SLOTS_PER_GROUP + idx;
               q_group->rda_slots[idx].ctl_status = RDACtlStatus_Idle;
            }
            q_group->size = RDA_SLOTS_PER_GROUP;
            q_group->used_cnt = 0;
            SpinLockInit(&(q_group->used_cnt_lock));
        }
        else
        {
            elog(ERROR, FORWARDER_PREFIX "fwd_new_rda_node_group failed.");
        }
    }
    return q_group;
}

/*
 * delete queue group
 */
void
fwd_delete_rda_node_group(NodeRDASlots *q_group)
{
    if (q_group != NULL)
    {
        fwd_free(q_group->rda_slots);
        fwd_free(q_group);
    }
}

/*
 * create an arary of queue group
 */
int
fwd_init_rda_queue_groups(void)
{
    int i = 0;
    if (g_fwd_server_rda_slots_group)
    {
        return -1;
    }

    /* build dn rda_control groups */
	g_fwd_server_rda_slots_group = (NodeRDASlots **)fwd_malloc(sizeof(NodeRDASlots *) * g_total_node_num);
    if (g_fwd_server_rda_slots_group != NULL)
    {
        memset(g_fwd_server_rda_slots_group, 0, sizeof(NodeRDASlots *) * g_total_node_num);
        for (i = 0; i < g_total_node_num; i++)
        {
			g_fwd_server_rda_slots_group[i] = fwd_new_rda_node_group(i);
            if (NULL == g_fwd_server_rda_slots_group[i])
            {
                fwd_check_thread_id();
            	elog(LOG, FORWARDER_PREFIX "fwd_init_rda_queue_groups dn rda control groups failed.");
                return -1;
            }
        }
    }
    return 0;
}

/*
 * free an array of queue groups
 */
void
fwd_delete_rda_node_groups()
{
    int i = 0;
    if (g_fwd_server_rda_slots_group)
    {
        for (i = 0; i < g_total_node_num; i ++)
        {
            fwd_delete_rda_node_group(g_fwd_server_rda_slots_group[i]);
        }
    }
    fwd_free(g_fwd_server_rda_slots_group);
    g_fwd_server_rda_slots_group = NULL;
}

static inline RDAController *search_rda_slots_group(NodeRDASlots *q_group, int start_bucket, int end_buckset, int64 rda_id, int node_id)
{
    int            bucket_idx   = 0;
    RDAController *rda_slot     = NULL;
    
    /* loop to find an idle rda conroller */
    for (bucket_idx = start_bucket; bucket_idx < end_buckset; bucket_idx++)
    {
        rda_slot = &q_group->rda_slots[bucket_idx];
        if (RDACtlStatus_Idle == rda_slot->ctl_status)
        {
            rda_slot->rda_id     = rda_id;
            rda_slot->socket     = -1;
            rda_slot->node_id    = node_id;
            rda_slot->ctl_status = RDACtlStatus_Init;
            
            SpinLockAcquire(&q_group->used_cnt_lock);
            q_group->used_cnt += 1;
            SpinLockRelease(&q_group->used_cnt_lock);

            fwd_check_thread_id();
        	elog(LOG,FORWARDER_PREFIX "search_rda_slots_group: succeed, bucket index %d, total %d, occupied %d, rda_id %ld, node_id %d, ctl_index %d",
                                      bucket_idx, q_group->size, q_group->used_cnt, rda_id, node_id, rda_slot->controller_id);
            return rda_slot;
        }
    }
    return NULL;
}

static inline int fwd_index_to_node_id(int index)
{
    /* handle coordinator */
    if (index >= g_fwd_dn_num)
    {
        return (index - g_fwd_dn_num + FWD_CN_NODEID_BASE);
    }
    else
    {
        return index;
    }
}


static inline int fwd_node_to_index(int node_id)
{
    /* handle coordinator */
    if (node_id >= FWD_CN_NODEID_BASE)
    {
        return (node_id - FWD_CN_NODEID_BASE + g_fwd_dn_num);
    }
    else
    {
        return node_id;
    }
}
/*
 * get available controller for rda_id and node_id, can only be called in main thread.
 */
RDAController *
fwd_alloc_rda_controller(int64 rda_id, int node_id)
{
    int           group_id     = 0;
    int           start_bucket = 0;
    NodeRDASlots *q_group;
    RDAController *rda_slot    = NULL;

    group_id = fwd_node_to_index(node_id);
	q_group  = g_fwd_server_rda_slots_group[group_id];
	if (NULL == q_group)
	{
		return NULL;
	}
    
	start_bucket = rda_id % q_group->size;

    /* find hash value to  q_group->size */
    rda_slot = search_rda_slots_group(q_group, start_bucket, q_group->size, rda_id, node_id);
    if (rda_slot)
    {
        return rda_slot;
    }

    /* find 0 to hash value */
    rda_slot = search_rda_slots_group(q_group, 0, start_bucket, rda_id, node_id);
    if (rda_slot)
    {
        return rda_slot;
    }
   
    fwd_check_thread_id();
    elog(LOG, FORWARDER_PREFIX "fwd_alloc_rda_controller: failed, rda_id %ld, node_id %d", rda_id, node_id);
    return NULL;
}

static inline RDAController *search_rda_slots_group_by_rda_id(NodeRDASlots *q_group, int start_bucket, int end_buckset, int64 rda_id)
{
    int        bucket_idx   = 0;
    
    /* loop to find an idle rda conroller s*/
    for (bucket_idx = start_bucket; bucket_idx < end_buckset; bucket_idx++)
    {

        if (q_group->rda_slots[bucket_idx].rda_id == rda_id)
        {
            if (RDACtlStatus_Running == q_group->rda_slots[bucket_idx].ctl_status)
            {
                return &q_group->rda_slots[bucket_idx];
            }
            else
            {
                /* rda control not ready */
                return NULL;
            }            
        }
    }
    return NULL;
}

/*
 * get rda controller by rda_id and node_id
 */
static RDAController *get_rda_controller_by_rda_id(int64 rda_id, int node_id)
{
    int           group_id = 0;
    int           start_bucket = 0;
    NodeRDASlots *q_group;
    RDAController *rda_slot    = NULL;

    group_id = fwd_node_to_index(node_id);
	q_group = g_fwd_server_rda_slots_group[group_id];
	if (NULL == q_group)
	{
		return NULL;
	}

    
	start_bucket = rda_id % q_group->size;

    /* find hash value to  q_group->size */
    rda_slot = search_rda_slots_group_by_rda_id(q_group, start_bucket, q_group->size, rda_id);
    if (rda_slot)
    {
        return rda_slot;
    }

    /* find 0 to hash value */
    rda_slot = search_rda_slots_group_by_rda_id(q_group, 0, start_bucket, rda_id);
    if (rda_slot)
    {
        return rda_slot;
    }
   
    forwarder_thread_logger(LOG, FORWARDER_PREFIX "get_rda_controller_by_rda_id: failed, rda_id %ld, node_id %d", rda_id, node_id);
    return NULL;
}

/*
 * get rda controller by rdas controller indexs
 */
static RDAController *get_rda_controller_by_id(int controller_id)
{
    int            node_id    = 0;
    int            bucket_idx = 0;

    if (controller_id >= 0)
    {   
        node_id = controller_id / RDA_SLOTS_PER_GROUP;
        if (node_id >= 0 && node_id < g_total_node_num)
        {
            bucket_idx = (controller_id % RDA_SLOTS_PER_GROUP);
            if (controller_id != g_fwd_server_rda_slots_group[node_id]->rda_slots[bucket_idx].controller_id)
            {
                /* impossible */
                abort();
            }

            if (RDACtlStatus_Running == g_fwd_server_rda_slots_group[node_id]->rda_slots[bucket_idx].ctl_status)
            {
                return &g_fwd_server_rda_slots_group[node_id]->rda_slots[bucket_idx];
            }
            else
            {
                /* rda control not ready */
                return NULL;
            }
        }
    }
    else
    {
        return NULL;
    }
    return NULL;
}

/*
 * reset rda controller
 */
void fwd_free_rda_controller(int controller_id)
{
    int            node_id    = 0;
    int            bucket_idx = 0;

    if (controller_id >= 0)
    {   
        node_id = controller_id / RDA_SLOTS_PER_GROUP;
        if (node_id >= 0 && node_id < g_fwd_dn_num)
        {
            bucket_idx = (controller_id % RDA_SLOTS_PER_GROUP);
            if (bucket_idx >= 0)
            {
                if (controller_id != g_fwd_server_rda_slots_group[node_id]->rda_slots[bucket_idx].controller_id)
                {
                    /* impossible */
                    abort();
                }
                g_fwd_server_rda_slots_group[node_id]->rda_slots[bucket_idx].rda_id     = INVALID_RDA_CONTROLLER_ID;
                g_fwd_server_rda_slots_group[node_id]->rda_slots[bucket_idx].data_queue = NULL;
                SpinLockAcquire(&g_fwd_server_rda_slots_group[node_id]->used_cnt_lock);
                g_fwd_server_rda_slots_group[node_id]->used_cnt -= 1;
                SpinLockRelease(&g_fwd_server_rda_slots_group[node_id]->used_cnt_lock);
                g_fwd_server_rda_slots_group[node_id]->rda_slots[bucket_idx].ctl_status = RDACtlStatus_Idle;
            }
        }
    }
}

void fwd_cleanup_socket_array()
{
    int i = 0;
    int j = 0;
    if (g_client_socket_array != NULL)
    {
        
        for (i = 0; i < g_total_node_num; i++)
        {
            if (g_client_socket_array[i] != NULL)
            {
                for (j = 0; j < g_rda_sender_socket_num_per_node; j++)
                {
                    close(g_client_socket_array[i][j].socket);
                }
                fwd_free(g_client_socket_array[i]);
            }
        }
        fwd_free(g_client_socket_array);
        g_client_socket_array = NULL;
        g_fwd_dn_num = 0;
        g_fwd_cn_num = 0;
        g_total_node_num = 0;
    }
}

/* socket reload service implementation. */
void fwd_socket_reload_impl()
{
    /* Nothing to do if forwarder is reloading */
    if (g_fwd_is_during_socket_reload)
    {
        return;
    }

    /* It means begin to socket reload */
    g_fwd_is_during_socket_reload = true;
    fwd_clean_fwd_resource();
    g_fwd_is_during_socket_reload = false;
}

ConnMgr* fwd_init_sender_socket(NodeDefinition *node_def)
{
    int                ret = 0;
    int                idx = 0;
    struct sockaddr_in server_address;
    ConnMgr *socket_mgr;

    server_address.sin_family = AF_INET;
    inet_pton(AF_INET, NameStr(node_def->nodehost), &server_address.sin_addr.s_addr);
    server_address.sin_port = htons(node_def->fwdserverport);

    socket_mgr = (ConnMgr*)fwd_malloc(sizeof(ConnMgr) * g_rda_sender_socket_num_per_node);
    if (NULL == socket_mgr)
    {
        fwd_check_thread_id();
        elog(LOG,FORWARDER_PREFIX "fwd_init_sender_socket malloc failed");
        return NULL;
    }
    memset((char*)socket_mgr, 0X00, sizeof(ConnMgr) * g_rda_sender_socket_num_per_node);

    for (idx = 0; idx < g_rda_sender_socket_num_per_node; idx++)
    {
        socket_mgr[idx].peernode = node_def->nodeoid;
        socket_mgr[idx].socket= socket(AF_INET, SOCK_STREAM, 0);
        if (socket_mgr[idx].socket == -1)
        {
            fwd_check_thread_id();
            elog(LOG,FORWARDER_PREFIX "failed to create socket to data node with port: %d for %s.", node_def->fwdserverport, strerror(errno));
            continue;
        }
        /* connect failed, maybe other node has not started, wait until fwd_maintain_socket_array to connect */
        ret = connect(socket_mgr[idx].socket, (struct sockaddr*)&server_address, sizeof(server_address));
        if (ret == -1)
        {
            close(socket_mgr[idx].socket);
            socket_mgr[idx].socket = -1;
            fwd_check_thread_id();
            elog(LOG,FORWARDER_PREFIX "failed to connect socket to data node with port: %d socket %d for %s.", node_def->fwdserverport, socket_mgr[idx].socket, strerror(errno));
            continue;
        }
        /* set socket non blocking */
        fwd_set_nonblock_connection(socket_mgr[idx].socket);
        socket_mgr[idx].datalen = 0;
        socket_mgr[idx].sendlen = 0;
        fwd_check_thread_id();
        elog(LOG,FORWARDER_PREFIX "succeed create socket %d from node[%s] to node[%s].", socket_mgr[idx].socket, PGXCNodeName, NameStr(node_def->nodename));
    }
    return socket_mgr;
}
static int fwd_init_sender_socket_array()
{
    bool succeed = true;
    int  idx = 0;
    int  ret = 0;
    int  dn_num = 0;
    int  cn_num = 0;
    int  node_num = 0;
    int  node_idx = 0;
    int  node_id_base = 0;
    TeleDBXNodeType node_type = 0;
    Oid *dn_oids = NULL;
    Oid *cn_oids = NULL;
    NodeDefinition *node_def = NULL;
    MemoryContext old_context;

    /* we have already inited */
    if (g_fwd_dn_rcv_rda_slots_group_ready)
    {
        return 0;
    }

    if (!PgxcSharedCacheInitDone())
    {
        return 0;
    }

    old_context = MemoryContextSwitchTo(ForwarderMemoryContext);

    if (IS_PGXC_COORDINATOR)
    {
        node_type    = TeleDBXNodeType_CN;
        node_id_base = FWD_CN_NODEID_BASE;
    }
    else
    {
        node_type    = TeleDBXNodeType_DN;
        node_id_base = 0;
    }
    /* query my_node_id index from shared memory */
    node_def = PgxcNodeGetDefinitionByName(PGXCNodeName, node_type, &my_node_id);
    if (NULL == node_def)
    {
        fwd_cleanup_socket_array();
        MemoryContextSwitchTo(old_context);
        fwd_check_thread_id();
        elog(LOG, FORWARDER_PREFIX "cannot get my node %s def when initializing socket array.", PGXCNodeName);
        return -1;
    }
    pfree(node_def);

    /* get my node id */
    my_node_id += node_id_base;


    PgxcNodeGetOids(&cn_oids, &dn_oids, &cn_num, &dn_num, false);
    if (dn_num <= 0 || cn_num <= 0)
    {
        fwd_check_thread_id();
        elog(LOG, FORWARDER_PREFIX "cannot get node information when initializing socket array.");
        MemoryContextSwitchTo(old_context);
        return -1;
    }

    node_num = dn_num + cn_num;
    g_client_socket_array = (ConnMgr**)fwd_malloc(sizeof(ConnMgr*) * node_num);
    if (NULL == g_client_socket_array)
    {
        fwd_check_thread_id();
        elog(LOG, FORWARDER_PREFIX "fwd_init_sender_socket_array malloc  dn socket array failed.");
        MemoryContextSwitchTo(old_context);
        return -1;
    }
   
    /* connect datanodes */
    node_idx = 0;
    for (idx = 0; idx < dn_num; idx++, node_idx++) 
    {
        if (idx == my_node_id && IS_PGXC_DATANODE) 
        {
            g_client_socket_array[node_idx] = NULL;
            continue;
        }
        node_def = PgxcNodeGetDefinition(dn_oids[idx]);
        if (node_def == NULL) 
        {
            fwd_check_thread_id();
            elog(LOG, FORWARDER_PREFIX "cannot get data node's info when building sockets from node[%d] to node[%u].",
                                    my_node_id, node_def->nodeoid);
            succeed = false;
            break;
        }
        g_client_socket_array[node_idx] = fwd_init_sender_socket(node_def);
        if (NULL == g_client_socket_array[node_idx])
        {
            fwd_check_thread_id();
            elog(LOG, FORWARDER_PREFIX "Failed to create sockets from node[%d] to node[%u].",
                                    my_node_id, node_def->nodeoid);
            succeed = false;
            pfree(node_def);
            break;
        }
        pfree(node_def);
    }

    /* failure */
    if (!succeed)
    {
        fwd_cleanup_socket_array();
        MemoryContextSwitchTo(old_context);
        return -1;
    }

    /* connect coordinators */
    for (idx = 0; idx < cn_num; idx++, node_idx++) 
    {
        /* coordinator node id start from FWD_CN_NODEID_BASE */
        if ((idx + FWD_CN_NODEID_BASE) == my_node_id && IS_PGXC_COORDINATOR) 
        {
            g_client_socket_array[node_idx] = NULL;
            continue;
        }

        node_def = PgxcNodeGetDefinition(cn_oids[idx]);
        if (node_def == NULL) 
        {
            fwd_check_thread_id();
            elog(LOG, FORWARDER_PREFIX "cannot get data node's info when building sockets from node[%d] to node[%u].",
                                    my_node_id, node_def->nodeoid);
            succeed = false;
            break;
        }
        g_client_socket_array[node_idx] = fwd_init_sender_socket(node_def);
        if (NULL == g_client_socket_array[node_idx])
        {
            fwd_check_thread_id();
            elog(LOG, FORWARDER_PREFIX "Failed to create sockets from node[%d] to node[%u].",
                                    my_node_id, node_def->nodeoid);
            succeed = false;
            pfree(node_def);
            break;
        }
        pfree(node_def);
    }
    /* failure */
    if (!succeed)
    {
        fwd_cleanup_socket_array();
        MemoryContextSwitchTo(old_context);
        return -1;
    }

    MemoryContextSwitchTo(old_context);
    
    /* release palloc'ed memory */
    pfree(dn_oids);
    pfree(cn_oids);
    g_fwd_dn_num = dn_num;
    g_fwd_cn_num = cn_num;
    g_total_node_num = g_fwd_dn_num + g_fwd_cn_num;

    ret = fwd_init_rda_queue_groups();
    if (0 == ret)
    {
        fwd_check_thread_id();
        elog(LOG, FORWARDER_PREFIX "FWDOpQueue groups init ready for %d nodes", g_total_node_num);
    }
    else
    {
        fwd_check_thread_id();
        elog(ERROR, FORWARDER_PREFIX "FWDOpQueue groups init failed");
    }
    /* must be here, don't move */
    g_fwd_data_control->status = FWD_RUNNING;
    g_fwd_dn_rcv_rda_slots_group_ready = true;


    fwd_init_rda_stat();

    /* register local rdas */
    fwd_register_local_rda();
    
    return 0;
}

static void fwd_maintain_socket_array(void)
{
    int idx;
    int socket_idx;
    MemoryContext old_context;
   
    old_context = MemoryContextSwitchTo(ForwarderMemoryContext);

    /* connect datanodes */
    for (idx = 0; idx < g_total_node_num; idx++) 
    {
        if (IS_PGXC_DATANODE)
        {
            if (idx == my_node_id) 
            {
                continue;
            }
        }
        

        if (IS_PGXC_COORDINATOR)
        {
            if ((idx - g_fwd_dn_num + FWD_CN_NODEID_BASE) == my_node_id)  
            {
                continue;
            }
        }
        
        for (socket_idx = 0; socket_idx < g_rda_sender_socket_num_per_node; socket_idx++)
        {
            if (-1 == g_client_socket_array[idx][socket_idx].socket)
            {
                fwd_make_connect(&g_client_socket_array[idx][socket_idx]);           
            }
        }        
    }    
    MemoryContextSwitchTo(old_context);   
}

static void fwd_init_rda_stat(void)
{
    HASHCTL ctl;
    
    memset(&ctl, 0, sizeof(HASHCTL));
    ctl.keysize     = sizeof(int64); /* rda id */
    ctl.entrysize   = RDA_STAT_ELEMENT_SIZE;
    ctl.hash        = tag_hash;
    ctl.hcxt        = ForwarderMemoryContext;
    g_rda_stat_hash = hash_create("forwarder rda stat hash", 
                                  1024, 
                                  &ctl,
                                  HASH_ELEM | HASH_FUNCTION | HASH_CONTEXT);

}

static void fwd_remove_rda_from_stat_hash(int64 rda_id)
{
    int64 *hash_tag = NULL;

    if (!g_rda_stat_hash)
    {
        return;
    }
    
    hash_tag = hash_search(g_rda_stat_hash, (void *) &rda_id, HASH_REMOVE, NULL);
    if (!hash_tag)
    {
	    elog(ERROR, "hash table corrupted");
    }
    g_rda_stat_number--;
}

static void fwd_add_rda_to_stat_hash(int64 rda_id)
{
    bool             found  = false;
    int              i      = 0;
    int              index  = 0;
    rda_stat_record *record = NULL;

    if (!g_rda_stat_hash)
    {
        return;
    }

    record = (rda_stat_record *) hash_search(g_rda_stat_hash, (const void *) &rda_id, HASH_ENTER, &found);
    if (!record)
    {
	    elog(ERROR, "hash table corrupted");
    }
    
    if (!found)
    {
        for (i = 0; i < g_total_node_num; i ++)
        {
            record[i * 2].is_in_frame = true;
        }

        for (i = 0; i < RDA_STAT_ELEMENT_NUM; i++)
        {
            record[i].rda_id = rda_id;
        }

        index = fwd_node_to_index(my_node_id);
        record[index * 2].node_id = my_node_id;
        record[index * 2 + 1].node_id = my_node_id;
        /* Init CN's node_id */
        for (i = g_fwd_dn_num; i < g_total_node_num; i++)
        {
            record[i * 2].node_id = fwd_index_to_node_id(i);
            record[i * 2 + 1].node_id = fwd_index_to_node_id(i);
        }

        g_rda_stat_number++;
    }
}

static void fwd_add_rda_stat_to_hash(void)
{
    bool             found       = false;
    int              i           = 0;
    int              index       = 0;
    int              total       = 0;
    int              pipe_num    = g_FwdWorkerNum + g_FwdWorkerNum; /* sender + receiver */
    rda_stat_record *hash_record = NULL;
    rda_stat_record *record      = NULL;

    if (!g_rda_stat_hash)
    {
        return;
    }
    
    for (i = 0; i < pipe_num; i++)
    {
        total = 0;
        while (total < RDA_STAT_LOOP_CNT)
        {
            record = (rda_stat_record*)PipeGet(g_rda_stat_pipes[i]);
            if (record)
            {
                hash_record = (rda_stat_record *) hash_search(g_rda_stat_hash, (const void *) &record->rda_id, HASH_ENTER, &found);
                if (hash_record)
                {
                    index = fwd_node_to_index(record->node_id);
                    /* in_frame : out_frame */
                    if (record->is_in_frame)
                    {
                        memcpy((void*)(hash_record + index * 2), (void*)record, sizeof(rda_stat_record));   
                    }
                    else
                    {
                        memcpy((void*)(hash_record + index * 2 + 1), (void*)record, sizeof(rda_stat_record));
                    }
                    
                }
                else
                {
                    elog(ERROR, "hash table corrupted");
                }
                total++;   
                fwd_free(record);
            }
            else
            {
                break;
            }
        }
    }
}

static void fwd_stat_rda_queue(data_queue *d_queue, ServiceThreadControl *thread_ctl)
{
    uint64           first_seq   = 0;
    uint8           *data        = NULL;
    rda_stat_record *stat_record = NULL;

    if (!g_rda_stat_hash || !d_queue || !g_fwd_local_rda_combiner_array)
    {
        return;
    }
    
    if (timeval_useconds_diff(&d_queue->stat_timestamp) > RDA_STAT_INTERVAL)
    {
        
        stat_record = fwd_malloc(sizeof(rda_stat_record));
        if (NULL == stat_record)
        {
            forwarder_thread_logger(LOG, FORWARDER_PREFIX "thread[%s]: %d, stat rda_id %ld, node_id %d out of memory",
                                                           g_fwd_thread_type_names[thread_ctl->type], thread_ctl->id,  d_queue->rda_id, d_queue->node_id);
            return;
        }

        gettimeofday(&d_queue->stat_timestamp, NULL);
        stat_record->rda_id          = d_queue->rda_id;
        stat_record->node_id         = d_queue->node_id;
        stat_record->is_in_frame     = FWD_THREAD_RECEIVER == thread_ctl->type;
        stat_record->status          = d_queue->status.cur_status;
        stat_record->status_duration = timeval_useconds_diff(&d_queue->stat_timestamp);
        data = rda_frame_get_data_off(d_queue->frame, &stat_record->frame_data_size);
        data = data;
        stat_record->send_offset     = d_queue->send_offset;

        if (FWD_THREAD_RECEIVER == thread_ctl->type)
        {
            fwd_queue_get_last_seq(d_queue->buffer_queue, &stat_record->cur_seq);
        }
        else
        {
            stat_record->cur_seq = d_queue->seq;
        }

        if (FWD_THREAD_SENDER == thread_ctl->type)
        {
            stat_record->ack_seq = d_queue->ack_seq;
        }
        else
        {
            fwd_queue_get_first_seq(d_queue->buffer_queue, &first_seq);
            stat_record->ack_seq = first_seq - 1;
        }

        if (FWD_THREAD_RECEIVER == thread_ctl->type)
        {
            stat_record->peer_next_send = stat_record->cur_seq + 1;
        }
        else
        {
            stat_record->peer_next_send = RDA_INVALID_PKG_SEQ;
        }

        fwd_queue_get_first_seq(d_queue->buffer_queue, &stat_record->first_buffer_seq);
        fwd_queue_get_last_seq(d_queue->buffer_queue, &stat_record->last_buffer_seq);

        stat_record->buffer_free_size = fwd_queue_free_length(d_queue->buffer_queue);
        if (FWD_THREAD_SENDER == thread_ctl->type)
        {
            stat_record->peer_free_slot_num = d_queue->peer_free_slot_num;
        }
        else
        {
            stat_record->peer_free_slot_num = -1;
        }

        stat_record->controller_index = d_queue->controller_idx;
        if (FWD_THREAD_RECEIVER == thread_ctl->type)
        {
            stat_record->controller_index = d_queue->controller->controller_id;
        }
        else
        {
            stat_record->controller_index = d_queue->controller_idx;
        }

        stat_record->bytes_sent     = d_queue->bytes_sent;
        stat_record->bytes_resent   = d_queue->bytes_resent;
        stat_record->bytes_received = d_queue->bytes_received;
        stat_record->bytes_dropped  = d_queue->bytes_dropped;

        stat_record->pkgs_sent      = d_queue->pkgs_sent;
        stat_record->pkgs_resent    = d_queue->pkgs_resent;
        stat_record->pkgs_received  = d_queue->pkgs_received;
        stat_record->pkgs_dropped   = d_queue->pkgs_dropped;
        
        if (PipePut(thread_ctl->rda_dump_pipe, (void*)stat_record))
        {
            forwarder_thread_logger(LOG, FORWARDER_PREFIX "thread[%s]: %d, stat rda_id %ld, node_id %d pipe is full",
                                                           g_fwd_thread_type_names[thread_ctl->type], thread_ctl->id,  d_queue->rda_id, d_queue->node_id);
            fwd_free(stat_record);
        }
    }
}



void init_data_thread_control(
                            ServiceThreadControl *control,
                            void *data_queue_map,
                            FWDOpQueue *data_chan_op_queue,
                            PGPipe     *rda_stat_pipe,
                            enum thread_type type,
                            int id)
{
    

    control->data_queue_map     = data_queue_map;
    control->data_chan_op_queue = data_chan_op_queue;
    control->rda_dump_pipe      = rda_stat_pipe;

    control->thread_need_quit   = false;
    control->thread_need_drain  = false;
    control->thread_running     = false;
    control->thread_draining    = false;
    control->error              = false;
    control->type               = type;
    control->id                 = id;
    /* replace forwarder_thread_logger with thread's own log file. */
    control->log_fd = NULL;
    ThreadSemaInit(&control->quit_sem, 0);
    ThreadSemaInit(&control->drain_sem, 0);
}

/* can only be called in main thread. */
DataFwdControl *
init_data_fwd_control(void)
{
    int     i         = 0;
    int     pipe_num  = g_FwdWorkerNum + g_FwdWorkerNum;/* sender + receiver + cleaner */
    
    /* free by free_data_fwd_control */
    DataFwdControl *control = NULL;

    control = (DataFwdControl *)palloc0(sizeof(DataFwdControl));
    control->sender_control = (ServiceThreadControl *)(palloc0(sizeof(ServiceThreadControl) * g_FwdWorkerNum));
    control->receiver_control = (ServiceThreadControl *)(palloc0(sizeof(ServiceThreadControl) * g_FwdWorkerNum));
    control->cleanner_control = (ServiceThreadControl *)(palloc0(sizeof(ServiceThreadControl)));

    control->sender_num   = g_FwdWorkerNum;
    control->receiver_num = g_FwdWorkerNum;    

    g_rda_stat_pipes = palloc0(sizeof(PGPipe*) * pipe_num);
    for (i = 0; i < pipe_num; i++)
    {
        g_rda_stat_pipes[i] = CreatePipe(RDA_STAT_PIPE_LENGTH);
    }
    
    
    for (i = 0; i < control->sender_num; i++)
    {
        init_data_thread_control(&control->sender_control[i], g_fwd_data_chan_send_map[i], g_fwd_data_chan_send_op_queue[i], g_rda_stat_pipes[i], FWD_THREAD_SENDER, i);
    }
    
    for (i = 0; i < control->receiver_num; i++)
    {
        init_data_thread_control(&control->receiver_control[i], g_fwd_data_chan_receive_map[i], g_fwd_data_chan_receive_op_queue[i], g_rda_stat_pipes[i + control->sender_num], FWD_THREAD_RECEIVER, i);
    }
    init_data_thread_control(control->cleanner_control, g_fwd_cleanner_map, g_fwd_cleanner_op_queue, g_rda_stat_pipes[control->sender_num + control->receiver_num], FWD_THREAD_CLEANER, 0);

    control->status = FWD_INIT;

    return control;
}
/* can only be called in main thread. */
void
free_data_fwd_control(DataFwdControl *control)
{
    int i = 0;
    int pipe_num = 0;
    if (control)
    {
        if (control->sender_control)
        {
            pfree(control->sender_control);
            control->sender_control = NULL;
        }
        if (control->receiver_control)
        {
            pfree(control->receiver_control);
            control->receiver_control = NULL;
        }
        if (control->cleanner_control)
        {
            pfree(control->cleanner_control);
        }
        
        pipe_num = g_FwdWorkerNum + g_FwdWorkerNum;
        for (i = 0; i < pipe_num; i++)
        {
            DestoryPipe(g_rda_stat_pipes[i]);
        }
        pfree(g_rda_stat_pipes);
        pfree(control);
    }
}

void
thread_sigmask(void)
{
    sigset_t new_mask;
    sigemptyset(&new_mask);
    (void)sigaddset(&new_mask, SIGQUIT);
    (void)sigaddset(&new_mask, SIGALRM);
    (void)sigaddset(&new_mask, SIGHUP);
    (void)sigaddset(&new_mask, SIGINT);
    (void)sigaddset(&new_mask, SIGTERM);
    (void)sigaddset(&new_mask, SIGQUIT);
    (void)sigaddset(&new_mask, SIGPIPE);
    (void)sigaddset(&new_mask, SIGUSR1);
    (void)sigaddset(&new_mask, SIGUSR2);
    (void)sigaddset(&new_mask, SIGFPE);
    (void)sigaddset(&new_mask, SIGCHLD);

    /* ignore signals*/
    (void)pthread_sigmask(SIG_BLOCK, &new_mask, NULL);
}

int32
create_thread(void *(*f)(void *), void *arg, int32 mode)
{

    pthread_attr_t attr;
    pthread_t threadid;
    int ret = 0;

    pthread_attr_init(&attr);
    switch (mode)
    {
    case MT_THR_JOINABLE:
    {
        pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_JOINABLE);
        break;
    }
    case MT_THR_DETACHED:
    {
        pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED);
        break;
    }
    default:
    {
        fwd_check_thread_id();
        elog(ERROR, "invalid thread mode %d\n", mode);
    }
    }
    ret = pthread_create(&threadid, &attr, f, arg);
    return ret;
}

/*
 * Create sender and receiver threads.
 */
bool
fwd_create_service_thread()
{
    bool succeed = true;
    int i = 0;
    int ret = 0;

    for (i = 0; i < g_fwd_data_control->sender_num; i++)
    {
        ret = create_thread(fwd_data_sender_thread_main, (void *)&g_fwd_data_control->sender_control[i], MT_THR_DETACHED);
        if (ret)
        {
            succeed = false;
            break;
        }
        /* Set running status for the thread. */
        g_fwd_data_control->sender_control[i].thread_running = true;
    }

    if (!succeed)
    {
        fwd_clean_data_thread_main();
        return succeed;
    }

    for (i = 0; i < g_fwd_data_control->receiver_num; i++)
    {
        ret = create_thread(fwd_data_receiver_thread_main, (void *)&g_fwd_data_control->receiver_control[i], MT_THR_DETACHED);
        if (ret)
        {
            succeed = false;
            break;
        }
        /* Set running status for the thread. */
        g_fwd_data_control->receiver_control[i].thread_running = true;
    }

    if (!succeed)
    {
        fwd_clean_data_thread_main();
        return succeed;
    }

    ret = create_thread(fwd_data_cleaner_thread_main, (void *)g_fwd_data_control->cleanner_control, MT_THR_DETACHED);
    if (ret)
    {
        succeed = false;
    }
    else
    {
        /* Set running status for the thread. */
        g_fwd_data_control->cleanner_control->thread_running = true;
    }

    if (!succeed)
    {
        fwd_clean_data_thread_main();
    }

    return succeed;
}

/*
 * Sender thread.
 */
void *
fwd_data_sender_thread_main(void *arg)
{
    bool drain_done   = false;
    bool op_done      = false;
    bool changed      = false;
    bool should_sleep = false;
    ServiceThreadControl *thread = NULL;

    thread = (ServiceThreadControl *)arg;
    thread_sigmask();
    thread->thread_running = true;
    while (1)
    {
        /* error, quit directly */
        if (thread->error)
        {
            break;
        }

        /* We have been told to quit. */
        if (thread->thread_need_quit)
        {
            break;
        }

        /* We have been told to drain. */
        if (thread->thread_need_drain)
        {
            thread->thread_draining = true;
            drain_done = false;
            op_done = false;
            while (!drain_done || !op_done)
            {
                drain_done = fwd_data_thread_drain_loop(thread);
                fwd_data_thread_op_handler(thread, &op_done);
            }
            thread->thread_draining = false;
            /* Tell main thread drain is done. */
            ThreadSemaUp(&thread->drain_sem);
        }

        changed      = false;
        should_sleep = false;

        /* Loop to send data. */
        changed = fwd_send_data_loop(thread);
        if (!changed) 
		{
            should_sleep = true;
        }

        /* Loop to process operation. */
        changed = fwd_data_thread_op_handler(thread, &op_done);
        if (!should_sleep && !changed)
        {
            should_sleep = true;
        }

        /* sleep if nothing to do */
        if (should_sleep)
        {
            usleep(fwd_sleep_inteval * 1000L);
        }
    }
    thread->thread_running = false;
    /* Tell main thread quit is done. */
    ThreadSemaUp(&thread->quit_sem);
    return NULL;
}

/*
 * Receiver thread.
 */
void *
fwd_data_receiver_thread_main(void *arg)
{
    bool drain_done   = false;
    bool op_done      = false;
    bool changed      = false;
    bool should_sleep = false;
    ServiceThreadControl *thread = NULL;

    thread = (ServiceThreadControl *)arg;
    thread_sigmask();
    thread->thread_running = true;
    while (1)
    {
        /* error, quit directly */
        if (thread->error)
        {
            break;
        }

        /* We have been told to quit. */
        if (thread->thread_need_quit)
        {
            break;
        }

        /* We have been told to drain. */
        if (thread->thread_need_drain)
        {
            thread->thread_draining = true;
            drain_done = false;
            op_done = false;
            while (!drain_done || !op_done)
            {
                drain_done = fwd_data_thread_drain_loop(thread);
                fwd_data_thread_op_handler(thread, &op_done);
            }
            thread->thread_draining = false;
            /* Tell main thread drain is done. */
            ThreadSemaUp(&thread->drain_sem);
        }

        changed      = false;
        should_sleep = false;

        /* Loop to receive data. */
        changed = fwd_receive_data_loop(thread);
        if (!changed)
        {
            should_sleep = true;
        }

        /* Loop to process operation. */
        changed = fwd_data_thread_op_handler(thread, &op_done);
        if (!should_sleep && !changed)
        {
            should_sleep = true;
        }

        /* sleep if nothing to do */
        if (should_sleep)
        {
            usleep(fwd_sleep_inteval * 1000L);
        }
    }
    thread->thread_running = false;
    /* Tell main thread quit is done. */
    ThreadSemaUp(&thread->quit_sem);
    return NULL;
}

/*
 * Cleanner thread.
 */
void *
fwd_data_cleaner_thread_main(void *arg)
{
    bool done    = false;
    bool changed = false;
    ServiceThreadControl *thread = NULL;

    thread = (ServiceThreadControl *)arg;
    thread_sigmask();
    thread->thread_running = true;
    while (1)
    {
        /* error, quit directly */
        if (thread->error)
        {
            break;
        }

        /* We have been told to quit. */
        if (thread->thread_need_quit)
        {
            break;
        }

        /* We have been told to drain. */
        if (thread->thread_need_drain)
        {
            thread->thread_draining = true;
            done = false;
            (void)fwd_cleaner_handle_rda_change(thread, &done);
            (void)fwd_cleaner_drain_data_loop(thread);
            thread->thread_draining = false;
            /* Tell main thread drain is done. */
            ThreadSemaUp(&thread->drain_sem);
            continue;
        }

        /* Loop to clean up mmap file. */
        changed = fwd_cleaner_handle_rda_change(thread, &done);

        /* sleep if nothing to do */
        if (!changed)
        {
            usleep(fwd_sleep_inteval * 1000L);
        }
    }
    thread->thread_running = false;
    /* Tell main thread quit is done. */
    ThreadSemaUp(&thread->quit_sem);
    return NULL;
}

/*
 * Loop to process operators to channel map.
 */
bool
fwd_cleaner_handle_rda_change(ServiceThreadControl *control, bool *done)
{
    bool         changed = false;
    int          ret     = 0;
    mmap_info   *m_info = NULL;
    FwdOperation op;

    changed = false;
    *done = true;
    /* Loop to process the operations. */
    while (fwd_pop_queue(control->data_chan_op_queue, &op))
    {
        changed = true;

        switch (control->type)
        {
            case FWD_THREAD_CLEANER:
            {
                /* cleaner thread */
                switch (op.type)
                {
                    case FWD_OP_CREATE:
                    {
                        /* we put mmap info into cleaner local map. */
                        m_info = (mmap_info *)(op.data);
                        ret = map_put(control->data_queue_map, op.rda_id, op.node_id, m_info, false);
                        if (ret != 0)
                        {
                            forwarder_thread_logger(LOG,
                                                    FORWARDER_PREFIX "cleanner[%d]: mmap info already exists, drop %s, rda_id %ld, node_id %d",
                                                    control->id, op_type_names[op.type], op.rda_id, op.node_id);
                        }
                        else
                        {
                            forwarder_thread_logger(LOG,
                                                    FORWARDER_PREFIX "cleanner[%d]: ok to process %s, rda_id %ld, node_id %d",
                                                    control->id, op_type_names[op.type], op.rda_id, op.node_id);
                        }
                        break;
                    }
                    case FWD_OP_DROP:
                    {
                        /* get rda mmap info and decrease the ref count */
                        m_info = (mmap_info *)map_get(control->data_queue_map, op.rda_id, op.node_id);
                        if (m_info == NULL)
                        {
                            /* mmap info not found, drop the request */
                            forwarder_thread_logger(LOG,
                                                    FORWARDER_PREFIX "cleanner[%d]: mmap info not found, drop %s, rda_id %ld, node_id %d",
                                                    control->id, op_type_names[op.type], op.rda_id, op.node_id);
                        }
                        else
                        {
                            --m_info->ref_cnt;
                            forwarder_thread_logger(LOG, FORWARDER_PREFIX "cleanner[%d]: ok to process %s, rda_id %ld, node_id %d",
                                                    control->id, op_type_names[op.type], op.rda_id, op.node_id);
                            if (m_info->ref_cnt == 0)
                            {
                                map_erase(control->data_queue_map, op.rda_id, op.node_id);
                                dsm_rda_detach_and_unlink(op.rda_id, &m_info->mmap_addr, &m_info->mmap_size);
                                delete_mmap_info(m_info);
                                forwarder_thread_logger(LOG, FORWARDER_PREFIX "cleanner[%d]: no longer referenced, ok to unmap and unlink, rda_id %ld",
                                                        control->id, op.rda_id);
                            }
                        }
                        break;
                    }
                    default:
                    {
                        forwarder_thread_logger(LOG,
                                                FORWARDER_PREFIX "%s[%d]:  invalid operation type %d",
                                                g_fwd_thread_type_names[control->type], control->id, op.type);
                        break;
                    }
                }
                break;
            }
            
            /* unknow thread type */
            default:
            {
                forwarder_thread_logger(LOG,
                                        FORWARDER_PREFIX "%s[%d]:  invalid thread type %d",
                                        g_fwd_thread_type_names[control->type], control->id, control->type);
                break;
            }
        }
    }
    return changed;
}
/*
 * Loop to process operators to channel map of data threads.
 */
static bool
fwd_data_thread_op_handler(ServiceThreadControl *control, bool *done)
{
    bool         changed = false;
    int          ret     = 0;
    data_queue  *d_queue = NULL;
    FwdOperation op;

    changed = false;
    *done = true;
    /* Loop to process the operations. */
    while (fwd_pop_queue(control->data_chan_op_queue, &op))
    {
        changed = true;

        switch (control->type)
        {
            /* sender and receiver thread */
            case FWD_THREAD_SENDER:
            case FWD_THREAD_RECEIVER:
            {
                /* create requst */
                switch (op.type)
                {
                    case FWD_OP_CREATE:
                    {
                        /* add data queue into thread local map for future data transfering */
                        d_queue = (data_queue *)(op.data);

                        /* put into the map */
                        ret = map_put(control->data_queue_map, op.rda_id, op.node_id, d_queue, false);
                        if (ret != 0)
                        {
                            forwarder_thread_logger(LOG,FORWARDER_PREFIX "%s[%d]: data channel already exists, drop %s, rda_id %ld, node_id %d",
                                                    g_fwd_thread_type_names[control->type], control->id, op_type_names[op.type], op.rda_id, op.node_id);
                        }
                        else
                        {
                            forwarder_thread_logger(LOG, FORWARDER_PREFIX "%s[%d]: ok to process %s, rda_id %ld, node_id %d",
                                                    g_fwd_thread_type_names[control->type], control->id, op_type_names[op.type], op.rda_id, op.node_id);
                        }

                        if (FWD_THREAD_RECEIVER == control->type)
                        {
                            d_queue->controller->ctl_status = RDACtlStatus_Running;
                            forwarder_thread_logger(LOG, FORWARDER_PREFIX "%s[%d]: ok to process %s set rda control RDACtlStatus_Running, rda_id %ld, node_id %d, group_id %d, controller_index %d",
                                                    g_fwd_thread_type_names[control->type], control->id, op_type_names[op.type], op.rda_id, op.node_id,
                                                    d_queue->controller->group_id, d_queue->controller->controller_id);
                        }

                        /* update status */
                        set_rda_status(&d_queue->status, RDA_LOCAL_REGISTERED);
                        break;
                    }

                    case FWD_OP_DROP:
                    {
                        /* erase data queue and data request queue from map */
                        d_queue = map_erase(control->data_queue_map, op.rda_id, op.node_id);
                        if (d_queue)
                        {
                            if (FWD_THREAD_RECEIVER == control->type)
                            {
                                fwd_free_rda_controller(d_queue->controller->controller_id);
                                d_queue->controller = NULL;
                            }
                            set_rda_status(&d_queue->status, RDA_UNREGISTERED);
                        }

                        /* create drop request and push to cleanner queue. */
                        if (fwd_push_queue(g_fwd_cleanner_op_queue, FWD_OP_DROP, op.rda_id, FWD_NODE_ID_ALL, NULL) < 0)
                        {
                            *done = false;
                            forwarder_thread_logger(LOG, FORWARDER_PREFIX "%s[%d]: ok to process %s, but cleanner queue full, rda_id %ld, node_id %d",
                                                    g_fwd_thread_type_names[control->type], control->id, op_type_names[op.type], op.rda_id, op.node_id);
                        }
                        else
                        {
                            forwarder_thread_logger(LOG, FORWARDER_PREFIX "%s[%d]: ok to process %s, rda_id %ld, node_id %d",
                                                    g_fwd_thread_type_names[control->type], control->id, op_type_names[op.type], op.rda_id, op.node_id);
                        }
                        break;
                    }

                    case FWD_OP_RESET:
                    {   
                        d_queue = (data_queue *)(op.data);

                        /* reset frame and data queue buffer */
                        fwd_reset_queue(d_queue->buffer_queue);
                        rda_set_frame_status(d_queue->frame, RDA_FRAME_STATUS_BAD_CONNECTION);
                        
                        /* set status to RDA_LOCAL_REGISTERED */
                        set_rda_status(&d_queue->status, RDA_LOCAL_REGISTERED);

                        if (FWD_THREAD_RECEIVER == control->type)
                        {
                            d_queue->controller->ctl_status = RDACtlStatus_Running;
                        }

                        gettimeofday(&d_queue->stat_timestamp, NULL);
                        d_queue->ack_bytes           = 0;
                        d_queue->resend_package_done = true;
                        d_queue->send_offset         = 0;
                        d_queue->seq                 = RDA_INVALID_PKG_SEQ;
                        d_queue->ack_seq	         = RDA_INVALID_PKG_SEQ;
                        d_queue->next_send           = 0;
                        d_queue->peer_free_slot_num  = 0;
                        
                        d_queue->bytes_sent     = 0;
                        d_queue->bytes_received = 0;
                        d_queue->bytes_dropped  = 0;
                		d_queue->bytes_resent   = 0;
                		
                		d_queue->pkgs_sent      = 0;
                        d_queue->pkgs_received  = 0;
                        d_queue->pkgs_dropped   = 0;
                		d_queue->pkgs_resent    = 0;

                        /* fresh data queue status. */
                        fwd_stat_rda_queue(d_queue, control);
                        forwarder_thread_logger(LOG, FORWARDER_PREFIX "%s[%d]: ok to process %s, rda_id %ld, node_id %d",
                                                                    g_fwd_thread_type_names[control->type], control->id, op_type_names[op.type], op.rda_id, op.node_id);
                        break;
                    }

                    default:
                    {
                        forwarder_thread_logger(LOG,
                                                FORWARDER_PREFIX "%s[%d]:  invalid operation type %d",
                                                g_fwd_thread_type_names[control->type], control->id, op.type);
                        break;
                    }
                }
                break;
            }
            /* unknown thread type */
            default:
            {
                forwarder_thread_logger(LOG,
                                        FORWARDER_PREFIX "%s[%d]:  invalid thread type %d", g_fwd_thread_type_names[control->type], control->id, control->type);
                break;
            }
        }
    }
    return changed;
}

int fwd_retry_last_package(ConnMgr *curr_socket, data_queue *d_queue, ServiceThreadControl *control)
{
    int    saved_pkg_left_len = 0;
    int    network_ret;
    uint64 latest_pkg;
    FWDPackage *send_pkg = NULL;
    
    fwd_queue_get_last_seq(d_queue->buffer_queue, &latest_pkg);

    send_pkg = fwd_queue_get_package(d_queue->buffer_queue, latest_pkg);
    if (send_pkg)
    {
        /* dump data if needed */
		if (g_fwd_dump_pkg_to_file)
		{
            saved_pkg_left_len = send_pkg->pkg_left_len;
        }

        network_ret = fwd_client_send_package(curr_socket, send_pkg);
	    if (network_bad_socket == network_ret)
    	{
            forwarder_thread_logger(LOG,
	                                FORWARDER_PREFIX "sender[%d]: RetrySendLatestPkg send request rda_id %ld, node_id %d, seq %lu, ack_seq %lu, send_offset %lu, snd_size %d failed",
	                	            control->id, d_queue->rda_id, my_node_id, d_queue->seq, d_queue->ack_bytes, d_queue->send_offset, send_pkg->pkg_len);    
            
            return network_ret;
        }
        else
        {
            /* dump data if needed */
			if (g_fwd_dump_pkg_to_file)
		    {
                fwd_dump_pkg_to_file(send_pkg, d_queue->debug_fp, saved_pkg_left_len);
            }
            return network_ret;
		}
    }
    return network_ret_ok;
}


/* Server socket functions. */
static int fwd_accept_connection(int server_fd, FWDServersPort *port)
{
    int         rc = 0;
    char		remote_host[NI_MAXHOST];
	char		remote_port[NI_MAXSERV];

    /* accept connection and fill in the client (remote) address */
	port->raddr.salen = sizeof(port->raddr.addr);
    port->sock = accept(server_fd,
							 (struct sockaddr *) &port->raddr.addr,
							 &port->raddr.salen);
	if (port->sock == PGINVALID_SOCKET)
	{
        forwarder_thread_logger(LOG, FORWARDER_PREFIX "thread:%ld fwd_accept_connection could not accept new connection of socket %d for %s ", pthread_self(), port->sock, strerror(errno));

		/*
		 * If accept() fails then postmaster.c will still see the server
		 * socket as read-ready, and will immediately try again.  To avoid
		 * uselessly sucking lots of CPU, delay a bit before trying again.
		 * (The most likely reason for failure is being out of kernel file
		 * table slots; we can do little except hope some will get freed up.)
		 */
		pg_usleep(100000L);		/* wait 0.1 sec */
		return STATUS_ERROR;
	}

    /* set nonblock socket */
    fwd_set_nonblock_connection(port->sock);

	/* fill in the server (local) address */
	port->laddr.salen = sizeof(port->laddr.addr);
	if (getsockname(port->sock,
					(struct sockaddr *) &port->laddr.addr,
					&port->laddr.salen) < 0)
	{
		forwarder_thread_logger(LOG, FORWARDER_PREFIX "thread:%ld  getsockname() failed: %s", pthread_self(), strerror(errno));
		return STATUS_ERROR;
	}

	/* select NODELAY and KEEPALIVE options if it's a TCP connection */
	if (!IS_AF_UNIX(port->laddr.addr.ss_family))
	{
		int			on;
#ifdef WIN32
		int			oldopt;
		int			optlen;
		int			newopt;
#endif

#ifdef	TCP_NODELAY
		on = 1;
		if (setsockopt(port->sock, IPPROTO_TCP, TCP_NODELAY,
					   (char *) &on, sizeof(on)) < 0)
		{
            forwarder_thread_logger(LOG, FORWARDER_PREFIX "thread:%ld  setsockopt(TCP_NODELAY) socket: %d failed: %s", pthread_self(), port->sock, strerror(errno));
        	return STATUS_ERROR;
		}
#endif
		on = 1;
		if (setsockopt(port->sock, SOL_SOCKET, SO_KEEPALIVE,
					   (char *) &on, sizeof(on)) < 0)
		{
            fwd_check_thread_id();
			elog(LOG, FORWARDER_PREFIX"setsockopt(%s) failed: %m", "SO_KEEPALIVE");
            forwarder_thread_logger(LOG, FORWARDER_PREFIX "thread:%ld  setsockopt(SO_KEEPALIVE) socket: %d failed: %s", pthread_self(), port->sock, strerror(errno));
        	
			return STATUS_ERROR;
		}

        if (tcp_keepalives_idle)
        {
            if (setsockopt(port->sock, IPPROTO_TCP, TCP_KEEPIDLE,
                    (char *) &tcp_keepalives_idle, sizeof(tcp_keepalives_idle)) < 0)
            {
                forwarder_thread_logger(LOG, FORWARDER_PREFIX "thread:%ld  setsockopt(TCP_KEEPIDLE) socket: %d failed: %s", pthread_self(), port->sock, strerror(errno));
                return STATUS_ERROR;
            }
        }

        if (tcp_keepalives_interval)
        {
            if (setsockopt(port->sock, IPPROTO_TCP, TCP_KEEPINTVL,
                    (char *) &tcp_keepalives_interval, sizeof(tcp_keepalives_interval)) < 0)
            {
                forwarder_thread_logger(LOG, FORWARDER_PREFIX "thread:%ld  setsockopt(TCP_KEEPINTVL) socket: %d failed: %s", pthread_self(), port->sock, strerror(errno));
                
                return STATUS_ERROR;
            }
        }

        if (tcp_keepalives_count)
        {
            if (setsockopt(port->sock, IPPROTO_TCP, TCP_KEEPCNT,
                    (char *) &tcp_keepalives_count, sizeof(tcp_keepalives_count)) < 0)
            {
                forwarder_thread_logger(LOG, FORWARDER_PREFIX "thread:%ld  setsockopt(TCP_KEEPCNT) socket: %d failed: %s", pthread_self(), port->sock, strerror(errno));
                
                return STATUS_ERROR;
            }
        }
	}

    rc = pg_getnameinfo_all(&port->raddr.addr, port->raddr.salen,
								  remote_host, sizeof(remote_host),
								  remote_port, sizeof(remote_port),
								  NI_NUMERICSERV) ;
    if (rc)
	{
        forwarder_thread_logger(LOG, FORWARDER_PREFIX "thread:%ld socket: %d pg_getnameinfo_all failed: %s", pthread_self(), port->sock, strerror(errno));
    		
	}

    port->remote_host = strdup(remote_host);
    port->remote_port = strdup(remote_port);
    forwarder_thread_logger(LOG, FORWARDER_PREFIX "thread:%ld fwd_accept_connection succeed accept new connection of socket %d, remote host %s port %s", pthread_self(), port->sock, remote_host, remote_port);
	return STATUS_OK;
}

static void
fwd_conn_free(FWDServersPort *conn)
{
	if (conn->sock >= 0)
    {   
        close(conn->sock);
        conn->sock = -1;
    }

    if (conn->remote_host)
    {
        free(conn->remote_host);
        conn->remote_host = NULL;
    }

    if (conn->remote_port)
    {
        free(conn->remote_port);
        conn->remote_port = NULL;
    }
    fwd_free(conn);
}

static void
fwd_set_nonblock_connection(int socket)
{
    int flags;
    flags = fcntl(socket, F_GETFL, 0);
    if (flags == -1)
    {
        forwarder_thread_logger(LOG, FORWARDER_PREFIX "thread:%ld fcntl get errors %s sock %d", pthread_self(), strerror(errno), socket);
        return;
    }

    flags = fcntl(socket, F_SETFL, flags | O_NONBLOCK);
    if (flags == -1)
    {
        forwarder_thread_logger(LOG, FORWARDER_PREFIX "thread:%ld fcntl set errors %s sock %d", pthread_self(), strerror(errno), socket);
        return;
    }
}


static FWDServersPort *fwd_create_conn(int server_fd)
{
    int                 rc   = 0;
	FWDServersPort	   *port = NULL;

    port = (FWDServersPort *)fwd_malloc(sizeof(FWDServersPort));
	if (NULL == port)
	{
        forwarder_thread_logger(LOG, FORWARDER_PREFIX "thread:%ld out of memory", pthread_self());
		return NULL;
	}

    memset((char*)port, 0X00, sizeof(FWDServersPort));
    rc = fwd_accept_connection(server_fd, port);
	if (rc != STATUS_OK)
	{
		if (port->sock >= 0)
        {      
			close(port->sock);
        }
		fwd_conn_free(port);
		return NULL;
	}
	
	return port;
}
static int fwd_init_server_threads(void)
{
    int i   = 0;
    int ret = 0;
    
    g_fwdServerThreadCTL.io_threads = (FWDIOThreadCtl*)fwd_malloc(sizeof(FWDIOThreadCtl) * g_FwdIOThreadNum);
    if (NULL == g_fwdServerThreadCTL.io_threads)
    {
        fwd_check_thread_id();
        elog(LOG, "fwd_init_server_threads failed for OOM");
        return -1;
    }
    memset((char*)g_fwdServerThreadCTL.io_threads, 0X00, sizeof(FWDIOThreadCtl) * g_FwdIOThreadNum);

    ret = create_thread(fwd_listen_thread_main, (void *)&g_fwdServerThreadCTL.listen_thread, MT_THR_DETACHED);
    if (ret)
    {
        fwd_check_thread_id();
        elog(LOG, "fwd_init_server_threads create listen thread failed");
        return -1;
    }

    for (i = 0; i < g_FwdIOThreadNum; i++)
    {
        ret = create_thread(fwd_io_thread_main, (void *)&g_fwdServerThreadCTL.io_threads[i], MT_THR_DETACHED);
        if (ret)
        {
            fwd_check_thread_id();
            elog(LOG, "fwd_init_server_threads create io thread failed");
            return -1;
        }
    }
    /* wait until server threads OK*/
    pg_usleep(1000L);
    /* check whether all thread are OK. */
    if (g_fwdServerThreadCTL.listen_thread.thread_status != IOThreadStatus_Running)
    {
        fwd_check_thread_id();
        elog(LOG, "fwd_init_server_threads start listen thread failed");
        return -1;
    }

    for (i = 0; i < g_FwdIOThreadNum; i++)
    {
        if (g_fwdServerThreadCTL.io_threads[i].thread_status != IOThreadStatus_Running || !g_fwdServerThreadCTL.io_threads[i].thr_epoll_ok)
        {
            fwd_check_thread_id();
            elog(LOG, "fwd_init_server_threads start io thread %d failed", i);
            return -1;
        }
    }
    return 0;
}

static void fwd_maintain_server_threads(void)
{
    int i   = 0;
    int ret = 0;

    /* check whether all thread are OK. */
    if (g_fwdServerThreadCTL.listen_thread.thread_status != IOThreadStatus_Running)
    {
        fwd_check_thread_id();
        elog(LOG, "fwd_maintain_server_threads  found bad listen thread, recreate listen thread");
        ret = create_thread(fwd_listen_thread_main, (void *)&g_fwdServerThreadCTL.listen_thread, MT_THR_DETACHED);
        if (ret)
        {
            fwd_check_thread_id();
            elog(LOG, "fwd_maintain_server_threads create listen thread failed");
        }
    }

    for (i = 0; i < g_FwdIOThreadNum; i++)
    {
        if (g_fwdServerThreadCTL.io_threads[i].thread_status != IOThreadStatus_Running)
        {
            fwd_check_thread_id();
            elog(LOG, "fwd_maintain_server_threads  found bad io thread %d, recreate io thread", i);

            ret = create_thread(fwd_io_thread_main, (void *)&g_fwdServerThreadCTL.io_threads[i], MT_THR_DETACHED);
            if (ret)
            {
                fwd_check_thread_id();
                elog(LOG, "fwd_maintain_server_threads create io thread failed");
            }
        }
    }
}


static int fwd_stop_server_threads(void)
{
    int i   = 0;
    int done_num = 0;

    g_fwdServerThreadCTL.listen_thread.need_exit = true;

    for (i = 0; i < g_FwdIOThreadNum; i++)
    {
        g_fwdServerThreadCTL.io_threads[i].need_exit = true;
    }

    /* wait listen thread exit */
    do
    {
        pg_usleep(1);
    } while ( g_fwdServerThreadCTL.listen_thread.thread_status == IOThreadStatus_Running);
    
    /* wait io thread exit */
    do
    {
        done_num = 0;
        for (i = 0; i < g_FwdIOThreadNum; i++)
        {
            if (g_fwdServerThreadCTL.io_threads[i].thread_status != IOThreadStatus_Running)
            {
                done_num ++;
            }
        }

        pg_usleep(1);
    } while (done_num != g_FwdIOThreadNum);
    return 0;
}

static int fwd_init_masks(fd_set *rmask)
{
    int			i;
	int			maxsock = -1;

	FD_ZERO(rmask);

	for (i = 0; i < FWD_MAXLISTEN; i++)
	{
		int			fd = FWDListenSocket[i];

		if (fd == -1)
			break;
		FD_SET(fd, rmask);
		if (fd > maxsock)
        {
            maxsock = fd;
        }
	}

	return maxsock + 1;
}

/*
 * listen thread of forwarder. 
 */
static void* fwd_listen_thread_main(void *arg)
{
	fd_set		readmask;
	int			nSockets;
    struct epoll_event event;
    struct timeval     timeout;	
    FWDListenThreadCtl *thread_ctl = (FWDListenThreadCtl*)arg;

    thread_ctl->thread_status = IOThreadStatus_Running;
    thread_ctl->need_exit = false;
	nSockets = fwd_init_masks(&readmask);
	for (;;)
	{
		fd_set		rmask;
		int			selres;

        /* exit if needed */
        if (thread_ctl->need_exit)
        {
            thread_ctl->thread_status = IOThreadStatus_Done;
            break;
        }
        
		/*
		 * Wait for a connection request to arrive.
		 *
		 * We wait at most one minute, to ensure that the other background
		 * tasks handled below get done even when no requests are arriving.
		 */
		memcpy((char *) &rmask, (char *) &readmask, sizeof(fd_set));
		
		/* must set timeout each time; some OSes change it! */
        timeout.tv_sec  = 1;
		timeout.tv_usec = 0;
		/* No need to take the lock now. open for all singals. s*/
        selres = select(nSockets, &rmask, NULL, NULL, &timeout);
		

		/* Now check the select() result */
		if (selres < 0)
		{
			if (errno != EINTR && errno != EWOULDBLOCK)
			{
				forwarder_thread_logger(LOG, FORWARDER_PREFIX "thread:%ld FWDListenThread select failed for %s", pthread_self(), strerror(errno));
                thread_ctl->thread_status = IOThreadStatus_Listen_Error;
                /* select failed, we need reboot forwarder process. */
                return NULL;
			}
		}
		
		/*
		 * New connection pending on any of our sockets? If so, fork a child
		 * process to deal with it.
		 */
		if (selres > 0)
		{
			int i;
            
			for (i = 0; i < FWD_MAXLISTEN; i++)
			{
				if (FWDListenSocket[i] == -1)
				{
					break;
				}
				
				if (FD_ISSET(FWDListenSocket[i], &rmask))
				{
					FWDServersPort	   *port     = NULL;
                    FWDConnectionInfo  *conninfo = NULL;
                    
                    conninfo = (FWDConnectionInfo *)fwd_malloc(sizeof(FWDConnectionInfo));
                    if (NULL == conninfo)
                    {
                        forwarder_thread_logger(LOG, FORWARDER_PREFIX "thread:%ld FWDListenThread create connection info failed", pthread_self());
                        break;
                    }
                    
                    port = fwd_create_conn(FWDListenSocket[i]);
					if (port)
					{
					    conninfo->con_port = port;
                        
						/* dispatch port to io threads */
                        event.data.ptr = conninfo;
                        event.events   = EPOLLIN | EPOLLERR | EPOLLHUP | EPOLLRDHUP;
                        if (g_fwdServerThreadCTL.io_threads[g_fwdServerThreadCTL.next_thread].thr_epoll_ok)
                        {
                            if(-1 == epoll_ctl (g_fwdServerThreadCTL.io_threads[g_fwdServerThreadCTL.next_thread].thr_efd, EPOLL_CTL_ADD, conninfo->con_port->sock, &event))
                            {
                                fwd_conn_free(port);
                                fwd_free(conninfo);
                                forwarder_thread_logger(LOG, FORWARDER_PREFIX "thread:%ld FWDListenThread failed to add socket to epoll for %s", pthread_self(), strerror(errno));
                                return NULL;
                            }
                            /* load balance */
                            g_fwdServerThreadCTL.next_thread = (g_fwdServerThreadCTL.next_thread + 1) % g_FwdIOThreadNum;
                        }
                        else
                        {
                            forwarder_thread_logger(LOG, FORWARDER_PREFIX "thread:%ld FWDListenThread thread %d epoll not ready", pthread_self(), g_fwdServerThreadCTL.next_thread);
                            thread_ctl->thread_status = IOThreadStatus_Epoll_Error;
                            return NULL;      
                        }
                        break;
					}
                    else
                    {
                        fwd_free(conninfo);
                        thread_ctl->thread_status = IOThreadStatus_Conn_Error;
                        forwarder_thread_logger(LOG, FORWARDER_PREFIX "thread:%ld FWDListenThread thread %d exit", pthread_self(), g_fwdServerThreadCTL.next_thread);
                        return NULL;
                    }
				}
			}
		}
	}
    return NULL;
}

/* IO thread of forwarder process. */
static void *fwd_io_thread_main(void *argp)
{
    int 		        efd;
	FWDIOThreadCtl      *thrinfo = (FWDIOThreadCtl *)argp;
 	struct epoll_event  events[FWD_MAX_CONNECTIONS_PER_THREAD];
	
	efd = epoll_create1(0);
	if(efd == -1)
	{
		forwarder_thread_logger(LOG, FORWARDER_PREFIX "thread:%ld fwd_io_thread_main failed to create epoll for %s", pthread_self(), strerror(errno));
        thrinfo->thr_epoll_ok = false;
        return NULL;
    }
	thrinfo->thr_efd = efd;
	thrinfo->thr_epoll_ok = true;
	thrinfo->thread_status = IOThreadStatus_Running;

	for (;;)
	{
		int 		i;
        int         n;
        int         ret;

		/* Wait for available event */
		n = epoll_wait(efd, events, FWD_MAX_CONNECTIONS_PER_THREAD, 1000);
        
		for(i = 0; i < n; i++)
		{
			FWDConnectionInfo *conn;
			
			if(!(events[i].events & EPOLLIN))
			{
				continue;
			}
			
			conn = (FWDConnectionInfo*)events[i].data.ptr;

            /* read data handle. */
            ret = fwd_server_port_process(conn->con_port);
            if (ret == network_bad_socket)
            {
                /* in case of error just close the socket. */
                epoll_ctl(thrinfo->thr_efd, EPOLL_CTL_DEL, conn->con_port->sock, NULL);

                /* tell receiver thread to skip the rda from the bad socket*/
                fwd_server_socket_close(conn->con_port->sock);
                fwd_conn_free(conn->con_port);
                fwd_free(conn);
                forwarder_thread_logger(LOG, FORWARDER_PREFIX "fwd_io_thread_main fwd_server_port_process get network_bad_socket, thread:%ld, close connection",
                                        pthread_self());
            }
		}

        /* exit if needed */
        if (thrinfo->need_exit)
        {
            thrinfo->thread_status = IOThreadStatus_Done;
            forwarder_thread_logger(LOG, FORWARDER_PREFIX "fwd_io_thread_main thread:%ld exit", pthread_self());
            break;
        }
	}

	return thrinfo;
}

/*
 * Loop to send data.
 */
bool fwd_send_data_loop(ServiceThreadControl *control)
{
    bool  changed        = false;
    bool  send_changed   = false;
    bool  rcv_changed    = false;
    bool  resend_changed = false;
    bool  force_drop     = false; /* socket error, need drop the rda file.*/
    int   ret            = 0;
    int   node_id        = 0;
    int   index          = 0;
    int64 rda_id         = 0;
    data_queue *d_queue  = NULL;
    rda_dsm_frame *frame = NULL;
    ConnMgr *curr_socket = NULL;

    if (g_fwd_dn_num <= 0)
    {
        return false;
    }

    /* Loop to send data. */
    map_begin_iter(control->data_queue_map);
    changed = false;

    while (0 == map_iter_key(control->data_queue_map, &rda_id, &node_id))
    {
        force_drop = false;

        if (node_id == my_node_id)
        {
            goto next;
        }

        /* get the data queue */
        d_queue = (data_queue *)(map_iter_value(control->data_queue_map));
        if (NULL == d_queue)
        {
            goto next;
        }
        
        if (node_id != d_queue->node_id)
        {
            abort();
        }

        if (route_to_node(node_id) != control->id)
        {
            abort();
        }

        frame = d_queue->frame;
        if (frame == NULL)
        {
            goto next;
        }

        /* skip frame that status not ok */
        if (rda_get_frame_status(frame) != RDA_FRAME_STATUS_OK)
        {
            goto next;
        }
       
        /* get socket */
        if (g_fwd_is_during_socket_reload)
        {
            goto next;
        }

        if (node_id < 0)
        {
            goto next;
        }

        index       = fwd_node_to_index(node_id);
        curr_socket = &g_client_socket_array[index][rda_id % g_rda_sender_socket_num_per_node];

		/* register is done, request remote status. */
        switch (get_rda_status(&d_queue->status))
        {
            case RDA_LOCAL_REGISTERED:
            {
                ret = fwd_request_server_status(curr_socket, rda_id, RDA_INIT_STATUS, d_queue->controller_idx, &force_drop);
                if (ret)
                {
                    forwarder_thread_logger(LOG,
                                            FORWARDER_PREFIX "thread %ld sender[%d] fwd_send_data_loop fwd_request_server_status RDA_INIT_STATUS failied, retry later, rda_id %ld, dst_node_id %d",
                                            pthread_self(), control->id, rda_id, node_id);
                    goto next;
                }

                set_rda_status(&d_queue->status, RDA_REQUEST_RDA_REMOTE);
                if (g_fwd_dump_pkg_to_file)
                {
                    forwarder_thread_logger(LOG,
                                            FORWARDER_PREFIX "thread %ld sender[%d] fwd_send_data_loop fwd_request_server_status RDA_INIT_STATUS succeed, enter RDA_REQUEST_RDA_REMOTE, rda_id %ld, dst_node_id %d",
                                            pthread_self(), control->id, rda_id, node_id);
                }
                
                goto next;
                break;
            }

            case RDA_REQUEST_RDA_REMOTE:
            {
                 /* timeout, resend request */
                if (rda_status_timeout(&d_queue->status))
                {
                    ret = fwd_request_server_status(curr_socket, rda_id, RDA_INIT_STATUS, d_queue->controller_idx, &force_drop);
                    if (ret)
                    {
                        forwarder_thread_logger(LOG,
                                                FORWARDER_PREFIX "thread %ld sender[%d] fwd_send_data_loop fwd_request_server_status RDA_INIT_STATUS failied, retry later, rda_id %ld, dst_node_id %d",
                                                pthread_self(), control->id, rda_id, node_id);
                        goto next;
                    }
                    set_rda_status(&d_queue->status, RDA_REQUEST_RDA_REMOTE);

                    if (g_fwd_dump_pkg_to_file)
                    {
                        forwarder_thread_logger(LOG,
                                                FORWARDER_PREFIX "thread %ld sender[%d] fwd_send_data_loop fwd_request_server_status RDA_INIT_STATUS timeout, retry succeed, set RDA_REQUEST_RDA_REMOTE, rda_id %ld, dst_node_id %d",
                                                pthread_self(), control->id, rda_id, node_id);
                    }
                }
                /* skip current, wait next time */
                goto next;
            }
            
            /* skip dqueues of the 4 following status. */
            case    RDA_INIT:
            case    RDA_UNREGISTERED:
            case    RDA_BAD_CONNECTION:
            case    RDA_STATUS_BUTTY:
            {
                goto next;
                break;
            }
            /* need to send data in the 2 following status. */
            case    RDA_RUNNING:
            case    RDA_BLOCKED:
            {
                /* fall through */
                break;
            }
        }
        
         /* resend timeout packages not finished, complete it first. */
        resend_changed = fwd_resend_timeout_pkg(control, d_queue, curr_socket, &force_drop);
        if (resend_changed)
        {
            changed = true;
        }

        /* if we need resend, finish resend it first, skip send. */
        if (d_queue->resend_package_done)
        {
            send_changed = fwd_send_pkgs(control, d_queue, curr_socket, &force_drop);
            if (send_changed)
            {
                changed = true;
            }
        }

        /* check whether we have been timeout */
        if (rda_status_timeout(&d_queue->status) && d_queue->peer_free_slot_num < RDA_BUFFER_SND_QUEUE_LEN)
        {
            ret = fwd_request_server_status(curr_socket, rda_id, RDA_SERVER_STATUS, d_queue->controller_idx, &force_drop);
            if (network_bad_socket == ret)
            {
                forwarder_thread_logger(LOG,
                                        FORWARDER_PREFIX "thread %ld sender[%d]: fwd_send_data_loop receiver no longer running, force drop, rda_id %lu, dst_controller_idx %d, dst_node_id %d, used_socket %d, ret %d",
                                        pthread_self(), control->id, rda_id, d_queue->controller_idx, node_id, curr_socket->socket, ret);
                goto next;
            }
            else if (network_need_retry == ret)
            {
                forwarder_thread_logger(LOG,
                                        FORWARDER_PREFIX "thread %ld sender[%d]: fwd_send_data_loop forwarder_request_server_status need retry rda_id %lu, dst_controller_idx %d, dst_node_id %d, used_socket %d, ret %d",
                                        pthread_self(), control->id, rda_id, d_queue->controller_idx, node_id, curr_socket->socket, ret);
                goto next;
            }

            /* update timestamp */
            rda_record_status_duration(&d_queue->status, d_queue->rda_id);
            rda_status_touch(&d_queue->status);
            if (g_fwd_dump_pkg_to_file)
            {
                forwarder_thread_logger(LOG,
                                        FORWARDER_PREFIX "thread %ld sender[%d]: %s timeout, fwd_send_data_loop fwd_request_server_status: RDA_SERVER_STATUS success, rda_id %lu, dst_controller_idx %d, dst_node_id %d, peer_free_slot_num %d buffer_queue data size %d, used_socket %d",
                                        pthread_self(), control->id, g_rda_status_names[get_rda_status(&d_queue->status)],rda_id, d_queue->controller_idx, node_id, d_queue->peer_free_slot_num, fwd_queue_data_length(d_queue->buffer_queue), curr_socket->socket);
            }
        }

next:
        fwd_stat_rda_queue(d_queue, control);
        /* validate the timestamp */
        if (d_queue != NULL)
        {
            if (force_drop)
            {
                /* we encountered the bad connection. */
                set_rda_status(&d_queue->status, RDA_BAD_CONNECTION);

                if (g_fwd_dump_pkg_to_file)
                {
                    forwarder_thread_logger(LOG, FORWARDER_PREFIX "thread %ld sender[%d]: %s, create FWD_OP_RESET, rda_id %ld, dst_controller_idx %d, dst_node_id %d",
                                                pthread_self(), control->id, force_drop ? "receiver no longer running" : "reach max idle time", rda_id,  d_queue->controller_idx, node_id);
                }

                if (fwd_push_queue(control->data_chan_op_queue, FWD_OP_RESET, rda_id, node_id, d_queue) < 0)
                {
                    forwarder_thread_logger(LOG, FORWARDER_PREFIX "thread %ld sender[%d]: %s, fwd_push_queue FWD_OP_RESET, rda_id %ld, dst_controller_idx %d, dst_node_id %d",
                                            pthread_self(), control->id, force_drop ? "receiver no longer running" : "reach max idle time failed", rda_id, d_queue->controller_idx, node_id);
                }
            }
        }
		
        /* get the next elem in the map */
        if (0 != map_iter_next(control->data_queue_map))
        {
            /* end of the map */
            break;
        }
    }

    /* handle response packages */
    rcv_changed = fwd_handle_sender_response(control);
    if (rcv_changed)
    {
        changed = true;
    }
    return changed;
}

/* resend timeout packages */
static bool fwd_send_pkgs(ServiceThreadControl *control, data_queue *d_queue, ConnMgr *curr_socket, bool *force_drop)
{
    bool changed = false;
    int ret = 0;      
    int network_ret = 0;   
    int saved_pkg_left_len = 0;
    uint32_t size = 0;
	uint32_t snd_len = 0;
    rda_dsm_frame *frame = NULL;
    char *data = NULL;
	FWDPackage *send_pkg = NULL;

    /* init sent flag to indicate we have not sent anything. */
    *force_drop = false;
    frame = d_queue->frame;
    if (frame == NULL)
    {
        return false;
    }

    /* we need ensure every package has beed sent complete. */
    ret = fwd_retry_last_package(curr_socket, d_queue, control);
    if (ret != network_ret_ok)
    {
        return false;
    } 

    /* get new data and send data until send all */
    data = (char *)rda_frame_get_data_off(frame, &size);
    while (size > 0 && fwd_queue_free_length(d_queue->buffer_queue))
    {
        snd_len = size >= FWD_MAX_DATA_PKG_SIZE ? FWD_MAX_DATA_PKG_SIZE: size; 
        /* 0 is an invalid sequence number, increase the sequence number first. */
        send_pkg = fwd_make_package(RDA_PUSH_DATA, 
                                my_node_id, 
                                d_queue->controller_idx,
                                d_queue->rda_id, 
                                d_queue->send_offset, 
                                snd_len, 
                                data, 
                                d_queue->seq);
        if (NULL == send_pkg)
        {
            forwarder_thread_logger(DEBUG1,
                                    FORWARDER_PREFIX "thread %ld sender[%d], fwd_send_data_loop rda_id %ld, dst_controller_idx %d, dst_node_id %d, make package failed, snd_len:%d, seq:%ld",
                                    pthread_self(), control->id, d_queue->rda_id, d_queue->controller_idx, d_queue->node_id, snd_len, d_queue->seq);
            return changed;
        }

        /* send the data. */
        
        if (g_fwd_dump_pkg_to_file)
        {
            saved_pkg_left_len = send_pkg->pkg_left_len;
        }

        network_ret = fwd_client_send_package(curr_socket, send_pkg);
        if (network_bad_socket == network_ret)
        {
            forwarder_thread_logger(LOG,
                    FORWARDER_PREFIX "thread %ld sender[%d]: fwd_send_data_loop send package rda_id %ld, dst_controller_idx %d, dst_node_id %d, seq %lu, ack_seq %lu, send_offset %lu, snd_size %d, used_socket %d failed",
                    pthread_self(), control->id, d_queue->rda_id, d_queue->controller_idx, d_queue->node_id, d_queue->seq, d_queue->ack_seq, d_queue->send_offset, snd_len, curr_socket->socket);    
            /* socket error, drop the rda file */
            *force_drop = true;
            return changed;
        }
        else
        {
            d_queue->bytes_sent += send_pkg->pkg_len;
            d_queue->pkgs_sent++;
            if (g_fwd_dump_pkg_to_file)
            {
                forwarder_thread_logger(LOG,
                    FORWARDER_PREFIX "thread %ld sender[%d]: fwd_send_data_loop send package rda_id %ld, dst_controller_idx %d, dst_node_id %d, seq %lu, ack_seq %lu, send_offset %lu, snd_size %d, used_socket %d succeed",
                    pthread_self(), control->id, d_queue->rda_id, d_queue->controller_idx, d_queue->node_id, d_queue->seq, d_queue->ack_seq, d_queue->send_offset, snd_len, curr_socket->socket);
            }
        }
        
        /* push the package into buffer queue. */
        ret = fwd_queue_push_pkg(d_queue->buffer_queue, send_pkg, FWD_RDA_SENDER);
        if (ret)
        {
            /* impossible, we have ensured the queue has space. */
            abort();
        }
            
        /* increase rda frame offset after package is pushed into buffer queue. */
        rda_frame_inc_data_off(frame, snd_len);

        /* dump data if needed */
        if (g_fwd_dump_pkg_to_file)
        {
            fwd_dump_pkg_to_file(send_pkg, d_queue->debug_fp, saved_pkg_left_len);
        }

        /* increase package sequence number */
        d_queue->seq++;
        d_queue->send_offset += snd_len; 

        if (network_need_retry == network_ret)
        {
            return changed;
        }

        changed = true;

        size -= snd_len;
        data += snd_len;

        /* update timestamp */
        rda_record_status_duration(&d_queue->status, d_queue->rda_id);
        rda_status_touch(&d_queue->status);
    }
    return changed;
}


/* resend timeout packages */
static bool fwd_resend_timeout_pkg(ServiceThreadControl *control, data_queue *d_queue, ConnMgr *curr_socket, bool *force_drop)
{
    bool   changed = false;
    int    network_ret = 0;   
    int    node_id = 0;
    int64  rda_id = 0;
    uint64 latest_send = 0;
    uint64 oldest_send = 0;
    uint64 next_send = 0;
	FWDPackage *send_pkg = NULL;
    FWDPackage *first_pkg = NULL;
#ifdef __ENABLE_RDA_MD5__
    char md5[STR_MD5_LEN];
#endif

    node_id = d_queue->node_id;
    rda_id  = d_queue->rda_id;
    *force_drop = false;
    /* check latest ack package sequence and resend the packages after ack. */
    fwd_queue_get_first_seq(d_queue->buffer_queue, &oldest_send);
    first_pkg = fwd_queue_get_package(d_queue->buffer_queue, oldest_send);
    if (NULL == first_pkg)
    {
        /* set resend_package_done to continue sending package from frame when buffer_queue is empty */
        d_queue->resend_package_done = true;
        return changed;
    }

    /* first package has been timeout */
    if (timeval_useconds_diff(&first_pkg->timestamp) >= rda_status_timeouts[RDA_BLOCKED])
    {
        /* timeout, if server has more space, resend the timed out packages. */
        if (fwd_queue_data_length(d_queue->buffer_queue) > 0 && d_queue->peer_free_slot_num > 0)
        {
            set_rda_status(&d_queue->status, RDA_RUNNING);
            if (g_fwd_dump_pkg_to_file)
            {
                forwarder_thread_logger(LOG,
                                        FORWARDER_PREFIX "thread %ld sender[%d]: timeout, fwd_resend_timeout_pkg begin to resend, set RDA_RUNNING, rda_id %lu, dst_controller_idx %d, dst_node_id %d, ack_seq %ld, peer_free_slot_num %d buffer_queue size %d, used_socket %d",
                                        pthread_self(), control->id, rda_id, d_queue->controller_idx, node_id, d_queue->ack_seq, d_queue->peer_free_slot_num, fwd_queue_data_length(d_queue->buffer_queue), curr_socket->socket);
            }

            /* resend the package need resend */
            next_send = d_queue->ack_seq + 1;
            fwd_queue_get_last_seq(d_queue->buffer_queue, &latest_send);
            if (next_send <= latest_send)
            {					
                do 
                {
                    send_pkg = fwd_queue_get_package(d_queue->buffer_queue, next_send);
                    if (send_pkg)
                    {
                        if (g_fwd_enable_fwd_md5)
                        {
                            memset(md5, 0, STR_MD5_LEN);
                            pg_md5_hash(send_pkg->data, send_pkg->pkg_len, md5);
                        }

                        /* init package pkg_left_len to the real size and start to resend. */
                        if (0 == send_pkg->pkg_left_len)
                        {
                            send_pkg->pkg_left_len = send_pkg->pkg_len;
                        }
                        
                        /* resend package. */
                        network_ret = fwd_client_send_package(curr_socket, send_pkg);
                        if (network_bad_socket == network_ret)
                        {
                            forwarder_thread_logger(LOG,
                                                    FORWARDER_PREFIX "thread %ld sender[%d]: fwd_resend_timeout_pkg  failed to send package with %d-bytes data, rda_id %ld, dst_controller_idx %d, dst_node_id %d, seq %lu, ack_seq %lu, next_send %lu, used_socket %d",
                                                    pthread_self(), control->id, send_pkg->pkg_len - send_pkg->pkg_left_len, rda_id, d_queue->controller_idx, node_id, d_queue->seq, d_queue->ack_seq, d_queue->send_offset, curr_socket->socket);
                            *force_drop = true;
                            return false;
                        }
                        else
                        {
                            if (0 == send_pkg->pkg_left_len)
                            {
                                d_queue->pkgs_resent++;
                                /* touch package, disable timeout */
                                touch_package(send_pkg);
                                d_queue->resend_package_done = true;
                            }                  
                            else
                            {
                                /* without disable timeout, need resent first pkg last time */
                                d_queue->resend_package_done = false;
                            }
                            d_queue->bytes_resent += send_pkg->pkg_len;;
                            
                            if (network_need_retry == network_ret)
                            {
                                *force_drop = false;
                                return changed;
                            }                           
                            next_send++;
                            changed = true;
                            if (g_fwd_dump_pkg_to_file)
                            {
                                forwarder_thread_logger(LOG,
                                        FORWARDER_PREFIX "thread %ld sender[%d]: fwd_resend_timeout_pkg ok to resent timeout package success with %d-bytes data, rda_id %ld, dst_controller_idx %d, dst_node_id %d, pkg_seq %lu, ack_seq %lu, send_offset %lu, used_socket %d",
                                        pthread_self(), control->id, send_pkg->pkg_len, rda_id, d_queue->controller_idx, node_id, send_pkg->pkg_seq, d_queue->ack_seq, d_queue->send_offset, curr_socket->socket);
                            }
                        }
                    }
                    else
                    {
                        forwarder_thread_logger(LOG,
                                        FORWARDER_PREFIX "thread %ld sender[%d]: fwd_queue_get_package failed, rda_id %ld, dst_controller_idx %d, dst_node_id %d, next_seq %lu, ack_seq %lu",
                                        pthread_self(), control->id, rda_id, d_queue->controller_idx, node_id, next_send, d_queue->ack_seq);
                        break;
                    }
                } while(--d_queue->peer_free_slot_num);
            }
        }
    }
    return changed;
}
/* Handle server response. */
static bool fwd_handle_sender_response(ServiceThreadControl *control)
{
    bool  changed        = false;
    bool  change_temp    = false;
    int   ret = 0;
    int   node_id = -1;
    int   index   = 0;
    int64 rda_id        = 0;
    data_queue *d_queue = NULL;
    ConnMgr *curr_socket = NULL;

    if (g_fwd_dn_num <= 0)
    {
        return false;
    }
    map_begin_iter(control->data_queue_map);

    /* get replies from receiver and handle. */
    while (0 == map_iter_key(control->data_queue_map, &rda_id, &node_id))
    {
        /* get the data queue */
        d_queue = (data_queue *)(map_iter_value(control->data_queue_map));
        if (NULL == d_queue)
        {
            goto response_recv_next;
        }
        
        if (node_id == my_node_id)
        {
            goto response_recv_next;
        }

        /* get socket */
        if (g_fwd_is_during_socket_reload)
        {
            goto response_recv_next;
        }

        if (node_id < 0)
        {
            goto response_recv_next;
        }

        /* skip frame that status not ok */
        if (rda_get_frame_status(d_queue->frame) != RDA_FRAME_STATUS_OK)
        {
            goto response_recv_next;
        }
        
        index = fwd_node_to_index(node_id);
        curr_socket = &g_client_socket_array[index][rda_id % g_rda_sender_socket_num_per_node];

        /* handle response as soon as possble */
        do
        {
            ret = fwd_client_handle_responses(curr_socket, control, &change_temp);
            if (ret == network_bad_socket)
            {
                /* we encountered the bad connection. */
                set_rda_status(&d_queue->status, RDA_BAD_CONNECTION);

                if (g_fwd_dump_pkg_to_file)
                {
                    forwarder_thread_logger(LOG, FORWARDER_PREFIX "fwd_handle_sender_response thread %ld sender[%d]: create FWD_OP_RESET, rda_id %ld, dst_controller_idx %d, dst_node_id %d",
                                                pthread_self(), control->id, rda_id,  d_queue->controller_idx, node_id);
                }

                if (fwd_push_queue(control->data_chan_op_queue, FWD_OP_RESET, rda_id, node_id, d_queue) < 0)
                {
                    forwarder_thread_logger(LOG, FORWARDER_PREFIX "fwd_handle_sender_response thread %ld sender[%d]: fwd_push_queue FWD_OP_RESET, rda_id %ld, dst_controller_idx %d, dst_node_id %d",
                                            pthread_self(), control->id, rda_id, d_queue->controller_idx, node_id);
                }
                break;
            }
        }while(ret == network_ret_ok);

        /* set change flag*/
        changed = !changed ? change_temp : changed;
        
response_recv_next:
        fwd_stat_rda_queue(d_queue, control);
        if (0 != map_iter_next(control->data_queue_map))
        {
            /* end of the map */
            break;
        }
    }
    return changed;
}

static bool
copy_pkg_to_frame(FWDPackage *pkg, rda_dsm_frame *frame)
{
    uint32 data_len  = 0;
    uint32 buf_len   = 0;
    uint32 write_len = 0;
    uint8_t *ptr	 = NULL;

    if (0 == pkg->pkg_left_len)
    {
        return true;
    }

    /* skip package header */
    if (pkg->pkg_len == pkg->pkg_left_len)
    {
        pkg->pkg_left_len = PayloadLen(pkg);
    }
	
    data_len = PayloadLen(pkg);
    while (rda_frame_free_space(frame) > 0)
    {
        /* copy package buffer to frame */
        buf_len = 0;
        ptr = rda_frame_get_write_off(frame, &buf_len);
        write_len = buf_len > pkg->pkg_left_len ? pkg->pkg_left_len : buf_len;

        memcpy(ptr, (char*)pkg->data + (data_len - pkg->pkg_left_len), write_len);

        rda_frame_inc_write_off(frame, write_len);
        pkg->pkg_left_len = pkg->pkg_left_len - write_len;

        if (pkg->pkg_left_len == 0)
        {
            return true;
        }
    }

    return false;
}


static inline void fwd_check_pkg_sequence(FWDPackage *pkg, FILE *file)
{
    long write_offset = 0;  

    write_offset = ftell(file);
    if (write_offset != pkg->pkg_offset + PayloadLen(pkg) - pkg->pkg_left_len)
    {
        abort();
    }
}

static inline void fwd_dump_pkg_to_file(FWDPackage *pkg, FILE *file, int saved_pkg_left_len)
{
    int  saved_header_left_len = 0;
    int  sent_data_len = 0;

    /* only dump payload data, skip header */
    if (saved_pkg_left_len > PayloadLen(pkg))
    {
        if (pkg->pkg_left_len < PayloadLen(pkg))
        {
            saved_header_left_len = saved_pkg_left_len - PayloadLen(pkg);
            sent_data_len  = saved_pkg_left_len - pkg->pkg_left_len - saved_header_left_len;
            debug_out_file_write_buffer(file, sent_data_len, pkg->data);
        }
    }
    else
    {
        debug_out_file_write_buffer(file, saved_pkg_left_len - pkg->pkg_left_len, pkg->data + PayloadLen(pkg) - saved_pkg_left_len);
    }
}


/*
 * Loop to receive data and write to frame.
 */
bool
fwd_receive_data_loop(ServiceThreadControl *control)
{
    int            ret         = 0;
	int            node_index  = 0;
    int            process_cnt = 0;
    int            ctl_index   = 0;
    int            saved_pkg_left_len = 0;
	int            free_size          = 0;
	int            controller_status  = 0;
    int            used_num           = 0;
    int            node_id            = 0;
	uint32         queue_free_size    = 0;
	bool           changed            = true;
    data_queue    *d_queue            = NULL;
    NodeRDASlots  *rda_control_group  = NULL;
	FWDQueue      *buf_queue          = NULL;
    RDAController *rda_ctrl           = NULL;
    FWDPackage    *pkg                = NULL;
    
    changed = false;

    if (!g_fwd_dn_rcv_rda_slots_group_ready)
    {
        return false;
    }

    for (node_index = 0; node_index < g_total_node_num; node_index++)
    {
    	/* locate queue group, skip myself */
        rda_control_group = g_fwd_server_rda_slots_group[node_index];
        if (NULL == rda_control_group)
        {
            continue;
        }

        node_id = fwd_index_to_node_id(node_index);
    	/* only process those belongs to me */
        if (route_to_node(node_id) != control->id)
        {
            continue;
        }

        /* now, handle all rda-controller in this group */
        SpinLockAcquire(&rda_control_group->used_cnt_lock);
		used_num = rda_control_group->used_cnt;
        SpinLockRelease(&rda_control_group->used_cnt_lock);
		if (0 == used_num)
		{
		    continue;
		}

        process_cnt = 0;
        for (ctl_index = 0; ctl_index < rda_control_group->size && process_cnt < used_num; ctl_index++)
        {
            rda_ctrl = &rda_control_group->rda_slots[ctl_index];
    		controller_status = rda_ctrl->ctl_status;

            if (controller_status != RDACtlStatus_Running)
            {
                continue;
            }

            process_cnt++;
            /* no socket yet, no need to handle */
            if (-1 == rda_ctrl->socket)
            {
                continue;
            }

            /* locator one rda-controller */
            d_queue = (data_queue *)(map_get(control->data_queue_map, rda_ctrl->rda_id, node_id));
            if (NULL == d_queue)
            {
                forwarder_thread_logger(LOG,
                                        FORWARDER_PREFIX "thread %ld, receiver[%d] fwd_receive_data_loop, rda_id %ld, src_node_id %d, unexpected null queue.",
                                        pthread_self(), control->id, rda_ctrl->rda_id, node_id);
                continue;
            }

            /* skip frame that status not ok */
            if (rda_get_frame_status(d_queue->frame) != RDA_FRAME_STATUS_OK)
            {
                continue;
            }
            
            /* check socket status */
            ret = fwd_server_socket_get_status(rda_ctrl->socket);
            if (ret != network_ret_ok)
            {
                /* socket has been closed, we need reset the data queue. */
                rda_ctrl->socket = -1;
                set_rda_status(&rda_ctrl->data_queue->status, RDA_BAD_CONNECTION);
                if (fwd_push_queue(control->data_chan_op_queue, FWD_OP_RESET, rda_ctrl->rda_id, rda_ctrl->node_id, rda_ctrl->data_queue) < 0)
                {
                    forwarder_thread_logger(LOG, FORWARDER_PREFIX "receiver[%d]: fwd_push_queue FWD_OP_RESET, rda_id %ld, src_node_id %d failed",
                                                 control->id, rda_ctrl->rda_id, rda_ctrl->node_id);
                }
                else
                {
                    rda_ctrl->ctl_status = RDACtlStatus_Exiting;
                }
                continue;
            }
        
            free_size = rda_frame_free_space(d_queue->frame);
            if (free_size <= 0)
            {
                continue;
            }

            /* fetch one package from queue, copy to frame here */
            buf_queue = (FWDQueue *)rda_ctrl->data_queue->buffer_queue;
            while (!fwd_queue_is_empty(buf_queue))
            {
                pkg = fwd_queue_sniffer(buf_queue);
                if (NULL == pkg)
                {
                    forwarder_thread_logger(LOG,
                                            FORWARDER_PREFIX "thread %ld, receiver[%d] fwd_receive_data_loop, rda_id %ld, src_node_id %d, rda_ctrl group %d, ctl_index %d fwd_queue_sniffer failed.",
                                            pthread_self(), control->id, rda_ctrl->rda_id, node_index, rda_ctrl->group_id, rda_ctrl->controller_id);
                    break;
                }

                /* save left length */
                if (g_fwd_dump_pkg_to_file)
                {
                    saved_pkg_left_len = pkg->pkg_left_len;
                }

                if (copy_pkg_to_frame(pkg, d_queue->frame))
                {
                    if (g_fwd_dump_pkg_to_file)
                    {
                        queue_free_size = fwd_queue_free_length(rda_ctrl->data_queue->buffer_queue);
                        forwarder_thread_logger(LOG,
                                                FORWARDER_PREFIX "thread %ld, receiver[%d] fwd_receive_data_loop consume data succeed, rda_id %ld, src_node_id %d, controller_id %d, write package seq:%ld , pkg_offset %ld, pkg_size %d, pkg_left_len %d, queue_free_size %d.",
                                                pthread_self(), control->id, rda_ctrl->rda_id, node_index, rda_ctrl->controller_id, pkg->pkg_seq, pkg->pkg_offset, pkg->pkg_len, pkg->pkg_left_len, queue_free_size);

                        /* only dump payload data, skip header */
                        fwd_dump_pkg_to_file(pkg, buf_queue->debug_fp, saved_pkg_left_len);
                        fwd_check_pkg_sequence(pkg, buf_queue->debug_fp);
                    }

                    rda_record_server_package_duration(pkg);
                    fwd_queue_drop_pkg(buf_queue, pkg->pkg_seq);
                    changed = true;
                }
                else
                {
                    /* not enough space to push package */
                    if (g_fwd_dump_pkg_to_file)
                    {
                        queue_free_size = fwd_queue_free_length(rda_ctrl->data_queue->buffer_queue);
                        forwarder_thread_logger(LOG,
                                                FORWARDER_PREFIX "thread %ld, receiver[%d] fwd_receive_data_loop consume data incomplete for lack of space, rda_id %ld, src_node_id %d, controller_id %d, write package seq:%ld , pkg_offset %ld, pkg_size %d, pkg_left_len %d, queue_free_size %d.",
                                                pthread_self(), control->id, rda_ctrl->rda_id, node_index, rda_ctrl->controller_id, pkg->pkg_seq, pkg->pkg_offset, pkg->pkg_len, pkg->pkg_left_len, queue_free_size);

                        fwd_dump_pkg_to_file(pkg, buf_queue->debug_fp, saved_pkg_left_len);
                        fwd_check_pkg_sequence(pkg, buf_queue->debug_fp);
                    }
                    changed = true;
                    break;
                }
            }

            /* stat the queue */
            fwd_stat_rda_queue(d_queue, control);
       }
        
    }
	
    return changed;
}

/*
 * Drain all elements in the map of cleaner.
 */
bool
fwd_cleaner_drain_data_loop(ServiceThreadControl *control)
{
    bool     done     = false;
    int64    rda_id   = 0;
    int64    p_rda_id = 0;
    int      node_id    = 0;
    uint32_t ref_cnt    = 0;
    mmap_info *m_info   = NULL;
    

    forwarder_thread_logger(LOG, FORWARDER_PREFIX "%s[%d]: start draining ...", g_fwd_thread_type_names[control->type], control->id);

    done = true;
    map_begin_iter(control->data_queue_map);
    while (0 == map_iter_key(control->data_queue_map, &rda_id, &node_id))
    {
        if (control->type == FWD_THREAD_CLEANER)
        {
            m_info = (mmap_info *)map_iter_value(control->data_queue_map);
            p_rda_id = rda_id;
            if (m_info == NULL)
            {
                map_erase(control->data_queue_map, rda_id, -1);
                forwarder_thread_logger(DEBUG1,
                    FORWARDER_PREFIX "cleanner[%d]: draining, mmap info not found, erase it directly, rda_id %ld",
                    control->id, p_rda_id);
            }
            else
            {
                ref_cnt = m_info->ref_cnt;
                map_erase(control->data_queue_map, rda_id, -1);
                dsm_rda_detach_and_unlink(p_rda_id, &m_info->mmap_addr, &m_info->mmap_size);
                delete_mmap_info(m_info);
                forwarder_thread_logger(LOG,
                    FORWARDER_PREFIX "cleanner[%d]: draining, unmap and unlink no matter referenced or not, rda_id %ld, ref_cnt %d",
                    control->id, p_rda_id, ref_cnt);
            }
        }
        else
        {
            abort();
        }

        /* get the next elem in the map */
        if (0 != map_iter_next(control->data_queue_map))
        {
            break;
        }
    }
    return done;
}

/*
 * Drain all elements in the map.
 */
bool
fwd_data_thread_drain_loop(ServiceThreadControl *control)
{
    int32 ret      = 0;
    int64 rda_id   = 0;
    int node_id    = 0;
    bool done = false;

    forwarder_thread_logger(LOG, FORWARDER_PREFIX "%s[%d]: start draining ...", g_fwd_thread_type_names[control->type], control->id);

    done = true;
    map_begin_iter(control->data_queue_map);
    while (0 == map_iter_key(control->data_queue_map, &rda_id, &node_id))
    {
        if (control->type == FWD_THREAD_SENDER || control->type == FWD_THREAD_RECEIVER)
        {
            ret = fwd_push_queue(control->data_chan_op_queue, FWD_OP_DROP, rda_id, node_id, NULL);
            if (ret < 0)
            {
                done = false;
                forwarder_thread_logger(DEBUG1, FORWARDER_PREFIX "%s[%d]: draining, fwd_push_queue create drop-request failed, rda_id %ld, node_id %d",
                                    g_fwd_thread_type_names[control->type], control->id, rda_id, node_id);
            }
            else
            {
                forwarder_thread_logger(DEBUG1, FORWARDER_PREFIX "%s[%d]: draining, create drop-request succeed, rda_id %ld, node_id %d",
                                    g_fwd_thread_type_names[control->type], control->id, rda_id, node_id);
            }
            
        }

        /* get the next elem in the map */
        if (0 != map_iter_next(control->data_queue_map))
        {
            break;
        }
    }
    return done;
}
/*
 * When create thread failed, tell other thread to quit.
 */
void
fwd_clean_data_thread_main()
{
    int32 threadid = 0;
    ServiceThreadControl *thread = NULL;

    if (NULL == g_fwd_data_control)
    {
        return;
    }

    g_fwd_data_control->status = FWD_DRAINING;
    for (threadid = 0; threadid < g_fwd_data_control->sender_num; threadid++)
    {
        thread = &g_fwd_data_control->sender_control[threadid];
        /* Set drain and quit flag and tell sender to quit. */
        if (thread->thread_running)
        {
            if (!thread->thread_draining)
            {
                thread->thread_need_drain = true;
                ThreadSemaDown(&thread->drain_sem);
            }
            thread->thread_need_quit = true;
            ThreadSemaDown(&thread->quit_sem);
        }
    }

    for (threadid = 0; threadid < g_fwd_data_control->receiver_num; threadid++)
    {
        thread = &g_fwd_data_control->receiver_control[threadid];
        /* Set drain quit flag and tell receiver to quit. */
        if (thread->thread_running)
        {
            if (!thread->thread_draining)
            {
                thread->thread_need_drain = true;
                ThreadSemaDown(&thread->drain_sem);
            }
            thread->thread_need_quit = true;
            ThreadSemaDown(&thread->quit_sem);
        }
    }

    thread = g_fwd_data_control->cleanner_control;
    /* Set quit flag and tell receiver to quit. */
    if (thread->thread_running)
    {
        if (!thread->thread_draining)
        {
            thread->thread_need_drain = true;
            ThreadSemaDown(&thread->drain_sem);
        }
        thread->thread_need_quit = true;
        ThreadSemaDown(&thread->quit_sem);
    }

    g_fwd_data_control->status = FWD_QUIT;
}

FWDPackage *fwd_make_package(int8 type, int32 nodeid, 
                         int32 ctrl_index, int64 rda_id, 
                         uint64 pkg_offset, uint32 pkg_len, 
                         void *data, uint64 seq)
{
	FWDPackage *pkg = NULL;
	pkg = (FWDPackage*)fwd_malloc(sizeof(FWDPackage) + pkg_len);
	if (NULL == pkg)
	{
		return NULL;
	}

    pkg->protocol       = type;	
    pkg->pkg_len        = pkg_len + sizeof(FWDPackage);
    pkg->pkg_left_len   = pkg->pkg_len;
	pkg->pkg_offset     = pkg_offset;
	pkg->pkg_seq        = seq;
    pkg->node_id        = nodeid;
    pkg->controller_idx = ctrl_index;
    pkg->rda_id         = rda_id;
    set_pkg_version(pkg);

    if (pkg_len)
    {
        memcpy(pkg->data, data, pkg_len);
#ifdef __ENABLE_RDA_MD5__        
        if (g_fwd_enable_fwd_md5)
        {
            memset(pkg->md5, 0, STR_MD5_LEN);
            pg_md5_hash(pkg->data, pkg_len, pkg->md5);
        }
#endif        
    }
    
	gettimeofday(&pkg->timestamp, NULL);
	return pkg;
}

void drop_package_real(FWDPackage *pkg, char *file, int lineno)
{
    if (g_fwd_dump_pkg_to_file)
    {
        forwarder_thread_logger(LOG, FORWARDER_PREFIX "thread:%ld fwd_drop_package %p file:%s, lineno:%d ", pthread_self(), pkg, file, lineno);
    }
    fwd_free(pkg);
}

inline void touch_package(FWDPackage *pkg)
{
    gettimeofday(&pkg->timestamp, NULL);
}

/*
 * forwarder push data to other datanodes, return none zero when send failed.
 */
static int
fwd_client_send_package(ConnMgr *mgr, FWDPackage *request)
{
    int         nneed_bytes = 0;
    int         ret         = 0;
    /* Bad socket, no need to send. */
    if (-1 == mgr->socket)
    {
        return network_bad_socket;
    }

    /* send buffered data */
    while (mgr->datalen > mgr->sendlen)
    {
        nneed_bytes = mgr->datalen - mgr->sendlen;
        ret = write(mgr->socket, mgr->sendpkg + mgr->sendlen, nneed_bytes);
        if (ret > 0)
        {
            mgr->sendlen += ret;
        }
        else if (0 == ret)
        {
            /* if OS buffer is full, wait then retry. */
            pg_usleep(1);
        }
        else
        {
            /* Anything except EAGAIN/EWOULDBLOCK/EINTR is trouble */
			switch (errno)
			{
#ifdef EAGAIN
				case EAGAIN:
                    return network_need_retry;
#endif
#if defined(EWOULDBLOCK) && (!defined(EAGAIN) || (EWOULDBLOCK != EAGAIN))
				case EWOULDBLOCK:
                    return network_need_retry;
#endif
				case EINTR:
					continue;

				default:
                    /* Close the bad conenction, wait for the main thread reconnect. */
                    close(mgr->socket);
                    mgr->socket = -1;
					return network_bad_socket;
			}
        }
    }
 
    /* all data have been sent, clear counter. */
    mgr->datalen = 0;
    mgr->sendlen = 0;
    if (0 == request->pkg_left_len)
    {
        return network_ret_ok;
    }
    
    ret = fwd_client_send_pkg_header(mgr, request);
    if (network_bad_socket == ret)
    {
        close(mgr->socket);
        mgr->socket = -1;
		return network_bad_socket;
    }
    
    if (request->pkg_len - request->pkg_left_len < MinSendNetLen)
    {
        abort();
    }

    /* have uncomplete data, copy into buffer. */
    if (mgr->datalen > 0)
    {
        if (request->pkg_left_len)
        {
            memcpy(mgr->sendpkg + mgr->datalen, (char*) request + request->pkg_len - request->pkg_left_len, request->pkg_left_len); 
            mgr->datalen += request->pkg_left_len;
            request->pkg_left_len = 0;
        }
        return network_need_retry;
    }
    
    /* send header successfully, send the body. */
    while (request->pkg_left_len)
    {

        ret = write(mgr->socket, 
                    (char*) request + request->pkg_len - request->pkg_left_len, request->pkg_left_len);
        if (ret > 0)
        {
            request->pkg_left_len -= ret;
        }
        else if (0 == ret)
        {
            /* if OS buffer is full, wait then retry. */
            pg_usleep(1);
        }
        else
        {
            /* Anything except EAGAIN/EWOULDBLOCK/EINTR is trouble */
			switch (errno)
			{
#if defined(EWOULDBLOCK) && (!defined(EAGAIN) || (EWOULDBLOCK != EAGAIN))
				case EWOULDBLOCK:
                /* buffer the leftover data */
                if (request->pkg_left_len > 0)
                {
                    memcpy(mgr->sendpkg, (char*) request + request->pkg_len - request->pkg_left_len, request->pkg_left_len); 
                    mgr->sendlen = 0;
                    mgr->datalen = request->pkg_left_len;
                    request->pkg_left_len = 0;
                }
                return network_need_retry;
#endif
#ifdef EAGAIN
				case EAGAIN:
                /* buffer the leftover data */
                if (request->pkg_left_len > 0)
                {
                    memcpy(mgr->sendpkg, (char*) request + request->pkg_len - request->pkg_left_len, request->pkg_left_len); 
                    mgr->sendlen = 0;
                    mgr->datalen = request->pkg_left_len;
                    request->pkg_left_len = 0;
                }
                return network_need_retry;
#endif
				case EINTR:
					continue;

				default:
                    /* Close the bad conenction, wait for the main thread reconnect. */
                    close(mgr->socket);
                    mgr->socket = -1;
					return network_bad_socket;
			}
        }
    }
    return network_ret_ok;
}

/*
 * forwarder send package header, return none zero when send failed.
 */
static int
fwd_client_send_pkg_header(ConnMgr *mgr, FWDPackage *request)
{
    int         ret    = 0;
    int         nneed_bytes = 0;
    int         sent_bytes  = 0;
    char        network_header[MinSendNetLen];

    /* hton transfer */
    sent_bytes = request->pkg_len - request->pkg_left_len;
    if (sent_bytes < MinSendNetLen)
    {
        fwd_encode_pkg_header(network_header, MinSendNetLen, request);
        do 
        {   
            nneed_bytes = MinSendNetLen - sent_bytes;
            ret = write(mgr->socket, 
                        (char*) network_header + sent_bytes, nneed_bytes);
            if (ret > 0)
            {
                request->pkg_left_len -= ret;
            }
            else if (0 == ret)
            {
                /* if OS buffer is full, wait then retry. */
                pg_usleep(1);
            }
            else
            {
                /* Anything except EAGAIN/EWOULDBLOCK/EINTR is trouble */
    			switch (errno)
    			{
#ifdef EAGAIN
    				case EAGAIN:
                        /* buffer the leftover data */
                        if (nneed_bytes > 0)
                        {
                            memcpy(mgr->sendpkg, (char*) network_header + sent_bytes, nneed_bytes); 
                            mgr->sendlen = 0;
                            mgr->datalen = nneed_bytes;
                            request->pkg_left_len -= nneed_bytes;
                        }
                        return network_ret_ok;
#endif
#if defined(EWOULDBLOCK) && (!defined(EAGAIN) || (EWOULDBLOCK != EAGAIN))
    				case EWOULDBLOCK:
                         /* buffer the leftover data */
                        if (nneed_bytes > 0)
                        {
                            memcpy(mgr->sendpkg, (char*) network_header + sent_bytes, nneed_bytes); 
                            mgr->sendlen = 0;
                            mgr->datalen = nneed_bytes;
                            request->pkg_left_len -= nneed_bytes;
                        }
                        return network_ret_ok;
#endif
    				case EINTR:
    					continue;

    				default:
                        /* Close the bad conenction, wait for the main thread reconnect. */
                        close(mgr->socket);
                        mgr->socket = -1;
    					return network_bad_socket;
    			}
            }
            sent_bytes = request->pkg_len - request->pkg_left_len;
        } while(sent_bytes < MinSendNetLen) ; 
    }    
    return network_ret_ok;
}

/*
 * encode a package header of network formats into buffer.
 */
static void
fwd_encode_pkg_header(char *network_header, int32 len, FWDPackage *request)
{
    int         offset = 0;
    uint32      u32    = 0;
    int64       dummy  = 0;

    /* hton transfer */
    if (len >= MinSendNetLen)
    {
        /*  
            int8                  protocol;
            int8                  major_version;
            int8                  minor_version;
            uint32				  pkg_len;	    
            uint32				  pkg_left_len; 
            int64                 rda_id;       
        	uint64				  pkg_offset;   
        	
        	uint64                pkg_seq;      

            int32                 node_id;
            int32                 controller_idx;
            uint32                pending_1;
            uint32                pending_2;    
        */

        /* protocol */
        network_header[0] = request->protocol;
        offset += 1;

        network_header[offset] = request->major_version;
        offset += 1;

        network_header[offset] = request->minor_version;
        offset += 1;

        /* pkg_len */
        u32 = htonl(request->pkg_len);
        memcpy(network_header + offset, (char*)&u32, sizeof(u32));
        offset += sizeof(u32);

        /* pkg_left_len */
        u32 = htonl(request->pkg_left_len);
        memcpy(network_header + offset, (char*)&u32, sizeof(u32));
        offset += sizeof(u32);

        /* rda_id */
        u32 = htonl((uint32)(0xFFFFFFFF & (request->rda_id>>32)));
        memcpy(network_header + offset, (char*)&u32, sizeof(u32));
        offset += sizeof(u32);

        
        u32 = htonl((uint32)(0xFFFFFFFF & (request->rda_id)));
        memcpy(network_header + offset, (char*)&u32, sizeof(u32));
        offset += sizeof(u32);

        /* pkg_offset */
        u32 = htonl((uint32)(0xFFFFFFFF & (request->pkg_offset>>32)));
        memcpy(network_header + offset, (char*)&u32, sizeof(u32));
        offset += sizeof(u32);

        
        u32 = htonl((uint32)(0xFFFFFFFF & (request->pkg_offset)));
        memcpy(network_header + offset, (char*)&u32, sizeof(u32));
        offset += sizeof(u32);

        /* pkg_seq */
        u32 = htonl((uint32)(0xFFFFFFFF & (request->pkg_seq>>32)));
        memcpy(network_header + offset, (char*)&u32, sizeof(u32));
        offset += sizeof(u32);

        
        u32 = htonl((uint32)(0xFFFFFFFF & (request->pkg_seq)));
        memcpy(network_header + offset, (char*)&u32, sizeof(u32));
        offset += sizeof(u32);

        /* node_id */
        u32 = htonl(request->node_id);
        memcpy(network_header + offset, (char*)&u32, sizeof(u32));
        offset += sizeof(u32);

        /* controller_idx */
        u32 = htonl(request->controller_idx);
        memcpy(network_header + offset, (char*)&u32, sizeof(u32));
        offset += sizeof(u32);

        /* pending_1 */
        u32 = htonl(request->pending_1);
        memcpy(network_header + offset, (char*)&u32, sizeof(u32));
        offset += sizeof(u32);

        /* pending_2 */
        u32 = htonl(request->pending_2);
        memcpy(network_header + offset, (char*)&u32, sizeof(u32));
        offset += sizeof(u32);

        
        /* timestamp */
        dummy = 0;
        memcpy(network_header + offset, (char*)&dummy, sizeof(int64));
        offset += sizeof(int64);

        dummy = 0;
        memcpy(network_header + offset, (char*)&dummy, sizeof(int64));
        offset += sizeof(int64);

        /* md5 */
#ifdef __ENABLE_RDA_MD5__
        memcpy(network_header + offset, (char*)request->md5, STR_MD5_LEN);
        offset += STR_MD5_LEN;
#endif

        if (offset != MinSendNetLen)
        {
            abort();
        }
   }
}

/*
 * encode a package header of network formats into buffer.
 */
static ResponsePackage*
fwd_decode_response_pkg(char *network_header, int32 len)
{
    int         ret    = 0;
    int         offset = 0;
    uint32      u32    = 0;
    int64		result;
	uint32		h32;
	uint32		l32;
    ResponsePackage *rsp = NULL;
    FWDPackage *request = NULL;

    rsp = (ResponsePackage*)fwd_malloc(sizeof(ResponsePackage));
    if (NULL == rsp)
    {
        return NULL;
    }

    request = (FWDPackage*)rsp;
    ret = fwd_decode_pkg_header(network_header, len, request);
    if (ret)
    {
        fwd_free(rsp);
        return NULL;
    }

    offset = MinSendNetLen;

    switch (rsp->header.protocol)
    {
        case RDA_PUSH_DATA_ACK:
        case RDA_SERVER_STATUS_ACK:
        {
            /*
                uint32      free_space;
                uint64      ack_seq;
                uint64      next_send;
                uint32      pending_1;
                uint32      pending_2;

            */
            /* free_space */
            memcpy((char*)&u32, network_header + offset, sizeof(u32));
            rsp->payload.datarsp.free_space = ntohl(u32);
            offset += sizeof(u32);

            /* ack_seq */
            memcpy((char*)&h32, network_header + offset, sizeof(h32));
            h32 = ntohl(h32);
            offset += sizeof(h32);

            memcpy((char*)&l32, network_header + offset, sizeof(l32));
            l32 = ntohl(l32);
            offset += sizeof(l32);


            result = h32;
    	    result <<= 32;
    	    result |= l32;

            rsp->payload.datarsp.ack_seq = result;

            /* next_send */
            memcpy((char*)&h32, network_header + offset, sizeof(h32));
            h32 = ntohl(h32);
            offset += sizeof(h32);

            memcpy((char*)&l32, network_header + offset, sizeof(l32));
            l32 = ntohl(l32);
            offset += sizeof(l32);


            result = h32;
    	    result <<= 32;
    	    result |= l32;

            rsp->payload.datarsp.next_send = result;

            /* pending_1 */
            memcpy((char*)&u32, network_header + offset, sizeof(u32));
            rsp->payload.datarsp.pending_1 = ntohl(u32);
            offset += sizeof(u32);

            /* pending_2 */
            memcpy((char*)&u32, network_header + offset, sizeof(u32));
            rsp->payload.datarsp.pending_2 = ntohl(u32);
            offset += sizeof(u32);
            break;
        }
        case RDA_INIT_STATUS_ACK:
        {
            /* pending_1 */
            memcpy((char*)&u32, network_header + offset, sizeof(u32));
            rsp->payload.rdastatrsp.pending_1 = ntohl(u32);
            offset += sizeof(u32);

            /* pending_2 */
            memcpy((char*)&u32, network_header + offset, sizeof(u32));
            rsp->payload.rdastatrsp.pending_2 = ntohl(u32);
            offset += sizeof(u32);
            break;
        }
        default:
        {
            abort();
            break;
        }
    }

    if (offset > len)
    {
        abort();
    }
    return rsp;
}

/*
 * encode a package header of network formats into buffer.
 */
static int
fwd_encode_response_pkg(char *network_header, int32 len, ResponsePackage *rsp)
{
    int         offset = 0;
    uint32      u32    = 0;
    FWDPackage *request = (FWDPackage*)rsp;

    fwd_encode_pkg_header(network_header, len, request);

    offset = MinSendNetLen;

    switch (rsp->header.protocol)
    {
        case RDA_PUSH_DATA_ACK:
        case RDA_SERVER_STATUS_ACK:
        {
            /*
                uint32      free_space;
                uint64      ack_seq;
                uint64      next_send;
                uint32      pending_1;
                uint32      pending_2;

            */
            /* free_space */
            u32 = htonl(rsp->payload.datarsp.free_space);
            memcpy(network_header + offset, (char*)&u32, sizeof(u32));
            offset += sizeof(u32);

            /* ack_seq */
            u32 = htonl((uint32)(0xFFFFFFFF & (rsp->payload.datarsp.ack_seq>>32)));
            memcpy(network_header + offset, (char*)&u32, sizeof(u32));
            offset += sizeof(u32);

            
            u32 = htonl((uint32)(0xFFFFFFFF & (rsp->payload.datarsp.ack_seq)));
            memcpy(network_header + offset, (char*)&u32, sizeof(u32));
            offset += sizeof(u32);

            /* next_send */
            u32 = htonl((uint32)(0xFFFFFFFF & (rsp->payload.datarsp.next_send>>32)));
            memcpy(network_header + offset, (char*)&u32, sizeof(u32));
            offset += sizeof(u32);

            
            u32 = htonl((uint32)(0xFFFFFFFF & (rsp->payload.datarsp.next_send)));
            memcpy(network_header + offset, (char*)&u32, sizeof(u32));
            offset += sizeof(u32);

            /* pending_1 */
            u32 = htonl(rsp->payload.datarsp.pending_1);
            memcpy(network_header + offset, (char*)&u32, sizeof(u32));
            offset += sizeof(u32);

            /* pending_2 */
            u32 = htonl(rsp->payload.datarsp.pending_2);
            memcpy(network_header + offset, (char*)&u32, sizeof(u32));
            offset += sizeof(u32);
            break;
        }
        case RDA_INIT_STATUS_ACK:
        {
            /* pending_1 */
            u32 = htonl(rsp->payload.rdastatrsp.pending_1);
            memcpy(network_header + offset, (char*)&u32, sizeof(u32));
            offset += sizeof(u32);

            /* pending_2 */
            u32 = htonl(rsp->payload.rdastatrsp.pending_2);
            memcpy(network_header + offset, (char*)&u32, sizeof(u32));
            offset += sizeof(u32);
            break;
        }
        default:
        {
            break;
        }
    }

    if (offset > len)
    {
        abort();
    }
    return 0;
}

/*
 * decode a package header of network formats.
 */
static int
fwd_decode_pkg_header(char *network_header, int32 len, FWDPackage *request)
{
    int         offset = 0;
    uint32      u32    = 0;
    int64		result;
	uint32		h32;
	uint32		l32;

    /* ntoh transfer */
    if (len >= MinSendNetLen)
    {

        /* 
            int8                  protocol;
            int8                  major_version;
            int8                  minor_version;
            uint32				  pkg_len;	    
            uint32				  pkg_left_len; 
            int64                 rda_id;       
        	uint64				  pkg_offset;   
        	
        	uint64                pkg_seq;      

            int32                 node_id;
            int32                 controller_idx;
            uint32                pending_1;
            uint32                pending_2;    
        */

        /* protocol */
        request->protocol = network_header[0];
        offset += 1;

        request->major_version = network_header[offset];
        offset += 1;

        request->minor_version = network_header[offset];
        offset += 1;
        
        /* pkg_len */
        memcpy((char*)&u32, network_header + offset, sizeof(u32));
        request->pkg_len = ntohl(u32);
        offset += sizeof(u32);

        /* pkg_left_len */
        memcpy((char*)&u32, network_header + offset, sizeof(u32));
        request->pkg_left_len = ntohl(u32);
        offset += sizeof(u32);

        /* rda_id */
        memcpy((char*)&h32, network_header + offset, sizeof(h32));
        h32 = ntohl(h32);
        offset += sizeof(h32);

        memcpy((char*)&l32, network_header + offset, sizeof(l32));
        l32 = ntohl(l32);
        offset += sizeof(l32);


        result = h32;
	    result <<= 32;
	    result |= l32;

        request->rda_id = result;

        /* pkg_offset */
        memcpy((char*)&h32, network_header + offset, sizeof(h32));
        h32 = ntohl(h32);
        offset += sizeof(h32);

        memcpy((char*)&l32, network_header + offset, sizeof(l32));
        l32 = ntohl(l32);
        offset += sizeof(l32);


        result = h32;
	    result <<= 32;
	    result |= l32;

        request->pkg_offset = result;

        /* pkg_seq */
        memcpy((char*)&h32, network_header + offset, sizeof(h32));
        h32 = ntohl(h32);
        offset += sizeof(h32);

        memcpy((char*)&l32, network_header + offset, sizeof(l32));
        l32 = ntohl(l32);
        offset += sizeof(l32);


        result = h32;
	    result <<= 32;
	    result |= l32;

        request->pkg_seq = result;

        /* node_id */
        memcpy((char*)&u32, network_header + offset, sizeof(u32));
        request->node_id = ntohl(u32);
        offset += sizeof(u32);

        /* controller_idx */
        memcpy((char*)&u32, network_header + offset, sizeof(u32));
        request->controller_idx = ntohl(u32);
        offset += sizeof(u32);

        /* pending_1 */
        memcpy((char*)&u32, network_header + offset, sizeof(u32));
        request->pending_1 = ntohl(u32);
        offset += sizeof(u32);

        /* pending_2 */
        memcpy((char*)&u32, network_header + offset, sizeof(u32));
        request->pending_2 = ntohl(u32);
        offset += sizeof(u32);

        /* set current timestamp */
        gettimeofday(&request->timestamp, NULL);
        offset += sizeof(int64);
        offset += sizeof(int64);

        /* md5 */
#ifdef __ENABLE_RDA_MD5__
        memcpy(request->md5, network_header + offset, STR_MD5_LEN);
        offset += STR_MD5_LEN;
#endif
        if (offset != MinSendNetLen)
        {
            abort();
        }
        return 0;
    }    
    return -1;
}


/* Can only be called in main thread. */
static int fwd_make_connect(ConnMgr *mgr)
{
    int ret;
    struct sockaddr_in server_address;    
    NodeDefinition *node_def;

    node_def = PgxcNodeGetDefinition(mgr->peernode);
    if (node_def)
    {
        server_address.sin_family = AF_INET;
        inet_pton(AF_INET, NameStr(node_def->nodehost), &server_address.sin_addr.s_addr);
        server_address.sin_port = htons(node_def->fwdserverport);

        if (mgr->socket != -1)
        {
            pfree(node_def);
            close(mgr->socket);
        }

        mgr->socket= socket(AF_INET, SOCK_STREAM, 0);
        if (mgr->socket == -1)
        {
            pfree(node_def);
            fwd_check_thread_id();
            elog(LOG, FORWARDER_PREFIX "fwd_make_connect make socket to node:%u failed", mgr->peernode);
            return -1;
        }
        ret = connect(mgr->socket, (struct sockaddr*)&server_address, sizeof(server_address));
        if (ret == -1)
        {
            pfree(node_def);
            fwd_check_thread_id();
            elog(LOG, FORWARDER_PREFIX "fwd_make_connect to node:%u failed", mgr->peernode);
            return -1;
        }
        /* set socket non blocking */
        fwd_set_nonblock_connection(mgr->socket);
        fwd_check_thread_id();
        elog(LOG,FORWARDER_PREFIX "fwd_make_connect succeed create socket %d from node[%s] to node[%s].", mgr->socket, PGXCNodeName, NameStr(node_def->nodename));
        pfree(node_def);
        return 0;
    }
    else
    {
        fwd_check_thread_id();
        elog(LOG, FORWARDER_PREFIX "fwd_make_connect get definition of node:%u failed", mgr->peernode);
        return -1;
    }
}

/*
 * forwarder receive response from other datanodes
 * then call callback by response's protocol
 */
static int
fwd_client_handle_responses(ConnMgr *mgr, ServiceThreadControl *control, bool *changed)
{
    int              nbytes  = 0;
    ResponsePackage *rsp_pkg = NULL;

    if (mgr->socket < 0)
    {
        return network_bad_socket;
    }
    
    do 
    {
        /* read header */
        nbytes = read(mgr->socket, mgr->respkg + mgr->readlen, FWD_PORT_SND_BUF_SIZE - mgr->readlen);
        if (nbytes < 0) 
        {   
            /* Anything except EAGAIN/EWOULDBLOCK/EINTR is trouble */
            switch (errno)
            {
#ifdef EAGAIN
                case EAGAIN:
                    return network_need_retry;
#endif
#if defined(EWOULDBLOCK) && (!defined(EAGAIN) || (EWOULDBLOCK != EAGAIN))
                case EWOULDBLOCK:
                    return network_need_retry;
#endif
                case EINTR: /* Different from below, just return. */
                    return network_need_retry;

                default:
                    /* Close the bad conenction, wait for the main thread reconnect. */
                    close(mgr->socket);
                    mgr->socket = -1;
                    return network_bad_socket;
            }
        }
        else if (0 == nbytes)
        {
            /* Close the bad conenction, wait for the main thread reconnect. */
            close(mgr->socket);
            mgr->socket = -1;
            return network_bad_socket;
        }
        
        mgr->readlen += nbytes;
        
        if (mgr->readlen == FWD_PORT_SND_BUF_SIZE)
        {
            rsp_pkg = fwd_decode_response_pkg(mgr->respkg, mgr->readlen);
            if (NULL == rsp_pkg)
            {
                return network_decode_error;
            }

            if (rsp_pkg->header.protocol >= RDA_PACKAGE_BUTTY)
            {
                abort();
            }

            if (!check_pkg_version((FWDPackage*)rsp_pkg))
            {
                fwd_free(rsp_pkg);
                forwarder_thread_logger(LOG, FORWARDER_PREFIX "thread %ld fwd_client_handle_responses check_pkg_version %d.%d not equal %d.%d", 
                                        pthread_self(), rsp_pkg->header.major_version, rsp_pkg->header.minor_version, FWD_MAJOR_VERSION, FWD_MINOR_VERSION);
                return network_decode_error;
            }
            
            g_fwd_client_rsp_callback_array[rsp_pkg->header.protocol](control, rsp_pkg);
            
            *changed = true;
            mgr->readlen = 0;
        }
        else
        {
            break;
        }
    }while (1);

    return network_ret_ok;
}

/* require server status */
static int fwd_request_server_status(ConnMgr *mgr, int64 rda_id, char protocol, int32 controller_index, bool *force_drop) 
{
    int         ret      = 0;
    FWDPackage  *package = NULL;
    package = fwd_make_package(protocol, 
                            my_node_id, 
                            controller_index, 
                            rda_id, 
                            0, 
                            0, 
                            NULL, 
                            RDA_INVALID_PKG_SEQ);
    if (NULL == package)
	{
        forwarder_thread_logger(LOG, FORWARDER_PREFIX "fwd_request_server_status make package failed for rda_id %ld", rda_id);
		return -1;
	}

    ret = fwd_client_send_package(mgr, package);
    if (ret)
    {
        forwarder_thread_logger(LOG, FORWARDER_PREFIX "fwd_request_server_status fwd_client_send_package rda_id %ld socket %d ret %d",
                                    rda_id, mgr->socket, ret);
        if (network_bad_socket == ret)
        {
            *force_drop = true;
        }
    }

   
    /* drop the alloced package. */
    fwd_drop_package(package);
    return ret;
}

static FWDPackage *fwd_server_receive_a_package(FWDServersPort *port, bool *bad_port)
{
    int     ret    = 0;
    int     nbytes = 0;
    int     nneed_bytes = 0;
    uint32  u32    = 0;
    uint32  package_len = 0;
    FWDPackage *pkg    = NULL;

    /* receive package header in network format */
    if (port->header_rcv_len < MinSendNetLen)
    {
        /* receive protocol and data_len */
        nneed_bytes = MinSendNetLen - port->header_rcv_len;
        nbytes = recv(port->sock, port->header + port->header_rcv_len,
                      nneed_bytes, 0);
        if (nbytes > 0)
        {
            port->header_rcv_len += nbytes;
            if (nbytes < nneed_bytes)
            {
                *bad_port = false;
                return NULL;
            }
            /* we have got protocol and pkg_len */
        }
        else if (nbytes < 0)
        {
            /* Anything except EAGAIN/EWOULDBLOCK/EINTR is trouble */
            switch (errno)
            {
#ifdef EAGAIN
            case EAGAIN:

#endif
#if defined(EWOULDBLOCK) && (!defined(EAGAIN) || (EWOULDBLOCK != EAGAIN))
            case EWOULDBLOCK:

#endif
            case EINTR:
                *bad_port = false;
                return NULL;

            default:
                *bad_port = true;
                return NULL;
            }
        }
        else
        {
            /* connection has been closed. */
           *bad_port = true;
            return NULL;
        }
    }

    /* get package len. */
    u32 = *((uint32 *)(port->header + PkglenOffset));
    package_len = ntohl(u32);
    if (package_len > FWD_MAX_PKG_SIZE || package_len < MinSendNetLen)
    {
        abort();
    }

    if (NULL == port->package)
    {
        /* alloc memmory for package */
        port->package = (char *)fwd_malloc(package_len);
        if (NULL == port->package)
        {
            *bad_port = false;
            forwarder_thread_logger(LOG, FORWARDER_PREFIX "thread %ld malloc %u  failed", pthread_self(), package_len);
            return NULL;
        }

        /* we have received protocol MinSendNetLen len. */
        ret = fwd_decode_pkg_header(port->header, MinSendNetLen, (FWDPackage*)port->package);
        if (ret)
        {
            *bad_port = false;
            forwarder_thread_logger(LOG, FORWARDER_PREFIX "thread %ld fwd_decode_pkg_header %u failed", pthread_self(), package_len);
            return NULL;
        }

        if (!check_pkg_version((FWDPackage*)port->package))
        {
             *bad_port = false;
            forwarder_thread_logger(LOG, FORWARDER_PREFIX "thread %ld fwd_server_receive_a_package check_pkg_version %d.%d not equal %d.%d", pthread_self(), ((FWDPackage*)port->package)->major_version, ((FWDPackage*)port->package)->minor_version, FWD_MAJOR_VERSION, FWD_MINOR_VERSION);
            return NULL;
        }
        port->package_rcv_len = MinSendNetLen;
    }

    /* receive package body. */
    nneed_bytes = package_len - port->package_rcv_len;
    if (nneed_bytes > 0)
    {
        nbytes = recv(port->sock, port->package + port->package_rcv_len,
                  nneed_bytes, 0);
        if (nbytes > 0)
        {
            port->package_rcv_len += nbytes;
            if (nbytes < nneed_bytes)
            {
                /* not complete, return at present. */
                *bad_port = false;
                return NULL;
            }
        }
        else if (nbytes < 0)
        {
            /* Anything except EAGAIN/EWOULDBLOCK/EINTR is trouble */
            switch (errno)
            {
    #ifdef EAGAIN
            case EAGAIN:

    #endif
    #if defined(EWOULDBLOCK) && (!defined(EAGAIN) || (EWOULDBLOCK != EAGAIN))
            case EWOULDBLOCK:

    #endif
            case EINTR:
                *bad_port = false;
                return NULL;

            default:
                *bad_port = true;
                return NULL;
            }
        }
        else 
        {
            /* connection closed. */
            *bad_port = true;
            return NULL;
        }
        
    }
    /* complete, we return the complete package, prepare for the next package at first. */
    port->header_rcv_len  = 0;
    port->package_rcv_len = 0;

    pkg = (FWDPackage *)port->package;
    if (pkg->node_id == my_node_id)
    {
        abort();
    }
    /* init pkg_left_len */
    pkg->pkg_left_len = pkg->pkg_len;
    port->package = NULL;
    return pkg;
    
}

/* send the response packages of server, sometimes the pkg can be ignored */
static int fwd_server_send_response(FWDServersPort *port, ResponsePackage *pkg, bool *ignored)
{
    int     ret    = 0;
    int     nbytes = 0;
    int     nneed_bytes = 0;
    
    /* if we failed to finish sending latest incomplete pkg, the pkg passed in will be ignored */
    *ignored = true;
    if (port->resp_buf_pkg_len != port->resp_buf_send_len)
    {
        nneed_bytes = port->resp_buf_pkg_len - port->resp_buf_send_len;
        nbytes = send(port->sock, port->resp_buf + port->resp_buf_send_len, nneed_bytes, 0);
        if (nbytes <= 0)
        {
            /* Anything except EAGAIN/EWOULDBLOCK/EINTR is trouble */
            switch (errno)
            {
#ifdef EAGAIN
            case EAGAIN:

#endif
#if defined(EWOULDBLOCK) && (!defined(EAGAIN) || (EWOULDBLOCK != EAGAIN))
            case EWOULDBLOCK:

#endif
            case EINTR:
                return network_need_retry;

            default:
                return network_bad_socket;
            }
        }
        else
        {
            port->resp_buf_send_len += nbytes;
            if (nbytes < nneed_bytes)
            {
                return network_need_retry;
            }

            /* we have sent all buffered data */
            port->resp_buf_pkg_len = 0;
            port->resp_buf_send_len = 0;
        }
    }

    /* send the package passed in */
    *ignored = false;

    /* encode package into buffers */
    ret = fwd_encode_response_pkg(port->resp_buf,  FWD_PORT_SND_BUF_SIZE, pkg);
    if (ret)
    {
        return network_decode_error;
    }
    
    port->resp_buf_pkg_len = pkg->header.pkg_len;
    port->resp_buf_send_len = 0;
    nneed_bytes = port->resp_buf_pkg_len;
    nbytes = send(port->sock, port->resp_buf, nneed_bytes, 0);
    if (nbytes <= 0)
    {
        /* Anything except EAGAIN/EWOULDBLOCK/EINTR is trouble */
        switch (errno)
        {
#ifdef EAGAIN
        case EAGAIN:

#endif
#if defined(EWOULDBLOCK) && (!defined(EAGAIN) || (EWOULDBLOCK != EAGAIN))
        case EWOULDBLOCK:

#endif
        case EINTR:
            return network_need_retry;

        default:
            return network_bad_socket;
        }
    }
    else
    {
        /* buffer incomplete package */
        port->resp_buf_send_len += nbytes;
            
        if (nbytes < nneed_bytes)
        { 
            return network_need_retry;
        }
        return network_ret_ok;
    }
}

/*
 * handle messages of port, including receiving and response.
 */
static int fwd_server_port_process(FWDServersPort *port)
{ 
    bool                    pkg_ignored = false;
    bool                    bad_port    = false;
    int                     ret         = 0;
    ResponsePackage         *response   = NULL;
    FWDPackage              *package          = NULL;
    RDAController           *rda_controller   = NULL;

    if (NULL == port)
    {
        forwarder_thread_logger(LOG, FORWARDER_PREFIX "receiver fwd_server_port_process get_rda_controller_by_id succeed, thread:%ld get null pointer ", pthread_self());
        return network_ret_ok;
    }

    /* loop to handle every package */
    do
    {
        /* receive package */
        package = fwd_server_receive_a_package(port, &bad_port);
        if (package)
        {
            /* remember socket into the rda control*/
            rda_controller = get_rda_controller_by_id(package->controller_idx);
            if (rda_controller)
            {
                if (g_fwd_dump_pkg_to_file)
                {
                    forwarder_thread_logger(LOG, FORWARDER_PREFIX "receiver fwd_server_port_process get_rda_controller_by_id succeed, thread:%ld, rda_id %ld, src_node_id %d, controller_idx %d, data len %d, seq %ld, used_socket %d ",
                                        pthread_self(), package->rda_id, package->node_id, package->controller_idx, package->pkg_len, package->pkg_seq, port->sock);
                }
                
                /* set socket into rda control */
                rda_controller->socket = port->sock;
                fwd_server_socket_set_status(port->sock, network_ret_ok);
            }

            if (g_fwd_dump_pkg_to_file)
            {
                forwarder_thread_logger(LOG, FORWARDER_PREFIX "receiver fwd_server_port_process receive package %s succeed, thread:%ld, rda_id %ld, src_node_id %d, controller_idx %d, data len %d, seq %ld, used_socket %d ",
                                        g_rda_package_names[package->protocol], pthread_self(), package->rda_id, package->node_id, package->controller_idx, package->pkg_len, package->pkg_seq, port->sock);

            }

            if (package->protocol >= RDA_PACKAGE_BUTTY)
            {
                abort();
            }

            /* handle package */
            response = g_fwd_server_callback_array[package->protocol](package);
            if (response)
            {
                ret = fwd_server_send_response(port, response, &pkg_ignored);
                if (network_bad_socket == ret)
                {
                    fwd_free(response);
                    forwarder_thread_logger(LOG, FORWARDER_PREFIX "receiver fwd_server_port_process fwd_server_send_response handle package failed for bad socket, thread:%ld, rda_id %ld, src_node_id %d, controller_idx %d, data len %d, seq %ld, used_socket %d",
                                        pthread_self(), response->header.rda_id, response->header.node_id, response->header.controller_idx, response->header.pkg_len, response->header.pkg_seq, port->sock);
                    return ret;
                }
                
                if (pkg_ignored)
                {
                    forwarder_thread_logger(LOG, FORWARDER_PREFIX "receiver fwd_server_port_process fwd_server_send_response ignore package, thread:%ld, rda_id %ld, src_node_id %d, controller_idx %d, data len %d, seq %ld, used_socket %d ",
                                        pthread_self(), response->header.rda_id, response->header.node_id, response->header.controller_idx, response->header.pkg_len, response->header.pkg_seq, port->sock);
                }

                if (g_fwd_dump_pkg_to_file)
                {
                    forwarder_thread_logger(LOG, FORWARDER_PREFIX "receiver fwd_server_port_process fwd_server_send_response succeed, thread:%ld, rda_id %ld, src_node_id %d, controller_idx %d, data len %d, seq %ld, used_socket %d ",
                                        pthread_self(), response->header.rda_id, response->header.node_id, response->header.controller_idx, response->header.pkg_len, response->header.pkg_seq, port->sock);
                }
                fwd_free(response);
            }
            else
            {
                forwarder_thread_logger(LOG, FORWARDER_PREFIX "receiver fwd_server_port_process handle package return NULL response, thread:%ld, rda_id %ld, src_node_id %d, controller_idx %d, data len %d, seq %ld, used_socket %d ",
                                        pthread_self(), package->rda_id, package->node_id, package->controller_idx, package->pkg_len, package->pkg_seq, port->sock);

            }
        }
        else
        {
            /* bad_port */
            if (bad_port)
            {
                fwd_server_socket_set_status(port->sock, network_bad_socket);
                return network_bad_socket;
            }
            return network_ret_ok;
        }
    } while(1);

    return network_ret_ok;
}

/* async push data callback function. rsp must be freed.*/
static int rda_client_pushdata_rsp_callback(ServiceThreadControl *control, ResponsePackage *rsp)
{
    int32  drop_pkg_num = 0;
    uint64 oldest_seq;
    data_queue *d_queue;
    FWDPackage *pkg;
    FWDPackage *drop_pkgs[RDA_BUFFER_SND_QUEUE_LEN]; /* for delayed drop */

    d_queue = (data_queue *)(map_get(control->data_queue_map, rsp->header.rda_id, rsp->header.node_id));
    if (d_queue)
    {
        d_queue->bytes_received += sizeof(ResponsePackage);
        d_queue->pkgs_received  += 1;
        if (g_fwd_dump_pkg_to_file)
        {
            forwarder_thread_logger(LOG,
                                        FORWARDER_PREFIX "thread %ld sender[%d] rda_client_pushdata_rsp_callback enter, rda_id %ld, rda_control_id %d, dst_node_id %d, pkg_seq %ld, ack_seq %ld, next_send %ld, free_space %d",
                                        pthread_self(), control->id, rsp->header.rda_id, rsp->header.controller_idx, rsp->header.node_id, rsp->header.pkg_seq, rsp->payload.datarsp.ack_seq, rsp->payload.datarsp.next_send, rsp->payload.datarsp.free_space);
        }

        /* drop all packages have been acked */
        fwd_queue_get_first_seq(d_queue->buffer_queue, &oldest_seq);
        if (oldest_seq != RDA_INVALID_PKG_SEQ)
        {
            if (g_fwd_dump_pkg_to_file)
            {
                forwarder_thread_logger(LOG,
                                            FORWARDER_PREFIX "thread %ld sender[%d] rda_client_pushdata_rsp_callback oldest_seq %ld, rda_id %ld, rda_control_id %d, dst_node_id %d, pkg_seq %ld, ack_seq %ld, next_send %ld, free_space %d",
                                            pthread_self(), control->id, oldest_seq, rsp->header.rda_id, rsp->header.controller_idx, rsp->header.node_id, rsp->header.pkg_seq,  rsp->payload.datarsp.ack_seq, rsp->payload.datarsp.next_send, rsp->payload.datarsp.free_space);
            }

            while (oldest_seq <= rsp->payload.datarsp.ack_seq && drop_pkg_num < RDA_BUFFER_SND_QUEUE_LEN)
            {
                pkg = fwd_queue_remove_pkg(d_queue->buffer_queue, oldest_seq);
                if (pkg)
                {
                    d_queue->ack_bytes = pkg->pkg_offset + pkg->pkg_len;
                    if (g_fwd_dump_pkg_to_file)
                    {
                        forwarder_thread_logger(LOG,
                                                    FORWARDER_PREFIX "thread %ld sender[%d] rda_client_pushdata_rsp_callback fwd_queue_remove_pkg pkg_seq %ld, rda_id %ld, rda_control_id %d, dst_node_id %d, ack_seq %ld, next_send %ld, free_space %d",
                                                    pthread_self(), control->id, rsp->header.pkg_seq, rsp->header.rda_id, rsp->header.controller_idx, rsp->header.node_id, rsp->payload.datarsp.ack_seq, 
                                                    rsp->payload.datarsp.next_send, rsp->payload.datarsp.free_space);
                    }
                }
                else
                {
                    break;
                }

                /* do the drop later */
                drop_pkgs[drop_pkg_num++] = pkg;
                oldest_seq++;
            }
        }

        /* update free slot number */
        if (d_queue->ack_seq <= rsp->payload.datarsp.ack_seq)
        {
            d_queue->peer_free_slot_num = rsp->payload.datarsp.free_space;
            d_queue->ack_seq = rsp->payload.datarsp.ack_seq;
            d_queue->next_send = rsp->payload.datarsp.next_send;
        }
        

        if (g_fwd_dump_pkg_to_file)
        {
            forwarder_thread_logger(LOG,
                                    FORWARDER_PREFIX "thread %ld sender[%d] rda_client_pushdata_rsp_callback exit, drop_pkg_num %d, rda_id %ld, rda_control_id %d, dst_node_id %d, pkg_seq %ld, ack_seq %ld, next_send %ld, free_space %d, queue_free_length:%d",
                                    pthread_self(), control->id, drop_pkg_num, rsp->header.rda_id, rsp->header.controller_idx, rsp->header.node_id, rsp->header.pkg_seq,
                                    rsp->payload.datarsp.ack_seq, rsp->payload.datarsp.next_send, rsp->payload.datarsp.free_space, 
                                    fwd_queue_free_length(d_queue->buffer_queue));
        }
        

        /* do the real drop. */
        while (drop_pkg_num > 0)
        {
            fwd_drop_package(drop_pkgs[drop_pkg_num - 1]);
            drop_pkg_num--;
        }
    }
    else
    {
        forwarder_thread_logger(LOG, FORWARDER_PREFIX "thread %ld sender[%d] rda_client_pushdata_rsp_callback can not find d_queue of rda_id:%ld, node_id:%d",
                                     pthread_self(), control->id, rsp->header.rda_id, rsp->header.node_id);
    }
    fwd_free(rsp);
    return 0;
}

/* async rda status request callback function. rsp must be freed.*/
static int rda_client_rda_status_rsp_callback(ServiceThreadControl *control, ResponsePackage *rsp)
{
    data_queue *d_queue = NULL;
    d_queue = (data_queue *)(map_get(control->data_queue_map, rsp->header.rda_id, rsp->header.node_id));
    if (d_queue)
    {
        d_queue->bytes_received += sizeof(ResponsePackage);
        d_queue->pkgs_received  += 1;
        
        d_queue->controller_idx = rsp->header.controller_idx;
        /* only set RDA_RUNNING when in RDA_REQUEST_RDA_REMOTE*/
        if (RDA_REQUEST_RDA_REMOTE == get_rda_status(&d_queue->status))
        {
            /* begin to send packages. */
            set_rda_status(&d_queue->status, RDA_RUNNING);
            if (g_fwd_dump_pkg_to_file)
            {
                forwarder_thread_logger(LOG,
                                        FORWARDER_PREFIX "rda_client_rda_status_rsp_callback sender[%d] is ready set RDA_RUNNING, rda_id %ld, dst_node_id %d, controller_idx %d",
                                        control->id, rsp->header.rda_id, rsp->header.node_id, rsp->header.controller_idx);
            }
        }
    }
    else
    {
        forwarder_thread_logger(LOG, FORWARDER_PREFIX "thread %ld sender[%d] rda_client_rda_status_rsp_callback can not find d_queue of rda_id:%ld, dst_node_id:%d",
                                     pthread_self(), control->id, rsp->header.rda_id, rsp->header.node_id);
    }
    fwd_free(rsp);
    return  0;
}

/* async server status request callback function. rsp can not be freed. */
static int rda_client_server_status_rsp_callback(ServiceThreadControl *control, ResponsePackage *rsp)
{
    data_queue *d_queue = NULL;

    d_queue = (data_queue *)(map_get(control->data_queue_map, rsp->header.rda_id, rsp->header.node_id));
    if (d_queue)
    {
        d_queue->bytes_received += sizeof(ResponsePackage);
        d_queue->pkgs_received  += 1;
        
        d_queue->peer_free_slot_num = rsp->payload.rcvstatusrsp.free_space;
        if (rsp->payload.rcvstatusrsp.ack_seq > d_queue->ack_seq)
        {
           d_queue->ack_seq = rsp->payload.rcvstatusrsp.ack_seq;
        }
        
        if (d_queue->next_send < rsp->payload.rcvstatusrsp.next_send)
        {
            d_queue->next_send = rsp->payload.rcvstatusrsp.next_send;
        }

        if (g_fwd_dump_pkg_to_file)
        {
            forwarder_thread_logger(LOG,
                                FORWARDER_PREFIX "thread %ld sender[%d] rda_server_status_response_call_back succeed free_space %d rda_control_id %d, rda_id %lu, dst_node_id %d, ack_seq %ld, local_ack %ld",
                                pthread_self(), control->id, rsp->payload.rcvstatusrsp.free_space, rsp->header.controller_idx, rsp->header.rda_id, rsp->header.node_id, rsp->payload.rcvstatusrsp.ack_seq, d_queue->ack_seq);
        }

        /* no free slots left in server */
        if (0 == d_queue->peer_free_slot_num)
        {
            if (g_fwd_dump_pkg_to_file)
            {

                forwarder_thread_logger(LOG,
                                        FORWARDER_PREFIX "thread %ld sender[%d] rda_server_status_response_call_back get avaialble free space %d, rda_control_id %d, rda_id %lu, dst_node_id %d, set RDA_BLOCKED status",
                                        pthread_self(), control->id, rsp->payload.rcvstatusrsp.free_space, rsp->header.controller_idx, rsp->header.rda_id, rsp->header.node_id);
            }

            set_rda_status(&d_queue->status, RDA_BLOCKED);
        }
    }
    else
    {
        forwarder_thread_logger(LOG, FORWARDER_PREFIX "thread %ld sender[%d] rda_client_server_status_rsp_callback can not find d_queue of rda_control_id %d, rda_id:%ld, dst_node_id:%d, free_space:%d",
                                     pthread_self(), control->id, rsp->header.controller_idx, rsp->header.rda_id, rsp->header.node_id, rsp->payload.rcvstatusrsp.free_space);
    }
    fwd_free(rsp);
    return 0;
}


#if 0
void SocketTest()
{
    int ret;
    int i;
    int j;
    bool success = true;

    /*
     * sleep to wait pgxc init finished. if not,
     * counter of coordinator nodes and data nodes in pgxc might still be 0.
     */
    sleep(10);

    /* test for sender socket array initialization. */
    if (!is_socket_ready)
    {
        if (g_client_socket_array != NULL)
        {
            fwd_cleanup_socket_array();
            g_client_socket_array = NULL;
        }
        ret = fwd_init_sender_socket_array(my_node_id);
        if (0 == ret)
        {
            elog(LOG, FORWARDER_PREFIX "Socket Test: successfully init sender socket array.");
        }
        else
        {
            elog(LOG, FORWARDER_PREFIX "Socket Test: failed to init sender socket array.");
        }
    }
    else
    {
        elog(LOG, FORWARDER_PREFIX "Socket Test: socket array has already been initialized.");
    }

    /* test for sender socket array normally closed by sender. */
    if (g_client_socket_array != NULL)
    {
        for (i = 0; i < g_fwd_dn_num; i++)
        {
            if (g_client_socket_array[i] != NULL)
            {
                for (j = 0; j < g_rda_sender_socket_num_per_node; j++)
                {
                    ret = close(g_client_socket_array[i][j].socket);
                    if (-1 == ret)
                    {
                        elog(LOG, FORWARDER_PREFIX "Socket Test: failed to close socket by sender.");
                        success = false;
                    }
                }
                free(g_client_socket_array[i]);
            }
        }
        if (success)
        {
            elog(LOG, FORWARDER_PREFIX "Socket Test: Successfully clean up sender socket array.");
        }
        free(g_client_socket_array);
        g_client_socket_array = NULL;
        g_fwd_dn_num = 0;
    }
}
#endif

#if 0
void RDAQueueTest()
{
	
	int         ret        = 0;
    int         i          = 0;
    int         j          = 0;
	uint64      firstseq   = 0;
    uint64      lastseq    = 0;
    uint64      dropseq    = 0;
    uint64      addseq     = 0;
    uint64      lastackseq = 0;
    size_t      str_len    = 0;
	FWDQueue    *tqueue    = NULL;
	FWDPackage  *p         = NULL;
	FWDPackage  *droppkg   = NULL;
    FWDPackage  *pkg_tmp   = NULL;
    uint8_t     str[]      = {0x61, 0x62, 0x63, 0x64, 0x65, 0x66};

    str_len = sizeof(str) / sizeof(uint8_t);

	sleep(10);
	// create and drop
	tqueue = fwd_create_queue(RDA_BUFFER_RCV_QUEUE_LEN);

	p = (FWDPackage*)malloc(sizeof(FWDPackage) + str_len);
	p->pkg_seq = 1;
	p->pkg_len = str_len;
	// p->data = malloc(10);
    memcpy(p->data, str, str_len);
		
	fwd_queue_push_pkg(tqueue, p);
	fwd_queue_drop_pkg(tqueue, 1);

	p = (FWDPackage*)malloc(sizeof(FWDPackage) + str_len);
	p->pkg_seq = 2;
	p->pkg_len = str_len;
	// p->data = malloc(10);
    memcpy(p->data, str, str_len);
		
	fwd_queue_push_pkg(tqueue, p);
	fwd_queue_drop_pkg(tqueue, 2);

	p = (FWDPackage*)malloc(sizeof(FWDPackage) + str_len);
	p->pkg_seq = 3;
	p->pkg_len = str_len;
	// p->data = malloc(10);
    memcpy(p->data, str, str_len);
		
	fwd_queue_push_pkg(tqueue, p);
	fwd_queue_drop_pkg(tqueue, 3);

	p = (FWDPackage*)malloc(sizeof(FWDPackage) + str_len);
	p->pkg_seq = 4;
	p->pkg_len = str_len;
	// p->data = malloc(10);
    memcpy(p->data, str, str_len);
		
	fwd_queue_push_pkg(tqueue, p);
	pkg_tmp = fwd_queue_get_package(tqueue, 4);
    elog(LOG, "RDAQueueTest fwd_queue_get_package pkg_seq: %ld, pkg_len: %d", pkg_tmp->pkg_seq, pkg_tmp->pkg_len);
	fwd_queue_drop_pkg(tqueue, 4);
	
	fwd_destory_queue(tqueue);
	
	#if 1 //sequncial push package and drop package
	// create and push package
	tqueue = fwd_create_queue(RDA_BUFFER_RCV_QUEUE_LEN);
	
	elog(LOG, "RDAQueueTest fwd_create_queue queue fwd_queue_is_full:%d, fwd_queue_is_empty:%d", fwd_queue_is_full(tqueue), fwd_queue_is_empty(tqueue));
    
    /* Add package to queue until full, pkg_seq belongs to [1, 63], here add to 64 to test */
    for (i = 1; i < RDA_BUFFER_RCV_QUEUE_LEN + 1; i++)
	{
		p = (FWDPackage*)malloc(sizeof(FWDPackage) + str_len);
		p->pkg_seq = i;
		p->pkg_len = str_len;
		// p->data = malloc(10);
        memcpy(p->data, str, str_len);
		
		ret = fwd_queue_push_pkg(tqueue, p);
		fwd_queue_get_first_seq(tqueue, &firstseq);
		fwd_queue_get_last_seq(tqueue, &lastseq);
		elog(LOG, "RDAQueueTest fwd_queue_push_pkg sequence: %d, ret: %d, datalength: %d, freelength: %d, first seq:%ld, last seq:%ld", i, ret, 
			                                                                                             fwd_queue_data_length(tqueue), 
			                                                                                             fwd_queue_free_length(tqueue),
			                                                                                             firstseq,
			                                                                                             lastseq);
	}

	sleep(2);

	fwd_queue_get_last_seq(tqueue, &lastackseq);  /* lastackseq should be 0 */
	elog(LOG, "RDAQueueTest fwd_queue_get_last_seq sequence after add all last ack seq:%ld", lastackseq);


	/* Drop every pkg, pkg_seq belongs to [1, 63], here add to 64 to test */
	for (i = 1; i < RDA_BUFFER_RCV_QUEUE_LEN + 1; i++)
	{
		fwd_queue_drop_pkg(tqueue, i);
		fwd_queue_get_first_seq(tqueue, &firstseq);
		fwd_queue_get_last_seq(tqueue, &lastseq);
        fwd_queue_get_last_seq(tqueue, &lastackseq);  /* lastackseq should be 1~63 */
		elog(LOG, "fwd_queue_drop_pkg sequence: %d, datalength: %d, freelength: %d, first seq:%ld, last seq:%ld, latest_ack: %ld", i,
			                                                                                             fwd_queue_data_length(tqueue), 
			                                                                                             fwd_queue_free_length(tqueue),
			                                                                                             firstseq,
			                                                                                             lastseq,
                                                                                                         lastackseq);
	}

	fwd_queue_get_last_seq(tqueue, &lastackseq);  /* lastackseq should be 63 */
	elog(LOG, "fwd_queue_get_last_seq sequence after drop all last ack seq:%ld", lastackseq);
	
    /* Get the first and last seq when queue is empty */
	fwd_queue_get_first_seq(tqueue, &firstseq); /* firstseq should be 0 */
	fwd_queue_get_last_seq(tqueue, &lastseq);   /* firstseq should be 0 */
	elog(LOG, "When queue is empty sequence: %d, datalength: %d, freelength: %d, first seq:%ld, last seq:%ld", i,
		                                                                                             	fwd_queue_data_length(tqueue), 
		                                                                                             	fwd_queue_free_length(tqueue),
		                                                                                             	firstseq,
			                                                                                          	lastseq);
    sleep(5);
	/* After droping every pkg, add 5 package, pkg_seq [64, 68] */
	addseq = RDA_BUFFER_RCV_QUEUE_LEN;
	for (i = addseq; i < addseq + 5; i++)
	{
		p = (FWDPackage*)malloc(sizeof(FWDPackage) + str_len);
		p->pkg_seq = i;
		p->pkg_len = str_len;
        memcpy(p->data, str, str_len);
		
		fwd_queue_push_pkg(tqueue, p);
		fwd_queue_get_first_seq(tqueue, &firstseq);
		fwd_queue_get_last_seq(tqueue, &lastseq);
		elog(LOG, "fwd_queue_push_pkg sequence: %d, datalength: %d, freelength: %d, first seq:%ld, last seq:%ld", i, 
			                                                                                             fwd_queue_data_length(tqueue), 
			                                                                                             fwd_queue_free_length(tqueue),
			                                                                                             firstseq,
			                                                                                             lastseq);
	}


    /* Test fwd_queue_sniffer */
	while(droppkg = fwd_queue_sniffer(tqueue))	
	{	
		dropseq = droppkg->pkg_seq;
		fwd_queue_drop_pkg(tqueue, dropseq);
		fwd_queue_get_first_seq(tqueue, &firstseq);
		fwd_queue_get_last_seq(tqueue, &lastseq);
		elog(LOG, "fwd_queue_drop_pkg sequence: %ld, datalength: %d, freelength: %d, first seq:%ld, last seq:%ld", dropseq,
			                                                                                             fwd_queue_data_length(tqueue), 
		    	                                                                                         fwd_queue_free_length(tqueue),
		        	                                                                                     firstseq,
		            	                                                                                 lastseq);
	}
	

	fwd_queue_get_last_seq(tqueue, &lastackseq);  /* lastackseq should be 68 */
	elog(LOG, "fwd_queue_get_last_seq sequence after drop all packeage: %ld", lastackseq);

	fwd_destory_queue(tqueue);

	#endif
}
#endif

#if 0
/* RDAControllerTest for multiple rda_id */
void RDAControllerTest()
{
    int           i              = 0;
    int           j              = 0;
    int           ret            = 0;
    int           src_num        = 3;        /* node_id: 0 1 2, my_node_id: -1 */
    int           rda_num        = 4;
    RDAController *rda_cntl      = NULL;
    RDAController *rda_cntl_tmp  = NULL;
    data_queue    **receive_data_queue  = NULL;
    int64         rda_id[] = {123, 456, 789, 246};  /* rda_id: 123, 456, 789, 246 */

    sleep(10);

    g_fwd_dn_num = src_num;
    my_node_id = -1;
    elog(LOG, "before fwd_init_rda_queue_groups");
    /* init global array g_fwd_server_rda_slots_group */
    ret = fwd_init_rda_queue_groups(my_node_id);
    if (0 == ret)
    {
        elog(LOG, "fwd_init_rda_queue_groups success");
    }
    else
    {
        elog(LOG, "fwd_init_rda_queue_groups failed");
        return;
    }

    /* register for every rda_id to datanode */
    receive_data_queue = (data_queue **)malloc(sizeof(data_queue*) * rda_num);
    if (NULL == receive_data_queue)
    {
        elog(LOG, "receive_data_queue malloc failed");
        return;
    }
    elog(LOG, "receive_data_queue malloc success");
    for (i = 0; i < rda_num; i++)
    {
        /* create receive_data_queue array */
        receive_data_queue[i] = (data_queue *)malloc(sizeof(data_queue) * src_num);
        if (NULL == receive_data_queue)
        {
            elog(LOG, "receive_data_queue malloc failed");
            return;
        }
        memset(receive_data_queue[i], 0, sizeof(data_queue) * src_num);
        elog(LOG, "receive_data_queue[%d] malloc success", i);

        for (j = 0; j < src_num; j++)
        {
            receive_data_queue[i][j].rda_id = rda_id[i];
            receive_data_queue[i][j].node_id = j;
        
            /* try to get a controller */
            receive_data_queue[i][j].controller = fwd_alloc_rda_controller(rda_id[i], j);
            if (NULL == receive_data_queue[i][j].controller)
            {
                elog(LOG, "fwd_alloc_rda_controller failed, node_id: %d", j);
                continue;
            }
            receive_data_queue[i][j].controller->rda_id = rda_id[i];
            receive_data_queue[i][j].controller->node_id = j;
            elog(LOG, "fwd_alloc_rda_controller success, rda_id %ld, node_id %d, controller_id %d", 
                        rda_id[i], j, receive_data_queue[i][j].controller->controller_id);

            /* set the buffer queue used by the rda receiver. */
            receive_data_queue[i][j].controller->data_queue = &receive_data_queue[i][j];
        }
    }

    sleep(5);
    elog(LOG, "test get_rda_controller_by_rda_id & get_rda_controller_by_id & fwd_free_rda_controller");
    /* test get_rda_controller_by_rda_id & get_rda_controller_by_id & fwd_free_rda_controller */
    for (i = 0; i < rda_num; i++)
    {
        for (j = 0; j < src_num; j++)
        {
            /* test get_rda_controller_by_rda_id */
            rda_cntl = get_rda_controller_by_rda_id(rda_id[i], j);
            if (NULL == rda_cntl)
            {
                elog(LOG, "get_rda_controller_by_rda_id failed, rda_id %ld, node_id %d", 
                            rda_id[i], j);
                continue;
            }
            elog(LOG, "get_rda_controller_by_rda_id success, rda_id %ld, node_id %d, controller_id %d",
                        rda_cntl->rda_id, rda_cntl->node_id, rda_cntl->controller_id);
            
            /* test get_rda_controller_by_id */
            rda_cntl_tmp = get_rda_controller_by_id(rda_cntl->controller_id);
            if (NULL == rda_cntl_tmp)
            {
                elog(LOG, "get_rda_controller_by_id failed, controller_idx: %d", i);
                continue;
            }
            elog(LOG, "get_rda_controller_by_id success, rda_id %ld, node_id %d, controller_id %d",
                 rda_cntl_tmp->rda_id, rda_cntl_tmp->node_id, rda_cntl_tmp->controller_id);
            
            /* test fwd_free_rda_controller */
            fwd_free_rda_controller(rda_cntl_tmp->controller_id);
            elog(LOG, "fwd_free_rda_controller success, rda_id %ld",
                 rda_cntl_tmp->rda_id);
        }
    }
    
    sleep(2);
    /* test fwd_delete_rda_node_groups */
    fwd_delete_rda_node_groups();

    g_fwd_dn_num = 0;
}
#endif

#if 0
/* RDAControllerTest for only one rda_id */
void RDAControllerTest()
{
    int           i              = 0;
    int           ret            = 0;
    int           src_num        = 1;
    int           node_id        = 0;
    int64         rda_id         = 123;
    RDAController *rda_cntl      = NULL;
    RDAController *rda_cntl_tmp  = NULL;
    data_queue    *receive_data_queue  = NULL;

    g_fwd_dn_num = src_num;

    sleep(10);

    my_node_id = -1;
    /* init global array g_fwd_server_rda_slots_group */
    ret = fwd_init_rda_queue_groups(my_node_id);
    if (0 == ret)
    {
        elog(LOG, "fwd_init_rda_queue_groups success");
    }
    else
    {
        elog(LOG, "fwd_init_rda_queue_groups failed");
        return;
    }

    /* register for every rda_id to datanode */
    receive_data_queue = (data_queue *)malloc(sizeof(data_queue) * src_num);
    if (receive_data_queue == NULL)
    {
        elog(LOG, "receive_data_queue malloc failed");
        return;
    }
    memset(receive_data_queue, 0, sizeof(data_queue *) * src_num);
    elog(LOG, "receive_data_queue malloc success");
    for (i = 0; i < src_num; i++)
    {
        /* free by cleanner thread */
        receive_data_queue[i].rda_id = rda_id;
        receive_data_queue[i].node_id = node_id;
        receive_data_queue[i].ack_bytes = 0;
        receive_data_queue[i].send_offset    = 0;
        receive_data_queue[i].seq            = RDA_INVALID_PKG_SEQ;
        receive_data_queue[i].controller_idx = -1;
        set_rda_status(&receive_data_queue[i].status, RDA_INIT);
        receive_data_queue[i].sender_silent_cnt = 0;
		
        receive_data_queue[i].bytes_sent = 0;
        receive_data_queue[i].bytes_received = 0;
        receive_data_queue[i].bytes_dropped = 0;
		receive_data_queue[i].bytes_resent = 0;
		
		receive_data_queue[i].pkgs_sent     = 0;
        receive_data_queue[i].pkgs_received = 0;
        receive_data_queue[i].pkgs_dropped  = 0;
		receive_data_queue[i].pkgs_resent   = 0;

		receive_data_queue[i].ack_seq	= RDA_INVALID_PKG_SEQ;
	
        /* try to get a controller */
		receive_data_queue[i].controller = fwd_alloc_rda_controller(rda_id, node_id);
        if (NULL == receive_data_queue[i].controller)
        {
            elog(LOG, "fwd_alloc_rda_controller failed, node_id: %d", node_id);
            return;
        }
        elog(LOG, "fwd_alloc_rda_controller success, rda_id: %ld, node_id: %d, controller_id: %d", 
                    receive_data_queue[i].controller->rda_id, receive_data_queue[i].controller->node_id, receive_data_queue[i].controller->controller_id);
        receive_data_queue[i].controller->data_queue = &receive_data_queue[i];
    }

    sleep(5);
    elog(LOG, "test get_rda_controller_by_rda_id & get_rda_controller_by_id & fwd_free_rda_controller");

    /* test get_rda_controller_by_rda_id */
    rda_cntl = get_rda_controller_by_rda_id(rda_id, node_id);
    if (NULL == rda_cntl)
    {
        elog(LOG, "get_rda_controller_by_rda_id failed, rda_id %ld, node_id %d", 
                    rda_id, node_id);
        return;
    }
    elog(LOG, "get_rda_controller_by_rda_id success, rda_id %ld, node_id %d, controller_id %d",
                rda_cntl->rda_id, rda_cntl->node_id, rda_cntl->controller_id);
    
    /* test get_rda_controller_by_id */
    rda_cntl_tmp = get_rda_controller_by_id(rda_cntl->controller_id);
    if (NULL == rda_cntl_tmp)
    {
        elog(LOG, "get_rda_controller_by_id failed");
        return;
    }
    elog(LOG, "get_rda_controller_by_id success, rda_id %ld, node_id %d, controller_id %d",
            rda_cntl_tmp->rda_id, rda_cntl_tmp->node_id, rda_cntl_tmp->controller_id);
    
    sleep(2);
    /* test fwd_free_rda_controller */
    fwd_free_rda_controller(rda_cntl_tmp->controller_id);
    elog(LOG, "fwd_free_rda_controller success, rda_id %ld",
            rda_cntl_tmp->rda_id);

    
    sleep(2);
    /* test fwd_delete_rda_node_groups */
    fwd_delete_rda_node_groups();

    g_fwd_dn_num = 0;
}
#endif

#if 0
extern bool rda_frame_test_dequeue(FILE **fpp, rda_dsm_frame *frame, uint8_t *data, int size);
static int  g_rda_test_data_node_num = 0;
static rda_dsm_frame       **g_rda_test_in_frames   = NULL;
static rda_dsm_frame       **g_rda_test_out_frames  = NULL;

void ForwarderTestSendRDADataInit()
{
    #define FWD_TEST_RDA_ID 123456789
    int     index = 0;
    int     ret   = 0;
    int     request_size = 0;
    int     rda_shmem_size = 0;
    int    *node_ids = NULL;
    char   *rda_shmem_ptr = NULL;

    PgxcNodeGetOids(NULL, NULL, NULL, &g_rda_test_data_node_num, false);

    /* init rda frames */
    request_size = RDA_FRAME_SIZE * (g_rda_test_data_node_num * 2);

    /* create shared-memory and map it */
    ret = dms_rda_create_and_attach(FWD_TEST_RDA_ID, request_size,
                    &rda_shmem_ptr, &rda_shmem_size);
    if (!ret)
    {
        ereport(ERROR,
                (errcode(ERRCODE_OUT_OF_MEMORY),
                    errmsg("Failed to create shared-memory for RDA: %ld", FWD_TEST_RDA_ID)));
    }

    /* get mmaped frames */
    g_rda_test_in_frames = (rda_dsm_frame **)
                    palloc(g_rda_test_data_node_num * sizeof(rda_dsm_frame *));

    g_rda_test_out_frames = (rda_dsm_frame **)
                    palloc(g_rda_test_data_node_num * sizeof(rda_dsm_frame *));

    dsm_map_rda_frames(rda_shmem_ptr, rda_shmem_size,
                       g_rda_test_data_node_num, g_rda_test_data_node_num,
                       g_rda_test_in_frames, g_rda_test_out_frames, true);


    
    /* register rda */
    node_ids = (int*)malloc(sizeof(int) * g_rda_test_data_node_num);
    if (NULL == node_ids)
    {
        return;
    }

    for (index = 0; index < g_rda_test_data_node_num; index++)
    {
        node_ids[index] = index;
    }

    ret = fwd_register_rda_impl(FWD_TEST_RDA_ID, node_ids, g_rda_test_data_node_num, node_ids, g_rda_test_data_node_num);
    if (ret)
    {
        return;
    }
}


/* IO thread of forwarder process. */
static void *ForwarderRDASenderTestThread(void *argp)
{
    int i = 0;
    uint32_t len     = 0;
    uint32_t seq_num = 0;
    char     *data = NULL;
    FILE *fpp = NULL;

    data = (char *)malloc(FWD_MAX_DATA_PKG_SIZE);
    if (NULL == data)
    {
        return NULL;
    }
    memset(data, 0x37, FWD_MAX_DATA_PKG_SIZE);

    for (;;)
	{
		for (i = 0; i < g_rda_test_data_node_num; i++)
        {
            if (i == my_node_id)
            {
                continue;
            }            
           
            len = FWD_MAX_DATA_PKG_SIZE;
            rda_frame_enqueue_data(&fpp, seq_num, g_rda_test_out_frames[i], 'r', len, my_node_id,  data);
            
            seq_num++;
        }
	}
    free(data);
	return NULL;
}


/* IO thread of forwarder process. */
static void *ForwarderRDAReceiveTestThread(void *argp)
{
    int i = 0;
    uint32_t len = 0;
    uint32_t seq_num = 0;
    char *data = NULL;
    FILE *fpp = NULL;

    data = (char *)malloc(FWD_MAX_DATA_PKG_SIZE);
    if (NULL == data)
    {
        return NULL;
    }

    for (;;)
    {
        for (i = 0; i < g_rda_test_data_node_num; i++)
        {
            if (i == my_node_id)
            {
                continue;
            }

            len = FWD_MAX_DATA_PKG_SIZE;
            rda_frame_test_dequeue(&fpp, g_rda_test_in_frames[i], data, len);

            seq_num++;
        }
    }

    free(data);
    return NULL;
}

void ForwarderTest()
{
    int ret = 0;
    ForwarderTestSendRDADataInit();

    ret = create_thread(ForwarderRDAReceiveTestThread, NULL, MT_THR_DETACHED);
    if (ret)
    {
        return;
    }

    ret = create_thread(ForwarderRDASenderTestThread, NULL, MT_THR_DETACHED);
    if (ret)
    {
        return;
    }
}
#endif
#if 0
void fwd_local_rda_send_test(void)
{
    char data[25];
    int len = 25;
    int rda_id;
    bool succeed = false;
    int index;
    int node_id;
    int total = g_fwd_dn_num + g_fwd_cn_num;
    forwarder_thread_logger(LOG, FORWARDER_PREFIX"---fwd_local_rda_send_test total%d  my_node_id %d", total,my_node_id);
    snprintf(data, 25, "hello,world%d",my_node_id);
    for (rda_id = FWD_DDS_RDA_ID; rda_id < FWD_MAX_RESERVED_RDA_ID; rda_id++)
    {
        for ( index = 0; index < total; index++)
        {
            node_id = index;
            if(index >= g_fwd_dn_num)
                node_id = index + FWD_CN_NODEID_BASE - g_fwd_dn_num;
            if( node_id == my_node_id)
                continue;
            /* code */
           succeed = fwd_local_rda_send(rda_id, node_id, data, len);
           if(succeed)
           {

                forwarder_thread_logger(LOG, FORWARDER_PREFIX"---YYY send data to %d , my_node_id :%d data: %s", node_id, my_node_id, data);
           }
           else
           {
                forwarder_thread_logger(LOG,FORWARDER_PREFIX"---XXX fail to send data to %d ,data: %s",node_id, data);
           }
        }
    }
}

void fwd_local_rda_receive_test(void)
{
    char *data ;
    int len ;
    int rda_id = FWD_DDS_RDA_ID;
    bool succeed = false;
    int index;
    rda_dsm_tup_datarow* datarow;
    int total = g_fwd_dn_num + g_fwd_cn_num;
    int i = 0;
    while (total > 0)
    {
        i++;
        /* code */
        datarow = fwd_local_rda_receive(rda_id);
        if (datarow)
        {
            data = malloc(datarow->msglen);
            if (NULL == data)
                forwarder_thread_logger(LOG,FORWARDER_PREFIX "---YYY data = malloc(datarow->msglen) failed!");
            memcpy(data, datarow->msg, datarow->msglen);
            forwarder_thread_logger(LOG,FORWARDER_PREFIX "---YYY receive data from %d , my_node_id :%ddata: %s",datarow->msgnode, my_node_id, data);
            free(data);
            free(datarow);
            datarow = NULL;
        }
        total--;
        sleep(1);
    }
    
}
/* IO thread of forwarder process. */
static void *ForwarderCNTestThread(void *argp)
{
    
    for (;;)
    {
        if (g_fwd_local_rda_combiner_complete)
        {
            fwd_local_rda_send_test();    
            fwd_local_rda_receive_test();
        }
        else
        {
            sleep(1);
        }
    }

    return NULL;
}

void ForwarderCNTest()
{
    int ret = 0;

    ret = create_thread(ForwarderCNTestThread, NULL, MT_THR_DETACHED);
    if (ret)
    {
        return;
    }
}
#endif

