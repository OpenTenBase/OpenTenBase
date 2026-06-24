#ifndef GPFXDIST_H
#define GPFXDIST_H

#include <apr.h>
#if APR_HAVE_UNISTD_H
#include <unistd.h>
#endif
#if APR_HAVE_IO_H
#include <io.h>
#endif
#include <apr_getopt.h>
#include <apr_env.h>
#include <apr_file_info.h>
#include <apr_hash.h>
#include <apr_pools.h>
#include <apr_strings.h>
#include <apr_time.h>
#include <apr_general.h>
#include <apr_thread_proc.h>
#include <fstream/fstream.h>
#include <event.h>
#include <event2/thread.h>
#include <semaphore.h>
#ifdef USE_SSL
#include <openssl/ssl.h>
#include <openssl/rand.h>
#include <openssl/err.h>
#endif
#include <librdkafka/rdkafka.h>
#include "pgtime.h"
#include "postgres.h"
#include "nodes/pg_list.h"
#include "nodes/parsenodes.h"
#include "nodes/primnodes.h"
#include "orcl_datetime.h"

#define SOCKET int

#define PROTOCOL_COS_STR "cos"
#define PROTOCOL_KAFKA_STR "kafka"
#define RK_TIMEOUT_MS_DEFAULT 1000
#define RK_MESSAGE_BATCH_DEFAULT 5000

typedef enum ProtocolType
{
	LOCAL_FILE,
	REMOTE_COS_PROTOCOL,
	REMOTE_KAFKA_PROTOCOL
} ProtocolType;

/*
 * gpfxdist uses this structure to hold additional options and state.
 */
struct gpfxdist_t
{
	char*		cmd;		/* transformation command */
	int			for_write;	/* 1 if writing to subprocess, 0 if reading from subprocess */
	int			pass_paths; /* 1 if subprocess expects filename to contain paths to data files, 0 otherwise */

	apr_pool_t* mp;			/* apache portable runtime memory pool */
	apr_proc_t	proc;		/* apache portable runtime child process structure */
	char*		tempfilename; /* name of temporary file containing file paths, removed at end */
	char*		errfilename; /* name of temporary file containing stderr output, removed at end */
	apr_file_t* errfile;	/* APR handle for errfilename */
};

typedef struct json_object json_c_object;

typedef struct gnet_request_t
{
	int 	argc;
	char** 	argv;
	int 	hc;			/* header parameter count */
	char* 	hname[50];	/* header parameter name */
	char* 	hvalue[50];	/* header parameter values*/
} gnet_request_t;

/*  A data block */
typedef struct blockhdr_t blockhdr_t;
struct blockhdr_t
{
	char 	hbyte[293];
	int 	hbot, htop;
};

/*
 * Data that is sent from server to client
 */
typedef struct block_t block_t;
struct block_t
{
	blockhdr_t 	hdr;
	int 		bot, top;
	char*      	data;
};

typedef struct redist_buff_t
{
	int 	is_full;
	block_t	outblock;
	int     num_messages;
} redist_buff_t;

typedef struct copy_options
{
	bool csv_mode;
	char *eol;
	int eol_len;
	char *delim;
	int delim_len;
	char quote;
	char escape;
	bool escape_off;
	char *null_print;            /* NULL marker string (server encoding!) */
	int null_print_len;            /* length of same */
	bool *force_notnull_flags;    /* per-column CSV FNN flags */
	bool *force_null_flags;        /* per-column CSV FN flags */
	bool *force_quote_flags;	/* per-column CSV FQ flags, for COPYTO only*/
	bool *convert_select_flags; /* per-column CSV/TEXT CS flags */
	bool fill_missing;        /* missing attrs at end of line are NULL */
	bool ignore_extra_data;    /* ignore extra column at end of line_buf */
	bool compatible_illegal_chars;
	int file_encoding;
	bool need_transcoding;
	char *date_format;          /* user-defined date format */
	char *time_format;          /* user-defined time format */
	char *timestamp_format;     /* user-defined timestamp format */
} copy_options;

/* structure for kafka consuming */
struct KafkaObjects
{
	rd_kafka_t *rk;
	rd_kafka_topic_t *rkt;
};
typedef struct KafkaObjects *KafkaObjects;

typedef struct PositionTDX
{
	PosType flag; 		/* start_offset is relative or absolute */
	int start_offset;	/* start from 1 */
	int end_offset;
} PositionTDX;			/* structure like Position in the kernal */

typedef struct OrclTimeFormat
{
	datetime_field_ord f_ord_dt;
	datetime_field_ord f_ord_ts;
	datetime_field_ord f_ord_tstz;
	datetime_field_str f_str_dt;
	datetime_field_str f_str_ts;
	datetime_field_str f_str_tstz;
} OrclTimeFormat;

/* A session */
typedef struct session_t
{
	long id;
	apr_pool_t *pool;
	const char *key;
	const char *tid;
	const char *path;            /* path requested */
	fstream_t *fstream;
	int is_error;        /* error flag */
	char *errmsg;        /* error message */
	int nrequest;        /* requests attached to this session */
	int is_get;        /* true for GET, false for POST */
	int *active_segids;    /* array indexed by segid. used for write operations
									   to indicate which segdbs are writing and when each
									   is done (sent a final request) */
	int active_seg_count;
	apr_int64_t *seq_segs;       /* array indexed by segid. used for write operations to record
									   the sequence number of data which has been written to disk */
	int maxsegs;        /* same as request->totalsegs. length of active_segs arr */
	apr_time_t mtime;            /* time when nrequest was modified */
	struct timeval tm;             /* timeout for struct event */
	struct event ev;             /* event we are watching for this session */
	apr_hash_t *requests;
	
	/* SHARDING */
	bool need_route;
	bool opentenbase_ora_compatible;
	pg_tz *timezone;
	copy_options *copy_options;
	int shard_count;
	int *shardmap;
	int dis_key_num;
	int fields_num;
	PositionTDX **col_pos;			/* column position info if exists */
	int *dis_key;                   /* start from 0 */
	Oid *dis_key_type;
	List *column_type;
	List *column_name;
	redist_buff_t **redist_buff;  /* shared buffer for every datanode */
	fstream_filename_and_offset **fo; /* line number and fd offset for every datanode */
	bool eof;   /* If this session reach EOF, true only after all read(consumer) threads exits. */
	
	/* thread lock */
	pthread_mutex_t *buffer_mutex;  /* mutex for each shared datanode raw-data buffer */
	pthread_mutex_t fs_mutex;       /* mutex for shared opened file stream */
	pthread_mutex_t err_mutex;      /* used while updating is_error and errmsg of session which is shared */
	pthread_cond_t *append_cond;    /* whether read_thread could append raw-data to shared buffer */
	pthread_cond_t *send_cond;      /* whether send_thread could send out raw-data in share buffer(or cache them locally)*/
	
	/* thread */
	pthread_t *pt;  /* reading and processing threads */
	pthread_t *st;  /* sending threads. Allocate one send_thread for per datanode */
	bool        pthread_setup;
	
	ProtocolType remote_protocol_type;
	/* FOR COS */
	char *bucket;
	cos_fstream_t *cos_fs;
	
	/* FOR KAFKA */
	int partition_cnt; /* Number of partitions in */
	int *volatile partition_ids;
	int64_t *offsets;
	
	int active_prts;    /* number of active thread(or partitions) */
	char *topic_name;
	char *group_id;     /* reserved */
	KafkaObjects kobj;  /* librdkafka internal state (not in apr_pool memory)*/
	rd_kafka_conf_t *kafka_conf;
	
	/* position of operator or values in message */
	List *op_pos;
	List *before_pos;
	List *after_pos;
	
	size_t batch_size;          /* Datanode request size on the number of Kafka messages for each partition */
	size_t rk_messages_size;    /* LibRdKafka API Consume Batch Limit on the number of Kafka messages retrieved once */
	uint rk_timeout_ms;         /* LibRdKafka API Consume Batch timeout Limit for Kafka message. */

	OrclTimeFormat orcl_time_format; /* used while in opentenbase_ora mode, to format the time */
	
	bool need_convert_datetime;
	bool is_logical_sync;
} session_t;

/*  An http request */
typedef struct request_t
{
	long			id;			/* request id (auto increment) */
	long			sid;		/* session id (auto increment) */
	long			bytes; 		/* bytes sent to TCP or receive from TCP */
	apr_time_t		last; 		/* last timestamp for send/receive data */
	apr_int64_t     seq;        /* sequence number */
	
	unsigned short  port;
	SOCKET 			sock; 		/* the socket */
	apr_pool_t* 	pool; 		/* memory pool container */
	struct timeval 	tm; 		/* timeout for struct event */
	struct event 	ev; 		/* event we are watching for this request */
	const char* 	peer; 		/* peer IP:port string */
	const char* 	path; 		/* path to file */
	const char* 	tid; 		/* transaction id */
	const char* 	csvopt; 	/* options for csv file */
	const char* 	formatopt; 	/* format options for file */

#ifdef GPFXDIST
	struct
	{
		const char* name;		/* requested transformation */
		char*		command;	/* command associated with transform */
		int			paths;		/* 1 if filename passed to transform should contain paths to data files */
		const char* errfilename; /* name of temporary file holding stderr to send to server */
		apr_file_t* errfile;	/* temporary file holding stderr to send to server */
	} trans;
#endif
	
	session_t* 		session; 	/* the session this request is attached to */
	int 			tdx_proto; 	/* the protocol to use, sent from client */
	int				is_get;     /* true for GET, false for POST */
	int				is_final;	/* the final POST request. a signal from client to end session */
	int				segid;		/* the segment id of the segdb with the request */
	int				totalsegs;	/* the total number of segdbs */
	
	struct
	{
		char* 	hbuf; 			/* buffer for raw incoming HTTP request */
		int 	hbuftop; 		/* bytes used in hbuf */
		int 	hbufmax; 		/* size of hbuf[] */
		gnet_request_t* req;	/* a parsed HTTP request, NULL if still incomplete. */
		int		davailable;		/* number of data bytes available to consume */
		char*	dbuf;			/* buffer for raw data from a POST request */
		int 	dbuftop; 		/* # bytes used in dbuf */
		int 	dbufmax; 		/* size of dbuf[] */
	} in;
	
	block_t	outblock;			/* next block to send out */
	
	char   *line_delim_str;
	int     line_delim_length;
	
	int     need_route;
	char   *col_delim_str;
	int     col_delim_length;
	int		shard_num_pos;
	int		shard_node_pos;
	int		fields_num_pos;
	int		col_position_pos;
	int		dis_key_num_pos;
	int		dis_key_pos;
	int		dis_key_type_pos;
	int		col_type_pos;
	int     col_name_pos;
	int     force_notnull_flags_pos;
	int     force_null_flags_pos;
	int     convert_select_flags_pos;
	int     force_quote_flags;
	char   *null_print;
	int     null_print_len;
	int     file_encoding;
	char   *extra_error_opts;
	int     time_format_len;
	char   *time_format;
	int     timestamp_format_len;
	char   *timestamp_format;
	int     date_format_len;
	char   *date_format;
	int     opentenbase_ora_compatible;
	char   *timezone_string;
	int     is_logical_msg;
	char   *timestamptz_format;
	int    timestamptz_formate_len;

#ifdef GPFXDIST
	char *remote_data_proto;
	struct
	{
		const char *name;        /* origin protocol */
		ossConf *conf;
		char *bucket_name;
	} cos;
	char    *conf_str;   /* conf passed in path */
	struct
	{
		char *topic;
		char *broker_id;
		char *group_id;
		int partition_cnt;
		char *offset_str;
		size_t r_max_messages;
	} kafka;
#endif

#ifdef USE_SSL
	/* SSL related */
	BIO			*io;		/* for the i.o. */
	BIO			*sbio;		/* for the accept */
	BIO			*ssl_bio;
	SSL			*ssl;
#endif
} request_t;

/* Memory Space allocated in advance for each batch-consumption-processing thread for Kafka messages. */
typedef struct batch_thread_para
{
	int j;
	int prt_no;
	sem_t *pre_finish_sem;
	sem_t *cur_finish_sem;
	session_t *session;     /* if this thread memory space bind to any session ? */
	size_t num_consumed;
	rd_kafka_message_t **messages;
	pthread_mutex_t u_mutex;    /* mem space could be reused, so need mutex. */
	bool used;
	
	/* kafka consume object */
	rd_kafka_conf_t *kafka_conf;
	rd_kafka_t *rk;
	rd_kafka_topic_t *rkt;
	
	int start_offset;
	size_t batch_size;
} batch_thread_para;

typedef struct read_thread_para
{
	int j;
	session_t *session;
	int num_batch_thread;
	batch_thread_para **bt_paras;
	sem_t *last_finish_sem;
} read_thread_para;

#define MAX_READ_THREAD_DEFAULT 16
#define THREAD_POOL_SIZE_DEFAULT 48

#if APR_IS_BIGENDIAN
#define local_htonll(n)  (n)
#define local_ntohll(n)  (n)
#else
#define local_htonll(n)  ((((apr_uint64_t) htonl(n)) << 32LL) | htonl((n) >> 32LL))
#define local_ntohll(n)  ((((apr_uint64_t) ntohl(n)) << 32LL) | (apr_uint32_t) ntohl(((apr_uint64_t)n) >> 32LL))
#endif

#define NO_SEQ    0
#define OPEN_SEQ  1

typedef union address
{
	struct sockaddr sa;
	struct sockaddr_in sa_in;
	struct sockaddr_in6 sa_in6;
	struct sockaddr_storage sa_stor;
} address_t;
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-variable"
/* Global control block */
static struct
{
	apr_pool_t* 	pool;
	int				listen_sock_count;
	SOCKET 			listen_socks[6];
	struct event 	listen_events[6];
	struct event    signal_event;
	struct
	{
		int 		gen;
		apr_hash_t* tab;
	} session;
	apr_int64_t 	read_bytes;		/* bytes already read, for debug */
	apr_int64_t 	total_bytes;	/* total bytes to be read, for debug */
	int 			total_sessions;
#ifdef USE_SSL
	BIO 			*bio_err;		/* for SSL */
	SSL_CTX 		*server_ctx;	/* for SSL */
#endif
	int 			wdtimer; /* Kill gpfdist after k seconds of inactivity. 0 to disable. */
} gcb;
#pragma GCC diagnostic pop
#define TDXPATHDELIM   '|'

char *find_first_eol_delim(char *start, char *end, const char *delimiter,
						   const int delimiter_length, const int max_line_len,
						   int *error);

void gfatal(const request_t *r, const char *fmt, ...);

void gwarning(const request_t *r, const char *fmt, ...);

void gprintln(const request_t *r, const char *fmt, ...);
#endif
