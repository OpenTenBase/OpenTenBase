#ifdef WIN32
/* exclude transformation features on windows for now */
#undef GPFXDIST
#ifndef _WIN32_WINNT
#define _WIN32_WINNT 0x0501
#endif
#endif

#include <fcntl.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>

#ifndef WIN32

#include <strings.h>
#include <unistd.h>

#include <sys/sysinfo.h>

#endif
#ifdef GPFXDIST
#include <regex.h>
#endif

#ifndef WIN32

#include <unistd.h>
#include <sys/socket.h>
#include <sys/ioctl.h>
#include <netinet/in.h>
#include <netdb.h>
#include <arpa/inet.h>
#include <pthread.h>

#ifndef closesocket
#define closesocket(x)   close(x)
#endif
#else
#define WIN32_LEAN_AND_MEAN
#include <winsock2.h>
#include <ws2tcpip.h>
#include <windows.h>
#include <io.h>
#define SHUT_WR SD_SEND
#define socklen_t int
#undef ECONNRESET
#define ECONNRESET   WSAECONNRESET
#endif

#include <fstream/fstream.h>
#include <pg_config.h>
#include <pg_config_manual.h>
#include "tdx_tool.h"
#include "threadpool.h"

#ifdef USE_SSL
#include <openssl/ssl.h>
#include <openssl/rand.h>
#include <openssl/err.h>
#endif

#include "utils/palloc.h"
#include "utils/memutils.h"
#include "catalog/pg_proc.h"
#include "pgxc/shardmap.h"
#include "datatype/timestamp.h"
#include "utils/numeric.h"
#include "mb/pg_wchar.h"
#include "utils/guc.h"
#include "commands/copy.h"
#include "commands/variable.h"
#include "../../../timezone/pgtz.h"
#include "utils/varlena.h"
#include "logical_proto.h"
#include "fmgr.h"
#include "utils/orcl_datetime_formatting.h"

#define TDX_VERSION_STR "2.1(>=OpenTenBase_V3.15.5)"
threadpool_t *threadpool = NULL;
int meta_timeout = 1000;
const char *progname;
const char *exename;
/*  Get session id for this request */
#define GET_SID(r)    ((r->sid))
static long REQUEST_SEQ = 0;        /*  sequence number for request */
static long SESSION_SEQ = 0;        /*  sequence number for session */

#define MAX_NODE_NUM 200

static int URLDecode(const char *str, const int strsz, char *result, const int resultsz);
#ifdef USE_SSL
/* SSL additions */
#define SSL_RENEGOTIATE_TIMEOUT_SEC    (600) /* 10 minutes */
const char *const CertificateFilename = "server.crt";
const char *const PrivateKeyFilename = "server.key";
const char *const TrustedCaFilename = "root.crt";

static SSL_CTX *initialize_ctx(void);

static void handle_ssl_error(SOCKET sock, BIO *sbio, SSL *ssl);

static void flush_ssl_buffer(int fd, short event, void *arg);
/* SSL end */
#endif
ossContext cosContext = NULL;

extern void _gprint(const request_t *r, const char *level, const char *fmt, va_list args);
extern void kafka_obj_close(KafkaObjects kobj);

/**************

 NOTE on TDX_PROTO
 ================
 When a segment connects to tdx, it must provide the following parameters:
 XID    	- transaction ID
 CID    	- command ID to distinguish different queries.
 SN     	- scan number to distinguish scans on
 			  the same external tables within the same query.
 CSVOPT		- CSV option format
 X-TDX-PROTO - protocol number, report error if not provided:

 X-TDX-PROTO = 0
 return the content of the file without any kind of meta info

 X-TDX-PROTO = 1
 each data block is tagged by meta info like this:
 byte 0: type (can be 'F'ilename, 'O'ffset, 'D'ata, 'E'rror, 'L'inenumber)
 byte 1-4: length. # bytes of following data block. in network-order.
 byte 5-X: the block itself.

 The stream is terminated by a Data block of length 0. If the stream is
 not property terminated, then tdx encountered some error, and caller
 should check the tdx error log.

 **************/
static gnet_request_t *gnet_parse_request(const char *buf, int *len,
                                          apr_pool_t *pool);

static char *gstring_trim(char *s);

static void percent_encoding_to_char(char *p, char *pp, char *path);

/* CR-2723 */
#define TDX_MAX_LINE_LOWER_LIMIT (32*1024)
#ifdef GPFXDIST
#define TDX_MAX_LINE_UPPER_LIMIT (256*1024*1024)
#define TDX_MAX_LINE_MESSAGE     "Error: -m max row length must be between 32KB and 256MB"
#else
#define TDX_MAX_LINE_UPPER_LIMIT (1024*1024)
#define TDX_MAX_LINE_MESSAGE     "Error: -m max row length must be between 32KB and 1MB"
#endif

static void log_tdx_status();

static void log_request_header(const request_t *r);

/* send gp-proto==1 ctl info */
static void gp1_send_eof(request_t *r);

static void gp1_send_errmsg(request_t *r, const char *msg);

#ifdef GPFXDIST
static void gp1_send_errfile(request_t* r, apr_file_t* errfile);
#endif

/* get the encoding id for a given encoding name */
static int get_encoding_id(const char *encoding_name)
{
	int			enc;
	
	if (encoding_name && *encoding_name)
	{
		if ((enc = pg_valid_server_encoding(encoding_name)) >= 0)
			return enc;
	}
	gfatal(NULL, "\"%s\" is not a valid server encoding name",
	       encoding_name ? encoding_name : "(null)");
	return -1;
}

static void stream_close(session_t *session);

static int setup_read(request_t *r);

static int setup_write(request_t *r);

static void setup_do_close(request_t *r);

static int session_attach(request_t *r);

static void session_detach(request_t *r);

static void session_end(session_t *session, int error);

static void session_free(session_t *session);

static void session_active_segs_dump(session_t *session);

static int session_active_segs_isempty(session_t *session);

static int request_validate(request_t *r);

static int request_set_path(request_t *r, const char *d, char *p, char *pp, char *path);

static int request_path_validate(request_t *r, const char *path);

static int request_parse_tdx_headers(request_t *r, int opt_g);

static int ReadAttributesText(session_t *session, StringInfo line_buf,
							  StringInfo attribute_buf, char ***raw_fields_ptr,
							  Bitmapset *dis_bitmap);
static int ReadAttributesPos(session_t *session, StringInfo line_buf,
							 StringInfo attribute_buf, char ***raw_fields_ptr);

static void free_session_cb(int fd, short event, void *arg);

#ifdef GPFXDIST
static int request_set_transform(request_t *r);
#endif

static void handle_post_request(request_t *r, int header_end);

static void handle_get_request(request_t *r);

static bool transform_position_info(const apr_pool_t *pool,
									const char *col_pos_str, int col_num,
									PositionTDX **pos);

/* static int data_locate(const request_t *r); */
static int tdx_socket_send(const request_t *r, const void *buf, const size_t buflen);

static int (*tdx_send)(const request_t *r, const void *buf, const size_t buflen); /* function pointer */
static int tdx_socket_receive(const request_t *r, void *buf, const size_t buflen);

static int (*tdx_receive)(const request_t *r, void *buf, const size_t buflen); /* function pointer */
static void request_cleanup(request_t *r);

#ifdef USE_SSL

static int tdx_SSL_send(const request_t *r, const void *buf, const size_t buflen);

static int tdx_SSL_receive(const request_t *r, void *buf, const size_t buflen);

static void free_SSL_resources(const request_t *r);

static void setup_flush_ssl_buffer(request_t *r);

static void request_cleanup_and_free_SSL_resources(request_t *r);

#endif

static int local_send(request_t *r, const char *buf, int buflen);

static int get_unsent_bytes(request_t *r);

static void *palloc_safe(request_t *r, apr_pool_t *pool, apr_size_t size, const char *fmt, ...)
pg_attribute_printf(4, 5);

static void *pcalloc_safe(request_t *r, apr_pool_t *pool, apr_size_t size, const char *fmt, ...)
pg_attribute_printf(4, 5);

static void process_term_signal(int sig, short event, void *arg);

int tdx_init(int argc, const char *const argv[]);

int tdx_run(void);

static bool CheckIfTruncMbCharTDX(int file_encoding, const char *start_ptr,
								  const char *cur_mb_ptr, char *last_mb_char);

static void delay_watchdog_timer(void);

#ifndef WIN32
static apr_time_t shutdown_time;

static void *watchdog_thread(void *);

#endif

static void* read_thread(void *para);
static void* send_thread(void *para);
static void* watch_thread(void *para);
static void* consumer_thread(void *para);

static int block_fill_header(const request_t *r, block_t *b,
                              const fstream_filename_and_offset *fos);
static int stream_block_fill_header(const request_t *r, block_t *b,
                                    const fstream_filename_and_offset *fos,
                                    int num_messages, int num_prt, int64_t *offsets);
/*
 * Prepare a block header for sending to the client. It includes various meta
 * data information such as filename, initial linenumber, etc. This will only
 * get used in PROTO-1. We store this header in block_t->hdr (a blockhdr_t)
 * and PROTO-0 never uses it.
 */
static int block_fill_header(const request_t *r, block_t *b,
                              const fstream_filename_and_offset *fos)
{
	blockhdr_t *h = &b->hdr;
	apr_int32_t len;
	apr_int64_t len8;
	char *p = h->hbyte;
	int fname_len = strlen(fos->fname);
	
	h->hbot = 0;
	
	/* FILENAME: 'F' + len + fname */
	*p++ = 'F';
	len = htonl(fname_len);
	memcpy(p, &len, 4);
	p += 4;
	memcpy(p, fos->fname, fname_len);
	p += fname_len;
	gdebug(r, "F %u %s", (unsigned int) ntohl(len), fos->fname);
	
	/* OFFSET: 'O' + len + foff */
	*p++ = 'O';
	len = htonl(8);
	len8 = local_htonll(fos->foff);
	memcpy(p, &len, 4);
	p += 4;
	memcpy(p, &len8, 8);
	p += 8;
#ifndef WIN32
	gdebug(r, "O %llu", (unsigned long long) local_ntohll(len8));
#else
	gdebug(r, "O %lu",(unsigned long) local_ntohll(len8));
#endif
	
	/* LINENUMBER: 'L' + len + linenumber */
	*p++ = 'L';
	len8 = local_htonll(fos->line_number);
	memcpy(p, &len, 4);
	p += 4;
	memcpy(p, &len8, 8);
	p += 8;
#ifndef WIN32
	gdebug(r, "L %llu", (unsigned long long) local_ntohll(len8));
#else
	gdebug(r, "L %lu", (unsigned long)local_ntohll(len8));
#endif
	
	/* DATA: 'D' + len */
	*p++ = 'D';
	len = htonl(b->top - b->bot);
	memcpy(p, &len, 4);
	p += 4;
	gdebug(r, "D %u", (unsigned int) ntohl(len));
	h->htop = p - h->hbyte;
	if (h->htop > sizeof(h->hbyte))
	{
		gwarning(NULL, "assert failed, h->htop = %d, max = %d", h->htop,
		       (int) sizeof(h->hbyte));
		return -1;
	}
	gdebug(r, "header size: %d", h->htop - h->hbot);
	return 0;
}

static int stream_block_fill_header(const request_t *r, block_t *b,
                              const fstream_filename_and_offset *fos,
							  int num_messages, int num_prt, int64_t *offsets)
{
	blockhdr_t *h = &b->hdr;
	apr_int32_t len;
	apr_int64_t len8;
	apr_int32_t len4;
	int prt_idx = 0;
	char *p = h->hbyte;
	int fname_len = strlen(fos->fname);
	
	h->hbot = 0;
	
	/* FILENAME: 'F' + len + fname */
	*p++ = 'F';
	len = htonl(fname_len);
	memcpy(p, &len, 4);
	p += 4;
	memcpy(p, fos->fname, fname_len);
	p += fname_len;
	gdebug(r, "F %u %s", (unsigned int) ntohl(len), fos->fname);
	
	/* OFFSET: 'O' + len + foff */
	*p++ = 'O';
	len = htonl(8);
	len8 = local_htonll(fos->foff);
	memcpy(p, &len, 4);
	p += 4;
	memcpy(p, &len8, 8);
	p += 8;
#ifndef WIN32
	gdebug(r, "O %llu", (unsigned long long) local_ntohll(len8));
#else
	gdebug(r, "O %lu",(unsigned long) local_ntohll(len8));
#endif
	
	/* LINENUMBER: 'L' + len + linenumber */
	*p++ = 'L';
	len8 = local_htonll(fos->line_number);
	memcpy(p, &len, 4);
	p += 4;
	memcpy(p, &len8, 8);
	p += 8;
#ifndef WIN32
	gdebug(r, "L %llu", (unsigned long long) local_ntohll(len8));
#else
	gdebug(r, "L %lu", (unsigned long)local_ntohll(len8));
#endif
	
	if (num_messages > 0)
	{
		/* Kafka message number: 'N' + len + num_messages */
		*p++ = 'N';
		len = htonl(4);
		len4 = htonl(num_messages);
		memcpy(p, &len, 4);
		p += 4;
		memcpy(p, &len4, 4);
		p += 4;
		gdebug(r, "N %llu", (unsigned long long) local_ntohll(len8));
	}
	
	/* Kafka prt number: 'P' + len + prt_num */
	*p++ = 'P';
	len4 = htonl(num_prt);
	memcpy(p, &len, 4);
	p += 4;
	memcpy(p, &len4, 4);
	p += 4;
	
	/* Kafka prt number: 'P' + len + prt_num */
	for (prt_idx = 0; prt_idx < num_prt; prt_idx++)
	{
		/* prt-offset pairs */
		len4 = htonl(prt_idx);
		len8 = local_htonll(offsets[prt_idx]);
		
		memcpy(p, &len4, 4);
		p += 4;
		memcpy(p, &len8, 8);
		p += 8;
		
		gdebug(r, "send prt and offset: %u %llu", (unsigned int) ntohl(len4), (unsigned long long) local_ntohll(len8));
	}
	
	/* DATA: 'D' + len */
	*p++ = 'D';
	len = htonl(b->top - b->bot);
	memcpy(p, &len, 4);
	p += 4;
	gdebug(r, "D %u", (unsigned int) ntohl(len));
	h->htop = p - h->hbyte;
	if (h->htop > sizeof(h->hbyte))
	{
		gwarning(NULL, "assert failed, h->htop = %d, max = %d", h->htop,
		       (int) sizeof(h->hbyte));
		return -1;
	}
	gdebug(r, "header size: %d", h->htop - h->hbot);
	return 0;
}

static unsigned short get_client_port(address_t *clientInformation)
{
	/*
	 * check the family version of client IP address, so you
	 * can know where to cast, either to sockaddr_in or sockaddr_in6
	 * and then grab the port after casting
	 */
	struct sockaddr *sa = (struct sockaddr *) clientInformation;
	if (sa->sa_family == AF_INET)
	{
		struct sockaddr_in *ipv4 = (struct sockaddr_in *) clientInformation;
		return ipv4->sin_port;
	}
	else
	{
		struct sockaddr_in6 *ipv6 = (struct sockaddr_in6 *) clientInformation;
		return ipv6->sin6_port;
	}
}

/* Print usage */
static void usage_error(const char *msg, int print_usage)
{
	if (print_usage)
	{
		char *GPHOME = 0;
		FILE *fp = 0;
		
		if (gcb.pool && apr_env_get(&GPHOME, "GPHOME", gcb.pool))
			GPHOME = 0;
		
		if (GPHOME)
		{
			char *path = apr_psprintf(gcb.pool, "%s/docs/cli_help/tdx_help",
			                          GPHOME);
			if (path)
				fp = fopen(path, "r");
		}
		
		if (fp)
		{
			int i;
			while ((i = getc(fp)) != EOF)
			{
				putchar(i);
			}
			fclose(fp);
		}
		else
		{
			fprintf(stderr,
			        "tdx -- file distribution web server\n\n"
			        "usage: tdx [--ssl <certificates_directory>] [-d <directory>] [-p <http(s)_port>] [-l <log_file>] [-t <timeout>] [-v | -V | -s] [-m <maxlen>] [-w <timeout>]"
			        #ifdef GPFXDIST
			        "[-c file]"
			        #endif
			        "\n\n"
			        "       tdx [-? | --help] | --version\n\n"
			        "        -?, --help : print this screen\n"
			        "        -v         : verbose mode\n"
			        "        -V         : more verbose\n"
			        "        -s         : simplified minimum log\n"
			        #ifdef USE_SSL
			        "        -p port    : port to serve HTTP(S), default is 8080\n"
			        #else
			        "        -p port    : port to serve HTTP, default is 8080\n"
			        #endif
			        "        -d dir     : serve files under the specified directory,  default is '.'\n"
			        "        -l logfn   : log filename\n"
			        "        -t tm      : timeout in seconds \n"
			        "        -m maxlen  : max data row length expected, in bytes. default is 32768\n"
			        #ifdef USE_SSL
			        "        --ssl dir  : start HTTPS server. Use the certificates from the specified directory\n"
			        #endif
			        #ifdef GPFXDIST
			        "        -c file    : configuration file for transformations\n"
			        #endif
			        "        --version  : print version information\n"
			        "        -w timeout : timeout in seconds before close target file\n"
			        "        -e encoding: encoding set same as OPENTENBASE server, default UTF-8\n"
			        "        -n parallel: max number of threads tdx server could launch\n\n");
		}
	}
	
	if (msg)
		fprintf(stderr, "%s\n", msg);
	
	exit(msg ? 1 : 0);
}

static void print_version(void)
{
	printf("TDX(Tencent Data eXchanger) version \"%s\"\n", TDX_VERSION_STR);
	exit(0);
}

static void print_q_x_h_are_gone(void)
{
	fprintf(stderr, "The -q, -h and -x options are gone.  Please specify these as in this example:\n");
	fprintf(stderr,
	        "create external table a (a int) location ('tdx://...') format 'csv' (escape as '\"' quote as '\"' header);\n");
	exit(1);
}

/* Parse command line */
static void parse_command_line(int argc, const char *const argv[],
                               apr_pool_t *pool)
{
	apr_getopt_t *os;
	const char *arg;
	char apr_errbuf[256];
	int status;
	int ch;
	int e;
	
	char *current_directory = NULL;
	
	static const apr_getopt_option_t option[] =
			{
					/* long-option, short-option, has-arg flag, description */
					{"help", '?', 0, "print help screen"},
					{NULL, 'V', 0, "very verbose"},
					{NULL, 'v', 0, "verbose mode"},
					{NULL, 's', 0, "simplified log without request header"},
					{NULL, 'p', 1, "which port to serve HTTP(S)"},
					{NULL, 'P', 1, "last port of range of ports to serve HTTP(S)"},
					{NULL, 'd', 1, "serve files under this directory"},
					{NULL, 'f', 1, "internal - force file to be given file name"},
					{NULL, 'b', 1, "internal - bind to ip4 address"},
					{NULL, 'q', 0, "gone"},
					{NULL, 'x', 0, "gone"},
					{NULL, 'h', 0, "gone"},
					{NULL, 'l', 1, "log filename"},
					{NULL, 't', 5, "timeout in seconds"},
					{NULL, 'g', 1, "internal - tdx_proto number"},
					{NULL, 'm', 1, "max data row length expected"},
					{NULL, 'S', 0, "use O_SYNC when opening files for write"},
					{NULL, 'z', 1, "internal - queue size for listen call"},
					{"ssl", 257, 1, "ssl - certificates files under this directory"},
#ifdef GPFXDIST
					{ NULL, 'c', 1, "transform configuration file" },
#endif
					{"version", 256, 0, "print version number"},
					{NULL, 'w', 1, "wait for session timeout in seconds"},
					{"encoding", 'e', 1, "opentenbase server encoding set"},
					{"parallel", 'n', 1, "max number of threads tdx server could launch"},
					{"poolsize", 'o', 1, "max size of thread pool for kafka streaming sync."},
					{0}};
	
	status = apr_getopt_init(&os, pool, argc, argv);
	
	if (status != APR_SUCCESS)
		gfatal(NULL, "apt_getopt_init failed: %s",
		       apr_strerror(status, apr_errbuf, sizeof(apr_errbuf)));
	
	while (APR_SUCCESS == (e = apr_getopt_long(os, option, &ch, &arg)))
	{
		switch (ch)
		{
			case '?':
				usage_error(0, 1);
				break;
			case 'v':
				opt.v = 1;
				break;
			case 'V':
				opt.v = opt.V = 1;
				break;
			case 's':
				opt.s = 1;
				break;
			case 'h':
				print_q_x_h_are_gone();
				break;
			case 'd':
				opt.d = arg;
				break;
			case 'f':
				opt.f = arg;
				break;
			case 'q':
				print_q_x_h_are_gone();
				break;
			case 'x':
				print_q_x_h_are_gone();
				break;
			case 'p':
				opt.last_port = opt.p = atoi(arg);
				break;
			case 'P':
				opt.last_port = atoi(arg);
				break;
			case 'l':
				opt.l = arg;
				break;
			case 't':
				opt.t = atoi(arg);
				break;
			case 'g':
				opt.g = atoi(arg);
				break;
			case 'b':
				opt.b = arg;
				break;
			case 'm':
				opt.m = atoi(arg);
				break;
			case 'S':
				opt.S = 1;
				break;
			case 'z':
				opt.z = atoi(arg);
				break;
			case 'c':
				opt.c = arg;
				break;
#ifdef USE_SSL
			case 257:
				opt.ssl = arg;
				break;
#else
			case 257:
				usage_error("SSL is not supported by this build", 0);
				break;
#endif
			case 256:
				print_version();
				break;
			case 'w':
				opt.w = atoi(arg);
				break;
			case 'e':
				opt.e = get_encoding_id(arg);
				break;
			case 'n':
				opt.parallel = atoi(arg);
				break;
			case 'o':
				opt.thread_pool_size = atoi(arg);
				break;
		}
	}
	
	if (e != APR_EOF)
		usage_error("Error: illegal arguments", 1);
	
	if (!(0 < opt.p && opt.p < (1 << 16)))
		usage_error("Error: please specify a valid port number for -p switch", 0);
	
	if (-1 != opt.g)
	{
		if (!(0 == opt.g || 1 == opt.g))
			usage_error("Error: please specify 0 or 1 for -g switch (note: this is internal)", 0);
	}
	
	if (!is_valid_timeout(opt.t))
		usage_error("Error: -t timeout must be between 2 and 7200, or 0 for no timeout", 0);
	
	if (!is_valid_session_timeout(opt.w))
		usage_error("Error: -w timeout must be between 1 and 7200, or 0 for no timeout", 0);
	
	/* validate max row length */
	if (!((TDX_MAX_LINE_LOWER_LIMIT <= opt.m) && (opt.m <= TDX_MAX_LINE_UPPER_LIMIT)))
		usage_error(TDX_MAX_LINE_MESSAGE, 0);
	
	if (!is_valid_listen_queue_size(opt.z))
		usage_error("Error: -z listen queue size must be between 16 and 512 (default is 256)", 0);
	
	/* get current directory, for ssl directory validation */
	if (0 != apr_filepath_get(&current_directory, APR_FILEPATH_NATIVE, pool))
		usage_error(apr_psprintf(pool, "Error: cannot access directory '.'\n"
		                               "Please run tdx from a different location"), 0);
	
	/* validate opt.d */
	{
		char *p = gstring_trim(apr_pstrdup(pool, opt.d));
		
		/* collapse // */
		while (p[0] == '/' && p[1] == '/')
		{
			p++;
		}
		
		/* disallow / */
		if (0 == strcmp(p, "/"))
			usage_error("Security Error:  You cannot specify the root"
			            " directory (/) as the source files directory.", 0);
		
		/* strip ending / */
		while (p[0] && p[strlen(p) - 1] == '/')
		{
			p[strlen(p) - 1] = 0;
		}
		opt.d = p;
		
		if (0 == strlen(opt.d))
			opt.d = ".";
		
		/* check that the dir exists */
		if (0 != apr_filepath_set(opt.d, pool))
			usage_error(apr_psprintf(pool, "Error: cannot access directory '%s'\n"
			                               "Please specify a valid directory for -d switch", opt.d), 0);
		
		if (0 != apr_filepath_get(&p, APR_FILEPATH_NATIVE, pool))
			usage_error(apr_psprintf(pool, "Error: cannot access directory '%s'\n"
			                               "Please specify a valid directory for -d switch", opt.d), 0);
		opt.d = p;
	}
	
	/* validate opt.l */
	if (opt.l)
	{
		FILE *f;
		
		char *p = gstring_trim(apr_pstrdup(pool, opt.l));
		
		/* collapse // */
		while (p[0] == '/' && p[1] == '/')
		{
			p++;
		}
		
		/* disallow / */
		if (0 == strcmp(p, "/"))
			usage_error("Security Error: You cannot specify the root"
			            " directory (/) as the log file directory.", 0);
		
		/* strip ending / */
		while (p[0] && p[strlen(p) - 1] == '/')
		{
			p[strlen(p) - 1] = 0;
		}
		opt.l = p;
		
		if (0 == strlen(opt.l))
			opt.l = ".";
		
		/* check that the file exists */
		f = fopen(opt.l, "a");
		if (!f)
		{
			fprintf(stderr, "unable to create log file %s: %s\n",
			        opt.l, strerror(errno));
			exit(1);
		}
		fclose(f);
		
	}

#ifdef USE_SSL
	/* validate opt.ssl */
	if (opt.ssl)
	{
		char *p = gstring_trim(apr_pstrdup(pool, opt.ssl));
		
		/* collapse // */
		while (p[0] == '/' && p[1] == '/')
		{
			p++;
		}
		
		/* disallow / */
		if (0 == strcmp(p, "/"))
			usage_error("Security Error: You cannot specify the root"
			            " directory (/) as the certificates directory", 0);
		
		/* strip ending / */
		while (p[0] && p[strlen(p) - 1] == '/')
		{
			p[strlen(p) - 1] = 0;
		}
		opt.ssl = p;
		
		/* change current directory to original one (after -d changed it) */
		if (0 != apr_filepath_set(current_directory, pool))
			usage_error(apr_psprintf(pool, "Error: cannot access directory '%s'\n"
			                               "Please run tdx from a different location", current_directory), 0);
		/* check that the dir exists */
		if ((0 != apr_filepath_set(opt.ssl, pool)) || (0 != apr_filepath_get(&p, APR_FILEPATH_NATIVE, pool)))
			usage_error(apr_psprintf(pool, "Error: cannot access directory '%s'\n"
			                               "Please specify a valid directory for --ssl switch", opt.ssl), 0);
		opt.ssl = p;
	}
#endif

#ifdef GPFXDIST
	/* validate opt.c */
	if (opt.c)
	{
		if (transform_config(opt.c, &opt.trlist, &opt.dslist, opt.V))
		{
			/* transform_config has already printed a message to stderr on failure */
			exit(1);
		}
	}
#endif

	gprintln(NULL, "Max Thread NUM %d", opt.parallel);
	gprintln(NULL, "Pool Max Thread NUM %d", opt.thread_pool_size);
	gprintln(NULL, "BLOCK SIZE %d", opt.m);

	/* encoding set */
	SetDatabaseEncoding(opt.e);
	// TODO: custom client encoding
	SetClientEncoding(GetDatabaseEncoding());
	gprintln(NULL, "Set Server Encoding (%d)", opt.e);
	
	/* there should not be any more args left */
	if (os->ind != argc)
		usage_error("Error: illegal arguments", 1);
}

/* http error codes used by tdx */
#define FDIST_OK 200
#define FDIST_BAD_REQUEST 400
#define FDIST_TIMEOUT 408
#define FDIST_INTERNAL_ERROR 500

/* send an error response */
static void http_error(request_t *r, int code, const char *msg)
{
	char buf[1024];
	int n;
	gwarning(r, "HTTP ERROR: %s - %d %s\n", r->peer, code, msg);
	n = apr_snprintf(buf, sizeof(buf), "HTTP/1.0 %d %s\r\n"
	                                   "Content-length: 0\r\n"
	                                   "Expires: 0\r\n"
	                                   "X-TDX-VERSION: "
	PGXC_VERSION/*TDX_VERSION*/ "\r\n"
	                           "Cache-Control: no-cache\r\n"
	                           "Connection: close\r\n\r\n", code, msg);
	
	local_send(r, buf, n);
}

/* send an empty response */
static void http_empty(request_t *r)
{
	static const char buf[] = "HTTP/1.0 200 ok\r\n"
	                          "Content-type: text/plain\r\n"
	                          "Content-length: 0\r\n"
	                          "Expires: 0\r\n"
	                          "X-TDX-VERSION: "
	PGXC_VERSION/*TDX_VERSION*/ "\r\n"
	                           "Cache-Control: no-cache\r\n"
	                           "Connection: close\r\n\r\n";
	gprintln(r, "HTTP EMPTY: %s %s %s - OK", r->peer, r->in.req->argv[0], r->in.req->argv[1]);
	local_send(r, buf, sizeof buf - 1);
}

/* send a Continue response */
static void http_continue(request_t *r)
{
	static const char buf[] = "HTTP/1.1 100 Continue\r\n\r\n";
	
	gprintlnif(r, "%s %s %s - Continue", r->peer, r->in.req->argv[0], r->in.req->argv[1]);
	
	local_send(r, buf, sizeof buf - 1);
}

/* send an OK response */
static apr_status_t http_ok(request_t *r)
{
	const char *fmt = "HTTP/1.0 200 ok\r\n"
	                  "Content-type: text/plain\r\n"
	                  "Expires: 0\r\n"
	                  "X-TDX-VERSION: "
	PGXC_VERSION/*TDX_VERSION*/"\r\n"
	                          "X-TDX-PROTO: %d\r\n"
	                          "Cache-Control: no-cache\r\n"
	                          "Connection: close\r\n\r\n";
	static char buf[1024];
	int m, n;
	
	n = apr_snprintf(buf, sizeof(buf), fmt, r->tdx_proto);
	if (n >= sizeof(buf) - 1)
		gfatal(r, "internal error - buffer overflow during http_ok");
	
	m = local_send(r, buf, n);
	if (m != n)
	{
		gprintln(r, "%s - socket error\n", r->peer);
		return APR_EGENERAL;
	}
	gprintlnif(r, "%s %s %s - OK", r->peer, r->in.req->argv[0], r->in.req->argv[1]);
	
	return 0;
}

static void log_tdx_status()
{
	int i, j;
	int num_requests;
	int num_sessions = apr_hash_count(gcb.session.tab);
	void *entry;
	char buf[1024];
	const char *ferror = NULL;
	request_t *r;
	session_t *session;
	apr_hash_index_t *hi;
	
	gprint(NULL, "---------------------------------------\n");
	gprint(NULL, "STATUS: total session(s) %d\n", num_sessions);
	
	hi = apr_hash_first(gcb.pool, gcb.session.tab);
	for (i = 0; hi && i < num_sessions; i++, hi = apr_hash_next(hi))
	{
		apr_hash_this(hi, 0, 0, &entry);
		session = (session_t *) entry;
		if (session == NULL)
		{
			gprint(NULL, "session %d: NULL\n", i);
			continue;
		}
		if (session->remote_protocol_type == REMOTE_COS_PROTOCOL)
			ferror = (session->cos_fs == NULL ? NULL : cos_fstream_get_error(session->cos_fs));
		else
			ferror = (session->fstream == NULL ? NULL : fstream_get_error(session->fstream));
		
		gprint(NULL, "session %d: tid=%s, fs_error=%s, is_error=%d, nrequest=%d is_get=%d, maxsegs=%d\n",
		       i, session->tid, (ferror == NULL ? "N/A" : ferror), session->is_error, session->nrequest, session->is_get, session->maxsegs);
		session_active_segs_dump(session);
	}
	
	printf("session: [\r\n");
	
	hi = apr_hash_first(gcb.pool, gcb.session.tab);
	for (i = 0; hi && i < num_sessions; i++, hi = apr_hash_next(hi))
	{
		apr_hash_index_t *hj;

		apr_hash_this(hi, 0, 0, &entry);
		session = (session_t *) entry;
		if (session == NULL)
			continue;
		
		(void) apr_snprintf(buf, sizeof(buf),
		                    "\t%s :{\r\n"
		                    "\t\tnrequest: %d\r\n"
		                    "\t\tis_get: %d\r\n"
		                    "\t\tpath: %s\r\n"
		                    "\t\trequest: [\r\n",
		                    session->tid, session->nrequest,
							session->is_get, session->path);
		
		printf("%s\n", buf);
		num_requests = apr_hash_count(session->requests);
		hj = apr_hash_first(session->pool, session->requests);
		for (j = 0; hj && j < num_requests; j++, hj = apr_hash_next(hj))
		{
			apr_hash_this(hj, 0, 0, &entry);
			r = (request_t *) entry;
			if (r == NULL)
				continue;
			
			apr_snprintf(buf, sizeof(buf),
			             "\t\t\t%ld : {\r\n"
			             "\t\t\t\tbytes: %ld\r\n"
			             "\t\t\t\tunsent_bytes: %d\r\n"
			             "\t\t\t\tlast: %s\r\n"
			             #ifdef WIN32
			             "\t\t\t\tseq: %ld\r\n"
			             #else
			             "\t\t\t\tseq: %"
			APR_INT64_T_FMT
			"\r\n"
			#endif
			"\t\t\t\tis_final: %d\r\n"
			"\t\t\t\tsegid: %d\r\n"
			"\t\t\t}\r\n",
					r->id,
					r->bytes,
					get_unsent_bytes(r),
					datetime(r->last),
#ifdef WIN32
					(long) r->seq,
#else
					r->seq,
#endif
					r->is_final,
					r->segid);
			
			printf("%s\n", buf);
		}
		printf("\t\t]\r\n\t}\r\n");
	}
	printf("]\r\n");
	
	gprint(NULL, "---------------------------------------\n");
}

/*
 * send some server status back to the client. This is a debug utility and is
 * not normally used in normal production environment unless triggered for
 * debugging purposes. For more information see do_read, search for
 * 'tdx/status'.
 */
static apr_status_t send_tdx_status(request_t *r)
{
	int m;
	char buf[1024];
	char *time;
	int n;

	log_tdx_status();
	
	/*
	 * TODO: return response body is json encoded like:
	 * {
	 *   "request_time": "requst_time 2014-08-13 16:17:13",
	 *   "read_bytes": 1787522407,
	 *   "total_bytes": 3147292500,
	 *   "total_sessions": 2,
	 *   sessions: [
	 *   	"1" : {
	 *   	   	"tid": session->tid,
	 *   	   	"nrequest": session->nrequest,
	 *		   	"is_get": session->is_get,
	 *		   	"path": session->path,
	 *		   	"requests": [
	 *		   		"segid1": {
	 *					"bytes": request->bytes,
	 *					"last": request->last,
	 *					"seq": request->seq,
	 *					"is_final": request->is_final,
	 *					"segid": request->segid,
	 *
	 *		   		},
	 *		   		"segid2": {
	 *
	 *		   		}
	 *		   	]
	 *   	},
	 *   	"2" : {
	 *   	}
	 *   ]
	 * }
	 */
	time = datetime_now();
	n = apr_snprintf(buf, sizeof(buf), "HTTP/1.0 200 ok\r\n"
	                                       "Content-type: text/plain\r\n"
	                                       "Expires: 0\r\n"
	                                       "X-TDX-VERSION: "
	PGXC_VERSION/*TDX_VERSION*/ "\r\n"
	                           "Cache-Control: no-cache\r\n"
	                           "Connection: close\r\n\r\n"
	                           "requst_time %s\r\n"
	                           #ifdef WIN32
	                           "read_bytes %ld\r\n"
										"total_bytes %ld\r\n"
	                           #else
	                           "read_bytes %"
	APR_INT64_T_FMT
	"\r\n"
	"total_bytes %"
	APR_INT64_T_FMT
	"\r\n"
	#endif
	"total_sessions %d\r\n",
			time,
#ifdef WIN32
			(long)gcb.read_bytes,
			(long)gcb.total_bytes,
#else
			gcb.read_bytes,
			gcb.total_bytes,
#endif
			gcb.total_sessions);
	
	
	if (n >= sizeof buf - 1)
		gfatal(r, "internal error - buffer overflow during send_tdx_status");
	
	m = local_send(r, buf, n);
	if (m != n)
	{
		gprint(r, "%s - socket error\n", r->peer);
		return APR_EGENERAL;
	}
	
	return 0;
}

/*
 * Finished a request. Close socket and cleanup.
 * Note: ending a request does not mean that the session is ended.
 * Maybe there is a request out there (of the same session), that still
 * has a block to send out.
 */
static void request_end(request_t *r, int error, const char *errmsg)
{
	session_t *session = r->session;

#ifdef GPFXDIST
	if (r->trans.errfile)
	{
		apr_status_t rv;

		/*
		 * close and then re-open (for reading) the temporary file
		 * we've used to capture stderr
		 */
		apr_file_flush(r->trans.errfile);
		apr_file_close(r->trans.errfile);
		gprintln(r, "request closed stderr file %s", r->trans.errfilename);

		/*
		 * send the first 8K of stderr to the server
		 */
		if ((rv = apr_file_open(&r->trans.errfile,
								r->trans.errfilename,
								APR_READ|APR_BUFFERED,
								APR_UREAD, r->pool)) == APR_SUCCESS)
		{
			gp1_send_errfile(r, r->trans.errfile);
			apr_file_close(r->trans.errfile);
		}

		/*
		 * remove the temp file
		 */
		apr_file_remove(r->trans.errfilename, r->pool);
		gprintln(r, "request removed stderr file %s", r->trans.errfilename);

		r->trans.errfile = NULL;
	}

#endif
	
	if (r->tdx_proto == 1)
	{
		if (!error)
			gp1_send_eof(r);
		else if (errmsg)
			gp1_send_errmsg(r, errmsg);
	}
	
	gprintlnif(r, "request end");
	
	/* If we still have a block outstanding, the session is corrupted. */
	if (r->outblock.top != r->outblock.bot)
	{
		gwarning(r, "request failure resulting in session failure: "
		            "top = %d, bot = %d", r->outblock.top, r->outblock.bot);
		if (session)
			session_end(session, 1);
	}
	else
	{
		/* detach this request from its session */
		session_detach(r);
	}
	
	/* If we still have data in the buffer - flush it */
#ifdef USE_SSL
	if (opt.ssl)
		flush_ssl_buffer(r->sock, 0, r);
	else
		request_cleanup(r);
#else
	request_cleanup(r);
#endif
}

static int local_send(request_t *r, const char *buf, int buflen)
{
	int n = tdx_send(r, buf, buflen);
	
	if (n < 0)
	{
#ifdef WIN32
		int e = WSAGetLastError();
		int ok = (e == WSAEINTR || e == WSAEWOULDBLOCK);
#else
		int e = errno;
		int ok = (e == EINTR || e == EAGAIN);
#endif
		if (e == EPIPE || e == ECONNRESET)
		{
			gwarning(r, "tdx_send failed - the connection(socket %d) was terminated by the client (%d: %s)",
			         r->sock, e, strerror(e));
		}
		else
		{
			if (!ok)
			{
				gwarning(r, "tdx_send failed - due to (%d: %s)", e, strerror(e));
			}
			else
			{
				gdebug(r, "tdx_send failed - due to (%d: %s), should try again", e, strerror(e));
			}
		}
		return ok ? 0 : -1;
	}
	
	return n;
}

static int local_sendall(request_t *r, const char *buf, int buflen)
{
	int oldlen = buflen;
	
	while (buflen)
	{
		int i = local_send(r, buf, buflen);
		
		if (i < 0)
			return i;
		
		buf += i;
		buflen -= i;
	}
	
	return oldlen;
}

/*
 * In PROTO-1 we can send all kinds of data blocks to the client. each data
 * block is tagged by meta info like this:
 * byte 0: type (can be 'F'ilename, 'O'ffset, 'D'ata, 'E'rror, 'L'inenumber)
 * byte 1-4: length. # bytes of following data block. in network-order.
 * byte 5-X: the block itself.
 *
 * this function creates and sends the meta info according to the passed in
 * arguments. It does not send the block itself (bytes 5-X).
 */
static int
gp1_send_header(request_t *r, char letter, int length)
{
	char hdr[5];
	const char *p = hdr;
	
	hdr[0] = letter;
	length = htonl(length);
	
	memcpy(hdr + 1, &length, 4);
	
	return local_sendall(r, p, 5) < 0 ? -1 : 0;
}

/*
 * Send a message to the client to indicate EOF - no more data. This is done
 * by sending a 'D' message type (Data) with length 0.
 */
static void
gp1_send_eof(request_t *r)
{
	int result = gp1_send_header(r, 'D', 0);
	gprintln(r, "sent EOF: %s", (result == 0 ? "succeed" : "fail"));
}

/* send an error message to the client, using the 'E' message type */
static void
gp1_send_errmsg(request_t *r, const char *errmsg)
{
	apr_int32_t len;

	gprintln(r, "send error message: %s", errmsg);
	len = strlen(errmsg);
	if (!gp1_send_header(r, 'E', len))
	{
		local_sendall(r, errmsg, len);
	}
	else
	{
		gwarning(r, "failed to send error message");
	}
}

#ifdef GPFXDIST
/* send the first 8k of the specified file as an error message */
static void gp1_send_errfile(request_t* r, apr_file_t* errfile)
{
	char         buf[8192];
	apr_size_t   nbytes = sizeof(buf);
	apr_status_t rv;

	if ((rv = apr_file_read(errfile, buf, &nbytes)) == APR_SUCCESS)
	{
		if (nbytes > 0)
		{
			if (! gp1_send_header(r, 'E', nbytes))
			{
				local_sendall(r, buf, nbytes);
				gdebug(r, "[%d] request sent %"APR_SIZE_T_FMT" stderr bytes to server", r->sock, nbytes);
			}
		}
	}
}
#endif

/*
 * Get a block out of the session. return error string. This includes a block
 * header (metadata for client such as filename, etc) and the data itself.
 */
static const char *
session_get_block(const request_t *r, block_t *retblock, char *line_delim_str, int line_delim_length)
{
	int size;
	fstream_filename_and_offset fos;
	
	session_t *session = r->session;
	
	retblock->bot = retblock->top = 0;
	
	if (session->is_error || IS_DATA_STREAM_NULL())
	{
		gprintln(NULL, "session_get_block: end session is_error: %d", session->is_error);
		session_end(session, session->is_error);
		return 0;
	}
	
	if (session->remote_protocol_type == REMOTE_COS_PROTOCOL)
	{
		gcb.read_bytes -= cos_fstream_get_compressed_position(session->cos_fs);
		
		/* read data from cos as a chunk with whole data rows */
		size = cos_fstream_read(cosContext, session->bucket, session->cos_fs, 
								retblock->data, opt.m, &fos, 1, 
								line_delim_str, line_delim_length);
		delay_watchdog_timer();
		
		if (size == 0)
		{
			gprintln(NULL, "session_get_block: end session due to EOF");
			gcb.read_bytes += cos_fstream_get_compressed_size(session->cos_fs);
			session_end(session, 0);
			return 0;
		}
		
		gcb.read_bytes += cos_fstream_get_compressed_position(session->cos_fs);
		
		if (size < 0)
		{
			const char *ferror = cos_fstream_get_error(session->cos_fs);
			gwarning(NULL, "session_get_block end session due to %s", ferror);
			session_end(session, 1);
			return ferror;
		}
	}
	else
	{
		gcb.read_bytes -= fstream_get_compressed_position(session->fstream);
		
		/* read data from our filestream as a chunk with whole data rows */
		size = fstream_read(session->fstream,
		                    retblock->data,
		                    opt.m,
		                    &fos,
		                    1,
		                    line_delim_str,
		                    line_delim_length);
		delay_watchdog_timer();
		
		if (size == 0)
		{
			gprintln(NULL, "session_get_block: end session due to EOF");
			gcb.read_bytes += fstream_get_compressed_size(session->fstream);
			session_end(session, 0);
			return 0;
		}
		
		gcb.read_bytes += fstream_get_compressed_position(session->fstream);
		
		if (size < 0)
		{
			const char *ferror = fstream_get_error(session->fstream);
			gwarning(NULL, "session_get_block end session due to %s", ferror);
			session_end(session, 1);
			return ferror;
		}
	}	
	retblock->top = size;
	
	/* fill the block header with metadata for the client to parse and use */
	if(block_fill_header(r, retblock, &fos) != 0)
		gfatal(NULL, "block_fill_header error.");
	
	return 0;
}

static block_t *
session_append_block(block_t *retblock, StringInfo raw_buf)
{
	memcpy(retblock->data + retblock->top, raw_buf->data, raw_buf->len);
	retblock->top += raw_buf->len;
	
	return retblock;
}

void kafka_obj_close(KafkaObjects kobj)
{
	if (!PointerIsValid(kobj))
		return;
	
	if (PointerIsValid(kobj->rk))
	{
		rd_kafka_consumer_close(kobj->rk);
		rd_kafka_destroy(kobj->rk);
		kobj->rk = NULL;
	}
	
	if (PointerIsValid(kobj->rkt))
	{
		rd_kafka_topic_destroy(kobj->rkt);
		kobj->rkt = NULL;
	}
}

/* finish the session - close the file */
static void session_end(session_t *session, int error)
{
	gprintln(NULL, "session end. id = %ld, error = %d", session->id, error);
	
	if (error && !session->is_error)
		session->is_error = error;

	stream_close(session);
}

/* deallocate session, remove from hashtable */
static void session_free(session_t *session)
{
	gprintln(NULL, "free session %s", session->key);

	stream_close(session);
	event_del(&session->ev);
	apr_hash_set(gcb.session.tab, session->key, APR_HASH_KEY_STRING, 0);
	apr_pool_destroy(session->pool);
}

/* detach a request from a session */
static void session_detach(request_t *r)
{
	session_t *session = r->session;

	gprintlnif(r, "detach segment request from session");
	r->session = 0;
	if (session)
	{
		if (session->nrequest <= 0)
			gfatal(r, "internal error - detaching a request from an empty session");
		
		session->nrequest--;
		session->mtime = apr_time_now();
		apr_hash_set(session->requests, &r->id, sizeof(r->id), NULL);
		if (session->is_get && session->nrequest == 0)
		{
			gprintln(r, "session has finished all segment requests");
		}
		
		if (!session->need_route)
		{
			/* for auto-tid sessions, we can free it now */
			if (0 == strncmp("auto-tid.", session->tid, 9))
			{
				if (session->nrequest != 0)
					gwarning(r,
					         "internal error - expected an empty auto-tid session but saw %d requests",
					         session->nrequest);
				
				session_free(session);
			}
			else if (!session->is_get && session->nrequest == 0 && session_active_segs_isempty(session))
			{
				/*
				 * free the session if this is a POST request and it's
				 * the last request for this session (we can tell is all
				 * segments sent a "done" request by calling session_active_isempty.
				 * (nrequest == 0 test isn't sufficient by itself).
				 *
				 * this is needed in order to make sure to close the out file
				 * when we're done writing. (only in write operations, not in read).
				 */
#ifdef WIN32
				if(!fstream_is_win_pipe(session->fstream))
				{
					session_free(session);
					return;
				}
#endif
				
				if (opt.w == 0)
				{
					session_free(session);
					return;
				}
				
				event_del(&session->ev);
				evtimer_set(&session->ev, free_session_cb, session);
				session->tm.tv_sec = opt.w;
				session->tm.tv_usec = 0;
				(void) evtimer_add(&session->ev, &session->tm);
			}
		}
	}
}

static void sessions_cleanup(void)
{
	apr_hash_index_t *hi;
	int i, n = 0;
	void *entry;
	session_t **session_p = NULL;
	session_t *session;
	int numses;
	
	gprintln(NULL, "remove sessions");
	
	numses = apr_hash_count(gcb.session.tab);
	gdebug(NULL, "numses %d", numses);
	
	if (numses == 0)
		return;
	
	if (!(session_p = malloc(sizeof(session_t *) * numses)))
		gfatal(NULL, "out of memory in sessions_cleanup");
	
	hi = apr_hash_first(gcb.pool, gcb.session.tab);
	
	/* find out those sessions without request or outdated */
	for (i = 0; hi && i < numses; i++, hi = apr_hash_next(hi))
	{
		apr_hash_this(hi, 0, 0, &entry);
		session = (session_t *) entry;
		gprint(NULL, "session %ld - %s\n", session->id, session->key);
		
		if (session->nrequest == 0 &&
		    (session->mtime < apr_time_now() - 300 * APR_USEC_PER_SEC))
		{
			session_p[n++] = session;
		}
	}
	
	for (i = 0; i < n; i++)
	{
		gprint(NULL, "remove outdated session %ld - %s\n", session_p[i]->id, session_p[i]->key);
		session_free(session_p[i]);
		session_p[i] = 0;
	}
	
	free(session_p);
}

/*
 * attach a request to a session (create the session if not exists yet).
 * return -  -1:error;
 * 0:Not the last node of current session;
 * 1:The Last datanode has joined in, constructed shardmap successfully.*/
static int session_attach(request_t *r)
{
	char key[1024] = {0};
	session_t *session = NULL;
	const char *node_in_shard = NULL;
	int flag;    /* return flag*/
	int i = 0;
	
	/* create the session key (tid:path) */
	if (sizeof(key) - 1 == apr_snprintf(key, sizeof(key), "%s:%s", r->tid, r->path))
	{
		http_error(r, FDIST_BAD_REQUEST, "path too long");
		request_end(r, 1, 0);
		return -1;
	}
	
	/* check if such session already exists in hashtable */
	session = apr_hash_get(gcb.session.tab, key, APR_HASH_KEY_STRING);
	
	if (!session)
	{
		/* not in hashtable - create new session */
		copy_options *cpara = NULL;
		fstream_t *fstream = 0;
		cos_fstream_t *cos_fstream = 0;
		apr_pool_t *pool;
		int response_code;
		const char *response_string;
		fstream_options fstream_options;
		
		/* remove any outdated sessions */
		sessions_cleanup();
		
		/*
		 * this is the special WET "session-end" request. Another similar
		 * request must have already come in from another segdb and finished
		 * the session we were at. we don't want to create a new session now,
		 * so just exit instead
		 */
		if (r->is_final)
		{
			gprintln(r, "got a final write request. skipping session creation");
			http_empty(r);
			request_end(r, 0, 0);
			return -1;
		}
		
		if (apr_pool_create(&pool, gcb.pool))
		{
			gwarning(r, "out of memory");
			http_error(r, FDIST_INTERNAL_ERROR, "internal error - out of memory");
			request_end(r, 1, 0);
			return -1;
		}
		
		/* parse csvopt header */
		memset(&fstream_options, 0, sizeof fstream_options);
		fstream_options.verbose = opt.v;
		fstream_options.bufsize = opt.m;
		
		{
			int quote = 0;
			int escape = 0;
			int eol_type = 0;
			/* csvopt is different in gp4 and later version */
			/* for gp4, csv opt is like "mxqnh"; for later version of gpdb, csv opt is like "mxqhn" */
			/* we check the number of successful match here to make sure eol_type and header is right */
			if (strcmp(r->csvopt, "") != 0)    //writable external table doesn't have csvopt
			{
				int n = sscanf(r->csvopt, "m%dx%dq%dn%dh%d", &fstream_options.is_csv, &escape,
				               &quote, &eol_type, &fstream_options.header);    /* "mxgnh" */
				if (n != 5)
				{
					n = sscanf(r->csvopt, "m%dx%dq%dh%dn%d", &fstream_options.is_csv, &escape,
					           &quote, &fstream_options.header, &eol_type);    /* "mxghn" */
				}
				if (n == 5)
				{
					fstream_options.quote = quote;
					fstream_options.escape = escape;
					fstream_options.eol_type = eol_type;
				}
				else
				{
					http_error(r, FDIST_BAD_REQUEST, "bad request, csvopt doesn't match the format");
					request_end(r, 1, 0);
					apr_pool_destroy(pool);
					return -1;
				}
			}
		}
		
		/* set fstream for read (GET) or write (PUT) */
		if (r->is_get)
			fstream_options.forwrite = 0; /* GET request */
		else
		{
			fstream_options.forwrite = 1; /* PUT request */
			fstream_options.usesync = opt.S;
		}

#ifdef GPFXDIST
		/* set transformation options */
		if (r->trans.command)
		{
			fstream_options.transform = (struct gpfxdist_t*) pcalloc_safe(r, pool, sizeof(struct gpfxdist_t),
																		  "out of memory in session_attach");
			fstream_options.transform->cmd        = r->trans.command;
			fstream_options.transform->pass_paths = r->trans.paths;
			fstream_options.transform->for_write  = fstream_options.forwrite;
			fstream_options.transform->mp         = pool;
			fstream_options.transform->errfile    = r->trans.errfile;
		}
		gprintlnif(r, "r->path \"%s\"", r->path);
#endif
		
		/* allocate session */
		session = pcalloc_safe(r, pool, sizeof(session_t), "out of memory in session_attach");
		memset(session, 0, sizeof(session_t));
		
		if (r->fields_num_pos > 0)
			session->fields_num = atoi(r->in.req->hvalue[r->fields_num_pos]);

		if (*r->in.req->hvalue[r->col_position_pos] != '~')
		{
			int i;

			session->col_pos = (PositionTDX **) pcalloc_safe(r, pool, session->fields_num * sizeof(PositionTDX *),
															 "out of memory when allocating position info session->col_pos");
			for (i = 0; i < session->fields_num; i++)
			{
				session->col_pos[i] = (PositionTDX *) pcalloc_safe(r, pool, sizeof(PositionTDX),
																   "out of memory when allocating position info session->col_pos");
			}

			if (!transform_position_info(pool,
										 r->in.req->hvalue[r->col_position_pos],
										 session->fields_num, session->col_pos))
			{
				gwarning(r, "invalid column position info: %s", r->in.req->hvalue[r->col_position_pos]);
				http_error(r, FDIST_BAD_REQUEST, "bad request, init column position failed");
				request_end(r, 1, 0);
				apr_pool_destroy(pool);
				return -1;
			}
		}

		/* init COS oss context or KAFKA conf during the first request */
		if (r->remote_data_proto)
		{
			/* lookup protocol conf */
			datasource *do_conf = datasource_lookup(opt.dslist, r->remote_data_proto, opt.V);
			if (!do_conf)
			{
				gwarning(r, "no valid conf for %s request.", r->remote_data_proto);
				http_error(r, FDIST_BAD_REQUEST, "bad request, no valid conf for remote data request.");
				request_end(r, 1, 0);
				apr_pool_destroy(pool);
				return -1;
			}
			
			/* Initialize the global configuration cosContext when processing a session requesting an object on COS. */
			if (strncasecmp(r->remote_data_proto, PROTOCOL_COS_STR, strlen(PROTOCOL_COS_STR)) == 0)
			{
				if (!r->cos.bucket_name)
				{
					gwarning(r, "no bucket name specified, reject cos request from %s, path %s", r->peer, r->path);
					http_error(r, FDIST_BAD_REQUEST, "no bucket name specified");
					request_end(r, 1, 0);
					apr_pool_destroy(pool);
					return -1;
				}
				
				session->remote_protocol_type = REMOTE_COS_PROTOCOL;
				session->bucket = apr_pstrdup(pool, r->cos.bucket_name);
				if (!cosContext)
				{
					gprintlnif(r, "new session trying to Init COS context");
					cosContext = ossInitContext(do_conf->oss_conf);
				}
			}
			else if (strncasecmp(r->remote_data_proto, PROTOCOL_KAFKA_STR, strlen(PROTOCOL_KAFKA_STR)) == 0)
			{
				char *before_pos_str = NULL;
				char *after_pos_str = NULL;
				char *op_pos_str = NULL;
				session->remote_protocol_type = REMOTE_KAFKA_PROTOCOL;
				
				if (!r->kafka.topic && !r->path)
				{
					gwarning(r, "no topic name specified, reject kafka request from %s, path %s", r->peer, r->path);
					http_error(r, FDIST_BAD_REQUEST, "no topic name specified");
					request_end(r, 1, 0);
					apr_pool_destroy(pool);
					return -1;
				}
				
				session->kafka_conf = rd_kafka_conf_dup(do_conf->kafka_conf);
				
				/* 1. Get fields and op position in payload */
				op_pos_str = pcalloc_safe(r, pool, strlen(do_conf->kafka_msg_format->op_pos) + 1,
				                              "out of memory in session_attach for op_pos copy");
				before_pos_str = pcalloc_safe(r, pool, strlen(do_conf->kafka_msg_format->before_pos) + 1,
				                              "out of memory in session_attach for before_pos_str");
				after_pos_str = pcalloc_safe(r, pool, strlen(do_conf->kafka_msg_format->after_pos) + 1,
				                             "out of memory in session_attach for after_pos_str");
				
				memcpy(op_pos_str, do_conf->kafka_msg_format->op_pos, strlen(do_conf->kafka_msg_format->op_pos));
				op_pos_str[strlen(do_conf->kafka_msg_format->op_pos)] = '\0';
				if (!SplitIdentifierString(op_pos_str, '-', &session->op_pos))
				{
					gwarning(r, "Fail to split op position in payload : %s", do_conf->kafka_msg_format->op_pos);
					http_error(r, FDIST_BAD_REQUEST, "invalid operator pos conf str");
					request_end(r, 1, 0);
					apr_pool_destroy(pool);
					return -1;
				}
				
				memcpy(before_pos_str, do_conf->kafka_msg_format->before_pos, strlen(do_conf->kafka_msg_format->before_pos));
				before_pos_str[strlen(do_conf->kafka_msg_format->before_pos)] = '\0';
				memcpy(after_pos_str, do_conf->kafka_msg_format->after_pos, strlen(do_conf->kafka_msg_format->after_pos));
				after_pos_str[strlen(do_conf->kafka_msg_format->after_pos)] = '\0';
				
				if (!SplitIdentifierString(before_pos_str, '-', &session->before_pos) ||
				    !SplitIdentifierString(after_pos_str, '-', &session->after_pos))
				{
					gwarning(r, "Fail to split %th fields position in payload : %s,%s",
					         i, do_conf->kafka_msg_format->before_pos, do_conf->kafka_msg_format->after_pos);
					http_error(r, FDIST_BAD_REQUEST, "invalid column pos conf str");
					request_end(r, 1, 0);
					apr_pool_destroy(pool);
					return -1;
				}
				
				/* 2. for consumer */
				if (do_conf->kafka_msg_format->k_timeout_ms > 0)
					session->rk_timeout_ms = do_conf->kafka_msg_format->k_timeout_ms;
				else
					session->rk_timeout_ms = RK_TIMEOUT_MS_DEFAULT;
					
				if (do_conf->kafka_msg_format->k_msg_batch > 0)
					session->rk_messages_size = do_conf->kafka_msg_format->k_msg_batch;
				else
					session->rk_messages_size = RK_MESSAGE_BATCH_DEFAULT;
			}
			
			session->need_convert_datetime = do_conf->kafka_msg_format->datetime_need_convert;
		}
		
		switch (session->remote_protocol_type)
		{
			case REMOTE_COS_PROTOCOL:
			{
				gprintlnif(r, "new session trying to open the COS data stream");
				cos_fstream = cos_fstream_open(cosContext,
				                               session->bucket,
				                               r->path,
				                               &fstream_options,
				                               &response_code,
				                               &response_string);
				delay_watchdog_timer();
				
				if (!cos_fstream)
				{
					gwarning(r, "Open cos object stream failed. reject cos request from %s, path %s", r->peer, r->path);
					http_error(r, response_code, response_string);
					request_end(r, 1, 0);
					apr_pool_destroy(pool);
					return -1;
				}
				gprintln(r, "new session successfully opened the COS data stream %s", r->path);
				
				gcb.total_bytes += cos_fstream_get_compressed_size(cos_fstream);
				break;
			}
			case REMOTE_KAFKA_PROTOCOL:
			{
				int p = 0;
				char errstr[512];
				const struct rd_kafka_metadata *kafka_metadata;
				rd_kafka_resp_err_t err;
				
				if (r->kafka.topic)
					session->topic_name = apr_pstrdup(pool, r->kafka.topic);
				else
					session->topic_name = apr_pstrdup(pool, r->path);
				session->kobj = (KafkaObjects) pcalloc_safe(r, pool, sizeof(struct KafkaObjects),
				                                            "out of memory in session_attach for KafkaObjects");

				// set parameters
				if (r->kafka.group_id)
				{
					session->group_id = apr_pstrdup(pool, r->kafka.group_id);
					RD_KAFKA_CONF_SET_CONF_CHECK(session->kafka_conf, "group.id", session->group_id, errstr);
				}
				else
				{
					size_t size = 0;
					char group_id[512] = {0};
					if (rd_kafka_conf_get(session->kafka_conf, "group.id", group_id, &size) == RD_KAFKA_CONF_OK)
						session->group_id = apr_pstrdup(pool, group_id);
					else
						gprintln(r, "No group.id configured");
				}
				
				if (r->kafka.broker_id)
					RD_KAFKA_CONF_SET_CONF_CHECK(session->kafka_conf, "bootstrap.servers", r->kafka.broker_id, errstr);
				
				// RD_KAFKA_CONF_SET_CONF_CHECK(kafka_conf, "enable.auto.commit", "false", errstr);
				// RD_KAFKA_CONF_SET_CONF_CHECK(kafka_conf, "enable.partition.eof", "true", errstr);
				
				/* create topic object */
				/* From this point, 'conf' is owned by consumer */
				session->kobj->rk = rd_kafka_new(RD_KAFKA_CONSUMER, session->kafka_conf, errstr, sizeof(errstr));
				if (!PointerIsValid(session->kobj->rk))
				{
					gwarning(r, "Failed to create a Kafka consumer: %s", errstr);
					http_error(r, FDIST_BAD_REQUEST, "Failed to create a Kafka consumer");
					request_end(r, 1, 0);
					apr_pool_destroy(pool);
					return -1;
				}
				
				session->kobj->rkt = rd_kafka_topic_new(session->kobj->rk, session->topic_name, NULL);
				if (!PointerIsValid(session->kobj->rkt))
				{
					gwarning(r, "Failed to create a Kafka topic: %s [%d]", rd_kafka_err2str(rd_kafka_last_error()), rd_kafka_last_error());
					http_error(r, FDIST_BAD_REQUEST, "Failed to create a Kafka consumer");
					request_end(r, 1, 0);
					apr_pool_destroy(pool);
					kafka_obj_close(session->kobj);
					return -1;
				}
				
				err = rd_kafka_metadata(session->kobj->rk, false, session->kobj->rkt, &kafka_metadata, meta_timeout);
				if (err != RD_KAFKA_RESP_ERR_NO_ERROR)
				{
					gwarning(r, "failed to acquire metadata: %s", rd_kafka_err2str(err));
					http_error(r, FDIST_BAD_REQUEST, "failed to acquire metadata");
					request_end(r, 1, 0);
					apr_pool_destroy(pool);
					kafka_obj_close(session->kobj);
					return -1;
				}
				
				if (kafka_metadata->topic_cnt != 1 || kafka_metadata->topics[0].partition_cnt <= 0)
				{
					gwarning(r, "topic metadata: topic_cnt - %d, partition_cnt - %d", kafka_metadata->topic_cnt, kafka_metadata->topics[0].partition_cnt);
					http_error(r, FDIST_BAD_REQUEST, "unexpected topic metadata");
					request_end(r, 1, 0);
					apr_pool_destroy(pool);
					kafka_obj_close(session->kobj);
					return -1;
				}
				
				session->partition_cnt = kafka_metadata->topics[0].partition_cnt;
				session->partition_ids = pcalloc_safe(r, pool, sizeof(int) * session->partition_cnt,
				                                      "out of memory in session_attach for partition_ids");
				for (p = 0; p < session->partition_cnt; p++)
					session->partition_ids[p] = kafka_metadata->topics[0].partitions[p].id;
				
				rd_kafka_metadata_destroy(kafka_metadata);
				
				/* parition_num in request http header must equal to partition_cnt in topic_meta or zero*/
				if (r->kafka.partition_cnt != 0 && session->partition_cnt != r->kafka.partition_cnt)
				{
					gwarning(r, "not matched partition_cnt: %d-%d(topic_meta-request)", session->partition_cnt, r->kafka.partition_cnt);
					http_error(r, FDIST_BAD_REQUEST, "not matched partition_cnt");
					request_end(r, 1, 0);
					apr_pool_destroy(pool);
					kafka_obj_close(session->kobj);
					return -1;
				}
				
				session->offsets = pcalloc_safe(r, pool, session->partition_cnt * sizeof(int64_t),
				                                "out of memory in allocating kafka offset");
				
				// session->prt_max_bytes = r->kafka.r_max_size;
				session->batch_size = r->kafka.r_max_messages;
				// session->prt_timeout = r->kafka.r_timeout_ms;
				
				if (session->batch_size <= 0)
				{
					gwarning(r, "session_prt_batch must be provided.", session->partition_cnt, r->kafka.partition_cnt);
					http_error(r, FDIST_BAD_REQUEST, "session_prt_batch must be provided.");
					request_end(r, 1, 0);
					apr_pool_destroy(pool);
					kafka_obj_close(session->kobj);
					return -1;
				}

				/* 
				 * choose the smaller one as the message batch size between 
				 * the parameter from extra conf and from the kernel 
				 */
				if (session->rk_messages_size > session->batch_size)
					session->rk_messages_size = session->batch_size;
				
				break;
			}
			default:
			{
				gprintlnif(r, "new session trying to open the LOCAL FILE data stream");
				fstream = fstream_open(r->path, &fstream_options, &response_code, &response_string);
				delay_watchdog_timer();
				
				if (!fstream)
				{
					gwarning(r, "reject request from %s, path %s", r->peer, r->path);
					http_error(r, response_code, response_string);
					request_end(r, 1, 0);
					apr_pool_destroy(pool);
					return -1;
				}
				
				gprintln(r, "new session successfully opened the LOCAL FILE data stream %s", r->path);
				
				gcb.total_bytes += fstream_get_compressed_size(fstream);
			}
		}
		gcb.total_sessions++;
		
		/* allocate active_segdb array (session member) */
		session->active_segids = (int *) pcalloc_safe(r, pool, sizeof(int) * r->totalsegs,
		                                              "out of memory when allocating active_segids array");
		
		/* allocate seq_segs array (session member) */
		session->seq_segs = (apr_int64_t *) pcalloc_safe(r, pool, sizeof(apr_int64_t) * r->totalsegs,
		                                                 "out of memory when allocating seq_segs array");
		
		/* allocate buffer_mutex and cond for thread */
		session->buffer_mutex = (pthread_mutex_t *) pcalloc_safe(r, pool, sizeof(pthread_mutex_t) * r->totalsegs,
		                                                  "out of memory when allocating pthread_mutex_t array");
		session->append_cond = (pthread_cond_t *) pcalloc_safe(r, pool, sizeof(pthread_cond_t) * r->totalsegs,
		                                                    "out of memory when allocating pthread_cond_t array");
		session->send_cond = (pthread_cond_t *) pcalloc_safe(r, pool, sizeof(pthread_cond_t) * r->totalsegs,
		                                                    "out of memory when allocating pthread_cond_t array");
		
		/* initialize session values */
		session->id = ++SESSION_SEQ;
		session->tid = apr_pstrdup(pool, r->tid);
		session->path = apr_pstrdup(pool, r->path);
		session->key = apr_pstrdup(pool, key);
		session->fstream = fstream;
		session->cos_fs = cos_fstream;
		session->pool = pool;
		session->is_get = r->is_get;
		session->need_route = r->need_route;
		session->active_segids[r->segid] = 1; /* mark this seg as active */
		session->active_seg_count = 1;
		session->maxsegs = r->totalsegs;
		session->requests = apr_hash_make(pool);
		session->eof = false;
		
		/* information only needed during importing data */
		if (session->need_route)
		{
			int shardid = -1;
			int dis_key_idx = 0;
			char *dis_key_str = NULL;
			char *dis_key_type_str = NULL;
			char *col_type_str = NULL;
			char *col_name_str = NULL;
			const char *force_notnull_flags_str = NULL;
			const char *force_null_flags_str = NULL;
			const char *force_quote_flags_str = NULL;
			const char *convert_select_flags_str = NULL;
			char *sc, *token1, *token2; /* for splitting key type string */
			
			/* 1. copy options */
			cpara = (copy_options *) pcalloc_safe(r, pool, sizeof(copy_options),
			                                      "out of memory when allocating copy_options");
			memset(cpara, 0, sizeof(copy_options));
			cpara->eol_len = r->line_delim_length;
			if (cpara->eol_len > 0)
			{
				cpara->eol = apr_pstrdup(pool, r->line_delim_str);
			}
			
			cpara->delim_len = r->col_delim_length;
			if (cpara->delim_len > 0)
			{
				int sz;

				cpara->delim = pcalloc_safe(r, pool, cpara->delim_len * sizeof(char) + 1,
				                            "out of memory when allocating copy_options delim");
				sz = URLDecode(r->col_delim_str, strlen(r->col_delim_str), cpara->delim, cpara->delim_len);
				if (sz < 0)
				{
					gwarning(r, "bad request, cannot decode delimiter \"%s\"", r->col_delim_str);
					http_error(r, FDIST_BAD_REQUEST, "bad request, cannot decode delimiter");
					request_end(r, 1, 0);
					if (!IS_DATA_STREAM_NULL())
						DATA_STREAM_CLOSE();
					apr_pool_destroy(pool);
					return -1;
				}
				cpara->delim[cpara->delim_len * sizeof(char)] = '\0';
			}
			
			cpara->csv_mode = fstream_options.is_csv;
			cpara->quote = fstream_options.quote;
			cpara->escape = fstream_options.escape;
			
			cpara->null_print_len = r->null_print_len;
			if (cpara->null_print_len > 0)
			{
				int sz;

				cpara->null_print = pcalloc_safe(r, pool, cpara->null_print_len * sizeof(char) + 1,
				                                 "out of memory when allocating copy_options null_print");
				sz = URLDecode(r->null_print, strlen(r->null_print), cpara->null_print, cpara->null_print_len);
				if (sz < 0)
				{
					gwarning(r, "bad request, cannot decode null print \"%s\"", r->null_print);
					http_error(r, FDIST_BAD_REQUEST, "bad request, cannot decode null print");
					request_end(r, 1, 0);
					if (!IS_DATA_STREAM_NULL())
						DATA_STREAM_CLOSE();
					apr_pool_destroy(pool);
					return -1;
				}
				cpara->null_print[cpara->null_print_len * sizeof(char)] = '\0';
			}
			else 
			{
				cpara->null_print = pcalloc_safe(r, pool, 1, "out of memory when allocating copy_options null_print");
				cpara->null_print[0] = '\0';
			}
			
			cpara->force_notnull_flags = pcalloc_safe(r, pool, session->fields_num * sizeof(bool),
			                                          "out of memory when allocating copy_options force_notnull_flags");
			if (r->force_notnull_flags_pos > 0)
			{
				force_notnull_flags_str = r->in.req->hvalue[r->force_notnull_flags_pos];
				for (i = 0; i < session->fields_num; i++)
				{
					if (force_notnull_flags_str[i] == '1')
						cpara->force_notnull_flags[i] = true;
					else
						cpara->force_notnull_flags[i] = false;
				}
			}
			
			cpara->force_null_flags = pcalloc_safe(r, pool, session->fields_num * sizeof(bool),
			                                       "out of memory when allocating copy_options force_null_flags");
			if (r->force_null_flags_pos > 0)
			{
				force_null_flags_str = r->in.req->hvalue[r->force_null_flags_pos];
				for (i = 0; i < session->fields_num; i++)
				{
					if (force_null_flags_str[i] == '1')
						cpara->force_null_flags[i] = true;
					else
						cpara->force_null_flags[i] = false;
				}
			}
			
			cpara->force_quote_flags = pcalloc_safe(r, pool, session->fields_num * sizeof(bool),
			                                        "out of memory when allocating copy_options force_quote_flags");
			if (r->force_quote_flags > 0)
			{
				force_quote_flags_str = r->in.req->hvalue[r->force_quote_flags];
				for (i = 0; i < session->fields_num; i++)
				{
					if (force_quote_flags_str[i] == '1')
						cpara->force_quote_flags[i] = true;
					else
						cpara->force_quote_flags[i] = false;
				}
			}
			
			cpara->convert_select_flags = pcalloc_safe(r, pool, session->fields_num * sizeof(bool),
			                                           "out of memory when allocating copy_options convert_select_flags");
			if (r->convert_select_flags_pos > 0)
			{
				convert_select_flags_str = r->in.req->hvalue[r->convert_select_flags_pos];
				for (i = 0; i < session->fields_num; i++)
				{
					if (convert_select_flags_str[i] == '1')
						cpara->convert_select_flags[i] = true;
					else
						cpara->convert_select_flags[i] = false;
				}
			}
			
			if (r->time_format_len > 0)
				cpara->time_format = apr_pstrdup(pool, r->time_format);
			if (r->date_format_len > 0)
				cpara->date_format = apr_pstrdup(pool, r->date_format);
			if (r->timestamp_format_len > 0)
				cpara->timestamp_format = apr_pstrdup(pool, r->timestamp_format);
			
			if (!cpara->delim)
				cpara->delim = cpara->csv_mode ? "," : "\t";
			
			if (!cpara->null_print)
				cpara->null_print = (cpara->csv_mode) ? "" : "\\N";
			cpara->null_print_len = strlen(cpara->null_print);
			
			if (r->extra_error_opts)
			{
				int fill_missing_int;
				int ignore_extra_data_int;
				int compatible_illegal_chars_int;
				int escape_off;
				int n = sscanf(r->extra_error_opts, "m%di%dc%de%d", &fill_missing_int,
				               &ignore_extra_data_int, &compatible_illegal_chars_int, &escape_off);    /* "mic" */
				
				if (n == 4)
				{
					cpara->fill_missing = fill_missing_int;
					cpara->ignore_extra_data = ignore_extra_data_int;
					cpara->compatible_illegal_chars = compatible_illegal_chars_int;
					cpara->escape_off = escape_off;
				}
				else
				{
					gwarning(r, "bad request, extra_error_opts doesn't match the format.");
					http_error(r, FDIST_BAD_REQUEST, "bad request, extra_error_opts doesn't match the format");
					request_end(r, 1, 0);
					if (!IS_DATA_STREAM_NULL())
						DATA_STREAM_CLOSE();
					apr_pool_destroy(pool);
					return -1;
				}
			}
			
			cpara->file_encoding = r->file_encoding;
			/* Set up encoding conversion info.  Even if the file and server encodings
			 * are the same, we must apply pg_any_to_server() to validate data
			 * in multibyte encodings.
			 */
			cpara->need_transcoding =
					(cpara->file_encoding != GetDatabaseEncoding() || pg_database_encoding_max_length() > 1);
			
			session->copy_options = cpara;

			session->opentenbase_ora_compatible = (bool) (r->opentenbase_ora_compatible == SQLMODE_OPENTENBASE_ORA);
			
			if (r->timezone_string)
			{ 
				void *time_zone = NULL;

				session->timezone = pcalloc_safe(r, pool, sizeof(pg_tz),
				                                           "out of memory when allocating timezone_string");
				check_timezone(&r->timezone_string, &time_zone, PGC_S_SESSION);
				if (!time_zone)
				{
					gwarning(r, "TDX initialize session timezone failed.");
					http_error(r, FDIST_BAD_REQUEST, "TDX initialize session timezone failed, please check tdx log");
					request_end(r, 1, 0);
					if (!IS_DATA_STREAM_NULL())
						DATA_STREAM_CLOSE();
					apr_pool_destroy(pool);
					return -1;
				}
				strcpy(session->timezone->TZname, (*((pg_tz **) time_zone))->TZname);
				memcpy(&(session->timezone->state), &((*((pg_tz **) time_zone))->state), sizeof(struct state));
				pfree_ext(time_zone);
			}
			/* 2. buffer_mutex and condition */
			pthread_mutex_init(&session->err_mutex, NULL);
			pthread_mutex_init(&session->fs_mutex, NULL);
			for (i = 0; i < r->totalsegs; ++i)
			{
				pthread_mutex_init(&session->buffer_mutex[i], NULL);
				pthread_cond_init(&session->append_cond[i], NULL);
				pthread_cond_init(&session->send_cond[i], NULL);
			}
			
			/* 3. shared buffer for every datanode */
			session->redist_buff = (redist_buff_t **) pcalloc_safe(r, pool, sizeof(redist_buff_t *) * r->totalsegs,
			                                                       "out of memory when allocating redist_buffs");
			for (i = 0; i < r->totalsegs; ++i)
			{
				session->redist_buff[i] = (redist_buff_t *) pcalloc_safe(r, pool, sizeof(redist_buff_t),
				                                                         "out of memory when allocating redist_buff");
				session->redist_buff[i]->is_full = false;
				session->redist_buff[i]->outblock.data = palloc_safe(r, pool, opt.m,
				                                                     "out of memory when allocating buffer: %d bytes", opt.m);
			}
			
			/* 4. fstream_filename_and_offset info for every datanode */
			session->fo = (fstream_filename_and_offset **) pcalloc_safe(r,
			                                                            pool,
			                                                            sizeof(fstream_filename_and_offset *) * r->totalsegs,
			                                                            "out of memory when allocating fstream_filename_and_offset");
			memset(session->fo, 0, sizeof(fstream_filename_and_offset *) * r->totalsegs);
			for (i = 0; i < r->totalsegs; ++i)
			{
				session->fo[i] = (fstream_filename_and_offset *) pcalloc_safe(r, pool, sizeof(fstream_filename_and_offset),
				                                                              "out of memory when allocating fstream_filename_and_offset");
				session->fo[i]->foff = 0;
				session->fo[i]->line_number = 0;
			}
			
			/* 5. shardmap */
			session->shard_count = atoi(r->in.req->hvalue[r->shard_num_pos]);
			session->shardmap = (int *) pcalloc_safe(r, pool, session->shard_count * sizeof(int),
			                                         "out of memory when allocating shardmap array");
			
			/* 5.x deparse shardmap information in this request */
			node_in_shard = r->in.req->hvalue[r->shard_node_pos];
			for (shardid = 0; shardid < session->shard_count; shardid++)
			{
				if (node_in_shard[shardid] == 'y')
					session->shardmap[shardid] = r->segid;
				else
					session->shardmap[shardid] = -1;
			}
			
			/* 6. distribution column and distribution key type */
			session->dis_key_num = atoi(r->in.req->hvalue[r->dis_key_num_pos]);
			dis_key_str = r->in.req->hvalue[r->dis_key_pos];
			dis_key_type_str = r->in.req->hvalue[r->dis_key_type_pos];
			
			session->dis_key = (int *) pcalloc_safe(r, pool, session->dis_key_num * sizeof(int),
			                                        "out of memory when allocating dis_key");
			session->dis_key_type = (Oid *) pcalloc_safe(r, pool, session->dis_key_num * sizeof(Oid),
			                                             "out of memory when allocating dis_key_type");

			/* DIS COLUMN in order */
			sc = apr_pstrdup(pool, dis_key_str);
			token1 = strtok(sc, ",");
			while (token1)
			{
				session->dis_key[dis_key_idx++] = atoi(token1);
				token1 = strtok(NULL, ",");
			}
			if (dis_key_idx != session->dis_key_num)
			{
				gwarning(r, "number of attributes in DIS-COLUMN(%d) unmatch number of DIS-COLUMN-NUM(%d).", dis_key_idx, session->dis_key_num);
				http_error(r, FDIST_BAD_REQUEST, "bad request, init shardmap failed.");
				request_end(r, 1, 0);
				if (!IS_DATA_STREAM_NULL())
					DATA_STREAM_CLOSE();
				apr_pool_destroy(pool);
				return -1;
			}
			
			/* DIS COLUMN TYPE */
			dis_key_idx = 0;
			sc = apr_pstrdup(pool, dis_key_type_str);
			token2 = strtok(sc, ",");
			while (token2)
			{
				session->dis_key_type[dis_key_idx++] = (Oid) atoi(token2);
				token2 = strtok(NULL, ",");
			}
			if (dis_key_idx != session->dis_key_num)
			{
				gwarning(r, "number of types in DIS-COLUMN-TYPE(%d) unmatch number of DIS-COLUMN-NUM(%d).", dis_key_idx, session->dis_key_num);
				http_error(r, FDIST_BAD_REQUEST, "bad request, init shardmap failed.");
				request_end(r, 1, 0);
				if (!IS_DATA_STREAM_NULL())
					DATA_STREAM_CLOSE();
				apr_pool_destroy(pool);
				return -1;
			}
			
			/* ALL COLUMN TYPE */
			col_type_str = pcalloc_safe(r, pool, strlen(r->in.req->hvalue[r->col_type_pos]) + 1,
			                          "out of memory in session_attach for col_type_str copy");
			memcpy(col_type_str, r->in.req->hvalue[r->col_type_pos], strlen(r->in.req->hvalue[r->col_type_pos]));
			if (!SplitIdentifierString(col_type_str, '-', &session->column_type))
			{
				gwarning(r, "Fail to split col type in payload : %s", r->in.req->hvalue[r->col_type_pos]);
				http_error(r, FDIST_BAD_REQUEST, "invalid col type conf str");
				request_end(r, 1, 0);
				DATA_STREAM_CLOSE();
				apr_pool_destroy(pool);
				return -1;
			}
			
			col_name_str = pcalloc_safe(r, pool, strlen(r->in.req->hvalue[r->col_name_pos]) + 1,
			                          "out of memory in session_attach for col_name_str copy");
			memcpy(col_name_str, r->in.req->hvalue[r->col_name_pos], strlen(r->in.req->hvalue[r->col_name_pos]));
			if (!SplitIdentifierString(col_name_str, '-', &session->column_name))
			{
				gwarning(r, "Fail to split col name in payload : %s", r->in.req->hvalue[r->col_name_pos]);
				http_error(r, FDIST_BAD_REQUEST, "invalid col name conf str");
				request_end(r, 1, 0);
				if (!IS_DATA_STREAM_NULL())
					DATA_STREAM_CLOSE();
				apr_pool_destroy(pool);
				return -1;
			}
			
			/* 7. allocate send/read/consumer thread */
			session->st = (pthread_t *) pcalloc_safe(r, pool, sizeof(pthread_t) * (r->totalsegs),
			                                         "failed to allocated send_thread");
			
			if (session->remote_protocol_type == REMOTE_KAFKA_PROTOCOL)
			{
				/* number of consumer threads must be smaller than topic num_partitions */
				if (session->partition_cnt >= opt.parallel)
				{
					gwarning(r, "Topic partition number is greater than the maximum parallelism.");
					http_error(r, FDIST_BAD_REQUEST, "Topic partition number is greater than the maximum parallelism.");
					request_end(r, 1, 0);
					if (!IS_DATA_STREAM_NULL())
						DATA_STREAM_CLOSE();
					apr_pool_destroy(pool);
					return -1;
				}
				
				session->pt = (pthread_t *) pcalloc_safe(r, pool,
				                                         sizeof(pthread_t) * (session->partition_cnt + 1),
				                                         "failed to allocated consume pthread_t");
			}
			else
			{
				session->pt = (pthread_t *) pcalloc_safe(r, pool, sizeof(pthread_t) * (opt.parallel + 1),
				                                         "failed to allocated read pthread_t");
			}
			
			/* 8. logical log reply ?*/
			if (r->is_logical_msg != 0)
				session->is_logical_sync = true;

			/* 9. init opentenbase_ora time format */
			if (session->opentenbase_ora_compatible)
			{
				assign_nls_datetime_fmt(r->date_format, &session->orcl_time_format.f_ord_dt,
										&session->orcl_time_format.f_str_dt);
				assign_nls_datetime_fmt(r->timestamp_format, &session->orcl_time_format.f_ord_ts,
										&session->orcl_time_format.f_str_ts);
				assign_nls_datetime_fmt(r->timestamptz_format, &session->orcl_time_format.f_ord_tstz,
										&session->orcl_time_format.f_str_tstz);
			}
		}
		
		event_set(&session->ev, -1, 0, 0, 0);
		if (session->tid == 0 || session->path == 0 || session->key == 0)
			gfatal(r, "out of memory in session_attach");
		
		/* insert into hashtable */
		apr_hash_set(gcb.session.tab, session->key, APR_HASH_KEY_STRING, session);
		
		gprintlnif(r, "new session (%ld): (\"%s\", %s)", session->id, session->path, session->tid);
	}
	else if (!session->active_segids[r->segid] && session->is_get && session->need_route)
	{
		/* found a session in hashtable, but the node is not active before this request */
		int shardid;
		/* 5.x deparse shardmap information in this request */
		node_in_shard = r->in.req->hvalue[r->shard_node_pos];
		for (shardid = 0; shardid < session->shard_count; shardid++)
		{
			if (node_in_shard[shardid] == 'y')
			{
				if (session->shardmap[shardid] == -1)
				{
					session->shardmap[shardid] = r->segid;
				}
				else
				{
					http_error(r, FDIST_BAD_REQUEST, "bad request, init shardmap failed.");
					session->errmsg = apr_psprintf(session->pool, "shardmap init repeated.");
					gwarning(r, session->errmsg);
					session->is_error = 1;
					request_end(r, 1, session->errmsg);
					return -1;
				}
			}
			else if (session->shardmap[shardid] == r->segid)
			{
				gwarning(r, "shardmap init error.");
				session->errmsg = apr_psprintf(session->pool, "bad request, init shardmap failed.");
				gwarning(r, session->errmsg);
				session->is_error = 1;
				request_end(r, 1, session->errmsg);
				return -1;
			}
		}
		
		session->active_segids[r->segid] = 1;
		session->active_seg_count += 1;
	}
	else if (session->active_segids[r->segid] && session->is_get && session->need_route)
	{
		/* found a session in hashtable, and the node is already active before this request ？？？*/
		;
	}
	
	/* get start offset from each datanode request and update to the biggest values.*/
	if (session && session->need_route && session->remote_protocol_type == REMOTE_KAFKA_PROTOCOL &&
	    r->kafka.partition_cnt != 0 && r->kafka.offset_str)
	{
		List *opts;
		ListCell *lc;
		int prt_idx = 0;
		int64_t off = 0;
		if (!SplitIdentifierString(r->kafka.offset_str, '-', &opts) ||
		    opts->length != session->partition_cnt)
		{
			http_error(r, FDIST_BAD_REQUEST, "invalid offset-str");
			session->errmsg = apr_psprintf(session->pool, "Fail to split offset-str passed by url path with '-': %s", r->kafka.offset_str);
			gwarning(r, session->errmsg);
			session->is_error = 1;
			request_end(r, 1, session->errmsg);
			stream_close(session);

			return -1;
		}
		
		foreach(lc, opts)
		{
			/* order of offset were arranged in the order of increasing partition number. */
			off = atol((char *) lfirst(lc));
			if (off > session->offsets[prt_idx])
				session->offsets[prt_idx] = off;
			prt_idx++;
		}
	}
	
	/* session already ended. send an empty response and close. */
	if (IS_DATA_STREAM_NULL())
	{
		gprintln(r, "session already ended. return empty response (OK)");
		
		http_empty(r);
		request_end(r, 0, 0);
		return -1;
	}
	
	/* if error, send an error and close */
	if (session->is_error)
	{
		http_error(r, FDIST_INTERNAL_ERROR, "session error");
		request_end(r, 1, 0);
		return -1;
	}
	
	/*
	 * disallow mixing GET and POST requests in one session.
	 * this will protect us from an infinitely running
	 * INSERT INTO ext_t SELECT FROM ext_t
	 */
	if (r->is_get != session->is_get)
	{
		http_error(r, FDIST_BAD_REQUEST, "can\'t write to and read from the same "
		                                 "tdx server simultaneously");
		session->is_error = 1;
		request_end(r, 1, 0);
		return -1;
	}
	
	if (session->need_route && session->active_seg_count < session->maxsegs)
	{
		/*
 		 * The first request of each datanode helps tdx construct a shardmap,
 		 * which will be complete when the last datanode joins in.
 		 * Here return 0 at once because this kind of request will not be
 		 * attached to a session.
  		 */
		gprintln(r, "Not all nodes are connected");
		flag = 0;
	}
	else if (session->need_route && session->active_seg_count == session->maxsegs)
	{
		/* The last datanode has joined in */
		flag = 1;
	}
	else
	{
		/* for post and Other Grammar except for (INSERT INTO l SELECT * FROM ext); */
		session->active_segids[r->segid] = !r->is_final;
		session_active_segs_dump(session);
		flag = 0;
	}
	
	gprintlnif(r, "joined session (\"%s\", %s)", session->path, session->tid);
	
	/* one more request for session */
	session->nrequest++;
	session->mtime = apr_time_now();
	apr_hash_set(session->requests, &r->id, sizeof(r->id), r);
	
	r->session = session;
	r->sid = session->id;
	
	return flag;
}

/* dump all the segdb ids that currently participate in this session */
static void session_active_segs_dump(session_t *session)
{
	if (opt.v)
	{
		int i = 0;
		
		gprint(NULL, "active segids in session: ");
		
		for (i = 0; i < session->maxsegs; i++)
		{
			if (session->active_segids[i])
				printf("%d ", i);
		}
		printf("\n");
	}
}

/*
 * Is there any segdb still sending us data?
 * Or are all of them done already? if empty all are done.
 */
static int session_active_segs_isempty(session_t *session)
{
	int i = 0;
	
	for (i = 0; i < session->maxsegs; i++)
	{
		if (session->active_segids[i])
			return 0; /* not empty */
	}
	
	return 1; /* empty */
}

static void do_write(int fd, short event, void *arg)
{
	request_t *r = (request_t *) arg;
	int n, i;
	block_t *datablock;
	
	if (fd != r->sock)
		gfatal(r, "internal error - non matching fd (%d) and socket (%d)",
		       fd, r->sock);
	
	/* Loop at most 3 blocks or until we choke on the socket */
	for (i = 0; i < 3; i++)
	{
		/*
		 * Get a block (or find a remaining block)
		 * If the block read last time is already sent, read another one
		 */
		if (r->outblock.top == r->outblock.bot)
		{
			const char* ferror = session_get_block(r, &r->outblock, r->line_delim_str, r->line_delim_length);

			if (ferror)
			{
				request_end(r, 1, ferror);
				gfile_printf_then_putc_newline("ERROR: %s", ferror);
				return;
			}
			if (!r->outblock.top)
			{
				request_end(r, 0, 0);
				return;
			}
		}
		
		datablock = &r->outblock;
		
		/* If PROTO-1: first write out the block header (metadata) */
		if (r->tdx_proto == 1)
		{
			n = datablock->hdr.htop - datablock->hdr.hbot;
			
			if (n > 0)
			{
				n = local_send(r, datablock->hdr.hbyte + datablock->hdr.hbot, n);
				if (n < 0)
				{
					/*
					 * TODO: It is not safe to check errno here, should check and
					 * return special value in local_send()
					 */
					if (errno == EPIPE || errno == ECONNRESET)
						r->outblock.bot = r->outblock.top;
					request_end(r, 1, "tdx send block header failure");
					return;
				}
				
				gdebug(r, "send header bytes %d .. %d (top %d)",
				       datablock->hdr.hbot, datablock->hdr.hbot + n, datablock->hdr.htop);
				
				datablock->hdr.hbot += n;
				n = datablock->hdr.htop - datablock->hdr.hbot;
				if (n > 0)
					break; /* network chocked */
			}
		}
		
		/* Write out the block data */
		n = datablock->top - datablock->bot;
		n = local_send(r, datablock->data + datablock->bot, n);
		if (n < 0)
		{
			/*
			 * EPIPE (or ECONNRESET some computers) indicates remote socket
			 * intentionally shut down half of the pipe.  If this was because
			 * of something like "select ... limit 10;", then it is fine that
			 * we couldn't transmit all the data--the segment didn't want it
			 * anyway.  If it is because the segment crashed or something like
			 * that, hopefully we would find out about that in some other way
			 * anyway, so it is okay if we don't poison the session.
			 */
			if (errno == EPIPE || errno == ECONNRESET)
				r->outblock.bot = r->outblock.top;
			request_end(r, 1, "tdx send data failure");
			return;
		}
		
		gdebug(r, "send data bytes off buf %d .. %d (top %d)",
		       datablock->bot, datablock->bot + n, datablock->top);
		
		r->bytes += n;
		r->last = apr_time_now();
		datablock->bot += n;
		
		if (datablock->top != datablock->bot)
		{ /* network chocked */
			gdebug(r, "network full");
			break;
		}
	}
	
	/* Set up for this routine to be called again */
	if (setup_write(r))
		request_end(r, 1, 0);
}

static void log_request_header(const request_t *r)
{
	int i;
	
	if (opt.s)
		return;
	
	/* Hurray, got a request !!! */
	gprintln(r, "%s requests %s", r->peer,
	         r->in.req->argv[1] ? r->in.req->argv[1] : "(none)");
	
	/* print the complete request to the log if in verbose mode */
	gprintln(r, "got a request at port %d:", r->port);
	for (i = 0; i < r->in.req->argc; i++)
		printf(" %s", r->in.req->argv[i]);
	printf("\n");
	
	gprintln(r, "request headers:");
	for (i = 0; i < r->in.req->hc; i++)
		gprintln(r, "%s:%s", r->in.req->hname[i], r->in.req->hvalue[i]);
}

/*
 * Callback when a socket is ready to be read.
 * Read the socket for a complete HTTP request.
 */
static void do_read_request(int fd, short event, void *arg)
{
	request_t *r = (request_t *) arg;
	char      *p = NULL;
	char      *pp = NULL;
	char      *path = NULL;
	int        i = 0;
	int        op;
	int        n;

	/* If we timeout, close the request. */
	if (event & EV_TIMEOUT)
	{
		gwarning(r, "do_read_request time out");
		http_error(r, FDIST_TIMEOUT, "time out");
		request_end(r, 1, "do_read_request time out");
		return;
	}

#ifdef USE_SSL
	/* Execute only once */
	if (opt.ssl && !r->io && !r->ssl_bio)
	{
		r->io = BIO_new(BIO_f_buffer());
		r->ssl_bio = BIO_new(BIO_f_ssl());
		BIO_set_ssl(r->ssl_bio, r->ssl, BIO_CLOSE);
		BIO_push(r->io, r->ssl_bio);
		
		/* Set the renegotiate timeout in seconds. */
		/* When the renegotiate timeout elapses the session is automatically renegotiated */
		BIO_set_ssl_renegotiate_timeout(r->ssl_bio, SSL_RENEGOTIATE_TIMEOUT_SEC);
	}
#endif

	/* how many bytes left in the header buf */
	n = r->in.hbufmax - r->in.hbuftop;
	if (n <= 0)
	{
		gwarning(r, "do_read_request internal error. max: %d, top: %d", r->in.hbufmax, r->in.hbuftop);
		http_error(r, FDIST_INTERNAL_ERROR, "internal error");
		request_end(r, 1, 0);
		return;
	}
	
	/* read into header buf */
	n = tdx_receive(r, r->in.hbuf + r->in.hbuftop, n);
	
	if (n < 0)
	{
#ifdef WIN32
		int e = WSAGetLastError();
		int ok = (e == WSAEINTR || e == WSAEWOULDBLOCK);
#else
		int e = errno;
		
		int ok = (e == EINTR || e == EAGAIN);
#endif
		gwarning(r, "do_read_request receive failed. errno: %d, msg: %s", errno, strerror(errno));
		if (!ok)
		{
			request_end(r, 1, 0);
			return;
		}
	}
	else if (n == 0)
	{
		/* socket close by peer will return 0 */
		gwarning(r,
		         "do_read_request receive failed. socket closed by peer. errno: %d, msg: %s",
		         errno,
		         strerror(errno));
		request_end(r, 1, 0);
		return;
	}
	else
	{
		/* check if a complete HTTP request is available in header buf */
		r->in.hbuftop += n;
		n = r->in.hbuftop;
		r->in.req = gnet_parse_request(r->in.hbuf, &n, r->pool);    /* parse the request */
		if (!r->in.req && r->in.hbuftop >= r->in.hbufmax)
		{
			/* not available, but headerbuf is full - send error and close */
			gwarning(r, "do_read_request bad request");
			http_error(r, FDIST_BAD_REQUEST, "forbidden");
			request_end(r, 1, 0);
			return;
		}
	}
	
	/* if we don't yet have a complete request, set up this function to be called again for. */
	if (!r->in.req)
	{
		if (setup_read(r))
		{
			gwarning(r, "do_read_request, failed to read a complete request");
			http_error(r, FDIST_INTERNAL_ERROR, "internal error");
			request_end(r, 1, 0);
		}
		return;
	}
	
	/* check that the request is validly formatted */
	if (request_validate(r))
	{
		log_request_header(r);
		return;
	}
	
	/* mark it as a GET or PUT request */
	if (0 == strcmp("GET", r->in.req->argv[0]))
		r->is_get = 1;
	
	if (r->is_get || opt.V)
		log_request_header(r);
	
	/* make a copy of the path */
	path = apr_pstrdup(r->pool, r->in.req->argv[1]);
	
	/* decode %xx to char */
	percent_encoding_to_char(p, pp, path);
	
	/* legit check for the path */
	if (request_path_validate(r, path) != 0)
	{
		return;
	}
	
	/*
	 * This is a debug hook. We'll get here By creating an external table with
	 * name(a text) location('tdx://<host>:<port>/tdx/status').
	 * Show some state of tdx (num sessions, num bytes).
	 */
	if (!strcmp(path, "/tdx/status"))
	{
		send_tdx_status(r);
		request_end(r, 0, 0);
		return;
	}
	
	/* 
	 * parse gp variables from the request before set up the requested path.
	 * Because path will differ depending on different remote data sources type. 
	 */
	if (request_parse_tdx_headers(r, opt.g) != 0)
		return;
	
	/* set up the requested path*/
	if (r->remote_data_proto &&
	    (strncasecmp(r->remote_data_proto, PROTOCOL_COS_STR, strlen(PROTOCOL_COS_STR)) == 0 ||
	     strncasecmp(r->remote_data_proto, PROTOCOL_KAFKA_STR, strlen(PROTOCOL_KAFKA_STR)) == 0))
	{
		while (*path == '/' || *path == ' ')
		{
			path++;
		}
		
		/* LIKE: "/prefix1|prefix2" */
		p = strchr(path, TDXPATHDELIM);
		if (p)
		{
			gwarning(r, "TDX does not support more than one prefix name of COS object.");
			http_error(r, FDIST_INTERNAL_ERROR, "internal error. More than one prefix name of COS object not supported.");
			request_end(r, 1, 0);
			return;
		}
		
		if (*path)
		{
			r->path = apr_psprintf(r->pool, "%s", path);
		}
		else
		{
			r->path = "";
		}
	}
	else
	{    
		if (opt.f)
		{
			/*
			 * we forced in a filename with the hidden -f option.
			 * once use it, the filename in the request will be invalid.
			 */
			r->path = opt.f;
		}
		else
		{
			/* format r.path like 'opt.d/file1 opt.d/file2 ...' */
			if (request_set_path(r, opt.d, p, pp, path) != 0)
				return;
		}
	}
	
	if (!r->is_get && r->need_route)
	{
		gwarning(r, "TDX does not support SHARDING while data export.");
		http_error(r, FDIST_INTERNAL_ERROR, "internal error");
		request_end(r, 1, 0);
		return;
	}
	
#ifdef GPFXDIST
	/* setup transform */
	if(request_set_transform(r) != 0)
		return;
#endif
	
	/* attach the request to a session */
	op = session_attach(r);
	
	if (op == -1) /* error */
		return;
	
	if (r->is_get)
	{
		/* handle GET, setup write_event or send thread for current node. */
		handle_get_request(r);
	}
	else
	{
		/* handle PUT */
		handle_post_request(r, n);
	}
	
	/* 
	 * after shardmap initilization, 
	 * 1. initialize kafka topic connection
	 * 2. setup read/consume thread 
	 */
	if (op == 1 && r->is_get)
	{
		session_t *session = r->session;
		pthread_t *pt = session->pt;
		int max_nparallel = (session->remote_protocol_type == REMOTE_KAFKA_PROTOCOL) ? session->partition_cnt : opt.parallel;
		int quotient = opt.parallel / max_nparallel;
		int remainder = opt.parallel % max_nparallel;

		read_thread_para **r_para = pcalloc_safe(NULL, session->pool, sizeof(read_thread_para *) * max_nparallel,
		                                        "failed to allocated read_thread_para array");
		for (i = 0; i < max_nparallel; i++)
		{
			/* 
			 * It is thread-unsafe for multiple threads to allocate in the same pool.
			 * So we apply parameter memory for threads in advance. 
			 */
			r_para[i] = pcalloc_safe(NULL, session->pool, sizeof(read_thread_para),
			                                        "failed to allocated read_thread_para");
			r_para[i]->j = i;
			r_para[i]->session = session;
			
			if (session->remote_protocol_type == REMOTE_KAFKA_PROTOCOL && (quotient > 1 || remainder > 0))
			{
				int k = 0;
				char errstr[512];
				/* need to process msgs in parallel in each partition thread */
				if (quotient > 0)
					r_para[i]->num_batch_thread = quotient;
				else
					r_para[i]->num_batch_thread = 0;
				
				if (i < remainder)
					r_para[i]->num_batch_thread++;
				
				r_para[i]->bt_paras = pcalloc_safe(NULL, session->pool,
				                                   r_para[i]->num_batch_thread * sizeof(batch_thread_para *),
				                                   "failed to allocated batch_thread_para");
				r_para[i]->last_finish_sem = pcalloc_safe(NULL,
				                                          session->pool,
				                                          sizeof(sem_t),
				                                          "failed to allocated last_finish_sem");
				for (k = 0; k < r_para[i]->num_batch_thread; ++k)
				{
					r_para[i]->bt_paras[k] = pcalloc_safe(NULL,
					                                      session->pool,
					                                      sizeof(batch_thread_para),
					                                      "failed to allocated batch_thread_para");
					r_para[i]->bt_paras[k]->session = NULL;
					r_para[i]->bt_paras[k]->messages = (rd_kafka_message_t **) palloc0(sizeof(rd_kafka_message_t *) * session->rk_messages_size);
					
					pthread_mutex_init(&r_para[i]->bt_paras[k]->u_mutex, NULL);
					r_para[i]->bt_paras[k]->used = false;
					
					r_para[i]->bt_paras[k]->kafka_conf = rd_kafka_conf_dup(session->kafka_conf);
					r_para[i]->bt_paras[k]->rk = rd_kafka_new(RD_KAFKA_CONSUMER, r_para[i]->bt_paras[k]->kafka_conf, errstr, sizeof(errstr));
					if (!PointerIsValid(r_para[i]->bt_paras[k]->rk))
					{
						gwarning(r, "Failed to create a Kafka consumer: %s", errstr);
						http_error(r, FDIST_BAD_REQUEST, "Failed to create a Kafka consumer");
						request_end(r, 1, 0);
						return;
					}
					
					r_para[i]->bt_paras[k]->rkt = rd_kafka_topic_new(r_para[i]->bt_paras[k]->rk, session->topic_name, NULL);
					if (!PointerIsValid(r_para[i]->bt_paras[k]->rkt))
					{
						gwarning(r, "Failed to create a Kafka topic: %s [%d]", rd_kafka_err2str(rd_kafka_last_error()), rd_kafka_last_error());
						http_error(r, FDIST_BAD_REQUEST, "Failed to create a Kafka consumer");
						request_end(r, 1, 0);
						kafka_obj_close(session->kobj);
						return;
					}
					/*if (rd_kafka_consume_start(r_para[i]->bt_paras[k]->rkt, 0, RD_KAFKA_OFFSET_BEGINNING) == -1)
					{
						rd_kafka_resp_err_t err = rd_kafka_last_error();
						gwarning(r, "Failed to start consuming: %s[%d]",
						         rd_kafka_err2str(rd_kafka_last_error()), rd_kafka_last_error());
						http_error(r, FDIST_BAD_REQUEST, "Failed to start consuming");
						request_end(r, 1, 0);
						kafka_obj_close(session->kobj);
						return;
					}*/
				}
			}
		}
		
		for (i = 0; i < max_nparallel; i++)
		{
			int ret = 0;
			
			if (session->remote_protocol_type == REMOTE_KAFKA_PROTOCOL)
				ret = pthread_create(&pt[i], NULL, consumer_thread, (void *) r_para[i]);
			else if (session->remote_protocol_type == LOCAL_FILE || session->remote_protocol_type == REMOTE_COS_PROTOCOL)
				ret = pthread_create(&pt[i], NULL, read_thread, (void *) r_para[i]);
			
			if (ret != 0)
			{
				gwarning(r, "do_read_request, fail to create the reading or consuming thread(s)");
				http_error(r, FDIST_INTERNAL_ERROR, "internal error");
				request_end(r, 1, 0);
			}
		}
		gprintln(r, "succeed in creating %d %s threads to process data.",
				 max_nparallel, (session->remote_protocol_type == REMOTE_KAFKA_PROTOCOL ? "consumer" : "read"));
		
		/* create the watching thread */
		if (pthread_create(&pt[max_nparallel], NULL, watch_thread, (void *) session) != 0)
		{
			gwarning(r, "do_read_request, fail to create the watching thread");
			http_error(r, FDIST_INTERNAL_ERROR, "internal error");
			request_end(r, 1, 0);
		}
		session->pthread_setup = true;
		gprintln(r, "succeed in creating watch thread to watch read/send threads.");
	}
}

static void *
watch_thread(void *para)
{
	session_t *session = (session_t *) para;
	int i = 0;
	int max_nparallel = (session->remote_protocol_type == REMOTE_KAFKA_PROTOCOL) ? session->partition_cnt : opt.parallel;
	
	pthread_detach(pthread_self());
	
	for (i = 0; i < max_nparallel; i++)
		pthread_join(session->pt[i], NULL);
	gprintln(NULL, "[sid - %ld] ALL %d read threads exit!", session->id, max_nparallel);
	
	if (session->is_error)
		gwarning(NULL, session->errmsg ? session->errmsg : "internal error");
	
	/* set EOF and signal to active all send_thread to send out remaining data in local buffer. */
	for (i = 0; i < session->maxsegs; i++)
	{
		pthread_mutex_lock(&session->buffer_mutex[i]);
		gdebug(NULL, "[sid - %ld] watch_thread Lock node %d buffer", session->id, i);
	}
	
	session->eof = true;
	for (i = 0; i < session->maxsegs; i++)
	{
		pthread_cond_broadcast(&session->send_cond[i]);
		gdebug(NULL, "[sid - %ld] watch_thread active node %d send signal", session->id, i);
		
		pthread_mutex_unlock(&session->buffer_mutex[i]);
		gdebug(NULL, "[sid - %ld] watch_thread unLock node %d buffer", session->id, i);
	}
	
	for (i = 0; i < session->maxsegs; i++)
		pthread_join(session->st[i], NULL);
	
	gprintln(NULL, "[sid - %ld] ALL %d send threads exit!", session->id, session->maxsegs);
	
	for (i = 0; i < session->maxsegs; i++)
	{
		pthread_cond_destroy(&session->append_cond[i]);
		pthread_cond_destroy(&session->send_cond[i]);
		pthread_mutex_destroy(&session->buffer_mutex[i]);
	}
	
	if (!IS_DATA_STREAM_NULL())
		DATA_STREAM_CLOSE();
	pthread_mutex_destroy(&session->fs_mutex);
	pthread_mutex_destroy(&session->err_mutex);
	pthread_exit(NULL);
}

/* Callback when the listen socket is ready to accept connections. */
static void do_accept(int fd, short event, void *arg)
{
	address_t a;
	socklen_t len = sizeof(a);
	SOCKET sock;
	request_t *r;
	apr_pool_t *pool;
	int on = 1;
	struct linger linger;

#ifdef USE_SSL
	BIO *sbio = NULL;    /* only for SSL */
	SSL *ssl = NULL;    /* only for SSL */
	int rd;                /* only for SSL */
#endif
	
	/* do the accept function to get the connection from the client */
	if ((sock = accept(fd, (struct sockaddr *) &a, &len)) < 0)
	{
		gwarning(NULL, "accept failed");
		goto failure;
	}

#ifdef USE_SSL
	if (opt.ssl)
	{
		sbio = BIO_new_socket(sock, BIO_NOCLOSE);
		ssl = SSL_new(gcb.server_ctx);
		SSL_set_bio(ssl, sbio, sbio);
		if ((rd = SSL_accept(ssl) <= 0))
		{
			handle_ssl_error(sock, sbio, ssl);
			/* Close the socket that was allocated by accept 			*/
			/* We also must perform this, in case that a user 			*/
			/* accidentaly connected via tdx, instead of gpfdits	*/
			closesocket(sock);
			return;
		}
		
		gprint(NULL, "[%d] Using SSL\n", (int) sock);
	}
#endif
	
	/* set to non-blocking, and close-on-exec */
#ifdef WIN32
	{
		unsigned long nonblocking = 1;
		ioctlsocket(sock, FIONBIO, &nonblocking);
	}
#else
	if (fcntl(sock, F_SETFL, O_NONBLOCK) == -1)
	{
		gwarning(NULL, "fcntl(F_SETFL, O_NONBLOCK) failed");
#ifdef USE_SSL
		if (opt.ssl)
		{
			handle_ssl_error(sock, sbio, ssl);
		}
#endif
		closesocket(sock);
		goto failure;
	}
	if (fcntl(sock, F_SETFD, 1) == -1)
	{
		gwarning(NULL, "fcntl(F_SETFD) failed");
#ifdef USE_SSL
		if (opt.ssl)
		{
			handle_ssl_error(sock, sbio, ssl);
		}
#endif
		closesocket(sock);
		goto failure;
	}
#endif
	
	/* set keepalive, reuseaddr, and linger */
	if (setsockopt(sock, SOL_SOCKET, SO_KEEPALIVE, (void *) &on, sizeof(on)) == -1)
	{
		gwarning(NULL, "Setting SO_KEEPALIVE failed");
		closesocket(sock);
		goto failure;
	}
	if (setsockopt(sock, SOL_SOCKET, SO_REUSEADDR, (void *) &on, sizeof(on)) == -1)
	{
		gwarning(NULL, "Setting SO_REUSEADDR on socket failed");
		closesocket(sock);
		goto failure;
	}
	linger.l_onoff = 1;
	linger.l_linger = 10;
	if (setsockopt(sock, SOL_SOCKET, SO_LINGER, (void *) &linger, sizeof(linger)) == -1)
	{
		gwarning(NULL, "Setting SO_LINGER on socket failed");
		closesocket(sock);
		goto failure;
	}
	
	/* create a pool container for this socket */
	if (apr_pool_create(&pool, gcb.pool))
		gfatal(NULL, "out of memory in do_accept");
	
	/* create the request in pool */
	r = pcalloc_safe(NULL, pool, sizeof(request_t), "failed to allocated request_t: %d bytes", (int) sizeof(request_t));
	
	r->port = ntohs(get_client_port((address_t *) &a));
	r->id = ++REQUEST_SEQ;
	r->pool = pool;
	r->sock = sock;
	
	event_set(&r->ev, -1, 0, 0, 0);
	
	/* use the block size specified by -m option */
	r->outblock.data = palloc_safe(r, pool, opt.m, "out of memory when allocating buffer: %d bytes", opt.m);

	r->line_delim_str = "";
	r->line_delim_length = -1;
	
	/* used for sharding only */
	r->col_delim_str = "";
	r->col_delim_length = -1;
	
	r->in.hbufmax = 1024 * 8;    /* 8K for reading the headers */
	r->in.hbuf = palloc_safe(r, pool, r->in.hbufmax, "out of memory when allocating r->in.hbuf: %d", r->in.hbufmax);
	
	r->is_final = 0;    /* initialize */
#ifdef USE_SSL
	r->ssl = ssl;
	r->sbio = sbio;
#endif
	
	{
		char host[128];
		getnameinfo((struct sockaddr *) &a, len, host, sizeof(host), NULL, 0, NI_NUMERICHOST
		                                                                      #ifdef NI_NUMERICSERV
		                                                                      | NI_NUMERICSERV
#endif
		           );
		r->peer = apr_psprintf(r->pool, "%s", host);
	}
	
	
	/* set up for callback when socket ready for reading the http request */
	if (setup_read(r))
	{
		http_error(r, FDIST_INTERNAL_ERROR, "internal error");
		request_end(r, 1, 0);
	}
	
	return;

failure:
	gwarning(NULL, "accept failed");
	return;
}

/* 
 * Close the data stream of a session - the way to close the data stream 
 * for KAFKA is different from other protocols.
 */
static void stream_close(session_t *session)
{
	gprintln(NULL, "close data stream if any");
	
	if (session->remote_protocol_type == REMOTE_KAFKA_PROTOCOL)
	{
		if (!IS_DATA_STREAM_NULL())
		{
			while (1)
			{
				pthread_mutex_lock(&session->fs_mutex);
				if (session->active_prts == 0)
					break;
				else
				{
					pthread_mutex_unlock(&session->fs_mutex);
					pg_usleep(100);
				}
			}
			kafka_obj_close(session->kobj);
			session->kobj = NULL;
			pthread_mutex_unlock(&session->fs_mutex);
		}
	}
	else
	{
		pthread_mutex_lock(&session->fs_mutex);
		if (!IS_DATA_STREAM_NULL())
			DATA_STREAM_CLOSE();
		pthread_mutex_unlock(&session->fs_mutex);
	}
}

/*
 * set up the write event to write data to the socket.
 * it uses the callback function 'do_write'.
 */
static int setup_write(request_t *r)
{
	if (r->sock < 0)
		gwarning(r, "internal error in setup_write - no socket to use");
	event_del(&r->ev);
	event_set(&r->ev, r->sock, EV_WRITE, do_write, r);
	return (event_add(&r->ev, 0));
}


/*
 * set up the read event to read data from a socket.
 *
 * we expect to be reading either:
 * 1) a GET or PUT request. or,
 * 2) the body of a PUT request (the raw data from client).
 *
 * this is controller by 'is_request' as follows:
 * -- if set to true, use the callback function 'do_read_request'.
 * -- if set to false, use the callback function 'do_read_body'.
 */
static int setup_read(request_t *r)
{
	int ret = 0;
	if (r->sock < 0)
		gwarning(r, "internal error in setup_read - no socket to use");
	
	event_del(&r->ev);
	event_set(&r->ev, r->sock, EV_READ, do_read_request, r);
	
	if (opt.t == 0)
	{
		ret = event_add(&r->ev, NULL); /* no timeout */
	}
	else
	{
		r->tm.tv_sec = opt.t;
		r->tm.tv_usec = 0;
		ret = event_add(&r->ev, &r->tm);
	}
	return ret;
}

static void
print_listening_address(struct addrinfo *rp)
{
	char full_address[220] = {0};
	
	if (rp->ai_family == AF_INET)
	{
#ifndef WIN32
		struct sockaddr_in *ain = (struct sockaddr_in *) rp->ai_addr;
		char stradd[200] = {0};
		inet_ntop(AF_INET, (const void *) &ain->sin_addr, stradd, 100);
		sprintf(full_address, "IPV4 socket: %s:%d", stradd, opt.p);
#else
		/*
		 * there is no alternative for inet_ntop in windows that works for all Win platforms
		 * and for IPV6. inet_ntop transform the integer representation of the IP addr. into a string
		 */
		sprintf(full_address, "IPV4 socket: IPv4:%d", opt.p);
#endif
	
	}
	else if (rp->ai_family == AF_INET6)
	{
#ifndef WIN32
		struct sockaddr_in6 *ain = (struct sockaddr_in6 *) rp->ai_addr;
		char stradd[200] = {0};
		inet_ntop(AF_INET6, (const void *) &ain->sin6_addr, stradd, 100);
		sprintf(full_address, "IPV6 socket: [%s]:%d", stradd, opt.p);
#else
		sprintf(full_address, "IPV6 socket: [IPV6]:%d", opt.p);
#endif
	
	}
	else
	{
		sprintf(full_address, "unknown protocol - %d", rp->ai_family);
	}
	
	gprint(NULL, "%s\n", full_address);
}

/*
 * Search linked list (head) for first element with family (first_family).
 * Moves first matching element to head of the list.
 */
static struct
addrinfo *rearrange_addrs(struct addrinfo *head, int first_family)
{
	struct addrinfo *iter;
	struct addrinfo *new_head = head;
	struct addrinfo *holder = NULL;
	
	if (head->ai_family == first_family)
		return head;
	
	for (iter = head; iter != NULL && iter->ai_next != NULL; iter = iter->ai_next)
	{
		if (iter->ai_next->ai_family == first_family)
		{
			holder = iter->ai_next;
			iter->ai_next = iter->ai_next->ai_next;
			/*
			 * we don't break here since if there are more addrinfo structure that belong to first_family
			 * in the list, we want to remove them all and keep only one in the holder.
			 * and then we will put the holder in the front
			 */
		}
	}
	
	if (holder != NULL)
	{
		holder->ai_next = new_head;
		new_head = holder;
	}
	
	return new_head;
}

static void
print_addrinfo_list(struct addrinfo *head)
{
	struct addrinfo *iter;
	for (iter = head; iter != NULL; iter = iter->ai_next)
	{
		print_listening_address(iter);
	}
}

static void
signal_register()
{
	/* when SIGTERM raised invoke process_term_signal */
	signal_set(&gcb.signal_event, SIGTERM, process_term_signal, 0);
	
	/* high priority so we accept as fast as possible */
	if (event_priority_set(&gcb.signal_event, 0))
		gwarning(NULL, "signal event priority set failed");
	
	/* start watching this event */
	if (signal_add(&gcb.signal_event, 0))
		gfatal(NULL, "cannot set up event on signal register");
	
}

static int
set_socket_nonblocking(int fd)
{
	int flags;

	if (!opt.V)
	{
		return 0;
	}

	/* get current fd flags */
	if ((flags = fcntl(fd, F_GETFL, 0)) == -1)
	{
		return -1;
	}

	/* set flag for this fd */
	if (fcntl(fd, F_SETFL, flags | O_NONBLOCK) == -1)
	{
		return -1;
	}

	return 0;
}
/* Create HTTP port and start to receive request */
static void
http_setup(void)
{
	SOCKET f;
	int on = 1;
	struct linger linger;
	struct addrinfo hints;
	struct addrinfo *addrs, *rp;
	int s;
	int i;
	
	char service[32];
	const char *hostaddr = NULL;

#ifdef USE_SSL
	if (opt.ssl)
	{
		/* Build our SSL context*/
		gcb.server_ctx = initialize_ctx();
		tdx_send = tdx_SSL_send;
		tdx_receive = tdx_SSL_receive;
	}
	else
	{
		gcb.server_ctx = NULL;
		tdx_send = tdx_socket_send;
		tdx_receive = tdx_socket_receive;
	}
#else
	tdx_send 	= tdx_socket_send;
	tdx_receive = tdx_socket_receive;
#endif
	
	gcb.listen_sock_count = 0;
	if (opt.b != NULL && strlen(opt.b) > 1)
		hostaddr = opt.b;
	
	/* setup event priority */
	if (event_priority_init(10))
		gwarning(NULL, "event_priority_init failed");
	
	
	/* Try each possible port from opt.p to opt.last_port */
	for (;;)
	{
		snprintf(service, 32, "%d", opt.p);
		memset(&hints, 0, sizeof(struct addrinfo));
		hints.ai_family = AF_UNSPEC;    /* Allow IPv4 or IPv6 */
		hints.ai_socktype = SOCK_STREAM; /* tcp socket */
		hints.ai_flags = AI_PASSIVE;    /* For wildcard IP address */
		hints.ai_protocol = 0;            /* Any protocol */
		
		s = getaddrinfo(hostaddr, service, &hints, &addrs);
		if (s != 0)
#if (!defined(WIN32)) || defined(gai_strerror)
			gfatal(NULL, "getaddrinfo says %s", gai_strerror(s));
#else
		/* Broken mingw header file from old version of mingw doesn't have gai_strerror */
		gfatal(NULL,"getaddrinfo says %d",s);
#endif
		
		addrs = rearrange_addrs(addrs, AF_INET6);
		
		gprint(NULL, "Before opening listening sockets - following listening sockets are available:\n");
		print_addrinfo_list(addrs);
		
		/*
		 * getaddrinfo() returns a list of address structures,
		 * one for each valid address and family we can use.
		 *
		 * Try each address until we successfully bind. If socket (or bind)
		 * fails, we (close the socket and) try the next address. This can
		 * happen if the system supports IPv6, but IPv6 is disabled from
		 * working, or if it supports IPv6 and IPv4 is disabled.
		 */
		for (rp = addrs; rp != NULL; rp = rp->ai_next)
		{
			gprint(NULL, "Trying to open listening socket:\n");
			print_listening_address(rp);
			
			/*
			 * getaddrinfo gives us all the parameters for the socket() call
			 * as well as the parameters for the bind() call.
			 */
			f = socket(rp->ai_family, rp->ai_socktype, rp->ai_protocol);
			
			if (f == -1)
			{
				gwarning(NULL, "Creating the socket failed\n");
				continue;
			}

#ifndef WIN32
			if (fcntl(f, F_SETFD, 1) == -1)
				gfatal(NULL, "cannot create socket - fcntl(F_SETFD) failed");

			if (set_socket_nonblocking(f) != 0)
				gfatal(NULL, "cannot set nonblocking - fcntl(F_SETFL) failed");
			
			/* For the Windows case, we could use SetHandleInformation to remove
			 the HANDLE_INHERIT property from fd.
			 But for our purposes this does not matter,
			 as by default handles are *not* inherited. */

#endif
			if (setsockopt(f, SOL_SOCKET, SO_KEEPALIVE, (void *) &on, sizeof(on)) == -1)
			{
				closesocket(f);
				gwarning(NULL, "Setting SO_KEEPALIVE on socket failed");
				continue;
			}
			
			/*
			 * We cannot use SO_REUSEADDR on win32 because it results in different
			 * behaviour -- it allows multiple servers to bind to the same port,
			 * resulting in totally unpredictable behaviour. What a silly operating
			 * system.
			 */
#ifndef WIN32
			if (setsockopt(f, SOL_SOCKET, SO_REUSEADDR, (void *) &on, sizeof(on)) == -1)
			{
				closesocket(f);
				gwarning(NULL, "Setting SO_REUSEADDR on socket failed");
				continue;
			}
#endif
			linger.l_onoff = 1;
			linger.l_linger = 5;
			if (setsockopt(f, SOL_SOCKET, SO_LINGER, (void *) &linger, sizeof(linger)) == -1)
			{
				closesocket(f);
				gwarning(NULL, "Setting SO_LINGER on socket failed");
				continue;
			}
			
			if (bind(f, rp->ai_addr, rp->ai_addrlen) != 0)
			{
				/*
				 * EADDRINUSE warning appears only if the -v or -V option is on,
				 * All the other warnings will appear anyway
				 * EADDRINUSE is not defined in win32, so all the warnings will always appear.
				 */
#ifdef WIN32
				if ( 1 )
#else
				if (errno == EADDRINUSE)
#endif
				{
					if (opt.v)
					{
						gwarning(NULL, "%s (errno = %d), port: %d",
						         strerror(errno), errno, opt.p);
					}
				}
				else
				{
					gwarning(NULL, "%s (errno=%d), port: %d", strerror(errno), errno, opt.p);
				}
				
				/* failed on bind, maybe this address family isn't supported */
				closesocket(f);
				continue;
			}
			
			/* listen with a big queue */
			if (listen(f, opt.z))
			{
				int saved_errno = errno;
				closesocket(f);
				gwarning(NULL, "listen with queue size %d on socket (%d) using port %d failed with error code (%d): %s",
				         opt.z,
				         (int) f,
				         opt.p,
				         saved_errno,
				         strerror(saved_errno));
				continue;
			}
			gcb.listen_socks[gcb.listen_sock_count++] = f;
			
			gprint(NULL, "Opening listening socket succeeded\n");
		}
		
		/* When we get here, we have either succeeded, or tried all address families for this port */
		
		if (addrs != NULL)
		{
			/* don't need this anymore */
			freeaddrinfo(addrs);
		}
		
		if (gcb.listen_sock_count > 0)
			break;
		
		if (opt.p >= opt.last_port)
			gfatal(NULL, "cannot create socket on port %d "
			             "(last port is %d)", opt.p, opt.last_port);
		
		opt.p++;
		if (opt.v)
			putchar('\n'); /* this is just to beautify the print outs */
	}
	
	for (i = 0; i < gcb.listen_sock_count; i++)
	{
		/* when this socket is ready, do accept */
		event_set(&gcb.listen_events[i], gcb.listen_socks[i],
		          EV_READ | EV_PERSIST, do_accept, 0);
		
		/* only signal process function priority higher than socket handler */
		if (event_priority_set(&gcb.listen_events[i], 1))
			gwarning(NULL, "event_priority_set failed");
		
		/* start watching this event */
		if (event_add(&gcb.listen_events[i], 0))
			gfatal(NULL, "cannot set up event on listen socket: %s",
			       strerror(errno));
	}
}

void
process_term_signal(int sig, short event, void *arg)
{
	int i;

	gwarning(NULL, "signal %d received. tdx exits", sig);
	log_tdx_status();
	fflush(stdout);

	for (i = 0; i < gcb.listen_sock_count; i++)
		if (gcb.listen_socks[i] > 0)
		{
			closesocket(gcb.listen_socks[i]);
		}
	_exit(1);
}


static gnet_request_t *
gnet_parse_request(const char *buf, int *len, apr_pool_t *pool)
{
	int n = *len;
	int empty, completed;
	const char *p;
	char *line;
	char *last = NULL;
	char *colon;
	gnet_request_t *req = 0;
	
	/* find an empty line */
	*len = 0;
	empty = 1, completed = 0;
	for (p = buf; n > 0 && *p; p++, n--)
	{
		int ch = *p;
		/* skip spaces */
		if (ch == ' ' || ch == '\t' || ch == '\r')
			continue;
		if (ch == '\n')
		{
			if (!empty)
			{
				empty = 1;
				continue;
			}
			p++;
			completed = 1;
			break;
		}
		empty = 0;
	}
	if (!completed)
		return 0;
	
	/* we have a complete HTTP-style request (terminated by empty line) */
	*len = n = p - buf; /* consume it */
	line = apr_pstrndup(pool, buf, n); /* dup it */
	req = pcalloc_safe(NULL, pool, sizeof(gnet_request_t), "out of memory in gnet_parse_request");
	
	/* for first line */
	line = apr_strtok(line, "\n", &last);
	if (!line)
		line = apr_pstrdup(pool, "");
	line = gstring_trim(line);
	
	if (0 != apr_tokenize_to_argv(line, &req->argv, pool))
		return req;
	
	while (req->argv[req->argc])
	{
		req->argc++;
	}
	
	if (last == NULL)
	{
		gwarning(NULL, "last is NULL");
		return req;
	}
	
	/* for each subsequent lines */
	while (0 != (line = apr_strtok(0, "\n", &last)))
	{
		if (*line == ' ' || *line == '\t')
		{
			/* continuation */
			if (req->hc == 0) /* illegal - missing first header */
				break;
			
			line = gstring_trim(line);
			if (*line == 0) /* empty line */
				break;
			
			/* add to previous hvalue */
			req->hvalue[req->hc - 1] = gstring_trim(apr_pstrcat(pool,
			                                                    req->hvalue[req->hc - 1], " ", line, (char *) 0));
			continue;
		}
		/* find a colon, and break the line in two */
		if (!(colon = strchr(line, ':')))
			colon = line + strlen(line);
		else
			*colon++ = 0;
		
		line = gstring_trim(line);
		if (*line == 0) /* empty line */
			break;
		
		/* save name, value pair and the parameter count */
		req->hname[req->hc] = line;
		req->hvalue[req->hc] = gstring_trim(colon);
		req->hc++;
		
		if (req->hc >= sizeof(req->hname) / sizeof(req->hname[0]))
			break;
		if (last == NULL)
			break;
	}
	
	return req;
}

static char *gstring_trim(char *s)
{
	char *p;
	s += strspn(s, " \t\r\n");
	for (p = s + strlen(s) - 1; p > s; p--)
	{
		if (strchr(" \t\r\n", *p))
			*p = 0;
		else
			break;
	}
	return s;
}

/*
 * decode any percent encoded characters that may be included in the http
 * request into normal characters ascii characters.
 */
void percent_encoding_to_char(char *p, char *pp, char *path)
{
	/*   - decode %xx to char */
	for (p = pp = path; *pp; p++, pp++)
	{
		if ('%' == (*p = *pp))
		{
			if (pp[1] && pp[2])
			{
				int x = pp[1];
				int y = pp[2];
				
				if ('0' <= x && x <= '9')
					x -= '0';
				else if ('a' <= x && x <= 'f')
					x = x - 'a' + 10;
				else if ('A' <= x && x <= 'F')
					x = x - 'A' + 10;
				else
					x = -1;
				
				if ('0' <= y && y <= '9')
					y -= '0';
				else if ('a' <= y && y <= 'f')
					y = y - 'a' + 10;
				else if ('A' <= y && y <= 'F')
					y = y - 'A' + 10;
				else
					y = -1;
				
				if (x >= 0 && y >= 0)
				{
					x = (x << 4) + y;
					*p = (char) x;
					pp++, pp++;
				}
			}
		}
	}
	
	*p = 0;
}

static void handle_get_request(request_t *r)
{
	if (r->need_route)
	{
		gprintlnif(r, "handle_get_request process sharding request.");
		/* setup send thread to write to socket */
		if (!r->session || !r->session->st)
		{
			gwarning(r, "handle_get_request failed to get current session");
			http_error(r, FDIST_INTERNAL_ERROR, "internal error");
			request_end(r, 1, 0);
			return;
		}
		if (pthread_create(&r->session->st[r->segid], NULL, send_thread, (void *) r) != 0)
			gprintln(r, "fail to create the sending thread for node %d", r->segid);
		gprintln(r, "succeed in creating the sending thread for node %d.", r->segid);
	}
	else if (r->remote_data_proto &&
	         (strncasecmp(r->remote_data_proto, PROTOCOL_KAFKA_STR, strlen(PROTOCOL_KAFKA_STR)) == 0))
	{
		gwarning(r, "streaming is only supported for data import.");
		http_error(r, FDIST_INTERNAL_ERROR, "streaming is only supported for data import.");
		request_end(r, 1, 0);
		return;
	}
	else
	{
		gprintlnif(r, "handle_get_request process non-sharding request.");
		/* setup to receive EV_WRITE events to write to socket */
		if (setup_write(r))
		{
			gwarning(r, "handle_get_request failed to setup write handler");
			http_error(r, FDIST_INTERNAL_ERROR, "internal error");
			request_end(r, 1, 0);
			return;
		}
	}

	if (0 != http_ok(r))
	{
		gwarning(r, "handle_get_request failed to send HTTP OK");
		request_end(r, 1, 0);
	}
}

static void handle_post_request(request_t *r, int header_end)
{
	int h_count = r->in.req->hc;
	char **h_names = r->in.req->hname;
	char **h_values = r->in.req->hvalue;
	int i = 0;
	int b_continue = 0;
	char *data_start = 0;
	int data_bytes_in_req = 0;
	int wrote = 0;
	session_t *session = r->session;
	const char *ferror = NULL;
	
	/*
	 * If this request is a "done" request (has TDX-DONE header set)
	 * it has already marked this segment as inactive in this session.
	 * This is all that a "done" request should do. no data to process.
	 * we send our success response and end the request.
	 */
	if (r->is_final)
		goto done_processing_request;
	
	for (i = 0; i < h_count; i++)
	{
		/* the request include a "Expect: 100-continue" header? */
		if (strcmp("Expect", h_names[i]) == 0 && strcmp("100-continue", h_values[i]) == 0)
			b_continue = 1;
		
		/* find out how long is our data by looking at "Content-Length" header*/
		if (strcmp("Content-Length", h_names[i]) == 0)
			r->in.davailable = atoi(h_values[i]);
	}
	
	/* if client asked for 100-Continue, send it. otherwise, move on. */
	if (b_continue)
		http_continue(r);
	
	gdebug(r, "available data to consume %d, starting at offset %d",
	       r->in.davailable, r->in.hbuftop);
	
	switch (r->seq)
	{
		case OPEN_SEQ:
			/* sequence number is 1, it's the first OPEN request */
			session->seq_segs[r->segid] = r->seq;
			goto done_processing_request;
		
		case NO_SEQ:
			/* don't have sequence number */
			if (session->seq_segs[r->segid] > 0)
			{
				/* missing sequence number */
#ifdef WIN32
				gprintln(r, "got an request missing sequence number, expected sequence number is %ld.",
					(long)session->seq_segs[r->segid]+1);
#else
				gprintln(r, "got an request missing sequence number, expected sequence number is %"
				APR_INT64_T_FMT,
						session->seq_segs[r->segid] + 1);
#endif
				http_error(r, FDIST_BAD_REQUEST, "invalid request due to missing sequence number");
				gwarning(r, "got an request missing sequence number");
				request_end(r, 1, 0);
				return;
			}
			else
			{
				/* old version GPDB, don't have sequence number */
				break;
			}
		
		default:
			/* sequence number > 1, it's the subsequent DATA request */
			if (session->seq_segs[r->segid] == r->seq)
			{
				/* duplicate DATA request, ignore it*/
#ifdef WIN32
				gdebug(r, "got a duplicate request, sequence number is %ld.", (long) r->seq);
#else
				gdebug(r, "got a duplicate request, sequence number is %"
				APR_INT64_T_FMT
				".",
						r->seq);
#endif
				goto done_processing_request;
			}
			else if (session->seq_segs[r->segid] != r->seq - 1)
			{
				/* out of order DATA request, ignore it*/
#ifdef WIN32
				gprintln(r, "got an out of order request, sequence number is %ld, expected sequence number is %ld.",
					(long)r->seq, (long)session->seq_segs[r->segid] + 1);
#else
				gprintln(r, "got an out of order request, sequence number is %"
				APR_INT64_T_FMT
				", expected sequence number is %"
				APR_INT64_T_FMT,
						r->seq, session->seq_segs[r->segid] + 1);
#endif
				http_error(r, FDIST_BAD_REQUEST, "invalid request due to wrong sequence number");
				gwarning(r, "got an out of order request");
				request_end(r, 1, 0);
				return;
			}
	}
	
	/* create a buffer to hold the incoming raw data */
	r->in.dbufmax = opt.m; /* size of max line size */
	r->in.dbuftop = 0;
	r->in.dbuf = palloc_safe(r,
	                         r->pool,
	                         r->in.dbufmax,
	                         "out of memory when allocating r->in.dbuf: %d bytes",
	                         r->in.dbufmax);
	
	/* if some data come along with the request, copy it first */
	data_start = strstr(r->in.hbuf, "\r\n\r\n");
	if (data_start)
	{
		data_start += 4;
		data_bytes_in_req = (r->in.hbuf + r->in.hbuftop) - data_start;
	}
	
	if (data_bytes_in_req > 0)
	{
		/* we have data after the request headers. consume it */
		/* should make sure r->in.dbuftop + data_bytes_in_req <  r->in.dbufmax */
		memcpy(r->in.dbuf, data_start, data_bytes_in_req);
		r->in.dbuftop += data_bytes_in_req;
		r->in.davailable -= data_bytes_in_req;
		
		/* only write it out if no more data is expected */
		if (r->in.davailable == 0)
		{
			if (session->remote_protocol_type == REMOTE_COS_PROTOCOL)
			{
				wrote = cos_fstream_write(cosContext, session->bucket, session->cos_fs,
				                          r->in.dbuf, data_bytes_in_req, 1, r->line_delim_str, r->line_delim_length);
			}
			else
			{
				wrote = fstream_write(session->fstream, r->in.dbuf, data_bytes_in_req, 1,
				                      r->line_delim_str, r->line_delim_length);
			}
			
			delay_watchdog_timer();
			if (wrote == -1)
			{
				/* write error */
				http_error(r, FDIST_INTERNAL_ERROR, fstream_get_error(session->fstream));
				request_end(r, 1, 0);
				return;
			}
		}
	}
	
	/*
	 * we've consumed all data that came in the first buffer (with the request)
	 * if we're still expecting more data, get it from socket now and process it.
	 */
	while (r->in.davailable > 0)
	{
		size_t want;
		ssize_t n;
		size_t buf_space_left = r->in.dbufmax - r->in.dbuftop;
		
		if (r->in.davailable > buf_space_left)
			want = buf_space_left;
		else
			want = r->in.davailable;
		
		/* read from socket into data buf */
		n = tdx_receive(r, r->in.dbuf + r->in.dbuftop, want);
		
		if (n < 0)
		{
#ifdef WIN32
			int e = WSAGetLastError();
			int ok = (e == WSAEINTR || e == WSAEWOULDBLOCK);
#else
			int e = errno;
			int ok = (e == EINTR || e == EAGAIN);
#endif
			if (!ok)
			{
				gwarning(r, "handle_post_request receive errno: %d, msg: %s", e, strerror(e));
				http_error(r, FDIST_INTERNAL_ERROR, "internal error");
				request_end(r, 1, 0);
				return;
			}
		}
		else if (n == 0)
		{
			/* socket close by peer will return 0 */
			gwarning(r, "handle_post_request socket closed by peer");
			request_end(r, 1, 0);
			return;
		}
		else
		{
			/*gprint("received %d bytes from client\n", n);*/
			
			r->bytes += n;
			r->last = apr_time_now();
			r->in.davailable -= n;
			r->in.dbuftop += n;
			
			/* if filled our buffer or no more data expected, write it */
			if (r->in.dbufmax == r->in.dbuftop || r->in.davailable == 0)
			{
				/* only write up to end of last row */
				
				if (session->remote_protocol_type == REMOTE_COS_PROTOCOL)
				{
					wrote = cos_fstream_write(cosContext, session->bucket, session->cos_fs,
					                          r->in.dbuf, r->in.dbuftop, 1, r->line_delim_str, r->line_delim_length);
					ferror = cos_fstream_get_error(session->cos_fs);
				}
				else
				{
					wrote = fstream_write(session->fstream, r->in.dbuf, r->in.dbuftop, 1,
					                      r->line_delim_str, r->line_delim_length);
					ferror = fstream_get_error(session->fstream);
				}
				gdebug(r, "wrote %d bytes to file", wrote);
				delay_watchdog_timer();
				
				if (wrote == -1)
				{
					/* write error */
					gwarning(r, "handle_post_request, write error: %s", ferror);
					http_error(r, FDIST_INTERNAL_ERROR, ferror);
					request_end(r, 1, 0);
					return;
				}
				else if (wrote == r->in.dbuftop)
				{
					/* wrote the whole buffer. clean it for next round */
					r->in.dbuftop = 0;
				}
				else
				{
					/* wrote up to last line, some data left over in buffer. move to front */
					int bytes_left_over = r->in.dbuftop - wrote;
					
					memmove(r->in.dbuf, r->in.dbuf + wrote, bytes_left_over);
					r->in.dbuftop = bytes_left_over;
				}
			}
		}
		
	}
	
	session->seq_segs[r->segid] = r->seq;

done_processing_request:
	
	/* send our success response and end the request */
	if (0 != http_ok(r))
		request_end(r, 1, 0);
	else
		request_end(r, 0, 0); /* we're done! */
}

static int request_set_path(request_t *r, const char *d, char *p, char *pp, char *path)
{
	r->path = 0;
	
	/* make the new path relative to the user's specified dir (opt.d) */
	do
	{
		while (*path == ' ')
		{
			path++;
		}
		
		/* split multiple files | */
		p = strchr(path, TDXPATHDELIM);
		if (p)
		{
			*p = 0;
			p++;
		}
		
		while (*path == '/')
		{
			path++;
		}
		
		if (*path)
		{
			if (r->path)
				r->path = apr_psprintf(r->pool, "%s %s/%s", r->path, d,
				                       path);
			else
				r->path = apr_psprintf(r->pool, "%s/%s", d, path);
		}
		
		path = p;
		
	} while (path);
	
	if (!r->path)
	{
		http_error(r, FDIST_BAD_REQUEST, "invalid request (unable to set path)");
		request_end(r, 1, 0);
		return -1;
	}
	
	return 0;
}

static int request_path_validate(request_t *r, const char *path)
{
	const char *warn_msg = NULL;
	const char *http_err_msg = NULL;

#ifdef WIN32
	if (strstr(path, ".."))
#else
	if (strstr(path, "\\"))
	{
		/*
		 * '\' is the path separator under windows.
		 * For *nix, escape char may cause some unexpected result with
		 * the file API. e.g.: 'ls \.\.' equals to 'ls ..'.
		 */
		warn_msg = "contains escape character backslash '\\'";
		http_err_msg = "invalid request, "
		               "escape character backslash '\\' is not allowed.";
	}
	else if (strstr(path, ".."))
#endif
	{
		/* disallow using a relative path in the request. CWE23 */
		warn_msg = "is using a relative path";
		http_err_msg = "invalid request due to relative path";
	}
	
	if (warn_msg)
	{
		gwarning(r, "reject invalid request from %s [%s %s] - request %s",
		         r->peer,
		         r->in.req->argv[0],
		         r->in.req->argv[1],
		         warn_msg);
		
		http_error(r, FDIST_BAD_REQUEST, http_err_msg);
		request_end(r, 1, 0);
		return -1;
	}
	
	return 0;
}

static int request_validate(request_t *r)
{
	/*
	 * parse the HTTP request.
	 * Expect "GET /path HTTP/1.X" or "PUT /path HTTP/1.X"
	 */
	if (r->in.req->argc != 3)
	{
		gprintln(r, "reject invalid request from %s", r->peer);
		http_error(r, FDIST_BAD_REQUEST, "invalid request");
		request_end(r, 1, 0);
		return -1;
	}
	if (0 != strncmp("HTTP/1.", r->in.req->argv[2], 7))
	{
		gprintln(r, "reject invalid protocol from %s [%s]", r->peer, r->in.req->argv[2]);
		http_error(r, FDIST_BAD_REQUEST, "invalid request");
		request_end(r, 1, 0);
		return -1;
	}
	if (0 != strcmp("GET", r->in.req->argv[0]) &&
	    0 != strcmp("POST", r->in.req->argv[0]))
	{
		gprintln(r, "reject invalid request from %s [%s %s]", r->peer,
		         r->in.req->argv[0], r->in.req->argv[1]);
		http_error(r, FDIST_BAD_REQUEST, "invalid request");
		request_end(r, 1, 0);
		return -1;
	}
	
	return 0;
}

/**
 * @brief URLDecode : decode the urlencoded str to base64 encoded string
 *
 * @param str:  the urlencoded string
 * @param strsz:  the str length (exclude the last \0)
 * @param result:  the result buffer
 * @param resultsz: the result buffer size(exclude the last \0)
 *
 * @return: >=0 represent the decoded result length
 *              <0 encode failure
 *
 * Note:
 * 1) to ensure the result buffer has enough space to contain the decoded string, we'd better
 *     to set resultsz to strsz
 *
 */
static int URLDecode(const char *str, const int strsz, char *result, const int resultsz)
{
	int i, j;
	char ch;
	char a;
	
	if (strsz < 0 || resultsz < 0)
		return -1;
	
	for (i = 0, j = 0; i < strsz && j < resultsz; j++)
	{
		ch = *(str + i);
		
		if (ch == '+')
		{
			result[j] = ' ';
			i += 1;
		}
		else if (ch == '%')
		{
			if (i + 3 <= strsz)
			{
				ch = *(str + i + 1);
				
				if (ch >= 'A' && ch <= 'F')
				{
					a = (ch - 'A') + 10;
				}
				else if (ch >= '0' && ch <= '9')
				{
					a = ch - '0';
				}
				else if (ch >= 'a' && ch <= 'f')
				{
					a = (ch - 'a') + 10;
				}
				else
				{
					return -2;
				}
				
				a <<= 4;
				
				ch = *(str + i + 2);
				if (ch >= 'A' && ch <= 'F')
				{
					a |= (ch - 'A') + 10;
				}
				else if (ch >= '0' && ch <= '9')
				{
					a |= (ch - '0');
				}
				else if (ch >= 'a' && ch <= 'f')
				{
					a |= (ch - 'a') + 10;
				}
				else
				{
					return -2;
				}
				
				result[j] = a;
				
				i += 3;
			}
			else
				break;
		}
		else if ((ch >= 'A' && ch <= 'Z') ||
		         (ch >= 'a' && ch <= 'z') ||
		         (ch >= '0' && ch <= '9') ||
		         ch == '.' || ch == '-' || ch == '*' || ch == '_')
		{
			
			result[j] = ch;
			i += 1;
		}
		else
		{
			return -2;
		}
		
	}
	
	return j;
}

/*
 * Extract all variables from the HTTP headers.
 * Create a unique TID value from it.
 */
static int request_parse_tdx_headers(request_t *r, int opt_g)
{
	const char *xid = 0;
	const char *cid = 0;
	const char *sn = 0;
	const char *tdx_proto = NULL; /* default to invalid, so that report error if not specified*/
	int i;
	
	r->csvopt = "";
	r->is_final = 0;
	r->seq = 0;
	
	for (i = 0; i < r->in.req->hc; i++)
	{
		if (0 == strcasecmp("XID", r->in.req->hname[i]))
			xid = r->in.req->hvalue[i];
		else if (0 == strcasecmp("CID", r->in.req->hname[i]))
			cid = r->in.req->hvalue[i];
		else if (0 == strcasecmp("SN", r->in.req->hname[i]))
			sn = r->in.req->hvalue[i];
		else if (0 == strcasecmp("CSVOPT", r->in.req->hname[i]))
			r->csvopt = r->in.req->hvalue[i];
		else if (0 == strcasecmp("FORMATOPT", r->in.req->hname[i]))
			r->formatopt = r->in.req->hvalue[i];
		else if (0 == strcasecmp("X-TDX-PROTO", r->in.req->hname[i]))
			tdx_proto = r->in.req->hvalue[i];
		else if (0 == strcasecmp("DONE", r->in.req->hname[i]))
			r->is_final = 1;        /* only for POST */
		else if (0 == strcasecmp("NODE-COUNT", r->in.req->hname[i]))
			r->totalsegs = atoi(r->in.req->hvalue[i]);
		else if (0 == strcasecmp("NODE-ID", r->in.req->hname[i]))
			r->segid = atoi(r->in.req->hvalue[i]);
		else if (0 == strcasecmp("NEED-ROUTE", r->in.req->hname[i]))
			r->need_route = true;
		else if (0 == strcasecmp("NODE-IN-SHARD", r->in.req->hname[i]))
			r->shard_node_pos = i;
		else if (0 == strcasecmp("SHARD-NUM", r->in.req->hname[i]))
			r->shard_num_pos = i;
		else if (0 == strcasecmp("FIELDS-NUM", r->in.req->hname[i]))
			r->fields_num_pos = i;
		else if (0 == strcasecmp("COLUMN-POSITION", r->in.req->hname[i]))
			r->col_position_pos = i;
		else if (0 == strcasecmp("DIS-COLUMN-NUM", r->in.req->hname[i]))
			r->dis_key_num_pos = i;
		else if (0 == strcasecmp("DIS-COLUMN", r->in.req->hname[i]))
			r->dis_key_pos = i;
		else if (0 == strcasecmp("DIS-COLUMN-TYPE", r->in.req->hname[i]))
			r->dis_key_type_pos = i;
		else if (0 == strcasecmp("COLUMN-TYPE", r->in.req->hname[i]))
			r->col_type_pos = i;
		else if (0 == strcasecmp("COLUMN-NAME", r->in.req->hname[i]))
			r->col_name_pos = i;
		else if (0 == strcasecmp("LOGICAL-MSG", r->in.req->hname[i]))
			r->is_logical_msg = atoi(r->in.req->hvalue[i]);
		else if (0 == strcasecmp("LINE-DELIM-STR", r->in.req->hname[i]))
		{
			int sz;

			if (NULL == r->in.req->hvalue[i])
			{
				gwarning(r, "reject invalid request from %s, invalid EOL", r->peer);
				http_error(r, FDIST_BAD_REQUEST, "invalid EOL");
				request_end(r, 1, 0);
				return -1;
			}
			
			r->line_delim_str = pcalloc_safe(r, r->pool, strlen(r->in.req->hvalue[i]) * sizeof(char) + 1,
			                          "out of memory when allocating line_delim_str");
			sz = URLDecode(r->in.req->hvalue[i], strlen(r->in.req->hvalue[i]), r->line_delim_str, strlen(r->in.req->hvalue[i]));
			if (sz < 0)
			{
				gwarning(r, "bad request, cannot decode eol \"%s\"", r->in.req->hvalue[i]);
				http_error(r, FDIST_BAD_REQUEST, "bad request, cannot decode eol");
				request_end(r, 1, 0);
				return -1;
			}
			r->line_delim_str[sz * sizeof(char)] = '\0';
		}
		else if (0 == strcasecmp("LINE-DELIM-LENGTH", r->in.req->hname[i]))
			r->line_delim_length = atoi(r->in.req->hvalue[i]);
		else if (0 == strcasecmp("COLUMN-DELIM-STR", r->in.req->hname[i]))
		{
			if (NULL == r->in.req->hvalue[i])
			{
				gwarning(r, "reject invalid request from %s, invalid DELIMITER", r->peer);
				http_error(r, FDIST_BAD_REQUEST, "invalid DELIMITER");
				request_end(r, 1, 0);
				return -1;
			}
			r->col_delim_str = r->in.req->hvalue[i];
		}
		else if (0 == strcasecmp("COLUMN-DELIM-LENGTH", r->in.req->hname[i]))
			r->col_delim_length = atoi(r->in.req->hvalue[i]);
#ifdef GPFXDIST
		else if (0 == strcasecmp("TRANSFORM", r->in.req->hname[i]))
			r->trans.name = r->in.req->hvalue[i];
#endif
		else if (0 == strcasecmp("SEQ", r->in.req->hname[i]))
		{
			r->seq = atol(r->in.req->hvalue[i]);
			/* sequence number starting from 1 */
			if (r->seq <= 0)
			{
				gwarning(r, "reject invalid request from %s, invalid sequence number: %s", r->peer, r->in.req->hvalue[i]);
				http_error(r, FDIST_BAD_REQUEST, "invalid sequence number");
				request_end(r, 1, 0);
				return -1;
			}
		}
		else if (0 == strcasecmp("FORCE-NOTNULL-FLAGS", r->in.req->hname[i]))
			r->force_notnull_flags_pos = i;
		else if (0 == strcasecmp("FORCE-NULL-FLAGS", r->in.req->hname[i]))
			r->force_null_flags_pos = i;
		else if (0 == strcasecmp("CONVERT-SELECT-FLAGS", r->in.req->hname[i]))
			r->convert_select_flags_pos = i;
		else if (0 == strcasecmp("FORCE-QUOTE-FLAGS", r->in.req->hname[i]))
			r->force_quote_flags = i;
		else if (0 == strcasecmp("NULL-PRINT-LEN", r->in.req->hname[i]))
			r->null_print_len = atoi(r->in.req->hvalue[i]);
		else if (0 == strcasecmp("NULL-PRINT", r->in.req->hname[i]))
		{
			if (NULL == r->in.req->hvalue[i])
			{
				gwarning(r, "reject invalid request from %s, invalid NULL PRINT", r->peer);
				http_error(r, FDIST_BAD_REQUEST, "invalid NULL PRINT length");
				request_end(r, 1, 0);
				return -1;
			}
			r->null_print = r->in.req->hvalue[i];
		}
		else if (0 == strcasecmp("FILE-ENCODING", r->in.req->hname[i]))
			r->file_encoding = atoi(r->in.req->hvalue[i]);
		else if (0 == strcasecmp("EXTRA-ERROR-OPTS", r->in.req->hname[i]))
			r->extra_error_opts = r->in.req->hvalue[i];
		else if (0 == strcasecmp("TIME-FORMAT-LENGTH", r->in.req->hname[i]))
			r->time_format_len = atoi(r->in.req->hvalue[i]);
		else if (0 == strcasecmp("TIMESTAMP-FORMAT-LENGTH", r->in.req->hname[i]))
			r->timestamp_format_len = atoi(r->in.req->hvalue[i]);
		else if (0 == strcasecmp("DATE-FORMAT-LENGTH", r->in.req->hname[i]))
			r->date_format_len = atoi(r->in.req->hvalue[i]);
		else if (0 == strcasecmp("TIMESTAMPTZ-FORMAT-LENGTH", r->in.req->hname[i]))
			r->timestamptz_formate_len = atoi(r->in.req->hvalue[i]);
		else if (0 == strcasecmp("TIME-FORMAT", r->in.req->hname[i]))
		{
			if (NULL == r->in.req->hvalue[i])
			{
				gwarning(r, "reject invalid request from %s, invalid TIME-FORMAT", r->peer);
				http_error(r, FDIST_BAD_REQUEST, "invalid TIME-FORMAT length");
				request_end(r, 1, 0);
				return -1;
			}
			r->time_format = r->in.req->hvalue[i];
			gprintln(NULL, "TIME-FORMAT = %s", r->time_format);
		}
		else if (0 == strcasecmp("TIMESTAMP-FORMAT", r->in.req->hname[i]))
		{
			if (NULL == r->in.req->hvalue[i])
			{
				gwarning(r, "reject invalid request from %s, invalid TIMESTAMP-FORMAT", r->peer);
				http_error(r, FDIST_BAD_REQUEST, "invalid TIMESTAMP-FORMAT length");
				request_end(r, 1, 0);
				return -1;
			}
			r->timestamp_format = r->in.req->hvalue[i];
			gprintln(NULL, "TIMESTAMP-FORMAT = %s", r->timestamp_format);
		}
		else if (0 == strcasecmp("DATE-FORMAT", r->in.req->hname[i]))
		{
			if (NULL == r->in.req->hvalue[i])
			{
				gwarning(r, "reject invalid request from %s, invalid DATE-FORMAT", r->peer);
				http_error(r, FDIST_BAD_REQUEST, "invalid DATE-FORMAT length");
				request_end(r, 1, 0);
				return -1;
			}
			r->date_format = r->in.req->hvalue[i];
		}
		else if (0 == strcasecmp("TIMESTAMPTZ-FORMAT", r->in.req->hname[i]))
		{
			if (NULL == r->in.req->hvalue[i])
			{
				gwarning(r, "reject invalid request from %s, invalid TIMESTAMPTZ-FORMAT", r->peer);
				http_error(r, FDIST_BAD_REQUEST, "invalid TIMESTAMPTZ-FORMAT length");
				request_end(r, 1, 0);
				return -1;
			}
			r->timestamptz_format = r->in.req->hvalue[i];
		}
		else if (0 == strcasecmp("OPENTENBASE-ORA-COMPATIBLE", r->in.req->hname[i]))
			r->opentenbase_ora_compatible = atoi(r->in.req->hvalue[i]);
		else if (0 == strcasecmp("TIMEZONE-STRING", r->in.req->hname[i]))
			r->timezone_string = r->in.req->hvalue[i];
		else if (0 == strcasecmp("REMOTE-DATA-PROTOCOL", r->in.req->hname[i]))
			r->remote_data_proto = r->in.req->hvalue[i];
		else if (0 == strcasecmp("COS-BUCKET", r->in.req->hname[i]))
			r->cos.bucket_name = r->in.req->hvalue[i];
		else if (0 == strcasecmp("KAFKA-TOPIC-ID", r->in.req->hname[i]))
			r->kafka.topic = r->in.req->hvalue[i];
		else if (0 == strcasecmp("KAFKA-BROKERS-ID", r->in.req->hname[i]))
			r->kafka.broker_id = r->in.req->hvalue[i];
		else if (0 == strcasecmp("KAFKA-GROUP-ID", r->in.req->hname[i]))
			r->kafka.group_id = r->in.req->hvalue[i];
		else if (0 == strcasecmp("PARTITION-CNT", r->in.req->hname[i]))
			r->kafka.partition_cnt = atoi(r->in.req->hvalue[i]);
		else if (0 == strcasecmp("PRT-OFFSET", r->in.req->hname[i]))
			r->kafka.offset_str = r->in.req->hvalue[i];
		else if (0 == strcasecmp("KAFKA-SEG-BATCH", r->in.req->hname[i]))
		{
			if (atol(r->in.req->hvalue[i]) > 0)
				r->kafka.r_max_messages = atol(r->in.req->hvalue[i]);
			else
				r->kafka.r_max_messages = 0;
		}
		// else if (0 == strcasecmp("KAFKA-TIMEOUT-MS", r->in.req->hname[i]))
		// {
		// 	if (atoi(r->in.req->hvalue[i]) > 0)
		// 		r->kafka.r_timeout_ms = atoi(r->in.req->hvalue[i]);
		// 	else
		// 		r->kafka.r_timeout_ms = 0;
		// }
		// else if (0 == strcasecmp("KAFKA-MAX-SIZE", r->in.req->hname[i]))
		// {
		// 	if (atol(r->in.req->hvalue[i]) > 0)
		// 		r->kafka.r_max_size = atol(r->in.req->hvalue[i]);
		// 	else
		// 		r->kafka.r_max_size = 0;
		// }
	}
	
	if (tdx_proto != NULL)
		r->tdx_proto = strtol(tdx_proto, 0, 0);
	
	if (tdx_proto == NULL || (r->tdx_proto != 0 && r->tdx_proto != 1))
	{
		if (tdx_proto == NULL)
		{
			gwarning(r, "reject invalid request from %s [%s %s] - no X-TDX-PROTO",
			         r->peer, r->in.req->argv[0], r->in.req->argv[1]);
			http_error(r, FDIST_BAD_REQUEST, "invalid request (no tdx-proto)");
		}
		else
		{
			gwarning(r, "reject invalid request from %s [%s %s] - X-TDX-PROTO invalid '%s'",
			         r->peer, r->in.req->argv[0], r->in.req->argv[1], tdx_proto);
			http_error(r, FDIST_BAD_REQUEST, "invalid request (invalid tdx-proto)");
		}
		
		request_end(r, 1, 0);
		return -1;
	}
	
	if (opt_g != -1) /* override? */
		r->tdx_proto = opt_g;
	
	if (xid && cid && sn)
	{
		r->tid = apr_psprintf(r->pool, "%s.%s.%s.%d", xid, cid, sn, r->tdx_proto);
	}
	else if (xid || cid || sn)
	{
		gwarning(r, "reject invalid request from %s [%s %s] - missing * header",
		         r->peer, r->in.req->argv[0], r->in.req->argv[1]);
		http_error(r, FDIST_BAD_REQUEST, "invalid request (missing * header)");
		request_end(r, 1, 0);
		return -1;
	}
	else
	{
		r->tid = apr_psprintf(r->pool, "auto-tid.%d", gcb.session.gen++);
	}
	
	return 0;
}


#ifdef GPFXDIST
static int request_set_transform(request_t *r)
{
	extern struct transform* transform_lookup(struct transform* trlist, const char* name, int for_write, int verbose);
	extern char* transform_command(struct transform* tr);
	extern int transform_stderr_server(struct transform* tr);
	extern int transform_content_paths(struct transform* tr);
	extern char* transform_safe(struct transform* tr);
	extern regex_t* transform_saferegex(struct transform* tr);

	struct transform* tr;
	char* safe;

	/*
	 * Requests involving transformations should have a #transform=name in the external
	 * table URL.  In Rio, GPDB moves the name into an TRANSFORM header.  However
	 * #transform= may still appear in the url in post requests.
	 *
	 * Note that ordinary HTTP clients and browsers do not typically transmit the portion
	 * of the URL after a #.  RFC 2396 calls this part the fragment identifier.
	 */

	char* param = "#transform=";
	char* start = strstr(r->path, param);
	if (start)
	{
		/*
		 * we have a transformation request encoded in the url
		 */
		*start = 0;
		if (! r->trans.name)
			r->trans.name = start + strlen(param);
	}

	if (! r->trans.name)
		return 0;

	/*
	 * at this point r->trans.name is the name of the transformation requested
	 * in the url and r->is_get tells us what kind (input or output) to look for.
	 * attempt to look it up.
	 */
	tr = transform_lookup(opt.trlist, r->trans.name, r->is_get ? 0 : 1, opt.V);
	if (! tr)
	{
		if (r->is_get)
		{
			gprintln(r, "reject invalid request from %s [%s %s] - unsppported input #transform",
					 r->peer, r->in.req->argv[0], r->in.req->argv[1]);
			http_error(r, FDIST_BAD_REQUEST, "invalid request (unsupported input #transform)");
		}
		else
		{
			gprintln(r, "reject invalid request from %s [%s %s] - unsppported output #transform",
					 r->peer, r->in.req->argv[0], r->in.req->argv[1]);
			http_error(r, FDIST_BAD_REQUEST, "invalid request (unsupported output #transform)");
		}
		request_end(r, 1, 0);
		return -1;
	}

	gprintln(r, "transform: %s", r->trans.name);

	/*
	 * propagate details for this transformation
	 */
	r->trans.command = transform_command(tr);
	r->trans.paths   = transform_content_paths(tr);

	/*
	 * if safe regex is specified, check that the path matches it
	 */
	safe = transform_safe(tr);
	if (safe)
	{
		regex_t* saferegex = transform_saferegex(tr);
		int rc = regexec(saferegex, r->path, 0, NULL, 0);
		if (rc)
		{
			char buf[1024];
			regerror(rc, saferegex, buf, sizeof(buf));

			gprintln(r, "reject invalid request from %s [%s %s] - path does not match safe regex %s: %s",
					 r->peer, r->in.req->argv[0], r->in.req->argv[1], safe, buf);
			http_error(r, FDIST_BAD_REQUEST, "invalid request (path does not match safe regex)");
			request_end(r, 1, 0);
			return -1;
		}
		else
		{
			gdebug(r, "[%d] safe regex %s matches %s", r->sock, safe, r->path);
		}
	}

	/*
	 * if we've been requested to send stderr output to the server,
	 * we prepare a temporary file to hold it.	when the request is
	 * done we'll forward the output as error messages.
	 */
	if (transform_stderr_server(tr))
	{
		apr_pool_t*	 mp = r->pool;
		apr_file_t*	 f = NULL;
		const char*	 tempdir = NULL;
		char*		 tempfilename = NULL;
		apr_status_t rv;

		if ((rv = apr_temp_dir_get(&tempdir, mp)) != APR_SUCCESS)
		{
			gprintln(r, "request failed from %s [%s %s] - failed to get temporary directory for stderr",
					 r->peer, r->in.req->argv[0], r->in.req->argv[1]);
			http_error(r, FDIST_INTERNAL_ERROR, "internal error");
			request_end(r, 1, 0);
			return -1;
		}

		tempfilename = apr_pstrcat(mp, tempdir, "/stderrXXXXXX", NULL);
		if ((rv = apr_file_mktemp(&f, tempfilename, APR_CREATE|APR_WRITE|APR_EXCL, mp)) != APR_SUCCESS)
		{
			gprintln(r, "request failed from %s [%s %s] - failed to create temporary file for stderr",
					 r->peer, r->in.req->argv[0], r->in.req->argv[1]);
			http_error(r, FDIST_INTERNAL_ERROR, "internal error");
			request_end(r, 1, 0);
			return -1;
		}

		gdebug(r, "[%d] request opened stderr file %s\n", r->sock, tempfilename);

		r->trans.errfilename = tempfilename;
		r->trans.errfile	 = f;
	}

	return 0;
}
#endif

/*
 * 1) get command line options from user
 * 2) setup internal memory pool, and signal handlers
 * 3) init event handler (libevent)
 * 4) create the requested HTTP port and start listening for requests.
 * 5) create the tdx log file and handle stderr/out redirection.
 * 6) sit and wait for an event.
 */
int tdx_init(int argc, const char *const argv[])
{
	struct event_base *base;
	char *wd;
	char *endptr;
	long val;

	if (0 != apr_app_initialize(&argc, &argv, 0))
		gfatal(NULL, "apr_app_initialize failed");
	atexit(apr_terminate);
	
	if (0 != apr_pool_create(&gcb.pool, 0))
		gfatal(NULL, "apr_app_initialize failed");
	
	//apr_signal_init(gcb.pool);
	
	gcb.session.tab = apr_hash_make(gcb.pool);
	
	parse_command_line(argc, argv, gcb.pool);

#ifndef WIN32
#ifdef SIGPIPE
	signal(SIGPIPE, SIG_IGN);
#endif
#endif
	/*
	 * apr_signal(SIGINT, process_signal);
     * apr_signal(SIGTERM, process_signal);
	 */
	if (opt.V)
	{
		putenv("EVENT_SHOW_METHOD=1");
		event_enable_debug_mode();
	}
	putenv("EVENT_NOKQUEUE=1");
	
	evthread_use_pthreads();
	base = event_init();
	evthread_make_base_notifiable(base);
	
	signal_register();
	http_setup();

#ifdef USE_SSL
	if (opt.ssl)
		printf("Serving HTTPS on port %d, directory %s\n", opt.p, opt.d);
	else
		printf("Serving HTTP on port %d, directory %s\n", opt.p, opt.d);
#else
	printf("Serving HTTP on port %d, directory %s\n", opt.p, opt.d);
#endif
	
	fflush(stdout);
	
	/* redirect stderr and stdout to log */
	if (opt.l)
	{
		FILE *f_stderr;
		FILE *f_stdout;
		
		f_stderr = freopen(opt.l, "a", stderr);
		if (f_stderr == NULL)
		{
			fprintf(stderr, "failed to redirect stderr to log: %s\n", strerror(errno));
			return -1;
		}
#ifndef WIN32
		setlinebuf(stderr);
#endif
		f_stdout = freopen(opt.l, "a", stdout);
		if (f_stdout == NULL)
		{
			fprintf(stderr, "failed to redirect stdout to log: %s\n", strerror(errno));
			return -1;
		}
#ifndef WIN32
		setlinebuf(stdout);
#endif
	}
	
	/*
	 * must identify errors in calls above and return non-zero for them
	 * behaviour required for the Windows service case
	 */

#ifndef WIN32
	wd = getenv("TDX_WATCHDOG_TIMER");
	if (wd != NULL)
	{
		errno = 0;
		val = strtol(wd, &endptr, 10);
		
		if (errno || endptr == wd || val > INT_MAX)
		{
			fprintf(stderr, "incorrect watchdog timer: %s\n", strerror(errno));
			return -1;
		}
		
		gcb.wdtimer = (int) val;
		if (gcb.wdtimer > 0)
		{
			static pthread_t watchdog;

			gprintln(NULL, "Watchdog enabled, abort in %d seconds if no activity", gcb.wdtimer);
			shutdown_time = apr_time_now() + gcb.wdtimer * APR_USEC_PER_SEC;
			pthread_create(&watchdog, 0, watchdog_thread, 0);
		}
	}
#endif
	return 0;
}

int tdx_run()
{
	return event_dispatch();
}

#ifndef WIN32
#define QUEUE  256

int main(int argc, const char *const argv[])
{
	TDX_ThreadInfo *thrinfo = (TDX_ThreadInfo *)malloc(sizeof (TDX_ThreadInfo));
	pthread_key_create(&threadinfo_key, NULL);
	memset(thrinfo, 0, sizeof(TDX_ThreadInfo));
	thrinfo->thr_id = (pthread_t) pthread_self();
	SetTDXThreadInfo(thrinfo);
	
	progname = get_progname(argv[0]);
	exename = strdup(argv[0]);
	if (find_my_exec(argv[0], my_exec_path) < 0)
		gfatal(NULL, "%s: could not locate my own executable path", progname);
	gprintln(NULL, "tdx executable path '%s'", my_exec_path);
	
	MemoryContextInit();
	if (tdx_init(argc, argv) == -1)
		gfatal(NULL, "Initialization failed");
	threadpool = threadpool_create(opt.thread_pool_size, QUEUE, 0);

	return tdx_run();
}


#else   /* in Windows tdx may run as a Windows service or a console application  */


SERVICE_STATUS          ServiceStatus;
SERVICE_STATUS_HANDLE   hStatus;

#define CMD_LINE_ARG_MAX_SIZE 1000
#define CMD_LINE_ARG_SIZE 500
#define CMD_LINE_ARG_NUM 40
char* cmd_line_buffer[CMD_LINE_ARG_NUM];
int cmd_line_args;

void  ServiceMain(int argc, char** argv);
void  ControlHandler(DWORD request);


/*
 * tdx service registration on the WINDOWS command line
 * sc create tdx binpath= "c:\temp\tdx.exe param1 param2 param3"
 * sc delete tdx
 */

/* HELPERS - START */
void report_event(LPCTSTR _error_msg)
{
	HANDLE hEventSource;
	LPCTSTR lpszStrings[2];
	TCHAR Buffer[100];

	hEventSource = RegisterEventSource(NULL, TEXT("tdx"));

	if( NULL != hEventSource )
	{
		memcpy(Buffer, _error_msg, 100);

		lpszStrings[0] = TEXT("tdx");
		lpszStrings[1] = Buffer;

		ReportEvent(hEventSource,			/* event log handle */
					EVENTLOG_ERROR_TYPE, 	/* event type */
					0,						/* event category */
					((DWORD)0xC0020100L),	/* event identifier */
					NULL,					/* no security identifier */
					2,						/* size of lpszStrings array */
					0,						/* no binary data */
					lpszStrings,			/* array of strings */
					NULL);					/* no binary data */

		DeregisterEventSource(hEventSource);
	}
}

int verify_buf_size(char** pBuf, const char* _in_val)
{
	int val_len, new_len;
	char *p;

	val_len = (int)strlen(_in_val);
	if (val_len >= CMD_LINE_ARG_SIZE)
	{
		new_len = ((val_len+1) >= CMD_LINE_ARG_MAX_SIZE) ? CMD_LINE_ARG_MAX_SIZE : (val_len+1);
		p = realloc(*pBuf, new_len);
		if (p == NULL)
			return 0;
		*pBuf = p;
		memset(*pBuf, 0, new_len);
	}
	else
	{
		new_len = val_len;
	}

	return new_len;
}

void init_cmd_buffer(int argc, const char* const argv[])
{
	int i;
	/* 1. initialize command line params buffer*/
	for (i = 0; i < CMD_LINE_ARG_NUM; i++)
	{
		cmd_line_buffer[i] = (char*)malloc(CMD_LINE_ARG_SIZE);
		if (cmd_line_buffer[i] == NULL)
			gfatal(NULL, "Out of memory");
		memset(cmd_line_buffer[i], 0, CMD_LINE_ARG_SIZE);
	}

	/*
	 * 2. the number of variables cannot be higher than a
	 *    a predifined const, that is because - down the line
	 *    this values get to a const buffer whose size is
	 *    defined at compile time
	 */
	cmd_line_args = (argc <= CMD_LINE_ARG_NUM) ? argc : CMD_LINE_ARG_NUM;
	if (argc > CMD_LINE_ARG_NUM)
	{
		char msg[200] = {0};
		sprintf(msg, "too many parameters - maximum allowed: %d.", CMD_LINE_ARG_NUM);
		report_event(TEXT("msg"));
	}

	for (i = 0; i < cmd_line_args; i++)
	{
		int len;
		len = verify_buf_size(&cmd_line_buffer[i], argv[i]);
		if (!len)
			gfatal(NULL, "Out of memory");
		memcpy(cmd_line_buffer[i], argv[i], len);
	}
}

void clean_cmd_buffer()
{
	int i;
	for (i = 0; i < CMD_LINE_ARG_NUM; i++)
	{
		free(cmd_line_buffer[i]);
	}
}

void init_service_status()
{
	ServiceStatus.dwServiceType = SERVICE_WIN32;
	ServiceStatus.dwCurrentState = SERVICE_START_PENDING;
	ServiceStatus.dwControlsAccepted   =  SERVICE_ACCEPT_STOP | SERVICE_ACCEPT_SHUTDOWN;
	ServiceStatus.dwWin32ExitCode = 0;
	ServiceStatus.dwServiceSpecificExitCode = 0;
	ServiceStatus.dwCheckPoint = 0;
	ServiceStatus.dwWaitHint = 0;
}

void do_set_srv_status(DWORD _currentState, DWORD _exitCode)
{
	ServiceStatus.dwCurrentState = _currentState;
	ServiceStatus.dwWin32ExitCode = _exitCode;
	SetServiceStatus(hStatus, &ServiceStatus);
}

void init_services_table(SERVICE_TABLE_ENTRY* ServiceTable)
{
	ServiceTable[0].lpServiceName = (LPSTR)"tdx";
	ServiceTable[0].lpServiceProc = (LPSERVICE_MAIN_FUNCTIONA)ServiceMain;
	ServiceTable[1].lpServiceName = (LPSTR)NULL;
	ServiceTable[1].lpServiceProc = (LPSERVICE_MAIN_FUNCTIONA)NULL;

}
/* HELPERS - STOP */

/* for Windows */
int main(int argc, const char* const argv[])
{
	int main_ret = 0, srv_ret;

	/* command line parameters transfer to a global buffer for ServiceMain */
	init_cmd_buffer(argc, argv);

	/* services table init */
	SERVICE_TABLE_ENTRY ServiceTable[2];
	init_services_table(ServiceTable);

	/* start the control dispatcher thread for our service */
	srv_ret = StartServiceCtrlDispatcher(ServiceTable);
	if (0 == srv_ret) /* program is being run as a Windows console application */
	{
		if (tdx_init(argc, argv) == -1)
			gfatal(NULL, "Initialization failed");
		main_ret = tdx_run();
	}

	return main_ret;
}

void ServiceMain(int argc, char** argv)
{
	int error = 0;
	init_service_status();

	hStatus = RegisterServiceCtrlHandler((LPCSTR)"tdx", (LPHANDLER_FUNCTION)ControlHandler);
	if (hStatus == (SERVICE_STATUS_HANDLE)0)
	{
		/* Registering Control Handler failed */
		return;
	}
	/*
	 * Initialize Service
	 * If we don't pass a const char* const [], to tdx_init
	 * we will get a warning that will fail the build
	 */
	const char* const buf[CMD_LINE_ARG_NUM] = {
		cmd_line_buffer[0],
		cmd_line_buffer[1],
		cmd_line_buffer[2],
		cmd_line_buffer[3],
		cmd_line_buffer[4],
		cmd_line_buffer[5],
		cmd_line_buffer[6],
		cmd_line_buffer[7],
		cmd_line_buffer[8],
		cmd_line_buffer[9],
		cmd_line_buffer[10],
		cmd_line_buffer[11],
		cmd_line_buffer[12],
		cmd_line_buffer[13],
		cmd_line_buffer[14],
		cmd_line_buffer[15],
		cmd_line_buffer[16],
		cmd_line_buffer[17],
		cmd_line_buffer[18],
		cmd_line_buffer[19],
		cmd_line_buffer[20],
		cmd_line_buffer[21],
		cmd_line_buffer[22],
		cmd_line_buffer[23],
		cmd_line_buffer[24],
		cmd_line_buffer[25],
		cmd_line_buffer[26],
		cmd_line_buffer[27],
		cmd_line_buffer[28],
		cmd_line_buffer[29],
		cmd_line_buffer[30],
		cmd_line_buffer[31],
		cmd_line_buffer[32],
		cmd_line_buffer[33],
		cmd_line_buffer[34],
		cmd_line_buffer[35],
		cmd_line_buffer[36],
		cmd_line_buffer[37],
		cmd_line_buffer[38],
		cmd_line_buffer[39]
	};
	error = tdx_init(cmd_line_args, buf);
	if (error != 0)
	{
		/* Initialization failed */
		do_set_srv_status(SERVICE_STOPPED, -1);
		return;
	}
	else
	{
		do_set_srv_status(SERVICE_RUNNING, 0);
	}

	/* free the command line arguments buffer - it's not used anymore */
	clean_cmd_buffer();

	/* actual service work */
	tdx_run();
}

void ControlHandler(DWORD request)
{
	switch(request)
	{
		case SERVICE_CONTROL_STOP:
		case SERVICE_CONTROL_SHUTDOWN:
		{
			do_set_srv_status(SERVICE_STOPPED, 0);
			return;
		}

		default:
			break;
	}

	/* Report current status */
	do_set_srv_status(SERVICE_RUNNING, 0);
}
#endif

#define find_max(a, b) ((a) >= (b) ? (a) : (b))

#ifdef USE_SSL

static SSL_CTX *initialize_ctx(void)
{
	int stringSize;
	char *fileName, slash;
	SSL_CTX *ctx;
	
	if (!gcb.bio_err)
	{
		/* Global system initialization*/
		SSL_library_init();
		SSL_load_error_strings();
		/* An error write context */
		gcb.bio_err = BIO_new_fp(stderr, BIO_NOCLOSE);
	}
	
	/* Create our context*/
	ctx = SSL_CTX_new(SSLv23_server_method());
	
	/* Generate random seed */
	if (RAND_poll() == 0)
		gfatal(NULL, "Can't generate random seed for SSL");
	
	/*
	 * The size of the string will consist of the path and the filename (the
	 * longest one)
	 * +1 for the '/' character (/filename)
	 * +1 for the \0
	 */
	stringSize = find_max(strlen(CertificateFilename),
	                      find_max(strlen(PrivateKeyFilename), strlen(TrustedCaFilename))) + strlen(opt.ssl) + 2;
	/* Allocate the memory for the file name */
	fileName = (char *) calloc((stringSize), sizeof(char));
	if (fileName == NULL)
		gfatal(NULL, "Unable to allocate memory for SSL initialization");

#ifdef WIN32
	slash = '\\';
#else
	slash = '/';
#endif
	
	/* Copy the path + the filename */
	snprintf(fileName, stringSize, "%s%c%s", opt.ssl, slash, CertificateFilename);
	
	/* Load our keys and certificates*/
	if (!(SSL_CTX_use_certificate_chain_file(ctx, fileName)))
	{
		gfatal(NULL, "Unable to load the certificate from file: \"%s\"", fileName);
	}
	else
	{
		if (opt.v)
		{
			gprint(NULL, "The certificate was successfully loaded from \"%s\"\n", fileName);
		}
	}
	
	/* Copy the path + the filename */
	snprintf(fileName, stringSize, "%s%c%s", opt.ssl, slash, PrivateKeyFilename);
	
	if (!(SSL_CTX_use_PrivateKey_file(ctx, fileName, SSL_FILETYPE_PEM)))
	{
		gfatal(NULL, "Unable to load the private key from file: \"%s\"", fileName);
	}
	else
	{
		if (opt.v)
		{
			gprint(NULL, "The private key was successfully loaded from \"%s\"\n", fileName);
		}
	}
	
	/* Copy the path + the filename */
	snprintf(fileName, stringSize, "%s%c%s", opt.ssl, slash, TrustedCaFilename);
	
	/* Load the CAs we trust*/
	if (!(SSL_CTX_load_verify_locations(ctx, fileName, 0)))
	{
		gfatal(NULL, "Unable to to load CA from file: \"%s\"", fileName);
	}
	else
	{
		if (opt.v)
		{
			gprint(NULL, "The CA file successfully loaded from \"%s\"\n", fileName);
		}
	}
	
	/* Set the verification flags for ctx 	*/
	/* We always require client certificate	*/
	SSL_CTX_set_verify(ctx, SSL_VERIFY_PEER | SSL_VERIFY_FAIL_IF_NO_PEER_CERT, 0);
	
	/* Consider using these - experinments on Mac showed no improvement,
	 * but perhaps it will on other platforms, or when opt.m is very big
	 */
	//SSL_CTX_set_mode(ctx, SSL_MODE_AUTO_RETRY | SSL_MODE_ENABLE_PARTIAL_WRITE);
	
	free(fileName);
	return ctx;
}

#endif

/* 
 * "1,3:6|1,5:9|0,0:40" -> pos
 *
 * transform position info and record it in pos, and return true if success
 */
static bool
transform_position_info(const apr_pool_t *pool,
						const char *col_pos_str,
						int col_num,
						PositionTDX **pos)
{
	int value;
	int col_num_idx = 0;
	char *str = apr_pstrdup((apr_pool_t *) pool, col_pos_str);
	char *token;
	const char *token_ptr;

	/* split col_pos_str into tokens, decode them and do some checks */
	token = strtok(str, "|");
	while (token && (col_num_idx < col_num))
	{
		token_ptr = token;

		/* position type */
		Assert(isdigit((unsigned char) *token));
		value = atoi(token_ptr);
		if (value != ABSOLUTE_POS && value != RELATIVE_POS)
			return false;
		pos[col_num_idx]->flag = value;

		while (isdigit((unsigned char) *token_ptr))
			token_ptr += 1;
		Assert(*token_ptr == ',');
		token_ptr += 1;

		/* start offset */
		Assert(isdigit((unsigned char) *token));
		value = atoi(token_ptr);
		/* check whether start offset is valid */
		if ((pos[col_num_idx]->flag == ABSOLUTE_POS && value < 1) ||
			(pos[col_num_idx]->flag == RELATIVE_POS && value != 0))
			return false;
		pos[col_num_idx]->start_offset = value;

		while (isdigit((unsigned char) *token_ptr))
			token_ptr += 1;
		Assert(*token_ptr == ':');
		token_ptr += 1;

		/* end offset */
		Assert(isdigit((unsigned char) *token));
		value = atoi(token_ptr);
		/* check whether end offset is valid */
		if ((pos[col_num_idx]->flag == ABSOLUTE_POS &&
			 value < pos[col_num_idx]->start_offset))
			return false;
		else if (pos[col_num_idx]->flag == RELATIVE_POS && 
				 ((col_num_idx == 0 && value < 1) ||
				  (col_num_idx != 0 &&
				   value <= pos[col_num_idx - 1]->end_offset)))
			return false;
		pos[col_num_idx]->end_offset = value;

		while (isdigit((unsigned char) *token_ptr))
			token_ptr += 1;
		col_num_idx += 1;
		token = strtok(NULL, "|");
	}

	/* check whether all columns contains position info */
	if (col_num_idx != col_num)
		return false;

	return true;
}

/*
 * Sends the requested buf, of size buflen to the network
 * via appropriate socket
 */
static int tdx_socket_send(const request_t *r, const void *buf, const size_t buflen)
{
	return send(r->sock, buf, buflen, 0);
}

#ifdef USE_SSL

/*
 * tdx_SSL_send
 *
 * Sends the requested buf, of size len to the network via SSL
 */
static int tdx_SSL_send(const request_t *r, const void *buf, const size_t buflen)
{
	
	/* Write the data to socket */
	int n = BIO_write(r->io, buf, buflen);
	/* Try to flush */
	(void) BIO_flush(r->io);
	
	/* If we could not write to BIO */
	if (n < 0)
	{
		/* If BIO indicates retry => we should retry later, this is not an error */
		if (BIO_should_retry(r->io) > 1)
		{
			/* Do not indicate error */
			n = 0;
		}
		else
		{
			/* If errno == 0 => this is not a real error */
			if (errno == 0)
			{
				/* Do not indicate error */
				n = 0;
			}
			else
			{
				/* If errno == EPIPE, it means that the client has closed the connection   	*/
				/* This error will be handled in the calling function, do not print it here	*/
				if (errno != EPIPE)
				{
					gwarning(r,
					         "Error during SSL tdx_send (Error = %d. errno = %d)",
					         SSL_get_error(r->ssl, n),
					         (int) errno);
					ERR_print_errors(gcb.bio_err);
				}
			}
		}
	}
	
	return n;
}

#endif


/* read from a socket */
static int tdx_socket_receive(const request_t *r, void *buf, const size_t buflen)
{
	return (recv(r->sock, buf, buflen, 0));
}

/* Shutdown request socket transmission */
static void request_shutdown_sock(const request_t *r)
{
	int ret = shutdown(r->sock, SHUT_WR);
	if (ret == 0)
	{
		gprintlnif(r, "successfully shutdown socket");
	}
	else
	{
		gprintln(r, "failed to shutdown socket, errno: %d, msg: %s", errno, strerror(errno));
	}
}

#ifdef USE_SSL

/*
 * tdx_SSL_receive
 *
 * read from an SSL socket
 */
static int tdx_SSL_receive(const request_t *r, void *buf, const size_t buflen)
{
	return (BIO_read(r->io, buf, buflen));
	/* todo: add error checks here */
}

/*
 * free_SSL_resources
 *
 * Frees all SSL resources that were allocated per request r
 */
static void free_SSL_resources(const request_t *r)
{
	//send close_notify to client
	SSL_shutdown(r->ssl);  //or BIO_ssl_shutdown(r->ssl_bio);
	
	request_shutdown_sock(r);
	
	BIO_vfree(r->io);  //ssl_bio is pushed to r->io list, so ssl_bio is freed too.
	BIO_vfree(r->sbio);
	//BIO_vfree(r->ssl_bio);
	SSL_free(r->ssl);
}

/* frees SSL resources that were allocated during do_accept */
static void handle_ssl_error(SOCKET sock, BIO *sbio, SSL *ssl)
{
	gwarning(NULL, "SSL accept failed");
	if (opt.v)
	{
		ERR_print_errors(gcb.bio_err);
	}
	
	SSL_shutdown(ssl);
	SSL_free(ssl);
}

/* flush all the data that is still pending in the current buffer */
static void flush_ssl_buffer(int fd, short event, void *arg)
{
	request_t *r = (request_t *) arg;
	
	(void) BIO_flush(r->io);
	
	if (event & EV_TIMEOUT)
	{
		gwarning(r, "Buffer flush timeout");
	}
	
	if (BIO_wpending(r->io))
	{
		setup_flush_ssl_buffer(r);
	}
	else
	{
		/* do ssl cleanup immediately */
		request_cleanup_and_free_SSL_resources(r);
	}
}


/* create event that will call to 'flush_ssl_buffer', with 5 seconds timeout */
static void setup_flush_ssl_buffer(request_t *r)
{
	event_del(&r->ev);
	event_set(&r->ev, r->sock, EV_WRITE, flush_ssl_buffer, r);
	r->tm.tv_sec = 5;
	r->tm.tv_usec = 0;
	(void) event_add(&r->ev, &r->tm);
}

#endif

/* log unsent/unacked bytes in socket buffer */
static int get_unsent_bytes(request_t *r)
{
	int unsent_bytes = -1;
#ifdef __linux__
	int ret = ioctl(r->sock, TIOCOUTQ, &unsent_bytes);
	if (ret < 0)
	{
		gwarning(r, "failed to use ioctl to get unsent bytes");
	}
#endif
	return unsent_bytes;
}

static void log_unsent_bytes(request_t *r)
{
	gprintlnif(r, "unsent bytes: %d (-1 means not supported)", get_unsent_bytes(r));
}

/* call close after timeout or EV_READ ready */
static void do_close(int fd, short event, void *arg)
{
	request_t *r = (request_t *) arg;
	char buffer[256] = {0};
	int ret;
	
	if (event & EV_TIMEOUT)
	{
		gwarning(r, "tdx shutdown the connection, while have not received response from datanode");
	}
	
	ret = recv(r->sock, buffer, sizeof(buffer) - 1, 0);
	if (ret < 0)
	{
		int e;
		bool should_retry;

		gwarning(r, "tdx read error after shutdown. errno: %d, msg: %s", errno, strerror(errno));

#ifdef WIN32
		e = WSAGetLastError();
		should_retry = (e == WSAEINTR || e == WSAEWOULDBLOCK);
#else
		e = errno;
		should_retry = (e == EINTR || e == EAGAIN);
#endif
		if (should_retry)
		{
			setup_do_close(r);
			return;
		}
	}
	else if (ret == 0)
	{
		gprintlnif(r, "peer closed after tdx shutdown");
	}
	else
	{
		gwarning(r, "tdx read unexpected data after shutdown %s", buffer);
	}
	
	log_unsent_bytes(r);
	
	ret = closesocket(r->sock);
	if (ret == 0)
	{
		gprintlnif(r, "successfully closed socket");
	}
	else
	{
		gwarning(r, "failed to close socket. errno: %d, msg: %s", errno, strerror(errno));
	}
	
	event_del(&r->ev);
	r->sock = -1;
	apr_pool_destroy(r->pool);
	
	fflush(stdout);
}

/* cleanup request related resources */
static void request_cleanup(request_t *r)
{
	gprint(r, "cleanup request related resources\n");
	request_shutdown_sock(r);
	setup_do_close(r);
}

static void setup_do_close(request_t *r)
{
	event_del(&r->ev);
	event_set(&r->ev, r->sock, EV_READ, do_close, r);
	
	r->tm.tv_sec = 60;
	r->tm.tv_usec = 0;
	if (0 != event_add(&r->ev, &r->tm))
		gfatal(r, "failed to event_add!");
}


#ifdef USE_SSL

/*
 * request_cleanup_and_free_SSL_resources
 *
 */
static void request_cleanup_and_free_SSL_resources(request_t *r)
{
	gprintln(r, "SSL cleanup and free");
	
	/* Clean up request resources */
	setup_do_close(r);
	
	/* Shutdown SSL gracefully and Release SSL related memory */
	free_SSL_resources(r);
}

#endif

/* the callback function of session timer */
static void free_session_cb(int fd, short event, void *arg)
{
	session_t *session = (session_t *) arg;
	/*
	 * free the session if there's no POST request from other
	 * segments since the timer get started.
	 */
	if (!session->is_get &&
	    session->nrequest == 0 &&
	    session_active_segs_isempty(session))
	{
		session_free(session);
	}
}

static void *
palloc_safe(request_t *r, apr_pool_t *pool, apr_size_t size, const char *fmt, ...)
{
	void *result = apr_palloc(pool, size);
	if (result == NULL)
	{
		va_list args;
		va_start(args, fmt);
		_gprint(r, "FATAL", fmt, args);
		va_end(args);
		exit(1);
	}
	
	return result;
}

static void *
pcalloc_safe(request_t *r, apr_pool_t *pool, apr_size_t size, const char *fmt, ...)
{
	void *result = apr_pcalloc(pool, size);
	if (result == NULL)
	{
		va_list args;
		va_start(args, fmt);
		_gprint(r, "FATAL", fmt, args);
		va_end(args);
		exit(1);
	}
	
	return result;
}

/* Return true if a multibyte character is truncated. */
static bool
CheckIfTruncMbCharTDX(int file_encoding, const char *start_ptr,
					  const char *cur_mb_ptr, char *last_mb_char)
{
	int mb_len = 0;

	memset(last_mb_char, 0, MAX_LEN);
	for (;;)
	{
		if (cur_mb_ptr > start_ptr)
			return true;
		else if (cur_mb_ptr == start_ptr)
			return false;

		mb_len = pg_encoding_mblen(file_encoding, cur_mb_ptr);
		if (mb_len > 1)
		{
			strncpy(last_mb_char, cur_mb_ptr, mb_len);
			last_mb_char[mb_len] = '\0';
		}
		cur_mb_ptr += mb_len;
	}

	return false;
}

#ifndef WIN32

static void *watchdog_thread(void *p)
{
	apr_time_t duration;
	
	do
	{
		/* apr_time_now is defined in microseconds since epoch */
		duration = apr_time_sec(shutdown_time - apr_time_now());
		if (duration > 0)
			(void) sleep(duration);
	} while (apr_time_now() < shutdown_time);
	gprintln(NULL, "Watchdog timer expired, abort tdx");
	abort();
}

static void delay_watchdog_timer()
{
	if (gcb.wdtimer > 0)
	{
		shutdown_time = apr_time_now() + gcb.wdtimer * APR_USEC_PER_SEC;
	}
}

#else
static void delay_watchdog_timer()
{
}
#endif

/* Return decimal value for a hexadecimal digit */
static int
GetDecimalFromHex(char hex)
{
	if (isdigit((unsigned char) hex))
		return hex - '0';
	else
		return tolower((unsigned char) hex) - 'a' + 10;
}

#define ISOCTAL(c) (((c) >= '0') && ((c) <= '7'))
#define OCTVALUE(c) ((c) - '0')


/*
 * Parse the current line into separate attributes (fields),
 * performing de-escaping as needed.
 *
 * The input is in line_buf->  We use attribute_buf to hold the result
 * strings.  raw_fields[k] is set to point to the k'th attribute
 * string, or NULL when the input matches the null marker string.
 * This array is expanded as necessary.
 *
 * (Note that the caller cannot check for nulls since the returned
 * string would be the post-de-escaping equivalent, which may look
 * the same as some valid data string.)
 *
 * delim is the column delimiter string (must be just one byte for now).
 * null_print is the null marker string.  Note that this is compared to
 * the pre-de-escaped input string.
 *
 * The return value is the number of fields actually read.
 */
static int
ReadAttributesText(session_t *session, StringInfo line_buf, StringInfo attribute_buf, char ***raw_fields_ptr, Bitmapset *dis_bitmap)
{
	copy_options *cpara = session->copy_options;
	char escapec = cpara->escape;
	int fieldno;
	char *output_ptr;
	char *cur_ptr;
	char *line_end_ptr;
	int fldct = session->fields_num;
	char **raw_fields = *raw_fields_ptr;
	pthread_mutex_t *err_mutex = &session->err_mutex;
	
	/*
	 * We need a special case for zero-column tables: check that the input
	 * line is empty, and return.
	 */
	if (fldct <= 0)
	{
		if (line_buf->len != 0)
		{
			gdebug(NULL, "extra data after last expected column, linebuf \"%s\"", line_buf->data);
			pthread_mutex_lock(err_mutex);
			if (!session->is_error)
			{
				session->errmsg = apr_psprintf(session->pool, "extra data after last expected column");
				session->is_error = 1;
				gdebug(NULL, "%s", session->errmsg);
			}
			pthread_mutex_unlock(err_mutex);
			return -1;
		}
		return 0;
	}
	
	resetStringInfo(attribute_buf);

	
	/*
	 * The de-escaped attributes will certainly not be longer than the input
	 * data line, so we can just force attribute_buf to be large enough and
	 * then transfer data without any checks for enough space.  We need to do
	 * it this way because enlarging attribute_buf mid-stream would invalidate
	 * pointers already stored into raw_fields[].
	 */
	if (attribute_buf->maxlen <= line_buf->len)
		enlargeStringInfo(attribute_buf, line_buf->len);
	output_ptr = attribute_buf->data;
	
	/* set pointer variables for loop */
	cur_ptr = line_buf->data;
	line_end_ptr = line_buf->data + line_buf->len;
	
	/* Outer loop iterates over fields */
	fieldno = 0;
	for (;;)
	{
		bool found_delim = false;
		char *start_ptr;
		char *end_ptr;
		int input_len;
		bool saw_non_ascii = false;
		
		/* Make sure there is enough space for the next value */
		if (fieldno >= fldct)
		{
			fldct *= 2;
			raw_fields = repalloc(raw_fields, fldct * sizeof(char *));
		}
		
		/* Remember start of field on both input and output sides */
		start_ptr = cur_ptr;
		raw_fields[fieldno] = output_ptr;
		
		/*
		 * Scan data for field.
		 *
		 * Note that in this loop, we are scanning to locate the end of field
		 * and also speculatively performing de-escaping.  Once we find the
		 * end-of-field, we can match the raw field contents against the null
		 * marker string.  Only after that comparison fails do we know that
		 * de-escaping is actually the right thing to do; therefore we *must
		 * not* throw any syntax errors before we've done the null-marker
		 * check.
		 */
		for (;;)
		{
			char c;
			
			end_ptr = cur_ptr;
			if (cur_ptr >= line_end_ptr)
				break;
			c = *cur_ptr++;
			if (strncmp(cpara->delim, cur_ptr - 1, cpara->delim_len) == 0)
			{
				cur_ptr += (cpara->delim_len - 1);
				found_delim = true;
				break;
			}
			if (c == escapec && !cpara->escape_off)
			{
				if (cur_ptr >= line_end_ptr)
					break;
				c = *cur_ptr++;
				switch (c)
				{
					case '0':
					case '1':
					case '2':
					case '3':
					case '4':
					case '5':
					case '6':
					case '7':
					{
						/* handle \013 */
						int val;
						
						val = OCTVALUE(c);
						if (cur_ptr < line_end_ptr)
						{
							c = *cur_ptr;
							if (ISOCTAL(c))
							{
								cur_ptr++;
								val = (val << 3) + OCTVALUE(c);
								if (cur_ptr < line_end_ptr)
								{
									c = *cur_ptr;
									if (ISOCTAL(c))
									{
										cur_ptr++;
										val = (val << 3) + OCTVALUE(c);
									}
								}
							}
						}
						c = val & 0377;
						if (c == '\0' || IS_HIGHBIT_SET(c))
							saw_non_ascii = true;
					}
						break;
					case 'x':
						/* Handle \x3F */
						if (cur_ptr < line_end_ptr)
						{
							char hexchar = *cur_ptr;
							
							if (isxdigit((unsigned char)hexchar))
							{
								int val = GetDecimalFromHex(hexchar);
								
								cur_ptr++;
								if (cur_ptr < line_end_ptr)
								{
									hexchar = *cur_ptr;
									if (isxdigit((unsigned char)hexchar))
									{
										cur_ptr++;
										val = (val << 4) + GetDecimalFromHex(hexchar);
									}
								}
								c = val & 0xff;
								if (c == '\0' || IS_HIGHBIT_SET(c))
									saw_non_ascii = true;
							}
						}
						break;
					case 'b':
						c = '\b';
						break;
					case 'f':
						c = '\f';
						break;
					case 'n':
						c = '\n';
						break;
					case 'r':
						c = '\r';
						break;
					case 't':
						c = '\t';
						break;
					case 'v':
						c = '\v';
						break;
						
						/*
							 * in all other cases, take the char after '\'
							 * literally
							 */
				}
			}
			
			/* Add c to output string */
			*output_ptr++ = c;
		}
		
		/* Check whether raw input matched null marker */
		input_len = end_ptr - start_ptr;
		if (input_len == cpara->null_print_len &&
		    strncmp(start_ptr, cpara->null_print, input_len) == 0)
			raw_fields[fieldno] = NULL;
		else if (bms_is_member(fieldno, dis_bitmap))
		{
			/*
			 * At this point we know the field is supposed to contain data.
			 *
			 * If we de-escaped any non-7-bit-ASCII chars, make sure the
			 * resulting string is valid data for the db encoding.
			 * 
			 * If it is not a distribution column, defer character set checking.
			 */
			if (saw_non_ascii)
			{
				char *fld = raw_fields[fieldno];
				PG_TRY();
				{
					pg_verifymbstr(fld, output_ptr - fld, false);
				}
				PG_CATCH();
				{
					pthread_mutex_lock(err_mutex);
					if (!session->is_error)
					{
						ErrorData *edata = CopyErrorData();
						session->errmsg = apr_psprintf(session->pool, "%s", edata->message);
						session->is_error = 1;
					}
					pthread_mutex_unlock(err_mutex);
					FlushErrorState();
					if (*raw_fields_ptr != raw_fields)
						*raw_fields_ptr = raw_fields;
					return -1;
				}
				PG_END_TRY();
			}
		}
		
		/* Terminate attribute value in output area */
		*output_ptr++ = '\0';
		
		fieldno++;
		
		/* Done if we hit EOL instead of a delim */
		if (!found_delim)
			break;
	}
	
	/* Clean up state of attribute_buf */
	output_ptr--;
	Assert(*output_ptr == '\0');
	attribute_buf->len = (output_ptr - attribute_buf->data);
	
	if (*raw_fields_ptr != raw_fields)
		*raw_fields_ptr = raw_fields;
	return fieldno;
}

/*
 * Parse the current line into separate attributes (fields).
 *
 * Below are the differences from ReadAttributesText():
 *  (1) ignore the designated delimiter;
 *  (2) ignore other options (TODO: perhaps should support later if necessary);
 *
 * The return value is the number of fields actually read.
 */
static int
ReadAttributesPos(session_t *session,
				  StringInfo line_buf,
				  StringInfo attribute_buf,
				  char ***raw_fields_ptr)
{
	int i;
	int file_encoding = session->copy_options->file_encoding;
	char *output_ptr;
	const char *cur_ptr;
	const char *line_end_ptr;
	char **raw_fields = *raw_fields_ptr;
	pthread_mutex_t *err_mutex = &session->err_mutex;
	PositionTDX *pos;
	bool has_null = false;		/* a line has a NULL data */
	/* for error message, if a multibyte char finished before end_ptr */
	int mb_left_bytes = 0;
	char last_mb_char1[MAX_LEN];
	char last_mb_char2[MAX_LEN];

	/*
	 * We need a special case for zero-column tables: check that the input
	 * line is empty, and return.
	 */
	if (session->fields_num <= 0)
	{
		if (line_buf->len != 0)
		{
			gdebug(NULL, "extra data after last expected column, linebuf \"%s\"", line_buf->data);
			pthread_mutex_lock(err_mutex);
			if (!session->is_error)
			{
				session->errmsg = apr_psprintf(session->pool, "extra data after last expected column");
				session->is_error = 1;
				gdebug(NULL, "%s", session->errmsg);
			}
			pthread_mutex_unlock(err_mutex);
			return -1;
		}
		return 0;
	}

	resetStringInfo(attribute_buf);

	/*
	 * The de-escaped attributes will certainly not be longer than the input
	 * data line, so we can just force attribute_buf to be large enough and
	 * then transfer data without any checks for enough space.  We need to do
	 * it this way because enlarging attribute_buf mid-stream would invalidate
	 * pointers already stored into raw_fields[].
	 */
	if (attribute_buf->maxlen <= line_buf->len)
		enlargeStringInfo(attribute_buf, line_buf->len);

	/* set or init some variables for loop */
	output_ptr = attribute_buf->data;
	cur_ptr = line_buf->data;
	line_end_ptr = line_buf->data + line_buf->len;
	memset(last_mb_char2, 0, MAX_LEN);

	/*
	 * parse with session->col_pos which must be valid assured by checking in
	 * transform_position_info() before
	 */
	raw_fields = repalloc(raw_fields, session->fields_num * sizeof(char *));
	for (i = 0; i < session->fields_num; i++)
	{
		pos = session->col_pos[i];

		/* Null by default if no data can be read */
		if (pos->start_offset > line_buf->len ||
			(pos->flag == RELATIVE_POS && (has_null || *cur_ptr == '\0')))
		{
			has_null = true;
			raw_fields[i++] = NULL;
			continue;
		}
		else
			has_null = false;

		/* current field start from start_offset */
		if (pos->flag == ABSOLUTE_POS)
			cur_ptr = line_buf->data + pos->start_offset - 1;

		raw_fields[i] = output_ptr;

		/* check if start_ptr is within a truncated multibyte character */
		if (CheckIfTruncMbCharTDX(file_encoding, cur_ptr, line_buf->data,
								  last_mb_char1))
		{
			pthread_mutex_lock(err_mutex);
			if (!session->is_error)
			{
				session->errmsg = apr_psprintf(session->pool,
											   "multibyte character \"%s\" is truncated",
											   last_mb_char1);
				session->is_error = 1;
			}
			pthread_mutex_unlock(err_mutex);
			if (*raw_fields_ptr != raw_fields)
				*raw_fields_ptr = raw_fields;
			return -1;
		}

		/* scan data for a field */
		for (;;)
		{
			char c;

			/* TODO: currently, quote is not supportted */
			for (;;)
			{
				/* if reaching the end of the line or the offset */
				if ((cur_ptr >= line_end_ptr) ||
					(cur_ptr - line_buf->data >= pos->end_offset))
					goto pos_endfield;

				/* optimization for single byte encoding and fewer comparision */
				if ((pg_database_encoding_max_length() > 1) &&
					(pos->end_offset - (cur_ptr - line_buf->data) <=
					 pg_database_encoding_max_length()))
				{
					/* a new character begins here */
					if (mb_left_bytes == 0)
					{
						mb_left_bytes = pg_encoding_mblen(file_encoding, cur_ptr);

						if (mb_left_bytes > 1)
						{
							strncpy(last_mb_char2, cur_ptr, mb_left_bytes);
							last_mb_char2[mb_left_bytes] = '\0';
						}
					}
					mb_left_bytes--;
				}

				c = *cur_ptr++;
				/* add c to output string */
				*output_ptr++ = c;
			}
		}

pos_endfield:
		if (mb_left_bytes != 0)
		{
			pthread_mutex_lock(err_mutex);
			if (!session->is_error)
			{
				session->errmsg = apr_psprintf(session->pool,
											   "multibyte character \"%s\" is truncated",
											   last_mb_char1);
				session->is_error = 1;
			}
			pthread_mutex_unlock(err_mutex);
			if (*raw_fields_ptr != raw_fields)
				*raw_fields_ptr = raw_fields;
			return -1;
		}

		*output_ptr++ = '\0';
	}

	return session->fields_num;
}

/*
 * Parse the current line into separate attributes (fields),
 * performing de-escaping as needed.  This has exactly the same API as
 * CopyReadAttributesText, except we parse the fields according to
 * "standard" (i.e. common) CSV usage.
 */
static int
ReadAttributesCSV(session_t *session, StringInfo line_buf, StringInfo attribute_buf, char ***raw_fields_ptr)
{
	copy_options *cpara = session->copy_options;
	char quotec = cpara->quote;
	char escapec = cpara->escape;
	int fieldno;
	char *output_ptr;
	char *cur_ptr;
	char *line_end_ptr;
	int fldct = session->fields_num;
	char **raw_fields = *raw_fields_ptr;
	pthread_mutex_t *err_mutex = &session->err_mutex;
	
	/*
	 * We need a special case for zero-column tables: check that the input
	 * line is empty, and return.
	 */
	if (fldct <= 0)
	{
		if (line_buf->len != 0)
		{
			gdebug(NULL, "extra data after last expected column, linebuf \"%s\"", line_buf->data);
			pthread_mutex_lock(err_mutex);
			if (!session->is_error)
			{
				session->errmsg = apr_psprintf(session->pool, "extra data after last expected column");
				session->is_error = 1;
				gdebug(NULL, "%s", session->errmsg);
			}
			pthread_mutex_unlock(err_mutex);
			return -1;
		}
		return 0;
	}
	
	resetStringInfo(attribute_buf);
	
	/*
	 * The de-escaped attributes will certainly not be longer than the input
	 * data line, so we can just force attribute_buf to be large enough and
	 * then transfer data without any checks for enough space.  We need to do
	 * it this way because enlarging attribute_buf mid-stream would invalidate
	 * pointers already stored into raw_fields[].
	 */
	if (attribute_buf->maxlen <= line_buf->len)
		enlargeStringInfo(attribute_buf, line_buf->len);
	output_ptr = attribute_buf->data;
	
	/* set pointer variables for loop */
	cur_ptr = line_buf->data;
	line_end_ptr = line_buf->data + line_buf->len;
	
	/* Outer loop iterates over fields */
	fieldno = 0;
	for (;;)
	{
		bool found_delim = false;
		bool saw_quote = false;
		char *start_ptr;
		char *end_ptr;
		int input_len;
		
		/* Make sure there is enough space for the next value */
		if (fieldno >= fldct)
		{
			fldct *= 2;
			raw_fields = repalloc(raw_fields, fldct * sizeof(char *));
		}
		
		/* Remember start of field on both input and output sides */
		start_ptr = cur_ptr;
		raw_fields[fieldno] = output_ptr;
		
		/*
		 * Scan data for field,
		 *
		 * The loop starts in "not quote" mode and then toggles between that
		 * and "in quote" mode. The loop exits normally if it is in "not
		 * quote" mode and a delimiter or line end is seen.
		 */
		for (;;)
		{
			char c;
			
			/* Not in quote */
			for (;;)
			{
				end_ptr = cur_ptr;
				if (cur_ptr >= line_end_ptr)
					goto endfield;
				c = *cur_ptr++;
				/* unquoted field delimiter */
				if (strncmp(cpara->delim, cur_ptr - 1, cpara->delim_len) == 0)
				{
					cur_ptr += (cpara->delim_len - 1);
					found_delim = true;
					goto endfield;
				}
				/* start of quoted field (or part of field) */
				if (c == quotec)
				{
					saw_quote = true;
					break;
				}
				/* Add c to output string */
				*output_ptr++ = c;
			}
			
			/* In quote */
			for (;;)
			{
				end_ptr = cur_ptr;
				if (cur_ptr >= line_end_ptr)
				{
					if (*cur_ptr == '\0' && *line_end_ptr == '\0')
						break;
					else
					{
						gdebug(NULL,
							   "unterminated CSV quoted field, linebuf \"%s\"",
							   line_buf->data);
						pthread_mutex_lock(err_mutex);
						if (!session->is_error)
						{
							session->errmsg = apr_psprintf(session->pool,
														   "unterminated CSV quoted field");
							session->is_error = 1;
							gdebug(NULL, "%s", session->errmsg);
						}
						pthread_mutex_unlock(err_mutex);
						if (*raw_fields_ptr != raw_fields)
							*raw_fields_ptr = raw_fields;
						return -1;
					}
				}
				c = *cur_ptr++;
				
				/* escape within a quoted field */
				if (c == escapec)
				{
					/*
					 * peek at the next char if available, and escape it if it
					 * is an escape char or a quote char
					 */
					if (cur_ptr < line_end_ptr)
					{
						char nextc = *cur_ptr;
						
						if (nextc == escapec || nextc == quotec)
						{
							*output_ptr++ = nextc;
							cur_ptr++;
							continue;
						}
					}
				}
				
				/*
				 * end of quoted field. Must do this test after testing for
				 * escape in case quote char and escape char are the same
				 * (which is the common case).
				 */
				if (c == quotec)
					break;
				
				/* Add c to output string */
				*output_ptr++ = c;
			}
		}
endfield:
		
		/* Terminate attribute value in output area */
		*output_ptr++ = '\0';
		
		/* Check whether raw input matched null marker */
		input_len = end_ptr - start_ptr;
		if (!saw_quote && input_len == cpara->null_print_len &&
		    strncmp(start_ptr, cpara->null_print, input_len) == 0)
			raw_fields[fieldno] = NULL;
		
		fieldno++;
		
		/* Done if we hit EOL instead of a delim */
		if (!found_delim)
			break;
	}
	
	/* Clean up state of attribute_buf */
	output_ptr--;
	Assert(*output_ptr == '\0');
	attribute_buf->len = (output_ptr - attribute_buf->data);
	
	if (*raw_fields_ptr != raw_fields)
		*raw_fields_ptr = raw_fields;
	return fieldno;
}

/*
 * Call a previously-looked-up datatype input function for bulkload compatibility, which prototype
 * is InputFunctionCall. The new one tries to input bulkload compatiblity-specific parameters to built-in datatype
 * input function.
 */
static Datum InputFunctionCallForTDX(session_t* session, copy_options *cpara,
									    FmgrInfo *flinfo, char *str, Oid typioparam, int32 typmod)
{
	FunctionCallInfoData fcinfo;
	Datum                result;
	short                nargs = 3;
	char                 *date_time_fmt = NULL;
	datetime_field_ord  *dfo = NULL;
	datetime_field_str  *dfs = NULL;

	if (str == NULL && flinfo->fn_strict)
		return (Datum) 0; /* just return null result */
	
	switch (typioparam)
	{
		case DATEOID:
		{
			if (cpara->date_format)
			{
				nargs         = 4;
				date_time_fmt = (char *) cpara->date_format;
			}
		}
			break;
		case TIMEOID:
		{
			if (cpara->time_format)
			{
				nargs         = 4;
				date_time_fmt = (char *) cpara->time_format;
			}
		}
			break;
		case TIMESTAMPOID:
		{
			if (cpara->timestamp_format)
			{
				nargs         = 4;
				date_time_fmt = (char *) cpara->timestamp_format;
			}
		}
			break;
			/* not support SMALLDATETIMEOID now. */
		default:
		{
			/* do nothing. */
		}
			break;
	}
	
	InitFunctionCallInfoData(fcinfo, flinfo, nargs, InvalidOid, NULL, NULL);
	
	fcinfo.arg[0] = CStringGetDatum(str);
	fcinfo.arg[1] = ObjectIdGetDatum(typioparam);
	fcinfo.arg[2] = Int32GetDatum(typmod);
	
	fcinfo.argnull[0] = (str == NULL);
	fcinfo.argnull[1] = false;
	fcinfo.argnull[2] = false;
	
	/*
	 * input specified datetime format.
	 */
	if (nargs == 4)
	{
		fcinfo.arg[3]     = CStringGetDatum(date_time_fmt);
		fcinfo.argnull[3] = false;
	}
	else if (nargs == 5)
	{
		gprintln(NULL, "in five args");
		fcinfo.arg[3] = PointerGetDatum(dfo);
		fcinfo.argnull[3] = false;

		fcinfo.arg[4] = PointerGetDatum(dfs);
		fcinfo.argnull[4] = false;
	}
	
	result = FunctionCallInvoke(&fcinfo);
	
	/* Should get null result if and only if str is NULL */
	if (str == NULL)
	{
		if (!fcinfo.isnull)
			ereport(ERROR,
			        (errcode(ERRCODE_INTERNAL_ERROR),
					        errmsg("input function %u returned non-NULL", fcinfo.flinfo->fn_oid)));
	}
	else
	{
		if (fcinfo.isnull)
			ereport(ERROR,
			        (errcode(ERRCODE_INTERNAL_ERROR),
					        errmsg("input function %u returned NULL", fcinfo.flinfo->fn_oid)));
	}
	
	return result;
}

#define IS_NOT_TYPE_BY_VALUE(type)	\
	((type) == NAMEOID || (type) == TEXTOID || (type) == VARCHAR2OID || (type) == NVARCHAR2OID ||(type) == OIDVECTOROID || \
	(type) == CASHOID || (type) == BPCHAROID || (type) == BYTEAOID || (type) == INTERVALOID || (type) == TIMETZOID || \
	(type) == NUMERICOID || (type) == JSONBOID || (type) == RIDOID || (type) == VARCHAROID || (type) == RAWOID)
static ShardID
ConvertAndEvaluateShardid(session_t *session, char **raw_fields, int fldct, Datum *values, bool *nulls, FmgrInfo *in_functions)
{
	int shardid = InvalidShardID;
	int dis_key_idx = 0;
	char *string = NULL;
	Oid *dis_key_type = session->dis_key_type;
	copy_options *cpara = session->copy_options;
	int dis_key_num = session->dis_key_num;
	int *dis_key = session->dis_key;
	pthread_mutex_t *err_mutex = &session->err_mutex;
	
	/* transform the value(s) into internal format(s) and evaluate shardid*/
	for (dis_key_idx = 0; dis_key_idx < dis_key_num; dis_key_idx++)
	{
		values[dis_key_idx] = (Datum) NULL;
		nulls[dis_key_idx] = true;
	}
	
	/* init input function */
	for (dis_key_idx = 0; dis_key_idx < dis_key_num; dis_key_idx++)
	{
		Oid in_func_oid = InvalidOid;
		switch (dis_key_type[dis_key_idx])
		{
			case INT8OID:
				in_func_oid = INT8_IN_OID;
				break;
			case INT2OID:
				in_func_oid = INT2_IN_OID;
				break;
			case OIDOID:
				in_func_oid = OID_IN_OID;
				break;
			case INT4OID:
				in_func_oid = INT4_IN_OID;
				break;
			case BOOLOID:
				in_func_oid = BOOL_IN_OID;
				break;
			case CHAROID:
				in_func_oid = CHAR_IN_OID;
				break;
			case NAMEOID:
				in_func_oid = NAME_IN_OID;
				break;
			case INT2VECTOROID:
				in_func_oid = INT2VECTOR_IN_OID;
				break;
			case TEXTOID:
				in_func_oid = TEXT_IN_OID;
				break;
			case OIDVECTOROID:
				in_func_oid = OIDVECTOR_IN_OID;
				break;
			case FLOAT4OID:
				in_func_oid = FLOAT4_IN_OID;
				break;
			case FLOAT8OID:
				in_func_oid = FLOAT8_IN_OID;
				break;
			case ABSTIMEOID:
				in_func_oid = ABSTIME_IN_OID;
				break;
			case RELTIMEOID:
				in_func_oid = RELTIME_IN_OID;
				break;
			case CASHOID:
				in_func_oid = CASH_IN_OID;
				break;
			case BPCHAROID:
				in_func_oid = BPCHAR_IN_OID;
				break;
			case BYTEAOID:
				in_func_oid = BYTEA_IN_OID;
				break;
			case VARCHAROID:
				in_func_oid = VARCHAR_IN_OID;
				break;
			case DATEOID:
				in_func_oid = DATA_IN_OID;
				break;
			case TIMEOID:
				in_func_oid = TIME_IN_OID;
				break;
			case TIMESTAMPOID:
				in_func_oid = TIMESTAMP_IN_OID;
				break;
			case TIMESTAMPTZOID:
				in_func_oid = TIMESTAMPTZ_IN_OID;
				break;
			case INTERVALOID:
				in_func_oid = INTERVAL_IN_OID;
				break;
			case TIMETZOID:
				in_func_oid = TIMETZ_IN_OID;
				break;
			case NUMERICOID:
				in_func_oid = NUMERIC_IN_OID;
				break;
			case VARCHAR2OID:
				in_func_oid = VARCHAR2_IN_OID;
				break;
			case NVARCHAR2OID:
				in_func_oid = NVARCHAR2_IN_OID;
				break;
			case RAWOID:
				in_func_oid = RAW_IN_OID;
				break;
			default:
				pthread_mutex_lock(err_mutex);
				if (!session->is_error)
				{
					session->errmsg = apr_psprintf(session->pool,
					                               "unsupported distribution datatype:%d ",
					                               dis_key_type[dis_key_idx]);
					session->is_error = 1;
					gdebug(NULL, session->errmsg);
				}
				pthread_mutex_unlock(err_mutex);
				return InvalidShardID;
		}
		
		PG_TRY();
		{
			fmgr_info(in_func_oid, &in_functions[dis_key_idx]);
		}
		PG_CATCH();
		{
			pthread_mutex_lock(err_mutex);
			if (!session->is_error)
			{
				ErrorData *edata = CopyErrorData();
				session->errmsg = apr_psprintf(session->pool, "%s", edata->message);
				session->is_error = 1;
				gdebug(NULL, session->errmsg);
			}
			pthread_mutex_unlock(err_mutex);
			FlushErrorState();
			return InvalidShardID;
		}
		PG_END_TRY();
	}
	
	for (dis_key_idx = 0; dis_key_idx < dis_key_num; ++dis_key_idx)
	{
		int attr_idx = dis_key[dis_key_idx];
		Oid dis_ket_type_oid = dis_key_type[dis_key_idx];
		nulls[dis_key_idx] = true;
		
		if (attr_idx >= fldct)
		{
			/*
			 * Some attributes are missing. In FILL MISSING FIELDS mode,
			 * treat them as NULLs.
			 */
			if (!cpara->fill_missing)
			{
				pthread_mutex_lock(err_mutex);
				if (!session->is_error)
				{
					session->errmsg = apr_psprintf(session->pool, "missing data for column %d", attr_idx);
					session->is_error = 1;
					gdebug(NULL, session->errmsg);
				}
				pthread_mutex_unlock(err_mutex);
				return InvalidShardID;
			}
			string = NULL;
		}
		else
		{
			string = raw_fields[attr_idx];
		}
		
		if (cpara->convert_select_flags && cpara->convert_select_flags[attr_idx])
		{
			/* ignore input field, leaving column as NULL */
			continue;
		}
		
		if (cpara->csv_mode)
		{
			if (string == NULL && cpara->force_notnull_flags[attr_idx])
			{
				/*
				 * FORCE_NOT_NULL option is set and column is NULL -
				 * convert it to the NULL string.
				 */
				string = cpara->null_print;
			}
			else if (string != NULL && cpara->force_null_flags[attr_idx] && strcmp(string, cpara->null_print) == 0)
			{
				/*
				 * FORCE_NULL option is set and column matches the NULL
				 * string. It must have been quoted, or otherwise the
				 * string would already have been set to NULL. Convert it
				 * to NULL as specified.
				 */
				string = NULL;
			}
		}
		
		/* convert to internal format */
		PG_TRY();
		{
			values[dis_key_idx] = InputFunctionCallForTDX(session, cpara, &in_functions[dis_key_idx],
														    string, dis_ket_type_oid, -1);
		}
		PG_CATCH();
		{
			pthread_mutex_lock(err_mutex);
			if (!session->is_error)
			{
				ErrorData *edata = CopyErrorData();
				session->errmsg = apr_psprintf(session->pool, "%s", edata->message);
				session->is_error = 1;
				gdebug(NULL, session->errmsg);
			}
			pthread_mutex_unlock(err_mutex);
			FlushErrorState();
			return InvalidShardID;
		}
		PG_END_TRY();
		
		if (string != NULL)
			nulls[dis_key_idx] = false;
	}
	
	PG_TRY();
	{
		shardid = EvaluateShardId(dis_key_type, nulls, values, dis_key_num);
	}		
	PG_CATCH();
	{
		pthread_mutex_lock(err_mutex);
		if (!session->is_error)
		{
			ErrorData *edata = CopyErrorData();
			session->errmsg = apr_psprintf(session->pool, "%s", edata->message);
			session->is_error = 1;
			gdebug(NULL, session->errmsg);
		}
		pthread_mutex_unlock(err_mutex);
		FlushErrorState();
		return InvalidShardID;
	}
	PG_END_TRY();
	
	for (dis_key_idx = 0; dis_key_idx < dis_key_num; ++dis_key_idx)
	{
		if (IS_NOT_TYPE_BY_VALUE(dis_key_type[dis_key_idx]))
		{
			pfree(DatumGetPointer(values[dis_key_idx]));
		}
	}

	
	return shardid;
}

static const char *get_encoding_text(int encoding)
{
	int i;
	for (i = 0; pg_enc2gettext_tbl[i].name != NULL; i++)
	{
		if (pg_enc2gettext_tbl[i].encoding == encoding)
		{
			return pg_enc2gettext_tbl[i].name;
		}
	}
	return NULL;
}

/* the thread to read files */
static void* read_thread(void *para)
{
	read_thread_para *t_para = (read_thread_para *) para;
	session_t *session = t_para->session;
	copy_options *cpara = session->copy_options;
	fstream_filename_and_offset *l_fo;
	StringInfo line_buf = NULL;
	StringInfo line_buf_after_conv = NULL;
	StringInfo *cache_buf = NULL;
	StringInfo attribute_buf = NULL;
	block_t *rawblock = NULL;
	int thread_id = t_para->j;
	redist_buff_t *redist_buff;
	fstream_filename_and_offset *fo;
	char **raw_fields = (char **) palloc0(session->fields_num * sizeof(char *));
	Datum *values = (Datum *) palloc0(session->dis_key_num * sizeof(Datum));
	bool *nulls = (bool *) palloc0(session->dis_key_num * sizeof(bool));
	FmgrInfo *in_functions = (FmgrInfo *) palloc0(session->dis_key_num * sizeof(FmgrInfo));
	int nodeid = -1;
	int i = 0;
	iconv_t cd = (iconv_t) -1;
	int row_size = 1024;
	char *out = (char *) palloc(row_size * sizeof(char));
	Bitmapset *dis_bitmap = bms_make_empty(session->fields_num);
	pthread_mutex_t *buffer_mutex = NULL;
	pthread_cond_t *append_cond = NULL;
	pthread_cond_t *send_cond = NULL;
	pthread_mutex_t *fs_mutex = &session->fs_mutex;
	pthread_mutex_t *err_mutex = &session->err_mutex;
	TDX_ThreadInfo *thrinfo;

	for (i = 0; i < session->dis_key_num; ++i)
		bms_add_member(dis_bitmap, session->dis_key[i]);
	
	thrinfo = (TDX_ThreadInfo *)palloc0(sizeof (TDX_ThreadInfo));
	thrinfo->thr_id = (pthread_t) pthread_self();
	thrinfo->opentenbase_ora_compatible = session->opentenbase_ora_compatible;
	thrinfo->session_tz = session->timezone;
	SetTDXThreadInfo(thrinfo);
	
	line_buf = makeStringInfo();
	line_buf_after_conv = makeStringInfo();
	attribute_buf = makeStringInfo();
	rawblock = (block_t *) palloc0(sizeof(block_t));
	rawblock->data = (char *) palloc0(opt.m * sizeof(char));
	l_fo = (fstream_filename_and_offset *) palloc0(sizeof(fstream_filename_and_offset));
	l_fo->foff = 0;
	l_fo->line_number = 0;
	
	cache_buf = (StringInfo *) palloc0(sizeof(StringInfo) * session->maxsegs);
	for (i = 0; i < session->maxsegs; ++i)
		cache_buf[i] = makeStringInfo();
	
	if (cpara->need_transcoding)
	{
		const char *to_enc = get_encoding_text(GetDatabaseEncoding());
		const char *from_enc = get_encoding_text(cpara->file_encoding);
		
		cd = iconv_open(to_enc, from_enc);
		if (cd == (iconv_t) -1)
		{
			pthread_mutex_lock(err_mutex);
			if (!session->is_error)
			{
				session->errmsg = apr_psprintf(session->pool,
				                               "Allocates descriptor for code conversion from %s to %s failed",
				                               from_enc,
				                               to_enc);
				session->is_error = 1;
				gwarning(NULL, session->errmsg);
			}
			pthread_mutex_unlock(err_mutex);
			goto cleanup;
		}
	}
	
	while (1)
	{
		int size;
		rawblock->bot = rawblock->top = 0;
		
		/* read block from local file or remote object */
		pthread_mutex_lock(fs_mutex);
		
		if (session->is_error)
		{
			gdebug(NULL, "end read_thread_%d before read data from stream ...", thread_id);
			pthread_mutex_unlock(fs_mutex);
			goto cleanup;
		}
		
		if (IS_DATA_STREAM_NULL())
		{
			gdebug(NULL, "end read_thread_%d due to data fstream error", thread_id);
			pthread_mutex_unlock(fs_mutex);
			session->is_error = 1;
			goto cleanup;
		}
		
		if (session->remote_protocol_type == REMOTE_COS_PROTOCOL)
		{
			/* read from cos stream */
			gcb.read_bytes -= cos_fstream_get_compressed_position(session->cos_fs);
			size = cos_fstream_read(cosContext, session->bucket, session->cos_fs,
			                        rawblock->data, opt.m, l_fo, 1,
			                        session->copy_options->eol,
			                        session->copy_options->eol_len);
			
			/* invalid buffer size, or all files are read */
			if (size == 0)
			{
				gdebug(NULL, "exit read_thread_%d due to EOF", thread_id);
				gcb.read_bytes += cos_fstream_get_compressed_size(session->cos_fs);
				pthread_mutex_unlock(fs_mutex);
				break;
			}
			
			gcb.read_bytes += cos_fstream_get_compressed_position(session->cos_fs);
			
			if (size < 0)
			{
				const char *ferror = cos_fstream_get_error(session->cos_fs);
				gdebug(NULL, "end read_thread_%d due to cos fstream error: %s", thread_id, ferror);
				session->is_error = 1;
				pthread_mutex_unlock(fs_mutex);
				goto cleanup;
			}
		}
		else
		{
			/* read from local file stream */
			gcb.read_bytes -= fstream_get_compressed_position(session->fstream);
			size = fstream_read(session->fstream,
			                    rawblock->data,
			                    opt.m,
			                    l_fo,
			                    1,
			                    session->copy_options->eol,
			                    session->copy_options->eol_len);
			
			/* invalid buffer size, or all files are read */
			if (size == 0)
			{
				gdebug(NULL, "exit read_thread_%d due to EOF", thread_id);
				gcb.read_bytes += fstream_get_compressed_size(session->fstream);
				pthread_mutex_unlock(fs_mutex);
				break;
			}
			
			gcb.read_bytes += fstream_get_compressed_position(session->fstream);
			
			if (size < 0)
			{
				const char *ferror = fstream_get_error(session->fstream);
				gdebug(NULL, "end read_thread_%d due to fstream error: %s", thread_id, ferror);
				session->is_error = 1;
				pthread_mutex_unlock(fs_mutex);
				goto cleanup;
			}
		}			
		
		gdebug(NULL, "read_thread_%d get file %s (%ld - %ld).", thread_id, l_fo->fname, l_fo->line_number, l_fo->foff);
		pthread_mutex_unlock(fs_mutex);
		
		/* deal with rawdata in a block */
		rawblock->top = size;
		
		while (rawblock->bot != rawblock->top)
		{
			/* read data from our filestream as a chunk with whole data rows */
			int shardid = -1, line_bytes;
			char *line_start, *line_end;
			int fldct;
			int delim_error;
			
			if (session->is_error)
				goto cleanup;
			
			resetStringInfo(line_buf);
			
			/* parse a line of data and extract the distribution key value(s) */
			/* scan for end of line delimiter (\n by default) for record boundary. */
			line_start = line_end = rawblock->data + rawblock->bot;
			if (session->copy_options->eol_len > 0)
			{
				line_end = find_first_eol_delim(line_start, 
												rawblock->data + rawblock->top, 
												cpara->eol, 
												cpara->eol_len, 
												opt.m, 
												&delim_error);
				
				if (delim_error != DELIM_FOUND)
				{
					gdebug(NULL, "end read_thread_%d due to failing to find delimiter \"%s\", error flag: %d",
						   thread_id, cpara->eol, delim_error);
					pthread_mutex_lock(err_mutex);
					if (!session->is_error)
					{
						session->errmsg = apr_psprintf(session->pool, 
													   "fail to find the delimiter \"%s\", "
													   "max data line len: %d, rawblock rest size: %ld, "
													   "perhaps should increase max data line len or designate a proper delimiter",
													   cpara->eol, opt.m, rawblock->data + rawblock->top - line_start);
						session->is_error = 1;
						gdebug(NULL, session->errmsg);
					}
					pthread_mutex_unlock(err_mutex);
					goto cleanup;
				}
			}
			else
			{
				/* text header with \n as delimiter (by default) */
				for (; line_end < rawblock->data + rawblock->top && *line_end != '\n'; line_end++);
			}
			
			/*
			 * line_end equals rawblock->top means the heard is too long 
			 * that the whole buffer can not contain it
			 */
			line_end = (line_end < rawblock->data + rawblock->top) ? line_end + 1 : 0;
			
			/* bytes of raw data to the end of eol char*/
			line_bytes = line_end - line_start;
			
			if (line_bytes < 0)
			{
				gdebug(NULL, "end read_thread_%d due to failing to get a line_buf from a rawblock %d to %d",
				         thread_id, rawblock->bot, rawblock->top);
				pthread_mutex_lock(err_mutex);
				if (!session->is_error)
				{
					session->errmsg = apr_psprintf(session->pool, "unexpected rawblock (%d:%d)",
					                               rawblock->bot, rawblock->top);
					session->is_error = 1;
					gdebug(NULL, session->errmsg);
				}
				pthread_mutex_unlock(err_mutex);
				goto cleanup;
			}
			
			/* A completely empty line is not allowed with FILL MISSING FIELDS.
			 * Without FILL MISSING FIELDS, it's almost surely an error, but not always:
			 * a table with a single text column, for example, needs to accept empty lines. */
			if (line_bytes == 0 && cpara->fill_missing && session->dis_key_num > 1)
			{
				gdebug(NULL, "missing data for columns, found empty data line");
				pthread_mutex_lock(err_mutex);
				if (!session->is_error)
				{
					session->errmsg = apr_psprintf(session->pool, "missing data for columns, found empty data line");
					session->is_error = 1;
					gdebug(NULL, session->errmsg);
				}
				pthread_mutex_unlock(err_mutex);
				goto cleanup;
			}
			
			/* move bot forward (including eol)*/
			rawblock->bot += line_bytes;
			
			line_bytes -= (session->copy_options->eol_len > 0) ? session->copy_options->eol_len : 1;
			
			/* raw data without eol char*/
			appendBinaryStringInfo(line_buf, line_start, line_bytes);
			
			/* Done reading the line.  Convert it to server encoding if needed. */
			if (cpara->need_transcoding && line_buf->len > 0)
			{
				size_t inlen = line_buf->len;
				size_t outlen = line_buf->len * 2;
				char *in = line_buf->data;
				char *temp_ptr = NULL;
				size_t ret = (size_t) -1;
				
				if (outlen > row_size)
				{
					row_size = outlen;
					out = (char *) repalloc(out, row_size * sizeof(char));
				}
				memset(out, 0, row_size);
				temp_ptr = out;
				
				while(ret == (size_t) -1 && inlen > 0)
				{
					ret = (size_t)iconv(cd, &in, &inlen, &temp_ptr, &outlen);
					
					/* success OR fail without compatible_illegal_chars. */
					if (!cpara->compatible_illegal_chars || ret != (size_t) -1)
						break;
						
					*temp_ptr = '?';
					inlen--;
					outlen--;
					in++;
				}
				
				/* There is remained data in input string does not been converted */
				/* ret may be -1 even after the string has been converted (compatible_illegal_chars) */
				if (inlen > 0) {
					pthread_mutex_lock(err_mutex);
					if (!session->is_error)
					{
						session->is_error = 1;
						session->errmsg = apr_psprintf(session->pool, "code conversion failed");
						gdebug(NULL, "%s from %s to %s failed, line_buf \"%s\","
						               "\nunconverted data \"%s\"",
						         session->errmsg,
						         pg_encoding_to_char(cpara->file_encoding),
						         pg_encoding_to_char(GetDatabaseEncoding()),
						         line_buf->data,
						         line_buf->data + (line_buf->len - inlen));
					}
					pthread_mutex_unlock(err_mutex);
					goto cleanup;
				}
				
				resetStringInfo(line_buf_after_conv);
				appendBinaryStringInfo(line_buf_after_conv, out, strlen(out));
			}
			
			if (session->col_pos)
			{
				fldct = ReadAttributesPos(session,
										  (cpara->need_transcoding && line_buf->len > 0) ? line_buf_after_conv : line_buf,
										  attribute_buf, &raw_fields);
			}
			else if (cpara->csv_mode)
			{
				fldct = ReadAttributesCSV(session, (cpara->need_transcoding && line_buf->len > 0) ? line_buf_after_conv : line_buf,
				                          attribute_buf, &raw_fields);
			}
			else
			{
				fldct = ReadAttributesText(session,
										   (cpara->need_transcoding && line_buf->len > 0) ? line_buf_after_conv : line_buf,
										   attribute_buf, &raw_fields,
										   dis_bitmap);
			}
			
			if (fldct < 0)
			{
				session->is_error = 1;
				goto cleanup;
			}
			
			shardid = ConvertAndEvaluateShardid(session, raw_fields, fldct, values, nulls, in_functions);
			
			if (!ShardIDIsValid(shardid))
			{
				gdebug(NULL, "Fail to evaluate shardid, errmsg %s, linebuf \"%s\"", 
						 session->errmsg ? session->errmsg : " ", line_buf->data);
				goto cleanup;
			}
			
			if (session->is_error)
			{
				gdebug(NULL, "%s, linebuf \"%s\"", 
						 session->errmsg ? session->errmsg : "Fail to evaluate shardid", line_buf->data);
				goto cleanup;
			}
			
			/* locate the DN to send, then append it to buffer, need lock when appending */
			nodeid = session->shardmap[shardid];
			
			redist_buff = session->redist_buff[nodeid];
			fo = session->fo[nodeid];
			buffer_mutex = &session->buffer_mutex[nodeid];
			append_cond = &session->append_cond[nodeid];
			send_cond = &session->send_cond[nodeid];
			
			/* append eol at the end of line_buf */
			if (cpara->eol_len > 0)
				appendBinaryStringInfo(line_buf, cpara->eol, cpara->eol_len);
			else
				appendBinaryStringInfo(line_buf, "\n", 1);
			
			/* store line_buf in local cache first, copy to shared buffer when full. */
			if (cache_buf[nodeid]->len + line_buf->len >= opt.m)
			{
				/* local buffer if full, move to shared buffer */
				pthread_mutex_lock(buffer_mutex);
				gdebug(NULL, "read_thread_%d Lock node %d", thread_id, nodeid);
				
				while (redist_buff->is_full)
				{
					if (session->is_error)
					{
						gdebug(NULL, "end read_thread_%d before append data to shared block ...", thread_id);
						pthread_mutex_unlock(buffer_mutex);
						goto cleanup;
					}
					
					/* WHILE: other read_thread may have filled the buffer. */
					gdebug(NULL, "read_thread_%d wait for node %d append signal", thread_id, nodeid);
					pthread_cond_wait(append_cond, buffer_mutex);
					gdebug(NULL, "read_thread_%d get node %d append signal", thread_id, nodeid);
				}
				
				/* move local buffer to shared buffer */
				if (redist_buff->outblock.bot != redist_buff->outblock.top)
				{
					gdebug(NULL, "There is data in shared datanode %d buffer already!!!", nodeid);
					pthread_mutex_unlock(buffer_mutex);
					session->is_error = 1;
					goto cleanup;
				}
				
				session_append_block(&redist_buff->outblock, cache_buf[nodeid]);
				gdebug(NULL, "read_thread_%d append %s to shared node %d's buffer.", thread_id, cache_buf[nodeid]->data, nodeid);
				
				fo->foff = l_fo->foff;
				fo->line_number = l_fo->line_number;
				strncpy(fo->fname, l_fo->fname, sizeof fo->fname);
				fo->fname[sizeof fo->fname - 1] = 0;
				
				/* set full flag and active send signal */
				redist_buff->is_full = true;
				gdebug(NULL, "node %d output buffer is full, wait for consuming done.", nodeid);
				pthread_cond_broadcast(send_cond);
				gdebug(NULL, "read_thread_%d active node %d send signal", thread_id, nodeid);
				
				pthread_mutex_unlock(buffer_mutex);
				gdebug(NULL, "read_thread_%d unLock node %d", thread_id, nodeid);
				
				resetStringInfo(cache_buf[nodeid]);
			}
			
			appendBinaryStringInfo(cache_buf[nodeid], line_buf->data, line_buf->len);
		}
	}
	
	/* read EOF, move all data in local buffer to shared buffer. */
	for (nodeid = 0; nodeid < session->maxsegs; ++nodeid)
	{
		if (cache_buf[nodeid]->len > 0)
		{
			buffer_mutex = &session->buffer_mutex[nodeid];
			append_cond = &session->append_cond[nodeid];
			send_cond = &session->send_cond[nodeid];
			redist_buff = session->redist_buff[nodeid];
			fo = session->fo[nodeid];
			
			pthread_mutex_lock(buffer_mutex);
			gdebug(NULL, "read_thread_%d Lock node %d", thread_id, nodeid);
			
			while (redist_buff->is_full)
			{
				if (session->is_error)
				{
					gdebug(NULL, "end read_thread_%d before append data to shared block due to EOF...", thread_id);
					pthread_mutex_unlock(buffer_mutex);
					goto cleanup;
				}
				
				gdebug(NULL, "read_thread_%d wait for node %d append signal", thread_id, nodeid);
				pthread_cond_wait(append_cond, buffer_mutex);
				gdebug(NULL, "read_thread_%d get node %d append signal", thread_id, nodeid);
			}
			
			session_append_block(&redist_buff->outblock, cache_buf[nodeid]);
			gdebug(NULL, "append %s to node %d's buffer.", cache_buf[nodeid]->data, nodeid);
			
			fo->line_number = l_fo->line_number;
			fo->foff = l_fo->foff;
			strncpy(fo->fname, l_fo->fname, sizeof fo->fname);
			fo->fname[sizeof fo->fname - 1] = 0;
			
			if (!redist_buff->is_full)
			{
				redist_buff->is_full = true;
				gdebug(NULL, "node %d output buffer is full, wait for consuming done.", nodeid);
				
				pthread_cond_broadcast(send_cond);
				gdebug(NULL, "read_thread_%d active node %d send signal", thread_id, nodeid);
			}
			pthread_mutex_unlock(buffer_mutex);
			gdebug(NULL, "read_thread_%d unLock node %d", thread_id, nodeid);
			
			resetStringInfo(cache_buf[nodeid]);
		}
	}

cleanup:
	bms_free(dis_bitmap);
	if (cd != (iconv_t) -1)
		iconv_close (cd);
	pfree(out);
	pfree(raw_fields);
	pfree(values);
	pfree(nulls);
	pfree(in_functions);
	for (i = 0; i < session->maxsegs; ++i)
	{
		pfree_ext(cache_buf[i]->data);
		pfree_ext(cache_buf[i]);
	}
	pfree_ext(cache_buf);
	pfree_ext(line_buf->data);
	pfree_ext(line_buf);
	pfree_ext(line_buf_after_conv->data);
	pfree_ext(line_buf_after_conv);
	pfree_ext(attribute_buf->data);
	pfree_ext(attribute_buf);
	pfree_ext(rawblock->data);
	pfree_ext(rawblock);
	pfree_ext(l_fo);
	pfree(thrinfo);
	SetTDXThreadInfo(NULL);
	gprintln(NULL, "[sid - %ld] read_thread_%d exit.", session->id, thread_id);
	pthread_exit(NULL);
}

static void *
send_thread(void *para)
{
	request_t *r = (request_t *)para;
	session_t *session = r->session;
	int nodeid = r->segid;
	redist_buff_t *s_redist_buff = session->redist_buff[nodeid];
	block_t *s_outblock = &s_redist_buff->outblock;
	pthread_mutex_t *buffer_mutex = &session->buffer_mutex[nodeid];
	pthread_mutex_t *err_mutex = &session->err_mutex;
	pthread_cond_t *append_cond = &session->append_cond[nodeid];
	pthread_cond_t *send_cond = &session->send_cond[nodeid];
	block_t *l_datablock = NULL;
	fstream_filename_and_offset l_fo;
	int n = 0;
	int l_nkmsg = 0;
	
	/* Do nothing until all read_thread started up. */
	while (!session->pthread_setup)
	{
		if (session->is_error)
		{
			/* close stream and release fd & flock on pipe file */
			session_end(session, 1);
			request_end(r, 1, session->errmsg ? session->errmsg : "internal error");
			gwarning(NULL, "send_thread exit due to %s", session->errmsg);
			gwarning(r, "send_thread for node %d exit before all read_thread started up.", nodeid);
			pthread_exit(NULL);
		}
		pg_usleep(10000L);/* sleep 10ms */
	}
	
	/* cache block locally. */
	l_datablock = palloc0(sizeof(block_t));
	l_datablock->data = palloc0(opt.m);
	l_datablock->bot = l_datablock->top = 0;
	memset(&l_datablock->hdr, 0, sizeof(l_datablock->hdr));
	memset(&l_fo, 0, sizeof(fstream_filename_and_offset));
	
	while(1)
	{
		/* Loop until ERROR or EOF */
		pthread_mutex_lock(buffer_mutex);
		gdebug(r, "send_thread_%d Lock node buffer", nodeid);
		
		if (!s_redist_buff->is_full && !session->eof)
		{
			/* IF: only this thread could reset corresponding s_redist_buff. */
			gdebug(r, "send_thread_%d wait for node %d send_signal", nodeid, nodeid);
			pthread_cond_wait(send_cond, buffer_mutex);
			gdebug(r, "send_thread_%d get node %d send_signal", nodeid, nodeid);
		}
		
		/* cache data in shared buffer locally. */
		if (s_outblock->bot != s_outblock->top)
		{
			memcpy(l_datablock->data, s_outblock->data, s_outblock->top);
			l_datablock->bot = s_outblock->bot;
			l_datablock->top = s_outblock->top;
			
			l_nkmsg = s_redist_buff->num_messages;
			
			/* reset shared buffer */
			memset(s_outblock->data, 0, opt.m);
			s_outblock->top = s_outblock->bot = 0;
			s_redist_buff->num_messages = 0;
			
			memcpy(&l_fo, session->fo[nodeid], sizeof(fstream_filename_and_offset));
		}
		
		if (s_redist_buff->is_full)
		{
			/* we have cached data locally. we can signal read_thread to cache new data now. */
			s_redist_buff->is_full = false;
			pthread_cond_broadcast(append_cond);
			gdebug(r, "send_thread_%d active node %d append signal", nodeid, nodeid);
		}
		pthread_mutex_unlock(buffer_mutex);
		gdebug(r, "send_thread_%d unLock node %d", nodeid, nodeid);
		
		/* Continue sending until clearing all data in the buffer. */
		while (l_datablock->bot != l_datablock->top)
		{
			if (l_datablock->bot == 0 && (l_datablock->hdr.hbot == l_datablock->hdr.htop && l_datablock->hdr.hbot == 0))
			{
				/* If the Buffer is a new block,fill block header */
				int err = 0;
				if (session->remote_protocol_type == REMOTE_KAFKA_PROTOCOL)
					err = stream_block_fill_header(r, l_datablock, &l_fo, l_nkmsg, 
												   session->partition_cnt, session->offsets);
				else
					err = block_fill_header(r, l_datablock, &l_fo);
							
				if (err != 0)
				{
					session->is_error = 1;
					break;
				}
			}
			
			/* If PROTO-1: first write out the block header (metadata) If there is. */
			if (r->tdx_proto == 1)
			{
				n = l_datablock->hdr.htop - l_datablock->hdr.hbot;
				
				/* If There is remaining data in buffer caused by network chocked, sendout directly. */
				if (n > 0)
				{
					n = local_send(r, l_datablock->hdr.hbyte + l_datablock->hdr.hbot, n);
					if (n < 0)
					{
						session->is_error = 1;
						break;
					}
					
					l_datablock->hdr.hbot += n;
					n = l_datablock->hdr.htop - l_datablock->hdr.hbot;
					if (n > 0)
					{
						/* network chocked */
						gdebug(r, "network full, remained header size %d, continue...", n);
						continue;
					}
					gdebug(r, "send_thread_%d Succ to send header bytes %d .. %d to node %d",
					       nodeid, l_datablock->hdr.hbot - n, l_datablock->hdr.htop, nodeid);
				}
			}
			
			/* Send out the block data */
			gdebug(r, "Begin send buffer \"%s\"", l_datablock->data);
			n = l_datablock->top - l_datablock->bot;
			n = local_send(r, l_datablock->data + l_datablock->bot, n);
			if (n < 0)
			{
				/*
				 * EPIPE (or ECONNRESET some computers) indicates remote socket
				 * intentionally shut down half of the pipe.  If this was because
				 * of something like "select ... limit 10;", then it is fine that
				 * we couldn't transmit all the data--the segment didn't want it
				 * anyway.  If it is because the segment crashed or something like
				 * that, hopefully we would find out about that in some other way
				 * anyway, so it is okay if we don't poison the session.
				 */
				session->is_error = 1;
				break;
			}
			
			r->bytes += n;
			r->last = apr_time_now();
			l_datablock->bot += n;
			
			if (l_datablock->top != l_datablock->bot)
			{
				/* network chocked */
				gdebug(r, "network full, ONLY send buffer size %d, continue...(%d...%d)",
					   n, l_datablock->bot, l_datablock->top);
				continue;
			}
			gdebug(r, "send_thread Succ to send data %d bytes (0-%d) , %d msgs, to node(%d)",
			       n, l_datablock->top, l_nkmsg, nodeid);
		}
		
		/* Something wrong */
		if (session->is_error)
		{
			pthread_cond_broadcast(append_cond);
			pthread_mutex_lock(err_mutex);
			if ((n < 0) && (errno == EPIPE || errno == ECONNRESET))
			{
				/* close stream and release fd & flock on pipe file*/
				session->errmsg = apr_psprintf(session->pool,
				                               "tdx_send failed - the connection(socket %d) was terminated by the client (%d: %s)",
				                               r->sock,
				                               errno,
				                               strerror(errno));
				if (r->session)
					session_end(r->session, 1);
				l_datablock->bot = l_datablock->top;
				request_end(r, errno, session->errmsg);
			}
			/* Check if all cached data sent out. */
			else if (l_datablock->top != l_datablock->bot)
			{
				/* request failure. */
				gwarning(r, "cached data does not sent completely (%d...%d)", l_datablock->bot, l_datablock->top);
				request_end(r, 1, "cached data does not sent completely");
			}
			else
			{
				/* read thread wrong */
				if (!session->errmsg)
					session->errmsg = apr_psprintf(session->pool,
					                               "tdx_send failed - due to (%d: %s)", errno, strerror(errno));
				request_end(r, 1, session->errmsg);
			}
			pthread_mutex_unlock(err_mutex);
			break;
		}
		
		/* received EOF and there is no data in shared buffer. */
		/* use share outblock without lock is OK, because EOF means all read threads exit already. */
		if (session->eof && (s_outblock->bot == s_outblock->top))
		{
			pthread_mutex_lock(err_mutex);
			/* send eof */
			request_end(r, 0, 0);
			pthread_mutex_unlock(err_mutex);
			gprintln(r, "send_thread_%d receive EOF.", nodeid);
			break;
		}
		
		/* reset local buffer */
		memset(l_datablock->data, 0, opt.m);
		memset(&l_datablock->hdr, 0, sizeof(l_datablock->hdr));
		l_datablock->top = l_datablock->bot = 0;
	}
	
	pfree(l_datablock->data);
	pfree(l_datablock);
	gdebug(r, "send_thread for node %d exit.", nodeid);
	pthread_exit(NULL);
}

static bool
text_message_handle(session_t *session, iconv_t cd, char ***raw_fields, StringInfo line_buf,
                    StringInfo converted_line_buf, StringInfo attribute_buf, Bitmapset *dis_bitmap,
                    Datum *values, bool *nulls, FmgrInfo *in_functions, char **iconv_out,
                    int *row_size, ShardID *shardid)
{
	copy_options *cpara = session->copy_options;
	int fldct;
	pthread_mutex_t *err_mutex = &session->err_mutex;
	
	// resetStringInfo(line_buf);
	/* Done reading the line.  Convert it to server encoding if needed. */
	if (cpara->need_transcoding && line_buf->len > 0)
	{
		size_t inlen = line_buf->len;
		size_t outlen = line_buf->len * 2;
		char *in = line_buf->data;
		char *temp_ptr = NULL;
		size_t ret = (size_t) -1;
		
		if (outlen > *row_size)
		{
			*row_size = outlen;
			*iconv_out = (char *) repalloc(*iconv_out, *row_size * sizeof(char));
		}
		memset(*iconv_out, 0, *row_size);
		temp_ptr = *iconv_out;
		
		while (ret == (size_t) -1 && inlen > 0)
		{
			ret = (size_t) iconv(cd, &in, &inlen, &temp_ptr, &outlen);
			
			/* success OR fail without compatible_illegal_chars. */
			if (!cpara->compatible_illegal_chars || ret != (size_t) -1)
				break;
			
			*temp_ptr = '?';
			inlen--;
			outlen--;
			in++;
		}
		
		/* There is remained data in input string does not been converted */
		/* ret may be -1 even after the string has been converted (compatible_illegal_chars) */
		if (inlen > 0)
		{
			pthread_mutex_lock(err_mutex);
			if (!session->is_error)
			{
				session->errmsg = apr_psprintf(session->pool, "code conversion failed");
				session->is_error = 1;
				gdebug(NULL, "%s, from %s to %s, line_buf \"%s\","
				               "\nunconverted data \"%s\"",
				         session->errmsg,
				         pg_encoding_to_char(cpara->file_encoding),
				         pg_encoding_to_char(GetDatabaseEncoding()),
				         line_buf->data,
				         line_buf->data + (line_buf->len - inlen));
			}
			pthread_mutex_unlock(err_mutex);
			return false;
		}
		
		resetStringInfo(converted_line_buf);
		appendBinaryStringInfo(converted_line_buf, *iconv_out, strlen(*iconv_out));
	}
	
	if (cpara->csv_mode)
		fldct = ReadAttributesCSV(session,
		                          (cpara->need_transcoding && line_buf->len > 0) ? converted_line_buf : line_buf,
		                          attribute_buf,
		                          raw_fields);
	else
		fldct = ReadAttributesText(session,
		                           (cpara->need_transcoding && line_buf->len > 0) ? converted_line_buf : line_buf,
		                           attribute_buf,
		                           raw_fields,
		                           dis_bitmap);
	
	if (fldct < 0)
		return false;
	
	*shardid = ConvertAndEvaluateShardid(session, *raw_fields, fldct, values, nulls, in_functions);
	
	if (!ShardIDIsValid(*shardid))
		return false;
	return true;
}

/*
 * return : shardid and line_buf.
 */
static bool
logical_message_handle(session_t *session, line_item *ret,
                       Datum *values, bool *nulls, FmgrInfo *in_functions,
                       // char *msg_str, size_t msg_len,
                       rd_kafka_message_t *message,
                       StringInfo line_buf, ShardID *shardid)
{
	bool result = true;
	char *msg_str = message->payload;
	pthread_mutex_t *err_mutex = &session->err_mutex;
	struct json_object *payload_json_obj = json_tokener_parse(msg_str);
	int i = 0;
	
	gdebug(NULL, "start processing messages %d.", message->offset);
	
	if (!json_deserialize(payload_json_obj, session, ret))
	{
		result = false;
		goto end;
	}
	
	/* convert and sharding */
	switch (ret->op)
	{
		case 'I':
		case 'i':
		case 'c':
		case 'r':
			*shardid = ConvertAndEvaluateShardid(session, ret->raw_fields_after, session->fields_num, values, nulls, in_functions);
			if (!ShardIDIsValid(*shardid))
			{
				result = false;
				goto end;
			}
			
			logical_write_insert_op(line_buf, ret->raw_fields_after, ret->isnull_after, session->fields_num);
			break;
		case 'D':
		case 'd':
			*shardid = ConvertAndEvaluateShardid(session, ret->raw_fields_before,
			                                     session->fields_num, values, nulls, in_functions);
			if (!ShardIDIsValid(*shardid))
			{
				result = false;
				goto end;
			}
			
			logical_write_delete_op(line_buf, ret->raw_fields_before, ret->isnull_before, session->fields_num, true);
			break;
		case 'U':
		case 'u':
			*shardid = ConvertAndEvaluateShardid(session, ret->raw_fields_after,
			                                     session->fields_num, values, nulls, in_functions);
			if (!ShardIDIsValid(*shardid))
			{
				result = false;
				goto end;
			}
			
			if (ret->has_before)
			{
				logical_write_update_op(line_buf, ret->raw_fields_before, ret->isnull_before,
				                        ret->raw_fields_after, ret->isnull_after, session->fields_num,
				                        true);
			}
			else
			{
				logical_write_update_op(line_buf, NULL, NULL,
				                        ret->raw_fields_after, ret->isnull_after, session->fields_num,
				                        true);
			}
			break;
		default:
		{
			pthread_mutex_lock(err_mutex);
			if (!session->is_error)
			{
				session->errmsg = apr_psprintf(session->pool, "unrecognized operator %c", ret->op);
				session->is_error = 1;
				gdebug(NULL, session->errmsg);
			}
			pthread_mutex_unlock(err_mutex);
			result = false;
			goto end;
		}
	}

end:
	/* 
	 * ret(struct line_item) will be used for every lines/msgs, 
	 * so string pointer in before and array must be freed.
	 */
	for (i = 0; i < session->fields_num; ++i)
	{
		pfree_ext(ret->raw_fields_before[i]);
		pfree_ext(ret->raw_fields_after[i]);
	}
	json_object_put(payload_json_obj);
	return result;
}

static void batch_process_thread(void * para)
{
	batch_thread_para *bt_para = (batch_thread_para *) para;
	int nodeid, msg_i = -1;
	int prt = bt_para->prt_no;
	session_t *session = bt_para->session;
	copy_options *cpara = session->copy_options;
	redist_buff_t *s_redist_buff;
	ShardID shardid;
	Bitmapset *done;
	StringInfo line_buf;
	StringInfo *l_cache_buf;
	line_item *ret = palloc0(sizeof(line_item));
	Datum *values = (Datum *) palloc(session->dis_key_num * sizeof(Datum));
	bool *nulls = (bool *) palloc(session->dis_key_num * sizeof(bool));
	FmgrInfo *in_functions = (FmgrInfo *) palloc(session->dis_key_num * sizeof(FmgrInfo));

	iconv_t cd = (iconv_t) -1;
	int row_size = 1024;
	int kafka_resp_err;
	int *num_messages = (int *) palloc0(sizeof(int) * session->maxsegs);
	char *iconv_out = NULL;
	char **raw_fields = NULL;
	StringInfo converted_line_buf = NULL;
	StringInfo attribute_buf = NULL;
	Bitmapset *dis_bitmap = NULL;
	int64_t offsets = 0;
	int64_t low_offset;
	int64_t high_offset;
	size_t num_consumed_already = 0;
	
	pthread_mutex_t *buffer_mutex = NULL;
	pthread_cond_t *append_cond = NULL;
	pthread_cond_t *send_cond = NULL;
	TDX_ThreadInfo *thrinfo;

	ret->raw_fields_before = palloc0(sizeof(char *) * session->fields_num);
	ret->isnull_before = palloc0(sizeof(bool) * session->fields_num);
	ret->raw_fields_after = palloc0(sizeof(char *) * session->fields_num);
	ret->isnull_after = palloc0(sizeof(bool) * session->fields_num);
	
	line_buf = makeStringInfo();
	l_cache_buf = (StringInfo*) palloc0(sizeof(StringInfo) * session->maxsegs);
	for (nodeid = 0; nodeid < session->maxsegs; ++nodeid)
	{
		int temp = 0;
		l_cache_buf[nodeid] = makeStringInfo();
		appendBinaryStringInfo(l_cache_buf[nodeid], (char*) &temp, sizeof(int));
		appendBinaryStringInfo(l_cache_buf[nodeid], (char*) &temp, sizeof(int));
		l_cache_buf[nodeid]->cursor = l_cache_buf[nodeid]->len;
	}
	shardid = InvalidShardID;
	done = bms_make_empty(session->maxsegs);
	
	thrinfo = (TDX_ThreadInfo*) palloc0(sizeof(TDX_ThreadInfo));
	thrinfo->thr_id = (pthread_t) pthread_self();
	thrinfo->opentenbase_ora_compatible = session->opentenbase_ora_compatible;
	thrinfo->session_tz = session->timezone;
	SetTDXThreadInfo(thrinfo);
	
	/* 
	 * if local start_offset is beyond remote offset range for the topic 
	 * partition, consume from low_offset, so that it still works on
	 * 
	 * TODO: perhaps an option parameter needs to be added to determine 
	 * 		 consume from the beginning or from the breakpoint 
	 */
	pthread_mutex_lock(&session->fs_mutex);
	kafka_resp_err = (int) rd_kafka_query_watermark_offsets(bt_para->rk, session->topic_name, bt_para->prt_no, &low_offset, &high_offset, session->rk_timeout_ms);
	if (bt_para->start_offset < low_offset || 
		bt_para->start_offset > high_offset)
		bt_para->start_offset = low_offset;
	pthread_mutex_unlock(&session->fs_mutex);
	
	/* 1. deserialize and process all messages and append to different node's local buffer */
	do
	{
		gdebug(NULL, "batch_process_thread_%d: start consuming prt %d from offset %ld.", bt_para->j, prt, bt_para->start_offset + num_consumed_already);
		if (rd_kafka_consume_start(bt_para->rkt, prt, bt_para->start_offset + num_consumed_already) == -1)
		{
			pthread_mutex_lock(&session->err_mutex);
			if (!session->is_error)
			{
				session->errmsg = apr_psprintf(session->pool, "KAFKA-CONSUME: failed to start consumer - %s", rd_kafka_err2str(rd_kafka_last_error()));
				session->is_error = 1;
				gdebug(NULL, "%s", session->errmsg);
			}
			pthread_mutex_unlock(&session->err_mutex);
			goto cleanup;
		}

		/* consume batch messages */
		bt_para->num_consumed = rd_kafka_consume_batch(bt_para->rkt, prt,
		                                               session->rk_timeout_ms,
		                                               bt_para->messages,
		                                               bt_para->batch_size - num_consumed_already);
		if (bt_para->num_consumed == 0)
		{
			if (kafka_resp_err == RD_KAFKA_RESP_ERR_NO_ERROR)
			{
				/* if there is no msgs within msg_timeout_ms, goto another partition */
				if (bt_para->start_offset + num_consumed_already < high_offset && bt_para->start_offset + num_consumed_already >= low_offset)
				{
					gprintln(NULL,
					         "batch_process_thread_%d: consume no msgs for topic(%s) prt(%d) from offset (%ld) within timeout %dms. "
					         "actually low_offset %ld, high_offset %ld, retry",
					         bt_para->j,
					         session->topic_name,
					         prt,
					         bt_para->start_offset + num_consumed_already,
					         session->rk_timeout_ms,
					         low_offset,
					         high_offset);
					continue;
				}
				else
				{
					gprintln(NULL,
					         "batch_process_thread_%d: consume no msgs for topic(%s) prt(%d) from offset (%ld) within timeout %dms. "
					         "actually low_offset %ld, high_offset %ld, exit",
					         bt_para->j,
					         session->topic_name,
					         prt,
					         bt_para->start_offset + num_consumed_already,
					         session->rk_timeout_ms,
					         low_offset,
					         high_offset);
					goto cleanup;
				}
			}
			else
			{
				char *err_str = "failed to get low and high offset";

				if (kafka_resp_err == RD_KAFKA_RESP_ERR_LEADER_NOT_AVAILABLE)
					err_str = "leader not available";

				pthread_mutex_lock(&session->err_mutex);
				if (!session->is_error)
				{
					session->errmsg = apr_psprintf(session->pool,
					                               "batch_process_thread_%d: %s for topic(%s) prt(%d)",
					                               bt_para->j,
												   err_str,
					                               session->topic_name,
					                               bt_para->prt_no);
					session->is_error = 1;
					gdebug(NULL, session->errmsg);
				}
				pthread_mutex_unlock(&session->err_mutex);
				goto cleanup;
			}
		}
		else
		{
			gdebug(NULL, "batch_process_thread_%d: topic(%s) prt(%d) get msgs (%ld).",
			       bt_para->j, session->topic_name, prt, bt_para->num_consumed);
		}
		num_consumed_already += bt_para->num_consumed;
		
		if (!session->is_logical_sync)
		{
			int i = 0;
			/* for text or csv */
			iconv_out = (char *) palloc(row_size * sizeof(char));
			raw_fields = (char **) palloc0(session->fields_num * sizeof(char *));
			
			converted_line_buf = makeStringInfo();
			attribute_buf = makeStringInfo();
			dis_bitmap = bms_make_empty(session->fields_num);
			
			for (i = 0; i < session->dis_key_num; ++i)
				bms_add_member(dis_bitmap, session->dis_key[i]);
			if (cpara->need_transcoding)
			{
				const char *to_enc = get_encoding_text(GetDatabaseEncoding());
				const char *from_enc = get_encoding_text(cpara->file_encoding);
				
				cd = iconv_open(to_enc, from_enc);
				if (cd == (iconv_t) -1)
				{
					pthread_mutex_lock(&session->err_mutex);
					if (!session->is_error)
					{
						session->errmsg = apr_psprintf(session->pool,
						                               "batch_process_thread_%d: Allocates descriptor for code conversion from %s to %s failed",
						                               bt_para->j,
						                               from_enc,
						                               to_enc);
						session->is_error = 1;
						gdebug(NULL, session->errmsg);
					}
					pthread_mutex_unlock(&session->err_mutex);
					goto cleanup;
				}
			}
		}
		
		for (msg_i = 0; msg_i < bt_para->num_consumed; msg_i++)
		{
			rd_kafka_message_t *message = bt_para->messages[msg_i];
			Assert(message);
			
			/* Check if other thread error before processing message, exit directly. */
			if (session->is_error)
			{
				gdebug(NULL,
				       "batch_process_thread_%d: %s",
				       bt_para->j,
				       session->errmsg ? session->errmsg : "internal error");
				goto cleanup;
			}
			
			if (message->err)
			{
				if (message->err != RD_KAFKA_RESP_ERR__PARTITION_EOF)
				{
					/* exit directly while meeting kafka error */
					pthread_mutex_lock(&session->err_mutex);
					if (!session->is_error)
					{
						session->errmsg = apr_psprintf(session->pool,
						                               "batch_process_thread_%d: librdkafka error: %s",
						                               bt_para->j, rd_kafka_err2str(message->err));
						session->is_error = 1;
						gdebug(NULL, "%s", session->errmsg);
					}
					pthread_mutex_unlock(&session->err_mutex);
					goto cleanup;
				}
				else
				{
					/* Ignore partition EOF internal error */
					gprintln(NULL, "batch_process_thread_%d: topic(%s) prt(%d) consumed EOF msg.",
							 bt_para->j, session->topic_name, prt);
					
					rd_kafka_message_destroy(message);
					bt_para->messages[msg_i] = NULL;
					goto eof;
				}
			}
			else if (message->len)
			{
				bool result = false;
				resetStringInfo(line_buf);
				if (session->is_logical_sync)
				{
					result = logical_message_handle(session, ret, values, nulls, in_functions,
					                                message, line_buf, &shardid);
				}
				else
				{
					appendBinaryStringInfo(line_buf, message->payload, message->len);
					
					result = text_message_handle(session, cd, &raw_fields,
					                             line_buf, converted_line_buf,
					                             attribute_buf, dis_bitmap,
					                             values, nulls, in_functions,
					                             &iconv_out, &row_size, &shardid);
					
					/* append eol at the end of line_buf */
					if (cpara->eol_len > 0)
						appendBinaryStringInfo(line_buf, cpara->eol, cpara->eol_len);
					else
						appendBinaryStringInfo(line_buf, "\n", 1);
				}
				
				if (!result || !ShardIDIsValid(shardid))
				{
					gdebug(NULL, "batch_process_thread_%d: Fail to evaluate shardid, errmsg %s, kafka msg \"%s\"",
					       bt_para->j, session->errmsg ? session->errmsg : " ", message->payload);
					goto cleanup;
				}
				
				/* locate the DN to send, then append it to buffer, need lock when appending */
				nodeid = session->shardmap[shardid];
				
				// int start_p = l_cache_buf[nodeid]->len;
				/* if l_cache reach to next block max size */
				/* position: 0..................opt.m|int +int +char.....opt.m*2|int +int +char.....opt.m*3| */
				/* memory  : data....................|len+nmsgs+data............|len+nmsgs+data............|len+nmsgs */
				if ((l_cache_buf[nodeid]->len - l_cache_buf[nodeid]->cursor + sizeof(int) * 2) + line_buf->len >= opt.m)
				{
					// size_t size = l_cache_buf[nodeid]->len - l_cache_buf[nodeid]->cursor - sizeof(int) * 2;
					size_t size = l_cache_buf[nodeid]->len - l_cache_buf[nodeid]->cursor;
					int remaining_size_in_block = opt.m - l_cache_buf[nodeid]->len % opt.m;
					
					enlargeStringInfo(l_cache_buf[nodeid], remaining_size_in_block);
					memset(l_cache_buf[nodeid]->data + l_cache_buf[nodeid]->len, '0', remaining_size_in_block);
					l_cache_buf[nodeid]->len += remaining_size_in_block;
					l_cache_buf[nodeid]->data[l_cache_buf[nodeid]->len] = '\0';
					// l_cache_buf[nodeid]->cursor = l_cache_buf[nodeid]->len;
					
					appendBinaryStringInfo(l_cache_buf[nodeid], (char *) &size, sizeof(int));
					appendBinaryStringInfo(l_cache_buf[nodeid], (char *) &(num_messages[nodeid]), sizeof(int));
					l_cache_buf[nodeid]->cursor = l_cache_buf[nodeid]->len;
					
					num_messages[nodeid] = 0;
				}
				
				appendBinaryStringInfo(l_cache_buf[nodeid], line_buf->data, line_buf->len);
				num_messages[nodeid]++;
				Assert(message->offset >= offsets);
				offsets = message->offset + 1;
			}
			
			rd_kafka_message_destroy(message);
			bt_para->messages[msg_i] = NULL;
		}
	} while (num_consumed_already < bt_para->batch_size);
	
eof:
	gdebug(NULL, "batch_process_thread_%d: Totally consume %d msgs from topic(%s) prt(%d).",
	         bt_para->j, num_consumed_already, session->topic_name, prt);
	
	/* append last block's size and nmsgs to l_cache_buf */
	for (nodeid = 0; nodeid < session->maxsegs; nodeid++)
	{
		size_t size;
		int remaining_size_in_block;

		/* no data left in l_cache_buf for nodeid */
		if (l_cache_buf[nodeid]->len <= l_cache_buf[nodeid]->cursor)
			continue;
		
		size = l_cache_buf[nodeid]->len - l_cache_buf[nodeid]->cursor;
		
		/* Align to opt.m*n with "0" */
		remaining_size_in_block = opt.m - l_cache_buf[nodeid]->len % opt.m;
		enlargeStringInfo(l_cache_buf[nodeid], remaining_size_in_block);
		memset(l_cache_buf[nodeid]->data + l_cache_buf[nodeid]->len, '0', remaining_size_in_block);
		l_cache_buf[nodeid]->len += remaining_size_in_block;
		l_cache_buf[nodeid]->data[l_cache_buf[nodeid]->len] = '\0';
		
		appendBinaryStringInfo(l_cache_buf[nodeid], (char *)&size, sizeof(int));
		appendBinaryStringInfo(l_cache_buf[nodeid], (char *)&(num_messages[nodeid]), sizeof(int));
		// l_cache_buf[nodeid]->cursor = l_cache_buf[nodeid]->len;
		
		num_messages[nodeid] = 0;
		l_cache_buf[nodeid]->cursor = sizeof(int) * 2;
	}
	
	/* 2. Waiting for the previous thread to exit to ensure messages are stored in order in shared buffer. */
	sem_wait(bt_para->pre_finish_sem);
	gdebug(NULL, "batch_process_thread_%d: get pre_finish_sem sem", bt_para->j);
	
	/* 3. send all data cached in local buffer for each node to shared buffer */
	nodeid = 0;
	do
	{
		nodeid = nodeid % session->maxsegs;
		
		s_redist_buff = session->redist_buff[nodeid];
		buffer_mutex = &session->buffer_mutex[nodeid];
		append_cond = &session->append_cond[nodeid];
		send_cond = &session->send_cond[nodeid];
		
		/* copy to shared buffer when there is data in local buffer. */
		if (l_cache_buf[nodeid]->len - l_cache_buf[nodeid]->cursor >= opt.m)
		{
			int32 size = 0;
			int32 num_message = 0;
			
			/* move size of opt.m from local buffer to shared buffer */
			pthread_mutex_lock(buffer_mutex);
			gdebug(NULL, "batch_process_thread_%d Lock node %d", bt_para->j, nodeid);
			
			while (s_redist_buff->is_full)
			{
				if (session->is_error)
				{
					gdebug(NULL,
					       "batch_process_thread_%d before append data to shared block ...", bt_para->j);
					pthread_mutex_unlock(buffer_mutex);
					goto cleanup;
				}
				
				/* WHILE: other consumer_thread may have filled the buffer. */
				gdebug(NULL, "batch_process_thread_%d wait for node %d append signal", bt_para->j, nodeid);
				pthread_cond_wait(append_cond, buffer_mutex);
				gdebug(NULL, "batch_process_thread_%d get node %d append signal", bt_para->j, nodeid);
			}
			
			/* 1.1 copy block size */
			memcpy(&size, l_cache_buf[nodeid]->data + l_cache_buf[nodeid]->cursor + opt.m - 2 * sizeof(int32), sizeof(int32));
			
			/* 1.2 copy number of messages */
			memcpy(&num_message, l_cache_buf[nodeid]->data + l_cache_buf[nodeid]->cursor + opt.m - sizeof(int32), sizeof(int32));
			
			// gprintln(NULL, "batch_process_thread_%d nodeid %d, cursor %d", bt_para->j, nodeid, l_cache_buf[nodeid]->cursor);
			// gprintln(NULL, "batch_process_thread_%d shared buffer size %d", bt_para->j, size);
			// gprintln(NULL, "batch_process_thread_%d shared num_message %d", bt_para->j, num_message);
			// gprintln(NULL, "batch_process_thread_%d shared buffer %s", bt_para->j, l_cache_buf[nodeid]->data + l_cache_buf[nodeid]->cursor);
			
			/* 2. copy block data */
			memcpy(s_redist_buff->outblock.data + s_redist_buff->outblock.top, l_cache_buf[nodeid]->data + l_cache_buf[nodeid]->cursor, size);
			s_redist_buff->outblock.top += size;
			
			gdebug(NULL, "batch_process_thread_%d: append %d msgs to node %d's shared buffer", bt_para->j, num_message, nodeid);
			
			l_cache_buf[nodeid]->cursor += opt.m;
			
			/* set full flag and active send signal */
			s_redist_buff->num_messages = num_message;
			
			if (offsets > session->offsets[prt])
			{
				gdebug(NULL, "batch_process_thread_%d: update shared topic(%s) prt(%d) offset from %d to %d",
				       bt_para->j, session->topic_name, prt, session->offsets[prt], offsets);
				session->offsets[prt] = offsets;
			}
			
			gdebug(NULL, "batch_process_thread_%d append %d bytes %d msgs to shared node %d's buffer.",
			       bt_para->j, size, num_message, nodeid);
			
			s_redist_buff->is_full = true;
			gdebug(NULL, "batch_process_thread_%d: node %d output buffer is full, wait for consuming done.", bt_para->j, nodeid);
			pthread_cond_broadcast(send_cond);
			gdebug(NULL, "batch_process_thread_%d active node %d send signal", bt_para->j, nodeid);
			
			pthread_mutex_unlock(buffer_mutex);
			gdebug(NULL, "batch_process_thread_%d unLock node %d", bt_para->j, nodeid);
		}
		else if (!bms_is_member(nodeid, done))
		{
			bms_add_member(done, nodeid);
		}
		
		nodeid ++;
	} while (bms_num_members(done) < session->maxsegs);
cleanup:
	for (msg_i = 0; msg_i < bt_para->num_consumed; msg_i++)
	{
		if (bt_para->messages[msg_i])
		{
			rd_kafka_message_destroy(bt_para->messages[msg_i]);
			bt_para->messages[msg_i] = NULL;
		}
	}
	// pthread_mutex_lock(&session->fs_mutex);
	/* would block rd_kafka_destroy */
	rd_kafka_consume_stop(bt_para->rkt, prt);
	// if (PointerIsValid(bt_para->rk))
	// {
	// 	rd_kafka_consumer_close(bt_para->rk);
	// 	rd_kafka_destroy(bt_para->rk);
	// 	bt_para->rk = NULL;
	// }
	//
	// if (PointerIsValid(bt_para->rkt))
	// {
	// 	rd_kafka_topic_destroy(bt_para->rkt);
	// 	bt_para->rkt = NULL;
	// }
	// pthread_mutex_unlock(&session->fs_mutex);
	if (!session->is_logical_sync)
	{
		bms_free(dis_bitmap);
		if (cd != (iconv_t) -1)
			iconv_close(cd);
		pfree(iconv_out);
		pfree(raw_fields);
		if (converted_line_buf != NULL)
		{
			pfree(converted_line_buf->data);
			pfree(converted_line_buf);
		}
		if (attribute_buf != NULL)
		{
			pfree(attribute_buf->data);
			pfree(attribute_buf);
		}
	}
	
	pfree(values);
	pfree(nulls);
	pfree(in_functions);
	for (nodeid = 0; nodeid < session->maxsegs; ++nodeid)
	{
		pfree_ext(l_cache_buf[nodeid]->data);
		pfree_ext(l_cache_buf[nodeid]);
	}
	pfree_ext(l_cache_buf);
	pfree(num_messages);
	pfree_ext(line_buf->data);
	pfree_ext(line_buf);
	pfree(done);

	/* memory in array is handled logical_message_handle */
	pfree(ret->raw_fields_before);
	pfree(ret->raw_fields_after);
	pfree(ret->isnull_before);
	pfree(ret->isnull_after);
	pfree(ret);
	pfree(thrinfo);
	SetTDXThreadInfo(NULL);
	
	/* notify next thread to write shared buffer. */	
	sem_post(bt_para->cur_finish_sem);
	sem_destroy(bt_para->pre_finish_sem);
	/* reset used flag. this mem space could be reused by other thread */
	bt_para->used = false;
}

static void*
consumer_thread(void *para)
{
	read_thread_para *t_para = (read_thread_para *) para;
	session_t *session = t_para->session;
	pthread_mutex_t *err_mutex = &session->err_mutex;
	int prt_no_mod = t_para->j;
	int p, prt = 0;
	int my_partitions = 0;
	int i, j = 0;
	int loop = 0;
	// apr_time_t t3, t4;
	
	// t3 = apr_time_now();
	/* Begin consuming all partitions that this process is responsible for */
	for (p = 0; p < session->partition_cnt; p++)
	{
		int needed_msg = session->rk_messages_size;	/* signed needed_msg */
		int bt_thread_id = 0;
		int64_t start_offset = RD_KAFKA_OFFSET_BEGINNING;
		int64_t log_start_offset = RD_KAFKA_OFFSET_BEGINNING;
		prt = session->partition_ids[p];
		
		Assert(prt < topic_metap->partition_cnt);
		if (prt % session->partition_cnt != prt_no_mod)
			continue;
		
		pthread_mutex_lock(&session->fs_mutex);
		
		/* compare with offset get from kernel and update */
		if (start_offset < session->offsets[prt])
			start_offset = session->offsets[prt];
		gprintlnif(NULL, "[sid - %ld] KAFKA-CONSUME: consuming (%s) partition %d from offset %ld, log_start_offset %ld",
		           session->id, session->topic_name, prt, start_offset, log_start_offset);

		session->active_prts ++;
		pthread_mutex_unlock(&session->fs_mutex);
		my_partitions++;
		
		/* loop until num_consumed reaches needed_msg */
		sem_init(t_para->last_finish_sem, 0, 1);
		do
		{
			/* prepare process threads parameter */
			batch_thread_para *bt_para = NULL;
			bt_thread_id = (loop % t_para->num_batch_thread);
			bt_para = t_para->bt_paras[bt_thread_id];
			
			pthread_mutex_lock(&bt_para->u_mutex);
			/* maybe this mem space has been bind to some other thread, wait until that thread exit */
			while (bt_para->used)
			{
				pthread_mutex_unlock(&bt_para->u_mutex);
				pg_usleep(10);
				bt_thread_id++;
				bt_thread_id = bt_thread_id % t_para->num_batch_thread;
				bt_para = t_para->bt_paras[bt_thread_id];
				pthread_mutex_lock(&bt_para->u_mutex);
			}
			bt_para->used = true;
			bt_para->j = bt_thread_id;
			bt_para->prt_no = prt;
			bt_para->pre_finish_sem = t_para->last_finish_sem;
			/* Allocate memory for cur_finish_sem before start thread,which will be destroyed in batch_process_thread. */
			bt_para->cur_finish_sem = pcalloc_safe(NULL,
			                                       session->pool,
			                                       sizeof(sem_t),
			                                       "failed to allocated cur_finish_sem");
			bt_para->start_offset = start_offset + loop * session->rk_messages_size;
			
			/* only consume need number of msgs. */
			if (session->rk_messages_size > needed_msg)
				bt_para->batch_size = needed_msg;
			else
				bt_para->batch_size = session->rk_messages_size;

			/* start process threads*/			
			sem_init(bt_para->cur_finish_sem, 0, 0);
			bt_para->session = session;
			pthread_mutex_unlock(&bt_para->u_mutex);
			
			if (threadpool_add(threadpool, &batch_process_thread, (void *) bt_para, 0) != 0)
			{
				/* wait for the last batch_process_thread end */
				sem_wait(t_para->last_finish_sem);
				gdebug(NULL, "[sid - %d] KAFKA-CONSUME: get last_finish_sem sem", session->id);
				sem_destroy(t_para->last_finish_sem);
				
				/* something wrong? */
				pthread_mutex_lock(err_mutex);
				if (!session->is_error)
				{
					session->errmsg = apr_psprintf(session->pool,
					                               "KAFKA-CONSUME: start parallel msg process worker failed.");
					session->is_error = 1;
					gdebug(NULL, "%s", session->errmsg);
				}
				pthread_mutex_unlock(err_mutex);
				goto cleanup;
			}
			
			t_para->last_finish_sem = bt_para->cur_finish_sem;
			needed_msg -= bt_para->batch_size;
			loop ++;
		} while (needed_msg > 0);
		
		/* wait for all batch_p_thread end */
		sem_wait(t_para->last_finish_sem);
		gdebug(NULL, "[sid - %d] KAFKA-CONSUME: get last_finish_sem sem", session->id);
		sem_destroy(t_para->last_finish_sem);
		gprintln(NULL, "[sid - %d] KAFKA-CONSUME: consume topic(%s) prt(%d) (%ld) msgs within (%d) loops totally.", 
				 session->id, session->topic_name, prt, session->rk_messages_size - needed_msg, loop);
	}
	// t4 = apr_time_now();
	// gprintln(NULL, "[sid - %d] KAFKA-CONSUME: topic(%s) prt(%d) total processing time %ldms.",
	//          session->id, session->topic_name, prt, datetime_diff(t3, t4));
	
	/* No point doing anything if we don't have any partitions assigned to us */
	if (my_partitions == 0)
	{						
		pthread_mutex_lock(err_mutex);
		if (!session->is_error)
		{
			session->errmsg = apr_psprintf(session->pool, "KAFKA-CONSUME: no partitions to read.");
			session->is_error = 1;
			gdebug(NULL, session->errmsg);
		}
		pthread_mutex_unlock(err_mutex);
		goto cleanup;
	}

cleanup:
	/* parallel process threads */
	for (i = 0; i < t_para->num_batch_thread; ++i)
	{
		if (PointerIsValid(t_para->bt_paras[i]->rk))
		{
			rd_kafka_consumer_close(t_para->bt_paras[i]->rk);
			rd_kafka_destroy(t_para->bt_paras[i]->rk);
			t_para->bt_paras[i]->rk = NULL;
		}
		
		if (PointerIsValid(t_para->bt_paras[i]->rkt))
		{
			rd_kafka_topic_destroy(t_para->bt_paras[i]->rkt);
			t_para->bt_paras[i]->rkt = NULL;
		}
		
		if (!t_para->bt_paras[i]->messages)
			continue;
		
		for (j = 0; j < t_para->bt_paras[i]->batch_size; j++)
		{
			if (t_para->bt_paras[i]->messages[j])
			{
				rd_kafka_message_destroy(t_para->bt_paras[i]->messages[j]);
				t_para->bt_paras[i]->messages[j] = NULL;
			}
		}
		pfree(t_para->bt_paras[i]->messages);
	}
	
	for (p = 0; p < session->partition_cnt && my_partitions > 0; p++)
	{
		prt = session->partition_ids[p];
		if (prt % session->partition_cnt != prt_no_mod)
			continue;
		
		my_partitions --;
		pthread_mutex_lock(&session->fs_mutex);
		session->active_prts --;
		pthread_mutex_unlock(&session->fs_mutex);
	}
	
	gprintln(NULL, "[sid - %ld] consumer_thread_%d exit.", session->id, prt_no_mod);
	pthread_exit(NULL);
}
