/*-------------------------------------------------------------------------
 *
 * fnconn.c
 *
 *	  Communication functions between the forward manager and session
 *
 *
 * Portions Copyright (c) 1996-2009, PostgreSQL Global Development Group
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 *
 *-------------------------------------------------------------------------
 */
#include <sys/epoll.h>
#include <arpa/inet.h>
#include <sys/prctl.h>

#include "postgres.h"
#include "miscadmin.h"

#include "../interfaces/libpq/libpq-int.h"
#include "../interfaces/libpq/pqexpbuffer.h"

#include "common/ip.h"
#include "forward/fnbuf_internals.h"
#include "forward/fnconn.h"
#include "forward/fnbufmgr.h"
#include "libpq/libpq.h"
#include "postmaster/forward.h"
#include "postmaster/postmaster.h"
#include "storage/ipc.h"
#include "storage/proc.h"

#include "pgxc/squeue.h"

#define TEMP_BUFFER_LEN (8)
#define FN_NEED_COMPRESS_DATA_LEN (256 + SizeOfFnPageHeaderData)

int FnSendBulkSize;
int fn_buffer_queue_len;
int fn_send_thread_num;

static int fn_send_bulk_bytes = 0;
static int fn_keepalives_interval = 5;
static int fn_keepalives_count = 3;
static int fn_keepalives_idle = 20;
static ForwardConn *fn_cn_conns = NULL;
static ForwardConn *fn_dn_conns = NULL;
static int fn_cn_count = 0;
static int fn_dn_count = 0;
static int fn_fail_cn_count = 0;

#define Socket(port) (port).fdsock

static int g_epollfd = 0;
static int maxfd;
static struct epoll_event *events;

static int fn_index;
NodeDefinition		*fn_myself;

typedef struct FnPageBuffer
{
    FnPage page;
	FnBufferDesc *buf_desc;
	ForwardConn *conn;
	pg_atomic_uint32 *num_to_send;
    struct FnPageBuffer *next;
} FnPageBuffer;

typedef struct
{
    pthread_mutex_t mutex;
    pthread_cond_t cond;
    FnPageBuffer *head;
    FnPageBuffer *tail;
    int buffer_count;
	int index;
	int timeline;
	bool closed;
} FnSendBufferQueue;

static int fn_timeline = 0;
static int fn_forked_thread_num = 0;
static FnSendBufferQueue *buffer_queues = NULL;
static pthread_t *send_threads = NULL;
static pthread_mutex_t log_mutex = PTHREAD_MUTEX_INITIALIZER;

void *senderFunc(void *arg);
static void freeFnPageBuffer(FnPage page, FnBufferDesc *buf_desc);

/*
 * Set the name of the calling thread, 
 * The name can be up to 16 bytes long, 
 * and should be null-terminated if it contains fewer bytes.
 *
 * ps -L -p $pid
 */
void
fn_set_thrd_name(const char * name)
{
	prctl(PR_SET_NAME, name, NULL, NULL, NULL);
}

void
flog(const char *format,...)
{
    pg_time_t stamp_time;
    struct timeval tv;
    struct pg_tm tm_info;
    char time_buffer[64];
    int milliseconds;
	char msbuf[13];
	va_list args;

    gettimeofday(&tv, NULL);
	stamp_time = (pg_time_t) tv.tv_sec;
    pg_localtime_r(&stamp_time, log_timezone, &tm_info);
    pg_strftime(time_buffer, sizeof(time_buffer), "%Y-%m-%d %H:%M:%S     %Z", &tm_info);

	/* 'paste' milliseconds into place... */
	milliseconds = tv.tv_usec / 1000;
	sprintf(msbuf, ".%03d", milliseconds);
	memcpy(time_buffer + 19, msbuf, 4);

    pthread_mutex_lock(&log_mutex);

    fprintf(stderr, "%s [%d] ", time_buffer, getpid());

    va_start(args, format);
    vfprintf(stderr, format, args);
    va_end(args);

	fputc('\n', stderr);

    pthread_mutex_unlock(&log_mutex);
}

static void
buffer_queue_init(FnSendBufferQueue *queue)
{
    pthread_mutex_init(&queue->mutex, NULL);
    pthread_cond_init(&queue->cond, NULL);
    queue->head = NULL;
    queue->tail = NULL;
    queue->buffer_count = 0;
    queue->closed = false;
}

static void
free_buffer(FnPageBuffer *buffer)
{
	if (NULL == buffer->num_to_send)
	{
		freeFnPageBuffer(buffer->page, buffer->buf_desc);
	}
	else
	{
		if (pg_atomic_sub_fetch_u32(buffer->num_to_send, 1) == 0)
		{
			freeFnPageBuffer(buffer->page, buffer->buf_desc);
			free(buffer->num_to_send);
		}
	}

	free(buffer);
}

static void
buffer_queue_clean(FnSendBufferQueue *queue)
{
	pthread_mutex_lock(&queue->mutex);

    while (queue->buffer_count > 0)
    {
        FnPageBuffer *buffer = queue->head;
        queue->head = buffer->next;
        queue->buffer_count--;

        free_buffer(buffer);
    }

    queue->head = NULL;
    queue->tail = NULL;

	pthread_mutex_unlock(&queue->mutex);
}

static bool
buffer_queue_push(FnSendBufferQueue *queue, FnPageBuffer *buffer)
{
    pthread_mutex_lock(&queue->mutex);

	if (queue->closed || queue->buffer_count > fn_buffer_queue_len)
	{
		pthread_cond_signal(&queue->cond);
		pthread_mutex_unlock(&queue->mutex);
		return false;
	}

    if (queue->buffer_count == 0)
	{
        queue->head = buffer;
        queue->tail = buffer;
    }
	else
	{
        queue->tail->next = buffer;
        queue->tail = buffer;
    }
	
    queue->buffer_count++;
    pthread_cond_signal(&queue->cond);
    pthread_mutex_unlock(&queue->mutex);

	return true;
}

static FnPageBuffer *
buffer_queue_pop(FnSendBufferQueue *queue)
{
	FnPageBuffer *buffer;

    pthread_mutex_lock(&queue->mutex);
    while (queue->buffer_count == 0) 
	{
		if (queue->timeline != fn_timeline)
		{
			pthread_mutex_unlock(&queue->mutex);
			return NULL;
		}
		
        pthread_cond_wait(&queue->cond, &queue->mutex);
    }

    buffer = queue->head;
    queue->head = buffer->next;
    queue->buffer_count--;
    pthread_mutex_unlock(&queue->mutex);

    return buffer;
}

static bool
SendDataInThread(int fd, char *bufptr, int len)
{
	char *bufend = bufptr + len;

	while (bufptr < bufend)
	{
		int	r = -1;

		r = send(fd, bufptr, bufend - bufptr, 0);

		if (r < 0)
		{
			if (errno == EINTR)
			{
				continue;        /* Ok if interrupted */
			}
			else
			{
				return false;
			}
		}

		bufptr += r;
	}

	return true;
}

static void
freeFnPageBuffer(FnPage page, FnBufferDesc *buf_desc)
{
	uint32 buf_state;

	FnPageReset(page);

	buf_state = LockFnBufHdr(buf_desc);
	/* clear the dirty and other flags */
	buf_state &= ~(BM_DIRTY | BM_FLUSH);
	UnlockFnBufHdr(buf_desc, buf_state);

	FnBufferFree(buf_desc);
}

void *
senderFunc(void *arg)
{
	FnPageBuffer *fn_buffer;
	FnSendBufferQueue *buffer_queue = (FnSendBufferQueue *)arg;
	char thrd_name[NAMEDATALEN] = {0};
	FnPage page;
	FnPageHeader head;
	ForwardConn *conn;
	bool send_ret;
	sigset_t mask;

	/* add sub thread longjmp check */
	SUB_THREAD_LONGJMP_DEFAULT_CHECK();

    sigfillset(&mask);
    pthread_sigmask(SIG_BLOCK, &mask, NULL);

	sprintf(thrd_name, "sender-%d", buffer_queue->index);

	fn_set_thrd_name(thrd_name);

	while (true)
	{
		fn_buffer = buffer_queue_pop(buffer_queue);
		if (NULL == fn_buffer)
		{
			break;
		}

		page = fn_buffer->page;
		head = (FnPageHeader) page;

		conn = fn_buffer->conn;

		if (NULL == conn->port || buffer_queue->timeline != fn_timeline)
		{
			free(fn_buffer);
			break;
		}

		if (head->lower > BLCKSZ)
		{
			flog("[senderFunc] invalid lower %d index %d fd %d", head->lower, buffer_queue->index, Socket(*conn->port));
			continue;
		}
		
		send_ret = SendDataInThread(Socket(*conn->port), (char *)head, head->lower);

		if (!send_ret)
		{
			free(fn_buffer);
			
			buffer_queue->closed = true;
			
			flog("req reconnect because send data in thread failed index %d ret %d msg %m", buffer_queue->index, send_ret);

			ReqReconnect();
			break;
		}

		if (NULL == fn_buffer->num_to_send)
		{
			freeFnPageBuffer(page, fn_buffer->buf_desc);
		}
		else
		{
			if (pg_atomic_sub_fetch_u32(fn_buffer->num_to_send, 1) == 0)
			{
				freeFnPageBuffer(page, fn_buffer->buf_desc);
				free(fn_buffer->num_to_send);
			}
		}

		free(fn_buffer);
	}

    return NULL;
}

/*
 * Connect to remote forward receiver listening on specified port
 */
static int
fnconn_connect_remote(ForwardConn *conn, bool iscoord)
{
	int retry_times = 0;
	ForwardPort *port = conn->port;
	char sebuf[256] = { 0 };
	int n;
	FnMgrStartupPacket packet;
	bool use_tcp = true;

#ifdef HAVE_UNIX_SOCKETS
	use_tcp = strcmp(NameStr(conn->node.nodehost), NameStr(fn_myself->nodehost)) != 0;
#endif

	/* CONNECTION_NEEDED */
	do
	{
		struct addrinfo hint;
		char 			portstr[MAXPGPATH];
		int				syncnt = 2;
		int				ret = 0;
		int				status = 0;

		/* Initialize hint structure */
		MemSet(&hint, 0, sizeof(hint));
		hint.ai_socktype = SOCK_STREAM;
		hint.ai_family = use_tcp ? AF_UNSPEC : AF_UNIX;

		if (use_tcp)
		{
			if (strcmp("localhost", NameStr(fn_myself->nodehost)) == 0)
				return STATUS_ERROR;
			
			snprintf(portstr, sizeof(portstr), "%d", conn->node.nodeforwardport);
			ret = pg_getaddrinfo_all(NameStr(conn->node.nodehost), portstr, &hint, &(conn->addr));
		}
		else
		{
			UNIXSOCK_PATH(portstr, conn->node.nodeforwardport, Unix_socket_directories);
			ret = pg_getaddrinfo_all(NULL, portstr, &hint, &(conn->addr));
		}

		if (ret != 0 || conn->addr == NULL)
		{
			elog(ERROR, "could not translate host name \"%s\" to address: err=%s",
				 NameStr(conn->node.nodehost), gai_strerror(ret));
			return STATUS_ERROR;
		}

		port->fdsock = socket(conn->addr->ai_family, SOCK_STREAM, 0);
		if (port->fdsock == PGINVALID_SOCKET)
		{
			elog(ERROR, "could not create socket for node: conn_node=%s, "
						"conn_host=%s, conn_port=%d, err=%s",
				 NameStr(conn->node.nodename), NameStr(conn->node.nodehost),
				 conn->node.nodeforwardport,
				 SOCK_STRERROR(SOCK_ERRNO, sebuf, sizeof(sebuf)));
			return STATUS_ERROR;
		}

		if (use_tcp)
		{
			int bind_ret = -1;
			if (conn->addr->ai_family == AF_INET)
			{
				struct sockaddr_in local_addr;
				memset(&local_addr, 0, sizeof(local_addr));
				local_addr.sin_family = AF_INET;
				local_addr.sin_addr.s_addr = inet_addr(NameStr(fn_myself->nodehost));
				bind_ret = bind(port->fdsock, (struct sockaddr *)&local_addr, sizeof(local_addr));
			}
			else
			{
				struct sockaddr_in6 local_addr;
				memset(&local_addr, 0, sizeof(local_addr));
				local_addr.sin6_family = AF_INET6;
				inet_pton(AF_INET6, NameStr(fn_myself->nodehost), &local_addr.sin6_addr);
				bind_ret = bind(port->fdsock, (struct sockaddr *)&local_addr, sizeof(local_addr));
			}

			if (bind_ret < 0)
			{
				elog(ERROR, "could not bind local_addr=%s to connect remote node "
							"conn_node=%s, conn_host=%s, conn_port=%d, err=%s",
					NameStr(fn_myself->nodehost),
					NameStr(conn->node.nodename), NameStr(conn->node.nodehost),
					conn->node.nodeforwardport,
					SOCK_STRERROR(SOCK_ERRNO, sebuf, sizeof(sebuf)));
				return STATUS_ERROR;
			}

			if (setsockopt(port->fdsock, IPPROTO_TCP, TCP_SYNCNT,
						(char *) &syncnt,
						sizeof(syncnt)) < 0)
			{
				elog(ERROR, "could not set socket to TCP no syncnt for node: "
							"conn_node=%s, conn_host=%s, conn_port=%d, err=%s",
					NameStr(conn->node.nodename), NameStr(conn->node.nodehost),
					conn->node.nodeforwardport,
					SOCK_STRERROR(SOCK_ERRNO, sebuf, sizeof(sebuf)));
				return STATUS_ERROR;
			}

#ifndef WIN32
			SetSockKeepAlive(port->fdsock, fn_keepalives_interval, fn_keepalives_count, fn_keepalives_idle);
#endif
		}

#ifdef SO_NOSIGPIPE
		do
		{
			int optval = 1;
			setsockopt(port->fdsock, SOL_SOCKET, SO_NOSIGPIPE,
					   (char *) &optval, sizeof(optval);
		} while(0);
#endif	/* SO_NOSIGPIPE */

		/*
		 * Start/make connection.  This should not block, since we
		 * are in nonblock mode.  If it does, well, too bad.
		 */
retry:
		if (forward_is_shutdown_requested())
			return STATUS_ERROR;

		fnmgr_process_sigusr1();

		if (forward_is_pgxc_pool_reloaded())
			return STATUS_ERROR;

		if (connect(port->fdsock,
					conn->addr->ai_addr,
					conn->addr->ai_addrlen) < 0)
		{
			if (SOCK_ERRNO == EINPROGRESS ||
				#ifdef WIN32
				SOCK_ERRNO == EWOULDBLOCK ||
				#endif
				SOCK_ERRNO == EINTR)
			{
				/*
				 * This is fine - we're in non-blocking mode, and
				 * the connection is in progress.  Tell caller to
				 * wait for write-ready on socket.
				 */
				if (SOCK_ERRNO == EINTR)
					goto retry;
			}
			else
			{
				/* log per 5 seconds */
				if (retry_times % 50 == 0)
				{
					/* otherwise, trouble */
					elog(LOG, "could not connect to server: %s\n"
							"\tIs the server running on host \"%s\" (%s) and accepting\n"
							"\tTCP/IP connections on port %d?",
						NameStr(conn->node.nodehost),
						NameStr(conn->node.nodehost),
						SOCK_STRERROR(SOCK_ERRNO, sebuf, sizeof(sebuf)),
						conn->node.nodeforwardport);
					elog(LOG, "could not connect to node: "
							"conn_node=%s, conn_host=%s, conn_port=%d",
						NameStr(conn->node.nodename), NameStr(conn->node.nodehost),
						conn->node.nodeforwardport);
				}

				if (iscoord && retry_times > 3)
					return STATUS_ERROR;
				
				/*
				 * FNREFACTOR TODO
				 * maybe we haven't startup the node yet, just retry for now..
				 */
				pg_usleep(100000L);
				retry_times++;
				goto retry;
			}
		}

		packet.node = *fn_myself;
		packet.index = fn_index;
		StrNCpy(packet.identify, PG_VERSION, NAMEDATALEN);

		/* send startup packet to server */
		n = SendData(port->fdsock, (char *)&packet, sizeof(packet));
		if (n <= 0)
		{
			elog(ERROR, "could not send startup packet to node "
						"conn_node=%s, conn_host=%s, conn_port=%d, err=%s",
				 NameStr(conn->node.nodename), NameStr(conn->node.nodehost),
				 conn->node.nodeforwardport,
				 SOCK_STRERROR(SOCK_ERRNO, sebuf, sizeof(sebuf)));
			return STATUS_ERROR;
		}

		n = fn_thread_read_buffer(port->fdsock, (char *)&status, sizeof(status));
		if (n < 0)
		{
			elog(ERROR, "could not recv status from node "
						"conn_node=%s, conn_host=%s, conn_port=%d, err=%s",
				 NameStr(conn->node.nodename), NameStr(conn->node.nodehost),
				 conn->node.nodeforwardport,
				 SOCK_STRERROR(SOCK_ERRNO, sebuf, sizeof(sebuf)));
			return STATUS_ERROR;
		}

		if (status != 0)
		{
			elog(ERROR, "failed to check clien valid from node "
						"conn_node=%s, conn_host=%s, conn_port=%d, status=%d",
				 NameStr(conn->node.nodename), NameStr(conn->node.nodehost),
				 conn->node.nodeforwardport, status);
			return STATUS_ERROR;
		}
		
	} while(0);

	/* Success */
	return STATUS_OK;
}

/* add a mechanism to detect connection exceptions to rebuild problematic connections as soon as possible */
static void *
send_connection_checker_thread(void *args)
{
	char buf[TEMP_BUFFER_LEN];
	sigset_t mask;

	/* add sub thread longjmp check */
	SUB_THREAD_LONGJMP_DEFAULT_CHECK();

    sigfillset(&mask);
    pthread_sigmask(SIG_BLOCK, &mask, NULL);

	fn_set_thrd_name("connect_checker");

	while (true)
	{
		int retval = epoll_wait(g_epollfd, events, maxfd, -1);
		if (retval > 0)
		{
			int i = 0;
			for (i = 0; i < retval; i++)
			{
				ForwardPort *port = (ForwardPort *)events[i].data.ptr;
				if (events[i].events & EPOLLIN)
				{
					int fd = port->fdsock;

					retval = recv(fd, buf, TEMP_BUFFER_LEN, 0);
					if (retval == 0)
					{
						epoll_ctl(g_epollfd, EPOLL_CTL_DEL, fd, NULL);

						port->fdsock = PGINVALID_SOCKET;
						close(fd);

						flog("req reconnect because recv data return eof in checker thread");
						
						ReqReconnect();
					}
				}
			}
		}
	}

	return NULL;
}

static void
add_connection_to_checker(ForwardConn *conn)
{
	ForwardPort *port = conn->port;
	int fd = port->fdsock;
	struct epoll_event	event;
	event.data.ptr = port;
	event.events  = EPOLLIN | EPOLLERR | EPOLLHUP;

	if(epoll_ctl(g_epollfd, EPOLL_CTL_ADD, fd, &event) == -1)
	{
		close(fd);
		elog(ERROR, "ForwardSender: add conntion for %s:%d fd %d to epoll failed, %m",
					NameStr(conn->node.nodehost), conn->node.nodeforwardport, fd);
	}
}

bool
InitForwardConns(void)
{
	int             i;
	int             cn_count, dn_count;
	NodeDefinition *cn_defs, *dn_defs;

	fn_send_bulk_bytes = 1024 * FnSendBulkSize;
 
 	/* notify thread to exit */
	fn_timeline++;

	PgxcNodeGetAllDefinitions(&cn_defs, &cn_count, &dn_defs, &dn_count, &fn_myself, &fn_index);

	if (0 == g_epollfd)
	{
		int32 ret;

		maxfd = OPENTENBASE_MAX_DATANODE_NUMBER + OPENTENBASE_MAX_COORDINATOR_NUMBER;
		events = palloc0(maxfd * sizeof(struct epoll_event));
		g_epollfd = epoll_create(maxfd);

		CreateThread(send_connection_checker_thread, NULL, MT_THR_DETACHED, &ret);
		if (ret != 0)
		{
			elog(WARNING, "create thead for check send connection failed, ret %d", ret);
			return false;
		}
	}

    /* wait thread to exit */
	if (NULL != send_threads)
	{
		for (i = 0; i < fn_forked_thread_num; i++)
		{
			int ret;
			
			pthread_cond_signal(&buffer_queues[i].cond);

			ret = pthread_join(send_threads[i], NULL);
			if (ret != 0)
				elog(WARNING, "Failed to join thread dn:%d ret:%d", i, ret);
		}

		pfree(send_threads);
		send_threads = NULL;
	}

	/* close old connection for cn */
	if (NULL != fn_cn_conns)
	{
		for (i = 0; i < fn_cn_count; i++)
		{
			if (NULL != fn_cn_conns[i].port)
			{
				epoll_ctl(g_epollfd, EPOLL_CTL_DEL, fn_cn_conns[i].port->fdsock, NULL);
				close(fn_cn_conns[i].port->fdsock);
				pfree(fn_cn_conns[i].port);
				fn_cn_conns[i].port = NULL;
				if (NULL != fn_cn_conns[i].buffer && !fn_cn_conns[i].fail)
				{
					pfree(fn_cn_conns[i].buffer);
					fn_cn_conns[i].buffer = NULL;
				}
			}
		}
		pfree(fn_cn_conns);
		fn_cn_conns = NULL;
	}

	/* close old connection for dn */
	if (NULL != fn_dn_conns)
	{
		for (i = 0; i < fn_dn_count; i++)
		{
			if (NULL != fn_dn_conns[i].port)
			{
				epoll_ctl(g_epollfd, EPOLL_CTL_DEL, fn_dn_conns[i].port->fdsock, NULL);
				close(fn_dn_conns[i].port->fdsock);
				pfree(fn_dn_conns[i].port);
				fn_dn_conns[i].port = NULL;

				if (NULL != fn_dn_conns[i].buffer && !fn_dn_conns[i].fail)
				{
					pfree(fn_dn_conns[i].buffer);
					fn_dn_conns[i].buffer = NULL;
				}
			}
		}
		pfree(fn_dn_conns);
		fn_dn_conns = NULL;
	}

	fn_cn_count = cn_count;
	fn_fail_cn_count = 0;
	fn_cn_conns = palloc0(cn_count * sizeof(ForwardConn));
	for (i = 0; i < cn_count; i++)
	{
		fn_cn_conns[i].node = cn_defs[i];
		if (0 == fn_cn_conns[i].node.nodeforwardport)
			continue;

		fn_cn_conns[i].port = palloc0(sizeof(ForwardPort));
		/* port->fdsock and addr will be filled in fnconn_connect_remote */
		if (fnconn_connect_remote(&fn_cn_conns[i], true) == STATUS_OK)
		{
			add_connection_to_checker(&fn_cn_conns[i]);
			fn_cn_conns[i].buffer = palloc0(fn_send_bulk_bytes);
		}
		else
		{
			fn_fail_cn_count++;
			fn_cn_conns[i].fail = true;
			elog(WARNING, "failed to connect to forward receiver, name=%s fn_fail_cn_count %d",
				 NameStr(fn_cn_conns[i].node.nodename), fn_fail_cn_count);
			continue;
		}
	}

	if (fn_cn_count == fn_fail_cn_count)
	{
		pfree(cn_defs);
		pfree(dn_defs);
		return false;
	}
	
	fn_dn_count = dn_count;
	fn_dn_conns = palloc0(dn_count * sizeof(ForwardConn));
	for (i = 0; i < dn_count; i++)
	{
		fn_dn_conns[i].node = dn_defs[i];
		if (0 == fn_dn_conns[i].node.nodeforwardport)
			continue;

		fn_dn_conns[i].port = palloc0(sizeof(ForwardPort));
		/* port->fdsock and addr will be filled in fnconn_connect_remote */
		if (fnconn_connect_remote(&fn_dn_conns[i], false) == STATUS_OK)
		{
			add_connection_to_checker(&fn_dn_conns[i]);
			fn_dn_conns[i].buffer = palloc0(fn_send_bulk_bytes);
		}
		else
		{
			fn_dn_conns[i].fail = true;
			elog(WARNING, "failed to connect to forward receiver, name=%s",
				 NameStr(fn_dn_conns[i].node.nodename));
			pfree(cn_defs);
			pfree(dn_defs);
			return false;
		}
	}

	if (fn_send_thread_num >= 0)
	{
		if (NULL != buffer_queues)
		{
			for (i = 0; i < fn_forked_thread_num; i++)
			{
				buffer_queue_clean(&buffer_queues[i]);
			}
			pfree(buffer_queues);
			buffer_queues = NULL;
		}

		if (fn_send_thread_num == 0)
			fn_forked_thread_num = dn_count;
		else
			fn_forked_thread_num = fn_send_thread_num;

		buffer_queues = palloc0(fn_forked_thread_num * sizeof(FnSendBufferQueue));
		send_threads = palloc0(fn_forked_thread_num * sizeof(pthread_t));
		for (i = 0; i < fn_forked_thread_num; i++)
		{
			buffer_queue_init(&buffer_queues[i]);
			buffer_queues[i].index = i;
			buffer_queues[i].timeline = fn_timeline;

			pthread_create(&send_threads[i], NULL, senderFunc, &buffer_queues[i]);
		}
	}

	/* free array pointer, not it's content */
	pfree(cn_defs);
	pfree(dn_defs);

	return true;
}

void
TermFidBackendProc(const char *reason)
{
	/* terminer all query which has local_fid */
	int idx;
	for (idx = 0; idx < ProcGlobal->allProcCount; idx++)
	{
		PGPROC *proc = &ProcGlobal->allProcs[idx];
		if (proc->local_fid != -1 && proc->pid > 0)
		{
			elog(WARNING, "because %s, force kill SIGTERM to pid %d", reason, proc->pid);
			kill(proc->pid, SIGTERM);
		}
	}
}

bool
SendData(int fd, char *bufptr, int len)
{
	char *bufend = bufptr + len;

	while (bufptr < bufend)
	{
		int	r = -1;

		r = send(fd, bufptr, bufend - bufptr, 0);

		if (r < 0)
		{
			if (errno == EINTR)
			{
				continue;        /* Ok if interrupted */
			}
			else
			{
				elog(ERROR, "[%s:%d] send to server failed fd:%d msg:%m",
							(__FUNCTION__), (__LINE__), fd);
				return false;
			}
		}

		bufptr += r;
	}

	return true;
}

bool
SendFnPage(FnPage page, uint16 nodeid, FnBufferDesc *buf, pg_atomic_uint32 *num_to_send)
{
	bool send_to_cn = NodeIsCoordinator(nodeid);
	ForwardConn *conn;
	FnPageHeader head = (FnPageHeader) page;
	char	*bufptr;

	Assert(head->lower <= BLCKSZ);

	nodeid = UnMaskCoordinatorNode(nodeid);
	if (send_to_cn)
	{
		if (unlikely(nodeid >= fn_cn_count))
		{
			elog(WARNING, "[SendFnPage] should reconnect nodeid %d great then fn_cn_count %d", nodeid, fn_cn_count);
			return false;
		}
		
		conn = &fn_cn_conns[nodeid];
	}
	else
	{
		if (unlikely(nodeid >= fn_dn_count))
		{
			elog(WARNING, "[SendFnPage] should reconnect nodeid %d great then fn_dn_count %d", nodeid, fn_dn_count);
			return false;
		}

		conn = &fn_dn_conns[nodeid];
	}

	if (send_to_cn && conn->fail)
	{
		int fn_send_bulk_bytes = 1024 * FnSendBulkSize;

		/* port->fdsock and addr will be filled in fnconn_connect_remote */
		if (fnconn_connect_remote(conn, true) == STATUS_OK)
		{
			conn->fail = false;
			fn_fail_cn_count--;
			add_connection_to_checker(conn);
			conn->buffer = MemoryContextAlloc(TopMemoryContext, fn_send_bulk_bytes);
		}
		else
		{
			elog(WARNING, "[SendFnPage] failed to reconnect to forward receiver, name=%s fn_fail_cn_count %d",
				 NameStr(conn->node.nodename), fn_fail_cn_count);
			return false;
		}
	}

	if (NULL == conn->port)
	{
		elog(WARNING, "the conn is not inited nodename %s host %s forward port %d",
					NameStr(conn->node.nodename), NameStr(conn->node.nodehost), conn->node.nodeforwardport);
		return false;
	}
	
	bufptr = page;

	if (fn_forked_thread_num > 0)
	{
		int dest_idx = send_to_cn ? (nodeid + fn_dn_count) % fn_forked_thread_num : nodeid % fn_forked_thread_num;
		FnPageBuffer *fn_buffer = (FnPageBuffer *)malloc(sizeof(FnPageBuffer));
		fn_buffer->page = page;
		fn_buffer->conn = conn;
		fn_buffer->num_to_send = num_to_send;
		fn_buffer->buf_desc = buf;
		fn_buffer->next = NULL;

		while (!buffer_queue_push(&buffer_queues[dest_idx], fn_buffer))
		{
			if (buffer_queues[dest_idx].closed)
				return false;
			
			pg_usleep(1000L);
		}

		return true;
	}
	
	if (BLCKSZ >= fn_send_bulk_bytes)
		return SendData(Socket(*conn->port), bufptr, head->lower);
	
	if (head->flag & (FNPAGE_END | FNPAGE_PARAMS))
	{
		if (conn->pos == 0)
			return SendData(Socket(*conn->port), bufptr, head->lower);
		
		if (head->lower + conn->pos <= fn_send_bulk_bytes)
		{
			int len = conn->pos + head->lower;
			memcpy(conn->buffer + conn->pos, bufptr, head->lower);
			conn->pos = 0;
			return SendData(Socket(*conn->port), conn->buffer, len);
		}
		else
		{
			bool ret = SendData(Socket(*conn->port), conn->buffer, conn->pos);
			conn->pos = 0;
			if (unlikely(!ret))
				return ret;

			return SendData(Socket(*conn->port), bufptr, head->lower);
		}
	}
	
	if (head->lower + conn->pos <= fn_send_bulk_bytes)
	{
		memcpy(conn->buffer + conn->pos, bufptr, head->lower);
		conn->pos += head->lower;
		return true;
	}
	else
	{
		bool ret = SendData(Socket(*conn->port), conn->buffer, conn->pos);
		if (unlikely(!ret))
		{
			conn->pos = 0;
			return ret;
		}

		memcpy(conn->buffer, bufptr, head->lower);
		conn->pos = head->lower;
		return true;
	}
}

/* read buffer as much as possible */
int
fn_thread_read_buffer(int fd, char *buf, int buf_len)
{
    int         read_cnt = 0;
    int         expect_len = 0;
    int         total_len = 0;
    char        *read_buf = NULL;

    expect_len = buf_len;
    read_buf = buf;
    total_len = 0;

   	while(true)
    {
        /* read the buffer */
        read_cnt = read(fd, read_buf, expect_len);
        if (read_cnt > 0)
        {
            total_len = total_len + read_cnt;

            /* receive enough buffer */
            if (read_cnt == expect_len)
            {
                return 0;
            }
            else /* not enough buffer, try read the left again. */
            {
                read_buf = read_buf + read_cnt;
                expect_len = expect_len - read_cnt;
            }
        }
        else if (read_cnt == 0) /* this indicate the connection is closed. */
        {
			flog("fn_thread_read_buffer recv zero len fd %d expect_len %d buf_len %d msg:%m", fd, expect_len, buf_len);
            return -1;
        }
        else /* handle error */
        {
            /* normal errno */
            if ((EINTR == errno) || (EWOULDBLOCK == errno)|| (EAGAIN == errno))
            {
                return 0;
            }
            else /* abnormal errno, return error */
            {
				flog("fn_thread_read_buffer recv error fd %d errno %d msg:%m", fd, errno);
                return -1;
            }
        }
    }

    return 0;
}
