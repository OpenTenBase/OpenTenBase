/*-------------------------------------------------------------------------
 *
 * forwardrecv.c
 *
 * The forward node receiver (FN receiver)
 * 
 * In current implementation, a fn receiver process is pretty much like a normal
 * backend! During a fn sender process start up, it request new connection to
 * other node's postmaster directly and the postmaster fork a backend to deal
 * with it. This backend is our fn receiver process, different from normal
 * backend, it calls ForwardReceiverMain as main loop instead of PostgresMain,
 * and the only message it will receive is FnPage sent by fn sender process.
 * 
 * IDENTIFICATION
 *	  src/backend/postmaster/forwardrecv.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <sys/prctl.h>
#include <sys/epoll.h>
#include <arpa/inet.h>
#include <signal.h>

#include "miscadmin.h"

#include "executor/execFragment.h"
#include "forward/fnconn.h"
#include "forward/fnbufmgr.h"
#include "libpq/libpq.h"
#include "libpq/pqsignal.h"
#include "postmaster/postmaster.h"
#include "postmaster/fork_process.h"
#include "postmaster/forward.h"
#include "storage/ipc.h"
#include "storage/proc.h"
#include "storage/pmsignal.h"
#include "tcop/tcopprot.h"
#include "utils/memutils.h"
#include "utils/timeout.h"
#include "utils/ps_status.h"
#include "utils/varlena.h"
#include "pgxc/squeue.h"

/* poll type */
#define POLL_LISTEN_TCP_TYPE		(1)
#define POLL_LISTEN_UNIX_TYPE		(2)
#define POLL_ACCEPT_TYPE			(3)

/* The socket(s) we're listening to. */
#define MAXLISTEN				(64)

#define BUFFER_SIZE 1024

int fn_recv_thread_num;

typedef struct FnRecvCache
{
	int			fd;             /* the fd value */
	int			type;
	NodeDefinition remote_node;
	char		*buf;           /* the receiving buffer. */
} FnRecvCache;

/*
 * For huge data, we will simply build a link list in bufferDesc->next field,
 * and use this hash mechanism to locate where the buffer comes from. so we
 * can set current's next correctly.
 */
typedef struct FnRcvHugeKey
{
	FNQueryId	queryid;    /* source query id */
	uint16		fid;        /* source fragment id */
	uint16		nodeid;     /* source node id */
	uint16		workerid;   /* source parallel worker id */
	uint8		virtualid;	/* dest virtual dn id */
} FnRcvHugeKey;

typedef struct FnRcvHugeEntry
{
	FnRcvHugeKey	key;
	uint32			queue_idx;	/* current queue index of huge page */
} FnRcvHugeEntry;

typedef struct FnRecvPipe
{
    int         read_fd;
    int         write_fd;
} FnRecvPipe;

typedef struct FnRecvMsg
{
    int         old_fd;                         /* old fd */
    int         new_fd;                         /* new fd */
	bool		stop;                         	/* flag for stop thread */
} FnRecvMsg;

typedef struct
{
	pthread_t thread;
	int index;
} FnRecvThread;

typedef struct FnRecvBuffer
{
    char *buffer;
    struct FnRecvBuffer *next;
} FnRecvBuffer;

static HTAB *FnRcvHugeHash = NULL;

static int 	g_epollfd;
static int	ListenSocket[MAXLISTEN] = {PGINVALID_SOCKET};

static FnRecvPipe *fn_recv_pipes = NULL;
static int 	fn_max_idx = 0;
static pthread_mutex_t recv_mutex;
static int 	*fn_recv_fds = NULL;
static FnRecvThread *recv_threads;

/*
 * Flags set by interrupt handlers for later service in the main loop.
 */
static volatile sig_atomic_t shutdown_requested = false;			/* pg_ctl stop -m smart */

static void ReqShutdownHandler(SIGNAL_ARGS);

static bool am_forward_recver = false;

void *consumerThreadFunc(void *arg);
void *multi_recv_thread(void *arg);

bool
IsForwardRecverProcess(void)
{
	return am_forward_recver;
}

void
ForwardRecverProcessIam(void)
{
	am_forward_recver = true;
}

/*
 * Put a page into share-memory structure FnRcvBufferBlocks.
 * The arg page should be allocated locally, return the descriptor
 * of allocated share-memory buffer.
 * 
 * Notice: FnBufferAlloc will be blocked if there is no free buffer.
 */
static FnBufferDesc *
shmPutFnPage(FnPage page, bool need_mutex_lock)
{
	FnPageHeader head = (FnPageHeader) page;
	FnBufferDesc *desc;
	FnPage  shmpage;

	if (need_mutex_lock)
		pthread_mutex_lock(&recv_mutex);
	
	desc = FnBufferAlloc(FNPAGE_RECV, head->queryid);

	if (need_mutex_lock)
		pthread_mutex_unlock(&recv_mutex);

	shmpage = FnBufferDescriptorGetPage(desc);

	memcpy(shmpage, page, head->lower);

	return desc;
}

/*
 * Link a page into share-memory hash table entry it belongs to, implement
 * with a share-memory queue. This is kind of "dispatch" process. The
 * data-pump thread in DProcess should drain from the queue ASAP.
 */
static void
shmLinkFnPage(FnBufferDesc *desc, bool need_mutex_lock)
{
#define GET_QUEUE_IDX ((entry->nqueue == 0) ? 0 : (queue_idx_seq++) % entry->nqueue)
	FnRcvQueueEntry	*entry;
	FragmentKey		 key = {};
	FnPageHeader	 header = (FnPageHeader)FnBufferDescriptorGetPage(desc);
	LWLock			*partitionLock;
	bool			 found;
	uint32 			 valid_idx;
	uint32			 queue_idx = 0;
	uint32			 hashvalue;
	static uint32 	 queue_idx_seq = 0;

	if (need_mutex_lock)
		pthread_mutex_lock(&recv_mutex);

	key.queryid = header->queryid;
	key.fid = header->fid;
	key.workerid = header->virtualid;

	hashvalue = get_hash_value(FnRcvBufferHash, &key);
	partitionLock = RecvBufHashPartitionLock(hashvalue);

	LWLockAcquire(partitionLock, LW_SHARED);
	entry = (FnRcvQueueEntry *)hash_search_with_hash_value(FnRcvBufferHash,
														   (const void *) &key,
														   hashvalue,
														   HASH_FIND, &found);
	if (!found)
	{
		DEBUG_FRAG(elog(LOG, "throw page qid_ts_node:%ld, qid_seq:%ld, fid:%d, node:%d, worker:%d",
						header->queryid.timestamp_nodeid, header->queryid.sequence,
						header->fid, header->nodeid, header->workerid));

		if ((header->flag & FNPAGE_HUGE) != 0)
		{
			/* we have a piece of huge data, consult FnRcvHugeHash */
			FnRcvHugeKey huge_key = {};
			huge_key.queryid = header->queryid;
			huge_key.fid = header->fid;
			huge_key.workerid = header->workerid;
			huge_key.nodeid = header->nodeid;
			huge_key.virtualid = header->virtualid;
			hash_search(FnRcvHugeHash, (const void *) &huge_key,
						HASH_REMOVE, &found);
		}

		LWLockRelease(partitionLock);

		FnBufferFree(desc);

		if (need_mutex_lock)
			pthread_mutex_unlock(&recv_mutex);

		return;
	}

	if ((header->flag & FNPAGE_HUGE) != 0)
	{
		/* we have a piece of huge data, consult FnRcvHugeHash */
		FnRcvHugeKey	 huge_key;
		FnRcvHugeEntry	*huge_entry;

		memset(&huge_key, 0, sizeof(FnRcvHugeKey));
		huge_key.queryid = header->queryid;
		huge_key.fid = header->fid;
		huge_key.workerid = header->workerid;
		huge_key.nodeid = header->nodeid;
		huge_key.virtualid = header->virtualid;

		huge_entry = (FnRcvHugeEntry *)hash_search(FnRcvHugeHash,
												   (const void *) &huge_key,
												   HASH_ENTER, &found);
		if (found)
		{
			/*
			 * If we already have a page of piece of huge data of this key,
			 * put data into same queue.
			 */
			queue_idx = huge_entry->queue_idx;

			if ((header->flag & FNPAGE_HUGE_END) != 0)
			{
				/* this is the end, remove entry */
				hash_search(FnRcvHugeHash,
							(const void *) &huge_key,
							HASH_REMOVE, &found);
				Assert(found);
			}

			SpinLockAcquireNoPanic(&entry->mutex[queue_idx]);
			if (!entry->valid[queue_idx])
			{
				Assert((header->flag & FNPAGE_END) == 0);
				SpinLockRelease(&entry->mutex[queue_idx]);
				LWLockRelease(partitionLock);
				FnBufferFree(desc);
				return;
			}
			else
				valid_idx = queue_idx;
		}
		else if ((header->flag & FNPAGE_HUGE_END) == 0)
		{
			/* we have a beginning of huge data, choose a queue and remember it */
			queue_idx = GET_QUEUE_IDX;
			SpinLockAcquireNoPanic(&entry->mutex[queue_idx]);
			valid_idx = FnRcvQueueGetIndex(entry, queue_idx);
			huge_entry->queue_idx = valid_idx;
		}
		else /* (header->flag & FNPAGE_HUGE_END) != 0 */
		{
			/* this is the end, remove entry, and choose a random queue */
			hash_search(FnRcvHugeHash,
						(const void *) &huge_key,
						HASH_REMOVE, &found);
			Assert(found);
			queue_idx = GET_QUEUE_IDX;
			SpinLockAcquireNoPanic(&entry->mutex[queue_idx]);
			valid_idx = FnRcvQueueGetIndex(entry, queue_idx);
		}
	}
	else
	{
		/* normal page, choose a random queue */
		queue_idx = GET_QUEUE_IDX;
		SpinLockAcquireNoPanic(&entry->mutex[queue_idx]);
		valid_idx = FnRcvQueueGetIndex(entry, queue_idx);
	}

	/*
	 * There is no need to consider race conditions here, but the round-robin
	 * selection is global rather than specific to a single entry, it's much
	 * easier...
	 */
	SHMQueueInsertBefore(&entry->queue[valid_idx], &desc->elem);
	SpinLockRelease(&entry->mutex[valid_idx]);

	LWLockRelease(partitionLock);

	if (need_mutex_lock)
		pthread_mutex_unlock(&recv_mutex);
}

/*
 * on_proc_exit callback to close server's listen sockets
 */
static void
fnmgr_close_svr_ports(int status, Datum arg)
{
	int	i = 0;

	/*
	 * First, explicitly close all the socket FDs.  We used to just let this
	 * happen implicitly at postmaster exit, but it's better to close them
	 * before we remove the postmaster.pid lockfile; otherwise there's a race
	 * condition if a new postmaster wants to re-use the TCP port number.
	 */
	for (i = 0; i < MAXLISTEN; i++)
	{
		if (ListenSocket[i] != PGINVALID_SOCKET)
		{
			closesocket(ListenSocket[i]);
			ListenSocket[i] = PGINVALID_SOCKET;
		}
	}

	/*
	 * Next, remove any filesystem entries for Unix sockets.  To avoid race
	 * conditions against incoming postmasters, this must happen after closing
	 * the sockets and before removing lock files.
	 */
	RemoveSocketFiles();

	/*
	 * We don't do anything about socket lock files here; those will be
	 * removed in a later on_proc_exit callback.
	 */
}

static void
fnmgr_setup_huge(void)
{
	HASHCTL	hash_ctl;

	hash_ctl.keysize = sizeof(FnRcvHugeKey);
	hash_ctl.entrysize = sizeof(FnRcvHugeEntry);

	FnRcvHugeHash = hash_create("Forward Recv Huge hash",
								Min(FnRecvNBuffers / 4, 1024),
								&hash_ctl,
								HASH_ELEM | HASH_BLOBS);
}

/*
 * Establish input sockets.
 */
static void
fnmgr_listen_address(void)
{
	int i = 0;
	int	status = 0;

	/*
	 * Establish input sockets.
	 *
	 * First, mark them all closed, and set up an on_proc_exit function that's
	 * charged with closing the sockets again at postmaster shutdown.
	 */
	for (i = 0; i < MAXLISTEN; i++)
		ListenSocket[i] = PGINVALID_SOCKET;

	on_proc_exit(fnmgr_close_svr_ports, 0);

	ForgetLockFiles();
	ForgetSocketFiles();

	if (ListenAddresses)
	{
		char	   *rawstring = NULL;
		List	   *elemlist = NIL;
		ListCell   *l = NULL;
		int			success = 0;

		/* Need a modifiable copy of ListenAddresses */
		rawstring = pstrdup(ListenAddresses);

		/* Parse string into list of hostnames */
		if (!SplitIdentifierString(rawstring, ',', &elemlist))
		{
			/* syntax error in list */
			ereport(FATAL,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						errmsg("forward manager invalid list syntax in parameter \"%s\"",
							   "listen_addresses")));
		}

		foreach(l, elemlist)
		{
			char	   *curhost = (char *) lfirst(l);

			if (strcmp(curhost, "*") == 0)
				status = StreamServerPort(AF_UNSPEC, NULL,
										  (unsigned short) ForwardPortNumber,
										  NULL,
										  ListenSocket, MAXLISTEN);
			else
				status = StreamServerPort(AF_UNSPEC, curhost,
										  (unsigned short) ForwardPortNumber,
										  NULL,
										  ListenSocket, MAXLISTEN);
			if (status == STATUS_OK)
			{
				success++;
			}
			else
				ereport(WARNING,
						(errmsg("forward manager could not create listen socket for \"%s\"",
								curhost)));
		}

		if (!success && elemlist != NIL)
			ereport(FATAL,
					(errmsg("forward manager could not create any TCP/IP sockets")));

		list_free(elemlist);
		pfree(rawstring);
	}

#ifdef HAVE_UNIX_SOCKETS
	if (Unix_socket_directories)
	{
		char	   *rawstring = NULL;
		List	   *elemlist = NIL;
		ListCell   *l = NULL;
		int 		success = 0;

		/* Need a modifiable copy of Unix_socket_directories */
		rawstring = pstrdup(Unix_socket_directories);

		/* Parse string into list of directories */
		if (!SplitDirectoriesString(rawstring, ',', &elemlist))
		{
			/* syntax error in list */
			ereport(FATAL,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						errmsg("forward manager invalid list syntax in parameter \"%s\"",
							   "unix_socket_directories")));
		}

		foreach(l, elemlist)
		{
			char	   *socketdir = (char *) lfirst(l);

			status = StreamServerPort(AF_UNIX, NULL,
									  (unsigned short) ForwardPortNumber,
									  socketdir,
									  ListenSocket, MAXLISTEN);
			if (status == STATUS_OK)
			{
				success++;
			}
			else
				ereport(WARNING,
						(errmsg("forward manager could not create Unix-domain socket in directory \"%s\"",
								socketdir)));
		}

		if (!success && elemlist != NIL)
			ereport(FATAL,
					(errmsg("forward manager could not create any Unix-domain sockets")));

		list_free_deep(elemlist);
		pfree(rawstring);
	}
#else
	/* TODO support for non-unix platform */
	ereport(FATAL,
			(errcode(ERRCODE_CONNECTION_EXCEPTION),
			 errmsg("forward manager must supports UNIX socket")));
	return -1;
#endif

	/*
	 * check that we have some socket to listen on
	 */
	if (ListenSocket[0] == PGINVALID_SOCKET)
		ereport(FATAL,
				(errmsg("forward manager no socket created for listening")));
}

static void
addListenPortToEpoll()
{
	FnRecvCache *recvCache;
	int i;

	/* only init myself node define */
	PgxcNodeGetAllDefinitions(NULL, NULL, NULL, NULL, &fn_myself, NULL);

	for (i = 0; i < MAXLISTEN; i++)
	{
		int domain;
		int len = sizeof(domain);

		struct epoll_event	event;

		if (ListenSocket[i] == PGINVALID_SOCKET)
		{
			continue;
		}

		if (getsockopt(ListenSocket[i], SOL_SOCKET, SO_DOMAIN, &domain, (socklen_t *)&len) < 0)
		{
			elog(ERROR, "ForwardRecv: getsockopt fd for add to epoll failed, %m");
			proc_exit(1);
		}

		recvCache = palloc0(sizeof(FnRecvCache));

		if (domain == AF_UNIX)
			recvCache->type = POLL_LISTEN_UNIX_TYPE;
		else
			recvCache->type = POLL_LISTEN_TCP_TYPE;
		
		recvCache->fd = ListenSocket[i];
		event.data.fd = ListenSocket[i];
		event.data.ptr = recvCache;
		event.events  = EPOLLIN | EPOLLERR | EPOLLHUP;

		if (epoll_ctl(g_epollfd, EPOLL_CTL_ADD, ListenSocket[i], &event) == -1)
		{
			close(ListenSocket[i]);
			close(g_epollfd);
			ListenSocket[i] = PGINVALID_SOCKET;
			elog(ERROR, "ForwardRecv: add server fd to epoll failed, %m");
			proc_exit(1);
		}
	}
}

static bool
check_client(int client_fd, FnRecvCache *recv_cache, char *remote_host, bool use_unix, int *remote_index)
{
	int flags;
	struct timeval timeout;
	char buffer[BUFFER_SIZE];
	int pack_len = sizeof(FnMgrStartupPacket);
	fd_set read_fds;
	FnMgrStartupPacket *pack;
	
	flags = fcntl(client_fd, F_GETFL, 0);

	/* set to nonblocking mode avoid hang */
    fcntl(client_fd, F_SETFL, flags | O_NONBLOCK);

    while (true)
	{
		int ready;

        FD_ZERO(&read_fds);
        FD_SET(client_fd, &read_fds);

        timeout.tv_sec = 1;
        timeout.tv_usec = 0;

        ready = select(client_fd + 1, &read_fds, NULL, NULL, &timeout);
        if (ready < 0)
		{
            flog("pselect error %m");
            break;
        }
		else if (ready > 0)
		{
            if (FD_ISSET(client_fd, &read_fds))
			{
                int bytes_received = recv(client_fd, buffer, BUFFER_SIZE - 1, 0);
                if (bytes_received <= 0)
				{
					flog("invalid client bytes_received %d fd %d", bytes_received, client_fd);
                    break;
                }

				if (bytes_received != pack_len)
				{
					flog("invalid client bytes_received %d expect pack_len %d fd %d", bytes_received, pack_len, client_fd);
					break;
				}
				
				pack = (FnMgrStartupPacket *)buffer;

				if (memcmp(pack->identify, PG_VERSION, Min(NAMEDATALEN - 1, strlen(PG_VERSION))) != 0)
				{
					flog("invalid client recv identify %s expect identify %s fd %d", pack->identify, PG_VERSION, client_fd);
					break;
				}

				recv_cache->remote_node = pack->node;

				if (!use_unix)
				{
					if (strcmp(remote_host, NameStr(recv_cache->remote_node.nodehost)) != 0)
					{
						flog("invalid client addr %s expect addr %s fd %d", remote_host, NameStr(recv_cache->remote_node.nodehost), client_fd);
						break;
					}
				}

				*remote_index = pack->index;

				/* restore to blocking mode */
				fcntl(client_fd, F_SETFL, flags & ~O_NONBLOCK);
				return true;
            }
        }
		else
		{
            break;
        }
    }

	return false;
}

static bool
consumeDataInThread(int acceptFd)
{
	char 			buffer[BLCKSZ];
	uint32 			len = 0;

	int ret = fn_thread_read_buffer(acceptFd, buffer, sizeof(len));

	if (-1 == ret)
	{
		flog("[consumeDataInThread] fn_thread_read_buffer return -1 for fd %d", acceptFd);
		return false;
	}

	/* discount length itself */
	len = *(uint32 *) buffer;

	if (len > BLCKSZ)
	{
		flog("[consumeDataInThread] fn_thread_read_buffer recv invalid len %d fd %d", len, acceptFd);
		return false;
	}

	ret = fn_thread_read_buffer(acceptFd,
								buffer + sizeof(uint32),
								len - sizeof(uint32));
	if (0 == ret)
	{
		FnBufferDesc *desc;
		FnPage page = (FnPage) buffer;

		desc = shmPutFnPage(page, true);
		shmLinkFnPage(desc, true);
	}
	else if (-1 == ret)
	{
		flog("[consumeDataInThread] fn_thread_read_buffer return -1 second for fd %d, len %d", acceptFd, len);
		return false;
	}

	return true;
}

void *consumerThreadFunc(void *arg)
{
    int fd = *(int *)arg;
	sigset_t mask;
	char thrd_name[NAMEDATALEN] = {0};

	/* add sub thread longjmp check */
	SUB_THREAD_LONGJMP_DEFAULT_CHECK();

	sprintf(thrd_name, "recv-%d", fd);

	fn_set_thrd_name(thrd_name);

    sigfillset(&mask);
    pthread_sigmask(SIG_BLOCK, &mask, NULL);

    while (1)
    {
        bool success = consumeDataInThread(fd);
		if (!success)
		{
			flog("consumeDataInThread break for fd %d", fd);
			close(fd);
			*((int *)arg) = 0;
            break;
        }
    }

    return NULL;
}

static void
acceptConnectToEpoll(FnRecvCache *recv_cache)
{
	int listenFd = recv_cache->fd;
	int fd;
	char *remote_host = NULL;
	char ipv6_host_buf[INET6_ADDRSTRLEN];
	socklen_t client_addr_len;
	bool use_unix = false;
	struct epoll_event event;
	int status = 0;
	int remote_index;
	FnRecvCache *recvCache;

	if (recv_cache->type == POLL_LISTEN_UNIX_TYPE)
	{
		fd = accept(listenFd, NULL, NULL);
		if (fd < 0)
		{
			elog(WARNING, "Forward recv failed to accept connection: %m");
			return;
		}

		remote_host = NameStr(fn_myself->nodehost);
		use_unix = true;
	}
	else
	{
		if (strcmp("localhost", NameStr(fn_myself->nodehost)) == 0)
			return;

		if (strchr(NameStr(fn_myself->nodehost), ':') != NULL)
		{
			struct sockaddr_in6 client_addr;
			client_addr_len = sizeof(client_addr);
			fd = accept(listenFd, (struct sockaddr *)&client_addr, &client_addr_len);
			if (fd < 0)
			{
				elog(WARNING, "Forward recv failed to accept connection: %m");
				return;
			}

			inet_ntop(AF_INET6, &client_addr.sin6_addr, ipv6_host_buf, INET6_ADDRSTRLEN);
			remote_host = ipv6_host_buf;
		}
		else
		{
			struct sockaddr_in client_addr;
			client_addr_len = sizeof(client_addr);
			fd = accept(listenFd, (struct sockaddr *)&client_addr, &client_addr_len);
			if (fd < 0)
			{
				elog(WARNING, "Forward recv failed to accept connection: %m");
				return;
			}

			remote_host = inet_ntoa(client_addr.sin_addr);
		}
	}

	recvCache = palloc0(sizeof(FnRecvCache));
	if (!check_client(fd, recv_cache, remote_host, use_unix, &remote_index))
	{
		elog(WARNING, "failed to check client fd %d", fd);
		close(fd);
		return;
	}

	if (!SendData(fd, (char *)&status, sizeof(status)))
	{
		elog(WARNING, "failed to ack client fd %d", fd);
		close(fd);
		return;
	}

	/* client is not valid */
	if (status != 0)
	{
		elog(WARNING, "ack client fd %d status -1", fd);
		close(fd);
		return;
	}

	/* start thread to read data */
	if (fn_recv_thread_num >= 0 && remote_index >= 0)
	{
		if (0 != fn_recv_fds[remote_index])
		{
			shutdown(fn_recv_fds[remote_index], SHUT_RDWR);
			close(fn_recv_fds[remote_index]);
		}

		if (fn_recv_thread_num == 0)
		{
			if (0 != recv_threads[remote_index].thread)
			{
				pthread_join(recv_threads[remote_index].thread, NULL);
				TermFidBackendProc("got reconnect to fork recv thread");
			}

			fn_recv_fds[remote_index] = fd;
			pthread_create(&recv_threads[remote_index].thread, NULL, consumerThreadFunc, fn_recv_fds + remote_index);
			fn_max_idx = Max(remote_index + 1, fn_max_idx);
		}
		else
		{
			FnRecvMsg recv_msg;
			int ret;
			FnRecvPipe *pipe = &fn_recv_pipes[remote_index % fn_recv_thread_num];
			int pfd = pipe->write_fd;

			recv_msg.old_fd = fn_recv_fds[remote_index];
			recv_msg.new_fd = fd;

			fn_recv_fds[remote_index] = fd;

			ret = write(pfd, &recv_msg, sizeof(FnRecvMsg));
			if (-1 == ret)
			{
				elog(WARNING, "fail to notify the socket change old fd %d new fd %d", recv_msg.old_fd, recv_msg.new_fd);
				return;
			}
		}

		return;
	}

	recvCache->type = POLL_ACCEPT_TYPE;
	recvCache->fd = fd;

	event.data.fd = fd;
	event.data.ptr = recvCache;
	event.events  = EPOLLIN | EPOLLERR | EPOLLHUP;

	if (epoll_ctl(g_epollfd, EPOLL_CTL_ADD, fd, &event) == -1)
	{
		elog(WARNING, "add accept fd %d to epoll failed, %m", fd);
		close(fd);
	}
}

static void
consumeDataFromSender(int acceptFd)
{
	FnPage		    page;
	FnBufferDesc   *desc;
	char 			buffer[BLCKSZ] = {0};
	uint32 			len = 0;

	int ret = fn_thread_read_buffer(acceptFd, buffer, sizeof(len));

	if (-1 == ret)
	{
		if (epoll_ctl(g_epollfd, EPOLL_CTL_DEL, acceptFd, NULL) == -1)
		{
			flog("Forward recv:remove fd %d from epoll failed, %m", acceptFd);
		}
		close(acceptFd);
		return;
	}

	/* discount length itself */
	len = *(uint32 *) buffer;

	if (len > BLCKSZ)
	{
		flog("Forward recv:recv invalid len %d greater then %d acceptFd %d", len, BLCKSZ, acceptFd);
		return;
	}

	ret = fn_thread_read_buffer(acceptFd,
								buffer + sizeof(uint32),
								len - sizeof(uint32));
	if (0 == ret)
	{
		bool need_mutex_lock = fn_recv_thread_num >= 0;
		page = (FnPage) buffer;

		desc = shmPutFnPage(page, need_mutex_lock);
		shmLinkFnPage(desc, need_mutex_lock);
	}
	else if (-1 == ret)
	{
		if (epoll_ctl(g_epollfd, EPOLL_CTL_DEL, acceptFd, NULL) == -1)
		{
			flog("Forward recv:remove fd %d from epoll failed, %m", acceptFd);
		}
		close(acceptFd);
		return;
	}
}

void *multi_recv_thread(void *arg)
{
    int thread_id = *(int *)arg;
	int efd;
	int pipe_fd;
	sigset_t mask;
	char thrd_name[NAMEDATALEN] = {0};
	struct epoll_event ev;
	struct epoll_event *events;

	/* add sub thread longjmp check */
	SUB_THREAD_LONGJMP_DEFAULT_CHECK();

    sigfillset(&mask);
    pthread_sigmask(SIG_BLOCK, &mask, NULL);
	
	sprintf(thrd_name, "mrecv-%d", thread_id);

	fn_set_thrd_name(thrd_name);

	efd = epoll_create1(0);
    if (efd == -1)
	{
        flog("epoll_create1 failed %m");
		return NULL;
    }

    pipe_fd = fn_recv_pipes[thread_id].read_fd;

    ev.data.fd = pipe_fd;
    ev.events = EPOLLIN | EPOLLERR;

    if (epoll_ctl(efd, EPOLL_CTL_ADD, pipe_fd, &ev) == -1)
    {
		flog("fail to add the pipe fd %d to epoll fd %d, reason %m.", pipe_fd, efd);
        return NULL;
    }

	events = malloc(8 * sizeof(struct epoll_event));

	while (true)
	{
		int i;
		int ret = epoll_wait(efd, events, 32, -1);

		for (i = 0; i < ret; i++)
		{
			if (events[i].events & EPOLLIN)
			{
				int data_fd = events[i].data.fd;
				FnRecvPipe *pipe = &fn_recv_pipes[thread_id];

				if (data_fd != pipe->read_fd)
				{
					bool success = consumeDataInThread(data_fd);
					if (!success)
					{
						if (epoll_ctl(efd, EPOLL_CTL_DEL, data_fd, NULL) == -1)
							flog("epoll_ctl: del bad fd %d failed %m", data_fd);
						close(data_fd);
					}
				}
				else
				{
					char buffer[sizeof(FnRecvMsg)];
					int bytes;
					FnRecvMsg *msg;
					int cfd;
					struct epoll_event ev;

					bytes = read(data_fd, buffer, sizeof(FnRecvMsg));
					if (bytes != sizeof(FnRecvMsg))
						flog("recv FnRecvMsg failed recv len %d expect len %ld %m", bytes, sizeof(FnRecvMsg));

					msg = (FnRecvMsg *)buffer;
					cfd = msg->new_fd;

					ev.events = EPOLLIN;
					ev.data.fd = cfd;

					if (msg->old_fd > 0)
					{
						if (epoll_ctl(efd, EPOLL_CTL_DEL, msg->old_fd, NULL) == -1)
							flog("epoll_ctl: del old fd %d failed %m", msg->old_fd);
					}

					if (epoll_ctl(efd, EPOLL_CTL_ADD, cfd, &ev) == -1)
						flog("epoll_ctl: add new fd %d old fd %d failed %m", cfd, msg->old_fd);
					else
						flog("epoll_ctl: add new fd %d old fd %d success %m", cfd, msg->old_fd);
				}
			}
		}
	}
	
    return NULL;
}

static void
initRecvThreadPool()
{
	int i;

	if (fn_recv_thread_num <= 0)
		return;

	if (NULL != fn_recv_pipes)
		pfree(fn_recv_pipes);

	fn_recv_pipes = palloc0(fn_recv_thread_num * sizeof(FnRecvPipe));

    for (i = 0; i < fn_recv_thread_num; ++i)
	{
		int ret;

		fn_recv_pipes[i].read_fd = -1;
        fn_recv_pipes[i].write_fd = -1;

		ret = pipe2((int *)&fn_recv_pipes[i], O_NONBLOCK);
		if (-1 == ret)
			elog(ERROR, "pipe2 failed ret:%d msg:%m", ret);

		recv_threads[i].index = i;

        ret = pthread_create(&recv_threads[i].thread, NULL, multi_recv_thread, &recv_threads[i].index);
		if (0 != ret)
			elog(ERROR, "pthread_create failed ret:%d msg:%m", ret);
    }
}

static void
initRecvThreads()
{
	if (fn_recv_thread_num < 0)
		return;

	if (NULL != fn_recv_fds)
		pfree(fn_recv_fds);

	fn_recv_fds = palloc0(OPENTENBASE_MAX_DATANODE_NUMBER * sizeof(int));

	if (NULL != recv_threads)
		pfree(recv_threads);

	recv_threads = palloc0(OPENTENBASE_MAX_DATANODE_NUMBER * sizeof(FnRecvThread));

	pthread_mutex_init(&recv_mutex, NULL);

	initRecvThreadPool();
}

/*
 * Main entry point for forward receiver process, loop forever
 */
void
ForwardReceiverMain(void)
{
	int 				maxfd;
	struct epoll_event *events;

#ifdef __OPENTENBASE__
	HOLD_POOLER_RELOAD();
#endif

	MyLocalThreadId = pthread_self();

	SetProcessingMode(InitProcessing);

	/*
	 * Set up signal handlers and masks.
	 *
	 * Note that postmaster blocked all signals before forking child process,
	 * so there is no race condition whereby we might receive a signal before
	 * we have set up the handler.
	 *
	 * Also note: it's best not to use any signals that are SIG_IGNored in the
	 * postmaster.  If such a signal arrives before we are able to change the
	 * handler to non-SIG_IGN, it'll get dropped.  Instead, make a dummy
	 * handler in the postmaster to reserve the signal. (Of course, this isn't
	 * an issue for signals that are locally generated, such as SIGALRM and
	 * SIGPIPE.)
	 */
	pqsignal(SIGHUP, PostgresSigHupHandler);	/* set flag to read config
													 * file */
	pqsignal(SIGINT, SIG_IGN);
	pqsignal(SIGTERM, ReqShutdownHandler);
	pqsignal(SIGQUIT, ReqShutdownHandler);

	InitializeTimeouts();	/* establishes SIGALRM handler */

	/*
	 * Ignore failure to write to frontend. Note: if frontend closes
	 * connection, we will notice it and exit cleanly when control next
	 * returns to outer loop.  This seems safer than forcing exit in the
	 * midst of output during who-knows-what operation...
	 */
	pqsignal(SIGPIPE, SIG_IGN);
	pqsignal(SIGUSR1, procsignal_sigusr1_handler);
#ifdef __OPENTENBASE_C__
	/* SIGUSR2 is registered for postgres in SetRemoteSubplan */
#endif
	pqsignal(SIGUSR2, SIG_IGN);
	pqsignal(SIGFPE, FloatExceptionHandler);

	/*
	 * Reset some signals that are accepted by postmaster but not by
	 * backend
	 */
	pqsignal(SIGCHLD, SIG_DFL); /* system() requires this on some
									 * platforms */

	/* postgres exited when postmaster has been killed */
	prctl(PR_SET_PDEATHSIG, SIGKILL);

	pqinitmask();

	/* We allow SIGQUIT (quickdie) at all times */
	sigdelset(&BlockSig, SIGQUIT);

	whereToSendOutput = DestNone;

	PG_SETMASK(&UnBlockSig);

	/*
	 * If the PostmasterContext is still around, recycle the space; we don't
	 * need it anymore after InitPostgres completes.  Note this does not trash
	 * *MyProcPort, because ConnCreate() allocated that space with malloc()
	 * ... else we'd need to copy the Port data first.  Also, subsidiary data
	 * such as the username isn't lost either; see ProcessStartupPacket().
	 */
	if (PostmasterContext)
	{
		MemoryContextDelete(PostmasterContext);
		PostmasterContext = NULL;
	}

	SetProcessingMode(NormalProcessing);

	/*
	 * process any libraries that should be preloaded at backend start (this
	 * likewise can't be done until GUC settings are complete)
	 */
	process_session_preload_libraries();

	/*
	 * Create the memory context we will use in the main loop.
	 *
	 * MessageContext is reset once per iteration of the main loop, ie, upon
	 * completion of processing of each command message from the client.
	 */
	MessageContext = AllocSetContextCreate(TopMemoryContext,
										   "MessageContext",
										   ALLOCSET_DEFAULT_SIZES);

	MemoryContextSwitchTo(TopMemoryContext);

	/* Prepare huge data support */
	fnmgr_setup_huge();

	/* Listen for forward recv port */
	fnmgr_listen_address();

	maxfd = OPENTENBASE_MAX_COORDINATOR_NUMBER + OPENTENBASE_MAX_DATANODE_NUMBER + MAXLISTEN;
	events = palloc0(maxfd * sizeof(struct epoll_event));
	g_epollfd = epoll_create(maxfd);

	addListenPortToEpoll();

	/* avoid hang when dn swap */
	TermFidBackendProc("before start recv proc");

	initRecvThreads();

	/*
	 * Loop forever
	 */
	for (;;)
	{
		int retval = epoll_wait(g_epollfd, events, maxfd, 1000);
		if (retval > 0)
		{
			int i = 0;
			for (i = 0; i < retval; i++)
			{
				FnRecvCache *recv_cache = (FnRecvCache *)events[i].data.ptr;

				if (!(events[i].events & EPOLLIN) || NULL == recv_cache)
				{
					continue;
				}

				if (events[i].events & EPOLLIN)
				{
					int fd = recv_cache->fd;
					if (POLL_ACCEPT_TYPE == recv_cache->type)
					{
						consumeDataFromSender(fd);
					}
					else
					{
						acceptConnectToEpoll(recv_cache);	
					}
				}
			}
		}

		/*
		 * check for any other interesting events that happened while we
		 * slept.
		 */
		if (ConfigReloadPending)
		{
			ConfigReloadPending = false;
			ProcessConfigFile(PGC_SIGHUP);
		}

		if (shutdown_requested)
		{
			/* close remote fd and threads */
			if (fn_recv_thread_num >= 0 && fn_max_idx > 0)
			{
				int idx = 0;

				for (idx = 0; idx < fn_max_idx; idx++)
				{
					if (0 != fn_recv_fds[idx])
						shutdown(fn_recv_fds[idx], SHUT_RDWR);
					
					if (0 != recv_threads[idx].thread)
						pthread_join(recv_threads[idx].thread, NULL);
				}
			}

			/*
			 * From here on, elog(ERROR) should end with exit(1), not send
			 * control back to the sigsetjmp block above
			 */
			ExitOnAnyError = true;
			/* Normal exit from the bgwriter is here */
			proc_exit(0);		/* done */
		}
	}
}

/* SIGTERM: set flag to shutdown and exit */
static void
ReqShutdownHandler(SIGNAL_ARGS)
{
	int			save_errno = errno;

	shutdown_requested = true;
	SetLatch(MyLatch);

	errno = save_errno;
}
