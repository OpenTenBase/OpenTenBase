/*-------------------------------------------------------------------------
 *
 * poolcomm.c
 *
 *	  Communication functions between the pool manager and session
 *
 *
 * Portions Copyright (c) 1996-2009, PostgreSQL Global Development Group
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 *
 *-------------------------------------------------------------------------
 */

#ifdef __sun
#define _XOPEN_SOURCE 500
#define uint uint32_t
#endif

#include <sys/types.h>
#include <sys/socket.h>
#include <sys/uio.h>
#include <sys/stat.h>
#include <sys/un.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <stddef.h>
#include "c.h"
#include "postgres.h"
#include "pgxc/poolcomm.h"
#include "storage/ipc.h"
#include "utils/elog.h"
#include "miscadmin.h"
#include "pgxc/poolmgr.h"
#include "pgxc/nodemgr.h"

static int	pool_recvbuf(PoolPort *port);
static int	pool_discardbytes(PoolPort *port, size_t len);
static int pool_recvres_internal(PoolPort *port, bool need_log);

/*
 * The kernel constant SCM_MAX_FD defines a limit on the number
 * of file descriptors in the array.  Attempting to send an array
 * larger than this limit causes sendmsg(2) to fail with the error EINVAL. 
 * SCM_MAX_FD has the value 253 (or 255 in kernels before 2.6.38). 
 */
#define MAX_SEND_FD_NUM		253

#ifdef HAVE_UNIX_SOCKETS

#define POOLER_UNIXSOCK_PATH(path, port, sockdir) \
	snprintf(path, sizeof(path), "%s/.s.PGPOOL.%d", \
			((sockdir) && *(sockdir) != '\0') ? (sockdir) : \
			DEFAULT_PGSOCKET_DIR, \
			(port))

static char sock_path[MAXPGPATH];

static void StreamDoUnlink(int code, Datum arg);

static int	Lock_AF_UNIX(unsigned short port, const char *unixSocketName);
#endif

/*
 * Open server socket on specified port to accept connection from sessions
 */
int
pool_listen(unsigned short port, const char *unixSocketName)
{
	int			fd,
				len;
	struct sockaddr_un unix_addr;
	int			maxconn;


#ifdef HAVE_UNIX_SOCKETS
	if (Lock_AF_UNIX(port, unixSocketName) < 0)
		return -1;

	/* create a Unix domain stream socket */
	if ((fd = socket(AF_UNIX, SOCK_STREAM, 0)) < 0)
		return -1;

	/* fill in socket address structure */
	memset(&unix_addr, 0, sizeof(unix_addr));
	unix_addr.sun_family = AF_UNIX;
	strcpy(unix_addr.sun_path, sock_path);
	len = sizeof(unix_addr.sun_family) +
		strlen(unix_addr.sun_path) + 1;



	/* bind the name to the descriptor */
	if (bind(fd, (struct sockaddr *) & unix_addr, len) < 0)
	{
		close(fd);
		return -1;
	}

	/*
	 * Select appropriate accept-queue length limit.  PG_SOMAXCONN is only
	 * intended to provide a clamp on the request on platforms where an
	 * overly large request provokes a kernel error (are there any?).
	 */
	maxconn = MaxBackends * 2;
	if (maxconn > PG_SOMAXCONN)
		maxconn = PG_SOMAXCONN;

	/* tell kernel we're a server */
	if (listen(fd, maxconn) < 0)
	{
		close(fd);
		return -1;
	}



	/* Arrange to unlink the socket file at exit */
	on_proc_exit(StreamDoUnlink, 0);

	return fd;
#else
	/* TODO support for non-unix platform */
	ereport(FATAL,
			(errcode(ERRCODE_CONNECTION_EXCEPTION),
			 errmsg("pool manager only supports UNIX socket")));
	return -1;
#endif
}

/* StreamDoUnlink()
 * Shutdown routine for pooler connection
 * If a Unix socket is used for communication, explicitly close it.
 */
#ifdef HAVE_UNIX_SOCKETS
static void
StreamDoUnlink(int code, Datum arg)
{
	Assert(sock_path[0]);
	unlink(sock_path);
}
#endif   /* HAVE_UNIX_SOCKETS */

#ifdef HAVE_UNIX_SOCKETS
static int
Lock_AF_UNIX(unsigned short port, const char *unixSocketName)
{
	POOLER_UNIXSOCK_PATH(sock_path, port, unixSocketName);

	CreateSocketLockFile(sock_path, true, "");

	unlink(sock_path);

	return 0;
}
#endif

/*
 * Connect to pooler listening on specified port
 */
int
pool_connect(unsigned short port, const char *unixSocketName)
{
	int			fd,
				len;
	struct sockaddr_un unix_addr;

#ifdef HAVE_UNIX_SOCKETS
	/* create a Unix domain stream socket */
	if ((fd = socket(AF_UNIX, SOCK_STREAM, 0)) < 0)
		return -1;

	/* fill socket address structure w/server's addr */
	POOLER_UNIXSOCK_PATH(sock_path, port, unixSocketName);

	memset(&unix_addr, 0, sizeof(unix_addr));
	unix_addr.sun_family = AF_UNIX;
	strcpy(unix_addr.sun_path, sock_path);
	len = sizeof(unix_addr.sun_family) +
		strlen(unix_addr.sun_path) + 1;

	if (connect(fd, (struct sockaddr *) & unix_addr, len) < 0)
	{
		close(fd);
		return -1;
	}

	return fd;
#else
	/* TODO support for non-unix platform */
	ereport(FATAL,
			(errcode(ERRCODE_CONNECTION_EXCEPTION),
			 errmsg("pool manager only supports UNIX socket")));
	return -1;
#endif
}


/*
 * Get one byte from the buffer, read data from the connection if buffer is empty
 */
int
pool_getbyte(PoolPort *port)
{
	while (port->RecvPointer >= port->RecvLength)
	{
		if (pool_recvbuf(port)) /* If nothing in buffer, then recv some */
			return EOF;			/* Failed to recv data */
	}
	return (unsigned char) port->RecvBuffer[port->RecvPointer++];
}


/*
 * Get one byte from the buffer if it is not empty
 */
int
pool_pollbyte(PoolPort *port)
{
	if (port->RecvPointer >= port->RecvLength)
	{
		return EOF;				/* Empty buffer */
	}
	return (unsigned char) port->RecvBuffer[port->RecvPointer++];
}


/*
 * Read pooler protocol message from the buffer.
 */
int
pool_getmessage(PoolPort *port, StringInfo s, int maxlen)
{
	int32		len;

	resetStringInfo(s);

	/* Read message length word */
	if (pool_getbytes(port, (char *) &len, 4) == EOF)
	{
		ereport(ERROR,
				(errcode(ERRCODE_PROTOCOL_VIOLATION),
				 errmsg("unexpected EOF within message length word")));
		return EOF;
	}

	len = pg_ntoh32(len);

	if (len < 4 ||
		(maxlen > 0 && len > maxlen))
	{
		ereport(ERROR,
				(errcode(ERRCODE_PROTOCOL_VIOLATION),
				 errmsg("invalid message length")));
		return EOF;
	}

	len -= 4;					/* discount length itself */

	if (len > 0)
	{
		/*
		 * Allocate space for message.	If we run out of room (ridiculously
		 * large message), we will elog(ERROR)
		 */
		PG_TRY();
		{
			enlargeStringInfo(s, len);
		}
		PG_CATCH();
		{
			if (pool_discardbytes(port, len) == EOF){
				//while(1);
				ereport(PANIC,
						(errcode(ERRCODE_PROTOCOL_VIOLATION),
						 errmsg("incomplete message from client")));
			}
			PG_RE_THROW();
		}
		PG_END_TRY();

		/* And grab the message */
		if (pool_getbytes(port, s->data, len) == EOF)
		{
			ereport(PANIC,
					(errcode(ERRCODE_PROTOCOL_VIOLATION),
					 errmsg("incomplete message from client")));
			return EOF;
		}
		s->len = len;
		/* Place a trailing null per StringInfo convention */
		s->data[len] = '\0';
	}

	return 0;
}


/* --------------------------------
 * pool_getbytes - get a known number of bytes from connection
 *
 * returns 0 if OK, EOF if trouble
 * --------------------------------
 */
int
pool_getbytes(PoolPort *port, char *s, size_t len)
{
	size_t		amount;

	while (len > 0)
	{
		while (port->RecvPointer >= port->RecvLength)
		{
			if (pool_recvbuf(port))		/* If nothing in buffer, then recv
										 * some */
				return EOF;		/* Failed to recv data */
		}
		amount = port->RecvLength - port->RecvPointer;
			
		elog(DEBUG1, "amount: %zu, len: %zu, RecvLength: %d, RecvPointer: %d", 
					amount, len, port->RecvLength, port->RecvPointer);
		if (amount > len)
			amount = len;
		memcpy(s, port->RecvBuffer + port->RecvPointer, amount);
		port->RecvPointer += amount;
		s += amount;
		len -= amount;
	}
	return 0;
}


/* --------------------------------
 * pool_discardbytes - discard a known number of bytes from connection
 *
 * returns 0 if OK, EOF if trouble
 * --------------------------------
 */
static int
pool_discardbytes(PoolPort *port, size_t len)
{
	size_t		amount;

	while (len > 0)
	{
		while (port->RecvPointer >= port->RecvLength)
		{
			if (pool_recvbuf(port))		/* If nothing in buffer, then recv
										 * some */
				return EOF;		/* Failed to recv data */
		}
		amount = port->RecvLength - port->RecvPointer;
		if (amount > len)
			amount = len;
		port->RecvPointer += amount;
		len -= amount;
	}
	return 0;
}


/* --------------------------------
 * pool_recvbuf - load some bytes into the input buffer
 *
 * returns 0 if OK, EOF if trouble
 * --------------------------------
 */
static int
pool_recvbuf(PoolPort *port)
{
	if (port->RecvPointer > 0)
	{
		if (port->RecvLength > port->RecvPointer)
		{
			/* still some unread data, left-justify it in the buffer */
			memmove(port->RecvBuffer, port->RecvBuffer + port->RecvPointer,
					port->RecvLength - port->RecvPointer);
			port->RecvLength -= port->RecvPointer;
			port->RecvPointer = 0;
		}
		else
			port->RecvLength = port->RecvPointer = 0;
	}

	/* Can fill buffer from PqRecvLength and upwards */
	for (;;)
	{
		int			r;

		/* check signal, e.g., SIGTERM */
		CHECK_FOR_INTERRUPTS();
		
		/* use MSG_DNOTWAIT flag to perform nonblocking receive */
		r = recv(Socket(*port), port->RecvBuffer + port->RecvLength,
				 POOL_BUFFER_SIZE - port->RecvLength, 0);

		if (r < 0)
		{
			if (errno == EINTR || errno == EAGAIN)
			{
				continue;
			}

			/*
			 * Report broken connection
			 */
			ereport(LOG,
					(errcode_for_socket_access(),
					 errmsg("could not receive data from client: %m"),
					 errdetail("pool_recvbuf received data: %d, last return: %d", port->RecvLength, r)));
			return EOF;
		}
		if (r == 0)
		{
			ereport(DEBUG5,
					(errcode_for_socket_access(),
					 errmsg("receive nothing from pool manager: %m"),
					 errdetail("pool_recvbuf received data: %d, last return: %d", port->RecvLength, r)));
			return EOF;
		}
		/* r contains number of bytes read, so just incr length */
		port->RecvLength += r;
		return 0;
	}
}


/*
 * Put a known number of bytes into the connection buffer
 */
int
pool_putbytes(PoolPort *port, const char *s, size_t len)
{
	size_t		amount;

	while (len > 0)
	{
		/* If buffer is full, then flush it out */
		if (port->SendPointer >= POOL_BUFFER_SIZE)
			if (pool_flush(port))
				return EOF;
		amount = POOL_BUFFER_SIZE - port->SendPointer;
		if (amount > len)
			amount = len;
		memcpy(port->SendBuffer + port->SendPointer, s, amount);
		port->SendPointer += amount;
		s += amount;
		len -= amount;
	}
	return 0;
}


/* --------------------------------
 *		pool_flush		- flush pending output
 *
 *		returns 0 if OK, EOF if trouble
 * --------------------------------
 */
int
pool_flush(PoolPort *port)
{
	static int	last_reported_send_errno = 0;

	char	   *bufptr = port->SendBuffer;
	char	   *bufend = port->SendBuffer + port->SendPointer;

	while (bufptr < bufend)
	{
		int			r;

		r = send(Socket(*port), bufptr, bufend - bufptr, 0);

		if (r <= 0)
		{
			if (errno == EINTR)
				continue;		/* Ok if we were interrupted */

			if (errno != last_reported_send_errno)
			{
				last_reported_send_errno = errno;

				/*
				 * Handle a seg fault that may later occur in proc array
				 * when this fails when we are already shutting down
				 * If shutting down already, do not call.
				 */
				if (!proc_exit_inprogress)
					return 0;
			}

			/*
			 * We drop the buffered data anyway so that processing can
			 * continue, even though we'll probably quit soon.
			 */
			port->SendPointer = 0;
			return EOF;
		}

		last_reported_send_errno = 0;	/* reset after any successful send */
		bufptr += r;
	}

	port->SendPointer = 0;
	return 0;
}


/*
 * Put the pooler protocol message into the connection buffer
 */
int
pool_putmessage(PoolPort *port, char msgtype, const char *s, size_t len)
{
	uint		n32;

	if (pool_putbytes(port, &msgtype, 1))
		return EOF;

	n32 = pg_hton32((uint32) (len + 4));
	if (pool_putbytes(port, (char *) &n32, 4))
		return EOF;

	if (pool_putbytes(port, s, len))
		return EOF;

	return 0;
}

/*
 * Put the pooler message into the connection buffer
 */
int
pool_putmessage_without_msgtype(PoolPort *port, const char *s, size_t len)
{
	uint		n32;

	n32 = pg_hton32((uint32) (len + 4));
	if (pool_putbytes(port, (char *) &n32, 4))
		return EOF;

	if (pool_putbytes(port, s, len))
		return EOF;

	return 0;
}

/* message code('f'), size(8), node_count */
#define SEND_MSG_BUFFER_SIZE 9

/* message code('s'), result */
#define SEND_RES_BUFFER_SIZE 5 /* tag + length */
#define PID_INFO_HEAD_SIZE 9
#define SEND_PID_BUFFER_SIZE (PID_INFO_HEAD_SIZE + (MaxConnections - 1) * 4)

/* message code('s'), result , commandID*/
#define SEND_RES_BUFFER_HEDAER_SIZE   5

/*
 * Build up a message carrying file descriptors or process numbers and send them over specified
 * connection
 */
int
pool_sendfds(PoolPort *port, int *fds, int count, char *errbuf, int32 buf_len, uint32 seq)
{
	int32  r 	  = 0;
	int32  offset = 0;
	int32  err	  = 0;
	struct iovec iov[1];
	struct msghdr msg;
	char		buf[SEND_MSG_BUFFER_SIZE];
	uint		n32;
	int			controllen = 0;
	struct cmsghdr *cmptr = NULL;

	if (count > MAX_SEND_FD_NUM)
	{
		int loop = 0;
		while (count > 0)
		{
			int sendcount = count > MAX_SEND_FD_NUM ? MAX_SEND_FD_NUM : count;

			r = pool_sendfds(port, fds + loop * MAX_SEND_FD_NUM, sendcount, errbuf, buf_len, seq);
			if (r != 0)
			{
				return r;
			}

			loop++;
			count -= MAX_SEND_FD_NUM;
		}

		return 0;
	}

	controllen = CMSG_LEN(count * sizeof(int));
	buf[0] = 'f';
	n32 = pg_hton32(seq);
	memcpy(buf + 1, &n32, 4);
	n32 = pg_hton32((uint32) count);
	memcpy(buf + 5, &n32, 4);

	iov[0].iov_base = buf;
	iov[0].iov_len = SEND_MSG_BUFFER_SIZE;
	msg.msg_iov = iov;
	msg.msg_iovlen = 1;
	msg.msg_name = NULL;
	msg.msg_namelen = 0;
	if (count == 0)
	{
		msg.msg_control = NULL;
		msg.msg_controllen = 0;
	}
	else
	{
		if ((cmptr = malloc(controllen)) == NULL)
		{
			return EOF;
		}
		cmptr->cmsg_level = SOL_SOCKET;
		cmptr->cmsg_type = SCM_RIGHTS;
		cmptr->cmsg_len = controllen;
		msg.msg_control = (caddr_t) cmptr;
		msg.msg_controllen = controllen;
		/* the fd to pass */
		memcpy(CMSG_DATA(cmptr), fds, count * sizeof(int));
	}

	r	   = 0;
	offset = 0;
	while (offset < SEND_MSG_BUFFER_SIZE)
	{
		r = sendmsg(Socket(*port), &msg, 0);
		if (r < 0)
		{
			if (cmptr)
			{
				free(cmptr);
			}

			if (errbuf && buf_len)
			{
				err = errno;			
				snprintf(errbuf, buf_len, POOL_MGR_PREFIX"Pooler pool_sendfds flush failed for:%s", strerror(err));
			}
			else
			{
				err = errno;
				elog(LOG, POOL_MGR_PREFIX"Pooler pool_sendfds flush failed for:%s", strerror(err));
			}
			return EOF;
		}
		else
		{
			offset += r;
			if (SEND_MSG_BUFFER_SIZE == offset)
			{
				break;
			}
			else if (offset < SEND_MSG_BUFFER_SIZE)
			{
				/* send the rest data. */
				iov[0].iov_base = buf + offset;
				iov[0].iov_len  = SEND_MSG_BUFFER_SIZE - offset;
			}
			else
			{
				if (cmptr)
				{
					free(cmptr);
				}
				
				if (errbuf && buf_len)
				{
					err = errno;			
					snprintf(errbuf, buf_len, POOL_MGR_PREFIX"Pooler invalid send length:%d", offset);
				}
				else
				{
					err = errno;
					elog(LOG, POOL_MGR_PREFIX"Pooler invalid send length:%d", offset);
				}
				return EOF;
			}	
		}
	}

	if (cmptr)
	{
		free(cmptr);
	}
	return 0;
}

/*
 * Read a message from the specified connection carrying file descriptors
 */
int
pool_recvfds(PoolPort *port, int *fds, int count, bool *need_receive_errmsg, uint32 seq)
{
	int			r      = 0;
	int			offset = 0;
	uint		n32    = 0;
	char		buf[SEND_MSG_BUFFER_SIZE];
	struct iovec iov[1];
	struct msghdr msg;
	int    controllen = 0;
	struct cmsghdr *cmptr = NULL;

	if (count > MAX_SEND_FD_NUM)
	{
		int loop = 0;
		while (count > 0)
		{
			int recvcount = count > MAX_SEND_FD_NUM ? MAX_SEND_FD_NUM : count;

			r = pool_recvfds(port, fds + loop * MAX_SEND_FD_NUM, recvcount, need_receive_errmsg, seq);
			if (r != 0)
			{
				return r;
			}

			loop++;
			count -= MAX_SEND_FD_NUM;
		}

		return 0;
	}

	controllen = CMSG_LEN(count * sizeof(int));
	cmptr = malloc(CMSG_SPACE(count * sizeof(int)));
	if (cmptr == NULL)
	{
		elog(LOG, "[pool_recvfds]cmptr == NULL, return EOF");
		return EOF;
	}

	/* Use recv buf to receive data. */
	iov[0].iov_base = buf;
	iov[0].iov_len  = SEND_MSG_BUFFER_SIZE;
	msg.msg_iov = iov;
	msg.msg_iovlen = 1;
	msg.msg_name = NULL;
	msg.msg_namelen = 0;
	msg.msg_control = (caddr_t) cmptr;
	msg.msg_controllen = controllen;
	
	offset = 0;
	while (offset < SEND_MSG_BUFFER_SIZE)
	{
		/* check signal, e.g., SIGTERM */
		CHECK_FOR_INTERRUPTS();

		r = recvmsg(Socket(*port), &msg, 0);

		if (r < 0)
		{
			if (errno == EINTR || errno == EAGAIN)
			{
				/* interrupted or timeout, just retry */
				continue;
			}

			/*
			 * report broken connection
			 */
			ereport(LOG,
					(errcode_for_socket_access(),
					 errmsg("could not receive data from client: %m")));
			goto failure;
		}
		else if (0 == r)
		{
			elog(LOG, "[pool_recvfds]r == 0, errmsg=%s", strerror(errno));
			goto failure;
		}
		else
		{
			offset += r;
			if (SEND_MSG_BUFFER_SIZE == offset)
			{
				break;
			}
			else if (offset < SEND_MSG_BUFFER_SIZE)
			{
				/* only receive the left data, no more. */
				iov[0].iov_len = SEND_MSG_BUFFER_SIZE - offset;
				iov[0].iov_base = buf + offset;
			}
			else
			{
				ereport(LOG,
						(errcode_for_socket_access(),
						 errmsg("invalid msg len:%d received from pooler.", offset)));
				goto failure;
			}
		}
	}

	/* Verify response */
	if (buf[0] != 'f')
	{
		ereport(LOG,
				(errcode(ERRCODE_PROTOCOL_VIOLATION),
				 errmsg("unexpected message code")));
		goto failure;
	}

	memcpy(&n32, buf + 1, 4);
	n32 = pg_ntoh32(n32);
	if (n32 != seq)
	{
		ereport(FATAL,
				(errcode(ERRCODE_PROTOCOL_VIOLATION),
				 errmsg("invalid message seq, received %u, expect %u", n32, seq)));
		goto failure;
	}

	/*
	 * If connection count is 0 it means pool does not have connections
	 * to  fulfill request. Otherwise number of returned connections
	 * should be equal to requested count. If it not the case consider this
	 * a protocol violation. (Probably connection went out of sync)
	 */
	memcpy(&n32, buf + 5, 4);
	n32 = pg_ntoh32(n32);
	if (n32 == 0)
	{
		ereport(LOG,
				(errcode(ERRCODE_INSUFFICIENT_RESOURCES),
				 errmsg("failed to acquire connections")));

		if (need_receive_errmsg)
			*need_receive_errmsg = true;
		goto failure;
	}

	if (n32 != count)
	{
		ereport(LOG,
				(errcode(ERRCODE_PROTOCOL_VIOLATION),
				 errmsg("unexpected connection count")));
		goto failure;
	}

	if (msg.msg_controllen < sizeof(struct cmsghdr))
	{
		ereport(LOG,
				(errcode(ERRCODE_PROTOCOL_VIOLATION),
				 errmsg("unexpected msg_controllen %ld", msg.msg_controllen)));
		goto failure;
	}
	
	if (CMSG_FIRSTHDR(&msg) == NULL)
		goto failure;

	memcpy(fds, CMSG_DATA(CMSG_FIRSTHDR(&msg)), count * sizeof(int));
	free(cmptr);
	if (PoolConnectDebugPrint)
	{
		elog(LOG, "[pool_recvfds]success. fds=%p", fds);
	}
	return 0;
failure:
	free(cmptr);
	elog(LOG, "[pool_recvfds]failure, return EOF");
	return EOF;
}

/*
 * Send result to specified connection
 */
int
pool_sendres(PoolPort *port, int res, char *errbuf, int32 buf_len, bool need_log)
{
	int32       err;
	uint		n32;
	char		buf[SEND_RES_BUFFER_SIZE];
	int sended = 0, r, size = SEND_RES_BUFFER_SIZE;
	char *ptr = buf;

	/* Header */
	buf[0] = 's';
	/* Result */
	n32 = pg_hton32(res);
	memcpy(buf + 1, &n32, 4);

	for(;;)
	{
		r = send(Socket(*port), ptr + sended, size - sended, 0);
		if (r <= 0)
		{
			if (r < 0)
			{
				if(errno == EINTR)
					continue;
				else 
					goto failure;
			}
			
			if((r == 0) && (sended == size))
			{	
				if (need_log)
					elog(DEBUG5, "send size %d size %d.", sended, size);
				return 0;
			} 
			if (errbuf && buf_len)
			{
				err = errno;
				snprintf(errbuf, buf_len, POOL_MGR_PREFIX" pool_sendres send data failed for %s", strerror(err));
			}
			else
			{
				err = errno;
				if (need_log)
				{
					elog(LOG, POOL_MGR_PREFIX" pool_sendres send data failed for %s", strerror(err));
				}
			}
			return EOF;
		}
		
		sended += r;
		if(sended == size)
		{
			if (need_log)
			{
				elog(DEBUG5, "send size %d size %d.", sended, size);
			}
			return 0;
		}
		
	}
	return 0;

failure:
	return EOF;	
}

/*
 * Send result and commandId to specified connection, used for 's' command.
 */
int
pool_sendres_with_command_id(PoolPort *port, int res, CommandId cmdID, char *errbuf, int32 buf_len, char *errmsg, bool need_log)
{
	int32       err;
	int32		n32			 = 0;
	int32       offset       = 0;
	int32       send_buf_len = 0;
	char		*buf		 = NULL;
	int         sended = 0;
	int32       r = 0; 
	int32       size = 0;
	char        *ptr = NULL;

	/* protocol format: command + total_len + return_code + command_id + error_msg */
	if (errmsg)
	{
		/* error. */
		send_buf_len = 1 + sizeof(int32) + sizeof(int32) + sizeof(CommandId) + strlen(errmsg) + 1; /* reserved space for '\0' */
	}
	else
	{
		/* no error. */
		send_buf_len = 1 + sizeof(int32) + sizeof(int32) + sizeof(CommandId);
	}
	
	if (PoolConnectDebugPrint)
	{
		if (need_log)
		{
			elog(LOG, POOL_MGR_PREFIX"pool_sendres_with_command_id ENTER, res:%d commandid:%u", res, cmdID);
		}
	}

	buf = malloc(send_buf_len);
	if (NULL == buf)
	{
		if (errbuf && buf_len)
		{
			err = errno;
			snprintf(errbuf, buf_len, POOL_MGR_PREFIX" pool_sendres_with_command_id out of memory size:%d", send_buf_len);			
		}
		else
		{
			err = errno;
			{
				if (need_log)
				{
					elog(LOG, POOL_MGR_PREFIX" pool_sendres_with_command_id out of memory size:%d", send_buf_len);
				}
			}
		}		
		
		return EOF;
	}
	
	/* Header */
	buf[0] = 's';
	offset = 1;
	
	/* Total len */
	n32 = pg_hton32(send_buf_len);
	memcpy(buf + offset, &n32, 4);
	offset += 4;
	
	/* Result */
	n32 = pg_hton32(res);
	memcpy(buf + offset, &n32, 4);
	offset += 4;

	/* CommandID */
	n32 = pg_hton32(cmdID);
	memcpy(buf + offset, &n32, 4);
	offset += 4;

	if (offset < send_buf_len)
	{
		memcpy(buf + offset, errmsg, strlen(errmsg));
		offset += strlen(errmsg);
		buf[offset] = '\0';
	}

	size = send_buf_len;
	ptr  = buf;
	for(;;)
	{
		r = send(Socket(*port), ptr + sended, size - sended, 0);
		if (r <= 0)
		{
			if (r < 0)
			{
				if(errno == EINTR)
				{
					continue;
				}
				else 
				{
					err = errno;
					if (errbuf && buf_len)
					{
						snprintf(errbuf, buf_len, POOL_MGR_PREFIX" pool_sendres_with_command_id send data failed for %s", strerror(err));
					}
					else if (need_log)
					{
						elog(LOG, POOL_MGR_PREFIX" pool_sendres_with_command_id send data failed for %s", strerror(err));
					}
					goto failure;
				}
			}			
			else if((r == 0) && (sended == size))
			{
				if (PoolConnectDebugPrint)
				{
					if (need_log)
					{
						elog(LOG, POOL_MGR_PREFIX"pool_sendres_with_command_id EXIT, res:%d commandid:%u send succeed", res, cmdID);
					}
				}
				
				if (buf)
				{
					free(buf);
					buf = NULL;
				}
				return 0;
			} 

			
			if (errbuf && buf_len)
			{
				err = errno;
				snprintf(errbuf, buf_len, POOL_MGR_PREFIX" pool_sendres_with_command_id send data failed for %s", strerror(err));
			}
			else
			{
				err = errno;
				{
					if (need_log)
					{
						elog(LOG, POOL_MGR_PREFIX" pool_sendres_with_command_id EXIT, send data failed for %s", strerror(err));
					}
				}
			}
			
			if (buf)
			{
				free(buf);
				buf = NULL;
			}
			return EOF;
		}
		
		sended += r;
		if(sended == size)
		{
			if (PoolConnectDebugPrint)
			{
				if (need_log)
				{
					elog(LOG, POOL_MGR_PREFIX"pool_sendres_with_command_id EXIT, res:%d commandid:%u send succeed", res, cmdID);
				}
			}
			
			if (buf)
			{
				free(buf);
				buf = NULL;
			}
			return 0;
		}
		
	}
	
	if (buf)
	{
		free(buf);
		buf = NULL;
	}	
	return 0;

failure:
	if (buf)
	{
		free(buf);
		buf = NULL;
	}
	
	if (PoolConnectDebugPrint)
	{
		if (need_log)
		{
			elog(LOG, POOL_MGR_PREFIX"pool_sendres_with_command_id EXIT, res:%d commandid:%u send failed", res, cmdID);
		}
	}
	return EOF;	
}

/*
 * Read result from specified connection.
 * Return 0 at success or EOF at error. Used for 's' command.
 */
int
pool_recvres_with_commandID(PoolPort *port, CommandId *cmdID, const char *sql)
{
	int		offset = 0;
	int		pooler_res = 0;
	int		result_len = 0;
	uint	n32 = 0;
	char	buf[SEND_RES_BUFFER_HEDAER_SIZE];
	int		rc = 0;
	int32	size = SEND_RES_BUFFER_HEDAER_SIZE; /* init the size to header size */
	char	*ptr = buf;
	char	*error = NULL;

	if (pool_getbytes(port, ptr, size) == EOF)
		goto failure;

	rc = size;
	/* Verify response */
	if (buf[0] != 's')
	{
		ereport(ERROR,
				(errcode(ERRCODE_PROTOCOL_VIOLATION),
				 errmsg("[pool_recvres_with_commandID] unexpected message code:%c", buf[0])));
		goto failure;
	}

	/* Get result len. */
	memcpy(&n32, buf + 1, 4);
	result_len = pg_ntoh32(n32);
	offset = SEND_RES_BUFFER_HEDAER_SIZE;

	/* Set the actual result len. */
	size = result_len;
	ptr = palloc0(result_len);
	if (NULL == ptr)
	{
		ereport(ERROR,
				(errcode(ERRCODE_PROTOCOL_VIOLATION),
				 errmsg("[pool_recvres_with_commandID] out of memory, size%d", result_len)));
	}
	memcpy(ptr, buf, SEND_RES_BUFFER_HEDAER_SIZE);
	size = result_len - rc;

	if (pool_getbytes(port, ptr + rc, size) == EOF)
		goto failure;

	/* result */
	memcpy(&n32, ptr + offset, 4);
	n32 = pg_ntoh32(n32);
	pooler_res = n32;
	offset += 4;
	
	/* command ID */
	memcpy(&n32, ptr + offset, 4);
	n32 = pg_ntoh32(n32);	
	*cmdID = n32;
	offset += 4;

	/* ERROR msg */
	if (result_len > offset)
	{
		error = ptr + offset;
		if (pooler_res)
		{
			elog(ERROR, "MyPid %d SET Command:%s failed for %s", MyProcPid, sql, error);
		}
	}
	

	if (PoolConnectDebugPrint)
	{
		elog(LOG, "[pool_recvres_with_commandID] res=%d, cmdID=%u", pooler_res, *cmdID);
	}
	
	if (ptr && ptr != buf)
	{
		pfree(ptr);
	}
	return pooler_res;

failure:
	if (ptr && ptr != buf)
	{
		pfree(ptr);
	}
	*cmdID = InvalidCommandId;
	elog(LOG, "[pool_recvres_with_commandID] ERROR failed res=%d, cmdID=%u", pooler_res, *cmdID);
	return EOF;
}

static int
pool_recvres_internal(PoolPort *port, bool need_log)
{
	uint		n32 = 0;
	char		buf[SEND_RES_BUFFER_SIZE];
	int			size = SEND_RES_BUFFER_SIZE;
	char *ptr = buf;

	/* receive message header first */
	if (pool_getbytes(port, ptr, size) == EOF)
		goto failure;

	/* Verify response */
	if (buf[0] != 's')
	{
		ereport(LOG,
				(errcode(ERRCODE_PROTOCOL_VIOLATION),
				 errmsg("unexpected message code:%c", buf[0])));
		goto failure;
	}

	memcpy(&n32, buf + 1, 4);
	n32 = pg_ntoh32(n32);
	if (n32 != 0 && need_log)
	{
		ereport(LOG,
				(errcode(ERRCODE_PROTOCOL_VIOLATION),
				 errmsg("pool_recvres return code:%d", n32)));
	}

	return n32;

failure:
	return EOF;
}
/*
 * Read result from specified connection.
 * Return 0 at success or EOF at error.
 */
int
pool_recvres(PoolPort *port, bool need_log)
{
	int res;
	
	PG_TRY();
	{
		res = pool_recvres_internal(port, need_log);
	}
	PG_CATCH();
	{
		handle_close_and_reset_all();
		PoolManagerDisconnect();
		PG_RE_THROW();
	}
	PG_END_TRY();
	
	return res;
}

/*
 * Read a message from the specified connection carrying pid numbers
 * of transactions interacting with pooler
 */
int
pool_recvpids(PoolPort *port, int **pids, uint32 seq)
{
	int			i   = 0;
	uint		n32 = 0;
	int  		size = PID_INFO_HEAD_SIZE;
	char 		*ptr = NULL;
	int			rcv_req = 0;
	char		head[PID_INFO_HEAD_SIZE];

	ptr = head;

	/*
	 * Buffer size is upper bounded by the maximum number of connections,
	 * as in the pooler each connection has one Pooler Agent.
	 */
	if (pool_getbytes(port, ptr, size) == EOF)
		goto failure;

	/* Verify response */
	if (*ptr != 'p')
	{
		ereport(LOG,
				(errcode(ERRCODE_PROTOCOL_VIOLATION),
				 errmsg("unexpected message code %c", *ptr)));
		goto failure;
	}

	++ptr;
	memcpy(&rcv_req, ptr, 4);
	rcv_req = pg_ntoh32(rcv_req);
	if (rcv_req != seq)
		elog(FATAL, "recv seq not matched recved:%d, expected %d", rcv_req, seq);
	ptr += 4;
	memcpy(&n32, ptr, 4);
	n32 = pg_ntoh32(n32);
	if (n32 == 0 || n32 >= MaxConnections)
	{
		elog(WARNING, "No transaction to abort");
		return 0;
	}

	*pids = (int *)palloc(sizeof(int) * n32);

	if (pool_getbytes(port, (char *) *pids, sizeof(int) * n32) == EOF)
		goto failure;

	for (i = 0; i < n32; i++)
	{
		(*pids)[i] = pg_ntoh32((*pids)[i]);
#ifdef _PG_REGRESS_
		check_pid((*pids)[i]);
#endif
	}

	if (PoolConnectDebugPrint)
	{
		elog(LOG, "received size %d n32 %d.", size, n32);
	}

	return n32;

failure:
	ereport(LOG,
				(errcode_for_socket_access(),
				 errmsg("recvpids failure size %d count %d.", size, n32)));
	return 0;
}

/*
 * Send a message containing pid numbers to the specified connection
 */
int
pool_sendpids(PoolPort *port, int *pids, int count, char *errbuf, int32 buf_len, uint32 seq)
{
	int 		i      = 0;
	int32       err    = 0;
	char		*buf   = NULL;
	uint		n32    = 0;
	int 		size   = 0;
	int 		sended = 0;
	int			r	   = 0;
	char 		*ptr   = NULL;

	size = PID_INFO_HEAD_SIZE + count * sizeof(int);
	buf = (char*)malloc(size);
	if (NULL == buf)
	{
		err = errno;
		snprintf(errbuf + strlen(errbuf) + 1, buf_len - strlen(errbuf) - 1,
				 POOL_MGR_PREFIX"pool_sendpids malloc %d memory failed.",
				 size);
		return EOF;
	}

	ptr = buf;

	*ptr = 'p';
	++ptr;

	n32 = pg_hton32((uint32) seq);
	memcpy(ptr, &n32, 4);
	ptr += 4;

	n32 = pg_hton32((uint32) count);
	memcpy(ptr, &n32, 4);
	ptr += 4;

	for (i = 0; i < count; i++)
	{
		int n = pg_hton32((uint32) pids[i]);
		memcpy(ptr, &n, 4);
		ptr += 4;
	}

	/* try to send data. */
	sended = 0;
	ptr = buf;
	for(;;)
	{
		r = send(Socket(*port), ptr + sended, size - sended, 0);
		if (r < 0)
		{
			if(errno == EINTR)
				continue;
			else 
				goto failure;
		}
		
		if(r == 0)
		{
			if(sended == size)
			{	
				if (!errbuf)
				{
					elog(DEBUG1, "send size %d size %d count %d.", sended, size, count);
				}
				free(buf);
				return 0;
			} 
			else 
			{
				goto failure;
			}
		}
		sended += r;
		if(sended == size)
		{
			if (!errbuf)
			{
				elog(DEBUG1, "send size %d size %d count %d.", sended, size, count);
			}
			free(buf);
			return 0;
		}
	}
failure:
	if (errbuf && buf_len)
	{
		err = errno;
		snprintf(errbuf+strlen(errbuf)+1, buf_len-strlen(errbuf)-1, 
				POOL_MGR_PREFIX"pool_sendpids send data failed for %s. failure send size %d size %d count %d.", 
				strerror(err), sended, size, count);
	}
	else
	{
		err = errno;
		elog(LOG, POOL_MGR_PREFIX"pool_sendpids send data failed for %s. failure send size %d size %d count %d.",
				strerror(err), sended, size, count);
	}
	free(buf);
	return EOF;
}
