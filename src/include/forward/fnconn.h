/*-------------------------------------------------------------------------
 *
 * fnconn.h
 *
 *	  Definitions for the Forward-Seesion communications.
 *
 *
 * Portions Copyright (c) 1996-2011, PostgreSQL Global Development Group
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 *
 * src/include/forward/fnconn.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef _FORWARD_CONN_H
#define _FORWARD_CONN_H

#include "lib/stringinfo.h"
#include "pgxc/nodemgr.h"
#include "forward/fnbuf_internals.h"
#include "forward/fnbufpage.h"

#define FNCONN_BUFFER_SIZE 1024

extern NodeDefinition		*fn_myself;
extern int	fn_send_thread_num;
extern int	fn_recv_thread_num;
extern int	fn_buffer_queue_len;

typedef struct ForwardPort
{
	/* file descriptors */
	int			fdsock;
} ForwardPort;

/* per forwardNode connect info */
typedef struct ForwardConn
{
	NodeDefinition   node;
	ForwardPort     *port;
	struct addrinfo *addr;      /* backend address currently being tried */
	bool             fail;      /* connection failed or broken */
	int				 pos;
	char			*buffer;
} ForwardConn;

typedef struct FnMgrStartupPacket		/* content for ForwardMgrMsgType_Startup */
{
	char			identify[NAMEDATALEN];
	NodeDefinition	node;				/* copy of fn_myself */
	int				index;
} FnMgrStartupPacket;

extern bool InitForwardConns(void);
extern void TermFidBackendProc(const char *reason);
extern bool SendFnPage(FnPage page, uint16 nodeid, FnBufferDesc *buf, pg_atomic_uint32 *num_to_send);
extern void fn_set_thrd_name(const char * name);
extern void flog(const char *format,...) pg_attribute_printf(1, 2);

extern bool SendData(int fd, char *bufptr, int len);
extern int fn_thread_read_buffer(int fd, char *buf, int buf_len);

#endif   /* _FORWARD_CONN_H */
