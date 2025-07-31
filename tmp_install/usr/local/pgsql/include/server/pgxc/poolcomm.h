/*-------------------------------------------------------------------------
 *
 * poolcomm.h
 *
 *      Definitions for the Pooler-Seesion communications.
 *
 *
 * Portions Copyright (c) 1996-2011, PostgreSQL Global Development Group
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 *
 * This source code file contains modifications made by THL A29 Limited ("Tencent Modifications").
 * All Tencent Modifications are Copyright (C) 2023 THL A29 Limited.
 *
 * src/include/pgxc/poolcomm.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef POOLCOMM_H
#define POOLCOMM_H

#include "lib/stringinfo.h"
#include "storage/s_lock.h"

#define POOL_MGR_PREFIX "PoolMgr: "

#define POOL_BUFFER_SIZE 1024
#define Socket(port) (port).fdsock

#define POOL_ERR_MSG_LEN 256

typedef struct
{
    /* file descriptors */
    int            fdsock;
    /* receive buffer */
    int            RecvLength;
    int            RecvPointer;
    char        RecvBuffer[POOL_BUFFER_SIZE];
    /* send buffer */
    int            SendPointer;
    char        SendBuffer[POOL_BUFFER_SIZE];
#ifdef __OPENTENBASE__
    /* error code */
    slock_t     lock;
    int         error_code;
    char        err_msg[POOL_ERR_MSG_LEN];
#endif
} PoolPort;

extern int    pool_listen(unsigned short port, const char *unixSocketName);
extern int    pool_connect(unsigned short port, const char *unixSocketName);
extern int    pool_getbyte(PoolPort *port);
extern int    pool_pollbyte(PoolPort *port);
extern int    pool_getmessage(PoolPort *port, StringInfo s, int maxlen);
extern int    pool_getbytes(PoolPort *port, char *s, size_t len);
extern int    pool_putmessage(PoolPort *port, char msgtype, const char *s, size_t len);
extern int    pool_putbytes(PoolPort *port, const char *s, size_t len);
extern int    pool_flush(PoolPort *port);
/*extern int    pool_sendfds(PoolPort *port, int *fds, int count);*/
extern int  pool_sendfds(PoolPort *port, int *fds, int count, char *errbuf, int32 buf_len);
extern int	pool_recvfds(PoolPort *port, int *fds, int count);
extern int	pool_sendres(PoolPort *port, int res, char *errbuf, int32 buf_len, bool need_log);
extern int	pool_recvres(PoolPort *port, bool need_log);
extern int	pool_sendpids(PoolPort *port, int *pids, int count, char *errbuf, int32 buf_len);
extern int	pool_recvpids(PoolPort *port, int **pids);
extern int	pool_sendres_with_command_id(PoolPort *port, int res, CommandId cmdID, char *errbuf, int32 buf_len, char *errmsg, bool need_log);
extern int  pool_recvres_with_commandID(PoolPort *port, CommandId *cmdID, const char *sql);
#endif   /* POOLCOMM_H */
