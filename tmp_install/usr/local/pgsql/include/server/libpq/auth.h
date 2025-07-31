/*-------------------------------------------------------------------------
 *
 * auth.h
 *      Definitions for network authentication routines
 *
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * This source code file contains modifications made by THL A29 Limited ("Tencent Modifications").
 * All Tencent Modifications are Copyright (C) 2023 THL A29 Limited.
 * 
 * src/include/libpq/auth.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef AUTH_H
#define AUTH_H

#include "libpq/libpq-be.h"

extern char *pg_krb_server_keyfile;
extern bool pg_krb_caseins_users;
extern char *pg_krb_realm;

extern void ClientAuthentication(Port *port);
#ifdef __OPENTENBASE__
extern Size UserAuthShmemSize(void);
extern void UserAuthShmemInit(void);
#endif


/* Hook for plugins to get control in ClientAuthentication() */
typedef void (*ClientAuthentication_hook_type) (Port *, int);
extern PGDLLIMPORT ClientAuthentication_hook_type ClientAuthentication_hook;

#ifdef __OPENTENBASE__
extern int    pgua_max;            
extern int    account_lock_time; 
extern int    account_lock_threshold; 
extern bool enable_lock_account;
extern bool lock_account_print;
#endif

#endif                            /* AUTH_H */
