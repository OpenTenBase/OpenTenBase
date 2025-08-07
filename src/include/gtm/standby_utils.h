/*-------------------------------------------------------------------------
 *
 * standby_utils.h
 *
 *
 * Portions Copyright (c) 1996-2010, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 *
 * src/include/gtm/standby_utils.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef STANDBY_UTILS_H
#define STANDBY_UTILS_H

#include "gtm/gtm_lock.h"

extern bool Recovery_IsStandby(void);
extern void Recovery_StandbySetStandby(bool standby);
extern void Recovery_StandbySetConnInfo(const char *addr, int port);
extern int Recovery_StandbyGetActivePort(void);
extern char* Recovery_StandbyGetActiveAddress(void);
extern void Recovery_InitStandbyLock(void);

#endif
