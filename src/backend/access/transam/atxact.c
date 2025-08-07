/*-------------------------------------------------------------------------
 *
 * atxact.c
 *	  autonomous transaction manangement.
 *
 * Portions Copyright (c) 2021 Tencent Development Group
 *
 *
 * IDENTIFICATION
 *	  src/backend/access/transam/atxact.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "pgstat.h"

#include "access/atxact.h"
#include "access/twophase.h"
#include "utils/snapmgr.h"
#include "miscadmin.h"
#define AT_DEFAULT_SIZES \
	32 * 1024, 32 * 1024, 32 * 1024
