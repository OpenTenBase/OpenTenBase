/*-------------------------------------------------------------------------
 *
 * atxact.h
 *	  autonomous transaction system definitions
 *
 *
 * Portions Copyright (c) 2021 Tencent Development Group
 *
 * src/include/access/atxact.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef _ATXACT_H
#define _ATXACT_H

#include "postgres.h"

#include "access/xact.h"
#include "datatype/timestamp.h"
#include "executor/spi_priv.h"
#include "pgxc/execRemote.h"
#include "pgxc/poolmgr.h"
#include "pgxc/squeue.h"
#include "storage/large_object.h"
#include "storage/proc.h"
#include "storage/predicate_internals.h"
#include "storage/smgr.h"
#include "utils/guc_tables.h"
#include "utils/palloc.h"
#include "utils/resowner.h"

#endif
