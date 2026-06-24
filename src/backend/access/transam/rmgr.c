/*
 * rmgr.c
 *
 * Resource managers definition
 *
 * src/backend/access/transam/rmgr.c
 */
#include "postgres.h"

#include "access/clog.h"
#include "access/commit_ts.h"
#include "access/csnlog.h"
#include "access/ginxlog.h"
#include "access/gistxlog.h"
#include "access/generic_xlog.h"
#include "access/hash_xlog.h"
#include "access/heapam_xlog.h"
#include "access/brin_xlog.h"
#include "access/multixact.h"
#include "access/nbtxlog.h"
#include "access/spgxlog.h"
#include "access/xact.h"
#include "access/xlog_internal.h"
#include "catalog/storage_xlog.h"
#include "commands/dbcommands_xlog.h"
#include "commands/sequence.h"
#include "commands/tablespace.h"
#ifdef PGXC
#include "pgxc/barrier.h"
#endif
#include "replication/message.h"
#include "replication/origin.h"
#include "storage/standby.h"
#ifdef _SHARDING_
#include "storage/extentmapping.h"
#include "storage/extent_xlog.h"
#endif
#include "utils/relmapper.h"
 #ifdef _PUB_SUB_RELIABLE_
#include "access/replslotdesc.h"
#endif

/* must be kept in sync with RmgrData definition in xlog_internal.h */
#define PG_RMGR(symname,name,redo,desc,identify,startup,cleanup,mask) \
	{ name, redo, desc, identify, startup, cleanup, mask },

const RmgrData RmgrTable[RM_MAX_ID + 1] = {
#include "access/rmgrlist.h"
};
