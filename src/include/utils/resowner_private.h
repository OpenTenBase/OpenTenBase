/*-------------------------------------------------------------------------
 *
 * resowner_private.h
 *	  POSTGRES resource owner private definitions.
 *
 * See utils/resowner/README for more info.
 *
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/utils/resowner_private.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef RESOWNER_PRIVATE_H
#define RESOWNER_PRIVATE_H

#include "executor/tqueueThread.h"
#include "forward/fnbuf.h"
#include "storage/dsm.h"
#include "storage/fd.h"
#include "storage/lock.h"
#include "utils/catcache.h"
#include "utils/plancache.h"
#include "utils/resowner.h"
#include "utils/snapshot.h"


/* support for buffer refcount management */
extern void ResourceOwnerEnlargeBuffers(ResourceOwner owner);
extern void ResourceOwnerEnlargeBuffersToSize(ResourceOwner owner, uint32 size);
extern void ResourceOwnerRememberBuffer(ResourceOwner owner, Buffer buffer);
extern void ResourceOwnerForgetBuffer(ResourceOwner owner, Buffer buffer);

/* support for local lock management */
extern void ResourceOwnerRememberLock(ResourceOwner owner, LOCALLOCK *locallock);
extern void ResourceOwnerForgetLock(ResourceOwner owner, LOCALLOCK *locallock);

#ifdef _PG_ORCL_
extern void ResourceOwnerUpdateLock(ResourceOwner owner, LOCALLOCK *old_lock, LOCALLOCK *new_lock);
#endif

/* support for catcache refcount management */
extern void ResourceOwnerEnlargeCatCacheRefs(ResourceOwner owner);
extern void ResourceOwnerRememberCatCacheRef(ResourceOwner owner,
								 HeapTuple tuple);
extern void ResourceOwnerForgetCatCacheRef(ResourceOwner owner,
							   HeapTuple tuple);
extern void ResourceOwnerEnlargeCatCacheListRefs(ResourceOwner owner);
extern void ResourceOwnerRememberCatCacheListRef(ResourceOwner owner,
									 CatCList *list);
extern void ResourceOwnerForgetCatCacheListRef(ResourceOwner owner,
								   CatCList *list);

/* support for relcache refcount management */
extern void ResourceOwnerEnlargeRelationRefs(ResourceOwner owner);
extern void ResourceOwnerRememberRelationRef(ResourceOwner owner,
								 Relation rel);
extern void ResourceOwnerForgetRelationRef(ResourceOwner owner,
							   Relation rel);

/* support for plancache refcount management */
extern void ResourceOwnerEnlargePlanCacheRefs(ResourceOwner owner);
extern void ResourceOwnerRememberPlanCacheRef(ResourceOwner owner,
								  CachedPlan *plan);
extern void ResourceOwnerForgetPlanCacheRef(ResourceOwner owner,
								CachedPlan *plan);

/* support for tupledesc refcount management */
extern void ResourceOwnerEnlargeTupleDescs(ResourceOwner owner);
extern void ResourceOwnerRememberTupleDesc(ResourceOwner owner,
							   TupleDesc tupdesc);
extern void ResourceOwnerForgetTupleDesc(ResourceOwner owner,
							 TupleDesc tupdesc);

/* support for snapshot refcount management */
extern void ResourceOwnerEnlargeSnapshots(ResourceOwner owner);
extern void ResourceOwnerRememberSnapshot(ResourceOwner owner,
							  Snapshot snapshot);
extern void ResourceOwnerForgetSnapshot(ResourceOwner owner,
							Snapshot snapshot);

/* support for temporary file management */
extern void ResourceOwnerEnlargeFiles(ResourceOwner owner);
extern void ResourceOwnerRememberFile(ResourceOwner owner,
						  File file);
extern void ResourceOwnerForgetFile(ResourceOwner owner,
						File file);

/* support for dynamic shared memory management */
extern void ResourceOwnerEnlargeDSMs(ResourceOwner owner);
extern void ResourceOwnerRememberDSM(ResourceOwner owner,
						 dsm_segment *);
extern void ResourceOwnerForgetDSM(ResourceOwner owner,
					   dsm_segment *);
/* support for JITContext management */
extern void ResourceOwnerEnlargeJIT(ResourceOwner owner);
extern void ResourceOwnerRememberJIT(ResourceOwner owner,
						 Datum handle);
extern void ResourceOwnerForgetJIT(ResourceOwner owner,
					   Datum handle);

#ifdef XCP
/* support for prepared statement management */
extern void ResourceOwnerEnlargePreparedStmts(ResourceOwner owner);
extern void ResourceOwnerRememberPreparedStmt(ResourceOwner owner,
						  char *stmt);
extern void ResourceOwnerForgetPreparedStmt(ResourceOwner owner,
						char *stmt);
#endif

/* support for fnbuffer refcount management */
extern void ResourceOwnerEnlargeFnBuffers(ResourceOwner owner);
extern void ResourceOwnerRememberFnBuffer(ResourceOwner owner, FnBuffer buffer);
extern void ResourceOwnerForgetFnBuffer(ResourceOwner owner, FnBuffer buffer);
/* forward send resource */
extern void ResourceOwnerEnlargeFnSend(ResourceOwner owner);
extern void ResourceOwnerRememberFnSend(ResourceOwner owner, FnSndQueueEntry *entry);
extern void ResourceOwnerForgetFnSend(ResourceOwner owner, FnSndQueueEntry *entry);
/* forward receive resource */
extern void ResourceOwnerEnlargeFnRecv(ResourceOwner owner);
extern void ResourceOwnerRememberFnRecv(ResourceOwner owner, TupleQueueReceiver *entry);
extern void ResourceOwnerForgetFnRecv(ResourceOwner owner, TupleQueueReceiver *entry);
/* fd resource */
extern void ResourceOwnerEnlargeFd(ResourceOwner owner);
extern void ResourceOwnerRememberFd(ResourceOwner owner, int fd);
extern void ResourceOwnerForgetFd(ResourceOwner owner, int fd);

#ifdef _MLS_
extern const char * ResourceOwnerGetName(void);
extern uint32 GetResourceArrayNitems(void);
extern uint32 GetResourceArrayLastidx(void);

#endif

#endif							/* RESOWNER_PRIVATE_H */
