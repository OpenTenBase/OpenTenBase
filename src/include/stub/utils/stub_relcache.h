/*-------------------------------------------------------------------------
 *
 * stub_relcache.h
 *	  Stub interface for relcache.
 *
 *
 * Portions Copyright (c) 2021 Tencent Development Group
 *
 * src/include/stub/utils/stub_relcache.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef STUB_RELCACHE_H
#define STUB_RELCACHE_H

#include "access/tupdesc.h"
#include "lib/ilist.h"
#include "nodes/bitmapset.h"
#include "utils/relcache.h"

extern void stub_RelationCacheInsert(Relation rel, bool rep);
extern Relation stub_RelationIdCacheLookup(Oid id);
extern void stub_RelationCacheDelete(Relation rel);

#endif
