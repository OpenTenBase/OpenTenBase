/*-------------------------------------------------------------------------
 *
 * stub_relcache.cpp
 *	  a stub for relcache.c
 *
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 *
 *
 * IDENTIFICATION
 *	  src/backend/stub/utils/cache/stub_relcache.c
 *
 *-------------------------------------------------------------------------
 */

#include "../../../../../utils/cache/relcache.c"
#include "stub/utils/stub_relcache.h"

void
stub_RelationCacheInsert(Relation rel, bool rep)
{
	RelationCacheInsert(rel, rep);
}

Relation
stub_RelationIdCacheLookup(Oid id)
{
	Relation rel;

	RelationIdCacheLookup(id, rel);
	return rel;
}

void
stub_RelationCacheDelete(Relation rel)
{
	RelationCacheDelete(rel);
}
