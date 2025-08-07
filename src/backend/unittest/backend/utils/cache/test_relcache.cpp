/*
 * test_relcache.cpp
 */

extern "C"
{
#include "postgres.h"
#include "c.h"

#include "access/atxact.h"
#include "access/xlog.h"
#include "storage/bufpage.h"
#include "utils/memdebug.h"
#include "utils/memutils.h"
#include "utils/resowner.h"

#include "stub/access/stub_atxact.h"
#include "stub/utils/stub_relcache.h"
}

#undef Max
#undef StrNCpy

#include "gtest/gtest.h"

TEST(XactRelationCache, save_reset_restore)
{
	TransactionExecuteContext	*e = stub_AllocateTransactionExecContext();

	TopTransactionContext = AllocSetContextCreate(TopMemoryContext,
									  "Transaction Execution Frame",
									  32 * 1024,
									  32 * 1024,
									  32 * 1024);

	AtXact_SaveRelationCache(e);
	AtXact_RestoreRelationCache(e);

	TopTransactionContext = NULL;
	stub_FreeTransactionExecuteContext(e);
}

TEST(XactRelationCache, restore_one_relation)
{
	TransactionExecuteContext	*e = stub_AllocateTransactionExecContext();
	Relation	rel;

	TopTransactionContext = AllocSetContextCreate(TopMemoryContext,
									  "Transaction Execution Frame",
									  32 * 1024,
									  32 * 1024,
									  32 * 1024);

	/* insert a relation */
	rel = (Relation) palloc0(sizeof(RelationData));
	rel->rd_refcnt = 2;
	rel->rd_id = 30;
	stub_RelationCacheInsert(rel, false);
	ASSERT_EQ(stub_RelationIdCacheLookup(30), rel);

	AtXact_SaveRelationCache(e);
	ASSERT_EQ(stub_RelationIdCacheLookup(30), (Relation) NULL);

	AtXact_RestoreRelationCache(e);
	ASSERT_EQ(stub_RelationIdCacheLookup(30), rel);
	ASSERT_EQ(stub_RelationIdCacheLookup(30)->rd_refcnt, 2);

	stub_RelationCacheDelete(rel);
	TopTransactionContext = NULL;

	stub_FreeTransactionExecuteContext(e);
}

TEST(XactRelationCache, restore_more_relation)
{
	TransactionExecuteContext	*e = stub_AllocateTransactionExecContext();
	Relation	rel;
	Relation	rel2;

	TopTransactionContext = AllocSetContextCreate(TopMemoryContext,
									  "Transaction Execution Frame",
									  32 * 1024,
									  32 * 1024,
									  32 * 1024);

	/* insert a relation */
	rel = (Relation) palloc0(sizeof(RelationData));
	rel->rd_refcnt = 2;
	rel->rd_id = 30;
	stub_RelationCacheInsert(rel, false);

	rel2 = (Relation) palloc0(sizeof(RelationData));
	rel2->rd_refcnt = 1;
	rel2->rd_id = 40;
	stub_RelationCacheInsert(rel2, false);
	ASSERT_EQ(stub_RelationIdCacheLookup(30), rel);
	AtXact_SaveRelationCache(e);
	ASSERT_EQ(stub_RelationIdCacheLookup(30), (Relation) NULL);
	ASSERT_EQ(stub_RelationIdCacheLookup(40), (Relation) NULL);
	AtXact_RestoreRelationCache(e);

	ASSERT_EQ(stub_RelationIdCacheLookup(30), rel);
	ASSERT_EQ(stub_RelationIdCacheLookup(40), rel2);

	stub_RelationCacheDelete(rel);
	stub_RelationCacheDelete(rel2);
	TopTransactionContext = NULL;

	stub_FreeTransactionExecuteContext(e);
}
