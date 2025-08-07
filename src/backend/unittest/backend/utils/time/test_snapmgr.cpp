/*
 * test_snapmgr.cpp
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
#include "utils/tqual.h"
}

#undef Max
#undef StrNCpy

#include "gtest/gtest.h"

TEST(XactSnapshot, save)
{
	TransactionExecuteContext	e;

	TopTransactionContext = e.frame_context
			= AllocSetContextCreate(TopMemoryContext,
									  "Transaction Execution Frame",
									  32 * 1024,
									  32 * 1024,
									  32 * 1024);
	AtXact_SaveSnapshot(&e);

	TopTransactionContext = NULL;
	MemoryContextDelete(e.frame_context);
}

TEST(XactSnapshot, save_reset_restore)
{
	TransactionExecuteContext	e;

	TopTransactionContext = e.frame_context
			= AllocSetContextCreate(TopMemoryContext,
									  "Transaction Execution Frame",
									  32 * 1024,
									  32 * 1024,
									  32 * 1024);
	CatalogSnapshotData.xmin = 1239;
	AtXact_SaveSnapshot(&e);
	AtXact_ResetSnapshot();
	AtXact_RestoreSnapshot(&e);

	TopTransactionContext = NULL;
	MemoryContextDelete(e.frame_context);

	ASSERT_EQ(CatalogSnapshotData.xmin, 1239);
}
