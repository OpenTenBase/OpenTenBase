/*
 * test_guc.cpp
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
}

#undef Max
#undef StrNCpy

#include "gtest/gtest.h"

TEST(XactGUC, save_reset_restore)
{
	TransactionExecuteContext	e;

	TopTransactionContext = e.frame_context
			= AllocSetContextCreate(TopMemoryContext,
									  "Transaction Execution Frame",
									  32 * 1024,
									  32 * 1024,
									  32 * 1024);

	NewGUCNestLevel();

	SetConfigOption("statement_timeout", "129", PGC_SUSET, PGC_S_SESSION);

	ASSERT_EQ(StatementTimeout, 129);
	AtXact_SaveGUC(&e);
	ASSERT_EQ(StatementTimeout, 129);
	AtXact_ResetGUC(&e);
	ASSERT_EQ(StatementTimeout, 0);
	AtXact_RestoreGUC(&e);
	ASSERT_EQ(StatementTimeout, 129);

	AtEOXact_GUC(true, 1);

	TopTransactionContext = NULL;
	MemoryContextDelete(e.frame_context);
}
