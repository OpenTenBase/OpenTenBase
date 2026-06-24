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

#include "stub/access/stub_atxact.h"
}

#undef Max
#undef StrNCpy

#include "gtest/gtest.h"

TEST(XactMXState, save_reset_restore)
{
	TransactionExecuteContext	*e = stub_AllocateTransactionExecContext();

	TopTransactionContext = e->frame_context;

	AtXact_SaveMXState(e);
	AtXact_RestoreMXState(e);

	TopTransactionContext = NULL;
	stub_FreeTransactionExecuteContext(e);
}

TEST(XactMXState, save_reset_restore_members_ne)
{
	TransactionExecuteContext	*e = stub_AllocateTransactionExecContext();

	TopTransactionContext = e->frame_context;

	AtXact_SaveMXState(e);
	e->mxstate._MXactCacheMembers = 20;
	AtXact_RestoreMXState(e);

	e->mxstate._MXactCacheMembers = 0;
	AtXact_SaveMXState(e);
	ASSERT_NE(e->mxstate._MXactCacheMembers, 10);

	TopTransactionContext = NULL;
	stub_FreeTransactionExecuteContext(e);
}

TEST(XactMXState, save_reset_restore_members)
{
	TransactionExecuteContext	*e = stub_AllocateTransactionExecContext();

	TopTransactionContext = e->frame_context;

	AtXact_SaveMXState(e);
	e->mxstate._MXactCacheMembers = 10;
	AtXact_RestoreMXState(e);

	e->mxstate._MXactCacheMembers = 0;
	AtXact_SaveMXState(e);
	ASSERT_EQ(e->mxstate._MXactCacheMembers, 10);

	TopTransactionContext = NULL;
	stub_FreeTransactionExecuteContext(e);
}
