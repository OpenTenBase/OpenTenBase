/*
 * test_resowner.cpp
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

TEST(XactResourceOwner, save)
{
	TransactionExecuteContext	e;

	AtXact_SaveResourceOwner(&e);

	ASSERT_EQ(e.res_owner._TopTransactionResourceOwner, TopTransactionResourceOwner);
	ASSERT_EQ(e.res_owner._CurTransactionResourceOwner, CurTransactionResourceOwner);
	ASSERT_EQ(e.res_owner._CurrentResourceOwner, CurrentResourceOwner);
}

TEST(XactResourceOwner, save_reset_restore)
{
	TransactionExecuteContext	e;
	ResourceOwner	tr = TopTransactionResourceOwner,
					cr = CurTransactionResourceOwner,
					ccr = CurrentResourceOwner;

	AtXact_SaveResourceOwner(&e);

	ASSERT_EQ(e.res_owner._TopTransactionResourceOwner, TopTransactionResourceOwner);
	ASSERT_EQ(e.res_owner._CurTransactionResourceOwner, CurTransactionResourceOwner);
	ASSERT_EQ(e.res_owner._CurrentResourceOwner, CurrentResourceOwner);

	AtXact_ResetResourceOwner();

	ASSERT_EQ(NULL, TopTransactionResourceOwner);
	ASSERT_EQ(NULL, CurTransactionResourceOwner);
	ASSERT_EQ(NULL, CurrentResourceOwner);

	AtXact_RestoreResourceOwner(&e);

	ASSERT_EQ(tr, TopTransactionResourceOwner);
	ASSERT_EQ(cr, CurTransactionResourceOwner);
	ASSERT_EQ(ccr, CurrentResourceOwner);
}
