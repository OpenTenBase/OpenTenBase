/*
 * test_lock.cpp
 */

extern "C"
{
#include "postgres.h"
#include "c.h"

#include "access/atxact.h"
#include "access/xlog.h"
#include "storage/bufpage.h"
#include "storage/lmgr.h"
#include "utils/memdebug.h"
#include "utils/memutils.h"
#include "utils/resowner.h"

#include "stub/access/stub_atxact.h"
}

#undef Max
#undef StrNCpy

#include "gtest/gtest.h"

TEST(XactLocks, base_call)
{
	TransactionExecuteContext	*e = stub_AllocateTransactionExecContext();

	TopTransactionContext = AllocSetContextCreate(TopMemoryContext,
									  "Transaction Execution Frame",
									  32 * 1024,
									  32 * 1024,
									  32 * 1024);

	AtXact_SaveLocks(e);
	AtXact_RestoreLocks(e);

	TopTransactionContext = NULL;
	stub_FreeTransactionExecuteContext(e);
}

TEST(XactLocks, vxid_lock)
{
	VirtualTransactionId	vxid;
	TransactionExecuteContext	*e = stub_AllocateTransactionExecContext();

	TopTransactionContext = AllocSetContextCreate(TopMemoryContext,
									  "Transaction Execution Frame",
									  32 * 1024,
									  32 * 1024,
									  32 * 1024);

	vxid.backendId = MyBackendId;
	vxid.localTransactionId = 40;
	VirtualXactLockTableInsert(vxid);

	ASSERT_EQ(MyProc->fpLocalTransactionId, 40);
	PreSaveXactLocks();
	AtXact_SaveLocks(e);
	ASSERT_EQ(MyProc->fpLocalTransactionId, InvalidLocalTransactionId);

	AtXact_RestoreLocks(e);
	ASSERT_EQ(MyProc->fpLocalTransactionId, 40);
	VirtualXactLockTableCleanup();

	TopTransactionContext = NULL;
	stub_FreeTransactionExecuteContext(e);
}

TEST(XactLocks, one_lock)
{
	VirtualTransactionId	vxid;
	TransactionExecuteContext	*e = stub_AllocateTransactionExecContext();

	TopTransactionContext = AllocSetContextCreate(TopMemoryContext,
									  "Transaction Execution Frame",
									  32 * 1024,
									  32 * 1024,
									  32 * 1024);

	LockRelationOid(30, AccessShareLock);

	vxid.backendId = MyBackendId;
	vxid.localTransactionId = 40;
	VirtualXactLockTableInsert(vxid);

	ASSERT_EQ(MyProc->fpLocalTransactionId, 40);
	PreSaveXactLocks();
	AtXact_SaveLocks(e);
	ASSERT_EQ(MyProc->fpLocalTransactionId, InvalidLocalTransactionId);

	AtXact_RestoreLocks(e);
	ASSERT_EQ(MyProc->fpLocalTransactionId, 40);
	UnlockRelationOid(30, AccessShareLock);
	VirtualXactLockTableCleanup();

	TopTransactionContext = NULL;
	stub_FreeTransactionExecuteContext(e);
}

TEST(XactLocks, sessn_more_lock)
{
	VirtualTransactionId	vxid;
	TransactionExecuteContext	*e = stub_AllocateTransactionExecContext();

	TopTransactionContext = AllocSetContextCreate(TopMemoryContext,
									  "Transaction Execution Frame",
									  32 * 1024,
									  32 * 1024,
									  32 * 1024);

	LockRelationOid(30, AccessShareLock);
	LockRelationOid(40, AccessShareLock);
	LockRelationOid(50, AccessShareLock);
	LockRelationOid(60, AccessShareLock);

	vxid.backendId = MyBackendId;
	vxid.localTransactionId = 40;
	VirtualXactLockTableInsert(vxid);

	ASSERT_EQ(MyProc->fpLocalTransactionId, 40);
	PreSaveXactLocks();
	AtXact_SaveLocks(e);
	ASSERT_EQ(MyProc->fpLocalTransactionId, InvalidLocalTransactionId);

	AtXact_RestoreLocks(e);
	ASSERT_EQ(MyProc->fpLocalTransactionId, 40);

	VirtualXactLockTableCleanup();
	UnlockRelationOid(30, AccessShareLock);
	UnlockRelationOid(40, AccessShareLock);
	UnlockRelationOid(50, AccessShareLock);
	UnlockRelationOid(60, AccessShareLock);

	TopTransactionContext = NULL;
	stub_FreeTransactionExecuteContext(e);
}

TEST(XactLocks, trans_more_lock)
{
	VirtualTransactionId	vxid;
	TransactionExecuteContext	*e = stub_AllocateTransactionExecContext();
	ResourceOwner	old;

	TopTransactionContext = AllocSetContextCreate(TopMemoryContext,
									  "Transaction Execution Frame",
									  32 * 1024,
									  32 * 1024,
									  32 * 1024);
	CurrentResourceOwner = ResourceOwnerCreate(NULL, "TopTransaction");

	LockRelationOid(30, AccessShareLock);
	LockRelationOid(40, AccessShareLock);
	LockRelationOid(50, AccessShareLock);
	LockRelationOid(60, AccessShareLock);

	vxid.backendId = MyBackendId;
	vxid.localTransactionId = 40;
	VirtualXactLockTableInsert(vxid);

	ASSERT_EQ(MyProc->fpLocalTransactionId, 40);
	PreSaveXactLocks();
	AtXact_SaveLocks(e);
	ASSERT_EQ(MyProc->fpLocalTransactionId, InvalidLocalTransactionId);

	StillHoldlock(); /* no lock, else abort. */

	AtXact_RestoreLocks(e);
	ASSERT_EQ(MyProc->fpLocalTransactionId, 40);

	VirtualXactLockTableCleanup();
	UnlockRelationOid(30, AccessShareLock);
	UnlockRelationOid(40, AccessShareLock);
	UnlockRelationOid(50, AccessShareLock);
	UnlockRelationOid(60, AccessShareLock);

	TopTransactionContext = NULL;
	old = CurrentResourceOwner;
	CurrentResourceOwner = NULL;
	ResourceOwnerDelete(old);
	stub_FreeTransactionExecuteContext(e);
}

TEST(XactLocks, trans_lots_lock)
{
	VirtualTransactionId	vxid;
	ResourceOwner	old;
	TransactionExecuteContext	*e = stub_AllocateTransactionExecContext();
	int	i = 0;

	TopTransactionContext = AllocSetContextCreate(TopMemoryContext,
									  "Transaction Execution Frame",
									  32 * 1024,
									  32 * 1024,
									  32 * 1024);
	CurrentResourceOwner = ResourceOwnerCreate(NULL, "TopTransaction");

	LockRelationOid(30, AccessShareLock);
	LockRelationOid(40, AccessShareLock);
	LockRelationOid(50, AccessShareLock);
	LockRelationOid(60, AccessShareLock);

	for (; i < 128; i++)
		LockRelationOid(i, RowShareLock);

	vxid.backendId = MyBackendId;
	vxid.localTransactionId = 40;
	VirtualXactLockTableInsert(vxid);

	ASSERT_EQ(MyProc->fpLocalTransactionId, 40);
	PreSaveXactLocks();
	AtXact_SaveLocks(e);
	ASSERT_EQ(MyProc->fpLocalTransactionId, InvalidLocalTransactionId);

	StillHoldlock(); /* no lock, else abort. */
	ASSERT_EQ(list_length(e->lock_state._locallock), 132);

	AtXact_RestoreLocks(e);
	ASSERT_EQ(MyProc->fpLocalTransactionId, 40);

	VirtualXactLockTableCleanup();
	UnlockRelationOid(30, AccessShareLock);
	UnlockRelationOid(40, AccessShareLock);
	UnlockRelationOid(50, AccessShareLock);
	UnlockRelationOid(60, AccessShareLock);

	for (i = 0; i < 128; i++)
		UnlockRelationOid(i, RowShareLock);

	TopTransactionContext = NULL;
	old = CurrentResourceOwner;
	CurrentResourceOwner = NULL;
	ResourceOwnerDelete(old);
	stub_FreeTransactionExecuteContext(e);
}
