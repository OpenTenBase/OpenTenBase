/*-------------------------------------------------------------------------
 *
 * stub_atxact.c
 *	  Unit test for atxact.c and export static function.
 *
 * Portions Copyright (c) 2021 Tencent Development Group
 *
 *
 * IDENTIFICATION
 *	  src/backend/unittest/backend/access/transam/stub/stub_atxact.c
 *
 *-------------------------------------------------------------------------
 */
#include "../../../../../access/transam/atxact.c"
#include "stub/access/stub_atxact.h"

TransactionExecuteContext *
stub_AllocateTransactionExecContext(void)
{
	return AllocateTransactionExecContext();
}

void
stub_FreeTransactionExecuteContext(TransactionExecuteContext *e)
{
	FreeTransactionExecuteContext(e);
}
