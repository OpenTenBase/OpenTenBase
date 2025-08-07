/*-------------------------------------------------------------------------
 *
 * stub_atxact.h
 *	  Unit test for autonomous transaction.
 *
 *
 * Portions Copyright (c) 2021 Tencent Development Group
 *
 * src/include/stub/access/stub_atxact.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef _STUB_ATXACT_H
#define _STUB_ATXACT_H

#include "postgres.h"

#include "access/atxact.h"
#include "miscadmin.h"
#include "storage/backendid.h"
#include "storage/proc.h"
#include "utils/memutils.h"


extern TransactionExecuteContext *stub_AllocateTransactionExecContext(void);
extern void stub_FreeTransactionExecuteContext(TransactionExecuteContext *e);

#endif
