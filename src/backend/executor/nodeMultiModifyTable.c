/*-------------------------------------------------------------------------
 *
 * nodeMultiModifyTable.c
 *	  routines to handle MultiModifyTable nodes.
 *
 * Portions Copyright (c) 2023, Tencent.com.
 *
 * IDENTIFICATION
 *	  src/backend/executor/nodeMultiModifyTable.c
 *
 *-------------------------------------------------------------------------
 */

/* INTERFACE ROUTINES
 *		ExecInitMultiModifyTable - initialize the MultiModifyTable node
 *		ExecMultiModifyTable		- retrieve the next tuple from the node
 *		ExecEndMultiModifyTable	- shut down the MultiModifyTable node
 *		ExecReScanMultiModifyTable - rescan the MultiModifyTable node
 *
 *	 NOTES
 *		Currently, MultiModifyTable only supports INSERT operation. Each of
 *		MultiModifyTable has a list of ModifyTable node. For each of input
 *		tuple from plan->lefttree, the target tuple contains the ModifyTable ID
 *		to be inserted in.
 *
 *		A rescan operation for ModifyTable should be support for each of input
 *		tuple for insertion, then we allow rescan for ModifyTable.
 */

#include "postgres.h"

#include "access/htup_details.h"
#include "catalog/pg_type.h"
#include "executor/executor.h"
#include "executor/nodeMultiModifyTable.h"
#include "executor/nodeSubplan.h"
#include "pgxc/planner.h"
#include "utils/array.h"
#include "utils/memutils.h"

