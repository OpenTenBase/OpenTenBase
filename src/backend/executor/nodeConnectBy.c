/*-------------------------------------------------------------------------
 *
 * nodeConnectBy.c
 *	  routines to handle ConnectBy nodes.
 *
 * IDENTIFICATION
 *	  src/backend/executor/nodeConnectBy.c
 *
 *-------------------------------------------------------------------------
 */
/* INTERFACE ROUTINES
 *		ExecInitConnectBy   - initialize the ConnectBy node
 *		ExecConnectBy		- retrieve the next tuple from the node
 *		ExecEndConnectBy	- shut down the ConnectBy node
 *		ExecReScanConnectBy - rescan the ConnectBy node
 */

#include "postgres.h"

#include "executor/executor.h"
#include "executor/nodeConnectBy.h"
#include "miscadmin.h"
#include "nodes/nodeFuncs.h"
#include "optimizer/tlist.h"
#include "utils/hsearch.h"
#include "utils/memutils.h"

