#ifndef NODEREMOTEMODIFYTABLE_H
#define NODEREMOTEMODIFYTABLE_H

#include "nodes/execnodes.h"
#include "executor/execPartition.h"

extern RemoteModifyTableState *ExecInitRemoteModifyTable(RemoteModifyTable *node, EState *estate, int eflags);
extern void ExecEndRemoteModifyTable(RemoteModifyTableState *node);
extern void ExecReScanRemoteModifyTable(RemoteModifyTableState *node);

#endif
