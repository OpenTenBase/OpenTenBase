/*-------------------------------------------------------------------------
 *
 * nodeMultiModifyTable.h
 *
 *
 * Portions Copyright (c) 2023, Tencent.com.
 *
 * src/include/executor/nodeMultiModifyTable.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef NODEMLMODIFYTABLE_H
#define NODEMLMODIFYTABLE_H

#include "nodes/execnodes.h"

extern MultiModifyTableState *ExecInitMultiModifyTable(MultiModifyTable *node,
														EState *estate, int eflags);
extern void ExecEndMultiModifyTable(MultiModifyTableState *node);
extern void ExecReScanMultiModifyTable(MultiModifyTableState *node);
#endif
