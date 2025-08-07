#ifndef NODECONNECTBY_H
#define NODECONNECTBY_H

#include "nodes/execnodes.h"

extern ConnectByState *ExecInitConnectBy(ConnectBy *node, EState *estate, int eflags);
extern void ExecReScanConnectBy(ConnectByState *node);
extern void ExecEndConnectBy(ConnectByState *node);

#endif							/* NODECONNECTBY_H */
