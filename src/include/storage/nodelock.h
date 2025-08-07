#ifndef  NODE_LOCK_H
#define  NODE_LOCK_H



#include "nodes/nodes.h"




typedef struct LockNodeStmt
{
	NodeTag tag;
	bool lock;
	List *params;
} LockNodeStmt;



extern Size NodeLockShmemSize(void);

extern void NodeLockShmemInit(void);

extern bool NodeLock(char *lockActions, char objectType, char *param1, char *param2, int loopTimes);

extern bool NodeUnLock(char *lockActions, char objectType, char *param1, char *param2);

extern void LightLockCheck(CmdType cmd, Oid table, int shard, int8 shardcluster);

extern void HeavyLockCheck(const char* cmdString, CmdType cmd, const char *query_string, void *parsetree);

extern void nodeLockRecovery(void);

extern void LockNode(LockNodeStmt *stmt);
extern void UnLockNode(LockNodeStmt *stmt);
#endif /* NODE_LOCK_H */
