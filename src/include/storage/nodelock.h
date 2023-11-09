/*
 * Copyright (c) 2023 THL A29 Limited, a Tencent company.
 *
 * This source code file is licensed under the BSD 3-Clause License,
 * you may obtain a copy of the License at http://opensource.org/license/bsd-3-clause/
 * 
 * Terms of the BSD 3-Clause License:
 * --------------------------------------------------------------------
 * Redistribution and use in source and binary forms, with or without modification, are permitted provided that the following conditions are met:
 * 
 * 1. Redistributions of source code must retain the above copyright notice, this list of conditions and the following disclaimer.
 * 
 * 2. Redistributions in binary form must reproduce the above copyright notice, this list of conditions and the following disclaimer in the documentation 
 * and/or other materials provided with the distribution.
 * 
 * 3. Neither the name of THL A29 Limited nor the names of its contributors may be used to endorse or promote products derived from this software without 
 * specific prior written permission.
 * 
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, 
 * THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS 
 * BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE 
 * GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT 
 * LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH 
 * DAMAGE.
 * 
 */
#ifndef  NODE_LOCK_H
#define  NODE_LOCK_H

#include "nodes/nodes.h"

typedef struct LockNodeStmt
{
    NodeTag tag;
    bool lock;
    List *params;
} LockNodeStmt;

/* 
 *the numbers of attribute  
 */

extern Size NodeLockShmemSize(void);

extern void NodeLockShmemInit(void);

extern bool NodeLock(char *lockActions, char objectType, char *param1, char *param2, int loopTimes);

extern bool NodeUnLock(char *lockActions, char objectType, char *param1, char *param2);

extern void LightLockCheck(CmdType cmd, Oid table, int shard);

extern void HeavyLockCheck(const char* cmdString, CmdType cmd, const char *query_string, void *parsetree);

extern void nodeLockRecovery(void);

extern void LockNode(LockNodeStmt *stmt);
extern void UnLockNode(LockNodeStmt *stmt);
#endif /* NODE_LOCK_H */
