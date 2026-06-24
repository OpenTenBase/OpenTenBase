/*-------------------------------------------------------------------------
 *
 * execFragment.h
 *
 * Portions Copyright (c) 2018, Tencent OpenTenBase-C Group.
 * Portions Copyright (c) 2012-2014, TransLattice, Inc.
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/executor/execFragment.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef EXECFAST_H
#define EXECFAST_H

#include "access/parallel.h"
#include "executor/tqueueThread.h"
#include "forward/fnbuf.h"
#include "forward/fnbuf_internals.h"
#include "pgxc/execRemote.h"
#include "pgxc/locator.h"
#include "pgxc/pgxcnode.h"
#include "nodes/execnodes.h"
#include "nodes/plannodes.h"
#include "optimizer/clauses.h"

extern bool enable_light_coord;

StringInfo light_construct_batchmsg_set(StringInfo input, const int* params_set_end, const int* node_idx_set,
                                        const int* batch_count_dnset, const int* params_size_dnset, 
                                        StringInfo desc_msg, StringInfo exec_msg);
int runBatchMsg(PGXCNodeHandle *handle, const char *query, const char *statement,
                int num_params, Oid *param_types, StringInfo batch_message,
                bool sendDMsg, int batch_count);
int light_execute_batchmsg_set(CachedPlanSource* psrc, const int* node_idx_set, const StringInfo batch_msg_dnset, 
                               const int* batch_count_dnset, bool send_DP_msg);
void light_preprocess_batchmsg_set(CachedPlanSource* psrc, const ParamListInfo* params_set, 
                                   const int* params_set_end, int batch_count, int* node_idx_set, 
                                   int* batch_count_dnset, int* params_size_dnset);
void exec_one_in_batch(CachedPlanSource* psrc, ParamListInfo params, int numRFormats, int16* rformats,
                       bool send_DP_msg, CommandDest dest, char* completionTag, const char* stmt_name, 
                       List** gpcCopyStmts, MemoryContext* tmpCxt, PreparedStatement* pstmt);
int errdetail_batch_params(int batchCount, int numParams, ParamListInfo* params_set);
ExecNodes* checkLightQuery(Query* query);

#endif							/* EXECFAST_H  */
