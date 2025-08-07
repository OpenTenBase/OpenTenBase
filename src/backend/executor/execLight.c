/*-------------------------------------------------------------------------
 *
 * execLight.c
 *
 *	  Functions to execute commands on remote Datanodes
 *
 *
 * Portions Copyright (c) 2012-2014, TransLattice, Inc.
 * Portions Copyright (c) 1996-2011, PostgreSQL Global Development Group
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 *
 *
 * IDENTIFICATION
 *	  src/backend/pgxc/pool/execLight.c
 *
 *-------------------------------------------------------------------------
 */
#include "c.h"
#include "postgres.h"
#include "access/printtup.h"
#include "commands/prepare.h"
#include "pgxc/nodemgr.h"
#include "pgxc/squeue.h"
#include "nodes/print.h"
#include "pgxc/pgxcnode.h"
#include "executor/execLight.h"
#include "nodes/nodeFuncs.h"
#include "optimizer/pgxcship.h"
#include "libpq/pqformat.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/snapmgr.h"

bool enable_light_coord = true;

int 
runBatchMsg(PGXCNodeHandle *handle, const char *query, const char *statement, 
            int num_params, Oid *param_types, StringInfo batch_message, 
            bool sendDMsg, int batch_count)
{
    int process_count = 0;
    Snapshot snapshot;
    
    snapshot = GetActiveSnapshot();

    if (snapshot && pgxc_node_send_snapshot(handle, snapshot))
        return -1;
    
    if (pgxc_node_send_batch_execute(handle, query, statement, num_params, param_types, batch_message))
        return -1;
    /*
     * We need a CommandCounterIncrement after every query, except
     * those that start or end a transaction block.
     */
    CommandCounterIncrement();
    return process_count;
}

/*
 * light_preprocess_batchmsg_set
 *	do preprocessing work before constructing batch message for each dn
 *
 * Parameters:
 *	@in psrc: plan
 *	@in params_set: params used to compute dn index
 *	@in params_set_end: params position for computing params size for each dn
 *	@in batch_count: batch count
 *
 *	@out node_idx_set: node index for each bind-execute
 *	@out batch_count_dnset: batch count for each dn
 *	@out params_size_dnset: params size for each dn
 *
 * Returns: void
 */
void 
light_preprocess_batchmsg_set(CachedPlanSource* psrc, const ParamListInfo* params_set, const int* params_set_end, 
                              int batch_count, int* node_idx_set, int* batch_count_dnset, int* params_size_dnset)
{
    int idx = -1;
    int len;
    ExecNodes* exec_nodes = psrc->single_exec_node;

    ResourceOwner currentOwner = CurrentResourceOwner;
    ResourceOwner tmpOwner = ResourceOwnerCreate(CurrentResourceOwner, "LightExecutor");
    CurrentResourceOwner = tmpOwner;

    if (exec_nodes->dis_exprs != NULL)
    {
        Datum* disValues;
        bool* disisnulls;
        Oid* distcol_type;
        int j = 0;
        RelationLocInfo* rel_loc_info;
        
        /* use my own context to store tmp info */
        MemoryContext old_context = CurrentMemoryContext;
        MemoryContext my_context = AllocSetContextCreate(TopMemoryContext,
                                                         "LightExecutorMemory",
                                                         ALLOCSET_DEFAULT_SIZES);
        MemoryContextSwitchTo(my_context);

        rel_loc_info = GetRelationLocInfo(exec_nodes->en_relid);
        if (rel_loc_info == NULL)
        {
            ereport(ERROR, (errcode(ERRCODE_SYSTEM_ERROR), errmsg("rel_loc_info is NULL.")));
        }

        len = rel_loc_info->nDisAttrs;
        
        disValues = (Datum*)palloc0(len * sizeof(Datum));
        disisnulls = (bool*)palloc0(len * sizeof(bool));
        distcol_type = (Oid*)palloc0(len * sizeof(Oid));

        for (j = 0; j < batch_count; j++)
        {
            List* idx_dist = NULL;
            int i = 0;
            ExecNodes* single_node = NULL;
            
            for (i = 0; i < rel_loc_info->nDisAttrs; i++) 
            {
                Expr* distcol_expr = NULL;
                
                if (exec_nodes->dis_exprs && exec_nodes->dis_exprs[i])
                {
                    distcol_expr = exec_nodes->dis_exprs[i];
                    distcol_expr = (Expr*)eval_const_expressions_with_params(params_set[j], (Node*)distcol_expr);    
                }
                
                if (distcol_expr && IsA(distcol_expr, Const))
                {
                    Const* const_expr = (Const*)distcol_expr;
                    disValues[i] = const_expr->constvalue;
                    disisnulls[i] = const_expr->constisnull;
                    distcol_type[i] = const_expr->consttype;
                    idx_dist = lappend_int(idx_dist, i);
                    i++;
                } else
                    ereport(ERROR,
                            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                                    errmsg("Batch param of distribute key only support const")));
            }

            single_node = GetRelationNodes(rel_loc_info, 
                                           disValues, disisnulls, rel_loc_info->nDisAttrs, 
                                           exec_nodes->accesstype);
            /* make sure it is one dn */
            if (single_node == NULL || list_length(single_node->nodeList) != 1)
                ereport(ERROR,
                        (errcode(ERRCODE_SYSTEM_ERROR),
                                errmsg("Failed to get DataNode id for Batch bind-execute: name %s, query %s",
                                       psrc->stmt_name,
                                       psrc->query_string)));
            
            
            
            idx = linitial_int(single_node->nodeList);
            node_idx_set[j] = idx;
            batch_count_dnset[idx]++;
            params_size_dnset[idx] += (params_set_end[j + 1] >= params_set_end[j] ?
                                       params_set_end[j + 1] - params_set_end[j] : 0);

            /* reset */
            memset(disValues, 0, len * sizeof(Datum));
            memset(disisnulls, 0, len * sizeof(bool));
            memset(distcol_type,  0, len * sizeof(Oid));
        }

        MemoryContextSwitchTo(old_context);
        MemoryContextDelete(my_context);
    }
    else
    {
        int j = 0;
        /* make sure it is one dn */
        if (list_length(exec_nodes->nodeList) != 1)
            ereport(ERROR,
                    (errcode(ERRCODE_SYSTEM_ERROR),
                            errmsg("Failed to get DataNode id for Batch bind-execute: name %s, query %s",
                                   psrc->stmt_name,
                                   psrc->query_string)));

        /* all same node index in this case */
        idx = linitial_int(exec_nodes->nodeList);
        batch_count_dnset[idx] = batch_count;
        for (j = 0; j < batch_count; j++)
        {
            node_idx_set[j] = idx;
            params_size_dnset[idx] += (params_set_end[j + 1] >= params_set_end[j] ?
                                       params_set_end[j + 1] - params_set_end[j] : 0);
        }
    }

    ResourceOwnerRelease(tmpOwner, RESOURCE_RELEASE_BEFORE_LOCKS, true, true);
    ResourceOwnerRelease(tmpOwner, RESOURCE_RELEASE_LOCKS, true, true);
    ResourceOwnerRelease(tmpOwner, RESOURCE_RELEASE_AFTER_LOCKS, true, true);
    CurrentResourceOwner = currentOwner;
    ResourceOwnerDelete(tmpOwner);
}

/*
 * light_construct_batchmsg_set
 *	construct batch messages for light cn for each dn
 */
StringInfo 
light_construct_batchmsg_set(StringInfo input, const int* params_set_end, const int* node_idx_set,
                             const int* batch_count_dnset, const int* params_size_dnset, 
                             StringInfo desc_msg, StringInfo exec_msg)
{
    int batch_count;
    int fix_part_size;
    int before_size;
    bool send_DP_msg = (desc_msg != NULL);

    int idx;
    int batch_count_dn;
    int tmp_batch_count_dn;
    int tmp_params_size;
    StringInfo batch_msg;
    StringInfo batch_msg_dnset;
    int i;
    /* parse it again */
    input->cursor = 0;
    batch_count = pq_getmsgint(input, 4);
    if (unlikely(batch_count <= 0))
    {
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("unexpected batch_count %d get from inputmessage", batch_count)));
    }
    /* size of common part before params */
    before_size = params_set_end[0] - input->cursor;
    Assert(before_size > 0);

    /* 1.construct common part before params */
    fix_part_size = 4 + before_size + exec_msg->len;
    batch_msg_dnset = (StringInfo)palloc0(NumDataNodes * sizeof(StringInfoData));
    for (i = 0; i < NumDataNodes; i++)
    {
        batch_count_dn = batch_count_dnset[i];
        /* not related to this dn */
        if (batch_count_dn == 0)
            continue;

        batch_msg = &batch_msg_dnset[i];
        /* only send DP message for the first dn */
        if (send_DP_msg)
        {
            batch_msg->len = fix_part_size + desc_msg->len + params_size_dnset[i];
            send_DP_msg = false;
        } else
            batch_msg->len = fix_part_size + params_size_dnset[i];

        batch_msg->maxlen = batch_msg->len + 1;
        batch_msg->data = (char*)palloc0(batch_msg->maxlen);

        /* batch_count */
        tmp_batch_count_dn = htonl(batch_count_dn);
        memcpy(batch_msg->data, &tmp_batch_count_dn, sizeof(int));
        batch_msg->cursor = 4;

        /* before params */
        memcpy(batch_msg->data + batch_msg->cursor, input->data + input->cursor, before_size);
        batch_msg->cursor += before_size;
    }

    /* 2.construct the params part */
    input->cursor += before_size;
    /* has params */
    if (params_set_end[1] > 0)
    {
        for (i = 0; i < batch_count; i++)
        {
            idx = node_idx_set[i];
            batch_msg = &batch_msg_dnset[idx];
            if (unlikely(batch_msg->maxlen <= 0))
            {
                ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("unexpected maxlen %d ", batch_msg->maxlen)));
            }

            /* append params */
            tmp_params_size = params_set_end[i + 1] - params_set_end[i];
            memcpy(
                    batch_msg->data + batch_msg->cursor, input->data + input->cursor, tmp_params_size);
            batch_msg->cursor += tmp_params_size;
            input->cursor += tmp_params_size;
        }
    }

    /* 3.construct common part after params */
    send_DP_msg = (desc_msg != NULL);
    for (i = 0; i < NumDataNodes; i++)
    {
        batch_count_dn = batch_count_dnset[i];
        /* not related to this dn */
        if (batch_count_dn == 0)
            continue;

        batch_msg = &batch_msg_dnset[i];
        if (send_DP_msg)
        {
            memcpy(batch_msg->data + batch_msg->cursor, desc_msg->data, desc_msg->len);
            batch_msg->cursor += desc_msg->len;
            send_DP_msg = false;
        }

        memcpy(batch_msg->data + batch_msg->cursor, exec_msg->data, exec_msg->len);
        batch_msg->cursor += exec_msg->len;

        /* check finished and reset cursor */
        Assert(batch_msg->cursor == batch_msg->len);
        batch_msg->cursor = 0;
    }

    return batch_msg_dnset;
}

/*
 * light_execute_batchmsg_set
 *	execute batch for light cn for each dn
 */
int 
light_execute_batchmsg_set(CachedPlanSource* psrc, const int* node_idx_set, const StringInfo batch_msg_dnset, 
                           const int* batch_count_dnset, bool send_DP_msg)
{
    int process_count = 0;
    int tmp_count = 0;
    bool sendDMsg = send_DP_msg;
    int batch_count_dn = 0;
    int i = 0;
    List		*nodeList = NULL;
    DatanodeStatement *entry = NULL;
    PGXCNodeAllHandles *pgxc_connections = NULL;
    bool		   *prepared;
    int				dn_conn_count;
    ResponseCombiner combiner;
    int idx = 0;
    bool need_tran_block = false;
    GlobalTransactionId gxid;
    
    /* Must set snapshot before starting executor. */
    PushActiveSnapshot(GetTransactionSnapshot());
    
    prepared = palloc0(sizeof(bool) * NumDataNodes);

    for (i = 0; i < NumDataNodes; i++) 
    {
        batch_count_dn = batch_count_dnset[i];
        /* not related to this dn */
        if (batch_count_dn == 0)
            continue;
        
        nodeList = lappend_int(nodeList, i);
    }

    entry = FetchDatanodeStatement(psrc->stmt_name, false);
    if (entry)
    {
        pgxc_connections = DatanodeStatementGetHandle(entry, &nodeList);
        if (pgxc_connections != NULL)
        {
            memset(prepared, 1, sizeof(bool) * pgxc_connections->dn_conn_count);
            register_transaction_handles_all(pgxc_connections, false);
        }
    }
    
    if (pgxc_connections == NULL || list_length(nodeList) > 0)
    {
        PGXCNodeAllHandles *diff;

        diff = get_handles(nodeList, NULL, false, true, FIRST_LEVEL, InvalidFid, true, false);
        if (diff->dn_conn_count > 0)
        {
            if (entry == NULL)
                entry = InitDatanodeStatementEntry(psrc->stmt_name);

            /* a new statement needs prepare, save related connections */
            DatanodeStatementStoreHandle(entry, diff);
        }

        if (pgxc_connections == NULL)
        {
            pgxc_connections = diff;
        }
        else
        {
            pgxc_connections->datanode_handles =
                    repalloc(pgxc_connections->datanode_handles,
                             sizeof(PGXCNodeHandle *) *
                             (pgxc_connections->dn_conn_count + diff->dn_conn_count));

            for (i = 0; i < diff->dn_conn_count; i++)
            {
                pgxc_connections->datanode_handles[pgxc_connections->dn_conn_count++] =
                        diff->datanode_handles[i];
            }
        }
    }

    need_tran_block = (pgxc_connections->dn_conn_count > 1) || (TransactionBlockStatusCode() == 'T');
    gxid = GetCurrentTransactionId();
            
    idx = 0;
    for (i = 0; i < NumDataNodes; i++) 
    {
        PGXCNodeHandle	  *handle;
        const char *query = NULL;
        
        batch_count_dn = batch_count_dnset[i];
        /* not related to this dn */
        if (batch_count_dn == 0)
            continue;

        /* Check for cancel signal before we start execution */
        CHECK_FOR_INTERRUPTS();

        handle = pgxc_connections->datanode_handles[idx];

        if(!prepared[idx])
            query = psrc->query_string;
        else
            query = NULL;

        idx++;

        if (pgxc_node_begin(1, &handle, gxid, need_tran_block, true))
            ereport(ERROR,
                    (errcode(ERRCODE_CONNECTION_EXCEPTION),
                            errmsg("Could not begin transaction on data node:%s.",
                                   handle->nodename)));
        
        tmp_count = runBatchMsg(handle, query, psrc->stmt_name, psrc->num_params, psrc->param_types, 
                                &batch_msg_dnset[i], sendDMsg, batch_count_dn);
        process_count += tmp_count;
        if (sendDMsg)
            sendDMsg = false;
    }

    i = 0;
    dn_conn_count = pgxc_connections->dn_conn_count;

    PopActiveSnapshot();

    InitResponseCombiner(&combiner, dn_conn_count, COMBINE_TYPE_NONE);
    combiner.waitforC = true;
    combiner.waitforE = true;
    combiner.extended_query = true;
    
    if (pgxc_node_receive_responses(dn_conn_count, pgxc_connections->datanode_handles, NULL, &combiner))
    {
        elog(ERROR, "pgxc_node_begin receive response fails.");
        return EOF;
    }
    
    if(!validate_combiner(&combiner))
    {
        if (combiner.errorMessage)
            pgxc_node_report_error(&combiner);
        else
            ereport(ERROR,
                    (errcode(ERRCODE_CONNECTION_EXCEPTION),
                            errmsg("Error while execute batch msg")));
    }
    else
        CloseCombiner(&combiner);
    
    return process_count;
}

/*
 * exec_one_in_batch
 *	main entry of execute one in bind-execute message for not light cn
 *
 * Parameters:
 *	@in psrc: CachedPlanSource
 *	@in params: input params
 *	@in numRFormats: num of result format codes
 *	@in rformats: result format codes
 *	@in send_DP_msg: if send the DP msg
 *	@in dest: command dest
 *	@in completionTag: used in PortalRun and to compute num of processed tuples later.
 *   NULL value means the query is not SELECT/INSERT/UPDATE/DELETE,
 *   and will not count the num of processed tuples.
 *
 * Returns: const char *
 *   If query is not SELECT/INSERT/UPDATE/DELETE, return completionTag, else NULL.
 */
void 
exec_one_in_batch(CachedPlanSource* psrc, ParamListInfo params, int numRFormats, int16* rformats,
                  bool send_DP_msg, CommandDest dest, char* completionTag,
                  const char* stmt_name, List** gpcCopyStmts, MemoryContext* tmpCxt, 
                  PreparedStatement* pstmt)
{
    CachedPlan* cplan = NULL;
    Portal portal;

    DestReceiver* receiver = NULL;
    bool completed = false;

    const char* cur_stmt_name = NULL;
    
    portal = CreatePortal("", true, true, false, NULL);

    /*
     * Obtain a plan from the CachedPlanSource.  Any cruft from (re)planning
     * will be generated in t_thrd.mem_cxt.msg_mem_cxt.  The plan refcount will be
     * assigned to the Portal, so it will be released at portal destruction.
     */
    cplan = GetCachedPlan(psrc, params, NULL, NULL);
    
    /*
     * Now we can define the portal.
     *
     * DO NOT put any code that could possibly throw an error between the
     * above GetCachedPlan call and here.
     */
    PortalDefineQuery(portal,
                      cur_stmt_name,
                      psrc->query_string,
                      psrc->commandTag,
                      cplan->stmt_list,
                      cplan);
    
    /*
     * And we're ready to start portal execution.
     */
    PortalStart(portal, params, 0, InvalidSnapshot);

    /*
     * Apply the result format requests to the portal.
     */
    PortalSetResultFormat(portal, numRFormats, rformats);

    /*
     * Create dest receiver in t_thrd.mem_cxt.msg_mem_cxt (we don't want it in transaction
     * context, because that may get deleted if portal contains VACUUM).
     */
    receiver = CreateDestReceiver(dest);
    if (dest == DestRemoteExecute)
        SetRemoteDestReceiverParams(receiver, portal);

    /* Check for cancel signal before we start execution */
    CHECK_FOR_INTERRUPTS();

    completed = PortalRun(portal,
                          FETCH_ALL,
                          true, /* always top level */
                          true,
                          receiver,
                          receiver,
                          completionTag);

    (*receiver->rDestroy)(receiver);

    if (completed)
    {
        /*
         * We need a CommandCounterIncrement after every query, except
         * those that start or end a transaction block.
         */
        CommandCounterIncrement();
    }
    else
    {
        /* Portal run not complete, maybe something wrong */
        ereport(ERROR,
                (errcode(ERRCODE_SYSTEM_ERROR),
                        errmsg("Portal run not complete for one in Batch bind-execute: name %s, query %s",
                               psrc->stmt_name,
                               psrc->query_string)));
    }
}

/*
 * Constraints specially defined for Light CN are checked here
 */
ExecNodes* 
checkLightQuery(Query* query)
{
    ExecNodes* exec_nodes = NULL;
    
    /* Do permissions checks */
    Assert(IS_PGXC_COORDINATOR && !IsConnFromCoord());
    (void)ExecCheckRTPerms(query->rtable, true);

    exec_nodes = pgxc_is_query_shippable(query, 0, NULL, 0);

    return exec_nodes;
}

/*
 * get_param_str
 *
 * Forming param string info from ParamListInfo.
 */
static void 
get_param_str(StringInfo param_str, ParamListInfo params)
{
    int paramno;
    
    Assert(params != NULL);
    
    for (paramno = 0; paramno < params->numParams; paramno++) 
    {
        ParamExternData* prm = &params->params[paramno];
        Oid typoutput;
        bool typisvarlena = false;
        char* pstring = NULL;
        char* chunk_search_start;
        char* chunk_copy_start;
        char* chunk_end = NULL;
        
        appendStringInfo(param_str, "%s$%d = ", (paramno > 0) ? ", " : "", paramno + 1);

        if (prm->isnull || !OidIsValid(prm->ptype)) 
        {
            appendStringInfoString(param_str, "NULL");
            continue;
        }

        getTypeOutputInfo(prm->ptype, &typoutput, &typisvarlena);
        pstring = OidOutputFunctionCall(typoutput, prm->value);

        appendStringInfoCharMacro(param_str, '\'');
        chunk_search_start = pstring;
        chunk_copy_start = pstring;
        chunk_end = NULL;
        while ((chunk_end = strchr(chunk_search_start, '\'')) != NULL) 
        {
            /* copy including the found delimiting ' */
            appendBinaryStringInfoNT(param_str,
                                     chunk_copy_start,
                                     chunk_end - chunk_copy_start + 1);
            /* in order to double it, include this ' into the next chunk as well */
            chunk_copy_start = chunk_end;
            chunk_search_start = chunk_end + 1;
        }
        appendStringInfo(param_str, "%s'", chunk_copy_start);
        pfree(pstring);
    }
}


int 
errdetail_batch_params(int batchCount, int numParams, ParamListInfo* params_set)
{
    int i;
    MemoryContext oldContext;
    StringInfoData param_str;
    
    /* We mustn't call user-defined I/O functions when in an aborted xact */
    if (batchCount <= 0 || params_set == NULL || numParams <= 0 || IsAbortedTransactionBlockState()) 
    {
        return 0;
    }
    /* Make sure any trash is generated in t_thrd.mem_cxt.msg_mem_cxt */
    oldContext = MemoryContextSwitchTo(MessageContext);

    initStringInfo(&param_str);

    for (i = 0; i < batchCount; i++) 
    {
        ParamListInfo params = params_set[i];
        
        appendStringInfo(&param_str, "(");
        get_param_str(&param_str, params);
        appendStringInfo(&param_str, ")");
        if (i < batchCount - 1) 
        {
            appendStringInfo(&param_str, ",");
        }
    }

    errdetail("batch parameters: %s", param_str.data);
    pfree(param_str.data);
    
    (void)MemoryContextSwitchTo(oldContext);

    return 0;
}
