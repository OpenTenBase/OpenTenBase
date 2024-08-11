/*-------------------------------------------------------------------------
 *
 *     dds.c
 *
 *      Function to manage DDS messages.
 *
 *
 * Copyright (c) 2019-2024 TeleDB Development Group
 *
 * src/backend/pgxc/dds/dds.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "access/transam.h"
#include "access/gtm.h"
#include "storage/procarray.h"
#include "utils/fmgrprotos.h"
#include "utils/timestamp.h"
#include "utils/snapmgr.h"
#include "pgxc/dds.h"
#include "pgxc/map.h"
#include "pgxc/nodemgr.h"
#include "pgxc/execRemote.h"
#include "pgxc/poolmgr.h"
#include "executor/tuptable.h"
#include "executor/executor.h"


static void process_dds_msg_proliferation(DDSMessage *msg, DDSNode *node);
static bool process_dds_msg_spread(DDSMessage *msg, DDSNode *node);
static int compare_dds_label(DDSLabel *label1, DDSLabel *label2);
static bool compare_dds_dependency(DDSDependency *dep1, DDSDependency *dep2);
static void node_send_dds_msg_to_downstream(DDSNode *node);

/*
    delete everything of a gxidlist.
*/
GxidList*
find_and_delete_dependent(GxidList *list, Dependent_Trans *dep_trans)
{
    GxidList   *tmp = list;
    GxidList   *pre = NULL;

    while (tmp)
    {
        if (0 == strcmp(tmp->gxid, dep_trans->dep_gxid.holder_gxid))
        {
            if (tmp == list)
            {
                list = list->next;
                free(tmp);
                tmp = NULL;
            }
            else
            {
                pre->next = tmp->next;
                free(tmp);
                tmp = pre->next;
            }
            return list;
        }
        else
        {
            pre = tmp;
            tmp = tmp->next;
        }
    }
    return list;
}

/*
    add a dependency into gxidlist.
*/
int
find_and_add_dependent(DependencyHashtableEntry *dep_entry, Dependent_Trans *dep_trans)
{
    GxidList   *tmp = dep_entry->gxid_entry;

    while (tmp)
    {
        if (0 == strcmp(tmp->gxid, dep_trans->dep_gxid.holder_gxid))
        {
            return 0;
        }
        else
        {
            tmp = tmp->next;
        }
    }
    if (tmp == NULL)
    {
        tmp = (GxidList *)malloc(sizeof(GxidList));
        if (NULL == tmp)
        {
            return -1;
        }
        memcpy(tmp->gxid, dep_trans->dep_gxid.holder_gxid, sizeof(tmp->gxid));
        tmp->next = dep_entry->gxid_entry;
        dep_entry->gxid_entry = tmp;
    }
    return 0;
}

/*
    set a dds message.
*/
void
set_dds_msg(DDSMessage *msg, DDSMsgType msgtype, char *src_gxid, char *dest_gxid, int src_txn_pid, TimestampTz ts)
{
    memcpy(msg->dependency.src_gxid, src_gxid, sizeof(msg->dependency.src_gxid));
    memcpy(msg->dependency.dest_gxid, dest_gxid, sizeof(msg->dependency.dest_gxid));
    msg->msgtype = msgtype;
    msg->send_ts = GetCurrentTimestamp();
    msg->label.txn_create_ts = ts;
    msg->dependency_pid = src_txn_pid;
}

/*
    dds algorithm deduction, process a DDS message.
*/
bool
process_dds_msg(DDSMessage *msg, DDSNode *node)
{
    bool    detected_flag = false;
    int64        current_ts;
    int64        new_period;
    int64        current_phase;

    /* if we get in a mew period, reset  */
    current_ts = GetCurrentTimestamp();
    //GetGlobalTimestampGTM();
    new_period = current_ts / DDS_PERIOD;
    if (new_period > node->last_dds_period)
    {
        node->last_dds_period = new_period;
        node->public_label = node->private_label;
    }

    current_phase = current_ts / DDS_PHASE;
    if (current_phase % 2 == 0)
    {
        process_dds_msg_proliferation(msg, node);
    }
    else
    {
        detected_flag = process_dds_msg_spread(msg, node);
    }
    return detected_flag;
}

/*
    ddsp deduction.
*/
void
process_dds_msg_proliferation(DDSMessage *msg, DDSNode *node)
{
    int tmp = msg->ddsv + 1;
    if (tmp > node->ddsv)
        node->ddsv = tmp;
}

/*
    ddss deduction.
*/
bool
process_dds_msg_spread(DDSMessage *msg, DDSNode *node)
{
    if (msg->ddsv >= node->ddsv)
    {
        node->ddsv = msg->ddsv;
        if (msg->ddsv == node->ddsv)
        {
            DDSLabel   *msg_label = &msg->label;
            DDSLabel   *node_public_label = &node->public_label;
            DDSLabel   *node_private_label = &node->private_label;

            if (compare_dds_label(msg_label, node_public_label) > 0)
            {
                node->public_label = msg->label;
            }

            if (compare_dds_label(msg_label, node_public_label) == 0
                && compare_dds_label(node_public_label, node_private_label) == 0)
            {
                return true;
            }
        }
    }
    return false;
}

/*
    compare two DDS labels, return 1 if label1 is bigger,
    return -1 if label2 is bigger, return 0 if equivalent.
*/
int
compare_dds_label(DDSLabel *label1, DDSLabel *label2)
{
    if (label1->txn_create_ts > label2->txn_create_ts)
        return 1;
    if (label1->txn_create_ts < label2->txn_create_ts)
        return -1;
    return 0;
}

/*
    compare two DDS dependencies, return true if equivlant.
*/
bool
compare_dds_dependency(DDSDependency *dep1, DDSDependency *dep2)
{
    return strcmp(dep1->dest_gxid, dep2->dest_gxid) == 0 && strcmp(dep1->src_gxid, dep2->src_gxid) == 0;
}

/*
    initialize one DDSNode by setting up parameters according to DDS message.
*/
void
init_ddsnode(DDSNode *node, DDSMessage *msg)
{
    node->ddsv = 0;
    node->last_dds_period = 0;
    node->create_ts = GetCurrentTimestamp();
    node->downstream_nodes = NULL;
    memcpy(node->gxid, msg->dependency.src_gxid, sizeof(node->gxid));
    node->private_label.txn_create_ts = msg->label.txn_create_ts;
    node->public_label.txn_create_ts = msg->label.txn_create_ts;
}

/*
    delete one DDSNode.
*/
void
delete_ddsnode(DDSNode * node)
{
    if (NULL == node)
    {
        return;
    }
    delete_ddsnode_downstream_list(node);
    free(node);
    node = NULL;
}

/*
    delete one DDSNode's downstream node list.
*/
void
delete_ddsnode_downstream_list(DDSNode * node)
{
    DDSDependencyList *curr;
    DDSDependencyList *next;

    if (NULL == node)
    {
        return;
    }

    curr = node->downstream_nodes;
    while (curr != NULL)
    {
        next = curr->next;
        free(curr);
        curr = next;
    }
    node->downstream_nodes = NULL;
}

/*
    delete one dependency hashtable entry.
*/
void
delete_gxidlist(DependencyHashtableEntry *entry)
{
    GxidList   *curr;
    GxidList   *next;

    if (NULL == entry)
    {
        return;
    }

    curr = entry->gxid_entry;
    while (curr != NULL)
    {
        next = curr->next;
        free(curr);
        curr = next;
    }
}

/*
    return nodeid giving gxid.
*/
int
get_nodeid_from_gxid(char *gxid)
{
    int            index;
    char       *colon;

    colon = strchr(gxid, ':');
    *colon = '\0';
    index = atoi(gxid);
    *colon = ':';
    return index;
}

/*
    check if each DDS node is still in life time, delete if not. every node alive sends DDS message.
*/
void
check_hashtable_nodes_alive_and_send_dds_msg(void *node_map)
{
    DDSNodeHashtableEntry    *entry;
    char waiter_gxid[MAX_GXID_LEN];
    char *gxid = waiter_gxid;
    DDSNode *node;
    TimestampTz current_time = 0;

    if (NULL == node_map)
    {
        return;
    }

    dds_map_begin_iter(node_map);

    while (0 == dds_map_iter_key(node_map, waiter_gxid))
    {
        entry = (DDSNodeHashtableEntry *)(dds_map_iter_value(node_map));
        if(entry == NULL)
        {
            dds_map_erase(node, gxid);
            continue;
        }

        node = &entry->node_entry;
        current_time = GetCurrentTimestamp();
        if (node->create_ts + DDSNODE_LIFE < current_time)
        {
            delete_ddsnode_downstream_list(node);
            entry = (DDSNodeHashtableEntry *) dds_map_erase(node_map, gxid);
            free(entry);
        }
        else
        {
            node_send_dds_msg_to_downstream(node);
        }
        dds_map_iter_next(node_map);
    }
}

/*
    node sends DDS message to its downstream nodes.
*/
void
node_send_dds_msg_to_downstream(DDSNode *node)
{
    DDSMessage *msg;
    DDSDependencyList *list;
    int            cn_nodeid;

    if (NULL == node)
    {
        return;
    }

    list = node->downstream_nodes;

    while (list != NULL)
    {
        msg = (DDSMessage *) malloc(sizeof(DDSMessage));
        if (NULL == msg)
        {
            continue;
        }
        msg->msgtype = DDS;
        msg->send_ts = GetCurrentTimestamp();
        msg->ddsv = node->ddsv;
        msg->dependency_pid = 0;
        memcpy(&msg->label, &node->public_label, sizeof(msg->label));
        memcpy(&msg->dependency, &list->dependency, sizeof(msg->dependency));
        cn_nodeid = get_nodeid_from_gxid(list->dependency.dest_gxid);
        send_dds_msg(msg, cn_nodeid, true);
        list = list->next;
    }
}

/*
    delete one downstream dependency from downstream nodes list.
*/
DDSDependencyList *delete_dependency_from_list(DDSDependencyList *list, char *gxid)
{
    DDSDependencyList *cur;
    DDSDependencyList *pre = NULL;

    if (NULL == list)
    {
        return NULL;
    }
    
    cur = list;
    if (strcmp(cur->dependency.dest_gxid, gxid) == 0)
    {
        pre = list;
        list = list->next;
        free(pre);
        return list;
    }

    while (cur != NULL && strcmp(cur->dependency.dest_gxid, gxid) != 0)
    {
        pre = cur;
        cur = cur->next;
    }

    if (cur != NULL)
    {
        pre->next = cur->next;
        free(cur);
    }

    return list;
}

/*
    insert one downstream dependency into downstream nodes list.
*/
DDSDependencyList *
insert_dependency_into_list(DDSDependencyList *list, DDSDependency *dep)
{
    DDSDependencyList *l = list;
    DDSDependencyList *temp;

    while (l != NULL)
    {
        if (compare_dds_dependency(&l->dependency, dep))
            return list;

        l = l->next;
    }
    temp = (DDSDependencyList *) malloc(sizeof(DDSDependencyList));
    if (NULL != temp)
    {
        memcpy(&temp->dependency.dest_gxid, &dep->dest_gxid, sizeof(temp->dependency.dest_gxid));
        memcpy(&temp->dependency.src_gxid, &dep->src_gxid, sizeof(temp->dependency.src_gxid));
        temp->next = list;
        list = temp;
    }

    return list;
}

/*
    check if a transancion still exists.
*/
bool
check_txn_exist(char *gxid)
{
    return GxidGetPid(gxid) != InvalidPid;
}

/*
    kill a process.
*/
int
kill_proccess(int pid)
{
#ifdef HAVE_SETSID
    return kill(-pid, SIGINT);
#else
    return kill(pid, SIGINT);
#endif
}

/*
    terminate a transaction on CN.
*/
int
terminate_transaction(char *gxid)
{
    int pid;

    pid = GxidGetPid(gxid);
    if (pid != InvalidPid)
    {
        return kill_proccess(pid);
    }
    return -1;
}

/*
    traverse hashtable and make pushState msg.
*/
void
traverse_and_make_pushState(void *dependency_map)
{
    DependencyHashtableEntry *entry;
    GxidList *list;
    char waiter_gxid[MAX_GXID_LEN];
    char *gxid = waiter_gxid;
    int cn_nodeid;
    int src_txn_pid;

    if (NULL == dependency_map)
    {
        return;
    }
    
    dds_map_begin_iter(dependency_map);

    while (0 == dds_map_iter_key(dependency_map, waiter_gxid))
    {
        entry = (DependencyHashtableEntry *)(dds_map_iter_value(dependency_map));
        if(entry == NULL)
        {
            dds_map_erase(dependency_map, gxid);
            dds_map_iter_next(dependency_map);
            continue;
        }

        list = entry->gxid_entry;
        src_txn_pid = GxidGetPid(gxid);
        if (src_txn_pid == InvalidPid)
        {
            delete_gxidlist(entry);
            entry = (DependencyHashtableEntry *) dds_map_erase(dependency_map, gxid);
            free(entry);
            dds_map_iter_next(dependency_map);
            continue;
        }

        while (list != NULL)
        {
            DDSMessage *pushstate_msg  = (DDSMessage *)malloc(sizeof(DDSMessage));
            if(NULL == pushstate_msg)
            {
                continue;
            }

            set_dds_msg(pushstate_msg, PushState, gxid, list->gxid, src_txn_pid, entry->xact_start_time);
            cn_nodeid = get_nodeid_from_gxid(gxid);
            send_dds_msg(pushstate_msg, cn_nodeid, true);
            list = list->next;
        }
        dds_map_iter_next(dependency_map);
    }
}

/*
    send kill transaction message to a node.
*/
void
set_and_send_kill_txn_msg(DDSMessage *msg, int nodeid, bool is_send_to_cn)
{
    DDSMessage *killtxn_msg  = (DDSMessage *)malloc(sizeof(DDSMessage));
    if (NULL == killtxn_msg)
    {
        return;
    }
    memcpy(&killtxn_msg->label, &msg->label, sizeof(msg->label));
    set_dds_msg(killtxn_msg, KillTransaction, msg->dependency.dest_gxid, msg->dependency.src_gxid, msg->dependency_pid, 0);  
    send_dds_msg(killtxn_msg, nodeid, is_send_to_cn);
}