/*-------------------------------------------------------------------------
 *
 *     dds.h
 *
 *      Function definitions for DDS algorithm
 *
 *
 * Copyright (c) 2019-2024 TeleDB Development Group
 *
 * src/include/pgxc/dds.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef DDS_H
#define DDS_H

#include "c.h"
#include "datatype/timestamp.h"
#include "utils/hsearch.h"
#include "storage/dds_dependency.h"


#define DDS_PERIOD 1400000L
#define DDS_PHASE 700000L
#define DDSNODE_LIFE 10000000L
#define DDSPOLLING_SLEEPTIME 200000L
#define DDSDETECTOR_SLEEPTIME 20000L
#define DDSRECEIVER_SLEEPTIME 20000L
#define MAX_GXID_LEN 64

typedef struct GxidList
{
    struct GxidList *next;
    char gxid[MAX_GXID_LEN];
} GxidList;

typedef char DependencyHashtableKey[MAX_GXID_LEN];
typedef struct DependencyHashtableEntry
{
    TimestampTz xact_start_time;
    struct GxidList *gxid_entry;
} DependencyHashtableEntry;

typedef struct DDSDependency
{
    char src_gxid[MAX_GXID_LEN];
    char dest_gxid[MAX_GXID_LEN];
} DDSDependency;

typedef struct DDSDependencyList
{
    struct DDSDependency dependency;
    struct DDSDependencyList *next;
} DDSDependencyList;

typedef struct DDSLabel
{
    int64 txn_create_ts;
} DDSLabel;

typedef struct DDSNode
{
    char gxid[MAX_GXID_LEN];
    DDSDependencyList *downstream_nodes;
    int64 create_ts;
    int ddsv;
    DDSLabel private_label;
    DDSLabel public_label;
    int64 last_dds_period;
} DDSNode;

typedef char DDSNodeHashtableKey[MAX_GXID_LEN];
typedef struct DDSNodeHashtableEntry
{
    DDSNode node_entry;
} DDSNodeHashtableEntry;

typedef enum {
  PushState = 1,
  DDS,
  UpdateDownstream,
  KillTransaction
} DDSMsgType;

typedef struct DDSMessage
{
    DDSDependency dependency;
    int ddsv;
    DDSLabel label;
    int64 send_ts;
    int dependency_pid;
    DDSMsgType msgtype;
} DDSMessage;

typedef struct DDSMsgNodeid
{
    DDSMessage *msg;
    int nodeid;
} DDSMsgNodeid;


/* DDS Algorithm functions */
extern bool process_dds_msg(DDSMessage *msg, DDSNode *node);
extern void init_ddsnode(DDSNode *node, DDSMessage *msg);
extern void delete_ddsnode(DDSNode *node);
extern void delete_ddsnode_downstream_list(DDSNode *node);
extern void check_hashtable_nodes_alive_and_send_dds_msg(void *node_map);
extern DDSDependencyList *delete_dependency_from_list(DDSDependencyList *list, char *gxid);
extern DDSDependencyList *insert_dependency_into_list(DDSDependencyList *list, DDSDependency *dep);
extern bool check_txn_exist(char *gxid);
extern int kill_proccess(int pid);
extern int terminate_transaction(char *gxid);
extern int get_nodeid_from_gxid(char *gxid);
extern void set_dds_msg(DDSMessage *msg, DDSMsgType msgtype, char *src_gxid, char *dest_gxid, int src_txn_pid, TimestampTz ts);
extern void send_dds_msg(DDSMessage *msg, int nodeid, bool is_send_to_cn);
extern void set_and_send_kill_txn_msg(DDSMessage *msg, int nodeid, bool is_send_to_cn);
extern void traverse_and_make_pushState(void *dependency_map);
extern GxidList *find_and_delete_dependent(GxidList *list, Dependent_Trans *dep_trans);
extern int find_and_add_dependent(DependencyHashtableEntry *dep_entry, Dependent_Trans *dep_trans);

/* shmem bucket init functions, use in ipci.c */
extern size_t DependentShmemSize(void);
extern void InitDependentShmem(void);

/* bucket functions */
extern bool is_empty_bucket(Dependent_Bucket *dep_bucket);
extern inline int get_bucket_len(Dependent_Bucket *dep_bucket);
extern inline Dependent_Trans *get_true_location(Dependent_Bucket *dep_bucket, Dependent_Trans *cur_loc);

/* backend proc functions */
extern void check_dep_in_list(Dependent_List *add_list, Dependent_List *del_list, Dependent_Cell *cell);
extern void put_add_to_delete(Dependent_List *add_list, Dependent_List *del_list);
extern void push_dep_to_bucket(Dependent_List *list, DepType res, int *bucket_index);

/* Dependent list functions */
extern Dependent_List *new_dep_list(void);
extern void free_dep_list(Dependent_List *dep_list);
extern void delete_gxidlist(DependencyHashtableEntry *entry);

/* DDSQueue functions */
extern DDSQueue *new_ddsqueue(void);
extern DDSElem *pop_ddsqueue(DDSQueue *dds_q);
extern void push_ddsqueue(DDSQueue *dds_q, DDSElem *dds_e);
extern void free_ddsqueue(DDSQueue *dds_q);

#endif
