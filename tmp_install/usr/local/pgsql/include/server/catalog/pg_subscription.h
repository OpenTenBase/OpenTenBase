/* -------------------------------------------------------------------------
 *
 * pg_subscription.h
 *        Definition of the subscription catalog (pg_subscription).
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * This source code file contains modifications made by THL A29 Limited ("Tencent Modifications").
 * All Tencent Modifications are Copyright (C) 2023 THL A29 Limited.
 *
 * -------------------------------------------------------------------------
 */
#ifndef PG_SUBSCRIPTION_H
#define PG_SUBSCRIPTION_H

#include "catalog/genbki.h"
#include "nodes/pg_list.h"
#ifdef __STORAGE_SCALABLE__
#include "catalog/objectaddress.h"
#endif

/* ----------------
 *        pg_subscription definition. cpp turns this into
 *        typedef struct FormData_pg_subscription
 * ----------------
 */
#define SubscriptionRelationId            6100
#define SubscriptionRelation_Rowtype_Id 6101

/*
 * Technically, the subscriptions live inside the database, so a shared catalog
 * seems weird, but the replication launcher process needs to access all of
 * them to be able to start the workers, so we have to put them in a shared,
 * nailed catalog.
 *
 * NOTE:  When adding a column, also update system_views.sql.
 */
CATALOG(pg_subscription,6100) BKI_SHARED_RELATION BKI_ROWTYPE_OID(6101) BKI_SCHEMA_MACRO
{
    Oid            subdbid;        /* Database the subscription is in. */
    NameData    subname;        /* Name of the subscription */

    Oid            subowner;        /* Owner of the subscription */

    bool        subenabled;        /* True if the subscription is enabled (the
                                 * worker should be running) */

#ifdef CATALOG_VARLEN            /* variable-length fields start here */
    /* Connection string to the publisher */
    text        subconninfo BKI_FORCE_NOT_NULL;

    /* Slot name on publisher */
    NameData    subslotname;

    /* Synchronous commit setting for worker */
    text        subsynccommit BKI_FORCE_NOT_NULL;

    /* List of publications subscribed to */
    text        subpublications[1] BKI_FORCE_NOT_NULL;
#endif
} FormData_pg_subscription;

typedef FormData_pg_subscription *Form_pg_subscription;

/* ----------------
 *        compiler constants for pg_subscription
 * ----------------
 */
#define Natts_pg_subscription                    8
#define Anum_pg_subscription_subdbid            1
#define Anum_pg_subscription_subname            2
#define Anum_pg_subscription_subowner            3
#define Anum_pg_subscription_subenabled            4
#define Anum_pg_subscription_subconninfo        5
#define Anum_pg_subscription_subslotname        6
#define Anum_pg_subscription_subsynccommit        7
#define Anum_pg_subscription_subpublications    8

typedef struct Subscription
{
    Oid            oid;            /* Oid of the subscription */
    Oid            dbid;            /* Oid of the database which subscription is
                                 * in */
    char       *name;            /* Name of the subscription */
    Oid            owner;            /* Oid of the subscription owner */
    bool        enabled;        /* Indicates if the subscription is enabled */
    char       *conninfo;        /* Connection string to the publisher */
    char       *slotname;        /* Name of the replication slot */
    char       *synccommit;        /* Synchronous commit setting for worker */
    List       *publications;    /* List of publication names to subscribe to */
#ifdef __SUBSCRIPTION__
    char       *parent_name;            /* Name of OpenTenBase subscription created on coordinator */
    int32        parallel_number;        /* Split OpenTenBase subscription into multiple parallel opentenbase-sub-subscription */
    int32        parallel_index;            /* Index of this opentenbase-sub-subscription in all parallel opentenbase-sub-subscriptions */
    bool        ignore_pk_conflict;        /* ignore primary key conflict occurs when apply */
    char       *manual_hot_date;        /* GUC parameter, manual_hot_date */
    char       *temp_hot_date;            /* GUC parameter, temp_hot_date */
    char       *temp_cold_date;            /* GUC parameter, temp_cold_date */
    bool        is_all_actived;            /* Whether all parallel opentenbase-sub-subscriptions are actived */
    bool        active_state;            /* Whether the current opentenbase-sub-subscription is activated by the first opentenbase-sub-subscription,
                                         * valid only when parallel_index > 0
                                         */
    XLogRecPtr    active_lsn;                /* The LSN value that was set when the current opentenbase-sub-subscription was activated by the first
                                         * opentenbase-sub-subscription, valid only when parallel_index > 0
                                         */
#endif
} Subscription;

extern Subscription *GetSubscription(Oid subid, bool missing_ok);
extern void FreeSubscription(Subscription *sub);
extern Oid    get_subscription_oid(const char *subname, bool missing_ok);
extern char *get_subscription_name(Oid subid);

extern int    CountDBSubscriptions(Oid dbid);

#ifdef __STORAGE_SCALABLE__
extern ObjectAddress subscription_add_shard(char * subname, Oid subid, int32 shardid, char *pubname,
                                                   bool if_not_exists);
extern ObjectAddress subscription_add_table(char * subname, Oid subid, Oid relid, char *pubname,
                                                   bool if_not_exists);
extern List *GetSubscriptionShards(Oid subid, char *pubname);

extern List *GetSubscriptionRelShards(Oid subid, Oid relid);

extern void RemoveSubscriptionShardById(Oid proid);

extern void RemoveSubscriptionTableById(Oid proid);

extern char *GetSubscriptionShardDesc(Oid proid);

extern char *GetSubscriptionTableDesc(Oid proid);
#endif

#ifdef __SUBSCRIPTION__

extern void ActiveAllParallelOpenTenBaseSubscriptions(XLogRecPtr active_lsn);
extern List *GetOpenTenBaseSubscriptnParallelChild(Oid subid);
extern List *GetOpenTenBaseSubscriptnParallelWorker(Oid subid);

#define Natts_opentenbase_subscription                            7
#define Anum_opentenbase_subscription_sub_name                    1
#define Anum_opentenbase_subscription_sub_ignore_pk_conflict        2
#define Anum_opentenbase_subscription_sub_manual_hot_date            3
#define Anum_opentenbase_subscription_sub_temp_hot_date            4
#define Anum_opentenbase_subscription_sub_temp_cold_date            5
#define Anum_opentenbase_subscription_sub_parallel_number            6
#define Anum_opentenbase_subscription_sub_is_all_actived            7

#define Natts_opentenbase_subscription_parallel                    5
#define Anum_opentenbase_subscription_parallel_sub_parent            1
#define Anum_opentenbase_subscription_parallel_sub_child            2
#define Anum_opentenbase_subscription_parallel_sub_index            3
#define Anum_opentenbase_subscription_parallel_sub_active_state    4
#define Anum_opentenbase_subscription_parallel_sub_active_lsn        5

#endif

#endif                            /* PG_SUBSCRIPTION_H */
