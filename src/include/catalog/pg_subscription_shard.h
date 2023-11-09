/* -------------------------------------------------------------------------
 *
 * pg_subscription_shard.h
 *        Local info about shards that come from the publisher of a
 *        subscription (pg_subscription_shard).
 *
 * Copyright (c) 2023 THL A29 Limited, a Tencent company.
 *
 * This source code file is licensed under the BSD 3-Clause License,
 * you may obtain a copy of the License at http://opensource.org/license/bsd-3-clause/
 *
 * -------------------------------------------------------------------------
 */
#ifndef PG_SUBSCRIPTION_SHARD_H
#define PG_SUBSCRIPTION_SHARD_H

#include "catalog/genbki.h"

/* ----------------
 *        pg_subscription_shard definition. cpp turns this into
 *        typedef struct FormData_pg_subscription_shard
 * ----------------
 */
#define SubscriptionShardRelationId            9030


CATALOG(pg_subscription_shard,9030)
{
    Oid            srsubid;        /* Oid of subscription */
    int32        srshardid;        /* Id of shard */
    NameData    pubname;        /* publication name */
} FormData_pg_subscription_shard;

typedef FormData_pg_subscription_shard *Form_pg_subscription_shard;

/* ----------------
 *        compiler constants for pg_subscription_shard
 * ----------------
 */
#define Natts_pg_subscription_shard                3
#define Anum_pg_subscription_shard_srsubid        1
#define Anum_pg_subscription_shard_srshardid    2
#define Anum_pg_subscription_shard_pubname      3

#endif                            /* PG_SUBSCRIPTION_SHARD_H */


