/* -------------------------------------------------------------------------
 *
 * pg_subscription_table.h
 *        Local info about tables that come from the publisher of a
 *        subscription (pg_subscription_table).
 *
 * Copyright (c) 2023 THL A29 Limited, a Tencent company.
 *
 * This source code file is licensed under the BSD 3-Clause License,
 * you may obtain a copy of the License at http://opensource.org/license/bsd-3-clause/
 *
 * -------------------------------------------------------------------------
 */
#ifndef PG_SUBSCRIPTION_TABLE_H
#define PG_SUBSCRIPTION_TABLE_H

#include "catalog/genbki.h"

/* ----------------
 *        pg_subscription_table definition. cpp turns this into
 *        typedef struct FormData_pg_subscription_table
 * ----------------
 */
#define SubscriptionTableRelationId            9033


CATALOG(pg_subscription_table,9033)
{
    Oid            srsubid;        /* Oid of subscription */
    Oid            srrelid;        /* Oid of table */
    NameData    pubname;        /* publication name */
} FormData_pg_subscription_table;

typedef FormData_pg_subscription_table *Form_pg_subscription_table;

/* ----------------
 *        compiler constants for pg_subscription_shard
 * ----------------
 */
#define Natts_pg_subscription_table                3
#define Anum_pg_subscription_table_srsubid        1
#define Anum_pg_subscription_table_srrelid      2
#define Anum_pg_subscription_table_pubname      3

#endif                            /* PG_SUBSCRIPTION_TABLE_H */

