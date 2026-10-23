/* SPDX-FileCopyrightText: 2026 messere1
 * SPDX-License-Identifier: BSD-3-Clause
 */

/* Standalone regression for transaction classification.
 * From contrib/pgxc_clean: cc -I../../src/include t/explicit_gid.c -o /tmp/explicit_gid && /tmp/explicit_gid
 * No running cluster or generated server headers are required.
 */
#include <assert.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* txninfo only needs the transaction ID definition from gtm_c.h. */
#define GTM_C_H
typedef uint32_t TransactionId;
#include "../txninfo.c"

node_info *pgxc_clean_node_info;
int pgxc_clean_node_count;
database_info *head_database_info;
database_info *last_database_info;

int main(void)
{
    node_info nodes[2] = {{.nodeid=11}, {.nodeid=12}};
    TXN_STATUS states[2] = {TXN_STATUS_PREPARED, TXN_STATUS_COMMITTED};
    txn_info txn = {0};
    pgxc_clean_node_info=nodes;
    pgxc_clean_node_count=2;
    txn.txn_stat=states;
    txn.xid="manual-transaction";
    assert(check_txn_global_status(&txn)==TXN_STATUS_PREPARED);
    txn.xid="manual:gid:with:colons";
    assert(check_txn_global_status(&txn)==TXN_STATUS_PREPARED);
    txn.xid="_$XC$1:cn1:0:2:0:11:12";
    assert(check_txn_global_status(&txn)==TXN_STATUS_COMMITTED);
    states[1]=TXN_STATUS_ABORTED;
    assert(check_txn_global_status(&txn)==TXN_STATUS_ABORTED);
    states[1]=TXN_STATUS_PREPARED;
    assert(check_txn_global_status(&txn)==TXN_STATUS_COMMITTED);
    puts("explicit and implicit transaction classification: ok");
    return 0;
}
