/* SPDX-FileCopyrightText: 2026 messere1
 * SPDX-License-Identifier: BSD-3-Clause
 */

/* Compile after configure, from contrib/pgxc_clean:
 * cc -DFRONTEND -I../../src/include -I../../src/interfaces/libpq -ffunction-sections -fdata-sections t/unsigned_xid.c -Wl,--gc-sections -o /tmp/unsigned_xid
 * /tmp/unsigned_xid
 */
#include "postgres_fe.h"
#include "libpq-fe.h"
#include <assert.h>
static int query_count;
static const char *expected;
static PGresult *test_exec(PGconn *conn,const char *query);
static ExecStatusType test_status(const PGresult *res);
static int test_isnull(const PGresult *res,int row,int col);
static char *test_value(const PGresult *res,int row,int col);
static void test_clear(PGresult *res);
#define PQexec test_exec
#define PQresultStatus test_status
#define PQgetisnull test_isnull
#define PQgetvalue test_value
#define PQclear test_clear
#define main pgxc_clean_main
#include "../pgxc_clean.c"
#undef main
static PGresult *test_exec(PGconn *conn,const char *query)
{
    char literal[64];
    snprintf(literal,sizeof(literal),"''%s''::xid",expected);
    assert(strstr(query,literal)!=NULL);
    query_count++;
    return (PGresult *)&query_count;
}
static ExecStatusType test_status(const PGresult *res) { return PGRES_TUPLES_OK; }
static int test_isnull(const PGresult *res,int row,int col) { return query_count==1; }
static char *test_value(const PGresult *res,int row,int col) { return "t"; }
static void test_clear(PGresult *res) { }
int main(void)
{
    const char *ids[]={"42","2147483647","2147483648","4294967295"};
    node_info node={.node_name="dn1"};
    int i;
    pgxc_clean_node_info=&node;
    for(i=0;i<4;i++) {
        GlobalTransactionId gxid=(GlobalTransactionId)strtoul(ids[i],NULL,10);
        expected=ids[i];
        query_count=0;
        assert(getTxnStatus(NULL,gxid,0)==TXN_STATUS_INPROGRESS);
        assert(query_count==2);
    }
    puts("status queries preserve the full unsigned xid range: ok");
    return 0;
}
