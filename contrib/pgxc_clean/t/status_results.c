/* SPDX-FileCopyrightText: 2026 messere1
 * SPDX-License-Identifier: BSD-3-Clause
 */

/* Compile after configure, from contrib/pgxc_clean:
 * cc -DFRONTEND -I../../src/include -I../../src/interfaces/libpq -ffunction-sections -fdata-sections t/status_results.c -Wl,--gc-sections -o /tmp/status_results
 * /tmp/status_results
 */
#include "postgres_fe.h"
#include "libpq-fe.h"
#include <assert.h>
typedef struct { ExecStatusType status; int isnull; char *value; int cleared; } TestResult;
static TestResult results[2];
static int query_count;
static PGresult *test_exec(PGconn *conn, const char *query);
static ExecStatusType test_status(const PGresult *res);
static int test_isnull(const PGresult *res, int row, int col);
static char *test_value(const PGresult *res, int row, int col);
static void test_clear(PGresult *res);
#define PQexec test_exec
#define PQresultStatus test_status
#define PQgetisnull test_isnull
#define PQgetvalue test_value
#define PQclear test_clear
#define main pgxc_clean_main
#include "../pgxc_clean.c"
#undef main

static PGresult *test_exec(PGconn *conn, const char *query)
{ assert(query_count < 2); return (PGresult *) &results[query_count++]; }
static ExecStatusType test_status(const PGresult *res)
{ return ((const TestResult *)res)->status; }
static int test_isnull(const PGresult *res, int row, int col)
{ return ((const TestResult *)res)->isnull; }
static char *test_value(const PGresult *res, int row, int col)
{ return ((const TestResult *)res)->value; }
static void test_clear(PGresult *res)
{ if (res) { TestResult *r=(TestResult *)res; assert(!r->cleared); r->cleared=1; } }
int main(void)
{
    node_info node={.node_name="dn1"};
    TXN_STATUS expected[6]={TXN_STATUS_COMMITTED,TXN_STATUS_ABORTED,TXN_STATUS_INPROGRESS,
        TXN_STATUS_UNKNOWN,TXN_STATUS_UNKNOWN,TXN_STATUS_UNKNOWN};
    int i,j;
    pgxc_clean_node_info=&node;
    for (i=0; i<6; i++) {
        query_count=0;
        memset(results,0,sizeof(results));
        results[0].status=PGRES_TUPLES_OK;
        results[0].value=i==0 ? "t" : "f";
        results[0].isnull=i>=2;
        results[1].status=i==4 ? PGRES_FATAL_ERROR : PGRES_TUPLES_OK;
        results[1].isnull=i==5;
        results[1].value=i==2 ? "t" : "f";
        assert(getTxnStatus(NULL,42,0)==expected[i]);
        assert(query_count==(i<2 ? 1 : 2));
        for (j=0;j<query_count;j++) assert(results[j].cleared==1);
    }
    puts("all status result paths release their PGresults: ok");
    return 0;
}
