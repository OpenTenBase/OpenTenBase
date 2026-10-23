/* SPDX-FileCopyrightText: 2026 messere1
 * SPDX-License-Identifier: BSD-3-Clause
 */

/* Compile after configure, from contrib/pgxc_clean:
 * cc -DFRONTEND -I../../src/include -I../../src/interfaces/libpq -ffunction-sections -fdata-sections t/password_retry.c -Wl,--gc-sections -o /tmp/password_retry
 * /tmp/password_retry
 */
#include "postgres_fe.h"
#include "libpq-fe.h"
#include <assert.h>
static int attempts;
static char secret[] = "a-password-longer-than-a-pointer";
static PGconn *test_connect(const char **keywords, const char **values, int expand);
static ConnStatusType test_status(const PGconn *conn);
static int test_needs_password(const PGconn *conn);
static void test_finish(PGconn *conn);
static void test_prompt(const char *prompt, char *dest, size_t size, bool echo);
#define PQconnectdbParams test_connect
#define PQstatus test_status
#define PQconnectionNeedsPassword test_needs_password
#define PQfinish test_finish
#define simple_prompt test_prompt
#define main pgxc_clean_main
#include "../pgxc_clean.c"
#undef main

static PGconn *test_connect(const char **keys, const char **values, int expand)
{
    assert(strcmp(keys[3], "password") == 0);
    if (attempts++) assert(strcmp(values[3], secret) == 0);
    return (PGconn *) &attempts;
}
static ConnStatusType test_status(const PGconn *conn)
{ return attempts == 1 ? CONNECTION_BAD : CONNECTION_OK; }
static int test_needs_password(const PGconn *conn) { return 1; }
static void test_finish(PGconn *conn) { }
static void test_prompt(const char *prompt, char *dest, size_t size, bool echo)
{
    assert(size == sizeof(password));
    snprintf(dest, size, "%s", secret);
}
int main(void)
{
    PGconn *conn;
    have_password=false;
    try_password_opt=TRI_DEFAULT;
    conn=loginDatabase("localhost", 5432, "tester", password, sizeof(password),
                       "postgres", "test", "auto", "Password: ");
    assert(conn && attempts == 2 && have_password);
    puts("password retry preserves full input: ok");
    return 0;
}
