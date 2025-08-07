/*
 * contrib/audit_test/audit_test.c
 */

#include "postgres_fe.h"

#include "libpq-fe.h"
#include "pg_getopt.h"

extern int test_PQexecParams(int argc, char ** argv);
extern int test_alog();
extern int test_alog0();

int main(int argc, char ** argv)
{
	test_alog();
	return 0;
}
