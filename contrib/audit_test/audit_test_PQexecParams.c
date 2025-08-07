/*
 * contrib/audit_test/audit_test.c
 */

#include "postgres_fe.h"

#include "libpq-fe.h"
#include "pg_getopt.h"

extern int test_PQexecParams(int argc, char ** argv);

static void
exit_nicely(PGconn *conn)
{
	PQfinish(conn);
	exit(1);
}

int test_PQexecParams(int argc, char ** argv)
{
	const char 	*conninfo;
	PGconn	   *conn;
	PGresult   *res;

	if (argc > 1)
		conninfo = argv[1];
	else
		conninfo = "hostaddr = '100.76.24.204' port = '52898' dbname = 'postgres' user = 'mpgxz' connect_timeout = '20'";

	conn = PQconnectdb(conninfo);

	if (PQstatus(conn) != CONNECTION_OK)
	{
		fprintf(stderr, "Connection to database failed: %s",
				PQerrorMessage(conn));
		exit_nicely(conn);
	}

	res = PQexecParams(conn,
					   "select sum(f1) from test where f3 > 200 union  all select sum(f1) from test where f3 > 100;",
					   0,
					   NULL,
					   NULL,
					   NULL,
					   NULL,
					   0);

	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		fprintf(stderr, "SELECT failed: %s", PQerrorMessage(conn));
		PQclear(res);
		exit_nicely(conn);
	}

	PQclear(res);

	PQfinish(conn);

	return 0;
}
