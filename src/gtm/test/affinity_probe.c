/*-------------------------------------------------------------------------
 *
 * affinity_probe.c
 *      Request real GTM timestamps across a timekeeper synchronization cycle.
 *
 * Copyright (c) 2026, OpenTenBase Authors
 * Licensed under the BSD-3-Clause license; see LICENSE.txt.
 *
 * IDENTIFICATION
 *      src/gtm/test/affinity_probe.c
 *
 *-------------------------------------------------------------------------
 */

#include "gtm/gtm_c.h"
#include "gtm/gtm_client.h"
#include "gtm/libpq-fe.h"

/* Required by the GTM client/common libraries, as in gtm_ctl. */
pthread_key_t threadinfo_key;
GTM_ThreadID TopMostThreadID;
int tcp_keepalives_idle;
int tcp_keepalives_interval;
int tcp_keepalives_count;

void CreateLockFile(const char *filename, const char *refName);
void DeleteLockFile(const char *filename);

/* Shared library objects reference server-only hooks, as in gtm_ctl. */
void
CreateLockFile(const char *filename, const char *refName)
{
	/* A client probe must never try to create a server socket lock file. */
	abort();
}

void
DeleteLockFile(const char *filename)
{
	abort();
}

int
main(int argc, char **argv)
{
	GTM_Conn   *conn;
	Get_GTS_Result result;
	GTM_Timestamp first = InvalidGlobalTimestamp;
	GTM_Timestamp previous = InvalidGlobalTimestamp;
	char		conninfo[256];
	char	   *end;
	long		port;
	int			i;

	if (argc != 2 && !(argc == 3 && strcmp(argv[2], "status") == 0))
	{
		fprintf(stderr, "usage: %s PORT [status]\n", argv[0]);
		return 2;
	}
	port = strtol(argv[1], &end, 10);
	if (end == argv[1] || *end != '\0' || port < 1 || port > 65535)
	{
		fprintf(stderr, "invalid GTM port: %s\n", argv[1]);
		return 2;
	}

	snprintf(conninfo, sizeof(conninfo),
			 "host=127.0.0.1 port=%ld connect_timeout=2 "
			 "node_name=affinity_probe remote_type=%d",
			 port, argc == 3 ? GTM_NODE_GTM_CTL : GTM_NODE_COORDINATOR);
	conn = PQconnectGTM(conninfo);
	if (conn == NULL || GTMPQstatus(conn) != CONNECTION_OK)
	{
		fprintf(stderr, "GTM connection failed: %s\n",
				conn ? GTMPQerrorMessage(conn) : "out of memory");
		if (conn)
			GTMPQfinish(conn);
		return 1;
	}

	if (argc == 3)
	{
		int			status;
		int			standby_count;
		int		   *slave_is_sync = NULL;
		GTM_Timestamp master;
		GTM_Timestamp *standby = NULL;
		XLogRecPtr	master_ptr;
		XLogRecPtr *slave_flush_ptr = NULL;
		char	  **application_name[GTM_MAX_WALSENDER] = {NULL};
		int			rc;

		/* The pre-startup standby service accepts status, but cannot issue GTS. */
		rc = check_gtm_status(conn, &status, &master, &master_ptr,
							  &standby_count, &slave_is_sync, &standby,
							  &slave_flush_ptr, application_name, 1);
		if (rc != GTM_RESULT_OK)
			fprintf(stderr, "GTM status request failed: %s\n", GTMPQerrorMessage(conn));
		else
			printf("gtm_status=%d\n", status);
		GTMPQfinish(conn);
		return rc == GTM_RESULT_OK && status == 1 ? 0 : 1;
	}

	for (i = 0; i < 3; i++)
	{
		/* Keep timestamp requests working across a sync interval. */
		if (i == 2)
			pg_usleep(GTM_SYNC_CYCLE + GTM_GTS_ONE_SECOND);
		result = get_global_timestamp(conn);
		if (result.gts == InvalidGlobalTimestamp || result.gts < previous)
		{
			fprintf(stderr, "invalid or decreasing GTM timestamp: " INT64_FORMAT
					" after " INT64_FORMAT "\n", result.gts, previous);
			GTMPQfinish(conn);
			return 1;
		}
		if (i == 0)
			first = result.gts;
		previous = result.gts;
		printf("timestamp[%d]=" INT64_FORMAT "\n", i, result.gts);
	}
	GTMPQfinish(conn);
	if (previous == first)
	{
		fprintf(stderr, "GTM timestamp did not advance across a sync cycle\n");
		return 1;
	}
	return 0;
}
