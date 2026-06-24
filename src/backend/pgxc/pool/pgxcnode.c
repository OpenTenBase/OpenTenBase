/*-------------------------------------------------------------------------
 *
 * pgxcnode.c
 *
 *	  Functions for the Coordinator communicating with the PGXC nodes:
 *	  Datanodes and Coordinators
 *
 *
 * Portions Copyright (c) 2012-2014, TransLattice, Inc.
 * Portions Copyright (c) 1996-2009, PostgreSQL Global Development Group
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 *
 * IDENTIFICATION
 *	  $$
 *
 *
 *-----------------------------------------------heaptuple.c--------------------------
 */

#include "c.h"
#include "postgres.h"
#include <poll.h>

#ifdef __sun
#include <sys/filio.h>
#endif

#include <sys/time.h>
#include <sys/types.h>
#include <sys/ioctl.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include "pgstat.h"
#include "access/gtm.h"
#include "access/transam.h"
#include "access/atxact.h"
#include "access/xact.h"
#include "access/htup_details.h"
#include "catalog/namespace.h"
#include "catalog/pg_type.h"
#include "commands/prepare.h"
#include "gtm/gtm_c.h"
#include "nodes/nodes.h"
#include "pgxc/pgxcnode.h"
#include "pgxc/execRemote.h"
#include "catalog/pgxc_node.h"
#include "catalog/pg_collation.h"
#include "pgxc/locator.h"
#include "pgxc/nodemgr.h"
#include "pgxc/pgxc.h"
#include "pgxc/poolmgr.h"
#include "tcop/dest.h"
#include "storage/lwlock.h"
#include "utils/builtins.h"
#include "utils/elog.h"
#include "utils/memutils.h"
#include "utils/fmgroids.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"
#include "utils/lsyscache.h"
#include "utils/formatting.h"
#include "utils/tqual.h"
#include "utils/opentenbase_test.h"
#include "optimizer/spm.h"
#include "../interfaces/libpq/libpq-int.h"
#include "../interfaces/libpq/libpq-fe.h"
#ifdef XCP
#include "miscadmin.h"
#include "storage/ipc.h"
#include "pgxc/pause.h"
#include "utils/snapmgr.h"
#endif
#ifdef _MLS_
#include "catalog/pg_authid.h"
#endif
#ifdef __OPENTENBASE_C__
#include "executor/execDispatchFragment.h"
#endif
#ifdef __RESOURCE_QUEUE__
#include "commands/resqueue.h"
#include "utils/resgroup.h"
#endif

#ifdef _PG_ORCL_
#include "access/atxact.h"
#endif
#include "postmaster/clean2pc.h"
#include "storage/ipc.h"

#define CMD_ID_MSG_LEN 8
#define NO_DESC_MSG_LEN 2
#ifdef __OPENTENBASE__
#define PGXC_CANCEL_DELAY    100
#endif
#define InvalidSessionId	-1

#ifdef __OPENTENBASE_C__
int max_handles = DEFAULT_MAX_HANDLES;
#define PLANE_BITS(plane_id) (1 << (plane_id)) 
#endif
uint32	PGXCLocalSessionId = 0;

extern TimestampTz SessionStartTime;
char *CurrentUserName = NULL;

/*
 * Datanode handles saved in Transaction memory context
 * when PostgresMain is launched.
 * Those handles are used inside a transaction by Coordinator to Datanodes.
 */
static PGXCNodeHandle **dn_handles = NULL;
static PGXCNodeHandle **writable_dn_handles = NULL;
static bool dn_handle_acquired[MAX_NODES_NUMBER] = {0};
static bool cn_handle_acquired[MAX_NODES_NUMBER] = {0};

/*
 * Coordinator handles saved in Transaction memory context
 * when PostgresMain is launched.
 * Those handles are used inside a transaction by Coordinator to Coordinators
 */
static PGXCNodeHandle **co_handles = NULL;
static PGXCNodeHandle **writable_co_handles = NULL;

/* record which datanode we have written to */
static Bitmapset *current_write_datanodes = NULL;

PGXCNodeAllHandles *current_transaction_handles = NULL;

List *sock_fatal_handles = NIL;
List *global_error_handles = NIL;

bool hold_first_backend = false;

#ifdef __OPENTENBASE__
/* Hash key: nodeoid value: index in  dn_handles or co_handles */
static HTAB *node_handles_hash = NULL; 
static HTAB *dn_node_handles_hash = NULL; 

typedef struct PGXCNodeHandlesLookupEnt
{
	Oid			nodeoid;	/* Node Oid */
	int32       nodeidx;    /* Node index*/
} PGXCNodeHandlesLookupEnt;

typedef struct PGXCNodePlaneLookupEnt
{
	char		host_name[NAMEDATALEN];	/* Node host name */
	uint32       plane_bitmap;    		/* Node plane bitmap, support 32 planes */
} PGXCNodePlaneLookupEnt;
static int	pgxc_coordinator_proc_pid = 0;
TransactionId pgxc_coordinator_proc_vxid = InvalidTransactionId;
#endif

/* Current size of dn_handles and co_handles */
int			NumDataNodes;
int 		NumCoords;

/* Use to check send times for use local latest gts snapshot */
int 		send_snapshot_times = 0;
char		send_latest_snapshot_dn[NAMEDATALEN] = {0};

bool 		enable_pgxcnode_message = false;
bool		record_history_messages = true;

#ifdef XCP
volatile bool HandlesInvalidatePending = false;
volatile bool HandlesRefreshPending = false;


/*
 * Session and transaction parameters need to to be set on newly connected
 * remote nodes.
 */
List *session_param_list = NIL;
uint64 max_session_param_cid = 0;
StringInfo 	session_params;

const char *TransactionStmtKindTag[] =
{
	"BEGIN",
	"START",
	"COMMIT TRANSACTION",
	"ROLLBACK TRANSACTION",
	"SAVEPOINT",
	"RELEASE",
	"ROLLBACK TO",
	"PREPARE TRANSACTION",
	"COMMIT PREPARED",
	"ROLLBACK PREPARED",
	"BEGIN SUBTXN",
	"ROLLBACK SUBTXN",
	"COMMIT SUBTXN",
	"COMMIT PREPARED CHECK",
	"ROLLBACK PREPARED CHECK",
	"AUTOXACT START",
	"AUTOXACT END"
};

const char *ConnectionStateTag[] =
{
	"IDLE",
	"BIND",
	"QUERY",
	"CLOSE",
	"ERROR",
	"FATAL",
	"COPY IN",
	"COPY OUT"
};


static bool DoInvalidateRemoteHandles(void);
static bool DoRefreshRemoteHandles(void);
#endif

static void pgxc_node_init(PGXCNodeHandle *handle, int sock, int pid);
static void pgxc_node_free(PGXCNodeHandle *handle);
static void pgxc_node_all_free(void);

static int	get_int(PGXCNodeHandle * conn, size_t len, int *out);
static int	get_char(PGXCNodeHandle * conn, char *out);
static void register_transaction_handles(PGXCNodeHandle *handle, bool readonly);
static void reinit_transaction_handles(int new_handles_num);
static void pgxc_node_dump_message(PGXCNodeHandle *handle, char *ptr, int len, bool isSend);
static uint8 PGXCNodeGetXactIsoLevelParam(void);
static void init_sock_fatal_handles(void);
static void InitHandleTransitionState(PGXCNodeHandle *handle);
static bool IsConnectionCritical(PGXCNodeHandle *handle);
static long CalculateTimeDifference(struct timespec start, struct timespec end);
static void pgxc_node_batch_set_query(PGXCNodeHandle *connections[], int count, bool newconn, uint64 guc_cid);
static void pgxc_node_set_query(PGXCNodeHandle *handle);
/*
 * Initialize PGXCNodeHandle struct
 */
static void
init_pgxc_handle(PGXCNodeHandle *pgxc_handle)
{
	/*
	 * Socket descriptor is small non-negative integer,
	 * Indicate the handle is not initialized yet
	 */
	pgxc_handle->sock = NO_SOCKET;

	/* Initialise buffers */
	pgxc_handle->error[0] = '\0';
	pgxc_handle->outSize = 32;
	pgxc_handle->outBuffer = (char *) palloc(pgxc_handle->outSize);
	pgxc_handle->inSize = 32;

	pgxc_handle->inBuffer = (char *) palloc(pgxc_handle->inSize);
	pgxc_handle->combiner = NULL;
	pgxc_handle->inStart = 0;
	pgxc_handle->inEnd = 0;
	pgxc_handle->inCursor = 0;
	pgxc_handle->outEnd = 0;
	pgxc_handle->needSync = false;
#ifdef __OPENTENBASE__	
	pgxc_handle->sock_fatal_occurred = false;
#endif
#ifndef __USE_GLOBAL_SNAPSHOT__
	pgxc_handle->sendGxidVersion = 0;
#endif
#ifdef __OPENTENBASE_C__
	pgxc_handle->fid = InvalidFid;
	pgxc_handle->fragmentlevel = 0;
	SetHandleUsed(pgxc_handle, false);
	pgxc_handle->read_only = true;
	pgxc_handle->current_user = InvalidOid;
	pgxc_handle->autoxact_level = 0;
	pgxc_handle->session_id = InvalidSessionId;
	pgxc_handle->msg_index = 0;
	pgxc_handle->block_connection = false;
	pgxc_handle->registered_subtranid = InvalidSubTransactionId;
#endif
	InitHandleState(pgxc_handle);
}

/*
 * set max_handles_per_node, need reinitialize the handles, copy the original
 * handles value
 */
void
ReInitHandles(int new_handles_num)
{
	PGXCNodeHandle **co_handles_tmp = NULL;
	PGXCNodeHandle **dn_handles_tmp = NULL;
	MemoryContext	oldcontext;
	int count, i, j;

	oldcontext = MemoryContextSwitchTo(TopMemoryContext);

	if (NumDataNodes > 0)
	{
		dn_handles_tmp = (PGXCNodeHandle **)
			palloc0(NumDataNodes * sizeof(PGXCNodeHandle *));
		for (count = 0; count < NumDataNodes; count++)
		{
			dn_handles_tmp[count] = (PGXCNodeHandle *)
				palloc0(new_handles_num * sizeof(PGXCNodeHandle));
		}

		/*
		 * Just init writer handles, ReInitHandles will
		 * not been called in transaction block
		 */
		writable_dn_handles = (PGXCNodeHandle **)
			palloc0(NumDataNodes * sizeof(PGXCNodeHandle *));
	}

	if (NumCoords > 0)
	{
		co_handles_tmp = (PGXCNodeHandle **)
			palloc0(NumCoords * sizeof(PGXCNodeHandle *));
		for (count = 0; count < NumCoords; count++)
		{
			co_handles_tmp[count] = (PGXCNodeHandle *)
				palloc0(new_handles_num * sizeof(PGXCNodeHandle));
		}

		/*
		 * Just init writer handles, ReInitHandles will
		 * not been called in transaction block
		 */
		writable_co_handles = (PGXCNodeHandle **)
			palloc0(NumCoords * sizeof(PGXCNodeHandle *));
	}

	for (i = 0; i < 2; i++)
	{
		int num_nodes = 0;
		PGXCNodeHandle **handles = NULL;
		PGXCNodeHandle **new_handles = NULL;
		char	node_type = PGXC_NODE_NONE;
		switch (i)
		{
			case 0:
				num_nodes = NumCoords;
				handles = co_handles;
				new_handles = co_handles_tmp;
				node_type = PGXC_NODE_COORDINATOR;
				break;
			case 1:
				num_nodes = NumDataNodes;
				handles = dn_handles;
				new_handles = dn_handles_tmp;
				node_type = PGXC_NODE_DATANODE;
				break;
			default:
				Assert(0);
		}

		if (handles != NULL)
		{
			for (count = 0; count < num_nodes; count++)
			{
				memcpy(new_handles[count], handles[count],
					   sizeof(PGXCNodeHandle) * Min(max_handles, new_handles_num));

				for (j = max_handles; j < new_handles_num; j++)
				{
					init_pgxc_handle(&new_handles[count][j]);
					new_handles[count][j].nodeoid = handles[count][0].nodeoid;
					new_handles[count][j].nodeid = handles[count][0].nodeid;
					new_handles[count][j].nodeidx = count;
					strncpy(new_handles[count][j].nodename,
							handles[count][0].nodename, NAMEDATALEN);
					strncpy(new_handles[count][j].nodehost,
							handles[count][0].nodehost, NAMEDATALEN);
					new_handles[count][j].nodeport = handles[count][0].nodeport;
					new_handles[count][j].node_type = node_type;
				}
				for (j = new_handles_num; j < max_handles; j++)
				{
					PGXCNodeHandle *handle = &handles[count][j];
					pgxc_node_free(handle);
				}
				pfree(handles[count]);
			}
			pfree(handles);
		}
	}

	dn_handles = dn_handles_tmp;
	co_handles = co_handles_tmp;

	reinit_transaction_handles(new_handles_num);
	max_handles = new_handles_num;
	MemoryContextSwitchTo(oldcontext);
}
/*
 * Allocate and initialize memory to store Datanode and Coordinator handles.
 */
void
InitMultinodeExecutor(bool is_force)
{
	int				count;
	Oid				*coOids, *dnOids;
	char			*name;
#ifdef XCP
	MemoryContext	oldcontext;
#endif

#ifdef __OPENTENBASE__
	/* Init node handles hashtable */
	HASHCTL 		hinfo;
	int 			hflags;
	bool			found;
	PGXCNodeHandlesLookupEnt *node_handle_ent = NULL;

	HASHCTL 		dnhinfo;
	int 			dnhflags;
	PGXCNodePlaneLookupEnt *node_plane_ent = NULL;
#endif

	/*
	 * To ensure the integrity of dn_handles and co_handles,
	 * interruptions are not allowed here
	 */
	HOLD_INTERRUPTS();

	if (!IS_PGXC_COORDINATOR)
		max_handles = 1;

	/* Free all the existing information first */
	if (is_force)
		pgxc_node_all_free();

	/* This function could get called multiple times because of sigjmp */
	if (dn_handles != NULL &&
		co_handles != NULL)
	{
		RESUME_INTERRUPTS();
		return;
	}

	elog(LOG, "InitMultinodeExecutor force:%d.", is_force);
	/* Update node table in the shared memory */
	PgxcNodeListAndCountWrapTransaction(is_force, false);

	/* Get classified list of node Oids */
	PgxcNodeGetOids(&coOids, &dnOids, &NumCoords, &NumDataNodes, true);

	/* Process node number related memory */
	RebuildDatanodeQueryHashTable();
	clean_stat_transaction();

#ifdef XCP
	/*
	 * Coordinator and datanode handles should be available during all the
	 * session lifetime
	 */
	oldcontext = MemoryContextSwitchTo(TopMemoryContext);
#endif

	/* Do proper initialization of handles */
	if (NumDataNodes > 0)
	{
		dn_handles = (PGXCNodeHandle **)
			palloc0(NumDataNodes * sizeof(PGXCNodeHandle *));
		for (count = 0; count < NumDataNodes; count++)
		{
			dn_handles[count] = (PGXCNodeHandle *)
				palloc0(max_handles * sizeof(PGXCNodeHandle));
		}

		writable_dn_handles = (PGXCNodeHandle **)
			palloc0(NumDataNodes * sizeof(PGXCNodeHandle *));
	}
	
	if (NumCoords > 0)
	{
		co_handles = (PGXCNodeHandle **)
			palloc0(NumCoords * sizeof(PGXCNodeHandle *));
		for (count = 0; count < NumCoords; count++)
		{
			co_handles[count] = (PGXCNodeHandle *)
				palloc0(max_handles * sizeof(PGXCNodeHandle));
		}

		writable_co_handles = (PGXCNodeHandle **)
			palloc0(NumCoords * sizeof(PGXCNodeHandle *));
	}
	
#ifdef __OPENTENBASE__	
	/* destory hash table */
	if (node_handles_hash)
	{
		hash_destroy(node_handles_hash);
		node_handles_hash = NULL;
	}

	MemSet(&hinfo, 0, sizeof(hinfo));
	hflags = 0;

	hinfo.keysize = sizeof(Oid);
	hinfo.entrysize = sizeof(PGXCNodeHandlesLookupEnt);
	hflags |= HASH_ELEM;

	hinfo.hash = oid_hash;
	hflags |= HASH_FUNCTION;

	node_handles_hash = hash_create("Node Handles Hash",
									NumCoords + NumDataNodes,
									&hinfo, hflags);
	/* destory hash table */
	if (dn_node_handles_hash)
	{
		hash_destroy(dn_node_handles_hash);
		dn_node_handles_hash = NULL;
	}

	MemSet(&dnhinfo, 0, sizeof(dnhinfo));
	dnhflags = 0;
	dnhinfo.keysize = NAMEDATALEN;
	dnhinfo.entrysize = sizeof(PGXCNodePlaneLookupEnt);

	dnhflags |= HASH_ELEM;

	dnhinfo.hash = string_hash;
	dnhflags |= HASH_FUNCTION;

	dn_node_handles_hash = hash_create("DN Node Handles Hash", NumDataNodes,
										  	&dnhinfo, dnhflags);	
#endif

	/* Initialize new empty slots */
	for (count = 0; count < NumDataNodes; count++)
	{
		int i = 0;
		char *node_host = get_pgxc_nodehost(dnOids[count]);

		for (i = 0; i < max_handles; i++)
		{
			init_pgxc_handle(&dn_handles[count][i]);
			dn_handles[count][i].nodeoid = dnOids[count];
			dn_handles[count][i].nodeid = get_pgxc_node_id(dnOids[count]);
			dn_handles[count][i].nodeidx = count;
#if 0
			strncpy(dn_handles[count][i].nodename, get_pgxc_nodename(dnOids[count]),
					NAMEDATALEN);
			strncpy(dn_handles[count][i].nodehost, node_host, NAMEDATALEN);
#endif
			name = get_pgxc_nodename(dnOids[count]);
			strncpy(dn_handles[count][i].nodename, name, NAMEDATALEN);
			pfree(name);

			name = get_pgxc_nodehost(dnOids[count]);
			strncpy(dn_handles[count][i].nodehost, name, NAMEDATALEN);
			pfree(name);

			dn_handles[count][i].nodeport = get_pgxc_nodeport(dnOids[count]);
			if (enable_multi_plane_print)
				elog(LOG, "dn handle %d level %d nodename %s nodehost %s nodeport %d Oid %d NumDataNodes %d", count, i, dn_handles[count][i].nodename,
					dn_handles[count][i].nodehost, dn_handles[count][i].nodeport, dnOids[count], NumDataNodes);
			 dn_handles[count][i].node_type = PGXC_NODE_DATANODE;
		}
#ifdef __OPENTENBASE__
		node_handle_ent = (PGXCNodeHandlesLookupEnt *) hash_search(node_handles_hash, &(dn_handles[count][0].nodeoid),
										    HASH_ENTER, &found);	
		if (node_handle_ent)
		{
			node_handle_ent->nodeoid = dn_handles[count][0].nodeoid;
			node_handle_ent->nodeidx = count;
		}
#endif		
		
		node_plane_ent = (PGXCNodePlaneLookupEnt *) hash_search(dn_node_handles_hash, node_host,
									HASH_FIND, &found);	
		if (found)
		{
			int plane_id = get_pgxc_node_planeid(dnOids[count]);
			node_plane_ent->plane_bitmap = PLANE_BITS(plane_id);
		}
		else
		{
			node_plane_ent = (PGXCNodePlaneLookupEnt *) hash_search(dn_node_handles_hash, node_host,
									HASH_ENTER, &found);
			if (node_plane_ent)
			{
				int plane_id = get_pgxc_node_planeid(dnOids[count]);
				node_plane_ent->plane_bitmap |= PLANE_BITS(plane_id);
			}
		}
	}

	for (count = 0; count < NumCoords; count++)
	{
		int i = 0;

		for (i = 0; i < max_handles; i++)
		{
			init_pgxc_handle(&co_handles[count][i]);
			co_handles[count][i].nodeoid = coOids[count];
			co_handles[count][i].nodeid = get_pgxc_node_id(coOids[count]);
			co_handles[count][i].nodeidx = count;

			strncpy(co_handles[count][i].nodename, get_pgxc_nodename(coOids[count]),
					NAMEDATALEN);
			strncpy(co_handles[count][i].nodehost, get_pgxc_nodehost(coOids[count]),
					NAMEDATALEN);

			co_handles[count][i].nodeport = get_pgxc_nodeport(coOids[count]);
			if (enable_multi_plane_print)
				elog(LOG, "cn handle %d level %d nodename %s nodehost %s nodeport %d Oid %d", count, i, co_handles[count][i].nodename,
					co_handles[count][i].nodehost, co_handles[count][i].nodeport, coOids[count]);
			co_handles[count][i].node_type = PGXC_NODE_COORDINATOR;
		}
		
#ifdef __OPENTENBASE__
		node_handle_ent = (PGXCNodeHandlesLookupEnt *) hash_search(node_handles_hash, &(co_handles[count][0].nodeoid),
										    HASH_ENTER, &found);	
		if (node_handle_ent)
		{
			node_handle_ent->nodeoid = co_handles[count][0].nodeoid;
			node_handle_ent->nodeidx = count;
		}
#endif	
	}

	PGXCNodeId = 0;

	MemoryContextSwitchTo(oldcontext);

	if (IS_PGXC_COORDINATOR)
	{
		for (count = 0; count < NumCoords; count++)
		{
			if (pg_strcasecmp(PGXCNodeName,
					   get_pgxc_nodename(co_handles[count][0].nodeoid)) == 0)
				PGXCNodeId = count + 1;
		}

		if (IsConnFromApp())
		{
			RefreshQueryId();
		}
	}
	else /* DataNode */
	{
		for (count = 0; count < NumDataNodes; count++)
		{
			if (pg_strcasecmp(PGXCNodeName,
					   get_pgxc_nodename(dn_handles[count][0].nodeoid)) == 0)
				PGXCNodeId = count + 1;
		}
	}
#ifdef __OPENTENBASE__
	if (PGXCMainPlaneNameID == PGXCPlaneNameID)
		IsPGXCMainPlane = true;

	PGXCNodeOid = get_pgxc_nodeoid(PGXCNodeName);

	init_transaction_handles();
#endif
	RESUME_INTERRUPTS();
}

inline void
DestroyHandle_internal(PGXCNodeHandle *handle, const char *file, const int line)
{
	HOLD_INTERRUPTS();
	elog(LOG, "handle %s pid %d sock %d state %s is going to be destroyed due to fatal error, invoker: %s: %d",
		 handle->nodename, handle->backend_pid, handle->sock, GetConnectionStateTag(handle), file, line);
	UpdateConnectionState(handle, DN_CONNECTION_STATE_FATAL);
	pgxc_node_free(handle);
	RESUME_INTERRUPTS();
}

/*
 * Used to retrieve oid of specified connection from pooler with
 * input nodeid(index from executor).
 */
Oid get_nodeoid_from_nodeid(int nodeid, char node_type)
{
    if (PGXC_NODE_COORDINATOR == node_type)
    {
        if (nodeid >= NumCoords)
        {
            return InvalidOid;
        }
        return co_handles[nodeid][0].nodeoid;
    } 
    else if (PGXC_NODE_DATANODE == node_type)
    {
        if (nodeid >= NumDataNodes)
        {
            return InvalidOid;
        }
        return dn_handles[nodeid][0].nodeoid;
    }
    return InvalidOid;
}

/*
 * Used to retrieve oid of specified connection from pooler with matched 
 * node_id(hashed result) in that connection.
 */
Oid get_nodeoid_from_node_id(int node_id, char node_type)
{	
	int i;

    if (PGXC_NODE_COORDINATOR == node_type)
    {
        for (i = 0; i < NumCoords; i++)
        {
			if (node_id == co_handles[i][0].nodeid)
			{
				return co_handles[i][0].nodeoid;
			}
		}
    } 
    else if (PGXC_NODE_DATANODE == node_type)
    {
        for (i = 0; i < NumDataNodes; i++)
        {
			if (node_id == dn_handles[i][0].nodeid)
			{
				return dn_handles[i][0].nodeoid;
			}
		}
    }
    return InvalidOid;
}

/*
 * Builds up a connection string
 */
char *
PGXCNodeConnStr(char *host, int port, char *dbname,
				char *user, char *pgoptions, char *remote_type, char *parent_node, char *out)
{
	char	    connstr[1024];
	int			num;
#ifdef __OPENTENBASE__
	bool       same_host = false;

	if (host && (strcmp(PGXCNodeHost, host) == 0))
	{
		same_host = true;
	}
#endif
    /*
	 * Build up connection string
	 * remote type can be Coordinator, Datanode or application.
	 */
#ifdef _MLS_
    if (strcmp(user, MLS_USER) == 0 || strcmp(user, AUDIT_USER))
    {
    	if (same_host)
		{
	        num = snprintf(connstr, sizeof(connstr),
				   "port=%d dbname=%s user=%s application_name='pgxc:%s' sslmode=disable options='-c remotetype=%s -c parentnode=%s %s %s'",
				   port, dbname, user, parent_node, remote_type, parent_node,
				   pgoptions, MLS_CONN_OPTION);
		}
		else
		{
	        num = snprintf(connstr, sizeof(connstr),
					   "host=%s port=%d dbname=%s user=%s application_name='pgxc:%s' sslmode=disable options='-c remotetype=%s -c parentnode=%s %s %s'",
					   host, port, dbname, user, parent_node, remote_type, parent_node,
					   pgoptions, MLS_CONN_OPTION);
		}
    }
    else
    {
#endif
		if (same_host)
		{
	    	num = snprintf(connstr, sizeof(connstr),
				   "port=%d dbname=%s user=%s application_name='pgxc:%s' sslmode=disable options='-c remotetype=%s -c parentnode=%s %s'",
				   port, dbname, user, parent_node, remote_type, parent_node,
				   pgoptions);	
		}
		else
		{
	    	num = snprintf(connstr, sizeof(connstr),
					   "host=%s port=%d dbname=%s user=%s application_name='pgxc:%s' sslmode=disable options='-c remotetype=%s -c parentnode=%s %s'",
					   host, port, dbname, user, parent_node, remote_type, parent_node,
					   pgoptions);
		}
#ifdef _MLS_
    }
#endif    
	if (tcp_keepalives_idle > 0)
	{
		num += snprintf(connstr + num, sizeof(connstr) - num,
					   " connect_timeout=%d", tcp_keepalives_idle);
	}
	/* Check for overflow */
	if (num > 0 && num < sizeof(connstr))
	{
		/* Output result */
		if (NULL == out)
		{
			out = (char *) palloc(num + 1);
			strcpy(out, connstr);
		}
		else
		{
			StrNCpy(out, connstr, num + 1);
		}
		return out;
	}

	/* return NULL if we have problem */
	return NULL;
}


/*
 * Connect to a Datanode using a connection string
 */
NODE_CONNECTION *
PGXCNodeConnect(char *connstr)
{
	PGconn	   *conn;

	/* Delegate call to the pglib */
	conn = PQconnectdb(connstr);
	return (NODE_CONNECTION *) conn;
}

#ifdef __OPENTENBASE__
NODE_CONNECTION *
PGXCNodeConnectBarely(char *connstr)
{
	PGconn	   *conn;
	/* Delegate call to the pglib */
	conn = PQconnectdb(connstr);	
	return (NODE_CONNECTION *) conn;
}


char*
PGXCNodeSendShowQuery(NODE_CONNECTION *conn, const char *sql_command)
{
	int32        resStatus;
	static char  number[128] = {'0'};
	PGresult	*result;
#if 0
	result = PQexec((PGconn *) conn, "set remotetype to application;");
	if (!result)
	{
		return number;
	}	
	PQclear(result);
#endif
	
	result = PQexec((PGconn *) conn, sql_command);
	if (!result)
	{
		return number;
	}

	resStatus = PQresultStatus(result);
	if (resStatus == PGRES_TUPLES_OK || resStatus == PGRES_COMMAND_OK)
    {       	
		snprintf(number, 128, "%s", PQgetvalue(result, 0, 0));			
    }	
	PQclear(result);
	
#if 0
	result = PQexec((PGconn *) conn, "set remotetype to coordinator;");
	if (!result)
	{
		return number;
	}
	PQclear(result);	
#endif
	return number;
}

#endif
int PGXCNodePing(const char *connstr)
{
	if (connstr[0])
	{
		PGPing status = PQping(connstr);
		if (status == PQPING_OK)
			return 0;
		else
			return 1;
	}
	else
		return -1;
}

/*
 * Close specified connection
 */
void
PGXCNodeClose(NODE_CONNECTION *conn)
{
	/* Delegate call to the pglib */
	PQfinish((PGconn *) conn);
}

/*
 * Checks if connection active
 */
int
PGXCNodeConnected(NODE_CONNECTION *conn)
{
	/* Delegate call to the pglib */
	PGconn	   *pgconn = (PGconn *) conn;

	/*
	 * Simple check, want to do more comprehencive -
	 * check if it is ready for guery
	 */
	return pgconn && PQstatus(pgconn) == CONNECTION_OK;
}



/* Close the socket handle (this process' copy) and free occupied memory
 *
 * Note that we do not free the handle and its members. This will be
 * taken care of when the transaction ends, when TopTransactionContext
 * is destroyed in xact.c.
 */
static inline void
pgxc_node_free(PGXCNodeHandle *handle)
{
	if (handle->sock != NO_SOCKET)
		close(handle->sock);
	handle->sock = NO_SOCKET;
	SetHandleUsed(handle, false);
}

/*
 * Free all the node handles cached
 */
static void
pgxc_node_all_free(void)
{
	int i, j;
	int index;

	for (i = 0; i < 2; i++)
	{
		int num_nodes = 0;
		PGXCNodeHandle **array_handles = NULL;

		switch (i)
		{
			case 0:
				num_nodes = NumCoords;
				array_handles = co_handles;
				break;
			case 1:
				num_nodes = NumDataNodes;
				array_handles = dn_handles;
				break;
			default:
				Assert(0);
		}

		if (array_handles == NULL)
			continue;

		for (j = 0; j < num_nodes; j++)
		{
			for (index = 0; index < max_handles; index++)
			{
				PGXCNodeHandle *handle = &array_handles[j][index];

				if (handle == NULL)
					continue;
				pgxc_node_free(handle);
			}
		}
		if (array_handles)
		{
			for (index = 0; index < num_nodes; index++)
			{
				pfree(array_handles[index]);
			}
			pfree(array_handles);
		}
	}

	co_handles = NULL;
	dn_handles = NULL;
	pfree_ext(writable_co_handles);
	pfree_ext(writable_dn_handles);

	/* old handles may be saved in sock_fatal_handles or global_error_handles, just clean it up. */
	reset_sock_fatal_handles();
	reset_error_handles(true);

	HandlesInvalidatePending = false;
	HandlesRefreshPending = false;
}

/*
 * Create and initialise internal structure to communicate to
 * Datanode via supplied socket descriptor.
 * Structure stores state info and I/O buffers
 */
static void
pgxc_node_init(PGXCNodeHandle *handle, int sock, int pid)
{
	handle->sock = sock;
	handle->backend_pid = pid;
	handle->ck_resp_rollback = false;
	handle->combiner = NULL;
#ifdef DN_CONNECTION_DEBUG
	handle->have_row_desc = false;
#endif
	memset(handle->error, 0X00, MAX_ERROR_MSG_LENGTH);
	handle->outEnd = 0;
	handle->inStart = 0;
	handle->inEnd = 0;
	handle->inCursor = 0;
	handle->needSync = false;
	handle->sock_fatal_occurred = false;
	handle->autoxact_level = 0;
	handle->msg_index = 0;
	handle->registered = false;
	handle->block_connection = false;
	handle->registered_subtranid = InvalidSubTransactionId;

	Assert(handle->inBuffer && handle->outBuffer);

	InitHandleState(handle);
}


/*
 * Wait while at least one of specified connections has data available and read
 * the data into the buffer
 */
#ifdef __OPENTENBASE__
int
pgxc_node_receive(const int conn_count,
				  PGXCNodeHandle ** connections, struct timeval * timeout)

#else
bool
pgxc_node_receive(const int conn_count,
				  PGXCNodeHandle ** connections, struct timeval * timeout)
#endif
{
#ifndef __OPENTENBASE__
#define ERROR_OCCURED		true
#define NO_ERROR_OCCURED	false
#endif

	int		i,
			sockets_to_poll,
			poll_val;
	bool	is_msg_buffered;
	long 	timeout_ms;
	long	timeout_total;
	struct timespec timeout_start;
	struct	pollfd pool_fd[conn_count];

	/* sockets to be polled index */
	sockets_to_poll = 0;

	is_msg_buffered = false;
	for (i = 0; i < conn_count; i++)
	{
		/* If connection has a buffered message */
		if (HAS_MESSAGE_BUFFERED(connections[i]))
		{
			is_msg_buffered = true;
			break;
		}
	}

	for (i = 0; i < conn_count; i++)
	{
		/* If connection finished sending do not wait input from it */
		if (IsConnectionStateIdle(connections[i]) || HAS_MESSAGE_BUFFERED(connections[i]))
		{
			pool_fd[i].fd = -1;
			pool_fd[i].events = 0;
			elog(DEBUG1, "pgxc_node_receive node:%s pid:%d in DN_CONNECTION_STATE_IDLE no need to receive. ", connections[i]->nodename, connections[i]->backend_pid);
			continue;
		}

		/* prepare select params */
		if (connections[i]->sock > 0)
		{
			pool_fd[i].fd = connections[i]->sock;
			pool_fd[i].events = POLLIN | POLLPRI | POLLRDNORM | POLLRDBAND;
			sockets_to_poll++;
		}
		else
		{
			/* flag as bad, it will be removed from the list */
			UpdateConnectionState(connections[i], DN_CONNECTION_STATE_FATAL);
			pool_fd[i].fd = -1;
			pool_fd[i].events = 0;
		}
	}

	/*
	 * Return if we do not have connections to receive input
	 */
	if (sockets_to_poll == 0)
	{
		if (is_msg_buffered)
		{
#ifdef __OPENTENBASE__
			return DNStatus_OK;			
#else
			return NO_ERROR_OCCURED;
#endif
		}
#ifdef __OPENTENBASE__
		elog(DEBUG1, "no message in buffer");
		return DNStatus_ERR;
#else
		return ERROR_OCCURED;
#endif
	}

	/* do conversion from the select behaviour */
	if (timeout == NULL)
	{
		/* 
		 * if timeout was not set by the caller, we just set 
		 * the timeout to 10 seconds, and the timeout will be
		 * handled inside this function.
		*/
		timeout_ms = -1;
		timeout_total = -1;
	}
	else
	{
		timeout_ms = (timeout->tv_sec * (uint64_t) 1000) + (timeout->tv_usec / 1000);
		timeout_total = timeout_ms;
		clock_gettime(CLOCK_REALTIME, &timeout_start);
	}

retry:
	CHECK_FOR_INTERRUPTS();
	poll_val  = poll(pool_fd, conn_count, timeout_ms);
	if (poll_val < 0)
	{
		/* error - retry if EINTR */
		if (errno == EINTR  || errno == EAGAIN)
		{
			if (timeout_total > 0)
			{
				struct timespec ts;
				long elapse;

				/* obtain current time and calculate the difference */
				clock_gettime(CLOCK_REALTIME, &ts);
				elapse = CalculateTimeDifference(timeout_start, ts);

				/* reset the timeout, or return if it already timeout */
				if (elapse < timeout_total)
					timeout_ms = timeout_total - elapse;
				else
					return DNStatus_EXPIRED;
			}
			goto retry;
		}

		if (errno == EBADF)
		{
			elog(LOG, "poll() bad file descriptor set");
		}
		elog(LOG, "poll() failed for error: %d, %s", errno, strerror(errno));

		if (errno)
		{
#ifdef __OPENTENBASE__
			return DNStatus_ERR;
#else
			return ERROR_OCCURED;
#endif
		}
		
#ifdef __OPENTENBASE__
		return DNStatus_OK;			
#else
		return NO_ERROR_OCCURED;
#endif
	}

	if (poll_val == 0)
	{
		/* Handle timeout */
		elog(DEBUG1, "timeout %ld while waiting for any response from %d connections", timeout_ms, conn_count);

		for (i = 0; i < conn_count; i++)
		{
			PGXCNodeHandle *conn = connections[i];
			elog(DEBUG1, "timeout %ld while waiting for any response from node:%s pid:%d connections", timeout_ms, conn->nodename, conn->backend_pid);
		}		
#ifdef __OPENTENBASE__
		return DNStatus_EXPIRED;			
#else
		return NO_ERROR_OCCURED;
#endif
	}

	/* read data */
	for (i = 0; i < conn_count; i++)
	{
		PGXCNodeHandle *conn = connections[i];

		if (pool_fd[i].fd == -1)
			continue;

		if (pool_fd[i].fd == conn->sock)
		{
			if (pool_fd[i].revents & POLLIN)
			{
				int	read_status = pgxc_node_read_data(conn, true);
				if (read_status == EOF || read_status < 0)
				{
					/* Can not read - no more actions, just discard connection */
					add_error_message(conn, "unexpected EOF on datanode connection.");
					elog(LOG, "unexpected EOF on node:%s pid:%d, read_status:%d, EOF:%d", conn->nodename, conn->backend_pid, read_status, EOF);
					#if 0
					/*
					 * before returning, also update the shared health
					 * status field to indicate that this node could be
					 * possibly unavailable.
					 *
					 * Note that this error could be due to a stale handle
					 * and it's possible that another backend might have
					 * already updated the health status OR the node
					 * might have already come back since the last disruption
					 */
					PoolPingNodeRecheck(conn->nodeoid);
					
					/* Should we read from the other connections before returning? */
					#endif
#ifdef __OPENTENBASE__
					return DNStatus_ERR;			
#else
					return ERROR_OCCURED;
#endif
				}

			}
			else if (
					(pool_fd[i].revents & POLLERR) ||
					(pool_fd[i].revents & POLLHUP) ||
					(pool_fd[i].revents & POLLNVAL)
					)
			{
				UpdateConnectionState(connections[i],
						DN_CONNECTION_STATE_FATAL);
				add_error_message(conn, "unexpected network error on datanode connection");
				elog(LOG, "unexpected EOF on datanode:%s pid:%d with event %d", conn->nodename, conn->backend_pid, pool_fd[i].revents);
				/* Should we check/read from the other connections before returning? */
#ifdef __OPENTENBASE__
				return DNStatus_ERR;			
#else
				return ERROR_OCCURED;
#endif
			}
		}
	}
#ifdef __OPENTENBASE__
	return DNStatus_OK;			
#else
	return NO_ERROR_OCCURED;
#endif
}


void
pgxc_print_pending_data(PGXCNodeHandle *handle, bool reset)
{
	char	   *msg;
	int32       ret;
	int 		msg_len;
	char		msg_type;
	struct timeval	timeout;

	timeout.tv_sec	= 0;
	timeout.tv_usec	= 1000;

	for (;;)
	{		
		UpdateConnectionState(handle, DN_CONNECTION_STATE_QUERY);
		ret = pgxc_node_receive(1, &handle, &timeout);
		if (DNStatus_ERR == ret)
		{			
			elog(LOG, "pgxc_print_pending_data pgxc_node_receive LEFT_OVER data ERROR");
			break;
		}
		else if (DNStatus_OK == ret)
		{
			elog(LOG, "pgxc_print_pending_data pgxc_node_receive LEFT_OVER data succeed");
		}
		else
		{
			elog(LOG, "pgxc_print_pending_data pgxc_node_receive LEFT_OVER data timeout");
		}
		
		
		/* No data available, exit */
		if (!HAS_MESSAGE_BUFFERED(handle))
		{
			elog(LOG, "pgxc_print_pending_data pgxc_node_receive LEFT_OVER data finished");
			break;
		}					

		/* TODO handle other possible responses */
		msg_type = get_message(handle, &msg_len, &msg);
		switch (msg_type)
		{
			case '\0':			/* Not enough data in the buffer */
				goto DONE;
			case 'c':			/* CopyToCommandComplete */
				elog(LOG, "LEFT_OVER CopyToCommandComplete found");
				break;
			case 'C':			/* CommandComplete */
				elog(LOG, "LEFT_OVER CommandComplete found");
				break;
			case 'T':			/* RowDescription */
				elog(LOG, "LEFT_OVER RowDescription found");
				break;
			case 'D':			/* DataRow */
				elog(LOG, "LEFT_OVER DataRow found");
				break;
			case 's':			/* PortalSuspended */
				elog(LOG, "LEFT_OVER PortalSuspended found");
				break;
			case '1': /* ParseComplete */
			case '2': /* BindComplete */
			case '3': /* CloseComplete */
			case 'n': /* NoData */
				/* simple notifications, continue reading */
				break;
			case 'G': /* CopyInResponse */
				elog(LOG, "LEFT_OVER CopyInResponse found");
				break;
			case 'H': /* CopyOutResponse */
				elog(LOG, "LEFT_OVER CopyOutResponse found");
				break;
			case 'd': /* CopyOutDataRow */							
				elog(LOG, "LEFT_OVER CopyOutDataRow found");
				break;
			case 'E':			/* ErrorResponse */
				elog(LOG, "LEFT_OVER ErrorResponse found");
				break;
			case 'A':			/* NotificationResponse */
			case 'N':			/* NoticeResponse */
			case 'S':			/* SetCommandComplete */
				/*
				 * Ignore these to prevent multiple messages, one from each
				 * node. Coordinator will send one for DDL anyway
				 */
				break;
			case 'Z':			/* ReadyForQuery */
			{
				elog(LOG, "LEFT_OVER ReadyForQuery found");
				break;
			}						

			case 'Y':			/* ReadyForQuery */
			{
				elog(LOG, "LEFT_OVER ReadyForQuery found");
				break;

			}
			case 'M':			/* Command Id */
				elog(LOG, "LEFT_OVER Command Id found");
				break;
			case 'b':
				elog(LOG, "LEFT_OVER DN_CONNECTION_STATE_IDLE found");
				break;
				
			case 'I':			/* EmptyQuery */
				elog(LOG, "LEFT_OVER EmptyQuery found");
				break;
			case 'W':
				elog(LOG, "LEFT_OVER W found");
				break;
			case 'x':
				elog(LOG, "LEFT_OVER RESPONSE_ASSIGN_GXID found");
				break;
			default:
				elog(LOG, "LEFT_OVER invalid status found");
				break;
		}
	}
	
DONE:	
	UpdateConnectionState(handle, DN_CONNECTION_STATE_IDLE);
	SetTxnState(handle, TXN_STATE_IDLE);
	handle->error[0] = '\0';
	
	/* reset the status */
	if (reset)
	{
		if (handle->error[0])
		{
			elog(LOG, "pgxc_print_pending_data LEFT_OVER errmsg:%s", handle->error);
			handle->error[0] = '\0';
		}
		handle->outEnd = 0;
		handle->inStart = 0;
		handle->inEnd = 0;
		handle->inCursor = 0;
		handle->needSync = false;
	}
}

/*
 * Is there any data enqueued in the TCP input buffer waiting
 * to be read sent by the PGXC node connection
 */

int
pgxc_node_is_data_enqueued(PGXCNodeHandle *conn)
{
	int ret;
	int enqueued;

	if (conn->sock < 0)
		return 0;
	ret = ioctl(conn->sock, FIONREAD, &enqueued);
	if (ret != 0)
		return 0;

	return enqueued;
}

/*
 * Read up incoming messages from the PGXC node connection
 */
int
pgxc_node_read_data(PGXCNodeHandle *conn, bool close_if_error)
{
	int			someread = 0;
	int			nread;

	if (conn->sock < 0)
	{
		if (close_if_error)
			add_error_message(conn, "bad socket");
		return EOF;
	}

	/* Left-justify any data in the buffer to make room */
	if (conn->inStart < conn->inEnd)
	{
		if (conn->inStart > 0)
		{
			memmove(conn->inBuffer, conn->inBuffer + conn->inStart,
					conn->inEnd - conn->inStart);
			conn->inEnd -= conn->inStart;
			conn->inCursor -= conn->inStart;
			conn->inStart = 0;
		}
	}
	else
	{
		/* buffer is logically empty, reset it */
		conn->inStart = conn->inCursor = conn->inEnd = 0;
	}

	/*
	 * If the buffer is fairly full, enlarge it. We need to be able to enlarge
	 * the buffer in case a single message exceeds the initial buffer size. We
	 * enlarge before filling the buffer entirely so as to avoid asking the
	 * kernel for a partial packet. The magic constant here should be large
	 * enough for a TCP packet or Unix pipe bufferload.  8K is the usual pipe
	 * buffer size, so...
	 */
	if (conn->inSize - conn->inEnd < 8192)
	{
		if (ensure_in_buffer_capacity(conn->inEnd + (size_t) 8192, conn) != 0)
		{
			/*
			 * We don't insist that the enlarge worked, but we need some room
			 */
			if (conn->inSize - conn->inEnd < 100)
			{
				if (close_if_error)
					add_error_message(conn, "can not allocate buffer");
				return -1;
			}
		}
	}

retry:
	nread = recv(conn->sock, conn->inBuffer + conn->inEnd,
				 conn->inSize - conn->inEnd, 0);

	if (nread < 0)
	{
		if (errno == EINTR)
			goto retry;
		/* Some systems return EAGAIN/EWOULDBLOCK for no data */
#ifdef EAGAIN
		if (errno == EAGAIN)
			return someread;
#endif
#if defined(EWOULDBLOCK) && (!defined(EAGAIN) || (EWOULDBLOCK != EAGAIN))
		if (errno == EWOULDBLOCK)
			return someread;
#endif
		/* We might get ECONNRESET here if using TCP and backend died */
#ifdef ECONNRESET
		if (errno == ECONNRESET)
		{
			/*
			 * OK, we are getting a zero read even though select() says ready. This
			 * means the connection has been closed.  Cope.
			 */
			if (close_if_error)
			{
				add_error_message(conn,
								"Datanode closed the connection unexpectedly\n"
					"\tThis probably means the Datanode terminated abnormally\n"
								"\tbefore or while processing the request.\n");
				elog(DEBUG5, "pgxc_node_read_data, fatal_conn=%p, fatal_conn->nodename=%s, fatal_conn->sock=%d, "
					"fatal_conn->read_only=%d, fatal_conn->state.transaction_state=%c, "
					"fatal_conn->sock_fatal_occurred=%d, conn->backend_pid=%d, fatal_conn->error=%s", 
					conn, conn->nodename, conn->sock, conn->read_only, conn->state.transaction_state,
					conn->sock_fatal_occurred, conn->backend_pid,  conn->error);
				DestroyHandle(conn);
			}
			return -1;
		}
#endif

#ifdef ETIMEDOUT
		if (errno == ETIMEDOUT)
		{
			elog(WARNING, "TCP is timeout unexpectedly, node: %s, port: %d, backend_pid: %d",
				 conn->nodename, conn->nodeport, conn->backend_pid);
	
			if (close_if_error)
			{
				add_error_message(conn,
								"The TCP connection is timeout unexpectedly, the connection was closed.");
				DestroyHandle(conn);
			}
			return -1;
		}
#endif
		if (close_if_error)
		{
			char msg[MAX_ERROR_MSG_LENGTH];

			snprintf(msg, MAX_ERROR_MSG_LENGTH, "could not receive data from server, fd:%d local_pid:%d remote_node:%s host:%s remote_pid:%d, ",
					conn->sock, MyProcPid, conn->nodename, conn->nodehost, conn->backend_pid);
			add_error_message(conn, msg);
			DestroyHandle(conn);
		}
		return -1;

	}

	if (nread > 0)
	{
		conn->inEnd += nread;

		/*
		 * Hack to deal with the fact that some kernels will only give us back
		 * 1 packet per recv() call, even if we asked for more and there is
		 * more available.	If it looks like we are reading a long message,
		 * loop back to recv() again immediately, until we run out of data or
		 * buffer space.  Without this, the block-and-restart behavior of
		 * libpq's higher levels leads to O(N^2) performance on long messages.
		 *
		 * Since we left-justified the data above, conn->inEnd gives the
		 * amount of data already read in the current message.	We consider
		 * the message "long" once we have acquired 32k ...
		 */
		if (conn->inEnd > 32768 &&
			(conn->inSize - conn->inEnd) >= 8192)
		{
			someread = 1;
			goto retry;
		}
		if (unlikely(enable_pgxcnode_message))
		{
			pgxc_node_dump_message(conn, conn->inBuffer, conn->inEnd, false);
		}
		return 1;
	}

	if (nread == 0)
	{
		elog(LOG, "execute recv() on connection %s pid %d returns 0", conn->nodename, conn->backend_pid);
		if (close_if_error)
		{
			elog(DEBUG1, "nread returned 0, p: %p, size: %zu", conn->inBuffer + conn->inEnd, conn->inSize - conn->inEnd);
			DestroyHandle(conn);
		}
		return EOF;
	}

	return 0;
}


/*
 * Get one character from the connection buffer and advance cursor
 */
static int
get_char(PGXCNodeHandle * conn, char *out)
{
	if (conn->inCursor < conn->inEnd)
	{
		*out = conn->inBuffer[conn->inCursor++];
		return 0;
	}
	return EOF;
}

/*
 * Read an integer from the connection buffer and advance cursor
 */
static int
get_int(PGXCNodeHandle *conn, size_t len, int *out)
{
	unsigned short tmp2;
	unsigned int tmp4;

	if (conn->inCursor + len > conn->inEnd)
		return EOF;

	switch (len)
	{
		case 2:
			memcpy(&tmp2, conn->inBuffer + conn->inCursor, 2);
			conn->inCursor += 2;
			*out = (int) pg_ntoh16(tmp2);
			break;
		case 4:
			memcpy(&tmp4, conn->inBuffer + conn->inCursor, 4);
			conn->inCursor += 4;
			*out = (int) pg_ntoh32(tmp4);
			break;
		default:
			add_error_message(conn, "not supported int size");
			return EOF;
	}

	return 0;
}


/*
 * get_message
 * If connection has enough data read entire message from the connection buffer
 * and returns message type. Message data and data length are returned as
 * var parameters.
 * If buffer does not have enough data leaves cursor unchanged, changes
 * connection status to DN_CONNECTION_STATE_QUERY indicating it needs to
 * receive more and returns \0
 * conn - connection to read from
 * len - returned length of the data where msg is pointing to
 * msg - returns pointer to memory in the incoming buffer. The buffer probably
 * will be overwritten upon next receive, so if caller wants to refer it later
 * it should make a copy.
 */
char
get_message(PGXCNodeHandle *conn, int *len, char **msg)
{
	char 		msgtype;

	if (get_char(conn, &msgtype) || get_int(conn, 4, len))
	{
		/* Successful get_char would move cursor, restore position */
		conn->inCursor = conn->inStart;
		return '\0';
	}

	*len -= 4;

	if (conn->inCursor + *len > conn->inEnd)
	{
		/*
		 * Not enough data in the buffer, we should read more.
		 * Reading function will discard already consumed data in the buffer
		 * till conn->inBegin. Then we want the message that is partly in the
		 * buffer now has been read completely, to avoid extra read/handle
		 * cycles. The space needed is 1 byte for message type, 4 bytes for
		 * message length and message itself which size is currently in *len.
		 * The buffer may already be large enough, in this case the function
		 * ensure_in_buffer_capacity() will immediately return
		 */
		ensure_in_buffer_capacity(5 + (size_t) *len, conn);
		conn->inCursor = conn->inStart;
		return '\0';
	}

	*msg = conn->inBuffer + conn->inCursor;
	conn->inCursor += *len;
	conn->inStart = conn->inCursor;
	return msgtype;
}


/*
 * Release all Datanode and Coordinator connections
 * back to pool and release occupied memory
 */
void
release_handles(PGXCNodeAllHandles *handles, bool all_handles)
{
	bool	destroy = false;
	int		i;
	int		nbytes = 0;
	int		*release_dn_list = NULL;
	int		*release_dn_pid_list = NULL;
	int		*release_co_list = NULL;
	int		release_dn_count = 0;
	int		release_co_count = 0;

	Assert(IS_PGXC_COORDINATOR);

	if (!handles)
		return;

	if (handles->co_conn_count == 0 && handles->dn_conn_count == 0)
		return;

	if (!all_handles)
	{
		release_dn_list = (int*)palloc(sizeof(int) * handles->dn_conn_count);
		release_dn_pid_list = (int*)palloc(sizeof(int) * handles->dn_conn_count);
		release_co_list = (int*)palloc(sizeof(int) * handles->co_conn_count);
	}

	/* Free Datanodes handles */
	for (i = 0; i < handles->dn_conn_count; i++)
	{
		PGXCNodeHandle *handle = handles->datanode_handles[i];

		if (!(IsConnectionStateIdle(handle) && IsTxnStateIdle(handle)))
		{
			destroy = true;
			elog(LOG, "Connection to Datanode %s pid %d is in an unexpected state %s (txn: %c) and will be dropped, "
					  "fd:%d backend_pid:%d fid:%d level:%d, sock: %d",
				 handle->nodename, handle->nodeoid, GetConnectionStateTag(handle), GetTxnState(handle),
				 handle->sock, handle->backend_pid, handle->fid, handle->fragmentlevel, handle->sock);
		}

		if (handle->sock != NO_SOCKET)
		{
			if (enable_distri_print)
				elog(LOG, "release_handles release a connection with datanode %s "
						  "remote backend PID %d, fd:%d fid:%d level:%d  ",
						handle->nodename, (int) handle->backend_pid,
						handle->sock, handle->fid, handle->fragmentlevel);
			pgxc_node_free(handle);

			/* if Session hold connection releases, issue fatal */
			if (handle->holdFlag == SessionHold)
				elog(FATAL, "releasing session hold handle %s pid %d", handle->nodename, handle->backend_pid);
		}

#ifndef __USE_GLOBAL_SNAPSHOT__
		handle->sendGxidVersion = 0;
#endif
#ifdef __OPENTENBASE_C__
		handle->fid = InvalidFid;
		handle->fragmentlevel = 0;
		SetHandleUsed(handle, false);
		handle->sock_fatal_occurred = false;
		handle->current_user = InvalidOid;
		handle->combiner = NULL;
		handle->session_id = InvalidSessionId;
		handle->block_connection = false;
#endif
		nbytes = pgxc_node_is_data_enqueued(handle);
		if (nbytes)
		{
			elog(PANIC, "Connection to Datanode %s has data %d pending",
					 handle->nodename, nbytes);
		}

		if (!all_handles)
		{
			release_dn_list[release_dn_count] = PGXCNodeGetNodeId(handle->nodeoid, NULL);
			release_dn_pid_list[release_dn_count] = handle->backend_pid;
			release_dn_count++;
		}
	}

	/* Collect Coordinator handles */
	for (i = 0; i < handles->co_conn_count; i++)
	{
		PGXCNodeHandle *handle = handles->coord_handles[i];

		if (!(IsConnectionStateIdle(handle) && IsTxnStateIdle(handle)))
		{
			destroy = true;
			elog(LOG, "Connection to Coordinator %s pid %d has unexpected state %s (txn: %c) and will be force dropped, "
					  "fd:%d backend_pid:%d fid:%d level:%d, sock: %d",
				 handle->nodename, handle->nodeoid, GetConnectionStateTag(handle), GetTxnState(handle),
				 handle->sock, handle->backend_pid, handle->fid, handle->fragmentlevel, handle->sock);
		}

		if (handle->sock != NO_SOCKET)
		{
			if (enable_distri_print)
				elog(LOG, "release_handles release a connection with coordinator %s "
						  "remote backend PID %d, fd:%d fid:%d level:%d  ",
					 handle->nodename, (int)handle->backend_pid,
					 handle->sock, handle->fid, handle->fragmentlevel);

			pgxc_node_free(handle);

			/* if Session hold connection releases, issue fatal */
			if (handle->holdFlag == SessionHold)
				elog(FATAL, "releasing session hold handle %s pid %d", handle->nodename, handle->backend_pid);
		}
#ifndef __USE_GLOBAL_SNAPSHOT__
		handle->sendGxidVersion = 0;
#endif
#ifdef __OPENTENBASE_C__
		handle->fid = InvalidFid;
		handle->fragmentlevel = 0;
		SetHandleUsed(handle, false);
		handle->sock_fatal_occurred = false;
		handle->current_user = InvalidOid;
		handle->combiner = NULL;
		handle->session_id = InvalidSessionId;
		handle->block_connection = false;
#endif
		nbytes = pgxc_node_is_data_enqueued(handle);
		if (nbytes)
		{
			elog(PANIC, "Connection to Datanode %s has data %d pending",
				 handle->nodename, nbytes);
		}

		if (!all_handles)
			release_co_list[release_co_count++] = PGXCNodeGetNodeId(handle->nodeoid, NULL);
	}

	/* And finally release the connections on pooler */
	if (all_handles)
		PoolManagerReleaseAllConnections(destroy);
	else
	{
		PoolManagerReleaseConnections(release_dn_count, release_dn_list, release_dn_pid_list,
										release_co_count, release_co_list, destroy);
		pfree(release_dn_list);
		pfree(release_dn_pid_list);
		pfree(release_co_list);
	}
}

/*
 * Check whether there bad connections to remote nodes when abort transactions.
 */
bool
validate_handles(void)
{
	int			i, j;

	/* Free Datanodes handles */
	for (i = 0; i < NumDataNodes; i++)
	{
		for (j = 0; j < max_handles; j++)
		{
			PGXCNodeHandle *handle = &dn_handles[i][j];

			if (handle->sock != NO_SOCKET)
			{
				/*
				 * Connections at this point should be completely inactive,
				 * otherwise abaandon them. We can not allow not cleaned up
				 * connection is returned to pool.
				 */
				if (IsConnectionStateFatal(handle))
				{
					elog(LOG, "Remote node \"%s\", running with pid %d state:%d is bad",
								handle->nodename, handle->backend_pid, GetConnectionState(handle));
					return true;
				}				
			}
		}
	}

	if (IS_PGXC_COORDINATOR)
	{
		/* Collect Coordinator handles */
		for (i = 0; i < NumCoords; i++)
		{
			for (j = 0; j < max_handles; j++)
			{
				PGXCNodeHandle *handle = &co_handles[i][j];

				if (handle->sock != NO_SOCKET)
				{
					/*
					 * Connections at this point should be completely inactive,
					 * otherwise abandon them. We can not allow not cleaned up
					 * connection is returned to pool.
					 */
					if (IsConnectionStateFatal(handle))
					{
						elog(LOG, "Remote node \"%s\", running with pid %d state:%d is bad",
								handle->nodename, handle->backend_pid, GetConnectionState(handle));
						return true;
					}
				}
			}
		}
	}
	return false;
}

/*
 * Ensure that the supplied buffer has enough capacity and if not, it's
 * extended to an appropriate size.
 *
 * currbuf is the currently used buffer of currsize. bytes_needed is the
 * minimum size required. We shall return the new buffer, if allocated
 * successfully and set newsize_p to contain the size of the repalloced buffer.
 * If allocation fails, NULL is returned.
 *
 * The function checks for requests beyond MaxAllocSize and throw an error.
 */
static char *
ensure_buffer_capacity(char *currbuf, size_t currsize, size_t bytes_needed, size_t *newsize_p)
{
	char	   *newbuf;
	Size		newsize = (Size) currsize;

	if (((Size) bytes_needed) >= MaxAllocSize)
		return NULL;

	if (bytes_needed <= newsize)
	{
		*newsize_p = currsize;
		return currbuf;
	}

	/*
	 * The current size of the buffer should never be zero (init_pgxc_handle
	 * guarantees that.
	 */
	Assert(newsize > 0);

	/*
	 * Double the buffer size until we have enough space to hold bytes_needed
	 */
	while (bytes_needed > newsize)
		newsize = 2 * newsize;

	/*
	 * Clamp to MaxAllocSize in case we went past it.  Note we are assuming
	 * here that MaxAllocSize <= INT_MAX/2, else the above loop could
	 * overflow.  We will still have newsize >= bytes_needed.
	 */
	if (newsize > (int) MaxAllocSize)
		newsize = (int) MaxAllocSize;

	newbuf = repalloc(currbuf, newsize);
	if (newbuf)
	{
		/* repalloc succeeded, set new size and return the buffer */
		*newsize_p = newsize;
		return newbuf;
	}

	/*
	 * If we fail to double the buffer, try to repalloc a buffer of the given
	 * size, rounded to the next multiple of 8192 and see if that works.
	 */
	newsize = bytes_needed;
	newsize = ((bytes_needed / 8192) + 1) * 8192;

	newbuf = repalloc(currbuf, newsize);
	if (newbuf)
	{
		/* repalloc succeeded, set new size and return the buffer */
		*newsize_p = newsize;
		return newbuf;
	}

	/* repalloc failed */
	return NULL;
}

/*
 * Ensure specified amount of data can fit to the incoming buffer and
 * increase it if necessary
 */
int
ensure_in_buffer_capacity(size_t bytes_needed, PGXCNodeHandle *handle)
{
	size_t newsize;
	char *newbuf = ensure_buffer_capacity(handle->inBuffer, handle->inSize,
			bytes_needed, &newsize);
	if (newbuf)
	{
		handle->inBuffer = newbuf;
		handle->inSize = newsize;
		return 0;
	}
	return EOF;
}

/*
 * Ensure specified amount of data can fit to the outgoing buffer and
 * increase it if necessary
 */
int
ensure_out_buffer_capacity(size_t bytes_needed, PGXCNodeHandle *handle)
{
	size_t newsize = 0;
	char *newbuf = ensure_buffer_capacity(handle->outBuffer, handle->outSize,
			bytes_needed, &newsize);
#ifdef __TWO_PHASE_TESTS__
    if ((IN_REMOTE_PREPARE == twophase_in &&
            ((PART_PREPARE_SEND_TIMESTAMP == twophase_exception_case && 
                SEND_PREPARE_TIMESTAMP == capacity_stack) || 
                (PART_PREPARE_SEND_STARTER == twophase_exception_case && 
                    SEND_STARTER == capacity_stack) ||
                (PART_PREPARE_SEND_STARTXID == twophase_exception_case && 
                    SEND_STARTXID == capacity_stack) ||
                (PART_PREPARE_SEND_PARTNODES == twophase_exception_case && 
                    SEND_PARTNODES == capacity_stack) ||
                (PART_PREPARE_SEND_QUERY == twophase_exception_case && 
                    SEND_QUERY == capacity_stack))) ||
        (IN_PREPARE_ERROR == twophase_in && 
            PREPARE_ERROR_SEND_QUERY == twophase_exception_case &&
                SEND_QUERY == capacity_stack) ||
        (IN_REMOTE_ABORT == twophase_in &&
            PART_ABORT_SEND_ROLLBACK == twophase_exception_case &&
                SEND_ROLLBACK == capacity_stack) ||
        (IN_REMOTE_FINISH == twophase_in &&
            ((PART_COMMIT_SEND_TIMESTAMP == twophase_exception_case &&
                SEND_COMMIT_TIMESTAMP == capacity_stack) ||
                (PART_COMMIT_SEND_QUERY == twophase_exception_case &&
                    SEND_QUERY == capacity_stack))))
    {
        exception_count++;
        if (2 == exception_count)
        {
            complish = true;
            if (IN_REMOTE_FINISH == twophase_in &&
                ((PART_COMMIT_SEND_TIMESTAMP == twophase_exception_case &&
                    SEND_COMMIT_TIMESTAMP == capacity_stack) ||
                    (PART_COMMIT_SEND_QUERY == twophase_exception_case &&
                        SEND_QUERY == capacity_stack)))
            {
                run_pg_clean = 0;
            }
            if (complish && run_pg_clean)
            {
                elog(ERROR,"complete test case");
            }
            else
            {
                newbuf = NULL;
            }
        }
        capacity_stack = 0;
    }
    if (IN_PG_CLEAN == twophase_in && 
        ((PG_CLEAN_SEND_CLEAN == twophase_exception_case &&
            SEND_PGCLEAN == capacity_stack) || 
            (PG_CLEAN_SEND_READONLY == twophase_exception_case &&
                SEND_READONLY == capacity_stack) ||
            (PG_CLEAN_SEND_AFTER_PREPARE == twophase_exception_case &&
                SEND_AFTER_PREPARE == capacity_stack) ||
            (PG_CLEAN_SEND_TIMESTAMP == twophase_exception_case &&
                SEND_COMMIT_TIMESTAMP == capacity_stack) ||
            (PG_CLEAN_SEND_STARTER == twophase_exception_case &&
                SEND_STARTER == capacity_stack) ||
            (PG_CLEAN_SEND_STARTXID == twophase_exception_case &&
                SEND_STARTXID == capacity_stack) ||
            (PG_CLEAN_SEND_PARTNODES == twophase_exception_case &&
                SEND_PARTNODES == capacity_stack) ||
            (PG_CLEAN_SEND_QUERY == twophase_exception_case &&
                SEND_QUERY == capacity_stack)))
    {
        if (2 == exception_count)
        {
            newbuf = NULL;
            sleep(3);
        }
    }
#endif
	if (newbuf)
	{
		handle->outBuffer = newbuf;
		handle->outSize = newsize;
		return 0;
	}
	return EOF;
}


/*
 * Send specified amount of data from the outgoing buffer over the connection
 */
int
send_some(PGXCNodeHandle *handle, int len)
{
	char	   *ptr = handle->outBuffer;
	int			remaining = handle->outEnd;
	int			result = 0;


	if (unlikely(enable_pgxcnode_message))
	{
		pgxc_node_dump_message(handle, ptr, len, true);
	}

	/* while there's still data to send */
	while (len > 0)
	{
		int			sent;

#ifndef WIN32
		sent = send(handle->sock, ptr, len, 0);
#else
		/*
		 * Windows can fail on large sends, per KB article Q201213. The failure-point
		 * appears to be different in different versions of Windows, but 64k should
		 * always be safe.
		 */
		sent = send(handle->sock, ptr, Min(len, 65536), 0);
#endif

		if (sent < 0)
		{
			/*
			 * Anything except EAGAIN/EWOULDBLOCK/EINTR is trouble. If it's
			 * EPIPE or ECONNRESET, assume we've lost the backend connection
			 * permanently.
			 */
			switch (errno)
			{
#ifdef EAGAIN
				case EAGAIN:
					break;
#endif
#if defined(EWOULDBLOCK) && (!defined(EAGAIN) || (EWOULDBLOCK != EAGAIN))
				case EWOULDBLOCK:
					break;
#endif
				case EINTR:
					continue;

				case EPIPE:
#ifdef ECONNRESET
				case ECONNRESET:
#endif
					add_error_message(handle, "server closed the connection unexpectedly\n"
					"\tThis probably means the server terminated abnormally\n"
							  "\tbefore or while processing the request.\n");
					UpdateConnectionState(handle,
							DN_CONNECTION_STATE_FATAL);
					/*
					 * We used to close the socket here, but that's a bad idea
					 * since there might be unread data waiting (typically, a
					 * NOTICE message from the backend telling us it's
					 * committing hara-kiri...).  Leave the socket open until
					 * pqReadData finds no more data can be read.  But abandon
					 * attempt to send data.
					 */
					handle->outEnd = 0;
					return -1;

				default:
					add_error_message(handle, "could not send data to server");
					/* We don't assume it's a fatal error... */
					handle->outEnd = 0;
					return -1;
			}
		}
		else
		{
			ptr += sent;
			len -= sent;
			remaining -= sent;
		}

		if (len > 0)
		{
			struct pollfd pool_fd;
			int poll_ret;

			/*
			 * Wait for the socket to become ready again to receive more data.
			 * For some cases, especially while writing large sums of data
			 * during COPY protocol and when the remote node is not capable of
			 * handling data at the same speed, we might otherwise go in a
			 * useless tight loop, consuming all available local resources
			 *
			 * Use a small timeout of 1s to avoid infinite wait
			 */
			pool_fd.fd = handle->sock;
			pool_fd.events = POLLOUT;

			poll_ret = poll(&pool_fd, 1, 1000);
			if (poll_ret < 0)
			{
				if (errno == EAGAIN || errno == EINTR)
					continue;
				else
				{
					char msg[MAX_ERROR_MSG_LENGTH];

					snprintf(msg, MAX_ERROR_MSG_LENGTH, "remote node %s pid %d poll failed, ",
							 handle->nodename, handle->backend_pid);
					add_error_message(handle, msg);
					handle->outEnd = 0;
					return -1;
				}
			}
			else if (poll_ret == 1)
			{
				if (pool_fd.revents & POLLHUP)
				{
					char msg[MAX_ERROR_MSG_LENGTH];

					snprintf(msg, MAX_ERROR_MSG_LENGTH, "remote node end disconnected fid:%d level:%d ftype:%d "
							"fd:%d local_pid:%d remote_node:%s host:%s remote_pid:%d, ",
							handle->fid, handle->fragmentlevel, handle->ftype,
							handle->sock, MyProcPid, handle->nodename, handle->nodehost, handle->backend_pid);

					add_error_message(handle, msg);
					handle->outEnd = 0;
					return -1;
				}
			}
		}
	}

	/* shift the remaining contents of the buffer */
	if (remaining > 0)
		memmove(handle->outBuffer, ptr, remaining);
	handle->outEnd = remaining;

	return result;
}

int
pgxc_node_send_msg(PGXCNodeHandle *handle, char *ptr, int len)
{
	int			result = 0;


	if (unlikely(enable_pgxcnode_message))
	{
		pgxc_node_dump_message(handle, ptr, len, true);
	}

	/* while there's still data to send */
	while (len > 0)
	{
		int			sent;

#ifndef WIN32
		sent = send(handle->sock, ptr, len, 0);
#else
		/*
		 * Windows can fail on large sends, per KB article Q201213. The failure-point
		 * appears to be different in different versions of Windows, but 64k should
		 * always be safe.
		 */
		sent = send(handle->sock, ptr, Min(len, 65536), 0);
#endif

		if (sent < 0)
		{
			/*
			 * Anything except EAGAIN/EWOULDBLOCK/EINTR is trouble. If it's
			 * EPIPE or ECONNRESET, assume we've lost the backend connection
			 * permanently.
			 */
			switch (errno)
			{
#ifdef EAGAIN
				case EAGAIN:
					break;
#endif
#if defined(EWOULDBLOCK) && (!defined(EAGAIN) || (EWOULDBLOCK != EAGAIN))
				case EWOULDBLOCK:
					break;
#endif
				case EINTR:
					continue;

				case EPIPE:
#ifdef ECONNRESET
				case ECONNRESET:
#endif
					add_error_message(handle, "server closed the connection unexpectedly\n"
					"\tThis probably means the server terminated abnormally\n"
							  "\tbefore or while processing the request.\n");
					UpdateConnectionState(handle, DN_CONNECTION_STATE_FATAL);
					
					return -1;

				default:
					add_error_message(handle, "could not send data to server");
					return -1;
			}
		}
		else
		{
			ptr += sent;
			len -= sent;
		}

		if (len > 0)
		{
			struct pollfd pool_fd;
			int poll_ret;

			/*
			 * Wait for the socket to become ready again to receive more data.
			 * For some cases, especially while writing large sums of data
			 * during COPY protocol and when the remote node is not capable of
			 * handling data at the same speed, we might otherwise go in a
			 * useless tight loop, consuming all available local resources
			 *
			 * Use a small timeout of 1s to avoid infinite wait
			 */
			pool_fd.fd = handle->sock;
			pool_fd.events = POLLOUT;

			poll_ret = poll(&pool_fd, 1, 1000);
			if (poll_ret < 0)
			{
				if (errno == EAGAIN || errno == EINTR)
					continue;
				else
				{
					char msg[MAX_ERROR_MSG_LENGTH];

					snprintf(msg, MAX_ERROR_MSG_LENGTH, "remote node %s pid %d poll failed, ",
							 handle->nodename, handle->backend_pid);
					add_error_message(handle, msg);
					return -1;
				}
			}
			else if (poll_ret == 1)
			{
				if (pool_fd.revents & POLLHUP)
				{
					char msg[MAX_ERROR_MSG_LENGTH];

					snprintf(msg, MAX_ERROR_MSG_LENGTH, "remote node end disconnected fid:%d level:%d ftype:%d "
							"fd:%d local_pid:%d remote_node:%s host:%s remote_pid:%d, ",
							handle->fid, handle->fragmentlevel, handle->ftype,
							handle->sock, MyProcPid, handle->nodename, handle->nodehost, handle->backend_pid);

					add_error_message(handle, msg);
					return -1;
				}
			}
		}
	}

	return result;
}
/*
 * Send PARSE message with specified statement down to the Datanode
 */
int
pgxc_node_send_parse(PGXCNodeHandle * handle, const char* statement,
					 const char *query, short num_params, Oid *param_types)
{
	/* statement name size (allow NULL) */
	int			stmtLen = statement ? strlen(statement) + 1 : 1;
	/* size of query string */
	int			strLen = strlen(query) + 1;
	char 		**paramTypes = (char **)palloc(sizeof(char *) * num_params);
	/* total size of parameter type names */
	int 		paramTypeLen;
	/* message length */
	int			msgLen;
	int			cnt_params;
#ifdef USE_ASSERT_CHECKING
	size_t		old_outEnd = handle->outEnd;
#endif

	/* if there are parameters, param_types should exist */
	Assert(num_params <= 0 || param_types);
	/* 2 bytes for number of parameters, preceding the type names */
	paramTypeLen = 2;
	/* find names of the types of parameters */
	for (cnt_params = 0; cnt_params < num_params; cnt_params++)
	{
		Oid typeoid;

		/* Parameters with no types are simply ignored */
		if (OidIsValid(param_types[cnt_params]))
			typeoid = param_types[cnt_params];
		else
			typeoid = INT4OID;

		paramTypes[cnt_params] = format_type_be(typeoid);
		paramTypeLen += strlen(paramTypes[cnt_params]) + 1;
	}

	/* size + stmtLen + strlen + paramTypeLen */
	msgLen = 4 + stmtLen + strLen + paramTypeLen;

	/* msgType + msgLen */
	if (ensure_out_buffer_capacity(handle->outEnd + 1 + msgLen, handle) != 0)
	{
		add_error_message(handle, "out of memory");
		return EOF;
	}

	handle->outBuffer[handle->outEnd++] = 'P';
	/* size */
	msgLen = pg_hton32(msgLen);
	memcpy(handle->outBuffer + handle->outEnd, &msgLen, 4);
	handle->outEnd += 4;

	/* statement name */
	if (statement)
	{
		memcpy(handle->outBuffer + handle->outEnd, statement, stmtLen);
		handle->outEnd += stmtLen;
	}
	else
		handle->outBuffer[handle->outEnd++] = '\0';
	/* query */
	memcpy(handle->outBuffer + handle->outEnd, query, strLen);
	handle->outEnd += strLen;
	/* parameter types */
	Assert(sizeof(num_params) == 2);
	*((short *)(handle->outBuffer + handle->outEnd)) = pg_hton16(num_params);
	handle->outEnd += sizeof(num_params);
	/*
	 * instead of parameter ids we should send parameter names (qualified by
	 * schema name if required). The OIDs of types can be different on
	 * Datanodes.
	 */
	for (cnt_params = 0; cnt_params < num_params; cnt_params++)
	{
		memcpy(handle->outBuffer + handle->outEnd, paramTypes[cnt_params],
					strlen(paramTypes[cnt_params]) + 1);
		handle->outEnd += strlen(paramTypes[cnt_params]) + 1;
		pfree(paramTypes[cnt_params]);
	}
	pfree(paramTypes);

	Assert(old_outEnd + pg_ntoh32(msgLen) + 1 == handle->outEnd);

 	return 0;
}

/*
 * Send PLAN message down to the Data node
 */
int
pgxc_node_send_plan(PGXCNodeHandle * handle, const char *statement,
					const char *query, const char *planstr,
					int num_params, Oid *param_types, int instrument_options)
{
	int			stmtLen;
	int			queryLen;
	int			planLen;
	int 		paramTypeLen;
	int			msgLen;
	char	  **paramTypes = (char **)palloc(sizeof(char *) * num_params);
	int			i;
	int			tmp_num_params;

	/* Invalid connection state, return error */
	if (!IsConnectionStateIdle(handle))
		return EOF;

	/* statement name size (do not allow NULL) */
	stmtLen = strlen(statement) + 1;
	/* source query size (do not allow NULL) */
	queryLen = strlen(query) + 1;
	/* query plan size (do not allow NULL) */
	planLen = strlen(planstr) + 1;
	/* 2 bytes for number of parameters, preceding the type names */
	paramTypeLen = 4;
	/* find names of the types of parameters */
	for (i = 0; i < num_params; i++)
	{
		paramTypes[i] = format_type_be(param_types[i]);
		paramTypeLen += strlen(paramTypes[i]) + 1;
	}
	/* size + pnameLen + queryLen + parameters + instrument_options */
	msgLen = 4 + queryLen + stmtLen + planLen + paramTypeLen + 4;

	/* msgType + msgLen */
	if (ensure_out_buffer_capacity(handle->outEnd + 1 + msgLen, handle) != 0)
	{
		add_error_message(handle, "out of memory");
		return EOF;
	}

	handle->outBuffer[handle->outEnd++] = 'p';
	/* size */
	msgLen = pg_hton32(msgLen);
	memcpy(handle->outBuffer + handle->outEnd, &msgLen, 4);
	handle->outEnd += 4;
	/* statement name */
	memcpy(handle->outBuffer + handle->outEnd, statement, stmtLen);
	handle->outEnd += stmtLen;
	/* source query */
	memcpy(handle->outBuffer + handle->outEnd, query, queryLen);
	handle->outEnd += queryLen;
	/* query plan */
	memcpy(handle->outBuffer + handle->outEnd, planstr, planLen);
	handle->outEnd += planLen;
	/* parameter types */
	tmp_num_params = pg_hton32(num_params);
	memcpy(handle->outBuffer + handle->outEnd, &tmp_num_params, sizeof(tmp_num_params));
	handle->outEnd += sizeof(tmp_num_params);
	/*
	 * instead of parameter ids we should send parameter names (qualified by
	 * schema name if required). The OIDs of types can be different on
	 * datanodes.
	 */
	for (i = 0; i < num_params; i++)
	{
		int plen = strlen(paramTypes[i]) + 1;
		memcpy(handle->outBuffer + handle->outEnd, paramTypes[i], plen);
		handle->outEnd += plen;
		pfree(paramTypes[i]);
	}
	pfree(paramTypes);

#ifdef __OPENTENBASE_C__
	/* instrument_options */
	instrument_options = pg_hton32(instrument_options);
	memcpy(handle->outBuffer + handle->outEnd, &instrument_options, 4);
	handle->outEnd += 4;
#endif

	SetTransitionInExtendedQuery(handle, true);
	RecordSendMessage(handle, 'p', statement, stmtLen);
 	return 0;
}

/*
 * Send BIND message down to the Datanode
 */
int
pgxc_node_send_bind(PGXCNodeHandle * handle, const char *portal,
					const char *statement, int paramlen, char *params,
					int16 *pformats, int n_pformats,
					int16 *rformats, int n_rformats)
{
	int			pnameLen;
	int			stmtLen;
	int 		paramFormatLen;
	int 		paramValueLen;
	int 		paramOutLen;
	int			pFormatLen = 0;
	int			rFormatLen = 0;
	int			msgLen;
	uint16		i;

	/* Invalid connection state, return error */
	if (!IsConnectionStateIdle(handle))
		return EOF;

	if (pgxc_node_send_cmd_seq(handle))
	{
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("fail to send command sequence to node %s pid:%d",
						handle->nodename, handle->backend_pid)));
	}

	/* portal name size (allow NULL) */
	pnameLen = portal ? strlen(portal) + 1 : 1;
	/* statement name size (allow NULL) */
	stmtLen = statement ? strlen(statement) + 1 : 1;
	/* size of parameter format */
	paramFormatLen = 2;
	/* size of parameter values array, 2 if no params */
	paramValueLen = paramlen ? paramlen : 2;
	/* size of output parameter codes array (always empty for now) */
	paramOutLen = 2;
	/* size + pnameLen + stmtLen + parameters */
	msgLen = 4 + pnameLen + stmtLen + paramFormatLen + paramValueLen + paramOutLen;

	if (pformats && n_pformats > 0)
	{
		pFormatLen = n_pformats * sizeof(int16);
		msgLen += pFormatLen;
	}

	if (rformats && n_rformats > 0)
	{
		rFormatLen = n_rformats * sizeof(int16);
		msgLen += rFormatLen;
	}
	/* msgType + msgLen */
	if (ensure_out_buffer_capacity(handle->outEnd + 1 + msgLen, handle) != 0)
	{
		add_error_message(handle, "out of memory");
		return EOF;
	}

	handle->outBuffer[handle->outEnd++] = 'B';
	/* size */
	msgLen = pg_hton32(msgLen);
	memcpy(handle->outBuffer + handle->outEnd, &msgLen, 4);
	handle->outEnd += 4;
	/* portal name */
	if (portal)
	{
		memcpy(handle->outBuffer + handle->outEnd, portal, pnameLen);
		handle->outEnd += pnameLen;
	}
	else
		handle->outBuffer[handle->outEnd++] = '\0';
	/* statement name */
	if (statement)
	{
		memcpy(handle->outBuffer + handle->outEnd, statement, stmtLen);
		handle->outEnd += stmtLen;
	}
	else
		handle->outBuffer[handle->outEnd++] = '\0';
	/* parameter fromat */
	if (pformats && n_pformats > 0)
	{
		int16	*send_formats;
		int		send_nformats;

		send_formats = (int16 *) palloc(pFormatLen);
		send_nformats = htons(n_pformats);

		memcpy(handle->outBuffer + handle->outEnd, &send_nformats, 2);
		handle->outEnd += 2;

		for (i = 0; i < n_pformats ; i++)
		{
			send_formats[i] = htons(pformats[i]);
		}
		memcpy(handle->outBuffer + handle->outEnd, send_formats, pFormatLen);
		handle->outEnd += pFormatLen;
	}
	else
	{
		/* output parameter codes (none) */
		handle->outBuffer[handle->outEnd++] = 0;
		handle->outBuffer[handle->outEnd++] = 0;
	}

	/* parameter values */
	if (paramlen)
	{
		memcpy(handle->outBuffer + handle->outEnd, params, paramlen);
		handle->outEnd += paramlen;
	}
	else
	{
		handle->outBuffer[handle->outEnd++] = 0;
		handle->outBuffer[handle->outEnd++] = 0;
	}
	
	if (rformats && n_rformats > 0)
	{
		int16	*send_formats;
		int		send_nformats;

		send_formats = (int16 *) palloc(rFormatLen);
		send_nformats = htons(n_rformats);

		memcpy(handle->outBuffer + handle->outEnd, &send_nformats, 2);
		handle->outEnd += 2;

		for (i = 0; i < n_rformats ; i++)
		{
			send_formats[i] = htons(rformats[i]);
		}
		memcpy(handle->outBuffer + handle->outEnd, send_formats, rFormatLen);
		handle->outEnd += rFormatLen;
	}
	else
	{
		/* output parameter codes (none) */
		handle->outBuffer[handle->outEnd++] = 0;
		handle->outBuffer[handle->outEnd++] = 0;
	}

	SetTransitionInExtendedQuery(handle, true);
	RecordSendMessage(handle, 'B', statement, stmtLen);
 	return 0;
}


/*
 * Send DESCRIBE message (portal or statement) down to the Datanode
 */
int
pgxc_node_send_describe(PGXCNodeHandle * handle, bool is_statement,
						const char *name)
{
	int			nameLen;
	int			msgLen;

	/* Invalid connection state, return error */
	if (!IsConnectionStateIdle(handle))
		return EOF;

	/* statement or portal name size (allow NULL) */
	nameLen = name ? strlen(name) + 1 : 1;

	/* size + statement/portal + name */
	msgLen = 4 + 1 + nameLen;

	/* msgType + msgLen */
	if (ensure_out_buffer_capacity(handle->outEnd + 1 + msgLen, handle) != 0)
	{
		add_error_message(handle, "out of memory");
		return EOF;
	}

	handle->outBuffer[handle->outEnd++] = 'D';
	/* size */
	msgLen = pg_hton32(msgLen);
	memcpy(handle->outBuffer + handle->outEnd, &msgLen, 4);
	handle->outEnd += 4;
	/* statement/portal flag */
	handle->outBuffer[handle->outEnd++] = is_statement ? 'S' : 'P';
	/* object name */
	if (name)
	{
		memcpy(handle->outBuffer + handle->outEnd, name, nameLen);
		handle->outEnd += nameLen;
	}
	else
		handle->outBuffer[handle->outEnd++] = '\0';

	SetTransitionInExtendedQuery(handle, true);

	RecordSendMessage(handle, 'D', NULL, 0);
 	return 0;
}


/*
 * Send CLOSE message (portal or statement) down to the Datanode
 */
int
pgxc_node_send_close(PGXCNodeHandle * handle, bool is_statement,
					 const char *name)
{
	/* statement or portal name size (allow NULL) */
	int			nameLen = name ? strlen(name) + 1 : 1;

	/* size + statement/portal + name */
	int			msgLen = 4 + 1 + nameLen;

	/* msgType + msgLen */
	if (ensure_out_buffer_capacity(handle->outEnd + 1 + msgLen, handle) != 0)
	{
		add_error_message(handle, "out of memory");
		return EOF;
	}

	handle->outBuffer[handle->outEnd++] = 'C';
	/* size */
	msgLen = pg_hton32(msgLen);
	memcpy(handle->outBuffer + handle->outEnd, &msgLen, 4);
	handle->outEnd += 4;
	/* statement/portal flag */
	handle->outBuffer[handle->outEnd++] = is_statement ? 'S' : 'P';
	/* object name */
	if (name)
	{
		memcpy(handle->outBuffer + handle->outEnd, name, nameLen);
		handle->outEnd += nameLen;
	}
	else
		handle->outBuffer[handle->outEnd++] = '\0';

	SetTransitionInExtendedQuery(handle, true);

	RecordSendMessage(handle, 'C', name?  name: NULL, name? strlen(name): 0);
 	return 0;
}

/*
 * Send EXECUTE message down to the Datanode
 */
int
pgxc_node_send_execute(PGXCNodeHandle * handle, const char *portal, int fetch, bool rewind)
{
	/* portal name size (allow NULL) */
	int			pnameLen = portal ? strlen(portal) + 1 : 1;

	/* size + pnameLen + fetchLen + rewind */
	int			msgLen = 4 + pnameLen + 4 + 1;

	/* msgType + msgLen */
	if (ensure_out_buffer_capacity(handle->outEnd + 1 + msgLen, handle) != 0)
	{
		add_error_message(handle, "out of memory");
		return EOF;
	}

	if (enable_distri_print)
		elog(LOG, "pgxc_node_send_execute %s %d %s %d %d", handle->nodename,
			 handle->backend_pid, portal, fetch, rewind);

	handle->outBuffer[handle->outEnd++] = 'E';
	/* size */
	msgLen = pg_hton32(msgLen);
	memcpy(handle->outBuffer + handle->outEnd, &msgLen, 4);
	handle->outEnd += 4;
	/* portal name */
	if (portal)
	{
		memcpy(handle->outBuffer + handle->outEnd, portal, pnameLen);
		handle->outEnd += pnameLen;
	}
	else
		handle->outBuffer[handle->outEnd++] = '\0';

	/* fetch */
	fetch = pg_hton32(fetch);
	memcpy(handle->outBuffer + handle->outEnd, &fetch, 4);
	handle->outEnd += 4;

	handle->outBuffer[handle->outEnd++] = rewind ? 't' : 'f';

	SetTransitionInExtendedQuery(handle, true);
	RecordSendMessage(handle, 'E', portal?  portal: NULL, portal? strlen(portal): 0);
	return 0;
}


/*
 * Send FLUSH message down to the Datanode
 */
int
pgxc_node_send_flush(PGXCNodeHandle * handle)
{
	/* size */
	int			msgLen = 4;

	/* msgType + msgLen */
	if (ensure_out_buffer_capacity(handle->outEnd + 1 + msgLen, handle) != 0)
	{
		add_error_message(handle, "out of memory");
		return EOF;
	}

	handle->outBuffer[handle->outEnd++] = 'H';
	/* size */
	msgLen = pg_hton32(msgLen);
	memcpy(handle->outBuffer + handle->outEnd, &msgLen, 4);
	handle->outEnd += 4;

	SetTransitionInExtendedQuery(handle, true);

	RecordSendMessage(handle, 'H', NULL, 0);
	return FlushAndStoreState(handle);
}


/*
 * Send SYNC message down to the Datanode
 */
int
pgxc_node_send_sync_internal(PGXCNodeHandle * handle, const char *filename, int lineno)
{
	/* size */
	int			msgLen = 4;

	if (pgxc_node_send_cmd_seq(handle))
	{
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("fail to send command sequence to node %s backend_pid:%d %s:%d",
						handle->nodename, handle->backend_pid, filename, lineno)));
	}

	if (enable_distri_print)
		ereport(LOG,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("Succeed to sync msg to node %s backend_pid:%d %s:%d",
						handle->nodename, handle->backend_pid, filename, lineno)));

	
	/* msgType + msgLen */
	if (ensure_out_buffer_capacity(handle->outEnd + 1 + msgLen, handle) != 0)
	{
		add_error_message(handle, "out of memory");
		return EOF;
	}

	handle->outBuffer[handle->outEnd++] = 'S';
	/* size */
	msgLen = pg_hton32(msgLen);
	memcpy(handle->outBuffer + handle->outEnd, &msgLen, 4);
	handle->outEnd += 4;


	SetTransitionInExtendedQuery(handle, false);
	handle->needSync = false;

	RecordSendMessage(handle, 'S', NULL, 0);
	return FlushAndStoreState(handle);
}

/*
 * Send SYNC message down to the Datanode
 */
int pgxc_node_send_my_sync(PGXCNodeHandle *handle)
{
	/* size */
	int	msgLen = 4;

	/* msgType + msgLen */
	if (ensure_out_buffer_capacity(handle->outEnd + 1 + msgLen, handle) != 0)
	{
		add_error_message(handle, "out of memory");
		return EOF;
	}

	handle->outBuffer[handle->outEnd++] = 'y';
	/* size */
	msgLen = htonl(msgLen);
	memcpy(handle->outBuffer + handle->outEnd, &msgLen, 4);
	handle->outEnd += 4;

	SetTransitionInExtendedQuery(handle, false);
	handle->needSync = false;

	msgLen = 4;
	/* msgType + msgLen */
	if (ensure_out_buffer_capacity(handle->outEnd + 1 + msgLen, handle) != 0)
	{
		add_error_message(handle, "out of memory");
		return EOF;
	}

	handle->outBuffer[handle->outEnd++] = 'H';
	/* size */
	msgLen = htonl(msgLen);
	memcpy(handle->outBuffer + handle->outEnd, &msgLen, 4);
	handle->outEnd += 4;

	SetTransitionInExtendedQuery(handle, true);

	RecordSendMessage(handle, 'y', NULL, 0);
	return SendRequest(handle);
}

#ifdef __SUBSCRIPTION__
/*
 * Send logical apply message down to the Datanode
 */
int pgxc_node_send_apply(PGXCNodeHandle * handle, char * buf, int len, bool ignore_pk_conflict)
{
	int	msgLen = 0;

	/* size + ignore_pk_conflict + len */
	msgLen = 4 + 1 + len;

	/* msgType + msgLen */
	if (ensure_out_buffer_capacity(handle->outEnd + 1 + msgLen, handle) != 0)
	{
		add_error_message(handle, "out of memory");
		return EOF;
	}

	handle->outBuffer[handle->outEnd++] = 'a';		/* logical apply */

	msgLen = pg_hton32(msgLen);
	memcpy(handle->outBuffer + handle->outEnd, &msgLen, 4);
	handle->outEnd += 4;

	if (ignore_pk_conflict)
		handle->outBuffer[handle->outEnd++] = 'Y';
	else
		handle->outBuffer[handle->outEnd++] = 'N';

	memcpy(handle->outBuffer + handle->outEnd, buf, len);
	handle->outEnd += len;
	SetTransitionInExtendedQuery(handle, false);

	RecordSendMessage(handle, 'a', NULL, 0);
	return SendRequest(handle);
}
#endif

/*
 * Send message to dn
 */
int
pgxc_node_send_on_proxy(PGXCNodeHandle *handle, int firstchar, StringInfo inBuf)
{
	/* size + len */
	int msgLen = 4 + inBuf->len;

	/* msgType + msgLen */
	if (ensure_out_buffer_capacity(handle->outEnd + 1 + msgLen, handle) != 0)
	{
		add_error_message(handle, "out of memory");
		return EOF;
	}

	/* msg type */
	handle->outBuffer[handle->outEnd++] = firstchar;

	/* size */
	msgLen = htonl(msgLen);
	memcpy(handle->outBuffer + handle->outEnd, &msgLen, 4);
	handle->outEnd += 4;

	/* msg data */
	memcpy(handle->outBuffer + handle->outEnd, inBuf->data, inBuf->len);
	handle->outEnd += inBuf->len;

	SetTransitionInExtendedQuery(handle, false);
	RecordSendMessage(handle, firstchar, NULL, 0);
	return SendRequest(handle);
}

/*
 * Send proxy configuration to dn
 */
int
pgxc_node_send_proxy_flag(PGXCNodeHandle *handle, int flag)
{
	/* size + flag */
	int msgLen = 4 + sizeof(int);

	/* msgType + msgLen */
	if (ensure_out_buffer_capacity(handle->outEnd + 1 + msgLen, handle) != 0)
	{
		add_error_message(handle, "out of memory");
		return EOF;
	}

	/* msg type */
	handle->outBuffer[handle->outEnd++] = 'v';

	/* size */
	msgLen = htonl(msgLen);
	memcpy(handle->outBuffer + handle->outEnd, &msgLen, 4);
	handle->outEnd += 4;

	/* flag */
	flag = htonl(flag);
	memcpy(handle->outBuffer + handle->outEnd, &flag, sizeof(int));
	handle->outEnd += sizeof(int);

	RecordSendMessage(handle, 'v', (char *)&flag, 4);
	return FlushAndStoreState(handle);
}

/*
 * Send series of Extended Query protocol messages to the data node
 */
int
pgxc_node_send_query_extended(PGXCNodeHandle *handle, const char *query, const char *statement,
                              const char *portal, int num_params, Oid *param_types,
                              int paramlen, char *params, int16 *pformats, int n_pformats, bool send_describe,
                              int fetch_size, int16 *rformats, int n_rformats)
{
	if (pgxc_node_send_cmd_seq(handle))
		return EOF;
	/* NULL query indicates already prepared statement */
	if (query && pgxc_node_send_parse(handle, statement, query, num_params, param_types))
		return EOF;
	if (pgxc_node_send_bind(handle, portal, statement, paramlen, params,
							pformats, n_pformats, rformats, n_rformats))
		return EOF;
	if (send_describe && pgxc_node_send_describe(handle, false, portal))
		return EOF;
	if (fetch_size >= 0 && pgxc_node_send_execute(handle, portal, fetch_size, false))
		return EOF;
	if (pgxc_node_send_my_sync(handle))
		return EOF;

	return 0;
}

int pgxc_node_send_batch_execute(PGXCNodeHandle *handle, const char *query,
								 const char *statement, int num_params, Oid *param_types,
								 StringInfo batch_message)
{
	int		strLen;
	int		msgLen;

	/* NULL query indicates already prepared statement */
	if (query && pgxc_node_send_parse(handle, statement, query, num_params, param_types))
		return EOF;

	if (pgxc_node_send_cmd_seq(handle))
		return EOF;

	/*
	 * Its appropriate to send ROLLBACK commands on a failed connection, but
	 * for everything else we expect the connection to be in a sane state
	 */
	elog(DEBUG5, "pgxc_node_send_batch_execute - handle->nodename=%s, handle->sock=%d, "
				 "handle->read_only=%d, handle->state.transaction_state=%c, GetConnectionState(handle) %d, node %s",
		 handle->nodename, handle->sock, handle->read_only, handle->state.transaction_state,
		 GetConnectionState(handle), handle->nodename);

	strLen = batch_message->len;
	/* size + strlen */
	msgLen = 4 + strLen;

	/* msgType + msgLen */
	if (ensure_out_buffer_capacity(handle->outEnd + 1 + msgLen, handle) != 0)
	{
		add_error_message(handle, "out of memory");
		return EOF;
	}

	handle->outBuffer[handle->outEnd++] = 'V';
	msgLen = pg_hton32(msgLen);
	memcpy(handle->outBuffer + handle->outEnd, &msgLen, 4);
	handle->outEnd += 4;
	memcpy(handle->outBuffer + handle->outEnd, batch_message->data, strLen);
	handle->outEnd += strLen;

	SetTransitionInExtendedQuery(handle, true);

	RecordSendMessage(handle, 'V', batch_message->data, strLen);

	if (pgxc_node_send_my_sync(handle))
		return EOF;

	return 0;
}

int
pgxc_node_flush_safe(PGXCNodeHandle *handle)
{
	while (handle->outEnd)
	{
		if (send_some(handle, handle->outEnd) < 0)
		{
			add_error_message(handle, "failed to send data to datanode");
			return EOF;
		}
	}
	return 0;
}


/*
 * This method won't return until connection buffer is empty or error occurs
 * To ensure all data are on the wire before waiting for response
 
 * Be carefully, If the accumulated messages require a response or will change the 
 * state of the DN node, the pgxc_node_flush function cannot be used. 
 * Instead, the pgxc_node_flush_and_set_state function should be called 
 * to combine the sending of messages and setting the state 
 * into a single atomic operation. 
 * This helps avoid inconsistencies caused by interruptions from signals 
 * affecting both operations.
 */
int
pgxc_node_flush(PGXCNodeHandle *handle)
{
	if (IS_CENTRALIZED_MODE)
	{
		elog(ERROR, "remote query is not supported in centralized mode");
	}

	while (handle->outEnd)
	{
		if (send_some(handle, handle->outEnd) < 0)
		{
			int32 error = errno;
			add_error_message(handle, "failed to send data to datanode");
#if 0
			/*
			 * before returning, also update the shared health
			 * status field to indicate that this node could be
			 * possibly unavailable.
			 *
			 * Note that this error could be due to a stale handle
			 * and it's possible that another backend might have
			 * already updated the health status OR the node
			 * might have already come back since the last disruption
			 */
			PoolPingNodeRecheck(handle->nodeoid);
#endif

			errno = error;
			return EOF;
		}
	}
	return 0;
}

/*
 * Send specified statement down to the PGXC node
 */
static int
pgxc_node_send_query_internal(PGXCNodeHandle * handle, const char *query,
		bool rollback)
{
	int			strLen;
	int			msgLen;

	if (pgxc_node_send_cmd_seq(handle))
		return EOF;

	/*
	 * Its appropriate to send ROLLBACK commands on a failed connection, but
	 * for everything else we expect the connection to be in a sane state
	 */
	if (enable_distri_print)
		elog(LOG, "pgxc_node_send_query - handle->nodename=%s, handle->sock=%d, "
					 "handle->read_only=%d, handle->transaction_status=%c, handle->state %d, node %s %d, query: %s",
			 handle->nodename, handle->sock, handle->read_only, GetTxnState(handle),
			 GetConnectionState(handle), handle->nodename, handle->backend_pid, query);

	strLen = strlen(query) + 1;
	/* size + strlen */
	msgLen = 4 + strLen;

	/* msgType + msgLen */
	if (ensure_out_buffer_capacity(handle->outEnd + 1 + msgLen, handle) != 0)
	{
		add_error_message(handle, "out of memory");
		return EOF;
	}

	handle->outBuffer[handle->outEnd++] = 'Q';
	msgLen = pg_hton32(msgLen);
	memcpy(handle->outBuffer + handle->outEnd, &msgLen, 4);
	handle->outEnd += 4;
	memcpy(handle->outBuffer + handle->outEnd, query, strLen);
	handle->outEnd += strLen;

	SetTransitionInExtendedQuery(handle, false);
	RecordSendMessage(handle, 'Q', query, strlen(query));
	return SendRequest(handle);
}

int
pgxc_node_send_rollback(PGXCNodeHandle *handle, const char *query)
{
	int ret = 0;
	elog(DEBUG1, "[NEST_LEVEL_CHECK:%d] send rollback. cur_level: %d", handle->backend_pid, GetCurrentTransactionNestLevel());
#ifdef __TWO_PHASE_TESTS__
	if ('\0' != g_twophase_state.gid[0])
	{
		capacity_stack = SEND_ROLLBACK;
	}
#endif
	/* sync newest command id first */
	ret = pgxc_node_send_cmd_id(handle, GetCurrentCommandId(false));
	if (ret)
		return ret;
	return pgxc_node_send_query_internal(handle, query, true);
}

int
pgxc_node_send_query(PGXCNodeHandle *handle, const char *query)
{
#ifdef __TWO_PHASE_TESTS__
	if ((IN_REMOTE_PREPARE == twophase_in && !handle->read_only) ||
        IN_PREPARE_ERROR == twophase_in ||
        IN_REMOTE_FINISH == twophase_in ||
        IN_PG_CLEAN == twophase_in)
    {
        capacity_stack = SEND_QUERY;
    }
#endif
	return pgxc_node_send_query_internal(handle, query, false);
}

/*
 * Send opentenbase internal command to the remote with an internal flag,
 * this is introduced to optimize pg_stat_statements performance
 */
int
pgxc_node_send_query_with_internal_flag(PGXCNodeHandle *handle, const char *query)
{
#ifdef __TWO_PHASE_TESTS__
	if ((IN_REMOTE_PREPARE == twophase_in && !handle->read_only) ||
        IN_PREPARE_ERROR == twophase_in ||
        IN_REMOTE_FINISH == twophase_in ||
        IN_PG_CLEAN == twophase_in)
    {
        capacity_stack = SEND_QUERY;
    }
#endif
	if (pgxc_node_send_internal_cmd_flag(handle) < 0)
		return EOF;

	return pgxc_node_send_query_internal(handle, query, false);
}


/*
 * Send the GID down to the PGXC node
 */
int
pgxc_node_send_gid(PGXCNodeHandle *handle, char* gid)
{
	int			msglen;
	int			gidlen = strlen(gid);

	msglen = 4 + gidlen + 1;

	/* Invalid connection state, return error */
	if (!IsConnectionStateIdle(handle))
	{
		elog(LOG, "pgxc_node_send_gid datanode %s pid %d invalid stauts:%d, no need to send data, return NOW",
				handle->nodename, handle->backend_pid, GetConnectionState(handle));
		return EOF;
	}

	/* msgType + msgLen */
	if (ensure_out_buffer_capacity(handle->outEnd + 1 + msglen, handle) != 0)
	{
		add_error_message(handle, "out of memory");
		return EOF;
	}
	elog(DEBUG8, "send gid %s", gid);

	handle->outBuffer[handle->outEnd++] = 'G';
	msglen = pg_hton32(msglen);
	memcpy(handle->outBuffer + handle->outEnd, &msglen, 4);
	handle->outEnd += 4;
	memcpy(handle->outBuffer + handle->outEnd, gid, strlen(gid) + 1);
	handle->outEnd += (strlen(gid) + 1);

	RecordSendMessage(handle, 'G', gid, gidlen);
	return 0;
}

#ifdef __TWO_PHASE_TRANS__
/*
 * Send the startnode down to the PGXC node
 */
int
pgxc_node_send_starter(PGXCNodeHandle *handle, char* startnode)
{
	int			msglen;
	int			snlen = strnlen(startnode, NAMEDATALEN);

	msglen = 4 + snlen + 1;

	/* Invalid connection state, return error */
	if (!IsConnectionStateIdle(handle))
	{
		elog(LOG, "pgxc_node_send_starter datanode:%s pid %d invalid stauts:%d, no need to send data, return NOW",
				handle->nodename, handle->backend_pid, GetConnectionState(handle));
		return EOF;
	}
    
#ifdef __TWO_PHASE_TESTS__
    capacity_stack = SEND_STARTER;
#endif

	/* msgType + msgLen */
	if (ensure_out_buffer_capacity(handle->outEnd + 1 + msglen, handle) != 0)
	{
		add_error_message(handle, "in function pgxc_node_send_starter, error: out of memory"); 
		return EOF;
	}
	elog(DEBUG8, "send startnode %s", startnode);

	handle->outBuffer[handle->outEnd++] = 'e';
	msglen = pg_hton32(msglen);
	memcpy(handle->outBuffer + handle->outEnd, &msglen, 4);
	handle->outEnd += 4;
	memcpy(handle->outBuffer + handle->outEnd, startnode, strnlen(startnode, NAMEDATALEN) + 1);
	handle->outEnd += (strnlen(startnode, NAMEDATALEN) + 1);

	RecordSendMessage(handle, 'e', startnode, snlen);
	return 0;
}

int 
pgxc_node_send_startxid(PGXCNodeHandle *handle, GlobalTransactionId transactionid)
{
	int			msglen = 4 + sizeof(GlobalTransactionId);
    int         i32;

	/* Invalid connection state, return error */
	if (!IsConnectionStateIdle(handle))
	{
		elog(LOG, "pgxc_node_send_startxid datanode: %s pid %d invalid stauts:%d, no need to send data, return NOW",
				handle->nodename, handle->backend_pid, GetConnectionState(handle));
		return EOF;
	}

#ifdef __TWO_PHASE_TESTS__
    capacity_stack = SEND_STARTXID;
#endif
	/* msgType + msgLen */
	if (ensure_out_buffer_capacity(handle->outEnd + 1 + msglen, handle) != 0)
	{
		add_error_message(handle, "in function pgxc_node_send_startxid, error: out of memory"); 
		return EOF;
	}
	elog(DEBUG8, "send transactionid %u", transactionid);

	handle->outBuffer[handle->outEnd++] = 'x';
	msglen = pg_hton32(msglen);
	memcpy(handle->outBuffer + handle->outEnd, &msglen, 4);
	handle->outEnd += 4;
	i32 = pg_hton32(transactionid);
	memcpy(handle->outBuffer + handle->outEnd, &i32, sizeof(GlobalTransactionId));
	handle->outEnd += sizeof(GlobalTransactionId);

	RecordSendMessage(handle, 'x', (char *)&transactionid, 4);
	return 0;
}

/*
 * Send the partnodes down to the PGXC node
 */
int
pgxc_node_send_partnodes(PGXCNodeHandle *handle, char* partnodes)
{
	int			msglen;
	int			pnlen = strlen(partnodes);

	msglen = 4 + pnlen + 1;

	/* Invalid connection state, return error */
	if (!IsConnectionStateIdle(handle))
	{
		elog(LOG, "pgxc_node_send_partnodes datanode: %s pid %d invalid stauts:%d, no need to send data, return NOW",
				handle->nodename, handle->backend_pid, GetConnectionState(handle));
		return EOF;
	}

#ifdef __TWO_PHASE_TESTS__
    capacity_stack = SEND_PARTNODES;
#endif
	/* msgType + msgLen */
	if (ensure_out_buffer_capacity(handle->outEnd + 1 + msglen, handle) != 0)
	{
		add_error_message(handle, "in function pgxc_node_send_partnodes, error: out of memory"); 
		return EOF;
	}
	elog(DEBUG8, "send partnodes %s", partnodes);

	handle->outBuffer[handle->outEnd++] = 'O';
	msglen = pg_hton32(msglen);
	memcpy(handle->outBuffer + handle->outEnd, &msglen, 4);
	handle->outEnd += 4;
	memcpy(handle->outBuffer + handle->outEnd, partnodes, strlen(partnodes) + 1);
	handle->outEnd += (strlen(partnodes) + 1);

	RecordSendMessage(handle, 'O', partnodes, pnlen);
	return 0;
}

/*
 * Send the database down to the PGXC node
 */
int
pgxc_node_send_database(PGXCNodeHandle *handle, char* database)
{
	int		msglen = 4 + strlen(database) + 1;
	int		dblen = strlen(database);

	msglen = 4 + dblen + 1;

	/* Invalid connection state, return error */
	if (!IsConnectionStateIdle(handle))
	{
		elog(LOG, "pgxc_node_send_database datanode: %s pid %d invalid stauts:%d, no need to send data, return NOW",
				handle->nodename, handle->backend_pid, GetConnectionState(handle));
		return EOF;
	}

#ifdef __TWO_PHASE_TESTS__
	capacity_stack = SEND_DATABASE;
#endif

	/* msgType + msgLen */
	if (ensure_out_buffer_capacity(handle->outEnd + 1 + msglen, handle) != 0)
	{
		add_error_message(handle, "pgxc_node_send_database, error: out of memory");
		return EOF;
	}
	elog(DEBUG8, "send database %s", database);

	handle->outBuffer[handle->outEnd++] = 'w';
	msglen = htonl(msglen);
	memcpy(handle->outBuffer + handle->outEnd, &msglen, 4);
	handle->outEnd += 4;
	memcpy(handle->outBuffer + handle->outEnd, database, strlen(database) + 1);
	handle->outEnd += (strlen(database) + 1);

	RecordSendMessage(handle, 'O', database, dblen);

	return 0;
}

/*
 * Send the user down to the PGXC node
 */
int
pgxc_node_send_user(PGXCNodeHandle *handle, char* user)
{
	int msglen = 4 + strlen(user) + 1;

	/* Invalid connection state, return error */
	if (!IsConnectionStateIdle(handle))
	{
		elog(LOG, "pgxc_node_send_user datanode: %s pid %d invalid stauts:%d, no need to send data, return NOW",
				handle->nodename, handle->backend_pid, GetConnectionState(handle));
		return EOF;
	}

#ifdef __TWO_PHASE_TESTS__
	capacity_stack = SEND_USER;
#endif

	/* msgType + msgLen */
	if (ensure_out_buffer_capacity(handle->outEnd + 1 + msglen, handle) != 0)
	{
		add_error_message(handle, "pgxc_node_send_user, error: out of memory");
		return EOF;
	}
	elog(DEBUG8, "send user %s", user);

	handle->outBuffer[handle->outEnd++] = 'u';
	msglen = htonl(msglen);
	memcpy(handle->outBuffer + handle->outEnd, &msglen, 4);
	handle->outEnd += 4;
	memcpy(handle->outBuffer + handle->outEnd, user, strlen(user) + 1);
	handle->outEnd += (strlen(user) + 1);

	RecordSendMessage(handle, 'u', user, strlen(user));

	return 0;
}

/*
 * when execute in pg_clean, we allowed to truncate the exists 2pc file 
 */
int
pgxc_node_send_clean(PGXCNodeHandle *handle)
{
	int			msglen = 4;

	/* Invalid connection state, return error */
	if (!IsConnectionStateIdle(handle))
	{
		elog(LOG, "pgxc_node_send_clean datanode: %s pid %d invalid stauts:%d, no need to send data, return NOW",
				handle->nodename, handle->backend_pid, GetConnectionState(handle));
		return EOF;
	}

#ifdef __TWO_PHASE_TESTS__
    if (IN_PG_CLEAN == twophase_in && 
        PG_CLEAN_SEND_CLEAN == twophase_exception_case)
    {
        capacity_stack = SEND_PGCLEAN;
    }
#endif
	/* msgType + msgLen */
	if (ensure_out_buffer_capacity(handle->outEnd + 1 + msglen, handle) != 0)
	{
		add_error_message(handle, "in function pgxc_node_send_clean, error: out of memory"); 
		return EOF;
	}

	handle->outBuffer[handle->outEnd++] = 'n';
	msglen = pg_hton32(msglen);
	memcpy(handle->outBuffer + handle->outEnd, &msglen, 4);
	handle->outEnd += 4;

	RecordSendMessage(handle, 'n', NULL, 0);
	return 0;
}

/*
 * when execute in cleanup handles
 */
int
pgxc_node_send_clean_distribute(PGXCNodeHandle *handle)
{
	int			msglen = 4;

	/* Invalid connection state, return error */
	if (!IsConnectionStateIdle(handle) && !IsConnectionStateFatal(handle))
	{
		elog(LOG, "pgxc_node_send_clean_distribute datanode: %s pid %d, invalid stauts:%d",
			handle->nodename, handle->backend_pid, GetConnectionState(handle));
		return EOF;
	}

	/* msgType + msgLen */
	if (ensure_out_buffer_capacity(handle->outEnd + 1 + msglen, handle) != 0)
	{
		add_error_message(handle, "in function pgxc_node_send_clean_distribute, error: out of memory"); 
		return EOF;
	}


	handle->outBuffer[handle->outEnd++] = 'q';
	msglen = pg_hton32(msglen);
	memcpy(handle->outBuffer + handle->outEnd, &msglen, 4);
	handle->outEnd += 4;

	RecordSendMessage(handle, 'q', NULL, 0);
	return 0;
}

int
pgxc_node_send_readonly(PGXCNodeHandle *handle, bool read_only)
{
	int			msglen = 4 + 1;

	/* Invalid connection state, return error */
	if (!IsConnectionStateIdle(handle))
	{
		elog(LOG, "pgxc_node_send_readonly datanode: %s pid %d invalid stauts:%d, no need to send data, return NOW",
				handle->nodename, handle->backend_pid, GetConnectionState(handle));
		return EOF;
	}

#ifdef __TWO_PHASE_TESTS__
    if (IN_PG_CLEAN == twophase_in && 
        PG_CLEAN_SEND_READONLY == twophase_exception_case)
    {
        capacity_stack = SEND_READONLY;
    }
#endif
	/* msgType + msgLen */
	if (ensure_out_buffer_capacity(handle->outEnd + 1 + msglen, handle) != 0)
	{
		add_error_message(handle, "in function pgxc_node_send_clean, error: out of memory"); 
		return EOF;
	}

	handle->outBuffer[handle->outEnd++] = 'r';
	msglen = pg_hton32(msglen);
	memcpy(handle->outBuffer + handle->outEnd, &msglen, 4);
	handle->outEnd += 4;

	if (read_only)
		handle->outBuffer[handle->outEnd++] = 'Y';
	else
		handle->outBuffer[handle->outEnd++] = 'N';

	RecordSendMessage(handle, 'r', &read_only, 1);
	return 0;
}

int
pgxc_node_send_after_prepare(PGXCNodeHandle *handle)
{
	int			msglen = 4;

	/* Invalid connection state, return error */
	if (!IsConnectionStateIdle(handle))
	{
		elog(LOG, "pgxc_node_send_after_prepare datanode: %s pid %d invalid stauts:%d, no need to send data, return NOW",
				handle->nodename, handle->backend_pid, GetConnectionState(handle));
		return EOF;
	}

#ifdef __TWO_PHASE_TESTS__
    if (IN_PG_CLEAN == twophase_in && 
        PG_CLEAN_SEND_AFTER_PREPARE == twophase_exception_case)
    {
        capacity_stack = SEND_AFTER_PREPARE;
    }
#endif
	/* msgType + msgLen */
	if (ensure_out_buffer_capacity(handle->outEnd + 1 + msglen, handle) != 0)
	{
		add_error_message(handle, "in function pgxc_node_send_clean, error: out of memory"); 
		return EOF;
	}

	handle->outBuffer[handle->outEnd++] = 'A';
	msglen = pg_hton32(msglen);
	memcpy(handle->outBuffer + handle->outEnd, &msglen, 4);
	handle->outEnd += 4;

	RecordSendMessage(handle, 'A', NULL, 0);
	return 0;
}
#endif

/*
 * Send the GXID down to the PGXC node
 */
int
pgxc_node_send_gxid(PGXCNodeHandle *handle)
{
	int			msglen = 8;
	bool		primary = !handle->read_only;
	char	   *globalXidString;
	bool		need_send_zero_gxid = false;

	globalXidString = GetGlobalXidNoCheck();
	if (NULL == globalXidString)
		need_send_zero_gxid = true;

	/*
	 * If this datanode has not participated in any writes, then the
	 * process of this dn actually does not need gxid to obtain the xid
	 * of the write process, which is used to see the tuple modified by
	 * the write process. However, because commit or abort transaction
	 * do not clean up gxid, an empty gxid needs to be issued to clean
	 * up local_xid. Otherwise, there will be issues with transaction
	 * visibility if the previous local_xid is used.
	 */
	if (handle->node_type == PGXC_NODE_DATANODE &&
		!bms_is_member(handle->nodeidx, current_write_datanodes))
	{
		Assert(handle->read_only);
		need_send_zero_gxid = true;
	}

	if (enable_distri_print)
	{
		if (!need_send_zero_gxid)
			elog(LOG, "handle node name %s %d send version " UINT64_FORMAT " global xid %s, primary: %d",
				handle->nodename, handle->backend_pid, handle->sendGxidVersion, globalXidString, primary);
		else
			elog(LOG, "handle node name %s, %d send null global xid, primary: %d", handle->nodename, handle->backend_pid, primary);
	}
	/* Invalid connection state, return error */
	if (need_send_zero_gxid)
		msglen = 4 + 1 + 1;
	else
		msglen = 4 + strlen(globalXidString) + 1 + 1;

	if (!IsConnectionStateIdle(handle))
	{
		elog(LOG, "pgxc_node_send_gid datanode:%s pid: %d invalid stauts:%d, connection state is not idle",
			handle->nodename, handle->backend_pid, GetConnectionState(handle));
		return EOF;
	}

	/* msgType + msgLen */
	if (ensure_out_buffer_capacity(handle->outEnd + 1 + msglen, handle) != 0)
	{
		add_error_message(handle, "out of memory");
		return EOF;
	}

	handle->outBuffer[handle->outEnd++] = 'g';
	msglen = pg_hton32(msglen);
	memcpy(handle->outBuffer + handle->outEnd, &msglen, 4);
	handle->outEnd += 4;
	memcpy(handle->outBuffer + handle->outEnd, &primary, 1);
	handle->outEnd += 1;
	if (need_send_zero_gxid)
	{
		handle->outBuffer[handle->outEnd++] = '\0';
	}
	else
	{
		memcpy(handle->outBuffer + handle->outEnd, globalXidString, strlen(globalXidString) + 1);
		handle->outEnd += (strlen(globalXidString) + 1);
		handle->sendGxidVersion = GetGlobalXidVersion();
	}
	RecordSendMessage(handle, 'g', globalXidString, globalXidString? strlen(globalXidString) : 0);

	/*
	 * When distributing the GXID, it is already known that the current connection release state is
	 * in "ready only." The cleanup of the read link by the DN write connection depends on these two
	 * states during an abort. Therefore, both are set atomically
	 */
	pgxc_node_send_readonly(handle, handle->read_only);

	return 0;
}

#ifdef __OPENTENBASE_C__
/*
 * Send the FID down to the PGXC node
 */
int
pgxc_node_send_fid_flevel(PGXCNodeHandle *handle, int fid, int flevel)
{
	int			msglen = 12;
	int			i32;

	if (enable_distri_print)
	{
		elog(LOG, "send fid %d to node %s pid %d", fid, handle->nodename, handle->backend_pid);
	}
	
	/* Invalid connection state, return error */
	if (!IsConnectionStateIdle(handle))
	{
		elog(LOG, "Connection is not idle %s, pid: %d, state: %d",
				handle->nodename, handle->backend_pid, GetConnectionState(handle));
		return EOF;
	}

	/* msgType + msgLen */
	if (ensure_out_buffer_capacity(handle->outEnd + 1 + msglen, handle) != 0)
	{
		add_error_message(handle, "out of memory");
		return EOF;
	}

	handle->outBuffer[handle->outEnd++] = 'l';
	msglen = pg_hton32(msglen);
	memcpy(handle->outBuffer + handle->outEnd, &msglen, 4);
	handle->outEnd += 4;
	i32 = pg_hton32(fid);
	memcpy(handle->outBuffer + handle->outEnd, &i32, 4);
	handle->outEnd += 4;
	i32 = pg_hton32(flevel);
	memcpy(handle->outBuffer + handle->outEnd, &i32, 4);
	handle->outEnd += 4;

	RecordSendMessage(handle, 'l', (char *)&fid, 4);
	return 0;
}
#endif

#ifdef __RESOURCE_QUEUE__
int
pgxc_node_send_resqinfo(PGXCNodeHandle *handle,
						ResQueueInfo * resqinfo,
						int64 network_bytes_limit)
{
	int	msglen = 4					/* msgLen */
				+ NAMEDATALEN		/* resq_name */
				+ NAMEDATALEN		/* resq_group */
				+ 8					/* resq_memory_limit */
				+ 8					/* resq_network_limit */
				+ 4					/* resq_active_stmts */
				+ 2					/* resq_wait_overload */
				+ 2					/* resq_priority */
				+ 8;				/* network_bytes_limit */

	uint32	n32 = 0;
	uint16	n16 = 0;
	int64	i = 0;

	/* Invalid connection state, return error */
	if (!IsConnectionStateIdle(handle))
	{
		elog(LOG, "pgxc_node_send_resqinfo datanode:%u invalid stauts:%d, no need to send data, return NOW",
			handle->nodeoid, GetConnectionState(handle));
		return EOF;
	}

	/* msgType + msgLen */
	if (ensure_out_buffer_capacity(handle->outEnd + 1 + msglen, handle) != 0)
	{
		add_error_message(handle, "out of memory");
		return EOF;
	}


	/* msgType : 1 */
	{
		handle->outBuffer[handle->outEnd++] = 'J';
	}

	/* msgLen : 4 */
	{
		msglen = pg_hton32(msglen);
		memcpy(handle->outBuffer + handle->outEnd, &msglen, 4);
		handle->outEnd += 4;
	}

	/* resq_name : NAMEDATALEN */
	{
		memcpy(handle->outBuffer + handle->outEnd, resqinfo->resq_name.data, NAMEDATALEN);
		handle->outEnd += NAMEDATALEN;
	}

	/* resq_group : NAMEDATALEN */
	{
		memcpy(handle->outBuffer + handle->outEnd, resqinfo->resq_group.data, NAMEDATALEN);
		handle->outEnd += NAMEDATALEN;
	}

	/* resq_memory_limit : 8 */
	{
		i = (int64) resqinfo->resq_memory_limit;
		/* High order half first */
#ifdef INT64_IS_BUSTED
		/* don't try a right shift of 32 on a 32-bit word */
		n32 = (i < 0) ? -1 : 0;
#else
		n32 = (uint32) (i >> 32);
#endif
		n32 = pg_hton32(n32);
		memcpy(handle->outBuffer + handle->outEnd, &n32, 4);
		handle->outEnd += 4;

		/* Now the low order half */
		n32 = (uint32) i;
		n32 = pg_hton32(n32);
		memcpy(handle->outBuffer + handle->outEnd, &n32, 4);
		handle->outEnd += 4;
	}

	/* resq_network_limit : 8 */
	{
		i = (int64) resqinfo->resq_network_limit;
		/* High order half first */
#ifdef INT64_IS_BUSTED
		/* don't try a right shift of 32 on a 32-bit word */
		n32 = (i < 0) ? -1 : 0;
#else
		n32 = (uint32) (i >> 32);
#endif
		n32 = pg_hton32(n32);
		memcpy(handle->outBuffer + handle->outEnd, &n32, 4);
		handle->outEnd += 4;

		/* Now the low order half */
		n32 = (uint32) i;
		n32 = pg_hton32(n32);
		memcpy(handle->outBuffer + handle->outEnd, &n32, 4);
		handle->outEnd += 4;
	}

	/* resq_active_stmts : 4 */
	{
		n32 = pg_hton32((uint32) resqinfo->resq_active_stmts);
		memcpy(handle->outBuffer + handle->outEnd, &n32, 4);
		handle->outEnd += 4;
	}

	/* resq_wait_overload : 2 */
	{
		n16 = pg_hton16((uint16) resqinfo->resq_wait_overload);
		memcpy(handle->outBuffer + handle->outEnd, &n16, 2);
		handle->outEnd += 2;
	}

	/* resq_priority : 2 */
	{
		n16 = pg_hton16((uint16) resqinfo->resq_priority);
		memcpy(handle->outBuffer + handle->outEnd, &n16, 2);
		handle->outEnd += 2;
	}

	/* network_bytes_limit : 8 */
	{
		i = (int64) network_bytes_limit;
		/* High order half first */
#ifdef INT64_IS_BUSTED
		/* don't try a right shift of 32 on a 32-bit word */
		n32 = (i < 0) ? -1 : 0;
#else
		n32 = (uint32) (i >> 32);
#endif
		n32 = pg_hton32(n32);
		memcpy(handle->outBuffer + handle->outEnd, &n32, 4);
		handle->outEnd += 4;

		/* Now the low order half */
		n32 = (uint32) i;
		n32 = pg_hton32(n32);
		memcpy(handle->outBuffer + handle->outEnd, &n32, 4);
		handle->outEnd += 4;
	}

	RecordSendMessage(handle, 'J', NULL, 0);
	return 0;
}
#endif

int
pgxc_node_send_resginfo(PGXCNodeHandle *handle)
{
	int	msglen = 0;
	StringInfoData resgroupStr;

	/* Avoid send resginfo if global session is not set */
	if (session_params == NULL || session_params->len == 0)
		return 0;

	/* Invalid connection state, return error */
	if (!IsConnectionStateIdle(handle))
	{
		elog(LOG, "pgxc_node_send_resginfo datanode:%u invalid stauts:%d, no need to send data, return NOW",
			handle->nodeoid, GetConnectionState(handle));
		return EOF;
	}

	initStringInfo(&resgroupStr);
	SerializeResGroupInfo(&resgroupStr);
	msglen = sizeof(resgroupStr.len) + resgroupStr.len;

	/* msgType + msgLen */
	if (ensure_out_buffer_capacity(handle->outEnd + 1 + msglen, handle) != 0)
	{
		add_error_message(handle, "out of memory");
		return EOF;
	}

	/* msgType : 1 */
	{
		handle->outBuffer[handle->outEnd++] = 'J';
	}

	/* msgLen : 4 */
	{
		msglen = pg_hton32(msglen);
		memcpy(handle->outBuffer + handle->outEnd, &msglen, 4);
		handle->outEnd += 4;
	}

	/* msgBody : resgroupStr.len */
	{
		memcpy(handle->outBuffer + handle->outEnd, resgroupStr.data, resgroupStr.len);
		handle->outEnd += resgroupStr.len;
	}

	RecordSendMessage(handle, 'J', NULL, 0);
	return 0;
}

int
pgxc_node_send_resgreq(PGXCNodeHandle *handle)
{
	int			msglen = 4;

	/* Invalid connection state, return error */
	if (!IsConnectionStateIdle(handle))
	{
		elog(LOG, "pgxc_node_send_resgreq coordinator:%u invalid stauts:%d, no need to send data, return NOW",
			handle->nodeoid, GetConnectionState(handle));
		return EOF;
	}

	/* msgType + msglen */
	if (ensure_out_buffer_capacity(handle->outEnd + 1 + msglen, handle) != 0)
	{
		add_error_message(handle, "out of memory");
		return EOF;
	}


	/* msgType : 1 */
	{
		handle->outBuffer[handle->outEnd++] = 'J';
	}

	/* msglen : 4 */
	{
		msglen = htonl(msglen);
		memcpy(handle->outBuffer + handle->outEnd, &msglen, 4);
		handle->outEnd += 4;
	}
	
	RecordSendMessage(handle, 'J', NULL, 0);
	return 0;
}

/*
 * Send the Command ID down to the PGXC node
 */
int
pgxc_node_send_cmd_id(PGXCNodeHandle *handle, CommandId cid)
{
	int			msglen = CMD_ID_MSG_LEN;
	int			i32;

	/* No need to send command ID if its sending flag is not enabled */
	/* XXX: parallel worker always send cid */
	if (!IsSendCommandId() && !IsParallelWorker())
	{
		return 0;
	}

	/* Invalid connection state, return error */
	if (!IsConnectionStateIdle(handle))
	{
		elog(LOG, "pgxc_node_send_cmd_id datanode: %s pid %d invalid stauts:%d, no need to send data, return NOW",
			handle->nodename, handle->backend_pid, GetConnectionState(handle));
		return EOF;
	}

	/* msgType + msgLen */
	if (ensure_out_buffer_capacity(handle->outEnd + 1 + msglen, handle) != 0)
	{
		add_error_message(handle, "out of memory");
		return EOF;
	}

	handle->outBuffer[handle->outEnd++] = 'M';
	msglen = pg_hton32(msglen);
	memcpy(handle->outBuffer + handle->outEnd, &msglen, 4);
	handle->outEnd += 4;
	i32 = pg_hton32(cid);
	memcpy(handle->outBuffer + handle->outEnd, &i32, 4);
	handle->outEnd += 4;

	RecordSendMessage(handle, 'M', (char*)&cid, 4);
	return 0;
}

static int
send_backend_exec_env(PGXCNodeHandle *handle, BackendEnv *env)
{
	int			msglen PG_USED_FOR_ASSERTS_ONLY;
	int         sec_context_32;

	/* Invalid connection state, return error */
	if (!IsConnectionStateIdle(handle))
	{
		elog(LOG,
				"pgxc_node_send_exec_env datanode: %s pid %d invalid stauts: %d, "
					"no need to send data, return NOW",
				handle->nodename, handle->backend_pid, GetConnectionState(handle));

		return EOF;
	}

	msglen = 4 + sizeof(int) + sizeof(bool) + sizeof(bool) + strlen(env->username.data) + 1;

	/* msgType + msgLen */
	if (ensure_out_buffer_capacity(handle->outEnd + 1 + msglen, handle) != 0)
	{
		add_error_message(handle, "out of memory");
		return EOF;
	}

	handle->outBuffer[handle->outEnd++] = 'K';
	msglen = htonl(msglen);
	memcpy(handle->outBuffer + handle->outEnd, &msglen, 4);
	handle->outEnd += 4;

	sec_context_32 = htonl(env->sec_context);
	memcpy(handle->outBuffer + handle->outEnd, &sec_context_32, sizeof(sec_context_32));
	handle->outEnd += sizeof(sec_context_32);

	memcpy(handle->outBuffer + handle->outEnd, &env->insec_context, sizeof(env->insec_context));
	handle->outEnd += sizeof(env->insec_context);

	memcpy(handle->outBuffer + handle->outEnd, &env->isquery, sizeof(env->isquery));
	handle->outEnd += sizeof(env->isquery);

	memcpy(handle->outBuffer + handle->outEnd, env->username.data, strlen(env->username.data) + 1);
	handle->outEnd += strlen(env->username.data) + 1;
	RecordSendMessage(handle, 'K', NULL, 0);
	return 0;
}

int
pgxc_node_cleanup_exec_env(PGXCNodeHandle *handle)
{
	BackendEnv	env;

	memset(&env, 0, sizeof(BackendEnv));
	env.username.data[0] = '\0';
	env.sec_context = 0;
	env.insec_context = false;
	env.isquery = false;

	return send_backend_exec_env(handle, &env);
}

int
pgxc_node_send_exec_env_if(PGXCNodeHandle *handle)
{
	if (!SecContextIsSet && !OidIsValid(handle->current_user))
		return 0;

	if (SecContextIsSet)
	{
		/*
		 * When we are in SecContextIsSet context, it inidicates that we are in AUTHID DEFINER function/procedure,
		 * we should guarantee that CurrentUserId in DN is the same as CN to avoid permission problems.
		 */
		if (handle->current_user == GetUserId())
			return 0;
		return pgxc_node_send_exec_env(handle, SecContextIsSet, false);
	}
	else
	{
		/*
		 * When we are not in SecContextIsSet context, but handle->current_user is set, it indicates that the handle
		 * was used in AUTHID DEFINER function/procedure, and has not reset before.
		 * it most likely to occur in handles used by fragment or handles achieved from get_current_handles.
		 */
		if (!pgxc_node_send_exec_env(handle, SecContextIsSet, false))
		{
			/*
			 * When we are not in SecContextIsSet context, we only need send CurrentUserId once.
			 */
			handle->current_user = InvalidOid;
			return 0;
		}
		else
			return EOF;
	}

}

/*
 * Send execution env to one node.
 */
int
pgxc_node_send_exec_env(PGXCNodeHandle *handle, bool insec_context, bool isquery)
{
	BackendEnv	env;
	Oid			uid;
	int			sec_context;
	char		*name = NULL;

	memset(&env, 0, sizeof(BackendEnv));

	GetUserIdAndSecContext(&uid, &sec_context);
	if (CurrentUserName != NULL)
		name = CurrentUserName;
	else if (uid == GetAuthenticatedUserId())
		name = GetClusterUserNameDirectly();
	else
		name = GetUserNameFromId(uid, false);
	Assert(name != NULL);
	StrNCpy(env.username.data, name, NAMEDATALEN);
	env.sec_context = sec_context;
	env.insec_context = insec_context;
	env.isquery = isquery;
	handle->current_user = uid;
	return send_backend_exec_env(handle, &env);
}

int pgxc_node_send_option(PGXCNodeHandle *handle, char *opt)
{
	int msglen = NO_DESC_MSG_LEN + 1 + 4;

	/* Invalid connection state, return error */
	if ((!IsConnectionStateIdle(handle)))
	{
		elog(LOG, "pgxc_node_send_option datanode: %s pid %d invalid stauts:%d, no need to send data, return NOW",
			handle->nodename, handle->backend_pid, GetConnectionState(handle));
		return EOF;
	}

	/* msgType + msgLen */
	if (ensure_out_buffer_capacity(handle->outEnd + 1 + msglen, handle) != 0)
	{
		add_error_message(handle, "out of memory");
		return EOF;
	}

	handle->outBuffer[handle->outEnd++] = 'Y';
	msglen = htonl(msglen);
	memcpy(handle->outBuffer + handle->outEnd, &msglen, 4);
	handle->outEnd += 4;

	handle->outBuffer[handle->outEnd++] = opt[0];
	handle->outBuffer[handle->outEnd++] = opt[1];
	handle->outBuffer[handle->outEnd++] = '\0';
	
	RecordSendMessage(handle, 'Y', opt, 2);
	return 0;
}

/*
 * Send messages cmd seq, to check whether the message is dislocation
 */
int
pgxc_node_send_cmd_seq(PGXCNodeHandle *handle)
{
	int		msglen = 5;
	uint8	sno;

	sno = GetNewSequenceNum(handle);

	/* msgType + msgLen */
	if (ensure_out_buffer_capacity(handle->outEnd + 1 + msglen, handle) != 0)
	{
		add_error_message(handle, "out of memory");
		return EOF;
	}

	handle->outBuffer[handle->outEnd++] = 'N';
	msglen = htonl(msglen);
	memcpy(handle->outBuffer + handle->outEnd, &msglen, 4);
	handle->outEnd += 4;
	handle->outBuffer[handle->outEnd++] = sno;
	RecordSendMessage(handle, 'N', (char *)&sno, 1);
	return 0;
}

int
pgxc_node_send_internal_cmd_flag(PGXCNodeHandle *handle)
{
	char option[2] = {'I', 't'};
	return pgxc_node_send_option(handle, option);
}


#ifdef __OPENTENBASE__
/*
 * Send the Coordinator info down to the PGXC node at the beginning of transaction,
 * In this way, Datanode can print this Coordinator info into logfile, 
 * and those infos can be found in Datanode logifile if needed during debugging
 */
int
pgxc_node_send_coord_info(PGXCNodeHandle * handle, int coord_pid, TransactionId coord_vxid)
{
	int	msgLen = 0;
	int	i32 = 0;


	if (!IS_PGXC_COORDINATOR)
		return 0;

	/* size + coord_pid + coord_vxid */
	msgLen = 4 + 4 + 4;

	/* msgType + msgLen */
	if (ensure_out_buffer_capacity(handle->outEnd + 1 + msgLen, handle) != 0)
	{
		add_error_message(handle, "pgxc_node_send_coord_info out of memory");
		return EOF;
	}

	handle->outBuffer[handle->outEnd++] = 'U';		/* coord info */

	msgLen = htonl(msgLen);
	memcpy(handle->outBuffer + handle->outEnd, &msgLen, 4);
	handle->outEnd += 4;

	i32 = htonl(coord_pid);
	memcpy(handle->outBuffer + handle->outEnd, &i32, 4);
	handle->outEnd += 4;

	i32 = htonl(coord_vxid);
	memcpy(handle->outBuffer + handle->outEnd, &i32, 4);
	handle->outEnd += 4;

	RecordSendMessage(handle, 'U', NULL, 0);

	return 0;
}

void pgxc_set_coordinator_proc_pid(int proc_pid)
{
	pgxc_coordinator_proc_pid = (IS_PGXC_LOCAL_COORDINATOR ? MyProcPid : proc_pid);
}

void pgxc_set_coordinator_proc_vxid(TransactionId proc_vxid)
{
	TransactionId lxid = (MyProc != NULL ? MyProc->lxid : InvalidTransactionId);

	pgxc_coordinator_proc_vxid = (IS_PGXC_LOCAL_COORDINATOR ? lxid : proc_vxid);
}

int pgxc_get_coordinator_proc_pid(void)
{
	return (IS_PGXC_LOCAL_COORDINATOR ? MyProcPid : pgxc_coordinator_proc_pid);
}

TransactionId pgxc_get_coordinator_proc_vxid(void)
{
	TransactionId lxid = (MyProc != NULL ? MyProc->lxid : InvalidTransactionId);

	return (IS_PGXC_LOCAL_COORDINATOR ? lxid : pgxc_coordinator_proc_vxid);
}
#endif

/*
 * Send no tuple descriptor down to the PGXC node
 */
int
pgxc_node_send_no_tuple_descriptor(PGXCNodeHandle *handle)
{
	char option[2] = {'D', 'f'};
	return pgxc_node_send_option(handle, option);
}

int
pgxc_node_send_dn_direct_printtup(PGXCNodeHandle *handle)
{
	char option[2] = {'N', 't'};
	return pgxc_node_send_option(handle, option);
}

/*
 * Send the snapshot down to the PGXC node
 */
int
pgxc_node_send_snapshot(PGXCNodeHandle *handle, Snapshot snapshot)
{
	int			msglen PG_USED_FOR_ASSERTS_ONLY;

	/* gts optimize send snapshot check */
	if (enable_gts_optimize && snapshot != NULL &&
		CSN_IS_USE_LATEST(snapshot->start_ts))
	{
		elog(DEBUG1, "send use local latest gts to node: %s", handle->nodename);

		if (send_snapshot_times > 0 && strcmp(handle->nodename, send_latest_snapshot_dn) != 0)
		{
			elog(ERROR, "send use local latest gts to node: %s, More than one datanode, The previous datanode is %s",
				handle->nodename, send_latest_snapshot_dn);
		}

		if (send_snapshot_times == 0)
			strncpy(send_latest_snapshot_dn, handle->nodename, NAMEDATALEN);

		send_snapshot_times++;
	}

	/* Invalid connection state, return error */
	if (!IsConnectionStateIdle(handle))
	{
		elog(LOG,
			 "pgxc_node_send_snapshot datanode:%u invalid status:%d, "
			 "no need to send data, return NOW",
			 handle->nodeoid, GetConnectionState(handle));
		return EOF;
	}

#ifdef __SUPPORT_DISTRIBUTED_TRANSACTION__
	if (snapshot->local)
	{
		if (false == CommitTimestampIsLocal(snapshot->start_ts))
		{
			elog(ERROR, "local snapshot should have local timestamp "INT64_FORMAT, snapshot->start_ts);
		}
	}
	msglen = 20; /* 4 bytes for msglen and (8 bytes for timestamp (int64)) * 2 */
	/* msgType + msgLen */
	if (ensure_out_buffer_capacity(handle->outEnd + 1 + msglen, handle) != 0)
	{
		add_error_message(handle, "out of memory");
		return EOF;
	}

	/* elog should not be called during the changing of outBuffer to avoid incomplete msg */
	if (enable_distri_print)
	{
		elog(LOG, "send snapshot to node %s pid %d start_ts" UINT64_FORMAT " xid %u.",
			handle->nodename, handle->backend_pid, snapshot->start_ts, GetTopTransactionIdIfAny());
	}

	handle->outBuffer[handle->outEnd++] = 's';
	msglen = pg_hton32(msglen);
	memcpy(handle->outBuffer + handle->outEnd, &msglen, 4);
	handle->outEnd += 4;

	memcpy(handle->outBuffer + handle->outEnd, &snapshot->start_ts, sizeof (GlobalTimestamp));
	handle->outEnd += sizeof (GlobalTimestamp);

	/* copy this one more time for double check because gts is so important */
	memcpy(handle->outBuffer + handle->outEnd, &snapshot->start_ts, sizeof (GlobalTimestamp));
	handle->outEnd += sizeof (GlobalTimestamp);

#endif
	RecordSendMessage(handle, 's', (char *)&snapshot->start_ts, sizeof(GlobalTimestamp));

	return 0;
}

/*
 * Send the timestamp down to the PGXC node
 */
int
pgxc_node_send_prefinish_timestamp(PGXCNodeHandle *handle, GlobalTimestamp timestamp)
{
	int 	msglen = 12; /* 4 bytes for msglen and 8 bytes for timestamp (int64) */
	uint32	n32;
	int64	i = (int64) timestamp;

	/* Invalid connection state, return error */
	if (!IsConnectionStateIdle(handle))
	{
		elog(LOG, "pgxc_node_send_prefinish_timestamp datanode: %s pid %d invalid stauts:%d, no need to send data, return NOW",
				handle->nodename, handle->backend_pid, GetConnectionState(handle));
		return EOF;
	}

	elog(DEBUG8, "send prefinish timestamp " INT64_FORMAT, timestamp);
	/* msgType + msgLen */
	if (ensure_out_buffer_capacity(handle->outEnd + 1 + msglen, handle) != 0)
	{
		add_error_message(handle, "out of memory");
		return EOF;
	}

	handle->outBuffer[handle->outEnd++] = 'W';
	msglen = pg_hton32(msglen);
	memcpy(handle->outBuffer + handle->outEnd, &msglen, 4);
	handle->outEnd += 4;

	/* High order half first */
#ifdef INT64_IS_BUSTED
	/* don't try a right shift of 32 on a 32-bit word */
	n32 = (i < 0) ? -1 : 0;
#else
	n32 = (uint32) (i >> 32);
#endif
	n32 = pg_hton32(n32);
	memcpy(handle->outBuffer + handle->outEnd, &n32, 4);
	handle->outEnd += 4;

	/* Now the low order half */
	n32 = (uint32) i;
	n32 = pg_hton32(n32);
	memcpy(handle->outBuffer + handle->outEnd, &n32, 4);
	handle->outEnd += 4;
	RecordSendMessage(handle, 'W', (char *)&timestamp, 8);
	return 0;
}


/*
 * Send the timestamp down to the PGXC node
 */
int
pgxc_node_send_prepare_timestamp(PGXCNodeHandle *handle, GlobalTimestamp timestamp)
{
	int 	msglen = 12; /* 4 bytes for msglen and 8 bytes for timestamp (int64) */
	uint32	n32;
	int64	i = (int64) timestamp;

	if (!GlobalTimestampIsValid(timestamp))
	{
		elog(ERROR, "prepare timestamp is not valid for sending");
	}

	/* Invalid connection state, return error */
	if (!IsConnectionStateIdle(handle))
	{
		elog(LOG, "pgxc_node_send_prepare_timestamp datanode: %s pid %d invalid stauts:%d, no need to send data, return NOW",
				handle->nodename, handle->backend_pid, GetConnectionState(handle));
		return EOF;
	}
    
#ifdef __TWO_PHASE_TESTS__
    if (!handle->read_only)
    {
        capacity_stack = SEND_PREPARE_TIMESTAMP;
    }
#endif
	/* msgType + msgLen */
	if (ensure_out_buffer_capacity(handle->outEnd + 1 + msglen, handle) != 0)
	{
		add_error_message(handle, "out of memory");
		return EOF;
	}


	handle->outBuffer[handle->outEnd++] = 'Z';
	msglen = pg_hton32(msglen);
	memcpy(handle->outBuffer + handle->outEnd, &msglen, 4);
	handle->outEnd += 4;

	/* High order half first */
#ifdef INT64_IS_BUSTED
	/* don't try a right shift of 32 on a 32-bit word */
	n32 = (i < 0) ? -1 : 0;
#else
	n32 = (uint32) (i >> 32);
#endif
	n32 = pg_hton32(n32);
	memcpy(handle->outBuffer + handle->outEnd, &n32, 4);
	handle->outEnd += 4;

	/* Now the low order half */
	n32 = (uint32) i;
	n32 = pg_hton32(n32);
	memcpy(handle->outBuffer + handle->outEnd, &n32, 4);
	handle->outEnd += 4;

	RecordSendMessage(handle, 'Z', (char *)&timestamp, 8);
	return 0;
}

/*
 * Send the timestamp down to the PGXC node
 */
int
pgxc_node_send_global_timestamp(PGXCNodeHandle *handle, GlobalTimestamp timestamp)
{
	int 	msglen = 12; /* 4 bytes for msglen and 8 bytes for timestamp (int64) */
	uint32	n32;
	int64	i = (int64) timestamp;

	if (!GlobalTimestampIsValid(timestamp))
		elog(ERROR, "timestamp is not valid for sending");
	/* Invalid connection state, return error */
	if (!IsConnectionStateIdle(handle))
	{
		elog(LOG, "pgxc_node_send_global_timestamp datanode: %s pid %d invalid stauts:%d, no need to send data, return NOW",
				handle->nodename, handle->backend_pid, GetConnectionState(handle));
		return EOF;
	}
#ifdef __TWO_PHASE_TESTS__
    capacity_stack = SEND_COMMIT_TIMESTAMP;
#endif
	/* msgType + msgLen */
	if (ensure_out_buffer_capacity(handle->outEnd + 1 + msglen, handle) != 0)
	{
		add_error_message(handle, "out of memory");
		return EOF;
	}


	handle->outBuffer[handle->outEnd++] = 'T';
	msglen = pg_hton32(msglen);
	memcpy(handle->outBuffer + handle->outEnd, &msglen, 4);
	handle->outEnd += 4;

	/* High order half first */
#ifdef INT64_IS_BUSTED
	/* don't try a right shift of 32 on a 32-bit word */
	n32 = (i < 0) ? -1 : 0;
#else
	n32 = (uint32) (i >> 32);
#endif
	n32 = pg_hton32(n32);
	memcpy(handle->outBuffer + handle->outEnd, &n32, 4);
	handle->outEnd += 4;

	/* Now the low order half */
	n32 = (uint32) i;
	n32 = pg_hton32(n32);
	memcpy(handle->outBuffer + handle->outEnd, &n32, 4);
	handle->outEnd += 4;
	
	RecordSendMessage(handle, 'T', (char *)&timestamp, 8);
	return 0;
}

/*
 * Send the timestamp down to the PGXC node
 */
int
pgxc_node_send_timestamp(PGXCNodeHandle *handle, TimestampTz timestamp)
{
	int		msglen = 12; /* 4 bytes for msglen and 8 bytes for timestamp (int64) */
	uint32	n32;
	int64	i = (int64) timestamp;

	/* Invalid connection state, return error */
	if (!IsConnectionStateIdle(handle))
	{
		elog(WARNING, "pgxc_node_send_timestamp datanode: %s pid %d invalid stauts:%d, no need to send data, return NOW",
				handle->nodename, handle->backend_pid, GetConnectionState(handle));
		return EOF;
	}

	/* msgType + msgLen */
	if (ensure_out_buffer_capacity(handle->outEnd + 1 + msglen, handle) != 0)
	{
		add_error_message(handle, "out of memory");
		return EOF;
	}


	handle->outBuffer[handle->outEnd++] = 't';
	msglen = pg_hton32(msglen);
	memcpy(handle->outBuffer + handle->outEnd, &msglen, 4);
	handle->outEnd += 4;

	/* High order half first */
#ifdef INT64_IS_BUSTED
	/* don't try a right shift of 32 on a 32-bit word */
	n32 = (i < 0) ? -1 : 0;
#else
	n32 = (uint32) (i >> 32);
#endif
	n32 = pg_hton32(n32);
	memcpy(handle->outBuffer + handle->outEnd, &n32, 4);
	handle->outEnd += 4;

	/* Now the low order half */
	n32 = (uint32) i;
	n32 = pg_hton32(n32);
	memcpy(handle->outBuffer + handle->outEnd, &n32, 4);
	handle->outEnd += 4;

	RecordSendMessage(handle, 't', (char *)&timestamp, sizeof(TimestampTz));
	return 0;
}

void 
pgxc_copy_global_trace_id_to_history(void)
{
	memcpy(GlobalTraceIdHistory, GlobalTraceId, TRACEID_LEN);
}

/* note: do not add any log statement here, which will cause recursive call */
char *
pgxc_get_global_trace_id(void)
{
	int i = 0;
	int offset = 0;
	CommandId cmd_id = GetCurrentCommandId(false);
#define NO_GLOBAL_SESSION_ID() (0 == PGXCQueryId[0])
#define PG_CLEAN_SET_TRACEID() ('X' == GlobalTraceId[TRACEID_LEN - 1])

    if (NO_GLOBAL_SESSION_ID()
	   && !(IsClean2pcWorker() && PG_CLEAN_SET_TRACEID()))
	{
		LocalTraceId[i++] = 'Y';
		LocalTraceId[i++] = 'B';
		snprintf(LocalTraceId + i, TRACEID_LEN, "%u_%u", MyProcPid, GetTopTransactionIdIfAny());
		LocalTraceId[TRACEID_LEN - 1] = 0;
		return LocalTraceId;
	}
	else if (!IS_PGXC_LOCAL_COORDINATOR && 'N' == GlobalTraceId[0])
	{
		/* do nothing, dn node just using the cn's traceid */
	}
	else if (PG_CLEAN_SET_TRACEID())
	{
		/* do nothing, was set by pg_clean */
	}
	else
	{
		/*
		 * format of global session id "%s_%lld",
		 * PGXCNodeName, PGXCDigitalQueryId
		 */
		for (i = 0; i < NAMEDATALEN; i++)
		{
			if ('_' == PGXCQueryId[i])
			{
				break;
			}
		}
		if (i >= NAMEDATALEN - 1)
		{
			/* some wrong happened with global session id,
			 do nothing and keep silent. */
		}
		else
		{
			i++;
			GlobalTraceId[0] = 'N';
			GlobalTraceId[1] = 'C';
			memcpy(GlobalTraceId + 2, PGXCQueryId + i, strlen(PGXCQueryId) - i);
			offset = strlen(PGXCQueryId) - i + 2;
			if (InvalidTransactionId == GlobalTraceVxid
				&& 0 != pgxc_get_coordinator_proc_vxid())
			{
				GlobalTraceVxid = pgxc_get_coordinator_proc_vxid();
			}
			snprintf(GlobalTraceId + offset, TRACEID_LEN, "_%d_%d_%d_%u_%u",
					 GlobalTraceVxid,
					 GetReceivedCommandId(),
					 cmd_id,
					 GetTopTransactionIdIfAny(),
					 GetCurrentTransactionIdIfAny());
			/* the last char is reserved for pg_clean, when pg_clean read trace id from xlog, it set it to 'X' */
			GlobalTraceId[TRACEID_LEN - 2] = 0;
		}
	}
	return GlobalTraceId;
}

int
pgxc_node_send_global_traceid(PGXCNodeHandle *handle)
{
	int	msgLen = 0;

	if (0 == GlobalTraceId[0])
	{
		return 0;
	}
	
	/* size + session + '\0' */
	msgLen = 4 + strlen(GlobalTraceId) + 1;
	
	/* msgType + msgLen */
	if (ensure_out_buffer_capacity(handle->outEnd + 1 + msgLen, handle) != 0)
	{
		add_error_message(handle, "pgxc_node_send_global trace id out of memory");
		return EOF;
	}
	
	handle->outBuffer[handle->outEnd++] = 'j';		/* trace id */
	
	msgLen = htonl(msgLen);
	memcpy(handle->outBuffer + handle->outEnd, &msgLen, 4);
	handle->outEnd += 4;
	
	memcpy(handle->outBuffer + handle->outEnd, GlobalTraceId, strlen(GlobalTraceId) + 1);
	handle->outEnd += strlen(GlobalTraceId) + 1;
	RecordSendMessage(handle, 'j', GlobalTraceId, strlen(GlobalTraceId));
	return 0;
}

int
pgxc_node_set_global_traceid(char *traceid)
{
	if (NULL != traceid)
	{
		int sz = strlen(traceid);
		int maxsz = TRACEID_LEN;
		if (0 >= sz || maxsz - 1 < sz)
		{
			elog(ERROR, "parameter traceid is invalid, size: %d",sz);
		}
		else
		{
			elog(DEBUG1, "traceid to be set is: %s", traceid);
			memcpy(GlobalTraceId, traceid, sz);
			GlobalTraceId[maxsz - 1] = 0;
		}
	}
	return 0;
}

void
pgxc_node_set_global_traceid_regenerate(bool regen)
{
	/* mark the trace id is generated by pg_clean */
	GlobalTraceId[TRACEID_LEN - 1] = regen ? 0 : 'X';
}

/*
 * Send the query_id down to the PGXC node
 */
int
pgxc_node_send_queryid(PGXCNodeHandle *handle)
{
	int	msgLen = 0;
	
	/* size + sessionid_str + '\0' */
	msgLen = 4 + strlen(PGXCQueryId) + 1;
	
	/* msgType + msgLen */
	if (ensure_out_buffer_capacity(handle->outEnd + 1 + msgLen, handle) != 0)
	{
		add_error_message(handle, "pgxc_node_send_queryid out of memory");
		return EOF;
	}

	handle->outBuffer[handle->outEnd++] = 'o';		/* query id */
	
	msgLen = htonl(msgLen);
	memcpy(handle->outBuffer + handle->outEnd, &msgLen, 4);
	handle->outEnd += 4;
	
	memcpy(handle->outBuffer + handle->outEnd, PGXCQueryId, strlen(PGXCQueryId) + 1);
	handle->outEnd += strlen(PGXCQueryId) + 1;
	RecordSendMessage(handle, 'o', PGXCQueryId, strlen(PGXCQueryId));
	return 0;
}


/*
 * Add another message to the list of errors to be returned back to the client
 * at the convenient time
 */
void
add_error_message(PGXCNodeHandle *handle, const char *message)
{
	elog(DEBUG1, "Remote node \"%s\", running with pid %d returned an error: %s",
			handle->nodename, handle->backend_pid, message);

	SetTxnState(handle, TXN_STATE_ERROR);
	if (handle->error[0] && message)
	{
		int32 offset = 0;
#ifdef _PG_REGRESS_
		elog(LOG, "add_error_message node:%s, running with pid %d non first time error before append: %s, error ptr:%lx, "
				"handle->nodename=%s, handle->sock=%d, "
			  	"handle->read_only=%d, handle->state.transaction_state=%c, GetConnectionState(handle) %d",
				handle->nodename, handle->backend_pid, message, (uint64)handle->error,
				handle->nodename, handle->sock, handle->read_only, handle->state.transaction_state,
			  	GetConnectionState(handle));
#endif

		offset = strnlen(handle->error, MAX_ERROR_MSG_LENGTH);
		snprintf(handle->error + offset, MAX_ERROR_MSG_LENGTH - offset, "%s", message);

#ifdef _PG_REGRESS_
		elog(LOG, "add_error_message node:%s, running with pid %d non first time after append error: %s, ptr:%lx, "
				"handle->nodename=%s, handle->sock=%d, "
			  	"handle->read_only=%d, handle->state.transaction_state=%c, GetConnectionState(handle) %d",
				handle->nodename, handle->backend_pid, handle->error, (uint64) handle->error,
				handle->nodename, handle->sock, handle->read_only, handle->state.transaction_state,
			  	GetConnectionState(handle));
#endif		

	}
	else
	{		
		snprintf(handle->error, MAX_ERROR_MSG_LENGTH, "%s", message);

#ifdef _PG_REGRESS_
		elog(LOG, "add_error_message node:%s, running with pid %d first time error: %s, ptr:%lx, "
				"handle->nodename=%s, handle->sock=%d, "
			  	"handle->read_only=%d, handle->state.transaction_state=%c, GetConnectionState(handle) %d",
				handle->nodename, handle->backend_pid, handle->error, (uint64)handle->error,
				handle->nodename, handle->sock, handle->read_only, handle->state.transaction_state,
			  	GetConnectionState(handle));
#endif

	}
}
#ifdef __OPENTENBASE__
void
add_error_message_from_combiner(PGXCNodeHandle *handle, void *combiner_input)
{
    ResponseCombiner *combiner;

    combiner = (ResponseCombiner*)combiner_input;
    
	elog(DEBUG1, "Remote node \"%s\", running with pid %d returned an error: %s",
			handle->nodename, handle->backend_pid, combiner->errorMessage);
	
	SetTxnState(handle, TXN_STATE_ERROR);
	if (handle->error[0] && combiner->errorMessage)
	{
		int32 offset = 0;

		offset = strnlen(handle->error, MAX_ERROR_MSG_LENGTH);

        if (combiner->errorMessage && combiner->errorDetail && combiner->errorHint)
        {
            snprintf(handle->error + offset, MAX_ERROR_MSG_LENGTH - offset, "%s:%s:%s", 
                combiner->errorMessage, combiner->errorDetail, combiner->errorHint);
        }
        else if (combiner->errorMessage && combiner->errorDetail)
        {
            snprintf(handle->error + offset, MAX_ERROR_MSG_LENGTH - offset, "%s:%s", 
                combiner->errorMessage, combiner->errorDetail);
        }
        else if (combiner->errorMessage)
        {
            snprintf(handle->error + offset, MAX_ERROR_MSG_LENGTH - offset, "%s", 
                combiner->errorMessage);
        }
        
#ifdef _PG_REGRESS_
        elog(LOG, "add_error_message_from_combiner node:%s, running with pid %d non first time error: %s, error ptr:%lx, "
        		"handle->nodename=%s, handle->sock=%d, "
			  	"handle->read_only=%d, handle->state.transaction_state=%c, GetConnectionState(handle) %d",
                handle->nodename, handle->backend_pid, handle->error, (uint64)handle->error,
                handle->nodename, handle->sock, handle->read_only, handle->state.transaction_state,
			  	GetConnectionState(handle));
#endif

	}
	else if (combiner->errorMessage)
	{	
        if (combiner->errorMessage && combiner->errorDetail && combiner->errorHint)
        {
            snprintf(handle->error, MAX_ERROR_MSG_LENGTH, "%s:%s:%s", 
                combiner->errorMessage, combiner->errorDetail, combiner->errorHint);
        }
        else if (combiner->errorMessage && combiner->errorDetail)
        {
            snprintf(handle->error, MAX_ERROR_MSG_LENGTH, "%s:%s", 
                combiner->errorMessage, combiner->errorDetail);
        }
        else if (combiner->errorMessage)
        {
            snprintf(handle->error, MAX_ERROR_MSG_LENGTH, "%s", 
                combiner->errorMessage);
        }
		
#ifdef _PG_REGRESS_
		elog(LOG, "add_error_message_from_combiner node:%s, running with pid %d first time error: %s, ptr:%lx, "
				"handle->nodename=%s, handle->sock=%d, "
			  	"handle->read_only=%d, handle->state.transaction_state=%c, GetConnectionState(handle) %d",
				handle->nodename, handle->backend_pid, handle->error, (uint64)handle->error,
				handle->nodename, handle->sock, handle->read_only, handle->state.transaction_state,
			  	GetConnectionState(handle));
#endif
	}
}
#endif


static int load_balancer = 0;
/*
 * Get one of the specified nodes to query replicated data source.
 * If session already owns one or more  of the requested connection,
 * the function returns existing one to avoid contacting pooler.
 * Performs basic load balancing.
 */
PGXCNodeHandle *
get_any_handle(List *datanodelist, int level, int fid, bool write)
{
	int			i, node;
	int         j;
	PGXCNodeHandle *node_handle;

	if (level < FIRST_LEVEL)
		elog(ERROR, "invalid level %d in get_any_handle", level);

	/* sanity check */
	Assert(list_length(datanodelist) > 0);

	if (0 == list_length(datanodelist))
		ereport(PANIC,
				(errcode(ERRCODE_QUERY_CANCELED),
				 errmsg("Invalid NULL node list")));

	if (HandlesRefreshPending && DoRefreshRemoteHandles())
		ereport(ERROR,
				(errcode(ERRCODE_QUERY_CANCELED),
					errmsg("canceling transaction due to cluster configuration "
						   "reset by administrator command")));

	/* loop through local datanode handles */
	for (i = 0, node = load_balancer; i < NumDataNodes; i++, node++)
	{
		/* At the moment node is an index in the array, and we may need to wrap it */
		if (node >= NumDataNodes)
			node -= NumDataNodes;

		if (!list_member_int(datanodelist, node))
			continue;

		if (write && writable_dn_handles[node] != NULL)
		{
			node_handle = writable_dn_handles[node];

			if (node_handle->sock == NO_SOCKET)
			{
				/*
				 * The previous handle was broken but somehow the
				 * writable_dn(co)_handles is not clean, do it now.
				 */
				writable_dn_handles[node] = NULL;
			}
			else
			{
				/* good, use this handle */
				SetHandleUsed(node_handle, true);
				node_handle->fragmentlevel = level;
				node_handle->fid = fid;
				node_handle->error[0] = '\0';
				/*
				 * The node is in the list of requested nodes,
				 * set load_balancer for next time and return the handle
				 */
				load_balancer = node + 1;
				register_transaction_handles(node_handle, !write);
				return node_handle;
			}
		}

		for (j = 0; j < max_handles; j++)
		{
			node_handle = &dn_handles[node][j];

			/* see if handle is already used */
			if (node_handle->sock == NO_SOCKET || node_handle->used ||
				(InPlpgsqlFunc() && !node_handle->read_only && !write))
				break;

			/* good, we can reuse it */
			node_handle->fragmentlevel = level;
			node_handle->fid = fid;
			SetHandleUsed(node_handle, true);
			node_handle->error[0] = '\0';
			/*
			 * The node is in the list of requested nodes,
			 * set load_balancer for next time and return the handle
			 */
			load_balancer = node + 1;
			if (write)
				writable_dn_handles[node] = node_handle;

			register_transaction_handles(node_handle, !write);
			return node_handle;
		}
	}

	/*
	 * None of requested nodes is in use, need to get one from the pool.
	 * Choose one.
	 */
	for (i = 0, node = load_balancer; i < NumDataNodes; i++, node++)
	{
		/* At the moment node is an index in the array, and we may need to wrap it */
		if (node >= NumDataNodes)
			node -= NumDataNodes;

		if (!list_member_int(datanodelist, node))
			continue;

		for (j = 0; j < max_handles; j++)
		{
			node_handle = &dn_handles[node][j];

			/* Look only at empty slots, we have already checked existing handles */
			if (node_handle->sock == NO_SOCKET)
			{
				/* The node is requested */
				List   *allocate = list_make1_int(node);
				int	   *pids;
				int    *fds = NULL;
				StringInfo error_msg = NULL;
				
				/* an empty slot must be unused... */
				Assert(!node_handle->used);

				fds = PoolManagerGetConnections(allocate, NIL, &pids, &error_msg);
				if (!fds)
				{
					Assert(pids != NULL);
					ereport(ERROR,
							(errcode(ERRCODE_INSUFFICIENT_RESOURCES),
								errmsg("Failed to get pooled connections: %s", (error_msg) ? error_msg->data : ""),
								errhint("This may happen because one or more nodes are "
										"currently unreachable, either because of node or "
										"network failure.\n Its also possible that the target node "
										"may have hit the connection limit or the pooler is "
										"configured with low connections.\n Please check "
										"if all nodes are running fine and also review pooler_scale_factor, "
										"max_connections and max_pool_size configuration "
										"parameters")));
				}

				pgxc_node_init(node_handle, fds[0], pids[0]);
				node_handle->fragmentlevel = level;
				node_handle->fid = fid;
				SetHandleUsed(node_handle, true);
				node_handle->error[0] = '\0';

				elog(DEBUG1, "Established a connection with datanode \"%s\","
							 "remote backend PID %d, socket fd %d, global session %c",
					 node_handle->nodename, (int) pids[0], fds[0], 'T');

				/*
				 * set load_balancer for next time and return the handle
				 */
				load_balancer = node + 1;
				list_free(allocate);
				if (write)
					writable_dn_handles[node] = node_handle;

				register_transaction_handles(node_handle, !write);

				/* initialize the global guc options */
				pgxc_node_set_query(node_handle);
				return node_handle;
			}
		}
	}

	ereport(ERROR,
			(errcode(ERRCODE_TOO_MANY_CONNECTIONS),
				errmsg("could not get valid handles, try to enlarge "
					   "GUC setting max_handles_per_node")));
	return NULL;
}

static void
get_handles_internal(PGXCNodeAllHandles *result, List *nodelist, char nodetype,
					 int level, int fid, List **node_allocate,
					 List **node_index, bool write, bool reserve_write)
{
	int number_nodes;
	PGXCNodeHandle **node_all_handles = NULL;
	PGXCNodeHandle **node_write_handles = NULL;
	ListCell		*node_list_item;
	PGXCNodeHandle	*node_handle;
	PGXCNodeHandle **worker_handles = NULL;
	/* index of the result array */
	int			i = 0;
	int         j = 0;
	List	   *tmp_nodelist = NIL;

	if (nodetype == PGXC_NODE_DATANODE)
	{
		number_nodes = NumDataNodes;
		node_all_handles = dn_handles;
		node_write_handles = writable_dn_handles;

		if (list_length(nodelist) == 0)
		{
			/*
			 * If "nodelist" is not given, we assume that connections need to be
			 * obtained for all nodes.
			 */
			nodelist = GetAllDataNodes();
			tmp_nodelist = nodelist;
		}
	}
	else
	{
		number_nodes = NumCoords;
		node_all_handles = co_handles;
		node_write_handles = writable_co_handles;

		if (list_length(nodelist) == 0)
		{
			/*
			 * If "nodelist" is not given, we assume that connections need to be
			 * obtained for all nodes.
			 */
			nodelist = GetAllCoordNodes(false);
			tmp_nodelist = nodelist;
		}

		/*
		 * The pooler currently does not support maintaining multiple sets of
		 * connections for the same CN, but the local code actually supports
		 * this capability. Therefore, to align with the pooler, all connections
		 * to the CN are forced to be writable.
		 */
		write = true;
	}

	/*
	 * We do not have to zero the array - on success all items will be set
	 * to correct pointers, on error the array will be freed
	 */
	worker_handles = (PGXCNodeHandle **)
		palloc(list_length(nodelist) * sizeof(PGXCNodeHandle *));

	foreach(node_list_item, nodelist)
	{
		int node = lfirst_int(node_list_item);

		if (node < 0 || node >= number_nodes)
			ereport(ERROR,
					(errcode(ERRCODE_OUT_OF_MEMORY),
						errmsg("Invalid Datanode number, node number %d, max nodes %d",
							   node, NumDataNodes)));

		if (write && node_write_handles[node] != NULL)
		{
			node_handle = node_write_handles[node];

			if (node_handle->sock == NO_SOCKET)
			{
				/*
				 * The previous handle was broken but somehow the
				 * writable_dn(co)_handles is not clean, do it now.
				 */
				node_write_handles[node] = NULL;
			}
			else
			{
				/* good, use this handle */
				SetHandleUsed(node_handle, true);
				node_handle->fragmentlevel = level;
				node_handle->fid = fid;
				node_handle->error[0] = '\0';
				worker_handles[i++] = node_handle;
				continue;
			}
		}

		for (j = 0; j < max_handles; j++)
		{
			bool	found = false;
			node_handle = &node_all_handles[node][j];

			if (unlikely(!write && reserve_write && node_write_handles[node] == node_handle))
			{
				/*
				 * If there is a writable fragment in the current execution,
				 * reserve the writable connection for the writable fragment.
				 */
				found = false;
			}
			else if (node_handle->sock == NO_SOCKET)
			{
				/* an empty slot must be unused... */
				// Assert(!node_handle->used);
				*node_allocate = lappend_int(*node_allocate, node);
				*node_index = lappend_int(*node_index, j);
				found = true;
			}
			else if (InPlpgsqlFunc() && !write && !node_handle->read_only)
			{
				/*
				 * In plpgsql, there are many complex scenarios. Here, we are
				 * using stricter validation to avoid errors in connection
				 * reuse, same logic in get_any_handle.
				 */
				found = false;
			}
			else if (!node_handle->used)
			{
				found = true;
			}

			if (found)
			{
				SetHandleUsed(node_handle, true);
				node_handle->fragmentlevel = level;
				node_handle->fid = fid;
				node_handle->error[0] = '\0';
				worker_handles[i++] = node_handle;
				if (write)
					node_write_handles[node] = node_handle;

				break;
			}
		}

		if (j == max_handles)
			ereport(ERROR,
					(errcode(ERRCODE_TOO_MANY_CONNECTIONS),
						errmsg("could not get valid handles, try to enlarge "
							   "GUC setting max_handles_per_node")));
	}

	if (nodetype == PGXC_NODE_DATANODE)
	{
		result->datanode_handles = worker_handles;
		result->dn_conn_count = list_length(nodelist);
	}
	else
	{
		result->coord_handles = worker_handles;
		result->co_conn_count = list_length(nodelist);
	}

	list_free(tmp_nodelist);
}

/*
 * for specified list return array of PGXCNodeHandles
 * acquire from pool if needed.
 * the lenth of returned array is the same as of nodelist
 * For Datanodes, Special case is empty or NIL nodeList, in this case return all the nodes.
 * The returned list should be pfree'd when no longer needed.
 * For Coordinator, do not get a connection if Coordinator list is NIL,
 * Coordinator fds is returned only if transaction uses a DDL
 */
PGXCNodeAllHandles *
get_handles(List *datanodelist, List *coordlist, bool is_coord_only_query,
			bool is_global_session, int level, int fid, bool write, bool reserve_write)
{
	PGXCNodeAllHandles	*result;
	ListCell		*node_list_item;
	List			*dn_allocate = NIL;
	List			*co_allocate = NIL;
	List			*dn_index = NIL;
	List			*co_index = NIL;
	PGXCNodeHandle	*node_handle;
	int				allocate_idx = 0;
	int				allocate_count = 0;
	PGXCNodeHandle	**allocate_connections = NULL;

	if (HandlesRefreshPending && DoRefreshRemoteHandles())
		ereport(ERROR,
				(errcode(ERRCODE_QUERY_CANCELED),
					errmsg("canceling transaction due to cluster configuration "
						   "reset by administrator command")));

	result = (PGXCNodeAllHandles *) palloc0(sizeof(PGXCNodeAllHandles));

	/*
	 * Get Handles for Datanodes
	 * If node list is empty execute request on current nodes.
	 * It is also possible that the query has to be launched only on Coordinators.
	 */
	if (!is_coord_only_query)
	{
		get_handles_internal(result, datanodelist, PGXC_NODE_DATANODE, level,
							 fid, &dn_allocate, &dn_index, write, reserve_write);
	}

	/*
	 * Get Handles for Coordinators
	 * If node list is empty execute request on current nodes
	 * There are transactions where the Coordinator list is NULL Ex:COPY
	 */
	if (coordlist)
	{
		get_handles_internal(result, coordlist, PGXC_NODE_COORDINATOR, level,
							 fid, &co_allocate, &co_index, write, reserve_write);
	}

	/*
	 * Pooler can get activated even if list of Coordinator or Datanode is NULL
	 * If both lists are NIL, we don't need to call Pooler.
	 */
	if (dn_allocate || co_allocate)
	{
		int i = 0;
		int	j = 0;
		int *pids;
		int	*fds = NULL;
		StringInfo error_msg = NULL;
		allocate_count = list_length(dn_allocate) + list_length(co_allocate);

		fds = PoolManagerGetConnections(dn_allocate, co_allocate, &pids, &error_msg);
		if (!fds)
			ereport(ERROR,
					(errcode(ERRCODE_INSUFFICIENT_RESOURCES),
					    errmsg("Failed to get pooled connections: %s", (error_msg) ? error_msg->data : ""),
						errhint("This may happen because one or more nodes are "
								"currently unreachable, either because of node or "
								"network failure.\n Its also possible that the target node "
								"may have hit the connection limit or the pooler is "
								"configured with low connections.\n Please check "
								"if all nodes are running fine and also review pooler_scale_factor, "
								"max_connections and max_pool_size configuration "
								"parameters")));


		allocate_connections = (PGXCNodeHandle **)palloc0(sizeof(PGXCNodeHandle *) * allocate_count);

		if (!allocate_connections)
			elog(ERROR, "out of memory");

		/* Initialisation for Datanodes */
		if (dn_allocate)
		{
			int index = 0;
			
			i = 0;
			foreach(node_list_item, dn_allocate)
			{
				int			node = lfirst_int(node_list_item);
				int			fdsock = fds[j];
				int			be_pid = pids[j++];

				if (node < 0 || node >= NumDataNodes)
					ereport(ERROR,
							(errcode(ERRCODE_OUT_OF_MEMORY),
								errmsg("Invalid Datanode number, node number %d, max nodes %d",
									   node, NumDataNodes)));

				index = list_nth_int(dn_index, i);
				node_handle = &dn_handles[node][index];
				pgxc_node_init(node_handle, fdsock, be_pid);
				dn_handles[node][index] = *node_handle;
				allocate_connections[allocate_idx++] = node_handle;

				if (hold_first_backend && !dn_handle_acquired[node])
				{
					node_handle->holdFlag = SessionHold;
					dn_handle_acquired[node] = true;
				}

				elog(DEBUG1, "Established a connection with datanode \"%s\","
						"remote backend PID %d, socket fd %d, global session %c,"
						"nodeport %d, fid %d, level %d",
						node_handle->nodename, (int) be_pid, fdsock,
						is_global_session ? 'T' : 'F',
						node_handle->nodeport, fid, level);
				i++;
			}
		}
		/* Initialisation for Coordinators */
		if (co_allocate)
		{
			int index = 0;

			i = 0;
			foreach(node_list_item, co_allocate)
			{
				int			node = lfirst_int(node_list_item);
				int			be_pid = pids[j];
				int			fdsock = fds[j++];

				if (node < 0 || node >= NumCoords)
					ereport(ERROR,
							(errcode(ERRCODE_OUT_OF_MEMORY),
							errmsg("Invalid coordinator number, node number %d, max nodes %d", node, NumCoords)));

				index = list_nth_int(co_index, i);
				node_handle = &co_handles[node][index];
				pgxc_node_init(node_handle, fdsock, be_pid);
				co_handles[node][index] = *node_handle;
				allocate_connections[allocate_idx++] = node_handle;

				if (hold_first_backend && !cn_handle_acquired[node])
				{
					node_handle->holdFlag = SessionHold;
					cn_handle_acquired[node] = true;
				}

				elog(DEBUG1, "Established a connection with coordinator \"%s\","
						"remote backend PID %d, socket fd %d, global session %c,"
						"nodeport %d, fid %d, level %d",
						node_handle->nodename, (int) be_pid, fdsock,
						is_global_session ? 'T' : 'F',
						node_handle->nodeport, fid, level);
				i++;
			}
		}

		pfree(fds);

		if (co_allocate)
			list_free(co_allocate);
		if (dn_allocate)
			list_free(dn_allocate);
		if (dn_index)
			list_free(dn_index);
		if (co_index)
			list_free(co_index);

	}

	/* reigster the handle on the transaciton handle list before sending any request */
	register_transaction_handles_all(result, !write);

	if (dn_allocate || co_allocate)
	{
		/*
		 * We got some new connections, batch set on the remote nodes the session parameters
		 * if defined. The transaction parameter should be sent after BEGIN
		 */
		if (is_global_session)
		{
			Assert(allocate_idx == allocate_count);
			pgxc_node_batch_set_query(allocate_connections, allocate_count, true, max_session_param_cid);
		}

		if (allocate_connections)
			pfree(allocate_connections);

	}
	return result;
}

void 
handle_reset(PGXCNodeHandle *handle)
{
	handle->sock = NO_SOCKET;
	SetHandleUsed(handle, false);
	handle->sendGxidVersion = 0;
	handle->fid = InvalidFid;
	handle->fragmentlevel = 0;
	handle->sock_fatal_occurred = false;
	handle->combiner = NULL;
	handle->backend_pid = 0;
	handle->block_connection = false;
}

void
handle_close_and_reset_all(void)
{
	PGXCNodeHandle	   *handle = NULL;
	int					i;
	int					j;

	for (i = 0; i < NumDataNodes; i++)
	{
		for (j = 0; j < max_handles; j++)
		{
			handle = &dn_handles[i][j];

			if (handle->sock != NO_SOCKET)
			{
				/* it cannot continue if connection with session hold flag was released */
				if (handle->holdFlag == SessionHold && !proc_exit_inprogress)
					elog(FATAL, "connection %s pid %d with sesison hold flag was released!", handle->nodename, handle->backend_pid);

				DestroyHandle(handle);
				handle_reset(handle);
			}
		}
	}

	for (i = 0; i < NumCoords; i++)
	{
		for (j = 0; j < max_handles; j++)
		{
			handle = &co_handles[i][j];

			if (handle->sock != NO_SOCKET)
			{
				/* it cannot continue if connection with session hold flag was released */
				if (handle->holdFlag == SessionHold && !proc_exit_inprogress)
					elog(FATAL, "connection %s pid %d with sesison hold flag was released!", handle->nodename, handle->backend_pid);

				DestroyHandle(handle);
				handle_reset(handle);
			}
		}
	}
	
	DestroyDatanodeStatementsHtab();
}

/*
 * Here "current" refers to all the connections currently hold，
 * some of them are maybe not in use and can be released after
 * persistent_connections_timeout.
 */
PGXCNodeAllHandles *
get_current_handles(void)
{
	PGXCNodeAllHandles *result;
	PGXCNodeHandle	   *node_handle;
	int					i;
	int                 j;

	result = (PGXCNodeAllHandles *) palloc0(sizeof(PGXCNodeAllHandles));
	result->co_conn_count = 0;
	result->dn_conn_count = 0;
	result->datanode_handles = (PGXCNodeHandle **)
		palloc((NumDataNodes * max_handles )* sizeof(PGXCNodeHandle *));
	result->coord_handles = (PGXCNodeHandle **)
		palloc((NumCoords * max_handles) * sizeof(PGXCNodeHandle *));

	for (i = 0; i < NumDataNodes; i++)
	{
		for (j = 0; j < max_handles; j++)
		{
			node_handle = &dn_handles[i][j];
			if (node_handle->sock == NO_SOCKET)
				continue;

			result->datanode_handles[result->dn_conn_count++] = node_handle;
		}
	}

	for (i = 0; i < NumCoords; i++)
	{
		for (j = 0; j < max_handles; j++)
		{
			node_handle = &co_handles[i][j];
			if (node_handle->sock == NO_SOCKET)
				continue;

			result->coord_handles[result->co_conn_count++] = node_handle;
		}
	}

	register_transaction_handles_all(result, true);

	return result;
}

#ifdef __OPENTENBASE__
/*
 * get current transaction handles that registered in current_transaction_handles,
 * including all handles get by top transaction and all sub transactions.
 */
PGXCNodeAllHandles *
get_current_txn_handles(bool *has_error)
{
	int		i, j;
	bool	is_primary_fatal = false;

	PGXCNodeAllHandles *result = (PGXCNodeAllHandles *) palloc0(sizeof(PGXCNodeAllHandles));

	HOLD_INTERRUPTS();
	if (current_transaction_handles != NULL)
	{
		result->dn_conn_count = current_transaction_handles->dn_conn_count;
		if (result->dn_conn_count > 0)
		{
			j = 0;
			result->datanode_handles =
				(PGXCNodeHandle **)palloc(result->dn_conn_count * sizeof(PGXCNodeHandle *));
			for (i = 0; i < current_transaction_handles->dn_conn_count; i++)
			{
				PGXCNodeHandle *handle = current_transaction_handles->datanode_handles[i];
				if (IsConnectionStateFatal(handle) || handle->sock == NO_SOCKET)
				{
					if (!handle->read_only && !is_primary_fatal)
					{
						/* primamary (writable) conneciton was fatal/released in this transaction */
						is_primary_fatal = true;
						elog(WARNING, "primary datanode connection %s pid %d is released, the current transaction will be aborted",
								handle->nodename, handle->backend_pid);
					}
					continue;
				}

				result->datanode_handles[j++] = handle;
			}
			result->dn_conn_count = j;
		}

		result->co_conn_count = current_transaction_handles->co_conn_count;
		if (result->co_conn_count > 0)
		{
			j = 0;
			result->coord_handles =
				(PGXCNodeHandle **)palloc(result->co_conn_count * sizeof(PGXCNodeHandle *));

			for (i = 0; i < current_transaction_handles->co_conn_count; i++)
			{
				PGXCNodeHandle *handle = current_transaction_handles->coord_handles[i];
				if (IsConnectionStateFatal(handle) || handle->sock == NO_SOCKET)
				{
					if (!handle->read_only && !is_primary_fatal)
					{
						/* primamary (writable) conneciton was fatal/released in this transaction */
						is_primary_fatal = true;
						elog(WARNING, "primary coordinator connection %s pid %d is released, the current transaction will be aborted",
							 handle->nodename, handle->backend_pid);
					}
					continue;
				}

				result->coord_handles[j++] = handle;
			}
			result->co_conn_count = j;
		}
	}
	RESUME_INTERRUPTS();

	/*
	 * return the error state to the caller if the primary connection
	 * was released
	 */
	if (has_error)
		*has_error = is_primary_fatal;

	return result;
}

/*
 * The connection is critical and there should be some issues
 * if it corrupts.
 */
static bool
IsConnectionCritical(PGXCNodeHandle *handle)
{
	HTAB *statement_handles_htab = GetActiveDatanodeStatementHandles();

	if (cluster_ex_lock_held)
		return true;

	if (handle->holdFlag == SessionHold)
		return true;

	statement_handles_htab = GetActiveDatanodeStatementHandles();

	if (statement_handles_htab)
	{
		bool found = false;
		uint64 key = (uint64)handle;
		hash_search(statement_handles_htab, &key, HASH_FIND, &found);

		if (found)
			return true;
	}

	return false;
}

PGXCNodeAllHandles *
GetCorruptedHandles(bool *all_handles)
{
	int		i, j;
	bool	all = true;
	PGXCNodeAllHandles *result;

	if (list_length(sock_fatal_handles) == 0)
		return NULL;

	result = (PGXCNodeAllHandles *) palloc0(sizeof(PGXCNodeAllHandles));
	result->datanode_handles = (PGXCNodeHandle **)
			palloc((NumDataNodes * max_handles )* sizeof(PGXCNodeHandle *));
	result->coord_handles = (PGXCNodeHandle **)
			palloc((NumCoords * max_handles) * sizeof(PGXCNodeHandle *));

	for (i = 0; i < NumDataNodes; i++)
	{
		for (j = 0; j < max_handles; j++)
		{
			PGXCNodeHandle *handle = &dn_handles[i][j];
			if (handle->sock == NO_SOCKET)
				continue;
			else if (IsConnectionStateFatal(handle))
			{
				if (IsConnectionCritical(handle))
					elog(FATAL, "critical DN connection %s pid %d is corrupted!", handle->nodename, handle->backend_pid);

				result->datanode_handles[result->dn_conn_count++] = handle;
			}
			else  if (all)
				all = false;

			if (IsConnectionStateError(handle))
			{
				UpdateConnectionState(handle, DN_CONNECTION_STATE_IDLE);
				handle->error[0] = '\0';
			}
		}
	}

	for (i = 0; i < NumCoords; i++)
	{
		for (j = 0; j < max_handles; j++)
		{
			PGXCNodeHandle *handle = &co_handles[i][j];
			if (handle->sock == NO_SOCKET)
				continue;
			else if (IsConnectionStateFatal(handle))
			{
				if (IsConnectionCritical(handle))
					elog(FATAL, "critical CN connection %s pid %d is corrupted!", handle->nodename, handle->backend_pid);

				result->coord_handles[result->co_conn_count++] = handle;
			}
			else if (all)
				all = false;

			if (IsConnectionStateError(handle))
			{
				UpdateConnectionState(handle, DN_CONNECTION_STATE_IDLE);
				handle->error[0] = '\0';
			}
		}
	}

	*all_handles = all;
	return result;
}

/*
 * Obtain all connections that can be released
 */
PGXCNodeAllHandles *
get_releasable_handles(bool force, bool waitOnResGroup, bool *all_handles)
{
	int	i;
	int	j;
	uint64 key;
	bool found;
	bool all = true;
	PGXCNodeAllHandles *result;
	PGXCNodeHandle *node_handle;
	HTAB *statement_handles_htab = NULL;

	result = (PGXCNodeAllHandles *) palloc0(sizeof(PGXCNodeAllHandles));
	result->datanode_handles = (PGXCNodeHandle **)
			palloc((NumDataNodes * max_handles )* sizeof(PGXCNodeHandle *));
	result->coord_handles = (PGXCNodeHandle **)
			palloc((NumCoords * max_handles) * sizeof(PGXCNodeHandle *));

	if (!force || waitOnResGroup)
		statement_handles_htab = GetActiveDatanodeStatementHandles();
	else if (IS_PGXC_COORDINATOR)
	{
		/* clear the binding relationship between prepared statements and Datanodes */
		DestroyDatanodeStatementsHtab();
	}

	for (i = 0; i < NumDataNodes; i++)
	{
		for (j = 0; j < max_handles; j++)
		{
			node_handle = &dn_handles[i][j];
			if (node_handle->sock == NO_SOCKET)
			{
				node_handle->sendGxidVersion = 0;
				node_handle->fid = InvalidFid;
				node_handle->fragmentlevel = 0;
				SetHandleUsed(node_handle, false);
				node_handle->sock_fatal_occurred = false;
				node_handle->combiner = NULL;
				node_handle->block_connection = false;
				node_handle->current_user = InvalidOid;
				node_handle->session_id = InvalidSessionId;
				continue;
			}

			/*
			 * This backend cannot be released for the following reasons:
			 * 1. there are existing automatic transactions in the backend.
			 * 2. the hold flag is set to SessionHold, indicating that a temporary
			 * table has been created on it.
			 */
			if (node_handle->autoxact_level > 0 || node_handle->holdFlag == SessionHold)
			{
				if (all)
					all = false;
				continue;
			}

			if (statement_handles_htab)
			{
				key = (uint64) node_handle;
				hash_search(statement_handles_htab, &key, HASH_FIND, &found);
			}
			else
				found = false;

			/* If the connection is not found in datanode_queries, it can be released */
			if (!found)
			{
				if (waitOnResGroup)
				{
					/* When waitOnResGroup, only connections that are idle can be released */
					Assert(!force);
					if (IsTxnStateIdle(node_handle) && IsConnectionStateIdle(node_handle))
						result->datanode_handles[result->dn_conn_count++] = node_handle;
					else if (all)
						all = false;
				}
				else
					result->datanode_handles[result->dn_conn_count++] = node_handle;
			}
			else if (all)
				all = false;
		}
	}

	for (i = 0; i < NumCoords; i++)
	{
		for (j = 0; j < max_handles; j++)
		{
			node_handle = &co_handles[i][j];
			if (node_handle->sock == NO_SOCKET)
			{
				node_handle->sendGxidVersion = 0;
				node_handle->fid = InvalidFid;
				node_handle->fragmentlevel = 0;
				SetHandleUsed(node_handle, false);
				node_handle->sock_fatal_occurred = false;
				node_handle->combiner = NULL;
				node_handle->block_connection = false;
				node_handle->current_user = InvalidOid;
				node_handle->session_id = InvalidSessionId;
				continue;
			}

			if (node_handle->autoxact_level > 0 || node_handle->holdFlag == SessionHold)
			{
				if (all)
					all = false;
				continue;
			}


			if (waitOnResGroup)
			{
				/* When waitOnResGroup, only connections that are idle can be released */
				Assert(!force);
				if (IsTxnStateIdle(node_handle) && IsConnectionStateIdle(node_handle))
					result->coord_handles[result->co_conn_count++] = node_handle;
				else if (all)
					all = false;
			}
			else
				result->coord_handles[result->co_conn_count++] = node_handle;
		}
	}

	if (statement_handles_htab)
		hash_destroy(statement_handles_htab);

	if (all_handles)
		*all_handles = all;

	return result;
}

/*
 * get current sub transaction handles, the result is the same as
 * get_current_txn_handles except there some error during the start
 * of a sub transaction.
 *
 * The parameter 'all' indicates that it retrieves all handles, including
 * those obtained by the sub-transaction that have not yet sent the subtransaction
 * command to the remote nodes. These handles should be cleaned up in
 * PreAbort_Remote() upon encountering any errors.
 */
PGXCNodeAllHandles *
get_current_subtxn_handles(int subtrans_id, bool all)
{
	int 	i;
	PGXCNodeHandle	   *handle;
	PGXCNodeAllHandles *result =
		(PGXCNodeAllHandles *) palloc0(sizeof(PGXCNodeAllHandles));

	if (current_transaction_handles != NULL)
	{
		int 	dn_count = current_transaction_handles->dn_conn_count;
		int 	cn_count = current_transaction_handles->co_conn_count;
		if (dn_count > 0)
		{
			result->datanode_handles =
				(PGXCNodeHandle **)palloc(dn_count * sizeof(PGXCNodeHandle *));
			for (i = 0; i < dn_count; i++)
			{
				handle = current_transaction_handles->datanode_handles[i];
				if (GetSubTransactionId(handle) >= subtrans_id)
					result->datanode_handles[result->dn_conn_count++] = handle;
				else if (all && handle->registered_subtranid >= subtrans_id)
					result->datanode_handles[result->dn_conn_count++] = handle;
			}
		}

		if (cn_count > 0)
		{
			result->coord_handles =
				(PGXCNodeHandle **)palloc(cn_count * sizeof(PGXCNodeHandle *));
			for (i = 0; i < cn_count; i++)
			{
				handle = current_transaction_handles->coord_handles[i];
				if (GetSubTransactionId(handle) >= subtrans_id)
					result->coord_handles[result->co_conn_count++] = handle;
				else if (all && handle->registered_subtranid >= subtrans_id)
					result->coord_handles[result->co_conn_count++] = handle;
			}
		}
	}

	return result;
}

PGXCNodeAllHandles *
get_current_write_handles(void)
{
	PGXCNodeAllHandles *result;
	PGXCNodeHandle *handle;
	int	i;

	result = (PGXCNodeAllHandles *) palloc0(sizeof(PGXCNodeAllHandles));
	result->datanode_handles =
		(PGXCNodeHandle **) palloc0(sizeof(PGXCNodeHandle *) * NumDataNodes);
	result->coord_handles =
		(PGXCNodeHandle **) palloc0(sizeof(PGXCNodeHandle *) * NumCoords);

	if (writable_dn_handles)
	{
		for (i = 0; i < NumDataNodes; i++)
		{
			handle = writable_dn_handles[i];
			if (handle && handle->sock != NO_SOCKET)
				result->datanode_handles[result->dn_conn_count++] = handle;
		}
	}

	if (writable_co_handles)
	{
		for (i = 0; i < NumCoords; i++)
		{
			handle = writable_co_handles[i];
			if (handle && handle->sock != NO_SOCKET)
				result->coord_handles[result->co_conn_count++] = handle;
		}
	}

	return result;
}

static void
init_sock_fatal_handles(void)
{
	sock_fatal_handles = NIL;
}

void
reset_sock_fatal_handles(void)
{
	if (sock_fatal_handles == NIL)
		return;

	list_free(sock_fatal_handles);
	sock_fatal_handles = NIL;
}

void
register_sock_fatal_handles(PGXCNodeHandle *handle)
{
	MemoryContext oldcontext;

	/*
	 * This must use TopMemoryContext because there may still be erroneous connections
	 * during the final stages of transaction commit or rollback. To record these 
	 * connections, it is necessary to operate within the TopMemoryContext.
	 */
	oldcontext = MemoryContextSwitchTo(TopMemoryContext);

	sock_fatal_handles = lappend(sock_fatal_handles, handle);

	MemoryContextSwitchTo(oldcontext);
}

void
register_error_handles(PGXCNodeHandle *handle)
{
	MemoryContext oldcontext;
	/*
	 * This must use TopMemoryContext because there may still be erroneous connections
	 * during the final stages of transaction commit or rollback. To record these
	 * connections, it is necessary to operate within the TopMemoryContext.
	 */
	oldcontext = MemoryContextSwitchTo(TopMemoryContext);

	global_error_handles = lappend(global_error_handles, handle);

	MemoryContextSwitchTo(oldcontext);
}

void
reset_error_handles(bool only_clear)
{
	if (global_error_handles != NIL)
	{
		if (!only_clear)
		{
			ListCell *lc;
			foreach(lc, global_error_handles)
			{
				PGXCNodeHandle *handle = lfirst(lc);
				if (IsConnectionStateError(handle))
				{
					UpdateConnectionState(handle, DN_CONNECTION_STATE_IDLE);
					handle->error[0] = '\0';
				}

				if (handle->used)
					handle->used = false;
			}
		}

		list_free(global_error_handles);
		global_error_handles = NIL;
	}
}


void
report_sock_fatal_handles(void)
{
	ListCell	*lc = NULL;
	int			sock_fatal_count = 0;

	foreach(lc, sock_fatal_handles)
	{
		PGXCNodeHandle *conn = (PGXCNodeHandle *)lfirst(lc);
		elog(LOG, "report_sock_fatal_handles, fatal_conn=%p, fatal_conn->nodename=%s, fatal_conn->sock=%d, "
			"fatal_conn->read_only=%d, fatal_conn->state.transaction_state=%c, "
			"fatal_conn->sock_fatal_occurred=%d, conn->backend_pid=%d, fatal_conn->error=%s", 
			conn, conn->nodename, conn->sock, conn->read_only, conn->state.transaction_state,
			conn->sock_fatal_occurred, conn->backend_pid,  conn->error);
	}

	sock_fatal_count = list_length(sock_fatal_handles);

	if (sock_fatal_count != 0)
	{
		elog(ERROR, "Found %d sock fatal handles exist", sock_fatal_count);
	}
}

PGXCNodeAllHandles *
get_sock_fatal_handles(void)
{
	PGXCNodeAllHandles *result = NULL;
	PGXCNodeHandle	   *node_handle = NULL;
	int					i = 0;
	int 				j = 0;

	result = (PGXCNodeAllHandles *) palloc0(sizeof(PGXCNodeAllHandles));
	result->co_conn_count = 0;
	result->dn_conn_count = 0;
	result->datanode_handles = (PGXCNodeHandle **)
							   palloc((NumDataNodes * max_handles) * sizeof(PGXCNodeHandle *));
	for (i = 0; i < NumDataNodes; i++)
	{
		for (j = 0; j < max_handles; j++)
		{
			node_handle = &dn_handles[i][j];
			if (node_handle->sock_fatal_occurred == true)
				result->datanode_handles[result->dn_conn_count++] = node_handle;
		}
	}

	result->coord_handles = (PGXCNodeHandle **)
							palloc((NumCoords * max_handles) * sizeof(PGXCNodeHandle *));
	for (i = 0; i < NumCoords; i++)
	{
		for (j = 0; j < max_handles; j++)
		{
			node_handle = &co_handles[i][j];
			if (node_handle->sock_fatal_occurred == true)
				result->coord_handles[result->co_conn_count++] = node_handle;
		}
	}

	return result;
}

static void
reinit_transaction_handles(int new_handles_num)
{
	PGXCNodeAllHandles *transaction_handles_tmp = NULL;

	transaction_handles_tmp = (PGXCNodeAllHandles *) palloc0(sizeof(PGXCNodeAllHandles));

	transaction_handles_tmp->dn_conn_count = 0;
	transaction_handles_tmp->datanode_handles = (PGXCNodeHandle **) palloc0((NumDataNodes * new_handles_num) *
													sizeof(PGXCNodeHandle *));
    
	transaction_handles_tmp->co_conn_count = 0;
	transaction_handles_tmp->coord_handles = (PGXCNodeHandle **) palloc0((NumCoords * new_handles_num) *
												sizeof(PGXCNodeHandle *));

	if (current_transaction_handles != NULL)
	{
		pfree_ext(current_transaction_handles->coord_handles);
		pfree_ext(current_transaction_handles->datanode_handles);
		pfree(current_transaction_handles);
	}
	current_transaction_handles = transaction_handles_tmp;

	reset_sock_fatal_handles();
	reset_error_handles(true);
}
/*
 * init current transaction handles for connections
 */
void
init_transaction_handles(void)
{
    MemoryContext oldcontext;
    oldcontext = MemoryContextSwitchTo(TopMemoryContext);
    if (current_transaction_handles == NULL)
    {
        current_transaction_handles = (PGXCNodeAllHandles *) palloc0(sizeof(PGXCNodeAllHandles));
    }

    current_transaction_handles->dn_conn_count = 0;
    if (current_transaction_handles->datanode_handles == NULL)
    {
        current_transaction_handles->datanode_handles = (PGXCNodeHandle **) palloc((NumDataNodes * max_handles) * sizeof(PGXCNodeHandle *));
    }
    else
    {
        current_transaction_handles->datanode_handles = (PGXCNodeHandle **) repalloc(current_transaction_handles->datanode_handles, (NumDataNodes * max_handles) * sizeof(PGXCNodeHandle *));
    }

    current_transaction_handles->co_conn_count = 0;
    if (current_transaction_handles->coord_handles == NULL)
    {
        current_transaction_handles->coord_handles = (PGXCNodeHandle **) palloc((NumCoords * max_handles) * sizeof(PGXCNodeHandle *));
    }
    else
    {
        current_transaction_handles->coord_handles = (PGXCNodeHandle **) repalloc(current_transaction_handles->coord_handles, (NumCoords * max_handles) * sizeof(PGXCNodeHandle *));
    }
    MemoryContextSwitchTo(oldcontext);

	init_sock_fatal_handles();
	reset_error_handles(true);
}

/*
 * reset current transaction handles
 */
void
reset_transaction_handles(void)
{
	int 	i;

	if (current_transaction_handles == NULL)
		return;

	if ((current_transaction_handles->co_conn_count + current_transaction_handles->dn_conn_count) == 0)
		return;

	check_for_release_handles(false);

	HOLD_INTERRUPTS();

	if (current_transaction_handles != NULL)
	{
		/* reset connection properties */
		for (i = 0; i < current_transaction_handles->dn_conn_count; i++)
		{
			PGXCNodeHandle *conn = current_transaction_handles->datanode_handles[i];

			if (!IsConnectionStateIdle(conn) || !IsTxnStateIdle(conn) || conn->used)
			{
				elog(LOG, "Reset handle, state error on handle %s %d sock %d", conn->nodename, conn->backend_pid, conn->sock);
				PrintHandleState(conn);
			}

			conn->sendGxidVersion = 0;
			SetHandleUsed(conn, false);
			conn->combiner = NULL;
			conn->block_connection = false;
			conn->read_only = true;
			conn->registered = false;
			if (IsExecExtendedQuery(conn))
				conn->state.in_extended_query = false;
			current_transaction_handles->datanode_handles[i] = NULL;
		}

		for (i = 0; i < current_transaction_handles->co_conn_count; i++)
		{
			PGXCNodeHandle *conn = current_transaction_handles->coord_handles[i];

			if (!IsConnectionStateIdle(conn) || !IsTxnStateIdle(conn) || conn->used)
			{
				elog(LOG, "Reset handle, state error on handle %s %d sock %d", conn->nodename, conn->backend_pid, conn->sock);
				PrintHandleState(conn);
			}

			conn->sendGxidVersion = 0;
			SetHandleUsed(conn, false);
			conn->combiner = NULL;
			conn->block_connection = false;
			conn->read_only = true;
			conn->registered = false;
			if (IsExecExtendedQuery(conn))
				conn->state.in_extended_query = false;
			current_transaction_handles->coord_handles[i] = NULL;
		}
		/* reset current transaction handles */
		current_transaction_handles->dn_conn_count = 0;
		current_transaction_handles->co_conn_count = 0;
	}

	/* reset the record of datanodes we have written */
	current_write_datanodes = bms_clean_members(current_write_datanodes);
	reset_sock_fatal_handles();
	reset_error_handles(false);
	RESUME_INTERRUPTS();
}

/*
 * register current transaction handle to current_transaction_handles
 */
static void
register_transaction_handles(PGXCNodeHandle *handle, bool readonly)
{
	int i = 0;
	char node_type = handle->node_type;
	bool type_error = false;

	Assert (current_transaction_handles != NULL);

	if (enable_distri_print)
	{
		elog(LOG, "register_transaction_handles - handle->nodename=%s  %d readonly %d, handle->subtrans_nesting_level=%d "
				  "dn_conn_count %d, co_conn_count %d",
			 handle->nodename, handle->backend_pid, readonly, GetSubTransactionId(handle),
			 current_transaction_handles->dn_conn_count,
			 current_transaction_handles->co_conn_count);

		for (i = 0; i < current_transaction_handles->dn_conn_count; i++)
		{
			PGXCNodeHandle *h = current_transaction_handles->datanode_handles[i];
			elog(LOG, "register_transaction_handles exists dn connection: %s pid: %d", h->nodename, h->backend_pid);
			PrintHandleState(h);
		}
	}

	/* disable interrupt */
	HOLD_INTERRUPTS();

	handle->registered_subtranid = GetCurrentSubTransactionId();
	if (handle->registered)
	{
		/* it was already registred, but not as a writable connection */
		if (handle->read_only && !readonly)
		{
			handle->read_only = false;
			if (node_type == PGXC_NODE_DATANODE)
			{
				MemoryContext old = MemoryContextSwitchTo(TopMemoryContext);
				current_write_datanodes = bms_add_member(current_write_datanodes, handle->nodeidx);
				MemoryContextSwitchTo(old);
			}
		}
		goto END;
	}

	if (node_type == PGXC_NODE_DATANODE)
	{
		if (!readonly)
		{
			MemoryContext old = MemoryContextSwitchTo(TopMemoryContext);
			current_write_datanodes =
				bms_add_member(current_write_datanodes, handle->nodeidx);
			MemoryContextSwitchTo(old);
		}

		if (!IsConnectionStateIdle(handle) || !IsTxnStateIdle(handle))
		{
			if (!IsTxnStateIdle(handle))
				SetTxnState(handle, TXN_STATE_IDLE);
			elog(LOG, "Remote data node \"%s\", running with pid %d transaction_status %c or state:%s is unexpected",
				 handle->nodename, handle->backend_pid, handle->state.transaction_state, GetConnectionStateTag(handle));
		}

		if (current_transaction_handles->dn_conn_count < NumDataNodes * max_handles)
		{
			current_transaction_handles->datanode_handles[current_transaction_handles->dn_conn_count ++] = handle;
			handle->read_only = readonly;
			handle->state.subtrans_id = TopSubTransactionId;
			handle->registered = true;
		}
		else
			elog(FATAL, "current transaction datanode handle list is overflow, current index: %d, maximum index: %d",
				current_transaction_handles->dn_conn_count, NumDataNodes * max_handles);

		Assert(current_transaction_handles->dn_conn_count <= NumDataNodes * max_handles);
	}
	else if (node_type == PGXC_NODE_COORDINATOR)
	{
		if (!IsConnectionStateIdle(handle) || !IsTxnStateIdle(handle))
		{
			if (!IsTxnStateIdle(handle))
				SetTxnState(handle, TXN_STATE_IDLE);
			elog(LOG, "Remote coord node \"%s\", running with pid %d transaction_status %c or state:%s is unexpected",
				 handle->nodename, handle->backend_pid, handle->state.transaction_state, GetConnectionStateTag(handle));
		}

		if (current_transaction_handles->co_conn_count < NumCoords * max_handles)
		{
			current_transaction_handles->coord_handles[current_transaction_handles->co_conn_count++] = handle;
			handle->read_only = readonly;
			handle->state.subtrans_id = TopSubTransactionId;
			handle->registered = true;
		}
		else
			elog(FATAL, "current transaction datanode handle list is overflow, current index: %d, maximum index: %d",
				current_transaction_handles->co_conn_count, NumCoords * max_handles);

		Assert(current_transaction_handles->co_conn_count <= NumCoords * max_handles);
	}
	else
		type_error = true;

END:
	/* enable the interrupt before raising an error */
	RESUME_INTERRUPTS();

	if (type_error)
		elog(ERROR, "invalid node_type %c in register_transaction_handles", node_type);

	return;
}

void
register_transaction_handles_all(PGXCNodeAllHandles *handles, bool readonly)
{
	int		i;

	for (i = 0; i < handles->co_conn_count; i++)
		register_transaction_handles(handles->coord_handles[i], readonly);
	for (i = 0; i < handles->dn_conn_count; i++)
		register_transaction_handles(handles->datanode_handles[i], readonly);
}

#endif


/* Free PGXCNodeAllHandles structure */
void
pfree_pgxc_all_handles(PGXCNodeAllHandles *pgxc_handles)
{
	if (!pgxc_handles)
		return;

#ifdef __TWO_PHASE_TRANS__
	if (g_twophase_state.origin_handles_ptr == pgxc_handles || g_twophase_state.handles == NULL)
	{
		if (g_twophase_state.handles != NULL)
		{
			if (g_twophase_state.handles->datanode_handles)
				pfree(g_twophase_state.handles->datanode_handles);
			if (g_twophase_state.handles->coord_handles)
				pfree(g_twophase_state.handles->coord_handles);
			pfree(g_twophase_state.handles);
		}
		g_twophase_state.handles = NULL;
		g_twophase_state.origin_handles_ptr = NULL;
		g_twophase_state.datanode_index = 0;
		g_twophase_state.coord_index = 0;
		g_twophase_state.connections_num = 0;
	}
#endif

	if (pgxc_handles->datanode_handles)
	{
		pfree(pgxc_handles->datanode_handles);
		pgxc_handles->datanode_handles = NULL;
	}
	if (pgxc_handles->coord_handles)
	{
		pfree(pgxc_handles->coord_handles);
		pgxc_handles->coord_handles = NULL;
	}

	pfree(pgxc_handles);
	pgxc_handles = NULL;
}

/* Do translation for non-main cluster */

Oid PGXCGetLocalNodeOid(Oid nodeoid)
{
	
	if (false == IsPGXCMainPlane)
	{
		char *nodename;
		
		nodename = get_pgxc_nodename(nodeoid);
		nodeoid = get_pgxc_nodeoid(nodename);
		if (InvalidOid == nodeoid)
		{
			elog(ERROR, "no such node:%s on plane %s", PGXCNodeName, pgxc_plane_name(PGXCPlaneNameID));
		}
		
	}
	return nodeoid;
}

Oid PGXCGetMainNodeOid(Oid nodeoid)
{

	if (false == IsPGXCMainPlane)
	{
		char *nodename;
		
		nodename = get_pgxc_nodename(nodeoid);
		nodeoid = get_pgxc_nodeoid_extend(nodename, pgxc_plane_name(PGXCMainPlaneNameID));
		if (InvalidOid == nodeoid)
		{
			elog(ERROR, "no such node:%s on main cluster %s", PGXCNodeName, pgxc_plane_name(PGXCMainPlaneNameID));
		}
		
	}
	return nodeoid;
}

/*
 * PGXCNodeGetNodeId
 *		Look at the data cached for handles and return node position
 * 		If node type is PGXC_NODE_COORDINATOR look only in coordinator list,
 *		if node type is PGXC_NODE_DATANODE look only in datanode list,
 *		if other (assume PGXC_NODE_NODE) search both, in last case return actual
 *		node type.
 */
int
PGXCNodeGetNodeId(Oid nodeoid, char *node_type)
{
	PGXCNodeHandlesLookupEnt *entry = NULL;	
	bool			found  = false;

	
	if (NULL == node_handles_hash)
	{
		goto NOT_FOUND;
	}
	nodeoid = PGXCGetLocalNodeOid(nodeoid);
	
	entry = (PGXCNodeHandlesLookupEnt *) hash_search(node_handles_hash, &nodeoid, HASH_FIND, &found);
	if (false == found)
	{
		goto NOT_FOUND;
	}

	/* First check datanodes, they referenced more often */
	if ((node_type == NULL || (*node_type != PGXC_NODE_COORDINATOR)) &&
		entry->nodeidx >= 0 && entry->nodeidx < NumDataNodes)
	{
		if (dn_handles && dn_handles[entry->nodeidx][0].nodeoid == nodeoid)
		{
			if (node_type)
				*node_type = PGXC_NODE_DATANODE;
			return entry->nodeidx;
		}
	}

	/* Then check coordinators */
	if ((node_type == NULL || (*node_type != PGXC_NODE_DATANODE)) &&
		entry->nodeidx >= 0 && entry->nodeidx < NumCoords)
	{
		if (co_handles && co_handles[entry->nodeidx][0].nodeoid == nodeoid)
		{
			if (node_type)
				*node_type = PGXC_NODE_COORDINATOR;
			return entry->nodeidx;
		}
	}	

NOT_FOUND:
	/* Not found, have caller handling it */
	if (node_type)
		*node_type = PGXC_NODE_NONE;
	return -1;
}

/*
 * PGXCNodeGetNodeOid
 *		Look at the data cached for handles and return node Oid
 */
Oid
PGXCNodeGetNodeOid(int nodeid, char node_type)
{
	PGXCNodeHandle **handles;

	switch (node_type)
	{
		case PGXC_NODE_COORDINATOR:
			handles = co_handles;
			break;
		case PGXC_NODE_DATANODE:
			handles = dn_handles;
			break;
		default:
			/* Should not happen */
			Assert(0);
			return InvalidOid;
	}

	return handles[nodeid][0].nodeoid;
}

/*
 * pgxc_node_str
 *
 * get the name of the node
 */
Datum
pgxc_node_str(PG_FUNCTION_ARGS)
{
	if (PGXCNodeName)
	{
		Name node_name = (Name) palloc0(NAMEDATALEN);
		namestrcpy(node_name, PGXCNodeName);
		PG_RETURN_NAME(node_name);
	}
	PG_RETURN_NULL();
}

/*
 * PGXCNodeGetNodeIdFromName
 *		Return node position in handles array
 */
int
PGXCNodeGetNodeIdFromName(char *node_name, char *node_type)
{
	char *nm;
	Oid nodeoid;

	if (node_name == NULL)
	{
		if (node_type)
			*node_type = PGXC_NODE_NONE;
		return -1;
	}

	nm = str_tolower(node_name, strlen(node_name), DEFAULT_COLLATION_OID);

	nodeoid = get_pgxc_nodeoid(nm);
	pfree(nm);
	if (!OidIsValid(nodeoid))
	{
		if (node_type)
			*node_type = PGXC_NODE_NONE;
		return -1;
	}

	return PGXCNodeGetNodeId(nodeoid, node_type);
}

/*
 * Remember new value of a session or transaction parameter, and set same
 * values on newly connected remote nodes.
 */
void
session_param_list_set(bool local, const char *name, const char *value, int flags, uint64 guc_cid)
{
	List *param_list;
	MemoryContext oldcontext;
	ParamEntry *entry = NULL;

	param_list = session_param_list;
	if (session_params)
		resetStringInfo(session_params);
	oldcontext = MemoryContextSwitchTo(TopMemoryContext);

	param_list = paramlist_delete_param(param_list, name, local);

	entry = (ParamEntry *)palloc0(sizeof(ParamEntry));

	if (value)
	{
		if (strlen(value) > PARAMDATALEN)
			elog(ERROR, "The size of guc %s value is larger than %d", name, PARAMDATALEN);
		strlcpy((char *)(&entry->value), value, PARAMDATALEN);
	}
	strlcpy((char *)(&entry->name), name, NAMEDATALEN);
	entry->flags = flags;
	entry->local = local;
	entry->reset = (value == NULL);
	entry->guc_cid = guc_cid;
	param_list = lappend(param_list, entry);

	/*
	 * Special case for
	 * 	RESET SESSION AUTHORIZATION
	 * 	SET SESSION AUTHORIZATION TO DEFAULT
	 *
	 * We must also forget any SET ROLE commands since RESET SESSION
	 * AUTHORIZATION also resets current role to session default
	 */
	if ((strcmp(name, "session_authorization") == 0) && (value == NULL))
		param_list = paramlist_delete_param(param_list, "role", false);

	session_param_list = param_list;

	if (guc_cid > max_session_param_cid)
		max_session_param_cid = guc_cid;

	MemoryContextSwitchTo(oldcontext);
}


/*
 * Forget all parameter values set either for transaction or both transaction
 * and session.
 */
void
session_param_list_reset(bool only_local)
{
	if (!only_local && session_param_list)
	{
		/* need to explicitly pfree session stuff, it is in TopMemoryContext */
		list_free_deep(session_param_list);
		session_param_list = NIL;
		if (session_params)
		{
			pfree(session_params->data);
			pfree(session_params);
			session_params = NULL;
		}
	}
}

/*
 * Returns SET commands needed to initialize remote session.
 * The command may already be biult and valid, return it right away if the case.
 * Otherwise build it up.
 * To support Distributed Session machinery coordinator should generate and
 * send a distributed session identifier to remote nodes. Generate it here.
 */
char *
session_param_list_get_str(uint64 start_cid)
{
	/*
	 * If no session parameters are set and that is a coordinator we need to set
	 * global_session anyway, even if there were no other parameters.
	 * We do not want this string to disappear, so create it in the
	 * TopMemoryContext. However if we add first session parameter we will need
	 * to free the buffer and recreate it in the same context as the hash table
	 * to avoid memory leakage.
	 */
	if (session_params == NULL)
	{
		MemoryContext oldcontext = MemoryContextSwitchTo(TopMemoryContext);
		session_params = makeStringInfo();
		MemoryContextSwitchTo(oldcontext);
	}
	else
	{
		resetStringInfo(session_params);
	}

	/* If the paramstr invalid build it up */
	if (session_params->len == 0)
	{
		char	*nodename;
		int		cpid;
		bool set_search_path = false;

		/*
		 * CN will setup the initial session id from the beginning, and the
		 * followers will send global session from 'coordlxid' this make sure
		 * all participants will share the same values of 'global_session'.
		 */
		if (IS_PGXC_COORDINATOR)
		{
			nodename = PGXCNodeName;
			cpid = MyProcPid;
		}
		else
		{
			Oid		nid;

			LWLockAcquire(ProcArrayLock, LW_SHARED);
			nid = MyProc->coordId;
			cpid = MyProc->coordPid;
			LWLockRelease(ProcArrayLock);

			nodename = get_pgxc_nodename(nid);
		}

		if (start_cid == 0)
		{
			if (strcmp(global_session_string, "none") != 0)
				appendStringInfo(session_params, "SET global_session TO %s;", global_session_string);
			else
				appendStringInfo(session_params, "SET global_session TO %s_%d_%lu;",
								 nodename, cpid, SessionStartTime);
	    }
		get_set_command(session_param_list, session_params, &set_search_path, start_cid);
		appendStringInfo(session_params, "SET parentPGXCPid TO %d;",
							 MyProcPid);
		if (!set_search_path && namespace_search_path != NULL && namespace_search_path[0] != '\0')
		{
			if (namespace_search_path[0] == '"' && namespace_search_path[1] == '"')
			{
				char* value = "''";
				appendStringInfo(session_params, "SET search_path TO %s;",
							  value);
			}
			else
				appendStringInfo(session_params, "SET search_path TO %s;",
							namespace_search_path);
		}
	}
	return session_params->len == 0 ? NULL : session_params->data;
}

static ParamEntry *
paramlist_get_paramentry(List *param_list, const char *name)
{
	ListCell   *cur_item;

	if (name)
	{
		foreach(cur_item, param_list)
		{
			ParamEntry *entry = (ParamEntry *) lfirst(cur_item);

			if (strcmp(NameStr(entry->name), name) == 0)
			{
				return entry;
			}
		}
	}

	return NULL;
}

static ParamEntry *
paramentry_copy(ParamEntry * src_entry)
{
	ParamEntry *dst_entry = NULL;
	if (src_entry)
	{
		dst_entry = (ParamEntry *) palloc0(sizeof (ParamEntry));
		strlcpy((char *) (&dst_entry->name),  (char *)(&src_entry->name),   NAMEDATALEN);
		strlcpy((char *) (&dst_entry->value), (char *)(&src_entry->value), PARAMDATALEN);
		dst_entry->flags = src_entry->flags;
		dst_entry->local = src_entry->local;
		dst_entry->reset = src_entry->reset;
	}

	return dst_entry;
}

/*
 * Get SET command for transaction_isolation. It should be executed first before
 * anything, especially subtransaction.
 */
static uint8
PGXCNodeGetXactIsoLevelParam(void)
{
	ParamEntry		*isoentry;
	char			*isostr;
	List 			*p_list = GetTopTransactionGucList();

	if (!p_list)
		return XACT_ISOLATION_INVALID;

	/*
	 * Fetch from 'TopTransactionParamList' for SATRT TRANSACTION.
	 */
	isoentry = paramlist_get_paramentry(p_list, "transaction_isolation");
	if (isoentry == NULL)
	{
		isoentry = paramlist_get_paramentry(p_list, "transaction_deferrable");
		if (isoentry == NULL)
			return XACT_ISOLATION_INVALID;
		isoentry = paramentry_copy(isoentry);
	}

	isostr = isoentry->value.data;
	if (strcmp(isostr, "serializable") == 0)
	{
		/*
		 * PGXCTODO - PGXC does not support 9.1 serializable transactions yet
		 */
		return XACT_REPEATABLE_READ;
	}
	else if (strcmp(isostr, "repeatable read") == 0)
	{
		return XACT_REPEATABLE_READ;
	}
	else if (strcmp(isostr, "read committed") == 0)
	{
		return XACT_READ_COMMITTED;
	}
	else if (strcmp(isostr, "read uncommitted") == 0)
	{
		return XACT_READ_UNCOMMITTED;
	}
	else if (strcmp(isostr, "default") == 0)
	{
		return DefaultXactIsoLevel;
	}
	else
		return XACT_ISOLATION_INVALID;
}

bool
session_param_list_has_param(bool local, const char *name, const char *value, int flags)
{
    List *param_list;
    ParamEntry *entry;

    /* Get the target hash table and invalidate command string */
	param_list = session_param_list;

    entry = paramlist_get_paramentry(param_list, name);
    if (entry && value)
    {
        if (strcmp(NameStr(entry->value), value) == 0 &&
			entry->flags == flags && entry->local == local)
            return true;
    }

    return false;
}

/*
 * Send down specified query, read and discard all responses until ReadyForQuery
 */
void
pgxc_node_set_query(PGXCNodeHandle *handle)
{
	ResponseCombiner	combiner;
	char				*session_init_param_str = session_param_list_get_str(0);

	if (!session_init_param_str)
		return;

	InitResponseCombiner(&combiner, 1, COMBINE_TYPE_NONE);
	combiner.connections = (PGXCNodeHandle **) palloc(sizeof(PGXCNodeHandle *));
	combiner.connections[0] = handle;
	handle->combiner = &combiner;


	if (pgxc_node_send_query_with_internal_flag(handle, session_init_param_str) != 0)
		ereport(ERROR,
				(errcode(ERRCODE_CONNECTION_EXCEPTION),
				 errmsg("Failed to send query %s", session_init_param_str)));

	elog(DEBUG5, "pgxc_node_set_query send query %s", session_init_param_str);

	/*
	 * Now read responses until ReadyForQuery.
	 * XXX We may need to handle possible errors here.
	 */
	ProcessResponseMessage(&combiner, NULL, RESPONSE_READY, false);
	SetHandleUsed(handle, true);
	CloseCombiner(&combiner);
}

/*
 * Batch send down specified query, read and discard all responses until ReadyForQuery
 */
void
pgxc_node_batch_set_query(PGXCNodeHandle *connections[], int count, bool newconn, uint64 guc_cid)
{
	int i = 0;
	bool error = false;
	PGXCNodeHandle *handle;
	ResponseCombiner combiner;
	struct timeval timeout;
	int sent_count = 0;
	char	*session_init_param_str = session_param_list_get_str(0);

	if (!session_init_param_str)
		return;

	timeout.tv_sec = 0;
	timeout.tv_usec = 1000;

	InitResponseCombiner(&combiner, 1, COMBINE_TYPE_NONE);
	combiner.connections = (PGXCNodeHandle **) palloc(sizeof(PGXCNodeHandle *) * count);

	for (i = 0; i < count; i++)
	{
		handle = connections[i];
		if (newconn)
		{
			if (pgxc_node_send_gxid(handle) != 0)
			{
				error = true;
				count = i;
				ereport(LOG,
						(errcode(ERRCODE_CONNECTION_EXCEPTION),
								errmsg("Failed to send gxid %s to nodename %s backend_pid %d", session_init_param_str, handle->nodename, handle->backend_pid)));
				break;
			}
		}

		combiner.connections[i] = handle;
		handle->combiner = &combiner;

		if (guc_cid > GetCurrentGucCid(handle))
			SetCurrentGucCid(handle, guc_cid);

		if (pgxc_node_send_query_with_internal_flag(handle, session_init_param_str) != 0)
		{
			error = true;
			count = i;
			ereport(LOG,
					(errcode(ERRCODE_CONNECTION_EXCEPTION),
							errmsg("Failed to send query %s to nodename %s backend_pid %d", session_init_param_str, handle->nodename, handle->backend_pid)));
			break;
		}
		sent_count++;
	}

	combiner.node_count = sent_count;

	elog(DEBUG5, "pgxc_node_batch_set_query send query %s", session_init_param_str);

	if (sent_count > 0)
	{
		/* Receive Ready from DN node */
		ProcessResponseMessage(&combiner, &timeout, RESPONSE_READY, false);
	}

	if (error)
		elog(ERROR, "error in pgxc_node_batch_set_query: %s", session_init_param_str);

	for (i = 0; i < count; i++)
		SetHandleUsed(connections[i], true);

	CloseCombiner(&combiner);
}

/*
 * Setup already established connection's execution env before use the connections.
 */
int
pgxc_node_batch_set_user(PGXCNodeHandle *connections[], int count, bool insec_context, bool error_out)
{
	int		sent_count = 0;
	int		i;
	ResponseCombiner combiner;
	int elevel = error_out ? ERROR : LOG;
	bool	errors = false;

	InitResponseCombiner(&combiner, 1, COMBINE_TYPE_NONE);
	combiner.connections = (PGXCNodeHandle **) palloc(sizeof(PGXCNodeHandle *) * count);

	for (i = 0; i < count; i++)
	{
		PGXCNodeHandle *conn = connections[i];

		/* Skip empty slots */
		if (conn->sock == NO_SOCKET)
			continue;
		
		/*
		 * if the conn has not change its user, we can ignore it.
		 */
		if (!OidIsValid(conn->current_user))
			continue;		
		/*
		 * it may happened when we use get_current_handles, we just skip it,
		 * and the related backend CurrentUserId will be set correctly when used
		 * next time in pgxc_node_send_exec_env_if.
		 */
		if (conn->used)
			continue;

		if (!IsConnectionStateIdle(conn))
		{
			elog(LOG, "pgxc_node_batch_set_user datanode:%u invalid stauts:%d, no need to send data, skip it.", conn->nodeoid, GetConnectionState(conn));
			continue;
		}
		if (pgxc_node_send_cmd_seq(conn))
			ereport(elevel, (errcode(ERRCODE_INTERNAL_ERROR),
						errmsg("failed to send command sequence")));
		if (pgxc_node_send_exec_env(conn, insec_context, true))
			ereport(elevel, (errcode(ERRCODE_INTERNAL_ERROR),
						errmsg("failed to setup security context")));
		Assert(conn->outEnd);

		if (SendRequest(conn))
			elog(elevel, "failed to flush message");
		else
		{
			conn->combiner = &combiner;
			combiner.connections[sent_count++] = conn;
		}
	}

	combiner.node_count = sent_count;

	if (sent_count > 0)
		errors = ProcessResponseMessage(&combiner, NULL, RESPONSE_READY, error_out ? false : true);

	if (errors)
		elog(elevel, "pgxc_node_batch_set_user: failed to process response message");
	 /*
	  * When we are not in SecContextIsSet context, we only need send CurrentUserId once.
	  */
	for (i = 0; i < sent_count; i++)
	{
		PGXCNodeHandle *conn = combiner.connections[i];
		SetHandleUsed(conn, true);
		if (!insec_context)
			conn->current_user = InvalidOid;
	}

	if (combiner.connections)
	{
		pfree(combiner.connections);
		combiner.connections = NULL;
	}
	CloseCombiner(&combiner);
	return 0;
}

/*
 * AtEOXact_SetUser
 * Check the transaction handles whether the handle's curren_user is changed
 * and set it if needed.
 */
void
AtEOXact_SetUser(bool error_out)
{
	MemoryContext oldctx;
	PGXCNodeAllHandles *result;

	if (PG_MODE)
		return ;

	/*
	 * CurrentTransactContext will be used if not switch to another ctx,
	 * and make ContextIsEmpty(CurrentTransactionContext) evaluate to false,
	 * which will lead to AtSubCommit_Memory not delete CurrentTransactionContext
	 * if this function is called inside an pl/sql loop, may cause oom,
	 * for CurrentTranscationContext not be deleted but palloced every loop.
	 */
	oldctx = MemoryContextSwitchTo(TopTransactionContext);
	if (IsSubTransaction())
		result = get_current_subtxn_handles(GetCurrentSubTransactionId(), false);
	else
		result = get_current_txn_handles(NULL);

	pgxc_node_batch_set_user(result->coord_handles, result->co_conn_count,
							 SecContextIsSet, error_out);
	pgxc_node_batch_set_user(result->datanode_handles, result->dn_conn_count,
							 SecContextIsSet, error_out);

	pfree_pgxc_all_handles(result);

	MemoryContextSwitchTo(oldctx);
}


void
RequestInvalidateRemoteHandles(void)
{
	HandlesInvalidatePending = true;
}

void
RequestRefreshRemoteHandles(void)
{
	HandlesRefreshPending = true;
}

bool
PoolerMessagesPending(void)
{
	if (HandlesRefreshPending)
		return true;

	return false;
}

/*
 * Check HandleInvalidatePending flag
 */
void
CheckInvalidateRemoteHandles(void)
{
	if (!HandlesInvalidatePending)
		return ;

	if (DoInvalidateRemoteHandles())
		ereport(ERROR,
		        (errcode(ERRCODE_QUERY_CANCELED),
				        errmsg("canceling transaction due to cluster configuration reset by administrator command")));
}

/*
 * For all handles, mark as they are not in use and discard pending input/output
 */
static bool
DoInvalidateRemoteHandles(void)
{
	bool			result = false;

	/*
	 * Not reload until transaction is complete.
	 * That contain two condition.
	 * 1. transaction status is idle.
	 * 2. GlobalCommitTimestamp has to be invalid
	 *    which makes sure we are not in 2pc commit phase.
	 */
	if (InterruptHoldoffCount || !IsTransactionIdle() || GetGlobalCommitTimestamp() != InvalidGlobalTimestamp)
	{
		return result;
	}

	HOLD_INTERRUPTS();

	/*
   	 * Reinitialize session, it updates the shared memory table.
     * Initialize XL executor. This must be done inside a transaction block.
     */
	StartTransactionCommand();
	InitMultinodeExecutor(true);
	CommitTransactionCommand();

	/* Disconnect from the pooler to get new connection infos next time */
	PoolManagerDisconnect();

	HandlesInvalidatePending = false;
	HandlesRefreshPending = false;

	RESUME_INTERRUPTS();

	return result;
}

/*
 * Diff handles using shmem, and remove ALTERed handles
 */
static bool
DoRefreshRemoteHandles(void)
{
	List			*altered = NIL, *deleted = NIL, *added = NIL;
	Oid				*coOids, *dnOids;
	int				numCoords, numDNodes, total_nodes;
	bool			res = true;

	HandlesRefreshPending = false;

	PgxcNodeGetOids(&coOids, &dnOids, &numCoords, &numDNodes, false);

	total_nodes = numCoords + numDNodes;
	if (total_nodes > 0)
	{
		int		i;
		List   *shmoids = NIL;
		Oid	   *allOids = (Oid *)palloc(total_nodes * sizeof(Oid));

		/* build array with Oids of all nodes (coordinators first) */
		memcpy(allOids, coOids, numCoords * sizeof(Oid));
		memcpy(allOids + numCoords, dnOids, numDNodes * sizeof(Oid));

		LWLockAcquire(NodeTableLock, LW_SHARED);

		for (i = 0; i < total_nodes; i++)
		{
			NodeDefinition	*nodeDef;
			PGXCNodeHandle	*handle;

			int nid;
			Oid nodeoid;
			char ntype = PGXC_NODE_NONE;

			nodeoid = allOids[i];
			shmoids = lappend_oid(shmoids, nodeoid);

			nodeDef = PgxcNodeGetDefinition(nodeoid);
			/*
			 * identify an entry with this nodeoid. If found
			 * compare the name/host/port entries. If the name is
			 * same and other info is different, it's an ALTER.
			 * If the local entry does not exist in the shmem, it's
			 * a DELETE. If the entry from shmem does not exist
			 * locally, it's an ADDITION
			 */
			nid = PGXCNodeGetNodeId(nodeoid, &ntype);

			if (nid == -1)
			{
				/* a new node has been added to the shmem */
				added = lappend_oid(added, nodeoid);
				elog(LOG, "Node added: name (%s) host (%s) port (%d)",
					 NameStr(nodeDef->nodename), NameStr(nodeDef->nodehost),
					 nodeDef->nodeport);
			}
			else
			{
				if (ntype == PGXC_NODE_COORDINATOR)
					handle = &co_handles[nid][0];
				else if (ntype == PGXC_NODE_DATANODE)
					handle = &dn_handles[nid][0];
				else
					elog(ERROR, "Node with non-existent node type!");

				/*
				 * compare name, host, port to see if this node
				 * has been ALTERed
				 */
				if (strncmp(handle->nodename, NameStr(nodeDef->nodename), NAMEDATALEN) != 0 ||
					strncmp(handle->nodehost, NameStr(nodeDef->nodehost), NAMEDATALEN) != 0 ||
					handle->nodeport != nodeDef->nodeport)
				{
					elog(LOG, "Node altered: old name (%s) old host (%s) old port (%d)"
							" new name (%s) new host (%s) new port (%d)",
						 handle->nodename, handle->nodehost, handle->nodeport,
						 NameStr(nodeDef->nodename), NameStr(nodeDef->nodehost),
						 nodeDef->nodeport);
					altered = lappend_oid(altered, nodeoid);
				}
				/* else do nothing */
			}
			pfree(nodeDef);
		}

		/*
		 * Any entry in backend area but not in shmem means that it has
		 * been deleted
		 */
		for (i = 0; i < NumCoords; i++)
		{
			PGXCNodeHandle	*handle = &co_handles[i][0];
			Oid nodeoid = handle->nodeoid;

			if (!list_member_oid(shmoids, nodeoid))
			{
				deleted = lappend_oid(deleted, nodeoid);
				elog(LOG, "Node deleted: name (%s) host (%s) port (%d)",
					 handle->nodename, handle->nodehost, handle->nodeport);
			}
		}

		for (i = 0; i < NumDataNodes; i++)
		{
			PGXCNodeHandle	*handle = &dn_handles[i][0];
			Oid nodeoid = handle->nodeoid;

			if (!list_member_oid(shmoids, nodeoid))
			{
				deleted = lappend_oid(deleted, nodeoid);
				elog(LOG, "Node deleted: name (%s) host (%s) port (%d)",
					 handle->nodename, handle->nodehost, handle->nodeport);
			}
		}

		LWLockRelease(NodeTableLock);

		/* Release palloc'ed memory */
		pfree(coOids);
		pfree(dnOids);
		pfree(allOids);
		list_free(shmoids);
	}

	if (deleted != NIL || added != NIL)
	{
		elog(LOG, "Nodes added/deleted. Reload needed!");
		res = false;
	}

	if (altered == NIL)
	{
		elog(LOG, "No nodes altered. Returning");
		res = true;
	}
	else
		PgxcNodeRefreshBackendHandlesShmem(altered);

	list_free(altered);
	list_free(added);
	list_free(deleted);

	return res;
}

void
UpdateConnectionState_internal(PGXCNodeHandle *handle, DNConnectionState new_state, const char *filename, int lineno)
{
	bool isfatal = (new_state == DN_CONNECTION_STATE_FATAL);
	DNConnectionState old_state = GetConnectionState(handle);

	if (old_state != DN_CONNECTION_STATE_FATAL && old_state != new_state)
	{
		handle->state.connection_state = new_state;

		if (new_state == DN_CONNECTION_STATE_FATAL)
			register_sock_fatal_handles(handle);

		if (new_state == DN_CONNECTION_STATE_ERROR)
			register_error_handles(handle);

		if (new_state == DN_CONNECTION_STATE_IDLE)
			handle->block_connection = false;
	}

	if (enable_pgxcnode_message || enable_distri_print || isfatal)
	{
		elog(LOG, "Changing connection state for node %s pid: %d, state %s ==> %s, invoker %s: %d",
			 handle->nodename,
			 handle->backend_pid,
			 ConnectionStateTag[old_state],
			 ConnectionStateTag[new_state],
			 filename,
			 lineno);
		PrintHandleState(handle);
	}
}

/*
 * Do a "Diff" of backend NODE metadata and the one present in catalog
 *
 * We do this in order to identify if we should do a destructive
 * cleanup or just invalidation of some specific handles
 */
bool
PgxcNodeDiffBackendHandles(List **nodes_alter,
			   List **nodes_delete, List **nodes_add)
{
	Relation rel;
	HeapScanDesc scan;
	HeapTuple   tuple;
	int	i;
	List *altered = NIL, *added = NIL, *deleted = NIL;
	List *catoids = NIL;
	PGXCNodeHandle *handle;
	Oid	nodeoid;
	bool res = true;

	/* Get relation lock first to avoid deadlock */
	rel = heap_open(PgxcNodeRelationId, AccessShareLock);
	LWLockAcquire(NodeTableLock, LW_SHARED);

	scan = heap_beginscan(rel, SnapshotSelf, 0, NULL);
	while ((tuple = heap_getnext(scan, ForwardScanDirection)) != NULL)
	{
		Form_pgxc_node  nodeForm = (Form_pgxc_node) GETSTRUCT(tuple);
		int nid;
		Oid nodeoid;
		char ntype = PGXC_NODE_NONE;
		
		if (enable_multi_plane && nodeForm->node_plane_id != PGXCPlaneNameID)
		{
			continue;
		}
		
		nodeoid = HeapTupleGetOid(tuple);
		catoids = lappend_oid(catoids, nodeoid);

		/*
		 * identify an entry with this nodeoid. If found
		 * compare the name/host/port entries. If the name is
		 * same and other info is different, it's an ALTER.
		 * If the local entry does not exist in the catalog, it's
		 * a DELETE. If the entry from catalog does not exist
		 * locally, it's an ADDITION
		 */
		nid = PGXCNodeGetNodeId(nodeoid, &ntype);

		if (nid == -1)
		{
			/* a new node has been added to the catalog */
			added = lappend_oid(added, nodeoid);
			elog(LOG, "Node added: name (%s) host (%s) port (%d)",
				 NameStr(nodeForm->node_name), NameStr(nodeForm->node_host),
				 nodeForm->node_port);
		}
		else
		{
			if (ntype == PGXC_NODE_COORDINATOR)
				handle = &co_handles[nid][0];
			else if (ntype == PGXC_NODE_DATANODE)
				handle = &dn_handles[nid][0];
			else
				elog(ERROR, "Node with non-existent node type!");

			/*
			 * compare name, host, port to see if this node
			 * has been ALTERed
			 */
			if (strncmp(handle->nodename, NameStr(nodeForm->node_name), NAMEDATALEN)
				!= 0 ||
				strncmp(handle->nodehost, NameStr(nodeForm->node_host), NAMEDATALEN)
				!= 0 ||
				handle->nodeport != nodeForm->node_port)
			{
				elog(LOG, "Node altered: old name (%s) old host (%s) old port (%d)"
						" new name (%s) new host (%s) new port (%d)",
					 handle->nodename, handle->nodehost, handle->nodeport,
					 NameStr(nodeForm->node_name), NameStr(nodeForm->node_host),
					 nodeForm->node_port);
				/*
				 * If this node itself is being altered, then we need to
				 * resort to a reload. Check so..
				 */
				if (pg_strcasecmp(PGXCNodeName,
								  NameStr(nodeForm->node_name)) == 0)
				{
					res = false;
				}
				altered = lappend_oid(altered, nodeoid);
			}
			/* else do nothing */
		}
	}
	heap_endscan(scan);

	/*
	 * Any entry in backend area but not in catalog means that it has
	 * been deleted
	 */
	for (i = 0; i < NumCoords; i++)
	{
		handle = &co_handles[i][0];
		nodeoid = handle->nodeoid;
		if (!list_member_oid(catoids, nodeoid))
		{
			deleted = lappend_oid(deleted, nodeoid);
			elog(LOG, "Node deleted: name (%s) host (%s) port (%d)",
				 handle->nodename, handle->nodehost, handle->nodeport);
		}
	}
	for (i = 0; i < NumDataNodes; i++)
	{
		handle = &dn_handles[i][0];
		nodeoid = handle->nodeoid;
		if (!list_member_oid(catoids, nodeoid))
		{
			deleted = lappend_oid(deleted, nodeoid);
			elog(LOG, "Node deleted: name (%s) host (%s) port (%d)",
				 handle->nodename, handle->nodehost, handle->nodeport);
		}
	}

	LWLockRelease(NodeTableLock);
	heap_close(rel, AccessShareLock);

	if (nodes_alter)
		*nodes_alter = altered;
	if (nodes_delete)
		*nodes_delete = deleted;
	if (nodes_add)
		*nodes_add = added;

	if (catoids)
		list_free(catoids);

	return res;
}

/*
 * Refresh specific backend handles associated with
 * nodes in the "nodes_alter" list below
 *
 * The handles are refreshed using shared memory
 */
void
PgxcNodeRefreshBackendHandlesShmem(List *nodes_alter)
{
	int i;
	ListCell *lc;
	Oid nodeoid;
	int nid;
	PGXCNodeHandle *handle = NULL;

	foreach(lc, nodes_alter)
	{
		char ntype = PGXC_NODE_NONE;
		NodeDefinition *nodedef;

		nodeoid = lfirst_oid(lc);
		nid = PGXCNodeGetNodeId(nodeoid, &ntype);

		if (nid == -1)
			elog(ERROR, "Looks like node metadata changed again");
		else
		{
			for (i = 0; i < max_handles; i++)
			{
				if (ntype == PGXC_NODE_COORDINATOR)
					handle = &co_handles[nid][i];
				else if (ntype == PGXC_NODE_DATANODE)
					handle = &dn_handles[nid][i];
				else
					elog(ERROR, "Node with non-existent node type!");
			
				/*
				 * Update the local backend handle data with data from catalog
				 * Free the handle first..
				 */
				pgxc_node_free(handle);
				elog(LOG, "Backend (%u), Node (%s) updated locally",
					 MyBackendId, handle->nodename);
				nodedef = PgxcNodeGetDefinition(nodeoid);
				strncpy(handle->nodename, NameStr(nodedef->nodename), NAMEDATALEN);
				strncpy(handle->nodehost, NameStr(nodedef->nodehost), NAMEDATALEN);
				handle->nodeport = nodedef->nodeport;
				pfree(nodedef);
			}
		}
	}
}

void
HandlePoolerMessages(void)
{
	if (HandlesRefreshPending)
	{
		DoRefreshRemoteHandles();

		elog(LOG, "Backend (%u), doing handles refresh", MyBackendId);
	}
}
#ifdef __OPENTENBASE__
/*
 * Send SET query to given connection.
 * Query is sent asynchronously and results are consumed
 */
int
PGXCNodeSendSetQuery(NODE_CONNECTION *conn, const char *sql_command, char *errmsg_buf, int32 buf_len, SendSetQueryStatus* status, CommandId *cmdId)
{
	int          error = 0;
	int          res_status;
	PGresult	*result;
	time_t		 now = time(NULL);
	bool 		 expired = false;
#define EXPIRE_STRING 	  "timeout expired"	
#define EXPIRE_STRING_LEN 15

	/* set default status to ok */
	*status = SendSetQuery_OK;
	*cmdId = InvalidCommandId;
	
	if (!PQsendQuery((PGconn *) conn, sql_command))
	{
		*status = SendSetQuery_SendQuery_ERROR;
		
		return -1;
	}
	
	/* Consume results from SET commands */
	while ((result = PQgetResultTimed((PGconn *) conn, now + PGXC_RESULT_TIME_OUT)) != NULL)
	{
		res_status = PQresultStatus(result);
		if (res_status != PGRES_TUPLES_OK && res_status != PGRES_COMMAND_OK)
	    {
			if (errmsg_buf && buf_len)
			{
				snprintf(errmsg_buf, buf_len, "%s !", PQerrorMessage((PGconn *) conn));
			}
			error++;

			/* Expired when set */
			if (strncmp(PQerrorMessage((PGconn *) conn), EXPIRE_STRING, EXPIRE_STRING_LEN) == 0)
			{
				expired = true;
			}
	    }
		
		*cmdId = PQresultCommandId(result);		
		
		/* TODO: Check that results are of type 'S' */
		PQclear(result);
	}

	if (expired)
		*status = SendSetQuery_EXPIRED;
	else if (error)
		*status = SendSetQuery_Set_ERROR;

	return error ? -1 : 0;
}

bool node_ready_for_query(PGXCNodeHandle *conn) 
{
	return ('Z' == (conn)->last_command);
}
#endif

#ifdef __AUDIT__
const char *
PGXCNodeTypeString(char node_type)
{
	switch (node_type)
	{
		case PGXC_NODE_COORDINATOR:
			return "Coordinator";
			break;
		case PGXC_NODE_DATANODE:
			return "Datanode";
			break;
		case PGXC_NODE_NONE:
			return "None";
			break;
		case PGXC_NODE_GTM:
			return "Gtm";
			break;	
		default:
			return "Unknown";
			break;
	}

	return "Unknown";
}

#endif

#ifdef __OPENTENBASE__
/*
 * In CENTRALIZED_MODE, the datanode slaves could process query work, 
 * so the dn_handles includes these datanode slaves.
 * When executing DDL/IUD of DML, it should get the master datanode.
 */
int PGXCGetAllMasterDnOid(Oid *nodelist)
{
	Oid	node_oid;
	int	i = 0;
	int	j = 0;
#ifdef __SUPPORT_CENTRALIZED_MODE__
	char	*node_cluster_name = NULL;
#endif

	for (i = 0; i < NumDataNodes; i++)
	{
		node_oid  = dn_handles[i][0].nodeoid;

#ifdef __SUPPORT_CENTRALIZED_MODE__
		node_cluster_name = get_pgxc_nodeclustername(node_oid);
		if (0 != pg_strcasecmp(PGXCDefaultClusterName, node_cluster_name))
		{
			pfree(node_cluster_name);
			continue;
		}

		pfree(node_cluster_name);
#endif

		nodelist[j++] = node_oid;
	}

	return j;
}

/*
 * In CENTRALIZED_MODE, When getting other cns, it should filter the slaves.
 */
int PGXCGetOtherMasterCnOid(Oid *nodelist)
{
	Oid node_oid;
	int i = 0;
	int j = 0;
#ifdef __SUPPORT_CENTRALIZED_MODE__
	char	*node_cluster_name = NULL;
#endif

	for (i = 0; i < NumCoords; i++)
	{
		node_oid  = co_handles[i][0].nodeoid;
		if (co_handles[PGXCNodeId - 1][0].nodeoid == node_oid)
			continue;

#ifdef __SUPPORT_CENTRALIZED_MODE__
		node_cluster_name = get_pgxc_nodeclustername(node_oid);
		if (0 != pg_strcasecmp(PGXCDefaultClusterName, node_cluster_name))
		{
			pfree(node_cluster_name);
			continue;
		}

		pfree(node_cluster_name);
#endif

		nodelist[j++] = node_oid;
	}

	return j;
}
#endif /*__OPENTENBASE__*/

#ifdef __AUDIT_FGA__
void PGXCGetCoordOidOthers(Oid *nodelist)
{
    Oid     node_oid; 
    int     i;
    int     j = 0;
	
    for (i = 0; i < NumCoords; i++)
    {
        node_oid  = co_handles[i][0].nodeoid;
        if (co_handles[PGXCNodeId - 1][0].nodeoid != node_oid)
        {
            nodelist[j] = node_oid;
            j++;
        }
    }

    return ;
}

void
PGXCGetAllDnOid(Oid *nodelist)
{
    Oid     node_oid;
    int     i;

    for (i = 0; i < NumDataNodes; i++)
    {
        node_oid  = dn_handles[i][0].nodeoid;
        nodelist[i] = node_oid;
    }
}

#endif

#ifdef __OPENTENBASE__
/*
 * Return the name of ascii-minimized coordinator as ddl leader cn
 */
inline PGXCNodeHandle*
find_ddl_leader_cn(void)
{
	int i = 0;
	char nodename[NAMEDATALEN] = "";
	PGXCNodeHandle *result = NULL;

	for (i = 0; i < NumCoords; i++)
	{
		if (nodename[0] == '\0' || strcmp(co_handles[i][0].nodename, nodename) < 0)
		{
			strcpy(nodename, co_handles[i][0].nodename);
			result = &co_handles[i][0];
		}
	}

	return result;
}

/*
 * Return whether I am the leader cn
 */
inline bool
is_ddl_leader_cn(PGXCNodeHandle * handle)
{
	if (handle == NULL)
		return false;

	return strcmp(handle->nodename, PGXCNodeName) == 0;
}

PGXCNodeHandle *
find_leader_cn_with_name(char *name)
{
	int i = 0;
	PGXCNodeHandle *result = NULL;

	for (i = 0; i < NumCoords; i++)
	{
		if (strcmp(name, co_handles[i][0].nodename) == 0)
		{
			result = &co_handles[i][0];
			break;
		}
	}
	return result;
}

inline PGXCNodeHandle*
find_leader_cn(void)
{
	if (resource_group_leader_coordinator != NULL)
	{
		return find_leader_cn_with_name(resource_group_leader_coordinator);
	}
	else
		return find_ddl_leader_cn();
}

/*
 * Return true if can connect to the leader cn
 */
bool
is_leader_cn_avaialble(PGXCNodeHandle * handle)
{
	PGXCNodeAllHandles *allhandle = NULL;
	List *remote_cn_list = NULL;
	List *co_allocate = NIL;
	List *co_index = NIL;
	int i = 0;

	allhandle = (PGXCNodeAllHandles *) palloc0(sizeof(PGXCNodeAllHandles));

	for (i = 0; i < NumCoords; i++)
	{
		if (handle == &co_handles[i][0])
			break;
	}
	remote_cn_list = list_make1_int(i);

	get_handles_internal(allhandle, remote_cn_list, PGXC_NODE_COORDINATOR, FIRST_LEVEL,
					 InvalidFid, &co_allocate, &co_index, true, false);

	pfree(allhandle);
	if (co_allocate)
	{
		int *pids;
		int	*fds = NULL;
		StringInfo error_msg = NULL;

		fds = PoolManagerGetConnections(NIL, co_allocate, &pids, &error_msg);
		if (!fds)
			return false;
	}
	else
		return true;

	return true;
}
#endif


char *
PGXCNodeGetNodename(int nodeid, char node_type)
{
	PGXCNodeHandle **handles;
	if (nodeid <= 0)
		return "Unknown";
	switch (node_type)
	{
		case PGXC_NODE_COORDINATOR:
			handles = co_handles;
			break;
		case PGXC_NODE_DATANODE:
			handles = dn_handles;
			break;
		default:
			/* Should not happen */
			Assert(0);
			return "Unknown";
	}

	return handles[nodeid-1][0].nodename;
}

/*
 * Return whether I am the global deadlock detector runner
 * (the name of ascii-minimized coordinator)
 */
bool
is_gdd_runner_cn(void)
{
    int i = 0;
	int j = 0;

    for (i = 0; i < NumCoords; i++)
    {
		for (j = 0; j < max_handles; j++)
		{
			if (strcmp(co_handles[i][j].nodename, PGXCNodeName) > 0)
			{
				return false;
			}
		}

    }
    return true;
}


Size
PgxcQueryIdShmemSize(void)
{
	Size size = 0;
	/* for query_id */
	size = add_size(size, sizeof(pg_atomic_uint64));
	return size;
}

void
CreateSharedPgxcQueryId(void)
{
	query_id = ShmemAlloc(sizeof(pg_atomic_uint64));
	pg_atomic_init_u64(query_id, 1);
}

void
RefreshQueryId(void)
{
	PGXCDigitalQueryId = pg_atomic_add_fetch_u64(query_id, 1);
	sprintf(PGXCQueryId, "%s_%lld", PGXCNodeName,
			(long long int) PGXCDigitalQueryId);
	pgstat_refresh_queryid();
}

void
CNSendResGroupReq(PGXCNodeHandle *handle)
{
	int i;
	List *remote_cn_list = NULL;
	PGXCNodeAllHandles *all_handles;
	ResponseCombiner combiner;
	struct timeval timeo = {0, 0};
	struct timeval *timeout = NULL;

	if (PersistentConnectionsTimeout > 0)
	{
		timeout = &timeo;
		timeout->tv_usec = PersistentConnectionsTimeout * 1000;
	}

	for (i = 0; i < NumCoords; i++)
	{
		if (strcmp(co_handles[i][0].nodename, handle->nodename) == 0)
			break;
	}

	if (i == NumCoords)
		elog(ERROR, "failed to find remote coordinator index");

	remote_cn_list = list_make1_int(i);

	all_handles = get_handles(NIL, remote_cn_list, true, true, FIRST_LEVEL, InvalidFid, true, false);

	handle = all_handles->coord_handles[0];

	pgxc_node_send_resgreq(handle);

	if (SendRequest(handle))
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
						errmsg("Failed to send command"),
						errnode(handle)));

	InitResponseCombiner(&combiner, 1, COMBINE_TYPE_NONE);

	while (true)
	{
		int res = pgxc_node_receive(1, &handle, timeout);
		if (DNStatus_EXPIRED == res)
		{
			/*
			 * To avoid excessive connections, release idle connections
			 * while the resource group is waiting, release if timeout occurs
			 * when applying for a resource group remotely
			 */
			if (!temp_object_included && PersistentConnectionsTimeout > 0)
				pgxc_node_cleanup_and_release_handles(false, true);

			/* Just release the connection at the first timeout */
			timeout = NULL;
			continue;
		}
		else if (res)
			elog(ERROR, "Failed to receive response from leader CN");

		res = handle_response(handle, &combiner);
		if (res == RESPONSE_COMPLETE || res == RESPONSE_ERROR)
		{
			break;
		}
	}

	ValidateAndCloseCombiner(&combiner);
	pfree_pgxc_all_handles(all_handles);
}

char *
get_nodename_from_nodeid(int nodeid, char node_type)
{
	if (PGXC_NODE_COORDINATOR == node_type)
	{
		if (nodeid >= NumCoords)
		{
			return NULL;
		}
		return co_handles[nodeid][0].nodename;
	}
	else if (PGXC_NODE_DATANODE == node_type)
	{
		if (nodeid >= NumDataNodes)
		{
			return NULL;
		}
		return dn_handles[nodeid][0].nodename;
	}

	return NULL;
}

int
pgxc_node_send_dml(PGXCNodeHandle *handle, CmdType cmdtype, char *relname,
							char *space, HeapTuple ituple, HeapTuple dtuple,
							ItemPointer target_ctid)
{
	MinimalTuple imtuple = NULL;
	MinimalTuple dmtuple = NULL;
	/* msglen + cmdtype + relname + spacename + ituple len + dtuple len + target_ctid*/
	int msgLen = 4 + 2 + strlen(relname) + 1 + strlen(space) + 1 + 4 + 4 + sizeof(ItemPointerData);
	uint16 n16 = htons(cmdtype);
	uint32 n32;
	
	if (ituple != NULL)
		imtuple = minimal_tuple_from_heap_tuple(ituple);
	if (dtuple != NULL)
		dmtuple = minimal_tuple_from_heap_tuple(dtuple);
	
	msgLen += imtuple ? imtuple->t_len : 0;
	msgLen += dmtuple ? dmtuple->t_len : 0;
	
	if (ensure_out_buffer_capacity(handle->outEnd + 1 + msgLen, handle) != 0)
	{
		add_error_message(handle, "out of memory");
		return EOF;
	}


	handle->outBuffer[handle->outEnd++] = 'm';
	msgLen = htonl(msgLen);
	memcpy(handle->outBuffer + handle->outEnd, &msgLen, 4);
	handle->outEnd += 4;
	memcpy(handle->outBuffer + handle->outEnd, &n16, 2);
	handle->outEnd += 2;
	memcpy(handle->outBuffer + handle->outEnd, relname, strlen(relname) + 1);
	handle->outEnd += strlen(relname) + 1;
	memcpy(handle->outBuffer + handle->outEnd, space, strlen(space) + 1);
	handle->outEnd += strlen(space) + 1;
	
	if (imtuple != NULL)
	{
		n32 = htonl(imtuple->t_len);
		memcpy(handle->outBuffer + handle->outEnd, &n32, 4);
		handle->outEnd += 4;
		memcpy(handle->outBuffer + handle->outEnd, imtuple, imtuple->t_len);
		handle->outEnd += imtuple->t_len;
	}
	else
	{
		memset(handle->outBuffer + handle->outEnd, 0, 4);
		handle->outEnd += 4;
	}
	
	if (dmtuple != NULL)
	{
		n32 = htonl(dmtuple->t_len);
		memcpy(handle->outBuffer + handle->outEnd, &n32, 4);
		handle->outEnd += 4;
		memcpy(handle->outBuffer + handle->outEnd, dmtuple, dmtuple->t_len);
		handle->outEnd += dmtuple->t_len;
	}
	else
	{
		memset(handle->outBuffer + handle->outEnd, 0, 4);
		handle->outEnd += 4;
	}

	if (target_ctid == NULL)
		memset(handle->outBuffer + handle->outEnd, 0, sizeof(ItemPointerData));
	else
		memcpy(handle->outBuffer + handle->outEnd, target_ctid, sizeof(ItemPointerData));
	handle->outEnd += sizeof(ItemPointerData);
	SetTransitionInExtendedQuery(handle, false);

	return SendRequest(handle);
}
/* pgxc_make_param
 *
 * Helper function to make a parameter
 */
Param *
pgxc_make_param(int param_num, Oid param_type)
{
    Param *param;

    param = makeNode(Param);
    /* Parameter values are supplied from outside the plan */
    param->paramkind = PARAM_EXTERN;
    /* Parameters are numbered from 1 to n */
    param->paramid = param_num;
    param->paramtype = param_type;
    /* The following members are not required for deparsing */
    param->paramtypmod = -1;
    param->paramcollid = InvalidOid;
    param->location = -1;

    return param;
}
/*
 * flag:
 *     true   ---  send
 *     false  ---  receive
 *  This function dump network data to pg_log/xxx.csv
 */
static void
pgxc_node_dump_message(PGXCNodeHandle *handle, char *ptr, int len, bool isSend)
{
#define MAX_MSG_SIZE_TO_PARSE 1024
#define MSG_SIZE_FILED_LEN 4
	int         i;
	bool        aln = false;
	bool        quo = false;
	int32       size = 0;
	int32       processed = 0;
	Timestamp   tms = 0;
	int         sock = handle->sock;
	unsigned char *pp = (unsigned char *)ptr;
	unsigned char *end = pp + len;
	unsigned char *pcursor = pp;
	StringInfoData buf;


	if (len <= 0)
	{
		elog(LOG, "\n==> NetworkDump: %5s [ %s PID: %d FD %-4d ] LEN: %d ==>\n",
			 isSend ? "SEND to" : "RECV from", handle->nodename, handle->backend_pid, sock, len);
		return;
	}

	initStringInfo(&buf);
	appendStringInfo(&buf, "\n==> NetworkDump: %5s [ %s PID: %d FD %-4d] LEN: %d >>>\n",
					 isSend ? "SEND to" : "RECV from", handle->nodename, handle->backend_pid, sock, len);

	while (pcursor < end)
	{
		unsigned char cmd;

		pp = pcursor;
		cmd = *pp++;

		if ((end - pp ) >= MSG_SIZE_FILED_LEN)
		{
			size = *(int *)pp;
			size = ntohl(size);
			pp += MSG_SIZE_FILED_LEN;
			quo = false;
			processed = pp - (unsigned char *)ptr;

			if (!isalpha(cmd) && !isalnum(cmd))
			{
				appendStringInfo(&buf, "[WARNING] Unrecognized command type: %d, size: %d\n", cmd, size);
				elog(WARNING, "unrecognized command type, pp: %p, pcursor: %p, processed: %d", pp, pcursor, processed);
				break;
			}
			appendStringInfo(&buf, " CMD: %c, msg len: %-5u: [", cmd, size - 4);
			pcursor += size + 1;
		}
		else
		{
			appendStringInfo(&buf, "[WARNING] CMD: %d, size: No size filed found", cmd);
			break;
		}

		/* A size bug, not yet solved*/
		if (size > 0xffff0000)
		{
			appendStringInfo(&buf, "Invalid size, size: %d, pp: %p, end: %p]\n", size, pp, end);
			break;
		}

		if (size > (len + MSG_SIZE_FILED_LEN - processed))
		{
			appendStringInfo(&buf, "[WARNING] Message size is greater than package length, msg size: %d, rest size: %d\n",
							 size, len - processed);
			break;
		}
		/* not dump all data for saving disk */
		if (size > MAX_MSG_SIZE_TO_PARSE && cmd != 'p' && cmd != 'Q')
		{
			appendStringInfo(&buf, " Data is too large, size: %d ]\n", size);
			pp += size;
			continue;
		}
		if (isSend)
		{
			bool is_continue = true;
			switch(cmd)
			{
				case 't':
				case 'T':
				{
					tms = *(int64 *) pp;
					tms = be64toh(tms);
					appendStringInfo(&buf, "%sTimestamp(8B): %zu ]\n", cmd == 't' ? " " : " Global ", tms);
					pp += 8;
					break;
				}
				case 's':
				{
					tms = *(int64 *)pp;
					tms = be64toh(tms);
					appendStringInfo(&buf, " Snapshot(8B): %zu ]\n", tms);
					pp += 8;
					break;
				}
				case 'M':
				{
					int cmdid = *(int *)pp;
					cmdid = ntohl(cmdid);
					appendStringInfo(&buf, " Command id(4B): %d ]\n", cmdid);
					pp += 4;
					break;
				}
				case 'U':
				{
					int pid = *(int *)pp;
					int vxid = 0;
					pid = ntohl(pid);
					pp += 4;
					vxid = *(int *)pp;
					vxid = ntohl(vxid);
					appendStringInfo(&buf, " CN pid(4B): %d, vxid(4B): %d ]\n", pid, vxid);
					pp += 4;
					break;
				}
				case 'p':
				{
					appendStringInfo(&buf, " Execution plan, size: %d ]\n", size);
					pp += size;
					break;
				}
				case 'k':
				{
					int kind = *pp;
					int iso = *++pp;
					appendStringInfo(&buf, " Transaction command: %s, tran type: %d, iso: %dsize: %d ]\n",
										getTranStmtKindTag(kind), kind, iso, size);
					pp += size;
					break;
				}
				case 'N':
				{
					uint8 sno = (uint8)*pp;
					appendStringInfo(&buf, " Sequence number: %d ]\n", sno);
					pp += size;
					break;
				}
				case 'Q':
				{
					if (size < MAX_MSG_SIZE_TO_PARSE)
						appendStringInfo(&buf, " %s ]\n", pp);
					else
					{
						char temp_buf[MAX_MSG_SIZE_TO_PARSE + 1] = {0};
						memcpy(temp_buf, pp, MAX_MSG_SIZE_TO_PARSE);
						appendStringInfo(&buf, " %s ]\n", temp_buf);
					}
					pp += size;
					break;
				}
				default:
					is_continue = false;
					break;
			}

			/* if the cursor is already advanced, just continue */
			if (is_continue)
				continue;
		}
		else
		{
			bool is_continue = true;
			/*
			 * 1 - ParseComplete
			 * 2 - BindComplete
			 * 3 - CLOSE complete
			 * */
			if (cmd >= 1 && cmd <= 3)
			{
				Assert(size == 0);
			}
			switch (cmd)
			{
				case '9':
				{
					uint8 sno = (uint8) *pp;
					appendStringInfo(&buf, " Sequence number: %d ]\n", sno);
					pp += size;
					break;
				}
				default:
					is_continue = false;
					break;
			}

			if (is_continue)
				continue;
		}

		for (i = 0; i < size - 4; i++)
		{
			/* print visible character [32, 127) */
			if (isprint(pp[i]))
			{
				/* last character is alphabet or digital? */
				if (aln)
				{
					appendStringInfoChar(&buf, pp[i]);
				}
				else
				{
					appendStringInfo(&buf, " \"%c", pp[i]);
					quo = true;
				}
				aln = true;
			}
			else
			{
				if (aln)
				{
					appendStringInfo(&buf, "\" %02X", (unsigned char)pp[i]);
					quo = false;
				}
				else
				{
					appendStringInfo(&buf, " %02X", (unsigned char)pp[i]);
				}
				aln = false;
			}
		}
		if (quo)
		{
			appendStringInfo(&buf, "\" ]\n");
		}
		else
		{
			appendStringInfo(&buf, " ]\n");
		}

		pp += (size - MSG_SIZE_FILED_LEN);
	} /* end while */

	appendStringInfo(&buf, "<<< END, state: %s, txn: %c, stxnid: %d, sno: %d(rs:%d), ex: %d, used: %d, readonly: %d",
					GetConnectionStateTag(handle),
					GetTxnState(handle),
					GetSubTransactionId(handle),
					handle->state.sequence_number,
					handle->state.received_sno,
					handle->state.in_extended_query,
					handle->used,
					handle->read_only);
	elog(LOG, "%s", buf.data);
	pfree(buf.data);
}

/*
 * New protocol to transfer transaction command to other nodes
 *
 * The transaction command will be executed directly on the Data Node
 * as a fast path method, bypassing parse phase on DN.
 */
int
pgxc_node_send_tran_command(PGXCNodeHandle *handle, 
							TransactionStmtKind kind, char *opt)
{
	int			opt_len = opt? strlen(opt) : 0;
	int			msglen = 7 + opt_len; 	/* msglen size(4B) + kind(1B) + isolevel(1B) + END(1B) + opt_len */
	bool		isBegin = (kind == TRANS_STMT_BEGIN || kind == TRANS_STMT_BEGIN_SUBTXN);
	const char	*kindtag = getTranStmtKindTag(kind);

	if (pgxc_node_send_internal_cmd_flag(handle) < 0)
		return EOF;

	if (!isBegin)
	{
		if (pgxc_node_send_cmd_seq(handle))
		{
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("fail to send command sequence to node %s pid %d",
							handle->nodename, handle->backend_pid)));
		}
	}

	if (unlikely(isBegin && !IsConnectionStateIdle(handle)))
	{
		elog(LOG, "State %d of node %u is not IDLE, cannot send BEGIN command",
			 GetConnectionState(handle), handle->nodeoid);
		return EOF;
	}

	if (enable_distri_print)
	{
		const char *tag = getTranStmtKindTag(kind);
		elog(LOG, "pgxc_node_send_tran_command %s %d %s %s", handle->nodename,
			 handle->backend_pid, tag, opt ? opt : "NULL");
	}

	if (ensure_out_buffer_capacity(handle->outEnd + 1 + msglen, handle) != 0)
	{
		add_error_message(handle, "out of memory");
		return EOF;
	}

	handle->outBuffer[handle->outEnd++] = 'k';
	msglen = htonl(msglen);
	memcpy(handle->outBuffer + handle->outEnd, &msglen, 4);
	handle->outEnd += 4;
	handle->outBuffer[handle->outEnd++] = (uint8)kind;
	/* 
	 * Set isolevel, it is ignored if the transaction kind is not BEGIN
	 * by the datanode 
	 */
	if (kind == TRANS_STMT_BEGIN)
		handle->outBuffer[handle->outEnd++] = PGXCNodeGetXactIsoLevelParam();
	else
		handle->outBuffer[handle->outEnd++] = (uint8)XACT_ISOLATION_INVALID;

	if (opt)
	{
		memcpy(handle->outBuffer + handle->outEnd, opt, opt_len);
		handle->outEnd += opt_len;
	}
	handle->outBuffer[handle->outEnd++] = '\0';
	SetTransitionInExtendedQuery(handle, false);

	RecordSendMessage(handle, 'k', kindtag, strlen(kindtag));

	if (isBegin)
	{
		/* there is no reply for BEGIN, just treat the transaction
		 * state of the DN as normal
		 */
		UpdateTransitionConnectionState(handle, DN_CONNECTION_STATE_IDLE);
		if (IsTxnStateIdle(handle))
			UpdateTransitionTxnState(handle, TXN_STATE_INPROGRESS);
		return 0;
	}

	return SendRequest(handle);
}

/*
 * Get the tag of transaction command
 */
const char *
getTranStmtKindTag(TransactionStmtKind kind)
{
	if (kind <= TRANS_STMT_AUTOXACT_END)
		return TransactionStmtKindTag[kind];
	return "UNKNOWN";
}

/*
 * New API
 */
inline void
InitHandleState(PGXCNodeHandle *handle)
{
	handle->state.connection_state = DN_CONNECTION_STATE_IDLE;
	handle->state.transaction_state = TXN_STATE_IDLE;
	handle->state.in_extended_query = false;
	handle->state.sequence_number = 0;
	handle->state.received_sno = 0;
	handle->state.subtrans_id = TopSubTransactionId;
	handle->state.dirty = false;
	handle->state.cur_guc_cid = 0;

	memcpy(&handle->transitionState, &handle->state, sizeof(HandleState));
}
static void
InitHandleTransitionState(PGXCNodeHandle *handle)
{
	memcpy(&handle->transitionState, &handle->state, sizeof(HandleState));
	handle->transitionState.dirty = true;
}

inline void
ResetTransitionState(PGXCNodeHandle *handle)
{
	char *reset = "SEND FAILED";
	if (handle->transitionState.dirty)
		handle->transitionState.dirty = false;

	if (handle->outEnd != 0)
		handle->outEnd = 0;

	RecordSendMessage(handle, '|', reset, strlen(reset));
}

inline void
StabilizeHandleState(PGXCNodeHandle *handle)
{
	char *succ = "SEND SUCCESSFULLY";
	DNConnectionState old_state = GetConnectionState(handle);

	HOLD_INTERRUPTS();
	/* copy transition state to persistent state*/
	if (handle->transitionState.dirty)
		memcpy(&handle->state, &handle->transitionState, sizeof(HandleState));

	if (enable_pgxcnode_message)
		elog(LOG, "settle handle %s pid %d state from %s to %s, extended query %d, need_sync: %d, sno: %d(R: %d), slevel: %d, used: %d",
			 handle->nodename,
			 handle->backend_pid,
			 ConnectionStateTag[old_state],
			 ConnectionStateTag[GetConnectionState(handle)],
			 handle->state.in_extended_query,
			 handle->needSync,
			 handle->state.sequence_number,
			 handle->state.received_sno,
			 handle->state.subtrans_id,
			 handle->used
			 );

	/*
	 * if the subtransaction comamnd was sent, remove temporal
	 * subtransaction id flag
	 */
	if (handle->registered_subtranid != InvalidSubTransactionId &&
		handle->registered_subtranid == handle->state.subtrans_id)
		handle->registered_subtranid = InvalidSubTransactionId;

	/* reset transition state */
	handle->transitionState.connection_state = DN_CONNECTION_STATE_IDLE;
	handle->transitionState.transaction_state = TXN_STATE_IDLE;
	handle->transitionState.in_extended_query = false;
	handle->transitionState.sequence_number = 0;
	handle->transitionState.received_sno = 0;
	handle->transitionState.subtrans_id = 0;
	handle->transitionState.dirty = false;

	RESUME_INTERRUPTS();
	RecordSendMessage(handle, '|', succ, strlen(succ));
}

inline void
UpdateTransitionConnectionState(PGXCNodeHandle *handle, DNConnectionState newstate)
{
	if(!handle->transitionState.dirty)
		InitHandleTransitionState(handle);
	handle->transitionState.connection_state = newstate;
}

void
UpdateTransitionTxnState(PGXCNodeHandle *handle, char state)
{
	if(!handle->transitionState.dirty)
		InitHandleTransitionState(handle);

	handle->transitionState.transaction_state = state;
}


inline uint8
GetNewSequenceNum(PGXCNodeHandle *handle)
{
	uint8 sno = handle->state.sequence_number + 1;

	if (!handle->transitionState.dirty)
		InitHandleTransitionState(handle);

	handle->transitionState.sequence_number = sno;
	if (enable_pgxcnode_message)
		elog(LOG, "node %s pid %d use new sequence: %d", handle->nodename, handle->backend_pid, sno);
	return sno;
}
inline bool
checkSequenceMatch(PGXCNodeHandle *handle, uint8 seqno)
{
	return (handle->state.sequence_number == seqno);
}

inline bool
HandledAllReplay(PGXCNodeHandle *handle)
{
	return (handle->state.sequence_number == handle->state.received_sno);
}

inline void
SetTransitionInExtendedQuery(PGXCNodeHandle *handle, bool exq)
{
	if (!handle->transitionState.dirty)
		InitHandleTransitionState(handle);

	handle->transitionState.in_extended_query = exq;
}

inline void
SetSubTransactionId(PGXCNodeHandle *handle, int level)
{
	if (!handle->transitionState.dirty)
		InitHandleTransitionState(handle);

	handle->transitionState.subtrans_id = level;
}

inline void
SetCurrentGucCid(PGXCNodeHandle *handle, int guc_cid)
{
	if (!handle->transitionState.dirty)
		InitHandleTransitionState(handle);

	handle->transitionState.cur_guc_cid = guc_cid;
}

inline int
FlushAndStoreState(PGXCNodeHandle *handle)
{
	int ret = pgxc_node_flush(handle);

	if (ret)
		ResetTransitionState(handle);
	else
		StabilizeHandleState(handle);

	return ret;
}

inline int
SendRequest(PGXCNodeHandle *handle)
{
	UpdateTransitionConnectionState(handle, DN_CONNECTION_STATE_QUERY);
	return FlushAndStoreState(handle);
}

void
SPMSetParamStr(char *outstr, int outstr_size)
{
	StringInfoData param_str;
	int            level = (debug_print_spm) ? LOG : DEBUG5;

	initStringInfo(&param_str);

	get_set_command(session_param_list, &param_str, NULL, 0);

	if (param_str.len > 0)
	{
		if (param_str.len + 1 < outstr_size)
		{
			strncpy(outstr, param_str.data, param_str.len);
			outstr[param_str.len] = '\0';
		}
		else
		{
			elog(level, "SPMSetParamStr failed, paramlen: %d, outbuff_size: %d", param_str.len + 1, outstr_size);
		}
	}
	pfree(param_str.data);
}

inline int
SendFlushRequest(PGXCNodeHandle *handle)
{
	UpdateTransitionConnectionState(handle, DN_CONNECTION_STATE_QUERY);
	return pgxc_node_send_flush(handle);
}

void
RecordOneMessage(PGXCNodeHandle *handle, char msgtype, const char *msg, int size, bool send)
{
	int				msglen;
	int				index = 0;
	char			*buf;

	index = handle->msg_index;

	if (index < 0 || index >= HISTORY_MSG_QUEUE_SIZE)
	{
		elog(WARNING, "Record history message index error, index: %d", index);
		return;
	}

	buf = &handle->history[index][0];

	memset(buf, 0, MAX_HISTORY_MSG_LEN);
	msglen = Min(MAX_HISTORY_MSG_LEN - 4, size);

	/* sent message starts with '> ', received message starts with '<' */
	buf[0] = send? '>' : '<';
	buf[1] = msgtype;
	buf[2] = msglen;
	if (msglen > 0 && msg != NULL)
		memcpy(&buf[3], msg, msglen);
	buf[3 + msglen] = '\0';

	index = (index + 1) % HISTORY_MSG_QUEUE_SIZE;
	handle->msg_index = index;
}

bool
ProcessResponseMessage(ResponseCombiner *combiner,
						struct timeval *timeout,
						int wait_event, bool ignore_error)
{
	int		response = 0;
	int		done_count = 0;
	int		i = -1;
	int		errors = 0;
	int		total = combiner->node_count;
	PGXCNodeHandle **connections;
	bool	*dones;
	MemoryContext ctx;

	if (total <= 0)
	{
		elog(WARNING, "No handle need to process");
		return false;
	}

	dones = (bool *)palloc0(sizeof(bool) * total);
	connections = combiner->connections;

	while (done_count < total)
	{
		i = (i + 1) % total;

		if (!dones[i])
		{
			PGXCNodeHandle *handle = connections[i];

			if (IsConnectionStateFatal(handle))
			{
				elog(LOG, "connection is in FATAL state, %s pid %d",
					 handle->nodename, handle->backend_pid);
				dones[i] = true;
				done_count++;
				errors++;

				continue;
			}
			else if (IsConnectionStateIdle(handle))
			{
				dones[i] = true;
				done_count++;
			}

			if (!HAS_MESSAGE_BUFFERED(handle))
			{
				if (pgxc_node_receive(1, &handle, timeout))
					continue;
			}

			ctx = CurrentMemoryContext;
			PG_TRY();
			{
				response = handle_response(handle, combiner);
			}
			PG_CATCH();
			{
				MemoryContextSwitchTo(ctx);
				FlushErrorState();

				handle->combiner = NULL;
				response = RESPONSE_ERROR;
				elog(LOG, "encountered an error while processing the message on handle %s pid %d",
					handle->nodename, handle->backend_pid);
			}
			PG_END_TRY();

			if (response == wait_event)
			{
				dones[i] = true;
				done_count++;

				/* ensure combiner is removed from handle */
				if (handle->combiner)
					handle->combiner = NULL;
			}
			else if (response == RESPONSE_ERROR && !ignore_error)
			{
				dones[i] = true;
				done_count++;
				if (DN_CONNECTION_ERROR(handle))
				{
					elog(LOG, "connections received ERROR messasge: %s", handle->error);
					errors++;
				}

				/* ensure combiner is removed from handle */
				if (handle->combiner)
					handle->combiner = NULL;
			}
			else if (response == RESPONSE_READY_MISMATCH)
			{
				if (ignore_error)
					UpdateConnectionState(handle, DN_CONNECTION_STATE_QUERY);
				else if (wait_event == RESPONSE_READY)
				{
					dones[i] = true;
					done_count++;
				}
			}
			else if (IsConnectionStateIdle(handle))
			{
				if (ignore_error)
					UpdateConnectionState(handle, DN_CONNECTION_STATE_QUERY);
				else
				{
					dones[i] = true;
					done_count++;
					elog(LOG, "ERROR: connection %s pid %d state IDLE is unexpected",
						 handle->nodename, handle->backend_pid);
					errors++;
				}

			}
		}
	}

	if (!ignore_error && errors)
	{
		if (errors)
		{
			bool valid = validate_combiner(combiner);

			if (!valid)
				pgxc_node_report_error(combiner);

			/* no error message in combiner */
			ereport(ERROR, (errcode(ERRCODE_CONNECTION_FAILURE),
					errmsg("Unexpect error in ProcessResponse_Message")));
		}
		if (combiner->errorMessage)
			pgxc_node_report_error(combiner);
	}

	pfree(dones);

	return errors > 0 ? true : false;
}

inline void
SetHandleUsed_internal(PGXCNodeHandle *handle, bool used, const char *filename, int lineno)
{

	if (handle->used != used)
	{
		handle->used = used;

		if (enable_pgxcnode_message)
			elog(LOG, "set used flag of handle %s pid %d to %d, %s:%d",
					handle->nodename, handle->backend_pid, used, filename, lineno);
	}
}
void
PrintHandleState(PGXCNodeHandle *handle)
{

	int loglevel = enable_pgxcnode_message ? LOG : DEBUG3;

	elog(loglevel, "node %s %d conn state: %s, txn state: %c, sublevel: %d, in extended query: %d, sno: %d (received: %d), used: %d",
			handle->nodename,
			handle->backend_pid,
			ConnectionStateTag[handle->state.connection_state],
			handle->state.transaction_state,
		 	GetSubTransactionId(handle),
			handle->state.in_extended_query,
			handle->state.sequence_number,
			handle->state.received_sno,
			handle->used);
}

inline static long
CalculateTimeDifference(struct timespec start, struct timespec end)
{
	long diff_sec = end.tv_sec - start.tv_sec;
	long diff_nsec = end.tv_nsec - start.tv_nsec;
	long diff_msec;

	if (diff_nsec < 0)
	{
		diff_sec--;
		diff_nsec += 1000000000;
	}

	diff_msec = diff_sec * 1000 + diff_nsec / 1000000;

	return diff_msec;
}

void check_pid(int pid)
{
	int a = 0;
	int j, i;


	for (i = 0; i < NumDataNodes; i++)
	{
		for (j = 0; j < max_handles; j++)
		{
			PGXCNodeHandle *handle = &dn_handles[i][j];

			if (handle->backend_pid == pid && handle->sock != NO_SOCKET)
			{
				a++;
				elog(LOG, "found duplicated handle pid: %d", pid); PrintHandleState(handle);
			}

		}
	}

	for (i = 0; i < NumCoords; i++)
	{
		for (j = 0; j < max_handles; j++)
		{
			PGXCNodeHandle *handle = &co_handles[i][j];

			if (handle->backend_pid == pid && handle->sock != NO_SOCKET)
			{
				a++;
				elog(LOG, "found duplicated handle pid: %d", pid);
				PrintHandleState(handle);

			}

		}
	}

	if (a)
		elog(PANIC, "Found duplicated handle!");
}
