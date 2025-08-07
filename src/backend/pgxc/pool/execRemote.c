/*-------------------------------------------------------------------------
 *
 * execRemote.c
 *
 *	  Functions to execute commands on remote Datanodes
 *
 *
 * Portions Copyright (c) 2012-2014, TransLattice, Inc.
 * Portions Copyright (c) 1996-2011, PostgreSQL Global Development Group
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 *
 *
 * IDENTIFICATION
 *	  src/backend/pgxc/pool/execRemote.c
 *
 *-------------------------------------------------------------------------
 */

#include <time.h>
#include "postgres.h"
#include "access/twophase.h"
#include "access/gtm.h"
#include "access/sysattr.h"
#include "access/transam.h"
#include "access/xact.h"
#include "access/relscan.h"
#include "catalog/pg_type.h"
#include "catalog/pgxc_node.h"
#include "commands/dbcommands.h"
#include "commands/prepare.h"
#include "commands/vacuum.h"
#include "common/opentenbase_ora.h"
#include "executor/executor.h"
#include "gtm/gtm_c.h"
#include "libpq/libpq.h"
#include "libpq/pqformat.h"
#include "miscadmin.h"
#include "pgxc/execRemote.h"
#include "tcop/tcopprot.h"
#include "executor/nodeSubplan.h"
#include "nodes/nodeFuncs.h"
#include "pgstat.h"
#include "nodes/nodes.h"
#include "nodes/nodeFuncs.h"
#include "optimizer/var.h"
#include "pgxc/copyops.h"
#include "pgxc/nodemgr.h"
#include "pgxc/poolmgr.h"
#include "storage/ipc.h"
#include "storage/proc.h"
#include "utils/datum.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/orcl_datetime.h"
#include "utils/pg_rusage.h"
#include "utils/tuplesort.h"
#include "utils/snapmgr.h"
#include "utils/builtins.h"
#include "pgxc/locator.h"
#include "pgxc/pgxc.h"
#include "parser/parse_type.h"
#include "parser/parsetree.h"
#include "pgxc/xc_maintenance_mode.h"
#include "catalog/pgxc_class.h"
#ifdef __OPENTENBASE__
#include "pgxc/squeue.h"
#include "executor/execParallel.h"
#include "postmaster/postmaster.h"
#include "executor/nodeModifyTable.h"
#include "utils/guc.h"
#include "utils/syscache.h"
#include "nodes/print.h"
#include "utils/xact_whitebox.h"
#include "utils/tqual.h"
#endif

#include "access/atxact.h"
#include "optimizer/clauses.h"
#include "optimizer/spm.h"

#include "pgxc/pgxcnode.h"
#ifdef __OPENTENBASE_C__
#include "executor/execDispatchFragment.h"
#include "utils/resgroup.h"
#endif
#include "pgxc/pause.h"
#include "commands/explain_dist.h"

/*
 * We do not want it too long, when query is terminating abnormally we just
 * want to read in already available data, if datanode connection will reach a
 * consistent state after that, we will go normal clean up procedure: send down
 * ABORT etc., if data node is not responding we will signal pooler to drop
 * the connection.
 * It is better to drop and recreate datanode connection then wait for several
 * seconds while it being cleaned up when, for example, cancelling query.
 */
#define END_QUERY_TIMEOUT	10000

bool exec_event_trigger_stmt = false;

/* Enable 2pc gts optimize? */
bool enable_2pc_gts_optimize = true;

/* Declarations used by guc.c */
int PGXLRemoteFetchSize;

#ifdef __OPENTENBASE__
int g_in_plpgsql_exec_fun = 0;
bool PlpgsqlDebugPrint = false;
bool postpone_slow_node = false;
bool delay_sending_begin = false;
int max_remote_query_fetch = 10000;

extern bool mult_sql_query_containing;
#endif
bool g_in_plpgsql_rollback_to = false;
bool g_in_plpgsql_savepoint = false;
bool g_in_rollback_to = false;
bool g_in_release_savepoint= false;

#ifdef __OPENTENBASE__
/* GUC parameter */
int DataRowBufferSize = 0;  /* MBytes */

#define DATA_ROW_BUFFER_SIZE(n) (DataRowBufferSize * 1024 * 1024 * (n))
#endif

typedef struct
{
	xact_callback function;
	void *fparams;
} abort_callback_type;

/*
 * Buffer size does not affect performance significantly, just do not allow
 * connection buffer grows infinitely
 */
#define COPY_BUFFER_SIZE 8192

/*
 * Flag to track if a temporary object is accessed by the current transaction
 */
bool temp_object_included = false;
static abort_callback_type dbcleanup_info = { NULL, NULL };

static int parse_row_count(const char *message, size_t len, uint64 *rowcount);

static PGXCNodeAllHandles *get_exec_connections(RemoteQueryState *planstate,
					 ExecNodes *exec_nodes,
					 RemoteQueryExecType exec_type,
					 bool is_global_session);

static bool pgxc_start_command_on_connection(PGXCNodeHandle *connection,
					RemoteQueryState *remotestate, Snapshot snapshot, bool prepared);

static void pgxc_node_remote_count(int *dnCount, int dnNodeIds[],
		int *coordCount, int coordNodeIds[]);
static char *pgxc_node_remote_prepare(char *prepareGID, bool localNode, bool implicit);
static bool pgxc_node_remote_finish(char *prepareGID, bool commit,
						char *nodestring, GlobalTransactionId gxid,
						GlobalTransactionId prepare_gxid);
static bool
pgxc_node_remote_prefinish(char *prepareGID, char *nodestring);

#ifdef __OPENTENBASE__
static void pgxc_node_remote_commit(TranscationType txn_type, bool need_release_handle);
static void pgxc_node_remote_abort(TranscationType txn_type, bool need_release_handle);
#endif

static int getStatsLen(AnalyzeRelStats *analyzeStats);
static int pgxc_node_send_stat(PGXCNodeHandle * handle, AnalyzeRelStats *analyzeStats);
static ExecNodes *paramGetExecNodes(ExecNodes *origin, PlanState *planstate);
static int handle_reply_msg_on_proxy(PGXCNodeHandle *conn);
static int pgxc_node_send_spm_plan(PGXCNodeHandle * handle,
									LocalSPMPlanInfo *spmplan);
static int pgxc_node_send_spm_plan_delete(PGXCNodeHandle * handle,
											LocalSPMPlanInfo *spmplan);
static int pgxc_node_send_spm_plan_update(PGXCNodeHandle *handle, int64 sql_id,
								int64 plan_id, char *attribute_name,
								char *attribute_value);
static void ReleaseCorruptedHandles(void);
static int SendSynchronizeMessage(PGXCNodeHandle *handles[], int count, int sent_idx, ResponseCombiner *combiner);
static int SendSessionGucConfigs(PGXCNodeHandle **connections, int conn_count, bool is_global_session);

static int twophase_add_participant(PGXCNodeHandle *handle, int handle_index, TwoPhaseTransState state);
static void twophase_update_state(PGXCNodeHandle *handle);

#define REMOVE_CURR_CONN(combiner) \
	if ((combiner)->current_conn < --((combiner)->conn_count)) \
	{ \
		(combiner)->connections[(combiner)->current_conn]->combiner = NULL; \
		(combiner)->connections[(combiner)->current_conn] = \
				(combiner)->connections[(combiner)->conn_count]; \
	} \
	else \
		(combiner)->current_conn = 0

#define MAX_STATEMENTS_PER_TRAN 10

/* Variables to collect statistics */
static int	total_transactions = 0;
static int	total_statements = 0;
static int	total_autocommit = 0;
static int	nonautocommit_2pc = 0;
static int	autocommit_2pc = 0;
static int	current_tran_statements = 0;
static int *statements_per_transaction = NULL;
static int *nodes_per_transaction = NULL;

/*
 * statistics collection: count a statement
 */
static void
stat_statement()
{
	total_statements++;
	current_tran_statements++;
}

/*
 * clean memory related to stat transaction
 */
void
clean_stat_transaction(void)
{
	if (!nodes_per_transaction)
	{
		return ;
	}

	free(nodes_per_transaction);
	nodes_per_transaction = NULL;
}

/*
 * To collect statistics: count a transaction
 */
static void
stat_transaction(int node_count)
{
	total_transactions++;

	if (!statements_per_transaction)
	{
		statements_per_transaction = (int *) malloc((MAX_STATEMENTS_PER_TRAN + 1) * sizeof(int));
		memset(statements_per_transaction, 0, (MAX_STATEMENTS_PER_TRAN + 1) * sizeof(int));
	}
	if (current_tran_statements > MAX_STATEMENTS_PER_TRAN)
		statements_per_transaction[MAX_STATEMENTS_PER_TRAN]++;
	else
		statements_per_transaction[current_tran_statements]++;
	current_tran_statements = 0;
	if (node_count > 0 && node_count <= NumDataNodes)
	{
		if (!nodes_per_transaction)
		{
			nodes_per_transaction = (int *) malloc(NumDataNodes * sizeof(int));
			memset(nodes_per_transaction, 0, NumDataNodes * sizeof(int));
		}
		nodes_per_transaction[node_count - 1]++;
	}
}


/*
 * Output collected statistics to the log
 */
static void
stat_log()
{
	elog(DEBUG1, "Total Transactions: %d Total Statements: %d", total_transactions, total_statements);
	elog(DEBUG1, "Autocommit: %d 2PC for Autocommit: %d 2PC for non-Autocommit: %d",
		 total_autocommit, autocommit_2pc, nonautocommit_2pc);
	if (total_transactions)
	{
		if (statements_per_transaction)
		{
			int			i;

			for (i = 0; i < MAX_STATEMENTS_PER_TRAN; i++)
				elog(DEBUG1, "%d Statements per Transaction: %d (%d%%)",
					 i, statements_per_transaction[i], statements_per_transaction[i] * 100 / total_transactions);

			elog(DEBUG1, "%d+ Statements per Transaction: %d (%d%%)",
				 MAX_STATEMENTS_PER_TRAN, statements_per_transaction[MAX_STATEMENTS_PER_TRAN], statements_per_transaction[MAX_STATEMENTS_PER_TRAN] * 100 / total_transactions);
		}

		if (nodes_per_transaction)
		{
			int			i;

			for (i = 0; i < NumDataNodes; i++)
				elog(DEBUG1, "%d Nodes per Transaction: %d (%d%%)",
					 i + 1, nodes_per_transaction[i], nodes_per_transaction[i] * 100 / total_transactions);
		}
	}
}


/*
 * Create a structure to store parameters needed to combine responses from
 * multiple connections as well as state information
 */
void
InitResponseCombiner_internal(ResponseCombiner *combiner, int node_count,
					 CombineType combine_type, char *file, int line)
{
	memset(combiner, 0, sizeof(ResponseCombiner));
	combiner->node_count = node_count;
	combiner->connections = NULL;
	combiner->conn_count = 0;
	combiner->combine_type = combine_type;
	combiner->current_conn_rows_consumed = 0;
	combiner->command_complete_count = 0;
#ifdef __OPENTENBASE_C__
	combiner->DML_replicated_return = false;
	combiner->handle_ftable_resp = false;
#endif
	combiner->request_type = REQUEST_TYPE_NOT_DEFINED;
	combiner->description_count = 0;
	combiner->copy_in_count = 0;
	combiner->copy_out_count = 0;
	combiner->copy_file = NULL;
	combiner->errorMessage = NULL;
	combiner->errorDetail = NULL;
	combiner->errorHint = NULL;
	combiner->tuple_desc = NULL;
	combiner->probing_primary = false;
	combiner->returning_node = InvalidOid;
	combiner->currentRow = NULL;
	combiner->rowBuffer = NIL;
	combiner->tapenodes = NULL;
	combiner->merge_sort = false;
	combiner->extended_query = false;
	combiner->tapemarks = NULL;
	combiner->tapemarks_count = 0;
	combiner->tuplesortstate = NULL;
	combiner->cursor = NULL;
	combiner->update_cursor = NULL;
	combiner->cursor_count = 0;
	combiner->cursor_connections = NULL;
	combiner->remoteCopyType = REMOTE_COPY_NONE;
#ifdef __OPENTENBASE__
	combiner->dataRowBuffer  = NULL;
	combiner->dataRowMemSize = NULL;
	combiner->nDataRows      = NULL;
	combiner->tmpslot        = NULL;
	combiner->errorNode      = NULL;
	combiner->backend_pid    = 0;
	combiner->recv_datarows  = 0;
	combiner->prerowBuffers  = NULL;
	combiner->is_abort = false;
	combiner->context = CurrentMemoryContext;
	combiner->resowner = CurrentResourceOwner;
	combiner->waitforC = false;
	combiner->waitforE = false;
	combiner->waitfor2 = false;
	combiner->ignore_datarow = false;
#endif
	elog(DEBUG3, "initialize combiner: %p, file: %s:%d", combiner, file, line);
}


/*
 * Parse out row count from the command status response and convert it to integer
 */
static int
parse_row_count(const char *message, size_t len, uint64 *rowcount)
{
	int			digits = 0;
	int			pos;

	*rowcount = 0;
	/* skip \0 string terminator */
	for (pos = 0; pos < (int)len - 1; pos++)
	{
		if (message[pos] >= '0' && message[pos] <= '9')
		{
			*rowcount = *rowcount * 10 + message[pos] - '0';
			digits++;
		}
		else
		{
			*rowcount = 0;
			digits = 0;
		}
	}
	return digits;
}

/*
 * Convert RowDescription message to a TupleDesc
 */
TupleDesc
create_tuple_desc(char *msg_body, size_t len)
{
	TupleDesc 	result;
	int 		i, nattr;
	uint16		n16;

	/* get number of attributes */
	memcpy(&n16, msg_body, 2);
	nattr = pg_ntoh16(n16);
	msg_body += 2;

	result = CreateTemplateTupleDesc(nattr, false);

	/* decode attributes */
	for (i = 1; i <= nattr; i++)
	{
		AttrNumber	attnum;
		char		*attname;
		char		*typname;
		Oid 		oidtypeid;
		int32 		typemode, typmod;

		attnum = (AttrNumber) i;

		/* attribute name */
		attname = msg_body;
		msg_body += strlen(attname) + 1;

		/* type name */
		typname = msg_body;
		msg_body += strlen(typname) + 1;

		/* table OID, ignored */
		msg_body += 4;

		/* column no, ignored */
		msg_body += 2;

		/* data type OID, ignored */
		msg_body += 4;

		/* type len, ignored */
		msg_body += 2;

		/* type mod */
		memcpy(&typemode, msg_body, 4);
		typmod = pg_ntoh32(typemode);
		msg_body += 4;

		/* PGXCTODO text/binary flag? */
		msg_body += 2;

		/* Get the OID type and mode type from typename */
		parseTypeString(typname, &oidtypeid, NULL, false);

		TupleDescInitEntry(result, attnum, attname, oidtypeid, typmod, 0);
	}
	return result;
}

/*
 * Handle CopyOutCommandComplete ('c') message from a Datanode connection
 */
static void
HandleCopyOutComplete(ResponseCombiner *combiner)
{
	if (combiner->request_type == REQUEST_TYPE_ERROR)
		return;
	if (combiner->request_type == REQUEST_TYPE_NOT_DEFINED)
		combiner->request_type = REQUEST_TYPE_COPY_OUT;
	if (combiner->request_type != REQUEST_TYPE_COPY_OUT)
		/* Inconsistent responses */
		ereport(ERROR,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("Unexpected response from the Datanodes for 'c' message, current request type %d", combiner->request_type)));
	/* Just do nothing, close message is managed by the Coordinator */
	combiner->copy_out_count++;
}

/*
 * Handle CommandComplete ('C') message from a Datanode connection
 */
static void
HandleCommandComplete(ResponseCombiner *combiner, char *msg_body, size_t len, PGXCNodeHandle *conn)
{
	int 			digits = 0;
	EState		   *estate = combiner->ss.ps.state;

	/*
	 * If we did not receive description we are having rowcount or OK response
	 */
	if (combiner->request_type == REQUEST_TYPE_NOT_DEFINED)
		combiner->request_type = REQUEST_TYPE_COMMAND;

	/* Extract rowcount */
	if (estate && combiner->combine_type != COMBINE_TYPE_NONE &&
		conn->fragmentlevel == 1 && conn->ftype == Fragment_Normal)
	{
		uint64	rowcount;
		digits = parse_row_count(msg_body, len, &rowcount);
		if (digits > 0)
		{
			/* Replicated write, make sure they are the same */
			if (combiner->combine_type == COMBINE_TYPE_SAME)
			{
				if (combiner->DML_replicated_return)
				{
					/*
					 * Replicated command may succeed on on node and fail on
					 * another. The example is if distributed table referenced
					 * by a foreign key constraint defined on a partitioned
					 * table. If command deletes rows from the replicated table
					 * they may be referenced on one Datanode but not on other.
					 * So, replicated command on each Datanode either affects
					 * proper number of rows, or returns error. Here if
					 * combiner got an error already, we allow to report it,
					 * not the scaring data corruption message.
					 */
					if (combiner->errorMessage == NULL && rowcount != estate->es_processed)
					{
						/* There is a consistency issue in the database with the replicated table */
						ereport(ERROR,
								(errcode(ERRCODE_DATA_CORRUPTED),
									errmsg("Write to replicated table returned different results from the Datanodes")));
					}
				}
				else
				{
					/* first result */
					estate->es_processed = rowcount;
					combiner->DML_replicated_return = true;
				}
			}
			else
				estate->es_processed += rowcount;
			combiner->DML_processed += rowcount;
		}
	}

	/* If response checking is enable only then do further processing */
	if (conn->ck_resp_rollback)
	{
		if (strcmp(msg_body, "ROLLBACK") == 0)
		{
			/*
			 * Subsequent clean up routine will be checking this flag
			 * to determine nodes where to send ROLLBACK PREPARED.
			 * On current node PREPARE has failed and the two-phase record
			 * does not exist, so clean this flag as if PREPARE was not sent
			 * to that node and avoid erroneous command.
			 */
			conn->ck_resp_rollback = false;
			/*
			 * Set the error, if none, to force throwing.
			 * If there is error already, it will be thrown anyway, do not add
			 * this potentially confusing message
			 */
			if (combiner->errorMessage == NULL)
			{
				MemoryContext oldcontext = MemoryContextSwitchTo(ErrorContext);
				combiner->errorMessage =
								pstrdup("unexpected ROLLBACK from remote node");
				MemoryContextSwitchTo(oldcontext);
				/*
				 * ERRMSG_PRODUCER_ERROR
				 * Messages with this code are replaced by others, if they are
				 * received, so if node will send relevant error message that
				 * one will be replaced.
				 */
				combiner->errorCode[0] = 'X';
				combiner->errorCode[1] = 'X';
				combiner->errorCode[2] = '0';
				combiner->errorCode[3] = '1';
				combiner->errorCode[4] = '0';
			}
		}
	}
	combiner->command_complete_count++;
}

/*
 * Handle RowDescription ('T') message from a Datanode connection
 */
static bool
HandleRowDescription(ResponseCombiner *combiner, char *msg_body, size_t len)
{
	if (combiner->request_type == REQUEST_TYPE_ERROR)
		return false;
	if (combiner->request_type == REQUEST_TYPE_NOT_DEFINED)
		combiner->request_type = REQUEST_TYPE_QUERY;
	if (combiner->request_type != REQUEST_TYPE_QUERY)
	{
		/* Inconsistent responses */
		ereport(ERROR,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("Unexpected response from the Datanodes for 'T' message, current request type %d", combiner->request_type)));
	}
	/* Increment counter and check if it was first */
	if (combiner->description_count == 0 &&
		!(combiner->ss.ps.ps_ResultTupleSlot &&
		  TTS_FIXED(combiner->ss.ps.ps_ResultTupleSlot)))
	{
		combiner->description_count++;
		combiner->tuple_desc = create_tuple_desc(msg_body, len);
		return true;
	}
	combiner->description_count++;
	return false;
}

#ifdef __SUBSCRIPTION__
/*
 * Handle Apply ('4') message from a Datanode connection
 */
static void
HandleApplyDone(ResponseCombiner *combiner)
{
	if (combiner->request_type == REQUEST_TYPE_ERROR)
		return;
	if (combiner->request_type == REQUEST_TYPE_NOT_DEFINED)
		combiner->request_type = REQUEST_TYPE_COMMAND;
	if (combiner->request_type != REQUEST_TYPE_COMMAND)
	{
		/* Inconsistent responses */
		ereport(ERROR,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("Unexpected response from the Datanodes for '4' message, current request type %d", combiner->request_type)));
	}

	combiner->command_complete_count++;
}
#endif

#ifdef __OPENTENBASE_C__
/*
 * Handle Apply ('5') message from a Datanode connection
 */
static void
HandleFtableDone(ResponseCombiner *combiner)
{
	if (combiner->request_type == REQUEST_TYPE_ERROR)
		return;
	if (combiner->request_type == REQUEST_TYPE_NOT_DEFINED)
		combiner->request_type = REQUEST_TYPE_COMMAND;
	if (combiner->request_type != REQUEST_TYPE_COMMAND)
	{
		/* Inconsistent responses */
		ereport(ERROR,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("Unexpected response from the Datanodes for '5' message, current request type %d", combiner->request_type)));
	}

	combiner->command_complete_count++;
}

#endif
/*
 * Handle CopyInResponse ('G') message from a Datanode connection
 */
static void
HandleCopyIn(ResponseCombiner *combiner)
{
	if (combiner->request_type == REQUEST_TYPE_ERROR)
		return;
	if (combiner->request_type == REQUEST_TYPE_NOT_DEFINED)
		combiner->request_type = REQUEST_TYPE_COPY_IN;
	if (combiner->request_type != REQUEST_TYPE_COPY_IN)
	{
		/* Inconsistent responses */
		ereport(ERROR,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("Unexpected response from the Datanodes for 'G' message, current request type %d", combiner->request_type)));
	}
	/*
	 * The normal PG code will output an G message when it runs in the
	 * Coordinator, so do not proxy message here, just count it.
	 */
	combiner->copy_in_count++;
}

/*
 * Handle CopyOutResponse ('H') message from a Datanode connection
 */
static void
HandleCopyOut(ResponseCombiner *combiner)
{
	if (combiner->request_type == REQUEST_TYPE_ERROR)
		return;
	if (combiner->request_type == REQUEST_TYPE_NOT_DEFINED)
		combiner->request_type = REQUEST_TYPE_COPY_OUT;
	if (combiner->request_type != REQUEST_TYPE_COPY_OUT)
	{
		/* Inconsistent responses */
		ereport(ERROR,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("Unexpected response from the Datanodes for 'H' message, current request type %d", combiner->request_type)));
	}
	/*
	 * The normal PG code will output an H message when it runs in the
	 * Coordinator, so do not proxy message here, just count it.
	 */
	combiner->copy_out_count++;
}

/*
 * Handle CopyOutDataRow ('d') message from a Datanode connection
 */
static void
HandleCopyDataRow(ResponseCombiner *combiner, char *msg_body, size_t len)
{
	if (combiner->request_type == REQUEST_TYPE_ERROR)
		return;
	if (combiner->request_type == REQUEST_TYPE_NOT_DEFINED)
		combiner->request_type = REQUEST_TYPE_COPY_OUT;

	/* Inconsistent responses */
	if (combiner->request_type != REQUEST_TYPE_COPY_OUT)
		ereport(ERROR,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("Unexpected response from the Datanodes for 'd' message, current request type %d", combiner->request_type)));

	/* count the row */
	combiner->processed++;

	/* Output remote COPY operation to correct location */
	switch (combiner->remoteCopyType)
	{
		case REMOTE_COPY_FILE:
			/* Write data directly to file */
			fwrite(msg_body, 1, len, combiner->copy_file);
			break;
		case REMOTE_COPY_STDOUT:
			/* Send back data to client */
			pq_putmessage('d', msg_body, len);
			break;
		case REMOTE_COPY_TUPLESTORE:
			/*
			 * Do not store trailing \n character.
			 * When tuplestore data are loaded to a table it automatically
			 * inserts line ends.
			 */
			tuplestore_putmessage(combiner->tuplestorestate, len-1, msg_body);
			break;
		case REMOTE_COPY_NONE:
		default:
			Assert(0); /* Should not happen */
	}
}

/*
 * Handle DataRow ('D') message from a Datanode connection
 * The function returns true if data row is accepted and successfully stored
 * within the combiner.
 */
static bool
HandleDataRow(ResponseCombiner *combiner, char *msg_body, size_t len, Oid node, bool data_from_remote_tid_scan)
{
	/* We expect previous message is consumed */
	Assert(combiner->currentRow == NULL);

	if (combiner->ignore_datarow)
		return false;

	if (combiner->request_type == REQUEST_TYPE_ERROR)
		return false;

	if (combiner->request_type != REQUEST_TYPE_QUERY)
	{
		/* Inconsistent responses */
		char data_buf[4096];

		snprintf(data_buf, len, "%s", msg_body);
		if (len > 4095)
			len = 4095;
		data_buf[len] = 0;
		ereport(ERROR,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("Unexpected response from the data nodes for 'D' message, "
						"current request type %d, data %s",
				 		combiner->request_type, data_buf)));
	}

	/*
	 * If we got an error already ignore incoming data rows from other nodes
	 * Still we want to continue reading until get CommandComplete
	 */
	if (combiner->errorMessage)
		return false;

	/*
	 * Replicated INSERT/UPDATE/DELETE with RETURNING: receive only tuples
	 * from one node, skip others as duplicates
	 */
	if (combiner->combine_type == COMBINE_TYPE_SAME)
	{
		/* Do not return rows when probing primary, instead return when doing
		 * first normal node. Just save some CPU and traffic in case if
		 * probing fails.
		 */
		if (combiner->probing_primary)
			return false;
		if (OidIsValid(combiner->returning_node))
		{
			if (combiner->returning_node != node)
				return false;
		}
		else
			combiner->returning_node = node;
	}

	/*
	 * We are copying message because it points into connection buffer, and
	 * will be overwritten on next socket read
	 */
	combiner->currentRow = (RemoteDataRow) palloc(sizeof(RemoteDataRowData) + len);
	memcpy(combiner->currentRow->msg, msg_body, len);
	combiner->currentRow->msglen = len;
	combiner->currentRow->msgnode = node;
	combiner->currentRow->data_from_remote_tid_scan = data_from_remote_tid_scan;

	return true;
}

/*
 * Handle ErrorResponse ('E') message from a Datanode connection
 */
static void
HandleError(ResponseCombiner *combiner, char *msg_body, size_t len,
			PGXCNodeHandle *conn)
{
	/* parse error message */
	char *code = NULL;
	char *message = NULL;
	char *detail = NULL;
	char *hint = NULL;
	int   offset = 0;

	/*
	 * Scan until point to terminating \0
	 */
	while (offset + 1 < len)
	{
		/* pointer to the field message */
		char *str = msg_body + offset + 1;

		switch (msg_body[offset])
		{
			case 'C':	/* code */
				code = str;
				break;
			case 'M':	/* message */
				message = str;
				break;
			case 'D':	/* details */
				detail = str;
				break;

			case 'H':	/* hint */
				hint = str;
				break;

			/* Fields not yet in use */
			case 'S':	/* severity */
			case 'V':   /* PG_DIAG_SEVERITY_NONLOCALIZED */
				if (strcmp(str, "FATAL") == 0 || strcmp(str, "PANIC") == 0 ||  strcmp(str, "STOP") == 0 )
				{
					UpdateConnectionState(conn, DN_CONNECTION_STATE_FATAL);
					add_error_message(conn, str);
				}
				break;
			
			case 'R':	/* routine */
			case 'P':	/* position string */
			case 'p':	/* position int */
			case 'q':	/* int query */
			case 'W':	/* where */
			case 'F':	/* file */
			case 'L':	/* line */
			default:
				break;
		}

		/* code, message and \0 */
		offset += strlen(str) + 2;
	}

	/*
	 * We may have special handling for some errors, default handling is to
	 * throw out error with the same message. We can not ereport immediately
	 * because we should read from this and other connections until
	 * ReadyForQuery is received, so we just store the error message.
	 * If multiple connections return errors only first one is reported.
	 *
	 * The producer error may be hiding primary error, so if previously received
	 * error is a producer error allow it to be overwritten.
	 */
	if (combiner->errorMessage == NULL ||
			MAKE_SQLSTATE(combiner->errorCode[0], combiner->errorCode[1],
						  combiner->errorCode[2], combiner->errorCode[3],
						  combiner->errorCode[4]) == ERRCODE_PRODUCER_ERROR)
	{
		/*
		 * ErrorContext will be reset if elog error recursion occured or FlushErrorState
		 * invoked, so use TopMemoryContext instead of ErrorContext.
		 */
		MemoryContext oldcontext = MemoryContextSwitchTo(TopMemoryContext);
		if (combiner->errorMessage)
		{
			pfree(combiner->errorMessage);
			combiner->errorMessage = NULL;
		}

		if (message)
			combiner->errorMessage = pstrdup(message);
		/* Error Code is exactly 5 significant bytes */
		if (code)
			memcpy(combiner->errorCode, code, 5);
		if (detail)
			combiner->errorDetail = pstrdup(detail);
		if (hint)
			combiner->errorHint = pstrdup(hint);
		MemoryContextSwitchTo(oldcontext);
	}

	/*
	 * If the PREPARE TRANSACTION command fails for whatever reason, we don't
	 * want to send down ROLLBACK PREPARED to this node. Otherwise, it may end
	 * up rolling back an unrelated prepared transaction with the same GID as
	 * used by this transaction
	 */
	if (conn->ck_resp_rollback)
		conn->ck_resp_rollback = false;

	/*
	 * If Datanode have sent ErrorResponse it will never send CommandComplete.
	 * Increment the counter to prevent endless waiting for it.
	 */
	combiner->command_complete_count++;
}

/*
 * HandleCmdComplete -
 *	combine deparsed sql statements execution results
 *
 * Input parameters:
 *	commandType is dml command type
 *	combineTag is used to combine the completion result
 *	msg_body is execution result needed to combine
 *	len is msg_body size
 */
void
HandleCmdComplete(CmdType commandType, CombineTag *combine,
						const char *msg_body, size_t len)
{
	int	digits = 0;
	uint64	originrowcount = 0;
	uint64	rowcount = 0;
	uint64	total = 0;

	if (msg_body == NULL)
		return;

	/* if there's nothing in combine, just copy the msg_body */
	if (strlen(combine->data) == 0)
	{
		strcpy(combine->data, msg_body);
		combine->cmdType = commandType;
		return;
	}
	else
	{
		/* commandType is conflict */
		if (combine->cmdType != commandType)
			return;

		/* get the processed row number from msg_body */
		digits = parse_row_count(msg_body, len + 1, &rowcount);
		elog(DEBUG1, "digits is %d\n", digits);
		Assert(digits >= 0);

		/* no need to combine */
		if (digits == 0)
			return;

		/* combine the processed row number */
		parse_row_count(combine->data, strlen(combine->data) + 1, &originrowcount);
		elog(DEBUG1, "originrowcount is %lu, rowcount is %lu\n", originrowcount, rowcount);
		total = originrowcount + rowcount;

	}

	/* output command completion tag */
	switch (commandType)
	{
		case CMD_SELECT:
			strcpy(combine->data, "SELECT");
			break;
		case CMD_INSERT:
			snprintf(combine->data, COMPLETION_TAG_BUFSIZE,
			   "INSERT %u %lu", 0, total);
			break;
		case CMD_UPDATE:
			snprintf(combine->data, COMPLETION_TAG_BUFSIZE,
					 "UPDATE %lu", total);
			break;
		case CMD_DELETE:
			snprintf(combine->data, COMPLETION_TAG_BUFSIZE,
					 "DELETE %lu", total);
			break;
		default:
			strcpy(combine->data, "");
			break;
	}

}

/*
 * HandleDatanodeCommandId ('M') message from a Datanode connection
 */
static void
HandleDatanodeCommandId(ResponseCombiner *combiner, char *msg_body, size_t len)
{
	uint32		n32;
	CommandId	cid;

	Assert(msg_body != NULL);
	Assert(len >= 2);

	/* Get the command Id */
	memcpy(&n32, &msg_body[0], 4);
	cid = pg_ntoh32(n32);

	/* If received command Id is higher than current one, set it to a new value */
	if (cid > GetReceivedCommandId())
		SetReceivedCommandId(cid);
}

static void
HandleSimpleDmlCtid(ResponseCombiner *combiner, char *msg_body, size_t len)
{
	/* Get the command Id */
	memcpy(&combiner->simpledml_newctid, &msg_body[0], sizeof(ItemPointerData));
}

/*
 * Record waited-for XIDs received from the remote nodes into the transaction
 * state
 */
static void
HandleWaitXids(char *msg_body, size_t len)
{
	int xid_count;
	uint32		n32;
	int cur;
	int i;

	/* Get the xid count */
	xid_count = len / sizeof (TransactionId);

	cur = 0;
	for (i = 0; i < xid_count; i++)
	{
		Assert(cur < len);
		memcpy(&n32, &msg_body[cur], sizeof (TransactionId));
		cur = cur + sizeof (TransactionId);
		TransactionRecordXidWait(pg_ntoh32(n32));
	}
}

#ifdef __USE_GLOBAL_SNAPSHOT__
static void
HandleGlobalTransactionId(char *msg_body, size_t len)
{
	GlobalTransactionId xid;

	Assert(len == sizeof (GlobalTransactionId));
	memcpy(&xid, &msg_body[0], sizeof (GlobalTransactionId));

	SetTopTransactionId(xid);
}
#endif
/*
 * Examine the specified combiner state and determine if command was completed
 * successfully
 */
bool
validate_combiner(ResponseCombiner *combiner)
{
	/* There was error message while combining */
	if (combiner->errorMessage)
	{
		elog(DEBUG1,
			"validate_combiner there is errorMessage in combiner, node:%s, backend_pid:%d, %s",
			combiner->errorNode, combiner->backend_pid, combiner->errorMessage);
		return false;
	}

	/* Check all nodes completed */
	if ((combiner->request_type == REQUEST_TYPE_COMMAND
			|| combiner->request_type == REQUEST_TYPE_QUERY)
			&& combiner->command_complete_count != combiner->node_count)
	{
		elog(DEBUG1,
			"validate_combiner request_type is %d, command_complete_count:%d not equal node_count:%d",
			combiner->request_type, combiner->command_complete_count, combiner->node_count);
		return false;
	}

	/* Check count of description responses */
	if ((combiner->request_type == REQUEST_TYPE_QUERY && combiner->description_count != combiner->node_count))
	{
		elog(LOG,
			"validate_combiner request_type is REQUEST_TYPE_QUERY, description_count:%d not equal node_count:%d",
			combiner->description_count, combiner->node_count);
		return false;
	}

	/* Check count of copy-in responses */
	if (combiner->request_type == REQUEST_TYPE_COPY_IN
			&& combiner->copy_in_count != combiner->node_count)
	{
		elog(LOG,
			"validate_combiner request_type is REQUEST_TYPE_COPY_IN, copy_in_count:%d not equal node_count:%d",
			combiner->copy_in_count, combiner->node_count);
		return false;
	}

	/* Check count of copy-out responses */
	if (combiner->request_type == REQUEST_TYPE_COPY_OUT
			&& combiner->copy_out_count != combiner->node_count)
	{
		elog(LOG,
			"validate_combiner request_type is REQUEST_TYPE_COPY_OUT, copy_out_count:%d not equal node_count:%d",
			combiner->copy_out_count, combiner->node_count);
		return false;
	}

	/* Add other checks here as needed */

	/* All is good if we are here */
	return true;
}

/*
 * Close combiner and free allocated memory, if it is not needed
 */
void
CloseCombiner(ResponseCombiner *combiner)
{
	/* in some cases the memory is allocated in connections handle, we can not free it and it is not necessary to free it here, because the memory context will be reset. */
#if 0
	if (combiner->connections)
	{
		pfree(combiner->connections);
		combiner->connections = NULL;
	}
#endif
	if (combiner->connections)
	{
		int i;
		for (i = 0; i < combiner->conn_count; i++)
		{
			if (combiner->connections[i]->combiner)
				combiner->connections[i]->combiner = NULL;
		}
	}

	if (combiner->tuple_desc)
	{
		FreeTupleDesc(combiner->tuple_desc);
		combiner->tuple_desc = NULL;
	}
	if (combiner->errorMessage)
	{
		pfree(combiner->errorMessage);
		combiner->errorMessage = NULL;
	}
	if (combiner->errorDetail)
	{
		pfree(combiner->errorDetail);
		combiner->errorDetail = NULL;
	}
	if (combiner->errorHint)
	{
		pfree(combiner->errorHint);
		combiner->errorHint = NULL;
	}
	if (combiner->cursor_connections)
	{
		pfree(combiner->cursor_connections);
		combiner->cursor_connections = NULL;
	}
	if (combiner->tapenodes)
	{
		pfree(combiner->tapenodes);
		combiner->tapenodes = NULL;
	}
	if (combiner->tapemarks)
	{
		pfree(combiner->tapemarks);
		combiner->tapemarks = NULL;
		combiner->tapemarks_count = 0;
	}
#ifdef __OPENTENBASE__
	combiner->context = NULL;
	combiner->resowner = NULL;
#endif
}

/*
 * Validate combiner and release storage freeing allocated memory
 */
bool
ValidateAndCloseCombiner(ResponseCombiner *combiner)
{
	bool		valid = validate_combiner(combiner);

	if (!valid)
		pgxc_node_report_error(combiner);

	CloseCombiner(combiner);

	return valid;
}

/*
 * It is possible if multiple steps share the same Datanode connection, when
 * executor is running multi-step query or client is running multiple queries
 * using Extended Query Protocol. After returning next tuple ExecRemoteQuery
 * function passes execution control to the executor and then it can be given
 * to the same RemoteQuery or to different one. It is possible that before
 * returning a tuple the function do not read all Datanode responses. In this
 * case pending responses should be read in context of original RemoteQueryState
 * till ReadyForQuery message and data rows should be stored (buffered) to be
 * available when fetch from that RemoteQueryState is requested again.
 * BufferConnection function does the job.
 * If a RemoteQuery is going to use connection it should check connection state.
 * DN_CONNECTION_STATE_QUERY indicates query has data to read and combiner
 * points to the original RemoteQueryState. If combiner differs from "this" the
 * connection should be buffered.
 */
void
BufferConnection(PGXCNodeHandle *conn)
{
	ResponseCombiner *combiner = conn->combiner;
	MemoryContext oldcontext;

	/* connection under subplan or cache send can't be buffered */
	if (combiner == NULL || combiner->conn_count == 0 ||
		!IsConnectionStateQuery(conn) || conn->block_connection)
		return;

	elog(DEBUG3, "Buffer connection %s pid: %d to step combiner: %p",
			conn->nodename, conn->backend_pid, combiner);

	/*
	 * When BufferConnection is invoked CurrentContext is related to other
	 * portal, which is trying to control the connection.
	 * TODO See if we can find better context to switch to
	 */
	if (combiner->controller)
		oldcontext = MemoryContextSwitchTo(combiner->context);
	else
		oldcontext = MemoryContextSwitchTo(combiner->ss.ps.ps_ResultTupleSlot->tts_mcxt);

	/* Verify the connection is in use by the combiner */
	combiner->current_conn = 0;
	while (combiner->current_conn < combiner->conn_count)
	{
		if (combiner->connections[combiner->current_conn] == conn)
			break;
		combiner->current_conn++;
	}
	//Assert(combiner->current_conn < combiner->conn_count);

	if (combiner->tapemarks == NULL)
	{
		elog(DEBUG1, "BufferConnection:tapemarks palloc, "
			"conn_count:%d", combiner->conn_count);

		Assert(0 == combiner->tapemarks_count);

		combiner->tapemarks = (ListCell**)
			palloc0(combiner->conn_count * sizeof(ListCell*));
		combiner->tapemarks_count = combiner->conn_count;
	}
	else if (combiner->conn_count > combiner->tapemarks_count)
	{
		elog(LOG, "BufferConnection:tapemarks repalloc, "
			"conn_count(%d) > tapemarks_count(%d)",
			combiner->conn_count, combiner->tapemarks_count);

		combiner->tapemarks = (ListCell**) repalloc(combiner->tapemarks,
			combiner->conn_count * sizeof(ListCell*));
		combiner->tapemarks_count = combiner->conn_count;
	}

	/*
	 * If current bookmark for the current tape is not set it means either
	 * first row in the buffer is from the current tape or no rows from
	 * the tape in the buffer, so if first row is not from current
	 * connection bookmark the last cell in the list.
	 */
	if (combiner->tapemarks[combiner->current_conn] == NULL &&
			list_length(combiner->rowBuffer) > 0)
	{
		RemoteDataRow dataRow = (RemoteDataRow) linitial(combiner->rowBuffer);
		if (dataRow->msgnode != conn->nodeoid)
			combiner->tapemarks[combiner->current_conn] = list_tail(combiner->rowBuffer);
	}

	/*
	 * Buffer data rows until data node return number of rows specified by the
	 * fetch_size parameter of last Execute message (PortalSuspended message)
	 * or end of result set is reached (CommandComplete message)
	 */
	while (true)
	{
		int res;

		/* Move to buffer currentRow (received from the data node) */
		if (combiner->currentRow)
		{
#ifdef __OPENTENBASE__
			if (combiner->merge_sort)
			{
				int node_index = combiner->current_conn;

				if (combiner->dataRowBuffer && combiner->dataRowBuffer[node_index])
				{
					combiner->tmpslot->tts_datarow = combiner->currentRow;

					tuplestore_puttupleslot(combiner->dataRowBuffer[node_index], combiner->tmpslot);

					combiner->nDataRows[node_index]++;

					pfree(combiner->currentRow);
				}
				else
				{
					if (!combiner->prerowBuffers)
					{
						combiner->prerowBuffers = (List **)palloc0(combiner->conn_count * sizeof(List*));
					}

					if (!combiner->dataRowMemSize)
					{
						combiner->dataRowMemSize = (long *)palloc0(sizeof(long) * combiner->conn_count);
					}

					combiner->prerowBuffers[node_index] = lappend(combiner->prerowBuffers[node_index],
																  combiner->currentRow);

					combiner->dataRowMemSize[node_index] += combiner->currentRow->msglen;

					if (combiner->dataRowMemSize[node_index] >= DATA_ROW_BUFFER_SIZE(1))
					{
						/* init datarow buffer */
						if (!combiner->dataRowBuffer)
						{
							combiner->dataRowBuffer = (Tuplestorestate **)palloc0(sizeof(Tuplestorestate *) * combiner->conn_count);
						}

						if (!combiner->dataRowBuffer[node_index])
						{
							combiner->dataRowBuffer[node_index] = tuplestore_begin_datarow(false, work_mem, NULL, combiner->context, combiner->resowner);
						}

						/* data row count in tuplestore */
						if (!combiner->nDataRows)
						{
							combiner->nDataRows = (int *)palloc0(sizeof(int) * combiner->conn_count);
						}

						if (!combiner->tmpslot)
						{
							TupleDesc desc = CreateTemplateTupleDesc(1, false);
							combiner->tmpslot = MakeSingleTupleTableSlot(desc);
						}
					}
				}
			}
			else
			{
#endif
				if (combiner->dataRowBuffer && combiner->dataRowBuffer[0])
				{
					/* put into tuplestore */
					combiner->tmpslot->tts_datarow = combiner->currentRow;

					tuplestore_puttupleslot(combiner->dataRowBuffer[0], combiner->tmpslot);

					combiner->nDataRows[0]++;

					pfree(combiner->currentRow);
				}
				else
				{
					/* init datarow size */
					if (!combiner->dataRowMemSize)
					{
						combiner->dataRowMemSize = (long *)palloc(sizeof(long));
						combiner->dataRowMemSize[0] = 0;
					}

					combiner->rowBuffer = lappend(combiner->rowBuffer,
												  combiner->currentRow);

					combiner->dataRowMemSize[0] += combiner->currentRow->msglen;

					if (combiner->dataRowMemSize[0] >= DATA_ROW_BUFFER_SIZE(1))
					{
											/* init datarow buffer */
						if (!combiner->dataRowBuffer)
						{
							combiner->dataRowBuffer = (Tuplestorestate **)palloc0(sizeof(Tuplestorestate *));
						}

						if (!combiner->dataRowBuffer[0])
						{
							combiner->dataRowBuffer[0] = tuplestore_begin_datarow(false, work_mem, NULL, combiner->context, combiner->resowner);
						}

						/* data row count in tuplestore */
						if (!combiner->nDataRows)
						{
							combiner->nDataRows = (int *)palloc(sizeof(int));
							combiner->nDataRows[0] = 0;
						}

						if (!combiner->tmpslot)
						{
							TupleDesc desc = CreateTemplateTupleDesc(1, false);
							combiner->tmpslot = MakeSingleTupleTableSlot(desc);
						}
					}
				}
#ifdef __OPENTENBASE__
			}
#endif
			combiner->currentRow = NULL;
		}

		res = handle_response(conn, combiner);
		/*
		 * If response message is a DataRow it will be handled on the next
		 * iteration.
		 * PortalSuspended will cause connection state change and break the loop
		 * The same is for CommandComplete, but we need additional handling -
		 * remove connection from the list of active connections.
		 * We may need to add handling error response
		 */

		/* Most often result check first */
		if (res == RESPONSE_DATAROW)
		{
			/*
			 * The row is in the combiner->currentRow, on next iteration it will
			 * be moved to the buffer
			 */
			continue;
		}

		if (res == RESPONSE_EOF)
		{
			/* incomplete message, read more */
			if (pgxc_node_receive(1, &conn, NULL))
			{
				UpdateConnectionState(conn, DN_CONNECTION_STATE_FATAL);
				add_error_message(conn, "Failed to fetch from data node");
			}
		}

		/*
		 * End of result set is reached, so either set the pointer to the
		 * connection to NULL (combiner with sort) or remove it from the list
		 * (combiner without sort)
		 */
		else if (res == RESPONSE_COMPLETE)
		{
			/*
			 * If combiner is doing merge sort we should set reference to the
			 * current connection to NULL in the array, indicating the end
			 * of the tape is reached. FetchTuple will try to access the buffer
			 * first anyway.
			 * Since we remove that reference we can not determine what node
			 * number was this connection, but we need this info to find proper
			 * tuple in the buffer if we are doing merge sort. So store node
			 * number in special array.
			 * NB: We can not test if combiner->tuplesortstate is set here:
			 * connection may require buffering inside tuplesort_begin_merge
			 * - while pre-read rows from the tapes, one of the tapes may be
			 * the local connection with RemoteSubplan in the tree. The
			 * combiner->tuplesortstate is set only after tuplesort_begin_merge
			 * returns.
			 */
			if (combiner->merge_sort)
			{
				combiner->connections[combiner->current_conn]->combiner = NULL;
				combiner->connections[combiner->current_conn] = NULL;
				if (combiner->tapenodes == NULL)
					combiner->tapenodes = (Oid *)
							palloc0(combiner->conn_count * sizeof(Oid));
				combiner->tapenodes[combiner->current_conn] = conn->nodeoid;
			}
			else
			{
				/* Remove current connection, move last in-place, adjust current_conn */
				REMOVE_CURR_CONN(combiner);
			}
			/*
			 * If combiner runs Simple Query Protocol we need to read in
			 * ReadyForQuery. In case of Extended Query Protocol it is not
			 * sent and we should quit.
			 */
			if (conn->state.in_extended_query)
				break;

			if (IsConnectionStateFatal(conn))
			{
				ereport(ERROR,
						(errcode(ERRCODE_CONNECTION_EXCEPTION),
						 errmsg("Unexpected FATAL ERROR on Connection to "
								"Datanode %s pid %d",
								conn->nodename, conn->backend_pid)));
			}
		}
		else if (res == RESPONSE_ERROR)
		{
			if (combiner->extended_query)
			{
				/*
				 * Need to sync connection to enable receiving commands
				 * by the datanode
				 */
				if (pgxc_node_send_sync(conn) != 0)
				{
					ereport(ERROR,
							(errcode(ERRCODE_CONNECTION_EXCEPTION),
							 errmsg("Failed to sync msg to node %s backend_pid:%d",
									conn->nodename, conn->backend_pid)));
				}
#ifdef _PG_REGRESS_
				ereport(LOG,
						(errcode(ERRCODE_CONNECTION_EXCEPTION),
						 errmsg("Succeed to sync msg to node %s backend_pid:%d",
								conn->nodename, conn->backend_pid)));
#endif

			}
		}
		else if (res == RESPONSE_SUSPENDED || res == RESPONSE_READY)
		{
			/* Now it is OK to quit */
			break;
		}
	}
	Assert(!IsConnectionStateQuery(conn));
	MemoryContextSwitchTo(oldcontext);

	if(enable_distri_print)
		PrintHandleState(conn);

	conn->combiner = NULL;
}
/*
 * Prefetch data from specific connection.
 */
bool
PreFetchConnection(PGXCNodeHandle *conn, int32 node_index)
{
	bool              bComplete  = false;
	DNConnectionState state   = DN_CONNECTION_STATE_IDLE;
	int    			  ret 		 = 0;
	ResponseCombiner *combiner   = conn->combiner;
	MemoryContext     oldcontext;
	struct timeval timeout;
	timeout.tv_sec			  = 0;
	timeout.tv_usec 		  = 1000;

	if (combiner == NULL || !IsA(&combiner->ss.ps, RemoteQueryState))
	{
		return bComplete;
	}

	elog(DEBUG1, "PreFetchConnection connection %u to step %s", conn->nodeoid, combiner->cursor);

	/*
	 * When BufferConnection is invoked CurrentContext is related to other
	 * portal, which is trying to control the connection.
	 * TODO See if we can find better context to switch to
	 */
	oldcontext = MemoryContextSwitchTo(combiner->ss.ps.ps_ResultTupleSlot->tts_mcxt);
	if (combiner->tapemarks == NULL)
	{
		elog(DEBUG1, "PreFetchConnection:tapemarks palloc, "
			"conn_count:%d, node_index:%d", combiner->conn_count, node_index);

		Assert(0 == combiner->tapemarks_count);

		combiner->tapemarks = (ListCell**)
			palloc0(combiner->conn_count * sizeof(ListCell*));
		combiner->tapemarks_count = combiner->conn_count;
	}
	else if (combiner->conn_count > combiner->tapemarks_count)
	{
		elog(LOG, "PreFetchConnection:tapemarks repalloc, "
			"conn_count(%d) > tapemarks_count(%d), node_index:%d",
			combiner->conn_count, combiner->tapemarks_count, node_index);

		combiner->tapemarks = (ListCell**) repalloc(combiner->tapemarks,
			combiner->conn_count * sizeof(ListCell*));
		combiner->tapemarks_count = combiner->conn_count;
	}

	/*
	 * If current bookmark for the current tape is not set it means either
	 * first row in the buffer is from the current tape or no rows from
	 * the tape in the buffer, so if first row is not from current
	 * connection bookmark the last cell in the list.
	 */
	if (combiner->tapemarks[node_index] == NULL &&
			list_length(combiner->rowBuffer) > 0)
	{
		RemoteDataRow dataRow = (RemoteDataRow) linitial(combiner->rowBuffer);
		if (dataRow->msgnode != conn->nodeoid)
		{
			combiner->tapemarks[node_index] = list_tail(combiner->rowBuffer);
		}
	}

	if (combiner->merge_sort)
	{
		if (!combiner->prerowBuffers)
			combiner->prerowBuffers = (List **)palloc0(combiner->conn_count * sizeof(List*));
	}

	/*
	 * Buffer data rows until data node return number of rows specified by the
	 * fetch_size parameter of last Execute message (PortalSuspended message)
	 * or end of result set is reached (CommandComplete message)
	 */
	while (true)
	{
		int res;

		/* Move to buffer currentRow (received from the data node) */
		if (combiner->currentRow)
		{
			int msglen = combiner->currentRow->msglen;

			if (combiner->merge_sort)
			{
				if (!combiner->dataRowMemSize)
				{
					combiner->dataRowMemSize = (long *)palloc0(sizeof(long) * combiner->conn_count);
				}

				if (combiner->dataRowMemSize[node_index] >= DATA_ROW_BUFFER_SIZE(1))
				{
					/* init datarow buffer */
					if (!combiner->dataRowBuffer)
					{
						combiner->dataRowBuffer = (Tuplestorestate **)palloc0(sizeof(Tuplestorestate *) * combiner->conn_count);
					}

					if (!combiner->dataRowBuffer[node_index])
					{
						combiner->dataRowBuffer[node_index] = tuplestore_begin_datarow(false, work_mem / NumDataNodes, NULL, combiner->context, combiner->resowner);
					}

					/* data row count in tuplestore */
					if (!combiner->nDataRows)
					{
						combiner->nDataRows = (int *)palloc0(sizeof(int) * combiner->conn_count);
					}

					if (!combiner->tmpslot)
					{
						TupleDesc desc = CreateTemplateTupleDesc(1, false);
						combiner->tmpslot = MakeSingleTupleTableSlot(desc);
					}

					combiner->tmpslot->tts_datarow = combiner->currentRow;

					tuplestore_puttupleslot(combiner->dataRowBuffer[node_index], combiner->tmpslot);

					combiner->nDataRows[node_index]++;

					pfree(combiner->currentRow);

					combiner->currentRow = NULL;
				}
				else
				{
					combiner->prerowBuffers[node_index] = lappend(combiner->prerowBuffers[node_index],
																  combiner->currentRow);

					combiner->dataRowMemSize[node_index] += msglen;

					combiner->currentRow = NULL;
				}
			}
			else
			{
				/* init datarow size */
				if (!combiner->dataRowMemSize)
				{
					combiner->dataRowMemSize = (long *)palloc(sizeof(long));
					combiner->dataRowMemSize[0] = 0;
				}

				/* exceed buffer size, store into tuplestore */
				if (combiner->dataRowMemSize[0] >= DATA_ROW_BUFFER_SIZE(1))
				{
					/* init datarow buffer */
					if (!combiner->dataRowBuffer)
					{
						combiner->dataRowBuffer = (Tuplestorestate **)palloc0(sizeof(Tuplestorestate *));
					}

					if (!combiner->dataRowBuffer[0])
					{
						combiner->dataRowBuffer[0] = tuplestore_begin_datarow(false, work_mem / NumDataNodes, NULL, combiner->context, combiner->resowner);
					}

					/* data row count in tuplestore */
					if (!combiner->nDataRows)
					{
						combiner->nDataRows = (int *)palloc(sizeof(int));
						combiner->nDataRows[0] = 0;
					}

					if (!combiner->tmpslot)
					{
						TupleDesc desc = CreateTemplateTupleDesc(1, false);
						combiner->tmpslot = MakeSingleTupleTableSlot(desc);
					}

					/* put into tuplestore */
					combiner->tmpslot->tts_datarow = combiner->currentRow;

					tuplestore_puttupleslot(combiner->dataRowBuffer[0], combiner->tmpslot);

					combiner->nDataRows[0]++;

					pfree(combiner->currentRow);

					combiner->currentRow = NULL;
				}
				else
				{
					combiner->dataRowMemSize[0] += msglen;

					combiner->rowBuffer = lappend(combiner->rowBuffer,
										  combiner->currentRow);

					combiner->currentRow = NULL;
				}
			}
		}

		res = handle_response(conn, combiner);
		/*
		 * If response message is a DataRow it will be handled on the next
		 * iteration.
		 * PortalSuspended will cause connection state change and break the loop
		 * The same is for CommandComplete, but we need additional handling -
		 * remove connection from the list of active connections.
		 * We may need to add handling error response
		 */

		/* Most often result check first */
		if (res == RESPONSE_DATAROW)
		{
			/*
			 * The row is in the combiner->currentRow, on next iteration it will
			 * be moved to the buffer
			 */
			continue;
		}

		/* incomplete message, read more */
		if (res == RESPONSE_EOF)
		{
			/* Here we will keep waiting without timeout. */
			state = GetConnectionState(conn);
			ret = pgxc_node_receive(1, &conn, &timeout);
			if (DNStatus_ERR == ret)
			{
				UpdateConnectionState(conn, DN_CONNECTION_STATE_FATAL);
				add_error_message(conn, "Failed to fetch from data node");
				MemoryContextSwitchTo(oldcontext);
				return false;
			}
			else if (DNStatus_EXPIRED == ret)
			{
				/* Restore the connection state. */
				UpdateConnectionState(conn, state);
				MemoryContextSwitchTo(oldcontext);
				return false;
			}
		}

		/*
		 * End of result set is reached, so either set the pointer to the
		 * connection to NULL (combiner with sort) or remove it from the list
		 * (combiner without sort)
		 */
		else if (res == RESPONSE_COMPLETE)
		{
			if (combiner->extended_query)
			{
				/*
				 * If combiner is doing merge sort we should set reference to the
				 * current connection to NULL in the array, indicating the end
				 * of the tape is reached. FetchTuple will try to access the buffer
				 * first anyway.
				 * Since we remove that reference we can not determine what node
				 * number was this connection, but we need this info to find proper
				 * tuple in the buffer if we are doing merge sort. So store node
				 * number in special array.
				 * NB: We can not test if combiner->tuplesortstate is set here:
				 * connection may require buffering inside tuplesort_begin_merge
				 * - while pre-read rows from the tapes, one of the tapes may be
				 * the local connection with RemoteSubplan in the tree. The
				 * combiner->tuplesortstate is set only after tuplesort_begin_merge
				 * returns.
				 */
				if (combiner->merge_sort)
				{
					combiner->connections[node_index] = NULL;
					if (combiner->tapenodes == NULL)
					{
						combiner->tapenodes = (Oid *)palloc0(combiner->conn_count * sizeof(Oid));
					}
					combiner->tapenodes[node_index] = conn->nodeoid;
				}
				else if (combiner->extended_query)
				{

#ifdef _PG_REGRESS_
					ereport(LOG,
							(errcode(ERRCODE_CONNECTION_EXCEPTION),
							 errmsg("CommandComplete remove extend query connection %s backend_pid:%d",
									conn->nodename, conn->backend_pid)));
#endif
					/* Remove connection with node_index, move last in-place. Here the node_index > current_conn*/
					if (node_index < --combiner->conn_count)
					{
						combiner->connections[node_index] = combiner->connections[combiner->conn_count];
						combiner->current_conn_rows_consumed = 0;
						bComplete = true;

						if (combiner->current_conn >= combiner->conn_count)
							combiner->current_conn = node_index;
					}
				}

				/*
				 * If combiner runs Simple Query Protocol we need to read in
				 * ReadyForQuery. In case of Extended Query Protocol it is not
				 * sent and we should quit.
				 */
				break;
			}
			else if (IsConnectionStateFatal(conn))
			{
				ereport(ERROR,
						(errcode(ERRCODE_CONNECTION_EXCEPTION),
						 errmsg("Unexpected FATAL ERROR on Connection to "
								"Datanode %s pid %d",
								conn->nodename, conn->backend_pid)));
			}
		}
		else if (res == RESPONSE_READY)
		{
#ifdef _PG_REGRESS_
			ereport(LOG,
					(errcode(ERRCODE_CONNECTION_EXCEPTION),
					 errmsg("ReadyForQuery remove connection %s backend_pid:%d",
							conn->nodename, conn->backend_pid)));
#endif

			/* If we are doing merge sort clean current connection and return
			 * NULL, otherwise remove current connection, move last in-place,
			 * adjust current_conn and continue if it is not last connection */
			if (combiner->merge_sort)
			{
				combiner->connections[node_index] = NULL;
				bComplete = true;
				break;
			}

			if (node_index < --combiner->conn_count)
			{
				combiner->connections[node_index] = combiner->connections[combiner->conn_count];
				combiner->current_conn_rows_consumed = 0;
				bComplete = true;

				if (combiner->current_conn >= combiner->conn_count)
					combiner->current_conn = node_index;
			}

			bComplete = true;
			break;
		}
		else if (res == RESPONSE_ERROR)
		{
			if (combiner->extended_query)
			{
				/*
				 * Need to sync connection to enable receiving commands
				 * by the datanode
				 */
				if (pgxc_node_send_sync(conn) != 0)
				{
					ereport(ERROR,
							(errcode(ERRCODE_CONNECTION_EXCEPTION),
							 errmsg("Failed to sync msg to node %s backend_pid:%d",
									conn->nodename, conn->backend_pid)));


				}
#ifdef _PG_REGRESS_
				ereport(LOG,
						(errcode(ERRCODE_CONNECTION_EXCEPTION),
						 errmsg("Succeed to sync msg to node %s backend_pid:%d",
								conn->nodename, conn->backend_pid)));
#endif
			}
		}
		else if (res == RESPONSE_SUSPENDED || res == RESPONSE_READY)
		{
			/* Now it is OK to quit */
			break;
		}
		else if (res == RESPONSE_TUPDESC)
		{
			ExecSetSlotDescriptor(combiner->ss.ps.ps_ResultTupleSlot,
								  combiner->tuple_desc);
			/* Now slot is responsible for freeng the descriptor */
			combiner->tuple_desc = NULL;
		}
	}
	Assert(!IsConnectionStateQuery(conn));
	MemoryContextSwitchTo(oldcontext);

	if (combiner->errorMessage)
	{
		pgxc_node_report_error(combiner);
	}
	return bComplete;
}

/*
 * copy the datarow from combiner to the given slot, in the slot's memory
 * context
 */
void
CopyDataRowTupleToSlot(ResponseCombiner *combiner, TupleTableSlot *slot)
{
	RemoteDataRow 	datarow;
	MemoryContext	oldcontext;
	oldcontext = MemoryContextSwitchTo(slot->tts_mcxt);
	datarow = (RemoteDataRow) palloc(sizeof(RemoteDataRowData) + combiner->currentRow->msglen);
	datarow->msgnode = combiner->currentRow->msgnode;
	datarow->msglen = combiner->currentRow->msglen;
	datarow->data_from_remote_tid_scan = combiner->currentRow->data_from_remote_tid_scan;
	memcpy(datarow->msg, combiner->currentRow->msg, datarow->msglen);
	ExecStoreDataRowTuple(datarow, slot, true);
	pfree(combiner->currentRow);
	combiner->currentRow = NULL;
	MemoryContextSwitchTo(oldcontext);
}


/*
 * FetchTuple
 *
		Get next tuple from one of the datanode connections.
 * The connections should be in combiner->connections, if "local" dummy
 * connection presents it should be the last active connection in the array.
 *      If combiner is set up to perform merge sort function returns tuple from
 * connection defined by combiner->current_conn, or NULL slot if no more tuple
 * are available from the connection. Otherwise it returns tuple from any
 * connection or NULL slot if no more available connections.
 * 		Function looks into combiner->rowBuffer before accessing connection
 * and return a tuple from there if found.
 * 		Function may wait while more data arrive from the data nodes. If there
 * is a locally executed subplan function advance it and buffer resulting rows
 * instead of waiting.
 */
TupleTableSlot *
FetchTuple(ResponseCombiner *combiner)
{
	PGXCNodeHandle *conn;
	TupleTableSlot *slot;
	RemoteFragmentController *ctl = combiner->ss.ps.state->es_remote_controller;
	Oid 			nodeOid = -1;

	/*
	 * Get current connection
	 */
	if (combiner->conn_count > combiner->current_conn)
		conn = combiner->connections[combiner->current_conn];
	else
		conn = NULL;

	/*
	 * If doing merge sort determine the node number.
	 * It may be needed to get buffered row.
	 */
	if (combiner->merge_sort)
	{
		Assert(conn || combiner->tapenodes);
		nodeOid = conn ? conn->nodeoid :
						 combiner->tapenodes[combiner->current_conn];
		Assert(OidIsValid(nodeOid));
	}

READ_ROWBUFFER:
	/*
	 * First look into the row buffer.
	 * When we are performing merge sort we need to get from the buffer record
	 * from the connection marked as "current". Otherwise get first.
	 */
	if (list_length(combiner->rowBuffer) > 0)
	{
		RemoteDataRow dataRow;

		Assert(combiner->currentRow == NULL);

		if (combiner->merge_sort)
		{
			ListCell *lc;
			ListCell *prev;

			elog(DEBUG1, "Getting buffered tuple from node %x", nodeOid);

			prev = combiner->tapemarks[combiner->current_conn];
			if (prev)
			{
				/*
				 * Start looking through the list from the bookmark.
				 * Probably the first cell we check contains row from the needed
				 * node. Otherwise continue scanning until we encounter one,
				 * advancing prev pointer as well.
				 */
				while ((lc = lnext(prev)) != NULL)
				{
					dataRow = (RemoteDataRow) lfirst(lc);
					if (dataRow->msgnode == nodeOid)
					{
						combiner->currentRow = dataRow;
						break;
					}
					prev = lc;
				}
			}
			else
			{
				/*
				 * Either needed row is the first in the buffer or no such row
				 */
				lc = list_head(combiner->rowBuffer);
				dataRow = (RemoteDataRow) lfirst(lc);
				if (dataRow->msgnode == nodeOid)
					combiner->currentRow = dataRow;
				else
					lc = NULL;
			}

			if (lc)
			{
				/*
				 * Delete cell from the buffer. Before we delete we must check
				 * the bookmarks, if the cell is a bookmark for any tape.
				 * If it is the case we are deleting last row of the current
				 * block from the current tape. That tape should have bookmark
				 * like current, and current bookmark will be advanced when we
				 * read the tape once again.
				 */
				int i;
				for (i = 0; i < combiner->conn_count; i++)
				{
					if (combiner->tapemarks[i] == lc)
						combiner->tapemarks[i] = prev;
				}
				elog(DEBUG1, "Found buffered tuple from node %x", nodeOid);
				combiner->rowBuffer = list_delete_cell(combiner->rowBuffer,
													   lc, prev);
			}
			elog(DEBUG1, "Update tapemark");
			combiner->tapemarks[combiner->current_conn] = prev;
		}
		else
		{
			dataRow = (RemoteDataRow) linitial(combiner->rowBuffer);
			combiner->currentRow = dataRow;
			combiner->rowBuffer = list_delete_first(combiner->rowBuffer);
		}
	}

#ifdef __OPENTENBASE__
	/* fetch datarow from prerowbuffers */
	if (!combiner->currentRow)
	{
		if (combiner->merge_sort)
		{
			RemoteDataRow dataRow;
			int node_index = combiner->current_conn;

			if (combiner->prerowBuffers &&
				list_length(combiner->prerowBuffers[node_index]) > 0)
			{
				dataRow = (RemoteDataRow) linitial(combiner->prerowBuffers[node_index]);
				combiner->currentRow = dataRow;
				combiner->prerowBuffers[node_index] = list_delete_first(combiner->prerowBuffers[node_index]);
			}
		}
	}

	if (!combiner->currentRow)
	{
		/* fetch data in tuplestore */
		if (combiner->merge_sort)
		{
			int node_index = combiner->current_conn;

			if (combiner->dataRowBuffer && combiner->dataRowBuffer[node_index])
			{
				if (tuplestore_gettupleslot(combiner->dataRowBuffer[node_index],
										   true, true, combiner->tmpslot))
				{
					combiner->tmpslot->tts_shouldFreeRow = false;
					combiner->currentRow = combiner->tmpslot->tts_datarow;
					combiner->nDataRows[node_index]--;
				}
				else
				{
					/* sanity check */
					if (combiner->nDataRows[node_index] != 0)
					{
						elog(ERROR, "connection %d has %d datarows left in tuplestore.",
									 node_index, combiner->nDataRows[node_index]);
					}

					/*
					  * datarows fetched from tuplestore in memory will be freed by caller,
					  * we do not need to free them in tuplestore_end, tuplestore_set_tupdeleted
					  * avoid to free memtuples in tuplestore_end.
					  */
					tuplestore_end(combiner->dataRowBuffer[node_index]);
					combiner->dataRowBuffer[node_index] = NULL;
					combiner->dataRowMemSize[node_index] = 0;
				}
			}
		}
		else
		{
			if (combiner->dataRowBuffer && combiner->dataRowBuffer[0])
			{
				if (tuplestore_gettupleslot(combiner->dataRowBuffer[0],
										   true, true, combiner->tmpslot))
				{
					combiner->tmpslot->tts_shouldFreeRow = false;
					combiner->currentRow = combiner->tmpslot->tts_datarow;
					combiner->nDataRows[0]--;
				}
				else
				{
					/* sanity check */
					if (combiner->nDataRows[0] != 0)
					{
						elog(ERROR, "%d datarows left in tuplestore.", combiner->nDataRows[0]);
					}

					/*
					  * datarows fetched from tuplestore in memory will be freed by caller,
					  * we do not need to free them in tuplestore_end, tuplestore_set_tupdeleted
					  * avoid to free memtuples in tuplestore_end.
					  */
					tuplestore_end(combiner->dataRowBuffer[0]);
					combiner->dataRowBuffer[0] = NULL;
					combiner->dataRowMemSize[0] = 0;
				}
			}
		}
	}
#endif

	/* If we have node message in the currentRow slot, and it is from a proper
	 * node, consume it.  */
	if (combiner->currentRow)
	{
		Assert(!combiner->merge_sort ||
			   combiner->currentRow->msgnode == nodeOid);
		slot = combiner->ss.ps.ps_ResultTupleSlot;
		CopyDataRowTupleToSlot(combiner, slot);

		return slot;
	}

	while (conn)
	{
		int res;

		/* Going to use a connection, buffer it if needed */
		DrainConnectionAndSetNewCombiner(conn, combiner);

		/*
		 * If current connection is idle it means portal on the data node is
		 * suspended. Request more and try to get it
		 */
		if (combiner->extended_query &&
				IsConnectionStateIdle(conn))
		{
			/*
			 * We do not allow to suspend if querying primary node, so that
			 * only may mean the current node is secondary and subplan was not
			 * executed there yet. Return and go on with second phase.
			 */
			if (combiner->probing_primary)
			{
				return NULL;
			}

			if (pgxc_node_send_execute(conn, combiner->cursor, PGXLRemoteFetchSize, false) != 0)
			{
				ereport(ERROR,
						(errcode(ERRCODE_CONNECTION_EXCEPTION),
						 errmsg("Failed to send execute cursor '%s' to node %u",
								combiner->cursor, conn->nodeoid)));
			}

			if (SendFlushRequest(conn) != 0)
			{
				ereport(ERROR,
						(errcode(ERRCODE_CONNECTION_EXCEPTION),
						 errmsg("Failed flush cursor '%s' node %u",
								combiner->cursor, conn->nodeoid)));
			}

			if (pgxc_node_receive(1, &conn, NULL))
			{
				ereport(ERROR,
						(errcode(ERRCODE_CONNECTION_EXCEPTION),
						 errmsg("Failed receive data from node %u cursor '%s'",
								conn->nodeoid, combiner->cursor)));
			}
		}

		/* read messages */
		res = handle_response(conn, combiner);
		if (res == RESPONSE_DATAROW)
		{
			slot = combiner->ss.ps.ps_ResultTupleSlot;
			CopyDataRowTupleToSlot(combiner, slot);
			combiner->current_conn_rows_consumed++;

			/*
			 * If we are running simple query protocol, yield the connection
			 * after we process PGXLRemoteFetchSize rows from the connection.
			 * This should allow us to consume rows quickly from other
			 * connections, while this node gets chance to generate more rows
			 * which would then be processed in the next iteration.
			 */
			if (!combiner->extended_query &&
				combiner->current_conn_rows_consumed >= PGXLRemoteFetchSize)
			{
				if (++combiner->current_conn >= combiner->conn_count)
				{
					combiner->current_conn = 0;
				}
				combiner->current_conn_rows_consumed = 0;
			}

			return slot;
		}
		else if (res == RESPONSE_EOF)
		{
			/*
			 * We encountered incomplete message.
			 * There are two strategies;
			 * 1. postpone the current node and receive from other nodes.
			 * 2. wait for current node to response.
			 *
			 * In most circumstances, the first one is faster because it decreases the waiting time.
			 * but when we encounter merge sort or other circumstance, it must wait for all nodes
			 * to calculate the merging result. if we use strategy one, we have to store all then data
			 * in memory which results in poor performance.
			 *
			 * TODO: remove switch and choose strategy according to work.
			 */
			if (postpone_slow_node)
			{
				/*
				 * We encountered incomplete message, try to read more.
				 * Here if we read timeout, then we move to other connections to read, because we
				 * easily got deadlock if a specific cursor run as producer on two nodes. If we can
				 * consume data from all all connections, we can break the deadlock loop.
				 */
				bool   bComplete          = false;
				DNConnectionState state   = DN_CONNECTION_STATE_IDLE;
				int    i                  = 0;
				int    ret 			      = 0;
				PGXCNodeHandle *save_conn = NULL;
				struct timeval timeout;
				timeout.tv_sec  	      = 0;
				timeout.tv_usec 	      = 1000;

				save_conn = conn;
				while (1)
				{
					conn  = save_conn;
					state = GetConnectionState(conn); /* Save the connection state. */

					if (ctl && !HAS_MESSAGE_BUFFERED(conn) && IsConnectionStateQuery(conn))
					{
						/* no timeout */
						if (RunRemoteControllerWithFD(conn->sock, ctl, 1) == RemoteControllerEventFN)
							timeout.tv_usec = 0;
					}

					ret   = pgxc_node_receive(1, &conn, &timeout);
					if (DNStatus_OK == ret)
					{
						/* We got data, handle it. */
						break;
					}
					else if (DNStatus_ERR == ret)
					{
						ereport(ERROR,
								(errcode(ERRCODE_EXEC_REMOTE_ERROR),
								 errmsg("Failed to receive more data from "
										"data node %s pid %d",
										conn->nodename, conn->backend_pid)));
					}
					else
					{
						/* Restore the saved state of connection. */
						UpdateConnectionState(conn, state);
					}

					/* Try to read data from other connections. */
					for (i = 0; i < combiner->conn_count; i ++)
					{
						conn  = combiner->connections[i];
						if (save_conn != conn && conn != NULL)
						{
							/* Save the connection state. */
							state = GetConnectionState(conn);
							if (IsConnectionStateQuery(conn))
							{
								ret = pgxc_node_receive(1, &conn, &timeout);
								if (DNStatus_OK == ret)
								{
									/* We got data, prefetch it. */
									bComplete = PreFetchConnection(conn, i);
									if (bComplete)
									{
										/* Receive Complete on one connection, we need retry to read from current_conn. */
										break;
									}
									else
									{
										/* Maybe Suspend or Expired, just move to next connection and read. */
										continue;
									}
								}
								else if (DNStatus_EXPIRED == ret)
								{
									/* Restore the saved state of connection. */
									UpdateConnectionState(conn, state);
									continue;
								}
								else
								{
									ereport(ERROR,
											(errcode(ERRCODE_EXEC_REMOTE_ERROR),
											 errmsg("Failed to receive more "
													"data from data node %s pid %d",
													conn->nodename, conn->backend_pid)));
								}
							}
						}
					}
				}
			}
			else
			{
				/* incomplete message, read more */
				if (pgxc_node_receive(1, &conn, NULL))
					ereport(ERROR,
							(errcode(ERRCODE_EXEC_REMOTE_ERROR),
							 errmsg("Failed to receive more data from data "
									"node %s pid %d",
									conn->nodename, conn->backend_pid)));
			}
			continue;
		}
		else if (res == RESPONSE_SUSPENDED)
		{
			/*
			 * If we are doing merge sort or probing primary node we should
			 * remain on the same node, so query next portion immediately.
			 * Otherwise leave node suspended and fetch lazily.
			 */
			if (combiner->merge_sort || combiner->probing_primary)
			{
				if (pgxc_node_send_execute(conn, combiner->cursor, PGXLRemoteFetchSize, false) != 0)
					ereport(ERROR,
							(errcode(ERRCODE_CONNECTION_EXCEPTION),
							 errmsg("Failed to send execute cursor '%s' to node %u",
									combiner->cursor, conn->nodeoid)));
				if (SendFlushRequest(conn) != 0)
					ereport(ERROR,
							(errcode(ERRCODE_CONNECTION_EXCEPTION),
							 errmsg("Failed flush cursor '%s' node %u",
									combiner->cursor, conn->nodeoid)));
				if (pgxc_node_receive(1, &conn, NULL))
					ereport(ERROR,
							(errcode(ERRCODE_CONNECTION_EXCEPTION),
							 errmsg("Failed receive node from node %u cursor '%s'",
									conn->nodeoid, combiner->cursor)));
				continue;
			}

			/*
			 * Tell the node to fetch data in background, next loop when we
			 * pgxc_node_receive, data is already there, so we can run faster
			 * */
			if (pgxc_node_send_execute(conn, combiner->cursor, PGXLRemoteFetchSize, false) != 0)
			{
				ereport(ERROR,
						(errcode(ERRCODE_CONNECTION_EXCEPTION),
						 errmsg("Failed to send execute cursor '%s' to node %u",
								combiner->cursor, conn->nodeoid)));
			}

			if (SendFlushRequest(conn) != 0)
			{
				ereport(ERROR,
						(errcode(ERRCODE_CONNECTION_EXCEPTION),
						 errmsg("Failed flush cursor '%s' node %u",
								combiner->cursor, conn->nodeoid)));
			}

			if (++combiner->current_conn >= combiner->conn_count)
			{
				combiner->current_conn = 0;
			}
			combiner->current_conn_rows_consumed = 0;
			conn = combiner->connections[combiner->current_conn];
		}
		else if (res == RESPONSE_COMPLETE)
		{
			if (IsConnectionStateFatal(conn))
			{
				ereport(ERROR,
						(errcode(ERRCODE_CONNECTION_EXCEPTION),
						 errmsg("Unexpected FATAL ERROR on Connection to Datanode %s pid %d",
								conn->nodename, conn->backend_pid)));
			}

			/*
			 * In case of Simple Query Protocol we should receive ReadyForQuery
			 * before removing connection from the list. In case of Extended
			 * Query Protocol we may remove connection right away.
			 */
			if (combiner->extended_query)
			{
				/* If we are doing merge sort clean current connection and return
				 * NULL, otherwise remove current connection, move last in-place,
				 * adjust current_conn and continue if it is not last connection */
				if (combiner->merge_sort)
				{

					combiner->connections[combiner->current_conn]->combiner = NULL;
					combiner->connections[combiner->current_conn] = NULL;

					/* data left in row_buffer, read it */
					if ((combiner->prerowBuffers && list_length(combiner->prerowBuffers[combiner->current_conn]) > 0) ||
						(combiner->dataRowBuffer && combiner->dataRowBuffer[combiner->current_conn]))
					{
						elog(DEBUG1, "FetchTuple:data left in rowbuffer while merge_sort.");
						conn = NULL;
						goto READ_ROWBUFFER;
					}

					return NULL;
				}

				REMOVE_CURR_CONN(combiner);
				if (combiner->conn_count > 0)
				{
					conn = combiner->connections[combiner->current_conn];
					combiner->current_conn_rows_consumed = 0;
				}
				else
				{
					/* data left in row_buffer, read it */
					if (list_length(combiner->rowBuffer) > 0 ||
						(combiner->dataRowBuffer && combiner->dataRowBuffer[0]))
					{
						elog(DEBUG1, "FetchTuple:data left in rowbuffer in extended_query.");
						goto READ_ROWBUFFER;
					}

					return NULL;
				}
			}
		}
		else if (res == RESPONSE_ERROR)
		{
			/*
			 * Do not wait for response from primary, it needs to wait
			 * for other nodes to respond. Instead go ahead and send query to
			 * other nodes. It will fail there, but we can continue with
			 * normal cleanup.
			 */
			if (combiner->probing_primary)
			{
				REMOVE_CURR_CONN(combiner);
			}
			return NULL;
		}
		else if (res == RESPONSE_READY)
		{
			/* If we are doing merge sort clean current connection and return
			 * NULL, otherwise remove current connection, move last in-place,
			 * adjust current_conn and continue if it is not last connection */
			if (combiner->merge_sort)
			{
				combiner->connections[combiner->current_conn]->combiner = NULL;
				combiner->connections[combiner->current_conn] = NULL;

				/* data left in row_buffer, read it */
				if ((combiner->prerowBuffers && list_length(combiner->prerowBuffers[combiner->current_conn]) > 0) ||
					(combiner->dataRowBuffer && combiner->dataRowBuffer[combiner->current_conn]))
				{
					elog(DEBUG1, "FetchTuple:data left in rowbuffer while merge_sort.");
					conn = NULL;
					goto READ_ROWBUFFER;
				}

				return NULL;
			}

			REMOVE_CURR_CONN(combiner);
			if (combiner->conn_count > 0)
				conn = combiner->connections[combiner->current_conn];
			else
			{
				/* data left in row_buffer, read it */
				if (list_length(combiner->rowBuffer) > 0 ||
					(combiner->dataRowBuffer && combiner->dataRowBuffer[0]))
				{
					elog(DEBUG1, "FetchTuple:data left in rowbuffer in simple_query.");
					goto READ_ROWBUFFER;
				}
				return NULL;
			}
		}
		else if (res == RESPONSE_TUPDESC)
		{
			ExecSetSlotDescriptor(combiner->ss.ps.ps_ResultTupleSlot,
								  combiner->tuple_desc);
			/* Now slot is responsible for freeng the descriptor */
			combiner->tuple_desc = NULL;
		}
		else if (res == RESPONSE_ASSIGN_GXID)
		{
			/* Do nothing. It must have been handled in handle_response() */
		}
		else if (res == RESPONSE_WAITXIDS)
		{
			/* Do nothing. It must have been handled in handle_response() */
		}
		else
		{
			/* Can not get here? */
			Assert(false);
		}
	}

	return NULL;
}

#ifdef __OPENTENBASE__
static int
pgxc_node_receive_copy_begin(const int conn_count, PGXCNodeHandle ** connections,
						 struct timeval * timeout, ResponseCombiner *combiner)
{
	int 		ret   = 0;
	int 		count = conn_count;
	PGXCNodeHandle *to_receive[conn_count];

	/* make a copy of the pointers to the connections */
	memcpy(to_receive, connections, conn_count * sizeof(PGXCNodeHandle *));

	/*
	 * Read results.
	 * Note we try and read from Datanode connections even if there is an error on one,
	 * so as to avoid reading incorrect results on the next statement.
	 * Other safegaurds exist to avoid this, however.
	 */
	while (count > 0)
	{
		int i = 0;
		ret = pgxc_node_receive(count, to_receive, timeout);
		if (DNStatus_ERR == ret)
		{
			elog(LOG, "pgxc_node_receive_copy_begin pgxc_node_receive data from node number:%d failed", count);
			return EOF;
		}

		while (i < count)
		{
			int result =  handle_response(to_receive[i], combiner);
			elog(DEBUG5, "Received response %d on connection to node %s",
					result, to_receive[i]->nodename);
			switch (result)
			{
				case RESPONSE_EOF: /* have something to read, keep receiving */
					i++;
					break;
				case RESPONSE_COMPLETE:
					if (!IsConnectionStateFatal(to_receive[i]))
					{
						/* Continue read until ReadyForQuery */
						break;
					}
					else
					{
						/* error occurred, set buffer logically empty */
						to_receive[i]->inStart = 0;
						to_receive[i]->inCursor = 0;
						to_receive[i]->inEnd = 0;
					}
					/* fallthru */
				case RESPONSE_READY:
					/* fallthru */
				case RESPONSE_COPY:
					/* Handling is done, do not track this connection */
					count--;
					/* Move last connection in place */
					if (i < count)
						to_receive[i] = to_receive[count];
					break;
				case RESPONSE_ERROR:
					/* no handling needed, just wait for ReadyForQuery */
					break;

				case RESPONSE_WAITXIDS:
				case RESPONSE_ASSIGN_GXID:
				case RESPONSE_TUPDESC:
					break;

				case RESPONSE_DATAROW:
					combiner->currentRow = NULL;
					break;

				default:
					/* Inconsistent responses */
					add_error_message(to_receive[i], "Unexpected response from the Datanodes");
					elog(DEBUG1, "Unexpected response from the Datanodes, result = %d, request type %d", result, combiner->request_type);
					/* Stop tracking and move last connection in place */
					count--;
					if (i < count)
						to_receive[i] = to_receive[count];
			}
		}
	}

	return 0;
}

#endif

/*
 * Handle responses from the Datanode connections
 */
int
pgxc_node_receive_responses(const int conn_count, PGXCNodeHandle ** connections,
						 struct timeval * timeout, ResponseCombiner *combiner)
{
#ifdef __TWO_PHASE_TRANS__
	bool        has_errmsg = false;
	int         connection_index;
#endif
	int			ret   = 0;
	int			count = conn_count;
	PGXCNodeHandle *to_receive[conn_count];
	int         func_ret = 0;
	int 		last_fatal_conn_count = 0;
#ifdef __TWO_PHASE_TRANS__
	int         receive_to_connections[conn_count];
#endif

	/* make a copy of the pointers to the connections */
	memcpy(to_receive, connections, conn_count * sizeof(PGXCNodeHandle *));

	/*
	 * Read results.
	 * Note we try and read from Datanode connections even if there is an error on one,
	 * so as to avoid reading incorrect results on the next statement.
	 * Other safegaurds exist to avoid this, however.
	 */
	while (count > 0)
	{
		int i = 0;
		ret = pgxc_node_receive(count, to_receive, timeout);
		if (DNStatus_ERR == ret)
		{
			int conn_loop;
			int rece_idx = 0;
			int fatal_conn_inner;
#ifdef __TWO_PHASE_TRANS__
			has_errmsg = true;
#endif
			elog(LOG, "pgxc_node_receive_responses pgxc_node_receive data from node number:%d failed", count);
			/* mark has error */
			func_ret = -1;
			memset((char*)to_receive, 0, conn_count*sizeof(PGXCNodeHandle *));

			fatal_conn_inner = 0;
			for (conn_loop = 0; conn_loop < conn_count; conn_loop++)
			{
#ifdef __TWO_PHASE_TRANS__
				receive_to_connections[conn_loop] = conn_loop;
#endif
				if (!IsConnectionStateFatal(connections[conn_loop]))
				{
					to_receive[rece_idx] = connections[conn_loop];
#ifdef __TWO_PHASE_TRANS__
					receive_to_connections[rece_idx] = conn_loop;
#endif
					rece_idx++;
				}
				else
				{
					fatal_conn_inner++;
				}
			}

			if (last_fatal_conn_count == fatal_conn_inner)
			{
#ifdef __TWO_PHASE_TRANS__
				/*
				 * if exit abnormally reset response_operation for next call
				 */
				g_twophase_state.response_operation = OTHER_OPERATIONS;
#endif
				return EOF;
			}
			last_fatal_conn_count = fatal_conn_inner;

			count = rece_idx;
		}

		while (i < count)
		{
			int32 nbytes = 0;
			int result =  0;

			if (am_proxy_for_dn)
			{
				result =  handle_response_on_proxy(to_receive[i], combiner);
			}
			else
			{
				result =  handle_response(to_receive[i], combiner);
			}
#ifdef __OPENTENBASE__
#ifdef 	_PG_REGRESS_
			elog(LOG, "Received response %d on connection to node %s",
					result, to_receive[i]->nodename);
#else
			elog(DEBUG5, "Received response %d on connection to node %s",
					result, to_receive[i]->nodename);
#endif
#endif
			switch (result)
			{
				case RESPONSE_EOF: /* have something to read, keep receiving */
					/* when receive fragment response and receive error msg, it should skip the receive, then report error on caller */
					if (combiner->handle_ftable_resp && DN_CONNECTION_ERROR(to_receive[i]))
					{
						/* Handling is done, do not track this connection */
						count--;
						/* Move last connection in place */
						if (i < count)
							to_receive[i] = to_receive[count];
						break;
					}

					i++;
					break;
				case RESPONSE_COMPLETE:
					if (!IsConnectionStateFatal(to_receive[i]))
					{
                        if (combiner->waitforC)
                        {
                            /* Handling is done, do not track this connection */
                            count--;
                            /* Move last connection in place */
                            if (i < count)
                                to_receive[i] = to_receive[count];
                        }
						/* Continue read until ReadyForQuery */
						break;
					}
					else
					{
						/* error occurred, set buffer logically empty */
						to_receive[i]->inStart = 0;
						to_receive[i]->inCursor = 0;
						to_receive[i]->inEnd = 0;
					}
					/* fallthru */
				case RESPONSE_READY:
					/* fallthru */
#ifdef __TWO_PHASE_TRANS__
					if (!has_errmsg)
					{
						connection_index = i;
					}
					else
					{
						connection_index = receive_to_connections[i];
					}
					UpdateLocalTwoPhaseState(result, to_receive[i], connection_index, combiner->errorMessage);
#endif
					if (IsConnectionStateIdle(to_receive[i]) || 
						IsConnectionStateError(to_receive[i]) || 
						IsConnectionStateFatal(to_receive[i]))
					{
						count--;
						if (i < count)
							to_receive[i] = to_receive[count];
					}
					break;	
				case RESPONSE_COPY:
					/* try to read every byte from peer. */
					nbytes = pgxc_node_is_data_enqueued(to_receive[i]);
					if (nbytes)
					{
						int32 			  ret = 0;
						DNConnectionState estate =  DN_CONNECTION_STATE_IDLE;
						/* Have data in buffer, try to receive and retry. */
						elog(DEBUG1,
							 "Pending response %d bytes on connection to node "
							 "%s, pid %d try to read again. ",
							 nbytes, to_receive[i]->nodename,
							 to_receive[i]->backend_pid);
						estate = GetConnectionState(to_receive[i]);
						UpdateConnectionState(to_receive[i], DN_CONNECTION_STATE_QUERY);
						ret = pgxc_node_receive(1, &to_receive[i], NULL);
						if (ret)
						{
							switch (ret)
							{
								case DNStatus_ERR:
									{
										elog(LOG, "pgxc_node_receive Pending data %d bytes from node:%s pid:%d failed for ERROR:%s. ", nbytes, to_receive[i]->nodename, to_receive[i]->backend_pid, strerror(errno));
										break;
									}

								case DNStatus_EXPIRED:
									{
										elog(LOG, "pgxc_node_receive Pending data %d bytes from node:%s pid:%d failed for EXPIRED. ", nbytes, to_receive[i]->nodename, to_receive[i]->backend_pid);
										break;
									}
								default:
									{
										/* Can not be here.*/
										break;
									}
							}
						}
						UpdateConnectionState(to_receive[i], estate);
						i++;
						break;
					}

					/* Handling is done, do not track this connection */
					count--;
					/* Move last connection in place */
					if (i < count)
						to_receive[i] = to_receive[count];
					break;
				case RESPONSE_ERROR:
					/* no handling needed, just wait for ReadyForQuery */
#ifdef __TWO_PHASE_TRANS__
					if (!has_errmsg)
					{
						connection_index = i;
					}
					else
					{
						connection_index = receive_to_connections[i];
					}
					UpdateLocalTwoPhaseState(result, to_receive[i], connection_index, combiner->errorMessage);
#endif
                    if (combiner->waitforE)
                    {
                        /* Handling is done, do not track this connection */
                        count--;
                        /* Move last connection in place */
                        if (i < count)
                            to_receive[i] = to_receive[count];
                    }
					break;

				case RESPONSE_WAITXIDS:
				case RESPONSE_ASSIGN_GXID:
				case RESPONSE_TUPDESC:
					break;

				case RESPONSE_DATAROW:
					combiner->currentRow = NULL;
					break;

				case RESPONSE_SUSPENDED:
					if (combiner->waitfor2)
					{
						UpdateConnectionState(to_receive[i], DN_CONNECTION_STATE_IDLE);
						/* Handling is done, do not track this connection */
						count--;
						/* Move last connection in place */
						if (i < count)
							to_receive[i] = to_receive[count];
					}
					break;
				default:
					/* Inconsistent responses */
					add_error_message(to_receive[i], "Unexpected response from the Datanodes");
					elog(DEBUG1, "Unexpected response from the Datanodes, result = %d, request type %d", result, combiner->request_type);
					/* Stop tracking and move last connection in place */
					count--;
					if (i < count)
						to_receive[i] = to_receive[count];
			}
		}
	}

#ifdef __TWO_PHASE_TRANS__
	g_twophase_state.response_operation = OTHER_OPERATIONS;
#endif
	return func_ret;
}

static inline void
SafeSwitchConnectionState(PGXCNodeHandle *handle, ResponseCombiner *combiner, char msgtype, int msg_len, char *msg)
{
	HOLD_INTERRUPTS();
	handle->last_command = msgtype;

	if (msg_len < 0)
	{
		UpdateConnectionState(handle, DN_CONNECTION_STATE_FATAL);
		elog(WARNING, "unexpected message length %d from node %s pid %d", 
				msg_len, handle->nodename, handle->backend_pid);
		return;
	}

	switch(msgtype)
	{
		case 'E':	/* error */
			UpdateConnectionState(handle, DN_CONNECTION_STATE_ERROR);
			break;
		case 'C':	/* complete */
			if (combiner && combiner->extended_query && IsConnectionStateQuery(handle))
			{
				UpdateConnectionState(handle, DN_CONNECTION_STATE_IDLE);
				SetHandleUsed(handle, false);
			}
			break;
		case 's':
		case '4':
		case '5':
		case 'Y':
		case 'b':
		case 'J':
		case 'a':
		case 'e':
			UpdateConnectionState(handle, DN_CONNECTION_STATE_IDLE);
			break;
		case 'F': /* a tmporary namespace is created on this connection */
			if (handle->holdFlag != SessionHold)
			{
				handle->holdFlag = SessionHold;
				if (enable_distri_print)
					elog(LOG, "connection %s pid %d session hold is on", handle->nodename, handle->backend_pid);
			}
			break;
		case 'G':
			UpdateConnectionState(handle, DN_CONNECTION_STATE_COPY_IN);
			break;
		case 'H':
		case 'd':
			UpdateConnectionState(handle, DN_CONNECTION_STATE_COPY_OUT);
			break;
		case 'Z':
			SetTxnState(handle, msg[0]);
			if (!IsConnectionStateError(handle) && HandledAllReplay(handle))
				UpdateConnectionState(handle, DN_CONNECTION_STATE_IDLE);
			break;
		case '9':
		{
			uint8 sno = (uint8)msg[0];
			if (!checkSequenceMatch(handle, sno))
			{
				elog(LOG, "the response sequence is mismatched, node: %s, pid: %d, expected sno: %d, response sno: %d",
					 handle->nodename, handle->backend_pid, handle->state.sequence_number, sno);
				if (handle->cmd_strict)
					UpdateConnectionState(handle, DN_CONNECTION_STATE_FATAL);
			}
		}
			break;
		case '2':
			if (combiner && combiner->waitfor2)
				UpdateConnectionState(handle, DN_CONNECTION_STATE_IDLE);
			break;
		default:
			/* need not to care abort other message */
			break;
	}
	RESUME_INTERRUPTS();
}

/*
 * Read next message from the connection and update the combiner
 * and connection state accordingly
 * If we are in an error state we just consume the messages, and do not proxy
 * Long term, we should look into cancelling executing statements
 * and closing the connections.
 * It returns if states need to be handled
 * Return values:
 * RESPONSE_EOF - need to receive more data for the connection
 * RESPONSE_READY - got ReadyForQuery
 * RESPONSE_COMPLETE - done with the connection, but not yet ready for query.
 * Also this result is output in case of error
 * RESPONSE_SUSPENDED - got PortalSuspended
 * RESPONSE_TUPLEDESC - got tuple description
 * RESPONSE_DATAROW - got data row
 * RESPONSE_COPY - got copy response
 * RESPONSE_BARRIER_OK - barrier command completed successfully
 */
int
handle_response_internal(PGXCNodeHandle *conn, ResponseCombiner *combiner, char *file, int line)
{
	char	   *msg;
	int			msg_len;
	char		msg_type;
	uint8		res_seq;

	for (;;)
	{
		/*
		 * If we are in the process of shutting down, we
		 * may be rolling back, and the buffer may contain other messages.
		 * We want to avoid a procarray exception
		 * as well as an error stack overflow.
		 */
		if (proc_exit_inprogress)
		{
			UpdateConnectionState(conn, DN_CONNECTION_STATE_FATAL);
		}

		/*
		 * Don't read from from the connection if there is a fatal error.
		 * We still return RESPONSE_COMPLETE, not RESPONSE_ERROR, since
		 * Handling of RESPONSE_ERROR assumes sending SYNC message, but
		 * State DN_CONNECTION_STATE_FATAL indicates connection is
		 * not usable.
		 */
		if (IsConnectionStateFatal(conn))
		{
			return RESPONSE_COMPLETE;
		}

		/* No data available, exit */
		if (!HAS_MESSAGE_BUFFERED(conn))
			return RESPONSE_EOF;

		/* TODO handle other possible responses */
		msg_type = get_message(conn, &msg_len, &msg);

		if (enable_distri_print)
			elog(LOG, "handle_response - received message %c, node %s %d, current_state %s txn state:  %c, file %s:%d",
				 msg_type, conn->nodename, conn->backend_pid, GetConnectionStateTag(conn), GetTxnState(conn), file, line);

		/*
		 * Add some protection code when receiving a messy message,
		 * close the connection, and throw error
		 */
		if (msg_len < 0)
			DestroyHandle(conn);

		if (msg_type != 'D' && msg_type != '9')
			RecordRecvMessage(conn, msg_type, NULL, 0);

		/*  the state */
		SafeSwitchConnectionState(conn, combiner, msg_type, msg_len, msg);

		switch (msg_type)
		{
			case '\0':			/* Not enough data in the buffer */
				return RESPONSE_EOF;

			case 'c':			/* CopyToCommandComplete */
				if (combiner)
					HandleCopyOutComplete(combiner);
				break;

			case 'C':			/* CommandComplete */
				/*
				 * After the CommandComplete message, some message dislocation can be tolerated.
				 * For example, the DN node reports an error and sends ReadyForQuery to this node
				 */
				conn->cmd_strict = false;

				if (combiner)
					HandleCommandComplete(combiner, msg, msg_len, conn);
				conn->combiner = NULL;
				return RESPONSE_COMPLETE;
			case 'F':			/* Session Hold flag */
				break;
			case 'T':			/* RowDescription */
#ifdef DN_CONNECTION_DEBUG
				Assert(!conn->have_row_desc);
				conn->have_row_desc = true;
#endif
				Assert(!IsTransactionCommit());
				if (IsTransactionCommit())
				{
					/* Shouldn't be here. */
					elog(FATAL, "Unexpected T message when transacation is already committed, "
								"remote node %s, remote pid %d, current_state %s, transaction_state %c, subtrans_id %d",
						 conn->nodename, conn->backend_pid,
						 GetConnectionStateTag(conn), GetTxnState(conn), GetSubTransactionId(conn));
				}
				if (combiner && HandleRowDescription(combiner, msg, msg_len))
					return RESPONSE_TUPDESC;
				break;

			case 'D':			/* DataRow */
			case 'X':			/* DataRow ,but from remote tid scan*/
#ifdef DN_CONNECTION_DEBUG
				Assert(conn->have_row_desc);
#endif
				/* Do not return if data row has not been actually handled */
				if (combiner && HandleDataRow(combiner, msg, msg_len, conn->nodeoid, msg_type == 'X'))
				{
					elog(DEBUG1, "HandleDataRow from node %s, remote pid %d",
						 conn->nodename, conn->backend_pid);
					return RESPONSE_DATAROW;
				}
				break;

			case 's':			/* PortalSuspended */
				/* No activity is expected on the connection until next query */
				return RESPONSE_SUSPENDED;

			case '2': /* BindComplete */
			{
				if (combiner && combiner->waitfor2)
					return RESPONSE_BIND_COMPLETE;
				break;
			}

			case '1': /* ParseComplete */
			case '3': /* CloseComplete */
			case 'n': /* NoData */
				/* simple notifications, continue reading */
				break;
#ifdef __SUBSCRIPTION__
			case '4': /* ApplyDone */
				if (combiner)
					HandleApplyDone(combiner);
				return RESPONSE_READY;
				break;
#endif
#ifdef __OPENTENBASE_C__
			case '5': /* FtableDone */
				if (combiner)
					HandleFtableDone(combiner);
				return RESPONSE_READY;
				break;
#endif
			case 'G': /* CopyInResponse */
				if(combiner)
					HandleCopyIn(combiner);
				/* Done, return to caller to let it know the data can be passed in */
				return RESPONSE_COPY;

			case 'H': /* CopyOutResponse */
				if (combiner)
					HandleCopyOut(combiner);
				return RESPONSE_COPY;

			case 'd': /* CopyOutDataRow */
				if (combiner)
					HandleCopyDataRow(combiner, msg, msg_len);
				break;

			case 'E':			/* ErrorResponse */
			    /*
			     * If an error occurs, cn may send sync messages for many times.
			     * At this time, it can tolerate some message misplacements
			     */
				conn->cmd_strict = false;
				if (combiner)
				{
					HandleError(combiner, msg, msg_len, conn);
					add_error_message_from_combiner(conn, combiner);
				}
				/*
				 * In case the remote node was running an extended query
				 * protocol and reported an error, it will keep ignoring all
				 * subsequent commands until it sees a SYNC message. So make
				 * sure that we send down SYNC even before sending a ROLLBACK
				 * command
				 */
				if (conn->state.in_extended_query)
				{
					conn->needSync = true;
				}
#ifdef 	_PG_REGRESS_
				elog(LOG, "HandleError from node %s, remote pid %d, errorMessage:%s, %s: %d",
						conn->nodename, conn->backend_pid, combiner ? combiner->errorMessage : "combiner NULL", file, line);
#endif
				return RESPONSE_ERROR;

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
				/*
				 * Return result depends on previous connection state.
				 * If it was PORTAL_SUSPENDED Coordinator want to send down
				 * another EXECUTE to fetch more rows, otherwise it is done
				 * with the connection
				 */
				SetHandleUsed(conn, false);
				conn->combiner = NULL;

				elog(DEBUG5, "remote_node %s remote_pid %d, conn->state.transaction_state %c",
					conn->nodename, conn->backend_pid, GetTxnState(conn));
#ifdef DN_CONNECTION_DEBUG
				conn->have_row_desc = false;
#endif

#ifdef 	_PG_REGRESS_
				elog(LOG, "ReadyForQuery from node %s, remote pid %d", conn->nodename, conn->backend_pid);
#endif
				if (HandledAllReplay(conn))
					return RESPONSE_READY;
				elog(LOG, "node %s pid %d received ready for query, but not expected one expected: %d, received: %d",
						conn->nodename, conn->backend_pid, conn->state.sequence_number, conn->state.received_sno);
				return RESPONSE_READY_MISMATCH;
			}
			case '8':
			{
				uint64 feedbackGts = pg_ntoh64(*(uint64*)msg);
				if(enable_distri_print)
					elog(LOG, "get feedbackGts " INT64_FORMAT " from datanode:%s", feedbackGts, conn->nodename);

				UpdatelocalCacheGtsUseFeedbackGts(feedbackGts);
				break;
			}

#ifdef __SUPPORT_DISTRIBUTED_TRANSACTION__
			case 'Y':			/* ReadyForCommit */
			{
				conn->combiner = NULL;
#ifdef DN_CONNECTION_DEBUG
				conn->have_row_desc = false;
#endif
				if (enable_distri_print)
					elog(LOG, "ReadyForQuery from node %s, remote pid %d", conn->nodename, conn->backend_pid);
				return RESPONSE_READY;
			}
#endif
			case 'M':			/* Command Id */
				if (combiner)
					HandleDatanodeCommandId(combiner, msg, msg_len);
				break;

			case 'b':
				return RESPONSE_BARRIER_OK;

			case 'I':			/* EmptyQuery */
#ifdef 	_PG_REGRESS_
				elog(LOG, "RESPONSE_COMPLETE_3 from node %s, remote pid %d", conn->nodename, conn->backend_pid);
#endif
				return RESPONSE_COMPLETE;

			case 'W':
				HandleWaitXids(msg, msg_len);
				return RESPONSE_WAITXIDS;

			case 'x':
				#ifdef __USE_GLOBAL_SNAPSHOT__
				HandleGlobalTransactionId(msg, msg_len);
				#else
				elog(ERROR, "should not set global xid");
				#endif
				return RESPONSE_ASSIGN_GXID;
			case 'J':
				CNReceiveResGroupReply();
				conn->combiner = NULL;
				return RESPONSE_COMPLETE;
			case 't': /*simple dml ctid*/
				if (combiner)
					HandleSimpleDmlCtid(combiner, msg, msg_len);
				break;
			case '9':
				res_seq = (uint8)msg[0];
				conn->state.received_sno = res_seq;
				RecordRecvMessage(conn, msg_type, (char *)&res_seq, 1);
				if (enable_distri_print)
					elog(LOG, "get cmd_seq from datanode:%s cmd_seq:%u", conn->nodename, res_seq);
				break;
			case 'a':
				conn->combiner = NULL;
				SetHandleUsed(conn, false);
				return ERSPONSE_ANALYZE;
			case 'e':
				conn->combiner = NULL;
				return RESPONSE_COMPLETE;
			case 'i': /* Remote Instrument */
				if (msg_len > 0)
					HandleRemoteInstr(msg, msg_len, conn->nodeoid, conn->fid);
				break;
			default:
				/* sync lost? */
				elog(WARNING, "Received unsupported message type: %c", msg_type);
				UpdateConnectionState(conn, DN_CONNECTION_STATE_FATAL);
				/* stop reading */
				elog(LOG, "RESPONSE_COMPLETE_4 from node %s, remote pid %d", conn->nodename, conn->backend_pid);
				return RESPONSE_COMPLETE;
		}
	}
	/* never happen, but keep compiler quiet */
	return RESPONSE_EOF;
}

/*
 * Has the data node sent Ready For Query
 */

bool
is_data_node_ready(PGXCNodeHandle * conn)
{
	char		*msg;
	int		    msg_len;
	char		msg_type;
	size_t      data_len = 0;

	for (;;)
	{
		/* No data available, exit */
		if (!HAS_MESSAGE_BUFFERED(conn))
		{
			return false;
		}

		/*
		* If the length of one data row is longger than MaxAllocSize>>1,
		* it seems there was something wrong,
		* to close this connection should be a better way to save reading loop and avoid overload read buffer.
		*/
		data_len = pg_ntoh32(*((uint32_t *) ((conn)->inBuffer + (conn)->inCursor + 1)));
		if (data_len >= (MaxAllocSize>>1))
		{
			elog(LOG, "size:%lu too big in buffer, close socket on node:%u now", data_len, conn->nodeoid);
			DestroyHandle(conn);
			return true;
		}

		msg_type = get_message(conn, &msg_len, &msg);
		if ('Z' == msg_type)
		{
			/*
			 * Return result depends on previous connection state.
			 * If it was PORTAL_SUSPENDED Coordinator want to send down
			 * another EXECUTE to fetch more rows, otherwise it is done
			 * with the connection
			 */
			conn->last_command = msg_type;
			SetTxnState(conn, msg[0]);
			UpdateConnectionState(conn, DN_CONNECTION_STATE_IDLE);
			conn->combiner = NULL;
			return true;
		}
		else if ('8' == msg_type)
		{
			uint64 feedbackGts = pg_ntoh64(*(uint64*)msg);
			if(enable_distri_print)
				elog(LOG, "get feedbackGts " INT64_FORMAT " from datanode:%s", feedbackGts, conn->nodename);

			UpdatelocalCacheGtsUseFeedbackGts(feedbackGts);
		}
        else if (msg_type == '9')
        {
            uint8 res_seq;
            res_seq = (int8)msg[0];
            elog(DEBUG5, "get cmd_seq from datanode:%s cmd_seq:%u", conn->nodename, res_seq);
            if (!checkSequenceMatch(conn, res_seq))
            {
                elog(LOG, "cmd_seq mismatch datanode %s cmd_seq %u, res_seq %u, cmd_strict %d",
                     conn->nodename, conn->state.sequence_number, res_seq, conn->cmd_strict);
                if (conn->cmd_strict)
                    UpdateConnectionState(conn, DN_CONNECTION_STATE_FATAL);
            }
		}
	}
	/* never happen, but keep compiler quiet */
	return false;
}

/*
 * Send BEGIN command to the Datanodes or Coordinators and receive responses.
 * Also send the GXID for the transaction.
 */
int
pgxc_node_begin(int conn_count, PGXCNodeHandle **connections,
				GlobalTransactionId gxid, bool need_tran_block, bool is_global_session)
{
	int				i;
	TimestampTz		timestamp = GetCurrentGTMStartTimestamp();
	PGXCNodeHandle	*new_connections[conn_count];
	int				new_count = 0;
	char 			*txn_guc_cmd_str = NULL;
	ResponseCombiner combiner;
	uint64			 guc_cid = 0;

	/* if no remote connections, we don't have anything to do */
	if (conn_count == 0)
		return 0;

	for (i = 0; i < conn_count; i++)
	{
		/* Drain the connection before using it */
		if (IsConnectionStateQuery(connections[i]))
			DrainConnectionAndSetNewCombiner(connections[i], NULL);
	}

	if (SendSessionGucConfigs(connections, conn_count, is_global_session))
	{
		elog(LOG, "failed to send session guc configs");
		return EOF;
	}

	for (i = 0; i < conn_count; i++)
	{
		bool	need_send_begin = false;

		if (connections[i]->used)
		{
			/*
			 * If a fragment was being executed on this connection before, but
			 * its corresponding control thread has not yet received a response
			 * message from remote (usually occurs in the case of using a
			 * cursor), we can only wait for the control thread to handle it.
			 * Eventually, it will set the connection's status to "idle".
			 */
			if (IsConnectionStateQuery(connections[i]) && connections[i]->used)
			{
				elog(ERROR, "handle->nodename=%s %d, has unexpected state state %d",
					 connections[i]->nodename, connections[i]->backend_pid,
					 connections[i]->state.connection_state);
			}
		}

		/* Send global query id */
		if (pgxc_node_send_queryid(connections[i]))
		{
			ereport(WARNING,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("Failed to send query id to data node:%s, connection state is %d",
						connections[i]->nodename, GetConnectionState(connections[i]))));
			return EOF;
		}

		/* Send GXID and check for errors */
		if (pgxc_node_send_gxid(connections[i]))
		{
			elog(WARNING, "pgxc_node_begin gxid is invalid.");
			return EOF;
		}

		/* send global traceid */
		if (pgxc_node_send_global_traceid(connections[i]))
		{
			elog(WARNING, "pgxc_node_begin failed to send global traceid.");
			return EOF;
		}

		/* Send timestamp and check for errors */
		if (GlobalTimestampIsValid(timestamp) && pgxc_node_send_timestamp(connections[i], timestamp))
		{
			elog(WARNING, "pgxc_node_begin sending timestamp fails: local start timestamp" INT64_FORMAT, timestamp);
			return EOF;
		}

		pgxc_node_send_coord_info(connections[i], MyProcPid, MyProc->lxid);

		if ((need_tran_block || InPlpgsqlFunc()) && IS_PGXC_LOCAL_COORDINATOR && IsTxnStateIdle(connections[i]))
			need_send_begin = true;

		if (IS_PGXC_LOCAL_COORDINATOR && (InSubTransaction() || GetCurrentTransactionGucList()))
		{
			uint64 guc_cid = 0;

			txn_guc_cmd_str = GetHandleTransactionAndGucString(connections[i], &guc_cid);

			if (txn_guc_cmd_str && IsTxnStateIdle(connections[i]))
				need_send_begin = true;

			if (guc_cid > GetCurrentGucCid(connections[i]))
				SetCurrentGucCid(connections[i], guc_cid);
		}


		if (PlpgsqlDebugPrint)
			elog(LOG, "[PLPGSQL] pgxc_node_begin need_tran_block %d, "
				  "connections[%d]->transaction_status %c need_send_begin:%d, "
				  " %s %d txn_guc_cmd_str %s handle subxid %u cur subxid %u guc_cid %lu handle guc_cid %lu",
			 need_tran_block, i, GetTxnState(connections[i]),
			 need_send_begin, connections[i]->nodename, connections[i]->backend_pid,
			 txn_guc_cmd_str ? txn_guc_cmd_str : "NULL", GetSubTransactionId(connections[i]), 
			 GetCurrentSubTransactionId(), guc_cid, GetCurrentGucCid(connections[i]));

		SetSubTransactionId(connections[i], GetCurrentSubTransactionId());
		elog(DEBUG1, "[NEST_LEVEL_CHECK:%d] set subid: %d.", connections[i]->backend_pid, GetCurrentSubTransactionId());

		/* Send the BEGIN command if not currently in a transaction */
		if (need_send_begin)
		{
			if (pgxc_node_send_tran_command(connections[i], TRANS_STMT_BEGIN, NULL))
				return EOF;
		}

		if (txn_guc_cmd_str)
		{

			if (guc_cid > GetCurrentGucCid(connections[i]))
				SetCurrentGucCid(connections[i], guc_cid);
			
			if (enable_distri_print)
				elog(LOG, "txn_guc_cmd_str %s ", txn_guc_cmd_str);
			elog(DEBUG1, "[NEST_LEVEL_CHECK:%d] send txn_cmd: %s. cur_level: %d",
				connections[i]->backend_pid, txn_guc_cmd_str, GetCurrentTransactionNestLevel());
			if (pgxc_node_send_query_with_internal_flag(connections[i], txn_guc_cmd_str))
				return EOF;
			new_connections[new_count++] = connections[i];
		}
	}

	/*
	 * If we did not send a BEGIN command to any node, we are done.
	 */
	if (new_count == 0)
		return 0;

	InitResponseCombiner(&combiner, new_count, COMBINE_TYPE_NONE);
	/*
	 * Make sure there are zeroes in unused fields
	 */
	memset(&combiner, 0, sizeof(ScanState));

	combiner.waitforE = true;
	/* Receive responses */
	if (pgxc_node_receive_responses(new_count, new_connections, NULL, &combiner))
	{
		elog(WARNING, "pgxc_node_begin receive response fails.");
		return EOF;
	}
	/* Verify status */
	if (!ValidateAndCloseCombiner(&combiner))
	{
		elog(LOG, "pgxc_node_begin validating response fails.");
		return EOF;
	}

	/* No problem, let's get going */
	return 0;
}


/*
 * Execute DISCARD ALL command on all allocated nodes to remove all session
 * specific stuff before releasing them to pool for reuse by other sessions.
 */
static void
pgxc_node_remote_cleanup(PGXCNodeAllHandles *handles)
{
	PGXCNodeHandle *new_connections[handles->co_conn_count + handles->dn_conn_count];
	int				new_conn_count = 0;
	int				i;
	char		   *resetcmd = "RESET ALL;"
							   "RESET SESSION AUTHORIZATION;"
							   "RESET transaction_isolation;"
							   "RESET global_session";

	elog(LOG, "pgxc_node_remote_cleanup_all - handles->co_conn_count %d,"
			"handles->dn_conn_count %d", handles->co_conn_count,
			handles->dn_conn_count);

	/*
	 * We must handle reader and writer connections both since even a read-only
	 * needs to be cleaned up.
	 */
	if (handles->co_conn_count + handles->dn_conn_count == 0)
		return;

	/*
	 * Send down snapshot followed by DISCARD ALL command.
	 */
	for (i = 0; i < handles->co_conn_count; i++)
	{
		PGXCNodeHandle *handle = handles->coord_handles[i];

		/* At this point connection should be in IDLE state */
		if (!IsConnectionStateIdle(handle))
		{
			UpdateConnectionState(handle, DN_CONNECTION_STATE_FATAL);
			continue;
		}

		if (pgxc_node_send_clean_distribute(handle))
		{
			ereport(LOG,
					(errcode(ERRCODE_CONNECTION_EXCEPTION),
					 errmsg("Failed to send clean distribute msg, node %s backend pid %d", handle->nodename, handle->backend_pid)));
		}

#ifdef __OPENTENBASE__
		/*
		 * At the end of the transaction,
		 * clean up the CN info sent to the DN in pgxc_node_begin
		 */
		if (IS_PGXC_COORDINATOR)
		{
			pgxc_node_send_coord_info(handle, 0, 0);
		}
#endif
		if (ORA_MODE && pgxc_node_cleanup_exec_env(handle))
		{
			ereport(WARNING,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("Failed to setup security context when cleanup coordinator")));
			UpdateConnectionState(handle, DN_CONNECTION_STATE_FATAL);
			continue;
		}
		/*
		 * We must go ahead and release connections anyway, so do not throw
		 * an error if we have a problem here.
		 */
		if (pgxc_node_send_query_with_internal_flag(handle, resetcmd))
		{
			ereport(WARNING,
					(errcode(ERRCODE_CONNECTION_EXCEPTION),
					 errmsg("Failed to clean up data nodes")));
			UpdateConnectionState(handle, DN_CONNECTION_STATE_FATAL);
			continue;
		}
		new_connections[new_conn_count++] = handle;
		handle->combiner = NULL;
	}
	for (i = 0; i < handles->dn_conn_count; i++)
	{
		PGXCNodeHandle *handle = handles->datanode_handles[i];

		/* At this point connection should be in IDLE state */
		if (!IsConnectionStateIdle(handle))
		{
			UpdateConnectionState(handle, DN_CONNECTION_STATE_FATAL);
			continue;
		}

		if (pgxc_node_send_clean_distribute(handle))
		{
			ereport(LOG,
					(errcode(ERRCODE_CONNECTION_EXCEPTION),
					 errmsg("Failed to send clean distribute msg, node %s backend pid %d", handle->nodename, handle->backend_pid)));
		}

#ifdef __OPENTENBASE__
		/*
		 * At the end of the transaction,
		 * clean up the CN info sent to the DN in pgxc_node_begin
		 */
		if (IS_PGXC_COORDINATOR)
		{
			pgxc_node_send_coord_info(handle, 0, 0);
		}
#endif
		if (ORA_MODE && pgxc_node_cleanup_exec_env(handle))
		{
			ereport(WARNING,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("Failed to setup security context when cleanup datanode")));
			UpdateConnectionState(handle, DN_CONNECTION_STATE_FATAL);
			continue;
		}
		/*
		 * We must go ahead and release connections anyway, so do not throw
		 * an error if we have a problem here.
		 */
		if (pgxc_node_send_query_with_internal_flag(handle, resetcmd))
		{
			ereport(WARNING,
					(errcode(ERRCODE_CONNECTION_EXCEPTION),
					 errmsg("Failed to clean up data nodes")));
			UpdateConnectionState(handle, DN_CONNECTION_STATE_FATAL);
			continue;
		}
		new_connections[new_conn_count++] = handle;
		handle->combiner = NULL;
	}

	if (new_conn_count)
	{
		ResponseCombiner combiner;
		InitResponseCombiner(&combiner, new_conn_count, COMBINE_TYPE_NONE);
		/* Receive responses */
		pgxc_node_receive_responses(new_conn_count, new_connections, NULL, &combiner);
		CloseCombiner(&combiner);
	}
}

/*
 * Release all connections that can be released based on conditions
 */
void
pgxc_node_cleanup_and_release_handles(bool force, bool waitOnResGroup)
{
	bool all_handles = false;
	PGXCNodeAllHandles *handles;

	if (IS_PGXC_DATANODE)
		return;

	handles = get_releasable_handles(force, waitOnResGroup, &all_handles);

	/* Clean up remote sessions */
	pgxc_node_remote_cleanup(handles);

	/* don't free connection if holding a cluster lock */
	if (force || !cluster_ex_lock_held)
		release_handles(handles, all_handles);

	pfree_pgxc_all_handles(handles);
}

/*
 * Send a cleanup distributed message to all connections.
 */
void
pgxc_node_clean_all_handles_distribute_msg(void)
{
	PGXCNodeAllHandles 	*current_handles = NULL;
	int i;

	current_handles = get_current_txn_handles(NULL);

	if (current_handles->co_conn_count + current_handles->dn_conn_count == 0)
	{
		pfree(current_handles);
		return;
	}

	for (i = 0; i < current_handles->co_conn_count; i++)
	{
		PGXCNodeHandle *handle = current_handles->coord_handles[i];
		if (pgxc_node_send_clean_distribute(handle))
		{
			ereport(LOG,
					(errcode(ERRCODE_CONNECTION_EXCEPTION),
					 errmsg("Failed to send clean distribute msg, node %s backend pid %d", handle->nodename, handle->backend_pid)));
		}
	}

	for (i = 0; i < current_handles->dn_conn_count; i++)
	{
		PGXCNodeHandle *handle = current_handles->datanode_handles[i];
		if (pgxc_node_send_clean_distribute(handle))
		{
			ereport(LOG,
					(errcode(ERRCODE_CONNECTION_EXCEPTION),
					 errmsg("Failed to send clean distribute msg, node %s backend pid %d", handle->nodename, handle->backend_pid)));
		}
	}

	pfree_pgxc_all_handles(current_handles);
}

/*
 * Count how many coordinators and datanodes are involved in this transaction
 * so that we can save that information in the GID
 */
static void
pgxc_node_remote_count(int *dnCount, int dnNodeIds[],
					   int *coordCount, int coordNodeIds[])
{
	int i;
	PGXCNodeAllHandles *handles = get_current_txn_handles(NULL);

	*dnCount = *coordCount = 0;
	for (i = 0; i < handles->dn_conn_count; i++)
	{
		PGXCNodeHandle *conn = handles->datanode_handles[i];
		/*
		 * Skip empty slots
		 */
		if (conn->sock == NO_SOCKET)
			continue;
		else if (IsTxnStateInprogress(conn))
		{
			if (!conn->read_only)
			{
				dnNodeIds[*dnCount] = conn->nodeid;
				*dnCount = *dnCount + 1;
			}
		}
	}

	for (i = 0; i < handles->co_conn_count; i++)
	{
		PGXCNodeHandle *conn = handles->coord_handles[i];
		/*
		 * Skip empty slots
		 */
		if (conn->sock == NO_SOCKET)
			continue;
		else if (IsTxnStateInprogress(conn))
		{
			if (!conn->read_only)
			{
				coordNodeIds[*coordCount] = conn->nodeid;
				*coordCount = *coordCount + 1;
			}
		}
	}
	pfree_pgxc_all_handles(handles);
}

/* 
 * add a writable handle to global 2pc state.
 */
static inline int
twophase_add_participant(PGXCNodeHandle *handle, int handle_index, TwoPhaseTransState state)
{
	int state_index;

	/* read only node should not take part in 2pc */
	if (handle->read_only)
		return -1;

	HOLD_INTERRUPTS();
	if (handle->node_type == PGXC_NODE_DATANODE)
	{
		state_index = g_twophase_state.datanode_index;
		g_twophase_state.datanode_state[state_index].is_participant = true;
		g_twophase_state.datanode_state[state_index].handle_idx = handle_index;
		g_twophase_state.datanode_state[state_index].state = state;
	}
	else
	{
		Assert(handle->node_type == PGXC_NODE_COORDINATOR);

		state_index = g_twophase_state.coord_index;
		g_twophase_state.coord_state[state_index].is_participant = true;
		g_twophase_state.coord_state[state_index].handle_idx = handle_index;
		g_twophase_state.coord_state[state_index].state = state;
	}
	handle->twophase_sindex = state_index;
	RESUME_INTERRUPTS();
	return state_index;
}

/*
 * Update the state of a handle in the global 2PC state and record 
 * the index of the state in the connection list.
 */
static inline void
twophase_update_state(PGXCNodeHandle *handle)
{
	int		conn_index;
	int		state_index = handle->twophase_sindex;
	char	node_type = handle->node_type;

	if (handle->read_only)
	{
		elog(LOG, "read only node does not need to update 2pc state");
		return;
	}

	HOLD_INTERRUPTS();
	if (node_type == PGXC_NODE_DATANODE)
	{
		g_twophase_state.datanode_state[state_index].conn_state = TWO_PHASE_HEALTHY;
		conn_index = g_twophase_state.connections_num;
		g_twophase_state.connections[conn_index].node_type = PGXC_NODE_DATANODE;
		g_twophase_state.connections[conn_index].conn_trans_state_index = g_twophase_state.datanode_index;
		g_twophase_state.connections_num++;
		g_twophase_state.datanode_index++;
	}
	else
	{
		Assert(node_type == PGXC_NODE_COORDINATOR);

		g_twophase_state.coord_state[state_index].conn_state = TWO_PHASE_HEALTHY;
		conn_index = g_twophase_state.connections_num;
		g_twophase_state.connections[conn_index].conn_trans_state_index = g_twophase_state.coord_index;
		g_twophase_state.connections[conn_index].node_type = PGXC_NODE_COORDINATOR;
		g_twophase_state.connections_num++;
		g_twophase_state.coord_index++;
	}
	handle->twophase_cindex = conn_index;
	RESUME_INTERRUPTS();
}

/*
 * Prepare nodes which ran write operations during the transaction.
 * Read only remote transactions are committed and connections are released
 * back to the pool.
 * Function returns the list of nodes where transaction is prepared, including
 * local node, if requested, in format expected by the GTM server.
 * If something went wrong the function tries to abort prepared transactions on
 * the nodes where it succeeded and throws error. A warning is emitted if abort
 * prepared fails.
 * After completion remote connection handles are released.
 */
static char *
pgxc_node_remote_prepare(char *prepareGID, bool localNode, bool implicit)
{
	bool 			isOK = true;
	StringInfoData 	nodestr;
	char			*prepare_cmd = (char *) palloc (64 + strlen(prepareGID));
	char			*abort_cmd;
	GlobalTransactionId startnodeXid = InvalidTransactionId;
#ifdef __USE_GLOBAL_SNAPSHOT__
	GlobalTransactionId auxXid;
#endif
	int				i;
	ResponseCombiner combiner;
	PGXCNodeHandle **connections = NULL;
	int				conn_count = 0;
	bool			conn_error = false;
	PGXCNodeAllHandles *handles = get_current_txn_handles(&conn_error);
#ifdef __SUPPORT_DISTRIBUTED_TRANSACTION__
	GlobalTimestamp global_prepare_ts = InvalidGlobalTimestamp;
#endif
#ifdef __TWO_PHASE_TRANS__
	/* conn_state_index record index in g_twophase_state.conn_state or g_twophase_state.datanode_state */
	int             conn_state_index = 0;
	int             twophase_index = 0;
	StringInfoData  partnodes;
#endif
	char           *database = NULL;
	char           *user = NULL;

	if (conn_error)
		elog(ERROR, "encounter fatal error when executing remote prepare");

	connections = (PGXCNodeHandle**)palloc(sizeof(PGXCNodeHandle*) *
							(handles->dn_conn_count + handles->co_conn_count));
	initStringInfo(&nodestr);
	if (localNode)
		appendStringInfoString(&nodestr, PGXCNodeName);

	sprintf(prepare_cmd, "PREPARE TRANSACTION '%s'", prepareGID);
#ifdef __TWO_PHASE_TESTS__
	twophase_in = IN_REMOTE_PREPARE;
#endif

#ifdef __SUPPORT_DISTRIBUTED_TRANSACTION__
	if (implicit)
	{
		if (enable_distri_print)
		{
			elog(LOG, "prepare remote transaction xid %d gid %s", GetTopTransactionIdIfAny(), prepareGID);
		}

		if (enable_2pc_gts_optimize && IS_PGXC_LOCAL_COORDINATOR)
			global_prepare_ts = GetGlobalTimestampLocal();
		else
			global_prepare_ts = GetGlobalTimestampGTM();

#ifdef __TWO_PHASE_TESTS__
	if (PART_PREPARE_GET_TIMESTAMP == twophase_exception_case)
	{
		global_prepare_ts = 0;
	}
#endif
		if (!GlobalTimestampIsValid(global_prepare_ts))
		{
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("failed to get global timestamp for PREPARED command")));
		}
		if (enable_distri_print)
		{
			elog(LOG, "prepare phase get global prepare timestamp gid %s, time " INT64_FORMAT, prepareGID, global_prepare_ts);
		}
		SetGlobalPrepareTimestamp(global_prepare_ts);
	}
#endif

#ifdef __TWO_PHASE_TRANS__
	/*
	 *g_twophase_state is cleared under the following circumstances:
	 *1.after explicit 'prepare transaction', since implicit twophase trans can be created after explicit prepare;
	 *2.in CleanupTransaction after AbortTransaction;
	 *3.in FinishPreparedTransaction;
	 *under all the above situations, g_twophase_state must be cleared here
	 */
#if 0
	Assert('\0' == g_twophase_state.gid[0]);
	Assert(TWO_PHASE_INITIALTRANS == g_twophase_state.state);
#endif
	ClearLocalTwoPhaseState();
	get_partnodes(handles, &partnodes);

	/*
	 *if conn->readonly == true, that is no update in this trans, do not record in g_twophase_state
	 */
	if ('\0' != partnodes.data[0])
	{
		g_twophase_state.is_start_node = true;
		/* strlen of prepareGID is checked in MarkAsPreparing, it satisfy strlen(gid) >= GIDSIZE  */
		strncpy(g_twophase_state.gid, prepareGID, GIDSIZE);
		g_twophase_state.state = TWO_PHASE_PREPARING;
		SetLocalTwoPhaseStateHandles(handles);
		strncpy(g_twophase_state.start_node_name, PGXCNodeName,
				strnlen(PGXCNodeName, NAMEDATALEN) + 1);
		strncpy(g_twophase_state.participants, partnodes.data, partnodes.len + 1);
		/*
		 *if startnode participate this twophase trans, then send CurrentTransactionId,
		 *or just send 0 as TransactionId
		 */

		startnodeXid = GetCurrentTransactionId();
		g_twophase_state.start_xid = startnodeXid;

		database = get_database_name(MyDatabaseId);
		Assert(strlen(database) < NAMEDATALEN);
		strncpy(g_twophase_state.database, database, strlen(database) + 1);

		user = GetUserNameFromId(GetUserId(), false);
		Assert(strlen(user) < NAMEDATALEN);
		strncpy(g_twophase_state.user, user, strlen(user) + 1);

		if (enable_distri_print)
		{
			if ('\0' == g_twophase_state.start_node_name[0])
			{
				elog(PANIC,
					 "remote prepare record remote 2pc on node:%s, "
					 "gid: %s, startnode:%s, startxid: %u, "
					 "partnodes:%s, localxid: %u",
					 PGXCNodeName,
					 g_twophase_state.gid,
					 g_twophase_state.start_node_name,
					 g_twophase_state.start_xid,
					 g_twophase_state.participants,
					 g_twophase_state.start_xid);
			}
			else
			{
				elog(LOG,
					 "remote prepare record remote 2pc on node:%s, "
					 "gid: %s, startnode:%s, startxid: %u, "
					 "partnodes:%s, localxid: %u",
					 PGXCNodeName,
					 g_twophase_state.gid,
					 g_twophase_state.start_node_name,
					 g_twophase_state.start_xid,
					 g_twophase_state.participants,
					 g_twophase_state.start_xid);
			}
		}
	}
#endif

	for (i = 0; i < handles->dn_conn_count; i++)
	{
		PGXCNodeHandle *conn = handles->datanode_handles[i];

		/*
		 * If something went wrong already we have nothing to do here. The error
		 * will be reported at the end of the function, and we will rollback
		 * remotes as part of the error handling.
		 * Just skip to clean up section and check if we have already prepared
		 * somewhere, we should abort that prepared transaction.
		 */
		if (!isOK)
			goto prepare_err;

#ifdef USE_WHITEBOX_INJECTION
		(void)whitebox_trigger_generic(REMOTE_PREPARE_SEND_ALL_FAILED, WHITEBOX_TRIGGER_DEFAULT,
												INJECTION_LOCATION, g_twophase_state.gid, NULL);

		if (i == handles->dn_conn_count - 1)
			(void)whitebox_trigger_generic(REMOTE_PREPARE_SEND_LAST_DN_FAILED, WHITEBOX_TRIGGER_DEFAULT,
												INJECTION_LOCATION, g_twophase_state.gid, NULL);
#endif
		/*
		 * Skip empty slots
		 */
		if (conn->sock == NO_SOCKET)
		{
			elog(ERROR,
				 "pgxc_node_remote_prepare, remote node %s's connection handle "
				 "is invalid, backend_pid: %d",
				 conn->nodename, conn->backend_pid);
		}
		else if (IsTxnStateInprogress(conn))
		{
			DrainConnectionAndSetNewCombiner(conn, NULL);

			if (conn->read_only)
			{
				/* Send down prepare command */
				if (pgxc_node_send_tran_command(conn, TRANS_STMT_COMMIT, NULL))
				{
					/*
					 * not a big deal, it was read only, the connection will be
					 * abandoned later.
					 */
					ereport(LOG,
							(errcode(ERRCODE_CONNECTION_EXCEPTION),
								errmsg("failed to send COMMIT command to "
									   "the node %u", conn->nodeoid)));
				}
				else
				{
					/* Read responses from these */
					connections[conn_count++] = conn;
				}
			}
			else
			{
				/*
				 *only record connections that satisfy !conn->readonly
				 */
				twophase_index = twophase_add_participant(conn, i, g_twophase_state.state);
				Assert(twophase_index >= 0);
#ifdef __SUPPORT_DISTRIBUTED_TRANSACTION__
				if (implicit)
				{
					if (enable_distri_print)
					{
						elog(LOG,
							 "send prepare timestamp for xid %d gid %s prepare ts "
							 INT64_FORMAT,
							 GetTopTransactionIdIfAny(),
							 prepareGID,
							 global_prepare_ts);
					}
					if (pgxc_node_send_prepare_timestamp(conn, global_prepare_ts))
					{
#ifdef __TWO_PHASE_TRANS__
						/* record connection error */
						g_twophase_state.datanode_state[twophase_index].conn_state =
							TWO_PHASE_SEND_TIMESTAMP_ERROR;
						g_twophase_state.datanode_state[twophase_index].state =
							TWO_PHASE_PREPARE_ERROR;
#endif
						ereport(ERROR,
								(errcode(ERRCODE_CONNECTION_EXCEPTION),
								 errmsg("failed to send global prepare committs for PREPARED command")));
					}
				}
#endif

#ifdef __TWO_PHASE_TRANS__
				if (pgxc_node_send_starter(conn, PGXCNodeName))
				{
					/* record connection error */
					g_twophase_state.datanode_state[twophase_index].conn_state =
						TWO_PHASE_SEND_STARTER_ERROR;
					g_twophase_state.datanode_state[twophase_index].state =
						TWO_PHASE_PREPARE_ERROR;
					ereport(ERROR,
							(errcode(ERRCODE_CONNECTION_EXCEPTION),
							 errmsg("failed to send startnode for PREPARED command")));
				}
				if (pgxc_node_send_startxid(conn, startnodeXid))
				{
					/* record connection error */
					g_twophase_state.datanode_state[twophase_index].conn_state =
						TWO_PHASE_SEND_STARTXID_ERROR;
					g_twophase_state.datanode_state[twophase_index].state =
						TWO_PHASE_PREPARE_ERROR;
					ereport(ERROR,
							(errcode(ERRCODE_CONNECTION_EXCEPTION),
							 errmsg("failed to send startxid for PREPARED command")));
				}
				Assert(0 != partnodes.len);
				if (enable_distri_print)
				{
					elog(LOG, "twophase trans: %s, partnodes: %s", prepareGID, partnodes.data);
				}
				if (pgxc_node_send_partnodes(conn, partnodes.data))
				{
					/* record connection error */
					g_twophase_state.datanode_state[twophase_index].conn_state =
						TWO_PHASE_SEND_PARTICIPANTS_ERROR;
					g_twophase_state.datanode_state[twophase_index].state =
						TWO_PHASE_PREPARE_ERROR;
					ereport(ERROR,
							(errcode(ERRCODE_CONNECTION_EXCEPTION),
							 errmsg("failed to send partnodes for PREPARED command")));
				}
				if (pgxc_node_send_database(conn, g_twophase_state.database))
				{
					/* record connection error */
					g_twophase_state.datanode_state[twophase_index].conn_state =
						TWO_PHASE_SEND_PARTICIPANTS_ERROR;
					g_twophase_state.datanode_state[twophase_index].state =
						TWO_PHASE_PREPARE_ERROR;
					ereport(ERROR,
							(errcode(ERRCODE_INTERNAL_ERROR),
							 errmsg("failed to send database for PREPARED command")));
				}
				if (pgxc_node_send_user(conn, g_twophase_state.user))
				{
					/* record connection error */
					g_twophase_state.datanode_state[twophase_index].conn_state =
						TWO_PHASE_SEND_PARTICIPANTS_ERROR;
					g_twophase_state.datanode_state[twophase_index].state =
						TWO_PHASE_PREPARE_ERROR;
					ereport(ERROR,
							(errcode(ERRCODE_INTERNAL_ERROR),
							 errmsg("failed to send user for PREPARED command")));
				}
#endif
				/* Send down prepare command */
				if (pgxc_node_send_tran_command(conn, TRANS_STMT_PREPARE, prepareGID))
				{
#ifdef __TWO_PHASE_TRANS__
					/* record connection error */
					g_twophase_state.datanode_state[twophase_index].conn_state =
						TWO_PHASE_SEND_QUERY_ERROR;
					g_twophase_state.datanode_state[twophase_index].state =
						TWO_PHASE_PREPARE_ERROR;
#endif
					/*
					 * That is the trouble, we really want to prepare it.
					 * Just emit warning so far and go to clean up.
					 */
					isOK = false;
					ereport(WARNING,
							(errcode(ERRCODE_CONNECTION_EXCEPTION),
							 errmsg("failed to send PREPARE TRANSACTION command to "
									"the node %u", conn->nodeoid)));
				}
				else
				{
					char *nodename = get_pgxc_nodename(conn->nodeoid);
					if (nodestr.len > 0)
						appendStringInfoChar(&nodestr, ',');
					appendStringInfoString(&nodestr, nodename);
					/* Read responses from these */
					connections[conn_count++] = conn;
					/*
					 * If it fails on remote node it would just return ROLLBACK.
					 * Set the flag for the message handler so the response is
					 * verified.
					 */
					conn->ck_resp_rollback = true;

					twophase_update_state(conn);
				}
			}

#ifdef USE_WHITEBOX_INJECTION
			if (whitebox_trigger_generic(REMOTE_PREPARE_MESSAGE_REPEAT_DN, WHITEBOX_TRIGGER_REPEAT,
													INJECTION_LOCATION, g_twophase_state.gid, NULL))
			{
				if (pgxc_node_send_tran_command(conn, TRANS_STMT_PREPARE, NULL))
				{
					ereport(LOG,
							(errcode(ERRCODE_CONNECTION_EXCEPTION),
							 errmsg("Transaction whitebox testing stub REMOTE_PREPARED_MESSAGE_REPEAT_DN,"
									" failed to send PREPARE TRANSACTION command to the node %u", conn->nodeoid)));
				}
			}
#endif
		}
		else if (IsTxnStateError(conn))
		{
			/*
			 * Probably can not happen, if there was an error the engine would
			 * abort anyway, even in case of explicit PREPARE.
			 * Anyway, just in case...
			 */
			isOK = false;
			ereport(WARNING,
					(errcode(ERRCODE_CONNECTION_EXCEPTION),
					 errmsg("remote node %u is in error state",
							conn->nodeoid)));
		}
	}

	for (i = 0; i < handles->co_conn_count; i++)
	{
		PGXCNodeHandle *conn = handles->coord_handles[i];

		/*
		 * If something went wrong already we have nothing to do here. The error
		 * will be reported at the end of the function, and we will rollback
		 * remotes as part of the error handling.
		 * Just skip to clean up section and check if we have already prepared
		 * somewhere, we should abort that prepared transaction.
		 */
		if (!isOK)
			goto prepare_err;

#ifdef USE_WHITEBOX_INJECTION
		(void)whitebox_trigger_generic(REMOTE_PREPARE_SEND_ALL_FAILED, WHITEBOX_TRIGGER_DEFAULT,
												INJECTION_LOCATION, g_twophase_state.gid, NULL);
		if (i == handles->co_conn_count - 1)
			(void)whitebox_trigger_generic(REMOTE_PREPARE_SEND_LAST_CN_FAILED, WHITEBOX_TRIGGER_DEFAULT,
												INJECTION_LOCATION, g_twophase_state.gid, NULL);
#endif

		/*
		 * Skip empty slots
		 */
		if (conn->sock == NO_SOCKET)
		{
			elog(ERROR,
				 "pgxc_node_remote_prepare, remote node %s's connection "
				 "handle is invalid, backend_pid: %d",
				 conn->nodename, conn->backend_pid);
		}
		else if (IsTxnStateInprogress(conn))
		{
			if (conn->read_only)
			{
#ifdef __SUPPORT_DISTRIBUTED_TRANSACTION__
				if (implicit)
				{
					if (enable_distri_print)
					{
						elog(LOG, "send prepare timestamp for xid %d gid %s prepare ts " INT64_FORMAT,GetTopTransactionIdIfAny(),
														prepareGID, global_prepare_ts);
					}
					if (pgxc_node_send_prepare_timestamp(conn, global_prepare_ts))
					{
						ereport(ERROR,
								(errcode(ERRCODE_CONNECTION_EXCEPTION),
								 errmsg("failed to send global prepare committs"
										" for PREPARED command")));
					}
				}
#endif
				/* Send down prepare command */
				if (pgxc_node_send_tran_command(conn, TRANS_STMT_COMMIT, NULL))
				{
					/*
					 * not a big deal, it was read only, the connection will be
					 * abandoned later.
					 */
					ereport(LOG,
							(errcode(ERRCODE_CONNECTION_EXCEPTION),
							 errmsg("failed to send COMMIT command to "
									"the node %u", conn->nodeoid)));
				}
				else
					connections[conn_count++] = conn;
			}
			else
			{
				twophase_index = twophase_add_participant(conn, i, g_twophase_state.state);
				Assert(twophase_index >= 0);
#ifdef __SUPPORT_DISTRIBUTED_TRANSACTION__
				if (implicit)
				{
					if (enable_distri_print)
					{
						elog(LOG, "send prepare timestamp for xid %d gid %s prepare ts " INT64_FORMAT,GetTopTransactionIdIfAny(),
														prepareGID, global_prepare_ts);
					}
					if (pgxc_node_send_prepare_timestamp(conn, global_prepare_ts))
					{
#ifdef __TWO_PHASE_TRANS__
						/* record connection error */
						g_twophase_state.coord_state[twophase_index].conn_state =
							TWO_PHASE_SEND_TIMESTAMP_ERROR;
						g_twophase_state.coord_state[twophase_index].state =
							TWO_PHASE_PREPARE_ERROR;
#endif
						ereport(ERROR,
								(errcode(ERRCODE_CONNECTION_EXCEPTION),
								 errmsg("failed to send global prepare "
										"committs for PREPARED command")));
					}
				}
#endif

#ifdef __TWO_PHASE_TRANS__
				if (pgxc_node_send_starter(conn, PGXCNodeName))
				{
					/* record connection error */
					g_twophase_state.coord_state[twophase_index].conn_state =
						TWO_PHASE_SEND_STARTER_ERROR;
					g_twophase_state.coord_state[twophase_index].state =
						TWO_PHASE_PREPARE_ERROR;
					ereport(ERROR,
							(errcode(ERRCODE_CONNECTION_EXCEPTION),
							 errmsg("failed to send startnode for PREPARED command")));
				}
				if (pgxc_node_send_startxid(conn, startnodeXid))
				{
					/* record connection error */
					g_twophase_state.coord_state[twophase_index].conn_state =
						TWO_PHASE_SEND_STARTXID_ERROR;
					g_twophase_state.coord_state[twophase_index].state =
						TWO_PHASE_PREPARE_ERROR;
					ereport(ERROR,
							(errcode(ERRCODE_CONNECTION_EXCEPTION),
							 errmsg("failed to send startxid for PREPARED command")));
				}
				Assert(0 != partnodes.len);
				if (enable_distri_print)
				{
					elog(LOG, "twophase trans: %s, partnodes: %s",
						 prepareGID, partnodes.data);
				}
				if (pgxc_node_send_partnodes(conn, partnodes.data))
				{
					/* record connection error */
					g_twophase_state.coord_state[twophase_index].conn_state =
						TWO_PHASE_SEND_PARTICIPANTS_ERROR;
					g_twophase_state.coord_state[twophase_index].state =
						TWO_PHASE_PREPARE_ERROR;
					ereport(ERROR,
							(errcode(ERRCODE_CONNECTION_EXCEPTION),
							 errmsg("failed to send partnodes for PREPARED command")));
				}
				if (pgxc_node_send_database(conn, g_twophase_state.database))
				{
					/* record connection error */
					g_twophase_state.coord_state[twophase_index].conn_state =
						TWO_PHASE_SEND_PARTICIPANTS_ERROR;
					g_twophase_state.coord_state[twophase_index].state =
						TWO_PHASE_PREPARE_ERROR;
					ereport(ERROR,
							(errcode(ERRCODE_INTERNAL_ERROR),
								errmsg("failed to send database for PREPARED command")));
				}
				if (pgxc_node_send_user(conn, g_twophase_state.user))
				{
					/* record connection error */
					g_twophase_state.coord_state[twophase_index].conn_state =
						TWO_PHASE_SEND_PARTICIPANTS_ERROR;
					g_twophase_state.coord_state[twophase_index].state =
						TWO_PHASE_PREPARE_ERROR;
					ereport(ERROR,
							(errcode(ERRCODE_INTERNAL_ERROR),
								errmsg("failed to send user for PREPARED command")));
				}
#endif

				/* Send down prepare command */
				if (pgxc_node_send_tran_command(conn, TRANS_STMT_PREPARE, prepareGID))
				{
#ifdef __TWO_PHASE_TRANS__
					/* record connection error */
					g_twophase_state.coord_state[twophase_index].conn_state =
						TWO_PHASE_SEND_QUERY_ERROR;
					g_twophase_state.coord_state[twophase_index].state =
						TWO_PHASE_PREPARE_ERROR;
#endif
					/*
					 * That is the trouble, we really want to prepare it.
					 * Just emit warning so far and go to clean up.
					 */
					isOK = false;
					ereport(WARNING,
							(errcode(ERRCODE_CONNECTION_EXCEPTION),
							 errmsg("failed to send PREPARE TRANSACTION command to "
									"the node %u", conn->nodeoid)));
				}
				else
				{
					char *nodename = get_pgxc_nodename(conn->nodeoid);
					if (nodestr.len > 0)
						appendStringInfoChar(&nodestr, ',');
					appendStringInfoString(&nodestr, nodename);
					/* Read responses from these */
					connections[conn_count++] = conn;
					/*
					 * If it fails on remote node it would just return ROLLBACK.
					 * Set the flag for the message handler so the response is
					 * verified.
					 */
					conn->ck_resp_rollback = true;
					twophase_update_state(conn);
				}
			}

#ifdef USE_WHITEBOX_INJECTION
			if (whitebox_trigger_generic(REMOTE_PREPARE_MESSAGE_REPEAT_CN, WHITEBOX_TRIGGER_REPEAT,
													INJECTION_LOCATION, g_twophase_state.gid, NULL))
			{
				if (pgxc_node_send_tran_command(conn, TRANS_STMT_PREPARE, NULL))
				{
					ereport(LOG,
							(errcode(ERRCODE_CONNECTION_EXCEPTION),
							 errmsg("Transaction whitebox testing stub REMOTE_PREPARED_MESSAGE_REPEAT_CN, "
									"failed to send PREPARE TRANSACTION command to the node %u", conn->nodeoid)));
				}
			}
#endif
		}
		else if (IsTxnStateError(conn))
		{
			/*
			 * Probably can not happen, if there was an error the engine would
			 * abort anyway, even in case of explicit PREPARE.
			 * Anyway, just in case...
			 */
			isOK = false;
			ereport(WARNING,
					(errcode(ERRCODE_CONNECTION_EXCEPTION),
					 errmsg("remote node %u is in error state",
							conn->nodeoid)));
		}
	}

	SetSendCommandId(false);
#ifdef __TWO_PHASE_TESTS__
	if (PREPARE_ERROR_SEND_QUERY == twophase_exception_case ||
		PREPARE_ERROR_RESPONSE_ERROR == twophase_exception_case)
	{
		isOK = false;
	}
	if (PART_ABORT_SEND_ROLLBACK == twophase_exception_case)
	{
		elog(ERROR, "PART_ABORT_SEND_ROLLBACK in twophase tests");
	}
#endif

#ifdef USE_WHITEBOX_INJECTION
	(void)whitebox_trigger_generic(REMOTE_PREPARE_FAILED_AFTER_SEND, WHITEBOX_TRIGGER_DEFAULT,
											INJECTION_LOCATION, g_twophase_state.gid, NULL);
#endif

	if (!isOK)
		goto prepare_err;

	/* exit if nothing has been prepared */
	if (conn_count > 0)
	{
		int result;
		/*
		 * Receive and check for any errors. In case of errors, we don't bail out
		 * just yet. We first go through the list of connections and look for
		 * errors on each connection. This is important to ensure that we run
		 * an appropriate ROLLBACK command later on (prepared transactions must be
		 * rolled back with ROLLBACK PREPARED commands).
		 *
		 * PGXCTODO - There doesn't seem to be a solid mechanism to track errors on
		 * individual connections. The transaction_status field doesn't get set
		 * every time there is an error on the connection. The combiner mechanism is
		 * good for parallel proessing, but I think we should have a leak-proof
		 * mechanism to track connection status
		 */
		InitResponseCombiner(&combiner, conn_count, COMBINE_TYPE_NONE);
#ifdef __TWO_PHASE_TRANS__
		g_twophase_state.response_operation = REMOTE_PREPARE;
#endif
		/* Receive responses */
		result = pgxc_node_receive_responses(conn_count, connections, NULL, &combiner);
		if (result || !validate_combiner(&combiner))
			goto prepare_err;
		else
			CloseCombiner(&combiner);

		/* Before exit clean the flag, to avoid unnecessary checks */
		for (i = 0; i < conn_count; i++)
			connections[i]->ck_resp_rollback = false;

		pfree_pgxc_all_handles(handles);
	}

	pfree(prepare_cmd);
#ifdef __TWO_PHASE_TRANS__
	g_twophase_state.state = TWO_PHASE_PREPARE_END;
	if (partnodes.maxlen)
	{
		resetStringInfo(&partnodes);
		pfree(partnodes.data);
	}
#endif

	if (connections)
	{
		pfree(connections);
		connections = NULL;
	}
	return nodestr.data;

prepare_err:
#ifdef __TWO_PHASE_TRANS__
	if (partnodes.maxlen)
	{
		resetStringInfo(&partnodes);
		pfree(partnodes.data);
	}
#endif


#ifdef __OPENTENBASE__
	/* read ReadyForQuery from connections which sent commit/commit prepared */
	if (!isOK)
	{
		if (conn_count > 0)
		{
			ResponseCombiner combiner3;
			InitResponseCombiner(&combiner3, conn_count, COMBINE_TYPE_NONE);
#ifdef __TWO_PHASE_TRANS__
			g_twophase_state.response_operation = REMOTE_PREPARE_ERROR;
#endif
			/* Receive responses */
			pgxc_node_receive_responses(conn_count, connections, NULL, &combiner3);
			CloseCombiner(&combiner3);
		}
	}
#endif

#ifdef __TWO_PHASE_TESTS__
	twophase_in = IN_PREPARE_ERROR;
#endif

	abort_cmd = (char *) palloc (64 + strlen(prepareGID));
	sprintf(abort_cmd, "ROLLBACK PREPARED '%s'", prepareGID);
#ifdef __USE_GLOBAL_SNAPSHOT__

	auxXid = GetAuxilliaryTransactionId();
#endif
	conn_count = 0;
	g_twophase_state.connections_num = 0;
	g_twophase_state.coord_index = 0;
	g_twophase_state.datanode_index = 0;
	conn_state_index = 0;

	for (i = 0; i < handles->dn_conn_count; i++)
	{
		PGXCNodeHandle *conn = handles->datanode_handles[i];

		if (conn->read_only)
			continue;
		/*
		 * PREPARE succeeded on that node, roll it back there
		 * this ensures only write handle
		 */
		if (conn->ck_resp_rollback)
		{
			conn->ck_resp_rollback = false;

			if (!IsConnectionStateIdle(conn))
			{
				ereport(WARNING,
						(errcode(ERRCODE_CONNECTION_EXCEPTION),
						 errmsg("Error while PREPARING transaction %s on "
								"node %s. Administrative action may be required "
								"to abort this transaction on the node",
								prepareGID, conn->nodename)));
				continue;
			}

			/* sanity checks */
			Assert(conn->sock != NO_SOCKET);
#ifdef __TWO_PHASE_TRANS__
			/* update datanode_state = TWO_PHASE_ABORTTING in prepare_err */
			while (g_twophase_state.datanode_index >= conn_state_index &&
				   g_twophase_state.datanode_state[conn_state_index].handle_idx != i)
			{
				conn_state_index++;
			}
			if (g_twophase_state.datanode_index < conn_state_index)
			{
				elog(ERROR,
					 "in pgxc_node_remote_prepare can not find twophase_state for node %s",
					 conn->nodename);
			}
			g_twophase_state.datanode_state[conn_state_index].state = TWO_PHASE_ABORTTING;
#endif
			/* Send down abort prepared command */
#ifdef __USE_GLOBAL_SNAPSHOT__
			if (pgxc_node_send_gxid(conn, auxXid))
			{
#ifdef __TWO_PHASE_TRANS__
				g_twophase_state.datanode_state[conn_state_index].conn_state =
					TWO_PHASE_SEND_GXID_ERROR;
				g_twophase_state.datanode_state[conn_state_index].state = TWO_PHASE_ABORT_ERROR;
#endif
				/*
				 * Prepared transaction is left on the node, but we can not
				 * do anything with that except warn the user.
				 */
				ereport(WARNING,
						(errcode(ERRCODE_CONNECTION_EXCEPTION),
						 errmsg("failed to send xid to "
								"the node %u", conn->nodeoid)));
			}
#endif
			if (pgxc_node_send_tran_command(conn, TRANS_STMT_ROLLBACK_PREPARED, prepareGID))
			{
#ifdef __TWO_PHASE_TRANS__
				g_twophase_state.datanode_state[conn_state_index].conn_state =
					TWO_PHASE_SEND_QUERY_ERROR;
				g_twophase_state.datanode_state[conn_state_index].state = TWO_PHASE_ABORT_ERROR;
#endif
				/*
				 * Prepared transaction is left on the node, but we can not
				 * do anything with that except warn the user.
				 */
				ereport(WARNING,
						(errcode(ERRCODE_CONNECTION_EXCEPTION),
						 errmsg("failed to send ABORT PREPARED command to "
								"the node %u", conn->nodeoid)));
			}
			else
			{
				/* Read responses from these */
				connections[conn_count++] = conn;
				twophase_update_state(conn);
			}
		}
	}
#ifdef __TWO_PHASE_TRANS__
	conn_state_index = 0;
#endif
	for (i = 0; i < handles->co_conn_count; i++)
	{
		PGXCNodeHandle *conn = handles->coord_handles[i];

		if (conn->read_only)
			continue;

		if (conn->ck_resp_rollback)
		{
			conn->ck_resp_rollback = false;

			if (!IsConnectionStateIdle(conn))
			{
				ereport(WARNING,
						(errcode(ERRCODE_CONNECTION_EXCEPTION),
						 errmsg("Error while PREPARING transaction %s on "
								"node %s. Administrative action may be required "
								"to abort this transaction on the node",
								prepareGID, conn->nodename)));
				continue;
			}

			/* sanity checks */
			Assert(conn->sock != NO_SOCKET);
#ifdef __TWO_PHASE_TRANS__
			while (g_twophase_state.coord_index >= conn_state_index &&
				   g_twophase_state.coord_state[conn_state_index].handle_idx != i)
			{
				conn_state_index++;
			}
			if (g_twophase_state.coord_index < conn_state_index)
			{
				elog(ERROR,
				 	 "in pgxc_node_remote_prepare can not find twophase_state for node %s",
				 	 conn->nodename);
			}
			g_twophase_state.coord_state[conn_state_index].state = TWO_PHASE_ABORTTING;
#endif
			/* Send down abort prepared command */
#ifdef __USE_GLOBAL_SNAPSHOT__
			if (pgxc_node_send_gxid(conn, auxXid))
			{
#ifdef __TWO_PHASE_TRANS__
				g_twophase_state.coord_state[conn_state_index].conn_state =
					TWO_PHASE_SEND_GXID_ERROR;
				g_twophase_state.coord_state[conn_state_index].state = TWO_PHASE_ABORT_ERROR;
#endif
				/*
				 * Prepared transaction is left on the node, but we can not
				 * do anything with that except warn the user.
				 */
				ereport(WARNING,
						(errcode(ERRCODE_CONNECTION_EXCEPTION),
						 errmsg("failed to send xid to "
								"the node %u", conn->nodeoid)));
			}
#endif
			if (pgxc_node_send_tran_command(conn, TRANS_STMT_ROLLBACK_PREPARED, prepareGID))
			{
#ifdef __TWO_PHASE_TRANS__
				g_twophase_state.coord_state[conn_state_index].conn_state =
					TWO_PHASE_SEND_QUERY_ERROR;
				g_twophase_state.coord_state[conn_state_index].state = TWO_PHASE_ABORT_ERROR;
#endif
				/*
				 * Prepared transaction is left on the node, but we can not
				 * do anything with that except warn the user.
				 */
				ereport(WARNING,
						(errcode(ERRCODE_CONNECTION_EXCEPTION),
						 errmsg("failed to send ABORT PREPARED command to "
								"the node %u", conn->nodeoid)));
			}
			else
			{
				/* Read responses from these */
				connections[conn_count++] = conn;
				twophase_update_state(conn);
			}
		}
	}
	if (conn_count > 0)
	{
		/* Just read out responses, throw error from the first combiner */
		ResponseCombiner combiner2;
		InitResponseCombiner(&combiner2, conn_count, COMBINE_TYPE_NONE);
#ifdef __TWO_PHASE_TRANS__
		g_twophase_state.response_operation = REMOTE_PREPARE_ABORT;
#endif
		/* Receive responses */
		pgxc_node_receive_responses(conn_count, connections, NULL, &combiner2);
		CloseCombiner(&combiner2);
	}

	/*
	 * If the flag is set we are here because combiner carries error message
	 */
	if (isOK)
		pgxc_node_report_error(&combiner);
	else
		elog(ERROR,
			 "failed to PREPARE transaction on one or more nodes");

	if (!temp_object_included && PersistentConnectionsTimeout == 0)
		pgxc_node_cleanup_and_release_handles(false, false);

	pfree_pgxc_all_handles(handles);
	pfree(abort_cmd);

#if 0
	/*
	 * If the flag is set we are here because combiner carries error message
	 */
	if (isOK)
		pgxc_node_report_error(&combiner);
	else
		elog(ERROR, "failed to PREPARE transaction on one or more nodes");
#endif

	if (connections)
	{
		pfree(connections);
		connections = NULL;
	}

	return NULL;
}

/*
 * Commit transactions on remote nodes.
 * If barrier lock is set wait while it is released.
 * Release remote connection after completion.
 */
static void
pgxc_node_remote_commit(TranscationType txn_type, bool need_release_handle)
{
	int				result = 0;
	char		   *commitCmd = NULL;
	int				i;
	ResponseCombiner combiner;
	PGXCNodeHandle **connections = NULL;
	int				conn_count = 0;
	PGXCNodeAllHandles *current_handles = NULL;
	TransactionStmtKind	transKind;
	char 			   *transOpt = NULL;
	bool			conn_error = false;

#ifdef __OPENTENBASE__
	switch (txn_type)
	{
		case TXN_TYPE_CommitTxn:
			commitCmd = "COMMIT TRANSACTION";
			transKind = TRANS_STMT_COMMIT;
			current_handles = get_current_txn_handles(&conn_error);
			break;
		case TXN_TYPE_CommitSubTxn:
			commitCmd = palloc0(COMMIT_CMD_LEN);
			snprintf(commitCmd, COMMIT_CMD_LEN, "COMMIT_SUBTXN %d", GetCurrentTransactionNestLevel());
			transKind = TRANS_STMT_COMMIT_SUBTXN;
			transOpt = palloc0(COMMIT_CMD_LEN);
			snprintf(transOpt, COMMIT_CMD_LEN, "%d", GetCurrentTransactionNestLevel());
			current_handles = get_current_subtxn_handles(GetCurrentSubTransactionId(), false);
			elog(DEBUG1, "[NEST_LEVEL_CHECK] send commit_sub. cur_level: %d", GetCurrentTransactionNestLevel());
			break;
		default:
			elog(PANIC, "pgxc_node_remote_commit invalid TranscationType:%d", txn_type);
			break;
	}
#endif

	if (conn_error)
		elog(ERROR, "encounter fatal error when executing remote commit");

	if ((current_handles->dn_conn_count + current_handles->co_conn_count) == 0)
	{
		if (enable_distri_print)
			elog(LOG, "Remote commit, there is no remote connections in this transaction");
		return;
	}

	/* palloc will FATAL when out of memory */
	connections = (PGXCNodeHandle**)palloc(sizeof(PGXCNodeHandle*) *
							(current_handles->dn_conn_count + current_handles->co_conn_count));

	SetSendCommandId(true);

	/*
	 * Barrier:
	 *
	 * We should acquire the BarrierLock in SHARE mode here to ensure that
	 * there are no in-progress barrier at this point. This mechanism would
	 * work as long as LWLock mechanism does not starve a EXCLUSIVE lock
	 * requester
	 */
	LWLockAcquire(BarrierLock, LW_SHARED);

	for (i = 0; i < current_handles->dn_conn_count; i++)
	{
		PGXCNodeHandle *conn = current_handles->datanode_handles[i];

		/* Skip empty slots */
		if (conn->sock == NO_SOCKET)
			continue;

#ifdef USE_WHITEBOX_INJECTION
		(void)whitebox_trigger_generic(REMOTE_COMMIT_SEND_ALL_FAILED, WHITEBOX_TRIGGER_DEFAULT,
			INJECTION_LOCATION, NULL, NULL);

		if (i == current_handles->dn_conn_count - 1)
			(void)whitebox_trigger_generic(REMOTE_COMMIT_SEND_LAST_DN_FAILED, WHITEBOX_TRIGGER_DEFAULT,
				INJECTION_LOCATION, NULL, NULL);
#endif

		if (enable_distri_print)
			elog(LOG, "%s - handle->nodename=%s %d, handle->subtrans_nesting_level=%d transaction_status %c %d",
				 commitCmd, conn->nodename, conn->backend_pid, GetSubTransactionId(conn),
				 GetTxnState(conn), GetConnectionState(conn));

		elog(DEBUG1, "[NEST_LEVEL_CHECK:%d] send commit_sub. cur_level: %d, cur_subid: %d, conn_subid: %d, txn_type: %d, txn_state: %d",
			conn->backend_pid, GetCurrentTransactionNestLevel(), GetCurrentSubTransactionId(), GetSubTransactionId(conn), txn_type, GetTxnState(conn));
		/*
		 * We do not need to commit remote node if it is not in transaction.
		 * If transaction is in error state the commit command will cause
		 * rollback, that is OK
		 */
		if ((!IsTxnStateIdle(conn) && TXN_TYPE_CommitTxn == txn_type) ||
		    (!IsTxnStateIdle(conn) && TXN_TYPE_CommitSubTxn == txn_type))
		{
			DrainConnectionAndSetNewCombiner(conn, NULL);

            if (pgxc_node_send_cmd_id(conn, GetCurrentCommandId(false)))
				ereport(ERROR,
						(errcode(ERRCODE_INTERNAL_ERROR),
								errmsg("Failed to send command ID"),
								errnode(conn)));

			if (enable_distri_print)
				elog(LOG, "%s - handle->nodename=%s %d, handle->subtrans_nesting_level=%d",
					 commitCmd, conn->nodename, conn->backend_pid, GetSubTransactionId(conn));

			if(pgxc_node_send_tran_command(conn, transKind, transOpt))
			{
				/*
				 * Do not bother with clean up, just bomb out. The error handler
				 * will invoke RollbackTransaction which will do the work.
				 */
				ereport(ERROR,
						(errcode(ERRCODE_CONNECTION_EXCEPTION),
								errmsg("pgxc_node_remote_commit failed to send COMMIT command for %s", strerror(errno)),
								errnode(conn)));
			}
			else
			{
				/* Read responses from these */
				connections[conn_count++] = conn;
			}
		}

#ifdef USE_WHITEBOX_INJECTION
		if (whitebox_trigger_generic(REMOTE_COMMIT_MESSAGE_REPEAT_DN, WHITEBOX_TRIGGER_REPEAT,
												INJECTION_LOCATION, NULL, NULL))
		{
			if(pgxc_node_send_tran_command(conn, transKind, NULL))
			{
				/*
				 * Do not bother with clean up, just bomb out. The error handler
				 * will invoke RollbackTransaction which will do the work.
				 */
				ereport(LOG,
						(errcode(ERRCODE_CONNECTION_EXCEPTION),
						errmsg("Transaction whitebox testing stub REMOTE_COMMIT_FAILED_REPEAT_SEND,"
							" failed to send COMMIT command to the node %u", conn->nodeoid)));
			}
		}
#endif
	}

	for (i = 0; i < current_handles->co_conn_count; i++)
	{
		PGXCNodeHandle *conn = current_handles->coord_handles[i];

		/* Skip empty slots */
		if (conn->sock == NO_SOCKET)
			continue;

#ifdef USE_WHITEBOX_INJECTION
		(void)whitebox_trigger_generic(REMOTE_COMMIT_SEND_ALL_FAILED, WHITEBOX_TRIGGER_DEFAULT,
			INJECTION_LOCATION, NULL, NULL);

		if (i == current_handles->co_conn_count - 1)
			(void)whitebox_trigger_generic(REMOTE_COMMIT_SEND_LAST_CN_FAILED, WHITEBOX_TRIGGER_DEFAULT,
				INJECTION_LOCATION, NULL, NULL);
#endif
		/*
		 * We do not need to commit remote node if it is not in transaction.
		 * If transaction is in error state the commit command will cause
		 * rollback, that is OK
		 */
		if ((!IsTxnStateIdle(conn) && TXN_TYPE_CommitTxn == txn_type) ||
		    (!IsTxnStateIdle(conn) && TXN_TYPE_CommitSubTxn == txn_type))
		{
			if (pgxc_node_send_cmd_id(conn, GetCurrentCommandId(false)))
			{
				ereport(ERROR,
						(errcode(ERRCODE_INTERNAL_ERROR),
								errmsg("Failed to send command ID"),
								errnode(conn)));
			}

			if(pgxc_node_send_tran_command(conn, transKind, transOpt))
			{
				/*
				 * Do not bother with clean up, just bomb out. The error handler
				 * will invoke RollbackTransaction which will do the work.
				 */
				ereport(ERROR,
						(errcode(ERRCODE_CONNECTION_EXCEPTION),
								errmsg("pgxc_node_remote_commit failed to send COMMIT command for %s", strerror(errno)),
								errnode(conn)));
			}
			else
			{
				/* Read responses from these */
				connections[conn_count++] = conn;
			}
		}

#ifdef USE_WHITEBOX_INJECTION
		if (whitebox_trigger_generic(REMOTE_COMMIT_MESSAGE_REPEAT_CN, WHITEBOX_TRIGGER_REPEAT,
											INJECTION_LOCATION, NULL, NULL))
		{
			if(pgxc_node_send_tran_command(conn, transKind, NULL))
			{
				ereport(LOG,
						(errcode(ERRCODE_CONNECTION_EXCEPTION),
						errmsg("Transaction whitebox testing stub REMOTE_COMMIT_FAILED_REPEAT_SEND,"
							" failed to send COMMIT command to the node %u", conn->nodeoid)));
			}
		}
#endif
	}

	/*
	 * Release the BarrierLock.
	 */
	LWLockRelease(BarrierLock);

	if (conn_count)
	{
		InitResponseCombiner(&combiner, conn_count, COMBINE_TYPE_NONE);

		/* Receive responses */
		result = pgxc_node_receive_responses(conn_count, connections, NULL, &combiner);
		if (result)
		{
			elog(LOG, "pgxc_node_remote_commit pgxc_node_receive_responses of COMMIT failed");
			result = EOF;
		}
		else if (!validate_combiner(&combiner))
		{
			elog(LOG, "pgxc_node_remote_commit validate_combiner responese of COMMIT failed");
			result = EOF;
		}

		if (result)
		{
			if (combiner.errorMessage)
			{
				pgxc_node_report_error(&combiner);
			}
			else if (IS_PGXC_LOCAL_COORDINATOR &&
				txn_type == TXN_TYPE_CommitTxn &&
				!IS_XACT_IN_2PC)
			{
				/*
				 * When commit transaction is not a 2pc, we do not know
				 * whether it is committed success on dn
				 */
				ereport(WARNING,
						(errcode(ERRCODE_INTERNAL_ERROR),
						 errmsg("Failed to receive response from dn "
							"when commit transaction, the transaction "
							"may be committed or aborted on dn")));
			}
			else
			{
				ereport(ERROR,
						(errcode(ERRCODE_CONNECTION_EXCEPTION),
						 errmsg("Failed to COMMIT the transaction on one or more nodes")));
			}
		}
		CloseCombiner(&combiner);
	}

#ifdef USE_WHITEBOX_INJECTION
	(void)whitebox_trigger_generic(REMOTE_COMMIT_FAILED_AFTER_SEND, WHITEBOX_TRIGGER_DEFAULT,
			INJECTION_LOCATION, NULL, NULL);
#endif

	stat_transaction(conn_count);

	if (!temp_object_included && PersistentConnectionsTimeout == 0)
	{
		if (need_release_handle)
			pgxc_node_cleanup_and_release_handles(false, false);
	}

	pfree_pgxc_all_handles(current_handles);

	if (connections)
	{
		pfree(connections);
		connections = NULL;
	}
}

#ifdef __TWO_PHASE_TRANS__
void
InitLocalTwoPhaseState(void)
{
	int participants_capacity;
	g_twophase_state.is_start_node = false;
	g_twophase_state.in_pg_clean = false;
	g_twophase_state.is_readonly = false;
	g_twophase_state.is_after_prepare = false;
	g_twophase_state.gid = (char *)MemoryContextAllocZero(TopMemoryContext, GIDSIZE);
	g_twophase_state.state = TWO_PHASE_INITIALTRANS;
	g_twophase_state.coord_index = g_twophase_state.datanode_index = 0;
	g_twophase_state.isprinted = false;
	g_twophase_state.start_node_name[0] = '\0';
	g_twophase_state.handles = NULL;
	g_twophase_state.origin_handles_ptr = NULL;
	g_twophase_state.start_xid = 0;
	g_twophase_state.connections_num = 0;
	g_twophase_state.response_operation = OTHER_OPERATIONS;
	g_twophase_state.database = (char *)MemoryContextAllocZero(
										TopMemoryContext, NAMEDATALEN);
	g_twophase_state.user = (char *)MemoryContextAllocZero(
										TopMemoryContext, NAMEDATALEN);

	if (IS_PGXC_COORDINATOR)
	{
		g_twophase_state.coord_state = (ConnTransState *)MemoryContextAllocZero(TopMemoryContext,
									OPENTENBASE_MAX_COORDINATOR_NUMBER * sizeof(ConnTransState));
	}
	else
	{
		g_twophase_state.coord_state = NULL;
	}
	g_twophase_state.datanode_state = (ConnTransState *)MemoryContextAllocZero(TopMemoryContext,
									OPENTENBASE_MAX_DATANODE_NUMBER * sizeof(ConnTransState));
	/* since participates conclude nodename and  ","*/
	participants_capacity = (NAMEDATALEN+1) * (OPENTENBASE_MAX_DATANODE_NUMBER + OPENTENBASE_MAX_COORDINATOR_NUMBER);
	g_twophase_state.participants = (char *)MemoryContextAllocZero(TopMemoryContext, participants_capacity);
	g_twophase_state.connections = (AllConnNodeInfo *)MemoryContextAllocZero(TopMemoryContext,
								  (OPENTENBASE_MAX_DATANODE_NUMBER + OPENTENBASE_MAX_COORDINATOR_NUMBER) *
								  sizeof(AllConnNodeInfo));
}

void
CheckLocalTwoPhaseState(void)
{
	if (g_twophase_state.datanode_index >= OPENTENBASE_MAX_DATANODE_NUMBER ||
		g_twophase_state.connections_num >= (OPENTENBASE_MAX_DATANODE_NUMBER + OPENTENBASE_MAX_COORDINATOR_NUMBER) ||
		(IS_PGXC_COORDINATOR && g_twophase_state.coord_index >= OPENTENBASE_MAX_COORDINATOR_NUMBER))
		elog(FATAL, "g_twophase_state datanode_index %d or connections_num %d or coord_index %d exceed limits",
			 g_twophase_state.datanode_index, g_twophase_state.connections_num, g_twophase_state.coord_index);
}

void
SetLocalTwoPhaseStateHandles(PGXCNodeAllHandles *handles)
{
	PGXCNodeAllHandles *two_handles = NULL;

	if (enable_distri_print && handles->dn_conn_count)
		elog(LOG, "SetLocalTwoPhaseStateHandles dn cnt %d", handles->dn_conn_count);

	if (enable_distri_print && handles->co_conn_count)
		elog(LOG, "SetLocalTwoPhaseStateHandles co cnt %d", handles->co_conn_count);

	if (handles != NULL)
	{
		MemoryContext old_context = SwitchToTransactionAbortContext();

		two_handles = (PGXCNodeAllHandles *) palloc0(sizeof(PGXCNodeAllHandles));
		two_handles->dn_conn_count = handles->dn_conn_count;
		two_handles->co_conn_count = handles->co_conn_count;

		if (two_handles->dn_conn_count > 0)
		{
			two_handles->datanode_handles = (PGXCNodeHandle **)palloc0(handles->dn_conn_count * sizeof(PGXCNodeHandle *));
			memcpy(two_handles->datanode_handles, handles->datanode_handles, two_handles->dn_conn_count * sizeof(PGXCNodeHandle *));
		}
		if (two_handles->co_conn_count > 0)
		{
			two_handles->coord_handles = (PGXCNodeHandle **)palloc0(handles->co_conn_count * sizeof(PGXCNodeHandle *));
			memcpy(two_handles->coord_handles, handles->coord_handles, two_handles->co_conn_count * sizeof(PGXCNodeHandle *));
		}

		MemoryContextSwitchTo(old_context);
	}

	g_twophase_state.handles = two_handles;
	g_twophase_state.origin_handles_ptr = handles;
	g_twophase_state.connections_num = 0;
	g_twophase_state.coord_index = 0;
	g_twophase_state.datanode_index = 0;
}

void
UpdateLocalTwoPhaseState(int result, PGXCNodeHandle *response_handle, int conn_index, char *errmsg)
{
	int index = 0;
	int twophase_index = 0;
	TwoPhaseTransState state = TWO_PHASE_INITIALTRANS;

	if (RESPONSE_READY != result && RESPONSE_ERROR != result)
	{
		return;
	}

	if (g_twophase_state.response_operation == OTHER_OPERATIONS ||
	  !IsTransactionState() ||
	  g_twophase_state.state == TWO_PHASE_INITIALTRANS)
	{
	   return ;
	}
	Assert(NULL != g_twophase_state.handles);
	if (RESPONSE_ERROR == result)
	{
		switch (g_twophase_state.state)
		{
			case TWO_PHASE_PREPARING:
				/* receive response in pgxc_node_remote_prepare or at the begining of prepare_err */
				if (REMOTE_PREPARE == g_twophase_state.response_operation ||
				  REMOTE_PREPARE_ERROR == g_twophase_state.response_operation)
				{
					state = TWO_PHASE_PREPARE_ERROR;
#ifdef __TWO_PHASE_TESTS__
					if (IN_REMOTE_PREPARE == twophase_in &&
						PART_PREPARE_RESPONSE_ERROR == twophase_exception_case)
					{
						complish = true;
						if (run_pg_clean)
						{
							elog(STOP, "PART_PREPARE_RESPONSE_ERROR in pg_clean tests");
						}
					}
#endif
				}
				else if (REMOTE_PREPARE_ABORT == g_twophase_state.response_operation)
				{
					state = TWO_PHASE_ABORT_ERROR;
#ifdef __TWO_PHASE_TESTS__
					if (IN_PREPARE_ERROR == twophase_in &&
						PREPARE_ERROR_RESPONSE_ERROR == twophase_exception_case)
					{
						complish = true;
						elog(ERROR, "PREPARE_ERROR_RESPONSE_ERROR in pg_clean tests");
					}
#endif
				}
			case TWO_PHASE_PREPARED:
				break;

			case TWO_PHASE_COMMITTING:
				if (REMOTE_FINISH_COMMIT == g_twophase_state.response_operation)
				{
					state = TWO_PHASE_COMMIT_ERROR;
				}
			case TWO_PHASE_COMMITTED:
				break;

			case TWO_PHASE_ABORTTING:
				if (REMOTE_FINISH_ABORT == g_twophase_state.response_operation ||
				  REMOTE_ABORT == g_twophase_state.response_operation)
				{
					state = TWO_PHASE_ABORT_ERROR;
				}
			case TWO_PHASE_ABORTTED:
				break;
			default:
				Assert((result < TWO_PHASE_INITIALTRANS) || (result > TWO_PHASE_ABORT_ERROR));
				return;
		}

		if (TWO_PHASE_INITIALTRANS != state && !response_handle->read_only)
		{
			int				node_index;
			char			node_type;
			AllConnNodeInfo *nodeinfo;

			/* update coord_state or datanode_state */
			twophase_index = response_handle->twophase_cindex;
			nodeinfo = &g_twophase_state.connections[twophase_index];
			/* index for cn or dn array */
			node_index = nodeinfo->conn_trans_state_index;
			node_type = nodeinfo->node_type;

			if (PGXC_NODE_COORDINATOR == node_type)
			{
				g_twophase_state.coord_state[node_index].state = state;
				if (enable_distri_print)
				{
					elog(LOG,
						 "In UpdateLocalTwoPhaseState connections[%d] imply "
						 "node: %s, g_twophase_state.coord_state[%d] imply node: %s",
						 twophase_index,
						 response_handle->nodename,
						 node_index,
						 g_twophase_state.handles->coord_handles[index]->nodename);
				}
			}
			else
			{
				g_twophase_state.datanode_state[twophase_index].state = state;
				if (enable_distri_print)
				{
					index = g_twophase_state.datanode_state[twophase_index].handle_idx;
					elog(LOG,
						 "In UpdateLocalTwoPhaseState connections[%d] imply "
						 "node: %s, g_twophase_state.datanode_state[%d] imply node: %s",
						 twophase_index,
						 response_handle->nodename,
						 node_index,
						 g_twophase_state.handles->datanode_handles[index]->nodename);
				}
			}
		}
		return;
	}
	else if (RESPONSE_READY == result && NULL == errmsg)
	{
		switch (g_twophase_state.state)
		{
			case TWO_PHASE_PREPARING:
				/* receive response in pgxc_node_remote_prepare or at the begining of prepare_err */
				if (REMOTE_PREPARE == g_twophase_state.response_operation ||
				  REMOTE_PREPARE_ERROR == g_twophase_state.response_operation)
				{
					state = TWO_PHASE_PREPARED;
				}
				else if (REMOTE_PREPARE_ABORT == g_twophase_state.response_operation)
				{
					state = TWO_PHASE_ABORTTED;
				}
			case TWO_PHASE_PREPARED:
				break;

			case TWO_PHASE_COMMITTING:
				if (REMOTE_FINISH_COMMIT == g_twophase_state.response_operation)
				{
					state = TWO_PHASE_COMMITTED;
				}
			case TWO_PHASE_COMMITTED:
				break;
			case TWO_PHASE_ABORTTING:
				if (REMOTE_FINISH_ABORT == g_twophase_state.response_operation ||
				  REMOTE_ABORT == g_twophase_state.response_operation)
				{
					state = TWO_PHASE_ABORTTED;
				}
			case TWO_PHASE_ABORTTED:
				break;
			default:
				Assert((result < TWO_PHASE_INITIALTRANS) || (result > TWO_PHASE_ABORT_ERROR));
				return;
		}

		if (TWO_PHASE_INITIALTRANS != state && !response_handle->read_only)
		{
			int				node_index;
			char			node_type;
			AllConnNodeInfo	*nodeinfo;

			/* update coord_state or datanode_state */
			twophase_index = response_handle->twophase_cindex;
			nodeinfo = &g_twophase_state.connections[twophase_index];
			node_index = nodeinfo->conn_trans_state_index;
			node_type = nodeinfo->node_type;

			if (PGXC_NODE_COORDINATOR == node_type)
			{
				g_twophase_state.coord_state[node_index].state = state;
				if (enable_distri_print)
				{
					index = g_twophase_state.coord_state[node_index].handle_idx;
					elog(LOG,
						 "In UpdateLocalTwoPhaseState connections[%d] imply "
						 "node: %s, g_twophase_state.coord_state[%d] imply "
						 "node: %s",
						 twophase_index,
						 response_handle->nodename,
						 node_index,
						 g_twophase_state.handles->coord_handles[index]->nodename);
				}
			}
			else
			{
				g_twophase_state.datanode_state[twophase_index].state = state;
 				if (enable_distri_print)
				{
					index = g_twophase_state.datanode_state[node_index].handle_idx;
					elog(LOG,
						 "In UpdateLocalTwoPhaseState connections[%d] imply "
						 "node: %s, g_twophase_state.datanode_state[%d] imply "
						 "node: %s",
						 twophase_index,
						 response_handle->nodename,
						 node_index,
						 g_twophase_state.handles->datanode_handles[index]->nodename);
				}
			}
		}
		return;
	}
}


void
ClearLocalTwoPhaseState(void)
{
	if (enable_distri_print)
	{
		if (TWO_PHASE_PREPARED == g_twophase_state.state &&
			IsXidImplicit(g_twophase_state.gid))
		{
			elog(LOG,
				 "clear g_twophase_state of transaction '%s' in state '%s'",
				 g_twophase_state.gid,
				 GetTransStateString(g_twophase_state.state));
		}
	}

	if (g_twophase_state.handles != NULL)
	{
		if (g_twophase_state.handles->datanode_handles)
			pfree(g_twophase_state.handles->datanode_handles);
		if (g_twophase_state.handles->coord_handles)
			pfree(g_twophase_state.handles->coord_handles);
		pfree(g_twophase_state.handles);
	}

	g_twophase_state.in_pg_clean = false;
	g_twophase_state.is_start_node = false;
	g_twophase_state.is_readonly = false;
	g_twophase_state.is_after_prepare = false;
	g_twophase_state.gid[0] = '\0';
	g_twophase_state.state = TWO_PHASE_INITIALTRANS;
	g_twophase_state.coord_index = 0;
	g_twophase_state.datanode_index = 0;
	g_twophase_state.isprinted = false;
	g_twophase_state.start_node_name[0] = '\0';
	g_twophase_state.handles = NULL;
	g_twophase_state.origin_handles_ptr = NULL;
	g_twophase_state.participants[0] = '\0';
	g_twophase_state.start_xid = 0;
	g_twophase_state.connections_num = 0;
	g_twophase_state.response_operation = OTHER_OPERATIONS;
	g_twophase_state.database[0] = '\0';
	g_twophase_state.user[0] = '\0';
}

char *
GetTransStateString(TwoPhaseTransState state)
{
	switch (state)
	{
		case TWO_PHASE_INITIALTRANS:
			return "TWO_PHASE_INITIALTRANS";
		case TWO_PHASE_PREPARING:
			return "TWO_PHASE_PREPARING";
		case TWO_PHASE_PREPARED:
			return "TWO_PHASE_PREPARED";
		case TWO_PHASE_PREPARE_ERROR:
			return "TWO_PHASE_PREPARE_ERROR";
		case TWO_PHASE_COMMITTING:
			return "TWO_PHASE_COMMITTING";
		case TWO_PHASE_COMMIT_END:
			return "TWO_PHASE_COMMIT_END";
		case TWO_PHASE_COMMITTED:
			return "TWO_PHASE_COMMITTED";
		case TWO_PHASE_COMMIT_ERROR:
			return "TWO_PHASE_COMMIT_ERROR";
		case TWO_PHASE_ABORTTING:
			return "TWO_PHASE_ABORTTING";
		case TWO_PHASE_ABORT_END:
			return "TWO_PHASE_ABORT_END";
		case TWO_PHASE_ABORTTED:
			return "TWO_PHASE_ABORTTED";
		case TWO_PHASE_ABORT_ERROR:
			return "TWO_PHASE_ABORT_ERROR";
		case TWO_PHASE_UNKNOW_STATUS:
			return "TWO_PHASE_UNKNOW_STATUS";
		default:
			return NULL;
	}
	return NULL;
}

char *
GetConnStateString(ConnState state)
{
	switch (state)
	{
		case TWO_PHASE_HEALTHY:
			return "TWO_PHASE_HEALTHY";
		case TWO_PHASE_SEND_GXID_ERROR:
			return "TWO_PHASE_SEND_GXID_ERROR";
		case TWO_PHASE_SEND_TIMESTAMP_ERROR:
			return "TWO_PHASE_SEND_TIMESTAMP_ERROR";
		case TWO_PHASE_SEND_STARTER_ERROR:
			return "TWO_PHASE_SEND_STARTER_ERROR";
		case TWO_PHASE_SEND_STARTXID_ERROR:
			return "TWO_PHASE_SEND_STARTXID_ERROR";
		case TWO_PHASE_SEND_PARTICIPANTS_ERROR:
			return "TWO_PHASE_SEND_PARTICIPANTS_ERROR";
		case TWO_PHASE_SEND_QUERY_ERROR:
			return "TWO_PHASE_SEND_QUERY_ERROR";
		default:
			return NULL;
	}
	return NULL;
}


void
get_partnodes(PGXCNodeAllHandles *handles, StringInfo participants)
{
	int i;
	PGXCNodeHandle *conn;
	const char *gid;
	bool is_readonly = true;
	gid = GetPrepareGID();

	initStringInfo(participants);

	/* start node participate the twophase transaction */
	if (IS_PGXC_LOCAL_COORDINATOR &&
		(isXactWriteLocalNode() || !IsXidImplicit(gid)))
	{
		appendStringInfo(participants, "%s,", PGXCNodeName);
	}

	for (i = 0; i < handles->dn_conn_count; i++)
	{
		conn = handles->datanode_handles[i];
		if (IsConnectionStateFatal(conn))
		{
			elog(ERROR, "Connection fatal error to node %s pid %d before PREPARE",
				conn->nodename, conn->backend_pid);
		}
		else if (conn->sock == NO_SOCKET)
		{
			elog(ERROR,
				 "get_partnodes, remote node %s's connection handle is invalid, backend_pid: %d",
				 conn->nodename, conn->backend_pid);
		}
		else if (IsTxnStateInprogress(conn))
		{
			if (!conn->read_only)
			{
				is_readonly = false;
				appendStringInfo(participants, "%s,", conn->nodename);
			}
		}
		else if (IsTxnStateError(conn))
		{
			elog(ERROR,
				 "get_partnodes, remote node %s is in error state, backend_pid: %d",
				 conn->nodename, conn->backend_pid);
		}
	}

	for (i = 0; i < handles->co_conn_count; i++)
	{
		conn = handles->coord_handles[i];
		if (IsConnectionStateFatal(conn))
		{
			elog(ERROR,
				 "get_partnodes, remote node %s is in error state, backend_pid: %d",
				 conn->nodename, conn->backend_pid);
		}
		else if (conn->sock == NO_SOCKET)
		{
			elog(ERROR,
				 "get_partnodes, remote node %s's connection handle is invalid, backend_pid: %d",
				 conn->nodename, conn->backend_pid);
		}
		else if (IsTxnStateInprogress(conn))
		{
			if (!conn->read_only)
			{
				is_readonly = false;
				appendStringInfo(participants, "%s,", conn->nodename);
			}
		}
		else if (IsTxnStateError(conn))
		{
			elog(ERROR,
				 "get_partnodes, remote node %s is in error state, backend_pid: %d",
				 conn->nodename, conn->backend_pid);
		}
	}
	if (is_readonly && !IsXidImplicit(gid))
	{
		g_twophase_state.is_readonly = true;
	}
}
#endif

/*
 *  release only corrupted handlles
 */
static void
ReleaseCorruptedHandles(void)
{
	bool all = false;
	PGXCNodeAllHandles *handles;

	HOLD_INTERRUPTS();
	handles = GetCorruptedHandles(&all);
	release_handles(handles, all);
	pfree_pgxc_all_handles(handles);
	RESUME_INTERRUPTS();
}

/*
 * check for release handles, and finally clear handles
 */
void
check_for_release_handles(bool release_all)
{
	/* do nothing when exit inprogress */
	if (proc_exit_inprogress)
		return;

	if (!IS_PGXC_COORDINATOR)
		return;

	if (release_all)
		release_handles(get_current_txn_handles(NULL), false);
	else
		ReleaseCorruptedHandles();
}

/*
 * Rollback transactions on remote nodes.
 * Release remote connection after completion.
 */
static void
pgxc_node_remote_abort(TranscationType txn_type, bool need_release_handle)
{
	int				result = 0;
	char		   *rollbackCmd = NULL;
	int				i;
	ResponseCombiner combiner;
	PGXCNodeHandle **connections = NULL;
	int				conn_count = 0;

	PGXCNodeHandle **sync_connections = NULL;
	int				sync_conn_count = 0;
	PGXCNodeAllHandles *current_handles = NULL;
	bool            rollback_implict_txn = false;
#ifdef __TWO_PHASE_TRANS__
	int             twophase_index = 0;
#endif
	bool			release_all = false;

#ifdef __OPENTENBASE__
	switch (txn_type)
	{
		case TXN_TYPE_RollbackTxn:
			if ('\0' != g_twophase_state.gid[0])
			{
				rollbackCmd = palloc0(COMMIT_CMD_LEN);
				snprintf(rollbackCmd, COMMIT_CMD_LEN, "rollback prepared '%s'", g_twophase_state.gid);
				rollback_implict_txn = true;
#ifdef __TWO_PHASE_TESTS__
				twophase_in = IN_REMOTE_ABORT;
#endif
			}
			else
			{
				rollbackCmd = "ROLLBACK TRANSACTION";
			}
			current_handles = get_current_txn_handles(NULL);
			break;
		case TXN_TYPE_RollbackSubTxn:
			rollbackCmd = palloc0(COMMIT_CMD_LEN);
			snprintf(rollbackCmd, COMMIT_CMD_LEN, "ROLLBACK_SUBTXN %d", GetCurrentTransactionNestLevel());
			current_handles = get_current_subtxn_handles(GetCurrentSubTransactionId(), false);
			elog(DEBUG1, "[NEST_LEVEL_CHECK] send rollback_cmd: %s. cur_level: %d", rollbackCmd, GetCurrentTransactionNestLevel());
			break;
		case TXN_TYPE_CleanConnection:
			return;
		default:
			elog(PANIC, "pgxc_node_remote_abort invalid TranscationType:%d", txn_type);
			break;
	}
#endif

	if ((current_handles->dn_conn_count + current_handles->co_conn_count) == 0)
	{
		if (enable_distri_print)
			elog(LOG, "Remote abort, there is no remote connections in this transaction");
		return;
	}

	/* palloc will FATAL when out of memory .*/
	connections = (PGXCNodeHandle**)palloc(sizeof(PGXCNodeHandle*) *
							(current_handles->dn_conn_count + current_handles->co_conn_count));
	sync_connections = (PGXCNodeHandle**)palloc(sizeof(PGXCNodeHandle*) *
							(current_handles->dn_conn_count + current_handles->co_conn_count));

	SetSendCommandId(true);

	if(enable_distri_print)
		elog(LOG, "pgxc_node_remote_abort - dn_conn_count %d, co_conn_count %d",
			 current_handles->dn_conn_count, current_handles->co_conn_count);

	/* Send Sync if needed. */
	for (i = 0; i < current_handles->dn_conn_count; i++)
	{
		PGXCNodeHandle *conn = current_handles->datanode_handles[i];

		/* Skip empty slots */
		if (conn->sock == NO_SOCKET || IsConnectionStateFatal(conn))
		{
			continue;
		}

		if (!IsTxnStateIdle(conn))
		{
			DrainConnectionAndSetNewCombiner(conn, NULL);

			/*
			 * If the remote session was running extended query protocol when
			 * it failed, it will expect a SYNC message before it accepts any
			 * other command
			 */
			if (conn->needSync)
			{
				result = pgxc_node_send_sync(conn);
				if (result)
				{
					add_error_message(conn,
						"Failed to send SYNC command");
					ereport(LOG,
							(errcode(ERRCODE_CONNECTION_EXCEPTION),
						 	 errmsg("Failed to send SYNC command nodename:%s, "
									 "pid:%d",
									 conn->nodename,
									 conn->backend_pid)));
				}
				else
				{
					/* Read responses from these */
					sync_connections[sync_conn_count++] = conn;
					result = EOF;
					elog(DEBUG5,
						 "send SYNC command to CN nodename %s, backend_pid %d",
						 conn->nodename,
						 conn->backend_pid);
				}
			}
		}
	}

	for (i = 0; i < current_handles->co_conn_count; i++)
	{
		PGXCNodeHandle *conn = current_handles->coord_handles[i];

		/* Skip empty slots */
		if (conn->sock == NO_SOCKET || IsConnectionStateFatal(conn))
		{
			continue;
		}

		if (!IsTxnStateIdle(conn))
		{
			/* Send SYNC if the remote session is expecting one */
			if (conn->needSync)
			{
				result = pgxc_node_send_sync(conn);
				if (result)
				{
					add_error_message(conn,
									  "Failed to send SYNC command nodename");
					ereport(LOG,
							(errcode(ERRCODE_CONNECTION_EXCEPTION),
						 	 errmsg("Failed to send SYNC command nodename:%s, "
									 "pid:%d",
									 conn->nodename,
									 conn->backend_pid)));
				}
				else
				{
					/* Read responses from these */
					sync_connections[sync_conn_count++] = conn;
					result = EOF;
					elog(DEBUG5,
						 "send SYNC command to DN nodename %s, backend_pid %d",
						 conn->nodename,
						 conn->backend_pid);
				}
			}
		}
	}

	if (sync_conn_count)
	{
		InitResponseCombiner(&combiner, sync_conn_count, COMBINE_TYPE_NONE);
		/* Receive responses */
		result = pgxc_node_receive_responses(sync_conn_count, sync_connections, NULL, &combiner);
		if (result)
		{
			elog(LOG,
				 "pgxc_node_remote_abort pgxc_node_receive_responses of SYNC failed");
			result = EOF;
		}
		else if (!validate_combiner(&combiner))
		{
			elog(LOG,
				 "pgxc_node_remote_abort validate_combiner responese of SYNC failed");
			result = EOF;
		}

		if (result)
		{
			if (combiner.errorMessage)
			{
				ereport(LOG,
						(errcode(ERRCODE_CONNECTION_EXCEPTION),
						 errmsg("Failed to send SYNC to on one or more nodes "
								"errmsg:%s", combiner.errorMessage)));
			}
			else
			{
				ereport(LOG,
						(errcode(ERRCODE_CONNECTION_EXCEPTION),
						 errmsg("Failed to send SYNC to on one or more nodes")));
			}
		}
		CloseCombiner(&combiner);
	}

#ifdef _PG_REGRESS_
	{
		int ii = 0;
		for (ii = 0; ii < sync_conn_count; ii++)
		{
			if (pgxc_node_is_data_enqueued(sync_connections[ii]))
			{
				elog(PANIC, "pgxc_node_remote_abort data left over in fd:%d, remote backendpid:%d",
							sync_connections[ii]->sock, sync_connections[ii]->backend_pid);
			}
		}
	}
#endif

#ifdef __TWO_PHASE_TRANS__
	if (TWO_PHASE_ABORTTING == g_twophase_state.state && rollback_implict_txn)
	{
		SetLocalTwoPhaseStateHandles(current_handles);
	}
#endif

	for (i = 0; i < current_handles->dn_conn_count; i++)
	{
		PGXCNodeHandle *conn = current_handles->datanode_handles[i];

		/* Skip empty slots */
		if (conn->sock == NO_SOCKET)
			continue;

		elog(DEBUG5, "node %s, conn->state.transaction_state %c",
				conn->nodename,
				conn->state.transaction_state);
		elog(DEBUG1, "[NEST_LEVEL_CHECK:%d] send rollback_cmd: %s. cur_level: %d, cur_subid: %d, conn_subid: %d, txn_type: %d, txn_state: %d",
			conn->backend_pid, rollbackCmd, GetCurrentTransactionNestLevel(), GetCurrentSubTransactionId(), GetSubTransactionId(conn), txn_type, GetTxnState(conn));

		if ((!IsTxnStateIdle(conn) && TXN_TYPE_RollbackTxn == txn_type) ||
		    (rollback_implict_txn && conn->ck_resp_rollback && TXN_TYPE_RollbackTxn == txn_type) ||
		    (!IsTxnStateIdle(conn) && TXN_TYPE_RollbackSubTxn == txn_type))
		{
			if (rollback_implict_txn && !conn->read_only)
				twophase_index = twophase_add_participant(conn, i, TWO_PHASE_ABORTTING);
			Assert(twophase_index >= 0);
			/*
			 * Do not matter, is there committed or failed transaction,
			 * just send down rollback to finish it.
			 */
			if (pgxc_node_send_rollback(conn, rollbackCmd))
			{
#ifdef __TWO_PHASE_TRANS__
				if (rollback_implict_txn && !conn->read_only)
				{
					twophase_index = g_twophase_state.datanode_index;
					g_twophase_state.datanode_state[twophase_index].state = TWO_PHASE_ABORT_ERROR;
					g_twophase_state.datanode_state[twophase_index].conn_state = TWO_PHASE_SEND_QUERY_ERROR;
					g_twophase_state.datanode_index++;
					CheckLocalTwoPhaseState();
				}
#endif
				result = EOF;
				add_error_message(conn, "failed to send ROLLBACK TRANSACTION command");

				ereport(LOG,
						(errcode(ERRCODE_CONNECTION_EXCEPTION),
						 errmsg("Failed to send ROLLBACK TRANSACTION "
								"command nodename:%s, pid:%d",
								conn->nodename,
								conn->backend_pid)));

				release_all = true;
			}
			else
			{
#ifdef __TWO_PHASE_TRANS__
				if (rollback_implict_txn && !conn->read_only)
					twophase_update_state(conn);
#endif
				/* Read responses from these */
				connections[conn_count++] = conn;
				if (conn->ck_resp_rollback)
				{
					conn->ck_resp_rollback = false;
				}
			}
		}
	}

	for (i = 0; i < current_handles->co_conn_count; i++)
	{
		PGXCNodeHandle *conn = current_handles->coord_handles[i];

		/* Skip empty slots */
		if (conn->sock == NO_SOCKET)
			continue;

		if ((!IsTxnStateIdle(conn) && TXN_TYPE_RollbackTxn == txn_type) ||
		    (rollback_implict_txn && conn->ck_resp_rollback && TXN_TYPE_RollbackTxn == txn_type) ||
		    (!IsTxnStateIdle(conn) && TXN_TYPE_RollbackSubTxn == txn_type))
		{
			if (rollback_implict_txn)
				twophase_add_participant(conn, i, TWO_PHASE_ABORTTING);
			Assert(twophase_index >= 0);
			/*
			 * Do not matter, is there committed or failed transaction,
			 * just send down rollback to finish it.
			 */
			if (pgxc_node_send_rollback(conn, rollbackCmd))
			{
#ifdef __TWO_PHASE_TRANS__
				if (rollback_implict_txn)
				{
					twophase_index = g_twophase_state.coord_index;
					g_twophase_state.coord_state[twophase_index].state =
						TWO_PHASE_ABORT_ERROR;
					g_twophase_state.coord_state[twophase_index].conn_state =
						TWO_PHASE_SEND_QUERY_ERROR;
					g_twophase_state.coord_index++;
					CheckLocalTwoPhaseState();
				}
#endif
				result = EOF;
				add_error_message(conn, "failed to send ROLLBACK TRANSACTION command");
				ereport(LOG,
						(errcode(ERRCODE_CONNECTION_EXCEPTION),
						 errmsg("Failed to send ROLLBACK TRANSACTION command "
								"nodename:%s, pid:%d",
								conn->nodename,
								conn->backend_pid)));

				release_all = true;
			}
			else
			{
				if (rollback_implict_txn)
					twophase_update_state(conn);

				/* Read responses from these */
				connections[conn_count++] = conn;
				if (conn->ck_resp_rollback)
				{
					conn->ck_resp_rollback = false;
				}
			}
		}
	}

	if (conn_count && !release_all)
	{
		InitResponseCombiner(&combiner, conn_count, COMBINE_TYPE_NONE);
#ifdef __TWO_PHASE_TRANS__
		if (rollback_implict_txn)
		{
			g_twophase_state.response_operation = REMOTE_ABORT;
		}
#endif
		/* Receive responses */
		result = pgxc_node_receive_responses(conn_count, connections, NULL, &combiner);
		if (result)
		{
			elog(LOG, "pgxc_node_remote_abort pgxc_node_receive_responses of ROLLBACK failed");
			result = EOF;
			release_all = true;
		}
		else if (!validate_combiner(&combiner))
		{
#ifdef _PG_REGRESS_
			elog(LOG, "pgxc_node_remote_abort validate_combiner responese of ROLLBACK failed");
#else
			elog(LOG, "pgxc_node_remote_abort validate_combiner responese of ROLLBACK failed");
#endif
			result = EOF;
			release_all = true;
		}

		if (result)
		{
			if (combiner.errorMessage)
			{
				ereport(LOG,
						(errcode(ERRCODE_INTERNAL_ERROR),
						 errmsg("Failed to send ROLLBACK to on one or more "
								"nodes errmsg:%s",
								combiner.errorMessage)));
			}
			else
			{
				ereport(LOG,
						(errcode(ERRCODE_INTERNAL_ERROR),
						 errmsg("Failed to send ROLLBACK to on one or more nodes")));
			}
		}
		CloseCombiner(&combiner);
	}

	stat_transaction(conn_count);
	check_for_release_handles(release_all);
	pfree_pgxc_all_handles(current_handles);

	if (connections)
	{
		pfree(connections);
		connections = NULL;
	}

	if (sync_connections)
	{
		pfree(sync_connections);
		sync_connections = NULL;
	}
}

/*
 * Begin COPY command
 * The copy_connections array must have room for NumDataNodes items
 */
void
DataNodeCopyBegin(RemoteCopyData *rcstate)
{
	int i;
	List *nodelist = rcstate->rel_loc->rl_nodeList;
	PGXCNodeHandle **connections;
	bool need_tran_block;
	GlobalTransactionId gxid;
	ResponseCombiner combiner;
	Snapshot snapshot = GetActiveSnapshot();
	int conn_count = list_length(nodelist);
	CommandId cid = GetCurrentCommandId(true);

	/* Get needed datanode connections */
	if (!rcstate->is_from && IsLocatorReplicated(rcstate->rel_loc->locatorType))
	{
		/* Connections is a single handle to read from */
		connections = (PGXCNodeHandle **) palloc(sizeof(PGXCNodeHandle *));
		connections[0] = get_any_handle(nodelist, FIRST_LEVEL, InvalidFid, true);
		conn_count = 1;
	}
	else
	{
		PGXCNodeAllHandles *pgxc_handles;
		pgxc_handles = get_handles(nodelist, NULL, false, true, FIRST_LEVEL, InvalidFid, true, false);
		connections = pgxc_handles->datanode_handles;
		Assert(pgxc_handles->dn_conn_count == conn_count);
		pfree(pgxc_handles);
	}

	/*
	 * If more than one nodes are involved or if we are already in a
	 * transaction block, we must the remote statements in a transaction block
	 */
	need_tran_block = (conn_count > 1) || (TransactionBlockStatusCode() == 'T');

	elog(DEBUG1, "conn_count = %d, need_tran_block = %s", conn_count,
			need_tran_block ? "true" : "false");

	/* Gather statistics */
	stat_statement();
	stat_transaction(conn_count);

	gxid = GetCurrentTransactionId();

	/* Start transaction on connections where it is not started */
	if (pgxc_node_begin(conn_count, connections, gxid, need_tran_block, true))
	{
		ereport(ERROR,
				(errcode(ERRCODE_CONNECTION_EXCEPTION),
				 errmsg("Could not begin transaction on data nodes.")));
	}

	/*
	 * COPY TO do not use locator, it just takes connections from it, and
	 * we do not look up distribution data type in this case.
	 * So always use LOCATOR_TYPE_RROBIN to avoid errors because of not
	 * defined partType if real locator type is HASH or MODULO.
	 * Create locator before sending down query, because createLocator may
	 * fail and we leave with dirty connections.
	 * If we get an error now datanode connection will be clean and error
	 * handler will issue transaction abort.
	 */
#ifdef _MIGRATE_
	rcstate->locator = createLocator(
				rcstate->is_from ? rcstate->rel_loc->locatorType
						: LOCATOR_TYPE_RROBIN,
				rcstate->is_from ? RELATION_ACCESS_INSERT : RELATION_ACCESS_READ,
				LOCATOR_LIST_POINTER,
				conn_count,
				(void *) connections,
				NULL,
				false,
				rcstate->rel_loc->groupId,
				rcstate->dist_types, rcstate->rel_loc->disAttrNums,
				rcstate->n_dist_types, NULL);
#else
	rcstate->locator = createLocator(
				rcstate->is_from ? rcstate->rel_loc->locatorType
						: LOCATOR_TYPE_RROBIN,
				rcstate->is_from ? RELATION_ACCESS_INSERT : RELATION_ACCESS_READ,
				rcstate->dist_type,
				LOCATOR_LIST_POINTER,
				conn_count,
				(void *) connections,
				NULL,
				false, NULL);
#endif

	/* Send query to nodes */
	for (i = 0; i < conn_count; i++)
	{
		DrainConnectionAndSetNewCombiner(connections[i], NULL);

		if (snapshot && pgxc_node_send_snapshot(connections[i], snapshot))
		{
			add_error_message(connections[i], "Can not send request");
			pfree(connections);
			freeLocator(rcstate->locator);
			rcstate->locator = NULL;
			return;
		}
		if (ORA_MODE && pgxc_node_send_exec_env_if(connections[i]))
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("Failed to setup security context to node"),
					 errnode(connections[i])));

		if (pgxc_node_send_cmd_id(connections[i], cid) < 0)
		{
			ereport(ERROR,
					(errcode(ERRCODE_CONNECTION_EXCEPTION ),
					 errmsg("Failed to send command ID"),
					 errnode(connections[i])));
		}

		if (pgxc_node_send_query_with_internal_flag(connections[i], rcstate->query_buf.data) != 0)
		{
			add_error_message(connections[i], "Can not send request");
			pfree(connections);
			freeLocator(rcstate->locator);
			rcstate->locator = NULL;
			return;
		}
	}

	/*
	 * We are expecting CopyIn response, but do not want to send it to client,
	 * caller should take care about this, because here we do not know if
	 * client runs console or file copy
	 */
	InitResponseCombiner(&combiner, conn_count, COMBINE_TYPE_NONE);

	/* Receive responses */
	if (pgxc_node_receive_copy_begin(conn_count, connections, NULL, &combiner)
			|| !ValidateAndCloseCombiner(&combiner))
	{
		DataNodeCopyFinish(conn_count, connections);
		freeLocator(rcstate->locator);
		rcstate->locator = NULL;
		return;
	}
	pfree(connections);
}


/*
 * Send a data row to the specified nodes
 */
int
DataNodeCopyIn(char *data_row, int len, const char *eol,
		int conn_count, PGXCNodeHandle** copy_connections,
		bool binary)
{
	int msgLen = 0;
	int nLen;
	int i;

	/* size + data row + eol, eol is \n in CSV mode */
	if (binary)
	{
		msgLen = 4 + len;
	}
	else
	{
		msgLen = 4 + len + ((eol == NULL) ? 1 : strlen(eol));
	}
	nLen = htonl(msgLen);

	for (i = 0; i < conn_count; i++)
	{
		PGXCNodeHandle *handle = copy_connections[i];
		if (IsConnectionStateCopyIn(handle))
		{
			/* precalculate to speed up access */
			int bytes_needed = handle->outEnd + 1 + msgLen;

			/* flush buffer if it is almost full */
			if (bytes_needed > COPY_BUFFER_SIZE)
			{
				int to_send = handle->outEnd;

				/* First look if data node has sent a error message */
				int read_status = pgxc_node_read_data(handle, true);
				if (read_status == EOF || read_status < 0)
				{
					add_error_message(handle, "failed to read data from data node");
					return EOF;
				}

				if (handle->inStart < handle->inEnd)
				{
					ResponseCombiner combiner;
					InitResponseCombiner(&combiner, 1, COMBINE_TYPE_NONE);

					/*
					 * Validate the combiner but only if we see a proper
					 * resposne for our COPY message. The problem is that
					 * sometimes we might receive async messages such as
					 * 'M' which is used to send back command ID generated and
					 * consumed by the datanode. While the message gets handled
					 * in handle_response(), we don't want to declare receipt
					 * of an invalid message below.
					 *
					 * If there is an actual error of some sort then the
					 * connection state is will be set appropriately and we
					 * shall catch that subsequently.
					 */
					if (handle_response(handle, &combiner) == RESPONSE_COPY &&
						!ValidateAndCloseCombiner(&combiner))
						return EOF;
				}

				if (DN_CONNECTION_ERROR(handle))
					return EOF;

				/*
				 * Try to send down buffered data if we have
				 */
				if (to_send && send_some(handle, to_send) < 0)
				{
					add_error_message(handle, "failed to send data to data node");
					return EOF;
				}
			}

			if (ensure_out_buffer_capacity(bytes_needed, handle) != 0)
			{
				ereport(ERROR,
						(errcode(ERRCODE_OUT_OF_MEMORY),
						 errmsg("out of memory")));
			}

			handle->outBuffer[handle->outEnd++] = 'd';
			memcpy(handle->outBuffer + handle->outEnd, &nLen, 4);
			handle->outEnd += 4;
			memcpy(handle->outBuffer + handle->outEnd, data_row, len);
			handle->outEnd += len;
			if (!binary)
			{
				if (eol == NULL)
				{
					handle->outBuffer[handle->outEnd++] = '\n';
				}
				else
				{
					memcpy(handle->outBuffer + handle->outEnd, eol, strlen(eol));
					handle->outEnd += strlen(eol);
				}
			}

			handle->state.in_extended_query = false;
		}
		else
		{
			add_error_message(handle, "Invalid data node connection");
			return EOF;
		}
	}
	return 0;
}

uint64
DataNodeCopyOut(PGXCNodeHandle** copy_connections,
							  int conn_count, FILE* copy_file)
{
	ResponseCombiner combiner;
	uint64           processed;
	bool             error;
	char            *errinfo = NULL;

	InitResponseCombiner(&combiner, conn_count, COMBINE_TYPE_SUM);
	combiner.processed = 0;
	/* If there is an existing file where to copy data, pass it to combiner */
	if (copy_file)
	{
		combiner.copy_file = copy_file;
		combiner.remoteCopyType = REMOTE_COPY_FILE;
	}
	else
	{
		combiner.copy_file = NULL;
		combiner.remoteCopyType = REMOTE_COPY_STDOUT;
	}
	error = (pgxc_node_receive_responses(conn_count, copy_connections, NULL, &combiner) != 0);

	processed = combiner.processed;

	/* Record error msg for error printout */
	if (combiner.errorMessage)
	{
		errinfo = pstrdup(combiner.errorMessage);
	}

	if (!validate_combiner(&combiner) || error)
	{
		ereport(ERROR,
		        (errcode(ERRCODE_DATA_CORRUPTED),
		         errmsg("Unexpected response from the data nodes when combining, request type %d, "
		                "errorMessage: %s",
		                combiner.request_type,
		                errinfo)));
	}
	ValidateAndCloseCombiner(&combiner);
	return processed;
}

uint64
DataNodeCopyStore(PGXCNodeHandle** copy_connections,
								int conn_count, Tuplestorestate* store)
{
	ResponseCombiner combiner;
	uint64		processed;
	bool 		error;

	InitResponseCombiner(&combiner, conn_count, COMBINE_TYPE_SUM);
	combiner.processed = 0;
	combiner.remoteCopyType = REMOTE_COPY_TUPLESTORE;
	combiner.tuplestorestate = store;

	error = (pgxc_node_receive_responses(conn_count, copy_connections, NULL, &combiner) != 0);

	processed = combiner.processed;

	if (!validate_combiner(&combiner) || error)
	{
		ereport(ERROR,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("Unexpected response from the data nodes when combining, request type %d", combiner.request_type)));
	}
	ValidateAndCloseCombiner(&combiner);
	return processed;
}


/*
 * Finish copy process on all connections
 */
void
DataNodeCopyFinish(int conn_count, PGXCNodeHandle** connections)
{
	int		i;
	ResponseCombiner combiner;
	bool 		error = false;

	for (i = 0; i < conn_count; i++)
	{
		PGXCNodeHandle *handle = connections[i];

		error = true;
		if (IsConnectionStateCopy(handle))
			error = DataNodeCopyEnd(handle, false);
	}

	InitResponseCombiner(&combiner, conn_count, COMBINE_TYPE_NONE);

	error = (pgxc_node_receive_responses(conn_count, connections, NULL, &combiner) != 0) || error;
	if (!validate_combiner(&combiner) || error)
	{
		if (combiner.errorMessage)
			pgxc_node_report_error(&combiner);
		else
			ereport(ERROR,
					(errcode(ERRCODE_CONNECTION_EXCEPTION),
					 errmsg("Error while running COPY")));
	}
	else
		CloseCombiner(&combiner);
}

/*
 * End copy process on a connection
 */
bool
DataNodeCopyEnd(PGXCNodeHandle *handle, bool is_error)
{
	int 		nLen = pg_hton32(4);

	if (handle == NULL)
		return true;

	/* msgType + msgLen */
	if (ensure_out_buffer_capacity(handle->outEnd + 1 + 4, handle) != 0)
		return true;

	if (is_error)
		handle->outBuffer[handle->outEnd++] = 'f';
	else
		handle->outBuffer[handle->outEnd++] = 'c';

	memcpy(handle->outBuffer + handle->outEnd, &nLen, 4);
	handle->outEnd += 4;

	SetTransitionInExtendedQuery(handle, false);
	/* We need response right away, so send immediately */
	return (FlushAndStoreState(handle) ? true : false);
}


/*
 * Get Node connections depending on the connection type:
 * Datanodes Only, Coordinators only or both types
 */
static PGXCNodeAllHandles *
get_exec_connections(RemoteQueryState *planstate,
					 ExecNodes *exec_nodes,
					 RemoteQueryExecType exec_type,
					 bool is_global_session)
{
	List 	   *nodelist = NIL;
#ifdef __OPENTENBASE__
	List 	   *temp_list = NIL;
#endif
	List	   *coordlist = NIL;
	int			co_conn_count, dn_conn_count;
	bool		is_query_coord_only = false;
	PGXCNodeAllHandles *pgxc_handles = NULL;
	bool        include_local_cn = (exec_nodes ? exec_nodes->include_local_cn : false);
	bool		write = true;

	if (include_local_cn)
	{
		Assert(exec_type == EXEC_ON_ALL_NODES);
		Assert(list_length(exec_nodes->nodeList) == 0);
	}

#ifdef __OPENTENBASE__
	if (IsParallelWorker())
	{
		if (EXEC_ON_CURRENT != exec_type)
		{
			if (NULL == exec_nodes)
			{
				elog(PANIC, "In parallel worker, exec_nodes can't be NULL!");
			}

			if (exec_nodes->accesstype != RELATION_ACCESS_READ &&
				exec_nodes->accesstype != RELATION_ACCESS_READ_FQS)
			{
				elog(PANIC, "Only read access can run in parallel worker!");
			}

			if (exec_type != EXEC_ON_DATANODES)
			{
				elog(PANIC, "Parallel worker can only run query on datanodes!");
			}
		}
	}
#endif

	/*
	 * If query is launched only on Coordinators, we have to inform get_handles
	 * not to ask for Datanode connections even if list of Datanodes is NIL.
	 */
	if (exec_type == EXEC_ON_COORDS)
		is_query_coord_only = true;

	if (exec_type == EXEC_ON_CURRENT)
		return get_current_handles();

	if (exec_type == EXEC_ON_CURRENT_TXN)
	{
		bool issue_error = false;
		PGXCNodeAllHandles *handles = get_current_txn_handles(&issue_error);
		if (issue_error)
			elog(ERROR, "encounter fatal error when gettig transaction connections");
		return handles;
	}

	if (exec_nodes)
	{
		if (exec_nodes->nExprs > 0 && exec_nodes->dis_exprs[0])
		{
			ExecNodes *nodes = paramGetExecNodes(exec_nodes, (PlanState *) planstate);
			if (nodes)
			{
				nodelist = nodes->nodeList;
				pfree(nodes);
			}
		}
		else if (OidIsValid(exec_nodes->en_relid))
		{
			RelationLocInfo *rel_loc_info = GetRelationLocInfo(exec_nodes->en_relid);
#ifdef __OPENTENBASE_C__
			ExecNodes *nodes = GetRelationNodes(rel_loc_info, NULL, NULL,
												0, exec_nodes->accesstype);
#else
			ExecNodes *nodes = GetRelationNodes(rel_loc_info, 0, true, exec_nodes->accesstype);
#endif
			/*
			 * en_relid is set only for DMLs, hence a select for update on a
			 * replicated table here is an assertion
			 */
			Assert(!(exec_nodes->accesstype == RELATION_ACCESS_READ_FOR_UPDATE &&
						IsRelationReplicated(rel_loc_info)));

			/* Use the obtained list for given table */
			if (nodes)
				nodelist = nodes->nodeList;

			/*
			 * Special handling for ROUND ROBIN distributed tables. The target
			 * node must be determined at the execution time
			 */
			if (rel_loc_info->locatorType == LOCATOR_TYPE_RROBIN && nodes)
			{
				nodelist = nodes->nodeList;
			}
			else if (nodes)
			{
				if (exec_type == EXEC_ON_DATANODES || exec_type == EXEC_ON_ALL_NODES)
				{
					nodelist = exec_nodes->nodeList;
				}
			}

			if (nodes)
				pfree(nodes);
			FreeRelationLocInfo(rel_loc_info);
		}
		else
		{
			if (exec_type == EXEC_ON_DATANODES || exec_type == EXEC_ON_ALL_NODES)
				nodelist = exec_nodes->nodeList;
			else if (exec_type == EXEC_ON_COORDS)
				coordlist = exec_nodes->nodeList;
		}

		/* determine if this connection can be read only */
		write = exec_nodes->accesstype != RELATION_ACCESS_READ_FQS;
	}

	if (planstate && ((RemoteQuery *) planstate->combiner.ss.ps.plan)->read_only)
		write = false;

	/* Set node list and DN number */
	if (list_length(nodelist) == 0 &&
		(exec_type == EXEC_ON_ALL_NODES ||
		 exec_type == EXEC_ON_DATANODES))
	{
		dn_conn_count = NumDataNodes;
	}
	else
	{
		if (exec_type == EXEC_ON_DATANODES || exec_type == EXEC_ON_ALL_NODES)
			dn_conn_count = list_length(nodelist);
		else
			dn_conn_count = 0;
	}

	/* Set Coordinator list and Coordinator number */
	if ((list_length(nodelist) == 0 && exec_type == EXEC_ON_ALL_NODES) ||
		(list_length(coordlist) == 0 && exec_type == EXEC_ON_COORDS))
	{
		coordlist = GetAllCoordNodes(include_local_cn);
		co_conn_count = list_length(coordlist);
	}
	else
	{
		if (exec_type == EXEC_ON_COORDS)
			co_conn_count = list_length(coordlist);
		else
			co_conn_count = 0;
	}

	if (list_length(nodelist) == 0 && exec_type == EXEC_ON_ALL_NODES && !IS_CENTRALIZED_MODE)
	{
		nodelist = GetAllDataNodes();
		dn_conn_count = NumDataNodes;
	}

#ifdef __OPENTENBASE__
	if (IsParallelWorker())
	{
		int32   i          = 0;
		int32   worker_num = 0;
		int32	length	   = 0;
		int32   step 	   = 0;
		int32   begin_node = 0;
		int32   end_node   = 0;
		ParallelWorkerStatus *parallel_status   = NULL;
		ListCell			 *node_list_item    = NULL;
		parallel_status = planstate->parallel_status;
		temp_list       = nodelist;
		nodelist		= NULL;

		worker_num = ExecGetForWorkerNumber(parallel_status);
		length = list_length(temp_list);

		step = DIVIDE_UP(length, worker_num);
		/* Last worker. */
		if (ParallelWorkerNumber == (worker_num - 1))
		{
			begin_node = ParallelWorkerNumber * step;
			end_node   = length;
		}
		else
		{
			begin_node = ParallelWorkerNumber * step;
			end_node   = begin_node + step;
		}

		/* Form the execNodes of our own node. */
		i = 0;
		foreach(node_list_item, temp_list)
		{
			int	node = lfirst_int(node_list_item);

			if (i >= begin_node && i < end_node)
			{
				nodelist = lappend_int(nodelist, node);
			}

			if (i >= end_node)
			{
				break;
			}
		}
		list_free(temp_list);
		exec_nodes->nodeList = nodelist;
		dn_conn_count = list_length(nodelist);
		if (list_length(coordlist) != 0)
		{
			elog(PANIC, "Parallel worker can not run query on coordinator!");
		}
	}
#endif

	pgxc_handles = get_handles(nodelist, coordlist, is_query_coord_only,
							   is_global_session, FIRST_LEVEL,
							   InvalidFid, write, false);
	if (!pgxc_handles)
		ereport(ERROR,
				(errcode(ERRCODE_CONNECTION_EXCEPTION),
				 errmsg("Could not obtain connection from pool")));

	/* Depending on the execution type, we still need to save the initial node counts */
	pgxc_handles->dn_conn_count = dn_conn_count;
	pgxc_handles->co_conn_count = co_conn_count;

	return pgxc_handles;
}


static bool
pgxc_start_command_on_connection(PGXCNodeHandle *connection,
								 RemoteQueryState *remotestate,
								 Snapshot snapshot,
								 bool prepared)
{
	CommandId	cid;
	ResponseCombiner *combiner = (ResponseCombiner *) remotestate;
	RemoteQuery	*step = (RemoteQuery *) combiner->ss.ps.plan;
	EState *estate = combiner->ss.ps.state;
	int16 *formats = NULL;
	int   n_formats = 0;

	DrainConnectionAndSetNewCombiner(connection, combiner);

	elog(DEBUG5, "pgxc_start_command_on_connection - node %s, state %d",
			connection->nodename, GetConnectionState(connection));

	/*
	 * Scan descriptor would be valid and would contain a valid snapshot
	 * in cases when we need to send out of order command id to data node
	 * e.g. in case of a fetch
	 */
	if (snapshot && ActivePortal && ActivePortal->cursor)
	{
		cid = snapshot->curcid;

		if (cid == InvalidCommandId)
		{
			elog(LOG, "commandId in snapshot is invalid.");
			cid = GetCurrentCommandId(false);
		}
	}
	else
	{
		cid = GetCurrentCommandId(false);
	}

	if (pgxc_node_send_cmd_id(connection, cid) < 0 )
		return false;

	if (IsA(combiner->ss.ps.plan, RemoteQuery) &&
		!(remotestate->eflags & (EXEC_FLAG_WITH_DESC | EXEC_FLAG_EXPLAIN_ONLY))
		&& !combiner->merge_sort && IS_PGXC_COORDINATOR && combiner->ss.ps.plan->targetlist != NULL)
	{
		combiner->ss.ps.state->cn_penetrate_tuple = true;
		if (pgxc_node_send_no_tuple_descriptor(connection) < 0 )
			return false;
		if (pgxc_node_send_dn_direct_printtup(connection) < 0 )
			return false;
	}

	if (snapshot && pgxc_node_send_snapshot(connection, snapshot))
		return false;
	if (ORA_MODE && pgxc_node_send_exec_env_if(connection))
		return false;
	if (step->statement || step->cursor || remotestate->rqs_num_params)
	{
		/* need to use Extended Query Protocol */
		int	fetch = 0;

		/*
		 * execute and fetch rows only if they will be consumed
		 * immediately by the sorter
		 */
		if (step->cursor)
#ifdef __OPENTENBASE__
		{
			/* we need all rows one time */
			if (step->dml_on_coordinator)
				fetch = 0;
			else
				fetch = 1;
		}
#else
			fetch = 1;
#endif
		combiner->extended_query = true;
		if (IsA(combiner->ss.ps.plan, RemoteQuery) &&
			!combiner->merge_sort && IS_PGXC_COORDINATOR)
		{
			formats = estate->resultFormats;
			n_formats = estate->nResultFormats;
		}

		if (pgxc_node_send_query_extended(connection,
							prepared ? NULL : step->sql_statement,
							step->statement,
							step->cursor,
							remotestate->rqs_num_params,
							remotestate->rqs_param_types,
							remotestate->paramval_len,
							remotestate->paramval_data,
							remotestate->rqs_param_formats,
							remotestate->rqs_num_params,
							step->has_row_marks ? true : step->read_only,
							fetch,
							formats,
							n_formats) != 0)
			return false;
	}
	else
	{
		combiner->extended_query = false;
		if (pgxc_node_send_query(connection, step->sql_statement) != 0)
			return false;
	}
	return true;
}

/*
 * Execute utility statement on multiple Datanodes
 * It does approximately the same as
 *
 * RemoteQueryState *state = ExecInitRemoteQuery(plan, estate, flags);
 * Assert(TupIsNull(ExecRemoteQuery(state));
 * ExecEndRemoteQuery(state)
 *
 * But does not need an Estate instance and does not do some unnecessary work,
 * like allocating tuple slots.
 */
void
ExecRemoteUtility(RemoteQuery *node)
{
	ResponseCombiner combiner;
	bool		force_autocommit = node->force_autocommit;
	RemoteQueryExecType exec_type = node->exec_type;
	GlobalTransactionId gxid = InvalidGlobalTransactionId;
	Snapshot snapshot = NULL;
	PGXCNodeAllHandles *pgxc_connections;
	int			co_conn_count;
	int			dn_conn_count;
	bool		need_tran_block;
	ExecDirectType		exec_direct_type = node->exec_direct_type;
	int					i;
	CommandId			cid = GetCurrentCommandId(true);
	bool                utility_need_transcation = true;
	bool 				exec_direct_dn_allow = node->exec_direct_dn_allow;
	int					send_count = 0;

	if (exec_type == EXEC_ON_DATANODES &&
		exec_direct_dn_allow == false &&
		exec_direct_type != EXEC_DIRECT_NONE &&
		exec_direct_type != EXEC_DIRECT_SELECT && !IsInplaceUpgrade)
	{
		elog(ERROR, "EXECUTE DIRECT non select command cannot be executed on Datanode");
	}

	if (exec_direct_type == EXEC_DIRECT_LOCAL_NOTHING)
		return;

	if (!force_autocommit)
		RegisterTransactionLocalNode(true);

	/*
	 * Do not set global_session if it is a utility statement.
	 * Avoids CREATE NODE error on cluster configuration.
	 */
	pgxc_connections = get_exec_connections(NULL, node->exec_nodes, exec_type,
											exec_direct_type != EXEC_DIRECT_UTILITY);

	dn_conn_count = pgxc_connections->dn_conn_count;
	co_conn_count = pgxc_connections->co_conn_count;
	/* exit right away if no nodes to run command on */
	if (dn_conn_count == 0 && co_conn_count == 0)
	{
		pfree_pgxc_all_handles(pgxc_connections);
		return;
	}

	if (force_autocommit)
		need_tran_block = false;
	else
		need_tran_block = true;

	/* Commands launched through EXECUTE DIRECT do not need start a transaction */
	if (exec_direct_type == EXEC_DIRECT_UTILITY)
	{
		need_tran_block = false;

		/* This check is not done when analyzing to limit dependencies */
		if (IsTransactionBlock())
			ereport(ERROR,
					(errcode(ERRCODE_ACTIVE_SQL_TRANSACTION),
						errmsg("cannot run EXECUTE DIRECT with utility inside a transaction block")));
	}

#ifdef __OPENTENBASE__
	/* Some DDL such as ROLLBACK, SET does not need transaction */
	utility_need_transcation =
		(!ExecDDLWithoutAcquireXid(node->parsetree) && !node->is_set);

	if (utility_need_transcation)
#endif
	{
		if (log_statement == LOGSTMT_ALL)
			elog(LOG, "[SAVEPOINT] node->sql_statement:%s", node->sql_statement);
		gxid = GetCurrentTransactionId();
	}

	if (!node->is_set && ActiveSnapshotSet())
		snapshot = GetActiveSnapshot();
#ifdef __OPENTENBASE__
	if (utility_need_transcation)
#endif
	{
		if (!GlobalTransactionIdIsValid(gxid))
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_TRANSACTION_STATE),
						errmsg("Failed to get next transaction ID")));
	}

#ifdef __SUPPORT_DISTRIBUTED_TRANSACTION__
	if (!IS_PGXC_LOCAL_COORDINATOR)
	{
		/*
		 * Global xid is not needed to send to remote nodes
		 * for connections from coord and datanode as
		 * normal DDLs except for set_config_option are all single level
		 * connections from Coords executing distributed DDLs.
		 */
		gxid = InvalidTransactionId;
	}

#endif
	if (!node->is_set &&

		pgxc_node_begin(dn_conn_count, pgxc_connections->datanode_handles,
						gxid, need_tran_block, exec_direct_type != EXEC_DIRECT_UTILITY))
		ereport(ERROR,
				(errcode(ERRCODE_CONNECTION_EXCEPTION),
					errmsg("Could not begin transaction on Datanodes")));
	
	InitResponseCombiner(&combiner, dn_conn_count + co_conn_count, COMBINE_TYPE_NONE);
	combiner.connections = (PGXCNodeHandle **)palloc(sizeof(PGXCNodeHandle *) * (dn_conn_count + co_conn_count));

	for (i = 0; i < dn_conn_count; i++)
	{
		PGXCNodeHandle *conn = pgxc_connections->datanode_handles[i];

		DrainConnectionAndSetNewCombiner(conn, NULL);
		if (!IsConnectionStateIdle(conn))
			ereport(ERROR,
					(errcode(ERRCODE_CONNECTION_EXCEPTION),
							errmsg("Connection is running under subplan,"
								   "please remove set or subplans"),
							errnode(conn)));

		if (node->is_set && GucSubtxnId > GetCurrentGucCid(conn))
			SetCurrentGucCid(conn, GucSubtxnId);

		if (snapshot && pgxc_node_send_snapshot(conn, snapshot))
			ereport(ERROR,
					(errcode(ERRCODE_CONNECTION_EXCEPTION),
							errmsg("Failed to send snapshot"),
							errnode(conn)));

		if (ORA_MODE && pgxc_node_send_exec_env_if(conn))
			ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
					errmsg("Failed to setup security context"),
					errnode(conn)));

		if (pgxc_node_send_cmd_id(conn, cid) < 0)
			ereport(ERROR,
					(errcode(ERRCODE_CONNECTION_EXCEPTION),
							errmsg("Failed to send command ID"),
							errnode(conn)));

		if (pgxc_node_send_query(conn, node->sql_statement) != 0)
			ereport(ERROR,
					(errcode(ERRCODE_CONNECTION_EXCEPTION),
							errmsg("Failed to send command"),
							errnode(conn)));
		combiner.connections[send_count++] = conn;
		conn->combiner = &combiner;
	}

	if (!node->is_set &&
		pgxc_node_begin(co_conn_count, pgxc_connections->coord_handles,
						gxid, need_tran_block, exec_direct_type != EXEC_DIRECT_UTILITY))
		ereport(ERROR,
				(errcode(ERRCODE_CONNECTION_EXCEPTION),
					errmsg("Could not begin transaction on coordinators")));

	/* Now send it to Coordinators if necessary */
	for (i = 0; i < co_conn_count; i++)
	{
		PGXCNodeHandle *conn = pgxc_connections->coord_handles[i];

		if (node->is_set && GucSubtxnId > GetCurrentGucCid(conn))
			SetCurrentGucCid(conn, GucSubtxnId);

		if (snapshot && pgxc_node_send_snapshot(conn, snapshot))
			ereport(ERROR,
					(errcode(ERRCODE_CONNECTION_EXCEPTION),
							errmsg("Failed to send snapshot"),
							errnode(conn)));

		if (ORA_MODE && pgxc_node_send_exec_env_if(conn))
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
							errmsg("Failed to setup security context"),
							errnode(conn)));

		if (pgxc_node_send_cmd_id(conn, cid) < 0)
			ereport(ERROR,
					(errcode(ERRCODE_CONNECTION_EXCEPTION),
							errmsg("Failed to send command ID"),
							errnode(conn)));

		if (pgxc_node_send_query(conn, node->sql_statement) != 0)
			ereport(ERROR,
					(errcode(ERRCODE_CONNECTION_EXCEPTION),
							errmsg("Failed to send command"),
							errnode(conn)));
		combiner.connections[send_count++] = conn;
		conn->combiner = &combiner;
	}

	if (send_count > 0)
	{
		combiner.node_count = send_count;
		ProcessResponseMessage(&combiner, NULL, RESPONSE_READY, false);
	}
	
	if (combiner.connections)
		pfree(combiner.connections);

	/*
	 * We have processed all responses from nodes and if we have
	 * error message pending we can report it. All connections should be in
	 * consistent state now and so they can be released to the pool after ROLLBACK.
	 */
	pfree_pgxc_all_handles(pgxc_connections);
	pgxc_node_report_error(&combiner);
}


/*
 * Called when the backend is ending.
 */
void
PGXCNodeCleanAndRelease(int code, Datum arg)
{
	/* Disconnect from Pooler, if any connection is still held Pooler close it */
	PoolManagerDisconnect();

	/* Close connection with GTM */
	CloseGTM();

#ifdef __RESOURCE_QUEUE__
	ResQueue_CloseGTM();
#endif

	/* Dump collected statistics to the log */
	stat_log();
}

static void
ExecCloseRemoteStatementInternal(const char *stmt_name, PGXCNodeAllHandles *handles)
{
	PGXCNodeHandle	  **connections;
	ResponseCombiner	combiner;
	int					conn_count;
	int 				i;

	conn_count = handles->dn_conn_count;
	connections = handles->datanode_handles;

	for (i = 0; i < conn_count; i++)
	{
		DrainConnectionAndSetNewCombiner(connections[i], NULL);

		if (pgxc_node_send_close(connections[i], true, stmt_name) != 0)
		{
			/*
			 * statements are not affected by statement end, so consider
			 * unclosed statement on the Datanode as a fatal issue and
			 * force connection is discarded
			 */
			UpdateConnectionState(connections[i],
					DN_CONNECTION_STATE_FATAL);
			ereport(WARNING,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("Failed to close Datanode statement")));
		}
		if (pgxc_node_send_sync(connections[i]) != 0)
		{
			UpdateConnectionState(connections[i],
					DN_CONNECTION_STATE_FATAL);
			ereport(WARNING,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("Failed to close Datanode statement")));

			ereport(LOG,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("Failed to sync msg to node %s backend_pid:%d",
							connections[i]->nodename,
							connections[i]->backend_pid)));
		}

		if (!IsConnectionStateFatal(connections[i]))
			UpdateConnectionState(connections[i], DN_CONNECTION_STATE_CLOSE);
	}

	InitResponseCombiner(&combiner, conn_count, COMBINE_TYPE_NONE);

	while (conn_count > 0)
	{
		if (pgxc_node_receive(conn_count, connections, NULL))
		{
			for (i = 0; i < conn_count; i++)
				UpdateConnectionState(connections[i],
						DN_CONNECTION_STATE_FATAL);

			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("Failed to close Datanode statement")));
		}
		i = 0;
		while (i < conn_count)
		{
			int res = handle_response(connections[i], &combiner);
			if (res == RESPONSE_EOF)
			{
				i++;
			}
			else if (res == RESPONSE_READY || IsConnectionStateFatal(connections[i]))
			{
				if (--conn_count > i)
					connections[i] = connections[conn_count];
			}
		}
	}

	ValidateAndCloseCombiner(&combiner);
}

/*
 * close remote statement needs to be inside a transaction so that syscache can be accessed
 */
void
ExecCloseRemoteStatement(const char *stmt_name, PGXCNodeAllHandles *handles)
{
	bool need_abort = false;

	if (IsTransactionIdle())
	{
		StartTransactionCommand();
		need_abort = true;
	}
	else if (!IsTransactionState() && !IsSubTransaction())
	{
		elog(WARNING, "unexpected transaction stat %s blockState %s",
			 CurrentTransactionTransStateAsString(),
			 CurrentTransactionBlockStateAsString());
	}

	ExecCloseRemoteStatementInternal(stmt_name, handles);

	if(enable_distri_print)
		elog(LOG, "ExecCloseRemoteStatementInternal %s", stmt_name);

	if (need_abort)
	{
		AbortCurrentTransaction();
	}
}

/*
 * DataNodeCopyInBinaryForAll
 *
 * In a COPY TO, send to all Datanodes PG_HEADER for a COPY TO in binary mode.
 */
int
DataNodeCopyInBinaryForAll(char *msg_buf, int len, int conn_count,
									  PGXCNodeHandle** connections)
{
	int 		i;
	int msgLen = 4 + len;
	int nLen = pg_hton32(msgLen);

	for (i = 0; i < conn_count; i++)
	{
		PGXCNodeHandle *handle = connections[i];
		if (IsConnectionStateCopyIn(handle))
		{
			/* msgType + msgLen */
			if (ensure_out_buffer_capacity(handle->outEnd + 1 + msgLen, handle) != 0)
			{
				ereport(ERROR,
					(errcode(ERRCODE_OUT_OF_MEMORY),
					errmsg("out of memory")));
			}

			handle->outBuffer[handle->outEnd++] = 'd';
			memcpy(handle->outBuffer + handle->outEnd, &nLen, 4);
			handle->outEnd += 4;
			memcpy(handle->outBuffer + handle->outEnd, msg_buf, len);
			handle->outEnd += len;
		}
		else
		{
			add_error_message(handle, "Invalid Datanode connection");
			return EOF;
		}
	}

	return 0;
}

/*
 * Encode parameter values to format of DataRow message (the same format is
 * used in Bind) to prepare for sending down to Datanodes.
 * The data row is copied to RemoteQueryState.paramval_data.
 */
void
SetDataRowForExtParams(ParamListInfo paraminfo, RemoteQueryState *rq_state)
{
	StringInfoData buf;
	uint16 n16;
	int i;
	int real_num_params = 0;

	/* If there are no parameters, there is no data to BIND. */
	if (!paraminfo)
		return;

	Assert(!rq_state->paramval_data);

	/*
	 * It is necessary to fetch parameters
	 * before looking at the output value.
	 */
	for (i = 0; i < paraminfo->numParams; i++)
	{
		ParamExternData *param;
		ParamExternData  param_data;

		if (paraminfo->paramFetch != NULL)
			param = (*paraminfo->paramFetch) (paraminfo, i + 1, false,
											  &param_data);
		else
			param = &paraminfo->params[i];

		/*
		 * This is the last parameter found as useful, so we need
		 * to include all the previous ones to keep silent the remote
		 * nodes. All the parameters prior to the last usable having no
		 * type available will be considered as NULL entries.
		 *
		 * Or if it's really a NULL entry, accept it.
		 */
		if (OidIsValid(param->ptype) || param->isnull)
			real_num_params = i + 1;
	}

	/*
	 * If there are no parameters available, simply leave.
	 * This is possible in the case of a query called through SPI
	 * and using no parameters.
	 */
	if (real_num_params == 0)
	{
		rq_state->paramval_data = NULL;
		rq_state->paramval_len = 0;
		return;
	}

	initStringInfo(&buf);

	/* Number of parameter values */
	n16 = pg_hton16(real_num_params);
	appendBinaryStringInfo(&buf, (char *) &n16, 2);

	/* Parameter values */
	for (i = 0; i < real_num_params; i++)
	{
		ParamExternData *param = &paraminfo->params[i];
		uint32 n32;

		/*
		 * Parameters with no types are considered as NULL and treated as integer
		 * The same trick is used for dropped columns for remote DML generation.
		 */
		if (param->isnull || !OidIsValid(param->ptype)
#ifdef _PG_ORCL_
				|| param->ptype == PLUDTOID
#endif
								)
		{
			n32 = pg_hton32(-1);
			appendBinaryStringInfo(&buf, (char *) &n32, 4);
		}
		else
		{
			Oid		typOutput;
			bool	typIsVarlena;
			Datum	pval;
			char   *pstring;
			int		len;
				/* Get info needed to output the value */
				getTypeOutputInfo(param->ptype, &typOutput, &typIsVarlena);

			/*
			 * If we have a toasted datum, forcibly detoast it here to avoid
			 * memory leakage inside the type's output routine.
			 */
			if (typIsVarlena)
				pval = PointerGetDatum(PG_DETOAST_DATUM(param->value));
			else
				pval = param->value;

			/* Convert Datum to string */
			pstring = OidOutputFunctionCall(typOutput, pval);

			/* copy data to the buffer */
			len = strlen(pstring);
			n32 = pg_hton32(len);
			appendBinaryStringInfo(&buf, (char *) &n32, 4);
			appendBinaryStringInfo(&buf, pstring, len);
			pfree(pstring);
		}
	}


	/*
	 * If parameter types are not already set, infer them from
	 * the paraminfo.
	 */
	rq_state->rqs_num_params = real_num_params;
	rq_state->rqs_param_types = (Oid *) palloc(sizeof(Oid) * real_num_params);
	rq_state->rqs_param_formats = (int16 *) palloc(sizeof(int16) * real_num_params);
	for (i = 0; i < real_num_params; i++)
	{
		rq_state->rqs_param_types[i] = paraminfo->params[i].ptype;

			/* text */
			rq_state->rqs_param_formats[i] = 0;
	}

	/* Assign the newly allocated data row to paramval */
	rq_state->paramval_data = buf.data;
	rq_state->paramval_len = buf.len;
}

/*
 * Clear per transaction remote information
 */
void
AtEOXact_Remote(void)
{
	if (enable_distri_print)
	{
		if (current_transaction_handles)
			elog(LOG, "reset_transaction_handles dn_conn_count %d co_conn_count %d", current_transaction_handles->dn_conn_count, current_transaction_handles->co_conn_count);
		else
			elog(LOG, "reset_transaction_handles dn_conn_count ");
	}

	/* Reset command Id sending flag */
	SetSendCommandId(false);

	session_param_list_reset(true);

	if (IS_PGXC_COORDINATOR)
		reset_transaction_handles();
}

/*
 * cleaup up the remaining fatal and error handles when start a new transaction. 
 */
void
AtStart_Remote(void)
{
	reset_sock_fatal_handles();
	reset_error_handles(false);
}
/*
 * Invoked when local transaction is about to be committed.
 * If nodestring is specified commit specified prepared transaction on remote
 * nodes, otherwise commit remote nodes which are in transaction.
 */
void
PreCommit_Remote(char *prepareGID, char *nodestring, bool preparedLocalNode)
{
	struct rusage		start_r;
	struct timeval		start_t;

	if (log_gtm_stats)
		ResetUsageCommon(&start_r, &start_t);

	/*
	 * Made node connections persistent if we are committing transaction
	 * that touched temporary tables. We never drop that flag, so after some
	 * transaction has created a temp table the session's remote connections
	 * become persistent.
	 * We do not need to set that flag if transaction that has created a temp
	 * table finally aborts - remote connections are not holding temporary
	 * objects in this case.
	 */
	if (IS_PGXC_LOCAL_COORDINATOR &&
		(MyXactFlags & XACT_FLAGS_ACCESSEDTEMPREL))
		temp_object_included = true;

	/*
	 * OK, everything went fine. At least one remote node is in PREPARED state
	 * and the transaction is successfully prepared on all the involved nodes.
	 * Now we are ready to commit the transaction. We need a new GXID to send
	 * down the remote nodes to execute the forthcoming COMMIT PREPARED
	 * command. So grab one from the GTM and track it. It will be closed along
	 * with the main transaction at the end.
	 */
	if (nodestring)
	{
		Assert(preparedLocalNode);
		if (enable_distri_print)
		{
			elog(LOG, "2PC commit precommit remote gid %s", prepareGID);
		}
		pgxc_node_remote_finish(prepareGID, true, nodestring,
								GetAuxilliaryTransactionId(),
								GetTopGlobalTransactionId());

	}

	if (!IsTwoPhaseCommitRequired(preparedLocalNode))
	{
		if (enable_distri_debug && GetTopGlobalTransactionId() && GetGlobalXidNoCheck())
		{
			if (enable_distri_print)
			{
				elog(LOG,
					 "Non-2PC Commit transaction top xid %u",
					 GetTopGlobalTransactionId());
			}
			LogCommitTranGTM(GetTopGlobalTransactionId(),
							 GetGlobalXid(),
							 NULL,
							 1,
							 true,
							 true,
							 InvalidGlobalTimestamp,
							 InvalidGlobalTimestamp);
			is_distri_report = true;
		}
		pgxc_node_remote_commit(TXN_TYPE_CommitTxn, true);
	}

	if (log_gtm_stats)
		ShowUsageCommon("PreCommit_Remote", &start_r, &start_t);
}

/*
 * Whether node need clean: last command is not finished
 * 'Z' message: ready for query
 * 'C' message: command complete
 */
static inline bool
node_need_clean(PGXCNodeHandle *handle)
{
	return (!IsConnectionStateFatal(handle) &&
			!IsConnectionStateError(handle) &&
			!IsConnectionStateIdle(handle));
}

/*
 * Do abort processing for the transaction. We must abort the transaction on
 * all the involved nodes. If a node has already prepared a transaction, we run
 * ROLLBACK PREPARED command on the node. Otherwise, a simple ROLLBACK command
 * is sufficient.
 *
 * We must guard against the case when a transaction is prepared succefully on
 * all the nodes and some error occurs after we send a COMMIT PREPARED message
 * to at lease one node. Such a transaction must not be aborted to preserve
 * global consistency. We handle this case by recording the nodes involved in
 * the transaction at the GTM and keep the transaction open at the GTM so that
 * its reported as "in-progress" on all the nodes until resolved
 *
 *   SPECIAL WARNGING:
 *   ONLY LOG LEVEL ELOG CALL allowed here, else will cause coredump or resource leak in some rare condition.
 */

bool
PreAbort_Remote(TranscationType txn_type, bool need_release_handle)
{
	/*
	 * We are about to abort current transaction, and there could be an
	 * unexpected error leaving the node connection in some state requiring
	 * clean up, like COPY or pending query results.
	 * If we are running copy we should send down CopyFail message and read
	 * all possible incoming messages, there could be copy rows (if running
	 * COPY TO) ErrorResponse, ReadyForQuery.
	 * If there are pending results (connection state is DN_CONNECTION_STATE_QUERY)
	 * we just need to read them in and discard, all necessary commands are
	 * already sent. The end of input could be CommandComplete or
	 * PortalSuspended, in either case subsequent ROLLBACK closes the portal.
	 */
	PGXCNodeAllHandles 	*current_handles = NULL;
	PGXCNodeHandle	   **clean_nodes = NULL;
	int					node_count = 0;
	int					cancel_dn_count = 0, cancel_co_count = 0;
	int					*cancel_dn_list = NULL;
	int					*cancel_dn_pid_list = NULL;
	int					*cancel_co_list = NULL;
	int 				i;
	struct rusage		start_r;
	struct timeval		start_t;

	Assert(txn_type == TXN_TYPE_RollbackTxn ||
		   txn_type == TXN_TYPE_RollbackSubTxn ||
		   txn_type == TXN_TYPE_CleanConnection);

	if (txn_type == TXN_TYPE_RollbackTxn)
		current_handles = get_current_txn_handles(NULL);
	else
		current_handles = get_current_subtxn_handles(GetCurrentSubTransactionId(), true);

	clean_nodes = (PGXCNodeHandle**)palloc(sizeof(PGXCNodeHandle*) *
										   (current_handles->dn_conn_count +
											current_handles->co_conn_count));
	if (log_gtm_stats)
		ResetUsageCommon(&start_r, &start_t);

	if ((current_handles->dn_conn_count + current_handles->co_conn_count) == 0)
	{
		if (enable_distri_print)
			elog(LOG, "PreAbort_remote, there is not remote connections used by this transaction");
		return true;
	}

	cancel_dn_list = (int*)palloc(sizeof(int) * current_handles->dn_conn_count);
	cancel_dn_pid_list = (int*)palloc(sizeof(int) * current_handles->dn_conn_count);
	cancel_co_list = (int*)palloc(sizeof(int) * current_handles->co_conn_count);

	if (enable_distri_print)
		elog (LOG, "PreAbort_Remote, txn_type %d, dns: %d, cns: %d",
				txn_type, current_handles->dn_conn_count, current_handles->co_conn_count);

	/*
	 * Find "dirty" coordinator connections.
	 * COPY is never running on a coordinator connections, we just check for
	 * pending data.
	 */
	for (i = 0; i < current_handles->co_conn_count; i++)
	{
		PGXCNodeHandle *handle = current_handles->coord_handles[i];
		ResetTransitionState(handle);
		if (handle->sock != NO_SOCKET)
		{
			if (enable_distri_print)
				ereport(LOG,
						(errcode(ERRCODE_INTERNAL_ERROR),
							errmsg("PreAbort_Remote node:%s pid:%d status:%d transaction_status %c.",
								   handle->nodename, handle->backend_pid, GetConnectionState(handle), GetTxnState(handle))));
			if (handle->combiner)
				handle->combiner = NULL;
			
			if (handle->registered_subtranid != InvalidSubTransactionId)
				handle->registered_subtranid = InvalidSubTransactionId;

			if (node_need_clean(handle))
			{
				/*
				 * Forget previous combiner if any since input will be handled by
				 * different one.
				 */
				handle->combiner = NULL;
				handle->last_command = 0;
				clean_nodes[node_count++] = handle;
				cancel_co_list[cancel_co_count++] = PGXCNodeGetNodeId(handle->nodeoid, NULL);

#ifdef _PG_REGRESS_
				ereport(LOG,
						(errcode(ERRCODE_INTERNAL_ERROR),
						 errmsg("PreAbort_Remote node:%s pid:%d status:%d need clean.",
								handle->nodename, handle->backend_pid, GetConnectionState(handle))));
#endif
				if (handle->state.in_extended_query)
				{
					if (pgxc_node_send_sync(handle))
					{
						ereport(LOG,
								(errcode(ERRCODE_INTERNAL_ERROR),
								 errmsg("Failed to sync msg to node:%s pid:%d when abort",
										handle->nodename, handle->backend_pid)));
					}
                    /*
                     * If connection is in DN_CONNECTION_STATE_IDLE state,
                     * for example, in the extension protocol, only the plan is sent,
                     * but the execute cmd has not been sent yet.
                     * We set the state DN_CONNECTION_STATE_QUERY to read ReadyForQuery responses.
                     */
					else if (IsConnectionStateIdle(handle))
						UpdateConnectionState(handle, DN_CONNECTION_STATE_QUERY);
#ifdef _PG_REGRESS_
					ereport(LOG,
							(errcode(ERRCODE_INTERNAL_ERROR),
							 errmsg("Succeed to sync msg to node:%s pid:%d when abort",
									handle->nodename, handle->backend_pid)));
#endif
				}
			}
			else
			{

				if (handle->needSync)
				{
					ereport(LOG,
							(errcode(ERRCODE_INTERNAL_ERROR),
							 errmsg("Invalid node:%s pid:%d needSync flag",
									handle->nodename, handle->backend_pid)));
				}

#ifdef _PG_REGRESS_
				ereport(LOG,
						(errcode(ERRCODE_INTERNAL_ERROR),
						 errmsg("PreAbort_Remote node:%s pid:%d status:%d no need clean.",
								handle->nodename,
								handle->backend_pid,
								GetConnectionState(handle))));
#endif
			}
		}
#ifdef __OPENTENBASE__
		else
		{
			elog(LOG, "PreAbort_Remote cn node %s pid %d, invalid socket %d!",
				handle->nodename, handle->backend_pid, handle->sock);
		}
#endif
	}

	/*
	 * The same for data nodes, but cancel COPY if it is running.
	 */
	for (i = 0; i < current_handles->dn_conn_count; i++)
	{
		PGXCNodeHandle *handle = current_handles->datanode_handles[i];

		if (handle->combiner)
			handle->combiner = NULL;

		if (handle->registered_subtranid != InvalidSubTransactionId)
			handle->registered_subtranid = InvalidSubTransactionId;

		if (enable_distri_print)
			ereport(LOG,
					(errcode(ERRCODE_INTERNAL_ERROR),
						errmsg("PreAbort_Remote node:%s pid:%d status:%d transaction_status %c.",
							   handle->nodename, handle->backend_pid, GetConnectionState(handle), GetTxnState(handle))));

		ResetTransitionState(handle);
		if (handle->sock != NO_SOCKET)
		{
			if (IsConnectionStateCopy(handle))
			{
#ifdef _PG_REGRESS_
				ereport(LOG,
						(errcode(ERRCODE_INTERNAL_ERROR),
						 errmsg("PreAbort_Remote node:%s pid:%d status:%d need clean.",
								handle->nodename,
								handle->backend_pid,
								GetConnectionState(handle))));
#endif
				if (handle->state.in_extended_query)
				{
					if (pgxc_node_send_sync(handle))
					{
						ereport(LOG,
								(errcode(ERRCODE_INTERNAL_ERROR),
								 errmsg("Failed to sync msg to node:%s pid:%d when abort",
										handle->nodename,
										handle->backend_pid)));

					}
#ifdef _PG_REGRESS_
					ereport(LOG,
							(errcode(ERRCODE_INTERNAL_ERROR),
							 errmsg("Succeed to sync msg to node:%s pid:%d when abort",
									handle->nodename,
									handle->backend_pid)));
#endif
				}

				DataNodeCopyEnd(handle, true);
				/*
				 * Forget previous combiner if any since input will be handled by
				 * different one.
				 */
				handle->combiner = NULL;
#ifdef __OPENTENBASE__
				/*
				 * if datanode report error, there is no need to send cancel to it,
				 * and would not wait this datanode reponse.
				 */
#endif
			}
			else if (node_need_clean(handle))
			{

				if (!IsTxnStateError(handle))
				{
					/*
					 * Forget previous combiner if any since input will be handled by
					 * different one.
					 */
					handle->combiner = NULL;
					handle->last_command = 0;
					clean_nodes[node_count++] = handle;
					cancel_dn_list[cancel_dn_count] = PGXCNodeGetNodeId(handle->nodeoid, NULL);
					cancel_dn_pid_list[cancel_dn_count] = handle->backend_pid;
					cancel_dn_count++;
				}

#ifdef _PG_REGRESS_
				ereport(LOG,
						(errcode(ERRCODE_INTERNAL_ERROR),
						 errmsg("PreAbort_Remote node:%s pid:%d status:%d need clean.",
								handle->nodename,
								handle->backend_pid,
								GetConnectionState(handle))));
#endif

				if (handle->state.in_extended_query)
				{
					if (pgxc_node_send_sync(handle))
					{
						ereport(LOG,
								(errcode(ERRCODE_INTERNAL_ERROR),
								 errmsg("Failed to sync msg to node:%s pid:%d when abort",
										handle->nodename,
										handle->backend_pid)));
					}

					if (node_count == 0 || clean_nodes[node_count-1] != handle)
						clean_nodes[node_count++] = handle;
					/*
					 * If connection is in DN_CONNECTION_STATE_IDLE state,
					 * for example, in the extension protocol, only the plan is sent,
					 * but the execute cmd has not been sent yet.
					 * We set the state DN_CONNECTION_STATE_QUERY to read ReadyForQuery responses.
					 */
					else if (IsConnectionStateIdle(handle))
						UpdateConnectionState(handle, DN_CONNECTION_STATE_QUERY);

#ifdef _PG_REGRESS_
					ereport(LOG,
							(errcode(ERRCODE_INTERNAL_ERROR),
							 errmsg("Succeed to sync msg to node:%s pid:%d when abort",
									handle->nodename,
									handle->backend_pid)));
#endif
				}
			}
			else
			{
				if (handle->needSync)
				{
					ereport(LOG,
							(errcode(ERRCODE_INTERNAL_ERROR),
							 errmsg("Invalid node:%s pid:%d needSync flag",
									handle->nodename,
									handle->backend_pid)));
				}
#ifdef _PG_REGRESS_
				ereport(LOG,
						(errcode(ERRCODE_INTERNAL_ERROR),
						 errmsg("PreAbort_Remote node:%s pid:%d status:%d no need clean.",
								handle->nodename,
								handle->backend_pid,
								GetConnectionState(handle))));
#endif
			}
		}
		else
		{
			elog(LOG, "PreAbort_Remote dn node %s pid %d, invalid socket %d!",
				handle->nodename, handle->backend_pid, handle->sock);
		}
	}

	if (cancel_co_count || cancel_dn_count)
	{
		/*
		 * Cancel running queries on the datanodes and the coordinators.
		 */
		bool cancel_ret = PoolManagerCancelQuery(cancel_dn_count,
												 cancel_dn_list,
												 cancel_dn_pid_list,
												 cancel_co_count,
												 cancel_co_list,
												 SIGNAL_SIGINT);
		if (!cancel_ret)
		{
			elog(LOG, "PreAbort_Remote cancel query failed");
		}
	}

	/*
	 * Now read and discard any data from the connections found "dirty"
	 */
	if (node_count > 0)
	{
		ResponseCombiner combiner;

		InitResponseCombiner(&combiner, node_count, COMBINE_TYPE_NONE);
		combiner.extended_query = clean_nodes[0]->state.in_extended_query;
		combiner.connections = clean_nodes;
		combiner.conn_count = node_count;
		combiner.request_type = REQUEST_TYPE_ERROR;
		combiner.is_abort = true;

		pgxc_connections_cleanup(&combiner, true);

		/* prevent pfree'ing local variable */
		combiner.connections = NULL;

		CloseCombiner(&combiner);
	}

	pgxc_sync_connections(current_handles, false);

	if (clean_nodes)
	{
		pfree(clean_nodes);
		clean_nodes = NULL;
		node_count = 0;
	}

	pgxc_node_remote_abort(txn_type, need_release_handle);

	if (GetNonLeaderCNHasSetResgroup())
	{
		CNSendResGroupReq(find_leader_cn());
	}

	/*
	 * Drop the connections to ensure aborts are handled properly.
	 *
	 * XXX We should really be consulting PersistentConnections parameter and
	 * keep the connections if its set. But as a short term measure, to address
	 * certain issues for aborted transactions, we drop the connections.
	 * Revisit and fix the issue
	 */
	elog(DEBUG5, "temp_object_included %d", temp_object_included);
	/* cleanup and release handles is already done in pgxc_node_remote_abort */
	pfree_pgxc_all_handles(current_handles);

	if (txn_type == TXN_TYPE_RollbackTxn)
		pgxc_node_clean_all_handles_distribute_msg();
	if (log_gtm_stats)
		ShowUsageCommon("PreAbort_Remote", &start_r, &start_t);

	return true;
}


/*
 * Invoked when local transaction is about to be prepared.
 * If invoked on a Datanode just commit transaction on remote connections,
 * since secondary sessions are read only and never need to be prepared.
 * Otherwise run PREPARE on remote connections, where writable commands were
 * sent (connections marked as not read-only).
 * If that is explicit PREPARE (issued by client) notify GTM.
 * In case of implicit PREPARE not involving local node (ex. caused by
 * INSERT, UPDATE or DELETE) commit prepared transaction immediately.
 * Return list of node names where transaction was actually prepared, include
 * the name of the local node if localNode is true.
 */
char *
PrePrepare_Remote(char *prepareGID, bool localNode, bool implicit)
{
	/* Always include local node if running explicit prepare */
#ifdef __OPENTENBASE__
	int    ret = 0;
#endif
	char *nodestring;
	struct rusage		start_r;
	struct timeval		start_t;

	if (log_gtm_stats)
		ResetUsageCommon(&start_r, &start_t);

	/*
	 * Primary session is doing 2PC, just commit secondary processes and exit
	 */
	if (IS_PGXC_DATANODE)
	{
		pgxc_node_remote_commit(TXN_TYPE_CommitTxn, true);
		return NULL;
	}
	if (enable_distri_print)
	{
		elog(LOG, "2PC commit PrePrepare_Remote xid %d", GetTopTransactionIdIfAny());
	}
	nodestring = pgxc_node_remote_prepare(prepareGID,
												!implicit || localNode,
												implicit);
#ifdef __TWO_PHASE_TESTS__
	twophase_in = IN_OTHER;
#endif
	if (!nodestring)
	{
		elog(ERROR, "Remote Prepare Transaction gid:%s Failed", prepareGID);
	}
#ifdef __TWO_PHASE_TRANS__
	if (nodestring && (!localNode || !IsXidImplicit(prepareGID)))
	{
		g_twophase_state.state = TWO_PHASE_PREPARED;
	}
#endif

	if (!implicit && IS_PGXC_LOCAL_COORDINATOR)
	{
		/* Save the node list and gid on GTM. */
		ret = StartPreparedTranGTM(GetTopGlobalTransactionId(), prepareGID, nodestring);
#ifdef __TWO_PHASE_TESTS__
		if (PART_PREPARE_PREPARE_GTM == twophase_exception_case)
		{
			complish = true;
			elog(ERROR, "ALL_PREPARE_PREPARE_GTM is running");
		}
#endif
#ifdef __OPENTENBASE__
		if (ret)
		{
			elog(ERROR, "Prepare Transaction gid:%s failed, "
				"gid already exist or GTM connection error.", prepareGID);
		}
#endif
	}

	if (enable_distri_print)
	{
		elog(LOG, "commit phase xid %d implicit %d local node %d prepareGID %s",
							GetTopTransactionIdIfAny(), implicit, localNode, prepareGID);
	}
	/*
	 * If no need to commit on local node go ahead and commit prepared
	 * transaction right away.
	 */
	if (implicit && !localNode && nodestring)
	{
#ifdef __TWO_PHASE_TRANS__
		/*
		 * if start node not participate, still record 2pc before  pgxc_node_remote_finish,
		 * and it will be flushed and sync to slave node when record commit_timestamp
		 */
		record_2pc_involved_nodes_xid(prepareGID, PGXCNodeName, g_twophase_state.start_xid,
			g_twophase_state.participants, g_twophase_state.start_xid, NULL,
			g_twophase_state.database, g_twophase_state.user);
#endif
		pgxc_node_remote_finish(prepareGID, true, nodestring,
								GetAuxilliaryTransactionId(),
								GetTopGlobalTransactionId());
		pfree(nodestring);
		nodestring = NULL;
	}

	if (log_gtm_stats)
		ShowUsageCommon("PrePrepare_Remote", &start_r, &start_t);

	return nodestring;
}

/*
 * Returns true if 2PC is required for consistent commit: if there was write
 * activity on two or more nodes within current transaction.
 */
bool
IsTwoPhaseCommitRequired(bool localWrite)
{
	PGXCNodeAllHandles *handles = NULL;
	bool                found = localWrite;
	int                 i = 0;

	/* Never run 2PC on Datanode-to-Datanode connection */
	if (IS_PGXC_DATANODE)
		return false;

	if (enable_2pc_optimization)
		return false;

	if (MyXactFlags & XACT_FLAGS_ACCESSEDTEMPREL)
	{
		elog(DEBUG1, "Transaction accessed temporary objects - "
		             "2PC will not be used and that can lead to data inconsistencies "
		             "in case of failures");
		return false;
	}

	/*
	 * If no XID assigned, no need to run 2PC since neither coordinator nor any
	 * remote nodes did write operation
	 */
	if (!TransactionIdIsValid(GetTopTransactionIdIfAny()))
		return false;

	report_sock_fatal_handles();

	/* get current transaction handles that we register when pgxc_node_begin */
	handles = get_current_txn_handles(NULL);
	for (i = 0; i < handles->dn_conn_count; i++)
	{
		PGXCNodeHandle *conn = handles->datanode_handles[i];

#ifdef __OPENTENBASE__
		elog(DEBUG5, "IsTwoPhaseCommitRequired, conn->nodename=%s, conn->sock=%d, conn->read_only=%d, conn->state.transaction_state=%c",
			conn->nodename, conn->sock, conn->read_only, conn->state.transaction_state);
#endif
		if (conn->sock == NO_SOCKET)
		{
			elog(ERROR,
				 "IsTwoPhaseCommitRequired, remote node %s's connection "
				 "handle is invalid, backend_pid: %d",
				 conn->nodename, conn->backend_pid);
		}
		else if (!conn->read_only && IsTxnStateInprogress(conn))
		{
			if (found)
			{
				pfree_pgxc_all_handles(handles);
				return true; /* second found */
			}
			else
			{
				found = true; /* first found */
			}
		}
		else if (IsTxnStateError(conn))
		{
			elog(ERROR,
				 "IsTwoPhaseCommitRequired, remote node %s is in error state, "
				 "backend_pid: %d",
				 conn->nodename,
				 conn->backend_pid);
		}
	}
	for (i = 0; i < handles->co_conn_count; i++)
	{
		PGXCNodeHandle *conn = handles->coord_handles[i];

#ifdef __OPENTENBASE__
		elog(DEBUG5, "IsTwoPhaseCommitRequired, conn->nodename=%s, conn->sock=%d, conn->read_only=%d, conn->state.transaction_state=%c",
			conn->nodename, conn->sock, conn->read_only, conn->state.transaction_state);
#endif
		if (conn->sock == NO_SOCKET)
		{
			elog(ERROR,
				 "IsTwoPhaseCommitRequired, remote node %s's connection "
				 "handle is invalid, backend_pid: %d",
				 conn->nodename, conn->backend_pid);
		}
		else if (!conn->read_only && IsTxnStateInprogress(conn))
		{
			if (found)
			{
				pfree_pgxc_all_handles(handles);
				return true; /* second found */
			}
			else
			{
				found = true; /* first found */
			}
		}
		else if (IsTxnStateError(conn))
		{
			elog(ERROR,
				 "IsTwoPhaseCommitRequired, remote node %s is in error state, "
				 "backend_pid: %d",
				 conn->nodename, conn->backend_pid);
		}
	}
	pfree_pgxc_all_handles(handles);

#ifdef __OPENTENBASE__
	elog(DEBUG5, "IsTwoPhaseCommitRequired return false");
#endif

	return false;
}


/* An additional phase for explicitly prepared transactions */

static bool
pgxc_node_remote_prefinish(char *prepareGID, char *nodestring)
{
	PGXCNodeHandle	   *connections[OPENTENBASE_MAX_COORDINATOR_NUMBER + OPENTENBASE_MAX_DATANODE_NUMBER];
	int					conn_count = 0;
	ResponseCombiner	combiner;
	PGXCNodeAllHandles *pgxc_handles;
	char			   *nodename;
	List			   *nodelist = NIL;
	List			   *coordlist = NIL;
	int					i;
	GlobalTimestamp    global_committs;
	/*
	 * Now based on the nodestring, run COMMIT/ROLLBACK PREPARED command on the
	 * remote nodes and also finish the transaction locally is required
	 */
	elog(DEBUG8, "pgxc_node_remote_prefinish nodestring %s gid %s", nodestring, prepareGID);
#ifdef __SUPPORT_DISTRIBUTED_TRANSACTION__
	elog(DEBUG8, "pgxc_node_remote_prefinish start GTM timestamp nodestring %s gid %s", nodestring, prepareGID);
	global_committs = GetGlobalTimestampGTM();
#ifdef __TWO_PHASE_TESTS__
	if (ALL_PREPARE_REMOTE_PREFINISH == twophase_exception_case)
	{
		global_committs = 0;
		complish = true;
	}
#endif
	if (!GlobalTimestampIsValid(global_committs))
	{
		ereport(ERROR,
			(errcode(ERRCODE_INTERNAL_ERROR),
			 errmsg("failed to get global timestamp for prefinish command gid %s",
					prepareGID)));
	}
	if (enable_distri_print)
	{
		elog(LOG,
			 "prefinish phase get global timestamp gid %s, time " INT64_FORMAT,
			 prepareGID, global_committs);
	}
	SetGlobalPrepareTimestamp(global_committs);/* Save for local commit */
	EndExplicitGlobalPrepare(prepareGID);
#endif


	nodename = strtok(nodestring, ",");
	while (nodename != NULL)
	{
		int		nodeIndex;
		char	nodetype;

		/* Get node type and index */
		nodetype = PGXC_NODE_NONE;
		nodeIndex = PGXCNodeGetNodeIdFromName(nodename, &nodetype);
		if (nodetype == PGXC_NODE_NONE)
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_OBJECT),
					 errmsg("PGXC Node %s: object not defined",
							nodename)));

		/* Check if node is requested is the self-node or not */
		if (nodetype == PGXC_NODE_COORDINATOR)
		{
			if (nodeIndex != PGXCNodeId - 1)
				coordlist = lappend_int(coordlist, nodeIndex);
		}
		else
			nodelist = lappend_int(nodelist, nodeIndex);

		nodename = strtok(NULL, ",");
	}

	if (nodelist == NIL && coordlist == NIL)
		return false;

	pgxc_handles = get_handles(nodelist, coordlist, nodelist == NIL,
							   true, FIRST_LEVEL, InvalidFid, true, false);

	for (i = 0; i < pgxc_handles->dn_conn_count; i++)
	{
		PGXCNodeHandle *conn = pgxc_handles->datanode_handles[i];

		if (pgxc_node_send_gid(conn, prepareGID))
		{
			ereport(ERROR,
					(errcode(ERRCODE_CONNECTION_EXCEPTION),
					 errmsg("failed to send GID to the node %u",
							conn->nodeoid)));
		}

		if (pgxc_node_send_prefinish_timestamp(conn, global_committs))
		{
			ereport(ERROR,
					(errcode(ERRCODE_CONNECTION_EXCEPTION),
					 errmsg("failed to send prefinish timestamp to the node %u",
							conn->nodeoid)));
		}
		if (SendRequest(conn))
		{
			/*
			 * Do not bother with clean up, just bomb out. The error handler
			 * will invoke RollbackTransaction which will do the work.
			 */
			ereport(ERROR,
					(errcode(ERRCODE_CONNECTION_EXCEPTION),
					 errmsg("pgxc_node_remote_prefinish failed to send "
							"prefinish command to the node %u for %s",
							conn->nodeoid, strerror(errno))));
		}
		else
		{
			/* Read responses from these */
			connections[conn_count++] = conn;
			elog(DEBUG8, "prefinish add connection");
		}

	}

	for (i = 0; i < pgxc_handles->co_conn_count; i++)
	{
		PGXCNodeHandle *conn = pgxc_handles->coord_handles[i];

		if (pgxc_node_send_gid(conn, prepareGID))
		{
			ereport(ERROR,
					(errcode(ERRCODE_CONNECTION_EXCEPTION),
					 errmsg("failed to send GID to the node %u",
							conn->nodeoid)));
		}

		if (pgxc_node_send_prefinish_timestamp(conn, global_committs))
		{
			ereport(ERROR,
					(errcode(ERRCODE_CONNECTION_EXCEPTION),
					 errmsg("failed to send prefinish timestamp to the node %u",
							conn->nodeoid)));
		}
		if (SendRequest(conn))
		{
			/*
			 * Do not bother with clean up, just bomb out. The error handler
			 * will invoke RollbackTransaction which will do the work.
			 */
			ereport(ERROR,
					(errcode(ERRCODE_CONNECTION_EXCEPTION),
					 errmsg("pgxc_node_remote_prefinish failed to send "
							"prefinish command to the node %u for %s",
							conn->nodeoid, strerror(errno))));
		}
		else
		{
			/* Read responses from these */
			connections[conn_count++] = conn;
			elog(DEBUG8, "prefinish add connection");
		}
	}

	if (conn_count)
	{
		InitResponseCombiner(&combiner, conn_count, COMBINE_TYPE_NONE);
		/* Receive responses */
		if (pgxc_node_receive_responses(conn_count, connections, NULL, &combiner))
		{
			if (combiner.errorMessage)
				pgxc_node_report_error(&combiner);
			else
				ereport(ERROR,
						(errcode(ERRCODE_INTERNAL_ERROR),
						 errmsg("Failed to COMMIT the transaction on one or more nodes")));
		}
		else
			CloseCombiner(&combiner);
	}

	pfree_pgxc_all_handles(pgxc_handles);

	return true;
}

/*
 * Execute COMMIT/ABORT PREPARED issued by the remote client on remote nodes.
 * Contacts GTM for the list of involved nodes and for work complete
 * notification. Returns true if prepared transaction on local node needs to be
 * finished too.
 */
bool
FinishRemotePreparedTransaction(char *prepareGID, bool commit)
{
	char				   *nodestring;
	char 				   *savenodestring PG_USED_FOR_ASSERTS_ONLY;
	GlobalTransactionId		gxid, prepare_gxid;
	bool					prepared_local = false;

#ifdef __TWO_PHASE_TRANS__
	/*
	 * Since g_twophase_state is cleared after prepare phase,
	 * g_twophase_state shoud be assigned here
	 */
	strncpy(g_twophase_state.gid, prepareGID, GIDSIZE);
	strncpy(g_twophase_state.start_node_name, PGXCNodeName, NAMEDATALEN);
	g_twophase_state.state = TWO_PHASE_PREPARED;
	g_twophase_state.is_start_node = true;
#endif

	/*
	 * Get the list of nodes involved in this transaction.
	 *
	 * This function returns the GXID of the prepared transaction. It also
	 * returns a fresh GXID which can be used for running COMMIT PREPARED
	 * commands on the remote nodes. Both these GXIDs can then be either
	 * committed or aborted together.
	 *
	 * XXX While I understand that we get the prepared and a new GXID with a
	 * single call, it doesn't look nicer and create confusion. We should
	 * probably split them into two parts. This is used only for explicit 2PC
	 * which should not be very common in XC
	 *
	 * In xc_maintenance_mode mode, we don't fail if the GTM does not have
	 * knowledge about the prepared transaction. That may happen for various
	 * reasons such that an earlier attempt cleaned up it from GTM or GTM was
	 * restarted in between. The xc_maintenance_mode is a kludge to come out of
	 * such situations. So it seems alright to not be too strict about the
	 * state
	 */
#ifdef __TWO_PHASE_TESTS__
	if (ALL_PREPARE_FINISH_REMOTE_PREPARED == twophase_exception_case)
	{
		complish = true;
		elog(ERROR, "ALL_PREPARE_FINISH_REMOTE_PREPARED complish");
	}
#endif
	if ((GetGIDDataGTM(prepareGID, &gxid, &prepare_gxid, &nodestring) < 0) &&
		!xc_maintenance_mode && !g_twophase_state.is_readonly)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_TRANSACTION_STATE),
				 errmsg("prepared transaction with identifier \"%s\" does not exist",
						prepareGID),
				 errhint("check GTM connection")));

	/*
	 * Please note that with xc_maintenance_mode = on, COMMIT/ROLLBACK PREPARED will not
	 * propagate to remote nodes. Only GTM status is cleaned up.
	 */
#ifdef __USE_GLOBAL_SNAPSHOT__
	if (xc_maintenance_mode)
	{
		if (commit)
		{
			pgxc_node_remote_commit(TXN_TYPE_CommitTxn, true);
			CommitPreparedTranGTM(prepare_gxid, gxid, 0, NULL);
		}
		else
		{
			pgxc_node_remote_abort(TXN_TYPE_RollbackTxn, true);
			RollbackTranGTM(prepare_gxid);
			RollbackTranGTM(gxid);
		}
		return false;
	}
#endif

#ifdef __TWO_PHASE_TRANS__
	/*
	 * not allowed user commit residual transaction in xc_maintenance_mode,
	 * since we need commit them in unified timestamp
	 */
	if (xc_maintenance_mode)
	{
		elog(ERROR, "can not commit transaction '%s' in xc_maintainence_mode", prepareGID);
	}

	if (nodestring)
	{
		strcpy(g_twophase_state.participants, nodestring);
	}
#endif

#ifdef __SUPPORT_DISTRIBUTED_TRANSACTION__
	savenodestring = MemoryContextStrdup(TopMemoryContext, nodestring);
	pgxc_node_remote_prefinish(prepareGID, savenodestring);
#endif
	prepared_local = pgxc_node_remote_finish(prepareGID, commit, nodestring,
											 gxid, prepare_gxid);
	free(nodestring);
#ifdef __USE_GLOBAL_SNAPSHOT__

	if (commit)
	{
		/*
		 * XXX For explicit 2PC, there will be enough delay for any
		 * waited-committed transactions to send a final COMMIT message to the
		 * GTM.
		 */
		CommitPreparedTranGTM(prepare_gxid, gxid, 0, NULL);
	}
	else
	{
		RollbackTranGTM(prepare_gxid);
		RollbackTranGTM(gxid);
	}
#endif

#ifdef __OPENTENBASE__
	FinishGIDGTM(prepareGID);
#endif

	if (enable_distri_print)
	{
		if (current_transaction_handles)
			elog(LOG, "reset_transaction_handles dn_conn_count %d co_conn_count %d", current_transaction_handles->dn_conn_count, current_transaction_handles->co_conn_count);
		else
			elog(LOG, "reset_transaction_handles dn_conn_count ");
	}

	reset_transaction_handles();
	return prepared_local;
}

/*
 * Complete previously prepared transactions on remote nodes.
 * Release remote connection after completion.
 */
static bool
pgxc_node_remote_finish(char *prepareGID, bool commit,
						char *nodestring, GlobalTransactionId gxid,
						GlobalTransactionId prepare_gxid)
{
	PGXCNodeHandle **connections = NULL;
	int					conn_count = 0;
	ResponseCombiner	combiner;
	PGXCNodeAllHandles *pgxc_handles;
	bool				prepared_local = false;
	char			   *nodename;
	List			   *nodelist = NIL;
	List			   *coordlist = NIL;
	int					i;
#ifdef __SUPPORT_DISTRIBUTED_TRANSACTION__
	GlobalTimestamp    global_committs;
	TransactionStmtKind transKind_readonly;
	TransactionStmtKind transKind;

#endif
#ifdef __TWO_PHASE_TRANS__
	/*
	 *any send error in twophase trans will set all_conn_healthy to false
	 *all transaction call pgxc_node_remote_finish is twophase trans
	 *only called by starter: set g_twophase_state just before send msg to remote nodes
	 */
	bool               all_conn_healthy = true;
	int                twophase_index = 0;
#endif
#ifdef __TWO_PHASE_TESTS__
	if (ALL_PREPARE_REMOTE_FINISH == twophase_exception_case)
	{
		complish = true;
		elog(ERROR, "stop transaction '%s' in ALL_PREPARE_REMOTE_FINISH", prepareGID);
	}

	if (PART_ABORT_REMOTE_FINISH == twophase_exception_case)
	{
		complish = false;
		elog(ERROR, "PART_ABORT_REMOTE_FINISH complete");
	}
	if (PART_COMMIT_SEND_TIMESTAMP == twophase_exception_case ||
		PART_COMMIT_SEND_QUERY == twophase_exception_case ||
		PART_COMMIT_RESPONSE_ERROR == twophase_exception_case)
	{
		twophase_in = IN_REMOTE_FINISH;
	}
#endif

	connections = (PGXCNodeHandle**)palloc(sizeof(PGXCNodeHandle*) * (OPENTENBASE_MAX_DATANODE_NUMBER + OPENTENBASE_MAX_COORDINATOR_NUMBER));

	/*
	 * Now based on the nodestring, run COMMIT/ROLLBACK PREPARED command on the
	 * remote nodes and also finish the transaction locally is required
	 */
	elog(DEBUG8, "pgxc_node_remote_finish nodestring %s gid %s", nodestring, prepareGID);
#ifdef __SUPPORT_DISTRIBUTED_TRANSACTION__
	elog(DEBUG8, "pgxc_node_remote_finish start GTM timestamp nodestring %s gid %s", nodestring, prepareGID);
	if (delay_before_acquire_committs)
	{
		pg_usleep(delay_before_acquire_committs);
	}

	if (commit)
		global_committs = GetGlobalTimestampGTMForCommit();
	else
		global_committs = GetGlobalTimestampGTM();

	if (!GlobalTimestampIsValid(global_committs))
	{
		ereport(ERROR,
		(errcode(ERRCODE_INTERNAL_ERROR),
		 errmsg("failed to get global timestamp for %s PREPARED command",
				commit ? "COMMIT" : "ROLLBACK")));
	}
	if (enable_distri_print)
	{
		elog(LOG, "commit phase get global commit timestamp gid %s, time " INT64_FORMAT, prepareGID, global_committs);
	}

	if (delay_after_acquire_committs)
	{
		pg_usleep(delay_after_acquire_committs);
	}
	SetGlobalCommitTimestamp(global_committs);/* Save for local commit */
#endif

	nodename = strtok(nodestring, ",");
	while (nodename != NULL)
	{
		int		nodeIndex;
		char	nodetype;

		/* Get node type and index */
		nodetype = PGXC_NODE_NONE;
		nodeIndex = PGXCNodeGetNodeIdFromName(nodename, &nodetype);
		if (nodetype == PGXC_NODE_NONE)
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_OBJECT),
					 errmsg("PGXC Node %s: object not defined",
							nodename)));

		/* Check if node is requested is the self-node or not */
		if (nodetype == PGXC_NODE_COORDINATOR)
		{
			if (nodeIndex == PGXCNodeId - 1)
				prepared_local = true;
			else
				coordlist = lappend_int(coordlist, nodeIndex);
		}
		else
			nodelist = lappend_int(nodelist, nodeIndex);

		nodename = strtok(NULL, ",");
	}

	if (nodelist == NIL && coordlist == NIL)
		return prepared_local;

	/* make sure only use write handles, and only one write handle for each node*/
	pgxc_handles = get_handles(nodelist, coordlist, nodelist == NIL,
							   true, FIRST_LEVEL, InvalidFid, true, false);

	SetLocalTwoPhaseStateHandles(pgxc_handles);

	if (enable_distri_debug)
	{
		int node_count = pgxc_handles->dn_conn_count + pgxc_handles->co_conn_count;

		if (prepared_local)
		{
			node_count++;
		}
		if (enable_distri_print)
		{
			elog(LOG, "Commit transaction prepareGID %s nodestring %s top xid %u auxid %u "INT64_FORMAT" "INT64_FORMAT,
				prepareGID, nodestring, GetAuxilliaryTransactionId(), GetTopGlobalTransactionId(),
				GetGlobalPrepareTimestamp(), GetGlobalCommitTimestamp());
		}
		LogCommitTranGTM(GetTopGlobalTransactionId(), prepareGID,
							 nodestring,
							 node_count,
							 true,
							 true,
							 GetGlobalPrepareTimestamp(),
							 GetGlobalCommitTimestamp());
		is_distri_report = true;
	}

#ifdef __TWO_PHASE_TRANS__
	g_twophase_state.state = commit ? TWO_PHASE_COMMITTING : TWO_PHASE_ABORTTING;
	/*
	 * transaction start node record 2pc file just before
	 * send query to remote participants
	 */
	if (IS_PGXC_LOCAL_COORDINATOR && commit)
	{
		/*
		 * record commit timestamp in 2pc file for start node
		 */
		record_2pc_commit_timestamp(prepareGID, global_committs);
	}
#endif

	if (commit)
	{
		transKind_readonly = TRANS_STMT_COMMIT;
		transKind = TRANS_STMT_COMMIT_PREPARED;
	}
	else
	{
		transKind_readonly = TRANS_STMT_ROLLBACK;
		transKind = TRANS_STMT_ROLLBACK_PREPARED;
	}

	for (i = 0; i < pgxc_handles->dn_conn_count; i++)
	{
		PGXCNodeHandle *conn = pgxc_handles->datanode_handles[i];

		if (conn->read_only)
		{
#ifdef __USE_GLOBAL_SNAPSHOT__
			if (pgxc_node_send_gxid(conn, gxid))
			{
				elog(LOG, "failed to send gxid to a readonly node %s %d", 
					 conn->nodename, conn->backend_pid);
				continue;
			}
#endif
			if (pgxc_node_send_global_timestamp(conn, global_committs))
			{
				elog(LOG, "failed to send global tempstammp to a readonly node %s %d", 
					 conn->nodename, conn->backend_pid);
				continue;
			}
			if (pgxc_node_send_tran_command(conn, transKind_readonly, prepareGID))
			{
				elog(LOG, "failed to send transaction command %d to  readonly node: %s %d", 
					 transKind_readonly, conn->nodename, conn->backend_pid);
				continue;
			}
		}
		else
		{
			twophase_index = twophase_add_participant(conn, i, g_twophase_state.state);
			Assert(twophase_index >= 0);
#ifdef __USE_GLOBAL_SNAPSHOT__
			if (pgxc_node_send_gxid(conn, gxid))
			{
#ifdef __TWO_PHASE_TRANS__
				g_twophase_state.datanode_state[twophase_index].conn_state =
					TWO_PHASE_SEND_GXID_ERROR;
				g_twophase_state.datanode_state[twophase_index] =
					(commit == true) ? TWO_PHASE_COMMIT_ERROR : TWO_PHASE_ABORT_ERROR;
				all_conn_healthy = false;
				g_twophase_state.datanode_index++;
				CheckLocalTwoPhaseState();
				continue;
#endif
			}
#endif

#ifdef __SUPPORT_DISTRIBUTED_TRANSACTION__
			if (pgxc_node_send_global_timestamp(conn, global_committs))
			{
#ifdef __TWO_PHASE_TRANS__
				g_twophase_state.datanode_state[twophase_index].conn_state =
					TWO_PHASE_SEND_TIMESTAMP_ERROR;
				g_twophase_state.datanode_state[twophase_index].state =
					(commit == true) ? TWO_PHASE_COMMIT_ERROR : TWO_PHASE_ABORT_ERROR;
				all_conn_healthy = false;
				g_twophase_state.datanode_index++;
				CheckLocalTwoPhaseState();
				continue;
#endif
			}
#endif

			if (pgxc_node_send_tran_command(conn, transKind, prepareGID))
			{
#ifdef __TWO_PHASE_TRANS__
				/* record conn state :send gxid fail */
				g_twophase_state.datanode_state[twophase_index].conn_state =
					TWO_PHASE_SEND_QUERY_ERROR;
				g_twophase_state.datanode_state[twophase_index].state =
					(commit == true) ? TWO_PHASE_COMMIT_ERROR : TWO_PHASE_ABORT_ERROR;
				all_conn_healthy = false;
				g_twophase_state.datanode_index++;
				CheckLocalTwoPhaseState();
				continue;
#endif
			}
			else
			{
				/* Read responses from these */
				connections[conn_count++] = conn;

				if (!conn->read_only)
					twophase_update_state(conn);
			}
		}
	}

	for (i = 0; i < pgxc_handles->co_conn_count; i++)
	{
		PGXCNodeHandle *conn = pgxc_handles->coord_handles[i];
		twophase_index = twophase_add_participant(conn, i, g_twophase_state.state);
		Assert(twophase_index >= 0);
#ifdef __USE_GLOBAL_SNAPSHOT__
		if (pgxc_node_send_gxid(conn, gxid))
		{
#ifdef __TWO_PHASE_TRANS__
			g_twophase_state.coord_state[twophase_index].conn_state =
				TWO_PHASE_SEND_GXID_ERROR;
			g_twophase_state.coord_state[twophase_index] =
				(commit == true) ? TWO_PHASE_COMMIT_ERROR : TWO_PHASE_ABORT_ERROR;
			all_conn_healthy = false;
			g_twophase_state.coord_index++;
			continue;
#endif
		}
#endif
#ifdef __SUPPORT_DISTRIBUTED_TRANSACTION__
		if (pgxc_node_send_global_timestamp(conn, global_committs))
		{
#ifdef __TWO_PHASE_TRANS__
			g_twophase_state.coord_state[twophase_index].conn_state =
				TWO_PHASE_SEND_TIMESTAMP_ERROR;
			g_twophase_state.coord_state[twophase_index].state =
				(commit == true) ? TWO_PHASE_COMMIT_ERROR : TWO_PHASE_ABORT_ERROR;
			all_conn_healthy = false;
			g_twophase_state.coord_index++;
			CheckLocalTwoPhaseState();
			continue;
#endif
		}
#endif

		if (pgxc_node_send_tran_command(conn, transKind, prepareGID))
		{
#ifdef __TWO_PHASE_TRANS__
			g_twophase_state.coord_state[twophase_index].conn_state =
				TWO_PHASE_SEND_QUERY_ERROR;
			g_twophase_state.coord_state[twophase_index].state =
				(commit == true) ? TWO_PHASE_COMMIT_ERROR : TWO_PHASE_ABORT_ERROR;
			all_conn_healthy = false;
			g_twophase_state.coord_index++;
			CheckLocalTwoPhaseState();
			continue;
#endif
		}
		else
		{
			/* Read responses from these */
			connections[conn_count++] = conn;
			if (!conn->read_only)
				twophase_update_state(conn);
		}
	}

	if (conn_count)
	{
		InitResponseCombiner(&combiner, conn_count, COMBINE_TYPE_NONE);
#ifdef __TWO_PHASE_TRANS__
		g_twophase_state.response_operation =
			(commit == true) ? REMOTE_FINISH_COMMIT : REMOTE_FINISH_ABORT;
#endif
		/* Receive responses */
		if (pgxc_node_receive_responses(conn_count, connections, NULL, &combiner) ||
				!validate_combiner(&combiner))
		{
			if (combiner.errorMessage)
				pgxc_node_report_error(&combiner);
			else
				ereport(ERROR,
						(errcode(ERRCODE_INTERNAL_ERROR),
						 errmsg("Failed to COMMIT the transaction on one or more nodes")));
		}
		else
			CloseCombiner(&combiner);
	}

#ifdef __TWO_PHASE_TRANS__
	if (all_conn_healthy == false)
	{
		if (commit)
		{
			elog(ERROR, "Failed to send COMMIT PREPARED '%s' to one or more nodes", prepareGID);
		}
		else
		{
			elog(ERROR, "Failed to send ROLLBACK PREPARED '%s' to one or more nodes", prepareGID);
		}
	}
#endif

	if (!temp_object_included && PersistentConnectionsTimeout == 0)
		pgxc_node_cleanup_and_release_handles(false, false);
	pfree_pgxc_all_handles(pgxc_handles);

	if (enable_distri_print)
	{
		if (current_transaction_handles)
			elog(LOG, "reset_transaction_handles dn_conn_count %d co_conn_count %d", current_transaction_handles->dn_conn_count, current_transaction_handles->co_conn_count);
		else
			elog(LOG, "reset_transaction_handles dn_conn_count ");
	}

	reset_transaction_handles();

#ifdef __TWO_PHASE_TRANS__
	if (prepared_local)
	{
		g_twophase_state.state = (commit == true) ? TWO_PHASE_COMMIT_END : TWO_PHASE_ABORT_END;
	}
	else
	{
		g_twophase_state.state = (commit == true) ? TWO_PHASE_COMMITTED : TWO_PHASE_ABORTTED;
		ClearLocalTwoPhaseState();
	}
#endif

	if (connections)
	{
		pfree(connections);
		connections = NULL;
	}

	return prepared_local;
}

static int
GetValidEnParamExprNum(ExecNodes *exec_nodes)
{
	int i = 0;
	int result = 0;

	if (!exec_nodes->nExprs || !exec_nodes->en_param_expr)
		return result;

	for (i = 0; i < exec_nodes->nExprs; i++)
	{
		if (!exec_nodes->en_param_expr[i])
			continue;

		result++;
	}
	return result;
}

/*****************************************************************************
 *
 * Simplified versions of ExecInitRemoteQuery, ExecRemoteQuery and
 * ExecEndRemoteQuery: in XCP they are only used to execute simple queries.
 *
 *****************************************************************************/
RemoteQueryState *
ExecInitRemoteQuery(RemoteQuery *node, EState *estate, int eflags)
{
	RemoteQueryState   *remotestate;
	ResponseCombiner   *combiner;
	RemoteQueryExecType exec_type = node->exec_type;
	ExecDirectType		exec_direct_type = node->exec_direct_type;
	bool 				exec_direct_dn_allow = node->exec_direct_dn_allow;
	bool				expr_context_from_estate = false;
	ExecNodes          *exec_nodes = node->exec_nodes;
	if (exec_type == EXEC_ON_DATANODES &&
		exec_direct_dn_allow == false &&
		exec_direct_type != EXEC_DIRECT_NONE &&
		exec_direct_type != EXEC_DIRECT_SELECT && !IsInplaceUpgrade)
	{
		elog(ERROR, "EXECUTE DIRECT non select command cannot be executed on Datanode");
	}

	remotestate = makeNode(RemoteQueryState);
	remotestate->eflags = eflags;
	combiner = &remotestate->combiner;
	combiner->combine_type = node->combine_type;
	combiner->context = CurrentMemoryContext;
	combiner->resowner = CurrentResourceOwner;
	combiner->ss.ps.plan = (Plan *) node;
	combiner->ss.ps.state = estate;
	combiner->ss.ps.ExecProcNode = ExecRemoteQuery;
	combiner->request_type = REQUEST_TYPE_QUERY;

	if (node->exec_nodes && node->exec_nodes->nExprs > 0 && node->exec_nodes->dis_exprs[0])
	{
		Expr *expr = node->exec_nodes->dis_exprs[0];

		if (IsA(expr, Var) && ((Var *) expr)->vartype == TIDOID)
		{
			/* Special case if expression does not need to be evaluated */
		}
		else
		{
			/* prepare expression evaluation */
			ExecAssignExprContext(estate, &combiner->ss.ps);
			expr_context_from_estate = true;
		}
	}

	/* We need expression context to evaluate */
	if (node->exec_nodes && node->exec_nodes->en_param_expr)
	{
		MemoryContext old;
		int				i;
		int				param_idx = 0;
		int				en_param_expr_num = 0;

		Assert(estate->es_query_cxt);

		en_param_expr_num = GetValidEnParamExprNum(node->exec_nodes);

		if (estate->es_param_list_info == NULL)
		{
			old = MemoryContextSwitchTo(estate->es_query_cxt);
			estate->es_param_list_info =
				(ParamListInfo) palloc0(offsetof(ParamListInfoData, params) +
				                        en_param_expr_num * sizeof(ParamExternData));
			MemoryContextSwitchTo(old);
			estate->es_param_list_info->numParams = en_param_expr_num;
		}
		else
		{
			param_idx = estate->es_param_list_info->numParams;
			estate->es_param_list_info->numParams += en_param_expr_num;
			old = MemoryContextSwitchTo(estate->es_query_cxt);
			estate->es_param_list_info = repalloc(estate->es_param_list_info,
			                                      offsetof(ParamListInfoData, params) +
			                                      estate->es_param_list_info->numParams *
			                                      sizeof(ParamExternData));
			MemoryContextSwitchTo(old);
		}

		if (expr_context_from_estate)
			combiner->ss.ps.ps_ExprContext->ecxt_param_list_info = estate->es_param_list_info;

		for (i = 0; i < node->exec_nodes->nExprs; i++)
		{
			ParamExternData data;
			Expr        *expr;
			Oid          type;

			if (!node->exec_nodes->en_param_expr[i])
				continue;

			/*
			 * Simplify constant expressions before evaluating exec_nodes->en_expr.
			 * The same work in preprocess_targetlist.
			 */
			expr = (Expr *) eval_const_expressions(NULL,
												(Node *) exec_nodes->en_param_expr[i]);
			/* Fill in opfuncid values if missing */
			fix_opfuncids((Node *) expr);
			type = exprType((Node *) expr);

			if (eflags & EXEC_FLAG_EXPLAIN_ONLY)
			{
				data.value = (Datum) 0;
				data.isnull = true;
			}
			else
			{
				ExprState *exprState = ExecInitExpr(expr, NULL);
				data.value = ExecEvalExpr(exprState,
										combiner->ss.ps.ps_ExprContext,
										&data.isnull);
			}
			data.ptype = type;
			data.pflags = PARAM_FLAG_CONST;
			estate->es_param_list_info->params[param_idx++] = data;
		}
	}

	if (!(remotestate->eflags & (EXEC_FLAG_WITH_DESC | EXEC_FLAG_EXPLAIN_ONLY)) &&
			!combiner->merge_sort && estate->cn_penetrate_tupleDesc)
	{
		combiner->ss.ps.state->cn_penetrate_tuple = true;
		/*
		 * when there has a tupleDesc from CachedPlanSource, and cn can only
		 * transparently transmits tuples, just copy the tupleDesc as the ResultTupleDesc
		 */
		combiner->ss.ps.ps_ResultTupleDesc = CreateTupleDescCopy(estate->cn_penetrate_tupleDesc);
		ExecInitResultSlot(&combiner->ss.ps);
	}
	/* Use tuple desc from dn if EXEC_FLAG_WITH_DESC */
	else if (eflags & EXEC_FLAG_WITH_DESC)
		ExecInitResultSlot(&combiner->ss.ps);
	else
		ExecInitResultTupleSlotTL(&combiner->ss.ps);

	/*
	 * If there are parameters supplied, get them into a form to be sent to the
	 * Datanodes with bind message. We should not have had done this before.
	 */
	SetDataRowForExtParams(estate->es_param_list_info, remotestate);

	return remotestate;
}

/*
 * Execute step of PGXC plan.
 * The step specifies a command to be executed on specified nodes.
 * On first invocation connections to the data nodes are initialized and
 * command is executed. Further, as well as within subsequent invocations,
 * responses are received until step is completed or there is a tuple to emit.
 * If there is a tuple it is returned, otherwise returned NULL. The NULL result
 * from the function indicates completed step.
 * The function returns at most one tuple per invocation.
 */
TupleTableSlot *
ExecRemoteQuery(PlanState *pstate)
{
	RemoteQueryState *node = castNode(RemoteQueryState, pstate);
	ResponseCombiner *combiner = (ResponseCombiner *) node;
	RemoteQuery    *step = (RemoteQuery *) combiner->ss.ps.plan;
	TupleTableSlot *resultslot = combiner->ss.ps.ps_ResultTupleSlot;
	bool			need_free_in_combiner = false;

	if (!node->query_Done)
	{
		GlobalTransactionId gxid = InvalidGlobalTransactionId;
		Snapshot		snapshot = GetActiveSnapshot();
		PGXCNodeHandle **connections = NULL;
		bool		   *prepared;
		int				i;
		int				regular_conn_count = 0;
		int				total_conn_count = 0;
		bool			need_tran_block;
		PGXCNodeAllHandles *pgxc_connections = NULL;
		DatanodeStatement *entry = NULL;
		ExecNodes *nodes = step->exec_nodes;

		prepared = palloc0(sizeof(bool) * NumDataNodes);

		/* if prepared statement is referenced see if it is already exist */
		if (step->statement && step->exec_nodes)
		{
			entry = FetchDatanodeStatement(step->statement, true);

			if (entry != NULL && entry->number_of_nodes > 0)
			{
				if (nodes->nExprs > 0 && nodes->dis_exprs[0])
				{
					/*
					 * The execnodes of this Query need to be redistributed based on the
					 * expression, and the new parameters may result in new redistribution
					 * results. Therefore, Attempt to obtain the connections corresponding
					 * to the nodelist.
					 *
					 * Notice a valid dis_exprs means this is a generic plan and we need
					 * to evaluate nodelist with params now. (custom plan has done this
					 * during planning a FQS).
					 */
					nodes = paramGetExecNodes(nodes, pstate);
				}
				else
				{
					nodes = copyObject(nodes);
				}

				pgxc_connections = DatanodeStatementGetHandle(entry, &nodes->nodeList);
				if (pgxc_connections != NULL)
				{
					/* determine if this connection can be read only */
					bool write = nodes->accesstype != RELATION_ACCESS_READ_FQS &&
								 !step->read_only;

					memset(prepared, 1, sizeof(bool) * pgxc_connections->dn_conn_count);
					register_transaction_handles_all(pgxc_connections, !write);
				}
			}
		}

		if (pgxc_connections == NULL || list_length(nodes->nodeList) > 0)
		{
			PGXCNodeAllHandles *diff;
			/*
			 * Get connections for Datanodes only, utilities and DDLs
			 * are launched in ExecRemoteUtility
			 */
			diff = get_exec_connections(node, nodes, step->exec_type, true);
			if (entry != NULL && diff->dn_conn_count > 0)
			{
				/* a new statement needs prepare, save related connections */
				DatanodeStatementStoreHandle(entry, diff);
			}

			if (pgxc_connections == NULL)
			{
				pgxc_connections = diff;
			}
			else
			{
				pgxc_connections->datanode_handles =
					repalloc(pgxc_connections->datanode_handles,
							 sizeof(PGXCNodeHandle *) *
							 (pgxc_connections->dn_conn_count + diff->dn_conn_count));

				for (i = 0; i < diff->dn_conn_count; i++)
				{
					pgxc_connections->datanode_handles[pgxc_connections->dn_conn_count++] =
						diff->datanode_handles[i];
				}
			}
		}

		if (step->exec_type == EXEC_ON_DATANODES)
		{
			connections = pgxc_connections->datanode_handles;
			total_conn_count = regular_conn_count = pgxc_connections->dn_conn_count;
		}
		else if (step->exec_type == EXEC_ON_COORDS)
		{
			connections = pgxc_connections->coord_handles;
			total_conn_count = regular_conn_count = pgxc_connections->co_conn_count;
		}
		else if (step->exec_type == EXEC_ON_ALL_NODES || step->exec_type == EXEC_ON_CURRENT_TXN)
		{
			total_conn_count = regular_conn_count =
				pgxc_connections->dn_conn_count + pgxc_connections->co_conn_count;

			connections = palloc(mul_size(total_conn_count, sizeof(PGXCNodeHandle *)));
			memcpy(connections, pgxc_connections->datanode_handles,
				   pgxc_connections->dn_conn_count * sizeof(PGXCNodeHandle *));
			memcpy(connections + pgxc_connections->dn_conn_count, pgxc_connections->coord_handles,
				   pgxc_connections->co_conn_count * sizeof(PGXCNodeHandle *));

			need_free_in_combiner = true;
		}

#ifdef __OPENTENBASE__
		/* initialize */
		combiner->recv_node_count = regular_conn_count;
		combiner->recv_tuples	  = 0;
		combiner->recv_total_time = -1;
		combiner->recv_datarows = 0;
#endif
		combiner->node_count = regular_conn_count;

		/*
		 * Start transaction on data nodes if we are in explicit transaction
		 * or going to use extended query protocol or write to multiple nodes
		 *
		 * for mult_sql_query_containing, the special syntax '\;' connects two SQL statements
		 * that need to be executed within the same transaction block.
		 * Here, we are actively starting a new transaction.
		 */
		if (step->force_autocommit)
			need_tran_block = false;
		else
			need_tran_block = step->cursor ||
					(!step->read_only && total_conn_count > 1) ||
					exec_event_trigger_stmt ||
					(TransactionBlockStatusCode() == 'T' ) ||
					(step->remote_mt && step->remote_mt->global_index) ||
					mult_sql_query_containing;

		stat_statement();
		stat_transaction(total_conn_count);
		gxid = GetCurrentTransactionIdIfAny();
#ifdef __OPENTENBASE__
		if (regular_conn_count > 0)
		{
			combiner->connections = connections;
			combiner->conn_count = regular_conn_count;
			combiner->current_conn = 0;
		}
#endif

		if (pgxc_node_begin(regular_conn_count, connections, gxid, need_tran_block, true))
			ereport(ERROR,
					(errcode(ERRCODE_CONNECTION_EXCEPTION),
						errmsg("Could not begin transaction on connections.")));

		for (i = 0; i < regular_conn_count; i++)
		{
			/* If explicit transaction is needed gxid is already sent */
			if (!pgxc_start_command_on_connection(connections[i], node,
												  snapshot, prepared[i]))
			{
				ereport(ERROR,
						(errcode(ERRCODE_CONNECTION_EXCEPTION),
								errmsg("Failed to send command"),
								errnode(connections[i])));
			}
			connections[i]->combiner = combiner;
		}

		if (step->cursor)
		{
			combiner->cursor = step->cursor;
			combiner->cursor_count = regular_conn_count;
			combiner->cursor_connections = (PGXCNodeHandle **) palloc(regular_conn_count * sizeof(PGXCNodeHandle *));
			memcpy(combiner->cursor_connections, connections, regular_conn_count * sizeof(PGXCNodeHandle *));
		}

		combiner->connections = connections;
		combiner->conn_count = regular_conn_count;
		combiner->current_conn = 0;

		if (combiner->cursor_count)
		{
			combiner->conn_count = combiner->cursor_count;
			memcpy(connections, combiner->cursor_connections,
				   combiner->cursor_count * sizeof(PGXCNodeHandle *));
			combiner->connections = connections;
		}

		node->query_Done = true;

		if (step->sort)
		{
			SimpleSort *sort = step->sort;

			/*
			 * First message is already in the buffer
			 * Further fetch will be under tuplesort control
			 * If query does not produce rows tuplesort will not
			 * be initialized
			 */
			combiner->tuplesortstate = tuplesort_begin_merge(
								   resultslot->tts_tupleDescriptor,
								   sort->numCols,
								   sort->sortColIdx,
								   sort->sortOperators,
								   sort->sortCollations,
								   sort->nullsFirst,
								   combiner,
								   work_mem);
		}
	}

	if (combiner->tuplesortstate)
	{
		if (tuplesort_gettupleslot((Tuplesortstate *) combiner->tuplesortstate,
									  true, true, resultslot, NULL))
			return resultslot;
		else
			ExecClearTuple(resultslot);
	}
	else
	{
		TupleTableSlot *slot = FetchTuple(combiner);
		if (!TupIsNull(slot))
			return slot;
	}

	if (combiner->errorMessage)
		pgxc_node_report_error(combiner);

	if (need_free_in_combiner)
		pfree(combiner->connections);

	return NULL;
}

/* ----------------------------------------------------------------
 *		ExecReScanRemoteQuery
 * ----------------------------------------------------------------
 */
void
ExecReScanRemoteQuery(RemoteQueryState *node)
{
	ResponseCombiner *combiner = (ResponseCombiner *)node;

	/*
	 * If we haven't queried remote nodes yet, just return. If outerplan'
	 * chgParam is not NULL then it will be re-scanned by ExecProcNode,
	 * else - no reason to re-scan it at all.
	 */
	if (!node->query_Done)
		return;

	/*
	 * If we execute locally rescan local copy of the plan
	 */
	if (outerPlanState(node))
		ExecReScan(outerPlanState(node));

	/*
	 * Consume any possible pending input
	 */
	pgxc_connections_cleanup(combiner, false);

	/* misc cleanup */
	combiner->command_complete_count = 0;
	combiner->description_count = 0;

	/*
	 * Force query is re-bound with new parameters
	 */
	node->query_Done = false;

}

/*
 * Clean up and discard any data on the data node connections that might not
 * handled yet, including pending on the remote connection.
 */
void
pgxc_connections_cleanup(ResponseCombiner *combiner, bool is_abort)
{
	int32	ret     = 0;
	struct timeval timeout;
	timeout.tv_sec  = END_QUERY_TIMEOUT / 1000;
	timeout.tv_usec = (END_QUERY_TIMEOUT % 1000) * 1000;

	/* clean up the buffer */
	list_free_deep(combiner->rowBuffer);
	combiner->rowBuffer = NIL;
#ifdef __OPENTENBASE__
	/* clean up tuplestore */
	if (combiner->merge_sort)
	{
		if (combiner->dataRowBuffer)
		{
			int i = 0;

			for (i = 0; i < combiner->conn_count; i++)
			{
				if (combiner->dataRowBuffer[i])
				{
					tuplestore_end(combiner->dataRowBuffer[i]);
					combiner->dataRowBuffer[i] = NULL;
				}
			}

			pfree(combiner->dataRowBuffer);
			combiner->dataRowBuffer = NULL;
		}

		if (combiner->prerowBuffers)
		{
			int i = 0;

			for (i = 0; i < combiner->conn_count; i++)
			{
				if (combiner->prerowBuffers[i])
				{
					list_free_deep(combiner->prerowBuffers[i]);
					combiner->prerowBuffers[i] = NULL;
				}
			}

			pfree(combiner->prerowBuffers);
			combiner->prerowBuffers = NULL;
		}
	}
	else
	{
		if (combiner->dataRowBuffer && combiner->dataRowBuffer[0])
		{
			tuplestore_end(combiner->dataRowBuffer[0]);
			combiner->dataRowBuffer[0] = NULL;

			pfree(combiner->dataRowBuffer);
			combiner->dataRowBuffer = NULL;
		}
	}

	if (combiner->nDataRows)
	{
		pfree(combiner->nDataRows);
		combiner->nDataRows = NULL;
	}

	if (combiner->dataRowMemSize)
	{
		pfree(combiner->dataRowMemSize);
		combiner->dataRowMemSize = NULL;
	}
#endif

	/*
	 * Read in and discard remaining data from the connections, if any
	 */
	combiner->current_conn = 0;
	while (combiner->conn_count > 0)
	{
		int res;
		PGXCNodeHandle *conn = combiner->connections[combiner->current_conn];

		/*
		 * Possible if we are doing merge sort.
		 * We can do usual procedure and move connections around since we are
		 * cleaning up and do not care what connection at what position
		 */
		if (conn == NULL)
		{
			REMOVE_CURR_CONN(combiner);
			continue;
		}

		/* throw away current message that may be in the buffer */
		if (combiner->currentRow)
		{
			pfree(combiner->currentRow);
			combiner->currentRow = NULL;
		}

		ret = pgxc_node_receive(1, &conn, &timeout);
		if (ret)
		{
			switch (ret)
			{
				case DNStatus_ERR:
					{
						elog(LOG, "Failed to read response from data node:%s pid:%d when ending query for ERROR, node status:%d",
								conn->nodename, conn->backend_pid, GetConnectionState(conn));
						break;
					}

				case DNStatus_EXPIRED:
					{
						if (is_abort)
						{
							int timeout_cn[1];
							int timeout_dn[1];
							int timeout_dn_pid[1];
							int timeout_cn_count = 0;
							int timeout_dn_count = 0;

							if (conn->node_type == PGXC_NODE_COORDINATOR)
							{
								timeout_cn[0] = PGXCNodeGetNodeId(conn->nodeoid, NULL);
								timeout_cn_count = 1;
							}
							else
							{
								Assert(conn->node_type == PGXC_NODE_DATANODE);
								timeout_dn[0] = PGXCNodeGetNodeId(conn->nodeoid, NULL);
								timeout_dn_pid[0] = conn->backend_pid;
								timeout_dn_count = 1;
							}

							PoolManagerCancelQuery(timeout_dn_count, timeout_dn, timeout_dn_pid,
												   timeout_cn_count, timeout_cn, SIGNAL_SIGINT);
						}
						else
						{
							elog(ERROR, "Failed to read response from data node:%s pid:%d when ending query for EXPIRED, node status:%d",
								conn->nodename, conn->backend_pid, GetConnectionState(conn));
						}
						break;
					}
				default:
					{
						/* Can not be here.*/
						break;
					}
			}
		}

		res = handle_response(conn, combiner);
		if (res == RESPONSE_EOF)
		{
			/* Continue to consume the data, until all connections are abosulately done. */
			if (pgxc_node_is_data_enqueued(conn) && !proc_exit_inprogress)
			{
				DNConnectionState state = DN_CONNECTION_STATE_IDLE;
				state = GetConnectionState(conn);
				UpdateConnectionState(conn, DN_CONNECTION_STATE_QUERY);

				ret = pgxc_node_receive(1, &conn, &timeout);
				if (ret)
				{
					switch (ret)
					{
						case DNStatus_ERR:
							{
								elog(DEBUG1, "Failed to read response from data node:%u when ending query for ERROR, state:%d", conn->nodeoid, GetConnectionState(conn));
								break;
							}

						case DNStatus_EXPIRED:
							{
								elog(DEBUG1, "Failed to read response from data node:%u when ending query for EXPIRED, state:%d", conn->nodeoid, GetConnectionState(conn));
								break;
							}
						default:
							{
								/* Can not be here.*/
								break;
							}
					}
				}

				if (IsConnectionStateIdle(conn))
				{
					UpdateConnectionState(conn, state);
				}
				continue;
			}
			else if (!proc_exit_inprogress)
			{
				if (IsConnectionStateQuery(conn))
				{
					combiner->current_conn = (combiner->current_conn + 1) % combiner->conn_count;
					continue;
				}
			}
		}
		else if (RESPONSE_ERROR == res)
		{
			if (conn->needSync)
			{
				if (pgxc_node_send_sync(conn))
				{
					ereport(LOG,
							(errcode(ERRCODE_INTERNAL_ERROR),
							 errmsg("Failed to sync msg to node:%s pid:%d when clean", conn->nodename, conn->backend_pid)));
				}

#ifdef _PG_REGRESS_
				ereport(LOG,
							(errcode(ERRCODE_INTERNAL_ERROR),
							 errmsg("Succeed to sync msg to node:%s pid:%d when clean", conn->nodename, conn->backend_pid)));
#endif
				conn->needSync = false;

			}
			continue;
		}
		else if (RESPONSE_COPY == res)
		{
			if (combiner->is_abort && IsConnectionStateCopyIn(conn))
			{
				DataNodeCopyEnd(conn, true);
			}
		}

		/* no data is expected */
		if (IsConnectionStateIdle(conn) ||
			IsConnectionStateFatal(conn) ||
			IsConnectionStateError(conn))
		{
#ifdef _PG_REGRESS_
			int32 nbytes = pgxc_node_is_data_enqueued(conn);
			if (nbytes && IsConnectionStateIdle(conn))
			{

				ereport(LOG,
						(errcode(ERRCODE_INTERNAL_ERROR),
						 errmsg("node:%s pid:%d status:%d %d bytes dataleft over.",
						 	conn->nodename, conn->backend_pid, GetConnectionState(conn), nbytes)));

			}
#endif
			REMOVE_CURR_CONN(combiner);
			continue;
		}

		/*
		 * Connection owner is different, so no our data pending at
		 * the connection, nothing to read in.
		 */
		if (conn->combiner && conn->combiner != combiner)
		{
			elog(LOG, "pgxc_connections_cleanup is different, remove connection:%s", conn->nodename);
			REMOVE_CURR_CONN(combiner);
			continue;
		}

	}

	/*
	 * Release tuplesort resources
	 */
	if (combiner->tuplesortstate)
	{
		/*
		 * Free these before tuplesort_end, because these arrays may appear
		 * in the tuplesort's memory context, tuplesort_end deletes this
		 * context and may invalidate the memory.
		 * We still want to free them here, because these may be in different
		 * context.
		 */
		if (combiner->tapenodes)
		{
			pfree(combiner->tapenodes);
			combiner->tapenodes = NULL;
		}
		if (combiner->tapemarks)
		{
			pfree(combiner->tapemarks);
			combiner->tapemarks = NULL;
			combiner->tapemarks_count = 0;
		}
		/*
		 * tuplesort_end invalidates minimal tuple if it is in the slot because
		 * deletes the TupleSort memory context, causing seg fault later when
		 * releasing tuple table
		 */
		ExecClearTuple(combiner->ss.ps.ps_ResultTupleSlot);
		tuplesort_end((Tuplesortstate *) combiner->tuplesortstate);
		combiner->tuplesortstate = NULL;
	}
}


/*
 * End the remote query
 */
void
ExecEndRemoteQuery(RemoteQueryState *node)
{
	ResponseCombiner *combiner = (ResponseCombiner *) node;

	/*
	 * Clean up remote connections
	 */
	pgxc_connections_cleanup(combiner, false);

	/*
	 * Clean up parameters if they were set, since plan may be reused
	 */
	if (node->paramval_data)
	{
		pfree(node->paramval_data);
		node->paramval_data = NULL;
		node->paramval_len = 0;
	}

	CloseCombiner(combiner);
	pfree(node);
}


/**********************************************
 *
 * Routines to support RemoteSubplan plan node
 *
 **********************************************/

static bool
determine_param_types_walker(Node *node, struct find_params_context *context)
{
	if (node == NULL)
		return false;

	if (IsA(node, Param))
	{
		Param *param = (Param *) node;
		int paramno = param->paramid;

		if (param->paramkind == PARAM_EXEC &&
				bms_is_member(paramno, context->defineParams))
		{
			RemoteParam *cur = context->rparams;
			while (cur->paramkind != PARAM_EXEC || cur->paramid != paramno)
				cur++;
			cur->paramtype = param->paramtype;
			context->defineParams = bms_del_member(context->defineParams,
												   paramno);
			return bms_is_empty(context->defineParams);
		}
	}

	if (IsA(node, SubPlan) && context->subplans)
	{
		Plan *plan;
		SubPlan *subplan = (SubPlan *)node;

		plan = (Plan *)list_nth(context->subplans, subplan->plan_id - 1);

		if (determine_param_types(plan, context))
			return true;
	}

	return expression_tree_walker(node, determine_param_types_walker,
								  (void *) context);

}

/*
 * Scan expressions in the plan tree to find Param nodes and get data types
 * from them
 */
bool
determine_param_types(Plan *plan,  struct find_params_context *context)
{
	Bitmapset *intersect;

	if (plan == NULL)
		return false;

	intersect = bms_intersect(plan->allParam, context->defineParams);
	if (bms_is_empty(intersect))
	{
		/* the subplan does not depend on params we are interested in */
		bms_free(intersect);
		return false;
	}
	bms_free(intersect);

	/* scan target list */
	if (expression_tree_walker((Node *) plan->targetlist,
							   determine_param_types_walker,
							   (void *) context))
		return true;
	/* scan qual */
	if (expression_tree_walker((Node *) plan->qual,
							   determine_param_types_walker,
							   (void *) context))
		return true;

	/* Check additional node-type-specific fields */
	switch (nodeTag(plan))
	{
		case T_Result:
			if (expression_tree_walker((Node *) ((Result *) plan)->resconstantqual,
									   determine_param_types_walker,
									   (void *) context))
				return true;
			break;

		case T_SeqScan:
		case T_SampleScan:
		case T_CteScan:
#ifdef __FDW__
		case T_ForeignScan:
#endif
			break;

		case T_IndexScan:
			if (expression_tree_walker((Node *) ((IndexScan *) plan)->indexqual,
									   determine_param_types_walker,
									   (void *) context))
				return true;
			break;

		case T_IndexOnlyScan:
			if (expression_tree_walker((Node *) ((IndexOnlyScan *) plan)->indexqual,
									   determine_param_types_walker,
									   (void *) context))
				return true;
			break;

		case T_BitmapIndexScan:
			if (expression_tree_walker((Node *) ((BitmapIndexScan *) plan)->indexqual,
									   determine_param_types_walker,
									   (void *) context))
				return true;
			break;

		case T_BitmapHeapScan:
			if (expression_tree_walker((Node *) ((BitmapHeapScan *) plan)->bitmapqualorig,
									   determine_param_types_walker,
									   (void *) context))
				return true;
			break;

		case T_TidScan:
			if (expression_tree_walker((Node *) ((TidScan *) plan)->tidquals,
									   determine_param_types_walker,
									   (void *) context))
				return true;
			break;

		case T_SubqueryScan:
			if (determine_param_types(((SubqueryScan *) plan)->subplan, context))
				return true;
			break;

		case T_FunctionScan:
			if (expression_tree_walker((Node *) ((FunctionScan *) plan)->functions,
									   determine_param_types_walker,
									   (void *) context))
				return true;
			break;

		case T_ValuesScan:
			if (expression_tree_walker((Node *) ((ValuesScan *) plan)->values_lists,
									   determine_param_types_walker,
									   (void *) context))
				return true;
			break;

		case T_ModifyTable:
			break;

		case T_RemoteSubplan:
			break;

		case T_Append:
			{
				ListCell   *l;

				foreach(l, ((Append *) plan)->appendplans)
				{
					if (determine_param_types((Plan *) lfirst(l), context))
						return true;
				}
			}
			break;

		case T_MergeAppend:
			{
				ListCell   *l;

				foreach(l, ((MergeAppend *) plan)->mergeplans)
				{
					if (determine_param_types((Plan *) lfirst(l), context))
						return true;
				}
			}
			break;

		case T_BitmapAnd:
			{
				ListCell   *l;

				foreach(l, ((BitmapAnd *) plan)->bitmapplans)
				{
					if (determine_param_types((Plan *) lfirst(l), context))
						return true;
				}
			}
			break;

		case T_BitmapOr:
			{
				ListCell   *l;

				foreach(l, ((BitmapOr *) plan)->bitmapplans)
				{
					if (determine_param_types((Plan *) lfirst(l), context))
						return true;
				}
			}
			break;

		case T_NestLoop:
			if (expression_tree_walker((Node *) ((Join *) plan)->joinqual,
									   determine_param_types_walker,
									   (void *) context))
				return true;
			break;

		case T_MergeJoin:
			if (expression_tree_walker((Node *) ((Join *) plan)->joinqual,
									   determine_param_types_walker,
									   (void *) context))
				return true;
			if (expression_tree_walker((Node *) ((MergeJoin *) plan)->mergeclauses,
									   determine_param_types_walker,
									   (void *) context))
				return true;
			break;

		case T_HashJoin:
			if (expression_tree_walker((Node *) ((Join *) plan)->joinqual,
									   determine_param_types_walker,
									   (void *) context))
				return true;
			if (expression_tree_walker((Node *) ((HashJoin *) plan)->hashclauses,
									   determine_param_types_walker,
									   (void *) context))
				return true;
			break;

		case T_Limit:
			if (expression_tree_walker((Node *) ((Limit *) plan)->limitOffset,
									   determine_param_types_walker,
									   (void *) context))
				return true;
			if (expression_tree_walker((Node *) ((Limit *) plan)->limitCount,
									   determine_param_types_walker,
									   (void *) context))
				return true;
			break;

		case T_RecursiveUnion:
			break;

		case T_LockRows:
			break;

		case T_WindowAgg:
			if (expression_tree_walker((Node *) ((WindowAgg *) plan)->startOffset,
									   determine_param_types_walker,
									   (void *) context))
			if (expression_tree_walker((Node *) ((WindowAgg *) plan)->endOffset,
									   determine_param_types_walker,
									   (void *) context))
			break;

		case T_Gather:
		case T_Hash:
		case T_Agg:
		case T_Material:
		case T_Sort:
		case T_Unique:
		case T_SetOp:
		case T_Group:
		case T_MergeQualProj:
		case T_ConnectBy:
		case T_PartIterator:
			break;

		/* begin ora */
		case T_MultiModifyTable:
			{
				ListCell	*lc;

				foreach(lc, ((MultiModifyTable *) plan)->sub_modifytable)
				{
					SubPlan		*splan = lfirst_node(SubPlan, lc);

					if (determine_param_types((Plan *) list_nth_node(ModifyTable,
											  context->subplans,
											  splan->plan_id - 1),
											  context))
						return true;
				}
			}
			break;
		/* end ora */

		default:
			elog(ERROR, "unrecognized node type: %d",
				 (int) nodeTag(plan));
	}

	/* check initplan if exists */
	if (plan->initPlan)
	{
		ListCell *l;

		foreach(l, plan->initPlan)
		{
			SubPlan  *subplan = (SubPlan *) lfirst(l);

			if (determine_param_types_walker((Node *)subplan, context))
				return true;
		}
	}

	/* recurse into subplans */
	return determine_param_types(plan->lefttree, context) ||
			determine_param_types(plan->righttree, context);
}

/*
 * pgxc_node_report_error
 * Throw error from Datanode if any.
 */
void
pgxc_node_report_error(ResponseCombiner *combiner)
{
	/* If no combiner, nothing to do */
	if (!combiner)
		return;

	if (combiner->errorMessage)
	{
		char *code = combiner->errorCode;

		if ((combiner->errorDetail == NULL) && (combiner->errorHint == NULL))
		{
			if (IS_PGXC_LOCAL_COORDINATOR && MAKE_SQLSTATE(code[0], code[1],
														   code[2], code[3],
														   code[4]) == SESSION_PROFILE_LIMIT_EXCEEDED)
			{
				if (ActivePortal && ActivePortal->queryDesc && ActivePortal->queryDesc->totaltime)
					InstrStopNode(ActivePortal->queryDesc->totaltime, 0);

				ereport(FATAL,
						(errcode(MAKE_SQLSTATE(code[0], code[1], code[2], code[3], code[4])),
						 errmsg("%s", combiner->errorMessage)));
			}
			else
				ereport(ERROR,
						(errcode(MAKE_SQLSTATE(code[0], code[1], code[2], code[3], code[4])),
						 errmsg("%s", combiner->errorMessage)));
		}
		else if ((combiner->errorDetail != NULL) && (combiner->errorHint != NULL))
			ereport(ERROR,
					(errcode(MAKE_SQLSTATE(code[0], code[1], code[2], code[3], code[4])),
					 errmsg("%s", combiner->errorMessage),
					 errdetail("%s", combiner->errorDetail),
					 errhint("%s", combiner->errorHint)));
		else if (combiner->errorDetail != NULL)
			ereport(ERROR,
					(errcode(MAKE_SQLSTATE(code[0], code[1], code[2], code[3], code[4])),
					 errmsg("%s", combiner->errorMessage),
					 errdetail("%s", combiner->errorDetail)));
		else
			ereport(ERROR,
					(errcode(MAKE_SQLSTATE(code[0], code[1], code[2], code[3], code[4])),
					 errmsg("%s", combiner->errorMessage),
					 errhint("%s", combiner->errorHint)));
	}
}


/*
 * get_success_nodes:
 * Currently called to print a user-friendly message about
 * which nodes the query failed.
 * Gets all the nodes where no 'E' (error) messages were received; i.e. where the
 * query ran successfully.
 */
static ExecNodes *
get_success_nodes(int node_count, PGXCNodeHandle **handles, char node_type, StringInfo failednodes)
{
	ExecNodes *success_nodes = NULL;
	int i;

	for (i = 0; i < node_count; i++)
	{
		PGXCNodeHandle *handle = handles[i];
		int nodenum = PGXCNodeGetNodeId(handle->nodeoid, &node_type);

		if (handle->error[0])
		{
			if (!success_nodes)
				success_nodes = makeNode(ExecNodes);
			success_nodes->nodeList = lappend_int(success_nodes->nodeList, nodenum);
		}
		else
		{
			if (failednodes->len == 0)
				appendStringInfo(failednodes, "Error message received from nodes:");
			appendStringInfo(failednodes, " %s#%d",
				(node_type == PGXC_NODE_COORDINATOR ? "coordinator" : "datanode"),
				nodenum + 1);
		}
	}
	return success_nodes;
}

/*
 * pgxc_all_success_nodes: Uses get_success_nodes() to collect the
 * user-friendly message from coordinator as well as datanode.
 */
void
pgxc_all_success_nodes(ExecNodes **d_nodes, ExecNodes **c_nodes, char **failednodes_msg)
{
	PGXCNodeAllHandles *connections = get_exec_connections(NULL, NULL, EXEC_ON_ALL_NODES, true);
	StringInfoData failednodes;
	initStringInfo(&failednodes);

	*d_nodes = get_success_nodes(connections->dn_conn_count,
								 connections->datanode_handles,
								 PGXC_NODE_DATANODE,
								 &failednodes);

	*c_nodes = get_success_nodes(connections->co_conn_count,
								 connections->coord_handles,
								 PGXC_NODE_COORDINATOR,
								 &failednodes);

	if (failednodes.len == 0)
		*failednodes_msg = NULL;
	else
		*failednodes_msg = failednodes.data;

	pfree_pgxc_all_handles(connections);
}


/*
 * set_dbcleanup_callback:
 * Register a callback function which does some non-critical cleanup tasks
 * on xact success or abort, such as tablespace/database directory cleanup.
 */
void
set_dbcleanup_callback(xact_callback function, void *paraminfo, int paraminfo_size)
{
	void *fparams;

	fparams = MemoryContextAlloc(TopMemoryContext, paraminfo_size);
	memcpy(fparams, paraminfo, paraminfo_size);

	dbcleanup_info.function = function;
	dbcleanup_info.fparams = fparams;
}

/*
 * AtEOXact_DBCleanup: To be called at post-commit or pre-abort.
 * Calls the cleanup function registered during this transaction, if any.
 */
void
AtEOXact_DBCleanup(bool isCommit)
{
	if (dbcleanup_info.function)
		(*dbcleanup_info.function)(isCommit, dbcleanup_info.fparams);

	/*
	 * Just reset the callbackinfo. We anyway don't want this to be called again,
	 * until explicitly set.
	 */
	dbcleanup_info.function = NULL;
	if (dbcleanup_info.fparams)
	{
		pfree(dbcleanup_info.fparams);
		dbcleanup_info.fparams = NULL;
	}
}

char *
GetImplicit2PCGID(const char *implicit2PC_head, bool localWrite)
{
	int dnCount = 0, coordCount = 0;
	int *dnNodeIds 	  = NULL;
	int *coordNodeIds = NULL;
	MemoryContext oldContext = CurrentMemoryContext;
	StringInfoData str;

	dnNodeIds = (int*)palloc(sizeof(int) * OPENTENBASE_MAX_DATANODE_NUMBER);
	coordNodeIds = (int*)palloc(sizeof(int) * OPENTENBASE_MAX_COORDINATOR_NUMBER);
	oldContext = MemoryContextSwitchTo(TopTransactionContext);
	initStringInfo(&str);
	/*
	 * Check how many coordinators and datanodes are involved in this
	 * transaction.
	 * MAX_IMPLICIT_2PC_STR_LEN (5 + 21 + 64 + 1 + 5 + 5)
	 */

	pgxc_node_remote_count(&dnCount, dnNodeIds, &coordCount, coordNodeIds);
	appendStringInfo(&str, "%s%u:%s:%c:%d:%d",
			implicit2PC_head,
			GetTopTransactionId(),
			PGXCNodeName,
			localWrite ? 'T' : 'F',
			dnCount,
			coordCount + (localWrite ? 1 : 0));

#if 0
	for (i = 0; i < dnCount; i++)
		appendStringInfo(&str, ":%d", dnNodeIds[i]);
	for (i = 0; i < coordCount; i++)
		appendStringInfo(&str, ":%d", coordNodeIds[i]);

	if (localWrite)
		appendStringInfo(&str, ":%d", PGXCNodeIdentifier);
#endif
	MemoryContextSwitchTo(oldContext);

	if (dnNodeIds)
	{
		pfree(dnNodeIds);
		dnNodeIds = NULL;
	}

	if (coordNodeIds)
	{
		pfree(coordNodeIds);
		coordNodeIds = NULL;
	}

	return str.data;
}

void
ExecRemoteQueryInitializeDSM(RemoteQueryState *node,
							   ParallelContext *pcxt)
{
	ParallelWorkerStatus *worker_status = NULL;
	worker_status  = GetParallelWorkerStatusInfo(pcxt->toc);
	node->parallel_status = worker_status;
}

void
ExecRemoteQueryInitializeDSMWorker(RemoteQueryState *node,
								   ParallelWorkerContext *pwcxt)
{
	ParallelWorkerStatus *worker_status = NULL;
	ResponseCombiner     *combiner      = NULL;
	RemoteQuery          *step          = NULL;

	combiner 			  = (ResponseCombiner *) node;
	step     			  = (RemoteQuery *) combiner->ss.ps.plan;
	worker_status  		  = GetParallelWorkerStatusInfo(pwcxt->toc);
	node->parallel_status = worker_status;
	if (step->exec_type != EXEC_ON_DATANODES)
	{
		elog(PANIC, "Only datanode remote quern can run in worker");
	}
}

/*
 * pgxc_append_param_val:
 * Append the parameter value for the SET clauses of the UPDATE statement.
 * These values are the table attribute values from the dataSlot.
 */
static void
pgxc_append_param_val(StringInfo buf, Datum val, Oid valtype)
{
	/* Convert Datum to string */
	char *pstring;
	int len;
	uint32 n32;
	Oid		typOutput;
	bool	typIsVarlena;

	/* Get info needed to output the value */
	getTypeOutputInfo(valtype, &typOutput, &typIsVarlena);
	/*
	 * If we have a toasted datum, forcibly detoast it here to avoid
	 * memory leakage inside the type's output routine.
	 */
	if (typIsVarlena)
		val = PointerGetDatum(PG_DETOAST_DATUM(val));

	pstring = OidOutputFunctionCall(typOutput, val);

	/* copy data to the buffer */
	len = strlen(pstring);
	n32 = pg_hton32(len);
	appendBinaryStringInfo(buf, (char *) &n32, 4);
	appendBinaryStringInfo(buf, pstring, len);
}

/*
 * pgxc_append_param_junkval:
 * Append into the data row the parameter whose value cooresponds to the junk
 * attributes in the source slot, namely ctid or node_id.
 */
static void
pgxc_append_param_junkval(TupleTableSlot *slot, AttrNumber attno,
						  Oid valtype, StringInfo buf)
{
	bool isNull;

	if (slot && attno != InvalidAttrNumber)
	{
		/* Junk attribute positions are saved by ExecFindJunkAttribute() */
		Datum val = ExecGetJunkAttribute(slot, attno, &isNull);
		/* shouldn't ever get a null result... */
		if (isNull)
			elog(ERROR, "NULL junk attribute");

		pgxc_append_param_val(buf, val, valtype);
	}
}

/* handle escape char '''  in source char sequence */
static char*
handleEscape(char *source, bool *special_case)
{
	char escape = '\''; /* escape char */
	int len = strlen(source);
	char *des = (char *)palloc(len * 3 + 1);
	int i = 0;
	int index = 0;

	/* when meet escape char, add another escape char */
	for (i = 0; i < len; i++)
	{
		if (source[i] == escape)
		{
			des[index++] = '\'';
			des[index++] = source[i];
		}
		else if (source[i] == '\r')
		{
			des[index++] = '\\';
			des[index++] = 'r';
			(*special_case) = true;
		}
		else if (source[i] == '\n')
		{
			des[index++] = '\\';
			des[index++] = 'n';
			(*special_case) = true;
		}
		else if (source[i] == '\t')
		{
			des[index++] = '\\';
			des[index++] = 't';
			(*special_case) = true;
		}
		else if (source[i] == '\b')
		{
			des[index++] = '\\';
			des[index++] = 'b';
			(*special_case) = true;
		}
		else if (source[i] == '\f')
		{
			des[index++] = '\\';
			des[index++] = 'f';
			(*special_case) = true;
		}
		else if (source[i] == '\v')
		{
			des[index++] = '\\';
			des[index++] = 'v';
			(*special_case) = true;
		}
		else
		{
			des[index++] = source[i];
		}
	}

	des[index++] = '\0';

	pfree(source);
	return des;
}


static void
SetDataRowParams(ModifyTableState *mtstate, RemoteQueryState *node,
				 TupleTableSlot *sourceSlot, TupleTableSlot *dataSlot,
				 ResultRelInfo *resultRelInfo)
{
	ModifyTable *mplan = (ModifyTable *) mtstate->ps.plan;
	CmdType	operation = mtstate->operation;
	StringInfoData	buf;
	StringInfoData	select_buf;
	uint16 numparams = node->rqs_num_params;
	TupleDesc	 	tdesc = dataSlot->tts_tupleDescriptor;
	int				attindex;
	int				numatts = tdesc->natts;
	ResponseCombiner *combiner = (ResponseCombiner *) node;
	RemoteQuery    *step = (RemoteQuery *) combiner->ss.ps.plan;
	Oid            *param_types = step->rq_param_types;
	Form_pg_attribute att;
	Oid         typeOutput;
	bool        typIsVarlena;
	char        *columnValue;
	char        *typename;
	bool special_case = false;
	int         cols = 0;

	initStringInfo(&buf);
	initStringInfo(&select_buf);

	switch(operation)
	{
		case CMD_INSERT:
		{
			/* ensure we have all values */
			slot_getallattrs(dataSlot);

			{
				uint16 params_nbo = pg_hton16(numparams); /* Network byte order */
				appendBinaryStringInfo(&buf, (char *) &params_nbo, sizeof(params_nbo));
			}

			for (attindex = 0; attindex < numatts; attindex++)
			{
				uint32 n32;
				Assert(attindex < numparams);

				if (dataSlot->tts_isnull[attindex] || !OidIsValid(param_types[attindex]))
				{
					n32 = pg_hton32(-1);
					appendBinaryStringInfo(&buf, (char *) &n32, 4);
				}
				else
					pgxc_append_param_val(&buf, dataSlot->tts_values[attindex], TupleDescAttr(tdesc, attindex)->atttypid);
			}

			if (attindex != numparams)
				elog(ERROR, "INSERT DataRowParams mismatch with dataSlot.");

			node->paramval_data = buf.data;
			node->paramval_len = buf.len;

			if (mplan->onConflictAction == ONCONFLICT_UPDATE)
			{
				appendStringInfoString(&select_buf, step->sql_select_base);
				appendStringInfoString(&select_buf, " where ");

				cols = 0;
				for (attindex = 0; attindex < numatts; attindex++)
				{
					special_case = false;

					if (!bms_is_member(attindex + 1, step->conflict_cols))
						continue;

					if (dataSlot->tts_isnull[attindex])
					{
						if (cols > 0)
						{
							appendStringInfo(&select_buf, " and ");
						}
						att = TupleDescAttr(tdesc, attindex);
						appendStringInfo(&select_buf, "%s is NULL", att->attname.data);
					}
					else
					{
						if (cols > 0)
						{
							appendStringInfo(&select_buf, " and ");
						}
						att = TupleDescAttr(tdesc, attindex);
						getTypeOutputInfo(att->atttypid, &typeOutput, &typIsVarlena);
						columnValue = DatumGetCString(OidFunctionCall1(typeOutput, dataSlot->tts_values[attindex]));
						typename = get_typename(att->atttypid);
						columnValue = handleEscape(columnValue, &special_case);
						if (special_case)
							appendStringInfo(&select_buf, "%s = E'%s'::%s", att->attname.data, columnValue, typename);
						else
							appendStringInfo(&select_buf, "%s = '%s'::%s", att->attname.data, columnValue, typename);
					}

					cols++;
				}

				if (step->forUpadte)
					appendStringInfoString(&select_buf, " FOR UPDATE");
				else
					appendStringInfoString(&select_buf, " FOR NO KEY UPDATE");

				step->sql_select = select_buf.data;
			}
		}
		break;
		case CMD_UPDATE:
		{
			/* ensure we have all values */
			slot_getallattrs(dataSlot);

			{
				uint16 params_nbo = pg_hton16(numparams); /* Network byte order */
				appendBinaryStringInfo(&buf, (char *) &params_nbo, sizeof(params_nbo));
			}

			for (attindex = 0; attindex < numatts; attindex++)
			{
				uint32 n32;
				Assert(attindex < numparams);

				if (dataSlot->tts_isnull[attindex] || !OidIsValid(param_types[attindex]))
				{
					n32 = pg_hton32(-1);
					appendBinaryStringInfo(&buf, (char *) &n32, 4);
				}
				else
					pgxc_append_param_val(&buf, dataSlot->tts_values[attindex], TupleDescAttr(tdesc, attindex)->atttypid);
			}

			if (attindex != numparams - 2)
				elog(ERROR, "UPDATE DataRowParams mismatch with dataSlot.");

			if (step->action == UPSERT_NONE)
			{
				pgxc_append_param_junkval(sourceSlot, resultRelInfo->ri_RowIdAttNo,
										  TIDOID, &buf);
				pgxc_append_param_junkval(sourceSlot, resultRelInfo->ri_xc_node_id,
										  INT4OID, &buf);
			}
			else
			{
				pgxc_append_param_junkval(sourceSlot, step->jf_ctid,
							  TIDOID, &buf);
				pgxc_append_param_junkval(sourceSlot, step->jf_xc_node_id,
										  INT4OID, &buf);
			}

			node->paramval_data = buf.data;
			node->paramval_len = buf.len;
		}
		break;
		case CMD_DELETE:
		{
			{
				uint16 params_nbo = pg_hton16(numparams); /* Network byte order */
				appendBinaryStringInfo(&buf, (char *) &params_nbo, sizeof(params_nbo));
			}

			pgxc_append_param_junkval(sourceSlot, resultRelInfo->ri_RowIdAttNo,
									  TIDOID, &buf);
			pgxc_append_param_junkval(sourceSlot, resultRelInfo->ri_xc_node_id,
									  INT4OID, &buf);

			node->paramval_data = buf.data;
			node->paramval_len = buf.len;
		}
		break;
		case CMD_MERGE:
		{
			{
				uint16 params_nbo = pg_hton16(numparams); /* Network byte order */
				appendBinaryStringInfo(&buf, (char *) &params_nbo, sizeof(params_nbo));
			}

			pgxc_append_param_junkval(sourceSlot, resultRelInfo->ri_RowIdAttNo,
									  TIDOID, &buf);
			pgxc_append_param_junkval(sourceSlot, resultRelInfo->ri_xc_node_id,
									  INT4OID, &buf);

			node->paramval_data = buf.data;
			node->paramval_len = buf.len;
		}
		break;
		default:
		{
			elog(ERROR, "unexpected CmdType in SetDataRowParams.");
		}
		break;
	}
}

static void
remember_prepared_node(RemoteQueryState *rstate, int node)
{
	int32 wordindex  = 0;
	int32 wordoffset = 0;
	if (node > MAX_NODES_NUMBER)
	{
		elog(ERROR, "invalid nodeid:%d is bigger than maximum node number of the cluster",node);
	}

	wordindex  = node/UINT32_BITS_NUM;
	wordoffset = node % UINT32_BITS_NUM;
	SET_BIT(rstate->dml_prepared_mask[wordindex], wordoffset);
}

static bool
is_node_prepared(RemoteQueryState *rstate, int node)
{
	int32 wordindex  = 0;
	int32 wordoffset = 0;
	if (node >= MAX_NODES_NUMBER)
	{
		elog(ERROR, "invalid nodeid:%d is bigger than maximum node number of the cluster", node);
	}

	wordindex  = node/UINT32_BITS_NUM;
	wordoffset = node % UINT32_BITS_NUM;
	return BIT_SET(rstate->dml_prepared_mask[wordindex], wordoffset) != 0;
}

/*
  *   ExecRemoteDML----execute DML on coordinator
  *   return true if insert/update/delete successfully, else false.
  *
  *   If DML target relation has unshippable triggers, we have to do DML on coordinator.
  *   Construct remote query about DML on plan phase, then adopt parse/bind/execute to
  *   execute the DML with the triggers on coordinator.
  *
  */
bool
ExecRemoteDML(ModifyTableState *mtstate, ItemPointer tupleid, HeapTuple oldtuple,
			  TupleTableSlot *slot, TupleTableSlot *planSlot, EState *estate, EPQState *epqstate,
			  bool canSetTag, TupleTableSlot **returning, UPSERT_ACTION *result,
			  ResultRelInfo *resultRelInfo, int rel_index)
{
	ModifyTable *mplan = (ModifyTable *) mtstate->ps.plan;
	CmdType	operation = mtstate->operation;
	RemoteQueryState *node = (RemoteQueryState *)mtstate->mt_remoterels[rel_index];
	ResponseCombiner *combiner = (ResponseCombiner *) node;
	RemoteQuery *step = (RemoteQuery *) combiner->ss.ps.plan;
	ExprContext	*econtext = combiner->ss.ps.ps_ExprContext;
	ExprContext	*proj_econtext = mtstate->ps.ps_ExprContext;
	PGXCNodeAllHandles *pgxc_connections = NULL;
	GlobalTransactionId gxid = InvalidGlobalTransactionId;
	Snapshot		snapshot = GetActiveSnapshot();
	PGXCNodeHandle **connections = NULL;
	int				i = 0;
	int				regular_conn_count = 0;
	TupleTableSlot *tupleslot = NULL;
	int nodeid;
	MemoryContext oldcontext;

	if (operation == CMD_INSERT && mplan->onConflictAction == ONCONFLICT_UPDATE)
	{
		if (step->action == UPSERT_NONE)
		{
			step->action = UPSERT_SELECT;
		}
	}

	ResetExprContext(econtext);

	if (step->action != UPSERT_UPDATE)
	{
		/*
		 * Get connections for remote query
		 */
		econtext->ecxt_scantuple = slot;
		pgxc_connections = get_exec_connections(node, step->exec_nodes,
												step->exec_type, true);

		Assert(step->exec_type == EXEC_ON_DATANODES);

		connections = pgxc_connections->datanode_handles;
		regular_conn_count = pgxc_connections->dn_conn_count;

		combiner->conns = pgxc_connections->datanode_handles;
		combiner->ccount = pgxc_connections->dn_conn_count;
	}
	else
	{
		/* reset connection */
		connections = combiner->conns;
		regular_conn_count = combiner->ccount;
	}

	/* get all parameters for execution */
	oldcontext = MemoryContextSwitchTo(econtext->ecxt_per_tuple_memory);
	SetDataRowParams(mtstate, node, planSlot, slot, resultRelInfo);
	MemoryContextSwitchTo(oldcontext);

	/* need to send commandid to datanode */
	SetSendCommandId(true);

	Assert(regular_conn_count == 1);

	nodeid = PGXCNodeGetNodeId(connections[i]->nodeoid, NULL);

	/* need transaction during execution, but only send begin to datanode once */
	if (!is_node_prepared(node, nodeid))
	{
		gxid = GetCurrentTransactionIdIfAny();

		if (pgxc_node_begin(1, &connections[i], gxid, true, true))
		{
			elog(ERROR,
				 "Could not begin transaction on datanode in ExecRemoteDML, nodeid:%d.",
				 connections[i]->nodeid);
		}

		remember_prepared_node(node, nodeid);
	}

	/*
	  * For update/delete and simple insert, we can send SQL statement  to datanode through
	  * parse/bind/execute.
	  *
	  * UPSERT is a little different, we separate UPSERT into insert, select and update.
	  *
	  * Step 1, we send select...for update to find the conflicted tuple and lock it; if we
	  * succeed, then do the update and finish. Else, it means no conflict tuple now, we can do
	  * the insert.
	  *
	  * Step 2, if insert succeed, then finish; else insert must conflict with others, this must be
	  * unexepected, but it does happen, goto step 1.
	  * We have to repeat step 1 and step 2, until finish.
	  */
	if (operation == CMD_UPDATE || operation == CMD_DELETE || operation == CMD_MERGE ||
		(operation == CMD_INSERT && mplan->onConflictAction != ONCONFLICT_UPDATE))
	{
		if (!pgxc_start_command_on_connection(connections[i], node, snapshot, false))
		{
			elog(ERROR, "Failed to send command to datanode in ExecRemoteDML, nodeid:%d.",
						connections[i]->nodeid);
		}
	}
	else
UPSERT:
	{
		/* insert...on conflict do update */
		char	   *paramval_data = node->paramval_data;
		int			paramval_len = node->paramval_len;
		Oid		   *rqs_param_types = node->rqs_param_types;
		int			rqs_num_params = node->rqs_num_params;
		char       *sql = step->sql_statement;
		char       *statement = step->statement;

		/* reset connection */
		connections = combiner->conns;
		regular_conn_count = combiner->ccount;

		switch(step->action)
		{
			/* select first, try to find conflict tupe */
			case UPSERT_SELECT:
			{
				node->paramval_data = node->ss_paramval_data;
				node->paramval_len = node->ss_paramval_len;
				node->rqs_param_types = node->ss_param_types;
				node->rqs_num_params = node->ss_num_params;
				step->sql_statement = step->sql_select;
				step->statement = step->select_cursor;

				if (!pgxc_start_command_on_connection(connections[i], node, snapshot, false))
				{
					elog(ERROR, "Failed to send up_select to datanode in ExecRemoteDML, nodeid:%d.",
								connections[i]->nodeid);
				}

				node->paramval_data = paramval_data;
				node->paramval_len = paramval_len;
				node->rqs_param_types = rqs_param_types;
				node->rqs_num_params = rqs_num_params;
				step->sql_statement = sql;
				step->statement = statement;

				break;
			}
			/* no conflict tuple found, try to insert */
			case UPSERT_INSERT:
			{
				if (!pgxc_start_command_on_connection(connections[i], node, snapshot, false))
				{
					elog(ERROR, "Failed to send up_insert to datanode in ExecRemoteDML, nodeid:%d.",
								connections[i]->nodeid);
				}

				break;
			}
			case UPSERT_UPDATE:
			{
				node->paramval_data = node->su_paramval_data;
				node->paramval_len = node->su_paramval_len;
				node->rqs_param_types = node->su_param_types;
				node->rqs_num_params = node->su_num_params;
				step->sql_statement = step->sql_update;
				step->statement = step->update_cursor;
				mtstate->operation = CMD_UPDATE;

				/* do update */
				proj_econtext->ecxt_scantuple = mtstate->mt_existing;
				proj_econtext->ecxt_innertuple = slot;
				proj_econtext->ecxt_outertuple = NULL;

				if (ExecQual(resultRelInfo->ri_onConflict->oc_WhereClause, proj_econtext))
				{
					Datum		datum;
					bool		isNull;
					ItemPointer conflictTid;
					HeapTuple new_oldtuple;

					datum = ExecGetJunkAttribute(mtstate->mt_existing,
												step->jf_ctid,
												&isNull);
					/* shouldn't ever get a null result... */
					if (isNull)
						elog(ERROR, "ctid is NULL");

					conflictTid = (ItemPointer) DatumGetPointer(datum);

					new_oldtuple = heap_form_tuple(mtstate->mt_existing->tts_tupleDescriptor,
											   mtstate->mt_existing->tts_values,
											   mtstate->mt_existing->tts_isnull);

					/* Project the new tuple version */
					ExecProject(resultRelInfo->ri_onConflict->oc_ProjInfo);

					/* Execute UPDATE with projection */
					*returning = ExecRemoteUpdate(mtstate, conflictTid, new_oldtuple,
											mtstate->mt_conflproj, mtstate->mt_existing,
											&mtstate->mt_epqstate, mtstate->ps.state,
											canSetTag);
				}

				node->rqs_param_types = rqs_param_types;
				node->rqs_num_params = rqs_num_params;
				step->sql_statement = sql;
				step->statement = statement;
				mtstate->operation = CMD_INSERT;
				step->action = UPSERT_NONE;

				*result = UPSERT_UPDATE;

				pfree_pgxc_all_handles(pgxc_connections);
				/*
				 * Unlike a regular RemoteQuery, the param data for RemoteDML is in the
				 * per-tuple context instead of the executor's context. To avoid double
				 * pfree, it is set to NULL and therefore does not need to be pfreed
				 * here.
				 */
				node->paramval_data = NULL;
				node->paramval_len = 0;
				return true;
			}
			default:
				elog(ERROR, "unexpected UPSERT action.");
				break;
		}
	}

	/* handle reponse */
	combiner->connections = connections;
	combiner->conn_count = regular_conn_count;
	combiner->current_conn = 0;
	combiner->DML_processed = 0;

	/*
	 * get response from remote datanode while executing DML,
	 * we just need to check the affected rows only.
	 */
	while ((tupleslot = FetchTuple(combiner)) != NULL)
	{
		/* we do nothing until we get the commandcomplete response, except for UPSERT case */
		mtstate->mt_existing = tupleslot;
	}

	if (combiner->errorMessage)
		pgxc_node_report_error(combiner);

	if (combiner->DML_processed)
	{
		if (combiner->DML_processed > 1)
		{
			elog(ERROR, "RemoteDML affects %d rows, more than one row.", combiner->DML_processed);
		}

		/* UPSERT: if select succeed, we can do update directly */
		if (step->action == UPSERT_SELECT)
		{
			step->action = UPSERT_UPDATE;
			goto UPSERT;
		}
		else if (step->action == UPSERT_INSERT)
		{
			step->action = UPSERT_NONE;
			*result = UPSERT_INSERT;
		}

		/* DML succeed */
		pfree_pgxc_all_handles(pgxc_connections);
		/*
		 * Unlike a regular RemoteQuery, the param data for RemoteDML is in the
		 * per-tuple context instead of the executor's context. To avoid double
		 * pfree, it is set to NULL and therefore does not need to be pfreed
		 * here.
		 */
		node->paramval_data = NULL;
		node->paramval_len = 0;
		return true;
	}
	else
	{
		/* UPSERT: if select failed, we do insert instead */
		if (step->action == UPSERT_SELECT)
		{
			step->action = UPSERT_INSERT;
			goto UPSERT;
		}
		/* UPSERT: insert failed, conflict tuple found, re-check the constraints. */
		else if (step->action == UPSERT_INSERT)
		{
			step->action = UPSERT_SELECT;
			goto UPSERT;
		}

		pfree_pgxc_all_handles(pgxc_connections);
		/*
		 * Unlike a regular RemoteQuery, the param data for RemoteDML is in the
		 * per-tuple context instead of the executor's context. To avoid double
		 * pfree, it is set to NULL and therefore does not need to be pfreed
		 * here.
		 */
		node->paramval_data = NULL;
		node->paramval_len = 0;
		return false;
	}

	pfree_pgxc_all_handles(pgxc_connections);
	return true;    /* keep compiler quiet */
}

/*
 * Currently, This function will only be called by CommitSubTranscation.
 * 1. When coordinator, need to send commit_subtxn and clean up connection,
 * 	  and will not release handle.
 * 2. When datanode, need to send commit transcation and clean up connection,
 *	  and will not release handle.
 * 3. When non-subtxn, the parameter of pgxc_node_remote_commit will be
 *	  (TXN_TYPE_CommitTxn , need_release_handle = true)
 */
void
SubTranscation_PreCommit_Remote(void)
{
	MemoryContext old;
	MemoryContext temp = AllocSetContextCreate(TopMemoryContext,
												  "SubTransaction remote commit context",
												  ALLOCSET_DEFAULT_SIZES);
	old = MemoryContextSwitchTo(temp);
	/* send down commit_subtxn when not explicit release savepoint */
	if ((InPlpgsqlFunc() || IS_STMT_LEVEL_ROLLBACK) && !g_in_release_savepoint)
	{
		pgxc_node_remote_commit(TXN_TYPE_CommitSubTxn, false);
	}
	else if (IS_PGXC_DATANODE)
	{
		pgxc_node_remote_commit(TXN_TYPE_CommitTxn, false);
	}
	MemoryContextSwitchTo(old);
	MemoryContextDelete(temp);
}

/*
 * Currently, This function will only be called by AbortSubTranscation.
 * 1. When coordinator, need to send rollback_subtxn and clean up connection,
 * 	  and will not release handle.
 * 2. When datanode, need to send rollback transcation and clean up connection,
 *	  and will not release handle.
 * 3. When non-subtxn, the parameter of pgxc_node_remote_abort will be
 *	  (TXN_TYPE_RollbackTxn , need_release_handle = true)
 */

void
SubTranscation_PreAbort_Remote(void)
{
	if (!IS_PGXC_COORDINATOR)
		return;

	/* send down rollback_subtxn when not explicit rollback to */
	if ((InPlpgsqlFunc() || IS_STMT_LEVEL_ROLLBACK) && !(g_in_plpgsql_rollback_to || g_in_rollback_to))
	{
		PreAbort_Remote(TXN_TYPE_RollbackSubTxn, false);
	}
	else if (IS_PGXC_LOCAL_COORDINATOR)
	{
		PreAbort_Remote(TXN_TYPE_CleanConnection, false);
	}
}

static int
SendSynchronizeMessage(PGXCNodeHandle *handles[], int count, int sent_count, ResponseCombiner *combiner)
{
	int i = 0;
	PGXCNodeHandle *handle;

	for (i = 0; i < count; i++)
	{
		handle = handles[i];

		if (IsConnectionStateFatal(handle) || handle->sock == NO_SOCKET)
			continue;
		/* 
		 * Send a SYNC to the node when executing an extended query, even 
		 * if it is not currently in an error state, as it is uncertain 
		 * whether an error will occur after synchronizing the connection. 
		 * If no SYNC is sent, the execution will hang if an error arises
		 * on the node.
		 */
		if (!IsConnectionStateIdle(handle) || 
			pgxc_node_is_data_enqueued(handle) ||
			IsExecExtendedQuery(handle))
		{
			if (pgxc_node_send_sync(handle))
			{
				elog(LOG, "failed to send SYNC to node %s pid %d", handle->nodename, handle->backend_pid);
				UpdateConnectionState(handle, DN_CONNECTION_STATE_FATAL);
			}

			if (IsConnectionStateIdle(handle))
				UpdateConnectionState(handle, DN_CONNECTION_STATE_QUERY);

			if (IsExecExtendedQuery(handle))
				handle->state.in_extended_query = false;

			combiner->connections[sent_count++] = handle;
			handle->combiner = combiner;
		}
	}

	return sent_count;
}


/* Sync the connections, ensure the connections are clean and empty. */
void
pgxc_sync_connections(PGXCNodeAllHandles *all_handles, bool ignore_invalid)
{
	int		i  = 0;
	int		wait_count  = 0;
	ResponseCombiner combiner;
	struct timeval timeout;
	bool   errors = 0;

	if (proc_exit_inprogress)
		return;

	timeout.tv_sec  = 0;
	timeout.tv_usec = 1000;

	if (all_handles && (all_handles->co_conn_count + all_handles->dn_conn_count) > 0)
	{
		int cns = all_handles->co_conn_count;
		int dns = all_handles->dn_conn_count;

		InitResponseCombiner(&combiner, cns+dns, COMBINE_TYPE_NONE);
		combiner.connections = (PGXCNodeHandle **)palloc(sizeof(PGXCNodeHandle *) * (cns + dns));

		wait_count = SendSynchronizeMessage(all_handles->coord_handles, cns, 0, &combiner);
		wait_count = SendSynchronizeMessage(all_handles->datanode_handles, dns, wait_count, &combiner);
		combiner.node_count = wait_count;

		if (wait_count > 0)
		{
			errors = ProcessResponseMessage(&combiner, &timeout, RESPONSE_READY, true);
			if (errors)
			{
				elog(LOG, "failed to process the response when synchronizing connections, errors: %d, all: %d",
						errors, wait_count);

				/* do not raise error in this step, just set all handles to FATAL */
				if (combiner.errorMessage)
					elog(LOG, "%s", combiner.errorMessage);

				for (i = 0; i < wait_count; i++)
				{
					/* Set the connections that failed to synchronize to FATAL */
					if (!IsConnectionStateIdle(combiner.connections[i]))
						UpdateConnectionState(combiner.connections[i], DN_CONNECTION_STATE_FATAL);
				}
			}
			else
			{
				for (i = 0; i < wait_count; i++)
				{
					/* ensure that the connection state was set to IDLE */
					if (!IsConnectionStateIdle(combiner.connections[i]))
						UpdateConnectionState(combiner.connections[i], DN_CONNECTION_STATE_IDLE);
				}
			}
		}
		pfree(combiner.connections);
	}
}

/*
 * ExecSimpleRemoteDML
 *
 * Simplified version of ExecRemoteDML, ituple and dtuple are tuple that
 * need to be inserted and deleted respectively, send them to DN, DN will
 * perform simplified version of INSERT, DELETE or UPDATE.
 *
 * private_gts is needed by global index table.
 */
bool
ExecSimpleRemoteDML(HeapTuple ituple, HeapTuple dtuple, Datum *diskey, bool *disnull,
					int ndiscols, char *relname, char *space, Locator *locator,
					CmdType cmdtype, GlobalTimestamp private_gts, ItemPointer target_ctid)
{
	int conn_count;
	ExecNodes *exec_nodes = makeNode(ExecNodes);
	PGXCNodeAllHandles *pgxc_connections;
	PGXCNodeHandle *conn;
	ResponseCombiner combiner;
	int			  response;
	Snapshot		 snapshot;

	if (IS_CENTRALIZED_MODE)
	{
		ExecSimpleDML(relname, space, cmdtype, ituple, dtuple, target_ctid);
		return true;
	}

	conn_count = GET_NODES(locator, diskey, disnull, ndiscols, NULL);
	Assert(conn_count == 1);

	exec_nodes->nodeList = list_make1_int(((int *)getLocatorResults(locator))[0]);
	pgxc_connections = get_exec_connections(NULL, exec_nodes, EXEC_ON_DATANODES, true);
	conn = pgxc_connections->datanode_handles[0];
	if (pgxc_node_begin(1, &conn, GetCurrentTransactionIdIfAny(), true, true))
	{
		elog(ERROR, "Could not begin transaction on datanode in "
					"ExecSimpleRemoteDML, nodeid:%d.", conn->nodeid);
		return false;
	}

	snapshot = GetActiveSnapshot();
	if (private_gts != InvalidGlobalTimestamp)
		snapshot->start_ts = private_gts;
	if (snapshot && pgxc_node_send_snapshot(conn, snapshot) != 0)
	{
		elog(ERROR, "Could not send snapshot to datanode in ExecSimpleRemoteDML, nodeid:%d.",
			 conn->nodeid);
	}
	if (ORA_MODE && pgxc_node_send_exec_env_if(conn))
		elog(ERROR, "Failed to setup security context to datanode in ExecSimpleRemoteDML, nodeid:%d.",
			 conn->nodeid);
	if (pgxc_node_send_dml(conn, cmdtype, relname, space,
						   ituple, dtuple, target_ctid) == EOF)
	{
		elog(ERROR, "Failed to send command to datanode in ExecSimpleRemoteDML, nodeid:%d.",
			 conn->nodeid);
	}

	InitResponseCombiner(&combiner, conn_count, COMBINE_TYPE_NONE);

	/* Receive responses */
	while (true)
	{
		if (pgxc_node_receive(conn_count, &conn, NULL))
		{
			elog(ERROR, "Failed to receive from datanode in ExecSimpleRemoteDML, nodeid:%d.",
				 conn->nodeid);
		}

		response = handle_response(conn, &combiner);
		/* Simple Query Protocol we should receive ReadyForQuery */
		if (response == RESPONSE_READY)
			break;
		else if (response == RESPONSE_EOF || response == RESPONSE_COMPLETE ||
				 response == RESPONSE_ERROR)
		{
			if (response == RESPONSE_COMPLETE &&
				IsConnectionStateFatal(conn))
			{
				ereport(ERROR,
						(errcode(ERRCODE_INTERNAL_ERROR),
							errmsg("Unexpected FATAL ERROR on Connection to Datanode %s pid %d",
								   conn->nodename,
								   conn->backend_pid)));

			}
			/* Get ReadyForQuery */
			continue;
		}
		else
		{
			elog(ERROR, "Received unexpected message %d from datanode "
						"in ExecSimpleRemoteDML, nodeid:%d.",
				 response, conn->nodeid);
		}
	}

	if (ItemPointerIsValid(target_ctid) && cmdtype == CMD_UPDATE)
	{
		ItemPointerSet(&(ituple->t_self),
					   ItemPointerGetBlockNumber(&combiner.simpledml_newctid),
					   ItemPointerGetOffsetNumber(&combiner.simpledml_newctid));
	}

	if (combiner.errorMessage)
		pgxc_node_report_error(&combiner);

	CloseCombiner(&combiner);
	pfree_pgxc_all_handles(pgxc_connections);

	return true;
}

/*
 * ExecLocalTidFetch
 *
 *	 Fetch tuple using ctid when nodeidx is local in RemoteTidNext
 */
TupleTableSlot *
ExecLocalTidFetch(TidScanState *node, Datum ctid, Oid tableOid)
{
	TupleTableSlot		*result = NULL;
	Relation			originRelation = NULL;

	if (node->tss_TidList == NULL)
	{
		node->tss_TidList = (ItemPointerData *)
				palloc(sizeof(ItemPointerData));
		node->tss_NumTids = 1;
	}
	else
	{
		Assert(node->tss_NumTids == 1);
	}

	/* set ctid into TidList and reset TidPtr */
	node->tss_TidList[0] = *(DatumGetItemPointer(ctid));
	node->tss_TidPtr = -1;

	/* use ExecProcNode to get tuple, cast remote to local */
	node->remote2local = true;
	if (OidIsValid(tableOid))
	{
		originRelation = node->ss.ss_currentRelation;
		node->ss.ss_currentRelation = heap_open(tableOid, AccessShareLock);
	}

	result = ((PlanState *)node)->ExecProcNode((PlanState *)node);

	if (OidIsValid(tableOid))
	{
		heap_close(node->ss.ss_currentRelation, NoLock);
		node->ss.ss_currentRelation = originRelation;
	}
	node->remote2local = false;

	if (TupIsNull(result))
	{
		return NULL;
	}

	slot_getallattrs(result);
	return result;
}

/*
 * ExecRemoteTidFetch
 *
 *	 Fetch remote tuple using tidscan.
 */
TupleTableSlot *
ExecRemoteTidFetch(TidScanState *node, int nodeid, Datum ctid, Oid tableOid)
{
	TidScan  *scan = (TidScan *)node->ss.ps.plan;
	EState				*estate;
	MemoryContext		oldcontext;
	RemoteQuery 		*plan;
	RemoteQueryState	*pstate;
	TupleTableSlot		*result = NULL;
	ParamListInfo paramLI;
	ParamExternData *prm;
	int	numParams = (OidIsValid(tableOid)) ? 2 : 1;

	/* Build remote query */
	plan = makeNode(RemoteQuery);
	plan->combine_type = COMBINE_TYPE_NONE;
	plan->exec_nodes = makeNode(ExecNodes);
	plan->exec_type = EXEC_ON_DATANODES;
	plan->exec_nodes->nodeList = list_make1_int(nodeid);

	plan->force_autocommit = false;
	plan->scan.plan.targetlist = scan->remote_targets;
	plan->ignore_tuple_desc = true;

	plan->sql_statement = scan->remote_sql;

	/* Use extend protocol to execute */
	paramLI = (ParamListInfo) palloc0(offsetof(ParamListInfoData, params) +
									  sizeof(ParamExternData) * numParams);
	/* we have static list of params, so no hooks needed */
	paramLI->numParams = numParams;
	prm = &paramLI->params[0];
	prm->value = ctid;
	prm->isnull = false;
	prm->pflags = PARAM_FLAG_CONST;
	prm->ptype = TIDOID;

	if (OidIsValid(tableOid))
	{
		prm = &paramLI->params[1];
		prm->value = tableOid;
		prm->isnull = false;
		prm->pflags = PARAM_FLAG_CONST;
		prm->ptype = OIDOID;
	}

	estate = CreateExecutorState();
	oldcontext = MemoryContextSwitchTo(estate->es_query_cxt);
	estate->es_snapshot = GetActiveSnapshot();
	estate->es_param_list_info = paramLI;
	pstate = ExecInitRemoteQuery(plan, estate, 0);
	MemoryContextSwitchTo(oldcontext);

	/* Run the query */
	result = ExecRemoteQuery((PlanState *) pstate);
	if (TupIsNull(result))
	{
		ExecEndRemoteQuery(pstate);
		return NULL;
	}

	slot_getallattrs(result);

	ExecEndRemoteQuery(pstate);
	return result;
}

static int
getStatsLen(AnalyzeRelStats *analyzeStats)
{
	int len = 0;
	int i, j, k;

	len += (strlen(analyzeStats->relname) + 1);      /* relname */
	len += (strlen(analyzeStats->namespacename) + 1);  /* namespace */
	len += (sizeof(int32) * 2 + sizeof(bool) * 3);	 /* class_info_num, relallvisible, relhashindex, inh, resetcounter */
	len += sizeof(float4);									/* rel dead rows*/
	len += (sizeof(int32) * analyzeStats->class_info_num); /* relpages*/
	len += (sizeof(float4) * analyzeStats->class_info_num); /* reltuples */

	len += (sizeof(NameData) * (analyzeStats->class_info_num - 1)); /* indexName */
	len += (sizeof(int32) * (analyzeStats->class_info_num));  /* stat_att_num */
	len += sizeof(int32);  /* stat_info_num */

	for (i = 0; i < analyzeStats->stat_info_num; i++)
	{
		len += (sizeof(int32) * 2);               /* attnum, stawidth */
		len += (sizeof(float4) * 2);              /* stanullfrac, stadistinct */
		len += (sizeof(int16) * STATISTIC_NUM_SLOTS);     /* stakind */
		len += (sizeof(Oid) * STATISTIC_NUM_SLOTS);      /* staop */
		len += (sizeof(int32) * STATISTIC_NUM_SLOTS);   /* numnumbers */
		len += (sizeof(int32) * STATISTIC_NUM_SLOTS);   /* numvalues */
		len += (sizeof(Oid) * STATISTIC_NUM_SLOTS);     /* statypid */
		len += (sizeof(int16) * STATISTIC_NUM_SLOTS);  /* statyplen */
		len += STATISTIC_NUM_SLOTS;                    /* statypbyval */
		len += STATISTIC_NUM_SLOTS;                   /* statypalign */
		for (j = 0; j < STATISTIC_NUM_SLOTS; j++)
		{
			len += (sizeof(float4) * analyzeStats->pgstat[i].numnumbers[j]); /* stanumbers */
			len += (sizeof(int32) * analyzeStats->pgstat[i].numvalues[j]);  /* stavalueslen */
			for (k = 0; k < analyzeStats->pgstat[i].numvalues[j]; k++)
			{
				len += analyzeStats->pgstat[i].stavalueslen[j][k];  /* stavalues */
			}
		}
	}

	len += sizeof(int32);    /* ext_info_num */
	for (i = 0; i < analyzeStats->ext_info_num; i++)
	{
		len += (strlen(analyzeStats->pgstatext[i].statname) + 1);  /* statname */
		len += (sizeof(int32) * 2);       /* distinct_len, depend_len */
		len += analyzeStats->pgstatext[i].distinct_len;  /* distinct */
		len += analyzeStats->pgstatext[i].depend_len;   /* depend */
	}
	return len;
}

/*
 * Send analyze statistic info to other Coordinator
 */
static int
pgxc_node_send_stat(PGXCNodeHandle * handle, AnalyzeRelStats *analyzeStats)
{
	int 		msgLen;
	int			i32;
	int16		i16;
	uint32		u32;
	int			i, j, k;
	Size		size;
	char		type = 'a';
	int 		typeLen = 1;
	union {
		float4 f;
		uint32 i;
	} swap = { .i = 0 };

	msgLen = getStatsLen(analyzeStats);
	msgLen = 4 + typeLen+ msgLen;

	/* msgType + msgLen */
	if (ensure_out_buffer_capacity(handle->outEnd + 1 + msgLen, handle) != 0)
	{
		add_error_message(handle, "out of memory");
		return EOF;
	}

	handle->outBuffer[handle->outEnd++] = 'h';

	/*
	 * size + type + relname + namesapcename + class_info_num + relallvisible +
	 * relhasindex + (class_info_num * relpages) + (class_info_num * reltuples) +
	 * ((class_info_num - 1) * indexName) + stat_info_num +
	 * (class_info_num * attnum) + (stat_info_num * AnalyzePgStats) +
	 * ext_info_num + (ext_info_num * AnalyzeExtStats)
	 */
	msgLen = pg_hton32(msgLen);
	memcpy(handle->outBuffer + handle->outEnd, &msgLen, 4);
	handle->outEnd += 4;

	handle->outBuffer[handle->outEnd++] = type;

	memcpy(handle->outBuffer + handle->outEnd, analyzeStats->relname,
			strlen(analyzeStats->relname));
	handle->outEnd += strlen(analyzeStats->relname);
	handle->outBuffer[handle->outEnd++] = '\0';

	memcpy(handle->outBuffer + handle->outEnd, analyzeStats->namespacename,
			strlen(analyzeStats->namespacename));
	handle->outEnd += strlen(analyzeStats->namespacename);
	handle->outBuffer[handle->outEnd++] = '\0';

	i32 = pg_hton32(analyzeStats->class_info_num);
	memcpy(handle->outBuffer + handle->outEnd, &i32, 4);
	handle->outEnd += 4;
	i32 = pg_hton32(analyzeStats->relallvisible);
	memcpy(handle->outBuffer + handle->outEnd, &i32, 4);
	handle->outEnd += 4;

	if (analyzeStats->relhasindex)
		handle->outBuffer[handle->outEnd++] = 'Y';
	else
		handle->outBuffer[handle->outEnd++] = 'N';

	if (analyzeStats->inh)
		handle->outBuffer[handle->outEnd++] = 'Y';
	else
		handle->outBuffer[handle->outEnd++] = 'N';

	if (analyzeStats->resetcounter)
		handle->outBuffer[handle->outEnd++] = 'Y';
	else
		handle->outBuffer[handle->outEnd++] = 'N';

	swap.f = analyzeStats->deadrows;
	swap.i = pg_hton32(swap.i);
	memcpy(handle->outBuffer + handle->outEnd, (char *) &swap.i, 4);
	handle->outEnd += 4;

	for (i = 0; i < analyzeStats->class_info_num; i++)
	{
		i32 = pg_hton32(analyzeStats->relpages[i]);
		memcpy(handle->outBuffer + handle->outEnd, &i32, 4);
		handle->outEnd += 4;
	}
	for (i = 0; i < analyzeStats->class_info_num; i++)
	{
		swap.f = analyzeStats->reltuples[i];
		swap.i = pg_hton32(swap.i);
		memcpy(handle->outBuffer + handle->outEnd, (char *) &swap.i, 4);
		handle->outEnd += 4;
	}

	for (i = 0; i < analyzeStats->class_info_num - 1; i++)
	{
		memcpy(handle->outBuffer + handle->outEnd, analyzeStats->indexName[i].data,
				NAMEDATALEN);
		handle->outEnd += NAMEDATALEN;
	}

	i32 = pg_hton32(analyzeStats->stat_info_num);
	memcpy(handle->outBuffer + handle->outEnd, &i32, 4);
	handle->outEnd += 4;

	for (i = 0; i < analyzeStats->class_info_num; i++)
	{
		i32 = pg_hton32(analyzeStats->stat_att_num[i]);
		memcpy(handle->outBuffer + handle->outEnd, &i32, 4);
		handle->outEnd += 4;
	}

	for (i = 0; i < analyzeStats->stat_info_num; i++)
	{
		i32 = pg_hton32(analyzeStats->pgstat[i].attnum);
		memcpy(handle->outBuffer + handle->outEnd, &i32, 4);
		handle->outEnd += 4;

		swap.f = analyzeStats->pgstat[i].stanullfrac;
		swap.i = pg_hton32(swap.i);
		memcpy(handle->outBuffer + handle->outEnd, (char *) &swap.i, 4);
		handle->outEnd += 4;

		i32 = pg_hton32(analyzeStats->pgstat[i].stawidth);
		memcpy(handle->outBuffer + handle->outEnd, &i32, 4);
		handle->outEnd += 4;

		swap.f = analyzeStats->pgstat[i].stadistinct;
		swap.i = pg_hton32(swap.i);
		memcpy(handle->outBuffer + handle->outEnd, (char *) &swap.i, 4);
		handle->outEnd += 4;

		for (j = 0 ; j < STATISTIC_NUM_SLOTS; j++)
		{
			i16 = pg_hton16(analyzeStats->pgstat[i].stakind[j]);
			memcpy(handle->outBuffer + handle->outEnd, &i16, 2);
			handle->outEnd += 2;
		}

		for (j = 0 ; j < STATISTIC_NUM_SLOTS; j++)
		{
			u32 = pg_hton32(analyzeStats->pgstat[i].staop[j]);
			memcpy(handle->outBuffer + handle->outEnd, &u32, 4);
			handle->outEnd += 4;
		}

		for (j = 0 ; j < STATISTIC_NUM_SLOTS; j++)
		{
			i32 = pg_hton32(analyzeStats->pgstat[i].numnumbers[j]);
			memcpy(handle->outBuffer + handle->outEnd, &i32, 4);
			handle->outEnd += 4;
		}

		for (j = 0 ; j < STATISTIC_NUM_SLOTS; j++)
		{
			i32 = pg_hton32(analyzeStats->pgstat[i].numvalues[j]);
			memcpy(handle->outBuffer + handle->outEnd, &i32, 4);
			handle->outEnd += 4;
		}

		for (j = 0 ; j < STATISTIC_NUM_SLOTS; j++)
		{
			u32 = pg_hton32(analyzeStats->pgstat[i].statypid[j]);
			memcpy(handle->outBuffer + handle->outEnd, &u32, 4);
			handle->outEnd += 4;
		}

		for (j = 0 ; j < STATISTIC_NUM_SLOTS; j++)
		{
			i16 = pg_hton16(analyzeStats->pgstat[i].statyplen[j]);
			memcpy(handle->outBuffer + handle->outEnd, &i16, 2);
			handle->outEnd += 2;
		}

		for (j = 0 ; j < STATISTIC_NUM_SLOTS; j++)
		{
			if (analyzeStats->pgstat[i].statypbyval[j])
				handle->outBuffer[handle->outEnd++] = 'Y';
			else
				handle->outBuffer[handle->outEnd++] = 'N';
		}

		for (j = 0 ; j < STATISTIC_NUM_SLOTS; j++)
		{
			handle->outBuffer[handle->outEnd++] = analyzeStats->pgstat[i].statypalign[j];
		}

		for (j = 0 ; j < STATISTIC_NUM_SLOTS; j++)
		{
			if (analyzeStats->pgstat[i].numnumbers[j] > 0)
			{
				for (k = 0; k < analyzeStats->pgstat[i].numnumbers[j]; k++)
				{
					swap.f = analyzeStats->pgstat[i].stanumbers[j][k];
					swap.i = pg_hton32(swap.i);
					memcpy(handle->outBuffer + handle->outEnd, (char *) &swap.i, 4);
					handle->outEnd += 4;
				}
			}
		}

		for (j = 0 ; j < STATISTIC_NUM_SLOTS; j++)
		{
			if (analyzeStats->pgstat[i].numvalues[j] > 0)
			{
				for (k = 0; k < analyzeStats->pgstat[i].numvalues[j]; k++)
				{
					i32 = pg_hton32(analyzeStats->pgstat[i].stavalueslen[j][k]);
					memcpy(handle->outBuffer + handle->outEnd, &i32, 4);
					handle->outEnd += 4;
				}

				for (k = 0; k < analyzeStats->pgstat[i].numvalues[j]; k++)
				{
					Datum datum = analyzeStats->pgstat[i].stavalues[j][k];
					int typlen = analyzeStats->pgstat[i].statyplen[j];
					if (analyzeStats->pgstat[i].statypbyval[j])
					{
						store_att_byval(handle->outBuffer + handle->outEnd, datum, typlen);
						handle->outEnd += typlen;
					}
					else
					{
						size = copyDatum(handle->outBuffer + handle->outEnd, datum,
										analyzeStats->pgstat[i].statypbyval[j],
										analyzeStats->pgstat[i].statyplen[j],
										analyzeStats->pgstat[i].stavalueslen[j][k]);

						handle->outEnd += size;
					}
				}
			}
		}
	}

	i32 = pg_hton32(analyzeStats->ext_info_num);
	memcpy(handle->outBuffer + handle->outEnd, &i32, 4);
	handle->outEnd += 4;

	for (i = 0; i < analyzeStats->ext_info_num; i++)
	{
		memcpy(handle->outBuffer + handle->outEnd, analyzeStats->pgstatext[i].statname,
				strlen(analyzeStats->pgstatext[i].statname));
		handle->outEnd += (strlen(analyzeStats->pgstatext[i].statname));
		handle->outBuffer[handle->outEnd++] = '\0';

		i32 = pg_hton32(analyzeStats->pgstatext[i].distinct_len);
		memcpy(handle->outBuffer + handle->outEnd, &i32, 4);
		handle->outEnd += 4;

		if (analyzeStats->pgstatext[i].distinct_len != 0)
		{
			memcpy(handle->outBuffer + handle->outEnd, analyzeStats->pgstatext[i].distinct,
					analyzeStats->pgstatext[i].distinct_len);
			handle->outEnd += analyzeStats->pgstatext[i].distinct_len;
		}

		i32 = pg_hton32(analyzeStats->pgstatext[i].depend_len);
		memcpy(handle->outBuffer + handle->outEnd, &i32, 4);
		handle->outEnd += 4;

		if (analyzeStats->pgstatext[i].depend_len != 0)
		{
			memcpy(handle->outBuffer + handle->outEnd, analyzeStats->pgstatext[i].depend,
					analyzeStats->pgstatext[i].depend_len);
			handle->outEnd += analyzeStats->pgstatext[i].depend_len;
		}
	}

	SetTransitionInExtendedQuery(handle, true);
	return SendRequest(handle);
}

void
ExecSendStats(AnalyzeRelStats *analyzeStats)
{
	PGXCNodeAllHandles *pgxc_connections;
	PGXCNodeHandle *connections = NULL;
	ResponseCombiner combiner;
	int		i, coor_count;
	Snapshot		snapshot = GetActiveSnapshot();

	pgxc_connections = get_exec_connections(NULL, NULL, EXEC_ON_COORDS, false);
	coor_count = pgxc_connections->co_conn_count;

	if (coor_count == 0)
	{
		pfree_pgxc_all_handles(pgxc_connections);
		return;
	}

	for (i = 0; i < coor_count; i++)
	{
		connections = pgxc_connections->coord_handles[i];
		if (snapshot && pgxc_node_send_snapshot(connections, snapshot))
			ereport(ERROR,
					(errcode(ERRCODE_CONNECTION_EXCEPTION),
							errmsg("Failed to send snapshot"),
							errnode(connections)));

		if (pgxc_node_send_cmd_id(connections, GetCurrentCommandId(false)))
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
							errmsg("Failed to send command ID"),
								   errnode(connections)));

		if (pgxc_node_send_stat(connections, analyzeStats))
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
							errmsg("Failed to send stat info"),
							errnode(connections)));
	}

	InitResponseCombiner(&combiner, coor_count, COMBINE_TYPE_NONE);

	for (i = 0; i < coor_count; i++)
	{
		PGXCNodeHandle *handle;
		int res = 0;

		handle = pgxc_connections->coord_handles[i];

		while (true)
		{
			if (pgxc_node_receive(1, &handle, NULL))
			{
				ereport(WARNING,
					(errmsg("Failed to receive from %s in ExecSendStats",
						pgxc_connections->coord_handles[i]->nodename)));
				break;
			}

			res = handle_response(handle, &combiner);
			if (res == RESPONSE_EOF)
			{
				if (combiner.errorMessage)
				{
					ereport(WARNING,
							(errmsg("%s: failed to update the statistics, %s",
							handle->nodename, combiner.errorMessage)));
				}
				if (IsConnectionStateFatal(handle))
					break;
				else
					continue;
			}
			else if (res == RESPONSE_ERROR)
			{
				break;
			}
			else if (res == RESPONSE_COMPLETE)
			{
				ereport(WARNING,
					(errmsg("%s: didn't get the relation lock, didn't update the statistics",
						pgxc_connections->coord_handles[i]->nodename)));
				break;
			}
			else if (res == ERSPONSE_ANALYZE)
			{
				ereport(LOG,
					(errmsg("%s: update the statistics success",
						pgxc_connections->coord_handles[i]->nodename)));
				break;
			}
		}
	}

	if (combiner.errorMessage)
		pgxc_node_report_error(&combiner);

	CloseCombiner(&combiner);
	pfree_pgxc_all_handles(pgxc_connections);
}

static int
getSpmPlanSendLen(LocalSPMPlanInfo *spmplan)
{
	int len = 0;
	int	nvalues;
	int	arrlen;
	int16 nrels;
	int16 nindexes;

	len += NAMEDATALEN;		/* name */
	len += sizeof(Oid);		/* namespace */
	len += sizeof(Oid);		/* ownerid */
	len += sizeof(int64);	/* sql_id */
	len += sizeof(int64);	/* plan_id */
	len += sizeof(int64);	/* hint_id */
	len += spmplan->hint_len;	/* hint */
	len += sizeof(int32);
	len += spmplan->hint_detail_len;	/* hint_detail */
	len += sizeof(int32);
	len += spmplan->guc_list_len;	/* guc_list */
	len += sizeof(int32);
	len += sizeof(int32);	/* priority */
	len += sizeof(bool);	/* enabled */
	len += sizeof(bool);	/* autopurge */
	len += sizeof(int64);	/* executions */
	len += sizeof(int64);	/* created */
	len += sizeof(int64);	/* last_modified */;
	len += sizeof(int64);	/* last_verified */
	len += sizeof(int64);	/* execute_time */;
	len += sizeof(int64);	/* version */;
	len += sizeof(int64);	/* elapsed_time */
	len += sizeof(int64);	/* task_id */;
	len += sizeof(int32);	/* sql_normalize_rule */
	len += sizeof(float4);	/* cost */
	len += sizeof(int16);   /* nrels */
	len += sizeof(int16);   /* nindexes */
	nindexes = spmplan->indexoids ? spmplan->indexoids->dim1 : 0;
	nrels = spmplan->reloids ? spmplan->reloids->dim1 : 0;

	len += spmplan->sql_len; /* sql */
	len += sizeof(int32);
	len += spmplan->sql_normalize_len; /* sql_normalize */
	len += sizeof(int32);
	len += spmplan->plan_len; 	/* plan */
	len += sizeof(int32);
	len += spmplan->param_list_len;	/* param_list */
	len += sizeof(int32);

	if (nrels > 0)
	{
		ArrayToList(spmplan->relnames, &nvalues, &arrlen, false);
		len += arrlen;	/* relnames */
	}

	if (nindexes > 0)
	{
		ArrayToList(spmplan->indexnames, &nvalues, &arrlen, false);
		len += arrlen;	/* indexnames */
	}
	len += spmplan->description_len;	/* description */
	len += sizeof(int32);
	len += NAMEDATALEN;	/* origin */
	return len;
}

/*
 * Send spm plan info to other Coordinator
 */
static int
pgxc_node_send_spm_plan(PGXCNodeHandle * handle, LocalSPMPlanInfo *spmplan)
{
	int 		msgLen;
	int			i32;
	int16		i16;
	int16		nrels;
	int16		nindexes;
	uint32		u32;
	int64		i64;
	int			i;
	char		type = 's';		/* spm plan */
	int 		typeLen = 1;
	char		**arr;
	int			arr_nvalues = 0;
	int 		arr_totallen = 0;

	union {
		float4 f;
		uint32 i;
	} swap = { .i = 0 };

	msgLen = getSpmPlanSendLen(spmplan);
	msgLen = 4 + typeLen + msgLen;

	/* msgType + msgLen */
	if (ensure_out_buffer_capacity(handle->outEnd + 1 + msgLen, handle) != 0)
	{
		add_error_message(handle, "out of memory");
		return EOF;
	}

	handle->outBuffer[handle->outEnd++] = 'h';

	msgLen = pg_hton32(msgLen);
	memcpy(handle->outBuffer + handle->outEnd, &msgLen, 4);
	handle->outEnd += 4;

	/* type */
	handle->outBuffer[handle->outEnd++] = type;
	/* name */
	memcpy(handle->outBuffer + handle->outEnd, spmplan->spmname.data,
				NAMEDATALEN);
	handle->outEnd += NAMEDATALEN;
	/* namespace */
	u32 = pg_hton32(spmplan->spmnsp);
	memcpy(handle->outBuffer + handle->outEnd, &u32, sizeof(Oid));
	handle->outEnd += sizeof(Oid);
	/* ownerid */
	u32 = pg_hton32(spmplan->ownerid);
	memcpy(handle->outBuffer + handle->outEnd, &u32, sizeof(Oid));
	handle->outEnd += sizeof(Oid);
	/* sql_id */
	i64 = pg_hton64(spmplan->sql_id);
	memcpy(handle->outBuffer + handle->outEnd, &i64, sizeof(int64));
	handle->outEnd += sizeof(int64);
	/* plan_id */
	i64 = pg_hton64(spmplan->plan_id);
	memcpy(handle->outBuffer + handle->outEnd, &i64, sizeof(int64));
	handle->outEnd += sizeof(int64);
	/* hint_id */
	i64 = pg_hton64(spmplan->hint_id);
	memcpy(handle->outBuffer + handle->outEnd, &i64, sizeof(int64));
	handle->outEnd += sizeof(int64);
	/* hint */
	i32 = pg_hton32(spmplan->hint_len);
	memcpy(handle->outBuffer + handle->outEnd, &i32, 4);
	handle->outEnd += 4;
	memcpy(handle->outBuffer + handle->outEnd, spmplan->hint,
			spmplan->hint_len);
	handle->outEnd += spmplan->hint_len;
	/* hint_detail */
	i32 = pg_hton32(spmplan->hint_detail_len);
	memcpy(handle->outBuffer + handle->outEnd, &i32, 4);
	handle->outEnd += 4;
	memcpy(handle->outBuffer + handle->outEnd, spmplan->hint_detail,
			spmplan->hint_detail_len);
	handle->outEnd += spmplan->hint_detail_len;

	/* guc_list */
	i32 = pg_hton32(spmplan->guc_list_len);
	memcpy(handle->outBuffer + handle->outEnd, &i32, 4);
	handle->outEnd += 4;
	memcpy(handle->outBuffer + handle->outEnd, spmplan->guc_list,
			spmplan->guc_list_len);
	handle->outEnd += spmplan->guc_list_len;

	/* priority */
	i32 = pg_hton32(spmplan->priority);
	memcpy(handle->outBuffer + handle->outEnd, &i32, 4);
	handle->outEnd += 4;
	/* enabled */
	if (spmplan->enabled)
		handle->outBuffer[handle->outEnd++] = 'Y';
	else
		handle->outBuffer[handle->outEnd++] = 'N';
	/* autopurge */
	if (spmplan->autopurge)
		handle->outBuffer[handle->outEnd++] = 'Y';
	else
		handle->outBuffer[handle->outEnd++] = 'N';
	/* executions */
	i64 = pg_hton64(spmplan->executions);
	memcpy(handle->outBuffer + handle->outEnd, &i64, sizeof(int64));
	handle->outEnd += sizeof(int64);
	/* created */
	i64 = pg_hton64(spmplan->created);
	memcpy(handle->outBuffer + handle->outEnd, &i64, sizeof(int64));
	handle->outEnd += sizeof(int64);
	/* last_modified */
	i64 = pg_hton64(spmplan->last_modified);
	memcpy(handle->outBuffer + handle->outEnd, &i64, sizeof(int64));
	handle->outEnd += sizeof(int64);
	/* last_verified */
	i64 = pg_hton64(spmplan->last_verified);
	memcpy(handle->outBuffer + handle->outEnd, &i64, sizeof(int64));
	handle->outEnd += sizeof(int64);
	/* execute_time */
	i64 = pg_hton64(spmplan->execute_time);
	memcpy(handle->outBuffer + handle->outEnd, &i64, sizeof(int64));
	handle->outEnd += sizeof(int64);
	/* version */
	i64 = pg_hton64(spmplan->version);
	memcpy(handle->outBuffer + handle->outEnd, &i64, sizeof(int64));
	handle->outEnd += sizeof(int64);
	/* elapsed_time */
	i64 = pg_hton64(spmplan->elapsed_time);
	memcpy(handle->outBuffer + handle->outEnd, &i64, sizeof(int64));
	handle->outEnd += sizeof(int64);
	/* task_id */
	i64 = pg_hton64(spmplan->task_id);
	memcpy(handle->outBuffer + handle->outEnd, &i64, sizeof(int64));
	handle->outEnd += sizeof(int64);
	/* sql_normalize_rule */
	i32 = pg_hton32(spmplan->sql_normalize_rule);
	memcpy(handle->outBuffer + handle->outEnd, &i32, 4);
	handle->outEnd += 4;
	/* cost */
	swap.f = spmplan->cost;
	swap.i = pg_hton32(swap.i);
	memcpy(handle->outBuffer + handle->outEnd, (char *) &swap.i, 4);
	handle->outEnd += 4;
	/* nrels */
	nrels = (spmplan->reloids) ? spmplan->reloids->dim1 : 0;
	i16 = pg_hton16(nrels);
	memcpy(handle->outBuffer + handle->outEnd, &i16, sizeof(int16));
	handle->outEnd += sizeof(int16);
	/* nindexes */
	nindexes = spmplan->indexoids ?  spmplan->indexoids->dim1 : 0;
	i16 = pg_hton16(nindexes);
	memcpy(handle->outBuffer + handle->outEnd, &i16, sizeof(int16));
	handle->outEnd += sizeof(int16);

	/* sql_normalize */
	i32 = pg_hton32(spmplan->sql_normalize_len);
	memcpy(handle->outBuffer + handle->outEnd, &i32, 4);
	handle->outEnd += 4;
	memcpy(handle->outBuffer + handle->outEnd, spmplan->sql_normalize,
			spmplan->sql_normalize_len);
	handle->outEnd += spmplan->sql_normalize_len;
	/* sql */
	i32 = pg_hton32(spmplan->sql_len);
	memcpy(handle->outBuffer + handle->outEnd, &i32, 4);
	handle->outEnd += 4;
	memcpy(handle->outBuffer + handle->outEnd, spmplan->sql,
			spmplan->sql_len);
	handle->outEnd += spmplan->sql_len;
	/* plan */
	i32 = pg_hton32(spmplan->plan_len);
	memcpy(handle->outBuffer + handle->outEnd, &i32, 4);
	handle->outEnd += 4;
	memcpy(handle->outBuffer + handle->outEnd, spmplan->plan,
			spmplan->plan_len);
	handle->outEnd += spmplan->plan_len;
	/* param_list */
	i32 = pg_hton32(spmplan->param_list_len);
	memcpy(handle->outBuffer + handle->outEnd, &i32, 4);
	handle->outEnd += 4;
	memcpy(handle->outBuffer + handle->outEnd, spmplan->param_list,
			spmplan->param_list_len);
	handle->outEnd += spmplan->param_list_len;

	/* relnames */
	if (nrels)
	{
		arr = ArrayToList(spmplan->relnames, &arr_nvalues, &arr_totallen, true);
		Assert(arr_nvalues == nrels);
		for (i = 0; i < arr_nvalues; i++)
		{
			memcpy(handle->outBuffer + handle->outEnd, arr[i], strlen(arr[i]));
			handle->outEnd += strlen(arr[i]);
			handle->outBuffer[handle->outEnd++] = '\0';
			pfree(arr[i]);
		}
		pfree(arr);
	}

	/* indexnames */
	if (nindexes)
	{
		arr = ArrayToList(spmplan->indexnames, &arr_nvalues, &arr_totallen, true);
		Assert(arr_nvalues == nindexes);
		for (i = 0; i < arr_nvalues; i++)
		{
			memcpy(handle->outBuffer + handle->outEnd, arr[i], strlen(arr[i]));
			handle->outEnd += strlen(arr[i]);
			handle->outBuffer[handle->outEnd++] = '\0';
			pfree(arr[i]);
		}
		pfree(arr);
	}

	/* description */
	i32 = pg_hton32(spmplan->description_len);
	memcpy(handle->outBuffer + handle->outEnd, &i32, 4);
	handle->outEnd += 4;
	memcpy(handle->outBuffer + handle->outEnd, spmplan->description,
			spmplan->description_len);
	handle->outEnd += spmplan->description_len;
	/* origin */
	memcpy(handle->outBuffer + handle->outEnd, spmplan->origin.data,
				NAMEDATALEN);
	handle->outEnd += NAMEDATALEN;

	handle->state.in_extended_query = true;

	return SendRequest(handle);
}

/*
 * Send spm plan delete info to other Coordinator
 */
static int
pgxc_node_send_spm_plan_delete(PGXCNodeHandle *handle, LocalSPMPlanInfo *spmplan)
{
	int 		msgLen;
	int64		i64;
	char		type = 'd';		/* spm plan */
	int 		typeLen = 1;

	msgLen = sizeof(int64) * 2; /* sql_id, plan_id */
	msgLen = 4 + typeLen + msgLen;

	/* msgType + msgLen */
	if (ensure_out_buffer_capacity(handle->outEnd + 1 + msgLen, handle) != 0)
	{
		add_error_message(handle, "out of memory");
		return EOF;
	}

	handle->outBuffer[handle->outEnd++] = 'h';

	msgLen = pg_hton32(msgLen);
	memcpy(handle->outBuffer + handle->outEnd, &msgLen, 4);
	handle->outEnd += 4;

	/* type */
	handle->outBuffer[handle->outEnd++] = type;
	/* sql_id */
	i64 = pg_hton64(spmplan->sql_id);
	memcpy(handle->outBuffer + handle->outEnd, &i64, sizeof(int64));
	handle->outEnd += sizeof(int64);
	/* plan_id */
	i64 = pg_hton64(spmplan->plan_id);
	memcpy(handle->outBuffer + handle->outEnd, &i64, sizeof(int64));
	handle->outEnd += sizeof(int64);

	handle->state.in_extended_query = true;

	return SendRequest(handle);
}

/*
 * Send spm plan update info to other Coordinator
 */
static int
pgxc_node_send_spm_plan_update(PGXCNodeHandle *handle, int64 sql_id,
								int64 plan_id, char *attribute_name,
								char *attribute_value)
{
	int 		msgLen = 0;
	int64		i64;
	char		type = 'u';		/* spm plan */
	int 		typeLen = 1;

	msgLen += sizeof(int64) * 2; /* sql_id, plan_id */
	msgLen += strlen(attribute_name) + 1;	/* attribute_name */
	msgLen += strlen(attribute_value) + 1;	/* attribute_value */
	msgLen = 4 + typeLen + msgLen;

	/* msgType + msgLen */
	if (ensure_out_buffer_capacity(handle->outEnd + 1 + msgLen, handle) != 0)
	{
		add_error_message(handle, "out of memory");
		return EOF;
	}

	handle->outBuffer[handle->outEnd++] = 'h';

	msgLen = pg_hton32(msgLen);
	memcpy(handle->outBuffer + handle->outEnd, &msgLen, 4);
	handle->outEnd += 4;

	/* type */
	handle->outBuffer[handle->outEnd++] = type;
	/* sql_id */
	i64 = pg_hton64(sql_id);
	memcpy(handle->outBuffer + handle->outEnd, &i64, sizeof(int64));
	handle->outEnd += sizeof(int64);
	/* plan_id */
	i64 = pg_hton64(plan_id);
	memcpy(handle->outBuffer + handle->outEnd, &i64, sizeof(int64));
	handle->outEnd += sizeof(int64);
	/* attribute_name */
	memcpy(handle->outBuffer + handle->outEnd, attribute_name, strlen(attribute_name));
	handle->outEnd += strlen(attribute_name);
	handle->outBuffer[handle->outEnd++] = '\0';
	/* attribute_value */
	memcpy(handle->outBuffer + handle->outEnd, attribute_value, strlen(attribute_value));
	handle->outEnd += strlen(attribute_value);
	handle->outBuffer[handle->outEnd++] = '\0';

	handle->state.in_extended_query = true;

	return SendRequest(handle);
}

void
ExecSendSPMPlan(LocalSPMPlanInfo *spmplan, SPMPlanSendType type, char *attribute_name, char *attribute_value)
{
	PGXCNodeAllHandles *pgxc_connections;
	PGXCNodeHandle     *connections = NULL;
	ResponseCombiner    combiner;
	GlobalTransactionId gxid;
	int                 i, coor_count;
	CommandId           cid;

	pgxc_connections = get_exec_connections(NULL, NULL, EXEC_ON_COORDS, true);
	coor_count = pgxc_connections->co_conn_count;

	if (coor_count == 0)
	{
		pfree_pgxc_all_handles(pgxc_connections);
		return;
	}

	gxid = GetCurrentTransactionId();
	cid = GetCurrentCommandId(true);
	/* Start transaction on connections where it is not started */
	if (pgxc_node_begin(coor_count, pgxc_connections->coord_handles, gxid, true, true))
	{
		ereport(ERROR,
				(errcode(ERRCODE_CONNECTION_EXCEPTION),
				errmsg("Could not begin transaction on data nodes.")));
	}

	for (i = 0; i < coor_count; i++)
	{
		connections = pgxc_connections->coord_handles[i];

		if (pgxc_node_send_cmd_id(connections, cid) < 0)
			ereport(ERROR,
			        (errcode(ERRCODE_CONNECTION_EXCEPTION),
			         errmsg("Failed to send command ID"),
			         errnode(connections)));

		switch (type)
		{
			case SPM_PLAN_SEND_PLAN:
				{
					if (pgxc_node_send_spm_plan(connections, spmplan))
					{
						ereport(ERROR,
							(errmsg("Failed to send spm plan info to CN: %s",
								connections->nodename)));
					}
				}
				break;
			case SPM_PLAN_SEND_PLAN_DELETE:
				{
					if (pgxc_node_send_spm_plan_delete(connections, spmplan))
					{
						ereport(ERROR,
							(errmsg("Failed to send spm plan delete info to CN: %s",
								connections->nodename)));
					}
				}
				break;
			case SPM_PLAN_SEND_PLAN_UPDATE:
				{
					if (pgxc_node_send_spm_plan_update(connections,
														spmplan->sql_id,
														spmplan->plan_id,
														attribute_name,
														attribute_value))
					{
						ereport(ERROR,
							(errmsg("Failed to send spm plan update info to CN: %s",
								connections->nodename)));
					}
				}
				break;
			default:
				break;
		}
	}

	InitResponseCombiner(&combiner, coor_count, COMBINE_TYPE_NONE);

	for (i = 0; i < coor_count; i++)
	{
		PGXCNodeHandle *handle;
		int res = 0;

		handle = pgxc_connections->coord_handles[i];

		while (true)
		{
			if (pgxc_node_receive(1, &handle, NULL))
			{
				ereport(WARNING,
					(errmsg("Failed to receive from %s in ExecSendSPMPlan",
						pgxc_connections->coord_handles[i]->nodename)));
				break;
			}

			res = handle_response(handle, &combiner);
			if (res == RESPONSE_EOF)
			{
				if (combiner.errorMessage)
				{
					ereport(WARNING,
							(errmsg("%s: failed to update spm plan, %s",
							handle->nodename, combiner.errorMessage)));
				}

				if (IsConnectionStateError(handle) || IsConnectionStateFatal(handle))
					break;
				else
					continue;
			}
			else if (res == RESPONSE_ERROR || res == RESPONSE_COMPLETE)
			{
				/* pgxc_node_report_error will report error, no need here */
				break;
			}
			else if (res == ERSPONSE_ANALYZE)
			{
				ereport(LOG,
					(errmsg("%s: update spm plan table success",
						pgxc_connections->coord_handles[i]->nodename)));
				break;
			}
		}
	}

	if (combiner.errorMessage)
		pgxc_node_report_error(&combiner);

	CloseCombiner(&combiner);
	pfree_pgxc_all_handles(pgxc_connections);
}

static ExecNodes *
paramGetExecNodes(ExecNodes *origin, PlanState *planstate)
{
	/* execution time determining of target Datanodes */
	ExecNodes *nodes;
	ExprState *estate;
#ifdef __OPENTENBASE_C__
	bool *disisnulls = NULL;
	Datum *disValues = NULL;
#endif
	ExprContext		*econtext = planstate->ps_ExprContext;
	RelationAccessType accessType = origin->accesstype;
	MemoryContext	 oldcontext;
	RelationLocInfo *rel_loc_info;

	oldcontext = MemoryContextSwitchTo(econtext->ecxt_per_tuple_memory);

	rel_loc_info = GetRelationLocInfo(origin->en_relid);
	disValues = (Datum *)palloc(sizeof(Datum) * rel_loc_info->nDisAttrs);
	disisnulls = (bool *)palloc(sizeof(bool) * rel_loc_info->nDisAttrs);

	if (rel_loc_info->nDisAttrs == 1)
	{
		if (origin->rewrite_done)
		{
			accessType = RELATION_ACCESS_INSERT;
			disValues[0] = origin->rewrite_value;
			disisnulls[0] = origin->isnull;
		}
		else
		{
			/*
			* Simplify constant expressions before evaluating exec_nodes->en_expr.
			* The same work in preprocess_targetlist.
			*/
			Expr *expr = (Expr *) eval_const_expressions_with_params(
				planstate->state->es_param_list_info, (Node *) origin->dis_exprs[0]);

			/*
			 * Force accessType to RELATION_ACCESS_INSERT to ensure this
			 * query executed on a single node if we have a const dist-expr
			 */
			if (IsA(expr, Const))
				accessType = RELATION_ACCESS_INSERT;

			/* Fill in opfuncid values if missing */
			fix_opfuncids((Node *) expr);

			estate = ExecInitExpr(expr, planstate);
			/* For explain, no need to execute expr. */
			if (!(planstate->state->es_top_eflags & EXEC_FLAG_EXPLAIN_ONLY))
				disValues[0] = ExecEvalExpr(estate, econtext, &disisnulls[0]);
		}
	}
	else
	{
		int i = 0;
		for (i = 0; i < rel_loc_info->nDisAttrs; i++)
		{
			if (origin->dis_exprs && origin->dis_exprs[i])
			{
				/*
				* Simplify constant expressions before evaluating exec_nodes->en_expr.
				* The same work in preprocess_targetlist.
				*/
				Expr *expr = (Expr *) eval_const_expressions_with_params(
					planstate->state->es_param_list_info, (Node *) origin->dis_exprs[i]);

				/* Fill in opfuncid values if missing */
				fix_opfuncids((Node *) expr);

				estate = ExecInitExpr(expr, planstate);
				disValues[i] = ExecEvalExpr(estate, econtext, &disisnulls[i]);
			}
			else
			{
				disisnulls[i] = true;
				disValues[i] = 0;
			}
		}
	}

	if (planstate->state->es_top_eflags & EXEC_FLAG_EXPLAIN_ONLY)
		nodes = GetRelationNodesForExplain(rel_loc_info, accessType);
	else
		/* PGXCTODO what is the type of partvalue here */
		nodes = GetRelationNodes(rel_loc_info,
#ifdef __OPENTENBASE_C__
								 disValues, disisnulls, rel_loc_info->nDisAttrs,
#endif
								 accessType);
	/*
	 * en_expr is set by pgxc_set_en_expr only for distributed
	 * relations while planning DMLs, hence a select for update
	 * on a replicated table here is an assertion
	 */
	Assert(!(nodes->accesstype == RELATION_ACCESS_READ_FOR_UPDATE &&
			 IsRelationReplicated(rel_loc_info)));

	FreeRelationLocInfo(rel_loc_info);
	if (disValues)
		pfree(disValues);
	if (disisnulls)
		pfree(disisnulls);

	MemoryContextSwitchTo(oldcontext);

	return nodes;
}


/*
 * Reveive dn message on proxy.
 * Forward the dn message to client and forward the client reply message to dn.
 */
int pgxc_node_receive_on_proxy(PGXCNodeHandle *handle)
{
	int result = 0;
	ResponseCombiner combiner;

	struct timeval timeout;
	timeout.tv_sec = 1;
	timeout.tv_usec = 0;

	MemSet(&combiner, 0, sizeof(ResponseCombiner));

	InitResponseCombiner(&combiner, 1, COMBINE_TYPE_NONE);

	/* Receive responses */
	result = pgxc_node_receive_responses(1, &handle, &timeout, &combiner);
	if (result != 0)
	{
		elog(LOG, "Proxy receive responses result is %d", result);
		return result;
	}

	CloseCombiner(&combiner);
	return result;
}

/*
 * Handle reply message on proxy.
 * Forward the client reply message to dn.
 */
int handle_reply_msg_on_proxy(PGXCNodeHandle *conn)
{
	int ret = 0;
	unsigned char firstchar;
	StringInfoData msg;

	Assert(IS_PGXC_COORDINATOR);

	initStringInfo(&msg);

	for (;;)
	{
		pq_startmsgread();
		ret = pq_getbyte_if_available(&firstchar);
		if (ret < 0)
		{
			/* Unexpected error or EOF */
			ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
					errmsg("unexpected EOF on proxy for %s", proxy_for_dn)));
		}

		if (ret == 0)
		{
			/* No data available without blocking */
			pq_endmsgread();
			break;
		}

		/* Read the message contents */
		if (pq_getmessage(&msg, 0))
		{
			ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
					errmsg("unexpected EOF on proxy for %s", proxy_for_dn)));
		}

		elog(DEBUG2, "%s proxy firstchar is %c(%d), reply message length: %d",
			proxy_for_dn, firstchar, firstchar, msg.len);

		ret = pgxc_node_send_on_proxy(conn, firstchar, &msg);
		if (ret != 0)
		{
			ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
					errmsg("%s proxy send reply message error: %d",
						proxy_for_dn, ret)));
		}

		/* Handle the very limited subset of commands expected in this phase */
		switch (firstchar)
		{
			/*
			 * 'd' means a client reply message.
			 */
			case 'd':
				break;

			/*
			 * 'c' means the client requested to finish streaming.
			 */
			case 'c':
				elog(LOG, "%s proxy: reply message type %c(%d), "
					"the client requested to finish streaming",
					proxy_for_dn, firstchar, firstchar);

				/* When replicate stream is closed, set stream_closed to true */
				conn->stream_closed = true;

				break;

			/*
			 * 'X' means the client is closing down the socket.
			 */
			case 'X':
				elog(LOG, "%s proxy: reply message type %c(%d), "
					"the client is closing down the socket",
					proxy_for_dn, firstchar, firstchar);

				proc_exit(0);

			default:
				elog(FATAL, "%s proxy: unexpected message type %c(%d), length: %d",
					proxy_for_dn, firstchar, firstchar, msg.len);
				break;
		}
	}

	return ret;
}

/*
 * Read next message from the connection and update
 * connection state accordingly on the proxy
 * If we are in an error state we just consume the messages, and do not proxy
 * Long term, we should look into cancelling executing statements
 * and closing the connections.
 * It returns if states need to be handled
 * Return values:
 * RESPONSE_EOF - need to receive more data for the connection
 * RESPONSE_READY - got ReadyForQuery
 * RESPONSE_COMPLETE - done with the connection, but not yet ready for query.
 * Also this result is output in case of error
 * RESPONSE_TUPLEDESC - got tuple description
 * RESPONSE_DATAROW - got data row
 */
int
handle_response_on_proxy(PGXCNodeHandle *conn, ResponseCombiner *combiner)
{
	char *msg;
	int  msg_len;
	char msg_type;
	int  ret = 0;
	StringInfoData buf;

	/* proxy must be cn */
	Assert(IS_PGXC_COORDINATOR);

	/* proxy must be not in extended query */
	Assert(!conn->state.in_extended_query);
	Assert(!combiner->extended_query);

	for (;;)
	{
		/*
		 * If we are in the process of shutting down, we
		 * may be rolling back, and the buffer may contain other messages.
		 * We want to avoid a procarray exception
		 * as well as an error stack overflow.
		 */
		if (proc_exit_inprogress)
		{
			UpdateConnectionState(conn, DN_CONNECTION_STATE_FATAL);
		}

		/*
		 * Don't read from from the connection if there is a fatal error.
		 * We still return RESPONSE_COMPLETE, not RESPONSE_ERROR, since
		 * Handling of RESPONSE_ERROR assumes sending SYNC message, but
		 * State DN_CONNECTION_STATE_FATAL indicates connection is
		 * not usable.
		 */
		if (IsConnectionStateFatal(conn))
		{
			return RESPONSE_COMPLETE;
		}

		ret = handle_reply_msg_on_proxy(conn);
		if (ret != 0)
		{
			ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
					errmsg("Handle reply message on proxy for %s error: %d",
						proxy_for_dn, ret)));
		}

		/* No data available, exit */
		if (!HAS_MESSAGE_BUFFERED(conn))
			return RESPONSE_EOF;

		Assert(conn->combiner == combiner || conn->combiner == NULL);

		msg_type = get_message(conn, &msg_len, &msg);
		if(enable_distri_print)
			elog(LOG, "handle_response_on_proxy - received message %c, node %s, "
				"current_state %d", msg_type, conn->nodename, GetConnectionState(conn));

		/*
		 * Add some protection code when receiving a messy message,
		 * close the connection, and throw error
		 */
		if (msg_len < 0)
		{
			UpdateConnectionState(conn, DN_CONNECTION_STATE_FATAL);

			elog(LOG, "handle_response_on_proxy, fatal_conn=%p, "
				"fatal_conn->nodename=%s, fatal_conn->sock=%d, "
				"fatal_conn->read_only=%d, fatal_conn->state.transaction_state=%c, "
				"fatal_conn->sock_fatal_occurred=%d, conn->backend_pid=%d, "
				"fatal_conn->error=%s", conn, conn->nodename, conn->sock,
				conn->read_only, GetTxnState(conn),
				conn->sock_fatal_occurred, conn->backend_pid,  conn->error);

			DestroyHandle(conn);

			elog(LOG, "Received messy message from node:%s host:%s port:%d pid:%d, "
				"inBuffer:%p inSize:%lu inStart:%lu inEnd:%lu inCursor:%lu "
				"msg_len:%d, This probably means the remote node terminated "
				"abnormally before or while processing the request.",
				conn->nodename, conn->nodehost, conn->nodeport, conn->backend_pid,
				conn->inBuffer, conn->inSize, conn->inStart, conn->inEnd,
				conn->inCursor, msg_len);

			ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
					errmsg("Proxy: handle_response_on_proxy - received message "
						"length %d, type %c, node %s, current_state %d",
						msg_len, msg_type, conn->nodename, GetConnectionState(conn))));
		}

		if (msg_type == '\0')
		{
			/* Not enough data in the buffer */
			return RESPONSE_EOF;
		}

		if (conn->stream_closed && msg_type == 'd')
		{
			/* When replicate stream is closed, skip 'd' message */
			elog(DEBUG1, "Proxy: handle_response_on_proxy - received message "
				"type %c, length %d, node %s, current_state %d, remote pid %d, skip",
				msg_type, msg_len, conn->nodename, GetConnectionState(conn), conn->backend_pid);
			continue;;
		}

		conn->last_command = msg_type;

		elog(DEBUG1, "Proxy: handle_response_on_proxy - received message "
			"type %c, length %d, node %s, current_state %d, remote pid %d",
			msg_type, msg_len, conn->nodename, GetConnectionState(conn), conn->backend_pid);

		/* Send message to client */
		pq_beginmessage(&buf, msg_type);
		pq_sendbytes(&buf, msg, msg_len);
		pq_endmessage(&buf);
		pq_flush();

		switch (msg_type)
		{
			case 'c':			/* CopyToCommandComplete */
				break;

			case 'C':			/* CommandComplete */
				conn->combiner = NULL;
				UpdateConnectionState(conn, DN_CONNECTION_STATE_IDLE);
				return RESPONSE_COMPLETE;

			case 'E':			/* ErrorResponse */
				HandleError(combiner, msg, msg_len, conn);
				add_error_message_from_combiner(conn, combiner);

				return RESPONSE_ERROR;

			case 'Z':			/* ReadyForQuery */
				SetTxnState(conn, msg[0]);
				UpdateConnectionState(conn, DN_CONNECTION_STATE_IDLE);
				conn->combiner = NULL;
				return RESPONSE_READY;

			case 'T':			/* RowDescription */
				return RESPONSE_TUPDESC;

			case 'D':			/* DataRow */
				return RESPONSE_DATAROW;

			case 'd': 			/* CopyOutDataRow */
				UpdateConnectionState(conn, DN_CONNECTION_STATE_COPY_OUT);
				break;

			case 'W':			/* CopyBothResponse */
				/* Get a CopyBothResponse message when start streaming */
				break;

			default:
				elog(DEBUG1, "Proxy received message type: %c", msg_type);
				break;
		}
	}

	/* Never happen, but keep compiler quiet */
	return RESPONSE_EOF;
}

void
DrainConnectionAndSetNewCombiner(PGXCNodeHandle *conn, ResponseCombiner *combiner)
{
	ResponseCombiner *old = conn->combiner;

	if (conn->combiner == combiner)
		return;

	if (enable_distri_print)
		elog(LOG, "set connection %s pid %d (state: %s) old combiner: %p new combiner: %p",
			 conn->nodename, conn->backend_pid, GetConnectionStateTag(conn), old, combiner);

	BufferConnection(conn);

	/* if Buffer success */
	if (!conn->combiner)
		conn->combiner = combiner;
}

static int
SendSessionGucConfigs(PGXCNodeHandle **connections, int conn_count, bool is_global_session)
{
	int i;
	int send_count = 0;
	ResponseCombiner combiner;
	bool error = false;

	InitResponseCombiner(&combiner, conn_count, COMBINE_TYPE_NONE);
	combiner.connections = (PGXCNodeHandle **) palloc(sizeof(PGXCNodeHandle *) * conn_count);

	for (i = 0; i < conn_count; i++)
	{
		PGXCNodeHandle *conn = connections[i];

		if (is_global_session && GetCurrentGucCid(conn) < max_session_param_cid)
		{
			char * param_str = session_param_list_get_str(GetCurrentGucCid(conn));
			if (param_str)
			{
				SetCurrentGucCid(conn, max_session_param_cid);
				if (pgxc_node_send_query_with_internal_flag(conn, param_str))
				{
					ereport(LOG, (errcode(ERRCODE_INTERNAL_ERROR),
								 errmsg("failed to send command sequence")));
					error = true;
					break;
				}
				conn->combiner = &combiner;
				combiner.connections[send_count++] = conn;
			}
		}
	}

	if (send_count > 0)
	{
		combiner.node_count  = send_count;
		/* need to receive two Z message , set ignore_error to true */
		error = error || ProcessResponseMessage(&combiner, NULL, RESPONSE_READY, false);
		CloseCombiner(&combiner);
	}

	send_count = 0;

	for (i = 0; i < conn_count; i++)
	{
		PGXCNodeHandle *conn = connections[i];

		if (OidIsValid(conn->current_user))
		{	
			if (pgxc_node_send_cmd_seq(conn))
			{
				ereport(LOG, (errcode(ERRCODE_INTERNAL_ERROR),
								 errmsg("failed to send command sequence")));
				error = true;
				break;
			}

			if (pgxc_node_send_exec_env(conn, SecContextIsSet, true))
			{
				ereport(LOG, (errcode(ERRCODE_INTERNAL_ERROR),
								 errmsg("failed to setup security context")));
				error = true;
				break;
			}

			if (SendRequest(conn))
			{
				ereport(LOG, (errcode(ERRCODE_INTERNAL_ERROR),
								 errmsg("failed to send requst to node %s pid %d", conn->nodename, conn->backend_pid)));
				error = true;
				break;
			}

			conn->combiner = &combiner;
			combiner.connections[send_count++] = conn;
		}
	}

	if (send_count > 0)
	{
		combiner.node_count  = send_count;
		/* need to receive two Z message , set ignore_error to true */
		error = error || ProcessResponseMessage(&combiner, NULL, RESPONSE_READY, false);
		CloseCombiner(&combiner);
	}

	if (combiner.connections)
		pfree(combiner.connections);

	if (error)
		return EOF;
	return 0;
}
/*
 * SendGucsToExistedConnections
 * ---------------------------
 * Send GUC (Grand Unified Configuration) settings to all existing remote connections.
 * This function ensures that all connected nodes have consistent GUC settings.
 *
 * Parameters:
 *      guc_str - string containing GUC settings to be sent
 *
 * Notes:
 *      - Sends GUC settings to both coordinator and datanode connections
 *      - Uses ResponseCombiner to handle responses from all nodes
 */
void
SendGucsToExistedConnections(char *guc_str)
{
	PGXCNodeAllHandles *handles;
	int nconn, i, send_count = 0;
	ResponseCombiner combiner;
	bool error = false;

	/* Get current connection handles */
	handles = get_current_handles();
	nconn = handles->co_conn_count + handles->dn_conn_count;

	/* Nothing to do if no connections */
	if (nconn == 0)
		return;

	/* Initialize response combiner */
	InitResponseCombiner(&combiner, nconn, COMBINE_TYPE_NONE);
	combiner.connections = (PGXCNodeHandle **) palloc(sizeof(PGXCNodeHandle *) * nconn);

	/* Send to coordinator connections */
	for (i = 0; i < handles->co_conn_count; i++)
	{
		PGXCNodeHandle *conn = handles->coord_handles[i];
		SetCurrentGucCid(conn, GucSubtxnId);
		if (pgxc_node_send_query_with_internal_flag(conn, guc_str))
		{
			elog(ERROR, "failed to send transaction guc parameters to node %s pid %d", 
					conn->nodename, conn->backend_pid);
		}

		conn->combiner = &combiner;
		combiner.connections[send_count++] = conn;
	}
	/* Send to datanode connections */
	for (i = 0; i < handles->dn_conn_count; i++)
	{
		PGXCNodeHandle *conn = handles->datanode_handles[i];
		SetCurrentGucCid(conn, GucSubtxnId);
		if (pgxc_node_send_query_with_internal_flag(conn, guc_str))
		{
			elog(ERROR, "failed to send transaction guc parameters to node %s pid %d", 
					conn->nodename, conn->backend_pid);
		}

		conn->combiner = &combiner;
		combiner.connections[send_count++] = conn;
	}

	/* Process responses if any messages were sent */
	if (send_count > 0)
	{
		combiner.node_count  = send_count;
		/* need to receive two Z message , set ignore_error to true */
		error = ProcessResponseMessage(&combiner, NULL, RESPONSE_READY, false);
		CloseCombiner(&combiner);
		if (error)
			elog(ERROR, "failed to receive all replies when synchronizing GUCs");
	}
}
