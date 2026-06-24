/*-------------------------------------------------------------------------
 *
 * postgres.c
 *	  POSTGRES C Backend Interface
 *
 * Portions Copyright (c) 2012-2014, TransLattice, Inc.
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 *
 *
 * IDENTIFICATION
 *	  src/backend/tcop/postgres.c
 *
 * NOTES
 *	  this is the "main" module of the postgres backend and
 *	  hence the main module of the "traffic cop".
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include <fcntl.h>
#include <limits.h>
#include <signal.h>
#include <unistd.h>
#include <sys/socket.h>
#include <sys/prctl.h>
#ifdef HAVE_SYS_SELECT_H
#include <sys/select.h>
#endif

#include "access/atxact.h"
#include "access/parallel.h"
#include "access/printtup.h"
#include "access/xact.h"
#include "access/xlog.h"
#include "catalog/pg_type.h"
#include "catalog/pg_collation.h"
#include "commands/async.h"
#include "commands/prepare.h"
#include "executor/spi.h"
#include "executor/nodeModifyTable.h"
#ifdef PGXC
#include "commands/trigger.h"
#endif
#include "jit/jit.h"
#include "libpq/libpq.h"
#include "libpq/pqformat.h"
#include "libpq/pqsignal.h"
#include "miscadmin.h"
#include "nodes/print.h"
#include "optimizer/planner.h"
#include "pgstat.h"
#include "pg_trace.h"
#include "parser/analyze.h"
#include "parser/parser.h"
#include "parser/parse_param.h"
#ifdef PGXC
#include "parser/parse_type.h"
#endif /* PGXC */
#include "pg_getopt.h"
#include "postmaster/autovacuum.h"
#include "postmaster/postmaster.h"
#include "replication/logicallauncher.h"
#include "replication/logicalworker.h"
#include "replication/slot.h"
#include "replication/walsender.h"
#include "rewrite/rewriteHandler.h"
#include "storage/bufmgr.h"
#include "storage/ipc.h"
#include "storage/proc.h"
#include "storage/procsignal.h"
#include "storage/sinval.h"
#include "tcop/fastpath.h"
#include "tcop/pquery.h"
#include "tcop/tcopprot.h"
#include "tcop/utility.h"
#include "utils/bitmap.h"
#include "utils/guc.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/ps_status.h"
#include "utils/snapmgr.h"
#include "utils/timeout.h"
#include "utils/timestamp.h"
#include "mb/pg_wchar.h"
#include "utils/varlena.h"
#include "utils/memutils.h"
#include "utils/inval.h"

#ifdef PGXC
#include "catalog/pgxc_node.h"
#include "storage/procarray.h"
#include "pgxc/pgxc.h"
#include "pgxc/pgxcnode.h"
#include "pgxc/nodemgr.h"
#include "executor/execLight.h"
#include "access/gtm.h"
/* PGXC_COORD */
#include "pgxc/execRemote.h"
#include "pgxc/barrier.h"
#include "pgxc/planner.h"
#include "nodes/nodes.h"
#include "pgxc/poolmgr.h"
#include "pgxc/pgxcnode.h"
#ifdef XCP
#include "pgxc/pause.h"
#include "pgxc/squeue.h"
#endif
#include "commands/copy.h"
#include "commands/tablespace.h"
/* PGXC_DATANODE */
#include "access/transam.h"

#ifdef __OPENTENBASE__
#include "storage/nodelock.h"
#include "optimizer/planmain.h"
#include "access/twophase.h"
#include "executor/execParallel.h"
#include "pgxc/poolutils.h"
#include "commands/vacuum.h"
#endif

#endif

#ifdef __AUDIT__
#include "audit/audit.h"
#include "postmaster/auditlogger.h"
#endif

#ifdef _MLS_
#include "utils/mls.h"
#endif

#ifdef __OPENTENBASE_C__
#include "commands/explain_dist.h"
#include "executor/execFragment.h"
#include "executor/execDispatchFragment.h"
#include "executor/nodeCtescan.h"
#include "access/result_cache.h"
#include "optimizer/cost.h"
#include "optimizer/spm.h"
#endif

#ifdef __SUBSCRIPTION__
#include "replication/logicalrelation.h"
#endif
#include "utils/backend_cancel.h"

#ifdef __RESOURCE_QUEUE__
#include "commands/resqueue.h"
#endif
#include "utils/resgroup.h"

#ifdef _PG_ORCL_
#include "catalog/pg_profile.h"
#endif

uint8       internal_cmd_sequence = 0;
bool        g_need_tuple_descriptor = true;
bool        g_dn_direct_printtup = false;
bool        debug_message_log = false;
bool        is_load_opentenbase_ora_fdw_guc = false;
bool        is_load_postgres_fdw_guc = false;
char		*proxy_for_dn = NULL;       /* Proxy for which dn? */
bool		am_proxy_for_dn = false;    /* Am I a proxy for dn? */
bool		am_conn_from_proxy = false; /* Am I connected from proxy? */

extern int	optind;
char* g_index_table_part_name;

bool        is_internal_cmd = false;
bool        is_pooler_show_cmd = false;
extern bool savepoint_define_cmd;
extern bool g_in_rollback_to;
extern bool g_in_release_savepoint;
/* ----------------
 *		global variables
 * ----------------
 */
const char *debug_query_string;	/* client-supplied query string or sanitized string */
const char *origin_query_string = NULL;	/* client-supplied query string */

/* Record that one query contains multiple statements. */
bool mult_sql_query_containing = false;

/* Note: whereToSendOutput is initialized for the bootstrap/standalone case */
CommandDest whereToSendOutput = DestDebug;

/* flag for logging end of session */
bool		Log_disconnections = false;

int			log_statement = LOGSTMT_NONE;

/* GUC variable for maximum stack depth (measured in kilobytes) */
int			max_stack_depth = 100;

/* wait N seconds to allow attach from a debugger */
int			PostAuthDelay = 0;

/* flags for non-system relation kinds to restrict use */
int			restrict_nonsystem_relation_kind;

uint32		PGXCCoordSessionId = 0;
#ifdef _PG_ORCL_
TimestampTz SessionStartTime;
TimestampTz LastTxEndTime;
#endif
/* ----------------
 *		private variables
 * ----------------
 */

/* max_stack_depth converted to bytes for speed of checking */
static long max_stack_depth_bytes = 100 * 1024L;

/*
 * Stack base pointer -- initialized by PostmasterMain and inherited by
 * subprocesses. This is not static because old versions of PL/Java modify
 * it directly. Newer versions use set_stack_base(), but we want to stay
 * binary-compatible for the time being.
 */
char	   *stack_base_ptr = NULL;

/*
 * On IA64 we also have to remember the register stack base.
 */
#if defined(__ia64__) || defined(__ia64)
char	   *register_stack_base_ptr = NULL;
#endif

/*
 * Flag to keep track of whether we have started a transaction.
 * For extended query protocol this has to be remembered across messages.
 */
static bool xact_started = false;

/*
 * Flag to indicate that we are doing the outer loop's read-from-client,
 * as opposed to any random read from client that might happen within
 * commands like COPY FROM STDIN.
 */
bool DoingCommandRead = false;

/*
 * Flags to implement skip-till-Sync-after-error behavior for messages of
 * the extended query protocol.
 */
static bool doing_extended_query_message = false;
static bool ignore_till_sync = false;

/*
 * If an unnamed prepared statement exists, it's stored here.
 * We keep it separate from the hashtable kept by commands/prepare.c
 * in order to reduce overhead for short-lived queries.
 */
static CachedPlanSource *unnamed_stmt_psrc = NULL;

/* assorted command-line switches */
static const char *userDoption = NULL;	/* -D switch */
static bool EchoQuery = false;	/* -E switch */
static bool UseSemiNewlineNewline = false;	/* -j switch */

/* whether or not, and why, we were canceled by conflict with recovery */
static volatile sig_atomic_t RecoveryConflictPending = false;
static volatile sig_atomic_t RecoveryConflictPendingReasons[NUM_PROCSIGNALS];

/* handle read only process */
static bool ReadOnlyProcessPending = false;

/* reused buffer to pass to SendRowDescriptionMessage() */
static MemoryContext row_description_context = NULL;
static StringInfoData row_description_buf;

/* hook used for query rewrite */
post_query_replace_function post_query_replace_hook = NULL;

#ifdef __OPENTENBASE__
static char *remotePrepareGID = NULL;
/* for error code contrib */
bool g_is_in_init_phase = false;

bool g_snapshot_for_analyze = true;

bool IsNormalPostgres = false;

bool explain_stmt = false;
#endif

#ifdef __AUDIT_FGA__
const char *g_commandTag = NULL;
#endif


/* ----------------------------------------------------------------
 *		decls for routines only used in this file
 * ----------------------------------------------------------------
 */
static int	InteractiveBackend(StringInfo inBuf);
static int	interactive_getc(void);
static int	SocketBackend(StringInfo inBuf);
static int	ReadCommand(StringInfo inBuf);
static void forbidden_in_wal_sender(char firstchar);
static List *pg_rewrite_query(Query *query);
static bool check_log_statement(List *stmt_list);
static int	errdetail_execute(List *raw_parsetree_list);
static int	errdetail_params(ParamListInfo params);
static int	errdetail_abort(void);
void start_xact_command(void);
void finish_xact_command(void);
static bool IsTransactionExitStmt(Node *parsetree);
static bool IsTransactionExitStmtList(List *pstmts);
static bool IsTransactionStmtList(List *pstmts);
static void drop_unnamed_stmt(void);
static void log_disconnections(int code, Datum arg);
#ifdef __OPENTENBASE__
static void replace_null_with_blank(char *src, int length);
static bool NeedResourceOwner(const char *stmt_name);
#endif
static void execute_update_stat(AnalyzeRelStats *analyzeStats, StringInfoData *input_message);
static void set_backend_to_resue(const char *query_string);
#ifdef _PG_ORCL_
static bool ch_is_space(char ch);
#endif

static PGXCNodeHandle *get_handle_on_proxy(void);
static PGXCNodeHandle *handle_request_msg_on_proxy(PGXCNodeHandle *conn, int firstchar,
                                                   StringInfo input_msg);
void                   set_flag_from_proxy(int flag, const char *username);
static void FinishReadOnlyProcess(void);
static void execute_sync_spm_plan(StringInfoData *input_message);
static void execute_sync_spm_plan_delete(StringInfoData *input_message);
static void execute_sync_spm_plan_update(StringInfoData *input_message);
static bool get_reloid_with_fullname(const char *fullname, Oid *reloid);

#ifdef PGXC /* PGXC_DATANODE */
/* ----------------------------------------------------------------
 *		PG-XC routines
 * ----------------------------------------------------------------
 */

/*
 * Called when the backend is ending.
 */
static void
DataNodeShutdown (int code, Datum arg)
{
	/* Close connection with GTM, if active */
	CloseGTM();

#ifdef __RESOURCE_QUEUE__
	ResQueue_CloseGTM();
#endif
	
}
#endif

/* ----------------------------------------------------------------
 *		routines to obtain user input
 * ----------------------------------------------------------------
 */

/* ----------------
 *	InteractiveBackend() is called for user interactive connections
 *
 *	the string entered by the user is placed in its parameter inBuf,
 *	and we act like a Q message was received.
 *
 *	EOF is returned if end-of-file input is seen; time to shut down.
 * ----------------
 */

static int
InteractiveBackend(StringInfo inBuf)
{
	int			c;				/* character read from getc() */

	/*
	 * display a prompt and obtain input from the user
	 */
	printf("backend> ");
	fflush(stdout);

	resetStringInfo(inBuf);

	/*
	 * Read characters until EOF or the appropriate delimiter is seen.
	 */
	while ((c = interactive_getc()) != EOF)
	{
		if (c == '\n')
		{
			if (UseSemiNewlineNewline)
			{
				/*
				 * In -j mode, semicolon followed by two newlines ends the
				 * command; otherwise treat newline as regular character.
				 */
				if (inBuf->len > 1 &&
					inBuf->data[inBuf->len - 1] == '\n' &&
					inBuf->data[inBuf->len - 2] == ';')
				{
					/* might as well drop the second newline */
					break;
				}
			}
			else
			{
				/*
				 * In plain mode, newline ends the command unless preceded by
				 * backslash.
				 */
				if (inBuf->len > 0 &&
					inBuf->data[inBuf->len - 1] == '\\')
				{
					/* discard backslash from inBuf */
					inBuf->data[--inBuf->len] = '\0';
					/* discard newline too */
					continue;
				}
				else
				{
					/* keep the newline character, but end the command */
					appendStringInfoChar(inBuf, '\n');
					break;
				}
			}
		}

		/* Not newline, or newline treated as regular character */
		appendStringInfoChar(inBuf, (char) c);
	}

	/* No input before EOF signal means time to quit. */
	if (c == EOF && inBuf->len == 0)
		return EOF;

	/*
	 * otherwise we have a user query so process it.
	 */

	/* Add '\0' to make it look the same as message case. */
	appendStringInfoChar(inBuf, (char) '\0');

	/*
	 * if the query echo flag was given, print the query..
	 */
	if (EchoQuery)
		printf("statement: %s\n", inBuf->data);
	fflush(stdout);

	return 'Q';
}

/*
 * interactive_getc -- collect one character from stdin
 *
 * Even though we are not reading from a "client" process, we still want to
 * respond to signals, particularly SIGTERM/SIGQUIT.
 */
static int
interactive_getc(void)
{
	int			c;

	/*
	 * This will not process catchup interrupts or notifications while
	 * reading. But those can't really be relevant for a standalone backend
	 * anyway. To properly handle SIGTERM there's a hack in die() that
	 * directly processes interrupts at this stage...
	 */
	CHECK_FOR_INTERRUPTS();

	c = getc(stdin);

	ProcessClientReadInterrupt(true);

	return c;
}

/* ----------------
 *	SocketBackend()		Is called for frontend-backend connections
 *
 *	Returns the message type code, and loads message body data into inBuf.
 *
 *	EOF is returned if the connection is lost.
 * ----------------
 */
static int
SocketBackend(StringInfo inBuf)
{
	int			qtype;

	/*
	 * Get message type code from the frontend.
	 */
	HOLD_CANCEL_INTERRUPTS();
	pq_startmsgread();

	qtype = pq_getbyte();

	if (qtype == EOF)			/* frontend disconnected */
	{
		if (IsTransactionState())
			ereport(COMMERROR,
					(errcode(ERRCODE_CONNECTION_FAILURE),
					 errmsg("unexpected EOF on client connection with an open transaction")));
		else
		{
			/*
			 * Can't send DEBUG log messages to client at this point. Since
			 * we're disconnecting right away, we don't need to restore
			 * whereToSendOutput.
			 */
			whereToSendOutput = DestNone;
			ereport(DEBUG1,
					(errcode(ERRCODE_CONNECTION_DOES_NOT_EXIST),
					 errmsg("unexpected EOF on client connection")));
		}
		return qtype;
	}

	if (enable_distri_print)
		elog(LOG, "read msg %c", qtype);

	/*
	 * Validate message type code before trying to read body; if we have lost
	 * sync, better to say "command unknown" than to run out of memory because
	 * we used garbage as a length word.
	 *
	 * This also gives us a place to set the doing_extended_query_message flag
	 * as soon as possible.
	 */
	switch (qtype)
	{
		case 'Q':				/* simple query */
			doing_extended_query_message = false;
			if (PG_PROTOCOL_MAJOR(FrontendProtocol) < 3)
			{
				/* old style without length word; convert */
				if (pq_getstring(inBuf))
				{
					if (IsTransactionState())
						ereport(COMMERROR,
								(errcode(ERRCODE_CONNECTION_FAILURE),
								 errmsg("unexpected EOF on client connection with an open transaction")));
					else
					{
						/*
						 * Can't send DEBUG log messages to client at this
						 * point. Since we're disconnecting right away, we
						 * don't need to restore whereToSendOutput.
						 */
						whereToSendOutput = DestNone;
						ereport(DEBUG1,
								(errcode(ERRCODE_CONNECTION_DOES_NOT_EXIST),
								 errmsg("unexpected EOF on client connection")));
					}
					return EOF;
				}
			}
			break;

		case 'F':				/* fastpath function call */
			doing_extended_query_message = false;
			if (PG_PROTOCOL_MAJOR(FrontendProtocol) < 3)
			{
				if (GetOldFunctionMessage(inBuf))
				{
					if (IsTransactionState())
						ereport(COMMERROR,
								(errcode(ERRCODE_CONNECTION_FAILURE),
								 errmsg("unexpected EOF on client connection with an open transaction")));
					else
					{
						/*
						 * Can't send DEBUG log messages to client at this
						 * point. Since we're disconnecting right away, we
						 * don't need to restore whereToSendOutput.
						 */
						whereToSendOutput = DestNone;
						ereport(DEBUG1,
								(errcode(ERRCODE_CONNECTION_DOES_NOT_EXIST),
								 errmsg("unexpected EOF on client connection")));
					}
					return EOF;
				}
			}
			break;

		case 'X':				/* terminate */
			doing_extended_query_message = false;
			ignore_till_sync = false;
			break;

		case 'B':				/* bind */
#ifdef XCP /* PGXC_DATANODE */
		case 'p':				/* plan */
#endif
		case 'C':				/* close */
		case 'D':				/* describe */
		case 'E':				/* execute */
		case 'H':				/* flush */
		case 'P':				/* parse */
        case 'V':               /* batch bind & execute */
			doing_extended_query_message = true;
			/* these are only legal in protocol 3 */
			if (PG_PROTOCOL_MAJOR(FrontendProtocol) < 3)
				ereport(FATAL,
						(errcode(ERRCODE_PROTOCOL_VIOLATION),
						 errmsg("invalid frontend message type %d", qtype)));
			break;

		case 'y':
		case 'S':				/* sync */
			/* stop any active skip-till-Sync */
			ignore_till_sync = false;
			/* mark not-extended, so that a new error doesn't begin skip */
			doing_extended_query_message = false;
			/* only legal in protocol 3 */
			if (PG_PROTOCOL_MAJOR(FrontendProtocol) < 3)
				ereport(FATAL,
						(errcode(ERRCODE_PROTOCOL_VIOLATION),
						 errmsg("invalid frontend message type %d", qtype)));
			break;

		case 'd':				/* copy data */
		case 'c':				/* copy done */
		case 'f':				/* copy fail */
			doing_extended_query_message = false;
			/* these are only legal in protocol 3 */
			if (PG_PROTOCOL_MAJOR(FrontendProtocol) < 3)
				ereport(FATAL,
						(errcode(ERRCODE_PROTOCOL_VIOLATION),
						 errmsg("invalid frontend message type %d", qtype)));
			break;
		case 'v':				/* Set connected by proxy */
#ifdef PGXC /* PGXC_DATANODE */
#ifdef __OPENTENBASE__
		case 'N':				/* sequence number */
		case 'U':				/* coord info: coord_pid and top_xid */
		case 'j':               /* global trace id */
#endif
		case 'M':				/* Command ID */
		case 'g':				/* GXID */
		case 's':				/* Snapshot */
		case 't':				/* Timestamp */
		case 'K':
		case 'o':				/* QueryId */
#ifdef __OPENTENBASE_C__
		case 'l':				/* fid */
		case 'L':				/* flevel */
		case 'I':				/* ftable */
		case 'R':               /* cleanup */
		case 'i':				/* instrument */
#endif
#ifdef __SUPPORT_DISTRIBUTED_TRANSACTION__
		case 'T':				/* Global Timestamp */
		case 'Z':				/* Global Prepare Timestamp */
		case 'G':				/* Explicit prepared gid */
		case 'W':				/* Prefinish phase */
#endif
		case 'b':				/* Barrier */
#ifdef __SUBSCRIPTION__
		case 'a':				/* logical apply */
#endif
#ifdef __RESOURCE_QUEUE__
		case 'J':				/* receive resources : pgxc_node_send_resqinfo */ 
#endif
		case 'q':				/* clean distribute msg: pgxc_node_send_clean_distribute */
#ifdef __TWO_PHASE_TRANS__
		case 'e':               /* startnode of 2pc transaction */
		case 'O':               /* PartNodes */
		case 'x':               /* xid of 2pc transaction in startnode */
		case 'w':               /* database of 2pc transaction */
		case 'u':               /* user of 2pc transaction */
		case 'n':               /* run in pg_clean */
		case 'r':               /* send readonly in pg_clean */
		case 'A':               /* send after prepare phase in pg_clean */
		case 'Y':               /* extend options */
		case 'k':               /* fast path to execute transaction command */
		case 'm':               /* remote modify table */
		case 'h':               /* analyze stat info */
		case 'z':               /* set backend for resue user and pgoptions */
#endif
			break;
#endif

		default:

			/*
			 * Otherwise we got garbage from the frontend.  We treat this as
			 * fatal because we have probably lost message boundary sync, and
			 * there's no good way to recover.
			 */
			ereport(FATAL,
					(errcode(ERRCODE_PROTOCOL_VIOLATION),
					 errmsg("invalid frontend message type %d", qtype)));
			break;
	}

	/*
	 * In protocol version 3, all frontend messages have a length word next
	 * after the type code; we can read the message contents independently of
	 * the type.
	 */
	if (PG_PROTOCOL_MAJOR(FrontendProtocol) >= 3)
	{
		if (pq_getmessage(inBuf, 0))
			return EOF;			/* suitable message already logged */
	}
	else
		pq_endmsgread();
	RESUME_CANCEL_INTERRUPTS();

	return qtype;
}

/* ----------------
 *		ReadCommand reads a command from either the frontend or
 *		standard input, places it in inBuf, and returns the
 *		message type code (first byte of the message).
 *		EOF is returned if end of file.
 * ----------------
 */
static int
ReadCommand(StringInfo inBuf)
{
	int			result;

	if (whereToSendOutput == DestRemote)
		result = SocketBackend(inBuf);
	else
		result = InteractiveBackend(inBuf);

	return result;
}

void
SetProcSessionId(int session_id)
{
	LWLockAcquire(ProcArrayLock, LW_EXCLUSIVE);
	MyProc->coordlxid = session_id;
	LWLockRelease(ProcArrayLock);
}

/*
 * ProcessClientReadInterrupt() - Process interrupts specific to client reads
 *
 * This is called just after low-level reads. That might be after the read
 * finished successfully, or it was interrupted via interrupt.
 *
 * Must preserve errno!
 */
void
ProcessClientReadInterrupt(bool blocked)
{
	int			save_errno = errno;

	if (DoingCommandRead)
	{
		/* Check for general interrupts that arrived while reading */
		CHECK_FOR_INTERRUPTS();

		if (ReadOnlyProcessPending)
			FinishReadOnlyProcess();

		/* Process sinval catchup interrupts that happened while reading */
		if (catchupInterruptPending)
			ProcessCatchupInterrupt();

		/* Process sinval catchup interrupts that happened while reading */
		if (notifyInterruptPending)
			ProcessNotifyInterrupt();
	}
	else if (ProcDiePending && blocked)
	{
		/*
		 * We're dying. It's safe (and sane) to handle that now.
		 */
		CHECK_FOR_INTERRUPTS();
	}

	errno = save_errno;
}

/*
 * ProcessClientWriteInterrupt() - Process interrupts specific to client writes
 *
 * This is called just after low-level writes. That might be after the read
 * finished successfully, or it was interrupted via interrupt. 'blocked' tells
 * us whether the
 *
 * Must preserve errno!
 */
void
ProcessClientWriteInterrupt(bool blocked)
{
	int			save_errno = errno;

	/*
	 * We only want to process the interrupt here if socket writes are
	 * blocking to increase the chance to get an error message to the client.
	 * If we're not blocked there'll soon be a CHECK_FOR_INTERRUPTS(). But if
	 * we're blocked we'll never get out of that situation if the client has
	 * died.
	 */
	if (ProcDiePending && blocked)
	{
		/*
		 * We're dying. It's safe (and sane) to handle that now. But we don't
		 * want to send the client the error message as that a) would possibly
		 * block again b) would possibly lead to sending an error message to
		 * the client, while we already started to send something else.
		 */
		if (whereToSendOutput == DestRemote)
			whereToSendOutput = DestNone;

		CHECK_FOR_INTERRUPTS();
	}

	errno = save_errno;
}

/*
 * Do raw parsing (only).
 *
 * A list of parsetrees (RawStmt nodes) is returned, since there might be
 * multiple commands in the given string.
 *
 * NOTE: for interactive queries, it is important to keep this routine
 * separate from the analysis & rewrite stages.  Analysis and rewriting
 * cannot be done in an aborted transaction, since they require access to
 * database tables.  So, we rely on the raw parser to determine whether
 * we've seen a COMMIT or ABORT command; when we are in abort state, other
 * commands are not processed any further than the raw parse stage.
 */
List *
pg_parse_query(const char *query_string)
{
	List	   *raw_parsetree_list;

	TRACE_POSTGRESQL_QUERY_PARSE_START(query_string);

	if (log_parser_stats)
		ResetUsage();

	raw_parsetree_list = raw_parser(query_string);

	if (log_parser_stats)
		ShowUsage("PARSER STATISTICS");

#ifdef COPY_PARSE_PLAN_TREES
	/* Optional debugging check: pass raw parsetrees through copyObject() */
	{
		List	   *new_list = copyObject(raw_parsetree_list);

		/* This checks both copyObject() and the equal() routines... */
		if (!equal(new_list, raw_parsetree_list))
			elog(WARNING, "copyObject() failed to produce an equal raw parse tree");
		else
			raw_parsetree_list = new_list;
	}
#endif

	TRACE_POSTGRESQL_QUERY_PARSE_DONE(query_string);

	return raw_parsetree_list;
}

/*
 * Given a raw parsetree (gram.y output), and optionally information about
 * types of parameter symbols ($n), perform parse analysis and rule rewriting.
 *
 * A list of Query nodes is returned, since either the analyzer or the
 * rewriter might expand one query to several.
 *
 * NOTE: for reasons mentioned above, this must be separate from raw parsing.
 */
List *
pg_analyze_and_rewrite(RawStmt *parsetree, const char *query_string, Oid *paramTypes, int numParams,
                       QueryEnvironment *queryEnv)
{
	return pg_analyze_and_rewrite_internal(parsetree, query_string, paramTypes, numParams, queryEnv, true, false);
}

List *
pg_analyze_and_rewrite_deparsed_string(RawStmt *parsetree, const char *query_string, Oid *paramTypes, int numParams,
                       QueryEnvironment *queryEnv, bool is_deparsed_query)
{
	return pg_analyze_and_rewrite_internal(parsetree, query_string, paramTypes, numParams, queryEnv, true, is_deparsed_query);
}

List *
pg_analyze_and_rewrite_internal(RawStmt *parsetree, const char *query_string,
					   Oid *paramTypes, int numParams,
					   QueryEnvironment *queryEnv, bool exec,
					   bool is_deparsed_query)
{
	Query	   *query;
	List	   *querytree_list;

	TRACE_POSTGRESQL_QUERY_REWRITE_START(query_string);

	/*
	 * (1) Perform parse analysis.
	 */
	if (log_parser_stats)
		ResetUsage();

	query = parse_analyze(parsetree, query_string, paramTypes, numParams,
						  queryEnv, is_deparsed_query);

	if (log_parser_stats)
		ShowUsage("PARSE ANALYSIS STATISTICS");

#ifdef __AUDIT__
	if (query->commandType == CMD_UTILITY &&
		IsA(query->utilityStmt, CreateTableAsStmt) &&
		xact_started)
	{
		/*
		 * first read utility from CreateTableAsStmt
		 */
		AuditReadQueryList(query_string, list_make1(query));
	}
#endif

	/*
	 * (2) Rewrite the queries, as necessary
	 */
#ifdef PGXC
	if (query->commandType == CMD_UTILITY &&
	    IsA(query->utilityStmt, CreateTableAsStmt))
	{
		if (exec)
		{
			/*
			 * CREATE TABLE AS SELECT and SELECT INTO are rewritten so that the
			 * target table is created first. The SELECT query is then transformed
			 * into an INSERT INTO statement
			 */
			List *ctas_list = QueryRewriteCTAS(query, NULL, NULL, queryEnv, paramTypes, numParams);
			ListCell *cell;

			querytree_list = NIL;
			foreach (cell, ctas_list)
			{
				querytree_list =
					list_concat_unique(querytree_list, pg_rewrite_query((Query *) lfirst(cell)));
			}
		}
		else
		{
			CreateTableAsStmt *stmt;
			stmt = (CreateTableAsStmt *) query->utilityStmt;
			querytree_list = pg_rewrite_query((Query *)stmt->query);
		}
	}
	else
#endif
	querytree_list = pg_rewrite_query(query);

	TRACE_POSTGRESQL_QUERY_REWRITE_DONE(query_string);

	return querytree_list;
}

/*
 * Do parse analysis and rewriting.  This is the same as pg_analyze_and_rewrite
 * except that external-parameter resolution is determined by parser callback
 * hooks instead of a fixed list of parameter datatypes.
 */
List *
pg_analyze_and_rewrite_params(RawStmt *parsetree,
							  const char *query_string,
							  ParserSetupHook parserSetup,
							  void *parserSetupArg,
							  QueryEnvironment *queryEnv,
							  bool fromCTAS,
							  bool checkFunc)
{
	ParseState *pstate;
	Query	   *query;
	List	   *querytree_list;
	JumbleState *jstate = NULL;

	Assert(query_string != NULL);	/* required as of 8.4 */

	TRACE_POSTGRESQL_QUERY_REWRITE_START(query_string);

	/*
	 * (1) Perform parse analysis.
	 */
	if (log_parser_stats)
		ResetUsage();

	pstate = make_parsestate(NULL);
	pstate->p_sourcetext = query_string;
	pstate->p_queryEnv = queryEnv;
	pstate->fromCTAS = fromCTAS;
    pstate->param_cnt = 0;
	(*parserSetup) (pstate, parserSetupArg);

	if (!checkFunc)
	{
		/*
		 * Currently checkFunc is false, try to get the check
		 * status from pstate.
		 */
		checkFunc = pstate->isInCheck;
	}

	query = transformTopLevelStmt(pstate, parsetree);

	if (compute_query_id)
		jstate = JumbleQuery(query, query_string);

	if (post_parse_analyze_hook)
		(*post_parse_analyze_hook) (pstate, query, jstate);

    parsetree->param_cnt = pstate->param_cnt;
    parsetree->param_list = pstate->param_list;
    
	free_parsestate(pstate);

	if (log_parser_stats)
		ShowUsage("PARSE ANALYSIS STATISTICS");

#ifdef __AUDIT__
	if (query->commandType == CMD_UTILITY &&
		IsA(query->utilityStmt, CreateTableAsStmt) &&
		xact_started)
	{
		/*
		 * first read utility from CreateTableAsStmt
		 */
		AuditReadQueryList(query_string, list_make1(query));
	}
#endif
	/*
	 * (2) Rewrite the queries, as necessary
	 */
#ifdef PGXC
	if (query->commandType == CMD_UTILITY &&
	    IsA(query->utilityStmt, CreateTableAsStmt) && !checkFunc)
	{
		/*
		 * CREATE TABLE AS SELECT and SELECT INTO are rewritten so that the
		 * target table is created first. The SELECT query is then transformed
		 * into an INSERT INTO statement
		 */
		List        *ctas_list = QueryRewriteCTAS(query, parserSetup, parserSetupArg, queryEnv, NULL, 0);
		ListCell    *cell;
		
		querytree_list = NIL;
		foreach(cell, ctas_list)
		{
			querytree_list = list_concat_unique(querytree_list, pg_rewrite_query((Query *) lfirst(cell)));
		}
	}
	else
#endif
	querytree_list = pg_rewrite_query(query);

	TRACE_POSTGRESQL_QUERY_REWRITE_DONE(query_string);

	return querytree_list;
}

/*
 * Perform rewriting of a query produced by parse analysis.
 *
 * Note: query must just have come from the parser, because we do not do
 * AcquireRewriteLocks() on it.
 */
static List *
pg_rewrite_query(Query *query)
{
	List	   *querytree_list;

	if (Debug_print_parse)
		elog_node_display(LOG, "parse tree", query,
						  Debug_pretty_print);

	if (log_parser_stats)
		ResetUsage();

	if (query->commandType == CMD_UTILITY)
	{
		/* don't rewrite utilities, just dump 'em into result list */
		querytree_list = list_make1(query);
	}
	else
	{
		/* rewrite regular queries */
		querytree_list = QueryRewrite(query);
	}

	if (log_parser_stats)
		ShowUsage("REWRITER STATISTICS");

#ifdef COPY_PARSE_PLAN_TREES
	/* Optional debugging check: pass querytree output through copyObject() */
	{
		List	   *new_list;

		new_list = copyObject(querytree_list);
		/* This checks both copyObject() and the equal() routines... */
		if (!equal(new_list, querytree_list))
			elog(WARNING, "copyObject() failed to produce equal parse tree");
		else
			querytree_list = new_list;
	}
#endif

	if (Debug_print_rewritten)
		elog_node_display(LOG, "rewritten parse tree", querytree_list,
						  Debug_pretty_print);


	return querytree_list;
}


/*
 * Generate a plan for a single already-rewritten query.
 * This is a thin wrapper around planner() and takes the same parameters.
 *
 * To implement the conversion of a distribution key function into a parameter,
 * we need to add the parameter for the distribution key function to
 * the existing parameter list. However, when generating the new generic plan,
 * the parameter list is not passed initially. Therefore, we need to add a
 * function parameter called cached_param_num to record the original number
 * of parameters.
 */
PlannedStmt *
pg_plan_query(Query *querytree, int cursorOptions,
						ParamListInfo boundParams,
						int cached_param_num,
						bool explain)
{
	PlannedStmt *plan;

	/* Utility commands have no plans. */
	if (querytree->commandType == CMD_UTILITY)
		return NULL;

	/* Planner must have a snapshot in case it calls user-defined functions. */
	Assert(ActiveSnapshotSet() || !g_snapshot_for_analyze);

	TRACE_POSTGRESQL_QUERY_PLAN_START();

	if (log_planner_stats)
		ResetUsage();

	SPMFillGUC();

	/* call the optimizer */
	plan = planner(querytree, cursorOptions, boundParams,
					cached_param_num, explain);

#ifdef __AUDIT__
	plan->queryString = NULL;
	plan->parseTree = copyObject(querytree);
#endif

	if (log_planner_stats)
		ShowUsage("PLANNER STATISTICS");

#ifdef COPY_PARSE_PLAN_TREES
	/* Optional debugging check: pass plan output through copyObject() */
	{
		PlannedStmt *new_plan = copyObject(plan);

		/*
		 * equal() currently does not have routines to compare Plan nodes, so
		 * don't try to test equality here.  Perhaps fix someday?
		 */
#ifdef NOT_USED
		/* This checks both copyObject() and the equal() routines... */
		if (!equal(new_plan, plan))
			elog(WARNING, "copyObject() failed to produce an equal plan tree");
		else
#endif
			plan = new_plan;
	}
#endif

	/*
	 * Print plan if debugging.
	 */
	if (Debug_print_plan)
		elog_node_display(LOG, "plan", plan, Debug_pretty_print);

	SPMFillExtraData(plan);

	TRACE_POSTGRESQL_QUERY_PLAN_DONE();

	return plan;
}

List *
pg_plan_queries(List *querytrees, int cursorOptions, ParamListInfo boundParams)
{
	return pg_plan_queries_internal(querytrees, cursorOptions, boundParams, 0);
}

/*
 * Generate plans for a list of already-rewritten queries.
 *
 * For normal optimizable statements, invoke the planner.  For utility
 * statements, just make a wrapper PlannedStmt node.
 *
 * The result is a list of PlannedStmt nodes.
 */
List *
pg_plan_queries_internal(List *querytrees, int cursorOptions,
							ParamListInfo boundParams, int cached_param_num)
{
	List	   *stmt_list = NIL;
	ListCell   *query_list;

	foreach(query_list, querytrees)
	{
		Query	   *query = lfirst_node(Query, query_list);
		PlannedStmt *stmt;

		if (query->commandType == CMD_UTILITY)
		{
			/* Utility commands require no planning. */
			stmt = makeNode(PlannedStmt);
			stmt->commandType = CMD_UTILITY;
			stmt->canSetTag = query->canSetTag;
			stmt->utilityStmt = query->utilityStmt;
			stmt->stmt_location = query->stmt_location;
			stmt->stmt_len = query->stmt_len;
			stmt->guc_str = NULL;
		}
		else
		{
			stmt = pg_plan_query(query, cursorOptions, boundParams,
									cached_param_num, false);
		}

		stmt_list = lappend(stmt_list, stmt);
	}

	return stmt_list;
}

/*
 * Get a single query string from the original query string if the query string
 * contains multi stmts.
 */
static char*
get_single_query_string(const char* query_string, RawStmt *parsetree)
{
	StringInfo	single_query_str = NULL;
	int			query_location;
	int			query_len;
	int			total_len = strlen(query_string);

	if (parsetree->stmt_location >= 0)
	{
		Assert(parsetree->stmt_location <= strlen(query_string));
		query_location = parsetree->stmt_location;
		/* Length of 0 (or -1) means "rest of string" */
		query_len =  (parsetree->stmt_len <= 0) ? total_len - query_location: parsetree->stmt_len;
	}
	else
	{
		/* If query location is unknown, distrust query_len as well */
		query_location = 0;
		query_len = total_len;
	}

	if (single_query_str == NULL)
	{
		single_query_str = makeStringInfo();
	}
	else
	{
		resetStringInfo(single_query_str);
	}

	/*
	 * The remaining parts of the query are substituted with spaces.
	 *
	 * example:
	 *   select * from t1; select * from t2; select * from t3;
	 * query1:
	 *   select * from t1-------------------------------------
	 * query2:
	 *   ------------------select * from t2-------------------
	 * query3:
	 *   ------------------------------------select * from t3-
	 * '-' represents a 'space'.
	 */

	/* set the front part to spaces */
	if (query_location > 0)
	{
		appendStringInfoSpaces(single_query_str, query_location);
	}

	appendBinaryStringInfo(single_query_str, query_string + query_location, query_len);

	/* set the tail part to spaces */
	if (query_location + query_len < total_len)
	{
		appendStringInfoSpaces(single_query_str, total_len - (query_location + query_len));
	}

	Assert(single_query_str->len == total_len);

	return single_query_str->data;
}

/*
 * Decide whether to use the local latest gts or the remote gtm gts
 * in the snapshot based on the parse tree.
 */
static Snapshot
GetSnapshotByParseTree(RawStmt *parsetree)
{
    if (unlikely(ReadWithLocalTs))
    {
        return GetLocalTransactionSnapshot();
    }

	if (enable_gts_optimize &&
		IS_PGXC_LOCAL_COORDINATOR &&
		XactIsoLevel == XACT_READ_COMMITTED &&
		parsetree != NULL)
	{
		switch (nodeTag(parsetree->stmt))
		{
			/* support to use local latest gts */
			case T_InsertStmt:
			case T_DeleteStmt:
			case T_UpdateStmt:
			case T_SelectStmt:
            case T_AlterNodeStmt:
            case T_CreateNodeStmt:
            case T_DropNodeStmt:
            case T_ExecDirectStmt:
				elog(DEBUG1, "parsetree statement type: %d, use local latest gts",
					nodeTag(parsetree->stmt));
				return GetLocalTransactionSnapshot();

			/* not support to use local latest gts */
			default:
				elog(DEBUG1, "parsetree statement type: %d, use remote gts",
					nodeTag(parsetree->stmt));
		}
	}

	return GetTransactionSnapshot();
}

/*
 * exec_simple_query
 *
 * Execute a "simple Query" protocol message.
 */
static void
exec_simple_query(const char *query_string)
{
	CommandDest dest = whereToSendOutput;
	MemoryContext oldcontext;
	List	   *parsetree_list;
	ListCell   *parsetree_item;
	bool		save_log_statement_stats = log_statement_stats;
	bool		was_logged = false;
	bool		isTopLevel;
	char		msec_str[32];
	bool		multiCommands = false;
    origin_query_string = debug_query_string = query_string;

	TRACE_POSTGRESQL_QUERY_START(query_string);

	/*
	 * We use save_log_statement_stats so ShowUsage doesn't report incorrect
	 * results because ResetUsage wasn't called.
	 */
	if (save_log_statement_stats)
		ResetUsage();

	/*
	 * Start up a transaction command.  All queries generated by the
	 * query_string will be in this same command block, *unless* we find a
	 * BEGIN/COMMIT/ABORT statement; we have to force a new xact command after
	 * one of those, else bad things will happen in xact.c. (Note that this
	 * will normally change current memory context.)
	 */
	start_xact_command();


	/*
	 * Zap any pre-existing unnamed statement.  (While not strictly necessary,
	 * it seems best to define simple-Query mode as if it used the unnamed
	 * statement and portal; this ensures we recover any storage used by prior
	 * unnamed operations.)
	 */
	drop_unnamed_stmt();

	/*
	 * Switch to appropriate context for constructing parsetrees.
	 */
	oldcontext = MemoryContextSwitchTo(MessageContext);

	/*
	 * Do basic parsing of the query or queries (this should be safe even if
	 * we are in aborted transaction state!)
	 */
	if (post_query_replace_hook)
	{
		char *rewrite_query = NULL;

		if ((*post_query_replace_hook)(query_string, &rewrite_query))
		{
			query_string = pstrdup(rewrite_query);
			pfree(rewrite_query);
		}
	}

	parsetree_list = pg_parse_query(query_string);


#ifdef XCP
	if (IS_PGXC_LOCAL_COORDINATOR && list_length(parsetree_list) > 1)
	{
		/*
		 * There is a bug in old code, if one query contains multiple utility
		 * statements, entire query may be sent multiple times to the Datanodes
		 * for execution. That is becoming a severe problem, if query contains
		 * COMMIT or ROLLBACK. After executed for the first time the transaction
		 * handling statement would write CLOG entry for current xid, but other
		 * executions would be done with the same xid, causing PANIC on the
		 * Datanodes because of already existing CLOG record. Datanode is
		 * restarting all sessions if it PANICs, and affects all cluster users.
		 * Multiple utility statements may result in strange error messages,
		 * but somteime they work, and used in many applications, so we do not
		 * want to disable them completely, just protect against severe
		 * vulnerability here.
		 */
		foreach(parsetree_item, parsetree_list)
		{
			Node	   *parsetree = (Node *) lfirst(parsetree_item);

			if (IsTransactionExitStmt(parsetree))
				ereport(ERROR,
						(errcode(ERRCODE_STATEMENT_TOO_COMPLEX),
						 errmsg("COMMIT or ROLLBACK "
								"in multi-statement queries not allowed")));
		}

		mult_sql_query_containing = true;
	}
	else
		mult_sql_query_containing = false;

	/*
	 * XXX We may receive multi-command string and the coordinator is not
	 * equipped to handle multiple command-complete messages. So just send a
	 * single command-complete until we fix the coordinator side of things
	 */
	if (!IS_LOCAL_ACCESS_NODE && list_length(parsetree_list) > 1)
		multiCommands = true;
#endif

	/* Log immediately if dictated by log_statement */
	if (check_log_statement(parsetree_list))
	{
		ereport(LOG,
				(errmsg("statement: %s", debug_query_string),
				 errhidestmt(true),
				 errdetail_execute(parsetree_list)));
		was_logged = true;
	}

	/*
	 * Switch back to transaction context to enter the loop.
	 */
	MemoryContextSwitchTo(oldcontext);

	/*
	 * We'll tell PortalRun it's a top-level command iff there's exactly one
	 * raw parsetree.  If more than one, it's effectively a transaction block
	 * and we want PreventTransactionChain to reject unsafe commands. (Note:
	 * we're assuming that query rewrite cannot add commands that are
	 * significant to PreventTransactionChain.)
	 */
	isTopLevel = (list_length(parsetree_list) == 1);

	/*
	 * Run through the raw parsetree(s) and process each one.
	 */
	foreach(parsetree_item, parsetree_list)
	{
		RawStmt    *parsetree = lfirst_node(RawStmt, parsetree_item);
		bool		snapshot_set = false;
		const char *commandTag;
		char		completionTag[COMPLETION_TAG_BUFSIZE];
		List	   *querytree_list,
				   *plantree_list;
		Portal		portal;
		DestReceiver *receiver;
		int16		format;
#ifdef __OPENTENBASE_C__
		Oid			*relids = NULL;
		char		*resultcache = NULL;
		uint32		queryid = 0;
		bool		return_cache = false;
#endif
		const char	*single_origin_query_str;
		const char *current_query_string;

		/*
		 * OK to analyze, rewrite, and plan this query.
		 *
		 * Switch to appropriate context for constructing querytrees (again,
		 * these must outlive the execution context).
		 */
		oldcontext = MemoryContextSwitchTo(MessageContext);

		/* get this portal's query when has multi parse tree */
		current_query_string = isTopLevel ? debug_query_string : 
											(const char *)get_single_query_string(debug_query_string, parsetree);

		/*
		 * single_origin_query_str  
		 *		- always keeps the original query string 
		 *
		 * current_query_string 
		 * 		- keeps sanitized query stirng if the original query string
		 * 		  contains sensitive data. otherwise, same as single_origin_query_str
		 */	
		if (debug_query_string != origin_query_string)
			single_origin_query_str = isTopLevel ? origin_query_string:
					(const char *)get_single_query_string(origin_query_string, parsetree);
		else
			single_origin_query_str = current_query_string;
#ifdef PGXC

		/*
		 * By default we do not want Datanodes or client Coordinators to contact GTM directly,
		 * it should get this information passed down to it.
		 */
		if (IS_PGXC_DATANODE || IsConnFromCoord())
			SetForceXidFromGTM(false);
#endif

		/*
		 * Switch back to transaction context
		 */
		MemoryContextSwitchTo(oldcontext);

		/*
		 * Get the command name for use in status display (it also becomes the
		 * default completion tag, down inside PortalRun).  Set ps_status and
		 * do any special start-of-SQL-command processing needed by the
		 * destination.
		 */
		commandTag = CreateCommandTag(parsetree->stmt);

#ifdef __AUDIT_FGA__
		g_commandTag = commandTag;
#endif

#ifdef __OPENTENBASE__
		HeavyLockCheck(commandTag, CMD_UNKNOWN,current_query_string, parsetree);

		explain_stmt = false;

		if (strcmp(commandTag, "EXPLAIN") == 0)
		{
			explain_stmt = true;
		}
#endif

		set_ps_display(commandTag, false);

		BeginCommand(commandTag, dest);

		/*
		 * If we are in an aborted transaction, reject all commands except
		 * COMMIT/ABORT.  It is important that this test occur before we try
		 * to do parse analysis, rewrite, or planning, since all those phases
		 * try to do database accesses, which may fail in abort state. (It
		 * might be safe to allow some additional utility commands in this
		 * state, but not many...)
		 */
		if (IsAbortedTransactionBlockState() &&
			!IsTransactionExitStmt(parsetree->stmt))
			ereport(ERROR,
					(errcode(ERRCODE_IN_FAILED_SQL_TRANSACTION),
					 errmsg("current transaction is aborted, "
							"commands ignored until end of transaction block"),
					 errdetail_abort()));

		/* Make sure we are in a transaction command */
		start_xact_command();
		/* If we got a cancel signal in parsing or prior command, quit */
		CHECK_FOR_INTERRUPTS();

		/*
		 * Set up a snapshot if parse analysis/planning will need one.
		 */
		if (analyze_requires_snapshot(parsetree) && g_snapshot_for_analyze)
		{
			PushActiveSnapshot(GetSnapshotByParseTree(parsetree));
			snapshot_set = true;
		}

		/*
		 * OK to analyze, rewrite, and plan this query.
		 *
		 * Switch to appropriate context for constructing querytrees (again,
		 * these must outlive the execution context).
		 */
		oldcontext = MemoryContextSwitchTo(MessageContext);

		querytree_list = pg_analyze_and_rewrite(parsetree,
												current_query_string, NULL, 0,
												NULL);
												
#ifdef __AUDIT__
		if (xact_started)
		{
			AuditReadQueryList(current_query_string, querytree_list);
		}
#endif
#ifdef __OPENTENBASE_C__
		if (dest == DestRemote
			&& PG_PROTOCOL_MAJOR(FrontendProtocol) >= 3
			&& CheckQuerySatisfyCache(querytree_list))
		{
			Query *query = linitial_node(Query, querytree_list);
			
			queryid = QueryTreeGetEncodedQueryId(query);
			resultcache = SearchQueryResultCache(queryid);
			if (resultcache == NULL)
			{
				relids = QueryTreeGetRelids(query);
			}
		}
		/* check again in case we convert CALL to SELECT */
		if (!snapshot_set)
		{
			ListCell   *query_list;
			foreach (query_list, querytree_list)
			{
				Query	   *query = lfirst_node(Query, query_list);
				if (query->commandType != CMD_UTILITY)
				{
					PushActiveSnapshot(GetTransactionSnapshot());
					snapshot_set = true;
					break;
				}
			}
		}
#endif
		plantree_list = pg_plan_queries(querytree_list,
										CURSOR_OPT_PARALLEL_OK, NULL);

		/* Done with the snapshot used for parsing/planning */
		if (snapshot_set)
			PopActiveSnapshot();

		/* If we got a cancel signal in analysis or planning, quit */
		CHECK_FOR_INTERRUPTS();

#ifdef PGXC
		/* 
		 * Force getting Xid from GTM for vacuum, cluster and reindex for
		 * database or schema
		 */
		if (IS_PGXC_DATANODE && IsPostmasterEnvironment)
		{
			if (IsA(parsetree->stmt, VacuumStmt) || IsA(parsetree->stmt, ClusterStmt))
				 SetForceXidFromGTM(true);
			else if (IsA(parsetree->stmt, ReindexStmt))
			{
				ReindexStmt *stmt = (ReindexStmt *) parsetree->stmt;
				if (stmt->kind == REINDEX_OBJECT_SCHEMA ||
					stmt->kind == REINDEX_OBJECT_DATABASE)
					SetForceXidFromGTM(true);
			}
		}
#endif
#ifdef __OPENTENBASE_C__
		/*
		 * Check if query tree satisfy result cache
		 *
		 */
		if (resultcache != NULL)
		{
			/*
			 * Send result to client if valid result is in cache
			 */
			receiver = CreateDestReceiver(dest);
			printtup_resultcache(resultcache);
			(*receiver->rShutdown) (receiver);
			(*receiver->rDestroy) (receiver);
			pfree(resultcache);
			return_cache = true;
		}
		
		if (!return_cache)
		{
#endif
		/*
		 * Create unnamed portal to run the query or queries in. If there
		 * already is one, silently drop it.
		 */
		portal = CreatePortal("", true, true, false, parsetree->stmt);
		/* Don't display the portal in pg_cursors */
		portal->visible = false;
#ifdef __OPENTENBASE__
		/*
		 * try to transform 'insert into values...' to 'COPY FROM'
		 */
		if (IS_PGXC_COORDINATOR && IsA(parsetree->stmt, InsertStmt) &&
			list_length(plantree_list) == 1)
		{
			PlannedStmt *planstmt = (PlannedStmt *)linitial(plantree_list);
			Query *parse = planstmt->parseTree;

			/* only handle multi-values without triggers */
			if (parse->isMultiValues && !parse->hasUnshippableDml)
			{
				bool success;
				InsertStmt *insert_stmt = (InsertStmt*)parsetree->stmt;

				plantree_list = transformInsertValuesIntoCopyFromPlan(NULL, insert_stmt, &success,
				                                                      parse->copy_filename, parse);
			}
		}
#endif
		/*
		 * We don't have to copy anything into the portal, because everything
		 * we are passing here is in MessageContext, which will outlive the
		 * portal anyway.
		 */
		PortalDefineQuery(portal,
						  NULL,
						  single_origin_query_str,
						  commandTag,
						  plantree_list,
						  NULL);

		/*
		 * Start the portal.  No parameters here.
		 */
		PortalStart(portal, NULL, 0, InvalidSnapshot);

		/*
		 * Select the appropriate output format: text unless we are doing a
		 * FETCH from a binary cursor.  (Pretty grotty to have to do this here
		 * --- but it avoids grottiness in other places.  Ah, the joys of
		 * backward compatibility...)
		 */
		format = 0;				/* TEXT is default */
		if (IsA(parsetree->stmt, FetchStmt))
		{
			FetchStmt  *stmt = (FetchStmt *) parsetree->stmt;

			if (!stmt->ismove)
			{
				Portal		fportal = GetPortalByName(stmt->portalname);

				if (PortalIsValid(fportal) &&
					(fportal->cursorOptions & CURSOR_OPT_BINARY))
					format = 1; /* BINARY */
			}
		}
		PortalSetResultFormat(portal, 1, &format);

		/*
		 * Now we can create the destination receiver object.
		 */
		receiver = CreateDestReceiver(dest);
		if (dest == DestRemote)
		{
			SetRemoteDestReceiverParams(receiver, portal);
#ifdef __OPENTENBASE_C__
			if (relids != NULL)
				SetRemoteDestReceiverResultCache(receiver, relids, queryid);
#endif
		}

		/*
		 * Switch back to transaction context for execution.
		 */
		MemoryContextSwitchTo(oldcontext);

		/*
		 * Run the portal to completion, and then drop it (and the receiver).
		 */
		(void) PortalRun(portal,
						 FETCH_ALL,
						 isTopLevel,
						 true,
						 receiver,
						 receiver,
						 completionTag);

		receiver->rDestroy(receiver);

		PortalDrop(portal, false);
#ifdef __OPENTENBASE_C__
		}
#endif

		if (IsA(parsetree->stmt, TransactionStmt))
		{
#ifdef __AUDIT__
			if (xact_started)
			{
				AuditProcessResultInfo(true);
			}
#endif
			/*
			 * If this was a transaction control statement, commit it. We will
			 * start a new xact command for the next command (if any).
			 */
			finish_xact_command();
		}
		else if (lnext(parsetree_item) == NULL)
		{
#ifdef __AUDIT__
			if (xact_started)
			{
				AuditProcessResultInfo(true);
			}
#endif
			/*
			 * If this is the last parsetree of the query string, close down
			 * transaction statement before reporting command-complete.  This
			 * is so that any end-of-transaction errors are reported before
			 * the command-complete message is issued, to avoid confusing
			 * clients who will expect either a command-complete message or an
			 * error, not one and then the other.  But for compatibility with
			 * historical Postgres behavior, we do not force a transaction
			 * boundary between queries appearing in a single query string.
			 */
			finish_xact_command();
		}
		else
		{
			/*
			 * We need a CommandCounterIncrement after every query, except
			 * those that start or end a transaction block.
			 */
			CommandCounterIncrement();
		}

		/*
		 * Tell client that we're done with this query.  Note we emit exactly
		 * one EndCommand report for each raw parsetree, thus one for each SQL
		 * command the client sent, regardless of rewriting. (But a command
		 * aborted by error will not send an EndCommand report at all.)
		 */
		if (!multiCommands)
			EndCommand(completionTag, dest);
	}							/* end loop over parsetrees */

	if (multiCommands)
		EndCommand("MultiCommand", dest);

#ifdef __AUDIT__
	if (xact_started)
	{
		AuditProcessResultInfo(true);
	}
#endif

	/*
	 * Close down transaction statement, if one is open.
	 */
	finish_xact_command();
	/*
	 * If there were no parsetrees, return EmptyQueryResponse message.
	 */
	if (!parsetree_list)
		NullCommand(dest);

	/*
	 * Emit duration logging if appropriate.
	 */
	switch (check_log_duration(msec_str, was_logged))
	{
		case 1:
			ereport(LOG,
					(errmsg("duration: %s ms", msec_str),
					 errhidestmt(true)));
			break;
		case 2:
			ereport(LOG,
					(errmsg("duration: %s ms  statement: %s",
							msec_str, query_string),
					 errhidestmt(true),
					 errdetail_execute(parsetree_list)));
			break;
	}

	if (save_log_statement_stats)
		ShowUsage("QUERY STATISTICS");

	TRACE_POSTGRESQL_QUERY_DONE(query_string);



	debug_query_string = NULL;
}

/*
 * exec_parse_message
 *
 * Execute a "Parse" protocol message.
 * If paramTypeNames is specified, paraTypes is filled with corresponding OIDs.
 * The caller is expected to allocate space for the paramTypes.
 */
static void
exec_parse_message(const char *query_string,	/* string to execute */
				   const char *stmt_name,	/* name for prepared stmt */
				   Oid *paramTypes, /* parameter types */
				   char **paramTypeNames,	/* parameter type names */
				   int numParams /* number of parameters */)
{
	MemoryContext unnamed_stmt_context = NULL;
	MemoryContext oldcontext;
	List	   *parsetree_list;
	RawStmt    *raw_parse_tree;
	const char *commandTag;
	List	   *querytree_list;
	CachedPlanSource *psrc;
	bool		is_named;
	bool		save_log_statement_stats = log_statement_stats;
	char		msec_str[32];
#ifdef __OPENTENBASE__
	bool        use_resowner = false;
#endif
    ExecNodes   *single_exec_node = NULL;

	/*
	 * Report query to various monitoring facilities.
	 */
	debug_query_string = query_string;
	set_ps_display("PARSE", false);

	if (save_log_statement_stats)
		ResetUsage();

	ereport(DEBUG2,
			(errmsg("parse %s: %s",
					*stmt_name ? stmt_name : "<unnamed>",
					query_string)));

	/*
	 * Start up a transaction command so we can run parse analysis etc. (Note
	 * that this will normally change current memory context.) Nothing happens
	 * if we are already in one.
	 */
	start_xact_command();

	/*
	 * Switch to appropriate context for constructing parsetrees.
	 *
	 * We have two strategies depending on whether the prepared statement is
	 * named or not.  For a named prepared statement, we do parsing in
	 * MessageContext and copy the finished trees into the prepared
	 * statement's plancache entry; then the reset of MessageContext releases
	 * temporary space used by parsing and rewriting. For an unnamed prepared
	 * statement, we assume the statement isn't going to hang around long, so
	 * getting rid of temp space quickly is probably not worth the costs of
	 * copying parse trees.  So in this case, we create the plancache entry's
	 * query_context here, and do all the parsing work therein.
	 */
	is_named = (stmt_name[0] != '\0');
	if (is_named)
	{
		/* Named prepared statement --- parse in MessageContext */
		oldcontext = MemoryContextSwitchTo(MessageContext);

#ifdef __OPENTENBASE__
		use_resowner = NeedResourceOwner(stmt_name);

		if (use_resowner && IS_PGXC_COORDINATOR)
		{
			elog(ERROR, "name with prefix %s/%s/%s for prepared stmt is forbidden",
						INSERT_TRIGGER, UPDATE_TRIGGER, DELETE_TRIGGER);
		}
#endif
	}
	else
	{
		/* Unnamed prepared statement --- release any prior unnamed stmt */
		drop_unnamed_stmt();
		/* Create context for parsing */
		unnamed_stmt_context =
			AllocSetContextCreate(MessageContext,
								  "unnamed prepared statement",
								  ALLOCSET_DEFAULT_SIZES);
		oldcontext = MemoryContextSwitchTo(unnamed_stmt_context);
	}

#ifdef PGXC
	/*
	 * if we have the parameter types passed, which happens only in case of
	 * connection from Coordinators, fill paramTypes with their OIDs for
	 * subsequent use. We have to do name to OID conversion, in a transaction
	 * context.
	 */
	if (IsConnFromCoord() || IsConnFromDatanode())
	{
		int cnt_param;

		if (paramTypeNames)
		{
			/* we don't expect type mod */
			for (cnt_param = 0; cnt_param < numParams; cnt_param++)
				parseTypeString(paramTypeNames[cnt_param], &paramTypes[cnt_param],
								NULL, false);
		}
	}
#endif /* PGXC */

	/*
	 * Do basic parsing of the query or queries (this should be safe even if
	 * we are in aborted transaction state!)
	 */
	if (post_query_replace_hook)
	{
		char *rewrite_query = NULL;

		if ((*post_query_replace_hook)(query_string, &rewrite_query))
		{
			query_string = pstrdup(rewrite_query);
			pfree(rewrite_query);
		}
	}

	parsetree_list = pg_parse_query(query_string);

	/*
	 * We only allow a single user statement in a prepared statement. This is
	 * mainly to keep the protocol simple --- otherwise we'd need to worry
	 * about multiple result tupdescs and things like that.
	 */
	if (list_length(parsetree_list) > 1)
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("cannot insert multiple commands into a prepared statement")));

	if (parsetree_list != NIL)
	{
		Query	   *query;
		bool		snapshot_set = false;
		int			i;

		raw_parse_tree = linitial_node(RawStmt, parsetree_list);

		/*
		 * Get the command name for possible use in status display.
		 */
		commandTag = CreateCommandTag(raw_parse_tree->stmt);
		
#ifdef __AUDIT_FGA__
		g_commandTag = pnstrdup(commandTag, strlen(commandTag));
#endif

#ifdef __OPENTENBASE__
		explain_stmt = false;

		if (strcmp(commandTag, "EXPLAIN") == 0)
		{
			explain_stmt = true;
		}

		HeavyLockCheck(commandTag, CMD_UNKNOWN, query_string, raw_parse_tree);
#endif

		/*
		 * If we are in an aborted transaction, reject all commands except
		 * COMMIT/ROLLBACK.  It is important that this test occur before we
		 * try to do parse analysis, rewrite, or planning, since all those
		 * phases try to do database accesses, which may fail in abort state.
		 * (It might be safe to allow some additional utility commands in this
		 * state, but not many...)
		 */
		if (IsAbortedTransactionBlockState() &&
			!IsTransactionExitStmt(raw_parse_tree->stmt))
			ereport(ERROR,
					(errcode(ERRCODE_IN_FAILED_SQL_TRANSACTION),
					 errmsg("current transaction is aborted, "
							"commands ignored until end of transaction block"),
					 errdetail_abort()));
		/*
		 * Create the CachedPlanSource before we do parse analysis, since it
		 * needs to see the unmodified raw parse tree.
		 */
#ifdef PGXC
		psrc = CreateCachedPlan(raw_parse_tree, query_string, stmt_name, commandTag);
#else
		psrc = CreateCachedPlan(raw_parse_tree, query_string, commandTag);
#endif

		/*
		 * Set up a snapshot if parse analysis will need one.
		 */
		if (analyze_requires_snapshot(raw_parse_tree) && g_snapshot_for_analyze)
		{
			PushActiveSnapshot(GetSnapshotByParseTree(raw_parse_tree));
			snapshot_set = true;
		}

		/*
		 * Analyze and rewrite the query.  Note that the originally specified
		 * parameter set is not required to be complete, so we have to use
		 * parse_analyze_varparams().
		 */
		if (log_parser_stats)
			ResetUsage();

		query = parse_analyze_varparams(raw_parse_tree,
										query_string,
										&paramTypes,
										&numParams,
										IsConnFromCoord() || IsConnFromDatanode());

#ifdef __OPENTENBASE__
		if (query->isMultiValues && !query->hasUnshippableDml)
		{
			InsertStmt *src_insert = (InsertStmt *)raw_parse_tree->stmt;
			InsertStmt *dest_insert = (InsertStmt *)psrc->raw_parse_tree->stmt;
			psrc->insert_into = true;
			dest_insert->ninsert_columns = src_insert->ninsert_columns;
		}
#endif

		/*
		 * Check all parameter types got determined.
		 */
		for (i = 0; i < numParams; i++)
		{
			Oid			ptype = paramTypes[i];

			if (ptype == InvalidOid || ptype == UNKNOWNOID)
				ereport(ERROR,
						(errcode(ERRCODE_INDETERMINATE_DATATYPE),
						 errmsg("could not determine data type of parameter $%d",
								i + 1)));
		}

		if (log_parser_stats)
			ShowUsage("PARSE ANALYSIS STATISTICS");
#ifdef PGXC
		if (query->commandType == CMD_UTILITY &&
		    IsA(query->utilityStmt, CreateTableAsStmt))
		{
			/*
			 * CREATE TABLE AS SELECT and SELECT INTO are rewritten so that the
			 * target table is created first. The SELECT query is then transformed
			 * into an INSERT INTO statement
			 */
			List        *ctas_list = QueryRewriteCTAS(query, NULL, NULL, NULL, NULL, 0);
			ListCell    *cell;
			
			querytree_list = NIL;
			foreach(cell, ctas_list)
			{
				querytree_list = list_concat_unique(querytree_list, pg_rewrite_query((Query *) lfirst(cell)));
			}
		}
		else
#endif
		querytree_list = pg_rewrite_query(query);

		if (IS_PGXC_COORDINATOR && !IsConnFromCoord())
		{
			bool runOnSingleNode = false;
			ListCell* lc = NULL;

			runOnSingleNode = list_length(querytree_list) == 1 &&
							!IsA(raw_parse_tree, CreateTableAsStmt) &&
							!IsA(raw_parse_tree, RefreshMatViewStmt);

			foreach (lc, querytree_list)
			{
				Query* cur_query = (Query*)lfirst(lc);

				if (runOnSingleNode)
				{
					if (!cur_query->returningList)
						single_exec_node = checkLightQuery(cur_query);

					/* only deal with single node/param */
					if (single_exec_node != NULL && single_exec_node->dis_exprs)
					{
						runOnSingleNode = true;
					}
					else
					{
						runOnSingleNode = false;
						FreeExecNodes(&single_exec_node);
					}
				}
			}
		}

		/* Done with the snapshot used for parsing */
		if (snapshot_set)
			PopActiveSnapshot();
	}
	else
	{
		/* Empty input string.  This is legal. */
		raw_parse_tree = NULL;
		commandTag = NULL;
#ifdef PGXC
		psrc = CreateCachedPlan(raw_parse_tree, query_string, stmt_name, commandTag);
#else
		psrc = CreateCachedPlan(raw_parse_tree, query_string, commandTag);
#endif
		querytree_list = NIL;
	}

	/*
	 * CachedPlanSource must be a direct child of MessageContext before we
	 * reparent unnamed_stmt_context under it, else we have a disconnected
	 * circular subgraph.  Klugy, but less so than flipping contexts even more
	 * above.
	 */
	if (unnamed_stmt_context)
		MemoryContextSetParent(psrc->context, MessageContext);

	/* Finish filling in the CachedPlanSource */
	CompleteCachedPlan(psrc,
					   querytree_list,
					   unnamed_stmt_context,
					   paramTypes,
					   numParams,
					   NULL,
					   NULL,
					   CURSOR_OPT_PARALLEL_OK,	/* allow parallel mode */
					   true,	/* fixed result */
					   single_exec_node,
					   IsConnFromCoord() || IsConnFromDatanode());

	/* If we got a cancel signal during analysis, quit */
	CHECK_FOR_INTERRUPTS();

	if (is_named)
	{
		/*
		 * Store the query as a prepared statement.
		 */
#ifdef __OPENTENBASE__
		if (use_resowner)
		{
			StorePreparedStatement(stmt_name, psrc, false, true);
		}
		else
#endif
		StorePreparedStatement(stmt_name, psrc, false, false);
	}
	else
	{
		/*
		 * We just save the CachedPlanSource into unnamed_stmt_psrc.
		 */
		SaveCachedPlan(psrc);
		unnamed_stmt_psrc = psrc;
	}

	MemoryContextSwitchTo(oldcontext);

	/*
	 * We do NOT close the open transaction command here; that only happens
	 * when the client sends Sync.  Instead, do CommandCounterIncrement just
	 * in case something happened during parse/plan.
	 */
	CommandCounterIncrement();

	/*
	 * Send ParseComplete.
	 */
	if (whereToSendOutput == DestRemote)
		pq_putemptymessage('1');

	/*
	 * Emit duration logging if appropriate.
	 */
	switch (check_log_duration(msec_str, false))
	{
		case 1:
			ereport(LOG,
					(errmsg("duration: %s ms", msec_str),
					 errhidestmt(true)));
			break;
		case 2:
			ereport(LOG,
					(errmsg("duration: %s ms  parse %s: %s",
							msec_str,
							*stmt_name ? stmt_name : "<unnamed>",
							query_string),
					 errhidestmt(true)));
			break;
	}

	if (save_log_statement_stats)
		ShowUsage("PARSE MESSAGE STATISTICS");

	debug_query_string = NULL;
}

#ifdef XCP
/*
 * exec_plan_message
 *
 * Execute a "Plan" protocol message - already planned statement.
 */
static void
exec_plan_message(const char *query_string,	/* source of the query */
				  const char *stmt_name,		/* name for prepared stmt */
				  const char *plan_string,		/* encoded plan to execute */
				  char **paramTypeNames,	/* parameter type names */
				  int numParams,		/* number of parameters */
				  int instrument_options)		/* explain analyze option */
{
	MemoryContext oldcontext;
	bool		save_log_statement_stats = log_statement_stats;
	char		msec_str[32];
	Oid		   *paramTypes = NULL;
	CachedPlanSource *psrc;

	/* Statement name should not be empty */
	Assert(stmt_name[0]);

	/*
	 * Report query to various monitoring facilities.
	 */
	debug_query_string = query_string;
#ifdef _MLS_
	if (is_mls_user())
	{
		debug_query_string = mls_query_string_prune(debug_query_string);
	}
	pgstat_report_activity(STATE_RUNNING, debug_query_string);
#endif

	set_ps_display("PLAN", false);

	if (save_log_statement_stats)
		ResetUsage();

	ereport(DEBUG2,
			(errmsg("plan %s: %s",
					*stmt_name ? stmt_name : "<unnamed>",
					query_string)));

	/*
	 * Start up a transaction command so we can decode plan etc. (Note
	 * that this will normally change current memory context.) Nothing happens
	 * if we are already in one.
	 */
	start_xact_command();

	/*
	 * XXX
	 * Postgres decides about memory context to use based on "named/unnamed"
	 * assuming named statement is executed multiple times and unnamed is
	 * executed once.
	 * Plan message always provide statement name, but we may use different
	 * criteria, like if plan is referencing "internal" parameters it probably
	 * will be executed multiple times, if not - once.
	 * So far optimize for multiple executions.
	 */
	/* Named prepared statement --- parse in MessageContext */
	oldcontext = MemoryContextSwitchTo(MessageContext);
//	unnamed_stmt_context =
//		AllocSetContextCreate(CacheMemoryContext,
//							  "unnamed prepared statement",
//							  ALLOCSET_DEFAULT_MINSIZE,
//							  ALLOCSET_DEFAULT_INITSIZE,
//							  ALLOCSET_DEFAULT_MAXSIZE);
//	oldcontext = MemoryContextSwitchTo(unnamed_stmt_context);

	/*
	 * Determine parameter types
	 */
	if (numParams > 0)
	{
		int cnt_param;
		paramTypes = (Oid *) palloc(numParams * sizeof(Oid));
		/* we don't expect type mod */
		for (cnt_param = 0; cnt_param < numParams; cnt_param++)
			parseTypeString(paramTypeNames[cnt_param], &paramTypes[cnt_param],
							NULL, false);

	}

	/* If we got a cancel signal, quit */
	CHECK_FOR_INTERRUPTS();
	
	psrc = CreateCachedPlan(NULL, query_string, stmt_name, NULL);

	CompleteCachedPlan(psrc, NIL, NULL, paramTypes, numParams, NULL, NULL,
					   CURSOR_OPT_GENERIC_PLAN, false, NULL, false);

	/*
	 * Store the query as a prepared statement.  See above comments.
	 */
	StorePreparedStatement(stmt_name, psrc, false, true);

	/*
	 * In some special scenarios, SQL needs to be executed during the scanDatum(geography_in) stage.
	 * For example, in the case of PostGIS, when processing geography_in, it is necessary to
	 * read data from the table spatial_ref_sys. Here, we need to obtain a snapshot.
	 */
	PushActiveSnapshot(GetTransactionSnapshot());
	SetRemoteSubplan(psrc, plan_string);
	PopActiveSnapshot();
#ifdef __OPENTENBASE_C__
	/* set instrument_options, default 0 */
	psrc->instrument_options = instrument_options;
#endif

	MemoryContextSwitchTo(oldcontext);

	/*
	 * We do NOT close the open transaction command here; that only happens
	 * when the client sends Sync.	Instead, do CommandCounterIncrement just
	 * in case something happened during parse/plan.
	 */
	CommandCounterIncrement();

	/*
	 * Send ParseComplete.
	 */
	if (whereToSendOutput == DestRemote)
		pq_putemptymessage('1');

	/*
	 * Emit duration logging if appropriate.
	 */
	switch (check_log_duration(msec_str, false))
	{
		case 1:
			ereport(LOG,
					(errmsg("duration: %s ms", msec_str),
					 errhidestmt(true)));
			break;
		case 2:
			ereport(LOG,
					(errmsg("duration: %s ms  parse %s: %s",
							msec_str,
							*stmt_name ? stmt_name : "<unnamed>",
							query_string),
					 errhidestmt(true)));
			break;
	}

	if (save_log_statement_stats)
		ShowUsage("PLAN MESSAGE STATISTICS");

	debug_query_string = NULL;
}
#endif

/*
 * exec_bind_message
 *
 * Process a "Bind" message to create a portal from a prepared statement
 */
static void
exec_bind_message(StringInfo input_message)
{
	const char       *portal_name;
	const char       *stmt_name;
	int               numPFormats;
	int16            *pformats = NULL;
	int               numParams;
	int               numRFormats;
	int16            *rformats = NULL;
	CachedPlanSource *psrc;
	CachedPlan       *cplan;
	Portal            portal;
	char             *query_string;
	char             *saved_stmt_name;
	ParamListInfo     params;
	MemoryContext     oldContext;
	bool              save_log_statement_stats = log_statement_stats;
	bool              snapshot_set = false;
	char              msec_str[32];
	int               ncolumns = 0;
	int               index;
	char           ***data_list = NULL;
	char            **param_string_list = NULL;
	int               param_string_list_len = 0;
	int               param_string_idx = 0;

	/* Get the fixed part of the message */
	portal_name = pq_getmsgstring(input_message);
	stmt_name = pq_getmsgstring(input_message);

	ereport(DEBUG2,
			(errmsg("bind %s to %s",
					*portal_name ? portal_name : "<unnamed>",
					*stmt_name ? stmt_name : "<unnamed>")));

	elog(DEBUG2, "pid:%d, exec_bind_message:%s %s", MyProcPid, *portal_name ? portal_name : "<unnamed>",
			   *stmt_name ? stmt_name : "<unnamed>");

	/* Find prepared statement */
	if (stmt_name[0] != '\0')
	{
		PreparedStatement *pstmt;

		pstmt = FetchPreparedStatement(stmt_name, true);
		psrc = pstmt->plansource;
	}
	else
	{
		/* special-case the unnamed statement */
		psrc = unnamed_stmt_psrc;
		if (!psrc)
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_PSTATEMENT),
					 errmsg("unnamed prepared statement does not exist")));
	}

	/*
	 * Report query to various monitoring facilities.
	 */
	debug_query_string = psrc->query_string;
#ifdef _MLS_
	if (is_mls_user())
	{
		debug_query_string = mls_query_string_prune(debug_query_string);
	}
	pgstat_report_activity(STATE_RUNNING, debug_query_string);
#endif

#ifdef __OPENTENBASE__
	HeavyLockCheck(psrc->commandTag, CMD_UNKNOWN, NULL, NULL);
#endif

	set_ps_display("BIND", false);

	if (save_log_statement_stats)
		ResetUsage();

	/*
	 * Start up a transaction command so we can call functions etc. (Note that
	 * this will normally change current memory context.) Nothing happens if
	 * we are already in one.
	 */
	start_xact_command();

	/* Switch back to message context */
	MemoryContextSwitchTo(MessageContext);

	/* Get the parameter format codes */
	numPFormats = pq_getmsgint(input_message, 2);
	if (numPFormats > 0)
	{
		int			i;

		pformats = (int16 *) palloc(numPFormats * sizeof(int16));
		for (i = 0; i < numPFormats; i++)
			pformats[i] = pq_getmsgint(input_message, 2);
	}

	/* Get the parameter value count */
	numParams = pq_getmsgint(input_message, 2);

	if (numPFormats > 1 && numPFormats != numParams)
		ereport(ERROR,
				(errcode(ERRCODE_PROTOCOL_VIOLATION),
				 errmsg("bind message has %d parameter formats but %d parameters",
						numPFormats, numParams)));

	if (numParams != psrc->num_params)
		ereport(ERROR,
				(errcode(ERRCODE_PROTOCOL_VIOLATION),
				 errmsg("bind message supplies %d parameters, but prepared statement \"%s\" requires %d",
						numParams, stmt_name, psrc->num_params)));

#ifdef __OPENTENBASE__
	/*
	 * if we told to do copy from instead of insert into multi-values,
	 * also need to do some check to ensure we can do copy from 
	 */
	if (psrc->insert_into)
	{
		Query *parse = (Query *)linitial(psrc->query_list);

		/* reset stmt_list */
		if (psrc->gplan)
		{
			if (psrc->gplan->stmt_list_backup)
			{
				psrc->gplan->stmt_list = psrc->gplan->stmt_list_backup;
			}
		}

		if (IsA(parse, Query) && parse->commandType == CMD_INSERT)
		{
			if (psrc->raw_parse_tree && IsA(psrc->raw_parse_tree->stmt, InsertStmt))
			{
				/* number of params must be times the insert columns */
				InsertStmt *insert_stmt = (InsertStmt *)psrc->raw_parse_tree->stmt;
				ncolumns = insert_stmt->ninsert_columns;

				if (ncolumns == 0)
				{
					psrc->insert_into = false;
				}
			}
			else
			{
				psrc->insert_into = false;
			}
		}
		else
		{
			psrc->insert_into = false;
		}

		if (psrc->insert_into)
		{
			/*
			 * In order to distinguish the call path of the transformInsertStmt function, 
			 * at least one memory is still required for the extension protocol that does not pass parameters
			 */
			param_string_list_len = numParams ? numParams : 1;
			param_string_list = (char **)palloc0(sizeof(char *) * param_string_list_len);
		}
	}
#endif

	/*
	 * If we are in aborted transaction state, the only portals we can
	 * actually run are those containing COMMIT or ROLLBACK commands. We
	 * disallow binding anything else to avoid problems with infrastructure
	 * that expects to run inside a valid transaction.  We also disallow
	 * binding any parameters, since we can't risk calling user-defined I/O
	 * functions.
	 */
	if (IsAbortedTransactionBlockState() &&
		(!(psrc->raw_parse_tree &&
		   IsTransactionExitStmt(psrc->raw_parse_tree->stmt)) ||
		 numParams != 0))
		ereport(ERROR,
				(errcode(ERRCODE_IN_FAILED_SQL_TRANSACTION),
				 errmsg("current transaction is aborted, "
						"commands ignored until end of transaction block"),
				 errdetail_abort()));

	/*
	 * Create the portal.  Allow silent replacement of an existing portal only
	 * if the unnamed portal is specified.
	 */
	if (portal_name[0] == '\0')
		portal = CreatePortal(portal_name, true, true, false, NULL);
	else
		portal = CreatePortal(portal_name, false, false, false, NULL);

	/*
	 * Prepare to copy stuff into the portal's memory context.  We do all this
	 * copying first, because it could possibly fail (out-of-memory) and we
	 * don't want a failure to occur between GetCachedPlan and
	 * PortalDefineQuery; that would result in leaking our plancache refcount.
	 */
	oldContext = MemoryContextSwitchTo(portal->portalContext);

	/* Copy the plan's query string into the portal */
	query_string = pstrdup(psrc->query_string);

#ifdef __AUDIT_FGA__
	if (portal && portal->commandTag)
	{
		g_commandTag = pnstrdup(portal->commandTag, strlen(portal->commandTag));
	}
	else
	{
		g_commandTag = NULL;
	}
#endif

	/*
	 * Do basic parsing of the query or queries (this should be safe even if
	 * we are in aborted transaction state!)
	 */
	if (post_query_replace_hook)
	{
		char *rewrite_query = NULL;

		if ((*post_query_replace_hook)(query_string, &rewrite_query))
		{
			pfree(query_string);
			query_string = pstrdup(rewrite_query);
			debug_query_string = query_string;
			pfree(rewrite_query);
		}
	}

	/* Likewise make a copy of the statement name, unless it's unnamed */
	if (stmt_name[0])
		saved_stmt_name = pstrdup(stmt_name);
	else
		saved_stmt_name = NULL;

	/*
	 * Set a snapshot if we have parameters to fetch (since the input
	 * functions might need it) or the query isn't a utility command (and
	 * hence could require redoing parse analysis and planning).  We keep the
	 * snapshot active till we're done, so that plancache.c doesn't have to
	 * take new ones.
	 */
	if ((numParams > 0 ||
		(psrc->raw_parse_tree &&
		 analyze_requires_snapshot(psrc->raw_parse_tree))) && g_snapshot_for_analyze)
	{
		PushActiveSnapshot(GetSnapshotByParseTree(psrc->raw_parse_tree));
		snapshot_set = true;
	}

	/*
	 * Fetch parameters, if any, and store in the portal's memory context.
	 */
	if (numParams > 0)
	{
		int			paramno;

		params = (ParamListInfo) palloc0(offsetof(ParamListInfoData, params) +
										numParams * sizeof(ParamExternData));
		/* we have static list of params, so no hooks needed */
		params->paramFetch = NULL;
		params->paramFetchArg = NULL;
		params->paramCompile = NULL;
		params->paramCompileArg = NULL;
		params->parserSetup = NULL;
		params->parserSetupArg = NULL;
		params->numParams = numParams;
#ifdef __OPENTENBASE__
		index = 0;
#endif
		for (paramno = 0; paramno < numParams; paramno++)
		{
			Oid			ptype = psrc->param_types[paramno];
			int32		plength;
			Datum		pval;
			bool		isNull;
			StringInfoData pbuf;
			char		csave;
			int16		pformat;

			plength = pq_getmsgint(input_message, 4);
#ifdef _PG_ORCL_
			/*
			 * treat blank string as NULL value in opentenbase_ora compatible mode.
			 */
			if (ORA_MODE || enable_lightweight_ora_syntax)
				isNull = (plength == -1 || plength == 0);
			else
#endif
			isNull = (plength == -1);

			if (!isNull)
			{
				const char *pvalue = pq_getmsgbytes(input_message, plength);

				/*
				 * Rather than copying data around, we just set up a phony
				 * StringInfo pointing to the correct portion of the message
				 * buffer.  We assume we can scribble on the message buffer so
				 * as to maintain the convention that StringInfos have a
				 * trailing null.  This is grotty but is a big win when
				 * dealing with very large parameter strings.
				 */
				pbuf.data = (char *) pvalue;
				pbuf.maxlen = plength + 1;
				pbuf.len = plength;
				pbuf.cursor = 0;

				csave = pbuf.data[plength];
				pbuf.data[plength] = '\0';
			}
			else
			{
				pbuf.data = NULL;	/* keep compiler quiet */
				csave = 0;
			}
#ifdef __OPENTENBASE__
			index++;
#endif
			if (numPFormats > 1)
				pformat = pformats[paramno];
			else if (numPFormats > 0)
				pformat = pformats[0];
			else
				pformat = 0;	/* default = text */

			if (pformat == 0)	/* text mode */
			{
				Oid			typinput;
				Oid			typioparam;
				char	   *pstring;

				getTypeInputInfo(ptype, &typinput, &typioparam);

				/*
				 * We have to do encoding conversion before calling the
				 * typinput routine.
				 */
				if (isNull
#ifdef _PG_ORCL_
						|| ptype == PLUDTOID
#endif
								)
					pstring = NULL;
				else
				{
#ifdef __OPENTENBASE__
					if (enable_null_string && IS_PGXC_LOCAL_COORDINATOR)
					{
						switch (ptype)
						{
							case CHAROID: 
							case TEXTOID:
							case VARCHAROID:
							case BPCHAROID:
							case VARCHAR2OID:
							case NVARCHAR2OID:
								{
									replace_null_with_blank(pbuf.data, plength);
									break;
								}
							default:
								break;
						}
					}
#endif
					pstring = pg_client_to_server(pbuf.data, plength);
				}

				pval = OidInputFunctionCall(typinput, pstring, typioparam, -1);

				if (psrc->insert_into)
				{
					if (isNull)
					{
						param_string_list[param_string_idx] = NULL;
					}
					else
					{
						param_string_list[param_string_idx] = pstrdup(pstring);
					}
					param_string_idx++;
				}

				/* Free result of encoding conversion, if any */
				if (pstring && pstring != pbuf.data)
					pfree(pstring);
			}
			else if (pformat == 1)	/* binary mode */
			{
				Oid			typreceive;
				Oid			typioparam;
				StringInfo	bufptr;

				/*
				 * Call the parameter type's binary input converter
				 */
				getTypeBinaryInputInfo(ptype, &typreceive, &typioparam);

				if (isNull)
					bufptr = NULL;
				else
					bufptr = &pbuf;

				pval = OidReceiveFunctionCall(typreceive, bufptr, typioparam, -1);

				if (psrc->insert_into)
				{
					if (isNull)
					{
						param_string_list[param_string_idx] = NULL;
					}
					else
					{
						Oid		typOutput;
						bool	typIsVarlena;
						Datum	value;
						char   *pstring;

						/* Get info needed to output the value */
						getTypeOutputInfo(ptype, &typOutput, &typIsVarlena);

						/*
						 * If we have a toasted datum, forcibly detoast it here to avoid
						 * memory leakage inside the type's output routine.
						 */
						if (typIsVarlena)
							value = PointerGetDatum(PG_DETOAST_DATUM(pval));
						else
							value = pval;

						/* Convert Datum to string */
						pstring = OidOutputFunctionCall(typOutput, value);
						param_string_list[param_string_idx] = pstrdup(pstring);
					}
					param_string_idx++;
				}

#ifdef __OPENTENBASE__
				if (enable_null_string && IS_PGXC_LOCAL_COORDINATOR && !isNull)
				{
					int length = 0;
					char *src = NULL;
					char c;

					switch (ptype)
					{
						case CHAROID:
							{
								c = DatumGetChar(pval);
								length = 1;
								src = &c;
								break;
							}
						case TEXTOID:
						case VARCHAROID:
						case BPCHAROID:
						case VARCHAR2OID:
						case NVARCHAR2OID:
							{
								length = VARSIZE_ANY_EXHDR(pval);
								src = VARDATA_ANY(pval);
								break;
							}
						default:
							break;
					}

					if (length)
					{
						replace_null_with_blank(src, length);
					}
				}
#endif

				/* Trouble if it didn't eat the whole buffer */
				if (!isNull && pbuf.cursor != pbuf.len)
					ereport(ERROR,
							(errcode(ERRCODE_INVALID_BINARY_REPRESENTATION),
							 errmsg("incorrect binary data format in bind parameter %d",
									paramno + 1)));
			}
			else
			{
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("unsupported format code: %d",
								pformat)));
				pval = 0;		/* keep compiler quiet */
			}

			/* Restore message buffer contents */
			if (!isNull)
				pbuf.data[plength] = csave;

			params->params[paramno].value = pval;
			params->params[paramno].isnull = isNull;

			/*
			 * We mark the params as CONST.  This ensures that any custom plan
			 * makes full use of the parameter values.
			 */
			params->params[paramno].pflags = PARAM_FLAG_CONST;
			params->params[paramno].ptype = ptype;
		}
	}
	else
		params = NULL;

	if (psrc->insert_into)
	{
		int                   i;
		ParseState           *pstate;
		InsertToCopyInfoData  infodata = {0};
		InsertStmt           *insert_stmt = (InsertStmt *) psrc->raw_parse_tree->stmt;

		pstate = make_parsestate(NULL);
		parse_variable_parameters(pstate, &psrc->param_types, &psrc->num_params);

		infodata.params = params;
		infodata.param_string_list = param_string_list;
		infodata.partialinsert = false;
		infodata.cxt = psrc->context;

		data_list = transformInsertValuesToCopyForBind(pstate, psrc->query_list, (SelectStmt *) insert_stmt->selectStmt, ncolumns, &infodata);

		/* free param_string_list */
		for (i = 0; i < param_string_list_len; i++)
		{
			if (param_string_list[i])
				pfree(param_string_list[i]);
		}
		pfree(param_string_list);
	}

	/* Done storing stuff in portal's context */
	MemoryContextSwitchTo(oldContext);

	/* Get the result format codes */
	numRFormats = pq_getmsgint(input_message, 2);
	if (numRFormats > 0)
	{
		int			i;

		rformats = (int16 *) palloc(numRFormats * sizeof(int16));
		for (i = 0; i < numRFormats; i++)
			rformats[i] = pq_getmsgint(input_message, 2);
	}

	pq_getmsgend(input_message);

	/*
	 * Obtain a plan from the CachedPlanSource.  Any cruft from (re)planning
	 * will be generated in MessageContext.  The plan refcount will be
	 * assigned to the Portal, so it will be released at portal destruction.
	 */
	cplan = GetCachedPlan(psrc, params, NULL, NULL);

	cplan->stmt_list_backup = NULL;
	if (psrc->insert_into && data_list)
	{
		bool          success;
		MemoryContext old;
		char *        copy_filename = NULL;
		Query *       parse = (Query *) linitial(psrc->query_list);
		InsertStmt *  insert_stmt = (InsertStmt *) psrc->raw_parse_tree->stmt;
		SelectStmt *  selectStmt = (SelectStmt *) insert_stmt->selectStmt;
		char ***      old_data_list = insert_stmt->data_list;

		cplan->stmt_list_backup = cplan->stmt_list;

		old = MemoryContextSwitchTo(psrc->context);
		copy_filename = palloc(MAXPGPATH);
		snprintf(copy_filename, MAXPGPATH, "%s", "Insert_into to Copy_from(Extended Protocol)");

		/* free old data_list */
		DeepfreeCopyDatalist(old_data_list, insert_stmt->ndatarows, insert_stmt->ninsert_columns);

		insert_stmt->data_list = data_list;
		insert_stmt->ndatarows = list_length(selectStmt->valuesLists);

		cplan->stmt_list =
			transformInsertValuesIntoCopyFromPlan(NULL,
		                                          (InsertStmt *) psrc->raw_parse_tree->stmt,
		                                          &success,
		                                          copy_filename,
		                                          parse);
		MemoryContextSwitchTo(old);
	}

	/*
	 * Now we can define the portal.
	 *
	 * DO NOT put any code that could possibly throw an error between the
	 * above GetCachedPlan call and here.
	 */
	PortalDefineQuery(portal,
					  saved_stmt_name,
					  query_string,
					  psrc->commandTag,
					  cplan->stmt_list,
					  cplan);
#ifdef __OPENTENBASE_C__
	/* set instrument before PortalStart, default 0 */
	portal->up_instrument = psrc->instrument_options;
#endif

	/* Done with the snapshot used for parameter I/O and parsing/planning */
	if (snapshot_set)
		PopActiveSnapshot();

	if (IS_PGXC_LOCAL_COORDINATOR && psrc->resultDesc)
		portal->cn_penetrate_tupleDesc = psrc->resultDesc;

	/*
	 * And we're ready to start portal execution.
	 */
	PortalStart(portal, params, 0, InvalidSnapshot);

	/*
	 * Apply the result format requests to the portal.
	 */
	PortalSetResultFormat(portal, numRFormats, rformats);

	/*
	 * Send BindComplete.
	 */
	if (whereToSendOutput == DestRemote)
	{
		if (!IsConnFromApp())
			pq_putmessage('9', (const char *)&internal_cmd_sequence, 1);
		pq_putemptymessage('2');
	}

	/*
	 * Emit duration logging if appropriate.
	 */
	switch (check_log_duration(msec_str, false))
	{
		case 1:
			ereport(LOG,
					(errmsg("duration: %s ms", msec_str),
					 errhidestmt(true)));
			break;
		case 2:
			ereport(LOG,
					(errmsg("duration: %s ms  bind %s%s%s: %s",
							msec_str,
							*stmt_name ? stmt_name : "<unnamed>",
							*portal_name ? "/" : "",
							*portal_name ? portal_name : "",
							psrc->query_string),
					 errhidestmt(true),
					 errdetail_params(params)));
			break;
	}

	if (save_log_statement_stats)
		ShowUsage("BIND MESSAGE STATISTICS");

	debug_query_string = NULL;

#ifdef __OPENTENBASE_C__
	/* If we got a cancel signal, quit */
	CHECK_FOR_INTERRUPTS();
#endif
}

/*
 * exec_execute_message
 *
 * Process an "Execute" message for a portal
 */
static void
exec_execute_message(const char *portal_name, long max_rows, bool rewind)
{
	CommandDest dest;
	DestReceiver *receiver;
	Portal		portal;
	bool		completed;
	char		completionTag[COMPLETION_TAG_BUFSIZE];
	const char *sourceText;
	const char *prepStmtName;
	ParamListInfo portalParams;
	bool		save_log_statement_stats = log_statement_stats;
	bool		is_xact_command;
	bool		execute_is_fetch;
	bool		was_logged = false;
	char		msec_str[32];

	/* Adjust destination to tell printtup.c what to do */
	dest = whereToSendOutput;
	if (dest == DestRemote)
		dest = DestRemoteExecute;

	portal = GetPortalByName(portal_name);
	if (!PortalIsValid(portal))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_CURSOR),
				 errmsg("portal \"%s\" does not exist. ", portal_name)));

	/*
	 * If the original query was a null string, just return
	 * EmptyQueryResponse.
	 */
	if (portal->commandTag == NULL)
	{
		Assert(portal->stmts == NIL);
		NullCommand(dest);
		return;
	}

	if (rewind && portal->atEnd && portal->queryDesc)
	{
		QueryDesc  *queryDesc = portal->queryDesc;
		Portal		saveActivePortal;
		ResourceOwner saveResourceOwner;
		MemoryContext savePortalContext;
		MemoryContext oldcxt = CurrentMemoryContext;

		/*
		 * If we're preserving a holdable portal, we had better be inside the
		 * transaction that originally created it.
		 */
		Assert(portal->createSubid != InvalidSubTransactionId);
		Assert(queryDesc != NULL);

		/*
		 * Caller must have created the tuplestore already ... but not a snapshot.
		 */
		Assert(portal->holdContext != NULL);
		Assert(portal->holdStore != NULL);
		Assert(portal->holdSnapshot == NULL);

		/*
		 * Set up global portal context pointers.
		 */
		saveActivePortal = ActivePortal;
		saveResourceOwner = CurrentResourceOwner;
		savePortalContext = PortalContext;
		PG_TRY();
		{
			ActivePortal = portal;
			if (portal->resowner)
				CurrentResourceOwner = portal->resowner;
			PortalContext = portal->portalContext;

			/* Rewind holdStore, if we have one */
			if (portal->holdStore)
			{
				MemoryContextSwitchTo(portal->holdContext);
				tuplestore_rescan(portal->holdStore);
			}

			MemoryContextSwitchTo(PortalContext);

			/*
			 * Rewind the executor: we need to store the entire result set in the
			 * tuplestore, so that subsequent backward FETCHs can be processed.
			 */
			PushActiveSnapshot(queryDesc->snapshot);
			ExecutorRewind(queryDesc);
			PopActiveSnapshot();
		}
		PG_CATCH();
		{
			/* Uncaught error while executing portal: mark it dead */
			MarkPortalFailed(portal);

			/* Restore global vars and propagate error */
			ActivePortal = saveActivePortal;
			CurrentResourceOwner = saveResourceOwner;
			PortalContext = savePortalContext;

			PG_RE_THROW();
		}
		PG_END_TRY();

		MemoryContextSwitchTo(oldcxt);

		ActivePortal = saveActivePortal;
		CurrentResourceOwner = saveResourceOwner;
		PortalContext = savePortalContext;

		/* reset portal pos */
		portal->atStart = true;
		portal->atEnd = false;
		portal->portalPos = 0;
	}

	/* Does the portal contain a transaction command? */
	is_xact_command = IsTransactionStmtList(portal->stmts);

	/*
	 * We must copy the sourceText and prepStmtName into MessageContext in
	 * case the portal is destroyed during finish_xact_command. Can avoid the
	 * copy if it's not an xact command, though.
	 */
	if (is_xact_command)
	{
		sourceText = pstrdup(portal->sourceText);
		if (portal->prepStmtName)
			prepStmtName = pstrdup(portal->prepStmtName);
		else
			prepStmtName = "<unnamed>";

		/*
		 * An xact command shouldn't have any parameters, which is a good
		 * thing because they wouldn't be around after finish_xact_command.
		 */
		portalParams = NULL;
	}
	else
	{
		sourceText = portal->sourceText;
		if (portal->prepStmtName)
			prepStmtName = portal->prepStmtName;
		else
			prepStmtName = "<unnamed>";
		portalParams = portal->portalParams;
	}

	/*
	 * Report query to various monitoring facilities.
	 */
	debug_query_string = sourceText;

#ifdef _MLS_
	if (is_mls_user())
	{
		debug_query_string = mls_query_string_prune(debug_query_string);
	}
	pgstat_report_activity(STATE_RUNNING, debug_query_string);
#endif

	set_ps_display(portal->commandTag, false);

	if (save_log_statement_stats)
		ResetUsage();

	BeginCommand(portal->commandTag, dest);

	/*
	 * Create dest receiver in MessageContext (we don't want it in transaction
	 * context, because that may get deleted if portal contains VACUUM).
	 */
	receiver = CreateDestReceiver(dest);
	if (dest == DestRemoteExecute)
		SetRemoteDestReceiverParams(receiver, portal);

	/*
	 * Ensure we are in a transaction command (this should normally be the
	 * case already due to prior BIND).
	 */
	start_xact_command();

	/*
	 * If we re-issue an Execute protocol request against an existing portal,
	 * then we are only fetching more rows rather than completely re-executing
	 * the query from the start. atStart is never reset for a v3 portal, so we
	 * are safe to use this check.
	 */
	execute_is_fetch = !portal->atStart;

	/* Log immediately if dictated by log_statement */
	if (check_log_statement(portal->stmts))
	{
		ereport(LOG,
				(errmsg("%s %s%s%s: %s",
						execute_is_fetch ?
						_("execute fetch from") :
						_("execute"),
						prepStmtName,
						*portal_name ? "/" : "",
						*portal_name ? portal_name : "",
#ifdef _MLS_						
						debug_query_string),
#endif						
				 errhidestmt(true),
				 errdetail_params(portalParams)));
		was_logged = true;
	}

	/*
	 * If we are in aborted transaction state, the only portals we can
	 * actually run are those containing COMMIT or ROLLBACK commands.
	 */
	if (IsAbortedTransactionBlockState() &&
		!IsTransactionExitStmtList(portal->stmts))
		ereport(ERROR,
				(errcode(ERRCODE_IN_FAILED_SQL_TRANSACTION),
				 errmsg("current transaction is aborted, "
						"commands ignored until end of transaction block"),
				 errdetail_abort()));

	/* Check for cancel signal before we start execution */
	CHECK_FOR_INTERRUPTS();

#ifdef __AUDIT__
	if (xact_started)
	{
		Query * parse_tree = NULL;
		const char * query_string = NULL;
		bool ret = PortalGetQueryInfo(portal, &parse_tree, &query_string);

		if (ret == true && parse_tree != NULL && query_string != NULL)
		{
			AuditReadQueryList(query_string, list_make1(parse_tree));
		}
	}
#endif

	/*
	 * Okay to run the portal.
	 */
	if (max_rows <= 0)
		max_rows = FETCH_ALL;

#ifdef __RESOURCE_QUEUE__
	if (IS_PGXC_DATANODE && IsResQueueEnabled() && !superuser())
	{
		ResQUsageAddLocalResourceInfo();
	}
#endif

	completed = PortalRun(portal,
						  max_rows,
						  true, /* always top level */
						  !execute_is_fetch && max_rows == FETCH_ALL,
						  receiver,
						  receiver,
						  completionTag);

	receiver->rDestroy(receiver);

#ifdef __OPENTENBASE__
	if (portal->cplan && portal->cplan->stmt_list_backup)
	{
		portal->cplan->stmt_list = portal->cplan->stmt_list_backup;
		portal->cplan->stmt_list_backup = NULL;
	}
#endif

#ifdef __RESOURCE_QUEUE__
	if (IS_PGXC_DATANODE && IsResQueueEnabled() && !superuser())
	{
		ResQUsageRemoveLocalResourceInfo();
	}
#endif

#ifdef __AUDIT__
	if (xact_started)
	{
		AuditProcessResultInfo(true);
	}
#endif

	if (completed)
	{
#ifdef __OPENTENBASE_C__
		if (portal->up_instrument)
			SendLocalInstr(portal->queryDesc);
#endif
		if (is_xact_command)
		{
			/*
			 * If this was a transaction control statement, commit it.  We
			 * will start a new xact command for the next command (if any).
			 */
			finish_xact_command();
		}
		else
		{
			/*
			 * We need a CommandCounterIncrement after every query, except
			 * those that start or end a transaction block.
			 * CN will definitely send an 'H' message after 'E' message, To 
			 * reduce overheada flush we do not flush commandid immediately.
			 * then when the DN processes the 'H' message sent by CN, it
			 * will flush the commandid to CN.
			 */
			CommandCounterIncrementNotFlushCid();
		}

		/* Send appropriate CommandComplete to client */
		EndCommand(completionTag, dest);

		elog(DEBUG1, "pid %d EndCommand", MyProcPid);
	}
	else
	{
		/* Portal run not complete, so send PortalSuspended */
		if (whereToSendOutput == DestRemote)
			pq_putemptymessage('s');
	}

	/*
	 * Emit duration logging if appropriate.
	 */
	switch (check_log_duration(msec_str, was_logged))
	{
		case 1:
			ereport(LOG,
					(errmsg("duration: %s ms", msec_str),
					 errhidestmt(true)));
			break;
		case 2:
			ereport(LOG,
					(errmsg("duration: %s ms  %s %s%s%s: %s",
							msec_str,
							execute_is_fetch ?
							_("execute fetch from") :
							_("execute"),
							prepStmtName,
							*portal_name ? "/" : "",
							*portal_name ? portal_name : "",
							sourceText),
					 errhidestmt(true),
					 errdetail_params(portalParams)));
			break;
	}

	if (save_log_statement_stats)
		ShowUsage("EXECUTE MESSAGE STATISTICS");

	debug_query_string = NULL;
}

static CmdType 
set_cmd_type(const char* commandTag)
{
    CmdType cmd_type = CMD_UNKNOWN;
    if (strcmp(commandTag, "SELECT") == 0)
        cmd_type = CMD_SELECT;
    else if (strcmp(commandTag, "UPDATE") == 0)
        cmd_type = CMD_UPDATE;
    else if (strcmp(commandTag, "INSERT") == 0)
        cmd_type = CMD_INSERT;
    else if (strcmp(commandTag, "DELETE") == 0)
        cmd_type = CMD_DELETE;
    else if (strcmp(commandTag, "MERGE") == 0)
        cmd_type = CMD_MERGE;
    return cmd_type;
}

/*
 * exec_batch_bind_execute
 * main entry of execute batch bind-execute message
 */
static void 
exec_batch_bind_execute(StringInfo input_message)
{
    /* special for U message */
    int batch_count;
    CmdType cmd_type;
    int* params_set_end = NULL;
    bool send_DP_msg = false;
    int process_count = 0;
    CommandDest dest;
    StringInfoData process_result;
    /* use original logic if not SELECT/INSERT/UPDATE/DELETE */
    bool use_original_logic = false;

    /* like B message */
    const char* portal_name = NULL;
    const char* stmt_name = NULL;
    int numPFormats;
    int16* pformats = NULL;
    int numParams;
    int numRFormats;
    int16* rformats = NULL;
    CachedPlanSource* psrc = NULL;
    ParamListInfo* params_set = NULL;
    MemoryContext oldContext;
    bool save_log_statement_stats = log_statement_stats;
    bool snapshot_set = false;

    int msg_type;
    /* D message */
    int describe_type = 0;
    const char* describe_target = NULL;
    /* E message */
    const char* exec_portal_name = NULL;
    int max_rows;
    PreparedStatement *pstmt = NULL;
    List* parse_list;
    
    /* 'V' Message format: many B-like and one D and one E message together
     * batchCount
     * portal_name
     * stmt_name
     * numPFormats
     * PFormats
     * numRFormats
     * RFormats
     * numParams
     * Params1
     * Params2...
     * 'D'
     * describe_type
     * describe_target
     * 'E'
     * portal_name
     * max_rows
     */
    /* A. Parse message */
    /* 1.specila for U message */
    /* batchCount: count of bind */
    batch_count = pq_getmsgint(input_message, 4);
    if (batch_count <= 0 || batch_count > PG_INT32_MAX / (int)sizeof(ParamListInfo))
        ereport(ERROR, (errcode(ERRCODE_PROTOCOL_VIOLATION),
                errmsg("Batch bind-execute message with invalid batch count: %d", batch_count)));

    /* 2.B-like message */
    /* Get the fixed part of the message */
    portal_name = pq_getmsgstring(input_message);
    stmt_name = pq_getmsgstring(input_message);
    
    if (portal_name[0] != '\0')
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("Not support portal_name %s for Batch bind-execute.", portal_name)));

    ereport(DEBUG2,
            (errmsg("Batch bind-execute %s to %s, batch count %d",
                    *portal_name ? portal_name : "<unnamed>",
                    *stmt_name ? stmt_name : "<unnamed>",
                    batch_count)));

    /* Find prepared statement */
    if (stmt_name[0] != '\0') 
    {
        pstmt = FetchPreparedStatement(stmt_name, true);
        psrc = pstmt->plansource;
    }
    else 
    {
        /* special-case the unnamed statement */
        psrc = unnamed_stmt_psrc;
        if (psrc == NULL)
            ereport(ERROR, (errcode(ERRCODE_UNDEFINED_PSTATEMENT), 
                    errmsg("unnamed prepared statement does not exist")));

		if ((!psrc->stmt_name) || psrc->stmt_name[0] == '\0')
			ereport(ERROR, (errcode(ERRCODE_UNDEFINED_PSTATEMENT),
				errmsg("exec_batch_bind_execute unnamed prepared statement does not supported")));
    }

    Assert(NULL != psrc);

    /* Check command type: only support IUD */
    initStringInfo(&process_result);
    cmd_type = set_cmd_type(psrc->commandTag);
    switch (cmd_type)
    {
        case CMD_INSERT:
            appendStringInfo(&process_result, "INSERT 0 ");
            break;
        case CMD_UPDATE:
            appendStringInfo(&process_result, "UPDATE ");
            break;
        /*   maybe supported in the future
        case CMD_DELETE:
            appendStringInfo(&process_result, "DELETE ");
            break;
        case CMD_SELECT:
            appendStringInfo(&process_result, "SELETE ");
            break;
        case CMD_MERGE:
            appendStringInfo(&process_result, "MERGE ");
            break;
        */    
        default:
            use_original_logic = true;
            ereport(LOG,
                    (errmsg("Not support Batch bind-execute for %s: stmt_name %s, query %s",
                            psrc->commandTag,
                            psrc->stmt_name,
                            psrc->query_string)));
            break;
    }

    /*
     * Report query to various monitoring facilities.
     */
    debug_query_string = psrc->query_string;
#ifdef _MLS_
    if (is_mls_user())
    {
        debug_query_string = mls_query_string_prune(debug_query_string);
    }
    pgstat_report_activity(STATE_RUNNING, debug_query_string);
#endif

    HeavyLockCheck(psrc->commandTag, CMD_UNKNOWN, NULL, NULL);
    
    set_ps_display(psrc->commandTag, false);

    if (save_log_statement_stats)
    {
        ResetUsage();
    }

    /*
     * Start up a transaction command so we can call functions etc. (Note that
     * this will normally change current memory context.) Nothing happens if
     * we are already in one.
     */
    start_xact_command();

    /*
     * Begin to parse the big B-like message.
     * First, common part:
     * numPFormats, PFormats
     * numRFormats, RFormats
     * numParams
     */

    /* Switch back to message context */
    oldContext = MemoryContextSwitchTo(MessageContext);

    /* Get the parameter format codes */
    numPFormats = pq_getmsgint(input_message, 2);
    if (numPFormats > PG_UINT16_MAX)
    {
        ereport(ERROR,
                (errcode(ERRCODE_PROTOCOL_VIOLATION),
                 errmsg("Batch bind-execute message with invalid parameter "
                        "number: %d", numPFormats)));
    }
    if (numPFormats > 0)
    {
        int i;
        
        pformats = (int16*) palloc0(numPFormats * sizeof(int16));
        for (i = 0; i < numPFormats; i++)
        {
            pformats[i] = pq_getmsgint(input_message, 2);
        }
    }

    /* Get the result format codes */
    numRFormats = pq_getmsgint(input_message, 2);
    if (numRFormats > PG_UINT16_MAX)
    {
        ereport(ERROR,
                (errcode(ERRCODE_PROTOCOL_VIOLATION),
                 errmsg("Batch bind-execute message with invalid "
                        "parameter number: %d", numRFormats)));
    }
    
    if (numRFormats > 0)
    {
        int i;
        
        rformats = (int16*)palloc0(numRFormats * sizeof(int16));
        for (i = 0; i < numRFormats; i++)
        {
            rformats[i] = pq_getmsgint(input_message, 2);
        }
    }

    /* Get the parameter value count */
    numParams = pq_getmsgint(input_message, 2);
    if (numPFormats > 1 && numPFormats != numParams)
        ereport(ERROR,
                (errcode(ERRCODE_PROTOCOL_VIOLATION),
                 errmsg("bind message has %d parameter formats but %d parameters",
                         numPFormats, numParams)));

    if (numParams != psrc->num_params)
        ereport(ERROR,
                (errcode(ERRCODE_PROTOCOL_VIOLATION),
                 errmsg("bind message supplies %d parameters, but prepared statement \"%s\" requires %d",
                         numParams, stmt_name, psrc->num_params)));

    /*
     * If we are in aborted transaction state, the only portals we can
     * actually run are those containing COMMIT or ROLLBACK commands. We
     * disallow binding anything else to avoid problems with infrastructure
     * that expects to run inside a valid transaction.	We also disallow
     * binding any parameters, since we can't risk calling user-defined I/O
     * functions.
     */
    if (IsAbortedTransactionBlockState() && 
        (!IsTransactionExitStmt((Node*)psrc->raw_parse_tree) || 
        numParams != 0))
        ereport(ERROR,
                (errcode(ERRCODE_IN_FAILED_SQL_TRANSACTION),
                 errmsg("current transaction is aborted, "
                        "commands ignored until end of transaction block"),
                         errdetail_abort()));

    /*
     * Set a snapshot if we have parameters to fetch (since the input
     * functions might need it) or the query isn't a utility command (and
     * hence could require redoing parse analysis and planning).  We keep the
     * snapshot active till we're done, so that plancache.c doesn't have to
     * take new ones.
     */
    if ((numParams > 0 ||
         (psrc->raw_parse_tree &&
          analyze_requires_snapshot(psrc->raw_parse_tree))) && g_snapshot_for_analyze)
    {
        PushActiveSnapshot(GetTransactionSnapshot());
        snapshot_set = true;
    }
    
    /* record the params set position for light cn to contruct batch message */
    if (psrc->single_exec_node != NULL)
    {
        params_set_end = (int*)palloc0((batch_count + 1) * sizeof(int));
        /* keep the end pos of message before params at last */
        params_set_end[0] = input_message->cursor;
    }
    
    /* Second, process each set of params */
    params_set = (ParamListInfo*)palloc0(batch_count * sizeof(ParamListInfo));
    if (numParams > 0)
    {
        int i = 0;
        for (i = 0; i < batch_count; i++)
        {
            int paramno = 0;
            ParamListInfo params =
                    (ParamListInfo)palloc0(offsetof(ParamListInfoData, params) + numParams * sizeof(ParamExternData));
            /* we have static list of params, so no hooks needed */
            params->paramFetch = NULL;
            params->paramFetchArg = NULL;
            params->parserSetup = NULL;
            params->parserSetupArg = NULL;
            params->numParams = numParams;

            for (paramno = 0; paramno < numParams; paramno++)
            {
                Oid ptype = psrc->param_types[paramno];
                int32 plength;
                Datum pval;
                bool isNull = false;
                StringInfoData pbuf;
                char csave;
                int16 pformat;

                plength = pq_getmsgint(input_message, 4);

#ifdef _PG_ORCL_
                /*
                 * treat blank string as NULL value in opentenbase_ora compatible mode.
                 */
                if (ORA_MODE)
                    isNull = (plength == -1 || plength == 0);
                else
#endif
                isNull = (plength == -1);

                if (!isNull)
                {
                    const char* pvalue = pq_getmsgbytes(input_message, plength);

                    /*
                     * Rather than copying data around, we just set up a phony
                     * StringInfo pointing to the correct portion of the message
                     * buffer.	We assume we can scribble on the message buffer so
                     * as to maintain the convention that StringInfos have a
                     * trailing null.  This is grotty but is a big win when
                     * dealing with very large parameter strings.
                     */
                    pbuf.data = (char*)pvalue;
                    pbuf.maxlen = plength + 1;
                    pbuf.len = plength;
                    pbuf.cursor = 0;

                    csave = pbuf.data[plength];
                    pbuf.data[plength] = '\0';
                }
                else
                {
                    pbuf.data = NULL; /* keep compiler quiet */
                    csave = 0;
                }

                if (numPFormats > 1)
                {
                    Assert(NULL != pformats);
                    pformat = pformats[paramno];
                } 
                else if (numPFormats > 0)
                {
                    Assert(NULL != pformats);
                    pformat = pformats[0];
                }
                else
                {
                    pformat = 0; /* default = text */
                }

                if (pformat == 0)
                {
                    /* text mode */
                    Oid typinput;
                    Oid typioparam;
                    char* pstring = NULL;

                    getTypeInputInfo(ptype, &typinput, &typioparam);

                    /*
                     * We have to do encoding conversion before calling the
                     * typinput routine.
                     */
                    if (isNull)
                        pstring = NULL;
                    else
                    {
#ifdef __OPENTENBASE__
                        if (enable_null_string && IS_PGXC_LOCAL_COORDINATOR)
                        {
                            switch (ptype)
                            {
                                case CHAROID:
                                case TEXTOID:
                                case VARCHAROID:
                                case BPCHAROID:
                                case VARCHAR2OID:
                                case NVARCHAR2OID:
                                {
                                    replace_null_with_blank(pbuf.data, plength);
                                    break;
                                }
                                default:
                                    break;
                            }
                        }
#endif
                        pstring = pg_client_to_server(pbuf.data, plength);
                    }

                    pval = OidInputFunctionCall(typinput, pstring, typioparam, -1);

                    /* Free result of encoding conversion, if any */
                    if (pstring != NULL && pstring != pbuf.data)
                        pfree(pstring);
                } 
                else if (pformat == 1)
                {
                    /* binary mode */
                    Oid typreceive;
                    Oid typioparam;
                    StringInfo bufptr;

                    /*
                     * Call the parameter type's binary input converter
                     */
                    getTypeBinaryInputInfo(ptype, &typreceive, &typioparam);

                    if (isNull)
                        bufptr = NULL;
                    else
                        bufptr = &pbuf;

                    pval = OidReceiveFunctionCall(typreceive, bufptr, typioparam, -1);

#ifdef __OPENTENBASE__
                    if (enable_null_string && IS_PGXC_LOCAL_COORDINATOR && !isNull)
                    {
                        int length = 0;
                        char *src = NULL;
                        char c;

                        switch (ptype)
                        {
                            case CHAROID:
                            {
                                c = DatumGetChar(pval);
                                length = 1;
                                src = &c;
                                break;
                            }
                            case TEXTOID:
                            case VARCHAROID:
                            case BPCHAROID:
                            case VARCHAR2OID:
                            case NVARCHAR2OID:
                            {
                                length = VARSIZE_ANY_EXHDR(pval);
                                src = VARDATA_ANY(pval);
                                break;
                            }
                            default:
                                break;
                        }

                        if (length)
                        {
                            replace_null_with_blank(src, length);
                        }
                    }
#endif
                    /* Trouble if it didn't eat the whole buffer */
                    if (!isNull && pbuf.cursor != pbuf.len)
                        ereport(ERROR,
                                (errcode(ERRCODE_INVALID_BINARY_REPRESENTATION),
                                        errmsg("incorrect binary data format in bind parameter %d",
                                               paramno + 1)));
                }
                else
                {
                    ereport(ERROR,
                            (errcode(ERRCODE_INVALID_PARAMETER_VALUE), 
                             errmsg("unsupported format code: %d", pformat)));
                    pval = 0; /* keep compiler quiet */
                }

                /* Restore message buffer contents */
                if (!isNull)
                    pbuf.data[plength] = csave;

                params->params[paramno].value = pval;
                params->params[paramno].isnull = isNull;
                
                /*
                 * We mark the params as CONST.  This ensures that any custom plan
                 * makes full use of the parameter values.
                 */
                params->params[paramno].pflags = PARAM_FLAG_CONST;
                params->params[paramno].ptype = ptype;
            }

            /* assign to the set */
            params_set[i] = params;
            if (params_set_end != NULL)
                params_set_end[i + 1] = input_message->cursor;
        }
    }

    /* Log immediately if dictated by log_statement */
    parse_list = list_make1(psrc->raw_parse_tree);
    if (check_log_statement(parse_list))
    {
        ereport(LOG,
                (errmsg("execute batch %s%s%s: %s",
                        psrc->stmt_name,
                        *portal_name ? "/" : "",
                        *portal_name ? portal_name : "",
                        psrc->query_string),
                        errhidestmt(true),
                        errdetail_batch_params(batch_count, numParams, params_set)));
    }
    list_free(parse_list);

    /* msg_type: maybe D or E */
    msg_type = pq_getmsgbyte(input_message);

    /* 3.D message */
    if (msg_type == 'D')
    {
        describe_type = pq_getmsgbyte(input_message);
        describe_target = pq_getmsgstring(input_message);

        if (describe_type == 'S')
        {
            if (strcmp(stmt_name, describe_target))
                ereport(ERROR,
                        (errcode(ERRCODE_PROTOCOL_VIOLATION),
                                errmsg("conflict stmt name in Batch bind-execute message: bind %s, describe %s",
                                       stmt_name,
                                       describe_target)));
        } 
        else if (describe_type == 'P')
        {
            if (strcmp(portal_name, describe_target))
                ereport(ERROR,
                        (errcode(ERRCODE_PROTOCOL_VIOLATION),
                                errmsg("conflict portal name in Batch bind-execute message: bind %s, describe %s",
                                       portal_name,
                                       describe_target)));
        }
        else
        {
            ereport(ERROR,
                    (errcode(ERRCODE_PROTOCOL_VIOLATION),
                            errmsg("invalid DESCRIBE message subtype in Batch bind-execute message: %d", describe_type)));
        }

        /* next should be E */
        msg_type = pq_getmsgbyte(input_message);
    }
    /* 4.E message */
    else if (msg_type == 'E')
    {
        exec_portal_name = pq_getmsgstring(input_message);
        if (strcmp(portal_name, exec_portal_name))
            ereport(ERROR,
                    (errcode(ERRCODE_PROTOCOL_VIOLATION),
                            errmsg("conflict portal name in Batch bind-execute message: bind %s, execute %s",
                                   portal_name,
                                   exec_portal_name)));

        max_rows = pq_getmsgint(input_message, 4);
        if (max_rows > 0)
            ereport(ERROR,
                    (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                            errmsg("Not support max_row in Batch bind-execute message: %d", max_rows)));

        max_rows = 0;
    }
    else
    {
        ereport(ERROR,
                (errcode(ERRCODE_PROTOCOL_VIOLATION), errmsg("invalid value in Batch bind-execute message: %d", msg_type)));
    }

    /* 5.Finish the message */
    pq_getmsgend(input_message);

    /* B.Set message for bind and describe */
    if (whereToSendOutput == DestRemote)
    {
        /* 1.Send BindComplete */
		if (!IsConnFromApp())
			pq_putmessage('9', (const char *)&internal_cmd_sequence, 1);
        pq_putemptymessage('2');

        /* 2.Send Describe */
        if (describe_type != 0)
        {
            switch (describe_type)
            {
                case 'S': 
                {
                    int i = 0;
                    StringInfoData buf;
                    /* Prepared statements shouldn't have changeable result descs */
                    Assert(psrc->fixed_result);

                    /*
                     * First describe the parameters...
                     */
                    pq_beginmessage(&buf, 't'); /* parameter description message type */
                    pq_sendint(&buf, psrc->num_params, 2);
                    for (i = 0; i < psrc->num_params; i++)
                    {
                        Oid ptype = psrc->param_types[i];
                        pq_sendint(&buf, (int)ptype, 4);
                    }
                    pq_endmessage(&buf);

                    /*
                     * Next send RowDescription or NoData to describe the result...
                     */
                    if (psrc->resultDesc && !(isPGXCDataNode && IsConnFromCoord() && !g_need_tuple_descriptor))
                    {
                        /* Get the plan's primary targetlist */
                        List* tlist = CachedPlanGetTargetList(psrc, NULL);
                        SendRowDescriptionMessage(&row_description_buf, 
                                                  psrc->resultDesc, 
                                                  tlist, 
                                                  NULL, 
                                                  NULL);
                    } 
                    else
                        pq_putemptymessage('n'); /* NoData */
                } 
                    break;

                case 'P':
                    /* set the message later before execute */
                    send_DP_msg = true;
                    break;
                default:
                    /* should not be here */
                    break;
            }
        }
    }

    /* Adjust destination to tell printtup.c what to do */
    dest = whereToSendOutput;
    if (dest == DestRemote)
        dest = DestRemoteExecute;

    /* C.Execute each one */
    if (enable_light_coord && !use_original_logic && psrc->single_exec_node != NULL)
    {
        StringInfo desc_msg = NULL;
        StringInfoData describe_body;
        StringInfoData execute_msg;
        int pnameLen = strlen(portal_name) + 1;

        StringInfo batch_msg_dnset;
        int* node_idx_set = NULL;
        int* batch_count_dnset = NULL;
        int* params_size_dnset = NULL;

        /* No nodes found ?? */
        if (NumDataNodes == 0)
            ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT), errmsg("No Datanode defined in cluster")));

        node_idx_set = (int*)palloc0(batch_count * sizeof(int));
        batch_count_dnset = (int*)palloc0(NumDataNodes * sizeof(int));
        params_size_dnset = (int*)palloc0(NumDataNodes * sizeof(int));

        /* 1.get necessary info before construct batch message for each dn */
        light_preprocess_batchmsg_set(
                psrc, params_set, params_set_end, batch_count, node_idx_set, batch_count_dnset, params_size_dnset);

        /* 2.construct batch message for each dn */
        /* construct D message if necessary */
        if (send_DP_msg)
        {
            /* 'D' + 'P' + portal_name(describe_target) */
            describe_body.len = 1 + 1 + pnameLen;
            describe_body.maxlen = describe_body.len + 1;
            describe_body.cursor = 0;
            describe_body.data = (char*)palloc0(describe_body.maxlen);

            describe_body.data[0] = 'D';
            describe_body.data[1] = 'P';
            memcpy(describe_body.data + 2, portal_name, pnameLen);

            desc_msg = &describe_body;
        }

        /* construct E message (all same): 'E', portal name, 0 max_row */
        execute_msg.len = 1 + pnameLen + 4;
        execute_msg.maxlen = execute_msg.len + 1;
        execute_msg.cursor = 0;
        execute_msg.data = (char*)palloc0(execute_msg.maxlen);

        execute_msg.data[0] = 'E';
        memcpy(execute_msg.data + 1, portal_name, pnameLen);

        batch_msg_dnset = light_construct_batchmsg_set(
                input_message, params_set_end, node_idx_set, batch_count_dnset, params_size_dnset, desc_msg, &execute_msg);
        
        process_count = light_execute_batchmsg_set(psrc, node_idx_set, batch_msg_dnset, batch_count_dnset, send_DP_msg);
    } 
    else
    {
        char* completionTag = (char*)palloc0(COMPLETION_TAG_BUFSIZE * sizeof(char));
        List* copyedStmts = NULL;
        MemoryContext tmpCxt = NULL;
        int i = 0;

        for (i = 0; i < batch_count; i++)
        {
            exec_one_in_batch(psrc, params_set[i], numRFormats, rformats,
                              (i == 0) ? send_DP_msg : false, dest, completionTag,
                              stmt_name, &copyedStmts, &tmpCxt, pstmt);
        }
    }


    /* Done with the snapshot used */
    if (snapshot_set)
        PopActiveSnapshot();

    if (!use_original_logic) 
    {
        /* Send appropriate CommandComplete to client */
        appendStringInfo(&process_result, "%d", process_count);
        EndCommand(process_result.data, dest);
    }

    MemoryContextSwitchTo(oldContext);
    
    /* finish_xact_command will be execute in 'S' msg */
    
    if (save_log_statement_stats) 
    {
        ShowUsage("BATCH BIND MESSAGE STATISTICS");
    }
}
/*
 * check_log_statement
 *		Determine whether command should be logged because of log_statement
 *
 * stmt_list can be either raw grammar output or a list of planned
 * statements
 */
static bool
check_log_statement(List *stmt_list)
{
	ListCell   *stmt_item;

	if (log_statement == LOGSTMT_NONE)
		return false;
	if (log_statement == LOGSTMT_ALL)
		return true;

	/* Else we have to inspect the statement(s) to see whether to log */
	foreach(stmt_item, stmt_list)
	{
		Node	   *stmt = (Node *) lfirst(stmt_item);

		if (GetCommandLogLevel(stmt) <= log_statement)
			return true;
	}

	return false;
}

/*
 * check_log_duration
 *		Determine whether current command's duration should be logged
 *
 * Returns:
 *		0 if no logging is needed
 *		1 if just the duration should be logged
 *		2 if duration and query details should be logged
 *
 * If logging is needed, the duration in msec is formatted into msec_str[],
 * which must be a 32-byte buffer.
 *
 * was_logged should be TRUE if caller already logged query details (this
 * essentially prevents 2 from being returned).
 */
int
check_log_duration(char *msec_str, bool was_logged)
{
	if (log_duration || log_min_duration_statement >= 0)
	{
		long		secs;
		int			usecs;
		int			msecs;
		bool		exceeded;

		/*
		 * Since GetCurrentTimestamp() returns OS time, use local time for
		 * statement-start for accurate comparison
		 */
		TimestampDifference(GetCurrentLocalStatementStartTimestamp(),
							GetCurrentTimestamp(),
							&secs, &usecs);
		msecs = usecs / 1000;

		/*
		 * This odd-looking test for log_min_duration_statement being exceeded
		 * is designed to avoid integer overflow with very long durations:
		 * don't compute secs * 1000 until we've verified it will fit in int.
		 */
		exceeded = (log_min_duration_statement == 0 ||
					(log_min_duration_statement > 0 &&
					 (secs > log_min_duration_statement / 1000 ||
					  secs * 1000 + msecs >= log_min_duration_statement)));

		if (exceeded || log_duration)
		{
			snprintf(msec_str, 32, "%ld.%03d",
					 secs * 1000 + msecs, usecs % 1000);
			if (exceeded && !was_logged)
				return 2;
			else
				return 1;
		}
	}

	return 0;
}

/*
 * errdetail_execute
 *
 * Add an errdetail() line showing the query referenced by an EXECUTE, if any.
 * The argument is the raw parsetree list.
 */
static int
errdetail_execute(List *raw_parsetree_list)
{
	ListCell   *parsetree_item;

	foreach(parsetree_item, raw_parsetree_list)
	{
		RawStmt    *parsetree = lfirst_node(RawStmt, parsetree_item);

		if (IsA(parsetree->stmt, ExecuteStmt))
		{
			ExecuteStmt *stmt = (ExecuteStmt *) parsetree->stmt;
			PreparedStatement *pstmt;

			pstmt = FetchPreparedStatement(stmt->name, false);
			if (pstmt)
			{
				errdetail("prepare: %s", pstmt->plansource->query_string);
				return 0;
			}
		}
	}

	return 0;
}

/*
 * errdetail_params
 *
 * Add an errdetail() line showing bind-parameter data, if available.
 */
static int
errdetail_params(ParamListInfo params)
{
	/* We mustn't call user-defined I/O functions when in an aborted xact */
	if (params && params->numParams > 0 && !IsAbortedTransactionBlockState())
	{
		StringInfoData param_str;
		MemoryContext oldcontext;
		int			paramno;

		/* This code doesn't support dynamic param lists */
		Assert(params->paramFetch == NULL);

		/* Make sure any trash is generated in MessageContext */
		oldcontext = MemoryContextSwitchTo(MessageContext);

		initStringInfo(&param_str);

		for (paramno = 0; paramno < params->numParams; paramno++)
		{
			ParamExternData *prm = &params->params[paramno];
			Oid			typoutput;
			bool		typisvarlena;
			char	   *pstring;
			char	   *p;

			appendStringInfo(&param_str, "%s$%d = ",
							 paramno > 0 ? ", " : "",
							 paramno + 1);

			if (prm->isnull || !OidIsValid(prm->ptype))
			{
				appendStringInfoString(&param_str, "NULL");
				continue;
			}

			getTypeOutputInfo(prm->ptype, &typoutput, &typisvarlena);

			pstring = OidOutputFunctionCall(typoutput, prm->value);

			appendStringInfoCharMacro(&param_str, '\'');
			for (p = pstring; *p; p++)
			{
				if (*p == '\'') /* double single quotes */
					appendStringInfoCharMacro(&param_str, *p);
				appendStringInfoCharMacro(&param_str, *p);
			}
			appendStringInfoCharMacro(&param_str, '\'');

			pfree(pstring);
		}

		errdetail("parameters: %s", param_str.data);

		pfree(param_str.data);

		MemoryContextSwitchTo(oldcontext);
	}

	return 0;
}

/*
 * errdetail_abort
 *
 * Add an errdetail() line showing abort reason, if any.
 */
static int
errdetail_abort(void)
{
	if (MyProc->recoveryConflictPending)
		errdetail("abort reason: recovery conflict");

	return 0;
}

/*
 * errdetail_recovery_conflict
 *
 * Add an errdetail() line showing conflict source.
 */
static int
errdetail_recovery_conflict(ProcSignalReason reason)
{
	switch (reason)
	{
		case PROCSIG_RECOVERY_CONFLICT_BUFFERPIN:
			errdetail("User was holding shared buffer pin for too long.");
			break;
		case PROCSIG_RECOVERY_CONFLICT_LOCK:
			errdetail("User was holding a relation lock for too long.");
			break;
		case PROCSIG_RECOVERY_CONFLICT_TABLESPACE:
			errdetail("User was or might have been using tablespace that must be dropped.");
			break;
		case PROCSIG_RECOVERY_CONFLICT_SNAPSHOT:
			errdetail("User query might have needed to see row versions that must be removed.");
			break;
		case PROCSIG_RECOVERY_CONFLICT_STARTUP_DEADLOCK:
			errdetail("User transaction caused buffer deadlock with recovery.");
			break;
		case PROCSIG_RECOVERY_CONFLICT_DATABASE:
			errdetail("User was connected to a database that must be dropped.");
			break;
		default:
			break;
			/* no errdetail */
	}

	return 0;
}

/*
 * exec_describe_statement_message
 *
 * Process a "Describe" message for a prepared statement
 */
static void
exec_describe_statement_message(const char *stmt_name)
{
	CachedPlanSource *psrc;
	int			i;

	/*
	 * Start up a transaction command. (Note that this will normally change
	 * current memory context.) Nothing happens if we are already in one.
	 */
	start_xact_command();

	/* Switch back to message context */
	MemoryContextSwitchTo(MessageContext);

	/* Find prepared statement */
	if (stmt_name[0] != '\0')
	{
		PreparedStatement *pstmt;

		pstmt = FetchPreparedStatement(stmt_name, true);
		psrc = pstmt->plansource;
	}
	else
	{
		/* special-case the unnamed statement */
		psrc = unnamed_stmt_psrc;
		if (!psrc)
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_PSTATEMENT),
					 errmsg("unnamed prepared statement does not exist")));
	}

	/* Prepared statements shouldn't have changeable result descs */
	Assert(psrc->fixed_result);

	/*
	 * If we are in aborted transaction state, we can't run
	 * SendRowDescriptionMessage(), because that needs catalog accesses.
	 * Hence, refuse to Describe statements that return data.  (We shouldn't
	 * just refuse all Describes, since that might break the ability of some
	 * clients to issue COMMIT or ROLLBACK commands, if they use code that
	 * blindly Describes whatever it does.)  We can Describe parameters
	 * without doing anything dangerous, so we don't restrict that.
	 */
	if (IsAbortedTransactionBlockState() &&
		psrc->resultDesc)
		ereport(ERROR,
				(errcode(ERRCODE_IN_FAILED_SQL_TRANSACTION),
				 errmsg("current transaction is aborted, "
						"commands ignored until end of transaction block"),
				 errdetail_abort()));

	if (whereToSendOutput != DestRemote)
		return;					/* can't actually do anything... */

	/*
	 * First describe the parameters...
	 */
	pq_beginmessage_reuse(&row_description_buf, 't'); /* parameter description
													   * message type */
	pq_sendint16(&row_description_buf, psrc->num_params);

	for (i = 0; i < psrc->num_params; i++)
	{
		Oid			ptype = psrc->param_types[i];

		pq_sendint32(&row_description_buf, (int) ptype);
	}
	pq_endmessage_reuse(&row_description_buf);

	/*
	 * Next send RowDescription or NoData to describe the result...
	 */
	if (psrc->resultDesc && !(isPGXCDataNode && IsConnFromCoord() && !g_need_tuple_descriptor))
	{
		List	   *tlist;

		/* Get the plan's primary targetlist */
		tlist = CachedPlanGetTargetList(psrc, NULL);

		SendRowDescriptionMessage(&row_description_buf,
								  psrc->resultDesc,
								  tlist,
								  NULL,
								  NULL);
	}
	else
		pq_putemptymessage('n');	/* NoData */

}

/*
 * exec_describe_portal_message
 *
 * Process a "Describe" message for a portal
 */
static void
exec_describe_portal_message(const char *portal_name)
{
	Portal		portal;

	/*
	 * Start up a transaction command. (Note that this will normally change
	 * current memory context.) Nothing happens if we are already in one.
	 */
	start_xact_command();

	/* Switch back to message context */
	MemoryContextSwitchTo(MessageContext);

	portal = GetPortalByName(portal_name);
	if (!PortalIsValid(portal))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_CURSOR),
				 errmsg("portal \"%s\" does not exist. ", portal_name)));

	/*
	 * If we are in aborted transaction state, we can't run
	 * SendRowDescriptionMessage(), because that needs catalog accesses.
	 * Hence, refuse to Describe portals that return data.  (We shouldn't just
	 * refuse all Describes, since that might break the ability of some
	 * clients to issue COMMIT or ROLLBACK commands, if they use code that
	 * blindly Describes whatever it does.)
	 */
	if (IsAbortedTransactionBlockState() &&
		portal->tupDesc)
		ereport(ERROR,
				(errcode(ERRCODE_IN_FAILED_SQL_TRANSACTION),
				 errmsg("current transaction is aborted, "
						"commands ignored until end of transaction block"),
				 errdetail_abort()));

	if (whereToSendOutput != DestRemote)
		return;					/* can't actually do anything... */

	if (portal->tupDesc && !(isPGXCDataNode && IsConnFromCoord() && !g_need_tuple_descriptor))
		SendRowDescriptionMessage(&row_description_buf,
								  portal->tupDesc,
								  FetchPortalTargetList(portal),
								  portal->formats,
								  NULL);
	else
		pq_putemptymessage('n');	/* NoData */
}


/*
 * Convenience routines for starting/committing a single command.
 */
void
start_xact_command(void)
{
	if (!xact_started)
	{
		StartTransactionCommand();

		/* Set statement timeout running, if any */
		/* NB: this mustn't be enabled until we are within an xact */
		if (StatementTimeout > 0 && !IsConnFromCoord())
			enable_timeout_after(STATEMENT_TIMEOUT, StatementTimeout);
		else
			disable_timeout(STATEMENT_TIMEOUT, false);

		xact_started = true;
	}
}

void
finish_xact_command(void)
{
	if (xact_started)
	{
		if(enable_distri_print)
		{
			elog(LOG, "finish xact");
		}
		/* Cancel any active statement timeout before committing */
		disable_timeout(STATEMENT_TIMEOUT, false);

		CommitTransactionCommand();
		g_in_rollback_to = false;
		g_in_release_savepoint = false;
#ifdef MEMORY_CONTEXT_CHECKING
		/* Check all memory contexts that weren't freed during commit */
		/* (those that were, were checked before being deleted) */
		MemoryContextCheck(TopMemoryContext);
#endif

#ifdef SHOW_MEMORY_STATS
		/* Print mem stats after each commit for leak tracking */
		MemoryContextStats(TopMemoryContext);
#endif

		xact_started = false;

	}
}


/*
 * Convenience routines for checking whether a statement is one of the
 * ones that we allow in transaction-aborted state.
 */

/* Test a bare parsetree */
static bool
IsTransactionExitStmt(Node *parsetree)
{
	if (parsetree && IsA(parsetree, TransactionStmt))
	{
		TransactionStmt *stmt = (TransactionStmt *) parsetree;

		if (stmt->kind == TRANS_STMT_COMMIT ||
			stmt->kind == TRANS_STMT_PREPARE ||
			stmt->kind == TRANS_STMT_ROLLBACK ||
			stmt->kind == TRANS_STMT_ROLLBACK_TO ||
#ifdef __OPENTENBASE__
			stmt->kind == TRANS_STMT_ROLLBACK_SUBTXN
#endif
			)
			return true;
	}
	return false;
}

/* Test a list that contains PlannedStmt nodes */
static bool
IsTransactionExitStmtList(List *pstmts)
{
	if (list_length(pstmts) == 1)
	{
		PlannedStmt *pstmt = linitial_node(PlannedStmt, pstmts);

		if (pstmt->commandType == CMD_UTILITY &&
			IsTransactionExitStmt(pstmt->utilityStmt))
			return true;
	}
	return false;
}

/* Test a list that contains PlannedStmt nodes */
static bool
IsTransactionStmtList(List *pstmts)
{
	if (list_length(pstmts) == 1)
	{
		PlannedStmt *pstmt = linitial_node(PlannedStmt, pstmts);

		if (pstmt->commandType == CMD_UTILITY &&
			IsA(pstmt->utilityStmt, TransactionStmt))
			return true;
	}
	return false;
}


/* Release any existing unnamed prepared statement */
static void
drop_unnamed_stmt(void)
{
	/* paranoia to avoid a dangling pointer in case of error */
	if (unnamed_stmt_psrc)
	{
		CachedPlanSource *psrc = unnamed_stmt_psrc;

		unnamed_stmt_psrc = NULL;
		DropCachedPlan(psrc);
	}
}


/* --------------------------------
 *		signal handler routines used in PostgresMain()
 * --------------------------------
 */

/*
 * quickdie() occurs when signalled SIGQUIT by the postmaster.
 *
 * Some backend has bought the farm,
 * so we need to stop what we're doing and exit.
 */

void
quickdie(SIGNAL_ARGS)
{
	if (MyLocalThreadId != MyMainThreadId && MyMainThreadId != 0)
	{
		pthread_kill(MyMainThreadId, SIGQUIT);
		return;
	}
	quickdie_internal(postgres_signal_arg);
}

void
quickdie_internal(SIGNAL_ARGS)
{
	sigaddset(&BlockSig, SIGQUIT);	/* prevent nested calls */
	PG_SETMASK(&BlockSig);

	/*
	 * Prevent interrupts while exiting; though we just blocked signals that
	 * would queue new interrupts, one may have been pending.  We don't want a
	 * quickdie() downgraded to a mere query cancel.
	 */
	HOLD_INTERRUPTS();

	/*
	 * If we're aborting out of client auth, don't risk trying to send
	 * anything to the client; we will likely violate the protocol, not to
	 * mention that we may have interrupted the guts of OpenSSL or some
	 * authentication library.
	 */
	if (ClientAuthInProgress && whereToSendOutput == DestRemote)
		whereToSendOutput = DestNone;

	/*
	 * Ideally this should be ereport(FATAL), but then we'd not get control
	 * back...
	 */
	ereport(WARNING,
			(errcode(ERRCODE_CRASH_SHUTDOWN),
			 errmsg("terminating connection because of crash of another server process"),
			 errdetail("The postmaster has commanded this server process to roll back"
					   " the current transaction and exit, because another"
					   " server process exited abnormally and possibly corrupted"
					   " shared memory."),
			 errhint("In a moment you should be able to reconnect to the"
					 " database and repeat your command.")));

#ifdef __RESOURCE_QUEUE__
	if ((IS_PGXC_COORDINATOR || IS_PGXC_DATANODE) &&
		IsUnderPostmaster && IsNormalPostgres)
	{
		ResQUsageRemoveLocalResourceInfo();
	}
#endif

	/*
	 * We DO NOT want to run proc_exit() callbacks -- we're here because
	 * shared memory may be corrupted, so we don't want to try to clean up our
	 * transaction.  Just nail the windows shut and get out of town.  Now that
	 * there's an atexit callback to prevent third-party code from breaking
	 * things by calling exit() directly, we have to reset the callbacks
	 * explicitly to make this work as intended.
	 */
	on_exit_reset();

	/*
	 * Note we do exit(2) not exit(0).  This is to force the postmaster into a
	 * system reset cycle if some idiot DBA sends a manual SIGQUIT to a random
	 * backend.  This is necessary precisely because we don't clean up our
	 * shared memory state.  (The "dead man switch" mechanism in pmsignal.c
	 * should ensure the postmaster sees this as a crash, too, but no harm in
	 * being doubly sure.)
	 */
	exit(2);
}

/*
 * Shutdown signal from postmaster: abort transaction and exit
 * at soonest convenient time
 */
void
die(SIGNAL_ARGS)
{
	int			save_errno = errno;

	/* Don't joggle the elbow of proc_exit */
	if (!proc_exit_inprogress)
	{
		InterruptPending = true;
		ProcDiePending = true;
	}

#ifdef XCP
	/* release cluster lock if holding it */
	if (cluster_ex_lock_held)
	{
		ReleaseClusterLock(true);
	}
#endif

	/* If we're still here, waken anything waiting on the process latch */
	SetLatch(MyLatch);

#ifdef __RESOURCE_QUEUE__
	if ((IS_PGXC_COORDINATOR || IS_PGXC_DATANODE) && 
		IsUnderPostmaster && IsNormalPostgres)
	{
		ResQUsageRemoveLocalResourceInfo();
	}
#endif

	/*
	 * If we're in single user mode, we want to quit immediately - we can't
	 * rely on latches as they wouldn't work when stdin/stdout is a file.
	 * Rather ugly, but it's unlikely to be worthwhile to invest much more
	 * effort just for the benefit of single user mode.
	 */
	if (DoingCommandRead && whereToSendOutput != DestRemote)
		ProcessInterrupts();

	errno = save_errno;
}


/*
 * Query-cancel signal from postmaster: abort current transaction
 * at soonest convenient time
 */
void
StatementCancelHandler(SIGNAL_ARGS)
{
	int			save_errno = errno;

	/*
	 * Don't joggle the elbow of proc_exit
	 */
	if (!proc_exit_inprogress)
	{
		InterruptPending = true;
		QueryCancelPending = true;
	}

	/* If we're still here, waken anything waiting on the process latch */
	SetLatch(MyLatch);

	errno = save_errno;
}

/* signal handler for floating point exception */
void
FloatExceptionHandler(SIGNAL_ARGS)
{
	/* We're not returning, so no need to save errno */
	ereport(ERROR,
			(errcode(ERRCODE_FLOATING_POINT_EXCEPTION),
			 errmsg("floating-point exception"),
			 errdetail("An invalid floating-point operation was signaled. "
					   "This probably means an out-of-range result or an "
					   "invalid operation, such as division by zero.")));
}

/*
 * SIGHUP: set flag to re-read config file at next convenient time.
 *
 * Sets the ConfigReloadPending flag, which should be checked at convenient
 * places inside main loops. (Better than doing the reading in the signal
 * handler, ey?)
 */
void
PostgresSigHupHandler(SIGNAL_ARGS)
{
	int			save_errno = errno;

	ConfigReloadPending = true;
	SetLatch(MyLatch);

	errno = save_errno;
}

/*
 * Tell the next CHECK_FOR_INTERRUPTS() to check for a particular type of
 * recovery conflict.  Runs in a SIGUSR1 handler.
 */
void
HandleRecoveryConflictInterrupt(ProcSignalReason reason)
{
	RecoveryConflictPendingReasons[reason] = true;
	RecoveryConflictPending = true;
	InterruptPending = true;
	/* latch will be set by procsignal_sigusr1_handler */
}

/*
 * Check one individual conflict reason.
 */
static void
ProcessRecoveryConflictInterrupt(ProcSignalReason reason)
{
	switch (reason)
 	{
		case PROCSIG_RECOVERY_CONFLICT_STARTUP_DEADLOCK:

			/*
			 * If we aren't waiting for a lock we can never deadlock.
			 */
			if (!IsWaitingForLock())
				return;

			/* Intentional fall through to check wait for pin */
			/* FALLTHROUGH */

		case PROCSIG_RECOVERY_CONFLICT_BUFFERPIN:

			/*
			 * If PROCSIG_RECOVERY_CONFLICT_BUFFERPIN is requested but we
			 * aren't blocking the Startup process there is nothing more to
			 * do.
			 *
			 * When PROCSIG_RECOVERY_CONFLICT_STARTUP_DEADLOCK is requested,
			 * if we're waiting for locks and the startup process is not
			 * waiting for buffer pin (i.e., also waiting for locks), we set
			 * the flag so that ProcSleep() will check for deadlocks.
			 */
			if (!HoldingBufferPinThatDelaysRecovery())
			{
				if (reason == PROCSIG_RECOVERY_CONFLICT_STARTUP_DEADLOCK &&
					GetStartupBufferPinWaitBufId() < 0)
					CheckDeadLockAlert();
				return;
			}

			MyProc->recoveryConflictPending = true;

			/* Intentional fall through to error handling */
			/* FALLTHROUGH */

		case PROCSIG_RECOVERY_CONFLICT_LOCK:
		case PROCSIG_RECOVERY_CONFLICT_TABLESPACE:
		case PROCSIG_RECOVERY_CONFLICT_SNAPSHOT:

			/*
			 * If we aren't in a transaction any longer then ignore.
			 */
			if (!IsTransactionOrTransactionBlock())
				return;

			/*
			 * If we're not in a subtransaction then we are OK to throw an
			 * ERROR to resolve the conflict.  Otherwise drop through to the
			 * FATAL case.
			 *
			 * XXX other times that we can throw just an ERROR *may* be
			 * PROCSIG_RECOVERY_CONFLICT_LOCK if no locks are held in parent
			 * transactions
			 *
			 * PROCSIG_RECOVERY_CONFLICT_SNAPSHOT if no snapshots are held by
			 * parent transactions and the transaction is not
			 * transaction-snapshot mode
			 *
			 * PROCSIG_RECOVERY_CONFLICT_TABLESPACE if no temp files or
			 * cursors open in parent transactions
			 */
			if (!IsSubTransaction())
			{
 				/*
				 * If we already aborted then we no longer need to cancel.  We
				 * do this here since we do not wish to ignore aborted
				 * subtransactions, which must cause FATAL, currently.
 				 */
				if (IsAbortedTransactionBlockState())
 					return;

 				/*
				 * If a recovery conflict happens while we are waiting for
				 * input from the client, the client is presumably just
				 * sitting idle in a transaction, preventing recovery from
				 * making progress.  We'll drop through to the FATAL case
				 * below to dislodge it, in that case.
 				 */
				if (!DoingCommandRead)
 				{
					/* Avoid losing sync in the FE/BE protocol. */
					if (QueryCancelHoldoffCount != 0)
					{
						/*
						 * Re-arm and defer this interrupt until later.  See
						 * similar code in ProcessInterrupts().
						 */
						RecoveryConflictPendingReasons[reason] = true;
						RecoveryConflictPending = true;
						InterruptPending = true;
 						return;
					}

					/*
					 * We are cleared to throw an ERROR.  Either it's the
					 * logical slot case, or we have a top-level transaction
					 * that we can abort and a conflict that isn't inherently
					 * non-retryable.
					 */
					LockErrorCleanup();
					pgstat_report_recovery_conflict(reason);
					ereport(ERROR,
							(errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),
							 errmsg("canceling statement due to conflict with recovery"),
							 errdetail_recovery_conflict(reason)));
 					break;
 				}
			}
			/* Intentional fall through to session cancel */
			/* FALLTHROUGH */
		case PROCSIG_RECOVERY_CONFLICT_DATABASE:

			/*
			 * Retrying is not possible because the database is dropped, or we
			 * decided above that we couldn't resolve the conflict with an
			 * ERROR and fell through.  Terminate the session.
			 */
			pgstat_report_recovery_conflict(reason);
			ereport(FATAL,
					(errcode(reason == PROCSIG_RECOVERY_CONFLICT_DATABASE ?
							 ERRCODE_DATABASE_DROPPED :
							 ERRCODE_T_R_SERIALIZATION_FAILURE),
					 errmsg("terminating connection due to conflict with recovery"),
					 errdetail_recovery_conflict(reason),
					 errhint("In a moment you should be able to reconnect to the"
							 " database and repeat your command.")));
			break;

		default:
			elog(FATAL, "unrecognized conflict mode: %d", (int) reason);
 	}
}

/*
 * Check each possible recovery conflict reason.
 */
static void
ProcessRecoveryConflictInterrupts(void)
{
	ProcSignalReason reason;
	/*
	 * We don't need to worry about joggling the elbow of proc_exit, because
	 * proc_exit_prepare() holds interrupts, so ProcessInterrupts() won't call
	 * us.
	 */
	Assert(!proc_exit_inprogress);
	Assert(InterruptHoldoffCount == 0);
	Assert(RecoveryConflictPending);

	RecoveryConflictPending = false;

	for (reason = PROCSIG_RECOVERY_CONFLICT_FIRST;
		 reason <= PROCSIG_RECOVERY_CONFLICT_LAST;
		 reason++)
	{
		if (RecoveryConflictPendingReasons[reason])
		{
			RecoveryConflictPendingReasons[reason] = false;
			ProcessRecoveryConflictInterrupt(reason);
		}
	}
}

/*
 * ProcessInterrupts: out-of-line portion of CHECK_FOR_INTERRUPTS() macro
 *
 * If an interrupt condition is pending, and it's safe to service it,
 * then clear the flag and accept the interrupt.  Called only when
 * InterruptPending is true.
 */
void
ProcessInterrupts(void)
{
	/* Do not process interrupts in sub thread */
	if (am_sub_thread)
		return;
	/* OK to accept any interrupts now? */
	if (InterruptHoldoffCount != 0 || CritSectionCount != 0)
		return;
	InterruptPending = false;

	if (SubThreadErrorPending)
	{
		SubThreadErrorPending = false;
		LockErrorCleanup();
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
					errmsg("sub thread error")));
	}

	if (ProcDiePending)
	{
		ProcDiePending = false;
		QueryCancelPending = false; /* ProcDie trumps QueryCancel */
		LockErrorCleanup();
		/* As in quickdie, don't risk sending to client during auth */
		if (ClientAuthInProgress && whereToSendOutput == DestRemote)
			whereToSendOutput = DestNone;
		if (ClientAuthInProgress)
			ereport(FATAL,
					(errcode(ERRCODE_QUERY_CANCELED),
					 errmsg("canceling authentication due to timeout")));
		else if (IsAutoVacuumWorkerProcess())
			ereport(FATAL,
					(errcode(ERRCODE_ADMIN_SHUTDOWN),
					 errmsg("terminating autovacuum process due to administrator command")));
		else if (IsLogicalWorker())
			ereport(FATAL,
					(errcode(ERRCODE_ADMIN_SHUTDOWN),
					 errmsg("terminating logical replication worker due to administrator command")));
		else if (IsLogicalLauncher())
		{
			ereport(DEBUG1,
					(errmsg("logical replication launcher shutting down")));

			/*
			 * The logical replication launcher can be stopped at any time.
			 * Use exit status 1 so the background worker is restarted.
			 */
			proc_exit(1);
		}
		else
        {
            if (HasCancelMessage())
            {
                char   *buffer = palloc0(MAX_CANCEL_MSG);

                GetCancelMessage(&buffer, MAX_CANCEL_MSG, true);
                ereport(FATAL,
                        (errcode(ERRCODE_ADMIN_SHUTDOWN),
                                errmsg("terminating connection due to administrator command: \"%s\"", buffer)));
            }
            else
				ereport(FATAL,
						(errcode(ERRCODE_ADMIN_SHUTDOWN),
					 errmsg("terminating connection due to administrator command")));
        }
	}
	if (ClientConnectionLost)
	{
		QueryCancelPending = false; /* lost connection trumps QueryCancel */
		LockErrorCleanup();
		/* don't send to client, we already know the connection to be dead. */
		whereToSendOutput = DestNone;
		ereport(FATAL,
				(errcode(ERRCODE_CONNECTION_FAILURE),
				 errmsg("connection to client lost")));
	}

	if (QueryCancelPending)
	{
		bool		lock_timeout_occurred;
		bool		stmt_timeout_occurred;
		bool		row_lock_timeout_occurred;

		/*
		 * Don't allow query cancel interrupts while reading input from the
		 * client, because we might lose sync in the FE/BE protocol.  (Die
		 * interrupts are OK, because we won't read any further messages from
		 * the client in that case.)
		 */
		if (QueryCancelHoldoffCount != 0)
		{
			/*
			 * Re-arm InterruptPending so that we process the cancel request
			 * as soon as we're done reading the message.
			 */
			InterruptPending = true;
			return;
		}

		QueryCancelPending = false;

		/*
		 * If LOCK_TIMEOUT and STATEMENT_TIMEOUT indicators are both set, we
		 * need to clear both, so always fetch both.
		 */
		lock_timeout_occurred = get_timeout_indicator(LOCK_TIMEOUT, true);
		stmt_timeout_occurred = get_timeout_indicator(STATEMENT_TIMEOUT, true);

		/*
		 * If both were set, we want to report whichever timeout completed
		 * earlier; this ensures consistent behavior if the machine is slow
		 * enough that the second timeout triggers before we get here.  A tie
		 * is arbitrarily broken in favor of reporting a lock timeout.
		 */
		if (lock_timeout_occurred && stmt_timeout_occurred &&
			get_timeout_finish_time(STATEMENT_TIMEOUT) < get_timeout_finish_time(LOCK_TIMEOUT))
			lock_timeout_occurred = false;	/* report stmt timeout */

		/*
		 * add RowLockTimerIdx, if all three indicators are set, we need to clear
		 * them all, so fetch all
		 */
		row_lock_timeout_occurred = get_timeout_indicator(RowLockTimerIdx, true);

		/*
		 * if all indicators were set, we want to report whichever timeout completed earlier;
		 * this ensures consistent behavior if the machine is slow
		 * enough that the second timeout triggers before we get here.  A tie
		 * is arbitrarily broken in favor of reporting a lock timeout.
		 */
		if (lock_timeout_occurred && row_lock_timeout_occurred &&
			get_timeout_finish_time(LOCK_TIMEOUT) < get_timeout_finish_time(RowLockTimerIdx))
			row_lock_timeout_occurred = false;     /* report lock timeout */
		if (stmt_timeout_occurred && row_lock_timeout_occurred &&
			get_timeout_finish_time(STATEMENT_TIMEOUT) < get_timeout_finish_time(RowLockTimerIdx))
			row_lock_timeout_occurred = false;    /* report stmt timeout */

		disable_timeout(RowLockTimerIdx, false);
		RowLockTimeout = 0;
		if (row_lock_timeout_occurred)
		{
			LockErrorCleanup();
			ereport(ERROR,
					(errcode(ERRCODE_LOCK_NOT_AVAILABLE),
					 errmsg("canceling statement due to row lock timeout")));
		}

		if (lock_timeout_occurred)
		{
			LockErrorCleanup();
			ereport(ERROR,
					(errcode(ERRCODE_LOCK_NOT_AVAILABLE),
					 errmsg("canceling statement due to lock timeout")));
		}
		if (stmt_timeout_occurred)
		{
			LockErrorCleanup();
			if (ActivePortal)
			{
				ExplainQueryFast(ActivePortal->queryDesc, true);
			}
			ereport(ERROR,
					(errcode(ERRCODE_QUERY_CANCELED),
					errmsg("canceling statement due to statement timeout")));
		}
		if (IsAutoVacuumWorkerProcess())
		{
			LockErrorCleanup();
			ereport(ERROR,
					(errcode(ERRCODE_QUERY_CANCELED),
					 errmsg("canceling autovacuum task")));
		}

		/*
		 * If we are reading a command from the client, just ignore the cancel
		 * request --- sending an extra error message won't accomplish
		 * anything.  Otherwise, go ahead and throw the error.
		 */
		if (!DoingCommandRead)
		{
			LockErrorCleanup();
            if (HasCancelMessage())
            {
                char   *buffer = palloc0(MAX_CANCEL_MSG);

                GetCancelMessage(&buffer, MAX_CANCEL_MSG, true);

                ereport(ERROR,
                        (errcode(ERRCODE_QUERY_CANCELED),
								errmsg("canceling statement due to user request: \"%s\"",
									   buffer)));
            }
            else
                ereport(ERROR,
					(errcode(ERRCODE_QUERY_CANCELED),
					 errmsg("canceling statement due to user request")));
		}
	}

	if (RecoveryConflictPending)
		ProcessRecoveryConflictInterrupts();

	if (IdleInTransactionSessionTimeoutPending)
	{
		/* Has the timeout setting changed since last we looked? */
		if (IdleInTransactionSessionTimeout > 0)
			ereport(FATAL,
					(errcode(ERRCODE_IDLE_IN_TRANSACTION_SESSION_TIMEOUT),
					 errmsg("terminating connection due to idle-in-transaction timeout")));
		else
			IdleInTransactionSessionTimeoutPending = false;

	}

	if (IdleSessionTimeoutPending)
	{
		/* Has the timeout setting changed since last we looked? */
		if (IdleSessionTimeout > 0)
			ereport(FATAL,
					(errcode(ERRCODE_IDLE_SESSION_TIMEOUT),
							errmsg("terminating connection due to idle-in-session timeout")));
		else
			IdleSessionTimeoutPending = false;

	}

	if (PersistentConnectionsTimeoutPending)
	{
		PersistentConnectionsTimeoutPending = false;
		if (!temp_object_included)
			pgxc_node_cleanup_and_release_handles(false, false);
	}

	if (ParallelMessagePending)
		HandleParallelMessages();

	if (PoolerMessagesPending())
		HandlePoolerMessages();

	CheckAndHandleCustomSignals();

	if (LogMemoryContextPending)
		ProcessLogMemoryContextInterrupt();

	if (GetMemoryDetailPending)
		ProcessGetMemoryDetailInterrupt();
	if (GetMemoryContextDetailPending)
		ProcessGetMemoryContextDetailInterrupt();

}


bool
IsQueryCancelPending(void)
{
	if (QueryCancelPending && InterruptPending &&
		!ProcDiePending && !ClientConnectionLost &&
		!(RecoveryConflictPending && DoingCommandRead))
	{
		return true;
	}
	return false;
}


/*
 * IA64-specific code to fetch the AR.BSP register for stack depth checks.
 *
 * We currently support gcc, icc, and HP-UX's native compiler here.
 *
 * Note: while icc accepts gcc asm blocks on x86[_64], this is not true on
 * ia64 (at least not in icc versions before 12.x).  So we have to carry a
 * separate implementation for it.
 */
#if defined(__ia64__) || defined(__ia64)

#if defined(__hpux) && !defined(__GNUC__) && !defined(__INTEL_COMPILER)
/* Assume it's HP-UX native compiler */
#include <ia64/sys/inline.h>
#define ia64_get_bsp() ((char *) (_Asm_mov_from_ar(_AREG_BSP, _NO_FENCE)))
#elif defined(__INTEL_COMPILER)
/* icc */
#include <asm/ia64regs.h>
#define ia64_get_bsp() ((char *) __getReg(_IA64_REG_AR_BSP))
#else
/* gcc */
static __inline__ char *
ia64_get_bsp(void)
{
	char	   *ret;

	/* the ;; is a "stop", seems to be required before fetching BSP */
	__asm__ __volatile__(
						 ";;\n"
						 "	mov	%0=ar.bsp	\n"
:						 "=r"(ret));

	return ret;
}
#endif
#endif							/* IA64 */


/*
 * set_stack_base: set up reference point for stack depth checking
 *
 * Returns the old reference point, if any.
 */
pg_stack_base_t
set_stack_base(void)
{
	char		stack_base;
	pg_stack_base_t old;

#if defined(__ia64__) || defined(__ia64)
	old.stack_base_ptr = stack_base_ptr;
	old.register_stack_base_ptr = register_stack_base_ptr;
#else
	old = stack_base_ptr;
#endif

	/* Set up reference point for stack depth checking */
	stack_base_ptr = &stack_base;
#if defined(__ia64__) || defined(__ia64)
	register_stack_base_ptr = ia64_get_bsp();
#endif

	return old;
}

/*
 * restore_stack_base: restore reference point for stack depth checking
 *
 * This can be used after set_stack_base() to restore the old value. This
 * is currently only used in PL/Java. When PL/Java calls a backend function
 * from different thread, the thread's stack is at a different location than
 * the main thread's stack, so it sets the base pointer before the call, and
 * restores it afterwards.
 */
void
restore_stack_base(pg_stack_base_t base)
{
#if defined(__ia64__) || defined(__ia64)
	stack_base_ptr = base.stack_base_ptr;
	register_stack_base_ptr = base.register_stack_base_ptr;
#else
	stack_base_ptr = base;
#endif
}

/*
 * check_stack_depth/stack_is_too_deep: check for excessively deep recursion
 *
 * This should be called someplace in any recursive routine that might possibly
 * recurse deep enough to overflow the stack.  Most Unixen treat stack
 * overflow as an unrecoverable SIGSEGV, so we want to error out ourselves
 * before hitting the hardware limit.
 *
 * check_stack_depth() just throws an error summarily.  stack_is_too_deep()
 * can be used by code that wants to handle the error condition itself.
 */
void
check_stack_depth(void)
{
	if (stack_is_too_deep())
	{
		ereport(ERROR,
				(errcode(ERRCODE_STATEMENT_TOO_COMPLEX),
				 errmsg("stack depth limit exceeded"),
				 errhint("Increase the configuration parameter \"max_stack_depth\" (currently %dkB), "
						 "after ensuring the platform's stack depth limit is adequate.",
						 max_stack_depth)));
	}
}

bool
stack_is_too_deep(void)
{
	char		stack_top_loc;
	long		stack_depth;

	/*
	 * Compute distance from reference point to my local variables
	 */
	stack_depth = (long) (stack_base_ptr - &stack_top_loc);

	/*
	 * Take abs value, since stacks grow up on some machines, down on others
	 */
	if (stack_depth < 0)
		stack_depth = -stack_depth;

	/*
	 * Trouble?
	 *
	 * The test on stack_base_ptr prevents us from erroring out if called
	 * during process setup or in a non-backend process.  Logically it should
	 * be done first, but putting it here avoids wasting cycles during normal
	 * cases.
	 */
	if (stack_depth > max_stack_depth_bytes &&
		stack_base_ptr != NULL)
		return true;

	/*
	 * On IA64 there is a separate "register" stack that requires its own
	 * independent check.  For this, we have to measure the change in the
	 * "BSP" pointer from PostgresMain to here.  Logic is just as above,
	 * except that we know IA64's register stack grows up.
	 *
	 * Note we assume that the same max_stack_depth applies to both stacks.
	 */
#if defined(__ia64__) || defined(__ia64)
	stack_depth = (long) (ia64_get_bsp() - register_stack_base_ptr);

	if (stack_depth > max_stack_depth_bytes &&
		register_stack_base_ptr != NULL)
		return true;
#endif							/* IA64 */

	return false;
}

/* GUC check hook for max_stack_depth */
bool
check_max_stack_depth(int *newval, void **extra, GucSource source)
{
	long		newval_bytes = *newval * 1024L;
	long		stack_rlimit = get_stack_depth_rlimit();

	if (stack_rlimit > 0 && newval_bytes > stack_rlimit - STACK_DEPTH_SLOP)
	{
		GUC_check_errdetail("\"max_stack_depth\" must not exceed %ldkB.",
							(stack_rlimit - STACK_DEPTH_SLOP) / 1024L);
		GUC_check_errhint("Increase the platform's stack depth limit via \"ulimit -s\" or local equivalent.");
		return false;
	}
	return true;
}

/* GUC assign hook for max_stack_depth */
void
assign_max_stack_depth(int newval, void *extra)
{
	long		newval_bytes = newval * 1024L;

	max_stack_depth_bytes = newval_bytes;
}

/*
 * GUC check_hook for restrict_nonsystem_relation_kind
 */
bool
check_restrict_nonsystem_relation_kind(char **newval, void **extra, GucSource source)
{
	char	   *rawstring;
	List	   *elemlist;
	ListCell   *l;
	int			flags = 0;

	/* Need a modifiable copy of string */
	rawstring = pstrdup(*newval);

	if (!SplitIdentifierString(rawstring, ',', &elemlist))
	{
		/* syntax error in list */
		GUC_check_errdetail("List syntax is invalid.");
		pfree(rawstring);
		list_free(elemlist);
		return false;
	}

	foreach(l, elemlist)
	{
		char	   *tok = (char *) lfirst(l);

		if (pg_strcasecmp(tok, "view") == 0)
			flags |= RESTRICT_RELKIND_VIEW;
		else if (pg_strcasecmp(tok, GUC_FOREIGN_REL_KIND) == 0)
			flags |= RESTRICT_RELKIND_FOREIGN_TABLE;
		else
		{
			GUC_check_errdetail("Unrecognized key word: \"%s\".", tok);
			pfree(rawstring);
			list_free(elemlist);
			return false;
		}
	}

	pfree(rawstring);
	list_free(elemlist);

	/* Save the flags in *extra, for use by the assign function */
	*extra = malloc(sizeof(int));
	*((int *) *extra) = flags;

	return true;
}

/*
 * GUC assign_hook for restrict_nonsystem_relation_kind
 */
void
assign_restrict_nonsystem_relation_kind(const char *newval, void *extra)
{
	int		   *flags = (int *) extra;

	restrict_nonsystem_relation_kind = *flags;
}

/*
 * set_debug_options --- apply "-d N" command line option
 *
 * -d is not quite the same as setting log_min_messages because it enables
 * other output options.
 */
void
set_debug_options(int debug_flag, GucContext context, GucSource source)
{
	if (debug_flag > 0)
	{
		char		debugstr[64];

		sprintf(debugstr, "debug%d", debug_flag);
		SetConfigOption("log_min_messages", debugstr, context, source);
	}
	else
		SetConfigOption("log_min_messages", "notice", context, source);

	if (debug_flag >= 1 && context == PGC_POSTMASTER)
	{
		SetConfigOption("log_connections", "true", context, source);
		SetConfigOption("log_disconnections", "true", context, source);
	}
	if (debug_flag >= 2)
		SetConfigOption("log_statement", "all", context, source);
	if (debug_flag >= 3)
		SetConfigOption("debug_print_parse", "true", context, source);
	if (debug_flag >= 4)
		SetConfigOption("debug_print_plan", "true", context, source);
	if (debug_flag >= 5)
		SetConfigOption("debug_print_rewritten", "true", context, source);
}


bool
set_plan_disabling_options(const char *arg, GucContext context, GucSource source)
{
	const char *tmp = NULL;

	switch (arg[0])
	{
		case 's':				/* seqscan */
			tmp = "enable_seqscan";
			break;
		case 'i':				/* indexscan */
			tmp = "enable_indexscan";
			break;
		case 'o':				/* indexonlyscan */
			tmp = "enable_indexonlyscan";
			break;
		case 'b':				/* bitmapscan */
			tmp = "enable_bitmapscan";
			break;
		case 't':				/* tidscan */
			tmp = "enable_tidscan";
			break;
		case 'n':				/* nestloop */
			tmp = "enable_nestloop";
			break;
		case 'm':				/* mergejoin */
			tmp = "enable_mergejoin";
			break;
		case 'h':				/* hashjoin */
			tmp = "enable_hashjoin";
			break;
	}
	if (tmp)
	{
		SetConfigOption(tmp, "false", context, source);
		return true;
	}
	else
		return false;
}


const char *
get_stats_option_name(const char *arg)
{
	switch (arg[0])
	{
		case 'p':
			if (optarg[1] == 'a')	/* "parser" */
				return "log_parser_stats";
			else if (optarg[1] == 'l')	/* "planner" */
				return "log_planner_stats";
			break;

		case 'e':				/* "executor" */
			return "log_executor_stats";
			break;
	}

	return NULL;
}


/* ----------------------------------------------------------------
 * process_postgres_switches
 *	   Parse command line arguments for PostgresMain
 *
 * This is called twice, once for the "secure" options coming from the
 * postmaster or command line, and once for the "insecure" options coming
 * from the client's startup packet.  The latter have the same syntax but
 * may be restricted in what they can do.
 *
 * argv[0] is ignored in either case (it's assumed to be the program name).
 *
 * ctx is PGC_POSTMASTER for secure options, PGC_BACKEND for insecure options
 * coming from the client, or PGC_SU_BACKEND for insecure options coming from
 * a superuser client.
 *
 * If a database name is present in the command line arguments, it's
 * returned into *dbname (this is allowed only if *dbname is initially NULL).
 * ----------------------------------------------------------------
 */
void
process_postgres_switches(int argc, char *argv[], GucContext ctx,
						  const char **dbname)
{
	bool		secure = (ctx == PGC_POSTMASTER);
	int			errs = 0;
	GucSource	gucsource;
	int			flag;
#ifdef PGXC
	bool		singleuser = false;
#endif

	if (secure)
	{
		gucsource = PGC_S_ARGV; /* switches came from command line */

		/* Ignore the initial --single argument, if present */
		if (argc > 1 && strcmp(argv[1], "--single") == 0)
		{
			argv++;
			argc--;
#ifdef PGXC
			singleuser = true;
#endif
		}
	}
	else
	{
		gucsource = PGC_S_CLIENT;	/* switches came from client */
	}

#ifdef HAVE_INT_OPTERR

	/*
	 * Turn this off because it's either printed to stderr and not the log
	 * where we'd want it, or argv[0] is now "--single", which would make for
	 * a weird error message.  We print our own error message below.
	 */
	opterr = 0;
#endif

	/*
	 * Parse command-line options.  CAUTION: keep this in sync with
	 * postmaster/postmaster.c (the option sets should not conflict) and with
	 * the common help() function in main/main.c.
	 */
	while ((flag = getopt(argc, argv, "B:bc:C:D:d:EeFf:h:ijk:lN:nOo:Pp:r:S:sTt:v:W:-:")) != -1)
	{
		switch (flag)
		{
			case 'B':
				SetConfigOption("shared_buffers", optarg, ctx, gucsource);
				break;

			case 'b':
				/* Undocumented flag used for binary upgrades */
				if (secure)
					IsBinaryUpgrade = true;
				break;

			case 'C':
				/* ignored for consistency with the postmaster */
				break;

			case 'D':
				if (secure)
					userDoption = strdup(optarg);
				break;

			case 'd':
				set_debug_options(atoi(optarg), ctx, gucsource);
				break;

			case 'E':
				if (secure)
					EchoQuery = true;
				break;

			case 'e':
				SetConfigOption("datestyle", "euro", ctx, gucsource);
				break;

			case 'F':
				SetConfigOption("fsync", "false", ctx, gucsource);
				break;

			case 'f':
				if (!set_plan_disabling_options(optarg, ctx, gucsource))
					errs++;
				break;

			case 'h':
				SetConfigOption("listen_addresses", optarg, ctx, gucsource);
				break;

			case 'i':
				SetConfigOption("listen_addresses", "*", ctx, gucsource);
				break;

			case 'j':
				if (secure)
					UseSemiNewlineNewline = true;
				break;

			case 'k':
				SetConfigOption("unix_socket_directories", optarg, ctx, gucsource);
				break;

			case 'l':
				SetConfigOption("ssl", "true", ctx, gucsource);
				break;

			case 'N':
				SetConfigOption("max_connections", optarg, ctx, gucsource);
				break;

			case 'n':
				/* ignored for consistency with postmaster */
				break;

			case 'O':
				SetConfigOption("allow_system_table_mods", "true", ctx, gucsource);
				break;

			case 'o':
				errs++;
				break;

			case 'P':
				SetConfigOption("ignore_system_indexes", "true", ctx, gucsource);
				break;

			case 'p':
				SetConfigOption("port", optarg, ctx, gucsource);
				break;

			case 'r':
				/* send output (stdout and stderr) to the given file */
				if (secure)
					strlcpy(OutputFileName, optarg, MAXPGPATH);
				break;

			case 'S':
				SetConfigOption("work_mem", optarg, ctx, gucsource);
				break;

			case 's':
				SetConfigOption("log_statement_stats", "true", ctx, gucsource);
				break;

			case 'T':
				/* ignored for consistency with the postmaster */
				break;

			case 't':
				{
					const char *tmp = get_stats_option_name(optarg);

					if (tmp)
						SetConfigOption(tmp, "true", ctx, gucsource);
					else
						errs++;
					break;
				}

			case 'v':

				/*
				 * -v is no longer used in normal operation, since
				 * FrontendProtocol is already set before we get here. We keep
				 * the switch only for possible use in standalone operation,
				 * in case we ever support using normal FE/BE protocol with a
				 * standalone backend.
				 */
				if (secure)
					FrontendProtocol = (ProtocolVersion) atoi(optarg);
				break;

			case 'W':
				SetConfigOption("post_auth_delay", optarg, ctx, gucsource);
				break;

			case 'c':
			case '-':
				{
					char	   *name,
							   *value;

					ParseLongOption(optarg, &name, &value);
#ifdef PGXC
					/* A Coordinator is being activated */
					if (strcmp(name, "coordinator") == 0 &&
						!value)
						isPGXCCoordinator = true;
					/* A Datanode is being activated */
					else if (strcmp(name, "datanode") == 0 &&
							 !value)
						isPGXCDataNode = true;
					else if (strcmp(name, "localxid") == 0 &&
							 !value)
					{
						if (!singleuser)
							ereport(ERROR,
									(errcode(ERRCODE_SYNTAX_ERROR),
									 errmsg("local xids can be used only in single user mode")));
						useLocalXid = true;
					}
					else if (strcmp(name, "tempoid") == 0 &&
							 !value)
					{
						if (!singleuser)
							ereport(ERROR,
									(errcode(ERRCODE_SYNTAX_ERROR),
											errmsg("tempoid can be used only in single user mode")));
						tempOid = true;
					}
					else /* default case */
					{
#endif /* PGXC */
					if (!value)
					{
						if (flag == '-')
							ereport(ERROR,
									(errcode(ERRCODE_SYNTAX_ERROR),
									 errmsg("--%s requires a value",
											optarg)));
						else
							ereport(ERROR,
									(errcode(ERRCODE_SYNTAX_ERROR),
									 errmsg("-c %s requires a value",
											optarg)));
					}
					SetConfigOption(name, value, ctx, gucsource);
#ifdef PGXC
					}
#endif
					free(name);
					if (value)
						free(value);
					break;
				}

			default:
				errs++;
				break;
		}

		if (errs)
			break;
	}

#ifdef PGXC
	/*
	 * Make sure we specified the mode if Coordinator or Datanode.
	 * Allow for the exception of initdb by checking config option
	 */
#ifdef __OPENTENBASE_C__
	if (!IS_PGXC_COORDINATOR && !IS_PGXC_DATANODE && IsUnderPostmaster)
	{
		ereport(FATAL,
				(errcode(ERRCODE_SYNTAX_ERROR),
			 errmsg("Postgres-XL: must start as either a Coordinator (--coordinator) or Datanode (-datanode) or ForwardNode (-forward)\n")));

	}
#else
	if (!IS_PGXC_COORDINATOR && !IS_PGXC_DATANODE && IsUnderPostmaster)
	{
		ereport(FATAL,
				(errcode(ERRCODE_SYNTAX_ERROR),
			 errmsg("Postgres-XL: must start as either a Coordinator (--coordinator) or Datanode (-datanode)\n")));

	}
#endif
	if (!IsPostmasterEnvironment)
	{
		/* Treat it as a Datanode for initdb to work properly */
		isPGXCDataNode = true;
	}
#endif

	/*
	 * Optional database name should be there only if *dbname is NULL.
	 */
	if (!errs && dbname && *dbname == NULL && argc - optind >= 1)
		*dbname = strdup(argv[optind++]);

	if (errs || argc != optind)
	{
		if (errs)
			optind--;			/* complain about the previous argument */

		/* spell the error message a bit differently depending on context */
		if (IsUnderPostmaster)
			ereport(FATAL,
					(errcode(ERRCODE_SYNTAX_ERROR),
					 errmsg("invalid command-line argument for server process: %s", argv[optind]),
					 errhint("Try \"%s --help\" for more information.", progname)));
		else
			ereport(FATAL,
					(errcode(ERRCODE_SYNTAX_ERROR),
					 errmsg("%s: invalid command-line argument: %s",
							progname, argv[optind]),
					 errhint("Try \"%s --help\" for more information.", progname)));
	}

	/*
	 * Reset getopt(3) library so that it will work correctly in subprocesses
	 * or when this function is called a second time with another array.
	 */
	optind = 1;
#ifdef HAVE_INT_OPTRESET
	optreset = 1;				/* some systems need this too */
#endif
}

void
process_last_pos_slash(const char *query_string, int query_len)
{
	char *t_char = (char *)(query_string + query_len -1);

	while (t_char != query_string)
	{
		if (ch_is_space(*t_char))
		{
			t_char--;
		}
		else if (*t_char == '/' && *(t_char-1) == '\n')
		{
			/* Remove the last / in opentenbase_ora stored procedure */
			*t_char = ' ';
			break;
		}
		else
		{
			break;
		}
	}
}

static bool
ch_is_space(char ch)
{
	if (ch == ' ' || ch == '\n' || ch == '\t' || ch == '\r' || ch == '\f')
	{
		return true;
	}
	else
	{
		return false;
	}
}

/*
 * String comparisons ignore case and ignore extra spaces.
 */
static bool
sentence_compare_ignore_case_extrspace(const char *str, const char *base, int words)
{
#define KEYWORLD_MAX_LEN 1024
	char tmp[KEYWORLD_MAX_LEN + 1] = {0};
	const char *head = str;
	int base_len,
		idx = 0,
		cur_words = 0;
	bool new_word = true;

	if (str == NULL || base == NULL)
	{
		ereport(ERROR,
			(errcode(ERRCODE_INTERNAL_ERROR),
			 errmsg("sentence_compare_ignore_extr_space: Invalid params[%s:%s]",
				str ? " " : "str is null",
				base ? " " : "base is null")));
	}

	base_len = strlen(base);
	if (base_len > KEYWORLD_MAX_LEN)
	{
		ereport(ERROR,
			(errcode(ERRCODE_INTERNAL_ERROR),
			 errmsg("sentence_compare_ignore_extr_space: bases[%d] is too long, support max size[%d]",
				(int)strlen(base), KEYWORLD_MAX_LEN)));
	}

	/* step1: Ignore extra spaces.. */
	while (*head != '\0' && ch_is_space(*head))
	{
		head++;
	}

	/* step2: Remove excess space and turn to lowercase. */
	for (; *head != '\0'; head++)
	{
		if (ch_is_space(*head))
		{
			if (cur_words == words)
				break;

			new_word = true;
			tmp[idx++] = *head;

			while (*head != '\0' && ch_is_space(*head))
			{
				head++;
			}
		}

		if (*head == ';')
			break;

		if (idx > KEYWORLD_MAX_LEN)
		{
			return false;
		}

		if (new_word)
		{
			new_word = false;
			cur_words++;
		}
		tmp[idx++] = tolower(*head);
	}

	if (idx == base_len &&
		(strncmp(tmp, base, base_len) == 0) &&
		((ch_is_space(*head)) || *head == ';' || *head == '\0'))
	{
		return true;
	}
	else
	{
		return false;
	}
}

/*
 * Only the case where the comment is first is handled
 */
char *
preprocess_comment(const char *query_string, bool *is_find)
{
	const char *begin = "/*";
	const char *end = "*/";
	const int len = 2;
	char *pos = NULL;

	if (strncmp(query_string, begin, len) != 0)
	{
		*is_find = false;
		return (char *) query_string;
	}
	pos = strstr(query_string, end);
	if (pos == NULL)
	{
		*is_find = false;
		return (char *)query_string;
	}

	pos += len;
	while (pos && *pos != '\0')
	{
		if (ch_is_space(*pos))
			pos++;
		else
			break;
	}

	*is_find = true;
	return pos;
}

/*
 * check opentenbase_ora user function or procedure add dollar position
 * ret_as_pos is  0 : do not need to add; not 0 : need to add
 * dollar_or_decl is true : just need to add $$; false: need to add $$ and declare
 */
int
check_func_proc_add_dollar_pos(const char *query_string, int query_len, bool *dollar_or_decl)
{
	int ret_as_pos = 0;  /* 0: do not need to add */
	const char *tmp_char = query_string;
	int i = 0;
	bool func_go_on = false;
	bool as_go_on = false;

	*dollar_or_decl = false; /* need to add $$ declare */

	if (query_len > 6)
	{
		if ((tolower(tmp_char[i]) == 'c') && (tolower(tmp_char[i+1]) == 'r') && (tolower(tmp_char[i+2]) == 'e')
				&& (tolower(tmp_char[i+3]) == 'a') && (tolower(tmp_char[i+4]) == 't') && (tolower(tmp_char[i+5]) == 'e'))
		{
			i = i + 6;
			while (i < query_len)
			{
				if (ch_is_space(tmp_char[i]))
				{
					i++;
					continue;
				}

				if (!func_go_on && !as_go_on)
				{
					if ((i+4 < query_len) && tolower(tmp_char[i]) == 'c' && tolower(tmp_char[i+1]) == 'a' && tolower(tmp_char[i+2]) == 's' && tolower(tmp_char[i+3]) == 't')
					{
						 if(ch_is_space(tmp_char[i-1]) && ch_is_space(tmp_char[i+4]))
						 {
							 /* case: create cast */
							 ret_as_pos = 0;
							 break;
						 }
					}

					if ((i+8 < query_len) && tolower(tmp_char[i]) == 'f' && tolower(tmp_char[i+1]) == 'u' && tolower(tmp_char[i+2]) == 'n' && tolower(tmp_char[i+3]) == 'c' &&
							tolower(tmp_char[i+4]) == 't' && tolower(tmp_char[i+5]) == 'i' && tolower(tmp_char[i+6]) == 'o' && tolower(tmp_char[i+7]) == 'n')
					{
						if(ch_is_space(tmp_char[i-1]) && ch_is_space(tmp_char[i+8]))
						{
							i = i + 9;
							func_go_on=true;
							continue;
						}
					}
					else if ((i+9 < query_len) && tolower(tmp_char[i]) == 'p' && tolower(tmp_char[i+1]) == 'r' && tolower(tmp_char[i+2]) == 'o' && tolower(tmp_char[i+3]) == 'c' &&
							tolower(tmp_char[i+4]) == 'e' && tolower(tmp_char[i+5]) == 'd' && tolower(tmp_char[i+6]) == 'u' && tolower(tmp_char[i+7]) == 'r' && tolower(tmp_char[i+8]) == 'e')
					{
						if (ch_is_space(tmp_char[i-1]) && ch_is_space(tmp_char[i+9]))
						 {
							 i = i + 10;
							 func_go_on=true;
							 continue;
						}
					}
				}

				if (func_go_on && !as_go_on)
				{
					if ((i+2 < query_len) && (tolower(tmp_char[i]) == 'i' || tolower(tmp_char[i]) == 'a') && (tolower(tmp_char[i+1]) == 's'))
					{
						if (ch_is_space(tmp_char[i-1]) && ch_is_space(tmp_char[i+2]))
						{
							i = i + 3;
							ret_as_pos = i;
							as_go_on=true;
							continue;
						}
					}
				}

				if (func_go_on && as_go_on)
				{
					if (tmp_char[i] == '\'' || tmp_char[i] == '$')
					{
						ret_as_pos = 0;
					}
					else
					{
						if ((i+5 < query_len) && tolower(tmp_char[i]) == 'b' && tolower(tmp_char[i+1]) == 'e' && tolower(tmp_char[i+2]) == 'g' && tolower(tmp_char[i+3]) == 'i' && tolower(tmp_char[i+4]) == 'n')
						{
							if(ch_is_space(tmp_char[i-1]) && ch_is_space(tmp_char[i+5]))
							{
								/* just need to add $$ */
								*dollar_or_decl = true;
							}
						}
					}

					break;
				}

				i++;
			}
		}
	}

	return ret_as_pos;
}

static char*
preprocess_comment_both(char *query_string, bool *is_find)
{
	const char *begin_mul = "/*";
	const char *begin_sin = "--";
	const char *end = "*/";
	const int len = 2;
	char *pos = NULL;
	char *new_query_string = query_string;

	if (query_string == NULL)
	{
		*is_find = false;
		return NULL;
	}

	while (new_query_string && *new_query_string != '\0')
	{
		if (ch_is_space(*new_query_string))
			new_query_string++;
		else
			break;
	}

	if (strncmp(new_query_string, begin_sin, len) == 0)
	{
		pos = new_query_string + 2;
		while (pos && *pos != '\0' && *pos != '\n')
			pos ++;
		while (pos && *pos != '\0')
		{
			if (ch_is_space(*pos))
				pos++;
			else
				break;
		}
		*is_find = true;
		return pos;
	}

	if (strncmp(new_query_string, begin_mul, len) != 0)
	{
		*is_find = false;
		return (char *)query_string;
	}

	pos = strstr(new_query_string, end);
	if (pos == NULL)
	{
		*is_find = false;
		return (char *)query_string;
	}

	pos += len;
	while (pos && *pos != '\0')
	{
		if (ch_is_space(*pos))
			pos++;
		else
			break;
	}

	*is_find = true;
	return pos;
}


static char*
skip_label(char *query, bool *is_find)
{
	const char *begin = "<<";
	const char *end = ">>";
	const int len = 2;
	char *pos = NULL;
	char *new_query = query;

	if (query == NULL)
	{
		*is_find = false;
		return NULL;
	}

	while (new_query && *new_query != '\0')
	{
		if (ch_is_space(*new_query))
			new_query++;
		else
			break;
	}

	if (strncmp(new_query, begin, len) != 0)
	{
		*is_find = false;
		return (char *)query;
	}

	pos = strstr(new_query, end);
	if (pos == NULL)
	{
		*is_find = false;
		return (char *)query;
	}

	pos += len;
	while (pos && *pos != '\0')
	{
		if (ch_is_space(*pos))
			pos++;
		else
			break;
	}

	*is_find = true;
	return pos;
}

static char*
skip_comments(char *query_string, int query_len)
{
	bool is_find = false;
	char *new_query = query_string;
	int new_query_len  = query_len;

	while(new_query && new_query_len > 2)
	{
		is_find = false;
		new_query = preprocess_comment_both(new_query, &is_find);
		if (new_query && is_find)
			new_query_len = strlen(new_query);
		else
			break;
	}

	return new_query;
}
/*
 * check if opentenbase_ora anonymous code block need to add do $$
 * need_add_do_dollar is true : need to add do $$; false: do not need to add;
 */
bool
check_anonymous_block_add_dollar(char *query_string, int query_len)
{
	bool need_add_do_dollar = false;
	int i=0;
	const char *tmp_char = NULL;
	char sql_start_cmd[12] = "";
	int c1 = '\0';
	int c2 = '\0';
	const int one_word = 1;
	const int two_words = 2;
	bool is_find = false;
	char *new_query = query_string;
	int new_query_len = query_len;

	new_query = skip_comments(new_query, new_query_len);
	if (new_query)
		new_query_len = strlen(new_query);

	is_find = false;
	new_query = skip_label(new_query, &is_find);
	if (new_query && is_find)
		new_query_len = strlen(new_query);

	new_query = skip_comments(new_query, new_query_len);
	if (new_query)
		new_query_len = strlen(new_query);

	query_len = new_query_len;
	query_string = new_query;

	memset(sql_start_cmd, 0, 12);
	if (query_len >= 8)
	{
		strncpy(sql_start_cmd, query_string, 7);
		for (i=0; i<7; i++)
		{
			sql_start_cmd[i] = tolower(sql_start_cmd[i]);
		}

		if (strncmp(sql_start_cmd,"declare",7) == 0)
		{
			/* check sql start: declare */
			tmp_char = query_string + 7;
		}
		else if (strncmp(sql_start_cmd,"begin",5) == 0)
		{
			/* check sql start: begin */
			tmp_char = query_string + 5;
		}
	}
	else if (query_len >= 6)
	{
		strncpy(sql_start_cmd, query_string, 5);
		for (i=0; i<5; i++)
		{
			sql_start_cmd[i] = tolower(sql_start_cmd[i]);
		}

		if (strncmp(sql_start_cmd,"begin",5) == 0)
		{
			/* check sql start: begin */
			tmp_char = query_string + 5;
		}
	}

	if (tmp_char)
	{
		if (ch_is_space(*tmp_char) || (*tmp_char == ';'))
		{
			while (*tmp_char != '\0')
			{
				if (ch_is_space(*tmp_char))
				{
					tmp_char++;
				}
				else if (*tmp_char == ';')
				{
					/* process case : begin ; */
					break;
				}
				else if (strncmp(sql_start_cmd,"declare",7) == 0)
				{
					/* check: DECLARE _psql_cursor NO SCROLL CURSOR FOR select ... */

					while (*tmp_char != '\0')
					{
						if (tolower(*(tmp_char-4)) == 'b' && tolower(*(tmp_char-3)) == 'e' &&
								tolower(*(tmp_char-2)) == 'g' && tolower(*(tmp_char-1)) == 'i' && tolower(*tmp_char) == 'n')
						{
							c1 = *(tmp_char - 5);
							c2 = *(tmp_char + 1);
							if ((ch_is_space(c1) || (c1 == ';')) && (ch_is_space(c2) || (c2 == ';')))
							{
								/* have begin-end blocks */
								need_add_do_dollar = true;
								break;
							}
						}
						tmp_char++;
					}
					break;
				}
				else if (strncmp(sql_start_cmd,"begin",5) == 0)
				{
					int tmp_char_len = strlen(tmp_char);
					char firstchar = tolower(*tmp_char);

					/* check: BEGIN TRANSACTION ... ;*/
					if ((firstchar == 't' &&
						tmp_char_len >= 11 &&
						sentence_compare_ignore_case_extrspace(tmp_char, "transaction", one_word)) ||

						/* check: BEGIN WORK ... ; */
						(firstchar == 'w' &&
						tmp_char_len >= 4 &&
						sentence_compare_ignore_case_extrspace(tmp_char, "work", one_word)) ||
					
						/* check: BEGIN isolation level ... ; */
						(firstchar == 'i' &&
						tmp_char_len >= 9 &&
						sentence_compare_ignore_case_extrspace(tmp_char, "isolation", one_word)) ||

						/* check: BEGIN read/write only ... ; */
						(firstchar == 'r' &&
						tmp_char_len >= 9 &&
						(sentence_compare_ignore_case_extrspace(tmp_char, "read only", two_words) ||
						sentence_compare_ignore_case_extrspace(tmp_char, "read write", two_words))) ||

						/* check: BEGIN DEFERRABLE ... ; */
						(firstchar == 'd' &&
						tmp_char_len >= 10 &&
						sentence_compare_ignore_case_extrspace(tmp_char, "deferrable", one_word)) ||

						/* check: BEGIN DEFERRABLE ... ; */
						(firstchar == 'n' &&
						tmp_char_len >= 14 &&
						sentence_compare_ignore_case_extrspace(tmp_char, "not deferrable", two_words)))
					{
						break;
					}

					need_add_do_dollar = true;
					break;
				}
				else
				{
					need_add_do_dollar = true;
					break;
				}
			}
		}
	}

	return need_add_do_dollar;
}

void
HandleReadOnlyInterrupt(void)
{
	ReadOnlyProcessPending =  true;

	/*
	 * Set the process latch. This function essentially emulates signal
	 * handlers like die() and StatementCancelHandler() and it seems prudent
	 * to behave similarly as they do.
	 */
	SetLatch(MyLatch);
}

static void
FinishReadOnlyProcess(void)
{
	ReadOnlyProcessPending = false;
	if (g_twophase_state.is_readonly)
	{
		finish_xact_command();
		if (MyProc->write_proc != NULL)
		{
			LWLockAcquire(&MyProc->write_proc->globalxidLock, LW_SHARED);
			/* clear my procno to write proc */
			bitmap_clear_atomic(MyProc->write_proc->read_proc_map, MyProc->pgprocno);
			LWLockRelease(&MyProc->write_proc->globalxidLock);

			/* reset read only process global xid */
			LWLockAcquire(&MyProc->globalxidLock, LW_EXCLUSIVE);
			MyProc->globalXid[0] = '\0';
			LWLockRelease(&MyProc->globalxidLock);
		}
		MyProc->write_proc = NULL;
	}
}

/* ----------------------------------------------------------------
 * PostgresMain
 *	   postgres main loop -- all backends, interactive or otherwise start here
 *
 * argc/argv are the command line arguments to be used.  (When being forked
 * by the postmaster, these are not the original argv array of the process.)
 * dbname is the name of the database to connect to, or NULL if the database
 * name should be extracted from the command line arguments or defaulted.
 * username is the PostgreSQL user name to be used for the session.
 * ----------------------------------------------------------------
 */
void
PostgresMain(int argc, char *argv[],
			 const char *dbname,
			 const char *username)
{
	int			firstchar;
	StringInfoData input_message;
	sigjmp_buf	local_sigjmp_buf;
	volatile bool send_ready_for_query = true;
	volatile bool need_report_activity = false;
	volatile bool idle_in_transaction_timeout_enabled = false;
	volatile bool idle_session_timeout_enabled = false;
	volatile bool persistent_connections_timeout_enabled = false;
#ifdef _PG_ORCL_
	int64      query_idle_duration;
	int64      new_conn_time;
	long		secs;
	int			usecs;
#endif
	
	PGXCNodeHandle *proxy_conn = NULL;

#ifdef PGXC /* PGXC_DATANODE */
	/* Snapshot info */
	GlobalTimestamp			gts;
	/* Timestamp info */
	TimestampTz		timestamp;

	remoteConnType = REMOTE_CONN_APP;
#endif

#ifdef XCP
	parentPGXCNode = NULL;
	cluster_lock_held = false;
	cluster_ex_lock_held = false;
#endif /* XCP */

#ifdef __OPENTENBASE__
	/* for error code contrib */
	g_is_in_init_phase = true;
	g_index_table_part_name = NULL;
#endif

#ifdef __OPENTENBASE__
	HOLD_POOLER_RELOAD();
#endif

	/* Initialize startup process environment if necessary. */
	if (!IsUnderPostmaster)
		InitStandaloneProcess(argv[0]);

	SetProcessingMode(InitProcessing);

	/*
	 * Set default values for command-line options.
	 */
	if (!IsUnderPostmaster)
		InitializeGUCOptions();

	/*
	 * Parse command-line options.
	 */
	process_postgres_switches(argc, argv, PGC_POSTMASTER, &dbname);

	/* Must have gotten a database name, or have a default (the username) */
	if (dbname == NULL)
	{
		dbname = username;
		if (dbname == NULL)
			ereport(FATAL,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("%s: no database nor user name specified",
							progname)));
	}

	/* Acquire configuration parameters, unless inherited from postmaster */
	if (!IsUnderPostmaster)
	{
		if (!SelectConfigFiles(userDoption, progname))
			proc_exit(1);
	}

	/*
	 * Set up signal handlers and masks.
	 *
	 * Note that postmaster blocked all signals before forking child process,
	 * so there is no race condition whereby we might receive a signal before
	 * we have set up the handler.
	 *
	 * Also note: it's best not to use any signals that are SIG_IGNored in the
	 * postmaster.  If such a signal arrives before we are able to change the
	 * handler to non-SIG_IGN, it'll get dropped.  Instead, make a dummy
	 * handler in the postmaster to reserve the signal. (Of course, this isn't
	 * an issue for signals that are locally generated, such as SIGALRM and
	 * SIGPIPE.)
	 */
	if (am_walsender)
		WalSndSignals();
	else
	{
		pqsignal(SIGHUP, PostgresSigHupHandler);	/* set flag to read config
													 * file */
		pqsignal(SIGINT, StatementCancelHandler);	/* cancel current query */
		pqsignal(SIGTERM, die); /* cancel current query and exit */

		/*
		 * In a standalone backend, SIGQUIT can be generated from the keyboard
		 * easily, while SIGTERM cannot, so we make both signals do die()
		 * rather than quickdie().
		 */
		if (IsUnderPostmaster)
			pqsignal(SIGQUIT, quickdie);	/* hard crash time */
		else
			pqsignal(SIGQUIT, die); /* cancel current query and exit */
		InitializeTimeouts();	/* establishes SIGALRM handler */

		/*
		 * Ignore failure to write to frontend. Note: if frontend closes
		 * connection, we will notice it and exit cleanly when control next
		 * returns to outer loop.  This seems safer than forcing exit in the
		 * midst of output during who-knows-what operation...
		 */
		pqsignal(SIGPIPE, SIG_IGN);
		pqsignal(SIGUSR1, procsignal_sigusr1_handler);
#ifdef __OPENTENBASE_C__
		/* SIGUSR2 is registered for postgres in SetRemoteSubplan */
#endif
		pqsignal(SIGUSR2, SIG_IGN);
		pqsignal(SIGFPE, FloatExceptionHandler);

		/*
		 * Reset some signals that are accepted by postmaster but not by
		 * backend
		 */
		pqsignal(SIGCHLD, SIG_DFL); /* system() requires this on some
									 * platforms */
	}

	/* postgres exited when postmaster has been killed */
	prctl(PR_SET_PDEATHSIG, SIGKILL);

	pqinitmask();

	if (IsUnderPostmaster)
	{
		/* We allow SIGQUIT (quickdie) at all times */
		sigdelset(&BlockSig, SIGQUIT);
	}

	PG_SETMASK(&BlockSig);		/* block everything except SIGQUIT */

	if (!IsUnderPostmaster)
	{
		/*
		 * Validate we have been given a reasonable-looking DataDir (if under
		 * postmaster, assume postmaster did this already).
		 */
		Assert(DataDir);
		ValidatePgVersion(DataDir);

		/* Change into DataDir (if under postmaster, was done already) */
		ChangeToDataDir();

		/*
		 * Create lockfile for data directory.
		 */
		CreateDataDirLockFile(false);

		/* Initialize MaxBackends (if under postmaster, was done already) */
		InitializeMaxBackends();
	}

	/* Early initialization */
	BaseInit();

	/*
	 * Create a per-backend PGPROC struct in shared memory, except in the
	 * EXEC_BACKEND case where this was done in SubPostmasterMain. We must do
	 * this before we can use LWLocks (and in the EXEC_BACKEND case we already
	 * had to do some stuff with LWLocks).
	 */
#ifdef EXEC_BACKEND
	if (!IsUnderPostmaster)
		InitProcess();
#else
	InitProcess();
#endif

	/* We need to allow SIGINT, etc during the initial transaction */
	PG_SETMASK(&UnBlockSig);

	/*
	 * General initialization.
	 *
	 * NOTE: if you are tempted to add code in this vicinity, consider putting
	 * it inside InitPostgres() instead.  In particular, anything that
	 * involves database access should be there, not here.
	 */
	InitPostgres(dbname, InvalidOid, username, InvalidOid, NULL);

#ifdef _PG_ORCL_
	SessionStartTime = GetCurrentTimestamp();
	LastTxEndTime = GetCurrentTimestamp();
#endif

#ifdef __AUDIT__
	if (IsBackendPostgres)
	{
		AuditLoggerQueueAcquire();	
	}
#endif

	/*
	 * If the PostmasterContext is still around, recycle the space; we don't
	 * need it anymore after InitPostgres completes.  Note this does not trash
	 * *MyProcPort, because ConnCreate() allocated that space with malloc()
	 * ... else we'd need to copy the Port data first.  Also, subsidiary data
	 * such as the username isn't lost either; see ProcessStartupPacket().
	 */
	if (PostmasterContext)
	{
		MemoryContextDelete(PostmasterContext);
		PostmasterContext = NULL;
	}

	SetProcessingMode(NormalProcessing);

	/*
	 * Now all GUC states are fully set up.  Report them to client if
	 * appropriate.
	 */
	BeginReportingGUCOptions();

	/*
	 * Also set up handler to log session end; we have to wait till now to be
	 * sure Log_disconnections has its final value.
	 */
	if (IsUnderPostmaster && Log_disconnections)
		on_proc_exit(log_disconnections, 0);

	/* Perform initialization specific to a WAL sender process. */
	if (am_walsender)
		InitWalSender();

	/*
	 * process any libraries that should be preloaded at backend start (this
	 * likewise can't be done until GUC settings are complete)
	 */
	process_session_preload_libraries();

	/*
	 * Send this backend's cancellation info to the frontend.
	 */
	if (whereToSendOutput == DestRemote)
	{
		StringInfoData buf;

		pq_beginmessage(&buf, 'K');
		pq_sendint32(&buf, (int32) MyProcPid);
		pq_sendint32(&buf, (int32) MyCancelKey);
		pq_endmessage(&buf);
		/* Need not flush since ReadyForQuery will do it. */
	}

	/* Welcome banner for standalone case */
	if (whereToSendOutput == DestDebug)
		printf("\nPostgreSQL stand-alone backend %s\n", PG_VERSION);

	/*
	 * Create the memory context we will use in the main loop.
	 *
	 * MessageContext is reset once per iteration of the main loop, ie, upon
	 * completion of processing of each command message from the client.
	 */
	MessageContext = AllocSetContextCreate(TopMemoryContext,
										   "MessageContext",
										   ALLOCSET_DEFAULT_SIZES);
#ifdef __AUDIT__
	AuditContext = AllocSetContextCreate(TopMemoryContext,
										   "AuditContext",
										   ALLOCSET_DEFAULT_SIZES);
#endif

	/*
	 * Create memory context and buffer used for RowDescription messages. As
	 * SendRowDescriptionMessage(), via exec_describe_statement_message(), is
	 * frequently executed for ever single statement, we don't want to
	 * allocate a separate buffer every time.
	 */
	row_description_context = AllocSetContextCreate(TopMemoryContext,
													"RowDescriptionContext",
													ALLOCSET_DEFAULT_SIZES);
	MemoryContextSwitchTo(row_description_context);
	initStringInfo(&row_description_buf);
	MemoryContextSwitchTo(TopMemoryContext);

	/*
	 * Remember stand-alone backend startup time
	 */
	if (!IsUnderPostmaster)
		PgStartTime = GetCurrentTimestamp();

#ifdef PGXC
	/*
	 * Initialize key pair to be used as object id while using advisory lock
	 * for backup
	 */
	xc_lockForBackupKey1 = Int32GetDatum(XC_LOCK_FOR_BACKUP_KEY_1);
	xc_lockForBackupKey1 = Int32GetDatum(XC_LOCK_FOR_BACKUP_KEY_2);

#ifdef XCP
	/*
	 * Prepare to handle distributed requests. Do that after sending down
	 * ReadyForQuery, to avoid pooler blocking.
	 *
	 * Also do this only when we can access the catalogs. For example, a
	 * wal-sender can't do that since its not connected to a specific database
	 */
	if (IsUnderPostmaster && !am_walsender)
	{
		start_xact_command();
		InitMultinodeExecutor(false);
		finish_xact_command();
	}

#ifdef USE_MODULE_MSGIDS
	AtProcStart_MsgModule();
#endif

	/* if we exit, try to release cluster lock properly */
	on_shmem_exit(PGXCCleanClusterLock, 0);

	/* If we exit, first try and clean connections and send to pool */
	on_proc_exit(PGXCNodeCleanAndRelease, 0);
#else
	/* If this postmaster is launched from another Coord, do not initialize handles. skip it */
	if (IS_PGXC_COORDINATOR && !IsPoolHandle())
	{
		CurrentResourceOwner = ResourceOwnerCreate(NULL, "ForPGXCNodes");

		InitMultinodeExecutor(false);

		pool_handle = GetPoolManagerHandle();
		if (pool_handle == NULL)
		{
			ereport(ERROR,
				(errcode(ERRCODE_IO_ERROR),
				 errmsg("Can not connect to pool manager")));
		}
		/* Pooler initialization has to be made before ressource is released */
		PoolManagerConnect(pool_handle, dbname, username, session_options());

		ResourceOwnerRelease(CurrentResourceOwner, RESOURCE_RELEASE_BEFORE_LOCKS, true, true);
		ResourceOwnerRelease(CurrentResourceOwner, RESOURCE_RELEASE_LOCKS, true, true);
		ResourceOwnerRelease(CurrentResourceOwner, RESOURCE_RELEASE_AFTER_LOCKS, true, true);
		CurrentResourceOwner = NULL;

		/* If we exit, first try and clean connections and send to pool */
		on_proc_exit (PGXCNodeCleanAndRelease, 0);
	}
#endif /* XCP */
	if (IS_PGXC_DATANODE)
	{
		/* If we exit, first try and clean connection to GTM */
		on_proc_exit (DataNodeShutdown, 0);
	}
#endif

	/*
	 * POSTGRES main processing loop begins here
	 *
	 * If an exception is encountered, processing resumes here so we abort the
	 * current transaction and start a new one.
	 *
	 * You might wonder why this isn't coded as an infinite loop around a
	 * PG_TRY construct.  The reason is that this is the bottom of the
	 * exception stack, and so with PG_TRY there would be no exception handler
	 * in force at all during the CATCH part.  By leaving the outermost setjmp
	 * always active, we have at least some chance of recovering from an error
	 * during error recovery.  (If we get into an infinite loop thereby, it
	 * will soon be stopped by overflow of elog.c's internal state stack.)
	 *
	 * Note that we use sigsetjmp(..., 1), so that this function's signal mask
	 * (to wit, UnBlockSig) will be restored when longjmp'ing to here.  This
	 * is essential in case we longjmp'd out of a signal handler on a platform
	 * where that leaves the signal blocked.  It's not redundant with the
	 * unblock in AbortTransaction() because the latter is only called if we
	 * were inside a transaction.
	 */

	if (sigsetjmp(local_sigjmp_buf, 1) != 0)
	{
		/*
		 * NOTE: if you are tempted to add more code in this if-block,
		 * consider the high probability that it should be in
		 * AbortTransaction() instead.  The only stuff done directly here
		 * should be stuff that is guaranteed to apply *only* for outer-level
		 * error recovery, such as adjusting the FE/BE protocol status.
		 */

		/* Since not using PG_TRY, must reset error stack by hand */
		error_context_stack = NULL;

		/* Prevent interrupts while cleaning up */
		HOLD_INTERRUPTS();

		/*
		 * Forget any pending QueryCancel request, since we're returning to
		 * the idle loop anyway, and cancel any active timeout requests.  (In
		 * future we might want to allow some timeout requests to survive, but
		 * at minimum it'd be necessary to do reschedule_timeouts(), in case
		 * we got here because of a query cancel interrupting the SIGALRM
		 * interrupt handler.)	Note in particular that we must clear the
		 * statement and lock timeout indicators, to prevent any future plain
		 * query cancels from being misreported as timeouts in case we're
		 * forgetting a timeout cancel.
		 */
		disable_all_timeouts(false);
		QueryCancelPending = false; /* second to avoid race condition */

		SubThreadErrorPending = false;

		/* Not reading from the client anymore. */
		DoingCommandRead = false;

		/* Make sure libpq is in a good state */
		pq_comm_reset();

		/* Report the error to the client and/or server log */
		EmitErrorReport();

		/*
		 * Make sure debug_query_string gets reset before we possibly clobber
		 * the storage it points at.
		 */
		debug_query_string = NULL;

		if (!PGXCNodeTypeCoordinator && backend_cn)
		{
			isPGXCCoordinator = false;
			isPGXCDataNode = true;
			backend_cn = false;
			remoteConnType = savedRemoteConn;
		}

		g_in_rollback_to = false;
		g_in_release_savepoint = false;

#ifdef __OPENTENBASE__
		if (g_in_plpgsql_exec_fun > 0)
		{
			/*
			 * There will be multi implicit savepoints if error is caused by statement.
			 * Here, we will rollback all but top implicit savepoint.
			 */
			g_in_plpgsql_exec_fun = 0;
		}
#endif
#ifdef __OPENTENBASE_C__
		end_query_requested = false;
#endif
#ifdef __AUDIT__
		/* transaction will abort or rollback */
		if (xact_started)
			AuditProcessResultInfo(false);
#endif

				while (IsSubTransaction() && ORA_MODE)
				{
					if (
						IsPLpgSQLSubTransaction())
						RollbackAndReleaseCurrentSubTransaction();
					else
						break;
				}
					AbortCurrentTransaction();

		if (am_walsender)
			WalSndErrorCleanup();
		PortalErrorCleanup();
		SPICleanup();
		ActivePortal = NULL;

		/*
		 * We can't release replication slots inside AbortTransaction() as we
		 * need to be able to start and abort transactions while having a slot
		 * acquired. But we never need to hold them across top level errors,
		 * so releasing here is fine. There also is a before_shmem_exit()
		 * callback ensuring correct cleanup on FATAL errors.
		 */
		if (MyReplicationSlot != NULL)
			ReplicationSlotRelease();

		/* We also want to cleanup temporary slots on error. */
		ReplicationSlotCleanup();

		jit_reset_after_error();

		/*
		 * Now return to normal top-level context and clear ErrorContext for
		 * next time.
		 */
		MemoryContextSwitchTo(TopMemoryContext);
		FlushErrorState();

		/*
		 * If we were handling an extended-query-protocol message, initiate
		 * skip till next Sync.  This also causes us not to issue
		 * ReadyForQuery (until we get Sync).
		 */
		if (doing_extended_query_message)
			ignore_till_sync = true;

		/* We don't have a transaction command open anymore */
		xact_started = false;

		/*
		 * If an error occurred while we were reading a message from the
		 * client, we have potentially lost track of where the previous
		 * message ends and the next one begins.  Even though we have
		 * otherwise recovered from the error, we cannot safely read any more
		 * messages from the client, so there isn't much we can do with the
		 * connection anymore.
		 */
		if (pq_is_reading_msg())
			ereport(FATAL,
					(errcode(ERRCODE_PROTOCOL_VIOLATION),
					 errmsg("terminating connection because protocol synchronization was lost")));
		allow_modify_index_table = false;
		pfree_ext(g_index_table_part_name);
		/* Now we can allow interrupts again */
		RESUME_INTERRUPTS();

		/* allow memtrack */
		RESUME_TRACK_MEMINFOS();

		ResetPreSPMPlan(true);

		if (IS_ACCESS_NODE)
			ResetLocalFidFlevel();
	}

#ifdef __OPENTENBASE__
	/* for error code contrib */
	g_is_in_init_phase = false;
#endif

	/* We can now handle ereport(ERROR) */
	PG_exception_stack = &local_sigjmp_buf;

	if (!ignore_till_sync)
		send_ready_for_query = true;	/* initially, or after error */
	g_need_tuple_descriptor = true;
	g_dn_direct_printtup =  false;
	is_internal_cmd = false;
	is_pooler_show_cmd = false;
	savepoint_define_cmd = false;
#ifdef __OPENTENBASE__
	if (!am_walsender)
	{
		IsNormalPostgres = true;
	}
#endif

	/*
	 * Non-error queries loop here.
	 */
	for (;;)
	{
#ifdef _MLS_
		mls_reset_command_tag();
#endif
#ifdef _PUB_SUB_RELIABLE_
		//should keep wal_stream_type set by user
		//wal_reset_stream();
#endif
#ifdef __SUBSCRIPTION__
		logical_apply_reset_ignore_pk_conflict();
		OpenTenBaseSubscriptionApplyWorkerReset();
#endif

		/*
		 * At top of loop, reset extended-query-message flag, so that any
		 * errors encountered in "idle" state don't provoke skip.
		 */
		doing_extended_query_message = false;

		/*
		 * Reset end_query_requested every command, don't worry about this
		 * being a mistake, the CN will send the signal repeatedly anyway.
		 */
		end_query_requested = false;

		/*
		 * Reset spi_query_string;
		 */
		spi_query_string = NULL;

		/*
		 * Release storage left over from prior query cycle, and create a new
		 * query input buffer in the cleared MessageContext.
		 */
		MemoryContextSwitchTo(MessageContext);
		MemoryContextResetAndDeleteChildren(MessageContext);

		/*
		 * Must reset _SPI_errstack when MessageContext is reset.
		 */
		SPI_reset_errstack();

#ifdef __AUDIT__
		MemoryContextResetAndDeleteChildren(AuditContext);
#endif
#ifdef __OPENTENBASE__
		CheckInvalidateRemoteHandles();
#endif

		initStringInfo(&input_message);

#ifdef __AUDIT__
		AuditClearResultInfo();
#endif
#ifdef __OPENTENBASE__
		ClearPrepareGID();
#endif

		/*
		 * Also consider releasing our catalog snapshot if any, so that it's
		 * not preventing advance of global xmin while we wait for the client.
		 */
		InvalidateCatalogSnapshotConditionally();

		/*
		 * (1) If we've reached idle state, tell the frontend we're ready for
		 * a new query.
		 *
		 * Note: this includes fflush()'ing the last of the prior output.
		 *
		 * This is also a good time to send collected statistics to the
		 * collector, and to update the PS stats display.  We avoid doing
		 * those every time through the message loop because it'd slow down
		 * processing of batched messages, and because we don't want to report
		 * uncommitted updates (that confuses autovacuum).  The notification
		 * processor wants a call too, if we are not in a transaction block.
		 */
		if (send_ready_for_query || need_report_activity)
		{
			if (IsAbortedTransactionBlockState())
			{
				set_ps_display("idle in transaction (aborted)", false);
				pgstat_report_activity(STATE_IDLEINTRANSACTION_ABORTED, NULL);

				/* Start the idle-in-transaction timer */
				if (IdleInTransactionSessionTimeout > 0 && IsConnFromApp())
				{
					idle_in_transaction_timeout_enabled = true;
					enable_timeout_after(IDLE_IN_TRANSACTION_SESSION_TIMEOUT,
										 IdleInTransactionSessionTimeout);
				}
			}
			else if (IsTransactionOrTransactionBlock())
			{
				set_ps_display("idle in transaction", false);
				pgstat_report_activity(STATE_IDLEINTRANSACTION, NULL);

				/* Start the idle-in-transaction timer */
				if (IdleInTransactionSessionTimeout > 0 && IsConnFromApp())
				{
					idle_in_transaction_timeout_enabled = true;
					enable_timeout_after(IDLE_IN_TRANSACTION_SESSION_TIMEOUT,
										 IdleInTransactionSessionTimeout);
				}
			}
			else
			{
				ProcessCompletedNotifies();
				pgstat_report_stat(false);

				set_ps_display("idle", false);
				pgstat_report_activity(STATE_IDLE, NULL);

				/* Start the idle-in-session timer */
				if (IdleSessionTimeout > 0 && IsConnFromApp())
				{
					idle_session_timeout_enabled = true;
					enable_timeout_after(IDLE_SESSION_TIMEOUT,
										 IdleSessionTimeout);
				}

				/* Start the persistent connections timeout timer */
				if (PersistentConnectionsTimeout > 0)
				{
					persistent_connections_timeout_enabled = true;
					enable_timeout_after(PERSISTENT_CONNECTIONS_TIMEOUT,
										 PersistentConnectionsTimeout);
				}
			}

			if (send_ready_for_query)
				ReadyForQuery(whereToSendOutput);

#ifdef PGXC
			/*
			 * Helps us catch any problems where we did not send down a snapshot
			 * when it was expected. However if any deferred trigger is supposed
			 * to be fired at commit time we need to preserve the snapshot sent previously
			 */
			if ((IS_PGXC_DATANODE || IsConnFromCoord()) && !IsAnyAfterTriggerDeferred())
				UnsetGlobalSnapshotData();
#endif
			ProcessMemTrackAchieveInterrupt();

			send_ready_for_query = false;
			need_report_activity = false;
		}

		/*
		 * (2) Allow asynchronous signals to be executed immediately if they
		 * come in while we are waiting for client input. (This must be
		 * conditional since we don't want, say, reads on behalf of COPY FROM
		 * STDIN doing the same thing.)
		 */
		DoingCommandRead = true;
#ifdef __OPENTENBASE__
		RESUME_POOLER_RELOAD();
#endif
		/*
		 * (3) read a command (loop blocks here)
		 */
		firstchar = ReadCommand(&input_message);
#ifdef __OPENTENBASE__
		HOLD_POOLER_RELOAD();
#endif

		/*
		 * (4) disable async signal conditions again.
		 *
		 * Query cancel is supposed to be a no-op when there is no query in
		 * progress, so if a query cancel arrived while we were idle, just
		 * reset QueryCancelPending. ProcessInterrupts() has that effect when
		 * it's called when DoingCommandRead is set, so check for interrupts
		 * before resetting DoingCommandRead.
		 */
		CHECK_FOR_INTERRUPTS();
		DoingCommandRead = false;

		/*
		 * (5) turn off the idle-in-transaction and idle-in-session and persistent-connections timeout
		 */
		if (idle_in_transaction_timeout_enabled)
		{
			disable_timeout(IDLE_IN_TRANSACTION_SESSION_TIMEOUT, false);
			idle_in_transaction_timeout_enabled = false;
		}

		if (idle_session_timeout_enabled)
		{
			disable_timeout(IDLE_SESSION_TIMEOUT, false);
			idle_session_timeout_enabled = false;
		}

		if (persistent_connections_timeout_enabled)
		{
			disable_timeout(PERSISTENT_CONNECTIONS_TIMEOUT, false);
			persistent_connections_timeout_enabled = false;
		}

		/*
		 * (6) check for any other interesting events that happened while we
		 * slept.
		 */
		if (ConfigReloadPending)
		{
			ConfigReloadPending = false;
			ProcessConfigFile(PGC_SIGHUP);
		}

		/*
		 * (7) process the command.  But ignore it if we're skipping till
		 * Sync.
		 */
		if (ignore_till_sync && firstchar != EOF && firstchar != 'N')
			continue;

#ifdef XCP
		/*
		 * Acquire the ClusterLock before starting query processing.
		 *
		 * If we are inside a transaction block, this lock will be already held
		 * when the transaction began
		 *
		 * If the session has invoked a PAUSE CLUSTER earlier, then this lock
		 * will be held already in exclusive mode. No need to lock in that case
		 */
		if (IsUnderPostmaster && IS_PGXC_COORDINATOR && !cluster_ex_lock_held && !cluster_lock_held)
		{
			bool exclusive = false;
			AcquireClusterLock(exclusive);
			cluster_lock_held = true;
		}
#endif /* XCP */

#ifdef _PG_ORCL_
		TimestampDifference(SessionStartTime,
							GetCurrentTimestamp(),
							&secs, &usecs);
		new_conn_time = (int64)(secs / SECS_PER_MINUTE);

		TimestampDifference(LastTxEndTime,
							GetCurrentTimestamp(),
							&secs, &usecs);
		query_idle_duration = (int64)(secs / SECS_PER_MINUTE);

		if (profile_resource_limit && MyProfile && IS_PGXC_LOCAL_COORDINATOR)
			ProfileSessionResourceLimitAuthCN(new_conn_time, query_idle_duration);
#endif

		if (am_proxy_for_dn)
		{
			proxy_conn = handle_request_msg_on_proxy(proxy_conn, firstchar, &input_message);
			continue;
		}
		
        pgxc_copy_global_trace_id_to_history();
		if (IS_PGXC_LOCAL_COORDINATOR && IsConnFromApp())
		{
			/* let global trace id to be regenerated */
			GlobalTraceVxid = InvalidTransactionId;
			/* new query reset the trace log, make it regenerate(the last one may be pg clean, and set it not regenerate) */
			pgxc_node_set_global_traceid_regenerate(true);
		}

		switch (firstchar)
		{
			case 'Q':			/* simple query */
				{
					const char *query_string;
#ifdef _PG_ORCL_
					char *query_str = NULL;
					int query_len = 0;
#endif
					/* Set statement_timestamp() */
					SetCurrentStatementStartTimestamp();

					query_string = pq_getmsgstring(&input_message);
					pq_getmsgend(&input_message);
#ifdef _PG_ORCL_
					if (IS_ACCESS_NODE)
					{
						bool dollar_or_decl = false;
						int add_dollar_pos = 0,
							comments_len = 0;
						bool is_find = false;
						char *new_query_string = NULL;

						new_query_string = preprocess_comment(query_string, &is_find);
						query_len = strlen(new_query_string);
						comments_len = is_find ? (new_query_string - query_string) : 0;
						add_dollar_pos = check_func_proc_add_dollar_pos(new_query_string, query_len, &dollar_or_decl);
						if (add_dollar_pos && false)
						{
							const char *d_dollar = "$$ ";
							const char *d_dollar_decl = "$$\n declare ";
							const char *dollar_lang = " $$ LANGUAGE oraplsql;";

							process_last_pos_slash(new_query_string, query_len);

							query_str = (char *)palloc0(query_len + strlen(d_dollar_decl) + strlen(dollar_lang) + 1);
							strncpy(query_str, new_query_string, add_dollar_pos);
							if(dollar_or_decl)
								strncpy(query_str + add_dollar_pos, d_dollar, strlen(d_dollar));
							else
								strncpy(query_str + add_dollar_pos, d_dollar_decl, strlen(d_dollar_decl));
							strcpy(query_str + strlen(query_str), new_query_string + add_dollar_pos);
							strcpy(query_str + strlen(query_str), dollar_lang);
							new_query_string = query_str;
						}
						else if (check_anonymous_block_add_dollar(new_query_string, query_len))
						{
							const char *do_dollar = "do\n$$";
							const char *dollar_semicolon = "\n$$;";

							process_last_pos_slash(new_query_string, query_len);

							query_str = (char *)palloc0(query_len + strlen(do_dollar) + strlen(dollar_semicolon) + 1);
							strncpy(query_str, do_dollar, strlen(do_dollar));
							strncpy(query_str+strlen(do_dollar), new_query_string, query_len);
							strncpy(query_str+strlen(do_dollar)+query_len, dollar_semicolon, strlen(dollar_semicolon));
							new_query_string = query_str;
						}

						/* Concatenate comments with SQL */
						if (is_find)
						{
							int new_query_len = strlen(new_query_string);
							char *tmp = palloc0((comments_len + new_query_len + 1) * sizeof(char));

							strncpy(tmp, query_string, comments_len);
							strncpy(tmp + comments_len, new_query_string, new_query_len);
							query_string = tmp;

							if (query_str)
							{
								pfree(query_str);
							}
							query_str = tmp;
						}
						else
						{
							query_string = new_query_string;
						}
					}
#endif

					if (am_walsender)
					{
						if (!exec_replication_command(query_string))
							exec_simple_query(query_string);
					}
					else
						exec_simple_query(query_string);

					g_need_tuple_descriptor = true;
					g_dn_direct_printtup =  false;
					is_internal_cmd = false;
					is_pooler_show_cmd = false;
					send_ready_for_query = true;
					GlobalTraceVxid = 0;
#ifdef _PG_ORCL_
					if (query_str)
						pfree(query_str);
#endif
				}
				break;

			case 'P':			/* parse */
				{
					const char *stmt_name;
					const char *query_string;
					int			numParams;
					Oid		   *paramTypes = NULL;
					char 	  **paramTypeNames = NULL;

					forbidden_in_wal_sender(firstchar);

					/* Set statement_timestamp() */
					SetCurrentStatementStartTimestamp();

					stmt_name = pq_getmsgstring(&input_message);
					query_string = pq_getmsgstring(&input_message);
					numParams = pq_getmsgint(&input_message, 2);
					paramTypes = (Oid *) palloc(numParams * sizeof(Oid));
					if (numParams > 0)
					{
						int			i;
#ifdef PGXC
						if (IsConnFromCoord() || IsConnFromDatanode())
						{
							paramTypeNames = (char **)palloc(numParams * sizeof(char *));
							for (i = 0; i < numParams; i++)
								paramTypeNames[i] = (char *)pq_getmsgstring(&input_message);
						}
						else
#endif /* PGXC */
						{
							for (i = 0; i < numParams; i++)
								paramTypes[i] = pq_getmsgint(&input_message, 4);
						}
					}
					pq_getmsgend(&input_message);
#ifdef _PG_ORCL_
					if (IS_ACCESS_NODE)
					{
						bool dollar_or_decl = false;
						int add_dollar_pos = 0,
							comments_len = 0;
						char *query_str = NULL;
						int query_len = 0;
						bool is_find = false;
						char *new_query_string = NULL;

						new_query_string = preprocess_comment(query_string, &is_find);
						query_len = strlen(new_query_string);
						comments_len = is_find ? (new_query_string - query_string) : 0;
						add_dollar_pos = check_func_proc_add_dollar_pos(new_query_string, query_len, &dollar_or_decl);
						if (add_dollar_pos && false)
						{
							const char *d_dollar = "$$ ";
							const char *d_dollar_decl = "$$\n declare ";
							const char *dollar_lang = " $$ LANGUAGE oraplsql;";

							process_last_pos_slash(new_query_string, query_len);

							query_str = (char *)palloc0(query_len + strlen(d_dollar_decl) + strlen(dollar_lang) + 1);
							strncpy(query_str,new_query_string,add_dollar_pos);
							if(dollar_or_decl)
							{
								strncpy(query_str+add_dollar_pos, d_dollar, strlen(d_dollar));
							}
							else
							{
								strncpy(query_str+add_dollar_pos, d_dollar_decl, strlen(d_dollar_decl));
							}
							strcpy(query_str+strlen(query_str), new_query_string+add_dollar_pos);
							strcpy(query_str+strlen(query_str), dollar_lang);
							new_query_string = query_str;
						}
						else if (check_anonymous_block_add_dollar(new_query_string, query_len))
						{
							const char *do_dollar = "do\n$$\n";
							const char *dollar_semicolon = "\n$$;";

							process_last_pos_slash(new_query_string, query_len);

							query_str = (char *)palloc0(query_len + strlen(do_dollar) + strlen(dollar_semicolon) + 1);
							strncpy(query_str, do_dollar, strlen(do_dollar));
							strncpy(query_str+strlen(do_dollar), new_query_string, query_len);
							strncpy(query_str+strlen(do_dollar)+query_len, dollar_semicolon, strlen(dollar_semicolon));
							new_query_string = query_str;
						}

						/* Concatenate comments with SQL */
						if (is_find)
						{
							int new_query_len = strlen(new_query_string);
							char *tmp = palloc0((comments_len + new_query_len + 1) * sizeof(char));

							strncpy(tmp, query_string, comments_len);
							strncpy(tmp + comments_len, new_query_string, new_query_len);
							query_string = tmp;

							if (query_str)
							{
								pfree(query_str);
							}
							query_str = tmp;
						}
						else
						{
							query_string = new_query_string;
						}
					}
#endif
					exec_parse_message(query_string, stmt_name,
									   paramTypes, paramTypeNames,
									   numParams);
				}
				break;

#ifdef XCP
			case 'p':			/* plan */
				{
					const char *stmt_name;
					const char *query_string;
					const char *plan_string;
					int			numParams;
					char 	  **paramTypes = NULL;
					int         instrument_options = 0;

					/* Set statement_timestamp() */
					SetCurrentStatementStartTimestamp();
					stmt_name = pq_getmsgstring(&input_message);
					query_string = pq_getmsgstring(&input_message);
					plan_string = pq_getmsgstring(&input_message);
					numParams = pq_getmsgint(&input_message, 4);
					paramTypes = (char **)palloc(numParams * sizeof(char *));
					if (numParams > 0)
					{
						int			i;
						for (i = 0; i < numParams; i++)
							paramTypes[i] = (char *)
									pq_getmsgstring(&input_message);
					}
#ifdef __OPENTENBASE_C__
					instrument_options = pq_getmsgint(&input_message, 4);
#endif
					pq_getmsgend(&input_message);

					exec_plan_message(query_string, stmt_name, plan_string,
									paramTypes, numParams,
									instrument_options);
				}
				break;
#endif

			case 'B':			/* bind */
				forbidden_in_wal_sender(firstchar);

				/* Set statement_timestamp() */
				SetCurrentStatementStartTimestamp();

				/*
				 * this message is complex enough that it seems best to put
				 * the field extraction out-of-line
				 */
				exec_bind_message(&input_message);
				break;

			case 'E':			/* execute */
				{
					const char *portal_name;
					int			max_rows;
					bool		rewind = false;

					forbidden_in_wal_sender(firstchar);

					/* Set statement_timestamp() */
					SetCurrentStatementStartTimestamp();

					portal_name = pq_getmsgstring(&input_message);
					max_rows = pq_getmsgint(&input_message, 4);

					if (IsConnFromCoord())
					{
						char c = pq_getmsgbyte(&input_message);
						Assert(c == 't' || c == 'f');
						rewind = (c == 't');
					}

					pq_getmsgend(&input_message);

					exec_execute_message(portal_name, max_rows, rewind);

					/*
					 * After the read-only process completes its execution, it removes itself
					 * from the write process's bitmap.
					 */
					if (g_twophase_state.is_readonly && MyProc->write_proc != NULL)
					{
						LWLockAcquire(&MyProc->write_proc->globalxidLock, LW_SHARED);
						/* clear my procno to write proc */
						bitmap_clear_atomic(MyProc->write_proc->read_proc_map, MyProc->pgprocno);
						LWLockRelease(&MyProc->write_proc->globalxidLock);

						/* reset read only process global xid */
						LWLockAcquire(&MyProc->globalxidLock, LW_EXCLUSIVE);
						MyProc->globalXid[0] = '\0';
						LWLockRelease(&MyProc->globalxidLock);
						MyProc->write_proc = NULL;
					}
				}
				break;
            
            case 'V': /* msg type for batch Bind-Execute for PBE */
                {
					if (!experiment_feature)
						ereport(ERROR,
								(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
									errmsg("exec_batch_bind_execute is experimen feature.")));

                    forbidden_in_wal_sender(firstchar);
                    /* Set statement_timestamp() */
                    SetCurrentStatementStartTimestamp();
                    exec_batch_bind_execute(&input_message);
                }
                break;
            
			case 'F':			/* fastpath function call */
				forbidden_in_wal_sender(firstchar);

				/* Set statement_timestamp() */
				SetCurrentStatementStartTimestamp();

				/* Report query to various monitoring facilities. */
				pgstat_report_activity(STATE_FASTPATH, NULL);
				set_ps_display("<FASTPATH>", false);

				/* start an xact for this function invocation */
				start_xact_command();

				/*
				 * Note: we may at this point be inside an aborted
				 * transaction.  We can't throw error for that until we've
				 * finished reading the function-call message, so
				 * HandleFunctionRequest() must check for it after doing so.
				 * Be careful not to do anything that assumes we're inside a
				 * valid transaction here.
				 */

				/* switch back to message context */
				MemoryContextSwitchTo(MessageContext);

				HandleFunctionRequest(&input_message);

				/* commit the function-invocation transaction */
				finish_xact_command();
				g_need_tuple_descriptor = true;
				g_dn_direct_printtup =  false;
				send_ready_for_query = true;
				is_internal_cmd = false;
				break;

			case 'C':			/* close */
				{
					int			close_type;
					const char *close_target;

					forbidden_in_wal_sender(firstchar);

					close_type = pq_getmsgbyte(&input_message);
					close_target = pq_getmsgstring(&input_message);
					pq_getmsgend(&input_message);

					elog(DEBUG3, "Received a 'C' (close) command for %s, type %c",
							close_target[0] ? close_target : "unnamed_stmt",
							close_type);

					switch (close_type)
					{
						case 'S':
							if (close_target[0] != '\0')
								DropPreparedStatement(close_target, false);
							else
							{
								/* special-case the unnamed statement */
								drop_unnamed_stmt();
							}
							break;
						case 'P':
							{
								Portal		portal;

								portal = GetPortalByName(close_target);
								if (PortalIsValid(portal))
									PortalDrop(portal, false);
							}
							break;
						default:
							ereport(ERROR,
									(errcode(ERRCODE_PROTOCOL_VIOLATION),
									 errmsg("invalid CLOSE message subtype %d",
											close_type)));
							break;
					}

					if (whereToSendOutput == DestRemote)
						pq_putemptymessage('3');	/* CloseComplete */
				}
				break;

			case 'D':			/* describe */
				{
					int			describe_type;
					const char *describe_target;

					forbidden_in_wal_sender(firstchar);

					/* Set statement_timestamp() (needed for xact) */
					SetCurrentStatementStartTimestamp();

					describe_type = pq_getmsgbyte(&input_message);
					describe_target = pq_getmsgstring(&input_message);
					pq_getmsgend(&input_message);

					switch (describe_type)
					{
						case 'S':
							exec_describe_statement_message(describe_target);
							break;
						case 'P':
							exec_describe_portal_message(describe_target);
							break;
						default:
							ereport(ERROR,
									(errcode(ERRCODE_PROTOCOL_VIOLATION),
									 errmsg("invalid DESCRIBE message subtype %d",
											describe_type)));
							break;
					}
				}
				break;

			case 'H':			/* flush */
				pq_getmsgend(&input_message);
				if (whereToSendOutput == DestRemote)
					pq_flush();
				break;

			case 'S':			/* sync */
				pq_getmsgend(&input_message);
				finish_xact_command();
				send_ready_for_query = true;
				g_dn_direct_printtup =  false;
				g_need_tuple_descriptor = true;
				is_internal_cmd = false;

				/*
				 * After the read-only process completes its execution, it removes itself
				 * from the write process's bitmap.
				 */
				if (g_twophase_state.is_readonly && MyProc->write_proc != NULL)
				{
					LWLockAcquire(&MyProc->write_proc->globalxidLock, LW_SHARED);
					/* clear my procno to write proc */
					bitmap_clear_atomic(MyProc->write_proc->read_proc_map, MyProc->pgprocno);
					LWLockRelease(&MyProc->write_proc->globalxidLock);

					/* reset read only process global xid */
					LWLockAcquire(&MyProc->globalxidLock, LW_EXCLUSIVE);
					MyProc->globalXid[0] = '\0';
					LWLockRelease(&MyProc->globalxidLock);
					MyProc->write_proc = NULL;
				}
				break;

			case 'y':			/* sync */
				pq_getmsgend(&input_message);
				finish_xact_command();
				g_dn_direct_printtup =  false;
				g_need_tuple_descriptor = true;
				need_report_activity = true;
				is_internal_cmd = false;
				break;

			case 'U':			/* coord info: coord_pid and coord_vxid */
				{
					int coord_pid = 0;
					TransactionId coord_vxid = InvalidTransactionId;
					TransactionId cur_coord_vxid = pgxc_get_coordinator_proc_vxid();

					coord_pid = (int) pq_getmsgint(&input_message, 4);
					coord_vxid = (TransactionId) pq_getmsgint(&input_message, 4);

					if (coord_vxid !=  cur_coord_vxid && !IsTransactionIdle())
						elog(ERROR, "coord_vxid should not change when transaction no idle. cur_coord_vxid %u "
							 "coord_vxid %u transaction stat %s blockState %s", cur_coord_vxid, coord_vxid,
							 CurrentTransactionTransStateAsString(), CurrentTransactionBlockStateAsString());

					pgxc_set_coordinator_proc_pid(coord_pid);
					pgxc_set_coordinator_proc_vxid(coord_vxid);
					if (InvalidTransactionId == GlobalTraceVxid && 0 != coord_pid && 0 != coord_vxid)
					{
						GlobalTraceVxid = coord_vxid;
					}
					/* the coord call pgxc_node_remote_clean_up_all, which mean this transaction is finished */
					if (0 == coord_pid && 0 == coord_vxid) 
					{
						GlobalTraceVxid = 0;
					}
					elog(DEBUG5, "Received coord_pid: %d, coord_vxid: %u, GlobalTraceVxid: %u", coord_pid, coord_vxid, GlobalTraceVxid);				}
				break;
			case 'j':     /* global trace id */
			    {
					const char *traceid = pq_getmsgstring(&input_message);
					pq_getmsgend(&input_message);
					if (NULL != traceid)
					{
						elog(DEBUG5, "receive global traceid: %s-%s\n", GlobalTraceId, traceid);
						strncpy((char*) GlobalTraceId, traceid, TRACEID_LEN);
					}
					else
					{
						elog(DEBUG5, "receive null global traceid: %s\n", GlobalTraceId);
					}
				}
				break;
				/*
				 * 'X' means that the frontend is closing down the socket. EOF
				 * means unexpected loss of frontend connection. Either way,
				 * perform normal shutdown.
				 */
			case 'X':
			case EOF:

				/*
				 * Reset whereToSendOutput to prevent ereport from attempting
				 * to send any more messages to client.
				 */
				if (whereToSendOutput == DestRemote)
					whereToSendOutput = DestNone;

				/*
				 * NOTE: if you are tempted to add more code here, DON'T!
				 * Whatever you had in mind to do should be set up as an
				 * on_proc_exit or on_shmem_exit callback, instead. Otherwise
				 * it will fail to be called during other backend-shutdown
				 * scenarios.
				 */
				proc_exit(0);

			case 'd':			/* copy data */
			case 'c':			/* copy done */
			case 'f':			/* copy fail */

				/*
				 * Accept but ignore these messages, per protocol spec; we
				 * probably got here because a COPY failed, and the frontend
				 * is still sending data.
				 */
				break;
#ifdef PGXC
			case 'M':			/* Command ID */
				{
					CommandId cid = (CommandId) pq_getmsgint(&input_message, 4);
					elog(DEBUG1, "Received cmd id %u", cid);
					SaveReceivedCommandId(cid);
				}
				break;

			case 'g':			/* gxid */
				{
#ifdef __SUPPORT_DISTRIBUTED_TRANSACTION__
					const char *globalXidString;
					bool isprimary;

					isprimary = pq_getmsgbyte(&input_message);
					globalXidString = pq_getmsgstring(&input_message);
					pq_getmsgend(&input_message);

					SetLocalTransactionId(InvalidTransactionId);
					if (globalXidString[0] != '\0')
					{
						StoreGlobalXid(globalXidString, isprimary);
					}

					if (enable_distri_print)
						elog(LOG, "recieved gxid: %s, primary: %d", globalXidString, isprimary);
#else
					/* Set the GXID we were passed down */
					TransactionId gxid;
					memcpy(&gxid, pq_getmsgbytes(&input_message, sizeof (TransactionId)),
							sizeof (TransactionId));
					elog(DEBUG1, "Received new gxid %u", gxid);
					SetNextTransactionId(gxid);
					pq_getmsgend(&input_message);
#endif
				}
				break;

			case 'K':
				{
					/* members of BackendEnv */
					int		sec_context;
					bool	insec_context;
					bool	isquery;
					const char	*username;

					sec_context = pq_getmsgint(&input_message, sizeof(int));
					insec_context = pq_getmsgint(&input_message, sizeof(bool));
					isquery = pq_getmsgint(&input_message, sizeof(bool));
					username = pq_getmsgstring(&input_message);

					pq_getmsgend(&input_message);

					if (!IsTransactionAbortState())
					{
						Oid		userid;

						if (username[0] != '\0')
						{
							bool	trans_started = false;

							if (!IsTransactionState())
							{
								start_xact_command();
								trans_started = true;
							}

							Assert(IsTransactionState());
							userid = get_role_oid(username, false);

							if (trans_started)
								finish_xact_command();
						}
						else
							userid = GetUserId();

						SetUserIdAndSecContext(userid, sec_context);
						SecContextIsSet = insec_context;
					}

					if (isquery)
						send_ready_for_query = true;

					/*
					 * Here, need not setup dow-stream nodes, since they are
					 * not established, and will be released when topmost node
					 * per-execution is finished.
					 */
				}
				break;

			case 'l':		/* fid and flevel */
				{
					int fid = (int) pq_getmsgint(&input_message, 4);
					int flevel = (int) pq_getmsgint(&input_message, 4);

					pq_getmsgend(&input_message);
					elog(DEBUG1, "Received fid %d flevel %d", fid, flevel);
					SetLocalFidFlevel(fid, flevel);
				}
				break;
			
			case 's':			/* snapshot */
				{
					GlobalTimestamp			gts_check;

					/* Set the snapshot we were passed down */
					memcpy(&gts,
						   pq_getmsgbytes(&input_message, sizeof (GlobalTimestamp)),
						   sizeof (GlobalTimestamp));

					memcpy(&gts_check,
						   pq_getmsgbytes(&input_message, sizeof (GlobalTimestamp)),
						   sizeof (GlobalTimestamp));

					if (gts != gts_check)
						elog(FATAL, "corrupted frontend msg gts: " INT64_FORMAT " " INT64_FORMAT " .", gts, gts_check);
					
					elog(DEBUG8, "receive snapshot gts " INT64_FORMAT, gts);
					pq_getmsgend(&input_message);
					SetGlobalTimestamp(gts, SNAPSHOT_COORDINATOR);
				}
				break;
#ifdef __SUPPORT_DISTRIBUTED_TRANSACTION__
			case 'Z':			/* global prepare timestamp */
				timestamp = (GlobalTimestamp) pq_getmsgint64(&input_message);
				pq_getmsgend(&input_message);
			
				/*
				 * Set Xact global prepare timestamp 
				 */
				if(enable_distri_print)
				{
					elog(LOG, "set global prepare gts " INT64_FORMAT, timestamp);
				}
				SetGlobalPrepareTimestamp(timestamp);
				
				break;

			case 'T':			/* global timestamp */
				timestamp = (GlobalTimestamp) pq_getmsgint64(&input_message);
				pq_getmsgend(&input_message);
			
				/*
				 * Set Xact global commit timestamp 
				 */
				if(enable_distri_print)
				{
					elog(LOG, "set global commit gts " INT64_FORMAT, timestamp);
				}
				SetGlobalCommitTimestamp(timestamp);
				break;

			case 'G':   /* Explicit prepared gid */
				{
					const char *gid;
					gid = pq_getmsgstring(&input_message);
					pq_getmsgend(&input_message);
					remotePrepareGID = MemoryContextStrdup(TopMemoryContext, gid);
					elog(DEBUG8, "receive remote prepare gid %s", remotePrepareGID);
				}
				break;

			case 'W':	/* Prefinish phase */
				timestamp = (GlobalTimestamp) pq_getmsgint64(&input_message);
				pq_getmsgend(&input_message);
				elog(DEBUG8, "get prefinish timestamp " INT64_FORMAT "for gid %s", timestamp, remotePrepareGID);
                LockGXact(remotePrepareGID, GetUserId(), false);
				SetGlobalPrepareTimestamp(timestamp);
				EndExplicitGlobalPrepare(remotePrepareGID);
				pfree(remotePrepareGID);
				remotePrepareGID = NULL;
				ReadyForCommit(whereToSendOutput);
				
				break;
#endif
			case 't':			/* timestamp */
				timestamp = (TimestampTz) pq_getmsgint64(&input_message);
				pq_getmsgend(&input_message);

				/*
				 * Set in xact.x the static Timestamp difference value with GTM
				 * and the timestampreceivedvalues for Datanode reference
				 */
				SetCurrentGTMDeltaTimestamp(timestamp);
				break;

			case 'b':			/* barrier */
				{
					int command;
					char *id;

					command = pq_getmsgbyte(&input_message);
					id = (char *) pq_getmsgstring(&input_message);
					pq_getmsgend(&input_message);

					switch (command)
					{
						case CREATE_BARRIER_PREPARE:
							ProcessCreateBarrierPrepare(id);
							break;

						case CREATE_BARRIER_END:
							ProcessCreateBarrierEnd(id);
							break;

						case CREATE_BARRIER_EXECUTE:
							ProcessCreateBarrierExecute(id);
							break;

						default:
							ereport(ERROR,
									(errcode(ERRCODE_CHARACTER_NOT_IN_REPERTOIRE),
									 errmsg("Invalid command received")));
					}
				}
				break;
#endif /* PGXC */

#ifdef __TWO_PHASE_TRANS__
			case 'e':       /*start node of twophase transaction*/
				{
					const char *startnode;
					startnode = pq_getmsgstring(&input_message);
					if (enable_distri_print)
					{
						elog(LOG, "2pc recv startnode: %s", startnode);
					}
					pq_getmsgend(&input_message);
					StoreStartNode(startnode);
				}
				break;
			case 'O':			/* participants of twophase transaction */
				{
					const char *partnodes;
					partnodes = pq_getmsgstring(&input_message);
					if (enable_distri_print)
					{
						elog(LOG, "2pc recv partnodes: %s", partnodes);
					}
					pq_getmsgend(&input_message);
					StorePartNodes(partnodes);
				}
				break;
			case 'x':           /* xid in start node of twophase transaction */
				{
					TransactionId startxid;
					if (enable_distri_print)
					{
						elog(LOG, "2pc recv str_startxid:%s", input_message.data);
					}
					startxid = (TransactionId)pq_getmsgint(&input_message, 4);
					if (enable_distri_print)
					{
						elog(LOG, "2pc recv startxid:%u", startxid);
					}
					StoreStartXid(startxid);
				}
				break;
			case 'w':       /* database of twophase transaction */
				{
					const char *database;
					database = pq_getmsgstring(&input_message);
					if (enable_distri_print)
					{
						elog(LOG, "2pc recv database: %s", database);
					}
					pq_getmsgend(&input_message);
					StoreDatabase(database);
				}
				break;
			case 'u':       /* user of twophase transaction */
				{
					const char *user;
					user = pq_getmsgstring(&input_message);
					if (enable_distri_print)
					{
						elog(LOG, "2pc recv user: %s", user);
					}
					pq_getmsgend(&input_message);
					StoreUser(user);
				}
				break;
			case 'n':           /* execute in pg_clean */
				{
					/* since g_twophase_state will be reinitialized in pg_clean */
					ClearLocalTwoPhaseState();
					g_twophase_state.in_pg_clean = true;
				}
				break;
			case 'r':
				{
					int c;

					c = pq_getmsgbyte(&input_message);
					if (c == 'Y')
					{
						if (!g_twophase_state.is_readonly && !IsTransactionIdle())
							elog(ERROR, "write handle should not be changed to readonly in transaction "
								 "transaction stat %s blockState %s", CurrentTransactionTransStateAsString(),
								 CurrentTransactionBlockStateAsString());

						g_twophase_state.is_readonly = true;
					}
					else if (c == 'N')
					{
						g_twophase_state.is_readonly = false;
					}
					else
					{
						ereport(ERROR,
							(errcode(ERRCODE_PROTOCOL_VIOLATION),
							 errmsg("invalid logical apply value of readonly %c", c)));
					}
					
				}
				break;
			case 'q':	/* clean distribute msg */
				{
					uint32	TotalProcs = TOTALPROCNUMS;

					MyProc->hasGlobalXid = false;
					SetLocalTransactionId(InvalidTransactionId);
					ResetLocalFidFlevel();
					g_twophase_state.is_readonly = false;

					LWLockAcquire(&MyProc->globalxidLock, LW_EXCLUSIVE);
					MyProc->globalXid[0] = '\0';
					LWLockRelease(&MyProc->globalxidLock);

					MyProc->write_proc = NULL;
					MemSet(MyProc->read_proc_map, 0, bitmap_size(TotalProcs));
				}
				break;
			case 'A':
				{
					g_twophase_state.is_after_prepare = true;
				}
				break;
#endif

#ifdef __SUBSCRIPTION__
			case 'a':			/* logical apply : pgxc_node_send_apply */
				{
					if (IS_PGXC_DATANODE && IsConnFromCoord())
					{
						int c;

						c = pq_getmsgbyte(&input_message);

						if (c == 'Y')
						{
							logical_apply_set_ignore_pk_conflict(true);
						}
						else if (c == 'N')
						{
							logical_apply_set_ignore_pk_conflict(false);
						}
						else
						{
							ereport(ERROR,
								(errcode(ERRCODE_PROTOCOL_VIOLATION),
								 errmsg("invalid logical apply value of ignore_pk_conflict %c", c)));
						}

						c = pq_getmsgbyte(&input_message);

						if (c == 'w')	/* WalSndPrepareWrite */
						{
							XLogRecPtr	start_lsn;
							XLogRecPtr	end_lsn;
							TimestampTz send_time;

							MemoryContext oldcontext;

							start_lsn = pq_getmsgint64(&input_message);
							end_lsn = pq_getmsgint64(&input_message);
							send_time = pq_getmsgint64(&input_message);

							elog(DEBUG8, "receive logical apply start_lsn: %X/%X, end_lsn: %X/%X, send_time: " INT64_FORMAT,
										 (uint32) (start_lsn >> 32), (uint32) start_lsn,
										 (uint32) (end_lsn >> 32), (uint32) end_lsn,
										 send_time);

							wal_set_cluster_stream();

							/* do apply */
							start_xact_command();
							oldcontext = MemoryContextSwitchTo(MessageContext);

							logical_apply_dispatch(&input_message);

							MemoryContextSwitchTo(oldcontext);
							finish_xact_command();

							pq_getmsgend(&input_message);

							/* Send ApplyDone message */
							pq_putmessage('4', NULL, 0);
							pq_flush();
						}
						else if ( c == 'B') /* batch process. */
						{
							MemoryContext oldcontext;
							wal_set_cluster_stream();
							
							/* do apply */
							start_xact_command();
							oldcontext = MemoryContextSwitchTo(MessageContext);
							
							logical_apply_dispatch_batch(&input_message);
							
							MemoryContextSwitchTo(oldcontext);
							finish_xact_command();
							
							pq_getmsgend(&input_message);
							
							/* Send ApplyDone message */
							pq_putmessage('4', NULL, 0);
							pq_flush();
						}
						else
						{
							ereport(FATAL,
								(errcode(ERRCODE_PROTOCOL_VIOLATION),
									 errmsg("invalid frontend message type %d, %d",
											firstchar, c)));
						}
					}
					else
					{
						ereport(FATAL,
							(errcode(ERRCODE_PROTOCOL_VIOLATION),
								 errmsg("invalid frontend message type %d",
										firstchar)));
					}
				}
				break;
#endif
#ifdef __RESOURCE_QUEUE__
			case 'J':	/* receive resources or resources requests */
			{
				if (IS_PGXC_DATANODE && IsConnFromCoord() && IsResQueueEnabled())
				{
					DNReceiveLocalResQueueInfo(&input_message);
				}
				else if (IS_PGXC_DATANODE && IsConnFromCoord() && IsResGroupActivated())
				{
					DNReceiveLocalResGroupInfo(&input_message);
				}
				else if (IS_PGXC_COORDINATOR && IsConnFromCoord() && IsResGroupActivated())
				{
					PGXCNodeHandle	*leaderCnHandle = find_leader_cn();
					if (leaderCnHandle != NULL && PGXCNodeName != NULL && strcmp(leaderCnHandle->nodename, PGXCNodeName) == 0)
					{
						start_xact_command();
						CNReceiveResGroupReqAndReply(&input_message);
						finish_xact_command();
					}
				}
				else
				{
					ereport(FATAL,
							(errcode(ERRCODE_PROTOCOL_VIOLATION),
								 errmsg("invalid frontend message type %d",
										firstchar)));
				}
				break;
			}
#endif
			case 'Y':			/* Options */
			{
				int			option_type;
				const char *option_string;

				forbidden_in_wal_sender(firstchar);

				option_type = pq_getmsgbyte(&input_message);
				option_string = pq_getmsgstring(&input_message);
				pq_getmsgend(&input_message);

				elog(DEBUG3, "Received a 'Y' (OPTION) command for %s, type %c",
					 option_string[0] ? option_string : "unnamed_stmt",
					 option_type);

				switch (option_type)
				{
					case 'D':
						if (option_string[0] == 'f')
							g_need_tuple_descriptor = false;
						break;
					case 'N':
						if (option_string[0] == 't')
							g_dn_direct_printtup = true;
						break;
					case 'I':
						if (option_string[0] == 't')
							is_internal_cmd = true;
						break;
					default:
						ereport(ERROR,
								(errcode(ERRCODE_PROTOCOL_VIOLATION),
										errmsg("invalid OPTION message subtype %d",
											   option_type)));
						break;
				}
				break;
			}
			case 'I':
			{
				int session_id = pq_getmsgint(&input_message, 4);
				bool need_reply = pq_getmsgbyte(&input_message);

				pq_getmsgend(&input_message);
				SetProcSessionId(session_id);
				if (session_id != PGXCCoordSessionId)
					AcceptInvalidationMessages();
				PGXCCoordSessionId = session_id;
				if (need_reply)
					send_ready_for_query = true;
				break;
			}
			case 'N':			/* sequence number */
			{
				internal_cmd_sequence = pq_getmsgbyte(&input_message);
				elog(DEBUG5, "Received message sequence %u", internal_cmd_sequence);
				pq_getmsgend(&input_message);
				break;
			}
			case 'k':			/* Fast path transaction command */
			{
				TransactionStmtKind kind = (TransactionStmtKind)pq_getmsgbyte(&input_message);
				const char *tag = getTranStmtKindTag(kind);
				int isolevel = pq_getmsgbyte(&input_message);
				const char *opt_str = NULL;
				bool need_reply = true;
				int cur_level = GetCurrentTransactionNestLevel();

				if (log_statement == LOGSTMT_ALL)
					elog(LOG, "execute transaction command kind: %s, isolevel: %d", tag, isolevel);
					start_xact_command();

					switch(kind)
					{
						case TRANS_STMT_BEGIN:
						case TRANS_STMT_START:
						{
							elog(DEBUG1, "[NEST_LEVEL_CHECK:%d] recv k_begin. cur_level: %d", MyProcPid, cur_level);
							BeginTransactionBlock();
							if (isolevel != XACT_ISOLATION_INVALID)
							{
								if (isolevel >=  XACT_READ_UNCOMMITTED &&
									isolevel <= XACT_SERIALIZABLE)
									SetTransIsolationLevel(isolevel);
								else
									elog(ERROR, "The transaction isolation level is invalid: %d", isolevel);
							}
							need_reply = false;
							break;
						}
						case TRANS_STMT_BEGIN_SUBTXN:
						{
							elog(DEBUG1, "[NEST_LEVEL_CHECK:%d] recv k_begin_sub. cur_level: %d", MyProcPid, cur_level);
							BeginInternalSubTransaction(NULL);
							need_reply = false;
							break;
						}
						case TRANS_STMT_ROLLBACK_SUBTXN:
						{
							elog(DEBUG1, "[NEST_LEVEL_CHECK:%d] recv k_rollback_sub. cur_level: %d", MyProcPid, cur_level);
							RollbackAndReleaseCurrentSubTransaction();
							break;
						}
						case TRANS_STMT_COMMIT_SUBTXN:
						{
							elog(DEBUG1, "[NEST_LEVEL_CHECK:%d] recv k_commit_sub. cur_level: %d", MyProcPid, cur_level);
							ReleaseCurrentSubTransaction();
							break;
						}
						case TRANS_STMT_COMMIT:
						{
							elog(DEBUG1, "[NEST_LEVEL_CHECK:%d] recv k_commit. cur_level: %d", MyProcPid, cur_level);
							EndTransactionBlock();
							break;
						}
						case TRANS_STMT_PREPARE:
						{
							opt_str = pq_getmsgstring(&input_message);
							PreventCommandDuringRecovery("PREPARE TRANSACTION");
							PrepareTransactionBlock(opt_str);
							break;
						}
						case TRANS_STMT_COMMIT_PREPARED:
						{
							opt_str = pq_getmsgstring(&input_message);
							PreventTransactionChain(true, "COMMIT PREPARED");
							PreventCommandDuringRecovery("COMMIT PREPARED");
							FinishPreparedTransaction(opt_str, true);
							break;
						}
						case TRANS_STMT_ROLLBACK_PREPARED:
						{
							opt_str = pq_getmsgstring(&input_message);
							PreventTransactionChain(true, "ROLLBACK PREPARED");
							PreventCommandDuringRecovery("ROLLBACK PREPARED");
							FinishPreparedTransaction(opt_str, false);
							break;
						}
						case TRANS_STMT_ROLLBACK:
						{
							elog(DEBUG1, "[NEST_LEVEL_CHECK:%d] recv k_rollback. cur_level: %d", MyProcPid, cur_level);
							UserAbortTransactionBlock();
							break;
						}
						default:
							elog(ERROR, "unsupported shortcut transaction command %d", kind);
					}
					finish_xact_command();
				if (log_statement == LOGSTMT_ALL)
					elog(LOG, "statement: %s %s, set isolevel: %d", tag, opt_str ? opt_str : " ", isolevel);

				if (need_reply)
				{
					Assert(tag);
					EndCommand(tag, DestRemote);
					send_ready_for_query = true;
				}
				break;
			}
			case 'o':			/* query_id */
			{
				const char *queryid = pq_getmsgstring(&input_message);
				pq_getmsgend(&input_message);
				strncpy((char *) PGXCQueryId, queryid, NAMEDATALEN);
				break;
			}
			case 'm': /* simple DML protocol */
			{
				/* type + insert_tuple_len + insert_tuple_data + delete_tuple_len + delete_tuple_data + delete_ctid*/
				ItemPointer target_ip;
				CmdType  cmdtype = (CmdType) pq_getmsgint(&input_message, 2);
				const char *relname = pq_getmsgstring(&input_message);
				const char *space = pq_getmsgstring(&input_message);
				int itup_len, dtup_len;
				HeapTuple   itup = NULL;
				HeapTuple   dtup = NULL;
				
				itup_len = (int) pq_getmsgint(&input_message, 4);
				if (itup_len > 0)
				{
					MinimalTuple imtup =
						(MinimalTuple) pq_getmsgbytes(&input_message, itup_len);
					itup = heap_tuple_from_minimal_tuple(imtup);
				}
				
				dtup_len = (int) pq_getmsgint(&input_message, 4);
				if (dtup_len > 0)
				{
					MinimalTuple dmtup =
						(MinimalTuple) pq_getmsgbytes(&input_message, dtup_len);
					dtup = heap_tuple_from_minimal_tuple(dmtup);
				}

				
				target_ip = (ItemPointer)pq_getmsgbytes(&input_message, sizeof(ItemPointerData));
				pq_getmsgend(&input_message);
				
				ExecSimpleDML(relname, space, cmdtype, itup, dtup, target_ip);
				if (ItemPointerIsValid(target_ip) && cmdtype == CMD_UPDATE)
				{
					pq_putmessage('t', (char *)(&itup->t_self), sizeof(ItemPointerData));
				}
				pq_putmessage('C', "DUMMY 0", 8);
				pq_flush();

				send_ready_for_query = true;
				break;
			}
			case 'h':   /* a: analyze, only update stat info; s,d,u: sync spm plan table info */
			{
				char type = pq_getmsgbyte(&input_message);
				if (IS_PGXC_COORDINATOR && IsConnFromCoord())
				{
					switch (type)
					{
						case 's':
							{
								execute_sync_spm_plan(&input_message);
							}
							break;
						case 'd':
							{
								execute_sync_spm_plan_delete(&input_message);
							}
							break;
						case 'u':
							{
								execute_sync_spm_plan_update(&input_message);
							}
							break;
						case 'a':
							{
								AnalyzeRelStats 	analyzeStat;
								execute_update_stat(&analyzeStat, &input_message);
							}
							break;
						default:
							elog(ERROR, "failed for h type: %c", type);
							break;
					}
				}
				break;
			}
			case 'z':
			{
				/* In pooler stateless resue mode reset connection params */
				const char *query_string = pq_getmsgstring(&input_message);

				pq_getmsgend(&input_message);

				/* Set statement_timestamp() */
				SetCurrentStatementStartTimestamp();

				is_internal_cmd = true;

				set_backend_to_resue(query_string);

				send_ready_for_query = true;
				is_internal_cmd = false;
				break;
			}
			case 'v':				/* Set connected by proxy */
			{
				int flag = 0;

				Assert(input_message.len == 4);

				flag = pq_getmsgint(&input_message, 4);
				pq_getmsgend(&input_message);

				set_flag_from_proxy(flag, username);
			}
			break;
			default:
				ereport(FATAL,
						(errcode(ERRCODE_PROTOCOL_VIOLATION),
						 errmsg("invalid frontend message type %d",
								firstchar)));
		}

#ifdef XCP
		/*
		 * If the connection is going idle, release the cluster lock. However
		 * if the session had invoked a PAUSE CLUSTER earlier, then wait for a
		 * subsequent UNPAUSE to release this lock
		 */
		if (IsUnderPostmaster && IS_PGXC_COORDINATOR && !IsAbortedTransactionBlockState()
			&& !IsTransactionOrTransactionBlock()
			&& cluster_lock_held && !cluster_ex_lock_held)
		{
			bool exclusive = false;
			ReleaseClusterLock(exclusive);
			cluster_lock_held = false;
		}
#endif /* XCP */
#ifdef _PG_ORCL_
		/* save last tx end time */
		LastTxEndTime = GetCurrentTimestamp();
#endif
	}							/* end of input-reading loop */
}

/* Job worker Process, execute procedure */
void execute_simple_query(const char* query_string)
{
    exec_simple_query(query_string);
}

/*
 * Throw an error if we're a WAL sender process.
 *
 * This is used to forbid anything else than simple query protocol messages
 * in a WAL sender process.  'firstchar' specifies what kind of a forbidden
 * message was received, and is used to construct the error message.
 */
static void
forbidden_in_wal_sender(char firstchar)
{
	if (am_walsender)
	{
		if (firstchar == 'F')
			ereport(ERROR,
					(errcode(ERRCODE_PROTOCOL_VIOLATION),
					 errmsg("fastpath function calls not supported in a replication connection")));
		else
			ereport(ERROR,
					(errcode(ERRCODE_PROTOCOL_VIOLATION),
					 errmsg("extended query protocol not supported in a replication connection")));
	}
}


/*
 * Obtain platform stack depth limit (in bytes)
 *
 * Return -1 if unknown
 */
long
get_stack_depth_rlimit(void)
{
#if defined(HAVE_GETRLIMIT) && defined(RLIMIT_STACK)
	static long val = 0;

	/* This won't change after process launch, so check just once */
	if (val == 0)
	{
		struct rlimit rlim;

		if (getrlimit(RLIMIT_STACK, &rlim) < 0)
			val = -1;
		else if (rlim.rlim_cur == RLIM_INFINITY)
			val = LONG_MAX;
		/* rlim_cur is probably of an unsigned type, so check for overflow */
		else if (rlim.rlim_cur >= LONG_MAX)
			val = LONG_MAX;
		else
			val = rlim.rlim_cur;
	}
	return val;
#else							/* no getrlimit */
#if defined(WIN32) || defined(__CYGWIN__)
	/* On Windows we set the backend stack size in src/backend/Makefile */
	return WIN32_STACK_RLIMIT;
#else							/* not windows ... give up */
	return -1;
#endif
#endif
}


static struct rusage Save_r;
static struct timeval Save_t;

void
ResetUsageCommon(struct rusage *save_r, struct timeval *save_t)
{
	getrusage(RUSAGE_SELF, save_r);
	gettimeofday(save_t, NULL);
}

void
ResetUsage(void)
{
	ResetUsageCommon(&Save_r, &Save_t);
}

void
ShowUsageCommon(const char *title, struct rusage *save_r, struct timeval *save_t)
{
	StringInfoData str;
	struct timeval user,
				sys;
	struct timeval elapse_t;
	struct rusage r;

	getrusage(RUSAGE_SELF, &r);
	gettimeofday(&elapse_t, NULL);
	memcpy((char *) &user, (char *) &r.ru_utime, sizeof(user));
	memcpy((char *) &sys, (char *) &r.ru_stime, sizeof(sys));
	if (elapse_t.tv_usec < save_t->tv_usec)
	{
		elapse_t.tv_sec--;
		elapse_t.tv_usec += 1000000;
	}
	if (r.ru_utime.tv_usec < save_r->ru_utime.tv_usec)
	{
		r.ru_utime.tv_sec--;
		r.ru_utime.tv_usec += 1000000;
	}
	if (r.ru_stime.tv_usec < save_r->ru_stime.tv_usec)
	{
		r.ru_stime.tv_sec--;
		r.ru_stime.tv_usec += 1000000;
	}

	/*
	 * the only stats we don't show here are for memory usage -- i can't
	 * figure out how to interpret the relevant fields in the rusage struct,
	 * and they change names across o/s platforms, anyway. if you can figure
	 * out what the entries mean, you can somehow extract resident set size,
	 * shared text size, and unshared data and stack sizes.
	 */
	initStringInfo(&str);

	appendStringInfoString(&str, "! system usage stats:\n");
	appendStringInfo(&str,
					 "!\t%ld.%06ld s user, %ld.%06ld s system, %ld.%06ld s elapsed\n",
					 (long) (r.ru_utime.tv_sec - save_r->ru_utime.tv_sec),
					 (long) (r.ru_utime.tv_usec - save_r->ru_utime.tv_usec),
					 (long) (r.ru_stime.tv_sec - save_r->ru_stime.tv_sec),
					 (long) (r.ru_stime.tv_usec - save_r->ru_stime.tv_usec),
					 (long) (elapse_t.tv_sec - save_t->tv_sec),
					 (long) (elapse_t.tv_usec - save_t->tv_usec));
	appendStringInfo(&str,
					 "!\t[%ld.%06ld s user, %ld.%06ld s system total]\n",
					 (long) user.tv_sec,
					 (long) user.tv_usec,
					 (long) sys.tv_sec,
					 (long) sys.tv_usec);
#if defined(HAVE_GETRUSAGE)
	appendStringInfo(&str,
					 "!\t%ld/%ld [%ld/%ld] filesystem blocks in/out\n",
					 r.ru_inblock - save_r->ru_inblock,
	/* they only drink coffee at dec */
					 r.ru_oublock - save_r->ru_oublock,
					 r.ru_inblock, r.ru_oublock);
	appendStringInfo(&str,
					 "!\t%ld/%ld [%ld/%ld] page faults/reclaims, %ld [%ld] swaps\n",
					 r.ru_majflt - save_r->ru_majflt,
					 r.ru_minflt - save_r->ru_minflt,
					 r.ru_majflt, r.ru_minflt,
					 r.ru_nswap - save_r->ru_nswap,
					 r.ru_nswap);
	appendStringInfo(&str,
					 "!\t%ld [%ld] signals rcvd, %ld/%ld [%ld/%ld] messages rcvd/sent\n",
					 r.ru_nsignals - save_r->ru_nsignals,
					 r.ru_nsignals,
					 r.ru_msgrcv - save_r->ru_msgrcv,
					 r.ru_msgsnd - save_r->ru_msgsnd,
					 r.ru_msgrcv, r.ru_msgsnd);
	appendStringInfo(&str,
					 "!\t%ld/%ld [%ld/%ld] voluntary/involuntary context switches\n",
					 r.ru_nvcsw - save_r->ru_nvcsw,
					 r.ru_nivcsw - save_r->ru_nivcsw,
					 r.ru_nvcsw, r.ru_nivcsw);
#endif							/* HAVE_GETRUSAGE */

	/* remove trailing newline */
	if (str.data[str.len - 1] == '\n')
		str.data[--str.len] = '\0';

	ereport(LOG,
			(errmsg_internal("%s", title),
			 errdetail_internal("%s", str.data)));

	pfree(str.data);
}

void
ShowUsage(const char *title)
{
	ShowUsageCommon(title, &Save_r, &Save_t);
}

/*
 * on_proc_exit handler to log end of session
 */
static void
log_disconnections(int code, Datum arg)
{
	Port	   *port = MyProcPort;
	long		secs;
	int			usecs;
	int			msecs;
	int			hours,
				minutes,
				seconds;

	TimestampDifference(port->SessionStartTime,
						GetCurrentTimestamp(),
						&secs, &usecs);
	msecs = usecs / 1000;

	hours = secs / SECS_PER_HOUR;
	secs %= SECS_PER_HOUR;
	minutes = secs / SECS_PER_MINUTE;
	seconds = secs % SECS_PER_MINUTE;

	ereport(LOG,
			(errmsg("disconnection: session time: %d:%02d:%02d.%03d "
					"user=%s database=%s host=%s%s%s",
					hours, minutes, seconds, msecs,
					port->user_name, port->database_name, port->remote_host,
					port->remote_port[0] ? " port=" : "", port->remote_port)));
}
#ifdef __OPENTENBASE__
static void
replace_null_with_blank(char *src, int length)
{
	int i = 0;

	for (i = 0; i < length; i++)
	{
		if (src[i] == '\0')
		{
			src[i] = ' ';
		}
	}
}
static bool
NeedResourceOwner(const char *stmt_name)
{
	if (!stmt_name)
	{
		return false;
	}

	if ((strncmp(INSERT_TRIGGER, stmt_name, strlen(INSERT_TRIGGER)) == 0 ||
		strncmp(UPDATE_TRIGGER, stmt_name, strlen(UPDATE_TRIGGER)) == 0 ||
		strncmp(DELETE_TRIGGER, stmt_name, strlen(DELETE_TRIGGER)) == 0))
	{
		return true;
	}


	return false;
}

bool
IsStandbyPostgres(void)
{
	return (IsNormalPostgres && false == IsPGXCMainPlane);
}

bool
IsExtendedQuery(void)
{
	return doing_extended_query_message;
}

void
execute_update_stat(AnalyzeRelStats *analyzeStats, StringInfoData *input_message)
{
	char	c;
	int		i, j, k;
	int		ret;
	bool	need_finish = false;

	DBUG_EXECUTE_IF("execute_update_stat_error", {
		ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR), errmsg("hit stub on execute_update_stat")));
	});

	analyzeStats->relname = (char*)pq_getmsgstring(input_message);
	analyzeStats->namespacename = (char*)pq_getmsgstring(input_message);
	analyzeStats->class_info_num = pq_getmsgint(input_message, 4);
	analyzeStats->relallvisible = pq_getmsgint(input_message, 4);
	
	c = pq_getmsgbyte(input_message);
	if (c == 'Y')
	{
		analyzeStats->relhasindex = true;
	}
	else if (c == 'N')
	{
		analyzeStats->relhasindex = false;
	}
	else
	{
		ereport(ERROR,
					(errcode(ERRCODE_PROTOCOL_VIOLATION),
						errmsg("invalid analyze stats info of relhasindex: %c", c)));
	}

	c = pq_getmsgbyte(input_message);
	if (c == 'Y')
	{
		analyzeStats->inh = true;
	}
	else if (c == 'N')
	{
		analyzeStats->inh = false;
	}
	else
	{
		ereport(ERROR,
					(errcode(ERRCODE_PROTOCOL_VIOLATION),
						errmsg("invalid analyze stats info of inh: %c", c)));
	}

	c = pq_getmsgbyte(input_message);
	if (c == 'Y')
	{
		analyzeStats->resetcounter = true;
	}
	else if (c == 'N')
	{
		analyzeStats->resetcounter = false;
	}
	else
	{
		ereport(ERROR,
					(errcode(ERRCODE_PROTOCOL_VIOLATION),
						errmsg("invalid analyze stats info of resetcounter: %c", c)));
	}

	analyzeStats->deadrows = pq_getmsgfloat4(input_message);

	if (analyzeStats->class_info_num > 0)
	{
		analyzeStats->relpages = (int32*)palloc0(sizeof(int32) * analyzeStats->class_info_num);
		analyzeStats->reltuples = (float4*)palloc0(sizeof(float4) * analyzeStats->class_info_num);
		analyzeStats->stat_att_num = (int32*)palloc0(sizeof(int32) * analyzeStats->class_info_num);
	}
	if (analyzeStats->class_info_num > 1)
	{
		analyzeStats->indexName = (NameData*)palloc0(sizeof(NameData) * (analyzeStats->class_info_num - 1));
	}

	for (i = 0; i < analyzeStats->class_info_num; i++)
	{
		analyzeStats->relpages[i] = pq_getmsgint(input_message, 4);
	}

	for (i = 0; i < analyzeStats->class_info_num; i++)
	{
		analyzeStats->reltuples[i] = pq_getmsgfloat4(input_message);
	}

	for (i = 0; i < analyzeStats->class_info_num - 1; i++)
	{
		memcpy(analyzeStats->indexName[i].data, pq_getmsgbytes(input_message, NAMEDATALEN), NAMEDATALEN);
	}

	analyzeStats->stat_info_num = pq_getmsgint(input_message, 4);
	for (i = 0; i < analyzeStats->class_info_num; i++)
	{
		analyzeStats->stat_att_num[i] = pq_getmsgint(input_message, 4);
	}

	if (analyzeStats->stat_info_num > 0)
	{
		analyzeStats->pgstat = (AnalyzePgStats*)palloc0(sizeof(AnalyzePgStats) * analyzeStats->stat_info_num);
	}
	for(i = 0; i < analyzeStats->stat_info_num; i++)
	{
		analyzeStats->pgstat[i].attnum = pq_getmsgint(input_message, 4);
		analyzeStats->pgstat[i].stanullfrac = pq_getmsgfloat4(input_message);
		analyzeStats->pgstat[i].stawidth = pq_getmsgint(input_message, 4);
		analyzeStats->pgstat[i].stadistinct = pq_getmsgfloat4(input_message);
		for (j = 0; j < STATISTIC_NUM_SLOTS; j++)
		{
			analyzeStats->pgstat[i].stakind[j] = pq_getmsgint(input_message, 2);
		}

		for (j = 0; j < STATISTIC_NUM_SLOTS; j++)
		{
			analyzeStats->pgstat[i].staop[j] = pq_getmsgint(input_message, 4);
		}

		for (j = 0; j < STATISTIC_NUM_SLOTS; j++)
		{
			analyzeStats->pgstat[i].numnumbers[j] = pq_getmsgint(input_message, 4);
		}

		for (j = 0; j < STATISTIC_NUM_SLOTS; j++)
		{
			analyzeStats->pgstat[i].numvalues[j] = pq_getmsgint(input_message, 4);
		}

		for (j = 0; j < STATISTIC_NUM_SLOTS; j++)
		{
			analyzeStats->pgstat[i].statypid[j] = pq_getmsgint(input_message, 4);
		}

		for (j = 0; j < STATISTIC_NUM_SLOTS; j++)
		{
			analyzeStats->pgstat[i].statyplen[j] = pq_getmsgint(input_message, 2);
		}

		for (j = 0; j < STATISTIC_NUM_SLOTS; j++)
		{
			c = pq_getmsgbyte(input_message);
			if (c == 'Y')
			{
				analyzeStats->pgstat[i].statypbyval[j] = true;
			}
			else if (c == 'N')
			{
				analyzeStats->pgstat[i].statypbyval[j] = false;
			}
			else
			{
				ereport(ERROR,
							(errcode(ERRCODE_PROTOCOL_VIOLATION),
								errmsg("invalid analyze stats info of statypbyval: %c", c)));
			}
		}

		for (j = 0; j < STATISTIC_NUM_SLOTS; j++)
		{
			c = pq_getmsgbyte(input_message);
			analyzeStats->pgstat[i].statypalign[j] = c;
		}

		for (j = 0 ; j < STATISTIC_NUM_SLOTS; j++)
		{
			if (analyzeStats->pgstat[i].numnumbers[j] > 0)
			{
				analyzeStats->pgstat[i].stanumbers[j] =
										(float4*)palloc0(analyzeStats->pgstat[i].numnumbers[j] * sizeof(float4));
				for (k = 0; k < analyzeStats->pgstat[i].numnumbers[j]; k++)
				{
					analyzeStats->pgstat[i].stanumbers[j][k] = pq_getmsgfloat4(input_message);
				}
			}
		}
		for (j = 0 ; j < STATISTIC_NUM_SLOTS; j++)
		{
			if (analyzeStats->pgstat[i].numvalues[j] > 0)
			{
				analyzeStats->pgstat[i].stavalues[j] = (Datum*)palloc0(analyzeStats->pgstat[i].numvalues[j] * sizeof(Datum));
				analyzeStats->pgstat[i].stavalueslen[j] = (int32*)palloc0(analyzeStats->pgstat[i].numvalues[j] * sizeof(int32));

				for (k = 0; k < analyzeStats->pgstat[i].numvalues[j]; k++)
				{
					analyzeStats->pgstat[i].stavalueslen[j][k] = pq_getmsgint(input_message, 4);
				}
				for (k = 0; k < analyzeStats->pgstat[i].numvalues[j]; k++)
				{
					if (analyzeStats->pgstat[i].statypbyval[j])
					{
						analyzeStats->pgstat[i].stavalues[j][k] =
							fetch_att((char*)pq_getmsgbytes(input_message, analyzeStats->pgstat[i].stavalueslen[j][k]),
										true, analyzeStats->pgstat[i].statyplen[j]);
					}
					else
					{
						analyzeStats->pgstat[i].stavalues[j][k] =
							PointerGetDatum((char*)pq_getmsgbytes(input_message,
											analyzeStats->pgstat[i].stavalueslen[j][k]));
					}
				}
			}
		}
	}
	analyzeStats->ext_info_num = pq_getmsgint(input_message, 4);
	if (analyzeStats->ext_info_num > 0)
	{
		analyzeStats->pgstatext = (AnalyzeExtStats*)palloc0(sizeof(AnalyzeExtStats) * analyzeStats->ext_info_num);
	}
	for (i = 0; i < analyzeStats->ext_info_num; i++)
	{
		analyzeStats->pgstatext[i].statname = (char*)pq_getmsgstring(input_message);

		analyzeStats->pgstatext[i].distinct_len = pq_getmsgint(input_message, 4);
		analyzeStats->pgstatext[i].distinct = 
			(char*)pq_getmsgbytes(input_message, analyzeStats->pgstatext[i].distinct_len);
		analyzeStats->pgstatext[i].depend_len = pq_getmsgint(input_message, 4);
		analyzeStats->pgstatext[i].depend = 
			(char*)pq_getmsgbytes(input_message, analyzeStats->pgstatext[i].depend_len);
	}

	if (IsTransactionIdle())
	{
		start_xact_command();
		need_finish = true;
	}
	else if (!IsTransactionState() && !IsSubTransaction())
	{
		elog(WARNING, "unexpected transaction stat %s blockState %s",
			 CurrentTransactionTransStateAsString(),
			 CurrentTransactionBlockStateAsString());
	}
	
	PushActiveSnapshot(GetTransactionSnapshot());
	ret = update_analyze_stat(analyzeStats);
	PopActiveSnapshot();
	if (need_finish)
		finish_xact_command();

	if (ret == -1)
	{
		pq_putmessage('e', NULL, 0);
		pq_flush();
	}
	else
	{
		pq_putmessage('a', NULL, 0);
		pq_flush();
	}
}

static void
execute_sync_spm_plan_delete(StringInfoData *input_message)
{
	int64 sql_id;
	int64 plan_id;

	MemoryContext old = NULL;

	if (unlikely(!IsTransactionState()))
	{
		return;
	}

	old = MemoryContextSwitchTo(spm_context);

	PushActiveSnapshot(GetTransactionSnapshot());

	sql_id = pq_getmsgint64(input_message);
	plan_id = pq_getmsgint64(input_message);

	(void)DropSPMPlan(SPMPlanRelationId, SPMPlanSqlidPlanidIndexId, sql_id, plan_id);

	PopActiveSnapshot();

	MemoryContextSwitchTo(old);
	pq_putmessage('a', NULL, 0);
	pq_flush();
}

static void
execute_sync_spm_plan_update(StringInfoData *input_message)
{
	int64 sql_id;
	int64 plan_id;
	char *attrname;
	char *attvalue;
	MemoryContext old = NULL;

	if (unlikely(!IsTransactionState()))
	{
		return;
	}

	old = MemoryContextSwitchTo(spm_context);

	PushActiveSnapshot(GetTransactionSnapshot());

	sql_id = pq_getmsgint64(input_message);
	plan_id = pq_getmsgint64(input_message);
	attrname = (char *) pq_getmsgstring(input_message);
	attvalue = (char *) pq_getmsgstring(input_message);

	(void)AlterSPMPlan(sql_id, plan_id, attrname, attvalue);

	PopActiveSnapshot();

	MemoryContextSwitchTo(old);
	pq_putmessage('a', NULL, 0);
	pq_flush();
}

static bool
get_reloid_with_fullname(const char *fullname, Oid *reloid)
{
	regex_t		regex;
	regmatch_t	matches[3];
	int			r;
	pg_wchar	*wregstr;
	int			wreglen;
	pg_wchar	*wmatchstr;
	int			wmatchlen;
	Oid		namespaceId;
	char	*relname;
	char 	*schema;

	const char	*pattern = "\"([^\"]*)\"\\.\"([^\"]*)\"";
	int			pattern_len = strlen(pattern);
	int			fullname_len = strlen(fullname);

	wregstr = (pg_wchar *) palloc0((pattern_len + 1) * sizeof(pg_wchar));
	wreglen = pg_mb2wchar_with_len(pattern, wregstr, pattern_len);
	r = pg_regcomp(&regex, wregstr, wreglen, REG_EXTENDED, C_COLLATION_OID);
	if (r)
	{
		char		errstr[100];

		pg_regerror(r, &regex, errstr, sizeof(errstr));
		ereport(LOG,
				(errcode(ERRCODE_INVALID_REGULAR_EXPRESSION),
					errmsg("invalid regular expression \"%s\": %s",
						pattern, errstr)));
		pfree(wregstr);

		return false;
	}

	pfree(wregstr);
	wmatchstr = (pg_wchar *) palloc0((fullname_len + 1) * sizeof(pg_wchar));
	wmatchlen = pg_mb2wchar_with_len(fullname, wmatchstr, fullname_len);
	r = pg_regexec(&regex, wmatchstr, wmatchlen, 0, NULL, 3, matches, 0);
	if (r)
	{
		char		errstr[100];

		if (r != REG_NOMATCH)
		{
			/* REG_NOMATCH is not an error, everything else is */
			pg_regerror(r, &regex, errstr, sizeof(errstr));
			ereport(LOG,
					(errcode(ERRCODE_INVALID_REGULAR_EXPRESSION),
						errmsg("regular expression match for \"%s\" failed: %s",
							fullname, errstr)));
		}

		pfree(wmatchstr);
		pg_regfree(&regex);
		return false;
	}

	pfree(wmatchstr);
	pg_regfree(&regex);

	schema = palloc0(matches[1].rm_eo - matches[1].rm_so + 1);
	StrNCpy(schema, fullname + matches[1].rm_so, matches[1].rm_eo - matches[1].rm_so + 1);
	relname = palloc0(matches[2].rm_eo - matches[2].rm_so + 1);
	StrNCpy(relname, fullname + matches[2].rm_so, matches[2].rm_eo - matches[2].rm_so + 1);

	namespaceId = LookupExplicitNamespace(schema, true);
	if (OidIsValid(namespaceId))
	{
		*reloid = get_relname_relid(relname, namespaceId);
		pfree(relname);
		pfree(schema);
		return true;
	}

	pfree(relname);
	pfree(schema);
	return false;
}

static void
execute_sync_spm_plan(StringInfoData *input_message)
{
	char	c;
	int		i;
	int     len = 0;
	Oid		*reloids;
	Oid		*indexoids;
	List 	*relnms = NIL;
	List 	*idxnms = NIL;
	int16	nrels;
	int16	nindexes;
	MemoryContext old = NULL;
	LocalSPMPlanInfo *spmplan = NULL;

	if (unlikely(!IsTransactionState()))
	{
		return;
	}

	spmplan = (LocalSPMPlanInfo *) palloc0(sizeof(LocalSPMPlanInfo));
	InitSPMPlanInfo(spmplan, CurrentMemoryContext);

	old = MemoryContextSwitchTo(spm_context);

	PushActiveSnapshot(GetTransactionSnapshot());

	memcpy(spmplan->spmname.data, pq_getmsgbytes(input_message, NAMEDATALEN), NAMEDATALEN);
	spmplan->spmnsp = pq_getmsgint(input_message, sizeof(Oid));
	spmplan->ownerid = pq_getmsgint(input_message, sizeof(Oid));
	spmplan->sql_id = pq_getmsgint64(input_message);
	spmplan->plan_id = pq_getmsgint64(input_message);
	spmplan->hint_id = pq_getmsgint64(input_message);

	len = pq_getmsgint(input_message, sizeof(int32));
	EnlargeSPMPlanInfoMember(&spmplan->hint, &spmplan->hint_len, len);
	memcpy(spmplan->hint, pq_getmsgbytes(input_message, len), len);
	spmplan->hint_len = len;

	len = pq_getmsgint(input_message, sizeof(int32));
	EnlargeSPMPlanInfoMember(&spmplan->hint_detail, &spmplan->hint_detail_len, len);
	memcpy(spmplan->hint_detail, pq_getmsgbytes(input_message, len), len);
	spmplan->hint_detail_len = len;

	len = pq_getmsgint(input_message, sizeof(int32));
	EnlargeSPMPlanInfoMember(&spmplan->guc_list, &spmplan->guc_list_len, len);
	memcpy(spmplan->guc_list, pq_getmsgbytes(input_message, len), len);
	spmplan->guc_list_len = len;

	spmplan->priority = pq_getmsgint(input_message, sizeof(int32));
	c = pq_getmsgbyte(input_message);
	if (c == 'Y')
	{
		spmplan->enabled = true;
	}
	else if (c == 'N')
	{
		spmplan->enabled = false;
	}
	else
	{
		ereport(ERROR,
					(errcode(ERRCODE_PROTOCOL_VIOLATION),
						errmsg("invalid spm plan info of enabled: %c", c)));
	}
	c = pq_getmsgbyte(input_message);
	if (c == 'Y')
	{
		spmplan->autopurge = true;
	}
	else if (c == 'N')
	{
		spmplan->autopurge = false;
	}
	else
	{
		ereport(ERROR,
					(errcode(ERRCODE_PROTOCOL_VIOLATION),
						errmsg("invalid spm plan info of autopurge: %c", c)));

	}
	spmplan->executions = pq_getmsgint64(input_message);
	spmplan->created = pq_getmsgint64(input_message);
	spmplan->last_modified = pq_getmsgint64(input_message);
	spmplan->last_verified = pq_getmsgint64(input_message);
	spmplan->execute_time = pq_getmsgint64(input_message);
	spmplan->version = pq_getmsgint64(input_message);
	spmplan->elapsed_time = pq_getmsgint64(input_message);
	spmplan->task_id = pq_getmsgint64(input_message);
	spmplan->sql_normalize_rule = pq_getmsgint(input_message, sizeof(int32));
	spmplan->cost = pq_getmsgfloat4(input_message);
	nrels = pq_getmsgint(input_message, sizeof(int16));
	nindexes = pq_getmsgint(input_message, sizeof(int16));

	len = pq_getmsgint(input_message, sizeof(int32));
	EnlargeSPMPlanInfoMember(&spmplan->sql_normalize, &spmplan->sql_normalize_len, len);
	memcpy(spmplan->sql_normalize, pq_getmsgbytes(input_message, len), len);
	spmplan->sql_normalize_len = len;

	len = pq_getmsgint(input_message, sizeof(int32));
	EnlargeSPMPlanInfoMember(&spmplan->sql, &spmplan->sql_len, len);
	memcpy(spmplan->sql, pq_getmsgbytes(input_message, len), len);
	spmplan->sql_len = len;

	len = pq_getmsgint(input_message, sizeof(int32));
	EnlargeSPMPlanInfoMember(&spmplan->plan, &spmplan->plan_len, len);
	memcpy(spmplan->plan, pq_getmsgbytes(input_message, len), len);
	spmplan->plan_len = len;

	len = pq_getmsgint(input_message, sizeof(int32));
	EnlargeSPMPlanInfoMember(&spmplan->param_list, &spmplan->param_list_len, len);
	memcpy(spmplan->param_list, pq_getmsgbytes(input_message, len), len);
	spmplan->param_list_len = len;

	if (nrels > 0)
	{
		reloids = palloc0(nrels * sizeof(Oid));
		for (i = 0; i < nrels; i++)
		{
			const char *fullname = pq_getmsgstring(input_message);
			relnms = lcons((void *) cstring_to_text(fullname), relnms);
			if (!get_reloid_with_fullname(fullname, &(reloids[nrels - 1 - i])))
			{
				PopActiveSnapshot();

				MemoryContextSwitchTo(old);
				pq_putmessage('e', NULL, 0);
				pq_flush();

				pfree(reloids);
				pfree(spmplan);
				return;
			}
		}
		spmplan->relnames = DatumGetArrayTypeP(TextListToArray(relnms));
		spmplan->reloids = buildoidvector(reloids, nrels);
		pfree(reloids);
	}
	else
		spmplan->relnames = NULL;

	if (nindexes > 0)
	{
		indexoids = palloc0(nindexes * sizeof(Oid));
		for (i = 0; i < nindexes; i++)
		{
			const char *fullidxname = pq_getmsgstring(input_message);
			idxnms = lcons((void *) cstring_to_text(fullidxname), relnms);
			if (!get_reloid_with_fullname(fullidxname, &(indexoids[nindexes - 1 - i])))
			{
				PopActiveSnapshot();

				MemoryContextSwitchTo(old);
				pq_putmessage('e', NULL, 0);
				pq_flush();

				pfree(indexoids);
				pfree(spmplan);
				return;
			}
		}
		spmplan->indexnames = DatumGetArrayTypeP(TextListToArray(idxnms));
		spmplan->indexoids = buildoidvector(indexoids, nindexes);
		pfree(indexoids);
	}
	else
		spmplan->indexnames = NULL;

	len = pq_getmsgint(input_message, sizeof(int32));
	EnlargeSPMPlanInfoMember(&spmplan->description, &spmplan->description_len, len);
	memcpy(spmplan->description, pq_getmsgbytes(input_message, len), len);
	spmplan->description_len = len;

	memcpy(spmplan->origin.data, pq_getmsgbytes(input_message, NAMEDATALEN), NAMEDATALEN);

	SyncSPMPlan(spmplan);

	PopActiveSnapshot();

	MemoryContextSwitchTo(old);
	pq_putmessage('a', NULL, 0);
	pq_flush();

	pfree(spmplan);
}


/*
 * Execute the archiving of memory usage statistics.
 *
 * All the actual work is deferred to ProcessMemTrackAchieveInterrupt(),
 * because we cannot safely emit a log message inside the signal handler.
 */
void
HandleMemTrackInfoAchieve(void)
{
	if (!track_memory_inited)
		return;

	InterruptPending = true;
	AchieveMemTrackInfoPending = true;

	if (DoingCommandRead)
		ProcessMemTrackAchieveInterrupt();
	/* latch will be set by procsignal_sigusr1_handler */
}

/*
 * Set the username and pgoptions of the backend
 * so that it can be reused
 */
void
set_backend_to_resue(const char *query_string)
{
	char *query         = NULL;
	char *user_name     = NULL;
	char *pgoptions     = NULL;
	char *sql_strtok_r  = NULL;
	char *session_reset = "SET SESSION AUTHORIZATION DEFAULT;";

	/* Only pooler stateless reuse mode in the cluster uses 'z' packets. Other paths are invalid. */
	if (!((IsConnFromCoord() || IsConnFromDatanode()) && PoolerStatelessReuse))
	{
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("Invalid packet path, remoteConnType[%d], remote_host[%s], remote_port[%s].",
							   remoteConnType, MyProcPort->remote_host, MyProcPort->remote_port)));
	}

	/* reset session first */
	exec_simple_query(session_reset);

	/* get user name and pgoptions */
	query = pstrdup(query_string);
	user_name = strtok_r(query, "@", &sql_strtok_r);
	pgoptions = sql_strtok_r;

	/* reset user name and pgoptions */
	ResetUserAndPgOptions(user_name, pgoptions);
}

#endif

/*
 * OPENTENBASE_ORA function: DBMS_UTILITY.GET_TIME
 * return  the number of hundredths of a second
 * from a time epoch.
 *
 * NB: The time epoch is a copy of SessionStartTime.
 */
int32
DbmsUtilityGetTime(void)
{
	return (int32) ((GetCurrentTimestamp() - SessionStartTime) / 10000);
}



/*
 * Get a dn connection on proxy
 */
PGXCNodeHandle *
get_handle_on_proxy(void)
{
	PGXCNodeHandle *conn = NULL;
	char node_type = PGXC_NODE_DATANODE;
	Oid node_oid = InvalidOid;
	int node_id = -1;
	int flag = 0;
	PGXCNodeAllHandles *handles = NULL;
	List *dnList = NIL;
	int ret = 0;

	Assert(IS_PGXC_COORDINATOR);

	/* Get dn oid */
	StartTransactionCommand();
	InitMultinodeExecutor(false);
	node_oid = get_pgxc_nodeoid(proxy_for_dn);
	CommitTransactionCommand();

	if (node_oid == InvalidOid)
	{
		ereport(FATAL,
			(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				errmsg("Unknow dn: %s, oid is invalid", proxy_for_dn)));
	}

	/* Get dn id */
	node_id = PGXCNodeGetNodeId(node_oid, &node_type);
	if (node_id == -1)
	{
		ereport(FATAL,
			(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				errmsg("Unknow dn: %s, oid: %d, id: -1", proxy_for_dn, node_oid)));
	}

	elog(LOG, "Proxy for dn %s, node oid %d, node id %d",
		proxy_for_dn, node_oid, node_id);

	/* Get dn connection */
	dnList = lappend_int(dnList, node_id);
	Assert(list_length(dnList) == 1);
	handles = get_handles(dnList, NIL, false, false, FIRST_LEVEL, InvalidFid, true, false);
	if (handles == NULL)
	{
		ereport(ERROR,
			(errcode(ERRCODE_INTERNAL_ERROR),
				errmsg("Get connections failed for %s", proxy_for_dn)));

	}
	if (handles->dn_conn_count == 0)
	{
		ereport(ERROR,
			(errcode(ERRCODE_INTERNAL_ERROR),
				errmsg("Get 0 connection for %s", proxy_for_dn)));
	}

	Assert(handles->co_conn_count == 0);
	Assert(handles->dn_conn_count == 1);

	conn = handles->datanode_handles[0];
	Assert(conn != NULL);

	pfree_pgxc_all_handles(handles);
	handles = NULL;

	/* Set dn process */
	if (am_walsender)
	{
		flag |= FLAG_AM_WALSENDER;
		if (am_db_walsender)
		{
			flag |= FLAG_AM_DB_WALSENDER;
		}
	}
	ret = pgxc_node_send_proxy_flag(conn, flag);
	if (ret != 0)
	{
		ereport(ERROR,
			(errcode(ERRCODE_INTERNAL_ERROR),
				errmsg("Proxy send flag to %s error: %d", proxy_for_dn, ret)));
	}

	return conn;
}

/*
 * Forward client request command to dn and receive response
 */
PGXCNodeHandle *
handle_request_msg_on_proxy(PGXCNodeHandle *conn, int firstchar, StringInfo input_msg)
{
	int ret = 0;

	Assert(IS_PGXC_COORDINATOR);

	if (conn == NULL)
	{
		conn = get_handle_on_proxy();
	}

	Assert(conn != NULL);

	/* Before query, replicate stream is not closed, set stream_closed to false */
	conn->stream_closed = false;

	if (firstchar == 'Q')
	{
		const char *query_string = pq_getmsgstring(input_msg);
		pq_getmsgend(input_msg);
		debug_query_string = query_string;
	}

	elog(DEBUG1, "Proxy: firstchar is %c(%d)", firstchar, firstchar);

	/* Send message */
	ret = pgxc_node_send_on_proxy(conn, firstchar, input_msg);
	if (ret != 0)
	{
		ereport(ERROR,
			(errcode(ERRCODE_INTERNAL_ERROR),
				errmsg("Proxy send request to %s error: %d", proxy_for_dn, ret)));
	}

	switch (firstchar)
	{
		/*
		 * 'X' means that the frontend is closing down the socket. EOF
		 * means unexpected loss of frontend connection. Either way,
		 * perform normal shutdown.
		 */
		case 'X':
		case EOF:
			/*
			 * Reset whereToSendOutput to prevent ereport from attempting
			 * to send any more messages to client.
			 */
			if (whereToSendOutput == DestRemote)
			{
				elog(LOG, "Set whereToSendOutput from %d to %d",
					whereToSendOutput, DestNone);
				whereToSendOutput = DestNone;
			}

			/* Destroy the dn connection on proxy */
			PoolManagerDisconnect();

			/*
			 * NOTE: if you are tempted to add more code here, DON'T!
			 * Whatever you had in mind to do should be set up as an
			 * on_proc_exit or on_shmem_exit callback, instead. Otherwise
			 * it will fail to be called during other backend-shutdown
			 * scenarios.
			 */
			proc_exit(0);

		default:
			break;
	}

	/* Receive message */
	ret = pgxc_node_receive_on_proxy(conn);
	if (ret != 0)
	{
		ereport(ERROR,
			(errcode(ERRCODE_INTERNAL_ERROR),
				errmsg("Proxy receive from %s error: %d", proxy_for_dn, ret)));
	}

	debug_query_string = NULL;

	return conn;
}

/*
 * Set flag from proxy
 */
void
set_flag_from_proxy(int flag, const char *username)
{
	if (am_conn_from_proxy)
	{
		ereport(ERROR,
			(errcode(ERRCODE_CONNECTION_EXCEPTION),
				errmsg("It is connected from proxy already")));
	}

	am_conn_from_proxy = true;

	elog(LOG, "It is connected from proxy");

	if (am_walsender)
	{
		ereport(ERROR,
			(errcode(ERRCODE_CONNECTION_EXCEPTION),
				errmsg("It is a wal sender already")));
	}

	if (flag & FLAG_AM_WALSENDER)
	{
		am_walsender = true;
		if (flag & FLAG_AM_DB_WALSENDER)
		{
			am_db_walsender = true;
		}
	}

	elog(LOG, "Set wal sender: am_walsender(%d), am_db_walsender(%d)",
		am_walsender, am_db_walsender);

	if (am_walsender)
	{
		int fixed_len = 0;
		const char *fixed = get_ps_display_fixed(&fixed_len);
		char fixed_buf[fixed_len + 1];
		char *display = NULL;

		if (fixed_len != 0)
		{
			Assert (fixed != NULL);

			snprintf(fixed_buf, fixed_len, "%s", fixed);
			fixed_buf[fixed_len] = '\0';

			display = strstr(fixed_buf, username);
			Assert (display != NULL);

			init_ps_display("wal sender used by proxy", display, "", "");
		}
		else
		{
			elog(WARNING, "Get ps display fixed length is 0");

			init_ps_display("wal sender used by proxy", "", "", "");
		}

		IsNormalPostgres = false;

		WalSndSignals();
		InitWalSender();
	}
}
