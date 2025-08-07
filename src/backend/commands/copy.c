/*-------------------------------------------------------------------------
 *
 * copy.c
 *		Implements the COPY utility command
 *
 * Portions Copyright (c) 2012-2014, TransLattice, Inc.
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/commands/copy.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <ctype.h>
#include <unistd.h>
#include <sys/stat.h>

#include "access/heapam.h"
#include "access/htup_details.h"
#include "access/sysattr.h"
#include "access/xact.h"
#include "access/xlog.h"
#include "catalog/pg_type.h"
#include "commands/copy.h"
#include "commands/copysreh.h"
#include "commands/defrem.h"
#include "commands/trigger.h"
#include "executor/execPartition.h"
#include "executor/executor.h"
#include "executor/tuptable.h"
#include "foreign/fdwapi.h"
#include "libpq/libpq.h"
#include "libpq/pqformat.h"
#include "mb/pg_wchar.h"
#include "miscadmin.h"
#include "optimizer/clauses.h"
#include "optimizer/planner.h"
#include "parser/parse_expr.h"
#include "parser/parse_param.h"
#include "parser/parse_coerce.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "optimizer/pgxcship.h"
#include "parser/parse_relation.h"
#include "port/pg_bswap.h"
#include "rewrite/rewriteHandler.h"
#include "storage/fd.h"
#include "tcop/tcopprot.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/portal.h"
#include "utils/rel.h"
#include "utils/rls.h"
#include "utils/snapmgr.h"
#ifdef XCP
#include "catalog/dependency.h"
#include "commands/sequence.h"
#include "pgstat.h"
#endif
#ifdef PGXC
#include "pgxc/pgxc.h"
#include "pgxc/execRemote.h"
#include "pgxc/locator.h"
#include "pgxc/remotecopy.h"
#include "nodes/nodes.h"
#include "pgxc/poolmgr.h"
#include "catalog/pgxc_node.h"
#endif
#ifdef __OPENTENBASE__
#include "utils/rel.h"
#include "utils/ruleutils.h"
#endif
#ifdef __STORAGE_SCALABLE__
#include "replication/logical_statistic.h"
#include "replication/slot.h"
#include "replication/walsender.h"
#include "replication/worker_internal.h"
#endif
#ifdef _MLS_
#include "utils/mls.h"
#include "utils/datamask.h"
#endif
#ifdef __OPENTENBASE_C__
#include "postmaster/bgwriter.h"
#include "replication/basebackup.h"
#include "access/relscan.h"
#include "storage/bufmgr.h"
#endif
#ifdef _PG_ORCL_
#include "opentenbase_ora/opentenbase_ora.h"
#include "catalog/heap.h"
#endif
#include "commands/vacuum.h"
#include "catalog/pg_trigger.h"
#include "catalog/pg_proc.h"

#define ISOCTAL(c) (((c) >= '0') && ((c) <= '7'))
#define OCTVALUE(c) ((c) - '0')

#define OID_FIXLEN (10)
#define ROWID_FIXLEN (20)

/* DestReceiver for COPY (query) TO */
typedef struct
{
	DestReceiver pub; /* publicly-known function pointers */
	CopyState cstate; /* CopyStateData for the command */
	uint64 processed; /* # of tuples processed */
} DR_copy;

/*
 * These macros centralize code used to process line_buf and raw_buf buffers.
 * They are macros because they often do continue/break control and to avoid
 * function call overhead in tight COPY loops.
 *
 * We must use "if (1)" because the usual "do {...} while(0)" wrapper would
 * prevent the continue/break processing from working.  We end the "if (1)"
 * with "else ((void) 0)" to ensure the "if" does not unintentionally match
 * any "else" in the calling code, and to avoid any compiler warnings about
 * empty statements.  See http://www.cit.gu.edu.au/~anthony/info/C/C.macros.
 */

/*
 * This keeps the character read at the top of the loop in the buffer
 * even if there is more than one read-ahead.
 */
#define IF_NEED_REFILL_AND_NOT_EOF_CONTINUE(extralen)             \
	if (1)                                                        \
	{                                                             \
		if (raw_buf_ptr + (extralen) >= copy_buf_len && !hit_eof) \
		{                                                         \
			raw_buf_ptr = prev_raw_ptr; /* undo fetch */          \
			need_data = true;                                     \
			continue;                                             \
		}                                                         \
	}                                                             \
	else                                                          \
		((void)0)

/* This consumes the remainder of the buffer and breaks */
#define IF_NEED_REFILL_AND_EOF_BREAK(extralen)                                  \
	if (1)                                                                      \
	{                                                                           \
		if (raw_buf_ptr + (extralen) >= copy_buf_len && hit_eof)                \
		{                                                                       \
			if (extralen)                                                       \
				raw_buf_ptr = copy_buf_len; /* consume the partial character */ \
			/* backslash just before EOF, treat as data char */                 \
			result = true;                                                      \
			break;                                                              \
		}                                                                       \
	}                                                                           \
	else                                                                        \
		((void)0)

/*
 * Transfer any approved data to line_buf; must do this to be sure
 * there is some room in raw_buf.
 */
#define REFILL_LINEBUF                                                      \
	if (1)                                                                  \
	{                                                                       \
		if (raw_buf_ptr > cstate->raw_buf_index)                            \
		{                                                                   \
			appendBinaryStringInfo(&cstate->line_buf,                       \
								   cstate->raw_buf + cstate->raw_buf_index, \
								   raw_buf_ptr - cstate->raw_buf_index);    \
			cstate->raw_buf_index = raw_buf_ptr;                            \
		}                                                                   \
	}                                                                       \
	else                                                                    \
		((void)0)

/* Undo any read-ahead and jump out of the block. */
#define NO_END_OF_COPY_GOTO             \
	if (1)                              \
	{                                   \
		raw_buf_ptr = prev_raw_ptr + 1; \
		goto not_end_of_copy;           \
	}                                   \
	else                                \
		((void)0)

#define EOL_MAX_LEN 10
#define INVALID_EOL_CHAR ".abcdefghijklmnopqrstuvwxyz0123456789"

static const char BinarySignature[11] = "PGCOPY\n\377\r\n\0";

/* non-export function prototypes */
static CopyState BeginCopy(ParseState *pstate, bool is_from, Relation rel,
						   RawStmt *raw_query, Oid queryRelId, List *attnamelist,
						   List *options,
#ifdef _SHARDING_
						   Bitmapset *shards,
#endif
							List *attinfolist, bool fixed_position
);
static void EndCopy(CopyState cstate);
static void ClosePipeToProgram(CopyState cstate);
static CopyState BeginCopyTo(ParseState *pstate, Relation rel, RawStmt *query,
							 Oid queryRelId, const char *filename, bool is_program,
							 List *attnamelist, List *options,
#ifdef _SHARDING_
							 Bitmapset *shards
#endif
);
static uint64 DoCopyTo(CopyState cstate);
static uint64 CopyTo(CopyState cstate);
static void CopyFromInsertBatch(CopyState cstate, EState *estate,
								CommandId mycid, int hi_options,
								ResultRelInfo *resultRelInfo, TupleTableSlot *myslot,
								BulkInsertState bistate,
								int nBufferedTuples, HeapTuple *bufferedTuples,
								int firstBufferedLineNo);
static bool CopyReadLine(CopyState cstate);
static bool CopyReadLineText(CopyState cstate);
#ifdef __OPENTENBASE__
static int CopyReadAttributesInternal(CopyState cstate);
#endif
static int CopyReadAttributesText(CopyState cstate);
static int CopyReadAttributesCSV(CopyState cstate);
static Datum CopyReadBinaryAttribute(CopyState cstate,
									 int column_no, FmgrInfo *flinfo,
									 Oid typioparam, int32 typmod,
									 bool *isnull);
static void CopyAttributeOutText(CopyState cstate, char *string);
static void CopyAttributeOutCSV(CopyState cstate, char *string,
								bool use_quote, bool single_attr);
static int   CopyReadAttributesFixedWith(CopyState cstate);
static List *CopyGetAttnums(TupleDesc tupDesc, Relation rel,
							List *attnamelist);

/* Low-level communications functions */
static void SendCopyBegin(CopyState cstate);
static void ReceiveCopyBegin(CopyState cstate);
static void SendCopyEnd(CopyState cstate);
static void CopySendData(CopyState cstate, const void *databuf, int datasize);
static void CopySendString(CopyState cstate, const char *str);
static void CopySendChar(CopyState cstate, char c);
static int CopyGetData(CopyState cstate, void *databuf,
					   int minread, int maxread);
static void CopySendInt32(CopyState cstate, int32 val);
static bool CopyGetInt32(CopyState cstate, int32 *val);
static void CopySendInt16(CopyState cstate, int16 val);
static bool CopyGetInt16(CopyState cstate, int16 *val);

#ifdef PGXC
static RemoteCopyOptions *GetRemoteCopyOptions(CopyState cstate);
static void append_defvals(CopyState cstate, Datum *values, bool *nulls);
#endif
static void transformFormatter(CopyState cstate, TupleDesc tupDesc,
                               Relation rel, List *attnamelist);

/*
 * Send copy start/stop messages for frontend copies.  These have changed
 * in past protocol redesigns.
 */
static void
SendCopyBegin(CopyState cstate)
{
	if (PG_PROTOCOL_MAJOR(FrontendProtocol) >= 3)
	{
		/* new way */
		StringInfoData buf;
		int natts = list_length(cstate->attnumlist);
		int16 format = (cstate->binary ? 1 : 0);
		int i;

		pq_beginmessage(&buf, 'H');
		pq_sendbyte(&buf, format);	/* overall format */
		pq_sendint16(&buf, natts);
		for (i = 0; i < natts; i++)
			pq_sendint16(&buf, format);	/* per-column formats */
		pq_endmessage(&buf);
		cstate->copy_dest = COPY_NEW_FE;
	}
	else
	{
		/* old way */
		if (cstate->binary)
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("COPY BINARY is not supported to stdout or from stdin")));
		pq_putemptymessage('H');
		/* grottiness needed for old COPY OUT protocol */
		pq_startcopyout();
		cstate->copy_dest = COPY_OLD_FE;
	}
}

static void
ReceiveCopyBegin(CopyState cstate)
{
	if (PG_PROTOCOL_MAJOR(FrontendProtocol) >= 3)
	{
		/* new way */
		StringInfoData buf;
		int natts = list_length(cstate->attnumlist);
		int16 format = (cstate->binary ? 1 : 0);
		int i;

		pq_beginmessage(&buf, 'G');
		pq_sendbyte(&buf, format);	/* overall format */
		pq_sendint16(&buf, natts);
		for (i = 0; i < natts; i++)
			pq_sendint16(&buf, format);	/* per-column formats */
		pq_endmessage(&buf);
		cstate->copy_dest = COPY_NEW_FE;
		cstate->fe_msgbuf = makeStringInfo();
	}
	else
	{
		/* old way */
		if (cstate->binary)
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("COPY BINARY is not supported to stdout or from stdin")));
		pq_putemptymessage('G');
		/* any error in old protocol will make us lose sync */
		pq_startmsgread();
		cstate->copy_dest = COPY_OLD_FE;
	}
	/* We *must* flush here to ensure FE knows it can send. */
	pq_flush();
}

static void
SendCopyEnd(CopyState cstate)
{
	if (cstate->copy_dest == COPY_NEW_FE)
	{
		/* Shouldn't have any unsent data */
		Assert(cstate->fe_msgbuf->len == 0);
		/* Send Copy Done message */
		pq_putemptymessage('c');
	}
	else
	{
		CopySendData(cstate, "\\.", 2);
		/* Need to flush out the trailer (this also appends a newline) */
		CopySendEndOfRow(cstate);
		pq_endcopyout(false);
	}
}

/*----------
 * CopySendData sends output data to the destination (file or frontend)
 * CopySendString does the same for null-terminated strings
 * CopySendChar does the same for single characters
 * CopySendEndOfRow does the appropriate thing at end of each data row
 *	(data is not actually flushed except by CopySendEndOfRow)
 *
 * NB: no data conversion is applied by these functions
 *----------
 */
static void
CopySendData(CopyState cstate, const void *databuf, int datasize)
{
	appendBinaryStringInfo(cstate->fe_msgbuf, databuf, datasize);
}

static void
CopySendString(CopyState cstate, const char *str)
{
	appendBinaryStringInfo(cstate->fe_msgbuf, str, strlen(str));
}

static void
CopySendChar(CopyState cstate, char c)
{
	appendStringInfoCharMacro(cstate->fe_msgbuf, c);
}

extern void
CopySendEndOfRow(CopyState cstate)
{
	StringInfo fe_msgbuf = cstate->fe_msgbuf;

	switch (cstate->copy_dest)
	{
	case COPY_FILE:
		if (cstate->eol_type == EOL_UD)
		{
			CopySendString(cstate, cstate->eol);
		}
		else if (!cstate->binary)
		{
			/* Default line termination depends on platform */
#ifndef WIN32
			CopySendChar(cstate, '\n');
#else
			CopySendString(cstate, "\r\n");
#endif
		}

		if (fwrite(fe_msgbuf->data, fe_msgbuf->len, 1,
				   cstate->copy_file) != 1 ||
			ferror(cstate->copy_file))
		{
			if (cstate->is_program)
			{
				if (errno == EPIPE)
				{
					/*
						 * The pipe will be closed automatically on error at
						 * the end of transaction, but we might get a better
						 * error message from the subprocess' exit code than
						 * just "Broken Pipe"
						 */
					ClosePipeToProgram(cstate);

					/*
						 * If ClosePipeToProgram() didn't throw an error, the
						 * program terminated normally, but closed the pipe
						 * first. Restore errno, and throw an error.
						 */
					errno = EPIPE;
				}
				ereport(ERROR,
						(errcode_for_file_access(),
						 errmsg("could not write to COPY program: %m")));
			}
			else
				ereport(ERROR,
						(errcode_for_file_access(),
						 errmsg("could not write to COPY file: %m")));
		}
		break;
	case COPY_OLD_FE:
		/* The FE/BE protocol uses \n as newline for all platforms */
		if (!cstate->binary)
			CopySendChar(cstate, '\n');

		if (pq_putbytes(fe_msgbuf->data, fe_msgbuf->len))
		{
			/* no hope of recovering connection sync, so FATAL */
			ereport(FATAL,
					(errcode(ERRCODE_CONNECTION_FAILURE),
					 errmsg("connection lost during COPY to stdout")));
		}
		break;
	case COPY_NEW_FE:
		if (cstate->eol_type == EOL_UD)
		{
			CopySendString(cstate, cstate->eol);
		}
		/* The FE/BE protocol uses \n as newline for all platforms */
		else if (!cstate->binary)
			CopySendChar(cstate, '\n');

		/* Dump the accumulated row as one CopyData message */
		(void)pq_putmessage('d', fe_msgbuf->data, fe_msgbuf->len);
		break;
#ifdef PGXC
	case COPY_BUFFER:
		/* Do not send yet anywhere, just return */
		return;
#endif
	case COPY_CALLBACK:
		if (cstate->eol_type == EOL_UD)
		{
			CopySendString(cstate, cstate->eol);
		}
		else
		{
#ifndef WIN32
			CopySendChar(cstate, '\n');
#else
			CopySendString(cstate, "\r\n");
#endif
		}
		return; /* don't want to reset msgbuf quite yet */
	}

	resetStringInfo(fe_msgbuf);
}

/*
 * CopyGetData reads data from the source (file or frontend)
 *
 * We attempt to read at least minread, and at most maxread, bytes from
 * the source.  The actual number of bytes read is returned; if this is
 * less than minread, EOF was detected.
 *
 * Note: when copying from the frontend, we expect a proper EOF mark per
 * protocol; if the frontend simply drops the connection, we raise error.
 * It seems unwise to allow the COPY IN to complete normally in that case.
 *
 * NB: no data conversion is applied here.
 */
static int
CopyGetData(CopyState cstate, void *databuf, int minread, int maxread)
{
	int bytesread = 0;
#ifdef __OPENTENBASE__
	cstate->whole_line = false;
#endif

	switch (cstate->copy_dest)
	{
	case COPY_FILE:
		bytesread = fread(databuf, 1, maxread, cstate->copy_file);
		if (ferror(cstate->copy_file))
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not read from COPY file: %m")));
		break;
	case COPY_OLD_FE:

		/*
			 * We cannot read more than minread bytes (which in practice is 1)
			 * because old protocol doesn't have any clear way of separating
			 * the COPY stream from following data.  This is slow, but not any
			 * slower than the code path was originally, and we don't care
			 * much anymore about the performance of old protocol.
			 */
		if (pq_getbytes((char *)databuf, minread))
		{
			/* Only a \. terminator is legal EOF in old protocol */
			ereport(ERROR,
					(errcode(ERRCODE_CONNECTION_FAILURE),
					 errmsg("unexpected EOF on client connection with an open transaction")));
		}
		bytesread = minread;
		break;
	case COPY_NEW_FE:
		while (maxread > 0 && bytesread < minread && !cstate->fe_eof)
		{
			int avail;

			while (cstate->fe_msgbuf->cursor >= cstate->fe_msgbuf->len)
			{
				/* Try to receive another message */
				int mtype;

			readmessage:
				HOLD_CANCEL_INTERRUPTS();
				pq_startmsgread();
				mtype = pq_getbyte();
				if (mtype == EOF)
					ereport(ERROR,
							(errcode(ERRCODE_CONNECTION_FAILURE),
							 errmsg("unexpected EOF on client connection with an open transaction")));
				if (pq_getmessage(cstate->fe_msgbuf, 0))
					ereport(ERROR,
							(errcode(ERRCODE_CONNECTION_FAILURE),
							 errmsg("unexpected EOF on client connection with an open transaction")));
				RESUME_CANCEL_INTERRUPTS();
				switch (mtype)
				{
				case 'd': /* CopyData */
					break;
				case 'c': /* CopyDone */
					/* COPY IN correctly terminated by frontend */
					cstate->fe_eof = true;
					return bytesread;
				case 'f': /* CopyFail */
					if (g_enable_copy_silence)
					{
						ereport(FATAL,
								(errcode(ERRCODE_QUERY_CANCELED),
								 errmsg("receive copyfail from cn, msg:%s",
								        cstate->fe_msgbuf->len ? pq_getmsgstring(cstate->fe_msgbuf)
								                               : " ")));
					}
					else
					{
						ereport(ERROR,
								(errcode(ERRCODE_QUERY_CANCELED),
								 errmsg("COPY from stdin failed: %s",
								        cstate->fe_msgbuf->len ? pq_getmsgstring(cstate->fe_msgbuf)
								                               : " ")));
					}
					break;
				case 'H': /* Flush */
				case 'S': /* Sync */

					/*
							 * Ignore Flush/Sync for the convenience of client
							 * libraries (such as libpq) that may send those
							 * without noticing that the command they just
							 * sent was COPY.
							 */
					goto readmessage;
				default:
					ereport(ERROR,
							(errcode(ERRCODE_PROTOCOL_VIOLATION),
							 errmsg("unexpected message type 0x%02X during COPY from stdin",
									mtype)));
					break;
				}
			}
			avail = cstate->fe_msgbuf->len - cstate->fe_msgbuf->cursor;
#ifdef __OPENTENBASE__
			if (avail > maxread)
			{
				cstate->whole_line = false;
			}
			else
			{
				cstate->whole_line = true;
			}
#endif
			if (avail > maxread)
				avail = maxread;
			pq_copymsgbytes(cstate->fe_msgbuf, databuf, avail);
			databuf = (void *)((char *)databuf + avail);
			maxread -= avail;
			bytesread += avail;
		}
		break;
#ifdef PGXC
	case COPY_BUFFER:
		elog(ERROR, "COPY_BUFFER not allowed in this context");
		break;
#endif
	case COPY_CALLBACK:
		bytesread = cstate->data_source_cb(databuf, minread, maxread, cstate->data_source_cb_extra);
		break;
	}

	return bytesread;
}

/*
 * These functions do apply some data conversion
 */

/*
 * CopySendInt32 sends an int32 in network byte order
 */
static void
CopySendInt32(CopyState cstate, int32 val)
{
	uint32 buf;

	buf = pg_hton32((uint32) val);
	CopySendData(cstate, &buf, sizeof(buf));
}

/*
 * CopyGetInt32 reads an int32 that appears in network byte order
 *
 * Returns true if OK, false if EOF
 */
static bool
CopyGetInt32(CopyState cstate, int32 *val)
{
	uint32 buf;

	if (CopyGetData(cstate, &buf, sizeof(buf), sizeof(buf)) != sizeof(buf))
	{
		*val = 0; /* suppress compiler warning */
		return false;
	}
	*val = (int32) pg_ntoh32(buf);
	return true;
}

/*
 * CopySendInt16 sends an int16 in network byte order
 */
static void
CopySendInt16(CopyState cstate, int16 val)
{
	uint16 buf;

	buf = pg_hton16((uint16) val);
	CopySendData(cstate, &buf, sizeof(buf));
}

/*
 * CopyGetInt16 reads an int16 that appears in network byte order
 */
static bool
CopyGetInt16(CopyState cstate, int16 *val)
{
	uint16 buf;

	if (CopyGetData(cstate, &buf, sizeof(buf), sizeof(buf)) != sizeof(buf))
	{
		*val = 0; /* suppress compiler warning */
		return false;
	}
	*val = (int16) pg_ntoh16(buf);
	return true;
}

/*
 * CopyLoadRawBuf loads some more data into raw_buf
 *
 * Returns TRUE if able to obtain at least one more byte, else FALSE.
 *
 * If raw_buf_index < raw_buf_len, the unprocessed bytes are transferred
 * down to the start of the buffer and then we load more data after that.
 * This case is used only when a frontend multibyte character crosses a
 * bufferload boundary.
 */
bool
CopyLoadRawBuf(CopyState cstate)
{
	int nbytes;
	int inbytes;

	if (cstate->raw_buf_index < cstate->raw_buf_len)
	{
		/* Copy down the unprocessed data */
		nbytes = cstate->raw_buf_len - cstate->raw_buf_index;
		memmove(cstate->raw_buf, cstate->raw_buf + cstate->raw_buf_index,
				nbytes);
	}
	else
		nbytes = 0; /* no data need be saved */

	inbytes = CopyGetData(cstate, cstate->raw_buf + nbytes,
						  1, RAW_BUF_SIZE - nbytes);
	nbytes += inbytes;
	cstate->raw_buf[nbytes] = '\0';
	cstate->raw_buf_index = 0;
	cstate->raw_buf_len = nbytes;
	return (inbytes > 0);
}

/*
 *	 DoCopy executes the SQL COPY statement
 *
 * Either unload or reload contents of table <relation>, depending on <from>.
 * (<from> = TRUE means we are inserting into the table.)  In the "TO" case
 * we also support copying the output of an arbitrary SELECT, INSERT, UPDATE
 * or DELETE query.
 *
 * If <pipe> is false, transfer is between the table and the file named
 * <filename>.  Otherwise, transfer is between the table and our regular
 * input/output stream. The latter could be either stdin/stdout or a
 * socket, depending on whether we're running under Postmaster control.
 *
 * Do not allow a Postgres user without superuser privilege to read from
 * or write to a file.
 *
 * Do not allow the copy if user doesn't have proper permission to access
 * the table or the specifically requested columns.
 */
void DoCopy(ParseState *pstate, const CopyStmt *stmt,
			int stmt_location, int stmt_len,
			uint64 *processed)
{
	CopyState cstate;
	bool is_from = stmt->is_from;
	bool pipe = (stmt->filename == NULL);
	Relation rel;
	Oid relid;
	RawStmt *query = NULL;
	List	   *options = stmt->options;


	/* Transfer any SREH options to the options list, so that BeginCopy can see them. */
	if (stmt->sreh)
	{
	        SingleRowErrorDesc *sreh = (SingleRowErrorDesc *) stmt->sreh;
	        
	        options = list_copy(options);
	        options = lappend(options, makeDefElem("sreh", (Node *) sreh, -1));
	}

	/* Disallow COPY to/from file or program except to superusers. */
	if (!pipe && !superuser())
	{
		if (stmt->is_program)
			ereport(ERROR,
					(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
					 errmsg("must be superuser to COPY to or from an external program"),
					 errhint("Anyone can COPY to stdout or from stdin. "
							 "psql's \\copy command also works for anyone.")));
		else
#ifdef __OPENTENBASE__
		{
			/* 
			 * if transformed from insert into multi-values, 
			 * non-superuser permitted to do copy 
			 */
			if (!stmt->insert_into)
			{
#endif
				ereport(ERROR,
						(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
						 errmsg("must be superuser to COPY to or from a file"),
						 errhint("Anyone can COPY to stdout or from stdin. "
								 "psql's \\copy command also works for anyone.")));
#ifdef __OPENTENBASE__
			}
		}
#endif
	}

#ifdef __OPENTENBASE__
	pstate->stmt = stmt;
#endif

	if (stmt->relation)
	{
		TupleDesc tupDesc;
		List *attnums;
		ListCell *cur;
		RangeTblEntry *rte;

		Assert(!stmt->query);

		/* Open and lock the relation, using the appropriate lock type. */
		rel = heap_openrv(stmt->relation,
						  (is_from ? RowExclusiveLock : AccessShareLock));

		relid = RelationGetRelid(rel);

		rte = addRangeTableEntryForRelation(pstate, rel, NULL, false, false);
		rte->requiredPerms = (is_from ? ACL_INSERT : ACL_SELECT);

		tupDesc = RelationGetDescr(rel);
		attnums = CopyGetAttnums(tupDesc, rel, stmt->attlist);
		foreach (cur, attnums)
		{
			int attno = lfirst_int(cur) -
						FirstLowInvalidHeapAttributeNumber;

			if (is_from)
				rte->insertedCols = bms_add_member(rte->insertedCols, attno);
			else
				rte->selectedCols = bms_add_member(rte->selectedCols, attno);
		}
		ExecCheckRTPerms(pstate->p_rtable, true);

		/*
		 * Permission check for row security policies.
		 *
		 * check_enable_rls will ereport(ERROR) if the user has requested
		 * something invalid and will otherwise indicate if we should enable
		 * RLS (returns RLS_ENABLED) or not for this COPY statement.
		 *
		 * If the relation has a row security policy and we are to apply it
		 * then perform a "query" copy and allow the normal query processing
		 * to handle the policies.
		 *
		 * If RLS is not enabled for this, then just fall through to the
		 * normal non-filtering relation handling.
		 */
		if (check_enable_rls(rte->relid, InvalidOid, false) == RLS_ENABLED)
		{
			SelectStmt *select;
			ColumnRef *cr;
			ResTarget *target;
			RangeVar *from;
			List *targetList = NIL;

			if (is_from)
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("COPY FROM not supported with row-level security"),
						 errhint("Use INSERT statements instead.")));

			/*
			 * Build target list
			 *
			 * If no columns are specified in the attribute list of the COPY
			 * command, then the target list is 'all' columns. Therefore, '*'
			 * should be used as the target list for the resulting SELECT
			 * statement.
			 *
			 * In the case that columns are specified in the attribute list,
			 * create a ColumnRef and ResTarget for each column and add them
			 * to the target list for the resulting SELECT statement.
			 */
			if (!stmt->attlist)
			{
				cr = makeNode(ColumnRef);
				cr->fields = list_make1(makeNode(A_Star));
				cr->location = -1;

				target = makeNode(ResTarget);
				target->name = NULL;
				target->indirection = NIL;
				target->val = (Node *)cr;
				target->location = -1;

				targetList = list_make1(target);
			}
			else
			{
				ListCell *lc;

				foreach (lc, stmt->attlist)
				{
					/*
					 * Build the ColumnRef for each column.  The ColumnRef
					 * 'fields' property is a String 'Value' node (see
					 * nodes/value.h) that corresponds to the column name
					 * respectively.
					 */
					cr = makeNode(ColumnRef);
					cr->fields = list_make1(lfirst(lc));
					cr->location = -1;

					/* Build the ResTarget and add the ColumnRef to it. */
					target = makeNode(ResTarget);
					target->name = NULL;
					target->indirection = NIL;
					target->val = (Node *)cr;
					target->location = -1;

					/* Add each column to the SELECT statement's target list */
					targetList = lappend(targetList, target);
				}
			}

			/*
			 * Build RangeVar for from clause, fully qualified based on the
			 * relation which we have opened and locked.
			 */
			from = makeRangeVar(get_namespace_name(RelationGetNamespace(rel)),
								pstrdup(RelationGetRelationName(rel)),
								-1);

			/* Build query */
			select = makeNode(SelectStmt);
			select->targetList = targetList;
			select->fromClause = list_make1(from);

			query = makeNode(RawStmt);
			query->stmt = (Node *)select;
			query->stmt_location = stmt_location;
			query->stmt_len = stmt_len;

			/*
			 * Close the relation for now, but keep the lock on it to prevent
			 * changes between now and when we start the query-based COPY.
			 *
			 * We'll reopen it later as part of the query-based COPY.
			 */
			heap_close(rel, NoLock);
			rel = NULL;
		}
	}
	else
	{
		Assert(stmt->query);

		/* MERGE is allowed by parser, but unimplemented. Reject for now */
		if (IsA(stmt->query, MergeStmt))
			ereport(ERROR,
			        (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			        errmsg("MERGE not supported in COPY")));

		query = makeNode(RawStmt);
		query->stmt = stmt->query;
		query->stmt_location = stmt_location;
		query->stmt_len = stmt_len;

		relid = InvalidOid;
		rel = NULL;
	}

	if (is_from)
	{
		Assert(rel);

		/* check read-only transaction,gtm read-only flag and parallel mode */
		if ((XactReadOnly || GTM_ReadOnly) && !rel->rd_islocaltemp)
			PreventCommandIfReadOnly("COPY FROM");
		PreventCommandIfParallelMode("COPY FROM");

		elog(DEBUG1, "[CopyFrom] BeginCopyFrom");

		cstate = BeginCopyFrom(pstate, rel, stmt->filename, stmt->is_program,
							   NULL, NULL, stmt->attlist, stmt->attinfolist, options, stmt->fixed_position);
		/* Initialize all the parsing and state variables */
		if (cstate->sred)
		{
		        if (cstate->sred->log_error_type == 't' || cstate->sred->log_error_type == 'p')
		                cstate->errMode = SREH_LOG;
				else if (cstate->sred->log_error_type == 'i')
			        cstate->errMode = SREH_IGNORE;
		        else /* 'f' */
			        cstate->errMode = ALL_OR_NOTHING;
		        
		        cstate->srehandler =  makeSrehandler(cstate->sred->rejectlimit,
                                                             cstate->sred->is_limit_in_rows,
                                                             cstate->filename,
                                                             stmt->relation->relname,
                                                             cstate->sred->log_error_type);
		        
		        if (rel)
		                cstate->srehandler->relid = RelationGetRelid(rel);
		}
		else
		{
		        cstate->errMode = ALL_OR_NOTHING;
		        cstate->srehandler = NULL;
		}
		*processed = CopyFrom(cstate); /* copy from file to database */
#ifdef XCP
		/*
		 * We should record insert to distributed table.
		 * Bulk inserts into local tables are recorded when heap tuples are
		 * written.
		 */
		if (IS_PGXC_COORDINATOR && rel->rd_locator_info)
			pgstat_count_remote_insert(rel, (int)*processed);
#endif
		EndCopyFrom(cstate);

		if (ORA_MODE &&
			IS_CENTRALIZED_MODE &&
			(RelationGetOnlineGatheringThreshold(rel) != -1) &&
			(*processed >= RelationGetOnlineGatheringThreshold(rel)))
		{
			elog(DEBUG1, "online gathing [%s] for copy: %ld", 
				 RelationGetRelationName(rel), *processed);

			online_update_relstats(rel, (double) *processed);
		}
	}
	else
	{
		Bitmapset *bms_shards = NULL;

		if (rel && !RelationIsSharded(rel) && stmt->shards)
		{
			elog(ERROR, "relation %d is not a sharded table, so it can not be export by specifed shardid.",
				 RelationGetRelid(rel));
		}

		if (stmt->shards)
		{
			ListCell *lc;
			int shardid;

			foreach (lc, stmt->shards)
			{
				A_Const *cons = (A_Const *)lfirst(lc);
				if (cons->val.type != T_Integer)
					elog(ERROR, "shard must be a integer");

				shardid = intVal(&cons->val);

				if (!ShardIDIsValid(shardid))
					elog(ERROR, "shardid %d is invalid.", shardid);
				bms_shards = bms_add_member(bms_shards, shardid);
			}
		}

		cstate = BeginCopyTo(pstate, rel, query, relid,
								stmt->filename, stmt->is_program,
								stmt->attlist, options,
								bms_shards);

		*processed = DoCopyTo(cstate); /* copy from database to file */

		if (cstate->shard_array)
		{
			bms_free(cstate->shard_array);
		}
		EndCopyTo(cstate);

		if (am_walsender && !AmOpenTenBaseSubscriptionWalSender())
		{
			Assert(MyReplicationSlot);

			if (OidIsValid(MyReplicationSlot->subid) && OidIsValid(MyReplicationSlot->relid))
			{
				if (MyReplicationSlot->relid == RelationGetRelid(rel))
				{
					UpdatePubStatistics(MyReplicationSlot->subname.data, *processed, 0, 0, 0, 0, false);
					UpdatePubTableStatistics(MyReplicationSlot->subid, MyReplicationSlot->relid, *processed, 0, 0, 0, 0, false);
				}
			}
		}
	}

	/*
	 * Close the relation. If reading, we can release the AccessShareLock we
	 * got; if writing, we should hold the lock until end of transaction to
	 * ensure that updates will be committed before lock is released.
	 */
	if (rel != NULL)
		heap_close(rel, (is_from ? NoLock : AccessShareLock));
}

/*
 * Process the statement option list for COPY.
 *
 * Scan the options list (a list of DefElem) and transpose the information
 * into cstate, applying appropriate error checking.
 *
 * cstate is assumed to be filled with zeroes initially.
 *
 * This is exported so that external users of the COPY API can sanity-check
 * a list of options.  In that usage, cstate should be passed as NULL
 * (since external users don't know sizeof(CopyStateData)) and the collected
 * data is just leaked until CurrentMemoryContext is reset.
 *
 * Note that additional checking, such as whether column names listed in FORCE
 * QUOTE actually exist, has to be applied later.  This just checks for
 * self-consistency of the options list.
 */
void ProcessCopyOptions(ParseState *pstate,
						CopyState cstate,
						bool is_from,
						List *options)
{
	bool format_specified = false;
	ListCell *option;
	SingleRowErrorDesc *sred = NULL;
	bool rejectlimit_found     = false;
	bool rejectlimittype_found = false;
	bool logerrors_found       = false;

	/* Support external use for option sanity checking */
	if (cstate == NULL)
		cstate = (CopyStateData *)palloc0(sizeof(CopyStateData));

	cstate->escape_off = false;
	cstate->file_encoding = -1;
	cstate->copy_mode = COPY_DEFAULT;
	cstate->time_format = NULL;
	cstate->timestamp_format = NULL;
	cstate->date_format = NULL;
	cstate->timestamp_tz_format = NULL;
	cstate->fill_missing = false;
	cstate->ignore_extra_data = false;
	cstate->compatible_illegal_chars = false;

	/* Extract options from the statement node tree */
	foreach (option, options)
	{
		DefElem *defel = lfirst_node(DefElem, option);

		if (strcmp(defel->defname, "format") == 0)
		{
			char *fmt = defGetString(defel);

			if (format_specified)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options"),
						 parser_errposition(pstate, defel->location)));
			format_specified = true;
			if (strcmp(fmt, "text") == 0)
				/* default format */;
			else if (strcmp(fmt, "csv") == 0)
				cstate->csv_mode = true;
			else if (strcmp(fmt, "binary") == 0)
				cstate->binary = true;
			else if (strcmp(fmt, "fixed") == 0)
				cstate->fixed_mode = true;
#ifdef __OPENTENBASE__
			else if (strcmp(fmt, "internal") == 0)
			{
				cstate->internal_mode = true;
				if (IS_PGXC_COORDINATOR)
				{
					elog(ERROR, "COPY format \"%s\" not recognized", fmt);
				}
			}
#endif
			else
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("COPY format \"%s\" not recognized", fmt),
						 parser_errposition(pstate, defel->location)));
		}
		else if (strcmp(defel->defname, "oids") == 0)
		{
			if (cstate->oids)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options"),
						 parser_errposition(pstate, defel->location)));
			cstate->oids = defGetBoolean(defel);

			if (cstate->fixed_position)
			{
			        ereport(ERROR,
			                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			                        errmsg("cannot support COLUMN POSITION AND OIDS at the same time.")));
			}
		}
#ifdef _PG_ORCL_
		else if (strcmp(defel->defname, "rowids") == 0)
		{
			if (cstate->rowids)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options"),
						 parser_errposition(pstate, defel->location)));
			cstate->rowids = defGetBoolean(defel);

			if (cstate->fixed_position)
			{
			        ereport(ERROR,
			                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			                        errmsg("cannot support COLUMN POSITION AND ROWIDS at the same time.")));
			}
                }
#endif
		else if (strcmp(defel->defname, "freeze") == 0)
		{
			if (cstate->freeze)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options"),
						 parser_errposition(pstate, defel->location)));
			cstate->freeze = defGetBoolean(defel);
		}
		else if (strcmp(defel->defname, "delimiter") == 0)
		{
			if (cstate->delim)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options"),
						 parser_errposition(pstate, defel->location)));
			cstate->delim = defGetString(defel);

			if (cstate->fixed_position)
			{
			        ereport(NOTICE,
			                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			                        errmsg("COLUMN POSITION will overwrite DELIMITER."),
			                        errhint("delete DELIMITER option."), -1));
			}
		}
		else if (strcmp(defel->defname, "null") == 0)
		{
			if (cstate->null_print)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options"),
						 parser_errposition(pstate, defel->location)));
			cstate->null_print = defGetString(defel);
		}
		else if (strcmp(defel->defname, "header") == 0)
		{
			if (cstate->header_line)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options"),
						 parser_errposition(pstate, defel->location)));
			cstate->header_line = defGetBoolean(defel);
		}
		else if (strcmp(defel->defname, "quote") == 0)
		{
			if (cstate->quote)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options"),
						 parser_errposition(pstate, defel->location)));
			cstate->quote = defGetString(defel);
		}
		else if (strcmp(defel->defname, "escape") == 0)
		{
			if (cstate->escape)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options"),
						 parser_errposition(pstate, defel->location)));
			cstate->escape = defGetString(defel);
		}
		else if (strcmp(defel->defname, "force_quote") == 0)
		{
			if (cstate->force_quote || cstate->force_quote_all)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options"),
						 parser_errposition(pstate, defel->location)));
			if (defel->arg && IsA(defel->arg, A_Star))
				cstate->force_quote_all = true;
			else if (defel->arg && IsA(defel->arg, List))
				cstate->force_quote = castNode(List, defel->arg);
			else
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("argument to option \"%s\" must be a list of column names",
								defel->defname),
						 parser_errposition(pstate, defel->location)));
		}
		else if (strcmp(defel->defname, "force_not_null") == 0)
		{
			if (cstate->force_notnull)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options"),
						 parser_errposition(pstate, defel->location)));
			if (defel->arg && IsA(defel->arg, List))
				cstate->force_notnull = castNode(List, defel->arg);
			else
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("argument to option \"%s\" must be a list of column names",
								defel->defname),
						 parser_errposition(pstate, defel->location)));
		}
		else if (strcmp(defel->defname, "force_null") == 0)
		{
			if (cstate->force_null)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options")));
			if (defel->arg && IsA(defel->arg, List))
				cstate->force_null = castNode(List, defel->arg);
			else
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("argument to option \"%s\" must be a list of column names",
								defel->defname),
						 parser_errposition(pstate, defel->location)));
		}
		else if (strcmp(defel->defname, "convert_selectively") == 0)
		{
			/*
			 * Undocumented, not-accessible-from-SQL option: convert only the
			 * named columns to binary form, storing the rest as NULLs. It's
			 * allowed for the column list to be NIL.
			 */
			if (cstate->convert_selectively)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options"),
						 parser_errposition(pstate, defel->location)));
			cstate->convert_selectively = true;
			if (defel->arg == NULL || IsA(defel->arg, List))
				cstate->convert_select = castNode(List, defel->arg);
			else
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("argument to option \"%s\" must be a list of column names",
								defel->defname),
						 parser_errposition(pstate, defel->location)));
		}
		else if (strcmp(defel->defname, "encoding") == 0)
		{
			if (cstate->file_encoding >= 0)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options"),
						 parser_errposition(pstate, defel->location)));
			if (IsA(defel->arg, Integer))
			{
			        const char *encoding_name;

			        cstate->file_encoding = atoi(defGetString(defel));
			        encoding_name = pg_encoding_to_char(cstate->file_encoding);
			        if (strcmp(encoding_name, "") == 0 ||
			                pg_valid_client_encoding(encoding_name) < 0)
			                ereport(ERROR,
                                                (errcode(ERRCODE_UNDEFINED_OBJECT),
                                                        errmsg("%d is not a valid encoding code", cstate->file_encoding)));
			}
			else
			        cstate->file_encoding = pg_char_to_encoding(defGetString(defel));
			if (cstate->file_encoding < 0)
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("argument to option \"%s\" must be a valid encoding name",
								defel->defname),
						 parser_errposition(pstate, defel->location)));
		}
		else if (strcmp(defel->defname, "eol") == 0)
		{
			if (cstate->eol_type == EOL_UD)
				ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("conflicting or redundant options")));

			cstate->eol_type = EOL_UD;
			if (strcasecmp(defGetString(defel), "0x0A") == 0 ||
				strcasecmp(defGetString(defel), "\\n") == 0)
			{
				cstate->eol = defGetString(defel);
				cstate->eol[0] = '\n';
				cstate->eol[1] = '\0';
			}
			else if (strcasecmp(defGetString(defel), "0x0D0A") == 0 ||
				strcasecmp(defGetString(defel), "\\r\\n") == 0)
			{
				cstate->eol = defGetString(defel);
				cstate->eol[0] = '\r';
				cstate->eol[1] = '\n';
				cstate->eol[2] = '\0';
			}
			else if (strcasecmp(defGetString(defel), "0x0D") == 0 ||
				strcasecmp(defGetString(defel), "\\r") == 0)
			{
				cstate->eol = defGetString(defel);
				cstate->eol[0] = '\r';
				cstate->eol[1] = '\0';
			}
			else
			{
				cstate->eol = defGetString(defel);
				if (strlen(defGetString(defel)) == 0)
					ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						errmsg("\"%s\" is not a valid EOL string, EOL string must not be empty",
						defGetString(defel))));
				else if (strlen(defGetString(defel)) > EOL_MAX_LEN)
					ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						errmsg("\"%s\" is not a valid EOL string, EOL string must not exceed "
							"the maximum length (10 bytes)",
				defGetString(defel))));
			}
		}
#ifdef __OPENTENBASE_C__
		else if (strcmp(defel->defname, "bysegment") == 0)
		{
			continue;
		}
#endif
		else if (strcmp(defel->defname, "log_errors") == 0)
		{
			char *arg;
			if (logerrors_found)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
								errmsg("conflicting or redundant options"),
								parser_errposition(pstate, defel->location)));
			if (!sred)
				sred = makeNode(SingleRowErrorDesc);
			arg      = defGetString(defel);
			sred->log_error_type = arg[0];
			logerrors_found = true;
		}
		else if (strcmp(defel->defname, "reject_limit_type") == 0)
		{
			char *arg;
			if (rejectlimittype_found)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
								errmsg("conflicting or redundant options"),
								parser_errposition(pstate, defel->location)));
			if (!sred)
				sred = makeNode(SingleRowErrorDesc);
			arg      = defGetString(defel);
			sred->is_limit_in_rows = (arg[0] == 'r' ? true : false);
			rejectlimittype_found = true;
		}
		else if (strcmp(defel->defname, "reject_limit") == 0)
		{
			if (rejectlimit_found)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
								errmsg("conflicting or redundant options"),
								parser_errposition(pstate, defel->location)));
			if (!sred)
				sred = makeNode(SingleRowErrorDesc);
			sred->rejectlimit = atoi(defGetString(defel));
			rejectlimit_found = true;
		}
		else if (strcmp(defel->defname, "sreh") == 0)
		{
			if (rejectlimit_found || rejectlimittype_found || logerrors_found)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
								errmsg("conflicting or redundant options"),
								parser_errposition(pstate, defel->location)));

			if (defel->arg == NULL || !IsA(defel->arg, SingleRowErrorDesc))
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
								errmsg("argument to option \"%s\" must be a list of column names",
									   defel->defname)));
			if (sred)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
								errmsg("conflicting or redundant options")));

			sred = (SingleRowErrorDesc *) defel->arg;

			rejectlimit_found     = true;
			rejectlimittype_found = true;
			logerrors_found       = true;
		}
		else if (strcmp(defel->defname, "date_format") == 0)
		{
			if (cstate->date_format)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
								errmsg("conflicting or redundant options")));
			cstate->date_format = defGetString(defel);
			/*
			 * Here no need to do a extra date format check, because as the following:
			 * (1)for foreign table date format checking occurs in ProcessDistImportOptions();
			 * (2)for copy date format checking occurs when date format is firstly used in date_in().
			 */
		}
		else if (strcmp(defel->defname, "time_format") == 0)
		{
			if (cstate->time_format)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
								errmsg("conflicting or redundant options")));
			cstate->time_format = defGetString(defel);
			/*
			 * Here no need to do a extra time format check, because as the following:
			 * (1)for foreign table time format checking occurs in ProcessDistImportOptions();
			 * (2)for copy time format checking occurs when date format is firstly used in time_in().
			 */
		}
		else if (strcmp(defel->defname, "timestamp_format") == 0)
		{
			if (cstate->timestamp_format)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
								errmsg("conflicting or redundant options")));
			cstate->timestamp_format = defGetString(defel);
			/*
			 * Here no need to do a extra timestamp format check, because as the following:
			 * (1)for foreign table timestamp format checking occurs in ProcessDistImportOptions();
			 * (2)for copy timestamp format checking occurs when timestamp format is firstly used in timestamp_in().
			 */
		}
		else if (strcmp(defel->defname, "col_position") == 0)
		{
			if (cstate->fixed_position)
				ereport(ERROR,
				        (errcode(ERRCODE_SYNTAX_ERROR),
						        errmsg("conflicting or redundant options")));
			/*
			 * just a flag to tell whether fixed positions exist,
			 * the exact position info is in cstate->attinfolist
			 */
			cstate->fixed_position = true;
		}
		else if (strcmp(defel->defname, "timestamp_tz_format") == 0)
		{
			if (cstate->timestamp_tz_format)
				ereport(ERROR,
				        (errcode(ERRCODE_SYNTAX_ERROR),
						        errmsg("conflicting or redundant options")));
			cstate->timestamp_tz_format = defGetString(defel);
			/*
	         * Here no need to do a extra timestamp format check, because as the following:
			 * (1)for foreign table timestamp format checking occurs in ProcessDistImportOptions();
			 * (2)for copy timestamp format checking occurs when timestamp format is firstly used in timestamp_in().
	         */
		}
		else if (strcmp(defel->defname, "fill_missing_fields") == 0)
		{
			if (cstate->fill_missing)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
								errmsg("conflicting or redundant options")));
			cstate->fill_missing = defGetBoolean(defel);
		}
		else if (strcmp(defel->defname, "ignore_extra_data") == 0)
		{
			if (cstate->ignore_extra_data)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
								errmsg("conflicting or redundant options")));
			cstate->ignore_extra_data = defGetBoolean(defel);
		}
		else if (strcmp(defel->defname, "compatible_illegal_chars") == 0)
		{
			if (cstate->compatible_illegal_chars)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
								errmsg("conflicting or redundant options")));
			cstate->compatible_illegal_chars = true;
			cstate->illegal_conv_chars = defGetString(defel);

			if (strlen(cstate->illegal_conv_chars) != 1)
			{
				if (pg_strcasecmp(cstate->illegal_conv_chars, "true") == 0 ||
				    pg_strcasecmp(cstate->illegal_conv_chars, "false") == 0 ||
				    pg_strcasecmp(cstate->illegal_conv_chars, "on") == 0 ||
				    pg_strcasecmp(cstate->illegal_conv_chars, "off") == 0)
				{
					cstate->compatible_illegal_chars = defGetBoolean(defel);
					cstate->illegal_conv_chars = "?";
				}
				else
					ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					                errmsg("The string used to replace illegal characters must be "
					                       "a single character")));
			}

			/* Disallow end-of-line characters */
			if (strchr(cstate->illegal_conv_chars, '\r') != NULL ||
			    strchr(cstate->illegal_conv_chars, '\n') != NULL)
				ereport(ERROR,
				        (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				         errmsg("The string used to replace illegal characters cannot contain "
				                "newline or carriage return")));
		}
		else if (strcmp(defel->defname, "formatter") == 0)
		{
			if (cstate->formatter)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
								errmsg("conflicting or redundant options")));
			if (defel->arg && IsA(defel->arg, CopyFormatter))
			{
				CopyFormatter *cf = castNode(CopyFormatter, defel->arg);
				cstate->formatter = cf->fixed_attrs;
				cstate->formatter_with_cut = cf->with_cut;
			}
			else
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("argument to option \"%s\" must be a list of column names",
								defel->defname),
						 parser_errposition(pstate, defel->location)));
		}
		else if (strcmp(defel->defname, "formatter_with_cut") == 0)
		{
			cstate->formatter_with_cut = defGetBoolean(defel);
		}
		else
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					 errmsg("option \"%s\" not recognized",
							defel->defname),
					 parser_errposition(pstate, defel->location)));
	}

	/*
	 * Check for incompatible options (must do these two before inserting
	 * defaults)
	 */
	if ((cstate->binary || cstate->internal_mode || cstate->fixed_mode) && cstate->fixed_position)
	        ereport(ERROR,
	                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
	                        errmsg("COLUMN POSITION available only in CSV or TEXT mode.")));

	if (cstate->binary && cstate->delim)
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("cannot specify DELIMITER in BINARY mode")));

	if (cstate->binary && cstate->null_print)
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("cannot specify NULL in BINARY mode")));

	/* Set defaults for omitted options */
	if (!cstate->delim)
		cstate->delim = cstate->csv_mode ? "," : "\t";

	if (!cstate->null_print)
	{
		cstate->null_print = cstate->csv_mode ? "" : "\\N";
	}
	cstate->null_print_len = strlen(cstate->null_print);

	if (cstate->csv_mode)
	{
		if (!cstate->quote)
			cstate->quote = "\"";
		if (!cstate->escape)
			cstate->escape = cstate->quote;
	}

	if (!cstate->csv_mode && !cstate->escape)
		cstate->escape = "\\";			/* default escape for text mode */

	/* delimiter strings should be no more than 10 bytes. */
	cstate->delim_len = strlen(cstate->delim);
	if (cstate->delim_len > DELIM_MAX_LEN || cstate->delim_len < 1)
		ereport(ERROR,
		        (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				        errmsg("COPY delimiter must be positive and no more than %d bytes", DELIM_MAX_LEN)));

	/* Disallow end-of-line characters */
	if (strchr(cstate->delim, '\r') != NULL ||
		strchr(cstate->delim, '\n') != NULL)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("COPY delimiter cannot contain newline or carriage return")));

	if (strchr(cstate->null_print, '\r') != NULL ||
		strchr(cstate->null_print, '\n') != NULL)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("COPY null representation cannot use newline or carriage return")));
	/* Disallow unsafe eol characters in non-CSV mode. */
	if (!cstate->csv_mode && cstate->eol_type == EOL_UD && strpbrk(cstate->eol, INVALID_EOL_CHAR) != NULL)
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
		                errmsg("EOL string \"%s\" cannot contain any characters in \"%s\"",
		                       cstate->eol, INVALID_EOL_CHAR)));
	
	if (cstate->eol_type == EOL_UD)
	{
		if (strstr(cstate->delim, cstate->eol) != NULL)
			ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				errmsg("COPY delimiter cannot contain user-define EOL string")));
		if (strstr(cstate->null_print, cstate->eol) != NULL)
			ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				errmsg("COPY null representation cannot contain user-define EOL string")));
	}

	/*
	 * Disallow unsafe delimiter characters in non-CSV mode.  We can't allow
	 * backslash because it would be ambiguous.  We can't allow the other
	 * cases because data characters matching the delimiter must be
	 * backslashed, and certain backslash combinations are interpreted
	 * non-literally by COPY IN.  Disallowing all lower case ASCII letters is
	 * more than strictly necessary, but seems best for consistency and
	 * future-proofing.  Likewise we disallow all digits though only octal
	 * digits are actually dangerous.
	 */
	if (!cstate->csv_mode && strpbrk(cstate->delim, INVALID_DELIM_CHAR) != NULL)
		ereport(ERROR,
		        (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				        errmsg("COPY delimiter \"%s\" cannot contain any characters in \"%s\"", cstate->delim, INVALID_DELIM_CHAR)));

	/* Disallow unsafe eol characters in non-CSV mode. */
	if (!cstate->csv_mode && cstate->eol_type == EOL_UD &&
		strpbrk(cstate->eol, INVALID_EOL_CHAR) != NULL)
			ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				errmsg("EOL string \"%s\" cannot contain any characters in\"%s\"", cstate->eol, INVALID_EOL_CHAR)));

	/* Check header */
	if ((!cstate->csv_mode && !cstate->fixed_mode) && cstate->header_line)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("COPY HEADER available only in CSV or FIXED mode")));

	/* Check quote */
	if (!cstate->csv_mode && cstate->quote != NULL)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("COPY quote available only in CSV mode")));

	if (cstate->csv_mode && strlen(cstate->quote) != 1)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("COPY quote must be a single one-byte character")));

	if (cstate->csv_mode && strchr(cstate->delim, cstate->quote[0]) != NULL)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("COPY delimiter cannot contain quote character")));

	/* Check escape */
	if (cstate->csv_mode && cstate->escape != NULL && strlen(cstate->escape) != 1)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("COPY escape must be a single one-byte character")));

	if (!cstate->csv_mode && cstate->escape != NULL &&
		(strchr(cstate->escape, '\r') != NULL ||
		strchr(cstate->escape, '\n') != NULL))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("COPY escape representation in text format cannot use newline or carriage return")));

	if (!cstate->csv_mode && cstate->escape != NULL && strlen(cstate->escape) != 1)
	{
		if (pg_strcasecmp(cstate->escape, "off") != 0)
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("COPY escape must be a single character, or [OFF/off] to disable escapes")));
	}

	/* Check force_quote */
	if (!cstate->csv_mode && (cstate->force_quote || cstate->force_quote_all))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("COPY force quote available only in CSV mode")));
	if ((cstate->force_quote || cstate->force_quote_all) && is_from)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("COPY force quote only available using COPY TO")));

	/* Check force_notnull */
	if (!cstate->csv_mode && cstate->force_notnull != NIL)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("COPY force not null available only in CSV mode")));
	if (cstate->force_notnull != NIL && !is_from)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("COPY force not null only available using COPY FROM")));

	/* Check force_null */
	if (!cstate->csv_mode && cstate->force_null != NIL)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("COPY force null available only in CSV mode")));

	if (cstate->force_null != NIL && !is_from)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("COPY force null only available using COPY FROM")));

	/* Don't allow the delimiter to appear in the null string. */
	if ((strlen(cstate->null_print) > 0) &&
	    (strlen(cstate->delim) > 0) &&
	    (strstr(cstate->null_print, cstate->delim) != NULL || strstr(cstate->delim, cstate->null_print) != NULL))
		ereport(ERROR,
		        (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				        errmsg("COPY delimiter must not appear in the NULL specification")));

	/* Don't allow the CSV quote char to appear in the null string. */
	if (cstate->csv_mode &&
		strchr(cstate->null_print, cstate->quote[0]) != NULL)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("CSV quote character must not appear in the NULL specification")));

	if (cstate->eol_type == EOL_UD &&
		strcmp(cstate->eol, "\r\n") != 0 && strcmp(cstate->eol, "\n") != 0 &&
		!is_from &&
		cstate->csv_mode)
			ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				errmsg("EOL specification can not be used with non-text format "
					"using COPY TO or WRITE ONLY foreign table except 0x0D0A and 0x0A")));
	if (cstate->eol_type == EOL_UD && cstate->binary)
		ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			errmsg("can not specify EOL in BINARY mode")));

	/* check data_time_format */
	if (!is_from && cstate->date_format)
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
						errmsg("DATE_FORMAT specification only available using COPY FROM or READ ONLY foreign table")));
	if (!is_from && cstate->time_format)
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
						errmsg("TIME_FORMAT specification only available using COPY FROM or READ ONLY foreign table")));
	if (!is_from && cstate->timestamp_format)
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
						errmsg("TIMESTAMP_FORMAT specification only available using COPY FROM or READ ONLY foreign table")));
	if (!is_from && cstate->timestamp_tz_format)
		ereport(ERROR,
		        (errcode(ERRCODE_SYNTAX_ERROR),
				        errmsg("TIMESTAMPTZ_FORMAT specification only available using COPY FROM or READ ONLY foreign table")));

	/* check fill_missing and ignore_extra_data*/
	if (cstate->binary && cstate->fill_missing)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("fill_missing_fields available only in CSV/TEXT mode")));

	if (!is_from && cstate->fill_missing)
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
						errmsg("fill_missing_fields specification only available using COPY FROM or READ ONLY foreign table")));

	if (cstate->binary && cstate->ignore_extra_data)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("ignore_extra_data available only in CSV/TEXT mode")));

	if (!is_from && cstate->ignore_extra_data)
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
						errmsg("ignore_extra_data specification only available using COPY FROM or READ ONLY foreign table")));

	if (!is_from && cstate->compatible_illegal_chars)
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
						errmsg("compatible_illegal_chars specification only available using COPY FROM or READ ONLY foreign table")));

	/* check compatible_illegal_chars */
	if ((cstate->binary || cstate->internal_mode) && cstate->compatible_illegal_chars)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("compatible_illegal_chars available only in CSV or TEXT mode.")));
	/*
	 * There are some confusion between the following flags with compatible_illegal_chars_specified flag.
	 *
	 * check null_print
	 */
	if (cstate->null_print && (cstate->null_print_len == 1) && cstate->compatible_illegal_chars && 
		((cstate->null_print[0] == ' ') || (strcmp(cstate->null_print, cstate->illegal_conv_chars) == 0)))
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
						errmsg("illegal chars conversion may confuse COPY null 0x%x", cstate->null_print[0])));
	/*
	 * check delimiter
	 */
	if (cstate->delim && cstate->compatible_illegal_chars && ((cstate->delim[0] == ' ') || (strchr(cstate->delim, cstate->illegal_conv_chars[0]) != NULL)))
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
						errmsg("illegal chars conversion may confuse COPY delimiter 0x%x", cstate->delim[0])));

	/*
	 * check quote
	 */
	if (cstate->quote && cstate->compatible_illegal_chars && ((cstate->quote[0] == ' ') || (cstate->quote[0] == cstate->illegal_conv_chars[0])))
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
						errmsg("illegal chars conversion may confuse COPY quote 0x%x", cstate->quote[0])));

	/*
	 * check escape
	 */
	if (cstate->escape && cstate->compatible_illegal_chars && ((cstate->escape[0] == ' ') || (cstate->escape[0] == cstate->illegal_conv_chars[0])))
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
						errmsg("illegal chars conversion may confuse COPY escape 0x%x", cstate->escape[0])));

	if (cstate->escape != NULL && pg_strcasecmp(cstate->escape, "off") == 0)
	{
		cstate->escape_off = true;
	}

	if (!cstate->fixed_mode && cstate->formatter != NULL)
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
		                errmsg("formatter only can be specified in FIXED mode")));

	if (cstate->fixed_mode && cstate->formatter == NULL)
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
		                errmsg("formatter must be specified in FIXED mode")));
        
	if (sred)
	{
	        cstate->sred = sred;
	}
}

static int
AttrCompare(const void *v1, const void *v2)
{
	AttInfo *p1 = (AttInfo *) lfirst(*(ListCell **) v1);
	AttInfo *p2 = (AttInfo *) lfirst(*(ListCell **) v2);

	if (p1->attrpos->start_offset > p2->attrpos->start_offset)
		return 1;
	else if (p1->attrpos->start_offset == p2->attrpos->start_offset)
		return 0;
	return -1;
}

static List *
GetSortedAttrList(List *attr_list)
{
	List     *new_attr_list = NIL;
	List     *attr_list_sorted = NIL;
	ListCell *cell = NULL;
	AttInfo  *attr_info;
	AttInfo  *new_attr_info;
	Position *attrpos;
	int       last_end_offset = 0;

	if (attr_list == NIL)
		return NIL;

	foreach (cell, attr_list)
	{
		attr_info = (AttInfo *) lfirst(cell);
		new_attr_info = copyObject(attr_info);
		attrpos = new_attr_info->attrpos;

		if (attrpos->flag == RELATIVE_POS)
		{
			/* eg. position(*+1:41) */
			attrpos->start_offset = last_end_offset + 1 + attrpos->start_offset;
			attrpos->flag = ABSOLUTE_POS;
		}

		last_end_offset = attrpos->end_offset;
		new_attr_list = lappend(new_attr_list, new_attr_info);
	}

	Assert(new_attr_list != NIL);
	attr_list_sorted = list_qsort(new_attr_list, AttrCompare);
	list_free(new_attr_list);
	return attr_list_sorted;
}

/*
 * Common setup routines used by BeginCopyFrom and BeginCopyTo.
 *
 * Iff <binary>, unload or reload in the binary format, as opposed to the
 * more wasteful but more robust and portable text format.
 *
 * Iff <oids>, unload or reload the format that includes OID information.
 * On input, we accept OIDs whether or not the table has an OID column,
 * but silently drop them if it does not.  On output, we report an error
 * if the user asks for OIDs in a table that has none (not providing an
 * OID column might seem friendlier, but could seriously confuse programs).
 *
 * If in the text format, delimit columns with delimiter <delim> and print
 * NULL values as <null_print>.
 */
static CopyState
BeginCopy(ParseState *pstate,
		  bool is_from,
		  Relation rel,
		  RawStmt *raw_query,
		  Oid queryRelId,
		  List *attnamelist,
		  List *options,
#ifdef _SHARDING_
		  Bitmapset * shards,
#endif
		  List *attinfolist,
		  bool fixed_position
)
{
	CopyState cstate;
	TupleDesc tupDesc;
	int num_phys_attrs;
	MemoryContext oldcontext;

	/* Allocate workspace and zero all fields */
	cstate = (CopyStateData *)palloc0(sizeof(CopyStateData));

	/*
	 * We allocate everything used by a cstate in a new memory context. This
	 * avoids memory leaks during repeated use of COPY in a query.
	 */
	cstate->copycontext = AllocSetContextCreate(CurrentMemoryContext,
												"COPY",
												ALLOCSET_DEFAULT_SIZES);

	oldcontext = MemoryContextSwitchTo(cstate->copycontext);

	cstate->attinfolist = attinfolist;
	cstate->attinfolist_sorted = fixed_position ? GetSortedAttrList(attinfolist) : NIL;
	cstate->fixed_position = fixed_position;

	/* Extract options from the statement node tree */
	ProcessCopyOptions(pstate, cstate, is_from, options);
#ifdef _SHARDING_
	cstate->shard_array = shards;
	cstate->insert_into = false;
#endif

	/* Process the source/target relation or query */
	if (rel)
	{
		Assert(!raw_query);

		cstate->rel = rel;

		tupDesc = RelationGetDescr(cstate->rel);

		/* Don't allow COPY w/ OIDs to or from a table without them */
		if (cstate->oids && !cstate->rel->rd_rel->relhasoids)
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_COLUMN),
					 errmsg("table \"%s\" does not have OIDs",
							RelationGetRelationName(cstate->rel))));


		/* Initialize state for CopyFrom tuple routing. */
		if (is_from && rel->rd_rel->relkind == RELKIND_PARTITIONED_TABLE)
		{
            PartitionTupleRouting *proute;

            proute = cstate->partition_tuple_routing =
			ExecSetupPartitionTupleRouting(NULL, cstate->rel);

			/*
			 * If we are capturing transition tuples, they may need to be
			 * converted from partition format back to partitioned table
			 * format (this is only ever necessary if a BEFORE trigger
			 * modifies the tuple).
			 */
			if (cstate->transition_capture != NULL)
				ExecSetupChildParentMapForLeaf(proute);
		}
#ifdef PGXC
		/* Get copy statement and execution node information */
		if (IS_PGXC_COORDINATOR)
		{
			RemoteCopyData *remoteCopyState = (RemoteCopyData *)palloc0(sizeof(RemoteCopyData));
			List *attnums = CopyGetAttnums(tupDesc, cstate->rel, attnamelist);

			/* Setup correct COPY FROM/TO flag */
			remoteCopyState->is_from = is_from;

			/* Get execution node list */
			RemoteCopy_GetRelationLoc(remoteCopyState,
									  cstate->rel,
									  attnums);
#ifdef __OPENTENBASE__
			if (pstate && pstate->stmt && pstate->stmt->insert_into)
			{
				cstate->insert_into = true;
			}
#endif
			/* Build remote query */
			RemoteCopy_BuildStatement(remoteCopyState,
									  cstate->rel,
									  GetRemoteCopyOptions(cstate),
									  attnamelist,
									  cstate->attinfolist,
									  attnums,
#ifdef _SHARDING_
									  shards
#endif
			);

			/* Then assign built structure */
			cstate->remoteCopyState = remoteCopyState;
		}
#endif
	}
	else
	{
		List *rewritten;
		Query *query;
		PlannedStmt *plan;
		DestReceiver *dest;

		Assert(!is_from);
		cstate->rel = NULL;

		/* Don't allow COPY w/ OIDs from a query */
		if (cstate->oids)
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("COPY (query) WITH OIDS is not supported")));

#ifdef _PG_ORCL_
		if (cstate->rowids)
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("COPY (query) WITH ROWIDs is not supported")));
#endif

		/*
		 * The grammar allows SELECT INTO, but we don't support that. We do
		 * this check before transforming the query since QueryRewriteCTAS will
		 * transform the statemet into a CREATE TABLE followed by INSERT INTO.
		 * The CREATE TABLE is processed separately by QueryRewriteCTAS and
		 * what we get back is just the INSERT statement. Not doing a check
		 * will ultimately lead to an error, but doing it here allows us to
		 * throw a more friendly and PG-compatible error.
		 */
		if (IsA(raw_query->stmt, SelectStmt))
		{
			SelectStmt *stmt = (SelectStmt *)raw_query->stmt;

			/*
			 * If it's a set-operation tree, drilldown to leftmost SelectStmt
			 */
			while (stmt && stmt->op != SETOP_NONE)
				stmt = stmt->larg;
			Assert(stmt && IsA(stmt, SelectStmt) && stmt->larg == NULL);

			if (stmt->intoClause)
			{
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("COPY (SELECT INTO) is not supported")));
			}
		}

		/*
		 * Run parse analysis and rewrite.  Note this also acquires sufficient
		 * locks on the source table(s).
		 *
		 * Because the parser and planner tend to scribble on their input, we
		 * make a preliminary copy of the source querytree.  This prevents
		 * problems in the case that the COPY is in a portal or plpgsql
		 * function and is executed repeatedly.  (See also the same hack in
		 * DECLARE CURSOR and PREPARE.)  XXX FIXME someday.
		 */
		rewritten = pg_analyze_and_rewrite(copyObject(raw_query),
										   pstate->p_sourcetext, NULL, 0,
										   NULL);

		/* check that we got back something we can work with */
		if (rewritten == NIL)
		{
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("DO INSTEAD NOTHING rules are not supported for COPY")));
		}
		else if (list_length(rewritten) > 1)
		{
			ListCell *lc;

			/* examine queries to determine which error message to issue */
			foreach (lc, rewritten)
			{
				Query *q = lfirst_node(Query, lc);

				if (q->querySource == QSRC_QUAL_INSTEAD_RULE)
					ereport(ERROR,
							(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							 errmsg("conditional DO INSTEAD rules are not supported for COPY")));
				if (q->querySource == QSRC_NON_INSTEAD_RULE)
					ereport(ERROR,
							(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							 errmsg("DO ALSO rules are not supported for the COPY")));
			}

			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("multi-statement DO INSTEAD rules are not supported for COPY")));
		}

		query = linitial_node(Query, rewritten);

		Assert(query->utilityStmt == NULL);

		/*
		 * Similarly the grammar doesn't enforce the presence of a RETURNING
		 * clause, but this is required here.
		 */
		if (query->commandType != CMD_SELECT &&
			query->returningList == NIL)
		{
			Assert(query->commandType == CMD_INSERT ||
				   query->commandType == CMD_UPDATE ||
				   query->commandType == CMD_DELETE);

			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("COPY query must have a RETURNING clause")));
		}

		/* plan the query */
		plan = pg_plan_query(query, CURSOR_OPT_PARALLEL_OK, NULL, 0, false);

		/*
		 * With row level security and a user using "COPY relation TO", we
		 * have to convert the "COPY relation TO" to a query-based COPY (eg:
		 * "COPY (SELECT * FROM relation) TO"), to allow the rewriter to add
		 * in any RLS clauses.
		 *
		 * When this happens, we are passed in the relid of the originally
		 * found relation (which we have locked).  As the planner will look up
		 * the relation again, we double-check here to make sure it found the
		 * same one that we have locked.
		 */
		if (queryRelId != InvalidOid)
		{
			/*
			 * Note that with RLS involved there may be multiple relations,
			 * and while the one we need is almost certainly first, we don't
			 * make any guarantees of that in the planner, so check the whole
			 * list and make sure we find the original relation.
			 */
			if (!list_member_oid(plan->relationOids, queryRelId))
				ereport(ERROR,
						(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
						 errmsg("relation referenced by COPY statement has changed")));
		}

		/*
		 * Use a snapshot with an updated command ID to ensure this query sees
		 * results of any previously executed queries.
		 */
		PushCopiedSnapshot(GetActiveSnapshot());
		UpdateActiveSnapshotCommandId();

		/* Create dest receiver for COPY OUT */
		dest = CreateDestReceiver(DestCopyOut);
		((DR_copy *)dest)->cstate = cstate;

		/* Create a QueryDesc requesting no output */
		cstate->queryDesc = CreateQueryDesc(plan, pstate->p_sourcetext,
											GetActiveSnapshot(),
											InvalidSnapshot,
											dest, NULL, NULL, 0);

		/*
		 * Call ExecutorStart to prepare the plan for execution.
		 *
		 * ExecutorStart computes a result tupdesc for us
		 */
		if (query->returningList != NIL)
			ExecutorStart(cstate->queryDesc, EXEC_FLAG_RETURNING);
		else
			ExecutorStart(cstate->queryDesc, 0);

		tupDesc = cstate->queryDesc->tupDesc;
	}

	/* Generate or convert list of attributes to process */
	cstate->attnumlist = CopyGetAttnums(tupDesc, cstate->rel, attnamelist);

	if (cstate->fixed_mode)
	{
		transformFormatter(cstate, tupDesc, cstate->rel, attnamelist);
	}

	num_phys_attrs = tupDesc->natts;

	/* Convert FORCE_QUOTE name list to per-column flags, check validity */
	cstate->force_quote_flags = (bool *)palloc0(num_phys_attrs * sizeof(bool));
	if (cstate->force_quote_all)
	{
		int i;

		for (i = 0; i < num_phys_attrs; i++)
			cstate->force_quote_flags[i] = true;
	}
	else if (cstate->force_quote)
	{
		List *attnums;
		ListCell *cur;

		attnums = CopyGetAttnums(tupDesc, cstate->rel, cstate->force_quote);

		foreach (cur, attnums)
		{
			int			attnum = lfirst_int(cur);
			Form_pg_attribute attr = TupleDescAttr(tupDesc, attnum - 1);

			if (!list_member_int(cstate->attnumlist, attnum))
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_COLUMN_REFERENCE),
						 errmsg("FORCE_QUOTE column \"%s\" not referenced by COPY",
								NameStr(attr->attname))));
			cstate->force_quote_flags[attnum - 1] = true;
		}
	}

	/* Convert FORCE_NOT_NULL name list to per-column flags, check validity */
	cstate->force_notnull_flags = (bool *)palloc0(num_phys_attrs * sizeof(bool));
	if (cstate->force_notnull)
	{
		List *attnums;
		ListCell *cur;

		attnums = CopyGetAttnums(tupDesc, cstate->rel, cstate->force_notnull);

		foreach (cur, attnums)
		{
			int			attnum = lfirst_int(cur);
			Form_pg_attribute attr = TupleDescAttr(tupDesc, attnum - 1);

			if (!list_member_int(cstate->attnumlist, attnum))
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_COLUMN_REFERENCE),
						 errmsg("FORCE_NOT_NULL column \"%s\" not referenced by COPY",
								NameStr(attr->attname))));
			cstate->force_notnull_flags[attnum - 1] = true;
		}
	}

	/* Convert FORCE_NULL name list to per-column flags, check validity */
	cstate->force_null_flags = (bool *)palloc0(num_phys_attrs * sizeof(bool));
	if (cstate->force_null)
	{
		List *attnums;
		ListCell *cur;

		attnums = CopyGetAttnums(tupDesc, cstate->rel, cstate->force_null);

		foreach (cur, attnums)
		{
			int			attnum = lfirst_int(cur);
			Form_pg_attribute attr = TupleDescAttr(tupDesc, attnum - 1);

			if (!list_member_int(cstate->attnumlist, attnum))
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_COLUMN_REFERENCE),
						 errmsg("FORCE_NULL column \"%s\" not referenced by COPY",
								NameStr(attr->attname))));
			cstate->force_null_flags[attnum - 1] = true;
		}
	}

	/* Convert convert_selectively name list to per-column flags */
	if (cstate->convert_selectively)
	{
		List *attnums;
		ListCell *cur;

		cstate->convert_select_flags = (bool *)palloc0(num_phys_attrs * sizeof(bool));

		attnums = CopyGetAttnums(tupDesc, cstate->rel, cstate->convert_select);

		foreach (cur, attnums)
		{
			int			attnum = lfirst_int(cur);
			Form_pg_attribute attr = TupleDescAttr(tupDesc, attnum - 1);

			if (!list_member_int(cstate->attnumlist, attnum))
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_COLUMN_REFERENCE),
						 errmsg_internal("selected column \"%s\" not referenced by COPY",
										 NameStr(attr->attname))));
			cstate->convert_select_flags[attnum - 1] = true;
		}
	}

	/* Use client encoding when ENCODING option is not specified. */
	if (cstate->file_encoding < 0)
		cstate->file_encoding = pg_get_client_encoding();

	/*
	 * Set up encoding conversion info.  Even if the file and server encodings
	 * are the same, we must apply pg_any_to_server() to validate data in
	 * multibyte encodings.
	 */
	cstate->need_transcoding =
		(cstate->file_encoding != GetDatabaseEncoding() ||
		 pg_database_encoding_max_length() > 1);
	/* See Multibyte encoding comment above */
	cstate->encoding_embeds_ascii = PG_ENCODING_IS_CLIENT_ONLY(cstate->file_encoding);

	cstate->copy_dest = COPY_FILE; /* default */

#ifdef PGXC
	/*
	 * We are here just at copy begin process,
	 * so only pick up the list of connections.
	 */
	if (IS_PGXC_COORDINATOR)
	{
		RemoteCopyData *remoteCopyState = cstate->remoteCopyState;

		/*
		 * In the case of CopyOut, it is just necessary to pick up one node randomly.
		 * This is done when rel_loc is found.
		 */
		if (remoteCopyState && remoteCopyState->rel_loc)
		{
			DataNodeCopyBegin(remoteCopyState);
			if (!remoteCopyState->locator)
				ereport(ERROR,
						(errcode(ERRCODE_CONNECTION_EXCEPTION),
						 errmsg("Failed to initialize Datanodes for COPY")));
		}
	}
#endif

	MemoryContextSwitchTo(oldcontext);

	return cstate;
}

/*
 * Closes the pipe to an external program, checking the pclose() return code.
 */
static void
ClosePipeToProgram(CopyState cstate)
{
	int pclose_rc;

	Assert(cstate->is_program);

	pclose_rc = ClosePipeStream(cstate->copy_file);
	if (pclose_rc == -1)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not close pipe to external command: %m")));
	else if (pclose_rc != 0)
		ereport(ERROR,
				(errcode(ERRCODE_EXTERNAL_ROUTINE_EXCEPTION),
				 errmsg("program \"%s\" failed",
						cstate->filename),
				 errdetail_internal("%s", wait_result_to_str(pclose_rc))));
}

/*
 * Release resources allocated in a cstate for COPY TO/FROM.
 */
static void
EndCopy(CopyState cstate)
{
	if (cstate->is_program)
	{
		ClosePipeToProgram(cstate);
	}
	else
	{
#ifdef __OPENTENBASE__
		if (cstate->filename != NULL && cstate->copy_file && FreeFile(cstate->copy_file))
#else
		if (cstate->filename != NULL && FreeFile(cstate->copy_file))
#endif
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not close file \"%s\": %m",
							cstate->filename)));
	}

	/* Clean up single row error handling related memory */
	if (cstate->srehandler)
        {
                ReportSrehResults(cstate->srehandler);
                destroyCopySrehandler(cstate->srehandler);
        }

	MemoryContextDelete(cstate->copycontext);
	pfree(cstate);
}

/*
 * Setup CopyState to read tuples from a table or a query for COPY TO.
 */
static CopyState
BeginCopyTo(ParseState *pstate,
			Relation rel,
			RawStmt *query,
			Oid queryRelId,
			const char *filename,
			bool is_program,
			List *attnamelist,
			List *options,
#ifdef _SHARDING_
			Bitmapset *shards
#endif
)
{
	CopyState cstate;
	bool pipe = (filename == NULL);
	MemoryContext oldcontext;

	if (rel != NULL && rel->rd_rel->relkind != RELKIND_RELATION)
	{
		if (rel->rd_rel->relkind == RELKIND_VIEW)
			ereport(ERROR,
					(errcode(ERRCODE_WRONG_OBJECT_TYPE),
					 errmsg("cannot copy from view \"%s\"",
							RelationGetRelationName(rel)),
					 errhint("Try the COPY (SELECT ...) TO variant.")));
		else if (rel->rd_rel->relkind == RELKIND_MATVIEW)
			ereport(ERROR,
					(errcode(ERRCODE_WRONG_OBJECT_TYPE),
					 errmsg("cannot copy from materialized view \"%s\"",
							RelationGetRelationName(rel)),
					 errhint("Try the COPY (SELECT ...) TO variant.")));
		else if (rel->rd_rel->relkind == RELKIND_FOREIGN_TABLE)
			ereport(ERROR,
					(errcode(ERRCODE_WRONG_OBJECT_TYPE),
					 errmsg("cannot copy from foreign table \"%s\"",
							RelationGetRelationName(rel)),
					 errhint("Try the COPY (SELECT ...) TO variant.")));
		else if (rel->rd_rel->relkind == RELKIND_SEQUENCE)
			ereport(ERROR,
					(errcode(ERRCODE_WRONG_OBJECT_TYPE),
					 errmsg("cannot copy from sequence \"%s\"",
							RelationGetRelationName(rel))));
		else if (rel->rd_rel->relkind == RELKIND_PARTITIONED_TABLE)
			ereport(ERROR,
					(errcode(ERRCODE_WRONG_OBJECT_TYPE),
					 errmsg("cannot copy from partitioned table \"%s\"",
							RelationGetRelationName(rel)),
					 errhint("Try the COPY (SELECT ...) TO variant.")));
		else
			ereport(ERROR,
					(errcode(ERRCODE_WRONG_OBJECT_TYPE),
					 errmsg("cannot copy from non-table relation \"%s\"",
							RelationGetRelationName(rel))));
	}

	cstate = BeginCopy(pstate, false, rel, query, queryRelId, attnamelist,
					   options,
#ifdef _SHARDING_
						shards,
#endif
						NIL, false
	);
	oldcontext = MemoryContextSwitchTo(cstate->copycontext);

	if (pipe)
	{
		Assert(!is_program); /* the grammar does not allow this */
		if (whereToSendOutput != DestRemote)
			cstate->copy_file = stdout;
	}
	else
	{
		cstate->filename = pstrdup(filename);
		cstate->is_program = is_program;

		if (is_program)
		{
			cstate->copy_file = OpenPipeStream(cstate->filename, PG_BINARY_W);
			if (cstate->copy_file == NULL)
				ereport(ERROR,
						(errcode_for_file_access(),
						 errmsg("could not execute command \"%s\": %m",
								cstate->filename)));
		}
		else
		{
			mode_t oumask; /* Pre-existing umask value */
			struct stat st;

			/*
			 * Prevent write to relative path ... too easy to shoot oneself in
			 * the foot by overwriting a database file ...
			 */
			if (!is_absolute_path(filename))
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_NAME),
						 errmsg("relative path not allowed for COPY to file")));

			oumask = umask(S_IWGRP | S_IWOTH);
			PG_TRY();
			{
				cstate->copy_file = AllocateFile(cstate->filename, PG_BINARY_W);
			}
			PG_CATCH();
			{
				umask(oumask);
				PG_RE_THROW();
			}
			PG_END_TRY();
			umask(oumask);
			if (cstate->copy_file == NULL)
			{
				/* copy errno because ereport subfunctions might change it */
				int save_errno = errno;

				ereport(ERROR,
						(errcode_for_file_access(),
						 errmsg("could not open file \"%s\" for writing: %m",
								cstate->filename),
						 (save_errno == ENOENT || save_errno == EACCES) ? errhint("COPY TO instructs the PostgreSQL server process to write a file. "
																				  "You may want a client-side facility such as psql's \\copy.")
																		: 0));
			}

			if (fstat(fileno(cstate->copy_file), &st))
				ereport(ERROR,
						(errcode_for_file_access(),
						 errmsg("could not stat file \"%s\": %m",
								cstate->filename)));

			if (S_ISDIR(st.st_mode))
				ereport(ERROR,
						(errcode(ERRCODE_WRONG_OBJECT_TYPE),
						 errmsg("\"%s\" is a directory", cstate->filename)));
		}
	}

	MemoryContextSwitchTo(oldcontext);

	return cstate;
}


/*
 * Set up CopyState for writing to a foreign or external table.
 */
CopyState
BeginCopyToForeignTable(Relation forrel, List *options)
{
	CopyState	cstate;

	Assert(forrel->rd_rel->relkind == RELKIND_FOREIGN_TABLE);

	cstate = BeginCopy(NULL, false, forrel,
					   NULL, /* raw_query */
					   InvalidOid,
					   NIL, options,
					   NULL, NIL, false);
	
	/* Check header */
	if (cstate->csv_mode && cstate->header_line)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("COPY TO HEADER not supported in foreign table.")));

	/*
	 * We use COPY_CALLBACK to mean that the each line should be
	 * left in fe_msgbuf. There is no actual callback!
	 */
	cstate->copy_dest = COPY_CALLBACK;

	/*
	 * Some more initialization, that in the normal COPY TO codepath, is done
	 * in CopyTo() itself.
	 */
	cstate->null_print_client = pstrdup(cstate->null_print);		/* default */

	return cstate;
}

/*
 * This intermediate routine exists mainly to localize the effects of setjmp
 * so we don't need to plaster a lot of variables with "volatile".
 */
static uint64
DoCopyTo(CopyState cstate)
{
	bool pipe = (cstate->filename == NULL);
	bool fe_copy = (pipe && whereToSendOutput == DestRemote);
	uint64 processed;

	PG_TRY();
	{
		if (fe_copy)
			SendCopyBegin(cstate);

		processed = CopyTo(cstate);

		if (fe_copy)
			SendCopyEnd(cstate);
	}
	PG_CATCH();
	{
		/*
		 * Make sure we turn off old-style COPY OUT mode upon error. It is
		 * okay to do this in all cases, since it does nothing if the mode is
		 * not on.
		 */
		pq_endcopyout(true);
		PG_RE_THROW();
	}
	PG_END_TRY();

	return processed;
}

/*
 * Clean up storage and release resources for COPY TO.
 */
void
EndCopyTo(CopyState cstate)
{
	if (cstate->queryDesc != NULL)
	{
		/* Close down the query and free resources. */
		ExecutorFinish(cstate->queryDesc);
		ExecutorEnd(cstate->queryDesc);
		FreeQueryDesc(cstate->queryDesc);
		PopActiveSnapshot();
	}

	/* Clean up storage */
	EndCopy(cstate);
}

/*
 * Copy from relation or query TO file.
 */
static uint64
CopyTo(CopyState cstate)
{
	TupleDesc	tupDesc;
	int			num_phys_attrs;
	ListCell   *cur;
	uint64		processed;

	if (cstate->rel)
		tupDesc = RelationGetDescr(cstate->rel);
	else
		tupDesc = cstate->queryDesc->tupDesc;
	num_phys_attrs = tupDesc->natts;
	cstate->null_print_client = pstrdup(cstate->null_print); /* default */

	/* We use fe_msgbuf as a per-row buffer regardless of copy_dest */
	cstate->fe_msgbuf = makeStringInfo();

	/* Get info about the columns we need to process. */
	cstate->out_functions = (FmgrInfo *)palloc(num_phys_attrs * sizeof(FmgrInfo));
	foreach (cur, cstate->attnumlist)
	{
		int			attnum = lfirst_int(cur);
		Oid			out_func_oid;
		bool		isvarlena;
		Form_pg_attribute attr = TupleDescAttr(tupDesc, attnum - 1);

		if (cstate->binary)
			getTypeBinaryOutputInfo(attr->atttypid,
									&out_func_oid,
									&isvarlena);
		else
			getTypeOutputInfo(attr->atttypid,
							  &out_func_oid,
							  &isvarlena);
		fmgr_info(out_func_oid, &cstate->out_functions[attnum - 1]);
	}

	/*
	 * Create a temporary memory context that we can reset once per row to
	 * recover palloc'd memory.  This avoids any problems with leaks inside
	 * datatype output routines, and should be faster than retail pfree's
	 * anyway.  (We don't need a whole econtext as CopyFrom does.)
	 */
	cstate->rowcontext = AllocSetContextCreate(CurrentMemoryContext,
											   "COPY TO",
											   ALLOCSET_DEFAULT_SIZES);

	if (cstate->binary)
	{
#ifdef PGXC
		if (IS_ACCESS_NODE)
		{
#endif
			/* Generate header for a binary copy */
			int32 tmp;

			/* Signature */
			CopySendData(cstate, BinarySignature, 11);
			/* Flags field */
			tmp = 0;
			if (cstate->oids)
				tmp |= (1 << 16);
#ifdef _PG_ORCL_
			if (cstate->rowids)
				tmp |= (1 << 17);
#endif
			CopySendInt32(cstate, tmp);
			/* No header extension */
			tmp = 0;
			CopySendInt32(cstate, tmp);

#ifdef PGXC
			/* Need to flush out the trailer */
			CopySendEndOfRow(cstate);
		}
#endif
	}
	else
	{
		/*
		 * For non-binary copy, we need to convert null_print to file
		 * encoding, because it will be sent directly with CopySendString.
		 */
		if (cstate->need_transcoding)
			cstate->null_print_client = pg_server_to_any(cstate->null_print,
														 cstate->null_print_len,
														 cstate->file_encoding);

		/* if a header has been requested send the line */
		if (cstate->header_line)
		{
			if (cstate->fixed_mode)
			{
				CopyFixedHeaderTo(cstate);
			}
			else
			{
				bool hdr_delim = false;

				foreach (cur, cstate->attnumlist)
				{
					int   attnum = lfirst_int(cur);
					char *colname;

					if (hdr_delim)
						CopySendString(cstate, cstate->delim);
					hdr_delim = true;

					colname = NameStr(TupleDescAttr(tupDesc, attnum - 1)->attname);

					CopyAttributeOutCSV(cstate,
					                    colname,
					                    false,
					                    list_length(cstate->attnumlist) == 1);
				}

				CopySendEndOfRow(cstate);
			}
		}
	}

#ifdef PGXC
	if (IS_PGXC_COORDINATOR &&
		cstate->remoteCopyState &&
		cstate->remoteCopyState->rel_loc)
	{
		RemoteCopyData *rcstate = cstate->remoteCopyState;
		processed = DataNodeCopyOut(
			(PGXCNodeHandle **)getLocatorNodeMap(rcstate->locator),
			getLocatorNodeCount(rcstate->locator),
			cstate->copy_dest == COPY_FILE ? cstate->copy_file : NULL);
	}
	else
	{
#endif
		if (cstate->rel)
		{
			Datum *values;
			bool *nulls;
			HeapScanDesc scandesc;
			HeapTuple tuple;
#ifdef _MLS_
			int16 cls_attnum = InvalidAttrNumber;
			Oid parent_oid = InvalidOid;
			bool has_datamask;
#endif
#ifdef _SHARDING_
			ShardID		shardid;
			AttrNumber  *discolnums = NULL;
			int         ndiscols = 0;
			int			error_shards = 0;

			if (RelationIsSharded(cstate->rel))
			{
				discolnums = RelationGetDisKeys(cstate->rel);
				ndiscols = RelationGetNumDisKeys(cstate->rel);
			}
#endif
#ifdef _MLS_
			if (cstate->rel->rd_cls_struct)
			{
				cls_attnum = cstate->rel->rd_cls_struct->attnum;
			}
#endif
#ifdef __OPENTENBASE__
			/* open partitions */
			parent_oid = mls_get_parent_oid(cstate->rel);

			has_datamask = datamask_check_table_has_datamask(parent_oid);
#endif

			values = (Datum *)palloc(num_phys_attrs * sizeof(Datum));
			nulls = (bool *)palloc(num_phys_attrs * sizeof(bool));

			{
				scandesc = heap_beginscan(cstate->rel, GetActiveSnapshot(), 0, NULL);
				
				processed = 0;
				while ((tuple = heap_getnext(scandesc, ForwardScanDirection)) != NULL)
				{
					CHECK_FOR_INTERRUPTS();

					/* Deconstruct the tuple ... faster than repeated heap_getattr */
					heap_deform_tuple(tuple, tupDesc, values, nulls);
#ifdef _SHARDING_
					if (cstate->shard_array)
					{
						shardid = (int) HeapTupleGetShardId(tuple);
						
						if (ShardIDIsValid(shardid) && !bms_is_member(shardid, cstate->shard_array))
							continue;
						
						//recoumpute shardid
						if (!ShardIDIsValid(shardid) && RelationIsSharded(cstate->rel))
						{
							Datum *value = (Datum *) palloc(sizeof(Datum) * ndiscols);
							bool *isdisnull = (bool *) palloc(sizeof(bool) * ndiscols);
							Oid *typeOfDistCol = (Oid *) palloc(sizeof(Oid) * ndiscols);
							
							error_shards++;
							
							/* process sharding maping */
							if (ndiscols > 0)
							{
								int i = 0;
								for (i = 0; i < ndiscols; i++)
								{
									typeOfDistCol[i] = TupleDescAttr(RelationGetDescr(cstate->rel),
									                                 discolnums[i] - 1)->atttypid;
									value[i] = values[discolnums[i] - 1];
									isdisnull[i] = nulls[discolnums[i] - 1];
								}
							}
							
							shardid = EvaluateShardId(typeOfDistCol, isdisnull, value, ndiscols);
							pfree(value);
							pfree(isdisnull);
							pfree(typeOfDistCol);
							
							if (ShardIDIsValid(shardid) && !bms_is_member(shardid, cstate->shard_array))
								
								continue;
							
							if (!ShardIDIsValid(shardid))
							{
								elog(ERROR, "recompute shardid failed. shardid is %d", shardid);
							}
						}
					}
#endif
#ifdef _MLS_
					if (has_datamask)
					{
						datamask_exchange_all_cols_value_copy(cstate->rel->rd_att, values, nulls, parent_oid);
					}
					if (InvalidAttrNumber != cls_attnum)
					{
						if (!mls_cls_check_row_validation_in_copy(values[cls_attnum - 1]))
						{
							continue;
						}
					}
#endif
					/* Format and send the data */
					CopyOneRowTo(cstate, HeapTupleGetOid(tuple), values, nulls
					            );
					processed++;
				}
				
				heap_endscan(scandesc);
			}


			if (error_shards > 0)
				elog(WARNING, "there are %d tuples's shardid which shardis in invalid in relation %s(%d).",
					 error_shards,
					 RelationGetRelationName(cstate->rel),
					 RelationGetRelid(cstate->rel));

			pfree(values);
			pfree(nulls);
		}
		else
		{
			/* run the plan --- the dest receiver will send tuples */
			ExecutorRun(cstate->queryDesc, ForwardScanDirection, 0L, true);
			processed = ((DR_copy *)cstate->queryDesc->dest)->processed;
		}

#ifdef PGXC
	}
#endif

#ifdef PGXC
	/*
	 * In PGXC, it is not necessary for a Datanode to generate
	 * the trailer as Coordinator is in charge of it
	 */
	if (cstate->binary && IS_PGXC_COORDINATOR)
#else
	if (cstate->binary)
#endif
	{
		/* Generate trailer for a binary copy */
		CopySendInt16(cstate, -1);
		/* Need to flush out the trailer */
		CopySendEndOfRow(cstate);
	}

	MemoryContextDelete(cstate->rowcontext);

	return processed;
}

static int
MbLen(const char *mbstr, int encoding)
{
	return ((*pg_wchar_table[encoding].mblen)((const unsigned char *) mbstr));
}

static void
MbCutString(int encoding, char *str, int limitLen, size_t *padsize)
{
	int   len = 0;
	int   strsize = strlen(str);
	char *mbstr = str;
	int   enclen = 0;

	while (len < strsize)
	{
		int mbLen = MbLen(mbstr, encoding);
		enclen++;
		if (enclen > limitLen)
		{
			break;
		}
		len += mbLen;
		mbstr += mbLen;
	}

	*padsize = enclen > limitLen ? 0 : limitLen - enclen;
	if (len < strsize)
		str[len] = '\0';
}

/*
 * Send text representation of one attribute, with conversion and
 * fixed-length
 */
static void
CopyAttributeFixedOut(CopyState cstate, char *string, CopyAttrFixed *caf, bool need_transcoding, bool isHeader)
{
	char      *ptr;
	char      *outPtr;
	StringInfo outbuf = cstate->fe_msgbuf;
	size_t     padSize = 0;

	if (need_transcoding)
		ptr = pg_server_to_any(string, strlen(string), cstate->file_encoding);
	else
		ptr = string;

	if (cstate->formatter_with_cut || caf->with_cut)
	{
		MbCutString(cstate->file_encoding, ptr, caf->fix_len, &padSize);
	}
	else if (strlen(ptr) > caf->fix_len)
	{
		ereport(ERROR,
		        (errcode(ERRCODE_BAD_COPY_FILE_FORMAT),
		         errmsg("length of field \"%s\" longer than limit of \'%d\'",
		                caf->attname,
		                caf->fix_len)));
	}
	else
	{
		char *src = NULL;
		int   enclen = 0;
		int   len = 0;
		int   srcsize = strlen(ptr);

		src = ptr;
		enclen = 0;
		while (len < srcsize)
		{
			int clen = pg_encoding_mblen(cstate->file_encoding, src);
			enclen++;
			len += clen;
			src += clen;
		}
		padSize = caf->fix_len - enclen;
	}

	if (cstate->fe_msgbuf->maxlen < cstate->fe_msgbuf->len + strlen(ptr) + padSize + 1)
		enlargeStringInfo(cstate->fe_msgbuf, cstate->fe_msgbuf->len + strlen(ptr) + padSize + 1);
	
	outbuf = cstate->fe_msgbuf;
	outPtr = outbuf->data + outbuf->len;
	switch (isHeader ? ALIGN_LEFT : caf->align)
	{
		case ALIGN_LEFT:
			memcpy(outPtr, ptr, strlen(ptr));
			outPtr += strlen(ptr);
			if (padSize > 0)
			{
				memset(outPtr, ' ', padSize);
			}
			break;
		case ALIGN_RIGHT:
			if (padSize > 0)
			{
				memset(outPtr, '0', padSize);
				outPtr += padSize;
			}
			memcpy(outPtr, ptr, strlen(ptr));
			break;
		default:
			Assert(false);
	}
	outbuf->len += strlen(ptr) + padSize;
	outbuf->data[outbuf->len] = '\0';
}

void
CopyFixedHeaderTo(CopyState cstate)
{
	char          *colname = NULL;
	ListCell      *lc;
	CopyAttrFixed *caf;

	enlargeStringInfo(cstate->fe_msgbuf, cstate->formatter_size);

	foreach (lc, cstate->formatter)
	{
		caf = lfirst_node(CopyAttrFixed, lc);
		if (cstate->need_transcoding)
			colname = pg_server_to_any(caf->attname, strlen(caf->attname), cstate->file_encoding);
		else
			colname = caf->attname;

		CopyAttributeFixedOut(cstate, colname, caf, false, true);
	}
	/*
	 * Finish off the row: write it to the destination, and update the count.
	 * However, if we're in the context of a writable external table, we let
	 * the caller do it - send the data to its local external source (see
	 * external_insert() ).
	 */
	if (cstate->copy_dest != COPY_CALLBACK)
	{
		CopySendEndOfRow(cstate);
	}
}


void
CopyOneRowFixedTo(CopyState cstate, Oid tupleOid, Datum *values, bool *nulls
)
{
	FmgrInfo      *out_functions = cstate->out_functions;
	MemoryContext  oldcontext;
	char          *string;
	ListCell      *lc;
	CopyAttrFixed *caf;
	Datum          value;
	bool           isnull;

	MemoryContextReset(cstate->rowcontext);
	oldcontext = MemoryContextSwitchTo(cstate->rowcontext);
	enlargeStringInfo(cstate->fe_msgbuf, cstate->formatter_size);

	foreach (lc, cstate->formatter)
	{
		caf = lfirst_node(CopyAttrFixed, lc);
		Assert(caf->attnum != InvalidAttrNumber);

		if (cstate->oids && caf->attname && strcasecmp(caf->attname, "OID") == 0)
		{
			string = DatumGetCString(DirectFunctionCall1(oidout,
														 ObjectIdGetDatum(tupleOid)));
			CopyAttributeFixedOut(cstate, string, caf, cstate->need_transcoding, false);
			continue;
		}
		value = values[caf->attnum - 1];
		isnull = nulls[caf->attnum - 1];

		if (isnull)
		{
			CopyAttributeFixedOut(cstate, cstate->null_print_client, caf, false, false);
		}
		else
		{
			string = OutputFunctionCall(&out_functions[caf->attnum - 1], value);
			Assert(string != NULL);

			CopyAttributeFixedOut(cstate, string, caf, cstate->need_transcoding, false);
		}
	}
	/*
	 * Finish off the row: write it to the destination, and update the count.
	 * However, if we're in the context of a writable external table, we let
	 * the caller do it - send the data to its local external source (see
	 * external_insert() ).
	 */
	if (cstate->copy_dest != COPY_CALLBACK)
	{
		CopySendEndOfRow(cstate);
	}
	MemoryContextSwitchTo(oldcontext);
}

/*
 * Emit one row during CopyTo().
 */
extern void
CopyOneRowTo(CopyState cstate, Oid tupleOid, Datum *values, bool *nulls
						)
{
	bool need_delim = false;
	FmgrInfo *out_functions = cstate->out_functions;
	MemoryContext oldcontext;
	ListCell *cur;
	char *string;


	if (cstate->fixed_mode)
	{
		CopyOneRowFixedTo(cstate, tupleOid, values, nulls
		);
		return;
	}

	MemoryContextReset(cstate->rowcontext);
	oldcontext = MemoryContextSwitchTo(cstate->rowcontext);

	if (cstate->binary)
	{
		/* Binary per-tuple header */
		CopySendInt16(cstate, list_length(cstate->attnumlist));
		/* Send OID if wanted --- note attnumlist doesn't include it */
		if (cstate->oids)
		{
			/* Hack --- assume Oid is same size as int32 */
			CopySendInt32(cstate, sizeof(int32));
			CopySendInt32(cstate, tupleOid);
		}


	}
	else
	{
		/* Text format has no per-tuple header, but send OID if wanted */
		/* Assume digits don't need any quoting or encoding conversion */
		if (cstate->oids)
		{
			string = DatumGetCString(DirectFunctionCall1(oidout,
														 ObjectIdGetDatum(tupleOid)));
			CopySendString(cstate, string);
			need_delim = true;
		}

	}

	foreach (cur, cstate->attnumlist)
	{
		int attnum = lfirst_int(cur);
		Datum value = values[attnum - 1];
		bool isnull = nulls[attnum - 1];

		if (!cstate->binary)
		{
			if (need_delim)
				CopySendString(cstate, cstate->delim);
			need_delim = true;
		}

		if (isnull)
		{
			if (!cstate->binary)
				CopySendString(cstate, cstate->null_print_client);
			else
				CopySendInt32(cstate, -1);
		}
		else
		{
			if (!cstate->binary)
			{
				bool is_prepend_zero = false;

				string = OutputFunctionCall(&out_functions[attnum - 1],
						value);

				/*
				 * The leading zero of numeric is missed in Oralce mode, it
				 * would be result in unexpected error when using copy command,
				 * so prepend leading zero to numeric to resolve this issue.
				 */
				if (ORA_MODE && out_functions[attnum - 1].fn_oid == NUMERIC_OUT_OID && string[0] == '.')
				{
					StringInfoData numstr;

					initStringInfo(&numstr);
					appendStringInfoChar(&numstr, '0');
					appendStringInfoString(&numstr, string);
					pfree(string);
					string = numstr.data;
				}

				if (cstate->csv_mode)
					CopyAttributeOutCSV(cstate, string,
						cstate->force_quote_flags[attnum - 1],
						list_length(cstate->attnumlist) == 1);
				else
					CopyAttributeOutText(cstate, string);

				if (is_prepend_zero)
					pfree(string);
			}
			else
			{
				bytea *outputbytes;

				outputbytes = SendFunctionCall(&out_functions[attnum - 1],
											   value);
				CopySendInt32(cstate, VARSIZE(outputbytes) - VARHDRSZ);
				CopySendData(cstate, VARDATA(outputbytes),
							 VARSIZE(outputbytes) - VARHDRSZ);
			}
		}
	}

	/*
	 * Finish off the row: write it to the destination, and update the count.
	 * However, if we're in the context of a writable external table, we let
	 * the caller do it - send the data to its local external source (see
	 * external_insert() ).
	 */
	if (cstate->copy_dest != COPY_CALLBACK)
	{
		CopySendEndOfRow(cstate);
	}

	MemoryContextSwitchTo(oldcontext);
}

/*
 * error context callback for COPY FROM
 *
 * The argument for the error context must be CopyState.
 */
void CopyFromErrorCallback(void *arg)
{
	CopyState cstate = (CopyState)arg;

	copy_illegal_chars_conversion = false;
	copy_illegal_conv_char = 0;

	if (cstate->binary)
	{
		/* can't usefully display the data */
		if (cstate->cur_attname)
			errcontext("COPY %s, line %d, column %s, nodetype:%d(1:cn,0:dn)",
					   cstate->cur_relname, cstate->cur_lineno,
					   cstate->cur_attname, IS_PGXC_COORDINATOR);
		else
			errcontext("COPY %s, line %d, nodetype:%d(1:cn,0:dn)",
					   cstate->cur_relname, cstate->cur_lineno, IS_PGXC_COORDINATOR);
	}
	else
	{
		if (cstate->cur_attname && cstate->cur_attval)
		{
			/* error is relevant to a particular column */
			char *attval;

			attval = limit_printout_length(cstate->cur_attval);
			errcontext("COPY %s, line %d, column %s: \"%s\", nodetype:%d(1:cn,0:dn)",
					   cstate->cur_relname, cstate->cur_lineno,
					   cstate->cur_attname, attval, IS_PGXC_COORDINATOR);
			pfree(attval);
		}
		else if (cstate->cur_attname)
		{
			/* error is relevant to a particular column, value is NULL */
			errcontext("COPY %s, line %d, column %s: null input, nodetype:%d(1:cn,0:dn)",
					   cstate->cur_relname, cstate->cur_lineno,
					   cstate->cur_attname, IS_PGXC_COORDINATOR);
		}
		else
		{
			/*
			 * Error is relevant to a particular line.
			 *
			 * If line_buf still contains the correct line, and it's already
			 * transcoded, print it. If it's still in a foreign encoding, it's
			 * quite likely that the error is precisely a failure to do
			 * encoding conversion (ie, bad data). We dare not try to convert
			 * it, and at present there's no way to regurgitate it without
			 * conversion. So we have to punt and just report the line number.
			 */
			if (cstate->line_buf_valid &&
				(cstate->line_buf_converted || !cstate->need_transcoding))
			{
				char *lineval;

				lineval = limit_printout_length(cstate->line_buf.data);
				errcontext("COPY %s, line %d: \"%s\", nodetype:%d(1:cn,0:dn)",
						   cstate->cur_relname, cstate->cur_lineno, lineval, IS_PGXC_COORDINATOR);
				pfree(lineval);
			}
			else
			{
				errcontext("COPY %s, line %d, nodetype:%d(1:cn,0:dn)",
						   cstate->cur_relname, cstate->cur_lineno, IS_PGXC_COORDINATOR);
			}
		}
	}
}

/*
 * Make sure we don't print an unreasonable amount of COPY data in a message.
 *
 * It would seem a lot easier to just use the sprintf "precision" limit to
 * truncate the string.  However, some versions of glibc have a bug/misfeature
 * that vsnprintf will always fail (return -1) if it is asked to truncate
 * a string that contains invalid byte sequences for the current encoding.
 * So, do our own truncation.  We return a pstrdup'd copy of the input.
 */
char *
limit_printout_length(const char *str)
{
#define MAX_COPY_DATA_DISPLAY 100

	int slen = strlen(str);
	int len;
	char *res;

	/* Fast path if definitely okay */
	if (slen <= MAX_COPY_DATA_DISPLAY)
		return pstrdup(str);

	/* Apply encoding-dependent truncation */
	len = pg_mbcliplen(str, slen, MAX_COPY_DATA_DISPLAY);

	/*
	 * Truncate, and add "..." to show we truncated the input.
	 */
	res = (char *)palloc(len + 4);
	memcpy(res, str, len);
	strcpy(res + len, "...");

	return res;
}

/* remove end of line chars from end of a buffer */
void truncateEol(StringInfo buf, EolType eol_type)
{
	int one_back = buf->len - 1;
	int two_back = buf->len - 2;

	if(eol_type == EOL_CRNL)
	{
		if(buf->len < 2)
			return;

		if(buf->data[two_back] == '\r' &&
		   buf->data[one_back] == '\n')
		{
			buf->data[two_back] = '\0';
			buf->data[one_back] = '\0';
			buf->len -= 2;
		}
	}
	else
	{
		if(buf->len < 1)
			return;

		if(buf->data[one_back] == '\r' ||
		   buf->data[one_back] == '\n')
		{
			buf->data[one_back] = '\0';
			buf->len--;
		}
	}
}


/* wrapper for truncateEol */
void
truncateEolStr(char *str, EolType eol_type)
{
	StringInfoData buf;

	buf.data = str;
	buf.len = strlen(str);
	buf.maxlen = buf.len;
	truncateEol(&buf, eol_type);
}

/*
 * Copy FROM file to relation.
 */
uint64
CopyFrom(CopyState cstate)
{
	HeapTuple tuple = NULL;
	TupleDesc tupDesc;
	Datum *values;
	bool *nulls;
	ResultRelInfo *resultRelInfo;
	ResultRelInfo *saved_resultRelInfo = NULL;
	EState *estate = CreateExecutorState(); /* for ExecConstraints() */
	TupleTableSlot *myslot;
	MemoryContext oldcontext = CurrentMemoryContext;
	ErrorContextCallback errcallback;
	CommandId mycid = GetCurrentCommandId(true);
	int hi_options = 0; /* start with default heap_insert options */
	BulkInsertState bistate;
	uint64 processed = 0;
	bool useHeapMultiInsert;
	int nBufferedTuples = 0;
	int prev_leaf_part_index = -1;

#define MAX_BUFFERED_TUPLES 1000
	HeapTuple *bufferedTuples = NULL; /* initialize to silence warning */
	Size bufferedTuplesSize = 0;
	int firstBufferedLineNo = 0;
#ifdef __OPENTENBASE__
	bool nomore = false;
#endif

    Assert(cstate->rel);

	/*
	 * The target must be a plain relation or have an INSTEAD OF INSERT row
	 * trigger.  (Currently, such triggers are only allowed on views, so we
	 * only hint about them in the view case.)
	 */
	if (cstate->rel->rd_rel->relkind != RELKIND_RELATION &&
		cstate->rel->rd_rel->relkind != RELKIND_PARTITIONED_TABLE &&
		!(cstate->rel->trigdesc &&
		  cstate->rel->trigdesc->trig_insert_instead_row))
	{
		if (cstate->rel->rd_rel->relkind == RELKIND_VIEW)
			ereport(ERROR,
					(errcode(ERRCODE_WRONG_OBJECT_TYPE),
					 errmsg("cannot copy to view \"%s\"",
							RelationGetRelationName(cstate->rel)),
					 errhint("To enable copying to a view, provide an INSTEAD OF INSERT trigger.")));
		else if (cstate->rel->rd_rel->relkind == RELKIND_MATVIEW)
			ereport(ERROR,
					(errcode(ERRCODE_WRONG_OBJECT_TYPE),
					 errmsg("cannot copy to materialized view \"%s\"",
							RelationGetRelationName(cstate->rel))));
		else if (cstate->rel->rd_rel->relkind == RELKIND_FOREIGN_TABLE)
			ereport(ERROR,
					(errcode(ERRCODE_WRONG_OBJECT_TYPE),
					 errmsg("cannot copy to foreign table \"%s\"",
							RelationGetRelationName(cstate->rel))));
		else if (cstate->rel->rd_rel->relkind == RELKIND_SEQUENCE)
			ereport(ERROR,
					(errcode(ERRCODE_WRONG_OBJECT_TYPE),
					 errmsg("cannot copy to sequence \"%s\"",
							RelationGetRelationName(cstate->rel))));
		else
			ereport(ERROR,
					(errcode(ERRCODE_WRONG_OBJECT_TYPE),
					 errmsg("cannot copy to non-table relation \"%s\"",
							RelationGetRelationName(cstate->rel))));
	}

	/*
	 * Data copy is inserted at DN, but trigger that could not be pushed down can only be triggered at CN,
	 * where necessary interception is performed
	 */
	if (!IS_CENTRALIZED_DATANODE &&
			cstate->rel->rd_rel->relkind ==  RELKIND_RELATION &&
			cstate->rel->trigdesc &&
			(cstate->rel->trigdesc->trig_pullup & TRIGGER_TYPE_INSERT))
	{
		/*
		 * We will prevent triggers when performing data migration,
		 * so no trigger will be fired during data migration. This
		 * check is not necessary for data migration.
		 */
		if (!(MyLogicalRepWorker != NULL && IS_PGXC_DATANODE))
			ereport(ERROR,
					(errcode(ERRCODE_WRONG_OBJECT_TYPE),
							errmsg("cannot copy to table with foreign key or insertTrigger that could not be pushed down")));
	}

	tupDesc = RelationGetDescr(cstate->rel);

	/*----------
	 * Check to see if we can avoid writing WAL
	 *
	 * If archive logging/streaming is not enabled *and* either
	 *	- table was created in same transaction as this COPY
	 *	- data is being written to relfilenode created in this transaction
	 * then we can skip writing WAL.  It's safe because if the transaction
	 * doesn't commit, we'll discard the table (or the new relfilenode file).
	 * If it does commit, we'll have done the heap_sync at the bottom of this
	 * routine first.
	 *
	 * As mentioned in comments in utils/rel.h, the in-same-transaction test
	 * is not always set correctly, since in rare cases rd_newRelfilenodeSubid
	 * can be cleared before the end of the transaction. The exact case is
	 * when a relation sets a new relfilenode twice in same transaction, yet
	 * the second one fails in an aborted subtransaction, e.g.
	 *
	 * BEGIN;
	 * TRUNCATE t;
	 * SAVEPOINT save;
	 * TRUNCATE t;
	 * ROLLBACK TO save;
	 * COPY ...
	 *
	 * Also, if the target file is new-in-transaction, we assume that checking
	 * FSM for free space is a waste of time, even if we must use WAL because
	 * of archiving.  This could possibly be wrong, but it's unlikely.
	 *
	 * The comments for heap_insert and RelationGetBufferForTuple specify that
	 * skipping WAL logging is only safe if we ensure that our tuples do not
	 * go into pages containing tuples from any other transactions --- but this
	 * must be the case if we have a new table or new relfilenode, so we need
	 * no additional work to enforce that.
	 *
	 * We currently don't support this optimization if the COPY target is a
	 * partitioned table as we currently only lazily initialize partition
	 * information when routing the first tuple to the partition.  We cannot
	 * know at this stage if we can perform this optimization.  It should be
	 * possible to improve on this, but it does mean maintaining heap insert
	 * option flags per partition and setting them when we first open the
	 * partition.
	 *----------
	 */
	/* createSubid is creation check, newRelfilenodeSubid is truncation check */
	if (cstate->rel->rd_rel->relkind != RELKIND_PARTITIONED_TABLE &&
		(cstate->rel->rd_createSubid != InvalidSubTransactionId ||
		 cstate->rel->rd_newRelfilenodeSubid != InvalidSubTransactionId))
	{
		hi_options |= HEAP_INSERT_SKIP_FSM;
		if (!XLogIsNeeded())
			hi_options |= HEAP_INSERT_SKIP_WAL;
	}

	/*
	 * Optimize if new relfilenode was created in this subxact or one of its
	 * committed children and we won't see those rows later as part of an
	 * earlier scan or command. This ensures that if this subtransaction
	 * aborts then the frozen rows won't be visible after xact cleanup. Note
	 * that the stronger test of exactly which subtransaction created it is
	 * crucial for correctness of this optimization.
	 */
	if (cstate->freeze)
	{
		/*
		* We currently disallow COPY FREEZE on partitioned tables.  The
		* reason for this is that we've simply not yet opened the partitions
		* to determine if the optimization can be applied to them.  We could
		* go and open them all here, but doing so may be quite a costly
		* overhead for small copies.  In any case, we may just end up routing
		* tuples to a small number of partitions.  It seems better just to
		* raise an ERROR for partitioned tables.
		*/
		if (cstate->rel->rd_rel->relkind == RELKIND_PARTITIONED_TABLE)
		{
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("cannot perform COPY FREEZE on a partitioned table")));
		}

		if (!ThereAreNoPriorRegisteredSnapshots() || !ThereAreNoReadyPortals())
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_TRANSACTION_STATE),
					 errmsg("cannot perform COPY FREEZE because of prior transaction activity")));

		if (cstate->rel->rd_createSubid != GetCurrentSubTransactionId() &&
			cstate->rel->rd_newRelfilenodeSubid != GetCurrentSubTransactionId())
			ereport(ERROR,
					(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
					 errmsg("cannot perform COPY FREEZE because the table was not created or truncated in the current subtransaction")));

		hi_options |= HEAP_INSERT_FROZEN;
	}

	/*
	 * We need a ResultRelInfo so we can use the regular executor's
	 * index-entry-making machinery.  (There used to be a huge amount of code
	 * here that basically duplicated execUtils.c ...)
	 */
	resultRelInfo = makeNode(ResultRelInfo);

	InitResultRelInfo(resultRelInfo,
					  cstate->rel,
					  1, /* dummy rangetable index */
					  NULL,
					  0,
					  false);

	ExecOpenIndices(resultRelInfo, false);

	estate->es_result_relations = resultRelInfo;
	estate->es_num_result_relations = 1;
	estate->es_result_relation_info = resultRelInfo;
	estate->es_range_table = cstate->range_table;

	/* Set up a tuple slot too */
	myslot = ExecInitExtraTupleSlot(estate, tupDesc);
	/* Triggers might need a slot as well */
	estate->es_trig_tuple_slot = ExecInitExtraTupleSlot(estate, NULL);

	/* Prepare to catch AFTER triggers. */
	AfterTriggerBeginQuery();

	/*
	 * It's more efficient to prepare a bunch of tuples for insertion, and
	 * insert them in one heap_multi_insert() call, than call heap_insert()
	 * separately for every tuple. However, we can't do that if there are
	 * BEFORE/INSTEAD OF triggers, or we need to evaluate volatile default
	 * expressions. Such triggers or expressions might query the table we're
	 * inserting to, and act differently if the tuples that have already been
	 * processed and prepared for insertion are not there.  We also can't do
	 * it if the table is partitioned.
	 */
	if ((resultRelInfo->ri_TrigDesc != NULL &&
		 (resultRelInfo->ri_TrigDesc->trig_insert_before_row ||
		  resultRelInfo->ri_TrigDesc->trig_insert_instead_row)) ||
		cstate->partition_tuple_routing != NULL ||
		cstate->volatile_defexprs)
	{
		useHeapMultiInsert = false;
	}
	else
	{
		useHeapMultiInsert = true;
		bufferedTuples = palloc(MAX_BUFFERED_TUPLES * sizeof(HeapTuple));
	}

	/*
	 * Check BEFORE STATEMENT insertion triggers. It's debatable whether we
	 * should do this for COPY, since it's not really an "INSERT" statement as
	 * such. However, executing these triggers maintains consistency with the
	 * EACH ROW triggers that we already fire on COPY.
	 */
	ExecBSInsertTriggers(estate, resultRelInfo);

	values = (Datum *)palloc(tupDesc->natts * sizeof(Datum));
	nulls = (bool *)palloc(tupDesc->natts * sizeof(bool));

	bistate = GetBulkInsertState();

	/* Set up callback to identify error line number */
	errcallback.callback = CopyFromErrorCallback;
	errcallback.arg = (void *)cstate;
	errcallback.previous = error_context_stack;
	error_context_stack = &errcallback;
	copy_illegal_chars_conversion = cstate->compatible_illegal_chars;
	copy_illegal_conv_char = cstate->compatible_illegal_chars ? cstate->illegal_conv_chars[0] : 0;

	for (;;)
	{
		TupleTableSlot *slot;
		bool skip_tuple;
		bool next_copy_from_have_data = false;
		Oid loaded_oid = InvalidOid;

#ifdef _PG_ORCL_
		RowId		loaded_rowid = InvalidRowId;
#endif

		CHECK_FOR_INTERRUPTS();
		if (nBufferedTuples == 0)
		{
			/*
				* Reset the per-tuple exprcontext. We can only do this if the
				* tuple buffer is empty. (Calling the context the per-tuple
				* memory context is a bit of a misnomer now.)
				*/
			ResetPerTupleExprContext(estate);
		}

		/* Switch into its memory context */
		MemoryContextSwitchTo(GetPerTupleMemoryContext(estate));
#ifdef __OPENTENBASE_C__
		if (cstate->partition_tuple_routing && IS_PGXC_DATANODE)
		{
			/*
			* the entry is the parent table of the partitioned table, read a record for partition routing
			*/

			next_copy_from_have_data = NextCopyFrom(cstate, estate, myslot->tts_values, myslot->tts_isnull, &loaded_oid
#ifdef _PG_ORCL_
								, &loaded_rowid
#endif
											);
			if (!next_copy_from_have_data)
			{	
				break;		
			}
		}
		else
		{
			/* skip error line when copy from */
#ifdef _MLS_
			if (g_enable_copy_silence)
			{
			readnextline_row:
				PG_TRY();
				{
					if (!NextCopyFrom(cstate, estate, values, nulls, &loaded_oid
#ifdef _PG_ORCL_
											, &loaded_rowid
#endif
												))
					{
						nomore = true;
					}
				}
				PG_CATCH();
				{
#ifdef __OPENTENBASE__
					if (cstate->insert_into)
					{
						PG_RE_THROW();
					}
					else
					{
#endif
						HOLD_INTERRUPTS();
						EmitErrorReport();
						FlushErrorState();
						RESUME_INTERRUPTS();
						/*
						 * omit errors from input files, PG_exception_stack has been reset.
						 */
						goto readnextline_row;
#ifdef __OPENTENBASE__
					}
#endif
				}
				PG_END_TRY();

				/*
				 * after PG_END_TRY(), break action would be allowed.
				 */
				if (nomore)
				{
					break;
				}
			}
			else
			{
#endif
				if (!NextCopyFrom(cstate, estate, values, nulls, &loaded_oid
#ifdef _PG_ORCL_
								, &loaded_rowid
#endif
											))
					break;
#ifdef __OPENTENBASE__
			}
#endif
		}
#else
		if (!NextCopyFrom(cstate, econtext, values, nulls, &loaded_oid
#ifdef _PG_ORCL_
								, &loaded_rowid
#endif
											))
			break;
#endif
		/*
		 * Send the data row as-is to the Datanodes. If default values
		 * are to be inserted, append them onto the data row.
		 */
		if (IS_PGXC_COORDINATOR && cstate->remoteCopyState->rel_loc)
		{
			RemoteCopyData *rcstate = cstate->remoteCopyState;
			Datum          *disvalues = NULL;
			bool           *disisnulls = NULL;

			if (rcstate->rel_loc->nDisAttrs > 0)
			{
				int i = 0;
				disvalues = (Datum *)palloc(sizeof(Datum) * rcstate->rel_loc->nDisAttrs);
				disisnulls = (bool *)palloc(sizeof(bool) * rcstate->rel_loc->nDisAttrs);
				for (i = 0; i < rcstate->rel_loc->nDisAttrs; i++)
				{
					disvalues[i] = values[rcstate->rel_loc->disAttrNums[i] - 1];
					disisnulls[i] = nulls[rcstate->rel_loc->disAttrNums[i] - 1];
				}
			}

			if (DataNodeCopyIn(
#ifdef _PG_ORCL_
							(cstate->rid_glob_seqId == InvalidOid || cstate->binary ||
							 (!cstate->binary && cstate->insert_into && cstate->data_list)) ?
									cstate->line_buf.data : cstate->text_line_buf_rid.data,
							(cstate->rid_glob_seqId == InvalidOid || cstate->binary ||
							 (!cstate->binary && cstate->insert_into && cstate->data_list)) ?
									cstate->line_buf.len : cstate->text_line_buf_rid.len,
							cstate->eol,
#else
							cstate->line_buf.data, cstate->line_buf.len, cstate->eol,
#endif
							GET_NODES(rcstate->locator, disvalues,
									  disisnulls, rcstate->rel_loc->nDisAttrs,
									  NULL),
							(PGXCNodeHandle **)getLocatorResults(rcstate->locator),
#ifdef __OPENTENBASE__
							(cstate->binary || cstate->insert_into)))
#else
							cstate->binary))
#endif
			{
				int conn_count;
				int loop;
				PGXCNodeHandle **copy_connections;
				StringInfoData sqldata;
				StringInfo sql;

				sql = &sqldata;

				initStringInfo(sql);
                conn_count = GET_NODES(rcstate->locator, disvalues,
									   disisnulls, rcstate->rel_loc->nDisAttrs,
									   NULL);
				copy_connections = (PGXCNodeHandle **)getLocatorResults(rcstate->locator);
				for (loop = 0; loop < conn_count; loop++)
				{
					PGXCNodeHandle *handle = copy_connections[loop];
					if ('\0' != handle->error[0])
					{
						appendStringInfo(sql, "%s;", handle->error);
					}
				}

				ereport(ERROR,
						(errcode(ERRCODE_CONNECTION_EXCEPTION),
						 errmsg("Copy failed on a data node:%s", sql->data)));
			}
			processed++;
			if (disvalues)
				pfree(disvalues);
			if (disisnulls)
				pfree(disisnulls);
		}
        else
        {
			Datum *vals;
			bool *nuls;

            /*
             * process row store and partitioned column store
             * And now we can form the input tuple.
             */
#ifdef _SHARDING_
			if (cstate->partition_tuple_routing && IS_PGXC_DATANODE)
			{
				vals = myslot->tts_values;
				nuls = myslot->tts_isnull;
			}
			else
			{
				vals = values;
				nuls = nulls;
			}

			if (RelationIsSharded(cstate->rel))
			{
				tuple = heap_form_tuple_plain(tupDesc, vals, nuls, RelationGetDisKeys(cstate->rel),
												RelationGetNumDisKeys(cstate->rel), RelationGetRelid(cstate->rel));

				if (IsShardBarriered(cstate->rel->rd_node, HeapTupleGetShardId(tuple)))
					elog(ERROR, "shard is be vacuuming now, so it is forbidden to write.");
			}
			else
			{
				tuple = heap_form_tuple(tupDesc, vals, nuls);
			}
#endif
			if (loaded_oid != InvalidOid)
				HeapTupleSetOid(tuple, loaded_oid);


			/*
			 * Constraints might reference the tableoid column, so initialize
			 * t_tableOid before evaluating them.
			 */
			tuple->t_tableOid = RelationGetRelid(resultRelInfo->ri_RelationDesc);

			/* Triggers and stuff need to be invoked in query context. */
			MemoryContextSwitchTo(oldcontext);

			/* Place tuple in tuple slot --- but slot shouldn't free it */
			slot = myslot;
			ExecStoreHeapTuple(tuple, slot, false);

            /* Determine the partition to heap_insert the tuple into */
            if (cstate->partition_tuple_routing)
            {
				int leaf_part_index;
				TupleConversionMap *map;
				PartitionTupleRouting *proute = cstate->partition_tuple_routing;

                /*
                 * Away we go ... If we end up not finding a partition after all,
                 * ExecFindPartition() does not return and errors out instead.
                 * Otherwise, the returned value is to be used as an index into
                 * arrays mt_partitions[] and mt_partition_tupconv_maps[] that
                 * will get us the ResultRelInfo and TupleConversionMap for the
                 * partition, respectively.
                 */
                leaf_part_index = ExecFindPartition(resultRelInfo,
                                                    proute->partition_dispatch_info,
                                                    slot,
                                                    estate);
                Assert(leaf_part_index >= 0 &&
                       leaf_part_index < proute->num_partitions);

                /*
                 * If this tuple is mapped to a partition that is not same as the
                 * previous one, we'd better make the bulk insert mechanism gets a
                 * new buffer.
                 */
                if (prev_leaf_part_index != leaf_part_index)
                {
                    ReleaseBulkInsertStatePin(bistate);
                    prev_leaf_part_index = leaf_part_index;
                }

                /*
                 * Save the old ResultRelInfo and switch to the one corresponding
                 * to the selected partition.
                 */
                saved_resultRelInfo = resultRelInfo;
                resultRelInfo = proute->partitions[leaf_part_index];
                if (resultRelInfo == NULL)
				{
						resultRelInfo = ExecInitPartitionInfo(NULL,
																saved_resultRelInfo,
																proute, estate,
																leaf_part_index, InvalidOid);
						Assert(resultRelInfo != NULL);
				}

                /* We do not yet have a way to insert into a foreign partition */
                if (resultRelInfo->ri_FdwRoutine)
                    ereport(ERROR,
                            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                                    errmsg("cannot route inserted tuples to a foreign table")));

                /*
                 * For ExecInsertIndexTuples() to work on the partition's indexes
                 */
                estate->es_result_relation_info = resultRelInfo;

                /*
                 * If we're capturing transition tuples, we might need to convert
                 * from the partition rowtype to parent rowtype.
                 */
                if (cstate->transition_capture != NULL)
                {
                    if (resultRelInfo->ri_TrigDesc &&
                        resultRelInfo->ri_TrigDesc->trig_insert_before_row)
                    {
                        /*
                         * If there are any BEFORE triggers on the partition,
						 * we'll have to be ready to convert their result back to
                         * tuplestore format.
                         */
                        cstate->transition_capture->tcs_original_insert_tuple = NULL;
                        cstate->transition_capture->tcs_map =
                                TupConvMapForLeaf(proute, saved_resultRelInfo,
                                                  leaf_part_index);
                    }
                    else
                    {
                        /*
                         * Otherwise, just remember the original unconverted
                         * tuple, to avoid a needless round trip conversion.
                         */
                        cstate->transition_capture->tcs_original_insert_tuple = tuple;
                        cstate->transition_capture->tcs_map = NULL;
                    }
                }

                /*
                 * We might need to convert from the parent rowtype to the
                 * partition rowtype.
                 */
				map = proute->parent_child_tupconv_maps[leaf_part_index];
				if (map != NULL)
				{
					TupleTableSlot *new_slot;
					MemoryContext oldcontext;

					Assert(proute->partition_tuple_slots != NULL &&
						proute->partition_tuple_slots[leaf_part_index] != NULL);
					new_slot = proute->partition_tuple_slots[leaf_part_index];
					slot = execute_attr_map_slot(map->attrMap, slot, new_slot, resultRelInfo->ri_RelationDesc);

					/*
					* Get the tuple in the per-tuple context, so that it will be
					* freed after each batch insert.
					*/
					oldcontext = MemoryContextSwitchTo(GetPerTupleMemoryContext(estate));
					tuple = ExecCopySlotTuple(slot);
					MemoryContextSwitchTo(oldcontext);
				}

                tuple->t_tableOid = RelationGetRelid(resultRelInfo->ri_RelationDesc);
            }

            skip_tuple = false;

            /* BEFORE ROW INSERT Triggers */
            if (resultRelInfo->ri_TrigDesc &&
                resultRelInfo->ri_TrigDesc->trig_insert_before_row)
            {
                slot = ExecBRInsertTriggers(estate, resultRelInfo, slot);

                if (slot == NULL) /* "do nothing" */
                    skip_tuple = true;
                else /* trigger might have changed tuple */
				{
					bool 		hasshard = false;
					AttrNumber *discolnums = NULL;
					int 		ndiscols = 0;
					Relation	rel = resultRelInfo->ri_RelationDesc;

					hasshard = RelationIsSharded(rel);
					if (hasshard)
					{
						ndiscols = RelationGetNumDisKeys(rel);
						discolnums = RelationGetDisKeys(rel);
					}

					ExecMaterializeSlot_shard(slot, hasshard, discolnums, ndiscols,
											RelationGetRelid(rel));
					tuple = slot->tts_tuple;
				}
            }

            if (!skip_tuple)
            {
                if (resultRelInfo->ri_TrigDesc &&
                    resultRelInfo->ri_TrigDesc->trig_insert_instead_row)
                {
                    /* Pass the data to the INSTEAD ROW INSERT trigger */
                    ExecIRInsertTriggers(estate, resultRelInfo, slot);
                }
                else
                {
                    /* Check the constraints of the tuple */
                   if (resultRelInfo->ri_RelationDesc->rd_att->constr)
				  		 ExecConstraints(resultRelInfo, slot, estate);

					/*
					* Also check the tuple against the partition constraint, if
					* there is one; except that if we got here via tuple-routing,
					* we don't need to if there's no BR trigger defined on the
					* partition.
					*/
					if (resultRelInfo->ri_PartitionCheck &&
						(saved_resultRelInfo == NULL ||
							(resultRelInfo->ri_TrigDesc &&
								resultRelInfo->ri_TrigDesc->trig_insert_before_row)))
						ExecPartitionCheck(resultRelInfo, slot, estate, true);

                    if (useHeapMultiInsert)
                    {
                        /* Add this tuple to the tuple buffer */
                        if (nBufferedTuples == 0)
                            firstBufferedLineNo = cstate->cur_lineno;

						bufferedTuples[nBufferedTuples++] = tuple;
						bufferedTuplesSize += tuple->t_len;

                        /*
                         * If the buffer filled up, flush it.  Also flush if the
                         * total size of all the tuples in the buffer becomes
                         * large, to avoid using large amounts of memory for the
                         * buffer when the tuples are exceptionally wide.
                         */
                        if (nBufferedTuples == MAX_BUFFERED_TUPLES ||
                            bufferedTuplesSize > 65535)
                        {
                            CopyFromInsertBatch(cstate, estate, mycid, hi_options,
                                                resultRelInfo, myslot, bistate,
                                                nBufferedTuples, bufferedTuples,
                                                firstBufferedLineNo);
                            nBufferedTuples = 0;
                            bufferedTuplesSize = 0;
                        }
                    }
                    else
                    {
                        List *recheckIndexes = NIL;

						/* OK, store the tuple and create index entries for it */
						heap_insert(resultRelInfo->ri_RelationDesc, tuple, mycid,
									hi_options, bistate);

						if (resultRelInfo->ri_NumIndices > 0)
							recheckIndexes = ExecInsertIndexTuples(slot,
																	&(tuple->t_self),
																	estate,
																	false,
																	NULL,
																	NIL);

                        /* AFTER ROW INSERT Triggers */
                        ExecARInsertTriggers(estate, resultRelInfo, tuple,
                                             recheckIndexes, cstate->transition_capture);

                        list_free(recheckIndexes);
                    }
                }

                /*
                 * We count only tuples not suppressed by a BEFORE INSERT trigger;
                 * this is the same definition used by execMain.c for counting
                 * tuples inserted by an INSERT command.
                 */
                processed++;
            }

			/* Restore the saved ResultRelInfo */
			if (saved_resultRelInfo)
			{
				resultRelInfo = saved_resultRelInfo;
				estate->es_result_relation_info = resultRelInfo;
			}
#ifdef PGXC
		}
#endif
	}
	
	copy_illegal_chars_conversion = false;
	copy_illegal_conv_char = 0;
	/* Flush any remaining buffered tuples */
	if (nBufferedTuples > 0)
	{
		CopyFromInsertBatch(cstate, estate, mycid, hi_options,
							resultRelInfo, myslot, bistate,
							nBufferedTuples, bufferedTuples,
							firstBufferedLineNo);

		nBufferedTuples = 0;
        bufferedTuplesSize = 0;
	}

#ifdef XCP
	/*
	 * Now if line buffer contains some data that is an EOF marker. We should
	 * send it to all the participating datanodes
	 */
	if (cstate->line_buf.len > 0)
	{
		RemoteCopyData *rcstate = cstate->remoteCopyState;
		if (DataNodeCopyIn(cstate->line_buf.data,
						   cstate->line_buf.len,
						   cstate->eol,
						   getLocatorNodeCount(rcstate->locator),
						   (PGXCNodeHandle **)getLocatorNodeMap(rcstate->locator),
						   cstate->binary))
		{
            int conn_count;
            int loop;
            PGXCNodeHandle** copy_connections;
            StringInfoData   sqldata;
            StringInfo       sql;

            sql = &sqldata;
            initStringInfo(sql);

            conn_count       = getLocatorNodeCount(rcstate->locator);
            copy_connections = (PGXCNodeHandle**) getLocatorNodeMap(rcstate->locator);

            for(loop = 0; loop < conn_count; loop++)
        	{
        		PGXCNodeHandle *handle = copy_connections[loop];
                if ('\0' != handle->error[0])
                {
                    appendStringInfo(sql, "%s;", handle->error);
                }
            }

			ereport(ERROR,
					(errcode(ERRCODE_CONNECTION_EXCEPTION),
					 errmsg("Copy failed on a data node:%s", sql->data)));
		}
	}
#endif
	/* Done, clean up */
	error_context_stack = errcallback.previous;

	FreeBulkInsertState(bistate);

	MemoryContextSwitchTo(oldcontext);

	/*
	 * In the old protocol, tell pqcomm that we can process normal protocol
	 * messages again.
	 */
	if (cstate->copy_dest == COPY_OLD_FE)
		pq_endmsgread();

	/* Execute AFTER STATEMENT insertion triggers */
	ExecASInsertTriggers(estate, resultRelInfo, cstate->transition_capture);

	/* Handle queued AFTER triggers */
	AfterTriggerEndQuery(estate);

	pfree(values);
	pfree(nulls);

	ExecResetTupleTable(estate->es_tupleTable, false);

	ExecCloseIndices(resultRelInfo);

	/* Close all the partitioned tables, leaf partitions, and their indices */
	if (cstate->partition_tuple_routing)
		ExecCleanupTupleRouting(cstate->partition_tuple_routing);

	/* Close any trigger target relations */
	ExecCleanUpTriggerState(estate);

	FreeExecutorState(estate);

	/*
	 * If we skipped writing WAL, then we need to sync the heap (but not
	 * indexes since those use WAL anyway)
	 */
	if (hi_options & HEAP_INSERT_SKIP_WAL)
		heap_sync(cstate->rel);

	return processed;
}

/*
 * A subroutine of CopyFrom, to write the current batch of buffered heap
 * tuples to the heap. Also updates indexes and runs AFTER ROW INSERT
 * triggers.
 */
static void
CopyFromInsertBatch(CopyState cstate, EState *estate, CommandId mycid,
					int hi_options, ResultRelInfo *resultRelInfo,
					TupleTableSlot *myslot, BulkInsertState bistate,
					int nBufferedTuples, HeapTuple *bufferedTuples,
					int firstBufferedLineNo)
{
	MemoryContext oldcontext;
    int i;
    int save_cur_lineno;

	/*
	 * Print error context information correctly, if one of the operations
	 * below fail.
	 */
	cstate->line_buf_valid = false;
	save_cur_lineno = cstate->cur_lineno;

	/*
	 * heap_multi_insert leaks memory, so switch to short-lived memory context
	 * before calling it.
	 */
	oldcontext = MemoryContextSwitchTo(GetPerTupleMemoryContext(estate));
	heap_multi_insert(cstate->rel,
					  bufferedTuples,
					  nBufferedTuples,
					  mycid,
					  hi_options,
					  bistate);
	MemoryContextSwitchTo(oldcontext);

	/*
	 * If there are any indexes, update them for all the inserted tuples, and
	 * run AFTER ROW INSERT triggers.
	 */
	if (resultRelInfo->ri_NumIndices > 0)
	{
		for (i = 0; i < nBufferedTuples; i++)
		{
			List *recheckIndexes;

			cstate->cur_lineno = firstBufferedLineNo + i;
			ExecStoreHeapTuple(bufferedTuples[i], myslot, false);
			recheckIndexes =
				ExecInsertIndexTuples(myslot, &(bufferedTuples[i]->t_self),
									  estate, false, NULL, NIL);
			ExecARInsertTriggers(estate, resultRelInfo,
								 bufferedTuples[i],
								 recheckIndexes, cstate->transition_capture);
			list_free(recheckIndexes);
		}
	}

	/*
	 * There's no indexes, but see if we need to run AFTER ROW INSERT triggers
	 * anyway.
	 */
	else if (resultRelInfo->ri_TrigDesc != NULL &&
			 (resultRelInfo->ri_TrigDesc->trig_insert_after_row ||
			  resultRelInfo->ri_TrigDesc->trig_insert_new_table))
	{
		for (i = 0; i < nBufferedTuples; i++)
		{
			cstate->cur_lineno = firstBufferedLineNo + i;
			ExecARInsertTriggers(estate, resultRelInfo,
								 bufferedTuples[i],
								 NIL, cstate->transition_capture);
		}
	}

	/* reset cur_lineno to where we were */
	cstate->cur_lineno = save_cur_lineno;
}

/*
 * Setup to read tuples from a file for COPY FROM.
 *
 * 'rel': Used as a template for the tuples
 * 'filename': Name of server-local file to read
 * 'attnamelist': List of char *, columns to include. NIL selects all cols.
 * 'options': List of DefElem. See copy_opt_item in gram.y for selections.
 *
 * Returns a CopyState, to be passed to NextCopyFrom and related functions.
 */
CopyState
BeginCopyFrom(ParseState *pstate,
			  Relation rel,
			  const char *filename,
			  bool is_program,
			  copy_data_source_cb data_source_cb,
			  void *data_source_cb_extra,
			  List *attnamelist,
			  List *attinfolist,
			  List *options,
			  bool fixed_position)
{
	CopyState	cstate;
	bool		pipe = (filename == NULL);
	TupleDesc	tupDesc;
	AttrNumber	num_phys_attrs,
				num_defaults;
	int			nfields;
	FmgrInfo   *in_functions;
	Oid		   *typioparams;
	int			attnum;
	Oid			in_func_oid;
	int		   *defmap;
	ExprState **defexprs;
    ExprState **attexprs;
    ParamListInfo param_list;
	MemoryContext oldcontext;
	bool volatile_defexprs;

	cstate = BeginCopy(pstate, true, rel, NULL, InvalidOid, attnamelist, options, NULL, attinfolist, fixed_position);
	oldcontext = MemoryContextSwitchTo(cstate->copycontext);

	/* Initialize state variables */
	cstate->fe_eof = false;
	if (cstate->eol_type != EOL_UD)
		cstate->eol_type = EOL_UNKNOWN;
	cstate->cur_relname = RelationGetRelationName(cstate->rel);
	cstate->cur_lineno = 0;
	cstate->cur_quote_lineno = 0;
	cstate->cur_bytenum = 0;
	cstate->cur_attname = NULL;
	cstate->cur_attval = NULL;
#ifdef __OPENTENBASE__
	cstate->data_list = NULL;
	cstate->data_index = 0;
	cstate->data_ncolumns = 0;
	cstate->ndatarows = 0;

	if (pstate && pstate->stmt && pstate->stmt->insert_into)
	{
		cstate->data_list = pstate->stmt->data_list;
		cstate->data_ncolumns = pstate->stmt->ncolumns;
		cstate->ndatarows = pstate->stmt->ndatarows;
	}
#endif

#ifdef _PG_ORCL_
	initStringInfo(&cstate->text_line_buf_rid);
#endif

	/* Set up variables to avoid per-attribute overhead. */
	initStringInfo(&cstate->attribute_buf_before);
	initStringInfo(&cstate->attribute_buf);
	initStringInfo(&cstate->line_buf);
	cstate->line_buf_converted = false;
	cstate->raw_buf = (char *)palloc(RAW_BUF_SIZE + 1);
	cstate->raw_buf_index = cstate->raw_buf_len = 0;

	/* Assign range table, we'll need it in CopyFrom. */
	if (pstate)
		cstate->range_table = pstate->p_rtable;

	tupDesc = RelationGetDescr(cstate->rel);
	num_phys_attrs = tupDesc->natts;
	nfields = num_phys_attrs;
	num_defaults = 0;
	volatile_defexprs = false;

	/* To support expressions in NextCopyFromX, reserve space for param in rowid and oid scenarios. */
	if (cstate->oids)
		nfields++;
	
	if (cstate->rowids)
		nfields++;

	/*
	 * Pick up the required catalog information for each attribute in the
	 * relation, including the input function, the element type (to pass to
	 * the input function), and info about defaults and constraints. (Which
	 * input function we use depends on text/binary format choice.)
	 */
	in_functions = (FmgrInfo *) palloc(num_phys_attrs * sizeof(FmgrInfo));
	typioparams = (Oid *) palloc(num_phys_attrs * sizeof(Oid));
	defmap = (int *) palloc(num_phys_attrs * sizeof(int));
	defexprs = (ExprState **) palloc(num_phys_attrs * sizeof(ExprState *));
    attexprs = (ExprState **) palloc0(num_phys_attrs * sizeof(ExprState *));
    param_list = (ParamListInfo) palloc(offsetof(ParamListInfoData, params) + nfields * sizeof(ParamExternData)); 
#ifdef PGXC
	/* Output functions are required to convert default values to output form */
	cstate->out_functions = (FmgrInfo *)palloc(num_phys_attrs * sizeof(FmgrInfo));
#endif

	for (attnum = 1; attnum <= num_phys_attrs; attnum++)
	{
		Form_pg_attribute att = TupleDescAttr(tupDesc, attnum - 1);

		/* We don't need info for dropped attributes */
		if (att->attisdropped)
			continue;

		/* Fetch the input function and typioparam info */
		if (cstate->binary)
			getTypeBinaryInputInfo(att->atttypid,
								   &in_func_oid, &typioparams[attnum - 1]);
		else
			getTypeInputInfo(att->atttypid,
							 &in_func_oid, &typioparams[attnum - 1]);
		fmgr_info(in_func_oid, &in_functions[attnum - 1]);

		/* Get default info if needed */
		if (!list_member_int(cstate->attnumlist, attnum))
		{
			/* attribute is NOT to be copied from input */
			/* use default value if one exists */
			Expr *defexpr = (Expr *)build_column_default(cstate->rel,
														 attnum);

			if (defexpr != NULL)
			{
#ifdef PGXC
				if (IS_PGXC_COORDINATOR)
				{
					/*
					 * If default expr is shippable to Datanode, don't include
					 * default values in the data row sent to the Datanode; let
					 * the Datanode insert the default values.
					 */
					Expr *planned_defexpr = expression_planner((Expr *)defexpr);
					defexpr = expression_planner(defexpr);

#ifdef __OPENTENBASE__
					/* treat distributed column as defaults too */
					if (!pgxc_is_expr_shippable(planned_defexpr, NULL) ||
						IsDistributedColumn(attnum, rel->rd_locator_info))
#else
					if (!pgxc_is_expr_shippable(planned_defexpr, NULL))
#endif
					{
						Oid out_func_oid;
						bool isvarlena;
						/* Initialize expressions in copycontext. */
						defexprs[num_defaults] = ExecInitExpr(planned_defexpr, NULL);
						defmap[num_defaults] = attnum - 1;
						num_defaults++;

						/*
						 * Initialize output functions needed to convert default
						 * values into output form before appending to data row.
						 */
						if (cstate->binary)
							getTypeBinaryOutputInfo(att->atttypid,
													&out_func_oid, &isvarlena);
						else
							getTypeOutputInfo(att->atttypid,
											  &out_func_oid, &isvarlena);
						fmgr_info(out_func_oid, &cstate->out_functions[attnum - 1]);
					}
				}
				else
				{
#endif /* PGXC */
					/* Run the expression through planner */
					defexpr = expression_planner(defexpr);

					/* Initialize executable expression in copycontext */
					defexprs[num_defaults] = ExecInitExpr(defexpr, NULL);
					defmap[num_defaults] = attnum - 1;
					num_defaults++;

					/*
				 * If a default expression looks at the table being loaded,
				 * then it could give the wrong answer when using
				 * multi-insert. Since database access can be dynamic this is
				 * hard to test for exactly, so we use the much wider test of
				 * whether the default expression is volatile. We allow for
				 * the special case of when the default expression is the
				 * nextval() of a sequence which in this specific case is
				 * known to be safe for use with the multi-insert
				 * optimization. Hence we use this special case function
				 * checker rather than the standard check for
				 * contain_volatile_functions().
				 */
					if (!volatile_defexprs)
						volatile_defexprs = contain_volatile_functions_not_nextval((Node *)defexpr);
#ifdef PGXC
				}
#endif
			}
		}
	}

	/* We keep those variables in cstate. */
	cstate->in_functions = in_functions;
	cstate->typioparams = typioparams;
	cstate->defmap = defmap;
	cstate->defexprs = defexprs;
    cstate->attexprs = attexprs;
    cstate->param_list = param_list;
	cstate->volatile_defexprs = volatile_defexprs;
	cstate->num_defaults = num_defaults;
	cstate->is_program = is_program;

	if (data_source_cb)
	{
		cstate->copy_dest = COPY_CALLBACK;
		cstate->data_source_cb = data_source_cb;
		cstate->data_source_cb_extra = data_source_cb_extra;
	}
	else if (pipe)
	{
		Assert(!is_program); /* the grammar does not allow this */
		if (whereToSendOutput == DestRemote)
			ReceiveCopyBegin(cstate);
		else
			cstate->copy_file = stdin;
	}
	else
	{
		cstate->filename = pstrdup(filename);

		if (cstate->is_program)
		{
			cstate->copy_file = OpenPipeStream(cstate->filename, PG_BINARY_R);
			if (cstate->copy_file == NULL)
				ereport(ERROR,
						(errcode_for_file_access(),
						 errmsg("could not execute command \"%s\": %m",
								cstate->filename)));
		}
		else
		{
#ifdef __OPENTENBASE__
			if (pstate && pstate->stmt && pstate->stmt->insert_into)
			{
				/* insert_into to copy_from */
			}
			else
			{
#endif
				struct stat st;

				cstate->copy_file = AllocateFile(cstate->filename, PG_BINARY_R);
				if (cstate->copy_file == NULL)
				{
					/* copy errno because ereport subfunctions might change it */
					int save_errno = errno;

					ereport(ERROR,
							(errcode_for_file_access(),
							 errmsg("could not open file \"%s\" for reading: %m",
									cstate->filename),
							 (save_errno == ENOENT || save_errno == EACCES) ? errhint("COPY FROM instructs the PostgreSQL server process to read a file. "
																					  "You may want a client-side facility such as psql's \\copy.")
																			: 0));
				}

				if (fstat(fileno(cstate->copy_file), &st))
					ereport(ERROR,
							(errcode_for_file_access(),
							 errmsg("could not stat file \"%s\": %m",
									cstate->filename)));

				if (S_ISDIR(st.st_mode))
					ereport(ERROR,
							(errcode(ERRCODE_WRONG_OBJECT_TYPE),
							 errmsg("\"%s\" is a directory", cstate->filename)));
#ifdef __OPENTENBASE__
			}
#endif
		}
	}

	if (!cstate->binary)
	{
		/* must rely on user to tell us... */
		cstate->file_has_oids = cstate->oids;
#ifdef _PG_ORCL_
		cstate->file_has_rowids = cstate->rowids;
#endif
	}
	else
	{
		/* Read and verify binary header */
		char readSig[11];
		int32 tmp;

		/* Signature */
		if (CopyGetData(cstate, readSig, 11, 11) != 11 ||
			memcmp(readSig, BinarySignature, 11) != 0)
			ereport(ERROR,
					(errcode(ERRCODE_BAD_COPY_FILE_FORMAT),
					 errmsg("COPY file signature not recognized")));
		/* Flags field */
		if (!CopyGetInt32(cstate, &tmp))
			ereport(ERROR,
					(errcode(ERRCODE_BAD_COPY_FILE_FORMAT),
					 errmsg("invalid COPY file header (missing flags)")));
		cstate->file_has_oids = (tmp & (1 << 16)) != 0;
		tmp &= ~(1 << 16);
#ifdef _PG_ORCL_
		cstate->file_has_rowids = (tmp & (1 << 17)) != 0;
		tmp &= ~(1 << 17);
#endif
		if ((tmp >> 16) != 0)
			ereport(ERROR,
					(errcode(ERRCODE_BAD_COPY_FILE_FORMAT),
					 errmsg("unrecognized critical flags in COPY file header")));
		/* Header extension length */
		if (!CopyGetInt32(cstate, &tmp) ||
			tmp < 0)
			ereport(ERROR,
					(errcode(ERRCODE_BAD_COPY_FILE_FORMAT),
					 errmsg("invalid COPY file header (missing length)")));
		/* Skip extension header, if present */
		while (tmp-- > 0)
		{
			if (CopyGetData(cstate, readSig, 1, 1) != 1)
				ereport(ERROR,
						(errcode(ERRCODE_BAD_COPY_FILE_FORMAT),
						 errmsg("invalid COPY file header (wrong length)")));
		}
#ifdef PGXC
		/* This is done at the beginning of COPY FROM from Coordinator to Datanodes */
		if (IS_PGXC_COORDINATOR)
		{
			RemoteCopyData *remoteCopyState = cstate->remoteCopyState;

			/* Empty buffer info and send header to all the backends involved in COPY */
			resetStringInfo(&cstate->line_buf);

			enlargeStringInfo(&cstate->line_buf, 19);
			appendBinaryStringInfo(&cstate->line_buf, BinarySignature, 11);
			tmp = 0;

			if (cstate->oids)
				tmp |= (1 << 16);
#ifdef _PG_ORCL_
			/*
			 * Input file contains rowid or the target relation is replicated
			 * and has global sequence for rowid. Later cooridinator will
			 * generate a rowid if not present and send it to datanodes.
			 */
			if (cstate->file_has_rowids || cstate->rid_glob_seqId != InvalidOid)
				tmp |= (1 << 17);
#endif
			tmp = pg_hton32(tmp);

			appendBinaryStringInfo(&cstate->line_buf, (char *)&tmp, 4);
			tmp = 0;
			tmp = pg_hton32(tmp);
			appendBinaryStringInfo(&cstate->line_buf, (char *)&tmp, 4);

			if (DataNodeCopyInBinaryForAll(cstate->line_buf.data, 19,
										   getLocatorNodeCount(remoteCopyState->locator),
										   (PGXCNodeHandle **)getLocatorNodeMap(remoteCopyState->locator)))
				ereport(ERROR,
						(errcode(ERRCODE_BAD_COPY_FILE_FORMAT),
						 errmsg("invalid COPY file header (COPY SEND)")));
		}
#endif
	}

	if (cstate->file_has_oids && cstate->binary)
	{
		getTypeBinaryInputInfo(OIDOID,
							   &in_func_oid, &cstate->oid_typioparam);
		fmgr_info(in_func_oid, &cstate->oid_in_function);
	}

#ifdef _PG_ORCL_
	if (cstate->file_has_rowids && cstate->binary)
	{
		getTypeBinaryInputInfo(INT8OID,
							   &in_func_oid, &cstate->rid_typioparam);
		fmgr_info(in_func_oid, &cstate->rid_in_function);
	}
#endif

	/* create workspace for CopyReadAttributes results */
	if (!cstate->binary)
	{
		AttrNumber attr_count = list_length(cstate->attnumlist);
		int nfields = cstate->file_has_oids ? (attr_count + 1) : attr_count;

#ifdef _PG_ORCL_
		nfields = cstate->file_has_rowids ? (nfields + 1) : nfields;
#endif
		cstate->max_fields = nfields;
		cstate->raw_fields = (char **)palloc(nfields * sizeof(char *));
	}

	MemoryContextSwitchTo(oldcontext);

	return cstate;
}

/*
 * Read raw fields in the next line for COPY FROM in text or csv mode.
 * Return false if no more lines.
 *
 * An internal temporary buffer is returned via 'fields'. It is valid until
 * the next call of the function. Since the function returns all raw fields
 * in the input file, 'nfields' could be different from the number of columns
 * in the relation.
 *
 * NOTE: force_not_null option are not applied to the returned fields.
 */
bool NextCopyFromRawFields(CopyState cstate, char ***fields, int *nfields)
{
	int fldct;
	bool done;

	/* only available for text or csv input */
	Assert(!cstate->binary);

	/* on input just throw the header line away */
	if (cstate->cur_lineno == 0 && cstate->header_line)
	{
		cstate->cur_lineno++;
		if (CopyReadLine(cstate))
			return false; /* done */
	}

	cstate->cur_lineno++;

	/* Actually read the line into memory here */
	done = CopyReadLine(cstate);

	/*
	 * EOF at start of line means we're done.  If we see EOF after some
	 * characters, we act as though it was newline followed by EOF, ie,
	 * process the line and then exit loop on next iteration.
	 */
	if (done && cstate->line_buf.len == 0)
		return false;

	/* Parse the line into de-escaped field values */
	if (cstate->csv_mode)
		fldct = CopyReadAttributesCSV(cstate);
#ifdef __OPENTENBASE__
	else if (cstate->internal_mode)
	{
		fldct = CopyReadAttributesInternal(cstate);
	}
#endif
	else if (cstate->fixed_mode)
	{
		fldct = CopyReadAttributesFixedWith(cstate);
	}
	else
		fldct = CopyReadAttributesText(cstate);

	*fields = cstate->raw_fields;
	*nfields = fldct;
	return true;
}


/*
 * A data error happened. This code block will always be inside a PG_CATCH()
 * block right when a higher stack level produced an error. We handle the error
 * by checking which error mode is set (SREH or all-or-nothing) and do the right
 * thing accordingly. Note that we MUST have this code in a macro (as opposed
 * to a function) as elog_dismiss() has to be inlined with PG_CATCH in order to
 * access local error state variables.
 */
static void
HandleCopyError(CopyState cstate)
{
        if (cstate->errMode == ALL_OR_NOTHING)
        {
                /* re-throw error and abort */
                PG_RE_THROW();
        }
        /* SREH must only handle data errors. all other errors must not be caught */
        if (ERRCODE_TO_CATEGORY(elog_geterrcode()) != ERRCODE_DATA_EXCEPTION)
        {
                /* re-throw error and abort */
                PG_RE_THROW();
        }
        else
        {
                /* SREH - release error state and handle error */
                MemoryContext oldcontext;
                ErrorData     *edata;
                char          *errormsg;
                Srehandler    *srehandler = cstate->srehandler;
                
                srehandler->processed++;
                
                oldcontext = MemoryContextSwitchTo(cstate->srehandler->badrowcontext);
                
                /* save a copy of the error info */
                edata = CopyErrorData();
                
                FlushErrorState();
                
                /*
                 * set the error message. Use original msg and add column name if available.
                 * We do this even if we're not logging the errors, because
                 * ErrorIfRejectLimit() below will use this information in the error message,
                 * if the error count is reached.
                 */
                srehandler->rawdata->cursor = 0;
                srehandler->rawdata->data   = cstate->line_buf.data;
                srehandler->rawdata->len    = cstate->line_buf.len;
                
                srehandler->is_server_enc = cstate->line_buf_converted;
                if (cstate->csv_mode)
                        srehandler->linenumber = cstate->cur_lineno;
                srehandler->bytenum = cstate->cur_bytenum;
                if (cstate->cur_attname)
                {
                        errormsg = psprintf("%s, column %s",
                                            edata->message, cstate->cur_attname);
                }
                else
                {
                        errormsg = edata->message;
                }
                cstate->srehandler->errmsg = errormsg;
                
                if (cstate->srehandler->log_to_file)
                {
                        /* after all the prep work let cdbsreh do the real work */
                        HandleSingleRowError(cstate->srehandler);
                }
                else
                        cstate->srehandler->rejectcount++;
                
                ErrorIfRejectLimitReached(cstate->srehandler);
                
                MemoryContextSwitchTo(oldcontext);
                MemoryContextReset(cstate->srehandler->badrowcontext);
        }
}

/**
 * @Description: Call a previously-looked-up datatype input function for bulkload compatibility, which prototype
 * is InputFunctionCall. The new one tries to input bulkload compatiblity-specific parameters to built-in datatype
 * input function.
 * @in cstate: the current CopyState
 * @in flinfo: inputed FmgrInfo
 * @in str: datatype value in string-format
 * @in typioparam: some datatype oid
 * @in typmod:  some datatype mode
 * @return: the datatype value outputed from built-in datatype input function
 */
Datum InputFunctionCallForBulkload(CopyState cstate, FmgrInfo *flinfo, char *str, Oid typioparam, int32 typmod)
{
	FunctionCallInfoData fcinfo;
	Datum                result;
	short                nargs = 3;
	char                 *date_time_fmt = NULL;

	if (str == NULL && flinfo->fn_strict)
		return (Datum) 0; /* just return null result */

	switch (typioparam)
	{
		case DATEOID:
		{
			if (cstate->date_format)
			{
				nargs         = 4;
				date_time_fmt = (char *) cstate->date_format;
			}
		}
			break;
		case TIMEOID:
		{
			if (cstate->time_format)
			{
				nargs         = 4;
				date_time_fmt = (char *) cstate->time_format;
			}
		}
			break;
		case TIMESTAMPOID:
		{
			if (cstate->timestamp_format)
			{
				nargs         = 4;
				date_time_fmt = (char *) cstate->timestamp_format;
			}
		}
			break;
			/* not support SMALLDATETIMEOID now. */
		default:
		{
			/* do nothing. */
		}
			break;
	}

	InitFunctionCallInfoData(fcinfo, flinfo, nargs, InvalidOid, NULL, NULL);

	fcinfo.arg[0] = CStringGetDatum(str);
	fcinfo.arg[1] = ObjectIdGetDatum(typioparam);
	fcinfo.arg[2] = Int32GetDatum(typmod);

	fcinfo.argnull[0] = (str == NULL);
	fcinfo.argnull[1] = false;
	fcinfo.argnull[2] = false;

	/*
	 * input specified datetime format.
	 */
	if (nargs == 4)
	{
		fcinfo.arg[3]     = CStringGetDatum(date_time_fmt);
		fcinfo.argnull[3] = false;
	}

	result = FunctionCallInvoke(&fcinfo);

	/* Should get null result if and only if str is NULL */
	if (str == NULL)
	{
		if (!fcinfo.isnull)
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
							errmsg("input function %u returned non-NULL", fcinfo.flinfo->fn_oid)));
	}
	else
	{
		if (fcinfo.isnull)
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
							errmsg("input function %u returned NULL", fcinfo.flinfo->fn_oid)));
	}

	return result;
}

/*
 * Read next tuple from file for COPY FROM. Return false if no more tuples.
 *
 * 'econtext' is used to evaluate default expression for each columns not
 * read from the file. It can be NULL when no default values are used, i.e.
 * when all columns are read from the file.
 *
 * 'values' and 'nulls' arrays must be the same length as columns of the
 * relation passed to BeginCopyFrom. This function fills the arrays.
 * Oid of the tuple is returned with 'tupleOid' separately.
 */
static bool
NextCopyFromX(CopyState cstate, EState *estate, Datum *values, bool *nulls,
			  Oid *tupleOid
#ifdef _PG_ORCL_
			  , RowId *rowId
#endif
			 )
{
	TupleDesc	tupDesc;
	AttrNumber	num_phys_attrs,
				attr_count,
				num_defaults = cstate->num_defaults;
	FmgrInfo   *in_functions = cstate->in_functions;
	Oid		   *typioparams = cstate->typioparams;
	int			i;
	int			nfields;
	bool		isnull;
	bool		file_has_oids = cstate->file_has_oids;
#ifdef _PG_ORCL_
	bool		file_has_rowids = cstate->file_has_rowids;
	int	rid_idx = -1;
#endif
	int		   *defmap = cstate->defmap;
	ExprState **defexprs = cstate->defexprs;
	StringInfoData hive_partition_key;
	ExprContext *econtext = estate ? GetPerTupleExprContext(estate) : NULL;

	tupDesc = RelationGetDescr(cstate->rel);
	num_phys_attrs = tupDesc->natts;
	attr_count = list_length(cstate->attnumlist);
	nfields = file_has_oids ? (attr_count + 1) : attr_count;

#ifdef _PG_ORCL_
	nfields = file_has_rowids ? (nfields + 1) : nfields;
#endif

	/* Initialize all values for row to NULL */
	MemSet(values, 0, num_phys_attrs * sizeof(Datum));
	MemSet(nulls, true, num_phys_attrs * sizeof(bool));

	if (!cstate->binary)
	{
		char        **field_strings;
		ListCell     *cur;
		int           fldct;
		int           fieldno;
		char         *string;
		/* field index in attlist and attinfolist, which is different from attnum. */
		int           field_index = 0;
		ParamListInfo paramLI;

		/* index in field_strings. (including oid or rowid)*/
		int           j = 0;
		List         *attinfolist = cstate->attinfolist;

#ifdef __OPENTENBASE__
		if (cstate->insert_into && cstate->data_list)
		{
			resetStringInfo(&cstate->line_buf);

			/* send data in datarow format to remote datanode */
			if (cstate->data_index < cstate->ndatarows)
			{
				int index = 0;
				uint16 n16 = 0;
				int total_columns = 0;

#ifdef _PG_ORCL_
				/*
				 * If target relation needs RowId, but input data does not
				 * include rowid value, just generate it. Rowid values should
				 * be setup in coordinator, and passed it to datanode.
				 */
				if (!file_has_rowids && cstate->rid_glob_seqId != InvalidOid &&
									IS_PGXC_COORDINATOR)
				{
					if (file_has_oids)
						rid_idx = 1;
					else
						rid_idx = 0;
				}
#endif

				field_strings = cstate->data_list[cstate->data_index];
				cstate->data_index++;
				fldct = cstate->data_ncolumns;
				total_columns = fldct + num_defaults;
#ifdef _PG_ORCL_
				if (rid_idx != -1)
					total_columns++;
#endif
				n16 = pg_hton16(total_columns);
				appendBinaryStringInfo(&cstate->line_buf, (char *)&n16, sizeof(n16));

				for (index = 0; index < fldct; index++)
				{
#ifdef _PG_ORCL_
					if (index == rid_idx)
					{
						/* Need to merge latest V5 commits */
						RowId	n_rid = -1;
						char	*rid_str;
						int		len;
						uint32	n32;

						if (rowId)
							*rowId = n_rid;

						/* Append rowid */
						n_rid = nextval_internal(cstate->rid_glob_seqId, false);
						rid_str = DatumGetCString(DirectFunctionCall1(int8out,
													Int64GetDatum(n_rid)));

						len = strlen(rid_str) + 1;
						n32 = pg_hton32(len);
						appendBinaryStringInfo(&cstate->line_buf, (char *) &n32, sizeof(n32));
						appendBinaryStringInfo(&cstate->line_buf, rid_str, len - 1);
						appendStringInfoChar(&cstate->line_buf, '\0');

						rid_idx = -1;
						pfree(rid_str);
					}
#endif
					if (field_strings[index])
					{
						int len = strlen(field_strings[index]) + 1;
						uint32 n32 = pg_hton32(len);
						appendBinaryStringInfo(&cstate->line_buf, (char *)&n32, sizeof(n32));
						appendBinaryStringInfo(&cstate->line_buf, field_strings[index], len - 1);
						appendStringInfoChar(&cstate->line_buf, '\0');
					}
					else
					{
						uint32 n32 = pg_hton32(-1);
						appendBinaryStringInfo(&cstate->line_buf, (char *)&n32, 4);
					}
				}
			}
			else
			{
				return false;
			}
		}
		else
#endif
			/* read raw fields in the next line */
			if (!NextCopyFromRawFields(cstate, &field_strings, &fldct))
			return false;

		/* check for overflowing fields */
		if (nfields > 0 && fldct > nfields)
		{
			/* Avoid out-of-bounds access when iterating through the param_list during the copy process. */
			if (cstate->ignore_extra_data && fldct > nfields)
			{
				fldct = nfields;
			}
			else
			{
				ereport(ERROR,
				        (errcode(ERRCODE_BAD_COPY_FILE_FORMAT),
				         errmsg("extra data after last expected column")));
			}
		}

		fieldno = 0;

		/* Read the OID field if present */
		if (file_has_oids)
		{
			if (fieldno >= fldct)
				ereport(ERROR,
						(errcode(ERRCODE_BAD_COPY_FILE_FORMAT),
						 errmsg("missing data for OID column")));
			string = field_strings[fieldno++];

			if (string == NULL)
				ereport(ERROR,
						(errcode(ERRCODE_BAD_COPY_FILE_FORMAT),
						 errmsg("null OID in COPY data")));
			else if (cstate->oids && tupleOid != NULL)
			{
				cstate->cur_attname = "oid";
				cstate->cur_attval = string;
				*tupleOid = DatumGetObjectId(DirectFunctionCall1(oidin,
																 CStringGetDatum(string)));
				if (*tupleOid == InvalidOid)
					ereport(ERROR,
							(errcode(ERRCODE_BAD_COPY_FILE_FORMAT),
							 errmsg("invalid OID in COPY data")));
				cstate->cur_attname = NULL;
				cstate->cur_attval = NULL;
			}
		}

#ifdef _PG_ORCL_
		if (file_has_rowids)
		{
			if (fieldno >= fldct)
				ereport(ERROR,
						(errcode(ERRCODE_BAD_COPY_FILE_FORMAT),
						 errmsg("missing data for ROWID column")));
			string = field_strings[fieldno++];

			if (string == NULL)
				ereport(ERROR,
						(errcode(ERRCODE_BAD_COPY_FILE_FORMAT),
						 errmsg("null ROWID in COPY data")));
			else if (cstate->rowids && rowId != NULL)
			{
				Datum	dm;

				cstate->cur_attname = "ROWID";
				cstate->cur_attval = string;

				dm = DirectFunctionCall1(int8in, CStringGetDatum(string));
				*rowId = (RowId) DatumGetInt64(dm);
				if (*rowId == InvalidRowId)
					ereport(ERROR,
							(errcode(ERRCODE_BAD_COPY_FILE_FORMAT),
							 errmsg("invalid RowId in COPY data")));
				cstate->cur_attname = NULL;
				cstate->cur_attval = NULL;
			}
		}
#endif
		if (estate)
		{
			/* Init paramList. */
			paramLI = cstate->param_list;

			/* we have static list of params, so no hooks needed */
			paramLI->paramFetch = NULL;
			paramLI->paramFetchArg = NULL;
			paramLI->parserSetup = NULL;
			paramLI->parserSetupArg = NULL;
			paramLI->numParams = 0;
			paramLI->param_context = 0;

			for (j = fieldno; j < fldct; j++)
			{
				ParamExternData *prm = &paramLI->params[paramLI->numParams++];

				/* opentenbase_ora: copy treat '' as null */
				if (ORA_MODE && !disable_empty_to_null && field_strings[j] && strlen(field_strings[j]) == 0)
				{
					prm->value = (Datum) 0;
					prm->isnull = true;
				}
				else
				{
					prm->value = field_strings[j] ? CStringGetTextDatum(field_strings[j]) : (Datum) 0;
					prm->isnull = (field_strings[j] == NULL);
				}

				prm->pflags = PARAM_FLAG_CONST;
				prm->ptype = TEXTOID;
			}

			Assert(!estate->es_param_list_info);
			Assert(!econtext->ecxt_param_list_info);

			estate->es_param_list_info = paramLI;
			econtext->ecxt_param_list_info = paramLI;
		}

		/*
		 * A completely empty line is not allowed with FILL MISSING FIELDS. Without
		 * FILL MISSING FIELDS, it's almost surely an error, but not always:
		 * a table with a single text column, for example, needs to accept empty
		 * lines.
		 */
		if (cstate->line_buf.len == 0 &&
			cstate->fill_missing &&
			list_length(cstate->attnumlist) > 1)
		{
			ereport(ERROR,
					(errcode(ERRCODE_BAD_COPY_FILE_FORMAT),
							errmsg("missing data for column \"%s\", found empty data line",
								   NameStr(TupleDescAttr(tupDesc, 1)->attname))));
		}

		/* Loop to read the user attributes on the line. */
		initStringInfo(&hive_partition_key);
		foreach (cur, cstate->attnumlist)
		{
			int               attnum = lfirst_int(cur);
			int               m = attnum - 1;
			Form_pg_attribute att = TupleDescAttr(tupDesc, m);
			bool              flag = false;
			AttInfo          *attr_info = NULL;

			if (fieldno >= fldct)
			{
				/*
				 * Some attributes are missing. In FILL MISSING FIELDS mode,
				 * treat them as NULLs.
				 */
				if (!cstate->fill_missing)
				{
					if (cstate->partitions_list)
					{
						ListCell *part_lc;
						int idx = 0;

						foreach(part_lc, cstate->partitions_list)
						{
							if (strcasecmp(strVal(lfirst(part_lc)), NameStr(att->attname)) == 0)
							{
								/* need to fill hive partition column values by parse filename */
								if (!cstate->hive_partition_dm[idx] && !cstate->hive_partition_default[idx])
                            	{
									char *part_info = NULL;
									char *part_col_val = NULL;
									int slash_pos = 0;
									FdwFileSegment *segment = (FdwFileSegment *)lfirst(cstate->curTaskPtr);
									MemoryContext oldcontext;

									resetStringInfo(&hive_partition_key);
									appendStringInfoString(&hive_partition_key, strVal(lfirst(part_lc)));
									appendStringInfoChar(&hive_partition_key, '=');

									part_info = strstr(segment->filename, hive_partition_key.data);
									part_info += hive_partition_key.len;
									while (part_info[slash_pos] != '/')
										slash_pos++;
									oldcontext = MemoryContextSwitchTo(cstate->copycontext);
									part_col_val = pnstrdup(part_info, slash_pos);
									MemoryContextSwitchTo(oldcontext);

									if (strlen(part_col_val) == 0 || strcasecmp(part_col_val, "__HIVE_DEFAULT_PARTITION__") == 0)
									{
										cstate->hive_partition_default[idx] = true;
										string = NULL;
										pfree(part_col_val);
									}
									else
									{
										cstate->hive_partition_dm[idx] = part_col_val;
										string = cstate->hive_partition_dm[idx];
									}
								}
								else if (cstate->hive_partition_dm[idx])
								{
									string = cstate->hive_partition_dm[idx];
								}
								else
								{
									string = NULL;
								}
								flag = true;
								break;
							}
							idx++;
						}

						if (!flag)
						{
							ereport(ERROR,
								(errcode(ERRCODE_BAD_COPY_FILE_FORMAT),
									errmsg("missing data for column \"%s\"",
										   NameStr(att->attname))));
						}
					}
					else
					{
						ereport(ERROR,
								(errcode(ERRCODE_BAD_COPY_FILE_FORMAT),
										errmsg("missing data for column \"%s\"",
											NameStr(att->attname))));
					}
				}
				fieldno++;
				if (!flag)
					string = NULL;
			}
			else
			{
				string = field_strings[fieldno++];
			}

			/* lightweight_ora: copy treat '' as null */
			if (((ORA_MODE && !disable_empty_to_null) ||enable_lightweight_ora_syntax ) &&
												string && strlen(string) == 0)
				string = NULL;

			if (cstate->convert_select_flags &&
				!cstate->convert_select_flags[m])
			{
				/* ignore input field, leaving column as NULL */
				continue;
			}

			if (cstate->csv_mode)
			{
				if (string == NULL &&
					cstate->force_notnull_flags[m])
				{
					/*
					 * FORCE_NOT_NULL option is set and column is NULL -
					 * convert it to the NULL string.
					 */
					string = cstate->null_print;
				}
				else if (string != NULL && cstate->force_null_flags[m] && strcmp(string, cstate->null_print) == 0)
				{
					/*
					 * FORCE_NULL option is set and column matches the NULL
					 * string. It must have been quoted, or otherwise the
					 * string would already have been set to NULL. Convert it
					 * to NULL as specified.
					 */
					string = NULL;
				}
			}

			cstate->cur_attname = NameStr(att->attname);
			cstate->cur_attval = string;
			if (attinfolist != NIL)
			{
				/* get attr_info and expr_spec for current field. */
				Assert(field_index < attinfolist->length);
				attr_info = (AttInfo *) lfirst(list_nth_cell(attinfolist, field_index++));
			}

			if (attr_info && attr_info->custom_expr)
			{
				ExprState *pram_exprstate;
				Expr      *expr = (Expr *) attr_info->custom_expr;

				/* convert output of expr to the type of values[m]. */
				expr = (Expr *) coerce_to_target_type(NULL,
				                                      (Node *) expr,
				                                      exprType((Node *) expr),
				                                      TupleDescAttr(tupDesc, m)->atttypid,
				                                      TupleDescAttr(tupDesc, m)->atttypmod,
				                                      COERCION_ASSIGNMENT,
				                                      COERCE_IMPLICIT_CAST,
				                                      -1);
				if(cstate->attexprs[m] == NULL)
                        cstate->attexprs[m] = ExecPrepareExpr(expr, estate);
				pram_exprstate = cstate->attexprs[m];
				values[m] = ExecEvalExprSwitchContext(pram_exprstate, econtext, &nulls[m]);
			}
			else
			{
				values[m] =
					InputFunctionCall(&in_functions[m], string, typioparams[m], TupleDescAttr(tupDesc, m)->atttypmod);
				if (string != NULL)
					nulls[m] = false;
			}
			cstate->cur_attname = NULL;
			cstate->cur_attval = NULL;
		}

		if (estate)
		{
			estate->es_param_list_info = NULL;
			econtext->ecxt_param_list_info = NULL;
		}

		Assert(fieldno == nfields);
	}
	else
	{
		/* binary */
		int16 fld_count;
		ListCell *cur;

		cstate->cur_lineno++;

		if (!CopyGetInt16(cstate, &fld_count))
		{
#ifdef PGXC
			if (IS_PGXC_COORDINATOR)
			{
				/* Empty buffer */
				resetStringInfo(&cstate->line_buf);

				enlargeStringInfo(&cstate->line_buf, sizeof(uint16));
				/* Receive field count directly from Datanodes */
				fld_count = pg_hton16(fld_count);
				appendBinaryStringInfo(&cstate->line_buf, (char *)&fld_count, sizeof(uint16));
			}
#endif

			/* EOF detected (end of file, or protocol-level EOF) */
			return false;
		}

		if (fld_count == -1)
		{
			/*
			 * Received EOF marker.  In a V3-protocol copy, wait for the
			 * protocol-level EOF, and complain if it doesn't come
			 * immediately.  This ensures that we correctly handle CopyFail,
			 * if client chooses to send that now.
			 *
			 * Note that we MUST NOT try to read more data in an old-protocol
			 * copy, since there is no protocol-level EOF marker then.  We
			 * could go either way for copy from file, but choose to throw
			 * error if there's data after the EOF marker, for consistency
			 * with the new-protocol case.
			 */
			char dummy;

#ifdef PGXC
			if (IS_PGXC_COORDINATOR)
			{
				/* Empty buffer */
				resetStringInfo(&cstate->line_buf);

				enlargeStringInfo(&cstate->line_buf, sizeof(uint16));
				/* Receive field count directly from Datanodes */
				fld_count = pg_hton16(fld_count);
				appendBinaryStringInfo(&cstate->line_buf, (char *)&fld_count, sizeof(uint16));
			}
#endif

			if (cstate->copy_dest != COPY_OLD_FE &&
				CopyGetData(cstate, &dummy, 1, 1) > 0)
				ereport(ERROR,
						(errcode(ERRCODE_BAD_COPY_FILE_FORMAT),
						 errmsg("received copy data after EOF marker")));
			return false;
		}

		if (fld_count != attr_count)
			ereport(ERROR,
					(errcode(ERRCODE_BAD_COPY_FILE_FORMAT),
					 errmsg("row field count is %d, expected %d",
							(int)fld_count, attr_count)));

#ifdef PGXC
		if (IS_PGXC_COORDINATOR)
		{
			/*
			 * Include the default value count also, because we are going to
			 * append default values to the user-supplied attributes.
			 */
			int16 total_fld_count = fld_count + num_defaults;

			/* Empty buffer */
			resetStringInfo(&cstate->line_buf);

			enlargeStringInfo(&cstate->line_buf, sizeof(uint16));
			total_fld_count = pg_hton16(total_fld_count);
			appendBinaryStringInfo(&cstate->line_buf, (char *)&total_fld_count, sizeof(uint16));
		}
#endif

		if (file_has_oids)
		{
			Oid loaded_oid;

			cstate->cur_attname = "oid";
			loaded_oid =
				DatumGetObjectId(CopyReadBinaryAttribute(cstate,
														 0,
														 &cstate->oid_in_function,
														 cstate->oid_typioparam,
														 -1,
														 &isnull));
			if (isnull || loaded_oid == InvalidOid)
				ereport(ERROR,
						(errcode(ERRCODE_BAD_COPY_FILE_FORMAT),
						 errmsg("invalid OID in COPY data")));
			cstate->cur_attname = NULL;
			if (cstate->oids && tupleOid != NULL)
				*tupleOid = loaded_oid;
		}

#ifdef _PG_ORCL_
		if (file_has_rowids)
		{
			RowId	loaded_rid;
			Datum	dm;

			cstate->cur_attname = "ROWID";
			dm = CopyReadBinaryAttribute(cstate,
											 0,
											 &cstate->rid_in_function,
											 cstate->rid_typioparam,
											 -1,
											 &isnull);
			if (isnull || (loaded_rid = (RowId) DatumGetInt64(dm)) == InvalidRowId)
				ereport(ERROR,
						(errcode(ERRCODE_BAD_COPY_FILE_FORMAT),
						 errmsg("invalid ROWID in COPY data")));

			cstate->cur_attname = NULL;
			if (cstate->rowids && rowId != NULL)
				*rowId = loaded_rid;
		}

		if (!file_has_rowids && cstate->rid_glob_seqId != InvalidOid &&
					IS_PGXC_COORDINATOR)
		{
			if (file_has_oids)
				rid_idx = 1;
			else
				rid_idx = 0;
		}
#endif

		i = 0;
		foreach (cur, cstate->attnumlist)
		{
			int			attnum = lfirst_int(cur);
			int			m = attnum - 1;
			Form_pg_attribute att = TupleDescAttr(tupDesc, m);

#ifdef _PG_ORCL_
			if (i == rid_idx)
			{
				uint32		u32;
				uint32		val;
				RowId		n_rid;

				Assert(IS_PGXC_COORDINATOR);

				n_rid = nextval_internal(cstate->rid_glob_seqId, false);
				if (rowId)
					*rowId = n_rid;

				val = sizeof(RowId);
				u32 = pg_hton32((uint32) val);
				appendBinaryStringInfo(&cstate->line_buf, (char *) &u32, sizeof(uint32));

				val = (uint32) (n_rid >> 32);
				u32 = pg_hton32((uint32) val);
				appendBinaryStringInfo(&cstate->line_buf, (char *) &u32, sizeof(uint32));

				val = (uint32) n_rid;
				u32 = pg_hton32((uint32) val);
				appendBinaryStringInfo(&cstate->line_buf, (char *) &u32, sizeof(uint32));

				rid_idx = -1;
			}
#endif

			cstate->cur_attname = NameStr(att->attname);
			i++;
			values[m] = CopyReadBinaryAttribute(cstate,
												i,
												&in_functions[m],
												typioparams[m],
												att->atttypmod,
												&nulls[m]);
			cstate->cur_attname = NULL;
		}
	}

	/*
	 * Now compute and insert any defaults available for the columns not
	 * provided by the input data.  Anything not processed here or above will
	 * remain NULL.
	 */
	for (i = 0; i < num_defaults; i++)
	{
		/*
		 * The caller must supply econtext and have switched into the
		 * per-tuple memory context in it.
		 */
		Assert(econtext != NULL);
		Assert(CurrentMemoryContext == econtext->ecxt_per_tuple_memory);

		values[defmap[i]] = ExecEvalExpr(defexprs[i], econtext,
										 &nulls[defmap[i]]);
	}

#ifdef PGXC
	if (IS_PGXC_COORDINATOR)
	{
		/* Append default values to the data-row in output format. */
		append_defvals(cstate, values, nulls);
	}
#endif

	return true;
}

bool NextCopyFrom(CopyState cstate, EState *estate,
				  Datum *values, bool *nulls, Oid *tupleOid
#ifdef _PG_ORCL_
				  ,RowId *rowid
#endif
                )
{
	if (!cstate->srehandler)
	{
#ifdef _PG_ORCL_
		return NextCopyFromX(cstate, estate, values, nulls, tupleOid, rowid);
#else
		return NextCopyFromX(cstate, estate, values, nulls, tupleOid);
#endif
	}
	else
	{
		MemoryContext oldcontext = CurrentMemoryContext;

		for (;;)
		{
			bool got_error = false;
			bool result    = false;

			PG_TRY();
			{
#ifdef _PG_ORCL_
				result = NextCopyFromX(cstate, estate, values, nulls, tupleOid, rowid);
#else
				result = NextCopyFromX(cstate, estate, values, nulls, tupleOid);
#endif
			}
			PG_CATCH();
			{
				HandleCopyError(cstate);
				got_error = true;
				MemoryContextSwitchTo(oldcontext);

				if (estate)
				{
					estate->es_param_list_info = NULL;
					if (estate->es_per_tuple_exprcontext)
						estate->es_per_tuple_exprcontext->ecxt_param_list_info = NULL;
				}
			}
			PG_END_TRY();

			if (!got_error)
			{
				if (result)
				{
					/*
					 * If NextCopyFrom failed, the processed row count will have already
					 * been updated, but we need to update it in a successful case.
					 * this is almost certainly not the right place for
					 * this, but row counts are currently scattered all over the place.
					 * Consolidate.
					 */
					cstate->srehandler->processed++;
				}
				return result;
			}
		}
	}
}

#ifdef PGXC
/*
 * append_defvals:
 * Append default values in output form onto the data-row.
 * 1. scans the default values with the help of defmap,
 * 2. converts each default value into its output form,
 * 3. then appends it into cstate->defval_buf buffer.
 * This buffer would later be appended into the final data row that is sent to
 * the Datanodes.
 * So for e.g., for a table :
 * tab (id1 int, v varchar, id2 default nextval('tab_id2_seq'::regclass), id3 )
 * with the user-supplied data  : "2 | abcd",
 * and the COPY command such as:
 * copy tab (id1, v) FROM '/tmp/a.txt' (delimiter '|');
 * Here, cstate->defval_buf will be populated with something like : "| 1"
 * and the final data row will be : "2 | abcd | 1"
 */
static void
append_defvals(CopyState cstate, Datum *values, bool *nulls)
{
	CopyStateData new_cstate = *cstate;
	int i;

	new_cstate.fe_msgbuf = makeStringInfo();

	for (i = 0; i < cstate->num_defaults; i++)
	{
		int attindex = cstate->defmap[i];
		Datum defvalue = values[attindex];
		bool isnull = nulls[attindex];

		if (!cstate->binary)
			CopySendString(&new_cstate, new_cstate.delim);

		/*
		 * For using the values in their output form, it is not sufficient
		 * to just call its output function. The format should match
		 * that of COPY because after all we are going to send this value as
		 * an input data row to the Datanode using COPY FROM syntax. So we call
		 * exactly those functions that are used to output the values in case
		 * of COPY TO. For instace, CopyAttributeOutText() takes care of
		 * escaping, CopySendInt32 take care of byte ordering, etc. All these
		 * functions use cstate->fe_msgbuf to copy the data. But this field
		 * already has the input data row. So, we need to use a separate
		 * temporary cstate for this purpose. All the COPY options remain the
		 * same, so new cstate will have all the fields copied from the original
		 * cstate, except fe_msgbuf.
		 */
		if (cstate->binary)
		{
			bytea *outputbytes;

			if (isnull)
			{
				CopySendInt32(&new_cstate, -1);
			}
			else
			{
				outputbytes = SendFunctionCall(&cstate->out_functions[attindex], defvalue);
				CopySendInt32(&new_cstate, VARSIZE(outputbytes) - VARHDRSZ);
				CopySendData(&new_cstate, VARDATA(outputbytes),
							VARSIZE(outputbytes) - VARHDRSZ);
			}
		}
		else
		{
			char *string;

			/*
			 * In normal copy scenario, the NULL value in line_buf is represented by 
			 * cstate->null_print, ("" is used in CSV format, "\\N" is used in text format).
			 * In insert into scenario, line_buf is used for internal mode, which use len = -1
			 * to represent NULL.
			 */
			if (isnull)
				CopySendData(&new_cstate, cstate->null_print, cstate->null_print_len);
			else
			{
				string = OutputFunctionCall(&cstate->out_functions[attindex], defvalue);

				if (cstate->csv_mode)
					CopyAttributeOutCSV(&new_cstate, string,
										false /* don't force quote */,
										false /* there's at least one user-supplied attribute */);
				else
					CopyAttributeOutText(&new_cstate, string);
			}

#ifdef __OPENTENBASE__
			if (cstate->insert_into && cstate->data_list)
			{
				int len;
				uint32 n32;

				if (isnull)
				{
					len = -1;
					n32 = pg_hton32(len);
					appendBinaryStringInfo(&cstate->line_buf, (char *)&n32, sizeof(n32));
				}
				else
				{
					len = strlen(string) + 1;
					n32 = pg_hton32(len);
					appendBinaryStringInfo(&cstate->line_buf, (char *)&n32, sizeof(n32));
					appendBinaryStringInfo(&cstate->line_buf, string, len - 1);
					appendStringInfoChar(&cstate->line_buf, '\0');
				}
			}
#endif
		}
	}

#ifdef __OPENTENBASE__
	if (cstate->insert_into && cstate->data_list)
	{
		/* do nothing, default values have been appended before */
	}
	else
	{
#endif
		/* Append the generated default values to the user-supplied data-row */
		appendBinaryStringInfo(&cstate->line_buf, new_cstate.fe_msgbuf->data,
							   new_cstate.fe_msgbuf->len);
		appendBinaryStringInfo(&cstate->text_line_buf_rid, new_cstate.fe_msgbuf->data,
							   new_cstate.fe_msgbuf->len);
#ifdef __OPENTENBASE__
	}
#endif
}
#endif

/*
 * Clean up storage and release resources for COPY FROM.
 */
void EndCopyFrom(CopyState cstate)
{
#ifdef PGXC
	RemoteCopyData *remoteCopyState = cstate->remoteCopyState;

	/* For PGXC related COPY, free also relation location data */
	if (IS_PGXC_COORDINATOR && remoteCopyState->rel_loc)
	{
		DataNodeCopyFinish(getLocatorNodeCount(remoteCopyState->locator),
						   (PGXCNodeHandle **)getLocatorNodeMap(remoteCopyState->locator));
		FreeRemoteCopyData(remoteCopyState);
	}
#endif
	/* No COPY FROM related resources except memory. */

	EndCopy(cstate);
}

/*
 * Read the next input line and stash it in line_buf, with conversion to
 * server encoding.
 *
 * Result is true if read was terminated by EOF, false if terminated
 * by newline.  The terminating newline or EOF marker is not included
 * in the final value of line_buf.
 */
static bool
CopyReadLine(CopyState cstate)
{
	bool result;

retry:
	resetStringInfo(&cstate->line_buf);
	cstate->line_buf_valid = true;

	/* Mark that encoding conversion hasn't occurred yet */
	cstate->line_buf_converted = false;

	/* Parse data and transfer into line_buf */
	result = CopyReadLineText(cstate);

	if (result)
	{
		/*
		 * Reached EOF.  In protocol version 3, we should ignore anything
		 * after \. up to the protocol end of copy data.  (XXX maybe better
		 * not to treat \. as special?)
		 */
		if (cstate->copy_dest == COPY_NEW_FE)
		{
			do
			{
				cstate->raw_buf_index = cstate->raw_buf_len;
			} while (CopyLoadRawBuf(cstate));
		}

		/* read data from other storage according taskList */
		if (cstate->taskList && cstate->next_task_cb)
		{
			if (cstate->line_buf.len != 0)
				elog(ERROR, "File %s ends lacks newline character(s)", 
						((FdwFileSegment *)lfirst(cstate->curTaskPtr))->filename);

			/* get next task from taskList */
			if (cstate->next_task_cb((void *)cstate))
			{
				cstate->eol_type = EOL_UNKNOWN;
				goto retry;
			}	
		}
	}
	else
	{
		/*
		 * If we didn't hit EOF, then we must have transferred the EOL marker
		 * to line_buf along with the data.  Get rid of it.
		 */
		switch (cstate->eol_type)
		{
		case EOL_NL:
			Assert(cstate->line_buf.len >= 1);
			Assert(cstate->line_buf.data[cstate->line_buf.len - 1] == '\n');
			cstate->line_buf.len--;
			cstate->line_buf.data[cstate->line_buf.len] = '\0';
			break;
		case EOL_CR:
			Assert(cstate->line_buf.len >= 1);
			Assert(cstate->line_buf.data[cstate->line_buf.len - 1] == '\r');
			cstate->line_buf.len--;
			cstate->line_buf.data[cstate->line_buf.len] = '\0';
			break;
		case EOL_CRNL:
			Assert(cstate->line_buf.len >= 2);
			Assert(cstate->line_buf.data[cstate->line_buf.len - 2] == '\r');
			Assert(cstate->line_buf.data[cstate->line_buf.len - 1] == '\n');
			cstate->line_buf.len -= 2;
			cstate->line_buf.data[cstate->line_buf.len] = '\0';
			break;
		case EOL_UD:
			if (cstate->line_buf.len < (int)strlen(cstate->eol) ||
				strstr(cstate->line_buf.data, cstate->eol) == NULL)
			{
				ereport(ERROR,
					(errcode(ERRCODE_DATA_EXCEPTION),
					errmsg("copy data corrupted")));
			}
			cstate->line_buf.len -= strlen(cstate->eol);
			cstate->line_buf.data[cstate->line_buf.len] = '\0';
			break;
		case EOL_UNKNOWN:
			/* shouldn't get here except we are transformed from insert */
			if (!cstate->internal_mode)
				Assert(false);
			break;
		}
	}

	/*
	 * Done reading the line.  Convert it to server encoding.
	 * Copy Position, an opentenbase_ora compatible feature, processes the raw data before the encoding conversion and then
	 * converts the encoding in the trans_field_encording function.
	 */
	if (cstate->need_transcoding && !cstate->fixed_mode && !cstate->fixed_position)
	{
		char *cvt;

		cvt = pg_any_to_server(cstate->line_buf.data,
							   cstate->line_buf.len,
							   cstate->file_encoding,
							   (cstate->copy_mode == COPY_TDX),
							   cstate->compatible_illegal_chars);
		if (cvt != cstate->line_buf.data)
		{
			/* transfer converted data back to line_buf */
			resetStringInfo(&cstate->line_buf);
			appendBinaryStringInfo(&cstate->line_buf, cvt, strlen(cvt));
			pfree(cvt);
		}
	}

	/* Now it's safe to use the buffer in error messages */
	cstate->line_buf_converted = true;

	return result;
}

/*
 * CopyReadLineText - inner loop of CopyReadLine for text mode
 */
static bool
CopyReadLineText(CopyState cstate)
{
	char *copy_raw_buf;
	int raw_buf_ptr;
	int copy_buf_len;
	bool need_data = false;
	bool hit_eof = false;
	bool result = false;
	char mblen_str[2];

	/* CSV variables */
	bool first_char_in_line = true;
	bool in_quote = false,
		 last_was_esc = false;
	char quotec = '\0';
	char escapec = '\0';
	int64 byte_processed = 0;

	if (cstate->csv_mode)
	{
		quotec = cstate->quote[0];
		escapec = cstate->escape[0];
		/* ignore special escape processing if it's the same as quotec */
		if (quotec == escapec)
			escapec = '\0';
	}

	mblen_str[1] = '\0';

	/*
	 * The objective of this loop is to transfer the entire next input line
	 * into line_buf.  Hence, we only care for detecting newlines (\r and/or
	 * \n) and the end-of-copy marker (\.).
	 *
	 * In CSV mode, \r and \n inside a quoted field are just part of the data
	 * value and are put in line_buf.  We keep just enough state to know if we
	 * are currently in a quoted field or not.
	 *
	 * These four characters, and the CSV escape and quote characters, are
	 * assumed the same in frontend and backend encodings.
	 *
	 * For speed, we try to move data from raw_buf to line_buf in chunks
	 * rather than one character at a time.  raw_buf_ptr points to the next
	 * character to examine; any characters from raw_buf_index to raw_buf_ptr
	 * have been determined to be part of the line, but not yet transferred to
	 * line_buf.
	 *
	 * For a little extra speed within the loop, we copy raw_buf and
	 * raw_buf_len into local variables.
	 */
	copy_raw_buf = cstate->raw_buf;
	raw_buf_ptr = cstate->raw_buf_index;
	copy_buf_len = cstate->raw_buf_len;

	for (;;)
	{
		int prev_raw_ptr;
		char c;

		byte_processed ++;

		/*
		 * Load more data if needed.  Ideally we would just force four bytes
		 * of read-ahead and avoid the many calls to
		 * IF_NEED_REFILL_AND_NOT_EOF_CONTINUE(), but the COPY_OLD_FE protocol
		 * does not allow us to read too far ahead or we might read into the
		 * next data, so we read-ahead only as far we know we can.  One
		 * optimization would be to read-ahead four byte here if
		 * cstate->copy_dest != COPY_OLD_FE, but it hardly seems worth it,
		 * considering the size of the buffer.
		 */
		if (raw_buf_ptr >= copy_buf_len || need_data)
		{
			REFILL_LINEBUF;

			/*
			 * Try to read some more data.  This will certainly reset
			 * raw_buf_index to zero, and raw_buf_ptr must go with it.
			 */
			if (!CopyLoadRawBuf(cstate))
				hit_eof = true;
			raw_buf_ptr = 0;
			copy_buf_len = cstate->raw_buf_len;

			/*
			 * If we are completely out of data, break out of the loop,
			 * reporting EOF.
			 */
			if (copy_buf_len <= 0)
			{
				result = true;
				break;
			}
			need_data = false;
#ifdef __OPENTENBASE__
			if (cstate->internal_mode)
			{
				if (!cstate->whole_line)
				{
					raw_buf_ptr = copy_buf_len;
					cstate->raw_buf_index = cstate->raw_buf_len;
					continue;
				}
				cstate->eol_type = EOL_UNKNOWN;
				return result;
			}
#endif
		}

		/* OK to fetch a character */
		prev_raw_ptr = raw_buf_ptr;
		c = copy_raw_buf[raw_buf_ptr++];

		if (cstate->csv_mode)
		{
			/*
			 * If character is '\\' or '\r', we may need to look ahead below.
			 * Force fetch of the next character if we don't already have it.
			 * We need to do this before changing CSV state, in case one of
			 * these characters is also the quote or escape character.
			 *
			 * Note: old-protocol does not like forced prefetch, but it's OK
			 * here since we cannot validly be at EOF.
			 */
			if (c == '\\' || c == '\r')
			{
				IF_NEED_REFILL_AND_NOT_EOF_CONTINUE(0);
			}

			/*
			 * Dealing with quotes and escapes here is mildly tricky. If the
			 * quote char is also the escape char, there's no problem - we
			 * just use the char as a toggle. If they are different, we need
			 * to ensure that we only take account of an escape inside a
			 * quoted field and immediately preceding a quote char, and not
			 * the second in an escape-escape sequence.
			 */
			if (in_quote && c == escapec)
				last_was_esc = !last_was_esc;
			if (c == quotec && !last_was_esc)
			{
				in_quote = !in_quote;
				if (in_quote)
					cstate->cur_quote_lineno = cstate->cur_lineno;
			}
			if (c != escapec)
				last_was_esc = false;

			/*
			 * Updating the line count for embedded CR and/or LF chars is
			 * necessarily a little fragile - this test is probably about the
			 * best we can do.  (XXX it's arguable whether we should do this
			 * at all --- is cur_lineno a physical or logical count?)
			 */
			if (in_quote && c == (cstate->eol_type == EOL_NL ? '\n' : '\r'))
				cstate->cur_lineno++;
		}

		/* Process \r */
		if (cstate->eol_type != EOL_UD && c == '\r' && (!cstate->csv_mode || !in_quote))
		{
			/* Check for \r\n on first line, _and_ handle \r\n. */
			if (cstate->eol_type == EOL_UNKNOWN ||
				cstate->eol_type == EOL_CRNL)
			{
				/*
				 * If need more data, go back to loop top to load it.
				 *
				 * Note that if we are at EOF, c will wind up as '\0' because
				 * of the guaranteed pad of raw_buf.
				 */
				IF_NEED_REFILL_AND_NOT_EOF_CONTINUE(0);

				/* get next char */
				c = copy_raw_buf[raw_buf_ptr];

				if (c == '\n')
				{
					raw_buf_ptr++;				 /* eat newline */
					cstate->eol_type = EOL_CRNL; /* in case not set yet */
				}
				else
				{
					/* found \r, but no \n */
					if (cstate->eol_type == EOL_CRNL)
						ereport(ERROR,
								(errcode(ERRCODE_BAD_COPY_FILE_FORMAT),
								 !cstate->csv_mode ? errmsg("literal carriage return found in data") : errmsg("unquoted carriage return found in data"),
								 !cstate->csv_mode ? errhint("Use \"\\r\" to represent carriage return.") : errhint("Use quoted CSV field to represent carriage return.")));

					/*
					 * if we got here, it is the first line and we didn't find
					 * \n, so don't consume the peeked character
					 */
					cstate->eol_type = EOL_CR;
				}
			}
			else if (cstate->eol_type == EOL_NL)
				ereport(ERROR,
						(errcode(ERRCODE_BAD_COPY_FILE_FORMAT),
						 !cstate->csv_mode ? errmsg("literal carriage return found in data") : errmsg("unquoted carriage return found in data"),
						 !cstate->csv_mode ? errhint("Use \"\\r\" to represent carriage return.") : errhint("Use quoted CSV field to represent carriage return.")));
			/* If reach here, we have found the line terminator */
			break;
		}

		/* Process \n */
		if (cstate->eol_type != EOL_UD && c == '\n' && (!cstate->csv_mode || !in_quote))
		{
			if (cstate->eol_type == EOL_CR || cstate->eol_type == EOL_CRNL)
				ereport(ERROR,
						(errcode(ERRCODE_BAD_COPY_FILE_FORMAT),
						 !cstate->csv_mode ? errmsg("literal newline found in data") : errmsg("unquoted newline found in data"),
						 !cstate->csv_mode ? errhint("Use \"\\n\" to represent newline.") : errhint("Use quoted CSV field to represent newline.")));
			cstate->eol_type = EOL_NL; /* in case not set yet */
			/* If reach here, we have found the line terminator */
			break;
		}

		/* Process user-define EOL string */
		if (cstate->eol_type == EOL_UD && c == cstate->eol[0] && (!cstate->csv_mode || !in_quote))
		{
			int remainLen = strlen(cstate->eol) - 1;
			int pos = 0;

			/*
			 *  If the length of EOL string is above one,
			 *  we need to look ahead several characters and check
			 *  if these characters equal remaining EOL string.
			 */
			if (remainLen > 0)
			{
				IF_NEED_REFILL_AND_NOT_EOF_CONTINUE(remainLen - 1);

				for (; pos < remainLen; pos++)
				{
					if (copy_raw_buf[raw_buf_ptr + pos] != cstate->eol[pos + 1])
					break;
				}
			}

			/* If reach here, we have found the line terminator */
			if (pos == remainLen)
			{
				raw_buf_ptr += pos;
				break;
			}
		}

		/*
		 * In CSV mode, we only recognize \. alone on a line.  This is because
		 * \. is a valid CSV data value.
		 */
		if (c == '\\' && (!cstate->csv_mode || first_char_in_line))
		{
			char c2;

			IF_NEED_REFILL_AND_NOT_EOF_CONTINUE(0);
			IF_NEED_REFILL_AND_EOF_BREAK(0);

			/* -----
			 * get next character
			 * Note: we do not change c so if it isn't \., we can fall
			 * through and continue processing for file encoding.
			 * -----
			 */
			c2 = copy_raw_buf[raw_buf_ptr];

			if (c2 == '.')
			{
				raw_buf_ptr++; /* consume the '.' */

				/*
				 * Note: if we loop back for more data here, it does not
				 * matter that the CSV state change checks are re-executed; we
				 * will come back here with no important state changed.
				 */
				if (cstate->eol_type == EOL_CRNL)
				{
					/* Get the next character */
					IF_NEED_REFILL_AND_NOT_EOF_CONTINUE(0);
					/* if hit_eof, c2 will become '\0' */
					c2 = copy_raw_buf[raw_buf_ptr++];

					if (c2 == '\n')
					{
						if (!cstate->csv_mode)
							ereport(ERROR,
									(errcode(ERRCODE_BAD_COPY_FILE_FORMAT),
									 errmsg("end-of-copy marker does not match previous newline style")));
						else
							NO_END_OF_COPY_GOTO;
					}
					else if (c2 != '\r')
					{
						if (!cstate->csv_mode)
							ereport(ERROR,
									(errcode(ERRCODE_BAD_COPY_FILE_FORMAT),
									 errmsg("end-of-copy marker corrupt")));
						else
							NO_END_OF_COPY_GOTO;
					}
				}

				/* Get the next character */
				IF_NEED_REFILL_AND_NOT_EOF_CONTINUE(0);
				/* if hit_eof, c2 will become '\0' */
				c2 = copy_raw_buf[raw_buf_ptr++];

				if (c2 != '\r' && c2 != '\n')
				{
					if (!cstate->csv_mode)
						ereport(ERROR,
								(errcode(ERRCODE_BAD_COPY_FILE_FORMAT),
								 errmsg("end-of-copy marker corrupt")));
					else
						NO_END_OF_COPY_GOTO;
				}

				if ((cstate->eol_type == EOL_NL && c2 != '\n') ||
					(cstate->eol_type == EOL_CRNL && c2 != '\n') ||
					(cstate->eol_type == EOL_CR && c2 != '\r'))
				{
					ereport(ERROR,
							(errcode(ERRCODE_BAD_COPY_FILE_FORMAT),
							 errmsg("end-of-copy marker does not match previous newline style")));
				}

				/*
				 * Transfer only the data before the \. into line_buf, then
				 * discard the data and the \. sequence.
				 */
				if (prev_raw_ptr > cstate->raw_buf_index)
					appendBinaryStringInfo(&cstate->line_buf,
										   cstate->raw_buf + cstate->raw_buf_index,
										   prev_raw_ptr - cstate->raw_buf_index);
				cstate->raw_buf_index = raw_buf_ptr;
				result = true; /* report EOF */
				break;
			}
			else if (!cstate->csv_mode)

				/*
				 * If we are here, it means we found a backslash followed by
				 * something other than a period.  In non-CSV mode, anything
				 * after a backslash is special, so we skip over that second
				 * character too.  If we didn't do that \\. would be
				 * considered an eof-of copy, while in non-CSV mode it is a
				 * literal backslash followed by a period.  In CSV mode,
				 * backslashes are not special, so we want to process the
				 * character after the backslash just like a normal character,
				 * so we don't increment in those cases.
				 */
				raw_buf_ptr++;
		}

		/*
		 * This label is for CSV cases where \. appears at the start of a
		 * line, but there is more text after it, meaning it was a data value.
		 * We are more strict for \. in CSV mode because \. could be a data
		 * value, while in non-CSV mode, \. cannot be a data value.
		 */
	not_end_of_copy:

		/*
		 * Process all bytes of a multi-byte character as a group.
		 *
		 * We only support multi-byte sequences where the first byte has the
		 * high-bit set, so as an optimization we can avoid this block
		 * entirely if it is not set.
		 */
		if (cstate->encoding_embeds_ascii && IS_HIGHBIT_SET(c))
		{
			int mblen;

			mblen_str[0] = c;
			/* All our encodings only read the first byte to get the length */
			mblen = pg_encoding_mblen(cstate->file_encoding, mblen_str);
			IF_NEED_REFILL_AND_NOT_EOF_CONTINUE(mblen - 1);
			IF_NEED_REFILL_AND_EOF_BREAK(mblen - 1);
			raw_buf_ptr += mblen - 1;
		}
		first_char_in_line = false;
	} /* end of outer loop */

	/*
	 * Transfer any still-uncopied data to line_buf.
	 */
	REFILL_LINEBUF;
	cstate->cur_bytenum += byte_processed;

	return result;
}

/*
 *	Return decimal value for a hexadecimal digit
 */
static int
GetDecimalFromHex(char hex)
{
	if (isdigit((unsigned char)hex))
		return hex - '0';
	else
		return tolower((unsigned char)hex) - 'a' + 10;
}

#ifdef __OPENTENBASE__
static int
CopyReadAttributesInternal(CopyState cstate)
{
	int fieldno;
	uint16 n16;
	uint32 n32;
	int i;
	int col_count;
	int data_len = 0;
	//char *cur = cstate->raw_buf + cstate->raw_buf_index;
	char *cur = cstate->fe_msgbuf->data;

	memcpy(&n16, cur, 2);
	cur += 2;
	//cstate->raw_buf_index += 2;
	data_len += 2;
	col_count = pg_ntoh16(n16);

	fieldno = 0;
	for (i = 0; i < col_count; i++)
	{
		int len;

		/* get size */
		memcpy(&n32, cur, 4);
		cur += 4;
		//cstate->raw_buf_index += 4;
		data_len += 4;
		len = pg_ntoh32(n32);

		if (i + 1 >= cstate->max_fields)
		{
			cstate->max_fields *= 2;
			cstate->raw_fields =
				repalloc(cstate->raw_fields, cstate->max_fields * sizeof(char *));
		}

		if (len == -1)
		{
			cstate->raw_fields[fieldno] = NULL;
		}
		else
		{
			cstate->raw_fields[fieldno] = cur;
			cur += len;
			//cstate->raw_buf_index += len;
			data_len += len;
		}

		fieldno++;
	}

	cstate->raw_buf_index = cstate->raw_buf_len;

	if (data_len != cstate->fe_msgbuf->len)
	{
		elog(ERROR, "copy datarow corrupted, expected data_length %d, result data_length %d",
			 cstate->fe_msgbuf->len, data_len);
	}

	return fieldno;
}
#endif

#ifdef _PG_ORCL_

static char*
get_rowid_attribute_text(CopyState cstate, int fix_size)
{
	RowId	n_rid;
	char	*rid_str;

	n_rid = nextval_internal(cstate->rid_glob_seqId, false);
	rid_str = DatumGetCString(DirectFunctionCall1(int8out,
												  Int64GetDatum(n_rid)));

	if (fix_size > 0)
	{
		char	*buf = (char *) palloc(fix_size + 1);
		int		size = (int) strlen(rid_str);

		memset(buf, (int) ' ', fix_size);
		buf[fix_size] = '\0';
		if (size > fix_size)
			elog(ERROR, "rowid too long.");

		memcpy(buf, rid_str, size);
		pfree(rid_str);
		return buf;
	}

	return rid_str;
}

/*
 * Insert a rowid in parsed text line.
 */
static void
insert_rowid_attribute_text(CopyState cstate, int brk_pos, int need_fix_size)
{
	char	*rid_str;

	Assert(IS_PGXC_COORDINATOR && cstate->rid_glob_seqId != InvalidOid);

	if (brk_pos > 0)
	{
		appendBinaryStringInfo(&cstate->text_line_buf_rid, cstate->line_buf.data,
									brk_pos);

		if (strncmp(cstate->line_buf.data + (brk_pos - cstate->delim_len), cstate->delim, cstate->delim_len) != 0)
			appendStringInfoString(&cstate->text_line_buf_rid, cstate->delim);

		/* Include a delimiter */
	}

	/* Append rowid */
	rid_str = get_rowid_attribute_text(cstate, need_fix_size);

	appendStringInfo(&cstate->text_line_buf_rid, "%s", rid_str);

	if (cstate->line_buf.len - brk_pos > 0)
	{
		if (need_fix_size <= 0)
			appendStringInfoString(&cstate->text_line_buf_rid, cstate->delim);

		/* Append remains */
		appendBinaryStringInfo(&cstate->text_line_buf_rid,
									cstate->line_buf.data + brk_pos,
									cstate->line_buf.len - brk_pos);
	}

	pfree(rid_str);
}
#endif

/*
 * Scan from cur_mb_char to start_ptr to find out all multibyte characters.
 * A field should not start from non-starting position of a multibyte character.
*/
static void
CheckIfTruncMbChar(CopyState cstate)
{
	int       mb_len = 0;
	ListCell *cell = NULL;
	Position *pos = NULL;
	AttInfo  *attr = NULL;
	char      last_mb_char[MAX_LEN];
	char     *start_ptr = cstate->line_buf.data;
	char     *cur_mb_ptr = cstate->line_buf.data;

	memset(last_mb_char, 0, MAX_LEN);

	foreach (cell, cstate->attinfolist_sorted)
	{
		attr = (AttInfo *) lfirst(cell);
		pos = attr->attrpos;

		start_ptr = cstate->line_buf.data + pos->start_offset - 1;
		for (;;)
		{
			mb_len = pg_encoding_mblen(cstate->file_encoding, cur_mb_ptr);

			if (cur_mb_ptr == start_ptr)
				break;
			if (cur_mb_ptr > start_ptr)
				ereport(ERROR,
				        (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				         errmsg("multibyte character \"%s\" is truncated for column %s",
				                last_mb_char,
				                strVal(attr->attname))));

			if (mb_len > 1)
			{
				strncpy(last_mb_char, cur_mb_ptr, mb_len);
				last_mb_char[mb_len] = '\0';
			}
			cur_mb_ptr += mb_len;
		}
	}
}

/*
 * Copy Position is used for opentenbase_ora compatibility.
 * When encoding conversion is required, the original encoding is converted to the corresponding
 * encoding of the current database, and the converted data is stored in attribute_buf.
 */
static void
transform_field_encording(CopyState cstate, int fieldno)
{
	int   i = 0, pos = 0, cvt_size = 0;
	char *cvt = NULL;
	int *raw_fields_pos = NULL;

	if (!cstate->need_transcoding || !fieldno)
	{
		return;
	}

	resetStringInfo(&cstate->attribute_buf);

	/*
	 * We need to record the actual position of each field after transcoding,
	 * as there is no guarantee that the first memory address applied by each appendBinaryStringInfo call
	 * will not change.
	 */
	raw_fields_pos = palloc(fieldno * sizeof(int));

	for (i = 0; i < fieldno; i++)
	{
		/*
		 * In clustered mode, DN does not repeat the conversion because the CN sends the converted
		 * result to the DN. However, with Copy Position from, the original data needs to be parsed,
		 * so the encoding conversion needs to be moved to the DN.
		 * e.g.
		 * "\copy dest_txt(a position (1:4), d_day position(*:41)) from stdin with csv encoding
		 * 'utf8';"
		 */
		cvt = pg_any_to_server(cstate->raw_fields[i],
		                       cstate->raw_fields[i] ? strlen(cstate->raw_fields[i]) : 0,
		                       cstate->file_encoding,
		                       true,
		                       false);
		if (cvt)
		{
			cvt_size = strlen(cvt) + 1;

			/* transfer converted data back to attribute_buf */
			appendBinaryStringInfo(&cstate->attribute_buf, cvt, cvt_size);

			/* Release the newly allocated memory of PG_any_TO_server */
			if (cvt != cstate->raw_fields[i])
			{
				pfree(cvt);
			}

			raw_fields_pos[i] = pos;
			pos += cvt_size;
		}
	}

	/* Sets the final raw_fields value */
	for (i = 0; i < fieldno && cstate->raw_fields[i]; i++)
	{
		cstate->raw_fields[i] = cstate->attribute_buf.data + raw_fields_pos[i];
	}

	cstate->attribute_buf.len = pos;
	pfree(raw_fields_pos);
}

static int
CopyReadAttributesTEXT_POS(CopyState cstate)
{
	List      *attinfolist = cstate->attinfolist;
	int        fieldno;
	char      *output_ptr;
	char      *cur_ptr;
	char      *line_end_ptr;
	ListCell  *cell = NULL;
	int        attribute_len = 0;
	int        rid_idx = -1;
	StringInfo attribute_buf =
		cstate->need_transcoding ? &cstate->attribute_buf_before : &cstate->attribute_buf;
	bool       has_nodata = false;

	/*
	 * We need a special case for zero-column tables: check that the input
	 * line is empty, and return.
	 */
	if (cstate->max_fields <= 0)
	{
		if (cstate->line_buf.len != 0)
			ereport(ERROR,
			        (errcode(ERRCODE_BAD_COPY_FILE_FORMAT),
			         errmsg("extra data after last expected column")));
		return 0;
	}

	resetStringInfo(attribute_buf);

#ifdef _PG_ORCL_
	resetStringInfo(&cstate->text_line_buf_rid);
	if (!cstate->file_has_rowids && cstate->rid_glob_seqId != InvalidOid && IS_PGXC_COORDINATOR)
	{
		if (cstate->file_has_oids)
			rid_idx = 1;
		else
		{
			insert_rowid_attribute_text(cstate, 0, 0);
			rid_idx = -1;
		}
	}
#endif

	/*
	 * The de-escaped attributes will certainly not be longer than the input
	 * data line and the sum of length of all fields, so we can just force attribute_buf to be large
	 * enough and then transfer data without any checks for enough space.  We need to do it this way
	 * because enlarging attribute_buf mid-stream would invalidate pointers already stored into
	 * cstate->raw_fields[].
	 */
	foreach (cell, attinfolist)
	{
		AttInfo *attr_info = (AttInfo *) lfirst(cell);
		Assert(attr_info->attrpos);

		/* Each attribute_len needs to contain '\0' */
		attribute_len += (attr_info->attrpos->end_offset - attr_info->attrpos->start_offset + 1) + 1;
	}
	attribute_len = (attribute_len > cstate->line_buf.len) ? attribute_len : cstate->line_buf.len;
	if (attribute_buf->maxlen <= attribute_len)
		enlargeStringInfo(attribute_buf, attribute_len);
	output_ptr = attribute_buf->data;

	/* set pointer variables for loop */
	cur_ptr = cstate->line_buf.data;
	line_end_ptr = cstate->line_buf.data + cstate->line_buf.len;

	CheckIfTruncMbChar(cstate);

	/* Outer loop iterates over fields */
	fieldno = 0;
	foreach (cell, attinfolist)
	{
		char     *start_ptr;
		char     *end_ptr;
		int       input_len;
		bool      saw_non_ascii = false;
		Position *attrpos = NULL;
		/* for error message, if a multiple-byte char finished before end_ptr. */
		int       mb_left_bytes = 0;
		char      last_mb_char[MAX_LEN];

		AttInfo  *attr_info = (AttInfo *) lfirst(cell);
		memset(last_mb_char, 0, MAX_LEN);

		if (attr_info->attrpos)
		{
			attrpos = attr_info->attrpos;
		}
		else
		{
			ereport(ERROR,
			        (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
			         errmsg("Invalid column position for column %s", strVal(attr_info->attname))));
		}

		/* Make sure there is enough space for the next value */
		if (fieldno >= cstate->max_fields)
		{
			cstate->max_fields *= 2;
			cstate->raw_fields = repalloc(cstate->raw_fields, cstate->max_fields * sizeof(char *));
		}

		/* Null by default if no data can be read */
		if (attrpos->start_offset > cstate->line_buf.len ||
		    (attrpos->flag == RELATIVE_POS && (has_nodata || *cur_ptr == '\0')))
		{
			has_nodata = true;
			cstate->raw_fields[fieldno++] = NULL;

			if (fieldno == rid_idx)
			{
				Assert(!cstate->file_has_rowids && cstate->rid_glob_seqId != InvalidOid);
				Assert(IS_PGXC_COORDINATOR);
				insert_rowid_attribute_text(cstate, cur_ptr - cstate->line_buf.data, 0);
				rid_idx = -1;
			}

			continue;
		}
		else
		{
			has_nodata = false;
		}

		/* current field start from start_offset, relative or absolute. */
		if (attrpos->flag == RELATIVE_POS)
			cur_ptr = cur_ptr + attrpos->start_offset;
		else
			cur_ptr = cstate->line_buf.data + attrpos->start_offset - 1;

		if (cur_ptr - cstate->line_buf.data + 1 > attrpos->end_offset)
			ereport(ERROR,
			        (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
			         errmsg("Fail to get value for column %s ", strVal(attr_info->attname)),
			         errhint("start position(%ld) > end position(%d)",
			                 cur_ptr - cstate->line_buf.data + 1,
			                 attrpos->end_offset)));

		/* Remember start of field on both input and output sides. */
		start_ptr = cur_ptr;
		cstate->raw_fields[fieldno] = output_ptr;

		/* Scan data for field. */
		for (;;)
		{
			char c;
			end_ptr = cur_ptr;

			/* if reach the file end position. */
			if (cur_ptr >= line_end_ptr)
				break;
			/* if reach the specified position. */
			if (cur_ptr - cstate->line_buf.data >= attrpos->end_offset)
				break;

			/* optimization for single byte encoding and fewer comparision*/
			if ((pg_database_encoding_max_length() > 1) &&
			    (attrpos->end_offset - (cur_ptr - cstate->line_buf.data) <=
			     pg_database_encoding_max_length()))
			{
				/* a new character begins here. */
				if (mb_left_bytes == 0)
				{
					mb_left_bytes = pg_encoding_mblen(cstate->file_encoding, cur_ptr);
					if (mb_left_bytes > 1)
					{
						strncpy(last_mb_char, cur_ptr, mb_left_bytes);
						last_mb_char[mb_left_bytes] = '\0';
					}
				}

				mb_left_bytes--;
			}

			c = *cur_ptr++;

			if (c == '\\')
			{
				if (cur_ptr >= line_end_ptr)
					break;
				c = *cur_ptr++;
				switch (c)
				{
					case '0':
					case '1':
					case '2':
					case '3':
					case '4':
					case '5':
					case '6':
					case '7':
					{
						/* handle \013 */
						int val;

						val = OCTVALUE(c);
						if (cur_ptr < line_end_ptr)
						{
							c = *cur_ptr;
							if (ISOCTAL(c))
							{
								cur_ptr++;
								val = (val << 3) + OCTVALUE(c);
								if (cur_ptr < line_end_ptr)
								{
									c = *cur_ptr;
									if (ISOCTAL(c))
									{
										cur_ptr++;
										val = (val << 3) + OCTVALUE(c);
									}
								}
							}
						}
						c = val & 0377;
						if (c == '\0' || IS_HIGHBIT_SET(c))
							saw_non_ascii = true;
					}
					break;
					case 'x':
						/* Handle \x3F */
						if (cur_ptr < line_end_ptr)
						{
							char hexchar = *cur_ptr;

							if (isxdigit((unsigned char) hexchar))
							{
								int val = GetDecimalFromHex(hexchar);

								cur_ptr++;
								if (cur_ptr < line_end_ptr)
								{
									hexchar = *cur_ptr;
									if (isxdigit((unsigned char) hexchar))
									{
										cur_ptr++;
										val = (val << 4) + GetDecimalFromHex(hexchar);
									}
								}
								c = val & 0xff;
								if (c == '\0' || IS_HIGHBIT_SET(c))
									saw_non_ascii = true;
							}
						}
						break;
					case 'b':
						c = '\b';
						break;
					case 'f':
						c = '\f';
						break;
					case 'n':
						c = '\n';
						break;
					case 'r':
						c = '\r';
						break;
					case 't':
						c = '\t';
						break;
					case 'v':
						c = '\v';
						break;
						/* in all other cases, take the char after '\' literally */
				}
			}
			/* Add c to output string */
			*output_ptr++ = c;
		}
		if (mb_left_bytes != 0)
			ereport(ERROR,
			        (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
			         errmsg("multibyte character \"%s\" is truncated for column %s",
			                last_mb_char,
			                strVal(attr_info->attname))));

		/* Check whether raw input matched null marker */
		input_len = end_ptr - start_ptr;
		if (input_len == cstate->null_print_len &&
		    strncmp(start_ptr, cstate->null_print, input_len) == 0)
			cstate->raw_fields[fieldno] = NULL;
		else
		{
			/*
			 * At this point we know the field is supposed to contain data.
			 *
			 * If we de-escaped any non-7-bit-ASCII chars, make sure the
			 * resulting string is valid data for the db encoding.
			 */
			if (saw_non_ascii)
			{
				char *fld = cstate->raw_fields[fieldno];

				pg_verifymbstr(fld, output_ptr - fld, false);
			}
		}

		/* Terminate attribute value in output area */
		*output_ptr++ = '\0';

		fieldno++;

#ifdef _PG_ORCL_
		if (fieldno == rid_idx)
		{
			Assert(!cstate->file_has_rowids && cstate->rid_glob_seqId != InvalidOid);
			Assert(IS_PGXC_COORDINATOR);

			insert_rowid_attribute_text(cstate, cur_ptr - cstate->line_buf.data, 0);
			rid_idx = -1;
		}
#endif
	}

	/* Clean up state of attribute_buf */
	output_ptr--;
	Assert(*output_ptr == '\0');
	attribute_buf->len = (output_ptr - attribute_buf->data);

	/* Now we can transform encoding formats */
	transform_field_encording(cstate, fieldno);

	return fieldno;
}

static int
CopyReadAttributesCSV_POS(CopyState cstate)
{
	char       quotec = cstate->quote[0];
	char       escapec = cstate->escape[0];
	List      *attinfolist = cstate->attinfolist;
	int        fieldno;
	char      *output_ptr;
	char      *cur_ptr;
	char      *line_end_ptr;
	ListCell  *cell = NULL;
	int        attribute_len = 0;
	int        rid_idx = -1;
	StringInfo attribute_buf =
		cstate->need_transcoding ? &cstate->attribute_buf_before : &cstate->attribute_buf;
	bool       has_nodata = false;

	/*
	 * We need a special case for zero-column tables: check that the input
	 * line is empty, and return.
	 */
	if (cstate->max_fields <= 0)
	{
		if (cstate->line_buf.len != 0)
			ereport(ERROR,
			        (errcode(ERRCODE_BAD_COPY_FILE_FORMAT),
			         errmsg("extra data after last expected column")));
		return 0;
	}

	resetStringInfo(attribute_buf);
#ifdef _PG_ORCL_
	resetStringInfo(&cstate->text_line_buf_rid);
	if (!cstate->file_has_rowids && cstate->rid_glob_seqId != InvalidOid && IS_PGXC_COORDINATOR)
	{
		if (cstate->file_has_oids)
			rid_idx = 1;
		else
		{
			insert_rowid_attribute_text(cstate, 0, 0);
			rid_idx = -1;
		}
	}
#endif

	/*
	 * The de-escaped attributes will certainly not be longer than the input
	 * data line and the sum of length of all fields, so we can just force attribute_buf to be large
	 * enough and then transfer data without any checks for enough space.  We need to do it this way
	 * because enlarging attribute_buf mid-stream would invalidate pointers already stored into
	 * cstate->raw_fields[].
	 */
	foreach (cell, attinfolist)
	{
		AttInfo *attr_info = (AttInfo *) lfirst(cell);
		Assert(attr_info->attrpos);

		/* Each attribute_len needs to contain '\0' */
		attribute_len += (attr_info->attrpos->end_offset - attr_info->attrpos->start_offset + 1) + 1;
	}
	attribute_len = (attribute_len > cstate->line_buf.len) ? attribute_len : cstate->line_buf.len;
	if (attribute_buf->maxlen <= attribute_len)
		enlargeStringInfo(attribute_buf, attribute_len);
	output_ptr = attribute_buf->data;

	/* set pointer variables for loop */
	cur_ptr = cstate->line_buf.data;
	line_end_ptr = cstate->line_buf.data + cstate->line_buf.len;

	CheckIfTruncMbChar(cstate);

	/* Outer loop iterates over fields */
	fieldno = 0;

	foreach (cell, attinfolist)
	{
		char     *start_ptr;
		char     *end_ptr;
		int       input_len;
		Position *attrpos = NULL;
		bool      saw_quote = false;
		/* for error message,if a multibyte char finished before end_ptr.  */
		int       mb_left_bytes = 0;
		char      last_mb_char[MAX_LEN];

		AttInfo  *attr_info = (AttInfo *) lfirst(cell);
		memset(last_mb_char, 0, MAX_LEN);
		if (attr_info->attrpos)
		{
			attrpos = attr_info->attrpos;
		}
		else
		{
			ereport(ERROR,
			        (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
			         errmsg("Invalid column position for column %s", strVal(attr_info->attname))));
		}

		/* Make sure there is enough space for the next value */
		if (fieldno >= cstate->max_fields)
		{
			cstate->max_fields *= 2;
			cstate->raw_fields = repalloc(cstate->raw_fields, cstate->max_fields * sizeof(char *));
		}

		/* Null by default if no data can be read */
		if (attrpos->start_offset > cstate->line_buf.len ||
		    (attrpos->flag == RELATIVE_POS && (has_nodata || *cur_ptr == '\0')))
		{
			has_nodata = true;
			cstate->raw_fields[fieldno++] = NULL;

			if (fieldno == rid_idx)
			{
				Assert(!cstate->file_has_rowids && cstate->rid_glob_seqId != InvalidOid);
				Assert(IS_PGXC_COORDINATOR);
				insert_rowid_attribute_text(cstate, cur_ptr - cstate->line_buf.data, 0);
				rid_idx = -1;
			}

			continue;
		}
		else
		{
			has_nodata = false;
		}

		/* current field start from start_offset, relative or absolute. */
		if (attrpos->flag == RELATIVE_POS)
			cur_ptr = cur_ptr + attrpos->start_offset;
		else
			cur_ptr = cstate->line_buf.data + (attrpos->start_offset - 1);

		if (cur_ptr - cstate->line_buf.data + 1 > attrpos->end_offset)
			ereport(ERROR,
			        (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
			         errmsg("Fail to get value for column %s ", strVal(attr_info->attname)),
			         errhint("start position(%ld) > end position(%d)",
			                 cur_ptr - cstate->line_buf.data + 1,
			                 attrpos->end_offset)));

		/* Remember start of field on both input and output sides. */
		start_ptr = cur_ptr;
		cstate->raw_fields[fieldno] = output_ptr;

		/* Scan data for field. */
		for (;;)
		{
			char c;
			/* Not in quote */
			for (;;)
			{
				end_ptr = cur_ptr;

				/* if reach the filed end position. */
				if (cur_ptr >= line_end_ptr)
					goto pos_endfield;
				if (cur_ptr - cstate->line_buf.data >= attrpos->end_offset)
					goto pos_endfield;

				/* optimization for single byte encoding and fewer comparision.*/
				if ((pg_database_encoding_max_length() > 1) &&
				    (attrpos->end_offset - (cur_ptr - cstate->line_buf.data) <=
				     pg_database_encoding_max_length()))
				{
					/* a new character begins here. */
					if (mb_left_bytes == 0)
					{
						mb_left_bytes = pg_encoding_mblen(cstate->file_encoding, cur_ptr);

						if (mb_left_bytes > 1)
						{
							strncpy(last_mb_char, cur_ptr, mb_left_bytes);
							last_mb_char[mb_left_bytes] = '\0';
						}
					}
					mb_left_bytes--;
				}

				c = *cur_ptr++;
				/* start of quoted field (or part of field) */
				if (c == quotec)
				{
					saw_quote = true;
					break;
				}

				/* Add c to output string */
				*output_ptr++ = c;
			}

			/* In quote */
			for (;;)
			{
				end_ptr = cur_ptr;
				if ((cur_ptr >= line_end_ptr) ||
				    (cur_ptr - cstate->line_buf.data >=
				     attrpos->end_offset)) /* if reach the filed end position. */
				{
					/* In quote, use the lineno that start with quotation marks. */
					cstate->cur_lineno = cstate->cur_quote_lineno;
					ereport(ERROR,
					        (errcode(ERRCODE_BAD_COPY_FILE_FORMAT),
					         errmsg("unterminated CSV quoted field")));
				}

				c = *cur_ptr++;
				/* escape within a quoted field */
				if (c == escapec)
				{
					/*
					 * peek at the next char if available, and escape it if it
					 * is an escape char or a quote char
					 */
					if (cur_ptr < line_end_ptr)
					{
						char nextc = *cur_ptr;

						if (nextc == escapec || nextc == quotec)
						{
							*output_ptr++ = nextc;
							cur_ptr++;
							continue;
						}
					}
				}

				/*
				 * end of quoted field. Must do this test after testing for
				 * escape in case quote char and escape char are the same
				 * (which is the common case).
				 */
				if (c == quotec)
					break;

				/* Add c to output string */
				*output_ptr++ = c;
			}
		}

	pos_endfield:
		if (mb_left_bytes != 0)
			ereport(ERROR,
			        (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
			         errmsg("multibyte character \"%s\" is truncated for column %s",
			                last_mb_char,
			                strVal(attr_info->attname))));

		/* Terminate attribute value in output area */
		*output_ptr++ = '\0';

		/* Check whether raw input matched null marker */
		input_len = end_ptr - start_ptr;
		if (!saw_quote && input_len == cstate->null_print_len &&
		    strncmp(start_ptr, cstate->null_print, input_len) == 0)
			cstate->raw_fields[fieldno] = NULL;

		fieldno++;

#ifdef _PG_ORCL_
		if (fieldno == rid_idx)
		{
			Assert(!cstate->file_has_rowids && cstate->rid_glob_seqId != InvalidOid);
			Assert(IS_PGXC_COORDINATOR);

			insert_rowid_attribute_text(cstate, cur_ptr - cstate->line_buf.data, 0);
			rid_idx = -1;
		}
#endif
	}

	/* Clean up state of attribute_buf */
	output_ptr--;
	Assert(*output_ptr == '\0');
	attribute_buf->len = (output_ptr - attribute_buf->data);

	/* Now we can transform encoding formats */
	transform_field_encording(cstate, fieldno);

	return fieldno;
}

/*
 * Parse the current line into separate attributes (fields),
 * performing de-escaping as needed.
 *
 * The input is in line_buf.  We use attribute_buf to hold the result
 * strings.  cstate->raw_fields[k] is set to point to the k'th attribute
 * string, or NULL when the input matches the null marker string.
 * This array is expanded as necessary.
 *
 * (Note that the caller cannot check for nulls since the returned
 * string would be the post-de-escaping equivalent, which may look
 * the same as some valid data string.)
 *
 * delim is the column delimiter string (must be just one byte for now).
 * null_print is the null marker string.  Note that this is compared to
 * the pre-de-escaped input string.
 *
 * The return value is the number of fields actually read.
 */
static int
CopyReadAttributesText(CopyState cstate)
{
	char escapec = cstate->escape[0];
	int fieldno;
	char *output_ptr;
	char *cur_ptr;
	char *line_end_ptr;
#ifdef _PG_ORCL_
	int			rid_idx = -1;
#endif

	/* TODO: data has been processed by tdx in advance, so skip position mode */
	if (cstate->fixed_position)
		return CopyReadAttributesTEXT_POS(cstate);
	/*
	 * We need a special case for zero-column tables: check that the input
	 * line is empty, and return.
	 */
	if (cstate->max_fields <= 0)
	{
		if (cstate->line_buf.len != 0)
			ereport(ERROR,
					(errcode(ERRCODE_BAD_COPY_FILE_FORMAT),
					 errmsg("extra data after last expected column")));
		return 0;
	}

	resetStringInfo(&cstate->attribute_buf);

#ifdef _PG_ORCL_
	resetStringInfo(&cstate->text_line_buf_rid);
	if (!cstate->file_has_rowids && cstate->rid_glob_seqId != InvalidOid &&
							IS_PGXC_COORDINATOR)
	{
		if (cstate->file_has_oids)
			rid_idx = 1;
		else
		{
			insert_rowid_attribute_text(cstate, 0, 0);
			rid_idx = -1;
		}
	}
#endif

	/*
	 * The de-escaped attributes will certainly not be longer than the input
	 * data line, so we can just force attribute_buf to be large enough and
	 * then transfer data without any checks for enough space.  We need to do
	 * it this way because enlarging attribute_buf mid-stream would invalidate
	 * pointers already stored into cstate->raw_fields[].
	 */
	if (cstate->attribute_buf.maxlen <= cstate->line_buf.len)
		enlargeStringInfo(&cstate->attribute_buf, cstate->line_buf.len);
	output_ptr = cstate->attribute_buf.data;

	/* set pointer variables for loop */
	cur_ptr = cstate->line_buf.data;
	line_end_ptr = cstate->line_buf.data + cstate->line_buf.len;

	/* Outer loop iterates over fields */
	fieldno = 0;
	for (;;)
	{
		bool found_delim = false;
		char *start_ptr;
		char *end_ptr;
		int input_len;
		bool saw_non_ascii = false;

		/* Make sure there is enough space for the next value */
		if (fieldno >= cstate->max_fields)
		{
			cstate->max_fields *= 2;
			cstate->raw_fields =
				repalloc(cstate->raw_fields, cstate->max_fields * sizeof(char *));
		}

		/* Remember start of field on both input and output sides */
		start_ptr = cur_ptr;
		cstate->raw_fields[fieldno] = output_ptr;

		/*
		 * Scan data for field.
		 *
		 * Note that in this loop, we are scanning to locate the end of field
		 * and also speculatively performing de-escaping.  Once we find the
		 * end-of-field, we can match the raw field contents against the null
		 * marker string.  Only after that comparison fails do we know that
		 * de-escaping is actually the right thing to do; therefore we *must
		 * not* throw any syntax errors before we've done the null-marker
		 * check.
		 */
		for (;;)
		{
			char c;

			end_ptr = cur_ptr;
			if (cur_ptr >= line_end_ptr)
				break;
			c = *cur_ptr++;
			if (strncmp(cstate->delim, cur_ptr - 1, cstate->delim_len) == 0)
			{
				cur_ptr += (cstate->delim_len - 1);
				found_delim = true;
				break;
			}
			if (c == escapec && !cstate->escape_off)
			{
				if (cur_ptr >= line_end_ptr)
					break;
				c = *cur_ptr++;
				switch (c)
				{
				case '0':
				case '1':
				case '2':
				case '3':
				case '4':
				case '5':
				case '6':
				case '7':
				{
					/* handle \013 */
					int val;

					val = OCTVALUE(c);
					if (cur_ptr < line_end_ptr)
					{
						c = *cur_ptr;
						if (ISOCTAL(c))
						{
							cur_ptr++;
							val = (val << 3) + OCTVALUE(c);
							if (cur_ptr < line_end_ptr)
							{
								c = *cur_ptr;
								if (ISOCTAL(c))
								{
									cur_ptr++;
									val = (val << 3) + OCTVALUE(c);
								}
							}
						}
					}
					c = val & 0377;
					if (c == '\0' || IS_HIGHBIT_SET(c))
						saw_non_ascii = true;
				}
				break;
				case 'x':
					/* Handle \x3F */
					if (cur_ptr < line_end_ptr)
					{
						char hexchar = *cur_ptr;

						if (isxdigit((unsigned char)hexchar))
						{
							int val = GetDecimalFromHex(hexchar);

							cur_ptr++;
							if (cur_ptr < line_end_ptr)
							{
								hexchar = *cur_ptr;
								if (isxdigit((unsigned char)hexchar))
								{
									cur_ptr++;
									val = (val << 4) + GetDecimalFromHex(hexchar);
								}
							}
							c = val & 0xff;
							if (c == '\0' || IS_HIGHBIT_SET(c))
								saw_non_ascii = true;
						}
					}
					break;
				case 'b':
					c = '\b';
					break;
				case 'f':
					c = '\f';
					break;
				case 'n':
					c = '\n';
					break;
				case 'r':
					c = '\r';
					break;
				case 't':
					c = '\t';
					break;
				case 'v':
					c = '\v';
					break;

					/*
						 * in all other cases, take the char after '\'
						 * literally
						 */
				}
			}

			/* Add c to output string */
			*output_ptr++ = c;
		}

		/* Check whether raw input matched null marker */
		input_len = end_ptr - start_ptr;
		if (input_len == cstate->null_print_len &&
			strncmp(start_ptr, cstate->null_print, input_len) == 0)
			cstate->raw_fields[fieldno] = NULL;
		else
		{
			/*
			 * At this point we know the field is supposed to contain data.
			 *
			 * If we de-escaped any non-7-bit-ASCII chars, make sure the
			 * resulting string is valid data for the db encoding.
			 */
			if (saw_non_ascii)
			{
				char *fld = cstate->raw_fields[fieldno];

				pg_verifymbstr(fld, output_ptr - fld, false);
			}
		}

		/* Terminate attribute value in output area */
		*output_ptr++ = '\0';

		fieldno++;

#ifdef _PG_ORCL_
		if (fieldno == rid_idx)
		{
			Assert(!cstate->file_has_rowids && cstate->rid_glob_seqId != InvalidOid);
			Assert(IS_PGXC_COORDINATOR);

			insert_rowid_attribute_text(cstate, cur_ptr - cstate->line_buf.data, 0);
			rid_idx = -1;
		}
#endif
		/* Done if we hit EOL instead of a delim */
		if (!found_delim)
			break;
	}

	/* Clean up state of attribute_buf */
	output_ptr--;
	Assert(*output_ptr == '\0');
	cstate->attribute_buf.len = (output_ptr - cstate->attribute_buf.data);

	return fieldno;
}

/*
 * Parse the current line into separate attributes (fields),
 * performing de-escaping as needed.  This has exactly the same API as
 * CopyReadAttributesText, except we parse the fields according to
 * "standard" (i.e. common) CSV usage.
 */
static int
CopyReadAttributesCSV(CopyState cstate)
{
	char quotec = cstate->quote[0];
	char escapec = cstate->escape[0];
	int fieldno;
	char *output_ptr;
	char *cur_ptr;
	char *line_end_ptr;
#ifdef _PG_ORCL_
	int			rid_idx = -1;
#endif

	/* TODO: data has been processed by tdx in advance, so skip position mode */
	if (cstate->fixed_position)
		return CopyReadAttributesCSV_POS(cstate);
	/*
	 * We need a special case for zero-column tables: check that the input
	 * line is empty, and return.
	 */
	if (cstate->max_fields <= 0)
	{
		if (cstate->line_buf.len != 0)
			ereport(ERROR,
					(errcode(ERRCODE_BAD_COPY_FILE_FORMAT),
					 errmsg("extra data after last expected column")));
		return 0;
	}

	resetStringInfo(&cstate->attribute_buf);
#ifdef _PG_ORCL_
	resetStringInfo(&cstate->text_line_buf_rid);
	if (!cstate->file_has_rowids && cstate->rid_glob_seqId != InvalidOid &&
							IS_PGXC_COORDINATOR)
	{
		if (cstate->file_has_oids)
			rid_idx = 1;
		else
		{
			insert_rowid_attribute_text(cstate, 0, 0);
			rid_idx = -1;
		}
	}
#endif

	/*
	 * The de-escaped attributes will certainly not be longer than the input
	 * data line, so we can just force attribute_buf to be large enough and
	 * then transfer data without any checks for enough space.  We need to do
	 * it this way because enlarging attribute_buf mid-stream would invalidate
	 * pointers already stored into cstate->raw_fields[].
	 */
	if (cstate->attribute_buf.maxlen <= cstate->line_buf.len)
		enlargeStringInfo(&cstate->attribute_buf, cstate->line_buf.len);
	output_ptr = cstate->attribute_buf.data;

	/* set pointer variables for loop */
	cur_ptr = cstate->line_buf.data;
	line_end_ptr = cstate->line_buf.data + cstate->line_buf.len;

	/* Outer loop iterates over fields */
	fieldno = 0;
	for (;;)
	{
		bool found_delim = false;
		bool saw_quote = false;
		char *start_ptr;
		char *end_ptr;
		int input_len;

		/* Make sure there is enough space for the next value */
		if (fieldno >= cstate->max_fields)
		{
			cstate->max_fields *= 2;
			cstate->raw_fields =
				repalloc(cstate->raw_fields, cstate->max_fields * sizeof(char *));
		}

		/* Remember start of field on both input and output sides */
		start_ptr = cur_ptr;
		cstate->raw_fields[fieldno] = output_ptr;

		/*
		 * Scan data for field,
		 *
		 * The loop starts in "not quote" mode and then toggles between that
		 * and "in quote" mode. The loop exits normally if it is in "not
		 * quote" mode and a delimiter or line end is seen.
		 */
		for (;;)
		{
			char c;

			/* Not in quote */
			for (;;)
			{
				end_ptr = cur_ptr;
				if (cur_ptr >= line_end_ptr)
					goto endfield;
				c = *cur_ptr++;
				/* unquoted field delimiter */
				if(strncmp(cstate->delim, cur_ptr - 1, cstate->delim_len) == 0)
				{
					cur_ptr += (cstate->delim_len - 1);
					found_delim = true;
					goto endfield;
				}
				/* start of quoted field (or part of field) */
				if (c == quotec)
				{
					saw_quote = true;
					break;
				}
				/* Add c to output string */
				*output_ptr++ = c;
			}

			/* In quote */
			for (;;)
			{
				end_ptr = cur_ptr;
				if (cur_ptr >= line_end_ptr)
				{
					/* In quote, use the lineno that start with quotation marks. */
					cstate->cur_lineno = cstate->cur_quote_lineno;
					ereport(ERROR,
					        (errcode(ERRCODE_BAD_COPY_FILE_FORMAT),
					         errmsg("unterminated CSV quoted field")));
				}

				c = *cur_ptr++;

				/* escape within a quoted field */
				if (c == escapec)
				{
					/*
					 * peek at the next char if available, and escape it if it
					 * is an escape char or a quote char
					 */
					if (cur_ptr < line_end_ptr)
					{
						char nextc = *cur_ptr;

						if (nextc == escapec || nextc == quotec)
						{
							*output_ptr++ = nextc;
							cur_ptr++;
							continue;
						}
					}
				}

				/*
				 * end of quoted field. Must do this test after testing for
				 * escape in case quote char and escape char are the same
				 * (which is the common case).
				 */
				if (c == quotec)
					break;

				/* Add c to output string */
				*output_ptr++ = c;
			}
		}
	endfield:

		/* Terminate attribute value in output area */
		*output_ptr++ = '\0';

		/* Check whether raw input matched null marker */
		input_len = end_ptr - start_ptr;
		if (!saw_quote && input_len == cstate->null_print_len &&
			strncmp(start_ptr, cstate->null_print, input_len) == 0)
			cstate->raw_fields[fieldno] = NULL;

		fieldno++;

#ifdef _PG_ORCL_
		if (fieldno == rid_idx)
		{
			Assert(!cstate->file_has_rowids && cstate->rid_glob_seqId != InvalidOid);
			Assert(IS_PGXC_COORDINATOR);

			insert_rowid_attribute_text(cstate, cur_ptr - cstate->line_buf.data, 0);
			rid_idx = -1;
		}
#endif
		/* Done if we hit EOL instead of a delim */
		if (!found_delim)
			break;
	}

	/* Clean up state of attribute_buf */
	output_ptr--;
	Assert(*output_ptr == '\0');
	cstate->attribute_buf.len = (output_ptr - cstate->attribute_buf.data);

	return fieldno;
}

/*
 * Read a binary attribute
 */
static Datum
CopyReadBinaryAttribute(CopyState cstate,
						int column_no, FmgrInfo *flinfo,
						Oid typioparam, int32 typmod,
						bool *isnull)
{
	int32 fld_size;
	int32 nSize;
	Datum result;

	if (!CopyGetInt32(cstate, &fld_size))
		ereport(ERROR,
				(errcode(ERRCODE_BAD_COPY_FILE_FORMAT),
				 errmsg("unexpected EOF in COPY data")));

#ifdef PGXC
	if (IS_PGXC_COORDINATOR)
	{
		/* Add field size to the data row, unless it is invalid. */
		if (fld_size >= -1 || ((ORA_MODE || enable_lightweight_ora_syntax) &&
		                       fld_size == 0)) /* -1 is valid; it means NULL value */
		{
			nSize = pg_hton32(fld_size);
			appendBinaryStringInfo(&cstate->line_buf,
								   (char *)&nSize, sizeof(int32));
		}
	}
#endif

	/* In opentenbase_ora mode, treat '' as null */
	if (fld_size == -1 || (ORA_MODE && !disable_empty_to_null && fld_size == 0))
	{
		*isnull = true;
		return ReceiveFunctionCall(flinfo, NULL, typioparam, typmod);
	}
	if (fld_size < 0)
		ereport(ERROR,
				(errcode(ERRCODE_BAD_COPY_FILE_FORMAT),
				 errmsg("invalid field size")));

	/* reset attribute_buf to empty, and load raw data in it */
	resetStringInfo(&cstate->attribute_buf);

	enlargeStringInfo(&cstate->attribute_buf, fld_size);
	if (CopyGetData(cstate, cstate->attribute_buf.data,
					fld_size, fld_size) != fld_size)
		ereport(ERROR,
				(errcode(ERRCODE_BAD_COPY_FILE_FORMAT),
				 errmsg("unexpected EOF in COPY data")));

	cstate->attribute_buf.len = fld_size;
	cstate->attribute_buf.data[fld_size] = '\0';
#ifdef PGXC
	if (IS_PGXC_COORDINATOR)
	{
		/* add the binary attribute value to the data row */
		appendBinaryStringInfo(&cstate->line_buf, cstate->attribute_buf.data, fld_size);
	}
#endif

	/* Call the column type's binary input converter */
	result = ReceiveFunctionCall(flinfo, &cstate->attribute_buf,
								 typioparam, typmod);

	/* Trouble if it didn't eat the whole buffer */
	if (cstate->attribute_buf.cursor != cstate->attribute_buf.len)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_BINARY_REPRESENTATION),
				 errmsg("incorrect binary data format")));

	*isnull = false;
	return result;
}

static char *
StrnTrimTo(CopyState cstate, char *dst, uint32 *dstlen, char *src, uint32 *enclen, uint32 srclen, ColAlign ca)
{
	const char *end = NULL;
	char       *ostr = NULL;
	char       *ptr = NULL;
	int         idx = 0;

	if (src == NULL || srclen == 0)
        return NULL;

	ptr = src;
	*enclen = 0;
	for (idx = 0; idx < srclen; idx++)
	{
		int clen = pg_encoding_mblen(cstate->file_encoding, ptr);
		*enclen += clen;
		ptr += clen;
	}

	end = src + *enclen - 1;

	if (ca == ALIGN_LEFT)
	{
		do
		{
			if (isspace((int) *end))
				end--;
			else
				break;
		} while (end >= src);
	}
	else if (ca == ALIGN_RIGHT)
	{
		do
		{
			if (*src == '0')
				src++;
			else
				break;
		} while (end >= src);
	}

	if (end < src)
		return NULL;
	
    *dstlen = end - src + 1;

	ostr = palloc(*dstlen + 1);
	memcpy(ostr, src, *dstlen);
	ostr[*dstlen] = '\0';

	if (cstate->need_transcoding)
		ptr = pg_any_to_server(ostr, *dstlen, cstate->file_encoding, true, cstate->compatible_illegal_chars);
	else
		ptr = src;
	
    *dstlen = strlen(ptr);
	memcpy(dst, ptr, *dstlen);
	pfree(ostr);
    return dst;
}

static int
CopyReadAttributesFixedWith(CopyState cstate)
{
	char     	*curPtr = NULL;
	char     	*outPtr = NULL;
	ListCell 	*cur = NULL;
	size_t    	curPos = 0;
	int       	fieldno = 0;
	int		  	ridIdx = -1;
	int		  	extra_size = 0;
	int			ridPos = -1;

	Assert(cstate != NULL);
    Assert(cstate->formatter != NULL);

	/*
     * We need a special case for zero-column tables: check that the input
     * line is empty, and return.
     */
	if (cstate->max_fields <= 0)
	{
		if (cstate->line_buf.len != 0)
			ereport(ERROR,
			        (errcode(ERRCODE_BAD_COPY_FILE_FORMAT),
			         errmsg("extra data after last expected column")));
		return 0;
	}

	resetStringInfo(&cstate->attribute_buf);

	if (!cstate->file_has_rowids && cstate->rid_glob_seqId != InvalidOid && IS_PGXC_COORDINATOR)
	{
		ridPos = 0;
		extra_size += ROWID_FIXLEN;
		if (cstate->file_has_oids)
		{
			extra_size += OID_FIXLEN;
			ridIdx = 1;
		}
		else
			ridIdx = 0;
	}

	/*
     * The de-escaped attributes will certainly not be longer than the input
     * data line, so we can just force attribute_buf to be large enough and
     * then transfer data without any checks for enough space.	We need to do
     * it this way because enlarging attribute_buf mid-stream would invalidate
     * pointers already stored into cstate->raw_fields[].
     */
    if (cstate->attribute_buf.maxlen <= cstate->line_buf.len + extra_size)
        enlargeStringInfo(&cstate->attribute_buf, cstate->line_buf.len + cstate->formatter_size * 2);
    outPtr = cstate->attribute_buf.data;

	foreach (cur, cstate->formatter)
	{
		CopyAttrFixed *caf = lfirst_node(CopyAttrFixed, cur);
		uint32         inputSize = 0;
		uint32         encodeSize = 0;

		/* Make sure there is enough space for the next value */
		if (fieldno >= cstate->max_fields)
		{
			cstate->max_fields *= 2;
			cstate->raw_fields =
				repalloc(cstate->raw_fields, cstate->max_fields * sizeof(char *));
		}

		if (fieldno == ridIdx)
			ridPos = curPos;
		
		curPtr = cstate->line_buf.len > curPos ? cstate->line_buf.data + curPos : NULL;
		if (curPtr != NULL)
		{
			StrnTrimTo(cstate,
			           outPtr,
			           &inputSize,
			           curPtr,
			           &encodeSize,
			           Min(caf->fix_len, cstate->line_buf.len - curPos),
			           caf->align);
			if (inputSize == cstate->null_print_len &&
			    strncmp(outPtr, cstate->null_print, inputSize) == 0)
			{
				cstate->raw_fields[fieldno++] = NULL;
			}
			else
			{
				cstate->raw_fields[fieldno++] = outPtr;
			}
			outPtr += inputSize;
		}
		curPos += encodeSize;
        *outPtr++ = '\0';
	}

    /* Clean up state of attribute_buf */
    outPtr--;
    Assert(*outPtr == '\0');
    cstate->attribute_buf.len = (outPtr - cstate->attribute_buf.data);

	/* Get the text line buf contain rowid into cstate->text_line_buf_rid */
	if (ridPos >= 0)
	{
		resetStringInfo(&cstate->text_line_buf_rid);
		insert_rowid_attribute_text(cstate, ridPos, ROWID_FIXLEN);
	}

    return fieldno;
}

/*
 * Send text representation of one attribute, with conversion and escaping
 */
#define DUMPSOFAR()                                   \
	do                                                \
	{                                                 \
		if (ptr > start)                              \
			CopySendData(cstate, start, ptr - start); \
	} while (0)

static void
CopyAttributeOutText(CopyState cstate, char *string)
{
	char *ptr;
	char *start;
	char c;
	char escapec = cstate->escape[0];

	if (cstate->need_transcoding)
		ptr = pg_server_to_any(string, strlen(string), cstate->file_encoding);
	else
		ptr = string;

	if (cstate->escape_off)
	{
		CopySendData(cstate, ptr, strlen(ptr));
		return;
	}

	/*
	 * We have to grovel through the string searching for control characters
	 * and instances of the delimiter character.  In most cases, though, these
	 * are infrequent.  To avoid overhead from calling CopySendData once per
	 * character, we dump out all characters between escaped characters in a
	 * single call.  The loop invariant is that the data from "start" to "ptr"
	 * can be sent literally, but hasn't yet been.
	 *
	 * We can skip pg_encoding_mblen() overhead when encoding is safe, because
	 * in valid backend encodings, extra bytes of a multibyte character never
	 * look like ASCII.  This loop is sufficiently performance-critical that
	 * it's worth making two copies of it to get the IS_HIGHBIT_SET() test out
	 * of the normal safe-encoding path.
	 */
	if (cstate->encoding_embeds_ascii)
	{
		start = ptr;
		while ((c = *ptr) != '\0')
		{
			if ((unsigned char)c < (unsigned char)0x20)
			{
				/*
				 * \r and \n must be escaped, the others are traditional. We
				 * prefer to dump these using the C-like notation, rather than
				 * a backslash and the literal character, because it makes the
				 * dump file a bit more proof against Microsoftish data
				 * mangling.
				 */
				switch (c)
				{
				case '\b':
					c = 'b';
					break;
				case '\f':
					c = 'f';
					break;
				case '\n':
					c = 'n';
					break;
				case '\r':
					c = 'r';
					break;
				case '\t':
					c = 't';
					break;
				case '\v':
					c = 'v';
					break;
				default:
					/* If it's the delimiter, must backslash it */
					if (strncmp(ptr, cstate->delim, cstate->delim_len) == 0)
					{
						break;
					}
						/* All ASCII control chars are length 1 */
						ptr++;
						continue;    /* fall to end of loop */
				}
				/* if we get here, we need to convert the control char */
				DUMPSOFAR();
				CopySendChar(cstate, escapec);
				CopySendChar(cstate, c);
				start = ++ptr; /* do not include char in next run */
			}
			else if (c == escapec || strncmp(ptr, cstate->delim, cstate->delim_len) == 0)
			{
				DUMPSOFAR();
				CopySendChar(cstate, escapec);
				start = ptr++; /* we include char in next run */
			}
			else if (IS_HIGHBIT_SET(c))
				ptr += pg_encoding_mblen(cstate->file_encoding, ptr);
			else
				ptr++;
		}
	}
	else
	{
		start = ptr;
		while ((c = *ptr) != '\0')
		{
			if ((unsigned char)c < (unsigned char)0x20)
			{
				/*
				 * \r and \n must be escaped, the others are traditional. We
				 * prefer to dump these using the C-like notation, rather than
				 * a backslash and the literal character, because it makes the
				 * dump file a bit more proof against Microsoftish data
				 * mangling.
				 */
				switch (c)
				{
				case '\b':
					c = 'b';
					break;
				case '\f':
					c = 'f';
					break;
				case '\n':
					c = 'n';
					break;
				case '\r':
					c = 'r';
					break;
				case '\t':
					c = 't';
					break;
				case '\v':
					c = 'v';
					break;
				default:
					/* If it's the delimiter, must backslash it */
					if (strncmp(ptr, cstate->delim, cstate->delim_len) == 0)
					{
						break;
					}
						/* All ASCII control chars are length 1 */
						ptr++;
						continue;    /* fall to end of loop */
				}
				/* if we get here, we need to convert the control char */
				DUMPSOFAR();
				CopySendChar(cstate, escapec);
				CopySendChar(cstate, c);
				start = ++ptr; /* do not include char in next run */
			}
			else if (c == escapec || strncmp(ptr, cstate->delim, cstate->delim_len) == 0)
			{
				DUMPSOFAR();
				CopySendChar(cstate, escapec);
				start = ptr++; /* we include char in next run */
			}
			else
				ptr++;
		}
	}

	DUMPSOFAR();
}

/*
 * Send text representation of one attribute, with conversion and
 * CSV-style escaping
 */
static void
CopyAttributeOutCSV(CopyState cstate, char *string,
					bool use_quote, bool single_attr)
{
	char *ptr;
	char *start;
	char c;
	char quotec = cstate->quote[0];
	char escapec = cstate->escape[0];

	/* force quoting if it matches null_print (before conversion!) */
	if (!use_quote && strcmp(string, cstate->null_print) == 0)
		use_quote = true;

	if (cstate->need_transcoding)
		ptr = pg_server_to_any(string, strlen(string), cstate->file_encoding);
	else
		ptr = string;

	/*
	 * Make a preliminary pass to discover if it needs quoting
	 */
	if (!use_quote)
	{
		/*
		 * Because '\.' can be a data value, quote it if it appears alone on a
		 * line so it is not interpreted as the end-of-data marker.
		 */
		if (single_attr && strcmp(ptr, "\\.") == 0)
			use_quote = true;
		else
		{
			char *tptr = ptr;

			while ((c = *tptr) != '\0')
			{
				if ((strncmp(tptr, cstate->delim, cstate->delim_len) == 0) || c == quotec || c == '\n' || c == '\r')
				{
					use_quote = true;
					break;
				}
				if (IS_HIGHBIT_SET(c) && cstate->encoding_embeds_ascii)
					tptr += pg_encoding_mblen(cstate->file_encoding, tptr);
				else
					tptr++;
			}
		}
	}

	if (use_quote)
	{
		CopySendChar(cstate, quotec);

		/*
		 * We adopt the same optimization strategy as in CopyAttributeOutText
		 */
		start = ptr;
		while ((c = *ptr) != '\0')
		{
			if (c == quotec || c == escapec)
			{
				DUMPSOFAR();
				CopySendChar(cstate, escapec);
				start = ptr; /* we include char in next run */
			}
			if (IS_HIGHBIT_SET(c) && cstate->encoding_embeds_ascii)
				ptr += pg_encoding_mblen(cstate->file_encoding, ptr);
			else
				ptr++;
		}
		DUMPSOFAR();

		CopySendChar(cstate, quotec);
	}
	else
	{
		/* If it doesn't need quoting, we can just dump it as-is */
		CopySendString(cstate, ptr);
	}
}

/*
 * CopyGetAttnums - build an integer list of attnums to be copied
 *
 * The input attnamelist is either the user-specified column list,
 * or NIL if there was none (in which case we want all the non-dropped
 * columns).
 *
 * rel can be NULL ... it's only used for error reports.
 */
static List *
CopyGetAttnums(TupleDesc tupDesc, Relation rel, List *attnamelist)
{
	List *attnums = NIL;

	if (attnamelist == NIL)
	{
		/* Generate default column list */
		int			attr_count = tupDesc->natts;
		int			i;

		for (i = 0; i < attr_count; i++)
		{
			if (TupleDescAttr(tupDesc, i)->attisdropped)
				continue;
			attnums = lappend_int(attnums, i + 1);
		}
	}
	else
	{
		/* Validate the user-supplied list and extract attnums */
		ListCell *l;

		foreach (l, attnamelist)
		{
			char *name = strVal(lfirst(l));
			int attnum;
			int i;

			/* Lookup column name */
			attnum = InvalidAttrNumber;
			for (i = 0; i < tupDesc->natts; i++)
			{
				Form_pg_attribute att = TupleDescAttr(tupDesc, i);

				if (att->attisdropped)
					continue;
				if (namestrcmp(&(att->attname), name) == 0)
				{
					attnum = att->attnum;
					break;
				}
			}
			if (attnum == InvalidAttrNumber)
			{
				if (rel != NULL)
					ereport(ERROR,
							(errcode(ERRCODE_UNDEFINED_COLUMN),
							 errmsg("column \"%s\" of relation \"%s\" does not exist",
									name, RelationGetRelationName(rel))));
				else
					ereport(ERROR,
							(errcode(ERRCODE_UNDEFINED_COLUMN),
							 errmsg("column \"%s\" does not exist",
									name)));
			}
			/* Check for duplicates */
			if (list_member_int(attnums, attnum))
				ereport(ERROR,
						(errcode(ERRCODE_DUPLICATE_COLUMN),
						 errmsg("column \"%s\" specified more than once",
								name)));
			attnums = lappend_int(attnums, attnum);
		}
	}

	return attnums;
}

/*
 * copy_dest_startup --- executor startup
 */
static void
copy_dest_startup(DestReceiver *self, int operation, TupleDesc typeinfo)
{
	/* no-op */
}

/*
 * copy_dest_receive --- receive one tuple
 */
static bool
copy_dest_receive(TupleTableSlot *slot, DestReceiver *self)
{
	DR_copy *myState = (DR_copy *)self;
	CopyState cstate = myState->cstate;

	/* Make sure the tuple is fully deconstructed */
	slot_getallattrs(slot);

	/* And send the data */
	CopyOneRowTo(cstate, InvalidOid, slot->tts_values, slot->tts_isnull
							);
	myState->processed++;

	return true;
}

/*
 * copy_dest_shutdown --- executor end
 */
static void
copy_dest_shutdown(DestReceiver *self)
{
	/* no-op */
}

/*
 * copy_dest_destroy --- release DestReceiver object
 */
static void
copy_dest_destroy(DestReceiver *self)
{
	pfree(self);
}

/*
 * CreateCopyDestReceiver -- create a suitable DestReceiver object
 */
DestReceiver *
CreateCopyDestReceiver(void)
{
	DR_copy *self = (DR_copy *)palloc(sizeof(DR_copy));

	self->pub.receiveSlot = copy_dest_receive;
	self->pub.rStartup = copy_dest_startup;
	self->pub.rShutdown = copy_dest_shutdown;
	self->pub.rDestroy = copy_dest_destroy;
	self->pub.mydest = DestCopyOut;

	self->cstate = NULL; /* will be set later */
	self->processed = 0;

	return (DestReceiver *)self;
}

#ifdef PGXC
static RemoteCopyOptions *
GetRemoteCopyOptions(CopyState cstate)
{
	RemoteCopyOptions *res = makeRemoteCopyOptions();
	Assert(cstate);

	/* Then fill in structure */
	res->rco_binary = cstate->binary;
	res->rco_oids = cstate->oids;
#ifdef _PG_ORCL_
	res->rco_rowids = cstate->rowids;
#endif
	res->rco_csv_mode = cstate->csv_mode;
	res->rco_fixed_mode = cstate->fixed_mode;
	res->rco_eol_type = cstate->eol_type;
	if (cstate->delim)
		res->rco_delim = pstrdup(cstate->delim);
	if (cstate->null_print)
		res->rco_null_print = pstrdup(cstate->null_print);
	if (cstate->quote)
		res->rco_quote = pstrdup(cstate->quote);
	if (cstate->escape)
		res->rco_escape = pstrdup(cstate->escape);
	if (cstate->eol)
		res->rco_eol = pstrdup(cstate->eol);
	if (cstate->force_quote)
		res->rco_force_quote = list_copy(cstate->force_quote);
	if (cstate->force_quote_all)
		res->rco_force_quote_all = cstate->force_quote_all;
	if (cstate->force_notnull)
		res->rco_force_notnull = list_copy(cstate->force_notnull);
	if (cstate->force_null)
		res->rco_force_null = list_copy(cstate->force_null);
	if (cstate->file_encoding >= 0)
		res->file_encoding = cstate->file_encoding;
#ifdef __OPENTENBASE__
	res->rco_insert_into = cstate->insert_into;
#endif

	res->fill_missing = cstate->fill_missing;
	res->ignore_extra_data = cstate->ignore_extra_data;
	res->compatible_illegal_chars = cstate->compatible_illegal_chars;

	if (cstate->compatible_illegal_chars)
	{
		res->illegal_conv_chars = pstrdup(cstate->illegal_conv_chars);
	}
	if (cstate->fixed_mode)
	{
		res->formatter = list_copy(cstate->formatter);
		res->formatter_with_cut = cstate->formatter_with_cut;
	}

	return res;
}
#endif

static CopyAttrFixed *
parseAttFormatter(CopyState cstate, Form_pg_attribute attr, int *fixedCnt)
{
	ListCell      *lc;
	CopyAttrFixed *col;
	foreach (lc, cstate->formatter)
	{
		col = lfirst_node(CopyAttrFixed, lc);
		if (namestrcmp(&(attr->attname), col->attname) == 0)
		{
			*fixedCnt = *fixedCnt + 1;
			break;
		}
	}
	/* Field length is not specified, using the defined length. */
	if (lc == NULL)
	{
		int32 fixed_len;
		if (attr->atttypid == NUMERICOID)
		{
			fixed_len = ((attr->atttypmod - VARHDRSZ) >> 16) & 0xffff;
		}
		else
			fixed_len = attr->atttypmod - VARHDRSZ;
		if (fixed_len <= 0)
		{
			ereport(
				ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
			     errmsg("Fixed length is not specified for column \"%s\"", attr->attname.data)));
		}
		col = makeNode(CopyAttrFixed);
		col->attname = pstrdup(attr->attname.data);
		col->fix_len = fixed_len;
	}

	switch (attr->atttypid)
	{
		case BYTEAOID:
		case CHAROID:
		case NAMEOID:
		case TEXTOID:
		case BPCHAROID:
		case VARCHAROID:
		case NVARCHAR2OID:
		case CSTRINGOID:
		case VARCHAR2OID:
			col->align = ALIGN_LEFT;
			break;
		default:
			col->align = ALIGN_RIGHT;
			break;
	}
	col->attnum = attr->attnum;
	return col;
}

static void
transformFormatter(CopyState cstate, TupleDesc tupDesc, Relation rel, List *attnamelist)
{
	List *attnums = NIL;
	List *orderedFormatter = NIL;
	ListCell      *lc;
	CopyAttrFixed *col;
	CopyAttrFixed *oid_col = NULL;
	int			  oid_col_pos = 0;
	int			  rowid_col_pos = 0;
	int 		  idx = 0;
	CopyAttrFixed *rowid_col = NULL;
	int fixedCnt = 0;
	size_t fixSize = 0;

	foreach (lc, cstate->formatter)
	{
		idx++;
		col = lfirst_node(CopyAttrFixed, lc);
		if (pg_strcasecmp("OID", col->attname) == 0)
		{
			oid_col = col;
			oid_col_pos = idx;
		}
		else if (pg_strcasecmp("ROWID", col->attname) == 0)
		{
			rowid_col = col;
			rowid_col_pos = idx;
		}
	}

	if (cstate->oids)
	{
		col = makeNode(CopyAttrFixed);
		if (oid_col == NULL)
		{
			col->fix_len = OID_FIXLEN;
			col->align = ALIGN_LEFT;
			col->with_cut = false;
		}
		else
		{
			col->fix_len = oid_col->fix_len;
			col->align = oid_col->align;
			col->with_cut = oid_col->with_cut;
			cstate->formatter = list_delete_nth(cstate->formatter, oid_col_pos);
		}

		col->attname = "OID";
		col->attnum = -1;
		orderedFormatter = lappend(orderedFormatter, col);
	}

	if (cstate->rowids)
	{
		col = makeNode(CopyAttrFixed);
		if (rowid_col == NULL)
		{
			col->fix_len = ROWID_FIXLEN;
			col->align = ALIGN_LEFT;
			col->with_cut = false;
		}
		else
		{
			col->fix_len = rowid_col->fix_len;
			col->align = rowid_col->align;
			col->with_cut = rowid_col->with_cut;
			cstate->formatter = list_delete_nth(cstate->formatter, rowid_col_pos);
		}

		col->attname = "ROWID";
		col->attnum = -2;
		orderedFormatter = lappend(orderedFormatter, col);
	}

	if(cstate->formatter == NIL)
	{
		cstate->formatter = orderedFormatter;
		return;
	}

	if (attnamelist == NIL)
	{
		/* Generate default column list */
		int			attr_count = tupDesc->natts;
		int			i;

		for (i = 0; i < attr_count; i++)
		{
			Form_pg_attribute attr = TupleDescAttr(tupDesc, i);
			if (attr->attisdropped)
				continue;
			col = parseAttFormatter(cstate, attr, &fixedCnt);
			fixSize += col->fix_len;
			orderedFormatter = lappend(orderedFormatter, col);
		}
	}
	else
	{
		/* Validate the user-supplied list and extract attnums */
		ListCell   *l;

		foreach(l, attnamelist)
		{
			char	   *name = strVal(lfirst(l));
			int			attnum;
			int			i;

			/* Lookup column name */
			attnum = InvalidAttrNumber;
			for (i = 0; i < tupDesc->natts; i++)
			{
				if (tupDesc->attrs[i].attisdropped)
					continue;
				if (namestrcmp(&(tupDesc->attrs[i].attname), name) == 0)
				{
					attnum = tupDesc->attrs[i].attnum;
					break;
				}
			}
			if (attnum == InvalidAttrNumber)
			{
				if (rel != NULL)
					ereport(ERROR,
							(errcode(ERRCODE_UNDEFINED_COLUMN),
							 errmsg("column \"%s\" of relation \"%s\" does not exist",
									name, RelationGetRelationName(rel))));
				else
					ereport(ERROR,
							(errcode(ERRCODE_UNDEFINED_COLUMN),
							 errmsg("column \"%s\" does not exist",
									name)));
			}
			/* Check for duplicates */
			if (list_member_int(attnums, attnum))
				ereport(ERROR,
						(errcode(ERRCODE_DUPLICATE_COLUMN),
						 errmsg("column \"%s\" specified more than once",
								name)));
			col = parseAttFormatter(cstate, TupleDescAttr(tupDesc, i), &fixedCnt);
			fixSize += col->fix_len;
			orderedFormatter = lappend(orderedFormatter, col);
		}
	}
	/*
	 * Some defined fomatter do not have corresponding attrs.
	 * We Add extra formatters in order to the orderedFormatter.
	 */
	if (fixedCnt < list_length(cstate->formatter))
	{
		if (cstate->ignore_extra_data)
		{
			foreach (lc, cstate->formatter)
			{
				col = lfirst_node(CopyAttrFixed, lc);
				if (col->attnum == InvalidAttrNumber)
				{
					/* By default, the extra fields are aligned using VARCHAR alignment. */
					col->align = ALIGN_LEFT;
					fixSize += col->fix_len;
					orderedFormatter = lappend(orderedFormatter, col);
				}
			}
		}else {
			ereport(ERROR,
			        (errcode(ERRCODE_BAD_COPY_FILE_FORMAT),
			         errmsg("extra formatter after last expected column")));
		}
	}
	if (fixSize > MaxAllocSize)
		ereport(ERROR,
		        (errcode(ERRCODE_SYNTAX_ERROR),
		         errmsg("The fixed-length row data cannot exceed 1GB at maximum.")));

	cstate->formatter_size = fixSize;
	cstate->formatter = orderedFormatter;
}
