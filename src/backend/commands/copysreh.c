/*-------------------------------------------------------------------------
 *
 * copysreh.c
 *		Provides routines for single row error handling for COPY and external tables.
 *
 *
 * IDENTIFICATION
 *	  src/backend/commands/copysreh.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include <unistd.h>
#include <sys/stat.h>

#include "access/transam.h"
#include "access/xact.h"
#include "access/htup_details.h"
#include "catalog/namespace.h"
#include "catalog/pg_attribute.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_type.h"
#include "commands/dbcommands.h"
#include "commands/tablecmds.h"
#include "commands/copysreh.h"
#include "funcapi.h"
#include "libpq/libpq.h"
#include "libpq/pqformat.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "nodes/nodes.h"
#include "storage/fd.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/bytea.h"
#include "utils/timestamp.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"
#include "utils/snapmgr.h"
#include "utils/varlena.h"
#include "pgxc/planner.h"
#include "pgxc/execRemote.h"

typedef enum RejectLimitCode
{
    REJECT_NONE = 0,
    REJECT_FIRST_BAD_LIMIT,
    REJECT_LIMIT_REACHED
} RejectLimitCode;

int copy_initial_bad_row_limit    = 1000;
int copy_reject_percent_threshold = 300;        /* SREH reject % kicks off only */

static RejectLimitCode GetRejectLimitCode(Srehandler *srehandler);

static HeapTuple FormErrorTuple(Srehandler *sreh);

static void ErrorLogWrite(Srehandler *sreh);

static HeapTuple ErrorLogRead(FILE *fp, pg_crc32 *crc);

static bool ErrorLogPrefixDelete(Oid databaseId, Oid namespaceId, bool persistent);

static bool TruncateRemoteErrorlog(text *relname, bool persistent, bool on_coordinator);

/*
 * ErrorLogFileName - get error log file path.
 *
 * If the current error log is set to be persistent.
 * The log file will be different from normal one.
 */
void
ErrorLogFileName(Oid dbid, Oid relid, bool persistent, char *fname /* out */)
{
	Assert(fname);
	Assert(OidIsValid(relid) && OidIsValid(dbid));

	if (persistent)
	{
			char *relname  = get_rel_name(relid);
			Oid  namespace = get_rel_namespace(relid);

			if (OidIsValid(namespace))
					ErrorLogPersistentFileName(fname, dbid, namespace, relname);
			else
			elog(ERROR, "relid %u does not exist for db %u", relid, dbid);
	}
	else
			ErrorLogNormalFileName(fname, dbid, relid);
}

/*
 * Returns the fixed schema for error log tuple.
 */
TupleDesc
GetErrorTupleDesc(void)
{
	static TupleDesc tupdesc = NULL;

	/*
	 * Create the tuple descriptor on first call, and reuse on subsequent
	 * calls. It should never be scribbled on.
	 */
	if (tupdesc == NULL)
	{
			TupleDesc     tmp;
			MemoryContext oldcontext = MemoryContextSwitchTo(CacheMemoryContext);

			tmp = CreateTemplateTupleDesc(NUM_ERRORTABLE_ATTR, false);
			TupleDescInitEntry(tmp, 1, "cmdtime", TIMESTAMPTZOID, -1, 0);
			TupleDescInitEntry(tmp, 2, "relname", TEXTOID, -1, 0);
			TupleDescInitEntry(tmp, 3, "filename", TEXTOID, -1, 0);
			TupleDescInitEntry(tmp, 4, "linenum", INT4OID, -1, 0);
			TupleDescInitEntry(tmp, 5, "bytenum", INT8OID, -1, 0);
			TupleDescInitEntry(tmp, 6, "errmsg", TEXTOID, -1, 0);
			TupleDescInitEntry(tmp, 7, "rawdata", TEXTOID, -1, 0);
			TupleDescInitEntry(tmp, 8, "rawbyte", BYTEAOID, -1, 0);
			TupleDescInitEntry(tmp, 9, "nodename", TEXTOID, -1, 0);

			MemoryContextSwitchTo(oldcontext);

			tupdesc = tmp;
	}

	return tupdesc;
}

static HeapTuple
FormErrorTuple(Srehandler *sreh)
{
	bool          nulls[NUM_ERRORTABLE_ATTR];
	Datum         values[NUM_ERRORTABLE_ATTR];
	MemoryContext oldcontext;

	oldcontext = MemoryContextSwitchTo(sreh->badrowcontext);

	/* Initialize all values for row to NULL */
	MemSet(values, 0, NUM_ERRORTABLE_ATTR * sizeof(Datum));
	MemSet(nulls, true, NUM_ERRORTABLE_ATTR * sizeof(bool));

	/* command start time */
	values[errtable_cmdtime - 1] = TimestampTzGetDatum(GetCurrentStatementStartTimestamp());
	nulls[errtable_cmdtime - 1]  = false;

	/* line number */
	if (sreh->linenumber > 0)
	{
		values[errtable_linenum - 1] = Int64GetDatum(sreh->linenumber);
		nulls[errtable_linenum - 1]  = false;
	}

	if (sreh->bytenum >= 0)
	{
		values[errtable_bytenum - 1] = Int64GetDatum(sreh->bytenum);
		nulls[errtable_bytenum - 1]  = false;
	}

	if (sreh->is_server_enc)
	{
		/* raw data */
		values[errtable_rawdata - 1] = CStringGetTextDatum(sreh->rawdata->data);
		nulls[errtable_rawdata - 1]  = false;
	}
	else
	{
		/* raw bytes */
		values[errtable_rawbyte - 1] = DirectFunctionCall1(bytearecv, PointerGetDatum(sreh->rawdata));
		nulls[errtable_rawbyte - 1]  = false;
	}

	/* nodename */
	values[errtable_nodename - 1] = CStringGetTextDatum((char *) sreh->nodename);
	nulls[errtable_nodename - 1]  = false;

	/* file name */
	values[errtable_filename - 1] = CStringGetTextDatum(sreh->filename);
	nulls[errtable_filename - 1]  = false;

	/* relation name */
	values[errtable_relname - 1] = CStringGetTextDatum(sreh->relname);
	nulls[errtable_relname - 1]  = false;

	/* error message */
	values[errtable_errmsg - 1] = CStringGetTextDatum(sreh->errmsg);
	nulls[errtable_errmsg - 1]  = false;

	MemoryContextSwitchTo(oldcontext);

	/*
	 * And now we can form the input tuple.
	 */
	return heap_form_tuple(GetErrorTupleDesc(), values, nulls);
}

/*
 * Write into the error log file.  This opens the file every time,
 * so that we can keep it simple to deal with concurrent write.
 */
static void
ErrorLogWrite(Srehandler *sreh)
{
	HeapTuple tuple;
	char      filename[MAXPGPATH];
	FILE      *fp;
	pg_crc32  crc;
	int       ret;

	Assert(OidIsValid(sreh->relid));
	ErrorLogFileName(MyDatabaseId, sreh->relid,
					 sreh->error_log_persistent, filename/* out */);
	tuple = FormErrorTuple(sreh);

	INIT_CRC32C(crc);
	COMP_CRC32C(crc, tuple->t_data, tuple->t_len);
	FIN_CRC32C(crc);

	LWLockAcquire(ErrorLogLock, LW_EXCLUSIVE);
	fp = AllocateFile(filename, "a");

	if (!fp && (errno == EMFILE || errno == ENFILE))
		ereport(ERROR,
			(errmsg("could not open \"%s\", too many open files: %m",
					filename)));

	if (!fp && errno == ENOENT)
	{
		char *errordir = ErrorLogDir;

		if (sreh->error_log_persistent)
				errordir = PersistentErrorLogDir;

		ret = mkdir(errordir, S_IRWXU);
		if (ret == 0)
				fp = AllocateFile(filename, "a");
		else
				ereport(ERROR,
						(errmsg("could not create directory for errorlog \"%s\": %m",
								errordir)));
	}
	if (!fp)
		ereport(ERROR, (errmsg("could not open \"%s\": %m", filename)));

	/*
	 * format: 0-4: length 5-8: crc 9-n: tuple data
	 */
	if (fwrite(&tuple->t_len, 1, sizeof(tuple->t_len), fp) != sizeof(tuple->t_len))
			elog(ERROR, "could not write tuple length: %m");
	if (fwrite(&crc, 1, sizeof(pg_crc32), fp) != sizeof(pg_crc32))
			elog(ERROR, "could not write checksum: %m");
	if (fwrite(tuple->t_data, 1, tuple->t_len, fp) != tuple->t_len)
			elog(ERROR, "could not write tuple data: %m");

	FreeFile(fp);
	LWLockRelease(ErrorLogLock);

	heap_freetuple(tuple);
}

/*
 * HandleSingleRowError
 *
 * The single row error handler. Get called when a data error happened
 * in SREH mode. Reponsible for making a decision of what to do at that time.
 *
 * Some of the main actions are:
 *  - Keep track of reject limit. if reached make sure notify caller.
 *  - If error logging used, call the error logger to log this error.
 *
 * TDX ON DATNODE, COPY ON COORDINATOR.
 */
void
HandleSingleRowError(Srehandler *sreh)
{
        
	/* increment total number of errors for this segment */
	sreh->rejectcount++;

	/*
	 * If not specified table or file, do nothing.
	 * Otherwise, record the error: log the error in the error log file.
	 */
	if (sreh->log_to_file)
	{
			ErrorLogWrite(sreh);
	}
}

/*
 * Return true if the first bad rows exceed the hard limit.  We assume the
 * input is not well configured or similar case.  Stop the work regardless of
 * SEGMENT REJECT LIMIT value before exhausting disk space. Setting the GUC
 * value to 0 indicates no hard limit.
 */
bool
ExceedSegmentRejectHardLimit(Srehandler *srehandler)
{
	/* Keep going if the hard limit is "unlimited" */
	if (copy_initial_bad_row_limit == 0)
			return false;

	/* Stop if all the first bad rows reach to the hard limit. */
	if (srehandler->processed == copy_initial_bad_row_limit &&
		srehandler->rejectcount >= copy_initial_bad_row_limit)
			return true;

	return false;
}

/* Identify the reject limit type */
static RejectLimitCode
GetRejectLimitCode(Srehandler *srehandler)
{
	RejectLimitCode code = REJECT_NONE;

	if (srehandler->rejectlimit == SREH_UNLIMITED)
			return REJECT_NONE;

	/* special case: check for the case that we are rejecting every single row */
	if (ExceedSegmentRejectHardLimit(srehandler))
			return REJECT_FIRST_BAD_LIMIT;

	/* now check if actual reject limit is reached */
	if (srehandler->is_limit_in_rows)
	{
		/* limit is in ROWS */
		if (srehandler->rejectcount >= srehandler->rejectlimit)
				code = REJECT_LIMIT_REACHED;
	}
	else
	{
		/* limit is in PERCENT */

		/* calculate the percent only if threshold is satisfied */
		if (srehandler->processed > copy_reject_percent_threshold)
		{
			if ((srehandler->rejectcount * 100) / srehandler->processed >= srehandler->rejectlimit)
					code = REJECT_LIMIT_REACHED;
		}
	}

	return code;
}

/*
 * Reports error if we reached segment reject limit.
 */
void
ErrorIfRejectLimitReached(Srehandler *srehandler)
{
	RejectLimitCode code;

	code = GetRejectLimitCode(srehandler);

	if (code == REJECT_NONE)
		return;

	switch (code)
	{
		case REJECT_FIRST_BAD_LIMIT:
			/* the special "first X rows are bad" case */
			ereport(ERROR,
					(errcode(ERRCODE_T_R_COPY_REJECT_LIMIT_REACHED),
							errmsg("all %d first rows on datanode %s were rejected",
								   copy_initial_bad_row_limit,
								   debug_message_log ? PGXCNodeName : ""),
							errdetail("Aborting operation regardless of REJECT LIMIT"
									  "value, last error was: (%s)",
									  srehandler->errmsg)));
			break;
		case REJECT_LIMIT_REACHED:
			/* the normal case */
			ereport(ERROR,
					(errcode(ERRCODE_T_R_COPY_REJECT_LIMIT_REACHED),
							errmsg("datanode %s reject limit reached(%d), aborting operation",
								   debug_message_log ? PGXCNodeName : "",
								   srehandler->rejectlimit),
							errdetail("Last error was: (%s)", srehandler->errmsg)));
			break;
		default:
			elog(ERROR, "unknown reject code %d", code);
	}
}

/*
 * IsRejectLimitValid
 *
 * verify that the reject limit specified by the user is within the
 * allowed values for ROWS or PERCENT.
 */
void
VerifyRejectLimit(char rejectlimittype, int rejectlimit)
{
	if (rejectlimittype == 'r')
	{
		/* ROWS */
		if (rejectlimit < 2)
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_TABLE_DEFINITION),
								errmsg("node reject limit in ROWS must be 2 or larger (got %d)",
									   rejectlimit)));
	}
	else
	{
		/* PERCENT */
		Assert(rejectlimittype == 'p');
		if (rejectlimit < 1 || rejectlimit > 100)
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_TABLE_DEFINITION),
								errmsg("node reject limit in PERCENT must be between 1 and 100 (got %d)",
									   rejectlimit)));
	}
}

static bool
ErrorLogPrefixDelete(Oid databaseId, Oid namespaceId, bool persistent)
{
	char          filename[MAXPGPATH];
	DIR           *dir;
	struct dirent *de;
	char          *dirpath;
	char          prefix[MAXPGPATH];
	int           len;

	if (persistent)
		dirpath = PersistentErrorLogDir;
	else
		dirpath = ErrorLogDir;

	if (OidIsValid(databaseId))
	{
		if (OidIsValid(namespaceId))
				snprintf(prefix, sizeof(prefix), "%u_%u_", databaseId, namespaceId);
		else
				snprintf(prefix, sizeof(prefix), "%u_", databaseId);
	}

	dir = AllocateDir(dirpath);

	/*
	 * If we cannot open the directory, most likely it does not exist. Do nothing.
	 */
	if (dir == NULL)
	{
		if (errno == ENOENT)
				return true;
		else
				return false;
	}

	while ((de = ReadDir(dir, dirpath)) != NULL)
	{
		if (strcmp(de->d_name, ".") == 0 ||
			strcmp(de->d_name, "..") == 0 ||
			strcmp(de->d_name, "/") == 0)
				continue;

		/*
		 * If database id is not given, delete all files.
		 * Or
		 * Filter by the prefix for persistent error log.
		 */
		if (!OidIsValid(databaseId) ||
			(strncmp(de->d_name, prefix, strlen(prefix)) == 0 && persistent))
		{
			len = snprintf(filename, MAXPGPATH, "%s/%s", dirpath, de->d_name);
			if (len >= (MAXPGPATH - 1))
			{
					ereport(WARNING,
							(errcode(ERRCODE_INTERNAL_ERROR),
									(errmsg("log filename truncation on \"%s\", unable to delete error log",
											de->d_name))));
					continue;
			}
			LWLockAcquire(ErrorLogLock, LW_EXCLUSIVE);
			unlink(filename);
			LWLockRelease(ErrorLogLock);
			continue;
		}

		/*
		 * Filter by the database id prefix.
		 */
		if (strncmp(de->d_name, prefix, strlen(prefix)) == 0)
		{
			int res;
			Oid dummyDbId,
				dummyOid;

			res = sscanf(de->d_name, "%u_%u", &dummyDbId, &dummyOid);
			Assert(dummyDbId == databaseId);

			/*
			 * Recursively delete the file.
			 */
			if (res == 2)
			{
					ErrorLogDelete(databaseId, dummyOid);
			}
		}
	}

	FreeDir(dir);
	return true;
}

/*
 * Delete the error log of the relation and return true if succeed or file does not exist.
 * If relationId is InvalidOid, scan the directory to look for
 * all the files prefixed with the databaseId, and delete them.
 */
bool
ErrorLogDelete(Oid databaseId, Oid relationId)
{
	char filename[MAXPGPATH];
	bool result = true;

	if (!OidIsValid(relationId))
			return ErrorLogPrefixDelete(databaseId, InvalidOid, false);

	LWLockAcquire(ErrorLogLock, LW_EXCLUSIVE);
	ErrorLogNormalFileName(filename, databaseId, relationId);
	if (unlink(filename) < 0 && errno != ENOENT)
	{
			/* Ignore if the file does not exist. */
			ereport(WARNING,
					(errcode_for_file_access(),
							errmsg("could not unlink file \"%s\": %m", filename)));
			result = false;
	}
	LWLockRelease(ErrorLogLock);

	return result;
}

/*
 * PersistentErrorLogDelete - Delete the persistent error log.
 *
 * Only the superuser has the permission to delete.
 */
bool
PersistentErrorLogDelete(Oid databaseId, Oid namespaceId, const char *fname)
{
	bool result = true;
	if (fname == NULL)
			return ErrorLogPrefixDelete(databaseId, namespaceId, true);

	LWLockAcquire(ErrorLogLock, LW_EXCLUSIVE);
	if (unlink(fname) < 0 && errno != ENOENT)
	{
			/* Ignore if the file does not exist. */
			ereport(WARNING,
					(errcode_for_file_access(),
							errmsg("could not unlink file \"%s\": %m", fname)));
			result = false;
	}
	LWLockRelease(ErrorLogLock);
	return result;
}

/* ----------
 * TruncateRemoteErrorlog
 *
 *  Execute tdx_truncate_error_log query remotely and save
 *  results in tuplestore.
 * ----------
 */
static bool
TruncateRemoteErrorlog(text *relname, bool persistent, bool on_coordinator)
{
#define QUERY_LEN 1024
	char             query[QUERY_LEN];
	EState           *estate;
	MemoryContext    oldcontext;
	RemoteQuery      *plan;
	RemoteQueryState *pstate;
	Var              *dummy;
	TupleTableSlot   *result    = NULL;
	bool             allResults = true;

	if (persistent)
		snprintf(query,
				 QUERY_LEN,
				 "SELECT tdx_truncate_persistent_error_log(%s)",
				 quote_literal_cstr(text_to_cstring(relname)));
	else
		snprintf(query,
				 QUERY_LEN,
				 "SELECT tdx_truncate_error_log(%s)",
				 quote_literal_cstr(text_to_cstring(relname)));

	plan = makeNode(RemoteQuery);
	plan->combine_type     = COMBINE_TYPE_SAME;
	plan->exec_nodes       = NULL;
	plan->exec_type        = on_coordinator ? EXEC_ON_COORDS : EXEC_ON_DATANODES;
	plan->sql_statement    = (char *) query;
	plan->force_autocommit = false;

	dummy = makeVar(1, 1, BOOLOID, 0, InvalidOid, 0);
	plan->scan.plan.targetlist = lappend(plan->scan.plan.targetlist,
										 makeTargetEntry((Expr *) dummy, 1, NULL, false));

	/* prepare to execute */
	estate     = CreateExecutorState();
	oldcontext = MemoryContextSwitchTo(estate->es_query_cxt);
	estate->es_snapshot = GetActiveSnapshot();
	pstate = ExecInitRemoteQuery(plan, estate, 0);
	MemoryContextSwitchTo(oldcontext);

	result = ExecRemoteQuery((PlanState *) pstate);

	while (result != NULL && !TupIsNull(result))
	{
		Datum datum  = (Datum) NULL;
		bool  isnull = false;

		datum  = slot_getattr(result, 1, &isnull);
		allResults &= (!isnull && DatumGetBool(datum));
		result = ExecRemoteQuery((PlanState *) pstate);
	}

	ExecEndRemoteQuery(pstate);
	/* Return true iif all segments return true. */
	return allResults;
}

bool
TruncateErrorLog(text *relname, bool persistent)
{
	char     *relname_str;
	RangeVar *relrv;
	Oid      relid;
	bool     result = true;

	/*
	 * Dispatch the work to other segments.
	 */
	if (IS_PGXC_LOCAL_COORDINATOR)
	{
		result &= TruncateRemoteErrorlog(relname, persistent, true);
		result &= TruncateRemoteErrorlog(relname, persistent, false);
	}

	relname_str = text_to_cstring(relname);
	if (strcmp(relname_str, "*.*") == 0)
	{
		/*
		 * Only superuser is allowed to delete log files across database.
		 */
		if (!superuser())
				ereport(ERROR,
						(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
								(errmsg("must be superuser to delete all error log files"))));

		result &= ErrorLogPrefixDelete(InvalidOid, InvalidOid, persistent);
	}
	else if (strcmp(relname_str, "*") == 0)
	{
		/*
		 * Database owner can delete error log files.
		 */
		if (!pg_database_ownercheck(MyDatabaseId, GetUserId()))
				aclcheck_error(ACLCHECK_NOT_OWNER, ACL_KIND_DATABASE,
							   get_database_name(MyDatabaseId));

		result &= ErrorLogPrefixDelete(MyDatabaseId, InvalidOid, persistent);
	}
	else
	{
		AclResult aclresult;

		relrv = makeRangeVarFromNameList(textToQualifiedNameList(relname));
		if (persistent)
		{
			char filename[MAXPGPATH];
			bool finderrorlog;

			/* Allow the table owner to truncate error log.
			 * But if the table is already dropped, allow the
			 * schema owner to truncate error log. */
			finderrorlog = RetrievePersistentErrorLogFromRangeVar(
					relrv, ACL_TRUNCATE, filename /* out */);

			/* We don't care if this fails or not. */
			if (finderrorlog)
					result &= PersistentErrorLogDelete(MyDatabaseId, InvalidOid, filename);
		}
		else
		{
			relid = RangeVarGetRelid(relrv, NoLock, true);

			/* Return false if the relation does not exist. */
			if (!OidIsValid(relid))
					PG_RETURN_BOOL(false);

			/*
			* Allow only the table owner to truncate error log.
			*/
			aclresult = pg_class_aclcheck(relid, GetUserId(), ACL_TRUNCATE);
			if (aclresult != ACLCHECK_OK)
					aclcheck_error(aclresult, ACL_KIND_CLASS, relrv->relname);

			/* We don't care if this fails or not. */
			result &= ErrorLogDelete(MyDatabaseId, relid);
		}
	}
	return result;
}

/*
 * Utility to retrieve the error log persistent file from RangeVar.
 */
bool
RetrievePersistentErrorLogFromRangeVar(RangeVar *relrv, AclMode mode, char *fname /*out*/)
{
	AclResult aclresult;
	bool      findfile = false;
	Oid       namespaceId;
	char      *schemaname;
	Oid       relid    = InvalidOid;

	relid = RangeVarGetRelid(relrv, NoLock, true);

	if (OidIsValid(relid))
	{
		/* Requires priv on relation to operate error log. */
		aclresult = pg_class_aclcheck(relid, GetUserId(), mode);
		if (aclresult != ACLCHECK_OK)
				aclcheck_error(aclresult, ACL_KIND_CLASS, relrv->relname);

		ErrorLogFileName(MyDatabaseId, relid, true, fname /* out */);
		return true;
	}

	if (relrv->schemaname)
	{
		/* use exact schema given */
		namespaceId = LookupExplicitNamespace(relrv->schemaname, true);
		if (OidIsValid(namespaceId))
		{
				schemaname = relrv->schemaname;
				findfile   = true;
				ErrorLogPersistentFileName(fname, MyDatabaseId, namespaceId, relrv->relname);
		}
	}
	else
	{
		ListCell           *cell;
		OverrideSearchPath *searchPath;
		char               filename[MAXPGPATH];

		searchPath = GetOverrideSearchPath(CurrentMemoryContext);
		foreach(cell, searchPath->schemas)
		{
			namespaceId = lfirst_oid(cell);
			ErrorLogPersistentFileName(filename, MyDatabaseId, namespaceId, relrv->relname);
			if (0 == access(filename, R_OK))
			{
				HeapTuple         tuple;
				Form_pg_namespace pg_namespace_tuple;

				tuple = SearchSysCache1(NAMESPACEOID, ObjectIdGetDatum(namespaceId));
				if (!HeapTupleIsValid(tuple))
				elog(ERROR, "cache lookup failed for namespace %u", namespaceId);

				pg_namespace_tuple = (Form_pg_namespace) GETSTRUCT(tuple);
				schemaname         = pstrdup(NameStr(pg_namespace_tuple->nspname));
				ReleaseSysCache(tuple);
				StrNCpy(fname, filename, MAXPGPATH);
				findfile = true;
				break;
			}
		}
	}
	if (findfile)
	{
		/* Requires priv on namespace to operate error log. */
		aclresult = pg_namespace_aclcheck(namespaceId, GetUserId(), mode);
		if (aclresult != ACLCHECK_OK)
				aclcheck_error(aclresult, ACL_KIND_NAMESPACE, schemaname);
		return true;
	}
	return false;
}

/*
 * Read one tuple and checksum value from the error log file pointed by fp.
 * This returns NULL whenever we see unexpected read or EOF.
 */
static HeapTuple
ErrorLogRead(FILE *fp, pg_crc32 *crc)
{
	uint32    t_len;
	HeapTuple tuple = NULL;

	LWLockAcquire(ErrorLogLock, LW_SHARED);

	do
	{
		if (fread(&t_len, 1, sizeof(uint32), fp) != sizeof(uint32))
				break;

		/*
		 * The tuple is "in-memory" format of HeapTuple.  Allocate the whole
		 * chunk consecutively.
		 */
		tuple = palloc(HEAPTUPLESIZE + t_len);
		tuple->t_len = t_len;
		ItemPointerSetInvalid(&tuple->t_self);
		tuple->t_data = (HeapTupleHeader) ((char *) tuple + HEAPTUPLESIZE);

		if (fread(crc, 1, sizeof(pg_crc32), fp) != sizeof(pg_crc32))
		{
			tuple = NULL;
			break;
		}

		if (fread(tuple->t_data, 1, tuple->t_len, fp) != tuple->t_len)
		{
			pfree(tuple);
			tuple = NULL;
			break;
		}
	} while (0);

	LWLockRelease(ErrorLogLock);

	return tuple;
}

/*
 * Utility to read error log and execute CRC check.
 */
HeapTuple
ReadValidErrorLogDatum(FILE *fp, const char *fname)
{
	HeapTuple tuple;
	pg_crc32c crc,
			  written_crc;

	tuple = ErrorLogRead(fp, &written_crc);

	/*
	 * CRC check.
	 */
	if (HeapTupleIsValid(tuple))
	{
		INIT_CRC32C(crc);
		COMP_CRC32C(crc, tuple->t_data, tuple->t_len);
		FIN_CRC32C(crc);

		if (!EQ_CRC32C(crc, written_crc))
		{
			elog(LOG, "incorrect checksum in error log %s", fname);
			tuple = NULL;
		}
		return tuple;
	}
	return NULL;
}

/*
 * makeSrehandler
 *
 * Allocate and initialize a Single Row Error Handling state object.
 * Pass in the only known parameters (both we get from the SQL stmt),
 * the other variables are set later on, when they are known.
 */
Srehandler *
makeSrehandler(int rejectlimit, bool is_limit_in_rows,
               char *filename, char *relname,
               char log_type)
{
	Srehandler *h = palloc0(sizeof(Srehandler));

	h->errmsg  = NULL;
	h->rawdata = (StringInfo) palloc(sizeof(StringInfoData));
	memset(h->rawdata, 0, sizeof(StringInfoData));
	h->linenumber           = 0;
	h->bytenum              = 0;
	h->processed            = 0;
	h->relname              = relname;
	h->rejectlimit          = rejectlimit;
	h->is_limit_in_rows     = is_limit_in_rows;
	h->rejectcount          = 0;
	h->is_server_enc        = false;
	h->log_to_file          = ((log_type == 't' || log_type == 'p') ? true : false);
	h->error_log_persistent = (log_type == 'p' ? true : false);
	snprintf(h->nodename, NAMEDATALEN, "%s", PGXCNodeName);
	snprintf(h->filename, sizeof(h->filename), "%s", filename ? filename : "<stdin>");

	/*
	 * Create a temporary memory context that we can reset once per row to
	 * recover palloc'd memory.  This avoids any problems with leaks inside
	 * datatype input routines, and should be faster than retail pfree's
	 * anyway.
	 */
	h->badrowcontext = AllocSetContextCreate(CurrentMemoryContext,
											 "SrehMemCtxt",
											 ALLOCSET_SMALL_SIZES);

	return h;
}


void
destroyCopySrehandler(Srehandler *srehandler)
{
	/* delete the bad row context */
	MemoryContextDelete(srehandler->badrowcontext);
	pfree(srehandler->rawdata);
	pfree(srehandler);
}

/*
 * ReportSrehResults
 *
 * When necessary emit a NOTICE that describes the end result of the
 * SREH operations. Information includes the total number of rejected
 * rows, and whether rows were ignored or logged into an error log file.
 */
void
ReportSrehResults(Srehandler *srehandler)
{
	if (srehandler->rejectcount > 0 || srehandler->processed > 0)
	{
		ereport(LOG,
				(errmsg("processed %ld rows and found %ld data formatting errors "
						"( %ld or more input rows) on %s, rejected related input data",
						srehandler->processed,
						srehandler->rejectcount,
						srehandler->rejectcount,
						PGXCNodeName)));
	}
}
