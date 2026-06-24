/*-------------------------------------------------------------------------
 *
 * exttablecmds.c
 *	  Commands for creating and altering external tables
 *
 * src/backend/commands/exttablecmds.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/external.h"
#include "catalog/namespace.h"
#include "catalog/pg_extprotocol.h"
#include "catalog/pg_foreign_server.h"
#include "catalog/pg_authid.h"
#include "commands/copy.h"
#include "commands/copysreh.h"
#include "commands/defrem.h"
#include "commands/tablecmds.h"
#include "mb/pg_wchar.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "utils/builtins.h"
#include "utils/inval.h"
#include "utils/syscache.h"
#include "utils/uri.h"
#include "utils/orcl_datetime_formatting.h"
#include "utils/orcl_datetime.h"

static char* transformLocationUris(List *locs, bool isweb, bool iswritable);
static char* transformExecOnClause(List *on_clause);
static char transformFormatType(char *formatname);
static List * transformFormatOpts(char formattype, List *formatOpts, int numcols, bool iswritable);
static void TransformColPositionOptions(CreateExternalStmt *stmt);
static List * GenerateExtTableEntryOptions(Oid tbloid,
										   bool iswritable,
										   bool issreh,
										   char formattype,
										   char rejectlimittype,
										   char* commandString,
										   int rejectlimit,
										   char logerrors,
										   int encoding,
										   char* locationExec,
										   char* locationUris);

static void
TransformColPositionOptions(CreateExternalStmt *stmt)
{
	ListCell   *lc            = NULL;
	StringInfo str            = makeStringInfo();
	bool       hasColPosition = false;

	foreach (lc, stmt->tableElts)
	{
		ColumnDef *coldef = (ColumnDef *) lfirst(lc);
		if (coldef->attinfo)
		{
			Position *pos = coldef->attinfo->attrpos;

			if (lc != list_head(stmt->tableElts))
				appendStringInfoChar(str, '.');

			appendStringInfo(str,
							 "%s(%d)(%d,%d)",
							 strVal(coldef->attinfo->attname),
							 (int) pos->flag,
							 pos->start_offset,
							 pos->end_offset);
			hasColPosition = true;
		}
	}

	if (hasColPosition)
		stmt->extOptions = lappend(stmt->extOptions, makeDefElem("col_position", (Node *) makeString(str->data), -1));
}

/* ----------------------------------------------------------------
*		DefineExternalRelation
*				Creates a new external relation.
*
* In here we first dispatch a normal DefineRelation() (with relstorage
* external) in order to create the external relation entries in pg_class
* pg_type etc. Then once this is done we dispatch ourselves (DefineExternalRelation)
* in order to create the pg_foreign_table entry across the gp array.
*
* Why don't we just do all of this in one dispatch run? Because that
* involves duplicating the DefineRelation() code or severely modifying it
* to have special cases for external tables. IMHO it's better and cleaner
* to leave it intact and do another dispatch.
* ----------------------------------------------------------------
*/
void
DefineExternalRelation(CreateExternalStmt *createExtStmt)
{
	CreateForeignTableStmt *createForeignTableStmt;
	CreateStmt *createStmt;
	ExtTableTypeDesc *exttypeDesc = (ExtTableTypeDesc *) createExtStmt->exttypedesc;
	DefElem    *dencoding = NULL;
	ListCell   *option;
	ObjectAddress objAddr;
	Oid			reloid = 0;
	List	   *formatOpts = NIL;
	List	   *entryOptions = NIL;
	char	   *locationUris = NULL;
	char	   *locationExec = NULL;
	char	   *commandString = NULL;
	char		rejectlimittype = '\0';
	char		formattype;
	int			rejectlimit = -1;
	int			encoding = -1;
	bool		issreh = false; /* is single row error handling requested? */
	char 		logerrors = 'f';/* does not log by default */
	bool		iswritable = createExtStmt->iswritable;
	bool		isweb = createExtStmt->isweb;
	SingleRowErrorDesc *singlerowerrorDesc = NULL;

	/*
	 * now set the parameters for keys/inheritance etc. Most of these are
	 * uninteresting for external relations...
	 */
	createForeignTableStmt = makeNode(CreateForeignTableStmt);
	createStmt = &createForeignTableStmt->base;
	createStmt->relation = createExtStmt->relation;
	createStmt->tableElts = createExtStmt->tableElts;
	createStmt->inhRelations = NIL;
	createStmt->constraints = NIL;
	createStmt->oncommit = ONCOMMIT_NOOP;
	createStmt->tablespacename = NULL;
	createStmt->distributeby = createExtStmt->distributeby; /* policy was set in transform */
	createForeignTableStmt->servername = EXTTABLE_SERVER_NAME;

	switch (exttypeDesc->exttabletype)
	{
		case EXTTBL_TYPE_LOCATION:
			/* Parse and validate URI strings (LOCATION clause) */
			locationExec = transformExecOnClause(exttypeDesc->on_clause);
			locationUris = transformLocationUris(exttypeDesc->location_list,
												 isweb, iswritable);
			break;

		case EXTTBL_TYPE_EXECUTE:
			locationExec = transformExecOnClause(exttypeDesc->on_clause);
			commandString = exttypeDesc->command_string;

			if (strlen(commandString) == 0)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("invalid EXECUTE clause, command string is empty")));
			break;

		default:
			elog(ERROR, "internal error: unknown external table type: %i",
				 exttypeDesc->exttabletype);
	}

	/*
	 * Parse and validate FORMAT clause.
	 */
	formattype = transformFormatType(createExtStmt->format);

	formatOpts = transformFormatOpts(formattype,
									   createExtStmt->formatOpts,
									   list_length(createExtStmt->tableElts),
									   iswritable);
	/*
	 * Parse Column Position info if needed.
	 */
	TransformColPositionOptions(createExtStmt);

	/*
	 * Parse single row error handling info if available
	 */
	singlerowerrorDesc = (SingleRowErrorDesc *) createExtStmt->sreh;
	if (singlerowerrorDesc)
	{
	        Assert(!iswritable);
	        issreh = true;
	        logerrors = singlerowerrorDesc->log_error_type;
	        
	        /* get reject limit, and reject limit type */
	        rejectlimit = singlerowerrorDesc->rejectlimit;

	        /* No limit set, log all errors. */
	        if (rejectlimit != SREH_UNLIMITED)
	        {
	                rejectlimittype = (singlerowerrorDesc->is_limit_in_rows ? 'r' : 'p');
	                VerifyRejectLimit(rejectlimittype, rejectlimit);
	        }
	}
	/*
	 * createExtStmt->extOptions is in a temporary context, duplicate it,
	 * checkout transformCreateExternalStmt() for the details.
	 */

	/* date_time_format are not allowed in writable external table. */
	if (createExtStmt->iswritable)
	{
		foreach (option, createExtStmt->extOptions)
		{
			DefElem *defel = lfirst_node(DefElem, option);

			if ((pg_strcasecmp(defel->defname, "date_format") == 0))
			{
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
								errmsg("DATE_FORMAT specification only available using COPY FROM or READ ONLY foreign table")));
			}
			if ((pg_strcasecmp(defel->defname, "time_format") == 0))
			{
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
								errmsg("TIME_FORMAT specification only available using COPY FROM or READ ONLY foreign table")));
			}
			if ((pg_strcasecmp(defel->defname, "timestamp_format") == 0))
			{
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
								errmsg("TIMESTAMP_FORMAT specification only available using COPY FROM or READ ONLY foreign table")));
			}
			if ((pg_strcasecmp(defel->defname, "col_position") == 0))
			{
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
								errmsg("COLUMN POSITION specification only available using COPY FROM or READ ONLY foreign table")));
			}
			if ((pg_strcasecmp(defel->defname, "fill_missing_fields") == 0) && defGetBoolean(defel))
			{
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
								errmsg("fill_missing_fields specification only available using COPY FROM or READ ONLY foreign table")));
			}
			if ((pg_strcasecmp(defel->defname, "ignore_extra_data") == 0) && defGetBoolean(defel))
			{
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
								errmsg("ignore_extra_data specification only available using COPY FROM or READ ONLY foreign table")));
			}

			if ((pg_strcasecmp(defel->defname, "compatible_illegal_chars") == 0) && defGetBoolean(defel))
			{
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
								errmsg("compatible_illegal_chars specification only available using COPY FROM or READ ONLY foreign table")));
			}
		}
	}


	formatOpts = list_concat(formatOpts, list_copy(createExtStmt->extOptions));

	/* Parse external table data encoding */
	foreach(option, createExtStmt->encoding)
	{
		DefElem    *defel = (DefElem *) lfirst(option);

		Assert(strcmp(defel->defname, "encoding") == 0);

		if (dencoding)
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					 errmsg("conflicting or redundant ENCODING specification")));
		dencoding = defel;
	}

	if (dencoding && dencoding->arg)
	{
		const char *encoding_name;

		if (IsA(dencoding->arg, Integer))
		{
			encoding = intVal(dencoding->arg);
			encoding_name = pg_encoding_to_char(encoding);
			if (strcmp(encoding_name, "") == 0 ||
				pg_valid_client_encoding(encoding_name) < 0)
				ereport(ERROR,
						(errcode(ERRCODE_UNDEFINED_OBJECT),
						 errmsg("%d is not a valid encoding code", encoding)));
		}
		else if (IsA(dencoding->arg, String))
		{
			encoding_name = strVal(dencoding->arg);
			if (pg_valid_client_encoding(encoding_name) < 0)
				ereport(ERROR,
						(errcode(ERRCODE_UNDEFINED_OBJECT),
						 errmsg("%s is not a valid encoding name",
								encoding_name)));
			encoding = pg_char_to_encoding(encoding_name);
		}
		else
			elog(ERROR, "unrecognized node type: %d",
				 nodeTag(dencoding->arg));
	}

	/* If encoding is defaulted, use database encoding */
	if (encoding < 0)
		encoding = pg_get_client_encoding();

	/*
	 * First, create the pg_class and other regular relation catalog entries.
	 */
	objAddr = DefineRelation(createStmt,
							 RELKIND_FOREIGN_TABLE,
							 InvalidOid,
							 NULL,
							 NULL,
							 InvalidOid);

	reloid = objAddr.objectId;

	/*
	 * Now add pg_foreign_table entries.
	 *
	 * get our pg_class external rel OID. If we're the QD we just created it
	 * above. If we're a QE DefineRelation() was already dispatched to us and
	 * therefore we have a local entry in pg_class. get the OID from cache.
	 */
	reloid = RangeVarGetRelid(createExtStmt->relation, NoLock, true);

	entryOptions = GenerateExtTableEntryOptions(reloid,
										   iswritable,
										   issreh,
										   formattype,
										   rejectlimittype,
										   commandString,
										   rejectlimit,
										   logerrors, /* logerrors */
										   encoding,
										   locationExec,
										   locationUris);

	createForeignTableStmt->options = list_concat(formatOpts, entryOptions);
	CreateForeignTable(createForeignTableStmt, reloid);

	/*
	 * DefineRelation loaded the new relation into relcache, but the relcache
	 * contains the distribution policy, which in turn depends on the contents
	 * of pg_foreign_table, for EXECUTE-type external tables (see GpPolicyFetch()).
	 * Now that we have created the pg_foreign_table entry, invalidate the
	 * relcache, so that it gets loaded with the correct information.
	 */
	CacheInvalidateRelcacheByRelid(reloid);
}

/* validate each URI string in the list then transform them into a string */
static char*
transformLocationUris(List *locs, bool isweb, bool iswritable)
{
	ListCell   *cell;
	StringInfoData buf;
	UriProtocol first_protocol = URI_FILE;		/* initialize to keep gcc
												 * quiet */
	bool		first_uri = true;

#define FDIST_DEF_PORT 8080

	initStringInfo(&buf);

	/* Parser should not let this happen */
	Assert(locs != NIL);

	/* iterate through the user supplied URI list from LOCATION clause */
	foreach(cell, locs)
	{
		Uri		   *uri;
		char	   *uri_str_orig;
		char	   *uri_str_final;
		Value	   *v = lfirst(cell);

		/* get the current URI string from the command */
		uri_str_orig = v->val.str;

		/* parse it to its components */
		uri = ParseExternalTableUri(uri_str_orig);

		/* in here edit the uri string if needed */

		/*
		 * no port was specified for tdx(gpfdist), tdxs(gpfdists) or hdfs.
		 * add the default
		 */
		if ((uri->protocol == URI_TDX || uri->protocol == URI_TDXS) && uri->port == -1)
		{
			char	   *at_hostname = (char *) uri_str_orig
			+ strlen(uri->protocol == URI_TDX ? "tdx://" : "tdxs://");
			char	   *after_hostname = strchr(at_hostname, '/');
			int			len = after_hostname - at_hostname;
			char	   *hostname = pstrdup(at_hostname);

			hostname[len] = '\0';

			/* add the default port number to the uri string */
			uri_str_final = psprintf("%s%s:%d%s",
					(uri->protocol == URI_TDX ? PROTOCOL_TDX : PROTOCOL_TDXS),
					hostname,
					FDIST_DEF_PORT, after_hostname);

			pfree(hostname);
		}
		else
		{
			/* no changes to original uri string */
			uri_str_final = pstrdup(uri_str_orig);
		}

		if (first_uri)
		{
			first_protocol = uri->protocol;
		}

		if (uri->protocol != first_protocol)
		{
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("URI protocols must be the same for all data sources"),
					 errhint("Available protocols are 'http', 'file', 'tdx' and 'tdxs'.")));

		}

		if (uri->protocol != URI_HTTP && isweb)
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					 errmsg("an EXTERNAL WEB TABLE may only use http URI\'s, problem in: \'%s\'", uri_str_final),
					 errhint("Use CREATE EXTERNAL TABLE instead.")));

		if (uri->protocol == URI_HTTP && !isweb)
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					 errmsg("http URI\'s can only be used in an external web table"),
					 errhint("Use CREATE EXTERNAL WEB TABLE instead.")));

		if (iswritable && (uri->protocol == URI_HTTP || uri->protocol == URI_FILE))
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("unsupported URI protocol \'%s\' for writable external table",
							(uri->protocol == URI_HTTP ? "http" : "file")),
					 errhint("Writable external tables may use \'tdx\' or \'tdxs\' URIs only.")));

		if (uri->protocol != URI_CUSTOM && iswritable && strchr(uri->path, '*'))
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					 errmsg("unsupported use of wildcard in a writable external web table definition: \'%s\'",
							uri_str_final),
					 errhint("Specify the explicit path and file name to write into.")));

		if ((uri->protocol == URI_TDX || uri->protocol == URI_TDXS) && iswritable && uri->path[strlen(uri->path) - 1] == '/')
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					 errmsg("unsupported use of a directory name in a writable gpfdist(s) external table : \'%s\'",
							uri_str_final),
					 errhint("Specify the explicit path and file name to write into.")));

		if (first_uri)
		{
			appendStringInfo(&buf, "%s", uri_str_final);
			first_uri = false;
		}
		else
		{
			appendStringInfo(&buf, "|%s", uri_str_final);
		}

		FreeExternalTableUri(uri);
		pfree(uri_str_final);
	}

	return buf.data;
}

static char*
transformExecOnClause(List *on_clause)
{
	ListCell   *exec_location_opt;
	char	   *exec_location_str = NULL;

	if (on_clause == NIL)
		exec_location_str = "ALL_SEGMENTS";
	else
	{
		/*
		 * Extract options from the statement node tree NOTE: as of now we only
		 * support one option in the ON clause and therefore more than one is an
		 * error (check here in case the sql parser isn't strict enough).
		 */
		foreach(exec_location_opt, on_clause)
		{
			DefElem    *defel = (DefElem *) lfirst(exec_location_opt);

			/* only one element is allowed! */
			if (exec_location_str)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("ON clause must not have more than one element")));

			if (strcmp(defel->defname, "all") == 0)
			{
				/* result: "ALL_SEGMENTS" */
				exec_location_str = "ALL_SEGMENTS";
			}
			else if (strcmp(defel->defname, "hostname") == 0)
			{
				/* result: "HOST:<hostname>" */
				exec_location_str = psprintf("HOST:%s", strVal(defel->arg));
			}
			else if (strcmp(defel->defname, "eachhost") == 0)
			{
				/* result: "PER_HOST" */
				exec_location_str = "PER_HOST";
			}
			else if (strcmp(defel->defname, "coordinator") == 0)
			{
				/* result: "COORDINATOR_ONLY" */
				exec_location_str = "COORDINATOR_ONLY";
			}
			else if (strcmp(defel->defname, "segment") == 0)
			{
				/* result: "SEGMENT_ID:<segid>" */
				exec_location_str = psprintf("SEGMENT_ID:%d", (int) intVal(defel->arg));
			}
			else if (strcmp(defel->defname, "random") == 0)
			{
				/* result: "TOTAL_SEGS:<number>" */
				exec_location_str = psprintf("TOTAL_SEGS:%d", (int) intVal(defel->arg));
			}
			else
			{
				ereport(ERROR,
						(errcode(ERRCODE_INTERNAL_ERROR),
						 errmsg("unknown location code for EXECUTE in tablecmds")));
			}
		}
	}

	return exec_location_str;
}


/*
 * Transform format name for external table FORMAT option to format code and
 * validate that the requested format is supported.
 */
static char
transformFormatType(char *formatname)
{
	char		result = '\0';

	if (pg_strcasecmp(formatname, "text") == 0)
		result = 't';
	else if (pg_strcasecmp(formatname, "csv") == 0)
		result = 'c';
	else if (pg_strcasecmp(formatname, "custom") == 0)
		result = 'b';
	else
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("unsupported format '%s'", formatname),
				 errhint("Available formats for external tables are \"text\", \"csv\" and \"custom\".")));

	return result;
}


/*
 * Transform the FORMAT options List into a new List suitable for storing in
 * pg_foreigntable.ftoptions.
 */
static List *
transformFormatOpts(char formattype, List *formatOpts, int numcols, bool iswritable)
{
	List 	   *cslist = NIL;
	ListCell   *option;
	ParseState *pstate;

	CopyState cstate = palloc0(sizeof(CopyStateData));

	pstate = make_parsestate(NULL);
	pstate->p_sourcetext = NULL;

	Assert(fmttype_is_custom(formattype) ||
		   fmttype_is_text(formattype) ||
		   fmttype_is_csv(formattype));

	/* Extract options from the statement node tree */
	if (fmttype_is_text(formattype) || fmttype_is_csv(formattype))
	{
		foreach(option, formatOpts)
		{
			DefElem    *defel = (DefElem *) lfirst(option);

			if (strcmp(defel->defname, "delimiter") == 0 ||
				strcmp(defel->defname, "null") == 0 ||
				strcmp(defel->defname, "header") == 0 ||
				strcmp(defel->defname, "quote") == 0 ||
				strcmp(defel->defname, "escape") == 0 ||
				strcmp(defel->defname, "force_not_null") == 0 ||
				strcmp(defel->defname, "force_quote") == 0 ||
				strcmp(defel->defname, "fill_missing_fields") == 0 ||
				strcmp(defel->defname, "ignore_extra_data") == 0 ||
				strcmp(defel->defname, "compatible_illegal_chars") == 0 ||
				strcmp(defel->defname, "eol") == 0 ||
				strcmp(defel->defname, "newline") == 0)
			{
				/* ok */
			}
			else if (strcmp(defel->defname, "formatter") == 0)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("formatter option only valid for custom formatters")));
			else
				elog(ERROR, "option \"%s\" not recognized",
					 defel->defname);
		}

		/* If CSV format was chosen, make it visible to ProcessCopyOptions. */
		if (fmttype_is_csv(formattype))
		{
			formatOpts = list_copy(formatOpts);
			formatOpts = lappend(formatOpts, makeDefElem("format", (Node *) makeString("csv"), -1));

			cslist = lappend(cslist, makeDefElem("format", (Node *) makeString("csv"), -1));
		}
		else
			cslist = lappend(cslist, makeDefElem("format", (Node *) makeString("text"), -1));

		/* verify all user supplied control char combinations are legal */
		ProcessCopyOptions(pstate,
						   cstate,
						   !iswritable, /* is_from */
						   formatOpts);

		/* keep the same order with the original pg_exttable catalog's fmtopt field */
		cslist = lappend(cslist, makeDefElem("delimiter", (Node *) makeString(cstate->delim), -1));
		cslist = lappend(cslist, makeDefElem("null", (Node *) makeString(cstate->null_print), -1));
		cslist = lappend(cslist, makeDefElem("escape", (Node *) makeString(cstate->escape), -1));

		if (fmttype_is_csv(formattype))
			cslist = lappend(cslist, makeDefElem("quote", (Node *) makeString(cstate->quote), -1));
		if (cstate->header_line)
			cslist = lappend(cslist, makeDefElem("header", (Node *) makeString("true"), -1));

		if (cstate->eol)
			cslist = lappend(cslist, makeDefElem("eol", (Node *) makeString(cstate->eol), -1));

		if (cstate->fill_missing)
			cslist = lappend(cslist, makeDefElem("fill_missing_fields", (Node *) makeString("true"), -1));

		if (cstate->ignore_extra_data)
			cslist = lappend(cslist, makeDefElem("ignore_extra_data", (Node *) makeString("true"), -1));

		if (cstate->compatible_illegal_chars)
			cslist = lappend(cslist, makeDefElem("compatible_illegal_chars", (Node *) makeString("true"), -1));
	}
	else
	{
		bool		found = false;

		foreach(option, formatOpts)
		{
			DefElem    *defel = (DefElem *) lfirst(option);

			if (strcmp(defel->defname, "formatter") == 0)
			{
				if (found)
					ereport(ERROR,
							(errcode(ERRCODE_SYNTAX_ERROR),
							 errmsg("redundant formatter option")));

				found = true;
			}
		}

		if (!found)
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					 errmsg("no formatter function specified")));

		cslist = list_copy(formatOpts);
		cslist = lappend(cslist, makeDefElem("format", (Node *) makeString("custom"), -1));
	}

	return cslist;
}


/*
 * Generate a list of ExtTableEntry options
 */
static List*
GenerateExtTableEntryOptions(Oid 	tbloid,
							 bool 	iswritable,
							 bool	issreh,
							 char	formattype,
							 char	rejectlimittype,
							 char*	commandString,
							 int	rejectlimit,
							 char	logerrors,
							 int	encoding,
							 char*	locationExec,
							 char*	locationUris)
{
	List		*entryOptions = NIL;

	entryOptions = lappend(entryOptions, makeDefElem("format_type", (Node *) makeString(psprintf("%c", formattype)), -1));

	if (commandString)
	{
		/* EXECUTE type table - store command and command location */
		entryOptions = lappend(entryOptions, makeDefElem("command", (Node *) makeString(pstrdup(commandString)), -1));
		entryOptions = lappend(entryOptions, makeDefElem("execute_on", (Node *) makeString(pstrdup(locationExec)), -1));
	}
	else
	{
		/* LOCATION type table - store uri locations. command is NULL */
		entryOptions = lappend(entryOptions, makeDefElem("location_uris", (Node *) makeString(pstrdup(locationUris)), -1));
		entryOptions = lappend(entryOptions, makeDefElem("execute_on", (Node *) makeString(pstrdup(locationExec)), -1));
	}

	if (issreh)
	{
		entryOptions = lappend(entryOptions, makeDefElem("reject_limit", (Node *) makeString(psprintf("%d", rejectlimit)), -1));
		if (rejectlimit != SREH_UNLIMITED)
		{
			entryOptions = lappend(entryOptions, makeDefElem("reject_limit_type", (Node *) makeString(psprintf("%c", rejectlimittype)), -1));
		}

	}
	entryOptions = lappend(entryOptions, makeDefElem("log_errors", (Node *) makeString(psprintf("%c", logerrors)), -1));
	entryOptions = lappend(entryOptions, makeDefElem("encoding", (Node *) makeString(psprintf("%d", encoding)), -1));
	entryOptions = lappend(entryOptions, makeDefElem("is_writable", (Node *) makeString(iswritable ? pstrdup("true") : pstrdup("false")), -1));

	/*
	 * Add the dependency of custom external table
	 */
	if (locationUris)
	{
		List *locationUris_list = NULL;
		ListCell *lc;

		foreach(lc, locationUris_list)
		{
			ObjectAddress	myself, referenced;
			char	   *location;
			char	   *protocol;
			Size		position;

			location = strVal(lfirst(lc));
			position = strchr(location, ':') - location;
			protocol = pnstrdup(location, position);

			myself.classId = RelationRelationId;
			myself.objectId = tbloid;
			myself.objectSubId = 0;

			referenced.classId = ExtprotocolRelationId;
			referenced.objectId = get_extprotocol_oid(protocol, true);
			referenced.objectSubId = 0;

			/*
			 * Only tables with custom protocol should create dependency, for
			 * internal protocols will get referenced.objectId as 0.
			 */
			if (referenced.objectId)
				recordDependencyOn(&myself, &referenced, DEPENDENCY_NORMAL);
		}
	}

	return entryOptions;
}
