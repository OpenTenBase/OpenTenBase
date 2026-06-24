/*-------------------------------------------------------------------------
 *
 * external.c
 *	  routines for getting external info from external table fdw.
 *
 * src/backend/access/external/external.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include <fstream/gfile.h>

#include "access/external.h"
#include "access/htup_details.h"
#include "access/reloptions.h"
#include "access/sysattr.h"
#include "catalog/indexing.h"
#include "catalog/pg_foreign_server.h"
#include "catalog/pg_foreign_table.h"
#include "commands/defrem.h"
#include "foreign/foreign.h"
#include "mb/pg_wchar.h"
#include "nodes/makefuncs.h"
#include "optimizer/var.h"
#include "pgxc/nodemgr.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"

static List *get_exttable_fdw_attribute_options(Oid relid);

void
gfile_printf_then_putc_newline(const char *format,...)
{
	char	   *a;
	va_list		va;
	int			i;

	va_start(va, format);
	i = vsnprintf(0, 0, format, va);
	va_end(va);

	if (i < 0)
		elog(NOTICE, "gfile_printf_then_putc_newline vsnprintf failed.");
	else if (!(a = palloc(i + 1)))
		elog(NOTICE, "gfile_printf_then_putc_newline palloc failed.");
	else
	{
		va_start(va, format);
		vsnprintf(a, i + 1, format, va);
		va_end(va);
		elog(NOTICE, "%s", a);
		pfree(a);
	}
}

void *
gfile_malloc(size_t size)
{
	return palloc(size);
}

void
gfile_free(void *a)
{
	pfree(a);
}

/* transform the locations string to a list */
List*
TokenizeLocationUris(char *uris)
{
	char *uri = NULL;
	List *result = NIL;

	Assert(uris != NULL);

	while ((uri = strsep(&uris, TDXSERVERDELIM)) != NULL)
	{
		result = lappend(result, makeString(uri));
	}

	return result;
}

/*
 * Get the entry for an exttable relation (from pg_foreign_table)
 */
ExtTableEntry*
GetExtTableEntry(Oid relid)
{
	ExtTableEntry *extentry;

	extentry = GetExtTableEntryIfExists(relid);
	if (!extentry)
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("missing pg_foreign_table entry for relation \"%s\"",
						get_rel_name(relid))));
	return extentry;
}

/*
 * Like GetExtTableEntry(Oid), but returns NULL instead of throwing
 * an error if no pg_foreign_table entry is found.
 */
ExtTableEntry*
GetExtTableEntryIfExists(Oid relid)
{
	Relation	pg_foreign_table_rel;
	ScanKeyData ftkey;
	SysScanDesc ftscan;
	HeapTuple	fttuple;
	ExtTableEntry *extentry;
	bool		isNull;
	List		*ftoptions_list = NIL;
	Datum		ftoptions;

	pg_foreign_table_rel = relation_open(ForeignTableRelationId, RowExclusiveLock);

	ScanKeyInit(&ftkey,
				Anum_pg_foreign_table_ftrelid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(relid));

	ftscan = systable_beginscan(pg_foreign_table_rel, ForeignTableRelidIndexId,
								true, NULL, 1, &ftkey);
	fttuple = systable_getnext(ftscan);

	if (!HeapTupleIsValid(fttuple))
	{
		systable_endscan(ftscan);
		relation_close(pg_foreign_table_rel, RowExclusiveLock);

		return NULL;
	}

	/* get the foreign table options */
	ftoptions = heap_getattr(fttuple,
						   Anum_pg_foreign_table_ftoptions,
						   RelationGetDescr(pg_foreign_table_rel),
						   &isNull);

	if (isNull)
	{
		/* options array is always populated, {} if no options set */
		elog(ERROR, "could not find options for external protocol");
	}
	else
	{
		ftoptions_list = untransformRelOptions(ftoptions);
	}

	extentry = GetExtFromForeignTableOptions(ftoptions_list, relid);

	/* Finish up scan and close catalogs */
	systable_endscan(ftscan);
	relation_close(pg_foreign_table_rel, RowExclusiveLock);

	return extentry;
}

ExtTableEntry *
GetExtFromForeignTableOptions(List *ftoptons, Oid relid)
{
	ExtTableEntry	   *extentry;
	ListCell		   *lc;
	List			   *entryOptions = NIL;
	char			   *arg;
	bool				fmtcode_found = false;
	bool				rejectlimit_found = false;
	bool				rejectlimittype_found = false;
	bool				logerrors_found = false;
	bool				encoding_found = false;
	bool				iswritable_found = false;
	bool				locationuris_found = false;
	bool				command_found = false;

	extentry = (ExtTableEntry *) palloc0(sizeof(ExtTableEntry));

	foreach(lc, ftoptons)
	{
		DefElem    *def = (DefElem *) lfirst(lc);

		if (pg_strcasecmp(def->defname, "location_uris") == 0)
		{
			extentry->urilocations = TokenizeLocationUris(defGetString(def));
			locationuris_found = true;
			continue;
		}

		if (pg_strcasecmp(def->defname, "execute_on") == 0)
		{
			extentry->execlocations = list_make1(makeString(defGetString(def)));
			continue;
		}

		if (pg_strcasecmp(def->defname, "command") == 0)
		{
			extentry->command = defGetString(def);
			command_found = true;
			continue;
		}

		if (pg_strcasecmp(def->defname, "format_type") == 0)
		{
			arg = defGetString(def);
			extentry->fmtcode = arg[0];
			fmtcode_found = true;
			continue;
		}

		/* only CSV format needs this for ProcessCopyOptions(), will do it later */
		if (pg_strcasecmp(def->defname, "format") == 0)
		{
			continue;
		}

		if (pg_strcasecmp(def->defname, "reject_limit") == 0)
		{
			extentry->rejectlimit = atoi(defGetString(def));
			rejectlimit_found = true;
			continue;
		}

		if (pg_strcasecmp(def->defname, "reject_limit_type") == 0)
		{
			arg = defGetString(def);
			extentry->rejectlimittype = arg[0];
			rejectlimittype_found = true;
			continue;
		}

		if (pg_strcasecmp(def->defname, "log_errors") == 0)
		{
			arg = defGetString(def);
			extentry->logerrors = arg[0];
			logerrors_found = true;
			continue;
		}

		if (pg_strcasecmp(def->defname, "encoding") == 0)
		{
			extentry->encoding = atoi(defGetString(def));
			encoding_found = true;
			continue;
		}

		if (pg_strcasecmp(def->defname, "is_writable") == 0)
		{
			extentry->iswritable = defGetBoolean(def);
			iswritable_found = true;
			continue;
		}

		entryOptions = lappend(entryOptions, makeDefElem(def->defname, (Node *) makeString(pstrdup(defGetString(def))), -1));
	}

	/* If CSV format was chosen, make it visible to ProcessCopyOptions. */
	if (fmttype_is_csv(extentry->fmtcode))
		entryOptions = lappend(entryOptions, makeDefElem("format", (Node *) makeString("csv"), -1));

	/*
	 * external table syntax does have these for sure, but errors could happen
	 * if using foreign table syntax
	 */
	if (!fmtcode_found || !logerrors_found || !encoding_found || !iswritable_found)
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("missing format, logerrors, encoding or iswritable options for relation \"%s\"",
						get_rel_name(relid))));

	if (locationuris_found && command_found)
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("locationuris and command options conflict with each other")));

	if (!fmttype_is_custom(extentry->fmtcode) &&
		!fmttype_is_csv(extentry->fmtcode) &&
		!fmttype_is_text(extentry->fmtcode))
		elog(ERROR, "unsupported format type %d for external table", extentry->fmtcode);

	if (!rejectlimit_found) {
		/* mark that no SREH requested */
		extentry->rejectlimit = -1;
	}

	if (rejectlimittype_found)
	{
		if (extentry->rejectlimittype != 'r' && extentry->rejectlimittype != 'p')
			elog(ERROR, "unsupported reject limit type %c for external table",
				 extentry->rejectlimittype);
	}
	else
		extentry->rejectlimittype = -1;

	if (!PG_VALID_ENCODING(extentry->encoding))
		elog(ERROR, "invalid encoding found for external table");

	extentry->options = entryOptions;

	return extentry;
}

static List *
create_external_scan_uri_list(List *urilocations)
{
	ListCell *c;
	List     *modifiedloclist = NIL;
	int      i;
	int      total_primaries;
	char     **segdb_file_map;
	
	/* various processing flags */
	bool found_match     = false;
	bool done            = false;
	List *filenames;
	
	/* tdx or EXECUTE specific variables */
	int  max_participants_allowed = 0;
	int  num_segs_participating   = 0;
	bool should_skip_tail         = false;
	
	/* get the total valid primary segdb count */
	total_primaries = NumDataNodes;
	
	/*
	 * initialize a file-to-segdb mapping. segdb_file_map string array indexes
	 * segindex and the entries are the external file path is assigned to this
	 * segment database. For example if segdb_file_map[2] has "/tmp/emp.1" then
	 * this file is assigned to primary segdb 2. if an entry has NULL then
	 * that segdb isn't assigned any file.
	 */
	segdb_file_map = (char **) palloc0(total_primaries * sizeof(char *));
	
	/*
	 * Now we do the actual assignment of work to the segment databases (where
	 * work is either a URI to open or a command to execute). Due to the big
	 * differences between the different protocols we handle each one
	 * separately. Unfortunately this means some code duplication, but keeping
	 * this separation makes the code much more understandable and (even) more
	 * maintainable.
	 *
	 * Outline of the following code blocks (from simplest to most complex):
	 * (only one of these will get executed for a statement)
	 *
	 * 2) segment mapping for tables with LOCATION tdx:// or custom
	 * protocol
	 *
	 * This protocol is more complicated - in here we usually duplicate the
	 * user supplied tdx:// URIs until there is one available to every
	 * segdb. However, in some cases (as determined by tdx_external_max_segs
	 * GUC) we don't want to use *all* segdbs but instead figure out how many
	 * and pick them randomly (this is mainly for better performance and
	 * resource mgmt).
	 */
	{
		/*
		 * Re-write the location list for tdx before mapping to segments.
		 *
		 * If we happen to be dealing with URI's with the 'tdx' protocol
		 * we do an extra step here.
		 *
		 * (*) We modify the urilocationlist so that every
		 * primary segdb will get a URI (therefore we duplicate the existing
		 * URI's until the list is of size = total_primaries).
		 * Example: 2 URIs, 7 total segdbs.
		 * Original LocationList: URI1->URI2
		 * Modified LocationList: URI1->URI2->URI1->URI2->URI1->URI2->URI1
		 *
		 * (**) We also make sure that we don't allocate more segdbs than
		 * (# of URIs x tdx_external_max_segs).
		 * Example: 2 URIs, 7 total segdbs, tdx_external_max_segs = 3
		 * Original LocationList: URI1->URI2
		 * Modified LocationList: URI1->URI2->URI1->URI2->URI1->URI2 (6 total).
		 *
		 * (***) In that case that we need to allocate only a subset of primary
		 * segdbs and not all we then also need to skip some-segment.
		 * Using the previous example a we create a map of 7 entries and need to
		 * skip the last segemnt(7 - 6 = 1). so it may look like this: [F F F F F F T].
		 */
		
		/* total num of segs that will participate in the external operation */
		num_segs_participating = total_primaries;
		
		/* max num of segs that are allowed to participate in the operation */
		max_participants_allowed = list_length(urilocations) * tdx_external_max_segs;
		
		elog(DEBUG5,
		     "num_segs_participating = %d. max_participants_allowed = %d. number of URIs = %d",
		     num_segs_participating, max_participants_allowed, list_length(urilocations));
		
		/* see (**) above */
		if (num_segs_participating > max_participants_allowed)
		{
			num_segs_participating = max_participants_allowed;
			should_skip_tail       = true;
			
			elog(NOTICE, "External scan will utilize %d out "
			             "of %d segment databases from tdx server",
			     num_segs_participating,
			     total_primaries);
		}
		
		if (list_length(urilocations) > num_segs_participating)
		{
			ereport(ERROR,
			        (errcode(ERRCODE_INVALID_TABLE_DEFINITION),
					        errmsg("there are more external files (URLs) than primary segments that can read them"),
					        errdetail("Found %d URLs and %d primary segments.",
					                  list_length(urilocations),
					                  num_segs_participating)));
		}
		
		/*
		 * restart location list and fill in new list until number of
		 * locations equals the number of segments participating in this
		 * action (see (*) above for more details).
		 */
		while (!done)
		{
			foreach(c, urilocations)
			{
				char *uri_str = (char *) strVal(lfirst(c));
				
				/* append to a list of Value nodes, size nelems */
				modifiedloclist = lappend(modifiedloclist, makeString(pstrdup(uri_str)));
				
				if (list_length(modifiedloclist) == num_segs_participating)
				{
					done = true;
					break;
				}
				
				if (list_length(modifiedloclist) > num_segs_participating)
				{
					elog(ERROR, "External scan location list failed building distribution.");
				}
			}
		}
		
		/*
		 * assign each URI from the new location list a primary segdb
		 */
		foreach(c, modifiedloclist)
		{
			const char *uri_str = strVal(lfirst(c));
			found_match     = false;
			
			/*
			 * look through our segment database list and try to find a
			 * datanode that can handle this uri.
			 */
			for (i = 0; i < NumDataNodes && !found_match; i++)
			{
				int segind = i;
				
				/*
				 * Assign mapping of external file to this segdb only if:
				 * 1) This segdb is a valid primary.
				 * 2) An external file wasn't already assigned to it.
				 */
				{
					/*
					 * skip this segdb if skip_map for this seg index tells us
					 * to skip it (set to 'true').
					 */
					if (should_skip_tail)
					{
						Assert(segind < total_primaries);
						
						if (segind >= num_segs_participating)
							continue;    /* skip it */
					}
					
					if (segdb_file_map[segind] == NULL)
					{
						/* segdb not taken yet. assign this URI to this segdb */
						segdb_file_map[segind] = pstrdup(uri_str);
						found_match = true;
					}
					
					/*
					 * too bad. this segdb already has an external source
					 * assigned
					 */
				}
			}
			
			/* We failed to find a segdb for this tdx URI */
			if (!found_match)
			{
				/* should never happen */
				elog(LOG,
				     "external tables tdx allocation error. "
				     "total_primaries: %d, num_segs_participating %d "
				     "max_participants_allowed %d, total_to_skip %d",
				     total_primaries, num_segs_participating,
				     max_participants_allowed, total_primaries - num_segs_participating);
				
				elog(ERROR,
				     "internal error in createplan for external tables when trying to assign segments for tdx");
			}
		}
	}
	
	/*
	 * convert array map to a list so it can be serialized as part of the plan
	 */
	filenames = NIL;
	for (i = 0; i < total_primaries; i++)
	{
		if (segdb_file_map[i] != NULL)
			filenames = lappend(filenames, makeString(segdb_file_map[i]));
		else
		{
			/* no file for this segdb. add a null entry */
			Value *n = makeNode(Value);
			
			n->type = T_Null;
			filenames = lappend(filenames, n);
		}
	}
	
	return filenames;
}

static List *UntransformFormatterOption(DefElem *def)
{
	char     *s       = pstrdup(strVal(def->arg));
	List     *entries = NIL;
	ListCell *lc      = NULL;
	char     *p       = NULL;
	char     *token   = NULL;
	List     *result  = NIL;
	
	/* get position string */
	token = strtok_r(s, ".", &p);
	while (token != NULL)
	{
		entries = lappend(entries, token);
		token   = strtok_r(NULL, ".", &p);
	}
	
	foreach (lc, entries)
	{
		Position *pos      = (Position *) makeNode(Position);
		AttInfo  *att_info = (AttInfo *) makeNode(AttInfo);
		char     *end      = NULL;
		int      pos_int   = 0;
		
		s = (char *) lfirst(lc);
		
		/* get column name */
		p = strchr(s, '(');
		if (p == NULL)
			ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					errmsg("Invalid formatter options \"%s\"", (char *) lfirst(lc))));
		att_info->attname = (Node *) makeString(pnstrdup(s, (Size) (p - s)));
		
		/* get postype */
		s = p + 1;
		p = strchr(s, ')');
		if (p == NULL)
			ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					errmsg("Invalid formatter options \"%s\"", (char *) lfirst(lc))));
		s       = pnstrdup(s, p - s);
		pos_int = (int) strtol(s, &end, 10);
		if ((end == NULL) || (*end != '\0'))
			ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					errmsg("Invalid formatter options \"%s\"", (char *) lfirst(lc))));
		pfree(s);
		if (pos_int)
			pos->flag = ABSOLUTE_POS;
		else
			pos->flag = RELATIVE_POS;
		
		/* get begin offset */
		s = p + 2;
		p = strchr(s, ',');
		if (p == NULL)
			ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					errmsg("Invalid formatter options \"%s\"", (char *) lfirst(lc))));
		s = pnstrdup(s, p - s);
		pos->start_offset = (int) strtol(s, &end, 10);
		if ((end == NULL) || (*end != '\0'))
			ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					errmsg("Invalid formatter options \"%s\"", (char *) lfirst(lc))));
		pfree(s);
		
		/* Get length of field */
		s = p + 1;
		p = strchr(s, ')');
		if (p == NULL)
			ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					errmsg("Invalid formatter options \"%s\"", (char *) lfirst(lc))));
		s = pnstrdup(s, p - s);
		pos->end_offset = (int) strtol(s, &end, 10);
		if ((end == NULL) || *end != '\0')
			ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					errmsg("Invalid formatter options \"%s\"", (char *) lfirst(lc))));
		pfree(s);
		
		att_info->attrpos = pos;
		result = lappend(result, att_info);
	}
	list_free_ext(entries);
	
	return result;
}

/* fetch the options for a exttable_fdw foreign table */
void
exttableGetOptions(Oid foreigntableid, FileScanDesc scan, List **extoptions, List **attinfolist)
{
	ForeignTable		*table;
	ForeignServer		*server;
	ForeignDataWrapper	*wrapper;
	List				*options;
	ListCell			*lc;
	bool				 format = false;
	bool				 delimiter = false;
	bool				 header = false;
	char				*uri = NULL;
	
	table = GetForeignTable(foreigntableid);
	server = GetForeignServer(table->serverid);
	wrapper = GetForeignDataWrapper(server->fdwid);
	
	options = NIL;
	options = list_concat(options, wrapper->options);
	options = list_concat(options, server->options);
	options = list_concat(options, table->options);
	options = list_concat(options, get_exttable_fdw_attribute_options(foreigntableid));
	
	foreach(lc, options)
	{
		DefElem    *def = (DefElem *) lfirst(lc);
		
		if (pg_strcasecmp(def->defname, "location_uris") == 0 && IS_PGXC_DATANODE)
		{
			if (scan->locator_info)
			{
				List *urilocations = TokenizeLocationUris(defGetString(def));
				int num_urls = list_length(urilocations);
				ListCell *c;
				int  i = 0;
				
				/* Check */
				/* if a uri is assigned to us - get a reference to it. */
				if (urilocations)
				{
					/* set external source (uri) */
					scan->fs_uri = (char **)palloc(sizeof(char *) * num_urls);
					scan->fs_num = num_urls;
					foreach(c, urilocations)
					{
						char *uri_str = (char *) strVal(lfirst(c));
						scan->fs_uri[i] = uri_str;
						i++;
					}
					/* NOTE: we delay actually opening the data source until external_getnext() å*/
				}
				else
				{
					/* segdb has no work to do. set to no-op */
					scan->fs_noop = true;
					scan->fs_uri = NULL;
				}
			}
			else
			{
				List *urilocations = TokenizeLocationUris(defGetString(def));
				int num_urls = list_length(urilocations);
				int my_url = PGXCNodeId - 1;
				Value *v;
				
				/* assign Uris to segments. */
				urilocations = create_external_scan_uri_list(urilocations);
				
				if (NumDataNodes < num_urls)
				{
					ereport(ERROR,
					        (errcode(ERRCODE_INVALID_TABLE_DEFINITION),
							        errmsg("Numbers of DNs: %d; Numbers of URIs: %d.", NumDataNodes, num_urls),
							        errdetail("There are more URIs than total primary datanode databases")));
				}
				
				/* get a url to use. we use datanode number modulo total num of urls */
				v = list_nth(urilocations, my_url);
				
				if (v->type == T_Null)
					uri = NULL;
				else
					uri = (char *) strVal(v);
				
				/*
				 * if a uri is assigned to us - get a reference to it. Some executors
				 * don't have a uri to scan (if # of uri's < # of primary segdbs). in
				 * which case uri will be NULL. If that's the case for this segdb set to
				 * no-op.
				 */
				if (uri)
				{
					/* set external source (uri) */
					scan->fs_uri = (char **)palloc(sizeof(char *));
					scan->fs_uri[0] = uri;
					scan->fs_num = 1;
					
					/*
					 * NOTE: we delay actually opening the data source until
					 * external_getnext()
					 */
				}
				else
				{
					/* segdb has no work to do. set to no-op */
					scan->fs_noop = true;
					scan->fs_uri = NULL;
				}
			}
			
			continue;
		}
		else if (pg_strcasecmp(def->defname, "format") == 0)
		{
			*extoptions = lappend(*extoptions,
			                      makeDefElem("format", (Node *) makeString(defGetString(def)), -1));
			format = true;
			continue;
		}
		else if (pg_strcasecmp(def->defname, "delimiter") == 0)
		{
			*extoptions = lappend(*extoptions,
			                      makeDefElem("delimiter", (Node *) makeString(defGetString(def)), -1));
			delimiter = true;
			continue;
		}
		else if (pg_strcasecmp(def->defname, "null") == 0)
		{
			*extoptions = lappend(*extoptions,
			                      makeDefElem("null", (Node *) makeString(defGetString(def)), -1));
			continue;
		}
		else if (pg_strcasecmp(def->defname, "header") == 0)
		{
			*extoptions = lappend(*extoptions,
			                      makeDefElem("header", (Node *) makeString(defGetString(def)), -1));
			header = true;
			continue;
		}
		else if (pg_strcasecmp(def->defname, "eol") == 0)
		{
			*extoptions = lappend(*extoptions,
			                      makeDefElem("eol", (Node *) makeString(defGetString(def)), -1));
			continue;
		}
		else if (pg_strcasecmp(def->defname, "quote") == 0)
		{
			*extoptions = lappend(*extoptions,
			                      makeDefElem("quote", (Node *) makeString(defGetString(def)), -1));
			continue;
		}
		else if (pg_strcasecmp(def->defname, "escape") == 0)
		{
			*extoptions = lappend(*extoptions,
			                      makeDefElem("escape", (Node *) makeString(defGetString(def)), -1));
			continue;
		}
		
		if (pg_strcasecmp(def->defname, "is_writable") == 0)
		{
			if (defGetBoolean(def))
				ereport(ERROR,
				        (errcode(ERRCODE_WRONG_OBJECT_TYPE),
						        errmsg("cannot read from a WRITABLE external table"),
						        errhint("Create the table as READABLE instead.")));
		}
		
		if (pg_strcasecmp(def->defname, "reject_limit") == 0)
		{
			*extoptions = lappend(*extoptions,
			                      makeDefElem("reject_limit",
			                                  (Node *) makeInteger(atoi(defGetString(def))),
			                                  -1));
			continue;
		}
		
		if (pg_strcasecmp(def->defname, "reject_limit_type") == 0)
		{
			*extoptions = lappend(*extoptions,
			                      makeDefElem("reject_limit_type",
			                                  (Node *) makeString(defGetString(def)),
			                                  -1));
			continue;
		}
		
		if (pg_strcasecmp(def->defname, "log_errors") == 0)
		{
			*extoptions = lappend(*extoptions,
			                      makeDefElem("log_errors", (Node *) makeString(defGetString(def)), -1));
			continue;
		}
		
		if (pg_strcasecmp(def->defname, "encoding") == 0)
		{
			*extoptions = lappend(*extoptions,
			                      makeDefElem("encoding", (Node *) makeInteger(atoi(defGetString(def))), -1));
			continue;
		}
		
		else if (pg_strcasecmp(def->defname, "date_format") == 0)
		{
			*extoptions = lappend(*extoptions,
			                      makeDefElem("date_format", (Node *) makeString(defGetString(def)), -1));
			continue;
		}
		else if (pg_strcasecmp(def->defname, "time_format") == 0)
		{
			*extoptions = lappend(*extoptions,
			                      makeDefElem("time_format", (Node *) makeString(defGetString(def)), -1));
			continue;
		}
		else if (pg_strcasecmp(def->defname, "timestamp_format") == 0)
		{
			*extoptions = lappend(*extoptions,
			                      makeDefElem("timestamp_format", (Node *) makeString(defGetString(def)), -1));
			continue;
		}
		else if (attinfolist && pg_strcasecmp(def->defname, "col_position") == 0)
		{
			*attinfolist = UntransformFormatterOption(def);
			*extoptions = lappend(*extoptions, makeDefElem("col_position", (Node *) makeString(defGetString(def)), -1));
			continue;
		}
		else if (pg_strcasecmp(def->defname, "fill_missing_fields") == 0)
		{
			*extoptions = lappend(*extoptions,
			                      makeDefElem("fill_missing_fields", (Node *) makeString(defGetString(def)), -1));
			continue;
		}
		else if (pg_strcasecmp(def->defname, "ignore_extra_data") == 0)
		{
			*extoptions = lappend(*extoptions,
			                      makeDefElem("ignore_extra_data", (Node *) makeString(defGetString(def)), -1));
			continue;
		}
		else if (pg_strcasecmp(def->defname, "compatible_illegal_chars") == 0)
		{
			*extoptions = lappend(*extoptions,
			                      makeDefElem("compatible_illegal_chars", (Node *) makeString(defGetString(def)), -1));
			continue;
		}
	}
	
	if (!format)
	{
		*extoptions = lappend(*extoptions,
		                      makeDefElem("format", (Node *) makeString("csv"), -1));
	}
	
	if (!delimiter)
	{
		*extoptions = lappend(*extoptions,
		                      makeDefElem("delimiter", (Node *) makeString(","), -1));
	}
	
	if (!header)
	{
		*extoptions = lappend(*extoptions,
		                      makeDefElem("header", (Node *) makeString("false"), -1));
	}
}

/*
 * retrieve per-column generic options from pg_attribute and construct a list
 * of DefElems representing them.
 *
 * At the moment we only have "force_not_null", and "force_null",
 * which should each be combined into a single DefElem listing all such
 * columns, since that's what COPY expects.
 */
static List *
get_exttable_fdw_attribute_options(Oid relid)
{
	Relation	rel;
	TupleDesc	tupleDesc;
	AttrNumber	natts;
	AttrNumber	attnum;
	List	   *fnncolumns = NIL;
	List	   *fncolumns = NIL;
	
	List	   *options = NIL;
	
	rel = heap_open(relid, AccessShareLock);
	tupleDesc = RelationGetDescr(rel);
	natts = tupleDesc->natts;
	
	/* retrieve FDW options for all user-defined attributes */
	for (attnum = 1; attnum <= natts; attnum++)
	{
		Form_pg_attribute attr = TupleDescAttr(tupleDesc, attnum - 1);
		List	   *options;
		ListCell   *lc;
		
		/* skip dropped attributes */
		if (attr->attisdropped)
			continue;
		
		options = GetForeignColumnOptions(relid, attnum);
		foreach(lc, options)
		{
			DefElem    *def = (DefElem *) lfirst(lc);
			
			if (strcmp(def->defname, "force_not_null") == 0)
			{
				if (defGetBoolean(def))
				{
					char	   *attname = pstrdup(NameStr(attr->attname));
					
					fnncolumns = lappend(fnncolumns, makeString(attname));
				}
			}
			else if (strcmp(def->defname, "force_null") == 0)
			{
				if (defGetBoolean(def))
				{
					char	   *attname = pstrdup(NameStr(attr->attname));
					
					fncolumns = lappend(fncolumns, makeString(attname));
				}
			}
			/* maybe in future handle other options here */
		}
	}
	
	heap_close(rel, AccessShareLock);
	
	/*
	 * return DefElem only when some column(s) have force_not_null /
	 * force_null options set
	 */
	if (fnncolumns != NIL)
		options = lappend(options, makeDefElem("force_not_null", (Node *) fnncolumns, -1));
	
	if (fncolumns != NIL)
		options = lappend(options, makeDefElem("force_null", (Node *) fncolumns, -1));
	
	return options;
}

/* get a chunk of data from the external data file */
static int
external_getdata(FileScanDesc scan, CopyState pstate, void *outbuf, int maxread)
{
	int			bytesread = 0;
	URL_FILE    *extfile = scan->fs_file[scan->fs_current];

	/* this fs file reached EOF, no need to scan again */
	if (!bms_is_member(scan->fs_current, scan->fs_bitmap))
		return bytesread;

	/*
	 * CK: this code is very delicate. The caller expects this:
	 * 		- if url_fread returns something, and the EOF is reached, it must
	 * 		  return with both the content and the reached_eof flag set.
	 * 		- failing to do so will result in skipping the last line.
	 */
	bytesread = url_fread((void *) outbuf, maxread, extfile, pstate);
	
	if (url_feof(extfile, bytesread))
	{
		bms_del_member(scan->fs_bitmap, scan->fs_current);
		elog(DEBUG5, "reached EOF, fs_current = %d", scan->fs_current);
		if (bms_is_empty(scan->fs_bitmap))
		{
			pstate->fe_eof = true;
		}
	}
	
	if (bytesread <= 0)
	{
		if (url_ferror(extfile, bytesread, NULL, 0))
			ereport(ERROR,
			        (errcode_for_file_access(),
					        errmsg("could not read from external file: %m")));
		
	}

	return bytesread;
}

static void
external_get_next_scan(FileScanDesc scan)
{
	int scanIndex = (scan->fs_current + 1) % scan->fs_num;
	scan->fs_current = scanIndex;
}

/*
 * fetch more data from the source,
 * and feed it to the COPY FROM machinery for parsing
 */ 
int
external_getdata_callback(void *outbuf, int minread, int maxread, void *extra)
{
	int bytesread;
	FileScanDesc scan = (FileScanDesc) extra;

	/* get the next scan fd */
	external_get_next_scan(scan);
	for(;;)
	{
		bytesread = external_getdata(scan, scan->fs_pstate, outbuf, maxread);

		/*
		 * bytesread is not need to check here, because it checked in
		 * external_getdata function. If bytesread <=0 maybe this scan fs
		 * is empty, so need to scan the next fs file.
		 */
		if (bytesread <= 0 && !bms_is_empty(scan->fs_bitmap))
			external_get_next_scan(scan);
		else
			return bytesread;
	}

	return bytesread;
}

/*
 * open the external source for scanning (RET only)
 *
 * an external source is one of the following:
 * 1) a local file (requested by 'file')
 * 2) a remote http server
 * 3) a remote gpfdist server
 * 4) a command to execute
 */
void
open_external_readable_source(FileScanDesc scan, ExternalSelectDesc desc)
{
	extvar_t	extvar;
	int i;
	
	/* set up extvar */
	memset(&extvar, 0, sizeof(extvar));
	
	if (scan->fs_pstate->eol_type != EOL_UD)
		scan->fs_pstate->eol_type = EOL_UNKNOWN;
	scan->fs_scancounter = 1;
	scan->fs_custom_formatter_params = NULL;
	
	external_set_env_vars_ext(&extvar,
	                          scan->fs_pstate,
	                          scan->fs_scancounter,
	                          scan->fs_custom_formatter_params,
	                          scan->locator_info);

	scan->fs_file = (URL_FILE **) palloc0(sizeof(URL_FILE *) * scan->fs_num);

	/* actually open the external source */
	for (i = 0; i < scan->fs_num; i++)
	{
		scan->fs_file[i] = url_fopen(scan->fs_uri[i],
		                          false,	/* for read */
		                          &extvar,
		                          scan->fs_pstate,
		                          desc);
	}
}
