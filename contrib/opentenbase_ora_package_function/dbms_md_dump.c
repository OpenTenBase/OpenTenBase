/*-------------------------------------------------------------------------
 *
 * pg_dump.c
 *	  pg_dump is a utility for dumping out a postgres database
 *	  into a script file.
 *
 * Portions Copyright (c) 2012-2014, TransLattice, Inc.
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *	pg_dump will read the system catalogs in a database and dump out a
 *	script that reproduces the schema in terms of SQL that is understood
 *	by PostgreSQL
 *
 *	Note that pg_dump runs in a transaction-snapshot mode transaction,
 *	so it sees a consistent snapshot of the database including system
 *	catalogs. However, it relies in part on various specialized backend
 *	functions like pg_get_indexdef(), and those things tend to look at
 *	the currently committed state.  So it is possible to get 'cache
 *	lookup failed' error if someone performs DDL changes while a dump is
 *	happening. The window for this sort of thing is from the acquisition
 *	of the transaction snapshot to getSchemaData() (when pg_dump acquires
 *	AccessShareLock on every table it intends to dump). It isn't very large,
 *	but it can happen.
 *
 *	http://archives.postgresql.org/pgsql-bugs/2010-02/msg00187.php
 *
 * IDENTIFICATION
 *	  src/bin/pg_dump/pg_dump.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include <unistd.h>
#include <ctype.h>
#ifdef HAVE_TERMIOS_H
#include <termios.h>
#endif

#include "getopt_long.h"
#include "mb/pg_wchar.h"
#include "access/attnum.h"
#include "access/sysattr.h"
#include "access/transam.h"
#include "access/htup_details.h"
#include "catalog/pg_am.h"
#include "catalog/pg_attribute.h"
#include "catalog/pg_cast.h"
#include "catalog/pg_class.h"
#include "catalog/pg_default_acl.h"
#include "catalog/pg_largeobject.h"
#include "catalog/pg_largeobject_metadata.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_trigger.h"
#include "catalog/pg_type.h"
#include "catalog/pg_authid.h"
#include "catalog/pg_extension.h"
#include "utils/memutils.h"
#include "miscadmin.h"
#include "lib/stringinfo.h"
#include "dbms_md_string_utils.h"
#include "dbms_md_dumputils.h"
#include "dbms_md_backup_db.h"
#include "dbms_md_dump.h"
#include "dbms_md_backup_archiver.h"

typedef struct
{
	const char *descr;			/* comment for an object */
	Oid			classoid;		/* object class (catalog OID) */
	Oid			objoid;			/* object OID */
	int			objsubid;		/* subobject (table column #) */
} CommentItem;

typedef struct
{
	const char *provider;		/* label provider of this security label */
	const char *label;			/* security label for an object */
	Oid			classoid;		/* object class (catalog OID) */
	Oid			objoid;			/* object OID */
	int			objsubid;		/* subobject (table column #) */
} SecLabelItem;

typedef enum OidOptions
{
	zeroAsOpaque = 1,
	zeroAsAny = 2,
	zeroAsStar = 4,
	zeroAsNone = 8
} OidOptions;

static const int SPI_RES_FIRST_FIELD_NO = 1;

/* subquery used to convert user ID (eg, datdba) to user name */
static const char *username_subquery;

/*
 * For 8.0 and earlier servers, pulled from pg_database, for 8.1+ we use
 * FirstNormalObjectId - 1.
 */
static Oid	g_last_builtin_oid; /* value of the last builtin oid */

/* name for the opaque type */
char		g_opaque_type[10];	

/* placeholders for the delimiters for comments */
char		g_comment_start[10];
char		g_comment_end[10];

static const CatalogId nilCatalogId = {0, 0};

/* sorted table of comments */
static CommentItem *comments = NULL;
static int ncomments;

static void init_dump_gobal_var(DumpOptions *dopt);
static void clean_dump_gobal_var(DumpOptions *dopt);
static void setup_connection(Archive *AH,
				 const char *dumpencoding, const char *dumpsnapshot,
				 char *use_role);
static ArchiveFormat parseArchiveFormat(const char *format, DBMS_MD_ArchiveMode *mode);
 static void
expand_object_name_patterns(Archive *fout,
									   DBMS_MD_DUMPOBJ_BI obi,
						   			   DbmsMdStringList *patterns, 
						   			   DbmsMdOidList *oids);
static void expand_schema_name_patterns(Archive *fout,
							DbmsMdStringList *patterns,
							DbmsMdOidList *oids);
static void expand_table_name_patterns(Archive *fout,
						   DbmsMdStringList *patterns,
						   DbmsMdOidList *oids);
static void
expand_function_name_patterns(Archive *fout,
						   			   DbmsMdStringList *patterns, 
						   			   DbmsMdOidList *oids);
static void
expand_index_name_patterns(Archive *fout,
						   			   DbmsMdStringList *patterns, 
						   			   DbmsMdOidList *oids);
static void
expand_type_name_patterns(Archive *fout,
						   			   DbmsMdStringList *patterns, 
						   			   DbmsMdOidList *oids);

static NamespaceInfo *findNamespace(Archive *fout, Oid nsoid);
//static void dumpTableData(Archive *fout, TableDataInfo *tdinfo);
//static void refreshMatViewData(Archive *fout, TableDataInfo *tdinfo);
static void guessConstraintInheritance(TableInfo *tblinfo, int numTables);
static void dumpComment(Archive *fout, const char *target,
			const char *namespace, const char *owner,
			CatalogId catalogId, int subid, DumpId dumpId);
static int findComments(Archive *fout, Oid classoid, Oid objoid,
			 CommentItem **items);
static void	collectComments(Archive *fout);
static void dumpSecLabel(Archive *fout, const char *target,
			 const char *namespace, const char *owner,
			 CatalogId catalogId, int subid, DumpId dumpId);
static int findSecLabels(Archive *fout, Oid classoid, Oid objoid,
			  SecLabelItem **items);
static int	collectSecLabels(Archive *fout, SecLabelItem **items);
static void dumpDumpableObject(Archive *fout, DumpableObject *dobj);
static void dumpNamespace(Archive *fout, NamespaceInfo *nspinfo);
static void dumpExtension(Archive *fout, ExtensionInfo *extinfo);
static void dumpType(Archive *fout, TypeInfo *tyinfo);
static void dumpBaseType(Archive *fout, TypeInfo *tyinfo);
static void dumpEnumType(Archive *fout, TypeInfo *tyinfo);
static void dumpRangeType(Archive *fout, TypeInfo *tyinfo);
static void dumpUndefinedType(Archive *fout, TypeInfo *tyinfo);
static void dumpDomain(Archive *fout, TypeInfo *tyinfo);
static void dumpCompositeType(Archive *fout, TypeInfo *tyinfo);
static void dumpCompositeTypeColComments(Archive *fout, TypeInfo *tyinfo);
static void dumpShellType(Archive *fout, ShellTypeInfo *stinfo);
static void dumpProcLang(Archive *fout, ProcLangInfo *plang);
static void dumpFunc(Archive *fout, FuncInfo *finfo);
static void dumpCast(Archive *fout, CastInfo *cast);
static void dumpTransform(Archive *fout, TransformInfo *transform);
static void dumpOpr(Archive *fout, OprInfo *oprinfo);
static void dumpAccessMethod(Archive *fout, AccessMethodInfo *oprinfo);
static void dumpOpclass(Archive *fout, OpclassInfo *opcinfo);
static void dumpOpfamily(Archive *fout, OpfamilyInfo *opfinfo);
static void dumpCollation(Archive *fout, CollInfo *collinfo);
static void dumpConversion(Archive *fout, ConvInfo *convinfo);
static void dumpRule(Archive *fout, RuleInfo *rinfo);
static void dumpAgg(Archive *fout, AggInfo *agginfo);
static void dumpTrigger(Archive *fout, TriggerInfo *tginfo);
static void dumpEventTrigger(Archive *fout, EventTriggerInfo *evtinfo);
static void dumpTable(Archive *fout, TableInfo *tbinfo);
static void dumpTableSchema(Archive *fout, TableInfo *tbinfo);
static void dumpAttrDef(Archive *fout, AttrDefInfo *adinfo);
static void dumpSequence(Archive *fout, TableInfo *tbinfo);
static void dumpSequenceData(Archive *fout, TableDataInfo *tdinfo);
static void dumpIndex(Archive *fout, IndxInfo *indxinfo);
static void dumpIndexAttach(Archive *fout, IndexAttachInfo *attachinfo);
static void dumpStatisticsExt(Archive *fout, StatsExtInfo *statsextinfo);
static void dumpConstraint(Archive *fout, ConstraintInfo *coninfo);
static void dumpTableConstraintComment(Archive *fout, ConstraintInfo *coninfo);
static void dumpTSParser(Archive *fout, TSParserInfo *prsinfo);
static void dumpTSDictionary(Archive *fout, TSDictInfo *dictinfo);
static void dumpTSTemplate(Archive *fout, TSTemplateInfo *tmplinfo);
static void dumpTSConfig(Archive *fout, TSConfigInfo *cfginfo);
static void dumpForeignDataWrapper(Archive *fout, FdwInfo *fdwinfo);
static void dumpForeignServer(Archive *fout, ForeignServerInfo *srvinfo);
static void dumpUserMappings(Archive *fout,
				 const char *servername, const char *namespace,
				 const char *owner, CatalogId catalogId, DumpId dumpId);
static void dumpDefaultACL(Archive *fout, DefaultACLInfo *daclinfo);

static void dumpACL(Archive *fout, CatalogId objCatId, DumpId objDumpId,
		const char *type, const char *name, const char *subname,
		const char *tag, const char *nspname, const char *owner,
		const char *acls, const char *racls,
		const char *initacls, const char *initracls);

static void getDependencies(Archive *fout);
static void buildArchiveDependencies(Archive *fout);
static void findDumpableDependencies(ArchiveHandle *AH, DumpableObject *dobj,
						 DumpId **dependencies, int *nDeps, int *allocDeps);

static DumpableObject *createBoundaryObjects(Archive *fout);
static void addBoundaryDependencies(Archive *fout, DumpableObject **dobjs, int numObjs,
						DumpableObject *boundaryObjs);

static void getDomainConstraints(Archive *fout, TypeInfo *tyinfo);
static void getTableData(Archive *fout, TableInfo *tblinfo, int numTables, bool oids, char relkind);
static void makeTableDataInfo(Archive *fout, TableInfo *tbinfo, bool oids);
static void buildMatViewRefreshDependencies(Archive *fout);
static void getTableDataFKConstraints(Archive *fout);
static char *format_function_arguments(FuncInfo *finfo, char *funcargs,
						  bool is_agg);
static char *format_function_arguments_old(Archive *fout,
							  FuncInfo *finfo, int nallargs,
							  char **allargtypes,
							  char **argmodes,
							  char **argnames);
static char *format_function_signature(Archive *fout,
						  FuncInfo *finfo, bool honor_quotes);
static char *convertRegProcReference(Archive *fout,
						const char *proc);
static char *convertOperatorReference(Archive *fout, const char *opr);
static char *convertTSFunction(Archive *fout, Oid funcOid);
//static Oid	findLastBuiltinOid_V71(Archive *fout, const char *);
static void selectSourceSchema(Archive *fout, const char *schemaName);
static char *getFormattedTypeName(Archive *fout, Oid oid, OidOptions opts);
//static void getBlobs(Archive *fout);
//static void dumpBlob(Archive *fout, BlobInfo *binfo);
//static int	dumpBlobs(Archive *fout, void *arg);
static void dumpPolicy(Archive *fout, PolicyInfo *polinfo);
static void dumpPublication(Archive *fout, PublicationInfo *pubinfo);
static void dumpPublicationTable(Archive *fout, PublicationRelInfo *pubrinfo);
static void dumpSubscription(Archive *fout, SubscriptionInfo *subinfo);
//static void dumpDatabase(Archive *AH);
//static void dumpEncoding(Archive *AH);
//static void dumpStdStrings(Archive *AH);
static const char *getAttrName(int attrnum, TableInfo *tblInfo);
//static const char *fmtCopyColumnList(const TableInfo *ti, StringInfo buffer);
static bool nonemptyReloptions(const char *reloptions);
static void appendReloptionsArrayAH(StringInfo buffer, const char *reloptions,
						const char *prefix, Archive *fout);
static char *get_synchronized_snapshot(Archive *fout);
static void setupDumpWorker(Archive *AHX);
static const char* dbms_md_get_current_schema(Archive *fout);

void
dump_main(int dobj_type, const char *schema_name, const char *object_name, StringInfo out_buf)
{
	StringInfo fmt_id_buffer = NULL;
	const char *filename = NULL;
	const char* cur_schema = NULL;
	const char *format = "p";
	TableInfo  *tblinfo;
	int			numTables;
	DumpableObject **dobjs;
	int			numObjs;
	DumpableObject *boundaryObjs;
	int			i;
	Archive    *fout;			/* the script file */
	const char *dumpencoding = NULL;
	const char *dumpsnapshot = NULL;
	char	   *use_role = NULL;
	Oid 		user_oid = InvalidOid;
	int			numWorkers = 1;
	trivalue	prompt_password = TRI_DEFAULT;
	int			compressLevel = -1;
	int			plainText = 0;
	ArchiveFormat archiveFormat = archUnknown;
	DBMS_MD_ArchiveMode archiveMode;
	MemoryContext current_mem_ctx = NULL;
	DBMS_MD_DUMP_GVAR_ST *dump_gvar = NULL;
	RestoreOptions *ropt = NULL;
	DumpOptions dopt;
	
	if (!dbms_md_check_dump_obj_valid(dobj_type))
		elog(ERROR, "Invalid object type");

	/* name for the opaque type */
	memset(&g_opaque_type[0], 0, sizeof(g_opaque_type));

	/* placeholders for the delimiters for comments */
	memset(&g_comment_start[0], 0, sizeof(g_comment_start));
	memset(&g_comment_end[0], 0, sizeof(g_comment_end));
	
	current_mem_ctx = CurrentMemoryContext;
	
	InitDumpOptions(&dopt);
	init_dump_gobal_var(&dopt);
	dopt.dump_obj_bm = (1<<dobj_type);
	dopt.column_inserts = 1;	
	dopt.column_inserts = 1;
	dopt.disable_dollar_quoting = 1;
	dopt.disable_triggers = 1;
	dopt.enable_row_security = 1;
	dopt.if_exists = 1;
	dopt.dump_inserts = 1;
	dopt.outputNoTablespaces = 1;
	dopt.serializable_deferrable = 1;
	dopt.use_setsessauth = 1;
	dopt.no_publications = 1;
	dopt.no_security_labels = 1;
	dopt.no_synchronized_snapshots = 1;
	dopt.no_unlogged_table_data = 1;
	dopt.no_subscriptions = 1;
	dopt.binary_upgrade = 0;

	/* Dump data only */
	dopt.dataOnly = false;

	/* Dump blobs */
	dopt.outputBlobs = false;

	/* Don't dump blobs */
	dopt.dontOutputBlobs = true;

	dump_gvar = &(dopt.dump_gvar);

	fmt_id_buffer = createStringInfo();
	SetLocalStringBuffer(fmt_id_buffer);

	/* include schema(s) */
	if (NULL != schema_name && strlen(schema_name) > 0)
	{
		dbms_md_string_list_append(current_mem_ctx, 
									&(dump_gvar->schema_include_patterns), 
									schema_name);
		dopt.include_everything = false;
	}

	/* dump schema only */
	dopt.schemaOnly = true;

	/* include table(s) */	
	if (NULL != object_name && strlen(object_name) > 0)
	{
		dbms_md_string_list_append(current_mem_ctx, 
									&(dump_gvar->table_include_patterns), 
									object_name);
		dopt.include_everything = false;
	}

	/* skip ACL dump */
	dopt.aclsSkip = true;
	
#ifdef _SHARDING_
	if(dump_gvar->dump_shards.head != NULL)
	{
		DbmsMdOidListCell *cell = dump_gvar->dump_shards.head;
		StringInfoData shardbuf; 
		bool isfirst = true;
		initStringInfo(&shardbuf);
		appendStringInfoString(&shardbuf, " (");
		while(cell)
		{
			if(isfirst)
				isfirst = false;
			else
				appendStringInfoString(&shardbuf,", ");

			appendStringInfo(&shardbuf, "%d", cell->val);
			cell = cell->next;
		}

		appendStringInfo(&shardbuf, ")");
		dump_gvar->shardstring = shardbuf.data;
	}
#endif

	/* Identify archive format to emit */
	archiveFormat = parseArchiveFormat(format, &archiveMode);

	/* archiveFormat specific setup */
	if (archiveFormat == archString)
		plainText = 1;

	/* Custom and directory formats are compressed by default, others not */
	if (compressLevel == -1)
	{
#ifdef HAVE_LIBZ
		if (archiveFormat == archCustom || archiveFormat == archDirectory)
			compressLevel = Z_DEFAULT_COMPRESSION;
		else
#endif
			compressLevel = 0;
	}

#ifndef HAVE_LIBZ
	if (compressLevel != 0)
		write_msg(NULL, "WARNING: requested compression not available in this "
				  "installation -- archive will be uncompressed\n");
	compressLevel = 0;
#endif
	
	/* Open the output file */
	fout = CreateArchive(filename, archiveFormat, compressLevel, archiveMode, 
						setupDumpWorker, current_mem_ctx);

	fout->memory_ctx = current_mem_ctx;
	fout->dump_out_buf = out_buf;

	/* Make dump options accessible right away */
	SetArchiveOptions(fout, &dopt, NULL);

	/*
	 * We allow the server to be back to 8.0, and up to any minor release of
	 * our own major version.  (See also version check in pg_dumpall.c.)
	 */
	fout->minRemoteVersion = 80000;
	fout->maxRemoteVersion = (PG_VERSION_NUM / 100) * 100 + 99;
	fout->remoteVersion = PG_VERSION_NUM;
	fout->remoteVersionStr = dbms_md_fmt_version_number(fout->memory_ctx, 
													       fout->remoteVersion, 
													       true);
	fout->encoding = pg_get_client_encoding();
	user_oid = GetUserId();
	fout->use_role = GetUserNameFromId(user_oid, false);	
	fout->numWorkers = numWorkers;
	
	/*
	 * Open the database using the Archiver, so it knows about it. Errors mean
	 * death.
	 */
	ConnectDatabase(fout, dopt.dbname, dopt.pghost, dopt.pgport, dopt.username, prompt_password);
	setup_connection(fout, dumpencoding, dumpsnapshot, use_role);

	cur_schema = dbms_md_get_current_schema(fout);
	if (NULL == schema_name || strlen(schema_name) < 1)
	{
		dbms_md_string_list_append(current_mem_ctx, 
			 					    &(dump_gvar->schema_include_patterns), 
			 					    cur_schema);
		dopt.include_everything = false;
	}

	/*
	 * Disable security label support if server version < v9.1.x (prevents
	 * access to nonexistent pg_seclabel catalog)
	 */
	if (fout->remoteVersion < 90100)
		dopt.no_security_labels = 1;

	/*
	 * On hot standbys, never try to dump unlogged table data, since it will
	 * just throw an error.
	 */
	if (fout->isStandby)
		dopt.no_unlogged_table_data = true;

	/* Select the appropriate subquery to convert user IDs to names */
	if (fout->remoteVersion >= 80100)
		username_subquery = "SELECT rolname FROM pg_catalog.pg_roles WHERE oid =";
	else
		username_subquery = "SELECT usename FROM pg_catalog.pg_user WHERE usesysid =";

	/* check the version for the synchronized snapshots feature */
	if (numWorkers > 1 && fout->remoteVersion < 90200
		&& !dopt.no_synchronized_snapshots)
		elog(ERROR,	"Synchronized snapshots are not supported by this server version.\n"
					"Run with --no-synchronized-snapshots instead if you do not need\n"
					"synchronized snapshots.\n");

	/* check the version when a snapshot is explicitly specified by user */
	if (dumpsnapshot && fout->remoteVersion < 90200)
		elog(ERROR, "Exported snapshots are not supported by this server version.\n");

	/* 
	 * With 8.1 and above, we can just use FirstNormalObjectId - 1.
	 */
	g_last_builtin_oid = FirstNormalObjectId - 1;

	/* Expand schema selection patterns into OID lists */
	if (dump_gvar->schema_include_patterns.head != NULL)
	{
		expand_schema_name_patterns(fout, &(dump_gvar->schema_include_patterns),
									&(dump_gvar->schema_include_oids));
		if (dump_gvar->schema_include_oids.head == NULL)
			elog(ERROR, "no matching schemas were found\n");

		if (NULL != schema_name && strlen(schema_name) > 0 
			&& 0 != pg_strcasecmp(cur_schema, schema_name))
			selectSourceSchema(fout, schema_name);
	}

	/* Expand table selection patterns into OID lists */
	if (dump_gvar->table_include_patterns.head != NULL)
	{
		expand_object_name_patterns(fout, dobj_type, &(dump_gvar->table_include_patterns),
								   &(dump_gvar->table_include_oids));
		if (dump_gvar->table_include_oids.head == NULL)
			elog(ERROR, "no matching object '%s' were found\n", object_name);
	}	

	/*
	 * Dumping blobs is the default for dumps where an inclusion switch is not
	 * used (an "include everything" dump).  -B can be used to exclude blobs
	 * from those dumps.  -b can be used to include blobs even when an
	 * inclusion switch is used.
	 *
	 * -s means "schema only" and blobs are data, not schema, so we never
	 * include blobs when -s is used.
	 */
	if (dopt.include_everything && !dopt.schemaOnly && !dopt.dontOutputBlobs)
		dopt.outputBlobs = true;

	/*
	 * Now scan the database and create DumpableObject structs for all the
	 * objects we intend to dump.
	 */
	tblinfo = dbms_md_get_schema_data(fout, &numTables);

	if (fout->remoteVersion < 80400)
		guessConstraintInheritance(tblinfo, numTables);

	if (!dopt.schemaOnly)
	{
		getTableData(fout, tblinfo, numTables, dopt.oids, 0);
		buildMatViewRefreshDependencies(fout);
		if (dopt.dataOnly)
			getTableDataFKConstraints(fout);
	}

	if (dopt.schemaOnly && dopt.sequence_data)
		getTableData(fout, tblinfo, numTables, dopt.oids, RELKIND_SEQUENCE);

#if 0
	/*
	 * In binary-upgrade mode, we do not have to worry about the actual blob
	 * data or the associated metadata that resides in the pg_largeobject and
	 * pg_largeobject_metadata tables, respectivly.
	 *
	 * However, we do need to collect blob information as there may be
	 * comments or other information on blobs that we do need to dump out.
	 */
	if (dopt.outputBlobs || dopt.binary_upgrade)
		getBlobs(fout);
#endif

	/*
	 * Collect dependency data to assist in ordering the objects.
	 */
	getDependencies(fout);

	/*
	 * Collect comments.
	 */
	collectComments(fout);

	/* Lastly, create dummy objects to represent the section boundaries */
	boundaryObjs = createBoundaryObjects(fout);

	/* Get pointers to all the known DumpableObjects */
	getDumpableObjects(fout, &dobjs, &numObjs);

	/*
	 * Add dummy dependencies to enforce the dump section ordering.
	 */
	addBoundaryDependencies(fout, dobjs, numObjs, boundaryObjs);

	/*
	 * Sort the objects into a safe dump order (no forward references).
	 *
	 * We rely on dependency information to help us determine a safe order, so
	 * the initial sort is mostly for cosmetic purposes: we sort by name to
	 * ensure that logically identical schemas will dump identically.
	 */
	sortDumpableObjectsByTypeName(fout, dobjs, numObjs);

	/* If we do a parallel dump, we want the largest tables to go first */
	if (archiveFormat == archDirectory && numWorkers > 1)
		sortDataAndIndexObjectsBySize(dobjs, numObjs);

	sortDumpableObjects(fout, dobjs, numObjs, boundaryObjs[0].dumpId, boundaryObjs[1].dumpId);

	/*
	 * Create archive TOC entries for all the objects to be dumped, in a safe
	 * order.
	 */

	#if 0
	/* First the special ENCODING and STDSTRINGS entries. */
	dumpEncoding(fout);
	dumpStdStrings(fout);

	/* The database item is always next, unless we don't want it at all */
	if (dopt.include_everything && !dopt.dataOnly)
		dumpDatabase(fout);
	#endif
	
	elog(DEBUG3, "dump object num=%d", numObjs);
	
	/* Now the rearrangeable objects. */
	for (i = 0; i < numObjs; i++)
		dumpDumpableObject(fout, dobjs[i]);

	/*	 * Set up options info to ensure we dump what we want.	 */	
	ropt = NewRestoreOptions();	
	ropt->filename = filename;	

	/* if you change this list, see dumpOptionsFromRestoreOptions */	
	ropt->dropSchema = dopt.outputClean;	
	ropt->dataOnly = dopt.dataOnly;
	ropt->schemaOnly = dopt.schemaOnly;
	ropt->if_exists = dopt.if_exists;
	ropt->column_inserts = dopt.column_inserts;
	ropt->dumpSections = dopt.dumpSections;
	ropt->aclsSkip = dopt.aclsSkip;
	ropt->superuser = dopt.outputSuperuser;
	ropt->createDB = dopt.outputCreateDB;
	ropt->noOwner = dopt.outputNoOwner;
	ropt->noTablespace = dopt.outputNoTablespaces;
	ropt->disable_triggers = dopt.disable_triggers;
	ropt->use_setsessauth = dopt.use_setsessauth;
	ropt->disable_dollar_quoting = dopt.disable_dollar_quoting;
	ropt->dump_inserts = dopt.dump_inserts;
	ropt->no_publications = dopt.no_publications;
	ropt->no_security_labels = dopt.no_security_labels;	
	ropt->no_subscriptions = dopt.no_subscriptions;
	ropt->lockWaitTimeout = dopt.lockWaitTimeout;
	ropt->include_everything = dopt.include_everything;
	ropt->enable_row_security = dopt.enable_row_security;
	ropt->sequence_data = dopt.sequence_data;

	if (compressLevel == -1)
		ropt->compression = 0;
	else
		ropt->compression = compressLevel;
	ropt->suppressDumpWarnings = true;	

	/* We've already shown them */
	SetArchiveOptions(fout, &dopt, ropt);

	/* Mark which entries should be output */
	ProcessArchiveRestoreOptions(fout);

	/*
	 * The archive's TOC entries are now marked as to which ones will actually
	 * be output, so we can set up their dependency lists properly. This isn't
	 * necessary for plain-text output, though.
	 */
	if (!plainText)
		buildArchiveDependencies(fout);

	/*
	 * And finally we can do the actual output.
	 *
	 * Note: for non-plain-text output formats, the output file is written
	 * inside CloseArchive().  This is, um, bizarre; but not worth changing
	 * right now.
	 */
	if (plainText)
		RestoreArchive(fout);

	selectSourceSchema(fout, cur_schema);
	SetLocalStringBuffer(NULL);
	destroyStringInfo(fmt_id_buffer);
	CloseArchive(fout);

	clean_dump_gobal_var(&dopt);
}

static void
setup_connection(Archive *AH, const char *dumpencoding,
				 const char *dumpsnapshot, char *use_role)
{
	DumpOptions *dopt = AH->dopt;
#if 0

	PGconn	   *conn = GetConnection(AH);
	const char *std_strings;

	/*
	 * Set the client encoding if requested.
	 */
	if (dumpencoding)
	{
		if (PQsetClientEncoding(conn, dumpencoding) < 0)
			elog(ERROR, "invalid client encoding \"%s\" specified\n",
						  dumpencoding);
	}

	/*
	 * Get the active encoding and the standard_conforming_strings setting, so
	 * we know how to escape strings.
	 */
	AH->encoding = PQclientEncoding(conn);

	std_strings = PQparameterStatus(conn, "standard_conforming_strings");
	AH->std_strings = (std_strings && strcmp(std_strings, "on") == 0);
#endif
	/*
	 * Set the role if requested.  In a parallel dump worker, we'll be passed
	 * use_role == NULL, but AH->use_role is already set (if user specified it
	 * originally) and we should use that.
	 */
	if (!use_role && AH->use_role)
		use_role = AH->use_role;

	/* Set the role if requested */
	if (use_role && AH->remoteVersion >= 80100)
	{
		StringInfoData query;
		initStringInfo(&query);

		appendStringInfo(&query, "SET ROLE %s", dbms_md_fmtId(use_role));
		ExecuteSqlStatement(AH, query.data);
		destroyStringInfo(&query);

		/* save it for possible later use by parallel workers */
		if (!AH->use_role)
			AH->use_role = dbms_md_strdup(use_role);
	}

	/* Set the datestyle to ISO to ensure the dump's portability */
	ExecuteSqlStatement(AH, "SET DATESTYLE = ISO");

	/* Likewise, avoid using sql_standard intervalstyle */
	if (AH->remoteVersion >= 80400)
		ExecuteSqlStatement(AH, "SET INTERVALSTYLE = POSTGRES");

	/*
	 * Set extra_float_digits so that we can dump float data exactly (given
	 * correctly implemented float I/O code, anyway)
	 */
	if (AH->remoteVersion >= 90000)
		ExecuteSqlStatement(AH, "SET extra_float_digits TO 3");
	else
		ExecuteSqlStatement(AH, "SET extra_float_digits TO 2");

	/*
	 * If synchronized scanning is supported, disable it, to prevent
	 * unpredictable changes in row ordering across a dump and reload.
	 */
	if (AH->remoteVersion >= 80300)
		ExecuteSqlStatement(AH, "SET synchronize_seqscans TO off");

	/*
	 * Disable timeouts if supported.
	 */
	ExecuteSqlStatement(AH, "SET statement_timeout = 0");
	if (AH->remoteVersion >= 90300)
		ExecuteSqlStatement(AH, "SET lock_timeout = 0");
	if (AH->remoteVersion >= 90600)
		ExecuteSqlStatement(AH, "SET idle_in_transaction_session_timeout = 0");

	/*
	 * Quote all identifiers, if requested.
	 */
	/*if (quote_all_identifiers && AH->remoteVersion >= 90100)
		ExecuteSqlStatement(AH, "SET quote_all_identifiers = true");*/

	/*
	 * Adjust row-security mode, if supported.
	 */
	if (AH->remoteVersion >= 90500)
	{
		if (dopt->enable_row_security)
			ExecuteSqlStatement(AH, "SET row_security = on");
		else
			ExecuteSqlStatement(AH, "SET row_security = off");
	}

	/*
	 * Start transaction-snapshot mode transaction to dump consistent data.
	 */
	ExecuteSqlStatement(AH, "BEGIN");
	if (AH->remoteVersion >= 90100)
	{
		/*
		 * To support the combination of serializable_deferrable with the jobs
		 * option we use REPEATABLE READ for the worker connections that are
		 * passed a snapshot.  As long as the snapshot is acquired in a
		 * SERIALIZABLE, READ ONLY, DEFERRABLE transaction, its use within a
		 * REPEATABLE READ transaction provides the appropriate integrity
		 * guarantees.  This is a kluge, but safe for back-patching.
		 */
		if (dopt->serializable_deferrable && AH->sync_snapshot_id == NULL)
			ExecuteSqlStatement(AH,
								"SET TRANSACTION ISOLATION LEVEL "
								"SERIALIZABLE, "
#ifndef XCP
								"READ ONLY, "
#endif
								"DEFERRABLE");
		else
			ExecuteSqlStatement(AH,
								"SET TRANSACTION ISOLATION LEVEL "
								"REPEATABLE READ"
#ifndef XCP
								", READ ONLY"
#endif
								);
	}
	else
	{
		ExecuteSqlStatement(AH,
							"SET TRANSACTION ISOLATION LEVEL "
							"SERIALIZABLE, READ ONLY");
	}

	/*
	 * If user specified a snapshot to use, select that.  In a parallel dump
	 * worker, we'll be passed dumpsnapshot == NULL, but AH->sync_snapshot_id
	 * is already set (if the server can handle it) and we should use that.
	 */
	if (dumpsnapshot)
		AH->sync_snapshot_id = dbms_md_strdup(dumpsnapshot);

	if (AH->sync_snapshot_id)
	{
		StringInfoData query;
		initStringInfo(&query);

		appendStringInfoString(&query, "SET TRANSACTION SNAPSHOT ");
		//appendStringLiteralConn(query, AH->sync_snapshot_id, conn);
		ExecuteSqlStatement(AH, query.data);
		destroyStringInfo(&query);
	}
	else if (AH->numWorkers > 1 &&
			 AH->remoteVersion >= 90200 &&
			 !dopt->no_synchronized_snapshots)
	{
		if (AH->isStandby)
		{	
			elog(ERROR,  "Synchronized snapshots are not supported on standby servers.\n"
						  "Run with --no-synchronized-snapshots instead if you do not need\n"
						  "synchronized snapshots.\n");
			return ;
		}


		AH->sync_snapshot_id = get_synchronized_snapshot(AH);
	}
}

/* Set up connection for a parallel worker process */
static void
setupDumpWorker(Archive *AH)
{
	/*
	 * We want to re-select all the same values the master connection is
	 * using.  We'll have inherited directly-usable values in
	 * AH->sync_snapshot_id and AH->use_role, but we need to translate the
	 * inherited encoding value back to a string to pass to setup_connection.
	 */
	setup_connection(AH,
					 pg_encoding_to_char(AH->encoding),
					 NULL,
					 NULL);
}

static char *
get_synchronized_snapshot(Archive *fout)
{
	char	   *query = "SELECT pg_catalog.pg_export_snapshot()";
	char	   *result;
	SPITupleTable   *res;

	res = ExecuteSqlQueryForSingleRow(fout, query);
	result = dbms_md_strdup(dbms_md_get_field_value(res, 0, SPI_RES_FIRST_FIELD_NO));
	dbms_md_free_tuples(res);

	return result;
}

static ArchiveFormat
parseArchiveFormat(const char *format, DBMS_MD_ArchiveMode *mode)
{
	ArchiveFormat archiveFormat;

	*mode = archModeWrite;

	if (pg_strcasecmp(format, "a") == 0 || pg_strcasecmp(format, "append") == 0)
	{
		/* This is used by pg_dumpall, and is not documented */
		archiveFormat = archString;
		*mode = archModeAppend;
	}
	else if (pg_strcasecmp(format, "c") == 0)
		archiveFormat = archCustom;
	else if (pg_strcasecmp(format, "custom") == 0)
		archiveFormat = archCustom;
	else if (pg_strcasecmp(format, "d") == 0)
		archiveFormat = archDirectory;
	else if (pg_strcasecmp(format, "directory") == 0)
		archiveFormat = archDirectory;
	else if (pg_strcasecmp(format, "p") == 0)
		archiveFormat = archString;
	else if (pg_strcasecmp(format, "plain") == 0)
		archiveFormat = archString;
	else if (pg_strcasecmp(format, "t") == 0)
		archiveFormat = archTar;
	else if (pg_strcasecmp(format, "tar") == 0)
		archiveFormat = archTar;
	else
		elog(ERROR, "invalid output format \"%s\" specified\n", format);
	return archiveFormat;
}

/*
 * Find the OIDs of all schemas matching the given list of patterns,
 * and append them to the given OID list.
 */
static void
expand_schema_name_patterns(Archive *fout,
							DbmsMdStringList *patterns,
							DbmsMdOidList *oids)
{
	StringInfo query;
	SPITupleTable   *res;
	DbmsMdStringListCell *cell;
	int			i;

	if (patterns->head == NULL)
		return;					/* nothing to do */

	query = createStringInfo();

	/*
	 * The loop below runs multiple SELECTs might sometimes result in
	 * duplicate entries in the OID list, but we don't care.
	 */

	for (cell = patterns->head; cell; cell = cell->next)
	{
		appendStringInfo(query,
						  "SELECT oid FROM pg_catalog.pg_namespace n\n");
		dbms_process_sql_name_pattern(fout->encoding, 
									   fout->remoteVersion, 
									   fout->std_strings, 
									   query, 
									   cell->val, 
									   false,
									   false, 
									   NULL, 
									   "n.nspname", 
									   NULL, 
									   NULL);

		res = ExecuteSqlQuery(fout, query->data);
		if ( dbms_md_get_tuple_num(res) == 0)
			elog(ERROR, "no matching schemas were found for pattern \"%s\"\n", cell->val);

		for (i = 0; i < dbms_md_get_tuple_num(res); i++)
		{
			Oid ns_oid = dbms_md_str_to_oid(dbms_md_get_field_value(res, i, SPI_RES_FIRST_FIELD_NO));
			dbms_md_oid_list_append(fout->memory_ctx, oids, ns_oid);
		}

		dbms_md_free_tuples(res);
		resetStringInfo(query);
	}

	destroyStringInfo(query);
	pfree(query);
}

/*
 * Find the OIDs of all tables matching the given list of patterns,
 * and append them to the given OID list.
 */
 static void
expand_object_name_patterns(Archive *fout,
									   DBMS_MD_DUMPOBJ_BI obi,
						   			   DbmsMdStringList *patterns, 
						   			   DbmsMdOidList *oids)
{
	switch (obi)
	{
	case DBMS_MD_DOBJ_TABLE:
		expand_table_name_patterns(fout, patterns, oids);
		break;
	case DBMS_MD_DOBJ_INDEX:
		expand_index_name_patterns(fout, patterns, oids);
		break;
	case DBMS_MD_DOBJ_TYPE:
		expand_type_name_patterns(fout, patterns, oids);
		break;
	case DBMS_MD_DOBJ_FUNC:
		expand_function_name_patterns(fout, patterns, oids);
		break;
	default:
		elog(DEBUG3, "Invalid object type to filter");
	}
}

static void
expand_table_name_patterns(Archive *fout,
						   			   DbmsMdStringList *patterns, 
						   			   DbmsMdOidList *oids)
{
	StringInfo query;
	SPITupleTable   *res;
	DbmsMdStringListCell *cell;
	int			i;

	if (patterns->head == NULL)
		return;					/* nothing to do */

	query = createStringInfo();
	
	/*
	 * this might sometimes result in duplicate entries in the OID list, but
	 * we don't care.
	 */

	for (cell = patterns->head; cell; cell = cell->next)
	{
		appendStringInfo(query,
						  "SELECT c.oid"
						  " FROM pg_catalog.pg_class c"
						  "     LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace"
						  " WHERE c.relkind in ('%c', '%c', '%c', '%c', '%c', '%c') ",
						  RELKIND_RELATION, RELKIND_SEQUENCE, RELKIND_VIEW,
						  RELKIND_MATVIEW, RELKIND_FOREIGN_TABLE,
						  RELKIND_PARTITIONED_TABLE);
		dbms_process_sql_name_pattern(fout->encoding, 
									   fout->remoteVersion, 
									   fout->std_strings, 
									   query, 
									   cell->val, 
									   true,
									   false, 
									   "n.nspname", 
									   "c.relname", 
									   NULL,
									   "pg_catalog.pg_table_is_visible(c.oid)");

		res = ExecuteSqlQuery(fout, query->data);
		if (dbms_md_get_tuple_num(res) == 0)
		{
			elog(ERROR, "no matching tables were found for pattern \"%s\"\n", cell->val);
			dbms_md_free_tuples(res);
			destroyStringInfo(query);
			pfree(query);
			return ;
		}

		for (i = 0; i < dbms_md_get_tuple_num(res); i++)
		{
			Oid toid = dbms_md_str_to_oid(dbms_md_get_field_value(res, i, SPI_RES_FIRST_FIELD_NO));
			dbms_md_oid_list_append(fout->memory_ctx, oids, toid);
		}

		dbms_md_free_tuples(res);
		resetStringInfo(query);
	}

	destroyStringInfo(query);
	pfree(query);
}

static void
expand_type_name_patterns(Archive *fout,
						   			   DbmsMdStringList *patterns, 
						   			   DbmsMdOidList *oids)
{
	StringInfo query;
	SPITupleTable   *res;
	DbmsMdStringListCell *cell;
	int			i;

	if (patterns->head == NULL)
		return;					/* nothing to do */

	query = createStringInfo();
	
	/*
	 * this might sometimes result in duplicate entries in the OID list, but
	 * we don't care.
	 */
	for (cell = patterns->head; cell; cell = cell->next)
	{
		appendStringInfo(query,
						  "SELECT t.oid"
						  " FROM pg_catalog.pg_type t "
						  "     LEFT JOIN pg_catalog.pg_namespace n ON n.oid = t.typnamespace"
						  " WHERE t.typtype in ('%c', '%c', '%c', '%c', '%c') ",
						  TYPTYPE_COMPOSITE, TYPTYPE_DOMAIN, TYPTYPE_ENUM,
						  TYPTYPE_PSEUDO, TYPTYPE_RANGE);
		dbms_process_sql_name_pattern(fout->encoding, 
									   fout->remoteVersion, 
									   fout->std_strings, 
									   query, 
									   cell->val, 
									   true,
									   false, 
									   "n.nspname", 
									   "t.typname", 
									   NULL,
									   "pg_catalog.pg_type_is_visible(t.oid)");

		res = ExecuteSqlQuery(fout, query->data);
		if (dbms_md_get_tuple_num(res) == 0)
		{
			elog(ERROR, "no matching types were found for pattern \"%s\"\n", cell->val);
			dbms_md_free_tuples(res);
			destroyStringInfo(query);
			pfree(query);
			return ;
		}

		for (i = 0; i < dbms_md_get_tuple_num(res); i++)
		{
			Oid toid = dbms_md_str_to_oid(dbms_md_get_field_value(res, i, SPI_RES_FIRST_FIELD_NO));
			dbms_md_oid_list_append(fout->memory_ctx, oids, toid);
		}

		dbms_md_free_tuples(res);
		resetStringInfo(query);
	}

	destroyStringInfo(query);
	pfree(query);
}


static void
expand_index_name_patterns(Archive *fout,
						   			   DbmsMdStringList *patterns, 
						   			   DbmsMdOidList *oids)
{
	StringInfo query;
	SPITupleTable   *res;
	DbmsMdStringListCell *cell;
	int			i;

	if (patterns->head == NULL)
		return;					/* nothing to do */

	query = createStringInfo();
	
	/*
	 * this might sometimes result in duplicate entries in the OID list, but
	 * we don't care.
	 */

	for (cell = patterns->head; cell; cell = cell->next)
	{
		appendStringInfo(query,
						  "SELECT i.indexrelid"
						  " FROM pg_catalog.pg_index i, pg_catalog.pg_class c"						  
						  " WHERE c.relkind in ('%c', '%c', '%c', '%c', '%c', '%c') AND c.oid = i.indrelid ",
						  RELKIND_RELATION, RELKIND_SEQUENCE, RELKIND_VIEW,
						  RELKIND_MATVIEW, RELKIND_FOREIGN_TABLE,
						  RELKIND_PARTITIONED_TABLE);
		dbms_process_sql_name_pattern(fout->encoding, 
									   fout->remoteVersion, 
									   fout->std_strings, 
									   query, 
									   cell->val, 
									   true,
									   false, 
									   NULL, 
									   "c.relname", 
									   NULL,
									   NULL);

		res = ExecuteSqlQuery(fout, query->data);
		if (dbms_md_get_tuple_num(res) == 0)
		{
			elog(ERROR, "no matching indexs were found for pattern \"%s\"\n", cell->val);
			dbms_md_free_tuples(res);
			destroyStringInfo(query);
			pfree(query);
			return ;
		}

		for (i = 0; i < dbms_md_get_tuple_num(res); i++)
		{
			Oid toid = dbms_md_str_to_oid(dbms_md_get_field_value(res, i, SPI_RES_FIRST_FIELD_NO));
			dbms_md_oid_list_append(fout->memory_ctx, oids, toid);
		}

		dbms_md_free_tuples(res);
		resetStringInfo(query);
	}

	destroyStringInfo(query);
	pfree(query);
}


static void
expand_function_name_patterns(Archive *fout,
						   			   DbmsMdStringList *patterns, 
						   			   DbmsMdOidList *oids)
{
	StringInfo query;
	SPITupleTable   *res;
	DbmsMdStringListCell *cell;
	int			i;

	if (patterns->head == NULL)
		return;					/* nothing to do */

	query = createStringInfo();
	
	/*
	 * this might sometimes result in duplicate entries in the OID list, but
	 * we don't care.
	 */

	for (cell = patterns->head; cell; cell = cell->next)
	{
		appendStringInfo(query,
						  "SELECT p.oid"
						  " FROM pg_catalog.pg_proc p "
						  "     LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace"
						  " WHERE p.prokind in ('%c', '%c', '%c', '%c') ",
						  PROKIND_FUNCTION, PROKIND_AGGREGATE, PROKIND_WINDOW,
						  PROKIND_PROCEDURE);
		dbms_process_sql_name_pattern(fout->encoding, 
									   fout->remoteVersion, 
									   fout->std_strings, 
									   query, 
									   cell->val, 
									   true,
									   false, 
									   "n.nspname", 
									   "p.proname", 
									   NULL,
									   "pg_catalog.pg_function_is_visible(p.oid)");

		res = ExecuteSqlQuery(fout, query->data);
		if (dbms_md_get_tuple_num(res) == 0)
		{
			elog(ERROR, "no matching functions were found for pattern \"%s\"\n", cell->val);
			dbms_md_free_tuples(res);
			destroyStringInfo(query);
			pfree(query);
			return ;
		}

		for (i = 0; i < dbms_md_get_tuple_num(res); i++)
		{
			Oid toid = dbms_md_str_to_oid(dbms_md_get_field_value(res, i, SPI_RES_FIRST_FIELD_NO));
			dbms_md_oid_list_append(fout->memory_ctx, oids, toid);
		}

		dbms_md_free_tuples(res);
		resetStringInfo(query);
	}

	destroyStringInfo(query);
	pfree(query);
}


/*
 * checkExtensionMembership
 *		Determine whether object is an extension member, and if so,
 *		record an appropriate dependency and set the object's dump flag.
 *
 * It's important to call this for each object that could be an extension
 * member.  Generally, we integrate this with determining the object's
 * to-be-dumped-ness, since extension membership overrides other rules for that.
 *
 * Returns true if object is an extension member, else false.
 */
static bool
checkExtensionMembership(DumpableObject *dobj, Archive *fout)
{
	ExtensionInfo *ext = findOwningExtension(fout, dobj->catId);

	if (ext == NULL)
		return false;

	dobj->ext_member = true;

	/* Record dependency so that getDependencies needn't deal with that */
	addObjectDependency(fout, dobj, ext->dobj.dumpId);

	/*
	 * In 9.6 and above, mark the member object to have any non-initial ACL,
	 * policies, and security labels dumped.
	 *
	 * Note that any initial ACLs (see pg_init_privs) will be removed when we
	 * extract the information about the object.  We don't provide support for
	 * initial policies and security labels and it seems unlikely for those to
	 * ever exist, but we may have to revisit this later.
	 *
	 * Prior to 9.6, we do not include any extension member components.
	 *
	 * In binary upgrades, we still dump all components of the members
	 * individually, since the idea is to exactly reproduce the database
	 * contents rather than replace the extension contents with something
	 * different.
	 */
	if (fout->dopt->binary_upgrade)
		dobj->dump = ext->dobj.dump;
	else
	{
		if (fout->remoteVersion < 90600)
			dobj->dump = DUMP_COMPONENT_NONE;
		else
			dobj->dump = ext->dobj.dump_contains & (DUMP_COMPONENT_ACL |
													DUMP_COMPONENT_SECLABEL |
													DUMP_COMPONENT_POLICY);
	}

	return true;
}

/*
 * selectDumpableNamespace: policy-setting subroutine
 *		Mark a namespace as to be dumped or not
 */
static void
selectDumpableNamespace(NamespaceInfo *nsinfo, Archive *fout)
{
	DBMS_MD_DUMP_GVAR_ST *dump_gvar = &(fout->dopt->dump_gvar);
	
	/*
	 * If specific tables are being dumped, do not dump any complete
	 * namespaces. If specific namespaces are being dumped, dump just those
	 * namespaces. Otherwise, dump all non-system namespaces.
	 */
	if (dump_gvar->table_include_oids.head != NULL)
		nsinfo->dobj.dump_contains = nsinfo->dobj.dump = DUMP_COMPONENT_NONE;
	else if (dump_gvar->schema_include_oids.head != NULL)
		nsinfo->dobj.dump_contains = nsinfo->dobj.dump =
			dbms_md_oid_list_member(&(dump_gvar->schema_include_oids),
								   nsinfo->dobj.catId.oid) ?
			DUMP_COMPONENT_ALL : DUMP_COMPONENT_NONE;
	else if (fout->remoteVersion >= 90600 &&
			 strcmp(nsinfo->dobj.name, "pg_catalog") == 0)
	{
		/*
		 * In 9.6 and above, we dump out any ACLs defined in pg_catalog, if
		 * they are interesting (and not the original ACLs which were set at
		 * initdb time, see pg_init_privs).
		 */
		nsinfo->dobj.dump_contains = nsinfo->dobj.dump = DUMP_COMPONENT_ACL;
	}
	else if (strncmp(nsinfo->dobj.name, "pg_", 3) == 0 ||
#ifdef XCP
			 strncmp(nsinfo->dobj.name, "storm_", 6) == 0 ||
#endif
			 strcmp(nsinfo->dobj.name, "information_schema") == 0)
	{
		/* Other system schemas don't get dumped */
		nsinfo->dobj.dump_contains = nsinfo->dobj.dump = DUMP_COMPONENT_NONE;
	}
	else
	{
		if  (dbms_md_is_dump_this_obj(fout->dopt, DBMS_MD_DOBJ_BUTT))
			nsinfo->dobj.dump =nsinfo->dobj.dump_contains = DUMP_COMPONENT_ALL;
		else
			nsinfo->dobj.dump = nsinfo->dobj.dump_contains = DUMP_COMPONENT_NONE;
		/*nsinfo->dobj.dump_contains = nsinfo->dobj.dump = DUMP_COMPONENT_ALL;*/
	}

	/*
	 * In any case, a namespace can be excluded by an exclusion switch
	 */
	/*if (nsinfo->dobj.dump_contains &&
		dbms_md_oid_list_member(&schema_exclude_oids,
							   nsinfo->dobj.catId.oid))
		nsinfo->dobj.dump_contains = nsinfo->dobj.dump = DUMP_COMPONENT_NONE;*/

	/*
	 * If the schema belongs to an extension, allow extension membership to
	 * override the dump decision for the schema itself.  However, this does
	 * not change dump_contains, so this won't change what we do with objects
	 * within the schema.  (If they belong to the extension, they'll get
	 * suppressed by it, otherwise not.)
	 */
	(void) checkExtensionMembership(&nsinfo->dobj, fout);
}

/*
 * selectDumpableTable: policy-setting subroutine
 *		Mark a table as to be dumped or not
 */
static void
selectDumpableTable(TableInfo *tbinfo, Archive *fout)
{
	DBMS_MD_DUMP_GVAR_ST *dump_gvar = &(fout->dopt->dump_gvar);
	if (checkExtensionMembership(&tbinfo->dobj, fout))
		return;					/* extension membership overrides all else */

	if  (!dbms_md_is_dump_this_obj(fout->dopt, DBMS_MD_DOBJ_TABLE))
	{
		tbinfo->dobj.dump = tbinfo->dobj.dump_contains = DUMP_COMPONENT_NONE;
		return ;
	}

	/*
	 * If specific tables are being dumped, dump just those tables; else, dump
	 * according to the parent namespace's dump flag.
	 */	 
	if (dump_gvar->table_include_oids.head != NULL)
	{
		bool include_obj_oid = dbms_md_oid_list_member(&(dump_gvar->table_include_oids),
												   tbinfo->dobj.catId.oid);
		if  (include_obj_oid)
		{
			Oid ns_oid = tbinfo->dobj.namespace->dobj.catId.oid;
			bool is_matched_ns = true;
			if (NULL != dump_gvar->schema_include_oids.head)
				is_matched_ns = dbms_md_oid_list_member(&(dump_gvar->schema_include_oids), ns_oid);
			
			if (is_matched_ns)
				tbinfo->dobj.dump = DUMP_COMPONENT_ALL;
			else
				tbinfo->dobj.dump = DUMP_COMPONENT_NONE;
		}
		else
			tbinfo->dobj.dump = DUMP_COMPONENT_NONE;
	}
	else
		tbinfo->dobj.dump = tbinfo->dobj.namespace->dobj.dump_contains;

}

/*
 * selectDumpableType: policy-setting subroutine
 *		Mark a type as to be dumped or not
 *
 * If it's a table's rowtype or an autogenerated array type, we also apply a
 * special type code to facilitate sorting into the desired order.  (We don't
 * want to consider those to be ordinary types because that would bring tables
 * up into the datatype part of the dump order.)  We still set the object's
 * dump flag; that's not going to cause the dummy type to be dumped, but we
 * need it so that casts involving such types will be dumped correctly -- see
 * dumpCast.  This means the flag should be set the same as for the underlying
 * object (the table or base type).
 */
static void
selectDumpableType(TypeInfo *tyinfo, Archive *fout)
{
	DBMS_MD_DUMP_GVAR_ST *dump_gvar = &(fout->dopt->dump_gvar);
	
	if  (!dbms_md_is_dump_this_obj(fout->dopt, DBMS_MD_DOBJ_TYPE))
	{
		tyinfo->dobj.dump = tyinfo->dobj.dump_contains = DUMP_COMPONENT_NONE;
		return ;
	}
	
	/* skip complex types, except for standalone composite types */
	if (OidIsValid(tyinfo->typrelid) &&
		tyinfo->typrelkind != RELKIND_COMPOSITE_TYPE)
	{
		TableInfo  *tytable = findTableByOid(fout, tyinfo->typrelid);

		tyinfo->dobj.objType = DO_DUMMY_TYPE;
		if (tytable != NULL)
			tyinfo->dobj.dump = tytable->dobj.dump;
		else
			tyinfo->dobj.dump = DUMP_COMPONENT_NONE;
		return;
	}

	/* skip auto-generated array types */
	if (tyinfo->isArray)
	{
		tyinfo->dobj.objType = DO_DUMMY_TYPE;

		/*
		 * Fall through to set the dump flag; we assume that the subsequent
		 * rules will do the same thing as they would for the array's base
		 * type.  (We cannot reliably look up the base type here, since
		 * getTypes may not have processed it yet.)
		 */
	}

	if (checkExtensionMembership(&tyinfo->dobj, fout))
		return;					/* extension membership overrides all else */

	/* Dump based on if the contents of the namespace are being dumped */
	if (dump_gvar->table_include_oids.head != NULL)
	{
		bool include_obj_oid = dbms_md_oid_list_member(&(dump_gvar->table_include_oids),
												   tyinfo->dobj.catId.oid);
		if  (include_obj_oid)
		{
			Oid ns_oid = tyinfo->dobj.namespace->dobj.catId.oid;
			bool is_matched_ns = true;
			if (NULL != dump_gvar->schema_include_oids.head)
				is_matched_ns = dbms_md_oid_list_member(&(dump_gvar->schema_include_oids), ns_oid);
			
			if (is_matched_ns)
				tyinfo->dobj.dump = DUMP_COMPONENT_ALL;
			else
				tyinfo->dobj.dump = DUMP_COMPONENT_NONE;
		}
		else
			tyinfo->dobj.dump = DUMP_COMPONENT_NONE;
	}
	else
		tyinfo->dobj.dump = tyinfo->dobj.namespace->dobj.dump_contains;
}

/*
 * selectDumpableDefaultACL: policy-setting subroutine
 *		Mark a default ACL as to be dumped or not
 *
 * For per-schema default ACLs, dump if the schema is to be dumped.
 * Otherwise dump if we are dumping "everything".  Note that dataOnly
 * and aclsSkip are checked separately.
 */
static void
selectDumpableDefaultACL(DefaultACLInfo *dinfo, DumpOptions *dopt)
{
	if  (!dbms_md_is_dump_this_obj(dopt, DBMS_MD_DOBJ_BUTT))
	{
		dinfo->dobj.dump = dinfo->dobj.dump_contains = DUMP_COMPONENT_NONE;
		return ;
	}
	
	/* Default ACLs can't be extension members */

	if (dinfo->dobj.namespace)
		/* default ACLs are considered part of the namespace */
		dinfo->dobj.dump = dinfo->dobj.namespace->dobj.dump_contains;
	else
		dinfo->dobj.dump = dopt->include_everything ?
			DUMP_COMPONENT_ALL : DUMP_COMPONENT_NONE;
}

/*
 * selectDumpableCast: policy-setting subroutine
 *		Mark a cast as to be dumped or not
 *
 * Casts do not belong to any particular namespace (since they haven't got
 * names), nor do they have identifiable owners.  To distinguish user-defined
 * casts from built-in ones, we must resort to checking whether the cast's
 * OID is in the range reserved for initdb.
 */
static void
selectDumpableCast(CastInfo *cast, Archive *fout)
{
	if (checkExtensionMembership(&cast->dobj, fout))
		return;					/* extension membership overrides all else */

	/*
	 * This would be DUMP_COMPONENT_ACL for from-initdb casts, but they do not
	 * support ACLs currently.
	 */
	if (cast->dobj.catId.oid <= (Oid) g_last_builtin_oid)
		cast->dobj.dump = DUMP_COMPONENT_NONE;
	else
		cast->dobj.dump = fout->dopt->include_everything ?
			DUMP_COMPONENT_ALL : DUMP_COMPONENT_NONE;
}

/*
 * selectDumpableProcLang: policy-setting subroutine
 *		Mark a procedural language as to be dumped or not
 *
 * Procedural languages do not belong to any particular namespace.  To
 * identify built-in languages, we must resort to checking whether the
 * language's OID is in the range reserved for initdb.
 */
static void
selectDumpableProcLang(ProcLangInfo *plang, Archive *fout)
{
	if  (!dbms_md_is_dump_this_obj(fout->dopt, DBMS_MD_DOBJ_BUTT))
	{
		plang->dobj.dump = plang->dobj.dump_contains = DUMP_COMPONENT_NONE;
		return ;
	}
	
	if (checkExtensionMembership(&plang->dobj, fout))
		return;					/* extension membership overrides all else */

	/*
	 * Only include procedural languages when we are dumping everything.
	 *
	 * For from-initdb procedural languages, only include ACLs, as we do for
	 * the pg_catalog namespace.  We need this because procedural languages do
	 * not live in any namespace.
	 */
	if (!fout->dopt->include_everything)
		plang->dobj.dump = DUMP_COMPONENT_NONE;
	else
	{
		if (plang->dobj.catId.oid <= (Oid) g_last_builtin_oid)
			plang->dobj.dump = fout->remoteVersion < 90600 ?
				DUMP_COMPONENT_NONE : DUMP_COMPONENT_ACL;
		else
			plang->dobj.dump = DUMP_COMPONENT_ALL;
	}
}

/*
 * selectDumpableAccessMethod: policy-setting subroutine
 *		Mark an access method as to be dumped or not
 *
 * Access methods do not belong to any particular namespace.  To identify
 * built-in access methods, we must resort to checking whether the
 * method's OID is in the range reserved for initdb.
 */
static void
selectDumpableAccessMethod(AccessMethodInfo *method, Archive *fout)
{
	if  (!dbms_md_is_dump_this_obj(fout->dopt, DBMS_MD_DOBJ_BUTT))
	{
		method->dobj.dump = method->dobj.dump_contains = DUMP_COMPONENT_NONE;
		return ;
	}
	
	if (checkExtensionMembership(&method->dobj, fout))
		return;					/* extension membership overrides all else */

	/*
	 * This would be DUMP_COMPONENT_ACL for from-initdb access methods, but
	 * they do not support ACLs currently.
	 */
	if (method->dobj.catId.oid <= (Oid) g_last_builtin_oid)
		method->dobj.dump = DUMP_COMPONENT_NONE;
	else
		method->dobj.dump = fout->dopt->include_everything ?
			DUMP_COMPONENT_ALL : DUMP_COMPONENT_NONE;
}

/*
 * selectDumpableExtension: policy-setting subroutine
 *		Mark an extension as to be dumped or not
 *
 * Normally, we dump all extensions, or none of them if include_everything
 * is false (i.e., a --schema or --table switch was given).  However, in
 * binary-upgrade mode it's necessary to skip built-in extensions, since we
 * assume those will already be installed in the target database.  We identify
 * such extensions by their having OIDs in the range reserved for initdb.
 */
static void
selectDumpableExtension(ExtensionInfo *extinfo, DumpOptions *dopt)
{
	/*
	 * Use DUMP_COMPONENT_ACL for from-initdb extensions, to allow users to
	 * change permissions on those objects, if they wish to, and have those
	 * changes preserved.
	 */
	/*if (dopt->binary_upgrade && extinfo->dobj.catId.oid <= (Oid) g_last_builtin_oid)
		extinfo->dobj.dump = extinfo->dobj.dump_contains = DUMP_COMPONENT_ACL;
	else*/

	if  (dbms_md_is_dump_this_obj(dopt, DBMS_MD_DOBJ_BUTT))
		extinfo->dobj.dump = extinfo->dobj.dump_contains = DUMP_COMPONENT_ALL;
	else
		extinfo->dobj.dump = extinfo->dobj.dump_contains = DUMP_COMPONENT_NONE;
}

/*
 * selectDumpablePublicationTable: policy-setting subroutine
 *		Mark a publication table as to be dumped or not
 *
 * Publication tables have schemas, but those are ignored in decision making,
 * because publications are only dumped when we are dumping everything.
 */
static void
selectDumpablePublicationTable(DumpableObject *dobj, Archive *fout)
{
	if (checkExtensionMembership(dobj, fout))
		return;					/* extension membership overrides all else */

	if  (dbms_md_is_dump_this_obj(fout->dopt, DBMS_MD_DOBJ_BUTT))
		dobj->dump = dobj->dump_contains = DUMP_COMPONENT_ALL;
	else
		dobj->dump = dobj->dump_contains = DUMP_COMPONENT_NONE;
}

static void
selectDumpableFunc(FuncInfo *fninfo, Archive *fout)
{
	DBMS_MD_DUMP_GVAR_ST *dump_gvar = &(fout->dopt->dump_gvar);
	if (checkExtensionMembership(&fninfo->dobj, fout))
		return;					/* extension membership overrides all else */

	if  (!dbms_md_is_dump_this_obj(fout->dopt, DBMS_MD_DOBJ_FUNC))
	{
		fninfo->dobj.dump = fninfo->dobj.dump_contains = DUMP_COMPONENT_NONE;
		return ;
	}

	/*
	 * If specific tables are being dumped, dump just those tables; else, dump
	 * according to the parent namespace's dump flag.
	 */
	 
	if (dump_gvar->table_include_oids.head != NULL)
	{
		bool include_obj_oid = dbms_md_oid_list_member(&(dump_gvar->table_include_oids),
												   fninfo->dobj.catId.oid);
		if  (include_obj_oid)
		{
			Oid ns_oid = fninfo->dobj.namespace->dobj.catId.oid;
			bool is_matched_ns = true;
			if (NULL != dump_gvar->schema_include_oids.head)
				is_matched_ns = dbms_md_oid_list_member(&(dump_gvar->schema_include_oids), ns_oid);
			
			if (is_matched_ns)
				fninfo->dobj.dump = DUMP_COMPONENT_ALL;
			else
				fninfo->dobj.dump = DUMP_COMPONENT_NONE;
		}
		else
			fninfo->dobj.dump = DUMP_COMPONENT_NONE;
	}
	else
		fninfo->dobj.dump = fninfo->dobj.namespace->dobj.dump_contains;

}


/*
 * selectDumpableObject: policy-setting subroutine
 *		Mark a generic dumpable object as to be dumped or not
 *
 * Use this only for object types without a special-case routine above.
 */
static void
selectDumpableObject(DumpableObject *dobj, Archive *fout, DBMS_MD_DUMPOBJ_BI obi)
{
	//DBMS_MD_DUMP_GVAR_ST *dump_gvar = &(fout->dopt->dump_gvar);

	if  (!dbms_md_is_dump_this_obj(fout->dopt, obi))
	{
		dobj->dump = dobj->dump_contains = DUMP_COMPONENT_NONE;
	}	
	
	if (checkExtensionMembership(dobj, fout))
		return;					/* extension membership overrides all else */

				
	/*
	 * Default policy is to dump if parent namespace is dumpable, or for
	 * non-namespace-associated items, dump if we're dumping "everything".
	 */
	if (dobj->namespace)
		dobj->dump = dobj->namespace->dobj.dump_contains;
	else
	{
		dobj->dump = dobj->dump_contains = DUMP_COMPONENT_NONE;

		/*dobj->dump = fout->dopt->include_everything ?
			DUMP_COMPONENT_ALL : DUMP_COMPONENT_NONE;*/
	}
}

/*
 * getTableData -
 *	  set up dumpable objects representing the contents of tables
 */
static void
getTableData(Archive *fout, TableInfo *tblinfo, int numTables, bool oids, char relkind)
{
	int			i;
	
	for (i = 0; i < numTables; i++)
	{
		if (tblinfo[i].dobj.dump & DUMP_COMPONENT_DATA &&
			(!relkind || tblinfo[i].relkind == relkind))
			makeTableDataInfo(fout, &(tblinfo[i]), oids);
	}
}

/*
 * Make a dumpable object for the data of this specific table
 *
 * Note: we make a TableDataInfo if and only if we are going to dump the
 * table data; the "dump" flag in such objects isn't used.
 */
static void
makeTableDataInfo(Archive *fout, TableInfo *tbinfo, bool oids)
{
	TableDataInfo *tdinfo;
	DumpOptions *dopt = fout->dopt;
	/*
	 * Nothing to do if we already decided to dump the table.  This will
	 * happen for "config" tables.
	 */
	if (tbinfo->dataObj != NULL)
		return;

	/* Skip VIEWs (no data to dump) */
	if (tbinfo->relkind == RELKIND_VIEW)
		return;
	/* Skip FOREIGN TABLEs (no data to dump) */
	if (tbinfo->relkind == RELKIND_FOREIGN_TABLE)
		return;
	/* Skip partitioned tables (data in partitions) */
	if (tbinfo->relkind == RELKIND_PARTITIONED_TABLE)
		return;

	/* Don't dump data in unlogged tables, if so requested */
	if (tbinfo->relpersistence == RELPERSISTENCE_UNLOGGED &&
		dopt->no_unlogged_table_data)
		return;

	/* OK, let's dump it */
	tdinfo = (TableDataInfo *) dbms_md_malloc0(fout->memory_ctx, sizeof(TableDataInfo));

	if (tbinfo->relkind == RELKIND_MATVIEW)
		tdinfo->dobj.objType = DO_REFRESH_MATVIEW;
	else if (tbinfo->relkind == RELKIND_SEQUENCE)
		tdinfo->dobj.objType = DO_SEQUENCE_SET;
	else
		tdinfo->dobj.objType = DO_TABLE_DATA;

	/*
	 * Note: use tableoid 0 so that this object won't be mistaken for
	 * something that pg_depend entries apply to.
	 */
	tdinfo->dobj.catId.tableoid = 0;
	tdinfo->dobj.catId.oid = tbinfo->dobj.catId.oid;
	AssignDumpId(fout, &tdinfo->dobj);
	tdinfo->dobj.name = tbinfo->dobj.name;
	tdinfo->dobj.namespace = tbinfo->dobj.namespace;
	tdinfo->tdtable = tbinfo;
	tdinfo->oids = oids;
	tdinfo->filtercond = NULL;	/* might get set later */
	addObjectDependency(fout, &tdinfo->dobj, tbinfo->dobj.dumpId);

	tbinfo->dataObj = tdinfo;
}

/*
 * The refresh for a materialized view must be dependent on the refresh for
 * any materialized view that this one is dependent on.
 *
 * This must be called after all the objects are created, but before they are
 * sorted.
 */
static void
buildMatViewRefreshDependencies(Archive *fout)
{
	StringInfo query;
	SPITupleTable   *res;
	int			ntups,
				i;
	int			i_classid,
				i_objid,
				i_refobjid;

	/* No Mat Views before 9.3. */
	if (fout->remoteVersion < 90300)
		return;

	/* Make sure we are in proper schema */
	selectSourceSchema(fout, "pg_catalog");

	query = createStringInfo();

	appendStringInfo(query, "WITH RECURSIVE w AS "
						 "( "
						 "SELECT d1.objid, d2.refobjid, c2.relkind AS refrelkind "
						 "FROM pg_depend d1 "
						 "JOIN pg_class c1 ON c1.oid = d1.objid "
						 "AND c1.relkind = " CppAsString2(RELKIND_MATVIEW)
						 " JOIN pg_rewrite r1 ON r1.ev_class = d1.objid "
						 "JOIN pg_depend d2 ON d2.classid = 'pg_rewrite'::regclass "
						 "AND d2.objid = r1.oid "
						 "AND d2.refobjid <> d1.objid "
						 "JOIN pg_class c2 ON c2.oid = d2.refobjid "
						 "AND c2.relkind IN (" CppAsString2(RELKIND_MATVIEW) ","
						 CppAsString2(RELKIND_VIEW) ") "
						 "WHERE d1.classid = 'pg_class'::regclass "
						 "UNION "
						 "SELECT w.objid, d3.refobjid, c3.relkind "
						 "FROM w "
						 "JOIN pg_rewrite r3 ON r3.ev_class = w.refobjid "
						 "JOIN pg_depend d3 ON d3.classid = 'pg_rewrite'::regclass "
						 "AND d3.objid = r3.oid "
						 "AND d3.refobjid <> w.refobjid "
						 "JOIN pg_class c3 ON c3.oid = d3.refobjid "
						 "AND c3.relkind IN (" CppAsString2(RELKIND_MATVIEW) ","
						 CppAsString2(RELKIND_VIEW) ") "
						 ") "
						 "SELECT 'pg_class'::regclass::oid AS classid, objid, refobjid "
						 "FROM w "
						 "WHERE refrelkind = " CppAsString2(RELKIND_MATVIEW));

	res = ExecuteSqlQuery(fout, query->data);

	ntups = dbms_md_get_tuple_num(res);

	i_classid = dbms_md_get_field_subscript(res, "classid");
	i_objid = dbms_md_get_field_subscript(res, "objid");
	i_refobjid = dbms_md_get_field_subscript(res, "refobjid");

	for (i = 0; i < ntups; i++)
	{
		CatalogId	objId;
		CatalogId	refobjId;
		DumpableObject *dobj;
		DumpableObject *refdobj;
		TableInfo  *tbinfo;
		TableInfo  *reftbinfo;

		objId.tableoid = dbms_md_str_to_oid(dbms_md_get_field_value(res, i, i_classid));
		objId.oid = dbms_md_str_to_oid(dbms_md_get_field_value(res, i, i_objid));
		refobjId.tableoid = objId.tableoid;
		refobjId.oid = dbms_md_str_to_oid(dbms_md_get_field_value(res, i, i_refobjid));

		dobj = findObjectByCatalogId(fout, objId);
		if (dobj == NULL)
			continue;

		Assert(dobj->objType == DO_TABLE);
		tbinfo = (TableInfo *) dobj;
		Assert(tbinfo->relkind == RELKIND_MATVIEW);
		dobj = (DumpableObject *) tbinfo->dataObj;
		if (dobj == NULL)
			continue;
		Assert(dobj->objType == DO_REFRESH_MATVIEW);

		refdobj = findObjectByCatalogId(fout, refobjId);
		if (refdobj == NULL)
			continue;

		Assert(refdobj->objType == DO_TABLE);
		reftbinfo = (TableInfo *) refdobj;
		Assert(reftbinfo->relkind == RELKIND_MATVIEW);
		refdobj = (DumpableObject *) reftbinfo->dataObj;
		if (refdobj == NULL)
			continue;
		Assert(refdobj->objType == DO_REFRESH_MATVIEW);

		addObjectDependency(fout, dobj, refdobj->dumpId);

		if (!reftbinfo->relispopulated)
			tbinfo->relispopulated = false;
	}

	dbms_md_free_tuples(res);

	destroyStringInfo(query);
	pfree(query);
}

/*
 * getTableDataFKConstraints -
 *	  add dump-order dependencies reflecting foreign key constraints
 *
 * This code is executed only in a data-only dump --- in schema+data dumps
 * we handle foreign key issues by not creating the FK constraints until
 * after the data is loaded.  In a data-only dump, however, we want to
 * order the table data objects in such a way that a table's referenced
 * tables are restored first.  (In the presence of circular references or
 * self-references this may be impossible; we'll detect and complain about
 * that during the dependency sorting step.)
 */
static void
getTableDataFKConstraints(Archive *fout)
{
	DumpableObject **dobjs;
	int			numObjs;
	int			i;

	/* Search through all the dumpable objects for FK constraints */
	getDumpableObjects(fout, &dobjs, &numObjs);
	for (i = 0; i < numObjs; i++)
	{
		if (dobjs[i]->objType == DO_FK_CONSTRAINT)
		{
			ConstraintInfo *cinfo = (ConstraintInfo *) dobjs[i];
			TableInfo  *ftable;

			/* Not interesting unless both tables are to be dumped */
			if (cinfo->contable == NULL ||
				cinfo->contable->dataObj == NULL)
				continue;
			ftable = findTableByOid(fout, cinfo->confrelid);
			if (ftable == NULL ||
				ftable->dataObj == NULL)
				continue;

			/*
			 * Okay, make referencing table's TABLE_DATA object depend on the
			 * referenced table's TABLE_DATA object.
			 */
			addObjectDependency(fout, &cinfo->contable->dataObj->dobj,
								ftable->dataObj->dobj.dumpId);
		}
	}
	dbms_md_free(dobjs);
}


/*
 * guessConstraintInheritance:
 *	In pre-8.4 databases, we can't tell for certain which constraints
 *	are inherited.  We assume a CHECK constraint is inherited if its name
 *	matches the name of any constraint in the parent.  Originally this code
 *	tried to compare the expression texts, but that can fail for various
 *	reasons --- for example, if the parent and child tables are in different
 *	schemas, reverse-listing of function calls may produce different text
 *	(schema-qualified or not) depending on search path.
 *
 *	In 8.4 and up we can rely on the conislocal field to decide which
 *	constraints must be dumped; much safer.
 *
 *	This function assumes all conislocal flags were initialized to TRUE.
 *	It clears the flag on anything that seems to be inherited.
 */
static void
guessConstraintInheritance(TableInfo *tblinfo, int numTables)
{
	int			i,
				j,
				k;

	for (i = 0; i < numTables; i++)
	{
		TableInfo  *tbinfo = &(tblinfo[i]);
		int			numParents;
		TableInfo **parents;
		TableInfo  *parent;

		/* Sequences and views never have parents */
		if (tbinfo->relkind == RELKIND_SEQUENCE ||
			tbinfo->relkind == RELKIND_VIEW)
			continue;

		/* Don't bother computing anything for non-target tables, either */
		if (!(tbinfo->dobj.dump & DUMP_COMPONENT_DEFINITION))
			continue;

		numParents = tbinfo->numParents;
		parents = tbinfo->parents;

		if (numParents == 0)
			continue;			/* nothing to see here, move along */

		/* scan for inherited CHECK constraints */
		for (j = 0; j < tbinfo->ncheck; j++)
		{
			ConstraintInfo *constr;

			constr = &(tbinfo->checkexprs[j]);

			for (k = 0; k < numParents; k++)
			{
				int			l;

				parent = parents[k];
				for (l = 0; l < parent->ncheck; l++)
				{
					ConstraintInfo *pconstr = &(parent->checkexprs[l]);

					if (strcmp(pconstr->dobj.name, constr->dobj.name) == 0)
					{
						constr->conislocal = false;
						break;
					}
				}
				if (!constr->conislocal)
					break;
			}
		}
	}
}

#if 0
/*
 * dumpDatabase:
 *	dump the database definition
 */
static void
dumpDatabase(Archive *fout)
{
	DumpOptions *dopt = fout->dopt;
	StringInfo dbQry = createStringInfo();
	StringInfo delQry = createStringInfo();
	StringInfo creaQry = createStringInfo();
	PGconn	   *conn = GetConnection(fout);
	SPITupleTable   *res;
	int			i_tableoid,
				i_oid,
				i_dba,
				i_encoding,
				i_collate,
				i_ctype,
				i_frozenxid,
				i_minmxid,
				i_tablespace;
	CatalogId	dbCatId;
	DumpId		dbDumpId;
	const char *datname,
			   *dba,
			   *encoding,
			   *collate,
			   *ctype,
			   *tablespace;
	uint32		frozenxid,
				minmxid;

	datname = PQdb(conn);

	write_msg(NULL, "saving database definition\n");

	/* Make sure we are in proper schema */
	selectSourceSchema(fout, "pg_catalog");

	/* Get the database owner and parameters from pg_database */
	if (fout->remoteVersion >= 90300)
	{
		appendStringInfo(dbQry, "SELECT tableoid, oid, "
						  "(%s datdba) AS dba, "
						  "pg_encoding_to_char(encoding) AS encoding, "
						  "datcollate, datctype, datfrozenxid, datminmxid, "
						  "(SELECT spcname FROM pg_tablespace t WHERE t.oid = dattablespace) AS tablespace, "
						  "shobj_description(oid, 'pg_database') AS description "

						  "FROM pg_database "
						  "WHERE datname = ",
						  username_subquery);
		appendStringLiteralAH(dbQry, datname, fout);
	}
	else if (fout->remoteVersion >= 80400)
	{
		appendStringInfo(dbQry, "SELECT tableoid, oid, "
						  "(%s datdba) AS dba, "
						  "pg_encoding_to_char(encoding) AS encoding, "
						  "datcollate, datctype, datfrozenxid, 0 AS datminmxid, "
						  "(SELECT spcname FROM pg_tablespace t WHERE t.oid = dattablespace) AS tablespace, "
						  "shobj_description(oid, 'pg_database') AS description "

						  "FROM pg_database "
						  "WHERE datname = ",
						  username_subquery);
		appendStringLiteralAH(dbQry, datname, fout);
	}
	else if (fout->remoteVersion >= 80200)
	{
		appendStringInfo(dbQry, "SELECT tableoid, oid, "
						  "(%s datdba) AS dba, "
						  "pg_encoding_to_char(encoding) AS encoding, "
						  "NULL AS datcollate, NULL AS datctype, datfrozenxid, 0 AS datminmxid, "
						  "(SELECT spcname FROM pg_tablespace t WHERE t.oid = dattablespace) AS tablespace, "
						  "shobj_description(oid, 'pg_database') AS description "

						  "FROM pg_database "
						  "WHERE datname = ",
						  username_subquery);
		appendStringLiteralAH(dbQry, datname, fout);
	}
	else
	{
		appendStringInfo(dbQry, "SELECT tableoid, oid, "
						  "(%s datdba) AS dba, "
						  "pg_encoding_to_char(encoding) AS encoding, "
						  "NULL AS datcollate, NULL AS datctype, datfrozenxid, 0 AS datminmxid, "
						  "(SELECT spcname FROM pg_tablespace t WHERE t.oid = dattablespace) AS tablespace "
						  "FROM pg_database "
						  "WHERE datname = ",
						  username_subquery);
		appendStringLiteralAH(dbQry, datname, fout);
	}

	res = ExecuteSqlQueryForSingleRow(fout, dbQry->data);

	i_tableoid = dbms_md_get_field_subscript(res, "tableoid");
	i_oid = dbms_md_get_field_subscript(res, "oid");
	i_dba = dbms_md_get_field_subscript(res, "dba");
	i_encoding = dbms_md_get_field_subscript(res, "encoding");
	i_collate = dbms_md_get_field_subscript(res, "datcollate");
	i_ctype = dbms_md_get_field_subscript(res, "datctype");
	i_frozenxid = dbms_md_get_field_subscript(res, "datfrozenxid");
	i_minmxid = dbms_md_get_field_subscript(res, "datminmxid");
	i_tablespace = dbms_md_get_field_subscript(res, "tablespace");

	dbCatId.tableoid = dbms_md_str_to_oid(dbms_md_get_field_value(res, 0, i_tableoid));
	dbCatId.oid = dbms_md_str_to_oid(dbms_md_get_field_value(res, 0, i_oid));
	dba = dbms_md_get_field_value(res, 0, i_dba);
	encoding = dbms_md_get_field_value(res, 0, i_encoding);
	collate = dbms_md_get_field_value(res, 0, i_collate);
	ctype = dbms_md_get_field_value(res, 0, i_ctype);
	frozenxid = dbms_md_str_to_oid(dbms_md_get_field_value(res, 0, i_frozenxid));
	minmxid = dbms_md_str_to_oid(dbms_md_get_field_value(res, 0, i_minmxid));
	tablespace = dbms_md_get_field_value(res, 0, i_tablespace);

	appendStringInfo(creaQry, "CREATE DATABASE %s WITH TEMPLATE = template0",
					  dbms_md_fmtId(datname));
	if (strlen(encoding) > 0)
	{
		appendStringInfoString(creaQry, " ENCODING = ");
		appendStringLiteralAH(creaQry, encoding, fout);
	}
	if (strlen(collate) > 0)
	{
		appendStringInfoString(creaQry, " LC_COLLATE = ");
		appendStringLiteralAH(creaQry, collate, fout);
	}
	if (strlen(ctype) > 0)
	{
		appendStringInfoString(creaQry, " LC_CTYPE = ");
		appendStringLiteralAH(creaQry, ctype, fout);
	}
	if (strlen(tablespace) > 0 && strcmp(tablespace, "pg_default") != 0 &&
		!dopt->outputNoTablespaces)
		appendStringInfo(creaQry, " TABLESPACE = %s",
						  dbms_md_fmtId(tablespace));
	appendStringInfoString(creaQry, ";\n");

	if (dopt->binary_upgrade)
	{
		appendStringInfoString(creaQry, "\n-- For binary upgrade, set datfrozenxid and datminmxid.\n");
		appendStringInfo(creaQry, "UPDATE pg_catalog.pg_database\n"
						  "SET datfrozenxid = '%u', datminmxid = '%u'\n"
						  "WHERE datname = ",
						  frozenxid, minmxid);
		appendStringLiteralAH(creaQry, datname, fout);
		appendStringInfoString(creaQry, ";\n");

	}

	appendStringInfo(delQry, "DROP DATABASE %s;\n",
					  dbms_md_fmtId(datname));

	dbDumpId = createDumpId();

	ArchiveEntry(fout,
				 dbCatId,		/* catalog ID */
				 dbDumpId,		/* dump ID */
				 datname,		/* Name */
				 NULL,			/* Namespace */
				 NULL,			/* Tablespace */
				 dba,			/* Owner */
				 false,			/* with oids */
				 "DATABASE",	/* Desc */
				 SECTION_PRE_DATA,	/* Section */
				 creaQry->data, /* Create */
				 delQry->data,	/* Del */
				 NULL,			/* Copy */
				 NULL,			/* Deps */
				 0,				/* # Deps */
				 NULL,			/* Dumper */
				 NULL);			/* Dumper Arg */

	/*
	 * pg_largeobject and pg_largeobject_metadata come from the old system
	 * intact, so set their relfrozenxids and relminmxids.
	 */
	if (dopt->binary_upgrade)
	{
		SPITupleTable   *lo_res;
		StringInfo loFrozenQry = createStringInfo();
		StringInfo loOutQry = createStringInfo();
		int			i_relfrozenxid,
					i_relminmxid;

		/*
		 * pg_largeobject
		 */
		if (fout->remoteVersion >= 90300)
			appendStringInfo(loFrozenQry, "SELECT relfrozenxid, relminmxid\n"
							  "FROM pg_catalog.pg_class\n"
							  "WHERE oid = %u;\n",
							  LargeObjectRelationId);
		else
			appendStringInfo(loFrozenQry, "SELECT relfrozenxid, 0 AS relminmxid\n"
							  "FROM pg_catalog.pg_class\n"
							  "WHERE oid = %u;\n",
							  LargeObjectRelationId);

		lo_res = ExecuteSqlQueryForSingleRow(fout, loFrozenQry->data);

		i_relfrozenxid = dbms_md_get_field_subscript(lo_res, "relfrozenxid");
		i_relminmxid = dbms_md_get_field_subscript(lo_res, "relminmxid");

		appendStringInfoString(loOutQry, "\n-- For binary upgrade, set pg_largeobject relfrozenxid and relminmxid\n");
		appendStringInfo(loOutQry, "UPDATE pg_catalog.pg_class\n"
						  "SET relfrozenxid = '%u', relminmxid = '%u'\n"
						  "WHERE oid = %u;\n",
						  atoi(dbms_md_get_field_value(lo_res, 0, i_relfrozenxid)),
						  atoi(dbms_md_get_field_value(lo_res, 0, i_relminmxid)),
						  LargeObjectRelationId);
		ArchiveEntry(fout, nilCatalogId, createDumpId(),
					 "pg_largeobject", NULL, NULL, "",
					 false, "pg_largeobject", SECTION_PRE_DATA,
					 loOutQry->data, "", NULL,
					 NULL, 0,
					 NULL, NULL);

		dbms_md_free_tuples(lo_res);

		/*
		 * pg_largeobject_metadata
		 */
		if (fout->remoteVersion >= 90000)
		{
			resetStringInfo(loFrozenQry);
			resetStringInfo(loOutQry);

			if (fout->remoteVersion >= 90300)
				appendStringInfo(loFrozenQry, "SELECT relfrozenxid, relminmxid\n"
								  "FROM pg_catalog.pg_class\n"
								  "WHERE oid = %u;\n",
								  LargeObjectMetadataRelationId);
			else
				appendStringInfo(loFrozenQry, "SELECT relfrozenxid, 0 AS relminmxid\n"
								  "FROM pg_catalog.pg_class\n"
								  "WHERE oid = %u;\n",
								  LargeObjectMetadataRelationId);

			lo_res = ExecuteSqlQueryForSingleRow(fout, loFrozenQry->data);

			i_relfrozenxid = dbms_md_get_field_subscript(lo_res, "relfrozenxid");
			i_relminmxid = dbms_md_get_field_subscript(lo_res, "relminmxid");

			appendStringInfoString(loOutQry, "\n-- For binary upgrade, set pg_largeobject_metadata relfrozenxid and relminmxid\n");
			appendStringInfo(loOutQry, "UPDATE pg_catalog.pg_class\n"
							  "SET relfrozenxid = '%u', relminmxid = '%u'\n"
							  "WHERE oid = %u;\n",
							  atoi(dbms_md_get_field_value(lo_res, 0, i_relfrozenxid)),
							  atoi(dbms_md_get_field_value(lo_res, 0, i_relminmxid)),
							  LargeObjectMetadataRelationId);
			ArchiveEntry(fout, nilCatalogId, createDumpId(),
						 "pg_largeobject_metadata", NULL, NULL, "",
						 false, "pg_largeobject_metadata", SECTION_PRE_DATA,
						 loOutQry->data, "", NULL,
						 NULL, 0,
						 NULL, NULL);

			dbms_md_free_tuples(lo_res);
		}

		destroyStringInfo(loFrozenQry);
		destroyStringInfo(loOutQry);
	}*/

	/* Dump DB comment if any */
	if (fout->remoteVersion >= 80200)
	{
		/*
		 * 8.2 keeps comments on shared objects in a shared table, so we
		 * cannot use the dumpComment used for other database objects.
		 */
		char	   *comment = dbms_md_get_field_value(res, 0, dbms_md_get_field_subscript(res, "description"));

		if (comment && strlen(comment))
		{
			resetStringInfo(dbQry);

			/*
			 * Generates warning when loaded into a differently-named
			 * database.
			 */
			appendStringInfo(dbQry, "COMMENT ON DATABASE %s IS ", dbms_md_fmtId(datname));
			appendStringLiteralAH(dbQry, comment, fout);
			appendStringInfoString(dbQry, ";\n");

			ArchiveEntry(fout, dbCatId, createDumpId(), datname, NULL, NULL,
						 dba, false, "COMMENT", SECTION_NONE,
						 dbQry->data, "", NULL,
						 &dbDumpId, 1, NULL, NULL);
		}
	}
	else
	{
		resetStringInfo(dbQry);
		appendStringInfo(dbQry, "DATABASE %s", dbms_md_fmtId(datname));
		dumpComment(fout, dbQry->data, NULL, "",
					dbCatId, 0, dbDumpId);
	}

	/* Dump shared security label. */
	if (!dopt->no_security_labels && fout->remoteVersion >= 90200)
	{
		SPITupleTable   *shres;
		StringInfo seclabelQry;

		seclabelQry = createStringInfo();

		buildShSecLabelQuery(conn, "pg_database", dbCatId.oid, seclabelQry);
		shres = ExecuteSqlQuery(fout, seclabelQry->data);
		resetStringInfo(seclabelQry);
		emitShSecLabels(conn, shres, seclabelQry, "DATABASE", datname);
		if (strlen(seclabelQry->data))
			ArchiveEntry(fout, dbCatId, createDumpId(), datname, NULL, NULL,
						 dba, false, "SECURITY LABEL", SECTION_NONE,
						 seclabelQry->data, "", NULL,
						 &dbDumpId, 1, NULL, NULL);
		destroyStringInfo(seclabelQry);
		dbms_md_free_tuples(shres);
	}

	dbms_md_free_tuples(res);

	destroyStringInfo(dbQry);
	destroyStringInfo(delQry);
	destroyStringInfo(creaQry);
}

/*
 * dumpEncoding: put the correct encoding into the archive
 */
static void
dumpEncoding(Archive *AH)
{
	const char *encname = pg_encoding_to_char(AH->encoding);
	StringInfo qry = createStringInfo();

	write_msg(NULL, "saving encoding = %s\n", encname);

	appendStringInfoString(qry, "SET client_encoding = ");
	appendStringLiteralAH(qry, encname, AH);
	appendStringInfoString(qry, ";\n");

	ArchiveEntry(AH, nilCatalogId, createDumpId(),
				 "ENCODING", NULL, NULL, "",
				 false, "ENCODING", SECTION_PRE_DATA,
				 qry->data, "", NULL,
				 NULL, 0,
				 NULL, NULL);

	destroyStringInfo(qry);
}



/*
 * dumpStdStrings: put the correct escape string behavior into the archive
 */
static void
dumpStdStrings(Archive *AH)
{
	const char *stdstrings = AH->std_strings ? "on" : "off";
	StringInfo qry = createStringInfo();

	write_msg(NULL, "saving standard_conforming_strings = %s\n",
				  stdstrings);

	appendStringInfo(qry, "SET standard_conforming_strings = '%s';\n",
					  stdstrings);

	ArchiveEntry(AH, nilCatalogId, createDumpId(),
				 "STDSTRINGS", NULL, NULL, "",
				 false, "STDSTRINGS", SECTION_PRE_DATA,
				 qry->data, "", NULL,
				 NULL, 0,
				 NULL, NULL);

	destroyStringInfo(qry);
}



/*
 * getBlobs:
 *	Collect schema-level data about large objects
 */
static void
getBlobs(Archive *fout)
{
//	DumpOptions *dopt = fout->dopt;
	StringInfo blobQry = createStringInfo();
	BlobInfo   *binfo;
	DumpableObject *bdata;
	SPITupleTable   *res;
	int			ntups;
	int			i;
	int			i_oid;
	int			i_lomowner;
	int			i_lomacl;
	int			i_rlomacl;
	int			i_initlomacl;
	int			i_initrlomacl;

	/* Verbose message */
	write_msg(NULL, "reading large objects\n");

	/* Make sure we are in proper schema */
	selectSourceSchema(fout, "pg_catalog");

	/* Fetch BLOB OIDs, and owner/ACL data if >= 9.0 */
	if (fout->remoteVersion >= 90600)
	{
		StringInfo acl_subquery = createStringInfo();
		StringInfo racl_subquery = createStringInfo();
		StringInfo init_acl_subquery = createStringInfo();
		StringInfo init_racl_subquery = createStringInfo();

		buildACLQueries(acl_subquery, racl_subquery, init_acl_subquery,
						init_racl_subquery, "l.lomacl", "l.lomowner", "'L'",
						dopt->binary_upgrade);

		appendStringInfo(blobQry,
						  "SELECT l.oid, (%s l.lomowner) AS rolname, "
						  "%s AS lomacl, "
						  "%s AS rlomacl, "
						  "%s AS initlomacl, "
						  "%s AS initrlomacl "
						  "FROM pg_largeobject_metadata l "
						  "LEFT JOIN pg_init_privs pip ON "
						  "(l.oid = pip.objoid "
						  "AND pip.classoid = 'pg_largeobject'::regclass "
						  "AND pip.objsubid = 0) ",
						  username_subquery,
						  acl_subquery->data,
						  racl_subquery->data,
						  init_acl_subquery->data,
						  init_racl_subquery->data);

		destroyStringInfo(acl_subquery);
		pfree(acl_subquery);
		destroyStringInfo(racl_subquery);
		pfree(racl_subquery);
		destroyStringInfo(init_acl_subquery);
		pfree(init_acl_subquery);
		destroyStringInfo(init_racl_subquery);
		pfree(init_racl_subquery);
	}
	else if (fout->remoteVersion >= 90000)
		appendStringInfo(blobQry,
						  "SELECT oid, (%s lomowner) AS rolname, lomacl, "
						  "NULL AS rlomacl, NULL AS initlomacl, "
						  "NULL AS initrlomacl "
						  " FROM pg_largeobject_metadata",
						  username_subquery);
	else
		appendStringInfo(blobQry,
							 "SELECT DISTINCT loid AS oid, "
							 "NULL::name AS rolname, NULL::oid AS lomacl, "
							 "NULL::oid AS rlomacl, NULL::oid AS initlomacl, "
							 "NULL::oid AS initrlomacl "
							 " FROM pg_largeobject");

	res = ExecuteSqlQuery(fout, blobQry->data);

	i_oid = dbms_md_get_field_subscript(res, "oid");
	i_lomowner = dbms_md_get_field_subscript(res, "rolname");
	i_lomacl = dbms_md_get_field_subscript(res, "lomacl");
	i_rlomacl = dbms_md_get_field_subscript(res, "rlomacl");
	i_initlomacl = dbms_md_get_field_subscript(res, "initlomacl");
	i_initrlomacl = dbms_md_get_field_subscript(res, "initrlomacl");

	ntups = dbms_md_get_tuple_num(res);

	/*
	 * Each large object has its own BLOB archive entry.
	 */
	binfo = (BlobInfo *) dbms_md_malloc0(fout->memory_ctx, ntups * sizeof(BlobInfo));

	for (i = 0; i < ntups; i++)
	{
		binfo[i].dobj.objType = DO_BLOB;
		binfo[i].dobj.catId.tableoid = LargeObjectRelationId;
		binfo[i].dobj.catId.oid = dbms_md_str_to_oid(dbms_md_get_field_value(res, i, i_oid));
		AssignDumpId(fout, &binfo[i].dobj);

		binfo[i].dobj.name = dbms_md_strdup(dbms_md_get_field_value(res, i, i_oid));
		binfo[i].rolname = dbms_md_strdup(dbms_md_get_field_value(res, i, i_lomowner));
		binfo[i].blobacl = dbms_md_strdup(dbms_md_get_field_value(res, i, i_lomacl));
		binfo[i].rblobacl = dbms_md_strdup(dbms_md_get_field_value(res, i, i_rlomacl));
		binfo[i].initblobacl = dbms_md_strdup(dbms_md_get_field_value(res, i, i_initlomacl));
		binfo[i].initrblobacl = dbms_md_strdup(dbms_md_get_field_value(res, i, i_initrlomacl));

		if (dbms_md_is_tuple_field_null(res, i, i_lomacl) &&
			dbms_md_is_tuple_field_null(res, i, i_rlomacl) &&
			dbms_md_is_tuple_field_null(res, i, i_initlomacl) &&
			dbms_md_is_tuple_field_null(res, i, i_initrlomacl))
			binfo[i].dobj.dump &= ~DUMP_COMPONENT_ACL;

		/*
		 * In binary-upgrade mode for blobs, we do *not* dump out the data or
		 * the ACLs, should any exist.  The data and ACL (if any) will be
		 * copied by pg_upgrade, which simply copies the pg_largeobject and
		 * pg_largeobject_metadata tables.
		 *
		 * We *do* dump out the definition of the blob because we need that to
		 * make the restoration of the comments, and anything else, work since
		 * pg_upgrade copies the files behind pg_largeobject and
		 * pg_largeobject_metadata after the dump is restored.
		 */
		if (dopt->binary_upgrade)
			binfo[i].dobj.dump &= ~(DUMP_COMPONENT_DATA | DUMP_COMPONENT_ACL);
	}

	/*
	 * If we have any large objects, a "BLOBS" archive entry is needed. This
	 * is just a placeholder for sorting; it carries no data now.
	 */
	if (ntups > 0)
	{
		bdata = (DumpableObject *) dbms_md_malloc0(fout->memory_ctx, sizeof(DumpableObject));
		bdata->objType = DO_BLOB_DATA;
		bdata->catId = nilCatalogId;
		AssignDumpId(fout, bdata);
		bdata->name = dbms_md_strdup("BLOBS");
	}

	dbms_md_free_tuples(res);
	destroyStringInfo(blobQry);
	pfree(blobQry);
}

/*
 * dumpBlob
 *
 * dump the definition (metadata) of the given large object
 */
static void
dumpBlob(Archive *fout, BlobInfo *binfo)
{
	StringInfo cquery = createStringInfo();
	StringInfo dquery = createStringInfo();

	appendStringInfo(cquery,
					  "SELECT pg_catalog.lo_create('%s');\n",
					  binfo->dobj.name);

	appendStringInfo(dquery,
					  "SELECT pg_catalog.lo_unlink('%s');\n",
					  binfo->dobj.name);

	if (binfo->dobj.dump & DUMP_COMPONENT_DEFINITION)
		ArchiveEntry(fout, binfo->dobj.catId, binfo->dobj.dumpId,
					 binfo->dobj.name,
					 NULL, NULL,
					 binfo->rolname, false,
					 "BLOB", SECTION_PRE_DATA,
					 cquery->data, dquery->data, NULL,
					 NULL, 0,
					 NULL, NULL);

	/* set up tag for comment and/or ACL */
	resetStringInfo(cquery);
	appendStringInfo(cquery, "LARGE OBJECT %s", binfo->dobj.name);

	/* Dump comment if any */
	if (binfo->dobj.dump & DUMP_COMPONENT_COMMENT)
		dumpComment(fout, cquery->data,
					NULL, binfo->rolname,
					binfo->dobj.catId, 0, binfo->dobj.dumpId);

	/* Dump security label if any */
	if (binfo->dobj.dump & DUMP_COMPONENT_SECLABEL)
		dumpSecLabel(fout, cquery->data,
					 NULL, binfo->rolname,
					 binfo->dobj.catId, 0, binfo->dobj.dumpId);

	/* Dump ACL if any */
	if (binfo->blobacl && (binfo->dobj.dump & DUMP_COMPONENT_ACL))
		dumpACL(fout, binfo->dobj.catId, binfo->dobj.dumpId, "LARGE OBJECT",
				binfo->dobj.name, NULL, cquery->data,
				NULL, binfo->rolname, binfo->blobacl, binfo->rblobacl,
				binfo->initblobacl, binfo->initrblobacl);

	destroyStringInfo(cquery);
	pfree(cquery);
	destroyStringInfo(dquery);
	pfree(dquery);
}


/*
 * dumpBlobs:
 *	dump the data contents of all large objects
 */

static int
dumpBlobs(Archive *fout, void *arg)
{
	const char *blobQry;
	const char *blobFetchQry;
	PGconn	   *conn = GetConnection(fout);
	SPITupleTable   *res;
	char		buf[LOBBUFSIZE];
	int			ntups;
	int			i;
	int			cnt;

	write_msg(NULL, "saving large objects\n");

	/* Make sure we are in proper schema */
	selectSourceSchema(fout, "pg_catalog");

	/*
	 * Currently, we re-fetch all BLOB OIDs using a cursor.  Consider scanning
	 * the already-in-memory dumpable objects instead...
	 */
	if (fout->remoteVersion >= 90000)
		blobQry = "DECLARE bloboid CURSOR FOR SELECT oid FROM pg_largeobject_metadata";
	else
		blobQry = "DECLARE bloboid CURSOR FOR SELECT DISTINCT loid FROM pg_largeobject";

	ExecuteSqlStatement(fout, blobQry);

	/* Command to fetch from cursor */
	blobFetchQry = "FETCH 1000 IN bloboid";

	do
	{
		/* Do a fetch */
		res = ExecuteSqlQuery(fout, blobFetchQry);

		/* Process the tuples, if any */
		ntups = dbms_md_get_tuple_num(res);
		for (i = 0; i < ntups; i++)
		{
			Oid			blobOid;
			int			loFd;

			blobOid = dbms_md_str_to_oid(dbms_md_get_field_value(res, i, SPI_RES_FIRST_FIELD_NO));
			/* Open the BLOB */
			loFd = lo_open(conn, blobOid, INV_READ);
			if (loFd == -1)
				elog(ERROR, "could not open large object %u: %s",
							  blobOid, PQerrorMessage(conn));

			StartBlob(fout, blobOid);

			/* Now read it in chunks, sending data to archive */
			do
			{
				cnt = lo_read(conn, loFd, buf, LOBBUFSIZE);
				if (cnt < 0)
					elog(ERROR, "error reading large object %u: %s",
								  blobOid, PQerrorMessage(conn));

				WriteData(fout, buf, cnt);
			} while (cnt > 0);

			lo_close(conn, loFd);

			EndBlob(fout, blobOid);
		}

		dbms_md_free_tuples(res);
	} while (ntups > 0);

	return 1;
}
#endif
/*
 * getPolicies
 *	  get information about policies on a dumpable table.
 */
void
getPolicies(Archive *fout, TableInfo tblinfo[], int numTables)
{
	StringInfo query;
	SPITupleTable   *res;
	PolicyInfo *polinfo;
	int			i_oid;
	int			i_tableoid;
	int			i_polname;
	int			i_polcmd;
	int			i_polpermissive;
	int			i_polroles;
	int			i_polqual;
	int			i_polwithcheck;
	int			i,
				j,
				ntups;

	if (fout->remoteVersion < 90500)
		return;

	query = createStringInfo();

	for (i = 0; i < numTables; i++)
	{
		TableInfo  *tbinfo = &tblinfo[i];

		/* Ignore row security on tables not to be dumped */
		if (!(tbinfo->dobj.dump & DUMP_COMPONENT_POLICY))
			continue;

		write_msg(NULL, "reading row security enabled for table \"%s.%s\"\n",
					  tbinfo->dobj.namespace->dobj.name,
					  tbinfo->dobj.name);

		/*
		 * Get row security enabled information for the table. We represent
		 * RLS enabled on a table by creating PolicyInfo object with an empty
		 * policy.
		 */
		if (tbinfo->rowsec)
		{
			/*
			 * Note: use tableoid 0 so that this object won't be mistaken for
			 * something that pg_depend entries apply to.
			 */
			polinfo = dbms_md_malloc0(fout->memory_ctx, sizeof(PolicyInfo));
			polinfo->dobj.objType = DO_POLICY;
			polinfo->dobj.catId.tableoid = 0;
			polinfo->dobj.catId.oid = tbinfo->dobj.catId.oid;
			AssignDumpId(fout, &polinfo->dobj);
			polinfo->dobj.namespace = tbinfo->dobj.namespace;
			polinfo->dobj.name = dbms_md_strdup(tbinfo->dobj.name);
			polinfo->poltable = tbinfo;
			polinfo->polname = NULL;
			polinfo->polcmd = '\0';
			polinfo->polpermissive = 0;
			polinfo->polroles = NULL;
			polinfo->polqual = NULL;
			polinfo->polwithcheck = NULL;
		}

		write_msg(NULL, "reading policies for table \"%s.%s\"\n",
					  tbinfo->dobj.namespace->dobj.name,
					  tbinfo->dobj.name);

		/*
		 * select table schema to ensure regproc name is qualified if needed
		 */
		selectSourceSchema(fout, tbinfo->dobj.namespace->dobj.name);

		resetStringInfo(query);

		/* Get the policies for the table. */
		if (fout->remoteVersion >= 100000)
			appendStringInfo(query,
							  "SELECT oid, tableoid, pol.polname, pol.polcmd, pol.polpermissive, "
							  "CASE WHEN pol.polroles = '{0}' THEN NULL ELSE "
							  "   pg_catalog.array_to_string(ARRAY(SELECT pg_catalog.quote_ident(rolname) from pg_catalog.pg_roles WHERE oid = ANY(pol.polroles)), ', ') END AS polroles, "
							  "pg_catalog.pg_get_expr(pol.polqual, pol.polrelid) AS polqual, "
							  "pg_catalog.pg_get_expr(pol.polwithcheck, pol.polrelid) AS polwithcheck "
							  "FROM pg_catalog.pg_policy pol "
							  "WHERE polrelid = '%u'",
							  tbinfo->dobj.catId.oid);
		else
			appendStringInfo(query,
							  "SELECT oid, tableoid, pol.polname, pol.polcmd, 't' as polpermissive, "
							  "CASE WHEN pol.polroles = '{0}' THEN NULL ELSE "
							  "   pg_catalog.array_to_string(ARRAY(SELECT pg_catalog.quote_ident(rolname) from pg_catalog.pg_roles WHERE oid = ANY(pol.polroles)), ', ') END AS polroles, "
							  "pg_catalog.pg_get_expr(pol.polqual, pol.polrelid) AS polqual, "
							  "pg_catalog.pg_get_expr(pol.polwithcheck, pol.polrelid) AS polwithcheck "
							  "FROM pg_catalog.pg_policy pol "
							  "WHERE polrelid = '%u'",
							  tbinfo->dobj.catId.oid);
		res = ExecuteSqlQuery(fout, query->data);

		ntups = dbms_md_get_tuple_num(res);

		if (ntups == 0)
		{
			/*
			 * No explicit policies to handle (only the default-deny policy,
			 * which is handled as part of the table definition).  Clean up
			 * and return.
			 */
			dbms_md_free_tuples(res);
			continue;
		}

		i_oid = dbms_md_get_field_subscript(res, "oid");
		i_tableoid = dbms_md_get_field_subscript(res, "tableoid");
		i_polname = dbms_md_get_field_subscript(res, "polname");
		i_polcmd = dbms_md_get_field_subscript(res, "polcmd");
		i_polpermissive = dbms_md_get_field_subscript(res, "polpermissive");
		i_polroles = dbms_md_get_field_subscript(res, "polroles");
		i_polqual = dbms_md_get_field_subscript(res, "polqual");
		i_polwithcheck = dbms_md_get_field_subscript(res, "polwithcheck");

		polinfo = dbms_md_malloc0(fout->memory_ctx, ntups * sizeof(PolicyInfo));

		for (j = 0; j < ntups; j++)
		{
			polinfo[j].dobj.objType = DO_POLICY;
			polinfo[j].dobj.catId.tableoid =
				dbms_md_str_to_oid(dbms_md_get_field_value(res, j, i_tableoid));
			polinfo[j].dobj.catId.oid = dbms_md_str_to_oid(dbms_md_get_field_value(res, j, i_oid));
			AssignDumpId(fout, &polinfo[j].dobj);
			polinfo[j].dobj.namespace = tbinfo->dobj.namespace;
			polinfo[j].poltable = tbinfo;
			polinfo[j].polname = dbms_md_strdup(dbms_md_get_field_value(res, j, i_polname));
			polinfo[j].dobj.name = dbms_md_strdup(polinfo[j].polname);

			polinfo[j].polcmd = *(dbms_md_get_field_value(res, j, i_polcmd));
			polinfo[j].polpermissive = *(dbms_md_get_field_value(res, j, i_polpermissive)) == 't';

			if (dbms_md_is_tuple_field_null(res, j, i_polroles))
				polinfo[j].polroles = NULL;
			else
				polinfo[j].polroles = dbms_md_strdup(dbms_md_get_field_value(res, j, i_polroles));

			if (dbms_md_is_tuple_field_null(res, j, i_polqual))
				polinfo[j].polqual = NULL;
			else
				polinfo[j].polqual = dbms_md_strdup(dbms_md_get_field_value(res, j, i_polqual));

			if (dbms_md_is_tuple_field_null(res, j, i_polwithcheck))
				polinfo[j].polwithcheck = NULL;
			else
				polinfo[j].polwithcheck
					= dbms_md_strdup(dbms_md_get_field_value(res, j, i_polwithcheck));
		}
		dbms_md_free_tuples(res);
	}
	destroyStringInfo(query);
	pfree(query);
}

/*
 * dumpPolicy
 *	  dump the definition of the given policy
 */
static void
dumpPolicy(Archive *fout, PolicyInfo *polinfo)
{
	DumpOptions *dopt = fout->dopt;
	TableInfo  *tbinfo = polinfo->poltable;
	StringInfo query;
	StringInfo delqry;
	const char *cmd;
	char	   *tag = NULL;

	if (dopt->dataOnly)
		return;

	/*
	 * If polname is NULL, then this record is just indicating that ROW LEVEL
	 * SECURITY is enabled for the table. Dump as ALTER TABLE <table> ENABLE
	 * ROW LEVEL SECURITY.
	 */
	if (polinfo->polname == NULL)
	{
		query = createStringInfo();

		appendStringInfo(query, "ALTER TABLE %s ENABLE ROW LEVEL SECURITY;",
						  dbms_md_fmtId(polinfo->dobj.name));

		if (polinfo->dobj.dump & DUMP_COMPONENT_POLICY)
			ArchiveEntry(fout, polinfo->dobj.catId, polinfo->dobj.dumpId,
						 polinfo->dobj.name,
						 polinfo->dobj.namespace->dobj.name,
						 NULL,
						 tbinfo->rolname, false,
						 "ROW SECURITY", SECTION_POST_DATA,
						 query->data, "", NULL,
						 NULL, 0,
						 NULL, NULL);

		destroyStringInfo(query);
		pfree(query);
		return;
	}

	if (polinfo->polcmd == '*')
		cmd = "";
	else if (polinfo->polcmd == 'r')
		cmd = " FOR SELECT";
	else if (polinfo->polcmd == 'a')
		cmd = " FOR INSERT";
	else if (polinfo->polcmd == 'w')
		cmd = " FOR UPDATE";
	else if (polinfo->polcmd == 'd')
		cmd = " FOR DELETE";
	else
	{
		elog(ERROR, "unexpected policy command type: %c\n",
				  polinfo->polcmd);
		return;
	}

	query = createStringInfo();
	delqry = createStringInfo();

	appendStringInfo(query, "CREATE POLICY %s", dbms_md_fmtId(polinfo->polname));

	appendStringInfo(query, " ON %s%s%s", dbms_md_fmtId(tbinfo->dobj.name),
					  !polinfo->polpermissive ? " AS RESTRICTIVE" : "", cmd);

	if (polinfo->polroles != NULL)
		appendStringInfo(query, " TO %s", polinfo->polroles);

	if (polinfo->polqual != NULL)
		appendStringInfo(query, " USING (%s)", polinfo->polqual);

	if (polinfo->polwithcheck != NULL)
		appendStringInfo(query, " WITH CHECK (%s)", polinfo->polwithcheck);

	appendStringInfo(query, ";\n");

	appendStringInfo(delqry, "DROP POLICY %s", dbms_md_fmtId(polinfo->polname));
	appendStringInfo(delqry, " ON %s;\n", dbms_md_fmtId(tbinfo->dobj.name));

	tag = psprintf("%s %s", tbinfo->dobj.name, polinfo->dobj.name);
	Assert(tag != NULL);

	if (polinfo->dobj.dump & DUMP_COMPONENT_POLICY)
		ArchiveEntry(fout, polinfo->dobj.catId, polinfo->dobj.dumpId,
					 tag,
					 polinfo->dobj.namespace->dobj.name,
					 NULL,
					 tbinfo->rolname, false,
					 "POLICY", SECTION_POST_DATA,
					 query->data, delqry->data, NULL,
					 NULL, 0,
					 NULL, NULL);

	Assert(tag != NULL);
	pfree(tag);

	destroyStringInfo(query);
	pfree(query);
	destroyStringInfo(delqry);
	pfree(delqry);
}

/*
 * getPublications
 *	  get information about publications
 */
void
getPublications(Archive *fout)
{
	DumpOptions *dopt = fout->dopt;
	StringInfo query;
	SPITupleTable   *res;
	PublicationInfo *pubinfo;
	int			i_tableoid;
	int			i_oid;
	int			i_pubname;
	int			i_rolname;
	int			i_puballtables;
	int			i_pubinsert;
	int			i_pubupdate;
	int			i_pubdelete;
	int			i,
				ntups;

	if (dopt->no_publications || fout->remoteVersion < 100000)
		return;

	query = createStringInfo();

	resetStringInfo(query);

	/* Get the publications. */
	appendStringInfo(query,
					  "SELECT p.tableoid, p.oid, p.pubname, "
					  "(%s p.pubowner) AS rolname, "
					  "p.puballtables, p.pubinsert, p.pubupdate, p.pubdelete "
					  "FROM pg_catalog.pg_publication p",
					  username_subquery);

	res = ExecuteSqlQuery(fout, query->data);

	ntups = dbms_md_get_tuple_num(res);

	i_tableoid = dbms_md_get_field_subscript(res, "tableoid");
	i_oid = dbms_md_get_field_subscript(res, "oid");
	i_pubname = dbms_md_get_field_subscript(res, "pubname");
	i_rolname = dbms_md_get_field_subscript(res, "rolname");
	i_puballtables = dbms_md_get_field_subscript(res, "puballtables");
	i_pubinsert = dbms_md_get_field_subscript(res, "pubinsert");
	i_pubupdate = dbms_md_get_field_subscript(res, "pubupdate");
	i_pubdelete = dbms_md_get_field_subscript(res, "pubdelete");

	pubinfo = dbms_md_malloc0(fout->memory_ctx, ntups * sizeof(PublicationInfo));

	for (i = 0; i < ntups; i++)
	{
		pubinfo[i].dobj.objType = DO_PUBLICATION;
		pubinfo[i].dobj.catId.tableoid =
			dbms_md_str_to_oid(dbms_md_get_field_value(res, i, i_tableoid));
		pubinfo[i].dobj.catId.oid = dbms_md_str_to_oid(dbms_md_get_field_value(res, i, i_oid));
		AssignDumpId(fout, &pubinfo[i].dobj);
		pubinfo[i].dobj.name = dbms_md_strdup(dbms_md_get_field_value(res, i, i_pubname));
		pubinfo[i].rolname = dbms_md_strdup(dbms_md_get_field_value(res, i, i_rolname));
		pubinfo[i].puballtables =
			(strcmp(dbms_md_get_field_value(res, i, i_puballtables), "t") == 0);
		pubinfo[i].pubinsert =
			(strcmp(dbms_md_get_field_value(res, i, i_pubinsert), "t") == 0);
		pubinfo[i].pubupdate =
			(strcmp(dbms_md_get_field_value(res, i, i_pubupdate), "t") == 0);
		pubinfo[i].pubdelete =
			(strcmp(dbms_md_get_field_value(res, i, i_pubdelete), "t") == 0);

		if (strlen(pubinfo[i].rolname) == 0)
			write_msg(NULL, "WARNING: owner of publication \"%s\" appears to be invalid\n",
					  pubinfo[i].dobj.name);

		/* Decide whether we want to dump it */
		selectDumpableObject(&(pubinfo[i].dobj), fout, DBMS_MD_DOBJ_BUTT);
	}
	dbms_md_free_tuples(res);

	destroyStringInfo(query);
	pfree(query);
}

/*
 * dumpPublication
 *	  dump the definition of the given publication
 */
static void
dumpPublication(Archive *fout, PublicationInfo *pubinfo)
{
	StringInfo delq;
	StringInfo query;
	StringInfo labelq;
	bool		first = true;

	if (!(pubinfo->dobj.dump & DUMP_COMPONENT_DEFINITION))
		return;

	delq = createStringInfo();
	query = createStringInfo();
	labelq = createStringInfo();

	appendStringInfo(delq, "DROP PUBLICATION %s;\n",
					  dbms_md_fmtId(pubinfo->dobj.name));

	appendStringInfo(query, "CREATE PUBLICATION %s",
					  dbms_md_fmtId(pubinfo->dobj.name));

	appendStringInfo(labelq, "PUBLICATION %s", dbms_md_fmtId(pubinfo->dobj.name));

	if (pubinfo->puballtables)
		appendStringInfo(query, " FOR ALL TABLES");

	appendStringInfo(query, " WITH (publish = '");
	if (pubinfo->pubinsert)
	{
		appendStringInfo(query, "insert");
		first = false;
	}

	if (pubinfo->pubupdate)
	{
		if (!first)
			appendStringInfo(query, ", ");

		appendStringInfo(query, "update");
		first = false;
	}

	if (pubinfo->pubdelete)
	{
		if (!first)
			appendStringInfo(query, ", ");

		appendStringInfo(query, "delete");
		first = false;
	}

	appendStringInfo(query, "');\n");

	ArchiveEntry(fout, pubinfo->dobj.catId, pubinfo->dobj.dumpId,
				 pubinfo->dobj.name,
				 NULL,
				 NULL,
				 pubinfo->rolname, false,
				 "PUBLICATION", SECTION_POST_DATA,
				 query->data, delq->data, NULL,
				 NULL, 0,
				 NULL, NULL);

	if (pubinfo->dobj.dump & DUMP_COMPONENT_COMMENT)
		dumpComment(fout, labelq->data,
					NULL, pubinfo->rolname,
					pubinfo->dobj.catId, 0, pubinfo->dobj.dumpId);

	if (pubinfo->dobj.dump & DUMP_COMPONENT_SECLABEL)
		dumpSecLabel(fout, labelq->data,
					 NULL, pubinfo->rolname,
					 pubinfo->dobj.catId, 0, pubinfo->dobj.dumpId);

	destroyStringInfo(delq);
	pfree(delq);
	destroyStringInfo(query);
	pfree(query);
}

/*
 * getPublicationTables
 *	  get information about publication membership for dumpable tables.
 */
void
getPublicationTables(Archive *fout, TableInfo tblinfo[], int numTables)
{
	StringInfo query;
	SPITupleTable   *res;
	PublicationRelInfo *pubrinfo;
	int			i_tableoid;
	int			i_oid;
	int			i_pubname;
	int			i,
				j,
				ntups;

	if (fout->remoteVersion < 100000)
		return;

	query = createStringInfo();

	for (i = 0; i < numTables; i++)
	{
		TableInfo  *tbinfo = &tblinfo[i];

		/* Only plain tables can be aded to publications. */
		if (tbinfo->relkind != RELKIND_RELATION)
			continue;

		/*
		 * Ignore publication membership of tables whose definitions are not
		 * to be dumped.
		 */
		if (!(tbinfo->dobj.dump & DUMP_COMPONENT_DEFINITION))
			continue;

		write_msg(NULL, "reading publication membership for table \"%s.%s\"\n",
					  tbinfo->dobj.namespace->dobj.name,
					  tbinfo->dobj.name);

		resetStringInfo(query);

		/* Get the publication membership for the table. */
		appendStringInfo(query,
						  "SELECT pr.tableoid, pr.oid, p.pubname "
						  "FROM pg_catalog.pg_publication_rel pr,"
						  "     pg_catalog.pg_publication p "
						  "WHERE pr.prrelid = '%u'"
						  "  AND p.oid = pr.prpubid",
						  tbinfo->dobj.catId.oid);
		res = ExecuteSqlQuery(fout, query->data);

		ntups = dbms_md_get_tuple_num(res);

		if (ntups == 0)
		{
			/*
			 * Table is not member of any publications. Clean up and return.
			 */
			dbms_md_free_tuples(res);
			continue;
		}

		i_tableoid = dbms_md_get_field_subscript(res, "tableoid");
		i_oid = dbms_md_get_field_subscript(res, "oid");
		i_pubname = dbms_md_get_field_subscript(res, "pubname");

		pubrinfo = dbms_md_malloc0(fout->memory_ctx, ntups * sizeof(PublicationRelInfo));

		for (j = 0; j < ntups; j++)
		{
			pubrinfo[j].dobj.objType = DO_PUBLICATION_REL;
			pubrinfo[j].dobj.catId.tableoid =
				dbms_md_str_to_oid(dbms_md_get_field_value(res, j, i_tableoid));
			pubrinfo[j].dobj.catId.oid = dbms_md_str_to_oid(dbms_md_get_field_value(res, j, i_oid));
			AssignDumpId(fout, &pubrinfo[j].dobj);
			pubrinfo[j].dobj.namespace = tbinfo->dobj.namespace;
			pubrinfo[j].dobj.name = tbinfo->dobj.name;
			pubrinfo[j].pubname = dbms_md_strdup(dbms_md_get_field_value(res, j, i_pubname));
			pubrinfo[j].pubtable = tbinfo;

			/* Decide whether we want to dump it */
			selectDumpablePublicationTable(&(pubrinfo[j].dobj), fout);
		}
		dbms_md_free_tuples(res);
	}
	destroyStringInfo(query);
	pfree(query);
}

/*
 * dumpPublicationTable
 *	  dump the definition of the given publication table mapping
 */
static void
dumpPublicationTable(Archive *fout, PublicationRelInfo *pubrinfo)
{
	TableInfo  *tbinfo = pubrinfo->pubtable;
	StringInfo query;
	char	   *tag = NULL;

	if (!(pubrinfo->dobj.dump & DUMP_COMPONENT_DEFINITION))
		return;

	tag = psprintf("%s %s", pubrinfo->pubname, tbinfo->dobj.name);
	Assert(tag != NULL);

	query = createStringInfo();

	appendStringInfo(query, "ALTER PUBLICATION %s ADD TABLE ONLY",
					  dbms_md_fmtId(pubrinfo->pubname));
	appendStringInfo(query, " %s;",
					  dbms_md_fmtId(tbinfo->dobj.name));

	/*
	 * There is no point in creating drop query as drop query as the drop is
	 * done by table drop.
	 */
	ArchiveEntry(fout, pubrinfo->dobj.catId, pubrinfo->dobj.dumpId,
				 tag,
				 tbinfo->dobj.namespace->dobj.name,
				 NULL,
				 "", false,
				 "PUBLICATION TABLE", SECTION_POST_DATA,
				 query->data, "", NULL,
				 NULL, 0,
				 NULL, NULL);

	Assert(tag != NULL);
	pfree(tag);

	destroyStringInfo(query);
	pfree(query);
}

/*
 * Is the currently connected user a superuser?
 */
static bool
is_superuser(Archive *fout)
{
	SysScanDesc scan;
	Relation    rel;
	HeapTuple   htuple;
	bool issuper = false;
	Oid userid = GetUserId();

	rel = heap_open(AuthIdRelationId, AccessShareLock);
	scan = systable_beginscan(rel, InvalidOid, false, NULL, 0, NULL);

	while (HeapTupleIsValid(htuple = systable_getnext(scan)))
	{
		if (userid == HeapTupleGetOid(htuple))
		{
			Form_pg_authid pg_authid_form = ((Form_pg_authid) GETSTRUCT(htuple));
			issuper = pg_authid_form->rolsuper;
			break;
		}
	}

	systable_endscan(scan);
	heap_close(rel, AccessShareLock);
	return issuper;
}

/*
 * getSubscriptions
 *	  get information about subscriptions
 */
void
getSubscriptions(Archive *fout)
{
	DumpOptions *dopt = fout->dopt;
	StringInfo query;
	SPITupleTable   *res;
	SubscriptionInfo *subinfo;
	int			i_tableoid;
	int			i_oid;
	int			i_subname;
	int			i_rolname;
	int			i_subconninfo;
	int			i_subslotname;
	int			i_subsynccommit;
	int			i_subpublications;
	int			i,
				ntups;

	if (dopt->no_subscriptions || fout->remoteVersion < 100000)
		return;

	if (!is_superuser(fout))
	{
		int			n;

		res = ExecuteSqlQuery(fout,
							  "SELECT count(*) FROM pg_subscription "
							  "WHERE subdbid = (SELECT oid FROM pg_catalog.pg_database"
							  "                 WHERE datname = current_database())");
		n = atoi(dbms_md_get_field_value(res, 0, SPI_RES_FIRST_FIELD_NO));
		if (n > 0)
			write_msg(NULL, "WARNING: subscriptions not dumped because current user is not a superuser\n");
		dbms_md_free_tuples(res);
		return;
	}

	query = createStringInfo();

	resetStringInfo(query);

	/* Get the subscriptions in current database. */
	appendStringInfo(query,
					  "SELECT s.tableoid, s.oid, s.subname,"
					  "(%s s.subowner) AS rolname, "
					  " s.subconninfo, s.subslotname, s.subsynccommit, "
					  " s.subpublications "
					  "FROM pg_catalog.pg_subscription s "
					  "WHERE s.subdbid = (SELECT oid FROM pg_catalog.pg_database"
					  "                   WHERE datname = current_database())",
					  username_subquery);
	res = ExecuteSqlQuery(fout, query->data);

	ntups = dbms_md_get_tuple_num(res);

	i_tableoid = dbms_md_get_field_subscript(res, "tableoid");
	i_oid = dbms_md_get_field_subscript(res, "oid");
	i_subname = dbms_md_get_field_subscript(res, "subname");
	i_rolname = dbms_md_get_field_subscript(res, "rolname");
	i_subconninfo = dbms_md_get_field_subscript(res, "subconninfo");
	i_subslotname = dbms_md_get_field_subscript(res, "subslotname");
	i_subsynccommit = dbms_md_get_field_subscript(res, "subsynccommit");
	i_subpublications = dbms_md_get_field_subscript(res, "subpublications");

	subinfo = dbms_md_malloc0(fout->memory_ctx, ntups * sizeof(SubscriptionInfo));

	for (i = 0; i < ntups; i++)
	{
		subinfo[i].dobj.objType = DO_SUBSCRIPTION;
		subinfo[i].dobj.catId.tableoid =
			dbms_md_str_to_oid(dbms_md_get_field_value(res, i, i_tableoid));
		subinfo[i].dobj.catId.oid = dbms_md_str_to_oid(dbms_md_get_field_value(res, i, i_oid));
		AssignDumpId(fout, &subinfo[i].dobj);
		subinfo[i].dobj.name = dbms_md_strdup(dbms_md_get_field_value(res, i, i_subname));
		subinfo[i].rolname = dbms_md_strdup(dbms_md_get_field_value(res, i, i_rolname));
		subinfo[i].subconninfo = dbms_md_strdup(dbms_md_get_field_value(res, i, i_subconninfo));
		if (dbms_md_is_tuple_field_null(res, i, i_subslotname))
			subinfo[i].subslotname = NULL;
		else
			subinfo[i].subslotname = dbms_md_strdup(dbms_md_get_field_value(res, i, i_subslotname));
		subinfo[i].subsynccommit =
			dbms_md_strdup(dbms_md_get_field_value(res, i, i_subsynccommit));
		subinfo[i].subpublications =
			dbms_md_strdup(dbms_md_get_field_value(res, i, i_subpublications));

		if (strlen(subinfo[i].rolname) == 0)
			write_msg(NULL, "WARNING: owner of subscription \"%s\" appears to be invalid\n",
					  subinfo[i].dobj.name);

		/* Decide whether we want to dump it */
		selectDumpableObject(&(subinfo[i].dobj), fout, DBMS_MD_DOBJ_BUTT);
	}
	dbms_md_free_tuples(res);

	destroyStringInfo(query);
	pfree(query);
}

/*
 * dumpSubscription
 *	  dump the definition of the given subscription
 */
static void
dumpSubscription(Archive *fout, SubscriptionInfo *subinfo)
{
	StringInfo delq;
	StringInfo query;
	StringInfo labelq;
	StringInfo publications;
	char	  **pubnames = NULL;
	int			npubnames = 0;
	int			i;

	if (!(subinfo->dobj.dump & DUMP_COMPONENT_DEFINITION))
		return;

	delq = createStringInfo();
	query = createStringInfo();
	labelq = createStringInfo();

	appendStringInfo(delq, "DROP SUBSCRIPTION %s;\n",
					  dbms_md_fmtId(subinfo->dobj.name));

	appendStringInfo(query, "CREATE SUBSCRIPTION %s CONNECTION ",
					  dbms_md_fmtId(subinfo->dobj.name));
	appendStringLiteralAH(query, subinfo->subconninfo, fout);

	/* Build list of quoted publications and append them to query. */
	if (!dbms_md_parse_array(subinfo->subpublications, &pubnames, &npubnames))
	{
		write_msg(NULL,
				  "WARNING: could not parse subpublications array\n");
		if (pubnames)
			pfree(pubnames);
		pubnames = NULL;
		npubnames = 0;
	}

	publications = createStringInfo();
	for (i = 0; i < npubnames; i++)
	{
		if (i > 0)
			appendStringInfoString(publications, ", ");

		appendStringInfoString(publications, dbms_md_fmtId(pubnames[i]));
	}

	appendStringInfo(query, " PUBLICATION %s WITH (connect = false, slot_name = ", publications->data);
	if (subinfo->subslotname)
		appendStringLiteralAH(query, subinfo->subslotname, fout);
	else
		appendStringInfoString(query, "NONE");

	if (strcmp(subinfo->subsynccommit, "off") != 0)
		appendStringInfo(query, ", synchronous_commit = %s", dbms_md_fmtId_internal(subinfo->subsynccommit, true));

	appendStringInfoString(query, ");\n");

	appendStringInfo(labelq, "SUBSCRIPTION %s", dbms_md_fmtId(subinfo->dobj.name));

	ArchiveEntry(fout, subinfo->dobj.catId, subinfo->dobj.dumpId,
				 subinfo->dobj.name,
				 NULL,
				 NULL,
				 subinfo->rolname, false,
				 "SUBSCRIPTION", SECTION_POST_DATA,
				 query->data, delq->data, NULL,
				 NULL, 0,
				 NULL, NULL);

	if (subinfo->dobj.dump & DUMP_COMPONENT_COMMENT)
		dumpComment(fout, labelq->data,
					NULL, subinfo->rolname,
					subinfo->dobj.catId, 0, subinfo->dobj.dumpId);

	if (subinfo->dobj.dump & DUMP_COMPONENT_SECLABEL)
		dumpSecLabel(fout, labelq->data,
					 NULL, subinfo->rolname,
					 subinfo->dobj.catId, 0, subinfo->dobj.dumpId);

	destroyStringInfo(publications);
	if (pubnames)
		pfree(pubnames);

	destroyStringInfo(delq);
	pfree(delq);
	destroyStringInfo(query);
	pfree(query);
}

/*
 * getNamespaces:
 *	  read all namespaces in the system catalogs and return them in the
 * NamespaceInfo* structure
 *
 *	numNamespaces is set to the number of namespaces read in
 */
NamespaceInfo *
getNamespaces(Archive *fout, int *numNamespaces)
{
	DumpOptions *dopt = fout->dopt;
	SPITupleTable   *res;
	int			ntups;
	int			i;
	StringInfo query;
	NamespaceInfo *nsinfo;
	int			i_tableoid;
	int			i_oid;
	int			i_nspname;
	int			i_rolname;
	int			i_nspacl;
	int			i_rnspacl;
	int			i_initnspacl;
	int			i_initrnspacl;

	query = createStringInfo();

	/* Make sure we are in proper schema */
	selectSourceSchema(fout, "pg_catalog");

	/*
	 * we fetch all namespaces including system ones, so that every object we
	 * read in can be linked to a containing namespace.
	 */
	if (fout->remoteVersion >= 90600)
	{
		StringInfo acl_subquery = createStringInfo();
		StringInfo racl_subquery = createStringInfo();
		StringInfo init_acl_subquery = createStringInfo();
		StringInfo init_racl_subquery = createStringInfo();

		buildACLQueries(acl_subquery, racl_subquery, init_acl_subquery,
						init_racl_subquery, "n.nspacl", "n.nspowner", "'n'", false);

		appendStringInfo(query, "SELECT n.tableoid, n.oid, n.nspname, "
						  "(%s nspowner) AS rolname, "
						  "%s as nspacl, "
						  "%s as rnspacl, "
						  "%s as initnspacl, "
						  "%s as initrnspacl "
						  "FROM pg_namespace n "
						  "LEFT JOIN pg_init_privs pip "
						  "ON (n.oid = pip.objoid "
						  "AND pip.classoid = 'pg_namespace'::regclass "
						  "AND pip.objsubid = 0",
						  username_subquery,
						  acl_subquery->data,
						  racl_subquery->data,
						  init_acl_subquery->data,
						  init_racl_subquery->data);

		/*
		 * When we are doing a 'clean' run, we will be dropping and recreating
		 * the 'public' schema (the only object which has that kind of
		 * treatment in the backend and which has an entry in pg_init_privs)
		 * and therefore we should not consider any initial privileges in
		 * pg_init_privs in that case.
		 *
		 * See pg_backup_archiver.c:_printTocEntry() for the details on why
		 * the public schema is special in this regard.
		 *
		 * Note that if the public schema is dropped and re-created, this is
		 * essentially a no-op because the new public schema won't have an
		 * entry in pg_init_privs anyway, as the entry will be removed when
		 * the public schema is dropped.
		 *
		 * Further, we have to handle the case where the public schema does
		 * not exist at all.
		 */
		if (dopt->outputClean)
			appendStringInfo(query, " AND pip.objoid <> "
							  "coalesce((select oid from pg_namespace "
							  "where nspname = 'public'),0)");

		appendStringInfo(query, ") ");

		destroyStringInfo(acl_subquery);
		destroyStringInfo(racl_subquery);
		destroyStringInfo(init_acl_subquery);
		destroyStringInfo(init_racl_subquery);

		pfree(acl_subquery);		
		pfree(racl_subquery);		
		pfree(init_acl_subquery);		
		pfree(init_racl_subquery);
	}
	else
		appendStringInfo(query, "SELECT tableoid, oid, nspname, "
						  "(%s nspowner) AS rolname, "
						  "nspacl, NULL as rnspacl, "
						  "NULL AS initnspacl, NULL as initrnspacl "
						  "FROM pg_namespace",
						  username_subquery);

	res = ExecuteSqlQuery(fout, query->data);

	ntups = dbms_md_get_tuple_num(res);

	nsinfo = (NamespaceInfo *) dbms_md_malloc0(fout->memory_ctx, ntups * sizeof(NamespaceInfo));

	i_tableoid = dbms_md_get_field_subscript(res, "tableoid");
	i_oid = dbms_md_get_field_subscript(res, "oid");
	i_nspname = dbms_md_get_field_subscript(res, "nspname");
	i_rolname = dbms_md_get_field_subscript(res, "rolname");
	i_nspacl = dbms_md_get_field_subscript(res, "nspacl");
	i_rnspacl = dbms_md_get_field_subscript(res, "rnspacl");
	i_initnspacl = dbms_md_get_field_subscript(res, "initnspacl");
	i_initrnspacl = dbms_md_get_field_subscript(res, "initrnspacl");

	for (i = 0; i < ntups; i++)
	{
		nsinfo[i].dobj.objType = DO_NAMESPACE;
		nsinfo[i].dobj.catId.tableoid = dbms_md_str_to_oid(dbms_md_get_field_value(res, i, i_tableoid));
		nsinfo[i].dobj.catId.oid = dbms_md_str_to_oid(dbms_md_get_field_value(res, i, i_oid));
		AssignDumpId(fout, &nsinfo[i].dobj);
		nsinfo[i].dobj.name = dbms_md_strdup(dbms_md_get_field_value(res, i, i_nspname));
		nsinfo[i].rolname = dbms_md_strdup(dbms_md_get_field_value(res, i, i_rolname));
		nsinfo[i].nspacl = dbms_md_strdup(dbms_md_get_field_value(res, i, i_nspacl));
		nsinfo[i].rnspacl = dbms_md_strdup(dbms_md_get_field_value(res, i, i_rnspacl));
		nsinfo[i].initnspacl = dbms_md_strdup(dbms_md_get_field_value(res, i, i_initnspacl));
		nsinfo[i].initrnspacl = dbms_md_strdup(dbms_md_get_field_value(res, i, i_initrnspacl));

		/* Decide whether to dump this namespace */
		selectDumpableNamespace(&nsinfo[i], fout);

		/*
		 * Do not try to dump ACL if the ACL is empty or the default.
		 *
		 * This is useful because, for some schemas/objects, the only
		 * component we are going to try and dump is the ACL and if we can
		 * remove that then 'dump' goes to zero/false and we don't consider
		 * this object for dumping at all later on.
		 */
		if (dbms_md_is_tuple_field_null(res, i, i_nspacl) 
			&& dbms_md_is_tuple_field_null(res, i, i_rnspacl) 
			&& dbms_md_is_tuple_field_null(res, i, i_initnspacl) 
			&& dbms_md_is_tuple_field_null(res, i, i_initrnspacl))
			nsinfo[i].dobj.dump &= ~DUMP_COMPONENT_ACL;

		if (strlen(nsinfo[i].rolname) == 0)
			write_msg(NULL, "WARNING: owner of schema \"%s\" appears to be invalid\n",
					  nsinfo[i].dobj.name);
	}

	dbms_md_free_tuples(res);
	destroyStringInfo(query);
	pfree(query);

	*numNamespaces = ntups;

	return nsinfo;
}

/*
 * findNamespace:
 *		given a namespace OID, look up the info read by getNamespaces
 */
static NamespaceInfo *
findNamespace(Archive *fout, Oid nsoid)
{
	NamespaceInfo *nsinfo = NULL;

	nsinfo = findNamespaceByOid(fout, nsoid);
	if (nsinfo == NULL)
		elog(ERROR, "schema with OID %u does not exist\n", nsoid);
	return nsinfo;
}

/*
 * getExtensions:
 *	  read all extensions in the system catalogs and return them in the
 * ExtensionInfo* structure
 *
 *	numExtensions is set to the number of extensions read in
 */
ExtensionInfo *
getExtensions(Archive *fout, int *numExtensions)
{
	DumpOptions *dopt = fout->dopt;
	SPITupleTable   *res;
	int			ntups;
	int			i;
	StringInfo query;
	ExtensionInfo *extinfo;
	int			i_tableoid;
	int			i_oid;
	int			i_extname;
	int			i_nspname;
	int			i_extrelocatable;
	int			i_extversion;
	int			i_extconfig;
	int			i_extcondition;

	/*
	 * Before 9.1, there are no extensions.
	 */
	if (fout->remoteVersion < 90100)
	{
		*numExtensions = 0;
		return NULL;
	}

	query = createStringInfo();

	/* Make sure we are in proper schema */
	selectSourceSchema(fout, "pg_catalog");

	appendStringInfoString(query, "SELECT x.tableoid, x.oid, "
						 "x.extname, n.nspname, x.extrelocatable, x.extversion, x.extconfig, x.extcondition "
						 "FROM pg_extension x "
						 "JOIN pg_namespace n ON n.oid = x.extnamespace");

	res = ExecuteSqlQuery(fout, query->data);

	ntups = dbms_md_get_tuple_num(res);

	extinfo = (ExtensionInfo *) dbms_md_malloc0(fout->memory_ctx, ntups * sizeof(ExtensionInfo));

	i_tableoid = dbms_md_get_field_subscript(res, "tableoid");
	i_oid = dbms_md_get_field_subscript(res, "oid");
	i_extname = dbms_md_get_field_subscript(res, "extname");
	i_nspname = dbms_md_get_field_subscript(res, "nspname");
	i_extrelocatable = dbms_md_get_field_subscript(res, "extrelocatable");
	i_extversion = dbms_md_get_field_subscript(res, "extversion");
	i_extconfig = dbms_md_get_field_subscript(res, "extconfig");
	i_extcondition = dbms_md_get_field_subscript(res, "extcondition");

	for (i = 0; i < ntups; i++)
	{
		extinfo[i].dobj.objType = DO_EXTENSION;
		extinfo[i].dobj.catId.tableoid = dbms_md_str_to_oid(dbms_md_get_field_value(res, i, i_tableoid));
		extinfo[i].dobj.catId.oid = dbms_md_str_to_oid(dbms_md_get_field_value(res, i, i_oid));
		AssignDumpId(fout, &extinfo[i].dobj);
		extinfo[i].dobj.name = dbms_md_strdup(dbms_md_get_field_value(res, i, i_extname));
		extinfo[i].namespace = dbms_md_strdup(dbms_md_get_field_value(res, i, i_nspname));
		extinfo[i].relocatable = *(dbms_md_get_field_value(res, i, i_extrelocatable)) == 't';
		extinfo[i].extversion = dbms_md_strdup(dbms_md_get_field_value(res, i, i_extversion));
		extinfo[i].extconfig = dbms_md_strdup(dbms_md_get_field_value(res, i, i_extconfig));
		extinfo[i].extcondition = dbms_md_strdup(dbms_md_get_field_value(res, i, i_extcondition));

		/* Decide whether we want to dump it */
		selectDumpableExtension(&(extinfo[i]), dopt);
	}

	dbms_md_free_tuples(res);
	destroyStringInfo(query);
	pfree(query);

	*numExtensions = ntups;

	return extinfo;
}

/*
 * getTypes:
 *	  read all types in the system catalogs and return them in the
 * TypeInfo* structure
 *
 *	numTypes is set to the number of types read in
 *
 * NB: this must run after getFuncs() because we assume we can do
 * findFuncByOid().
 */
TypeInfo *
getTypes(Archive *fout, int *numTypes)
{
	DumpOptions *dopt = fout->dopt;
	SPITupleTable   *res;
	int			ntups;
	int			i;
	StringInfo query = createStringInfo();
	TypeInfo   *tyinfo;
	ShellTypeInfo *stinfo;
	int			i_tableoid;
	int			i_oid;
	int			i_typname;
	int			i_typnamespace;
	int			i_typacl;
	int			i_rtypacl;
	int			i_inittypacl;
	int			i_initrtypacl;
	int			i_rolname;
	int			i_typelem;
	int			i_typrelid;
	int			i_typrelkind;
	int			i_typtype;
	int			i_typisdefined;
	int			i_isarray;

	/*
	 * we include even the built-in types because those may be used as array
	 * elements by user-defined types
	 *
	 * we filter out the built-in types when we dump out the types
	 *
	 * same approach for undefined (shell) types and array types
	 *
	 * Note: as of 8.3 we can reliably detect whether a type is an
	 * auto-generated array type by checking the element type's typarray.
	 * (Before that the test is capable of generating false positives.) We
	 * still check for name beginning with '_', though, so as to avoid the
	 * cost of the subselect probe for all standard types.  This would have to
	 * be revisited if the backend ever allows renaming of array types.
	 */

	/* Make sure we are in proper schema */
	selectSourceSchema(fout, "pg_catalog");

	if (fout->remoteVersion >= 90600)
	{
		StringInfo acl_subquery = createStringInfo();
		StringInfo racl_subquery = createStringInfo();
		StringInfo initacl_subquery = createStringInfo();
		StringInfo initracl_subquery = createStringInfo();

		buildACLQueries(acl_subquery, racl_subquery, initacl_subquery,
						initracl_subquery, "t.typacl", "t.typowner", "'T'",
						dopt->binary_upgrade);

		appendStringInfo(query, "SELECT t.tableoid, t.oid, t.typname, "
						  "t.typnamespace, "
						  "%s AS typacl, "
						  "%s AS rtypacl, "
						  "%s AS inittypacl, "
						  "%s AS initrtypacl, "
						  "(%s t.typowner) AS rolname, "
						  "t.typelem, t.typrelid, "
						  "CASE WHEN t.typrelid = 0 THEN ' '::\"char\" "
						  "ELSE (SELECT relkind FROM pg_class WHERE oid = t.typrelid) END AS typrelkind, "
						  "t.typtype, t.typisdefined, "
						  "t.typname[0] = '_' AND t.typelem != 0 AND "
						  "(SELECT typarray FROM pg_type te WHERE oid = t.typelem) = t.oid AS isarray "
						  "FROM pg_type t "
						  "LEFT JOIN pg_init_privs pip ON "
						  "(t.oid = pip.objoid "
						  "AND pip.classoid = 'pg_type'::regclass "
						  "AND pip.objsubid = 0) ",
						  acl_subquery->data,
						  racl_subquery->data,
						  initacl_subquery->data,
						  initracl_subquery->data,
						  username_subquery);

		destroyStringInfo(acl_subquery);
		pfree(acl_subquery);
		destroyStringInfo(racl_subquery);
		pfree(racl_subquery);
		destroyStringInfo(initacl_subquery);
		pfree(initacl_subquery);
		destroyStringInfo(initracl_subquery);
		pfree(initracl_subquery);
	}
	else if (fout->remoteVersion >= 90200)
	{
		appendStringInfo(query, "SELECT tableoid, oid, typname, "
						  "typnamespace, typacl, NULL as rtypacl, "
						  "NULL AS inittypacl, NULL AS initrtypacl, "
						  "(%s typowner) AS rolname, "
						  "typelem, typrelid, "
						  "CASE WHEN typrelid = 0 THEN ' '::\"char\" "
						  "ELSE (SELECT relkind FROM pg_class WHERE oid = typrelid) END AS typrelkind, "
						  "typtype, typisdefined, "
						  "typname[0] = '_' AND typelem != 0 AND "
						  "(SELECT typarray FROM pg_type te WHERE oid = pg_type.typelem) = oid AS isarray "
						  "FROM pg_type",
						  username_subquery);
	}
	else if (fout->remoteVersion >= 80300)
	{
		appendStringInfo(query, "SELECT tableoid, oid, typname, "
						  "typnamespace, NULL AS typacl, NULL as rtypacl, "
						  "NULL AS inittypacl, NULL AS initrtypacl, "
						  "(%s typowner) AS rolname, "
						  "typelem, typrelid, "
						  "CASE WHEN typrelid = 0 THEN ' '::\"char\" "
						  "ELSE (SELECT relkind FROM pg_class WHERE oid = typrelid) END AS typrelkind, "
						  "typtype, typisdefined, "
						  "typname[0] = '_' AND typelem != 0 AND "
						  "(SELECT typarray FROM pg_type te WHERE oid = pg_type.typelem) = oid AS isarray "
						  "FROM pg_type",
						  username_subquery);
	}
	else
	{
		appendStringInfo(query, "SELECT tableoid, oid, typname, "
						  "typnamespace, NULL AS typacl, NULL as rtypacl, "
						  "NULL AS inittypacl, NULL AS initrtypacl, "
						  "(%s typowner) AS rolname, "
						  "typelem, typrelid, "
						  "CASE WHEN typrelid = 0 THEN ' '::\"char\" "
						  "ELSE (SELECT relkind FROM pg_class WHERE oid = typrelid) END AS typrelkind, "
						  "typtype, typisdefined, "
						  "typname[0] = '_' AND typelem != 0 AS isarray "
						  "FROM pg_type",
						  username_subquery);
	}

	res = ExecuteSqlQuery(fout, query->data);

	ntups = dbms_md_get_tuple_num(res);

	tyinfo = (TypeInfo *) dbms_md_malloc0(fout->memory_ctx, ntups * sizeof(TypeInfo));

	i_tableoid = dbms_md_get_field_subscript(res, "tableoid");
	i_oid = dbms_md_get_field_subscript(res, "oid");
	i_typname = dbms_md_get_field_subscript(res, "typname");
	i_typnamespace = dbms_md_get_field_subscript(res, "typnamespace");
	i_typacl = dbms_md_get_field_subscript(res, "typacl");
	i_rtypacl = dbms_md_get_field_subscript(res, "rtypacl");
	i_inittypacl = dbms_md_get_field_subscript(res, "inittypacl");
	i_initrtypacl = dbms_md_get_field_subscript(res, "initrtypacl");
	i_rolname = dbms_md_get_field_subscript(res, "rolname");
	i_typelem = dbms_md_get_field_subscript(res, "typelem");
	i_typrelid = dbms_md_get_field_subscript(res, "typrelid");
	i_typrelkind = dbms_md_get_field_subscript(res, "typrelkind");
	i_typtype = dbms_md_get_field_subscript(res, "typtype");
	i_typisdefined = dbms_md_get_field_subscript(res, "typisdefined");
	i_isarray = dbms_md_get_field_subscript(res, "isarray");

	for (i = 0; i < ntups; i++)
	{
		tyinfo[i].dobj.objType = DO_TYPE;
		tyinfo[i].dobj.catId.tableoid = dbms_md_str_to_oid(dbms_md_get_field_value(res, i, i_tableoid));
		tyinfo[i].dobj.catId.oid = dbms_md_str_to_oid(dbms_md_get_field_value(res, i, i_oid));
		AssignDumpId(fout, &tyinfo[i].dobj);
		tyinfo[i].dobj.name = dbms_md_strdup(dbms_md_get_field_value(res, i, i_typname));
		tyinfo[i].dobj.namespace =
			findNamespace(fout,
						  dbms_md_str_to_oid(dbms_md_get_field_value(res, i, i_typnamespace)));
		tyinfo[i].rolname = dbms_md_strdup(dbms_md_get_field_value(res, i, i_rolname));
		tyinfo[i].typacl = dbms_md_strdup(dbms_md_get_field_value(res, i, i_typacl));
		tyinfo[i].rtypacl = dbms_md_strdup(dbms_md_get_field_value(res, i, i_rtypacl));
		tyinfo[i].inittypacl = dbms_md_strdup(dbms_md_get_field_value(res, i, i_inittypacl));
		tyinfo[i].initrtypacl = dbms_md_strdup(dbms_md_get_field_value(res, i, i_initrtypacl));
		tyinfo[i].typelem = dbms_md_str_to_oid(dbms_md_get_field_value(res, i, i_typelem));
		tyinfo[i].typrelid = dbms_md_str_to_oid(dbms_md_get_field_value(res, i, i_typrelid));
		tyinfo[i].typrelkind = *dbms_md_get_field_value(res, i, i_typrelkind);
		tyinfo[i].typtype = *dbms_md_get_field_value(res, i, i_typtype);
		tyinfo[i].shellType = NULL;

		if (strcmp(dbms_md_get_field_value(res, i, i_typisdefined), "t") == 0)
			tyinfo[i].isDefined = true;
		else
			tyinfo[i].isDefined = false;

		if (strcmp(dbms_md_get_field_value(res, i, i_isarray), "t") == 0)
			tyinfo[i].isArray = true;
		else
			tyinfo[i].isArray = false;

		/* Decide whether we want to dump it */
		selectDumpableType(&tyinfo[i], fout);

		/* Do not try to dump ACL if no ACL exists. */
		if (dbms_md_is_tuple_field_null(res, i, i_typacl) 
			&& dbms_md_is_tuple_field_null(res, i, i_rtypacl) 
			&& dbms_md_is_tuple_field_null(res, i, i_inittypacl) 
			&& dbms_md_is_tuple_field_null(res, i, i_initrtypacl))
			tyinfo[i].dobj.dump &= ~DUMP_COMPONENT_ACL;

		/*
		 * If it's a domain, fetch info about its constraints, if any
		 */
		tyinfo[i].nDomChecks = 0;
		tyinfo[i].domChecks = NULL;
		if ((tyinfo[i].dobj.dump & DUMP_COMPONENT_DEFINITION) &&
			tyinfo[i].typtype == TYPTYPE_DOMAIN)
			getDomainConstraints(fout, &(tyinfo[i]));

		/*
		 * If it's a base type, make a DumpableObject representing a shell
		 * definition of the type.  We will need to dump that ahead of the I/O
		 * functions for the type.  Similarly, range types need a shell
		 * definition in case they have a canonicalize function.
		 *
		 * Note: the shell type doesn't have a catId.  You might think it
		 * should copy the base type's catId, but then it might capture the
		 * pg_depend entries for the type, which we don't want.
		 */
		if ((tyinfo[i].dobj.dump & DUMP_COMPONENT_DEFINITION) &&
			(tyinfo[i].typtype == TYPTYPE_BASE ||
			 tyinfo[i].typtype == TYPTYPE_RANGE))
		{
			stinfo = (ShellTypeInfo *) dbms_md_malloc0(fout->memory_ctx, sizeof(ShellTypeInfo));
			stinfo->dobj.objType = DO_SHELL_TYPE;
			stinfo->dobj.catId = nilCatalogId;
			AssignDumpId(fout, &stinfo->dobj);
			stinfo->dobj.name = dbms_md_strdup(tyinfo[i].dobj.name);
			stinfo->dobj.namespace = tyinfo[i].dobj.namespace;
			stinfo->baseType = &(tyinfo[i]);
			tyinfo[i].shellType = stinfo;

			/*
			 * Initially mark the shell type as not to be dumped.  We'll only
			 * dump it if the I/O or canonicalize functions need to be dumped;
			 * this is taken care of while sorting dependencies.
			 */
			stinfo->dobj.dump = DUMP_COMPONENT_NONE;
		}

		if (strlen(tyinfo[i].rolname) == 0)
			write_msg(NULL, "WARNING: owner of data type \"%s\" appears to be invalid\n",
					  tyinfo[i].dobj.name);
	}

	*numTypes = ntups;

	dbms_md_free_tuples(res);

	destroyStringInfo(query);
	pfree(query);
	return tyinfo;
}

/*
 * getOperators:
 *	  read all operators in the system catalogs and return them in the
 * OprInfo* structure
 *
 *	numOprs is set to the number of operators read in
 */
OprInfo *
getOperators(Archive *fout, int *numOprs)
{
	SPITupleTable   *res;
	int			ntups;
	int			i;
	StringInfo query = createStringInfo();
	OprInfo    *oprinfo;
	int			i_tableoid;
	int			i_oid;
	int			i_oprname;
	int			i_oprnamespace;
	int			i_rolname;
	int			i_oprkind;
	int			i_oprcode;

	/*
	 * find all operators, including builtin operators; we filter out
	 * system-defined operators at dump-out time.
	 */

	/* Make sure we are in proper schema */
	selectSourceSchema(fout, "pg_catalog");

	appendStringInfo(query, "SELECT tableoid, oid, oprname, "
					  "oprnamespace, "
					  "(%s oprowner) AS rolname, "
					  "oprkind, "
					  "oprcode::oid AS oprcode "
					  "FROM pg_operator",
					  username_subquery);

	res = ExecuteSqlQuery(fout, query->data);

	ntups = dbms_md_get_tuple_num(res);
	*numOprs = ntups;

	oprinfo = (OprInfo *) dbms_md_malloc0(fout->memory_ctx, ntups * sizeof(OprInfo));

	i_tableoid = dbms_md_get_field_subscript(res, "tableoid");
	i_oid = dbms_md_get_field_subscript(res, "oid");
	i_oprname = dbms_md_get_field_subscript(res, "oprname");
	i_oprnamespace = dbms_md_get_field_subscript(res, "oprnamespace");
	i_rolname = dbms_md_get_field_subscript(res, "rolname");
	i_oprkind = dbms_md_get_field_subscript(res, "oprkind");
	i_oprcode = dbms_md_get_field_subscript(res, "oprcode");

	for (i = 0; i < ntups; i++)
	{
		oprinfo[i].dobj.objType = DO_OPERATOR;
		oprinfo[i].dobj.catId.tableoid = dbms_md_str_to_oid(dbms_md_get_field_value(res, i, i_tableoid));
		oprinfo[i].dobj.catId.oid = dbms_md_str_to_oid(dbms_md_get_field_value(res, i, i_oid));
		AssignDumpId(fout, &oprinfo[i].dobj);
		oprinfo[i].dobj.name = dbms_md_strdup(dbms_md_get_field_value(res, i, i_oprname));
		oprinfo[i].dobj.namespace =
			findNamespace(fout,
						  dbms_md_str_to_oid(dbms_md_get_field_value(res, i, i_oprnamespace)));
		oprinfo[i].rolname = dbms_md_strdup(dbms_md_get_field_value(res, i, i_rolname));
		oprinfo[i].oprkind = (dbms_md_get_field_value(res, i, i_oprkind))[0];
		oprinfo[i].oprcode = dbms_md_str_to_oid(dbms_md_get_field_value(res, i, i_oprcode));

		/* Decide whether we want to dump it */
		selectDumpableObject(&(oprinfo[i].dobj), fout, DBMS_MD_DOBJ_BUTT);

		/* Operators do not currently have ACLs. */
		oprinfo[i].dobj.dump &= ~DUMP_COMPONENT_ACL;

		if (strlen(oprinfo[i].rolname) == 0)
			write_msg(NULL, "WARNING: owner of operator \"%s\" appears to be invalid\n",
					  oprinfo[i].dobj.name);
	}

	dbms_md_free_tuples(res);

	destroyStringInfo(query);
	pfree(query);

	return oprinfo;
}

/*
 * getCollations:
 *	  read all collations in the system catalogs and return them in the
 * CollInfo* structure
 *
 *	numCollations is set to the number of collations read in
 */
CollInfo *
getCollations(Archive *fout, int *numCollations)
{
	SPITupleTable   *res;
	int			ntups;
	int			i;
	StringInfo query;
	CollInfo   *collinfo;
	int			i_tableoid;
	int			i_oid;
	int			i_collname;
	int			i_collnamespace;
	int			i_rolname;

	/* Collations didn't exist pre-9.1 */
	if (fout->remoteVersion < 90100)
	{
		*numCollations = 0;
		return NULL;
	}

	query = createStringInfo();

	/*
	 * find all collations, including builtin collations; we filter out
	 * system-defined collations at dump-out time.
	 */

	/* Make sure we are in proper schema */
	selectSourceSchema(fout, "pg_catalog");

	appendStringInfo(query, "SELECT tableoid, oid, collname, "
					  "collnamespace, "
					  "(%s collowner) AS rolname "
					  "FROM pg_collation",
					  username_subquery);

	res = ExecuteSqlQuery(fout, query->data);

	ntups = dbms_md_get_tuple_num(res);
	*numCollations = ntups;

	collinfo = (CollInfo *) dbms_md_malloc0(fout->memory_ctx, ntups * sizeof(CollInfo));

	i_tableoid = dbms_md_get_field_subscript(res, "tableoid");
	i_oid = dbms_md_get_field_subscript(res, "oid");
	i_collname = dbms_md_get_field_subscript(res, "collname");
	i_collnamespace = dbms_md_get_field_subscript(res, "collnamespace");
	i_rolname = dbms_md_get_field_subscript(res, "rolname");

	for (i = 0; i < ntups; i++)
	{
		collinfo[i].dobj.objType = DO_COLLATION;
		collinfo[i].dobj.catId.tableoid = dbms_md_str_to_oid(dbms_md_get_field_value(res, i, i_tableoid));
		collinfo[i].dobj.catId.oid = dbms_md_str_to_oid(dbms_md_get_field_value(res, i, i_oid));
		AssignDumpId(fout, &collinfo[i].dobj);
		collinfo[i].dobj.name = dbms_md_strdup(dbms_md_get_field_value(res, i, i_collname));
		collinfo[i].dobj.namespace =
			findNamespace(fout,
						  dbms_md_str_to_oid(dbms_md_get_field_value(res, i, i_collnamespace)));
		collinfo[i].rolname = dbms_md_strdup(dbms_md_get_field_value(res, i, i_rolname));

		/* Decide whether we want to dump it */
		selectDumpableObject(&(collinfo[i].dobj), fout, DBMS_MD_DOBJ_BUTT);

		/* Collations do not currently have ACLs. */
		collinfo[i].dobj.dump &= ~DUMP_COMPONENT_ACL;
	}

	dbms_md_free_tuples(res);

	destroyStringInfo(query);
	pfree(query);

	return collinfo;
}

/*
 * getConversions:
 *	  read all conversions in the system catalogs and return them in the
 * ConvInfo* structure
 *
 *	numConversions is set to the number of conversions read in
 */
ConvInfo *
getConversions(Archive *fout, int *numConversions)
{
	SPITupleTable   *res;
	int			ntups;
	int			i;
	StringInfo query;
	ConvInfo   *convinfo;
	int			i_tableoid;
	int			i_oid;
	int			i_conname;
	int			i_connamespace;
	int			i_rolname;

	query = createStringInfo();

	/*
	 * find all conversions, including builtin conversions; we filter out
	 * system-defined conversions at dump-out time.
	 */

	/* Make sure we are in proper schema */
	selectSourceSchema(fout, "pg_catalog");

	appendStringInfo(query, "SELECT tableoid, oid, conname, "
					  "connamespace, "
					  "(%s conowner) AS rolname "
					  "FROM pg_conversion",
					  username_subquery);

	res = ExecuteSqlQuery(fout, query->data);

	ntups = dbms_md_get_tuple_num(res);
	*numConversions = ntups;

	convinfo = (ConvInfo *) dbms_md_malloc0(fout->memory_ctx, ntups * sizeof(ConvInfo));

	i_tableoid = dbms_md_get_field_subscript(res, "tableoid");
	i_oid = dbms_md_get_field_subscript(res, "oid");
	i_conname = dbms_md_get_field_subscript(res, "conname");
	i_connamespace = dbms_md_get_field_subscript(res, "connamespace");
	i_rolname = dbms_md_get_field_subscript(res, "rolname");

	for (i = 0; i < ntups; i++)
	{
		convinfo[i].dobj.objType = DO_CONVERSION;
		convinfo[i].dobj.catId.tableoid = dbms_md_str_to_oid(dbms_md_get_field_value(res, i, i_tableoid));
		convinfo[i].dobj.catId.oid = dbms_md_str_to_oid(dbms_md_get_field_value(res, i, i_oid));
		AssignDumpId(fout, &convinfo[i].dobj);
		convinfo[i].dobj.name = dbms_md_strdup(dbms_md_get_field_value(res, i, i_conname));
		convinfo[i].dobj.namespace =
			findNamespace(fout,
						  dbms_md_str_to_oid(dbms_md_get_field_value(res, i, i_connamespace)));
		convinfo[i].rolname = dbms_md_strdup(dbms_md_get_field_value(res, i, i_rolname));

		/* Decide whether we want to dump it */
		selectDumpableObject(&(convinfo[i].dobj), fout, DBMS_MD_DOBJ_BUTT);

		/* Conversions do not currently have ACLs. */
		convinfo[i].dobj.dump &= ~DUMP_COMPONENT_ACL;
	}

	dbms_md_free_tuples(res);

	destroyStringInfo(query);
	pfree(query);

	return convinfo;
}

/*
 * getAccessMethods:
 *	  read all user-defined access methods in the system catalogs and return
 *	  them in the AccessMethodInfo* structure
 *
 *	numAccessMethods is set to the number of access methods read in
 */
AccessMethodInfo *
getAccessMethods(Archive *fout, int *numAccessMethods)
{
	SPITupleTable   *res;
	int			ntups;
	int			i;
	StringInfo query;
	AccessMethodInfo *aminfo;
	int			i_tableoid;
	int			i_oid;
	int			i_amname;
	int			i_amhandler;
	int			i_amtype;

	/* Before 9.6, there are no user-defined access methods */
	if (fout->remoteVersion < 90600)
	{
		*numAccessMethods = 0;
		return NULL;
	}

	query = createStringInfo();

	/* Make sure we are in proper schema */
	selectSourceSchema(fout, "pg_catalog");

	/* Select all access methods from pg_am table */
	appendStringInfo(query, "SELECT tableoid, oid, amname, amtype, "
					  "amhandler::pg_catalog.regproc AS amhandler "
					  "FROM pg_am");

	res = ExecuteSqlQuery(fout, query->data);

	ntups = dbms_md_get_tuple_num(res);
	*numAccessMethods = ntups;

	aminfo = (AccessMethodInfo *) dbms_md_malloc0(fout->memory_ctx, ntups * sizeof(AccessMethodInfo));

	i_tableoid = dbms_md_get_field_subscript(res, "tableoid");
	i_oid = dbms_md_get_field_subscript(res, "oid");
	i_amname = dbms_md_get_field_subscript(res, "amname");
	i_amhandler = dbms_md_get_field_subscript(res, "amhandler");
	i_amtype = dbms_md_get_field_subscript(res, "amtype");

	for (i = 0; i < ntups; i++)
	{
		aminfo[i].dobj.objType = DO_ACCESS_METHOD;
		aminfo[i].dobj.catId.tableoid = dbms_md_str_to_oid(dbms_md_get_field_value(res, i, i_tableoid));
		aminfo[i].dobj.catId.oid = dbms_md_str_to_oid(dbms_md_get_field_value(res, i, i_oid));
		AssignDumpId(fout, &aminfo[i].dobj);
		aminfo[i].dobj.name = dbms_md_strdup(dbms_md_get_field_value(res, i, i_amname));
		aminfo[i].dobj.namespace = NULL;
		aminfo[i].amhandler = dbms_md_strdup(dbms_md_get_field_value(res, i, i_amhandler));
		aminfo[i].amtype = *(dbms_md_get_field_value(res, i, i_amtype));

		/* Decide whether we want to dump it */
		selectDumpableAccessMethod(&(aminfo[i]), fout);

		/* Access methods do not currently have ACLs. */
		aminfo[i].dobj.dump &= ~DUMP_COMPONENT_ACL;
	}

	dbms_md_free_tuples(res);

	destroyStringInfo(query);
	pfree(query);

	return aminfo;
}


/*
 * getOpclasses:
 *	  read all opclasses in the system catalogs and return them in the
 * OpclassInfo* structure
 *
 *	numOpclasses is set to the number of opclasses read in
 */
OpclassInfo *
getOpclasses(Archive *fout, int *numOpclasses)
{
	SPITupleTable   *res;
	int			ntups;
	int			i;
	StringInfo query = createStringInfo();
	OpclassInfo *opcinfo;
	int			i_tableoid;
	int			i_oid;
	int			i_opcname;
	int			i_opcnamespace;
	int			i_rolname;

	/*
	 * find all opclasses, including builtin opclasses; we filter out
	 * system-defined opclasses at dump-out time.
	 */

	/* Make sure we are in proper schema */
	selectSourceSchema(fout, "pg_catalog");

	appendStringInfo(query, "SELECT tableoid, oid, opcname, "
					  "opcnamespace, "
					  "(%s opcowner) AS rolname "
					  "FROM pg_opclass",
					  username_subquery);

	res = ExecuteSqlQuery(fout, query->data);

	ntups = dbms_md_get_tuple_num(res);
	*numOpclasses = ntups;

	opcinfo = (OpclassInfo *) dbms_md_malloc0(fout->memory_ctx, ntups * sizeof(OpclassInfo));

	i_tableoid = dbms_md_get_field_subscript(res, "tableoid");
	i_oid = dbms_md_get_field_subscript(res, "oid");
	i_opcname = dbms_md_get_field_subscript(res, "opcname");
	i_opcnamespace = dbms_md_get_field_subscript(res, "opcnamespace");
	i_rolname = dbms_md_get_field_subscript(res, "rolname");

	for (i = 0; i < ntups; i++)
	{
		opcinfo[i].dobj.objType = DO_OPCLASS;
		opcinfo[i].dobj.catId.tableoid = dbms_md_str_to_oid(dbms_md_get_field_value(res, i, i_tableoid));
		opcinfo[i].dobj.catId.oid = dbms_md_str_to_oid(dbms_md_get_field_value(res, i, i_oid));
		AssignDumpId(fout, &opcinfo[i].dobj);
		opcinfo[i].dobj.name = dbms_md_strdup(dbms_md_get_field_value(res, i, i_opcname));
		opcinfo[i].dobj.namespace =
			findNamespace(fout,
						  dbms_md_str_to_oid(dbms_md_get_field_value(res, i, i_opcnamespace)));
		opcinfo[i].rolname = dbms_md_strdup(dbms_md_get_field_value(res, i, i_rolname));

		/* Decide whether we want to dump it */
		selectDumpableObject(&(opcinfo[i].dobj), fout, DBMS_MD_DOBJ_BUTT);

		/* Op Classes do not currently have ACLs. */
		opcinfo[i].dobj.dump &= ~DUMP_COMPONENT_ACL;

		if (strlen(opcinfo[i].rolname) == 0)
			write_msg(NULL, "WARNING: owner of operator class \"%s\" appears to be invalid\n",
					  opcinfo[i].dobj.name);
	}

	dbms_md_free_tuples(res);

	destroyStringInfo(query);
	pfree(query);

	return opcinfo;
}

/*
 * getOpfamilies:
 *	  read all opfamilies in the system catalogs and return them in the
 * OpfamilyInfo* structure
 *
 *	numOpfamilies is set to the number of opfamilies read in
 */
OpfamilyInfo *
getOpfamilies(Archive *fout, int *numOpfamilies)
{
	SPITupleTable   *res;
	int			ntups;
	int			i;
	StringInfo query;
	OpfamilyInfo *opfinfo;
	int			i_tableoid;
	int			i_oid;
	int			i_opfname;
	int			i_opfnamespace;
	int			i_rolname;

	/* Before 8.3, there is no separate concept of opfamilies */
	if (fout->remoteVersion < 80300)
	{
		*numOpfamilies = 0;
		return NULL;
	}

	query = createStringInfo();

	/*
	 * find all opfamilies, including builtin opfamilies; we filter out
	 * system-defined opfamilies at dump-out time.
	 */

	/* Make sure we are in proper schema */
	selectSourceSchema(fout, "pg_catalog");

	appendStringInfo(query, "SELECT tableoid, oid, opfname, "
					  "opfnamespace, "
					  "(%s opfowner) AS rolname "
					  "FROM pg_opfamily",
					  username_subquery);

	res = ExecuteSqlQuery(fout, query->data);

	ntups = dbms_md_get_tuple_num(res);
	*numOpfamilies = ntups;

	opfinfo = (OpfamilyInfo *) dbms_md_malloc0(fout->memory_ctx, ntups * sizeof(OpfamilyInfo));

	i_tableoid = dbms_md_get_field_subscript(res, "tableoid");
	i_oid = dbms_md_get_field_subscript(res, "oid");
	i_opfname = dbms_md_get_field_subscript(res, "opfname");
	i_opfnamespace = dbms_md_get_field_subscript(res, "opfnamespace");
	i_rolname = dbms_md_get_field_subscript(res, "rolname");

	for (i = 0; i < ntups; i++)
	{
		opfinfo[i].dobj.objType = DO_OPFAMILY;
		opfinfo[i].dobj.catId.tableoid = dbms_md_str_to_oid(dbms_md_get_field_value(res, i, i_tableoid));
		opfinfo[i].dobj.catId.oid = dbms_md_str_to_oid(dbms_md_get_field_value(res, i, i_oid));
		AssignDumpId(fout, &opfinfo[i].dobj);
		opfinfo[i].dobj.name = dbms_md_strdup(dbms_md_get_field_value(res, i, i_opfname));
		opfinfo[i].dobj.namespace =
			findNamespace(fout,
						  dbms_md_str_to_oid(dbms_md_get_field_value(res, i, i_opfnamespace)));
		opfinfo[i].rolname = dbms_md_strdup(dbms_md_get_field_value(res, i, i_rolname));

		/* Decide whether we want to dump it */
		selectDumpableObject(&(opfinfo[i].dobj), fout, DBMS_MD_DOBJ_BUTT);

		/* Extensions do not currently have ACLs. */
		opfinfo[i].dobj.dump &= ~DUMP_COMPONENT_ACL;

		if (strlen(opfinfo[i].rolname) == 0)
			write_msg(NULL, "WARNING: owner of operator family \"%s\" appears to be invalid\n",
					  opfinfo[i].dobj.name);
	}

	dbms_md_free_tuples(res);

	destroyStringInfo(query);
	pfree(query);

	return opfinfo;
}

/*
 * getAggregates:
 *	  read all the user-defined aggregates in the system catalogs and
 * return them in the AggInfo* structure
 *
 * numAggs is set to the number of aggregates read in
 */
AggInfo *
getAggregates(Archive *fout, int *numAggs)
{
	DumpOptions *dopt = fout->dopt;
	SPITupleTable   *res;
	int			ntups;
	int			i;
	StringInfo query = createStringInfo();
	AggInfo    *agginfo;
	int			i_tableoid;
	int			i_oid;
	int			i_aggname;
	int			i_aggnamespace;
	int			i_pronargs;
	int			i_proargtypes;
	int			i_rolname;
	int			i_aggacl;
	int			i_raggacl;
	int			i_initaggacl;
	int			i_initraggacl;

	/* Make sure we are in proper schema */
	selectSourceSchema(fout, "pg_catalog");

	/*
	 * Find all interesting aggregates.  See comment in getFuncs() for the
	 * rationale behind the filtering logic.
	 */
	if (fout->remoteVersion >= 90600)
	{
		StringInfo acl_subquery = createStringInfo();
		StringInfo racl_subquery = createStringInfo();
		StringInfo initacl_subquery = createStringInfo();
		StringInfo initracl_subquery = createStringInfo();
		const char *agg_check;

		buildACLQueries(acl_subquery, racl_subquery, initacl_subquery,
						initracl_subquery, "p.proacl", "p.proowner", "'f'",
						dopt->binary_upgrade);

		agg_check = (fout->remoteVersion >= 100000 ? "p.prokind = 'a'"
					 : "p.proisagg");

		appendStringInfo(query, "SELECT p.tableoid, p.oid, "
						  "p.proname AS aggname, "
						  "p.pronamespace AS aggnamespace, "
						  "p.pronargs, p.proargtypes, "
						  "(%s p.proowner) AS rolname, "
						  "%s AS aggacl, "
						  "%s AS raggacl, "
						  "%s AS initaggacl, "
						  "%s AS initraggacl "
						  "FROM pg_proc p "
						  "LEFT JOIN pg_init_privs pip ON "
						  "(p.oid = pip.objoid "
						  "AND pip.classoid = 'pg_proc'::regclass "
						  "AND pip.objsubid = 0) "
						  "WHERE %s AND ("
						  "p.pronamespace != "
						  "(SELECT oid FROM pg_namespace "
						  "WHERE nspname = 'pg_catalog') OR "
						  "p.proacl IS DISTINCT FROM pip.initprivs",
						  username_subquery,
						  acl_subquery->data,
						  racl_subquery->data,
						  initacl_subquery->data,
						  initracl_subquery->data,
						  agg_check);
		if (dopt->binary_upgrade)
			appendStringInfoString(query,
								 " OR EXISTS(SELECT 1 FROM pg_depend WHERE "
								 "classid = 'pg_proc'::regclass AND "
								 "objid = p.oid AND "
								 "refclassid = 'pg_extension'::regclass AND "
								 "deptype = 'e')");
		appendStringInfoChar(query, ')');

		destroyStringInfo(acl_subquery);
		pfree(acl_subquery);
		destroyStringInfo(racl_subquery);
		pfree(racl_subquery);
		destroyStringInfo(initacl_subquery);
		pfree(initacl_subquery);
		destroyStringInfo(initracl_subquery);
		pfree(initracl_subquery);
	}
	else if (fout->remoteVersion >= 80200)
	{
		appendStringInfo(query, "SELECT tableoid, oid, proname AS aggname, "
						  "pronamespace AS aggnamespace, "
						  "pronargs, proargtypes, "
						  "(%s proowner) AS rolname, "
						  "proacl AS aggacl, "
						  "NULL AS raggacl, "
						  "NULL AS initaggacl, NULL AS initraggacl "
						  "FROM pg_proc p "
						  "WHERE proisagg AND ("
						  "pronamespace != "
						  "(SELECT oid FROM pg_namespace "
						  "WHERE nspname = 'pg_catalog')",
						  username_subquery);
		if (dopt->binary_upgrade && fout->remoteVersion >= 90100)
			appendStringInfoString(query,
								 " OR EXISTS(SELECT 1 FROM pg_depend WHERE "
								 "classid = 'pg_proc'::regclass AND "
								 "objid = p.oid AND "
								 "refclassid = 'pg_extension'::regclass AND "
								 "deptype = 'e')");
		appendStringInfoChar(query, ')');
	}
	else
	{
		appendStringInfo(query, "SELECT tableoid, oid, proname AS aggname, "
						  "pronamespace AS aggnamespace, "
						  "CASE WHEN proargtypes[0] = 'pg_catalog.\"any\"'::pg_catalog.regtype THEN 0 ELSE 1 END AS pronargs, "
						  "proargtypes, "
						  "(%s proowner) AS rolname, "
						  "proacl AS aggacl, "
						  "NULL AS raggacl, "
						  "NULL AS initaggacl, NULL AS initraggacl "
						  "FROM pg_proc "
						  "WHERE proisagg "
						  "AND pronamespace != "
						  "(SELECT oid FROM pg_namespace WHERE nspname = 'pg_catalog')",
						  username_subquery);
	}

	res = ExecuteSqlQuery(fout, query->data);

	ntups = dbms_md_get_tuple_num(res);
	*numAggs = ntups;

	agginfo = (AggInfo *) dbms_md_malloc0(fout->memory_ctx, ntups * sizeof(AggInfo));

	i_tableoid = dbms_md_get_field_subscript(res, "tableoid");
	i_oid = dbms_md_get_field_subscript(res, "oid");
	i_aggname = dbms_md_get_field_subscript(res, "aggname");
	i_aggnamespace = dbms_md_get_field_subscript(res, "aggnamespace");
	i_pronargs = dbms_md_get_field_subscript(res, "pronargs");
	i_proargtypes = dbms_md_get_field_subscript(res, "proargtypes");
	i_rolname = dbms_md_get_field_subscript(res, "rolname");
	i_aggacl = dbms_md_get_field_subscript(res, "aggacl");
	i_raggacl = dbms_md_get_field_subscript(res, "raggacl");
	i_initaggacl = dbms_md_get_field_subscript(res, "initaggacl");
	i_initraggacl = dbms_md_get_field_subscript(res, "initraggacl");

	for (i = 0; i < ntups; i++)
	{
		agginfo[i].aggfn.dobj.objType = DO_AGG;
		agginfo[i].aggfn.dobj.catId.tableoid = dbms_md_str_to_oid(dbms_md_get_field_value(res, i, i_tableoid));
		agginfo[i].aggfn.dobj.catId.oid = dbms_md_str_to_oid(dbms_md_get_field_value(res, i, i_oid));
		AssignDumpId(fout, &agginfo[i].aggfn.dobj);
		agginfo[i].aggfn.dobj.name = 
			dbms_md_strdup(dbms_md_get_field_value(res, i, i_aggname));
		agginfo[i].aggfn.dobj.namespace =
			findNamespace(fout,
						  dbms_md_str_to_oid(dbms_md_get_field_value(res, i, i_aggnamespace)));
		agginfo[i].aggfn.rolname = dbms_md_strdup(dbms_md_get_field_value(res, i, i_rolname));
		if (strlen(agginfo[i].aggfn.rolname) == 0)
			write_msg(NULL, "WARNING: owner of aggregate function \"%s\" appears to be invalid\n",
					  agginfo[i].aggfn.dobj.name);
		agginfo[i].aggfn.lang = InvalidOid; /* not currently interesting */
		agginfo[i].aggfn.prorettype = InvalidOid;	/* not saved */
		agginfo[i].aggfn.proacl = dbms_md_strdup(dbms_md_get_field_value(res, i, i_aggacl));
		agginfo[i].aggfn.rproacl = dbms_md_strdup(dbms_md_get_field_value(res, i, i_raggacl));
		agginfo[i].aggfn.initproacl = dbms_md_strdup(dbms_md_get_field_value(res, i, i_initaggacl));
		agginfo[i].aggfn.initrproacl = dbms_md_strdup(dbms_md_get_field_value(res, i, i_initraggacl));
		agginfo[i].aggfn.nargs = atoi(dbms_md_get_field_value(res, i, i_pronargs));
		if (agginfo[i].aggfn.nargs == 0)
			agginfo[i].aggfn.argtypes = NULL;
		else
		{
			agginfo[i].aggfn.argtypes = (Oid *) dbms_md_malloc0(fout->memory_ctx, 
												agginfo[i].aggfn.nargs * sizeof(Oid));
			parseOidArray(dbms_md_get_field_value(res, i, i_proargtypes),
						  agginfo[i].aggfn.argtypes,
						  agginfo[i].aggfn.nargs);
		}

		/* Decide whether we want to dump it */
		selectDumpableObject(&(agginfo[i].aggfn.dobj), fout, DBMS_MD_DOBJ_BUTT);

		/* Do not try to dump ACL if no ACL exists. */
		if (dbms_md_is_tuple_field_null(res, i, i_aggacl) && dbms_md_is_tuple_field_null(res, i, i_raggacl) &&
			dbms_md_is_tuple_field_null(res, i, i_initaggacl) &&
			dbms_md_is_tuple_field_null(res, i, i_initraggacl))
			agginfo[i].aggfn.dobj.dump &= ~DUMP_COMPONENT_ACL;
	}

	dbms_md_free_tuples(res);

	destroyStringInfo(query);
	pfree(query);

	return agginfo;
}

/*
 * getFuncs:
 *	  read all the user-defined functions in the system catalogs and
 * return them in the FuncInfo* structure
 *
 * numFuncs is set to the number of functions read in
 */
FuncInfo *
getFuncs(Archive *fout, int *numFuncs)
{
	DumpOptions *dopt = fout->dopt;
	SPITupleTable   *res;
	int			ntups;
	int			i;
	StringInfo query = createStringInfo();
	FuncInfo   *finfo;
	int			i_tableoid;
	int			i_oid;
	int			i_proname;
	int			i_pronamespace;
	int			i_rolname;
	int			i_prolang;
	int			i_pronargs;
	int			i_proargtypes;
	int			i_prorettype;
	int			i_proacl;
	int			i_rproacl;
	int			i_initproacl;
	int			i_initrproacl;

	/* Make sure we are in proper schema */
	selectSourceSchema(fout, "pg_catalog");

	/*
	 * Find all interesting functions.  This is a bit complicated:
	 *
	 * 1. Always exclude aggregates; those are handled elsewhere.
	 *
	 * 2. Always exclude functions that are internally dependent on something
	 * else, since presumably those will be created as a result of creating
	 * the something else.  This currently acts only to suppress constructor
	 * functions for range types (so we only need it in 9.2 and up).  Note
	 * this is OK only because the constructors don't have any dependencies
	 * the range type doesn't have; otherwise we might not get creation
	 * ordering correct.
	 *
	 * 3. Otherwise, we normally exclude functions in pg_catalog.  However, if
	 * they're members of extensions and we are in binary-upgrade mode then
	 * include them, since we want to dump extension members individually in
	 * that mode.  Also, if they are used by casts or transforms then we need
	 * to gather the information about them, though they won't be dumped if
	 * they are built-in.  Also, in 9.6 and up, include functions in
	 * pg_catalog if they have an ACL different from what's shown in
	 * pg_init_privs.
	 */
	if (fout->remoteVersion >= 90600)
	{
		StringInfo acl_subquery = createStringInfo();
		StringInfo racl_subquery = createStringInfo();
		StringInfo initacl_subquery = createStringInfo();
		StringInfo initracl_subquery = createStringInfo();
		const char *not_agg_check;

		buildACLQueries(acl_subquery, racl_subquery, initacl_subquery,
						initracl_subquery, "p.proacl", "p.proowner", "'f'",
						dopt->binary_upgrade);

		not_agg_check = (fout->remoteVersion >= 100000 ? "p.prokind <> 'a'"
						 : "NOT p.proisagg");

		appendStringInfo(query,
						  "SELECT p.tableoid, p.oid, p.proname, p.prolang, "
						  "p.pronargs, p.proargtypes, p.prorettype, "
						  "%s AS proacl, "
						  "%s AS rproacl, "
						  "%s AS initproacl, "
						  "%s AS initrproacl, "
						  "p.pronamespace, "
						  "(%s p.proowner) AS rolname "
						  "FROM pg_proc p "
						  "LEFT JOIN pg_init_privs pip ON "
						  "(p.oid = pip.objoid "
						  "AND pip.classoid = 'pg_proc'::regclass "
						  "AND pip.objsubid = 0) "
						  "WHERE %s"
						  "\n  AND NOT EXISTS (SELECT 1 FROM pg_depend "
						  "WHERE classid = 'pg_proc'::regclass AND "
						  "objid = p.oid AND deptype = 'i')"
						  "\n  AND ("
						  "\n  pronamespace != "
						  "(SELECT oid FROM pg_namespace "
						  "WHERE nspname = 'pg_catalog')"
						  "\n  OR EXISTS (SELECT 1 FROM pg_cast"
						  "\n  WHERE pg_cast.oid > %u "
						  "\n  AND p.oid = pg_cast.castfunc)"
						  "\n  OR EXISTS (SELECT 1 FROM pg_transform"
						  "\n  WHERE pg_transform.oid > %u AND "
						  "\n  (p.oid = pg_transform.trffromsql"
						  "\n  OR p.oid = pg_transform.trftosql))",
						  acl_subquery->data,
						  racl_subquery->data,
						  initacl_subquery->data,
						  initracl_subquery->data,
						  username_subquery,
						  not_agg_check,
						  g_last_builtin_oid,
						  g_last_builtin_oid);
		if (dopt->binary_upgrade)
			appendStringInfoString(query,
								 "\n  OR EXISTS(SELECT 1 FROM pg_depend WHERE "
								 "classid = 'pg_proc'::regclass AND "
								 "objid = p.oid AND "
								 "refclassid = 'pg_extension'::regclass AND "
								 "deptype = 'e')");
		appendStringInfoString(query,
							 "\n  OR p.proacl IS DISTINCT FROM pip.initprivs");
		appendStringInfoChar(query, ')');

		destroyStringInfo(acl_subquery);
		pfree(acl_subquery);
		destroyStringInfo(racl_subquery);
		pfree(racl_subquery);
		destroyStringInfo(initacl_subquery);
		pfree(initacl_subquery);
		destroyStringInfo(initracl_subquery);
		pfree(initracl_subquery);
	}
	else
	{
		appendStringInfo(query,
						  "SELECT tableoid, oid, proname, prolang, "
						  "pronargs, proargtypes, prorettype, proacl, "
						  "NULL as rproacl, "
						  "NULL as initproacl, NULL AS initrproacl, "
						  "pronamespace, "
						  "(%s proowner) AS rolname "
						  "FROM pg_proc p "
						  "WHERE NOT proisagg",
						  username_subquery);
		if (fout->remoteVersion >= 90200)
			appendStringInfoString(query,
								 "\n  AND NOT EXISTS (SELECT 1 FROM pg_depend "
								 "WHERE classid = 'pg_proc'::regclass AND "
								 "objid = p.oid AND deptype = 'i')");
		appendStringInfo(query,
						  "\n  AND ("
						  "\n  pronamespace != "
						  "(SELECT oid FROM pg_namespace "
						  "WHERE nspname = 'pg_catalog')"
						  "\n  OR EXISTS (SELECT 1 FROM pg_cast"
						  "\n  WHERE pg_cast.oid > '%u'::oid"
						  "\n  AND p.oid = pg_cast.castfunc)",
						  g_last_builtin_oid);

		if (fout->remoteVersion >= 90500)
			appendStringInfo(query,
							  "\n  OR EXISTS (SELECT 1 FROM pg_transform"
							  "\n  WHERE pg_transform.oid > '%u'::oid"
							  "\n  AND (p.oid = pg_transform.trffromsql"
							  "\n  OR p.oid = pg_transform.trftosql))",
							  g_last_builtin_oid);

		if (dopt->binary_upgrade && fout->remoteVersion >= 90100)
			appendStringInfoString(query,
								 "\n  OR EXISTS(SELECT 1 FROM pg_depend WHERE "
								 "classid = 'pg_proc'::regclass AND "
								 "objid = p.oid AND "
								 "refclassid = 'pg_extension'::regclass AND "
								 "deptype = 'e')");
		appendStringInfoChar(query, ')');
	}

	res = ExecuteSqlQuery(fout, query->data);

	ntups = dbms_md_get_tuple_num(res);

	*numFuncs = ntups;

	finfo = (FuncInfo *) dbms_md_malloc0(fout->memory_ctx, ntups * sizeof(FuncInfo));

	i_tableoid = dbms_md_get_field_subscript(res, "tableoid");
	i_oid = dbms_md_get_field_subscript(res, "oid");
	i_proname = dbms_md_get_field_subscript(res, "proname");
	i_pronamespace = dbms_md_get_field_subscript(res, "pronamespace");
	i_rolname = dbms_md_get_field_subscript(res, "rolname");
	i_prolang = dbms_md_get_field_subscript(res, "prolang");
	i_pronargs = dbms_md_get_field_subscript(res, "pronargs");
	i_proargtypes = dbms_md_get_field_subscript(res, "proargtypes");
	i_prorettype = dbms_md_get_field_subscript(res, "prorettype");
	i_proacl = dbms_md_get_field_subscript(res, "proacl");
	i_rproacl = dbms_md_get_field_subscript(res, "rproacl");
	i_initproacl = dbms_md_get_field_subscript(res, "initproacl");
	i_initrproacl = dbms_md_get_field_subscript(res, "initrproacl");

	for (i = 0; i < ntups; i++)
	{
		finfo[i].dobj.objType = DO_FUNC;
		finfo[i].dobj.catId.tableoid = dbms_md_str_to_oid(dbms_md_get_field_value(res, i, i_tableoid));
		finfo[i].dobj.catId.oid = dbms_md_str_to_oid(dbms_md_get_field_value(res, i, i_oid));
		AssignDumpId(fout, &finfo[i].dobj);
		finfo[i].dobj.name = dbms_md_strdup(dbms_md_get_field_value(res, i, i_proname));
		finfo[i].dobj.namespace =
			findNamespace(fout,
						  dbms_md_str_to_oid(dbms_md_get_field_value(res, i, i_pronamespace)));
		finfo[i].rolname = dbms_md_strdup(dbms_md_get_field_value(res, i, i_rolname));
		finfo[i].lang = dbms_md_str_to_oid(dbms_md_get_field_value(res, i, i_prolang));
		finfo[i].prorettype = dbms_md_str_to_oid(dbms_md_get_field_value(res, i, i_prorettype));
		finfo[i].proacl = dbms_md_strdup(dbms_md_get_field_value(res, i, i_proacl));
		finfo[i].rproacl = dbms_md_strdup(dbms_md_get_field_value(res, i, i_rproacl));
		finfo[i].initproacl = dbms_md_strdup(dbms_md_get_field_value(res, i, i_initproacl));
		finfo[i].initrproacl = dbms_md_strdup(dbms_md_get_field_value(res, i, i_initrproacl));
		finfo[i].nargs = atoi(dbms_md_get_field_value(res, i, i_pronargs));
		if (finfo[i].nargs == 0)
			finfo[i].argtypes = NULL;
		else
		{
			finfo[i].argtypes = (Oid *) dbms_md_malloc0(fout->memory_ctx, 
													finfo[i].nargs * sizeof(Oid));
			parseOidArray(dbms_md_get_field_value(res, i, i_proargtypes),
						  finfo[i].argtypes, finfo[i].nargs);
		}

		/* Decide whether we want to dump it */
		selectDumpableFunc(&(finfo[i]), fout);

		/* Do not try to dump ACL if no ACL exists. */
		if (dbms_md_is_tuple_field_null(res, i, i_proacl) 
			&& dbms_md_is_tuple_field_null(res, i, i_rproacl) 
			&& dbms_md_is_tuple_field_null(res, i, i_initproacl) 
			&&dbms_md_is_tuple_field_null(res, i, i_initrproacl))
			finfo[i].dobj.dump &= ~DUMP_COMPONENT_ACL;

		if (strlen(finfo[i].rolname) == 0)
			write_msg(NULL,
					  "WARNING: owner of function \"%s\" appears to be invalid\n",
					  finfo[i].dobj.name);
	}

	dbms_md_free_tuples(res);

	destroyStringInfo(query);
	pfree(query);

	return finfo;
}

#ifdef __OPENTENBASE__
static void getKeyValues(Archive *fout, int32 ntable, TableInfo *tblinfo)
{
	if (tblinfo)
	{
		int i;
		for (i = 0; i < ntable; i++)
		{
			int32    ntups;
			int32    tupid;
			int32    i_schema;
			int32    i_table;
			SPITupleTable *res;
			tblinfo[i].nKeys = 0;
			if (RELKIND_RELATION == tblinfo[i].relkind)
			{
				StringInfo query = createStringInfo();
				
				/* chooose proper schema */
				selectSourceSchema(fout, "pg_catalog");
				appendStringInfo(query,
								"select " 	
									"n.nspname as schema, "	
									"c.relname as table "
								"FROM pg_class c ,pg_namespace n "
								"WHERE c.relnamespace = n.oid and c.oid = %u;",
								tblinfo[i].dobj.catId.oid);
				/*
				appendStringInfo(query,
						   "select n.nspname as schema,"
       					   "c.relname as table ," 
   						   "k.keyvalue as keyvalue ,"
   					       "g.group_name as hotgroup," 
   						   "CASE WHEN k.coldnodegroup  = 0 THEN NULL ELSE (SELECT g1.group_name from pgxc_group g1, pgxc_key_value k1 where k1.coldnodegroup = g1.oid and k1.reloid = %u and k1.keyvalue = k.keyvalue) END as coldgroup from pgxc_key_value k, pg_class c, pg_namespace n, pgxc_group g " 
 						   "where c.oid = k.reloid and c.relnamespace = n.oid and k.nodegroup = g.oid and c.oid = %u;",
							tblinfo[i].dobj.catId.oid,
						    tblinfo[i].dobj.catId.oid);
						    */
				res         = ExecuteSqlQuery(fout, query->data);
				ntups 	    = dbms_md_get_tuple_num(res);
				if (ntups)
				{
					i_schema    = dbms_md_get_field_subscript(res, "schema");
					i_table     = dbms_md_get_field_subscript(res, "table");
					//i_keyvalue  = dbms_md_get_field_subscript(res, "keyvalue");
					//i_hotgroup  = dbms_md_get_field_subscript(res, "hotgroup");
					//i_coldgroup = dbms_md_get_field_subscript(res, "coldgroup");
					tblinfo[i].nKeys    = ntups;
					tblinfo[i].keyvalue = (KeyValueInfo *) dbms_md_malloc0(fout->memory_ctx, 
														ntups * sizeof(KeyValueInfo));
					for (tupid = 0; tupid < ntups; tupid++)
					{
						tblinfo[i].keyvalue[tupid].schema   = dbms_md_strdup(dbms_md_get_field_value(res, tupid, i_schema));
						tblinfo[i].keyvalue[tupid].table    = dbms_md_strdup(dbms_md_get_field_value(res, tupid, i_table));
						/*tblinfo[i].keyvalue[tupid].keyvalue = dbms_md_strdup(dbms_md_get_field_value(res, tupid, i_keyvalue));
						tblinfo[i].keyvalue[tupid].hotdisgroup = dbms_md_strdup(dbms_md_get_field_value(res, tupid, i_hotgroup));
						if (dbms_md_get_field_value(res, tupid, i_coldgroup))
						{
							tblinfo[i].keyvalue[tupid].colddisgroup = dbms_md_strdup(dbms_md_get_field_value(res, tupid, i_coldgroup));
						}*/
					}
				}
				dbms_md_free_tuples(res);

				destroyStringInfo(query);
			}
		}
	}
}
#endif

/*
 * getTables
 *	  read all the tables (no indexes)
 * in the system catalogs return them in the TableInfo* structure
 *
 * numTables is set to the number of tables read in
 */
TableInfo *
getTables(Archive *fout, int *numTables)
{
	DumpOptions *dopt = fout->dopt;
	SPITupleTable   *res;
	int			ntups;
	int			i;
	StringInfo query = createStringInfo();
	TableInfo  *tblinfo;
	int			i_reltableoid;
	int			i_reloid;
	int			i_relname;
	int			i_relnamespace;
	int			i_relkind;
	int			i_relacl;
	int			i_rrelacl;
	int			i_initrelacl;
	int			i_initrrelacl;
	int			i_rolname;
	int			i_relchecks;
	int			i_relhastriggers;
	int			i_relhasindex;
	int			i_relhasrules;
	int			i_relrowsec;
	int			i_relforcerowsec;
	int			i_relhasoids;
	int			i_relfrozenxid;
	int			i_relminmxid;
	int			i_toastoid;
	int			i_toastfrozenxid;
	int			i_toastminmxid;
	int			i_relpersistence;
	int			i_relispopulated;
	int			i_relreplident;
	int			i_owning_tab;
	int			i_owning_col;
#ifdef __OPENTENBASE__
	int 		i_parttype;
	int 		i_partattnum;
#endif
#ifdef PGXC
	int			i_pgxclocatortype;
	int			i_pgxcattnum;
	int         i_pgxcsecattnum;
	int			i_pgxc_node_names;
#endif
	int			i_reltablespace;
	int			i_reloptions;
	int			i_checkoption;
	int			i_toastreloptions;
	int			i_reloftype;
	int			i_relpages;
	int			i_is_identity_sequence;
	int			i_changed_acl;
	int			i_partkeydef;
	int			i_ispartition;
	int			i_partbound;

	/* Make sure we are in proper schema */
	selectSourceSchema(fout, "pg_catalog");

	/*
	 * Find all the tables and table-like objects.
	 *
	 * We include system catalogs, so that we can work if a user table is
	 * defined to inherit from a system catalog (pretty weird, but...)
	 *
	 * We ignore relations that are not ordinary tables, sequences, views,
	 * materialized views, composite types, or foreign tables.
	 *
	 * Composite-type table entries won't be dumped as such, but we have to
	 * make a DumpableObject for them so that we can track dependencies of the
	 * composite type (pg_depend entries for columns of the composite type
	 * link to the pg_class entry not the pg_type entry).
	 *
	 * Note: in this phase we should collect only a minimal amount of
	 * information about each table, basically just enough to decide if it is
	 * interesting. We must fetch all tables in this phase because otherwise
	 * we cannot correctly identify inherited columns, owned sequences, etc.
	 *
	 * We purposefully ignore toast OIDs for partitioned tables; the reason is
	 * that versions 10 and 11 have them, but 12 does not, so emitting them
	 * causes the upgrade to fail.
	 */

	if (fout->remoteVersion >= 90600)
	{
		char	   *partkeydef = "NULL";
		char	   *ispartition = "false";
		char	   *partbound = "NULL";

		StringInfo acl_subquery = createStringInfo();
		StringInfo racl_subquery = createStringInfo();
		StringInfo initacl_subquery = createStringInfo();
		StringInfo initracl_subquery = createStringInfo();

		StringInfo attacl_subquery = createStringInfo();
		StringInfo attracl_subquery = createStringInfo();
		StringInfo attinitacl_subquery = createStringInfo();
		StringInfo attinitracl_subquery = createStringInfo();

		/*
		 * Collect the information about any partitioned tables, which were
		 * added in PG10.
		 */

		if (fout->remoteVersion >= 100000)
		{
			partkeydef = "pg_get_partkeydef(c.oid)";
			ispartition = "c.relispartition";
			partbound = "pg_get_expr(c.relpartbound, c.oid)";
		}

		/*
		 * Left join to pick up dependency info linking sequences to their
		 * owning column, if any (note this dependency is AUTO as of 8.2)
		 *
		 * Left join to detect if any privileges are still as-set-at-init, in
		 * which case we won't dump out ACL commands for those.
		 */

		buildACLQueries(acl_subquery, racl_subquery, initacl_subquery,
						initracl_subquery, "c.relacl", "c.relowner",
						"CASE WHEN c.relkind = " CppAsString2(RELKIND_SEQUENCE)
						" THEN 's' ELSE 'r' END::\"char\"", dopt->binary_upgrade);

		buildACLQueries(attacl_subquery, attracl_subquery, attinitacl_subquery,
						attinitracl_subquery, "at.attacl", "c.relowner", "'c'",
						dopt->binary_upgrade);

		appendStringInfo(query,
						  "SELECT c.tableoid, c.oid, c.relname, "
						  "%s AS relacl, %s as rrelacl, "
						  "%s AS initrelacl, %s as initrrelacl, "
						  "c.relkind, c.relnamespace, "
						  "(%s c.relowner) AS rolname, "
						  "c.relchecks, c.relhastriggers, "
						  "c.relhasindex, c.relhasrules, c.relhasoids, "
						  "c.relrowsecurity, c.relforcerowsecurity, "
						  "c.relfrozenxid, c.relminmxid, tc.oid AS toid, "
						  "tc.relfrozenxid AS tfrozenxid, "
						  "tc.relminmxid AS tminmxid, "
						  "c.relpersistence, c.relispopulated, "
						  "c.relreplident, c.relpages, "
						  "CASE WHEN c.reloftype <> 0 THEN c.reloftype::pg_catalog.regtype ELSE NULL END AS reloftype, "
						  "d.refobjid AS owning_tab, "
						  "d.refobjsubid AS owning_col, "
						  "(SELECT spcname FROM pg_tablespace t WHERE t.oid = c.reltablespace) AS reltablespace, "
#ifdef PGXC
						  "%s"
#endif
#ifdef __OPENTENBASE__
						 "c.relpartkind AS parttype, "
					      	"(select group_name from pgxc_class, pgxc_group where pcrelid = c.oid and pgroup = pgxc_group.oid) As groupname, "
					      	"(select group_name from pgxc_class, pgxc_group where pcrelid = c.oid and pcoldgroup = pgxc_group.oid) As coldgroupname, "
#endif

						  "array_remove(array_remove(c.reloptions,'check_option=local'),'check_option=cascaded') AS reloptions, "
						  "CASE WHEN 'check_option=local' = ANY (c.reloptions) THEN 'LOCAL'::text "
						  "WHEN 'check_option=cascaded' = ANY (c.reloptions) THEN 'CASCADED'::text ELSE NULL END AS checkoption, "
						  "tc.reloptions AS toast_reloptions, "
						  "c.relkind = '%c' AND EXISTS (SELECT 1 FROM pg_depend WHERE classid = 'pg_class'::regclass AND objid = c.oid AND objsubid = 0 AND refclassid = 'pg_class'::regclass AND deptype = 'i') AS is_identity_sequence, "
						  "EXISTS (SELECT 1 FROM pg_attribute at LEFT JOIN pg_init_privs pip ON "
						  "(c.oid = pip.objoid "
						  "AND pip.classoid = 'pg_class'::regclass "
						  "AND pip.objsubid = at.attnum)"
						  "WHERE at.attrelid = c.oid AND ("
						  "%s IS NOT NULL "
						  "OR %s IS NOT NULL "
						  "OR %s IS NOT NULL "
						  "OR %s IS NOT NULL"
						  "))"
						  "AS changed_acl, "
						  "%s AS partkeydef, "
						  "%s AS ispartition, "
						  "%s AS partbound "
						  "FROM pg_class c "
						  "LEFT JOIN pg_depend d ON "
						  "(c.relkind = '%c' AND "
						  "d.classid = c.tableoid AND d.objid = c.oid AND "
						  "d.objsubid = 0 AND "
						  "d.refclassid = c.tableoid AND d.deptype IN ('a', 'i')) "
						  "LEFT JOIN pg_class tc ON (c.reltoastrelid = tc.oid AND c.relkind <> '%c') "
						  "LEFT JOIN pg_init_privs pip ON "
						  "(c.oid = pip.objoid "
						  "AND pip.classoid = 'pg_class'::regclass "
						  "AND pip.objsubid = 0) "
						  "WHERE c.relkind in ('%c', '%c', '%c', '%c', '%c', '%c', '%c') and c.relpartkind != 'c' "
						  "ORDER BY c.oid",
						  acl_subquery->data,
						  racl_subquery->data,
						  initacl_subquery->data,
						  initracl_subquery->data,
						  username_subquery,
						  fout->isPostgresXL
							  ?  "(SELECT pclocatortype from pgxc_class v where v.pcrelid = c.oid) AS pgxclocatortype,"
							  "(SELECT pcattnum from pgxc_class v where v.pcrelid = c.oid) AS pgxcattnum,"
							  "(SELECT psecondattnum from pgxc_class v where v.pcrelid = c.oid) AS pgxcsecattnum,"
							  "(SELECT string_agg(node_name,',') AS pgxc_node_names from pgxc_node n where n.oid in (select unnest(nodeoids) from pgxc_class v where v.pcrelid=c.oid) ) , "
							  : "",
						  RELKIND_SEQUENCE,
						  attacl_subquery->data,
						  attracl_subquery->data,
						  attinitacl_subquery->data,
						  attinitracl_subquery->data,
						  partkeydef,
						  ispartition,
						  partbound,
						  RELKIND_SEQUENCE,
						  RELKIND_PARTITIONED_TABLE,
						  RELKIND_RELATION, RELKIND_SEQUENCE,
						  RELKIND_VIEW, RELKIND_COMPOSITE_TYPE,
						  RELKIND_MATVIEW, RELKIND_FOREIGN_TABLE,
						  RELKIND_PARTITIONED_TABLE);

		destroyStringInfo(acl_subquery);
		pfree(acl_subquery);
		destroyStringInfo(racl_subquery);
		pfree(racl_subquery);
		destroyStringInfo(initacl_subquery);
		pfree(initacl_subquery);
		destroyStringInfo(initracl_subquery);
		pfree(initracl_subquery);

		destroyStringInfo(attacl_subquery);
		pfree(attacl_subquery);
		destroyStringInfo(attracl_subquery);
		pfree(attracl_subquery);
		destroyStringInfo(attinitacl_subquery);
		pfree(attinitacl_subquery);
		destroyStringInfo(attinitracl_subquery);
		pfree(attinitracl_subquery);
	}
	else if (fout->remoteVersion >= 90500)
	{
		/*
		 * Left join to pick up dependency info linking sequences to their
		 * owning column, if any (note this dependency is AUTO as of 8.2)
		 */
		appendStringInfo(query,
						  "SELECT c.tableoid, c.oid, c.relname, "
						  "c.relacl, NULL as rrelacl, "
						  "NULL AS initrelacl, NULL AS initrrelacl, "
						  "c.relkind, "
						  "c.relnamespace, "
						  "(%s c.relowner) AS rolname, "
						  "c.relchecks, c.relhastriggers, "
						  "c.relhasindex, c.relhasrules, c.relhasoids, "
						  "c.relrowsecurity, c.relforcerowsecurity, "
						  "c.relfrozenxid, c.relminmxid, tc.oid AS toid, "
						  "tc.relfrozenxid AS tfrozenxid, "
						  "tc.relminmxid AS tminmxid, "
						  "c.relpersistence, c.relispopulated, "
						  "c.relreplident, c.relpages, "
						  "CASE WHEN c.reloftype <> 0 THEN c.reloftype::pg_catalog.regtype ELSE NULL END AS reloftype, "
						  "d.refobjid AS owning_tab, "
						  "d.refobjsubid AS owning_col, "
						  "(SELECT spcname FROM pg_tablespace t WHERE t.oid = c.reltablespace) AS reltablespace, "
#ifdef PGXC
						  "%s"
#endif
						  "array_remove(array_remove(c.reloptions,'check_option=local'),'check_option=cascaded') AS reloptions, "
						  "CASE WHEN 'check_option=local' = ANY (c.reloptions) THEN 'LOCAL'::text "
						  "WHEN 'check_option=cascaded' = ANY (c.reloptions) THEN 'CASCADED'::text ELSE NULL END AS checkoption, "
						  "tc.reloptions AS toast_reloptions, "
						  "NULL AS changed_acl, "
						  "NULL AS partkeydef, "
						  "false AS ispartition, "
						  "NULL AS partbound "
						  "FROM pg_class c "
						  "LEFT JOIN pg_depend d ON "
						  "(c.relkind = '%c' AND "
						  "d.classid = c.tableoid AND d.objid = c.oid AND "
						  "d.objsubid = 0 AND "
						  "d.refclassid = c.tableoid AND d.deptype = 'a') "
						  "LEFT JOIN pg_class tc ON (c.reltoastrelid = tc.oid) "
						  "WHERE c.relkind in ('%c', '%c', '%c', '%c', '%c', '%c') "
						  "ORDER BY c.oid",
						  username_subquery,
						  fout->isPostgresXL
							  ?  "(SELECT pclocatortype from pgxc_class v where v.pcrelid = c.oid) AS pgxclocatortype,"
							  "(SELECT pcattnum from pgxc_class v where v.pcrelid = c.oid) AS pgxcattnum,"
							  "(SELECT string_agg(node_name,',') AS pgxc_node_names from pgxc_node n where n.oid in (select unnest(nodeoids) from pgxc_class v where v.pcrelid=c.oid) ) , "
							  : "",
						  RELKIND_SEQUENCE,
						  RELKIND_RELATION, RELKIND_SEQUENCE,
						  RELKIND_VIEW, RELKIND_COMPOSITE_TYPE,
						  RELKIND_MATVIEW, RELKIND_FOREIGN_TABLE);
	}
	else if (fout->remoteVersion >= 90400)
	{
		/*
		 * Left join to pick up dependency info linking sequences to their
		 * owning column, if any (note this dependency is AUTO as of 8.2)
		 */
		appendStringInfo(query,
						  "SELECT c.tableoid, c.oid, c.relname, "
						  "c.relacl, NULL as rrelacl, "
						  "NULL AS initrelacl, NULL AS initrrelacl, "
						  "c.relkind, "
						  "c.relnamespace, "
						  "(%s c.relowner) AS rolname, "
						  "c.relchecks, c.relhastriggers, "
						  "c.relhasindex, c.relhasrules, c.relhasoids, "
						  "'f'::bool AS relrowsecurity, "
						  "'f'::bool AS relforcerowsecurity, "
						  "c.relfrozenxid, c.relminmxid, tc.oid AS toid, "
						  "tc.relfrozenxid AS tfrozenxid, "
						  "tc.relminmxid AS tminmxid, "
						  "c.relpersistence, c.relispopulated, "
						  "c.relreplident, c.relpages, "
						  "CASE WHEN c.reloftype <> 0 THEN c.reloftype::pg_catalog.regtype ELSE NULL END AS reloftype, "
						  "d.refobjid AS owning_tab, "
						  "d.refobjsubid AS owning_col, "
						  "(SELECT spcname FROM pg_tablespace t WHERE t.oid = c.reltablespace) AS reltablespace, "
#ifdef PGXC
						  "%s"
#endif
						  "array_remove(array_remove(c.reloptions,'check_option=local'),'check_option=cascaded') AS reloptions, "
						  "CASE WHEN 'check_option=local' = ANY (c.reloptions) THEN 'LOCAL'::text "
						  "WHEN 'check_option=cascaded' = ANY (c.reloptions) THEN 'CASCADED'::text ELSE NULL END AS checkoption, "
						  "tc.reloptions AS toast_reloptions, "
						  "NULL AS changed_acl, "
						  "NULL AS partkeydef, "
						  "false AS ispartition, "
						  "NULL AS partbound "
						  "FROM pg_class c "
						  "LEFT JOIN pg_depend d ON "
						  "(c.relkind = '%c' AND "
						  "d.classid = c.tableoid AND d.objid = c.oid AND "
						  "d.objsubid = 0 AND "
						  "d.refclassid = c.tableoid AND d.deptype = 'a') "
						  "LEFT JOIN pg_class tc ON (c.reltoastrelid = tc.oid) "
						  "WHERE c.relkind in ('%c', '%c', '%c', '%c', '%c', '%c') "
						  "ORDER BY c.oid",
						  username_subquery,
						  fout->isPostgresXL
							  ? "(SELECT pclocatortype from pgxc_class v where v.pcrelid = c.oid) AS pgxclocatortype,"
							  "(SELECT pcattnum from pgxc_class v where v.pcrelid = c.oid) AS pgxcattnum,"
							  "(SELECT string_agg(node_name,',') AS pgxc_node_names from pgxc_node n where n.oid in (select unnest(nodeoids) from pgxc_class v where v.pcrelid=c.oid) ) , "
							  : "",
						  RELKIND_SEQUENCE,
						  RELKIND_RELATION, RELKIND_SEQUENCE,
						  RELKIND_VIEW, RELKIND_COMPOSITE_TYPE,
						  RELKIND_MATVIEW, RELKIND_FOREIGN_TABLE);
	}
	else if (fout->remoteVersion >= 90300)
	{
		/*
		 * Left join to pick up dependency info linking sequences to their
		 * owning column, if any (note this dependency is AUTO as of 8.2)
		 */
		appendStringInfo(query,
						  "SELECT c.tableoid, c.oid, c.relname, "
						  "c.relacl, NULL as rrelacl, "
						  "NULL AS initrelacl, NULL AS initrrelacl, "
						  "c.relkind, "
						  "c.relnamespace, "
						  "(%s c.relowner) AS rolname, "
						  "c.relchecks, c.relhastriggers, "
						  "c.relhasindex, c.relhasrules, c.relhasoids, "
						  "'f'::bool AS relrowsecurity, "
						  "'f'::bool AS relforcerowsecurity, "
						  "c.relfrozenxid, c.relminmxid, tc.oid AS toid, "
						  "tc.relfrozenxid AS tfrozenxid, "
						  "tc.relminmxid AS tminmxid, "
						  "c.relpersistence, c.relispopulated, "
						  "'d' AS relreplident, c.relpages, "
						  "CASE WHEN c.reloftype <> 0 THEN c.reloftype::pg_catalog.regtype ELSE NULL END AS reloftype, "
						  "d.refobjid AS owning_tab, "
						  "d.refobjsubid AS owning_col, "
						  "(SELECT spcname FROM pg_tablespace t WHERE t.oid = c.reltablespace) AS reltablespace, "
#ifdef PGXC
						  "%s"
#endif
						  "array_remove(array_remove(c.reloptions,'check_option=local'),'check_option=cascaded') AS reloptions, "
						  "CASE WHEN 'check_option=local' = ANY (c.reloptions) THEN 'LOCAL'::text "
						  "WHEN 'check_option=cascaded' = ANY (c.reloptions) THEN 'CASCADED'::text ELSE NULL END AS checkoption, "
						  "tc.reloptions AS toast_reloptions, "
						  "NULL AS changed_acl, "
						  "NULL AS partkeydef, "
						  "false AS ispartition, "
						  "NULL AS partbound "
						  "FROM pg_class c "
						  "LEFT JOIN pg_depend d ON "
						  "(c.relkind = '%c' AND "
						  "d.classid = c.tableoid AND d.objid = c.oid AND "
						  "d.objsubid = 0 AND "
						  "d.refclassid = c.tableoid AND d.deptype = 'a') "
						  "LEFT JOIN pg_class tc ON (c.reltoastrelid = tc.oid) "
						  "WHERE c.relkind in ('%c', '%c', '%c', '%c', '%c', '%c') "
						  "ORDER BY c.oid",
						  username_subquery,
						  fout->isPostgresXL
						  	? "(SELECT pclocatortype from pgxc_class v where v.pcrelid = c.oid) AS pgxclocatortype,"
							  "(SELECT pcattnum from pgxc_class v where v.pcrelid = c.oid) AS pgxcattnum,"
							  "(SELECT string_agg(node_name,',') AS pgxc_node_names from pgxc_node n where n.oid in (select unnest(nodeoids) from pgxc_class v where v.pcrelid=c.oid) ) , "
							: "",
						  RELKIND_SEQUENCE,
						  RELKIND_RELATION, RELKIND_SEQUENCE,
						  RELKIND_VIEW, RELKIND_COMPOSITE_TYPE,
						  RELKIND_MATVIEW, RELKIND_FOREIGN_TABLE);
	}
	else if (fout->remoteVersion >= 90100)
	{
		/*
		 * Left join to pick up dependency info linking sequences to their
		 * owning column, if any (note this dependency is AUTO as of 8.2)
		 */
		appendStringInfo(query,
						  "SELECT c.tableoid, c.oid, c.relname, "
						  "c.relacl, NULL as rrelacl, "
						  "NULL AS initrelacl, NULL AS initrrelacl, "
						  "c.relkind, "
						  "c.relnamespace, "
						  "(%s c.relowner) AS rolname, "
						  "c.relchecks, c.relhastriggers, "
						  "c.relhasindex, c.relhasrules, c.relhasoids, "
						  "'f'::bool AS relrowsecurity, "
						  "'f'::bool AS relforcerowsecurity, "
						  "c.relfrozenxid, 0 AS relminmxid, tc.oid AS toid, "
						  "tc.relfrozenxid AS tfrozenxid, "
						  "0 AS tminmxid, "
						  "c.relpersistence, 't' as relispopulated, "
						  "'d' AS relreplident, c.relpages, "
						  "CASE WHEN c.reloftype <> 0 THEN c.reloftype::pg_catalog.regtype ELSE NULL END AS reloftype, "
						  "d.refobjid AS owning_tab, "
						  "d.refobjsubid AS owning_col, "
						  "(SELECT spcname FROM pg_tablespace t WHERE t.oid = c.reltablespace) AS reltablespace, "
#ifdef PGXC
						  "%s"
#endif
						  "c.reloptions AS reloptions, "
						  "tc.reloptions AS toast_reloptions, "
						  "NULL AS changed_acl, "
						  "NULL AS partkeydef, "
						  "false AS ispartition, "
						  "NULL AS partbound "
						  "FROM pg_class c "
						  "LEFT JOIN pg_depend d ON "
						  "(c.relkind = '%c' AND "
						  "d.classid = c.tableoid AND d.objid = c.oid AND "
						  "d.objsubid = 0 AND "
						  "d.refclassid = c.tableoid AND d.deptype = 'a') "
						  "LEFT JOIN pg_class tc ON (c.reltoastrelid = tc.oid) "
						  "WHERE c.relkind in ('%c', '%c', '%c', '%c', '%c', '%c') "
						  "ORDER BY c.oid",
						  username_subquery,
						  fout->isPostgresXL
							  ?  "(SELECT pclocatortype from pgxc_class v where v.pcrelid = c.oid) AS pgxclocatortype,"
							  "(SELECT pcattnum from pgxc_class v where v.pcrelid = c.oid) AS pgxcattnum,"
							  "(SELECT string_agg(node_name,',') AS pgxc_node_names from pgxc_node n where n.oid in (select unnest(nodeoids) from pgxc_class v where v.pcrelid=c.oid) ) , "
							  : "",
						  RELKIND_SEQUENCE,
						  RELKIND_RELATION, RELKIND_SEQUENCE,
						  RELKIND_VIEW, RELKIND_COMPOSITE_TYPE,
						  RELKIND_MATVIEW, RELKIND_FOREIGN_TABLE);
	}
	else if (fout->remoteVersion >= 90000)
	{
		/*
		 * Left join to pick up dependency info linking sequences to their
		 * owning column, if any (note this dependency is AUTO as of 8.2)
		 */
		appendStringInfo(query,
						  "SELECT c.tableoid, c.oid, c.relname, "
						  "c.relacl, NULL as rrelacl, "
						  "NULL AS initrelacl, NULL AS initrrelacl, "
						  "c.relkind, "
						  "c.relnamespace, "
						  "(%s c.relowner) AS rolname, "
						  "c.relchecks, c.relhastriggers, "
						  "c.relhasindex, c.relhasrules, c.relhasoids, "
						  "'f'::bool AS relrowsecurity, "
						  "'f'::bool AS relforcerowsecurity, "
						  "c.relfrozenxid, 0 AS relminmxid, tc.oid AS toid, "
						  "tc.relfrozenxid AS tfrozenxid, "
						  "0 AS tminmxid, "
						  "'p' AS relpersistence, 't' as relispopulated, "
						  "'d' AS relreplident, c.relpages, "
						  "CASE WHEN c.reloftype <> 0 THEN c.reloftype::pg_catalog.regtype ELSE NULL END AS reloftype, "
						  "d.refobjid AS owning_tab, "
						  "d.refobjsubid AS owning_col, "
						  "(SELECT spcname FROM pg_tablespace t WHERE t.oid = c.reltablespace) AS reltablespace, "
						  "c.reloptions AS reloptions, "
						  "tc.reloptions AS toast_reloptions, "
						  "NULL AS changed_acl, "
						  "NULL AS partkeydef, "
						  "false AS ispartition, "
						  "NULL AS partbound "
						  "FROM pg_class c "
						  "LEFT JOIN pg_depend d ON "
						  "(c.relkind = '%c' AND "
						  "d.classid = c.tableoid AND d.objid = c.oid AND "
						  "d.objsubid = 0 AND "
						  "d.refclassid = c.tableoid AND d.deptype = 'a') "
						  "LEFT JOIN pg_class tc ON (c.reltoastrelid = tc.oid) "
						  "WHERE c.relkind in ('%c', '%c', '%c', '%c') "
						  "ORDER BY c.oid",
						  username_subquery,
						  RELKIND_SEQUENCE,
						  RELKIND_RELATION, RELKIND_SEQUENCE,
						  RELKIND_VIEW, RELKIND_COMPOSITE_TYPE);
	}
	else if (fout->remoteVersion >= 80400)
	{
		/*
		 * Left join to pick up dependency info linking sequences to their
		 * owning column, if any (note this dependency is AUTO as of 8.2)
		 */
		appendStringInfo(query,
						  "SELECT c.tableoid, c.oid, c.relname, "
						  "c.relacl, NULL as rrelacl, "
						  "NULL AS initrelacl, NULL AS initrrelacl, "
						  "c.relkind, "
						  "c.relnamespace, "
						  "(%s c.relowner) AS rolname, "
						  "c.relchecks, c.relhastriggers, "
						  "c.relhasindex, c.relhasrules, c.relhasoids, "
						  "'f'::bool AS relrowsecurity, "
						  "'f'::bool AS relforcerowsecurity, "
						  "c.relfrozenxid, 0 AS relminmxid, tc.oid AS toid, "
						  "tc.relfrozenxid AS tfrozenxid, "
						  "0 AS tminmxid, "
						  "'p' AS relpersistence, 't' as relispopulated, "
						  "'d' AS relreplident, c.relpages, "
						  "NULL AS reloftype, "
						  "d.refobjid AS owning_tab, "
						  "d.refobjsubid AS owning_col, "
						  "(SELECT spcname FROM pg_tablespace t WHERE t.oid = c.reltablespace) AS reltablespace, "
						  "c.reloptions AS reloptions, "
						  "tc.reloptions AS toast_reloptions, "
						  "NULL AS changed_acl, "
						  "NULL AS partkeydef, "
						  "false AS ispartition, "
						  "NULL AS partbound "
						  "FROM pg_class c "
						  "LEFT JOIN pg_depend d ON "
						  "(c.relkind = '%c' AND "
						  "d.classid = c.tableoid AND d.objid = c.oid AND "
						  "d.objsubid = 0 AND "
						  "d.refclassid = c.tableoid AND d.deptype = 'a') "
						  "LEFT JOIN pg_class tc ON (c.reltoastrelid = tc.oid) "
						  "WHERE c.relkind in ('%c', '%c', '%c', '%c') "
						  "ORDER BY c.oid",
						  username_subquery,
						  RELKIND_SEQUENCE,
						  RELKIND_RELATION, RELKIND_SEQUENCE,
						  RELKIND_VIEW, RELKIND_COMPOSITE_TYPE);
	}
	else if (fout->remoteVersion >= 80200)
	{
		/*
		 * Left join to pick up dependency info linking sequences to their
		 * owning column, if any (note this dependency is AUTO as of 8.2)
		 */
		appendStringInfo(query,
						  "SELECT c.tableoid, c.oid, c.relname, "
						  "c.relacl, NULL as rrelacl, "
						  "NULL AS initrelacl, NULL AS initrrelacl, "
						  "c.relkind, "
						  "c.relnamespace, "
						  "(%s c.relowner) AS rolname, "
						  "c.relchecks, (c.reltriggers <> 0) AS relhastriggers, "
						  "c.relhasindex, c.relhasrules, c.relhasoids, "
						  "'f'::bool AS relrowsecurity, "
						  "'f'::bool AS relforcerowsecurity, "
						  "c.relfrozenxid, 0 AS relminmxid, tc.oid AS toid, "
						  "tc.relfrozenxid AS tfrozenxid, "
						  "0 AS tminmxid, "
						  "'p' AS relpersistence, 't' as relispopulated, "
						  "'d' AS relreplident, c.relpages, "
						  "NULL AS reloftype, "
						  "d.refobjid AS owning_tab, "
						  "d.refobjsubid AS owning_col, "
						  "(SELECT spcname FROM pg_tablespace t WHERE t.oid = c.reltablespace) AS reltablespace, "
						  "c.reloptions AS reloptions, "
						  "NULL AS toast_reloptions, "
						  "NULL AS changed_acl, "
						  "NULL AS partkeydef, "
						  "false AS ispartition, "
						  "NULL AS partbound "
						  "FROM pg_class c "
						  "LEFT JOIN pg_depend d ON "
						  "(c.relkind = '%c' AND "
						  "d.classid = c.tableoid AND d.objid = c.oid AND "
						  "d.objsubid = 0 AND "
						  "d.refclassid = c.tableoid AND d.deptype = 'a') "
						  "LEFT JOIN pg_class tc ON (c.reltoastrelid = tc.oid) "
						  "WHERE c.relkind in ('%c', '%c', '%c', '%c') "
						  "ORDER BY c.oid",
						  username_subquery,
						  RELKIND_SEQUENCE,
						  RELKIND_RELATION, RELKIND_SEQUENCE,
						  RELKIND_VIEW, RELKIND_COMPOSITE_TYPE);
	}
	else
	{
		/*
		 * Left join to pick up dependency info linking sequences to their
		 * owning column, if any
		 */
		appendStringInfo(query,
						  "SELECT c.tableoid, c.oid, relname, "
						  "relacl, NULL as rrelacl, "
						  "NULL AS initrelacl, NULL AS initrrelacl, "
						  "relkind, relnamespace, "
						  "(%s relowner) AS rolname, "
						  "relchecks, (reltriggers <> 0) AS relhastriggers, "
						  "relhasindex, relhasrules, relhasoids, "
						  "'f'::bool AS relrowsecurity, "
						  "'f'::bool AS relforcerowsecurity, "
						  "0 AS relfrozenxid, 0 AS relminmxid,"
						  "0 AS toid, "
						  "0 AS tfrozenxid, 0 AS tminmxid,"
						  "'p' AS relpersistence, 't' as relispopulated, "
						  "'d' AS relreplident, relpages, "
						  "NULL AS reloftype, "
						  "d.refobjid AS owning_tab, "
						  "d.refobjsubid AS owning_col, "
						  "(SELECT spcname FROM pg_tablespace t WHERE t.oid = c.reltablespace) AS reltablespace, "
						  "NULL AS reloptions, "
						  "NULL AS toast_reloptions, "
						  "NULL AS changed_acl, "
						  "NULL AS partkeydef, "
						  "false AS ispartition, "
						  "NULL AS partbound "
						  "FROM pg_class c "
						  "LEFT JOIN pg_depend d ON "
						  "(c.relkind = '%c' AND "
						  "d.classid = c.tableoid AND d.objid = c.oid AND "
						  "d.objsubid = 0 AND "
						  "d.refclassid = c.tableoid AND d.deptype = 'i') "
						  "WHERE relkind in ('%c', '%c', '%c', '%c') "
						  "ORDER BY c.oid",
						  username_subquery,
						  RELKIND_SEQUENCE,
						  RELKIND_RELATION, RELKIND_SEQUENCE,
						  RELKIND_VIEW, RELKIND_COMPOSITE_TYPE);
	}

	res = ExecuteSqlQuery(fout, query->data);

	ntups = dbms_md_get_tuple_num(res);

	*numTables = ntups;

	/*
	 * Extract data from result and lock dumpable tables.  We do the locking
	 * before anything else, to minimize the window wherein a table could
	 * disappear under us.
	 *
	 * Note that we have to save info about all tables here, even when dumping
	 * only one, because we don't yet know which tables might be inheritance
	 * ancestors of the target table.
	 */
	tblinfo = (TableInfo *) dbms_md_malloc0(fout->memory_ctx, ntups * sizeof(TableInfo));

	i_reltableoid = dbms_md_get_field_subscript(res, "tableoid");
	i_reloid = dbms_md_get_field_subscript(res, "oid");
	i_relname = dbms_md_get_field_subscript(res, "relname");
	i_relnamespace = dbms_md_get_field_subscript(res, "relnamespace");
	i_relacl = dbms_md_get_field_subscript(res, "relacl");
	i_rrelacl = dbms_md_get_field_subscript(res, "rrelacl");
	i_initrelacl = dbms_md_get_field_subscript(res, "initrelacl");
	i_initrrelacl = dbms_md_get_field_subscript(res, "initrrelacl");
	i_relkind = dbms_md_get_field_subscript(res, "relkind");
	i_rolname = dbms_md_get_field_subscript(res, "rolname");
	i_relchecks = dbms_md_get_field_subscript(res, "relchecks");
	i_relhastriggers = dbms_md_get_field_subscript(res, "relhastriggers");
	i_relhasindex = dbms_md_get_field_subscript(res, "relhasindex");
	i_relhasrules = dbms_md_get_field_subscript(res, "relhasrules");
	i_relrowsec = dbms_md_get_field_subscript(res, "relrowsecurity");
	i_relforcerowsec = dbms_md_get_field_subscript(res, "relforcerowsecurity");
	i_relhasoids = dbms_md_get_field_subscript(res, "relhasoids");
	i_relfrozenxid = dbms_md_get_field_subscript(res, "relfrozenxid");
	i_relminmxid = dbms_md_get_field_subscript(res, "relminmxid");
	i_toastoid = dbms_md_get_field_subscript(res, "toid");
	i_toastfrozenxid = dbms_md_get_field_subscript(res, "tfrozenxid");
	i_toastminmxid = dbms_md_get_field_subscript(res, "tminmxid");
	i_relpersistence = dbms_md_get_field_subscript(res, "relpersistence");
	i_relispopulated = dbms_md_get_field_subscript(res, "relispopulated");
	i_relreplident = dbms_md_get_field_subscript(res, "relreplident");
	i_relpages = dbms_md_get_field_subscript(res, "relpages");
	i_owning_tab = dbms_md_get_field_subscript(res, "owning_tab");
	i_owning_col = dbms_md_get_field_subscript(res, "owning_col");
#ifdef PGXC
	i_pgxclocatortype = dbms_md_get_field_subscript(res, "pgxclocatortype");
	i_pgxcattnum = dbms_md_get_field_subscript(res, "pgxcattnum");
	i_pgxcsecattnum = dbms_md_get_field_subscript(res, "pgxcsecattnum");
	i_pgxc_node_names = dbms_md_get_field_subscript(res, "pgxc_node_names");
#endif
#ifdef __OPENTENBASE__
	i_parttype = dbms_md_get_field_subscript(res, "parttype");
	i_partattnum = dbms_md_get_field_subscript(res, "partattnum");
	//i_nparts = dbms_md_get_field_subscript(res, "nparts");
	//i_partstartvalue = dbms_md_get_field_subscript(res, "partstartvalue");
	//i_partinterval = dbms_md_get_field_subscript(res, "partinterval");	
	//i_groupname = dbms_md_get_field_subscript(res, "groupname");
	//i_coldgroupname = dbms_md_get_field_subscript(res, "coldgroupname");
#endif

	i_reltablespace = dbms_md_get_field_subscript(res, "reltablespace");
	i_reloptions = dbms_md_get_field_subscript(res, "reloptions");
	i_checkoption = dbms_md_get_field_subscript(res, "checkoption");
	i_toastreloptions = dbms_md_get_field_subscript(res, "toast_reloptions");
	i_reloftype = dbms_md_get_field_subscript(res, "reloftype");
	i_is_identity_sequence = dbms_md_get_field_subscript(res, "is_identity_sequence");
	i_changed_acl = dbms_md_get_field_subscript(res, "changed_acl");
	i_partkeydef = dbms_md_get_field_subscript(res, "partkeydef");
	i_ispartition = dbms_md_get_field_subscript(res, "ispartition");
	i_partbound = dbms_md_get_field_subscript(res, "partbound");

	if (dopt->lockWaitTimeout)
	{
		/*
		 * Arrange to fail instead of waiting forever for a table lock.
		 *
		 * NB: this coding assumes that the only queries issued within the
		 * following loop are LOCK TABLEs; else the timeout may be undesirably
		 * applied to other things too.
		 */
		resetStringInfo(query);
		appendStringInfoString(query, "SET statement_timeout = ");
		dbms_md_append_string_literal_conn(query, 
										    dopt->lockWaitTimeout, 
										    fout->remoteVersion, 
										    fout->encoding, 
										    fout->std_strings);
		ExecuteSqlStatement(fout, query->data);
	}

	for (i = 0; i < ntups; i++)
	{
		tblinfo[i].dobj.objType = DO_TABLE;
		tblinfo[i].dobj.catId.tableoid = dbms_md_str_to_oid(dbms_md_get_field_value(res, i, i_reltableoid));
		tblinfo[i].dobj.catId.oid = dbms_md_str_to_oid(dbms_md_get_field_value(res, i, i_reloid));
		AssignDumpId(fout, &tblinfo[i].dobj);
		tblinfo[i].dobj.name = dbms_md_strdup(dbms_md_get_field_value(res, i, i_relname));
		tblinfo[i].dobj.namespace =
			findNamespace(fout,
						  dbms_md_str_to_oid(dbms_md_get_field_value(res, i, i_relnamespace)));
		tblinfo[i].rolname = dbms_md_strdup(dbms_md_get_field_value(res, i, i_rolname));
		tblinfo[i].relacl = dbms_md_strdup(dbms_md_get_field_value(res, i, i_relacl));
		tblinfo[i].rrelacl = dbms_md_strdup(dbms_md_get_field_value(res, i, i_rrelacl));
		tblinfo[i].initrelacl = dbms_md_strdup(dbms_md_get_field_value(res, i, i_initrelacl));
		tblinfo[i].initrrelacl = dbms_md_strdup(dbms_md_get_field_value(res, i, i_initrrelacl));
		tblinfo[i].relkind = *(dbms_md_get_field_value(res, i, i_relkind));
		tblinfo[i].relpersistence = *(dbms_md_get_field_value(res, i, i_relpersistence));
		tblinfo[i].hasindex = (strcmp(dbms_md_get_field_value(res, i, i_relhasindex), "t") == 0);
		tblinfo[i].hasrules = (strcmp(dbms_md_get_field_value(res, i, i_relhasrules), "t") == 0);
		tblinfo[i].hastriggers = (strcmp(dbms_md_get_field_value(res, i, i_relhastriggers), "t") == 0);
		tblinfo[i].rowsec = (strcmp(dbms_md_get_field_value(res, i, i_relrowsec), "t") == 0);
		tblinfo[i].forcerowsec = (strcmp(dbms_md_get_field_value(res, i, i_relforcerowsec), "t") == 0);
		tblinfo[i].hasoids = (strcmp(dbms_md_get_field_value(res, i, i_relhasoids), "t") == 0);
		tblinfo[i].relispopulated = (strcmp(dbms_md_get_field_value(res, i, i_relispopulated), "t") == 0);
		tblinfo[i].relreplident = *(dbms_md_get_field_value(res, i, i_relreplident));
		tblinfo[i].relpages = atoi(dbms_md_get_field_value(res, i, i_relpages));
		tblinfo[i].frozenxid = dbms_md_str_to_oid(dbms_md_get_field_value(res, i, i_relfrozenxid));
		tblinfo[i].minmxid = dbms_md_str_to_oid(dbms_md_get_field_value(res, i, i_relminmxid));
		if (!dbms_md_is_tuple_field_null(res, i, i_toastoid))
			tblinfo[i].toast_oid = dbms_md_str_to_oid(dbms_md_get_field_value(res, i, i_toastoid));
		else
			tblinfo[i].toast_oid = InvalidOid;
		if (!dbms_md_is_tuple_field_null(res, i, i_toastfrozenxid))
			tblinfo[i].toast_frozenxid = dbms_md_str_to_oid(dbms_md_get_field_value(res, i, i_toastfrozenxid));
		else
			tblinfo[i].toast_frozenxid = InvalidOid;
		
		if (!dbms_md_is_tuple_field_null(res, i, i_toastminmxid))
			tblinfo[i].toast_minmxid = dbms_md_str_to_oid(dbms_md_get_field_value(res, i, i_toastminmxid));
		else
			tblinfo[i].toast_minmxid = InvalidOid;
		
		if (dbms_md_is_tuple_field_null(res, i, i_reloftype))
			tblinfo[i].reloftype = NULL;
		else
			tblinfo[i].reloftype = dbms_md_strdup(dbms_md_get_field_value(res, i, i_reloftype));
		tblinfo[i].ncheck = atoi(dbms_md_get_field_value(res, i, i_relchecks));
		if (dbms_md_is_tuple_field_null(res, i, i_owning_tab))
		{
			tblinfo[i].owning_tab = InvalidOid;
			tblinfo[i].owning_col = 0;
		}
		else
		{
			tblinfo[i].owning_tab = dbms_md_str_to_oid(dbms_md_get_field_value(res, i, i_owning_tab));
			tblinfo[i].owning_col = atoi(dbms_md_get_field_value(res, i, i_owning_col));
		}
#ifdef PGXC
		if (fout->isPostgresXL)
		{
			/* Not all the tables have pgxc locator Data */
			if (dbms_md_is_tuple_field_null(res, i, i_pgxclocatortype))
			{
				tblinfo[i].pgxclocatortype = 'E';
				tblinfo[i].pgxcattnum = 0;
			}
			else
			{
				tblinfo[i].pgxclocatortype = *(dbms_md_get_field_value(res, i, i_pgxclocatortype));
				tblinfo[i].pgxcattnum = atoi(dbms_md_get_field_value(res, i, i_pgxcattnum));
			}
			tblinfo[i].pgxc_node_names = dbms_md_strdup(dbms_md_get_field_value(res, i, i_pgxc_node_names));

			if (dbms_md_is_tuple_field_null(res, i, i_pgxcsecattnum))
			{
				tblinfo[i].pgxcsecattnum = 0;
			}
			else
			{
				tblinfo[i].pgxcsecattnum = atoi(dbms_md_get_field_value(res, i, i_pgxcsecattnum));
			}
		}
#endif
#ifdef __OPENTENBASE__
		tblinfo[i].parttype = *(dbms_md_get_field_value(res, i, i_parttype));
		if(tblinfo[i].parttype == 'p')
		{
			tblinfo[i].partattnum = atoi(dbms_md_get_field_value(res, i, i_partattnum));
			/*tblinfo[i].nparts = atoi(dbms_md_get_field_value(res, i, i_nparts));
			tblinfo[i].partstartvalue = dbms_md_strdup(dbms_md_get_field_value(res, i, i_partstartvalue));
			tblinfo[i].partinterval = dbms_md_strdup(dbms_md_get_field_value(res, i, i_partinterval));*/
			
		}
		else
		{
			tblinfo[i].partattnum = 0;
			tblinfo[i].nparts = 0;
			tblinfo[i].partstartvalue = NULL;
			tblinfo[i].partinterval = NULL;
		}

		/*if (dbms_md_is_tuple_field_null(res, i, i_groupname))
			tblinfo[i].groupname = NULL;
		else
			tblinfo[i].groupname = dbms_md_strdup(dbms_md_get_field_value(res, i, i_groupname));

		if (dbms_md_is_tuple_field_null(res, i, i_coldgroupname))
			tblinfo[i].coldgroupname = NULL;
		else
			tblinfo[i].coldgroupname = dbms_md_strdup(dbms_md_get_field_value(res, i, i_coldgroupname));*/
#endif

		tblinfo[i].reltablespace = dbms_md_strdup(dbms_md_get_field_value(res, i, i_reltablespace));
		tblinfo[i].reloptions = dbms_md_strdup(dbms_md_get_field_value(res, i, i_reloptions));
		if (i_checkoption == -1 || dbms_md_is_tuple_field_null(res, i, i_checkoption))
			tblinfo[i].checkoption = NULL;
		else
			tblinfo[i].checkoption = dbms_md_strdup(dbms_md_get_field_value(res, i, i_checkoption));
		tblinfo[i].toast_reloptions = dbms_md_strdup(dbms_md_get_field_value(res, i, i_toastreloptions));

		/* other fields were zeroed above */

		/*
		 * Decide whether we want to dump this table.
		 */
		if (tblinfo[i].relkind == RELKIND_COMPOSITE_TYPE)
			tblinfo[i].dobj.dump = DUMP_COMPONENT_NONE;
		else
			selectDumpableTable(&tblinfo[i], fout);

		/*
		 * If the table-level and all column-level ACLs for this table are
		 * unchanged, then we don't need to worry about including the ACLs for
		 * this table.  If any column-level ACLs have been changed, the
		 * 'changed_acl' column from the query will indicate that.
		 *
		 * This can result in a significant performance improvement in cases
		 * where we are only looking to dump out the ACL (eg: pg_catalog).
		 */
		if (dbms_md_is_tuple_field_null(res, i, i_relacl) && dbms_md_is_tuple_field_null(res, i, i_rrelacl) &&
			dbms_md_is_tuple_field_null(res, i, i_initrelacl) &&
			dbms_md_is_tuple_field_null(res, i, i_initrrelacl) &&
			strcmp(dbms_md_get_field_value(res, i, i_changed_acl), "f") == 0)
			tblinfo[i].dobj.dump &= ~DUMP_COMPONENT_ACL;

		tblinfo[i].interesting = tblinfo[i].dobj.dump ? true : false;
		tblinfo[i].dummy_view = false;	/* might get set during sort */
		tblinfo[i].postponed_def = false;	/* might get set during sort */

		tblinfo[i].is_identity_sequence = (i_is_identity_sequence >= 0 &&
									 strcmp(dbms_md_get_field_value(res, i, i_is_identity_sequence), "t") == 0);

		/* Partition key string or NULL */
		tblinfo[i].partkeydef = dbms_md_strdup(dbms_md_get_field_value(res, i, i_partkeydef));
		tblinfo[i].ispartition = (strcmp(dbms_md_get_field_value(res, i, i_ispartition), "t") == 0);
		tblinfo[i].partbound = dbms_md_strdup(dbms_md_get_field_value(res, i, i_partbound));

		/*
		 * Read-lock target tables to make sure they aren't DROPPED or altered
		 * in schema before we get around to dumping them.
		 *
		 * Note that we don't explicitly lock parents of the target tables; we
		 * assume our lock on the child is enough to prevent schema
		 * alterations to parent tables.
		 *
		 * NOTE: it'd be kinda nice to lock other relations too, not only
		 * plain tables, but the backend doesn't presently allow that.
		 *
		 * We only need to lock the table for certain components; see
		 * pg_dump.h
		 */
		if (tblinfo[i].dobj.dump &&
			(tblinfo[i].relkind == RELKIND_RELATION ||
			 tblinfo->relkind == RELKIND_PARTITIONED_TABLE) &&
			(tblinfo[i].dobj.dump & DUMP_COMPONENTS_REQUIRING_LOCK))
		{
			resetStringInfo(query);
			appendStringInfo(query,
							  "LOCK TABLE %s IN ACCESS SHARE MODE",
							  dbms_md_fmt_qualified_id(fout->remoteVersion,
											 tblinfo[i].dobj.namespace->dobj.name,
											 tblinfo[i].dobj.name));
			ExecuteSqlStatement(fout, query->data);
		}

		/* Emit notice if join for owner failed */
		if (strlen(tblinfo[i].rolname) == 0)
			write_msg(NULL, "WARNING: owner of table \"%s\" appears to be invalid\n",
					  tblinfo[i].dobj.name);
	}

	if (dopt->lockWaitTimeout)
	{
		ExecuteSqlStatement(fout, "SET statement_timeout = 0");
	}

	dbms_md_free_tuples(res);

	destroyStringInfo(query);
	pfree(query);

#ifdef __OPENTENBASE__
	getKeyValues(fout, ntups, tblinfo);
#endif

	return tblinfo;
}

/*
 * getOwnedSeqs
 *	  identify owned sequences and mark them as dumpable if owning table is
 *
 * We used to do this in getTables(), but it's better to do it after the
 * index used by findTableByOid() has been set up.
 */
void
getOwnedSeqs(Archive *fout, TableInfo tblinfo[], int numTables)
{
	int			i;

	/*
	 * Force sequences that are "owned" by table columns to be dumped whenever
	 * their owning table is being dumped.
	 */
	for (i = 0; i < numTables; i++)
	{
		TableInfo  *seqinfo = &tblinfo[i];
		TableInfo  *owning_tab;

		if (!OidIsValid(seqinfo->owning_tab))
			continue;			/* not an owned sequence */

		owning_tab = findTableByOid(fout, seqinfo->owning_tab);
		if (owning_tab == NULL)
		{
			elog(ERROR, "failed sanity check, parent table with OID %u of sequence with OID %u not found\n",
						  seqinfo->owning_tab, seqinfo->dobj.catId.oid);
			return ;
		}

		/*
		 * We need to dump the components that are being dumped for the table
		 * and any components which the sequence is explicitly marked with.
		 *
		 * We can't simply use the set of components which are being dumped
		 * for the table as the table might be in an extension (and only the
		 * non-extension components, eg: ACLs if changed, security labels, and
		 * policies, are being dumped) while the sequence is not (and
		 * therefore the definition and other components should also be
		 * dumped).
		 *
		 * If the sequence is part of the extension then it should be properly
		 * marked by checkExtensionMembership() and this will be a no-op as
		 * the table will be equivalently marked.
		 */
		seqinfo->dobj.dump = seqinfo->dobj.dump | owning_tab->dobj.dump;

		if (seqinfo->dobj.dump != DUMP_COMPONENT_NONE)
			seqinfo->interesting = true;
	}
}

/*
 * getInherits
 *	  read all the inheritance information
 * from the system catalogs return them in the InhInfo* structure
 *
 * numInherits is set to the number of pairs read in
 */
InhInfo *
getInherits(Archive *fout, int *numInherits)
{
	SPITupleTable   *res;
	int			ntups;
	int			i;
	StringInfo query = createStringInfo();
	InhInfo    *inhinfo;

	int			i_inhrelid;
	int			i_inhparent;

	/* Make sure we are in proper schema */
	selectSourceSchema(fout, "pg_catalog");

	/*
	 * Find all the inheritance information, excluding implicit inheritance
	 * via partitioning.  We handle that case using getPartitions(), because
	 * we want more information about partitions than just the parent-child
	 * relationship.
	 */
	appendStringInfoString(query, "SELECT inhrelid, inhparent FROM pg_inherits");

	res = ExecuteSqlQuery(fout, query->data);

	ntups = dbms_md_get_tuple_num(res);

	*numInherits = ntups;

	inhinfo = (InhInfo *) dbms_md_malloc0(fout->memory_ctx, ntups * sizeof(InhInfo));

	i_inhrelid = dbms_md_get_field_subscript(res, "inhrelid");
	i_inhparent = dbms_md_get_field_subscript(res, "inhparent");

	for (i = 0; i < ntups; i++)
	{
		inhinfo[i].inhrelid = dbms_md_str_to_oid(dbms_md_get_field_value(res, i, i_inhrelid));
		inhinfo[i].inhparent = dbms_md_str_to_oid(dbms_md_get_field_value(res, i, i_inhparent));
	}

	dbms_md_free_tuples(res);

	destroyStringInfo(query);
	pfree(query);

	return inhinfo;
}

/*
 * getIndexes
 *	  get information about every index on a dumpable table
 *
 * Note: index data is not returned directly to the caller, but it
 * does get entered into the DumpableObject tables.
 */
void
getIndexes(Archive *fout, TableInfo tblinfo[], int numTables)
{
	int			i,
				j;
	StringInfo query = createStringInfo();
	SPITupleTable   *res;
	IndxInfo   *indxinfo;
	ConstraintInfo *constrinfo;
	int			i_tableoid,
				i_oid,
				i_indexname,
				i_parentidx,
				i_indexdef,
				i_indnkeys,
				i_indkey,
				i_indisclustered,
				i_indisreplident,
				i_contype,
				i_conname,
				i_condeferrable,
				i_condeferred,
				i_contableoid,
				i_conoid,
				i_condef,
				i_tablespace,
				i_indreloptions,
				i_relpages;
	int			ntups;

	for (i = 0; i < numTables; i++)
	{
		TableInfo  *tbinfo = &tblinfo[i];

		if (!tbinfo->hasindex)
			continue;

		/*
		 * Ignore indexes of tables whose definitions are not to be dumped.
		 *
		 * We also need indexes on partitioned tables which have partitions to
		 * be dumped, in order to dump the indexes on the partitions.
		 */
		if (!(tbinfo->dobj.dump & DUMP_COMPONENT_DEFINITION) &&
			!tbinfo->interesting)
			continue;

		write_msg("Index", "reading indexes for table \"%s.%s\"\n",
					  tbinfo->dobj.namespace->dobj.name,
					  tbinfo->dobj.name);

		/* Make sure we are in proper schema so indexdef is right */
		selectSourceSchema(fout, tbinfo->dobj.namespace->dobj.name);

		/*
		 * The point of the messy-looking outer join is to find a constraint
		 * that is related by an internal dependency link to the index. If we
		 * find one, create a CONSTRAINT entry linked to the INDEX entry.  We
		 * assume an index won't have more than one internal dependency.
		 *
		 * As of 9.0 we don't need to look at pg_depend but can check for a
		 * match to pg_constraint.conindid.  The check on conrelid is
		 * redundant but useful because that column is indexed while conindid
		 * is not.
		 */
		resetStringInfo(query);
		if (fout->remoteVersion >= 11000)
		{
			appendStringInfo(query,
							  "SELECT t.tableoid, t.oid, "
							  "t.relname AS indexname, "
							  "inh.inhparent AS parentidx, "
							  "pg_catalog.pg_get_indexdef(i.indexrelid) AS indexdef, "
							  "t.relnatts AS indnkeys, "
							  "i.indkey, i.indisclustered, "
							  "i.indisreplident, t.relpages, "
							  "c.contype, c.conname, "
							  "c.condeferrable, c.condeferred, "
							  "c.tableoid AS contableoid, "
							  "c.oid AS conoid, "
							  "pg_catalog.pg_get_constraintdef(c.oid, false) AS condef, "
							  "(SELECT spcname FROM pg_catalog.pg_tablespace s WHERE s.oid = t.reltablespace) AS tablespace, "
							  "t.reloptions AS indreloptions "
							  "FROM pg_catalog.pg_index i "
							  "JOIN pg_catalog.pg_class t ON (t.oid = i.indexrelid) "
							  "JOIN pg_catalog.pg_class t2 ON (t2.oid = i.indrelid) "
							  "LEFT JOIN pg_catalog.pg_constraint c "
							  "ON (i.indrelid = c.conrelid AND "
							  "i.indexrelid = c.conindid AND "
							  "c.contype IN ('p','u','x')) "
							  "LEFT JOIN pg_catalog.pg_inherits inh "
							  "ON (inh.inhrelid = indexrelid) "
							  "WHERE i.indrelid = '%u'::pg_catalog.oid "
							  "AND (i.indisvalid OR t2.relkind = 'p') "
							  "AND i.indisready "
							  "ORDER BY indexname",
							  tbinfo->dobj.catId.oid);
		}
		else if (fout->remoteVersion >= 90400)
		{
			/*
			 * the test on indisready is necessary in 9.2, and harmless in
			 * earlier/later versions
			 */
			appendStringInfo(query,
							  "SELECT t.tableoid, t.oid, "
							  "t.relname AS indexname, "
							  "0 AS parentidx, "
							  "pg_catalog.pg_get_indexdef(i.indexrelid) AS indexdef, "
							  "t.relnatts AS indnkeys, "
							  "i.indkey, i.indisclustered, "
							  "i.indisreplident, t.relpages, "
							  "c.contype, c.conname, "
							  "c.condeferrable, c.condeferred, "
							  "c.tableoid AS contableoid, "
							  "c.oid AS conoid, "
							  "pg_catalog.pg_get_constraintdef(c.oid, false) AS condef, "
							  "(SELECT spcname FROM pg_catalog.pg_tablespace s WHERE s.oid = t.reltablespace) AS tablespace, "
							  "t.reloptions AS indreloptions "
							  "FROM pg_catalog.pg_index i "
							  "JOIN pg_catalog.pg_class t ON (t.oid = i.indexrelid) "
							  "LEFT JOIN pg_catalog.pg_constraint c "
							  "ON (i.indrelid = c.conrelid AND "
							  "i.indexrelid = c.conindid AND "
							  "c.contype IN ('p','u','x')) "
							  "WHERE i.indrelid = '%u'::pg_catalog.oid "
							  "AND i.indisvalid AND i.indisready "
							  "ORDER BY indexname",
							  tbinfo->dobj.catId.oid);
		}
		else if (fout->remoteVersion >= 90000)
		{
			/*
			 * the test on indisready is necessary in 9.2, and harmless in
			 * earlier/later versions
			 */
			appendStringInfo(query,
							  "SELECT t.tableoid, t.oid, "
							  "t.relname AS indexname, "
							  "0 AS parentidx, "
							  "pg_catalog.pg_get_indexdef(i.indexrelid) AS indexdef, "
							  "t.relnatts AS indnkeys, "
							  "i.indkey, i.indisclustered, "
							  "false AS indisreplident, t.relpages, "
							  "c.contype, c.conname, "
							  "c.condeferrable, c.condeferred, "
							  "c.tableoid AS contableoid, "
							  "c.oid AS conoid, "
							  "pg_catalog.pg_get_constraintdef(c.oid, false) AS condef, "
							  "(SELECT spcname FROM pg_catalog.pg_tablespace s WHERE s.oid = t.reltablespace) AS tablespace, "
							  "t.reloptions AS indreloptions "
							  "FROM pg_catalog.pg_index i "
							  "JOIN pg_catalog.pg_class t ON (t.oid = i.indexrelid) "
							  "LEFT JOIN pg_catalog.pg_constraint c "
							  "ON (i.indrelid = c.conrelid AND "
							  "i.indexrelid = c.conindid AND "
							  "c.contype IN ('p','u','x')) "
							  "WHERE i.indrelid = '%u'::pg_catalog.oid "
							  "AND i.indisvalid AND i.indisready "
							  "ORDER BY indexname",
							  tbinfo->dobj.catId.oid);
		}
		else if (fout->remoteVersion >= 80200)
		{
			appendStringInfo(query,
							  "SELECT t.tableoid, t.oid, "
							  "t.relname AS indexname, "
							  "0 AS parentidx, "
							  "pg_catalog.pg_get_indexdef(i.indexrelid) AS indexdef, "
							  "t.relnatts AS indnkeys, "
							  "i.indkey, i.indisclustered, "
							  "false AS indisreplident, t.relpages, "
							  "c.contype, c.conname, "
							  "c.condeferrable, c.condeferred, "
							  "c.tableoid AS contableoid, "
							  "c.oid AS conoid, "
							  "null AS condef, "
							  "(SELECT spcname FROM pg_catalog.pg_tablespace s WHERE s.oid = t.reltablespace) AS tablespace, "
							  "t.reloptions AS indreloptions "
							  "FROM pg_catalog.pg_index i "
							  "JOIN pg_catalog.pg_class t ON (t.oid = i.indexrelid) "
							  "LEFT JOIN pg_catalog.pg_depend d "
							  "ON (d.classid = t.tableoid "
							  "AND d.objid = t.oid "
							  "AND d.deptype = 'i') "
							  "LEFT JOIN pg_catalog.pg_constraint c "
							  "ON (d.refclassid = c.tableoid "
							  "AND d.refobjid = c.oid) "
							  "WHERE i.indrelid = '%u'::pg_catalog.oid "
							  "AND i.indisvalid "
							  "ORDER BY indexname",
							  tbinfo->dobj.catId.oid);
		}
		else
		{
			appendStringInfo(query,
							  "SELECT t.tableoid, t.oid, "
							  "t.relname AS indexname, "
							  "0 AS parentidx, "
							  "pg_catalog.pg_get_indexdef(i.indexrelid) AS indexdef, "
							  "t.relnatts AS indnkeys, "
							  "i.indkey, i.indisclustered, "
							  "false AS indisreplident, t.relpages, "
							  "c.contype, c.conname, "
							  "c.condeferrable, c.condeferred, "
							  "c.tableoid AS contableoid, "
							  "c.oid AS conoid, "
							  "null AS condef, "
							  "(SELECT spcname FROM pg_catalog.pg_tablespace s WHERE s.oid = t.reltablespace) AS tablespace, "
							  "null AS indreloptions "
							  "FROM pg_catalog.pg_index i "
							  "JOIN pg_catalog.pg_class t ON (t.oid = i.indexrelid) "
							  "LEFT JOIN pg_catalog.pg_depend d "
							  "ON (d.classid = t.tableoid "
							  "AND d.objid = t.oid "
							  "AND d.deptype = 'i') "
							  "LEFT JOIN pg_catalog.pg_constraint c "
							  "ON (d.refclassid = c.tableoid "
							  "AND d.refobjid = c.oid) "
							  "WHERE i.indrelid = '%u'::pg_catalog.oid "
							  "ORDER BY indexname",
							  tbinfo->dobj.catId.oid);
		}

		res = ExecuteSqlQuery(fout, query->data);

		ntups = dbms_md_get_tuple_num(res);

		i_tableoid = dbms_md_get_field_subscript(res, "tableoid");
		i_oid = dbms_md_get_field_subscript(res, "oid");
		i_indexname = dbms_md_get_field_subscript(res, "indexname");
		i_parentidx = dbms_md_get_field_subscript(res, "parentidx");
		i_indexdef = dbms_md_get_field_subscript(res, "indexdef");
		i_indnkeys = dbms_md_get_field_subscript(res, "indnkeys");
		i_indkey = dbms_md_get_field_subscript(res, "indkey");
		i_indisclustered = dbms_md_get_field_subscript(res, "indisclustered");
		i_indisreplident = dbms_md_get_field_subscript(res, "indisreplident");
		i_relpages = dbms_md_get_field_subscript(res, "relpages");
		i_contype = dbms_md_get_field_subscript(res, "contype");
		i_conname = dbms_md_get_field_subscript(res, "conname");
		i_condeferrable = dbms_md_get_field_subscript(res, "condeferrable");
		i_condeferred = dbms_md_get_field_subscript(res, "condeferred");
		i_contableoid = dbms_md_get_field_subscript(res, "contableoid");
		i_conoid = dbms_md_get_field_subscript(res, "conoid");
		i_condef = dbms_md_get_field_subscript(res, "condef");
		i_tablespace = dbms_md_get_field_subscript(res, "tablespace");
		i_indreloptions = dbms_md_get_field_subscript(res, "indreloptions");

		tbinfo->indexes = indxinfo =
			(IndxInfo *) dbms_md_malloc0(fout->memory_ctx, ntups * sizeof(IndxInfo));
		constrinfo = (ConstraintInfo *) dbms_md_malloc0(fout->memory_ctx, ntups * sizeof(ConstraintInfo));
		tbinfo->numIndexes = ntups;

		for (j = 0; j < ntups; j++)
		{
			char		contype;

			indxinfo[j].dobj.objType = DO_INDEX;
			indxinfo[j].dobj.catId.tableoid = dbms_md_str_to_oid(dbms_md_get_field_value(res, j, i_tableoid));
			indxinfo[j].dobj.catId.oid = dbms_md_str_to_oid(dbms_md_get_field_value(res, j, i_oid));
			AssignDumpId(fout, &indxinfo[j].dobj);
			indxinfo[j].dobj.dump = tbinfo->dobj.dump;
			indxinfo[j].dobj.name = dbms_md_strdup(dbms_md_get_field_value(res, j, i_indexname));
			indxinfo[j].dobj.namespace = tbinfo->dobj.namespace;
			indxinfo[j].indextable = tbinfo;
			indxinfo[j].indexdef = dbms_md_strdup(dbms_md_get_field_value(res, j, i_indexdef));
			indxinfo[j].indnkeys = atoi(dbms_md_get_field_value(res, j, i_indnkeys));
			indxinfo[j].tablespace = dbms_md_strdup(dbms_md_get_field_value(res, j, i_tablespace));
			indxinfo[j].indreloptions = dbms_md_strdup(dbms_md_get_field_value(res, j, i_indreloptions));
			indxinfo[j].indkeys = (Oid *) dbms_md_malloc0(fout->memory_ctx, indxinfo[j].indnkeys * sizeof(Oid));
			parseOidArray(dbms_md_get_field_value(res, j, i_indkey),
						  indxinfo[j].indkeys, indxinfo[j].indnkeys);
			indxinfo[j].indisclustered = (dbms_md_get_field_value(res, j, i_indisclustered)[0] == 't');
			indxinfo[j].indisreplident = (dbms_md_get_field_value(res, j, i_indisreplident)[0] == 't');
			indxinfo[j].parentidx = dbms_md_str_to_oid(dbms_md_get_field_value(res, j, i_parentidx));
			indxinfo[j].relpages = atoi(dbms_md_get_field_value(res, j, i_relpages));
			contype = *(dbms_md_get_field_value(res, j, i_contype));

			if (contype == 'p' || contype == 'u' || contype == 'x')
			{
				/*
				 * If we found a constraint matching the index, create an
				 * entry for it.
				 */
				constrinfo[j].dobj.objType = DO_CONSTRAINT;
				constrinfo[j].dobj.catId.tableoid = dbms_md_str_to_oid(dbms_md_get_field_value(res, j, i_contableoid));
				constrinfo[j].dobj.catId.oid = dbms_md_str_to_oid(dbms_md_get_field_value(res, j, i_conoid));
				AssignDumpId(fout, &constrinfo[j].dobj);
				constrinfo[j].dobj.dump = tbinfo->dobj.dump;
				constrinfo[j].dobj.name = dbms_md_strdup(dbms_md_get_field_value(res, j, i_conname));
				constrinfo[j].dobj.namespace = tbinfo->dobj.namespace;
				constrinfo[j].contable = tbinfo;
				constrinfo[j].condomain = NULL;
				constrinfo[j].contype = contype;
				if (contype == 'x')
					constrinfo[j].condef = dbms_md_strdup(dbms_md_get_field_value(res, j, i_condef));
				else
					constrinfo[j].condef = NULL;
				constrinfo[j].confrelid = InvalidOid;
				constrinfo[j].conindex = indxinfo[j].dobj.dumpId;
				constrinfo[j].condeferrable = *(dbms_md_get_field_value(res, j, i_condeferrable)) == 't';
				constrinfo[j].condeferred = *(dbms_md_get_field_value(res, j, i_condeferred)) == 't';
				constrinfo[j].conislocal = true;
				constrinfo[j].separate = true;

				indxinfo[j].indexconstraint = constrinfo[j].dobj.dumpId;
			}
			else
			{
				/* Plain secondary index */
				indxinfo[j].indexconstraint = 0;
			}
		}

		dbms_md_free_tuples(res);
	}

	destroyStringInfo(query);
	pfree(query);
}

/*
 * getExtendedStatistics
 *	  get information about extended statistics on a dumpable table
 *	  or materialized view.
 *
 * Note: extended statistics data is not returned directly to the caller, but
 * it does get entered into the DumpableObject tables.
 */
void
getExtendedStatistics(Archive *fout, TableInfo tblinfo[], int numTables)
{
	int			i,
				j;
	StringInfo query;
	SPITupleTable   *res;
	StatsExtInfo *statsextinfo;
	int			ntups;
	int			i_tableoid;
	int			i_oid;
	int			i_stxname;
	int			i_stxdef;

	/* Extended statistics were new in v10 */
	if (fout->remoteVersion < 100000)
		return;

	query = createStringInfo();

	for (i = 0; i < numTables; i++)
	{
		TableInfo  *tbinfo = &tblinfo[i];

		/*
		 * Only plain tables, materialized views, foreign tables and
		 * partitioned tables can have extended statistics.
		 */
		if (tbinfo->relkind != RELKIND_RELATION &&
			tbinfo->relkind != RELKIND_MATVIEW &&
			tbinfo->relkind != RELKIND_FOREIGN_TABLE &&
			tbinfo->relkind != RELKIND_PARTITIONED_TABLE)
			continue;

		/*
		 * Ignore extended statistics of tables whose definitions are not to
		 * be dumped.
		 */
		if (!(tbinfo->dobj.dump & DUMP_COMPONENT_DEFINITION))
			continue;

		write_msg(NULL, "reading extended statistics for table \"%s.%s\"\n",
					  tbinfo->dobj.namespace->dobj.name,
					  tbinfo->dobj.name);

		/* Make sure we are in proper schema so stadef is right */
		selectSourceSchema(fout, tbinfo->dobj.namespace->dobj.name);

		resetStringInfo(query);

		appendStringInfo(query,
						  "SELECT "
						  "tableoid, "
						  "oid, "
						  "stxname, "
						  "pg_catalog.pg_get_statisticsobjdef(oid) AS stxdef "
						  "FROM pg_statistic_ext "
						  "WHERE stxrelid = '%u' "
						  "ORDER BY stxname", tbinfo->dobj.catId.oid);

		res = ExecuteSqlQuery(fout, query->data);

		ntups = dbms_md_get_tuple_num(res);

		i_tableoid = dbms_md_get_field_subscript(res, "tableoid");
		i_oid = dbms_md_get_field_subscript(res, "oid");
		i_stxname = dbms_md_get_field_subscript(res, "stxname");
		i_stxdef = dbms_md_get_field_subscript(res, "stxdef");

		statsextinfo = (StatsExtInfo *) dbms_md_malloc0(fout->memory_ctx, ntups * sizeof(StatsExtInfo));

		for (j = 0; j < ntups; j++)
		{
			statsextinfo[j].dobj.objType = DO_STATSEXT;
			statsextinfo[j].dobj.catId.tableoid = dbms_md_str_to_oid(dbms_md_get_field_value(res, j, i_tableoid));
			statsextinfo[j].dobj.catId.oid = dbms_md_str_to_oid(dbms_md_get_field_value(res, j, i_oid));
			AssignDumpId(fout, &statsextinfo[j].dobj);
			statsextinfo[j].dobj.name = dbms_md_strdup(dbms_md_get_field_value(res, j, i_stxname));
			statsextinfo[j].dobj.namespace = tbinfo->dobj.namespace;
			statsextinfo[j].statsexttable = tbinfo;
			statsextinfo[j].statsextdef = dbms_md_strdup(dbms_md_get_field_value(res, j, i_stxdef));
		}

		dbms_md_free_tuples(res);
	}

	destroyStringInfo(query);
	pfree(query);
}

/*
 * getConstraints
 *
 * Get info about constraints on dumpable tables.
 *
 * Currently handles foreign keys only.
 * Unique and primary key constraints are handled with indexes,
 * while check constraints are processed in getTableAttrs().
 */
void
getConstraints(Archive *fout, TableInfo tblinfo[], int numTables)
{
	int			i,
				j;
	ConstraintInfo *constrinfo;
	StringInfo query;
	SPITupleTable   *res;
	int			i_contableoid,
				i_conoid,
				i_conname,
				i_confrelid,
				i_condef;
	int			ntups;

	query = createStringInfo();

	for (i = 0; i < numTables; i++)
	{
		TableInfo  *tbinfo = &tblinfo[i];

		if (!tbinfo->hastriggers ||
			!(tbinfo->dobj.dump & DUMP_COMPONENT_DEFINITION))
			continue;

		write_msg(NULL, "reading foreign key constraints for table \"%s.%s\"\n",
					  tbinfo->dobj.namespace->dobj.name,
					  tbinfo->dobj.name);

		/*
		 * select table schema to ensure constraint expr is qualified if
		 * needed
		 */
		selectSourceSchema(fout, tbinfo->dobj.namespace->dobj.name);

		resetStringInfo(query);
		appendStringInfo(query,
						  "SELECT tableoid, oid, conname, confrelid, "
						  "pg_catalog.pg_get_constraintdef(oid) AS condef "
						  "FROM pg_catalog.pg_constraint "
						  "WHERE conrelid = '%u'::pg_catalog.oid "
						  "AND contype = 'f'",
						  tbinfo->dobj.catId.oid);
		res = ExecuteSqlQuery(fout, query->data);

		ntups = dbms_md_get_tuple_num(res);

		i_contableoid = dbms_md_get_field_subscript(res, "tableoid");
		i_conoid = dbms_md_get_field_subscript(res, "oid");
		i_conname = dbms_md_get_field_subscript(res, "conname");
		i_confrelid = dbms_md_get_field_subscript(res, "confrelid");
		i_condef = dbms_md_get_field_subscript(res, "condef");

		constrinfo = (ConstraintInfo *) dbms_md_malloc0(fout->memory_ctx, ntups * sizeof(ConstraintInfo));

		for (j = 0; j < ntups; j++)
		{
			constrinfo[j].dobj.objType = DO_FK_CONSTRAINT;
			constrinfo[j].dobj.catId.tableoid = dbms_md_str_to_oid(dbms_md_get_field_value(res, j, i_contableoid));
			constrinfo[j].dobj.catId.oid = dbms_md_str_to_oid(dbms_md_get_field_value(res, j, i_conoid));
			AssignDumpId(fout, &constrinfo[j].dobj);
			constrinfo[j].dobj.name = dbms_md_strdup(dbms_md_get_field_value(res, j, i_conname));
			constrinfo[j].dobj.namespace = tbinfo->dobj.namespace;
			constrinfo[j].contable = tbinfo;
			constrinfo[j].condomain = NULL;
			constrinfo[j].contype = 'f';
			constrinfo[j].condef = dbms_md_strdup(dbms_md_get_field_value(res, j, i_condef));
			constrinfo[j].confrelid = dbms_md_str_to_oid(dbms_md_get_field_value(res, j, i_confrelid));
			constrinfo[j].conindex = 0;
			constrinfo[j].condeferrable = false;
			constrinfo[j].condeferred = false;
			constrinfo[j].conislocal = true;
			constrinfo[j].separate = true;
		}

		dbms_md_free_tuples(res);
	}

	destroyStringInfo(query);
	pfree(query);
}

/*
 * getDomainConstraints
 *
 * Get info about constraints on a domain.
 */
static void
getDomainConstraints(Archive *fout, TypeInfo *tyinfo)
{
	int			i;
	ConstraintInfo *constrinfo;
	StringInfo query;
	SPITupleTable   *res;
	int			i_tableoid,
				i_oid,
				i_conname,
				i_consrc;
	int			ntups;

	/*
	 * select appropriate schema to ensure names in constraint are properly
	 * qualified
	 */
	selectSourceSchema(fout, tyinfo->dobj.namespace->dobj.name);

	query = createStringInfo();

	if (fout->remoteVersion >= 90100)
		appendStringInfo(query, "SELECT tableoid, oid, conname, "
						  "pg_catalog.pg_get_constraintdef(oid) AS consrc, "
						  "convalidated "
						  "FROM pg_catalog.pg_constraint "
						  "WHERE contypid = '%u'::pg_catalog.oid "
						  "ORDER BY conname",
						  tyinfo->dobj.catId.oid);

	else
		appendStringInfo(query, "SELECT tableoid, oid, conname, "
						  "pg_catalog.pg_get_constraintdef(oid) AS consrc, "
						  "true as convalidated "
						  "FROM pg_catalog.pg_constraint "
						  "WHERE contypid = '%u'::pg_catalog.oid "
						  "ORDER BY conname",
						  tyinfo->dobj.catId.oid);

	res = ExecuteSqlQuery(fout, query->data);

	ntups = dbms_md_get_tuple_num(res);

	i_tableoid = dbms_md_get_field_subscript(res, "tableoid");
	i_oid = dbms_md_get_field_subscript(res, "oid");
	i_conname = dbms_md_get_field_subscript(res, "conname");
	i_consrc = dbms_md_get_field_subscript(res, "consrc");

	constrinfo = (ConstraintInfo *) dbms_md_malloc0(fout->memory_ctx, ntups * sizeof(ConstraintInfo));

	tyinfo->nDomChecks = ntups;
	tyinfo->domChecks = constrinfo;

	for (i = 0; i < ntups; i++)
	{
		bool		validated = dbms_md_get_field_value(res, i, SPI_RES_FIRST_FIELD_NO + 4)[0] == 't';

		constrinfo[i].dobj.objType = DO_CONSTRAINT;
		constrinfo[i].dobj.catId.tableoid = dbms_md_str_to_oid(dbms_md_get_field_value(res, i, i_tableoid));
		constrinfo[i].dobj.catId.oid = dbms_md_str_to_oid(dbms_md_get_field_value(res, i, i_oid));
		AssignDumpId(fout, &constrinfo[i].dobj);
		constrinfo[i].dobj.name = dbms_md_strdup(dbms_md_get_field_value(res, i, i_conname));
		constrinfo[i].dobj.namespace = tyinfo->dobj.namespace;
		constrinfo[i].contable = NULL;
		constrinfo[i].condomain = tyinfo;
		constrinfo[i].contype = 'c';
		constrinfo[i].condef = dbms_md_strdup(dbms_md_get_field_value(res, i, i_consrc));
		constrinfo[i].confrelid = InvalidOid;
		constrinfo[i].conindex = 0;
		constrinfo[i].condeferrable = false;
		constrinfo[i].condeferred = false;
		constrinfo[i].conislocal = true;

		constrinfo[i].separate = !validated;

		/*
		 * Make the domain depend on the constraint, ensuring it won't be
		 * output till any constraint dependencies are OK.  If the constraint
		 * has not been validated, it's going to be dumped after the domain
		 * anyway, so this doesn't matter.
		 */
		if (validated)
			addObjectDependency(fout, &tyinfo->dobj,
								constrinfo[i].dobj.dumpId);
	}

	dbms_md_free_tuples(res);

	destroyStringInfo(query);
	pfree(query);
}

/*
 * getRules
 *	  get basic information about every rule in the system
 *
 * numRules is set to the number of rules read in
 */
RuleInfo *
getRules(Archive *fout, int *numRules)
{
	SPITupleTable   *res;
	int			ntups;
	int			i;
	StringInfo query = createStringInfo();
	RuleInfo   *ruleinfo;
	int			i_tableoid;
	int			i_oid;
	int			i_rulename;
	int			i_ruletable;
	int			i_ev_type;
	int			i_is_instead;
	int			i_ev_enabled;

	/* Make sure we are in proper schema */
	selectSourceSchema(fout, "pg_catalog");

	if (fout->remoteVersion >= 80300)
	{
		appendStringInfoString(query, "SELECT "
							 "tableoid, oid, rulename, "
							 "ev_class AS ruletable, ev_type, is_instead, "
							 "ev_enabled "
							 "FROM pg_rewrite "
							 "ORDER BY oid");
	}
	else
	{
		appendStringInfoString(query, "SELECT "
							 "tableoid, oid, rulename, "
							 "ev_class AS ruletable, ev_type, is_instead, "
							 "'O'::char AS ev_enabled "
							 "FROM pg_rewrite "
							 "ORDER BY oid");
	}

	res = ExecuteSqlQuery(fout, query->data);

	ntups = dbms_md_get_tuple_num(res);

	*numRules = ntups;

	ruleinfo = (RuleInfo *) dbms_md_malloc0(fout->memory_ctx, ntups * sizeof(RuleInfo));

	i_tableoid = dbms_md_get_field_subscript(res, "tableoid");
	i_oid = dbms_md_get_field_subscript(res, "oid");
	i_rulename = dbms_md_get_field_subscript(res, "rulename");
	i_ruletable = dbms_md_get_field_subscript(res, "ruletable");
	i_ev_type = dbms_md_get_field_subscript(res, "ev_type");
	i_is_instead = dbms_md_get_field_subscript(res, "is_instead");
	i_ev_enabled = dbms_md_get_field_subscript(res, "ev_enabled");

	for (i = 0; i < ntups; i++)
	{
		Oid			ruletableoid;

		ruleinfo[i].dobj.objType = DO_RULE;
		ruleinfo[i].dobj.catId.tableoid = dbms_md_str_to_oid(dbms_md_get_field_value(res, i, i_tableoid));
		ruleinfo[i].dobj.catId.oid = dbms_md_str_to_oid(dbms_md_get_field_value(res, i, i_oid));
		AssignDumpId(fout, &ruleinfo[i].dobj);
		ruleinfo[i].dobj.name = dbms_md_strdup(dbms_md_get_field_value(res, i, i_rulename));
		ruletableoid = dbms_md_str_to_oid(dbms_md_get_field_value(res, i, i_ruletable));
		ruleinfo[i].ruletable = findTableByOid(fout, ruletableoid);
		if (ruleinfo[i].ruletable == NULL)
			elog(ERROR, "failed sanity check, parent table with OID %u of pg_rewrite entry with OID %u not found\n",
						  ruletableoid, ruleinfo[i].dobj.catId.oid);
		ruleinfo[i].dobj.namespace = ruleinfo[i].ruletable->dobj.namespace;
		ruleinfo[i].dobj.dump = ruleinfo[i].ruletable->dobj.dump;
		ruleinfo[i].ev_type = *(dbms_md_get_field_value(res, i, i_ev_type));
		ruleinfo[i].is_instead = *(dbms_md_get_field_value(res, i, i_is_instead)) == 't';
		ruleinfo[i].ev_enabled = *(dbms_md_get_field_value(res, i, i_ev_enabled));
		if (ruleinfo[i].ruletable)
		{
			/*
			 * If the table is a view or materialized view, force its ON
			 * SELECT rule to be sorted before the view itself --- this
			 * ensures that any dependencies for the rule affect the table's
			 * positioning. Other rules are forced to appear after their
			 * table.
			 */
			if ((ruleinfo[i].ruletable->relkind == RELKIND_VIEW ||
				 ruleinfo[i].ruletable->relkind == RELKIND_MATVIEW) &&
				ruleinfo[i].ev_type == '1' && ruleinfo[i].is_instead)
			{
				addObjectDependency(fout, &ruleinfo[i].ruletable->dobj,
									ruleinfo[i].dobj.dumpId);
				/* We'll merge the rule into CREATE VIEW, if possible */
				ruleinfo[i].separate = false;
			}
			else
			{
				addObjectDependency(fout, &ruleinfo[i].dobj,
									ruleinfo[i].ruletable->dobj.dumpId);
				ruleinfo[i].separate = true;
			}
		}
		else
			ruleinfo[i].separate = true;
	}

	dbms_md_free_tuples(res);

	destroyStringInfo(query);
	pfree(query);
	
	return ruleinfo;
}

/*
 * getTriggers
 *	  get information about every trigger on a dumpable table
 *
 * Note: trigger data is not returned directly to the caller, but it
 * does get entered into the DumpableObject tables.
 */
void
getTriggers(Archive *fout, TableInfo tblinfo[], int numTables)
{
	int			i,
				j;
	StringInfo query = createStringInfo();
	SPITupleTable   *res;
	TriggerInfo *tginfo;
	int			i_tableoid,
				i_oid,
				i_tgname,
				i_tgfname,
				i_tgtype,
				i_tgnargs,
				i_tgargs,
				i_tgisconstraint,
				i_tgconstrname,
				i_tgconstrrelid,
				i_tgconstrrelname,
				i_tgenabled,
				i_tgdeferrable,
				i_tginitdeferred,
				i_tgdef;
	int			ntups;

	for (i = 0; i < numTables; i++)
	{
		TableInfo  *tbinfo = &tblinfo[i];

		if (!tbinfo->hastriggers ||
			!(tbinfo->dobj.dump & DUMP_COMPONENT_DEFINITION))
			continue;

		write_msg(NULL, "reading triggers for table \"%s.%s\"\n",
					  tbinfo->dobj.namespace->dobj.name,
					  tbinfo->dobj.name);

		/*
		 * select table schema to ensure regproc name is qualified if needed
		 */
		selectSourceSchema(fout, tbinfo->dobj.namespace->dobj.name);

		resetStringInfo(query);
		if (fout->remoteVersion >= 90000)
		{
			/*
			 * NB: think not to use pretty=true in pg_get_triggerdef.  It
			 * could result in non-forward-compatible dumps of WHEN clauses
			 * due to under-parenthesization.
			 */
			appendStringInfo(query,
							  "SELECT tgname, "
							  "tgfoid::pg_catalog.regproc AS tgfname, "
							  "pg_catalog.pg_get_triggerdef(oid, false) AS tgdef, "
							  "tgenabled, tableoid, oid "
							  "FROM pg_catalog.pg_trigger t "
							  "WHERE tgrelid = '%u'::pg_catalog.oid "
							  "AND NOT tgisinternal",
							  tbinfo->dobj.catId.oid);
		}
		else if (fout->remoteVersion >= 80300)
		{
			/*
			 * We ignore triggers that are tied to a foreign-key constraint
			 */
			appendStringInfo(query,
							  "SELECT tgname, "
							  "tgfoid::pg_catalog.regproc AS tgfname, "
							  "tgtype, tgnargs, tgargs, tgenabled, "
							  "tgisconstraint, tgconstrname, tgdeferrable, "
							  "tgconstrrelid, tginitdeferred, tableoid, oid, "
							  "tgconstrrelid::pg_catalog.regclass AS tgconstrrelname "
							  "FROM pg_catalog.pg_trigger t "
							  "WHERE tgrelid = '%u'::pg_catalog.oid "
							  "AND tgconstraint = 0",
							  tbinfo->dobj.catId.oid);
		}
		else
		{
			/*
			 * We ignore triggers that are tied to a foreign-key constraint,
			 * but in these versions we have to grovel through pg_constraint
			 * to find out
			 */
			appendStringInfo(query,
							  "SELECT tgname, "
							  "tgfoid::pg_catalog.regproc AS tgfname, "
							  "tgtype, tgnargs, tgargs, tgenabled, "
							  "tgisconstraint, tgconstrname, tgdeferrable, "
							  "tgconstrrelid, tginitdeferred, tableoid, oid, "
							  "tgconstrrelid::pg_catalog.regclass AS tgconstrrelname "
							  "FROM pg_catalog.pg_trigger t "
							  "WHERE tgrelid = '%u'::pg_catalog.oid "
							  "AND (NOT tgisconstraint "
							  " OR NOT EXISTS"
							  "  (SELECT 1 FROM pg_catalog.pg_depend d "
							  "   JOIN pg_catalog.pg_constraint c ON (d.refclassid = c.tableoid AND d.refobjid = c.oid) "
							  "   WHERE d.classid = t.tableoid AND d.objid = t.oid AND d.deptype = 'i' AND c.contype = 'f'))",
							  tbinfo->dobj.catId.oid);
		}

		res = ExecuteSqlQuery(fout, query->data);

		ntups = dbms_md_get_tuple_num(res);

		i_tableoid = dbms_md_get_field_subscript(res, "tableoid");
		i_oid = dbms_md_get_field_subscript(res, "oid");
		i_tgname = dbms_md_get_field_subscript(res, "tgname");
		i_tgfname = dbms_md_get_field_subscript(res, "tgfname");
		i_tgtype = dbms_md_get_field_subscript(res, "tgtype");
		i_tgnargs = dbms_md_get_field_subscript(res, "tgnargs");
		i_tgargs = dbms_md_get_field_subscript(res, "tgargs");
		i_tgisconstraint = dbms_md_get_field_subscript(res, "tgisconstraint");
		i_tgconstrname = dbms_md_get_field_subscript(res, "tgconstrname");
		i_tgconstrrelid = dbms_md_get_field_subscript(res, "tgconstrrelid");
		i_tgconstrrelname = dbms_md_get_field_subscript(res, "tgconstrrelname");
		i_tgenabled = dbms_md_get_field_subscript(res, "tgenabled");
		i_tgdeferrable = dbms_md_get_field_subscript(res, "tgdeferrable");
		i_tginitdeferred = dbms_md_get_field_subscript(res, "tginitdeferred");
		i_tgdef = dbms_md_get_field_subscript(res, "tgdef");

		tginfo = (TriggerInfo *) dbms_md_malloc0(fout->memory_ctx, ntups * sizeof(TriggerInfo));

		tbinfo->numTriggers = ntups;
		tbinfo->triggers = tginfo;

		for (j = 0; j < ntups; j++)
		{
			tginfo[j].dobj.objType = DO_TRIGGER;
			tginfo[j].dobj.catId.tableoid = dbms_md_str_to_oid(dbms_md_get_field_value(res, j, i_tableoid));
			tginfo[j].dobj.catId.oid = dbms_md_str_to_oid(dbms_md_get_field_value(res, j, i_oid));
			AssignDumpId(fout, &tginfo[j].dobj);
			tginfo[j].dobj.name = dbms_md_strdup(dbms_md_get_field_value(res, j, i_tgname));
			tginfo[j].dobj.namespace = tbinfo->dobj.namespace;
			tginfo[j].tgtable = tbinfo;
			tginfo[j].tgenabled = *(dbms_md_get_field_value(res, j, i_tgenabled));
			if (i_tgdef >= 0)
			{
				tginfo[j].tgdef = dbms_md_strdup(dbms_md_get_field_value(res, j, i_tgdef));

				/* remaining fields are not valid if we have tgdef */
				tginfo[j].tgfname = NULL;
				tginfo[j].tgtype = 0;
				tginfo[j].tgnargs = 0;
				tginfo[j].tgargs = NULL;
				tginfo[j].tgisconstraint = false;
				tginfo[j].tgdeferrable = false;
				tginfo[j].tginitdeferred = false;
				tginfo[j].tgconstrname = NULL;
				tginfo[j].tgconstrrelid = InvalidOid;
				tginfo[j].tgconstrrelname = NULL;
			}
			else
			{
				tginfo[j].tgdef = NULL;

				tginfo[j].tgfname = dbms_md_strdup(dbms_md_get_field_value(res, j, i_tgfname));
				tginfo[j].tgtype = atoi(dbms_md_get_field_value(res, j, i_tgtype));
				tginfo[j].tgnargs = atoi(dbms_md_get_field_value(res, j, i_tgnargs));
				tginfo[j].tgargs = dbms_md_strdup(dbms_md_get_field_value(res, j, i_tgargs));
				tginfo[j].tgisconstraint = *(dbms_md_get_field_value(res, j, i_tgisconstraint)) == 't';
				tginfo[j].tgdeferrable = *(dbms_md_get_field_value(res, j, i_tgdeferrable)) == 't';
				tginfo[j].tginitdeferred = *(dbms_md_get_field_value(res, j, i_tginitdeferred)) == 't';

				if (tginfo[j].tgisconstraint)
				{
					tginfo[j].tgconstrname = dbms_md_strdup(dbms_md_get_field_value(res, j, i_tgconstrname));
					tginfo[j].tgconstrrelid = dbms_md_str_to_oid(dbms_md_get_field_value(res, j, i_tgconstrrelid));
					if (OidIsValid(tginfo[j].tgconstrrelid))
					{
						if (dbms_md_is_tuple_field_null(res, j, i_tgconstrrelname))
							elog(ERROR, "query produced null referenced table name for foreign key trigger \"%s\" on table \"%s\" (OID of table: %u)\n",
										  tginfo[j].dobj.name,
										  tbinfo->dobj.name,
										  tginfo[j].tgconstrrelid);
						tginfo[j].tgconstrrelname = dbms_md_strdup(dbms_md_get_field_value(res, j, i_tgconstrrelname));
					}
					else
						tginfo[j].tgconstrrelname = NULL;
				}
				else
				{
					tginfo[j].tgconstrname = NULL;
					tginfo[j].tgconstrrelid = InvalidOid;
					tginfo[j].tgconstrrelname = NULL;
				}
			}
		}

		dbms_md_free_tuples(res);
	}

	destroyStringInfo(query);
	pfree(query);
}

/*
 * getEventTriggers
 *	  get information about event triggers
 */
EventTriggerInfo *
getEventTriggers(Archive *fout, int *numEventTriggers)
{
	int			i;
	StringInfo query;
	SPITupleTable   *res;
	EventTriggerInfo *evtinfo;
	int			i_tableoid,
				i_oid,
				i_evtname,
				i_evtevent,
				i_evtowner,
				i_evttags,
				i_evtfname,
				i_evtenabled;
	int			ntups;

	/* Before 9.3, there are no event triggers */
	if (fout->remoteVersion < 90300)
	{
		*numEventTriggers = 0;
		return NULL;
	}

	query = createStringInfo();

	/* Make sure we are in proper schema */
	selectSourceSchema(fout, "pg_catalog");

	appendStringInfo(query,
					  "SELECT e.tableoid, e.oid, evtname, evtenabled, "
					  "evtevent, (%s evtowner) AS evtowner, "
					  "array_to_string(array("
					  "select quote_literal(x) "
					  " from unnest(evttags) as t(x)), ', ') as evttags, "
					  "e.evtfoid::regproc as evtfname "
					  "FROM pg_event_trigger e "
					  "ORDER BY e.oid",
					  username_subquery);

	res = ExecuteSqlQuery(fout, query->data);

	ntups = dbms_md_get_tuple_num(res);

	*numEventTriggers = ntups;

	evtinfo = (EventTriggerInfo *) dbms_md_malloc0(fout->memory_ctx, ntups * sizeof(EventTriggerInfo));

	i_tableoid = dbms_md_get_field_subscript(res, "tableoid");
	i_oid = dbms_md_get_field_subscript(res, "oid");
	i_evtname = dbms_md_get_field_subscript(res, "evtname");
	i_evtevent = dbms_md_get_field_subscript(res, "evtevent");
	i_evtowner = dbms_md_get_field_subscript(res, "evtowner");
	i_evttags = dbms_md_get_field_subscript(res, "evttags");
	i_evtfname = dbms_md_get_field_subscript(res, "evtfname");
	i_evtenabled = dbms_md_get_field_subscript(res, "evtenabled");

	for (i = 0; i < ntups; i++)
	{
		evtinfo[i].dobj.objType = DO_EVENT_TRIGGER;
		evtinfo[i].dobj.catId.tableoid = dbms_md_str_to_oid(dbms_md_get_field_value(res, i, i_tableoid));
		evtinfo[i].dobj.catId.oid = dbms_md_str_to_oid(dbms_md_get_field_value(res, i, i_oid));
		AssignDumpId(fout, &evtinfo[i].dobj);
		evtinfo[i].dobj.name = dbms_md_strdup(dbms_md_get_field_value(res, i, i_evtname));
		evtinfo[i].evtname = dbms_md_strdup(dbms_md_get_field_value(res, i, i_evtname));
		evtinfo[i].evtevent = dbms_md_strdup(dbms_md_get_field_value(res, i, i_evtevent));
		evtinfo[i].evtowner = dbms_md_strdup(dbms_md_get_field_value(res, i, i_evtowner));
		evtinfo[i].evttags = dbms_md_strdup(dbms_md_get_field_value(res, i, i_evttags));
		evtinfo[i].evtfname = dbms_md_strdup(dbms_md_get_field_value(res, i, i_evtfname));
		evtinfo[i].evtenabled = *(dbms_md_get_field_value(res, i, i_evtenabled));

		/* Decide whether we want to dump it */
		selectDumpableObject(&(evtinfo[i].dobj), fout, DBMS_MD_DOBJ_BUTT);

		/* Event Triggers do not currently have ACLs. */
		evtinfo[i].dobj.dump &= ~DUMP_COMPONENT_ACL;
	}

	dbms_md_free_tuples(res);

	destroyStringInfo(query);
	pfree(query);

	return evtinfo;
}

/*
 * getProcLangs
 *	  get basic information about every procedural language in the system
 *
 * numProcLangs is set to the number of langs read in
 *
 * NB: this must run after getFuncs() because we assume we can do
 * findFuncByOid().
 */
ProcLangInfo *
getProcLangs(Archive *fout, int *numProcLangs)
{
	DumpOptions *dopt = fout->dopt;
	SPITupleTable   *res;
	int			ntups;
	int			i;
	StringInfo query = createStringInfo();
	ProcLangInfo *planginfo;
	int			i_tableoid;
	int			i_oid;
	int			i_lanname;
	int			i_lanpltrusted;
	int			i_lanplcallfoid;
	int			i_laninline;
	int			i_lanvalidator;
	int			i_lanacl;
	int			i_rlanacl;
	int			i_initlanacl;
	int			i_initrlanacl;
	int			i_lanowner;

	/* Make sure we are in proper schema */
	selectSourceSchema(fout, "pg_catalog");

	if (fout->remoteVersion >= 90600)
	{
		StringInfo acl_subquery = createStringInfo();
		StringInfo racl_subquery = createStringInfo();
		StringInfo initacl_subquery = createStringInfo();
		StringInfo initracl_subquery = createStringInfo();

		buildACLQueries(acl_subquery, racl_subquery, initacl_subquery,
						initracl_subquery, "l.lanacl", "l.lanowner", "'l'",
						dopt->binary_upgrade);

		/* pg_language has a laninline column */
		appendStringInfo(query, "SELECT l.tableoid, l.oid, "
						  "l.lanname, l.lanpltrusted, l.lanplcallfoid, "
						  "l.laninline, l.lanvalidator, "
						  "%s AS lanacl, "
						  "%s AS rlanacl, "
						  "%s AS initlanacl, "
						  "%s AS initrlanacl, "
						  "(%s l.lanowner) AS lanowner "
						  "FROM pg_language l "
						  "LEFT JOIN pg_init_privs pip ON "
						  "(l.oid = pip.objoid "
						  "AND pip.classoid = 'pg_language'::regclass "
						  "AND pip.objsubid = 0) "
						  "WHERE l.lanispl "
						  "ORDER BY l.oid",
						  acl_subquery->data,
						  racl_subquery->data,
						  initacl_subquery->data,
						  initracl_subquery->data,
						  username_subquery);

		destroyStringInfo(acl_subquery);
		destroyStringInfo(racl_subquery);
		destroyStringInfo(initacl_subquery);
		destroyStringInfo(initracl_subquery);
	}
	else if (fout->remoteVersion >= 90000)
	{
		/* pg_language has a laninline column */
		appendStringInfo(query, "SELECT tableoid, oid, "
						  "lanname, lanpltrusted, lanplcallfoid, "
						  "laninline, lanvalidator, lanacl, NULL AS rlanacl, "
						  "NULL AS initlanacl, NULL AS initrlanacl, "
						  "(%s lanowner) AS lanowner "
						  "FROM pg_language "
						  "WHERE lanispl "
						  "ORDER BY oid",
						  username_subquery);
	}
	else if (fout->remoteVersion >= 80300)
	{
		/* pg_language has a lanowner column */
		appendStringInfo(query, "SELECT tableoid, oid, "
						  "lanname, lanpltrusted, lanplcallfoid, "
						  "0 AS laninline, lanvalidator, lanacl, "
						  "NULL AS rlanacl, "
						  "NULL AS initlanacl, NULL AS initrlanacl, "
						  "(%s lanowner) AS lanowner "
						  "FROM pg_language "
						  "WHERE lanispl "
						  "ORDER BY oid",
						  username_subquery);
	}
	else if (fout->remoteVersion >= 80100)
	{
		/* Languages are owned by the bootstrap superuser, OID 10 */
		appendStringInfo(query, "SELECT tableoid, oid, "
						  "lanname, lanpltrusted, lanplcallfoid, "
						  "0 AS laninline, lanvalidator, lanacl, "
						  "NULL AS rlanacl, "
						  "NULL AS initlanacl, NULL AS initrlanacl, "
						  "(%s '10') AS lanowner "
						  "FROM pg_language "
						  "WHERE lanispl "
						  "ORDER BY oid",
						  username_subquery);
	}
	else
	{
		/* Languages are owned by the bootstrap superuser, sysid 1 */
		appendStringInfo(query, "SELECT tableoid, oid, "
						  "lanname, lanpltrusted, lanplcallfoid, "
						  "0 AS laninline, lanvalidator, lanacl, "
						  "NULL AS rlanacl, "
						  "NULL AS initlanacl, NULL AS initrlanacl, "
						  "(%s '1') AS lanowner "
						  "FROM pg_language "
						  "WHERE lanispl "
						  "ORDER BY oid",
						  username_subquery);
	}

	res = ExecuteSqlQuery(fout, query->data);

	ntups = dbms_md_get_tuple_num(res);

	*numProcLangs = ntups;

	planginfo = (ProcLangInfo *) dbms_md_malloc0(fout->memory_ctx, ntups * sizeof(ProcLangInfo));

	i_tableoid = dbms_md_get_field_subscript(res, "tableoid");
	i_oid = dbms_md_get_field_subscript(res, "oid");
	i_lanname = dbms_md_get_field_subscript(res, "lanname");
	i_lanpltrusted = dbms_md_get_field_subscript(res, "lanpltrusted");
	i_lanplcallfoid = dbms_md_get_field_subscript(res, "lanplcallfoid");
	i_laninline = dbms_md_get_field_subscript(res, "laninline");
	i_lanvalidator = dbms_md_get_field_subscript(res, "lanvalidator");
	i_lanacl = dbms_md_get_field_subscript(res, "lanacl");
	i_rlanacl = dbms_md_get_field_subscript(res, "rlanacl");
	i_initlanacl = dbms_md_get_field_subscript(res, "initlanacl");
	i_initrlanacl = dbms_md_get_field_subscript(res, "initrlanacl");
	i_lanowner = dbms_md_get_field_subscript(res, "lanowner");

	for (i = 0; i < ntups; i++)
	{
		planginfo[i].dobj.objType = DO_PROCLANG;
		planginfo[i].dobj.catId.tableoid = dbms_md_str_to_oid(dbms_md_get_field_value(res, i, i_tableoid));
		planginfo[i].dobj.catId.oid = dbms_md_str_to_oid(dbms_md_get_field_value(res, i, i_oid));
		AssignDumpId(fout, &planginfo[i].dobj);

		planginfo[i].dobj.name = dbms_md_strdup(dbms_md_get_field_value(res, i, i_lanname));
		planginfo[i].lanpltrusted = *(dbms_md_get_field_value(res, i, i_lanpltrusted)) == 't';
		planginfo[i].lanplcallfoid = dbms_md_str_to_oid(dbms_md_get_field_value(res, i, i_lanplcallfoid));
		planginfo[i].laninline = dbms_md_str_to_oid(dbms_md_get_field_value(res, i, i_laninline));
		planginfo[i].lanvalidator = dbms_md_str_to_oid(dbms_md_get_field_value(res, i, i_lanvalidator));
		planginfo[i].lanacl = dbms_md_strdup(dbms_md_get_field_value(res, i, i_lanacl));
		planginfo[i].rlanacl = dbms_md_strdup(dbms_md_get_field_value(res, i, i_rlanacl));
		planginfo[i].initlanacl = dbms_md_strdup(dbms_md_get_field_value(res, i, i_initlanacl));
		planginfo[i].initrlanacl = dbms_md_strdup(dbms_md_get_field_value(res, i, i_initrlanacl));
		planginfo[i].lanowner = dbms_md_strdup(dbms_md_get_field_value(res, i, i_lanowner));

		/* Decide whether we want to dump it */
		selectDumpableProcLang(&(planginfo[i]), fout);

		/* Do not try to dump ACL if no ACL exists. */
		if (dbms_md_is_tuple_field_null(res, i, i_lanacl) && dbms_md_is_tuple_field_null(res, i, i_rlanacl) &&
			dbms_md_is_tuple_field_null(res, i, i_initlanacl) &&
			dbms_md_is_tuple_field_null(res, i, i_initrlanacl))
			planginfo[i].dobj.dump &= ~DUMP_COMPONENT_ACL;
	}

	dbms_md_free_tuples(res);

	destroyStringInfo(query);
	pfree(query);

	return planginfo;
}

/*
 * getCasts
 *	  get basic information about every cast in the system
 *
 * numCasts is set to the number of casts read in
 */
CastInfo *
getCasts(Archive *fout, int *numCasts)
{
	SPITupleTable   *res;
	int			ntups;
	int			i;
	StringInfo query = createStringInfo();
	CastInfo   *castinfo;
	int			i_tableoid;
	int			i_oid;
	int			i_castsource;
	int			i_casttarget;
	int			i_castfunc;
	int			i_castcontext;
	int			i_castmethod;
	MemoryContext oldContext;

	/* Make sure we are in proper schema */
	selectSourceSchema(fout, "pg_catalog");

	if (fout->remoteVersion >= 80400)
	{
		appendStringInfoString(query, "SELECT tableoid, oid, "
							 "castsource, casttarget, castfunc, castcontext, "
							 "castmethod "
							 "FROM pg_cast ORDER BY 3,4");
	}
	else
	{
		appendStringInfoString(query, "SELECT tableoid, oid, "
							 "castsource, casttarget, castfunc, castcontext, "
							 "CASE WHEN castfunc = 0 THEN 'b' ELSE 'f' END AS castmethod "
							 "FROM pg_cast ORDER BY 3,4");
	}

	res = ExecuteSqlQuery(fout, query->data);

	ntups = dbms_md_get_tuple_num(res);

	*numCasts = ntups;

	castinfo = (CastInfo *) dbms_md_malloc0(fout->memory_ctx, ntups * sizeof(CastInfo));

	i_tableoid = dbms_md_get_field_subscript(res, "tableoid");
	i_oid = dbms_md_get_field_subscript(res, "oid");
	i_castsource = dbms_md_get_field_subscript(res, "castsource");
	i_casttarget = dbms_md_get_field_subscript(res, "casttarget");
	i_castfunc = dbms_md_get_field_subscript(res, "castfunc");
	i_castcontext = dbms_md_get_field_subscript(res, "castcontext");
	i_castmethod = dbms_md_get_field_subscript(res, "castmethod");

	oldContext = MemoryContextSwitchTo(fout->memory_ctx);
	for (i = 0; i < ntups; i++)
	{
		StringInfoData namebuf;
		TypeInfo   *sTypeInfo;
		TypeInfo   *tTypeInfo;

		castinfo[i].dobj.objType = DO_CAST;
		castinfo[i].dobj.catId.tableoid = dbms_md_str_to_oid(dbms_md_get_field_value(res, i, i_tableoid));
		castinfo[i].dobj.catId.oid = dbms_md_str_to_oid(dbms_md_get_field_value(res, i, i_oid));
		AssignDumpId(fout, &castinfo[i].dobj);
		castinfo[i].castsource = dbms_md_str_to_oid(dbms_md_get_field_value(res, i, i_castsource));
		castinfo[i].casttarget = dbms_md_str_to_oid(dbms_md_get_field_value(res, i, i_casttarget));
		castinfo[i].castfunc = dbms_md_str_to_oid(dbms_md_get_field_value(res, i, i_castfunc));
		castinfo[i].castcontext = *(dbms_md_get_field_value(res, i, i_castcontext));
		castinfo[i].castmethod = *(dbms_md_get_field_value(res, i, i_castmethod));

		/*
		 * Try to name cast as concatenation of typnames.  This is only used
		 * for purposes of sorting.  If we fail to find either type, the name
		 * will be an empty string.
		 */
		initStringInfo(&namebuf);
		sTypeInfo = findTypeByOid(fout, castinfo[i].castsource);
		tTypeInfo = findTypeByOid(fout, castinfo[i].casttarget);
		if (sTypeInfo && tTypeInfo)
			appendStringInfo(&namebuf, "%s %s",
							  sTypeInfo->dobj.name, tTypeInfo->dobj.name);
		castinfo[i].dobj.name = namebuf.data;

		/* Decide whether we want to dump it */
		selectDumpableCast(&(castinfo[i]), fout);

		/* Casts do not currently have ACLs. */
		castinfo[i].dobj.dump &= ~DUMP_COMPONENT_ACL;
	}
	MemoryContextSwitchTo(oldContext);

	dbms_md_free_tuples(res);

	destroyStringInfo(query);
	pfree(query);
	
	return castinfo;
}

static char *
get_language_name(Archive *fout, Oid langid)
{
	StringInfo query;
	SPITupleTable   *res;
	char	   *lanname;
	char * fieldval = NULL;

	query = createStringInfo();
	appendStringInfo(query, "SELECT lanname FROM pg_language WHERE oid = %u", langid);
	res = ExecuteSqlQueryForSingleRow(fout, query->data);
	fieldval = dbms_md_get_field_value(res, 0, SPI_RES_FIRST_FIELD_NO);
	lanname = dbms_md_strdup(dbms_md_fmtId_internal(fieldval, true));
	destroyStringInfo(query);
	pfree(query);

	dbms_md_free_tuples(res);	

	return lanname;
}

/*
 * getTransforms
 *	  get basic information about every transform in the system
 *
 * numTransforms is set to the number of transforms read in
 */
TransformInfo *
getTransforms(Archive *fout, int *numTransforms)
{
	SPITupleTable   *res;
	int			ntups;
	int			i;
	StringInfo query;
	TransformInfo *transforminfo;
	int			i_tableoid;
	int			i_oid;
	int			i_trftype;
	int			i_trflang;
	int			i_trffromsql;
	int			i_trftosql;

	/* Transforms didn't exist pre-9.5 */
	if (fout->remoteVersion < 90500)
	{
		*numTransforms = 0;
		return NULL;
	}

	query = createStringInfo();

	/* Make sure we are in proper schema */
	selectSourceSchema(fout, "pg_catalog");

	appendStringInfo(query, "SELECT tableoid, oid, "
					  "trftype, trflang, trffromsql::oid, trftosql::oid "
					  "FROM pg_transform "
					  "ORDER BY 3,4");

	res = ExecuteSqlQuery(fout, query->data);

	ntups = dbms_md_get_tuple_num(res);

	*numTransforms = ntups;

	transforminfo = (TransformInfo *) dbms_md_malloc0(fout->memory_ctx, ntups * sizeof(TransformInfo));

	i_tableoid = dbms_md_get_field_subscript(res, "tableoid");
	i_oid = dbms_md_get_field_subscript(res, "oid");
	i_trftype = dbms_md_get_field_subscript(res, "trftype");
	i_trflang = dbms_md_get_field_subscript(res, "trflang");
	i_trffromsql = dbms_md_get_field_subscript(res, "trffromsql");
	i_trftosql = dbms_md_get_field_subscript(res, "trftosql");

	for (i = 0; i < ntups; i++)
	{
		StringInfoData namebuf;
		TypeInfo   *typeInfo;
		char	   *lanname;

		transforminfo[i].dobj.objType = DO_TRANSFORM;
		transforminfo[i].dobj.catId.tableoid = dbms_md_str_to_oid(dbms_md_get_field_value(res, i, i_tableoid));
		transforminfo[i].dobj.catId.oid = dbms_md_str_to_oid(dbms_md_get_field_value(res, i, i_oid));
		AssignDumpId(fout, &transforminfo[i].dobj);
		transforminfo[i].trftype = dbms_md_str_to_oid(dbms_md_get_field_value(res, i, i_trftype));
		transforminfo[i].trflang = dbms_md_str_to_oid(dbms_md_get_field_value(res, i, i_trflang));
		transforminfo[i].trffromsql = dbms_md_str_to_oid(dbms_md_get_field_value(res, i, i_trffromsql));
		transforminfo[i].trftosql = dbms_md_str_to_oid(dbms_md_get_field_value(res, i, i_trftosql));

		/*
		 * Try to name transform as concatenation of type and language name.
		 * This is only used for purposes of sorting.  If we fail to find
		 * either, the name will be an empty string.
		 */
		initStringInfo(&namebuf);
		typeInfo = findTypeByOid(fout, transforminfo[i].trftype);
		lanname = get_language_name(fout, transforminfo[i].trflang);
		if (typeInfo && lanname)
			appendStringInfo(&namebuf, "%s %s",
							  typeInfo->dobj.name, lanname);
		transforminfo[i].dobj.name = dbms_md_strdup(namebuf.data);
		free(lanname);

		/* Decide whether we want to dump it */
		selectDumpableObject(&(transforminfo[i].dobj), fout, DBMS_MD_DOBJ_BUTT);
	}

	dbms_md_free_tuples(res);

	destroyStringInfo(query);
	pfree(query);

	return transforminfo;
}

/*
 * getTableAttrs -
 *	  for each interesting table, read info about its attributes
 *	  (names, types, default values, CHECK constraints, etc)
 *
 * This is implemented in a very inefficient way right now, looping
 * through the tblinfo and doing a join per table to find the attrs and their
 * types.  However, because we want type names and so forth to be named
 * relative to the schema of each table, we couldn't do it in just one
 * query.  (Maybe one query per schema?)
 *
 *	modifies tblinfo
 */
void
getTableAttrs(Archive *fout, TableInfo *tblinfo, int numTables)
{
	DumpOptions *dopt = fout->dopt;
	int			i,
				j;
	StringInfo q = createStringInfo();
	int			i_attnum;
	int			i_attname;
	int			i_atttypname;
	int			i_atttypmod;
	int			i_attstattarget;
	int			i_attstorage;
	int			i_typstorage;
	int			i_attnotnull;
	int			i_atthasdef;
	int			i_attidentity;
	int			i_attisdropped;
	int			i_attlen;
	int			i_attalign;
	int			i_attislocal;
	int			i_attoptions;
	int			i_attcollation;
	int			i_attfdwoptions;
	SPITupleTable   *res;
	int			ntups;
	bool		hasdefaults;

	for (i = 0; i < numTables; i++)
	{
		TableInfo  *tbinfo = &tblinfo[i];

		/* Don't bother to collect info for sequences */
		if (tbinfo->relkind == RELKIND_SEQUENCE)
			continue;

		/* Don't bother with uninteresting tables, either */
		if (!tbinfo->interesting)
			continue;

		/*
		 * Make sure we are in proper schema for this table; this allows
		 * correct retrieval of formatted type names and default exprs
		 */
		if (tbinfo->dobj.namespace->dobj.name && strlen(tbinfo->dobj.namespace->dobj.name) > 0)
			selectSourceSchema(fout, tbinfo->dobj.namespace->dobj.name);

		/* find all the user attributes and their types */

		/*
		 * we must read the attribute names in attribute number order! because
		 * we will use the attnum to index into the attnames array later.
		 */
		write_msg(NULL, "finding the columns and types of table \"%s.%s\"\n",
					  tbinfo->dobj.namespace->dobj.name,
					  tbinfo->dobj.name);

		resetStringInfo(q);

		if (fout->remoteVersion >= 100000)
		{
			/*
			 * attidentity is new in version 10.
			 */
			appendStringInfo(q, "SELECT a.attnum, a.attname, a.atttypmod, "
							  "a.attstattarget, a.attstorage, t.typstorage, "
							  "a.attnotnull, a.atthasdef, a.attisdropped, "
							  "a.attlen, a.attalign, a.attislocal, "
							  "pg_catalog.format_type(t.oid,a.atttypmod) AS atttypname, "
							  "array_to_string(a.attoptions, ', ') AS attoptions, "
							  "CASE WHEN a.attcollation <> t.typcollation "
							  "THEN a.attcollation ELSE 0 END AS attcollation, "
							  "a.attidentity, "
							  "pg_catalog.array_to_string(ARRAY("
							  "SELECT pg_catalog.quote_ident(option_name) || "
							  "' ' || pg_catalog.quote_literal(option_value) "
							  "FROM pg_catalog.pg_options_to_table(attfdwoptions) "
							  "ORDER BY option_name"
							  "), E',\n    ') AS attfdwoptions "
							  "FROM pg_catalog.pg_attribute a LEFT JOIN pg_catalog.pg_type t "
							  "ON a.atttypid = t.oid "
							  "WHERE a.attrelid = '%u'::pg_catalog.oid "
							  "AND a.attnum > 0::pg_catalog.int2 "
							  "ORDER BY a.attnum",
							  tbinfo->dobj.catId.oid);
		}
		else if (fout->remoteVersion >= 90200)
		{
			/*
			 * attfdwoptions is new in 9.2.
			 */
			appendStringInfo(q, "SELECT a.attnum, a.attname, a.atttypmod, "
							  "a.attstattarget, a.attstorage, t.typstorage, "
							  "a.attnotnull, a.atthasdef, a.attisdropped, "
							  "a.attlen, a.attalign, a.attislocal, "
							  "pg_catalog.format_type(t.oid,a.atttypmod) AS atttypname, "
							  "array_to_string(a.attoptions, ', ') AS attoptions, "
							  "CASE WHEN a.attcollation <> t.typcollation "
							  "THEN a.attcollation ELSE 0 END AS attcollation, "
							  "pg_catalog.array_to_string(ARRAY("
							  "SELECT pg_catalog.quote_ident(option_name) || "
							  "' ' || pg_catalog.quote_literal(option_value) "
							  "FROM pg_catalog.pg_options_to_table(attfdwoptions) "
							  "ORDER BY option_name"
							  "), E',\n    ') AS attfdwoptions "
							  "FROM pg_catalog.pg_attribute a LEFT JOIN pg_catalog.pg_type t "
							  "ON a.atttypid = t.oid "
							  "WHERE a.attrelid = '%u'::pg_catalog.oid "
							  "AND a.attnum > 0::pg_catalog.int2 "
							  "ORDER BY a.attnum",
							  tbinfo->dobj.catId.oid);
		}
		else if (fout->remoteVersion >= 90100)
		{
			/*
			 * attcollation is new in 9.1.  Since we only want to dump COLLATE
			 * clauses for attributes whose collation is different from their
			 * type's default, we use a CASE here to suppress uninteresting
			 * attcollations cheaply.
			 */
			appendStringInfo(q, "SELECT a.attnum, a.attname, a.atttypmod, "
							  "a.attstattarget, a.attstorage, t.typstorage, "
							  "a.attnotnull, a.atthasdef, a.attisdropped, "
							  "a.attlen, a.attalign, a.attislocal, "
							  "pg_catalog.format_type(t.oid,a.atttypmod) AS atttypname, "
							  "array_to_string(a.attoptions, ', ') AS attoptions, "
							  "CASE WHEN a.attcollation <> t.typcollation "
							  "THEN a.attcollation ELSE 0 END AS attcollation, "
							  "NULL AS attfdwoptions "
							  "FROM pg_catalog.pg_attribute a LEFT JOIN pg_catalog.pg_type t "
							  "ON a.atttypid = t.oid "
							  "WHERE a.attrelid = '%u'::pg_catalog.oid "
							  "AND a.attnum > 0::pg_catalog.int2 "
							  "ORDER BY a.attnum",
							  tbinfo->dobj.catId.oid);
		}
		else if (fout->remoteVersion >= 90000)
		{
			/* attoptions is new in 9.0 */
			appendStringInfo(q, "SELECT a.attnum, a.attname, a.atttypmod, "
							  "a.attstattarget, a.attstorage, t.typstorage, "
							  "a.attnotnull, a.atthasdef, a.attisdropped, "
							  "a.attlen, a.attalign, a.attislocal, "
							  "pg_catalog.format_type(t.oid,a.atttypmod) AS atttypname, "
							  "array_to_string(a.attoptions, ', ') AS attoptions, "
							  "0 AS attcollation, "
							  "NULL AS attfdwoptions "
							  "FROM pg_catalog.pg_attribute a LEFT JOIN pg_catalog.pg_type t "
							  "ON a.atttypid = t.oid "
							  "WHERE a.attrelid = '%u'::pg_catalog.oid "
							  "AND a.attnum > 0::pg_catalog.int2 "
							  "ORDER BY a.attnum",
							  tbinfo->dobj.catId.oid);
		}
		else
		{
			/* need left join here to not fail on dropped columns ... */
			appendStringInfo(q, "SELECT a.attnum, a.attname, a.atttypmod, "
							  "a.attstattarget, a.attstorage, t.typstorage, "
							  "a.attnotnull, a.atthasdef, a.attisdropped, "
							  "a.attlen, a.attalign, a.attislocal, "
							  "pg_catalog.format_type(t.oid,a.atttypmod) AS atttypname, "
							  "'' AS attoptions, 0 AS attcollation, "
							  "NULL AS attfdwoptions "
							  "FROM pg_catalog.pg_attribute a LEFT JOIN pg_catalog.pg_type t "
							  "ON a.atttypid = t.oid "
							  "WHERE a.attrelid = '%u'::pg_catalog.oid "
							  "AND a.attnum > 0::pg_catalog.int2 "
							  "ORDER BY a.attnum",
							  tbinfo->dobj.catId.oid);
		}

		res = ExecuteSqlQuery(fout, q->data);

		ntups = dbms_md_get_tuple_num(res);

		i_attnum = dbms_md_get_field_subscript(res, "attnum");
		i_attname = dbms_md_get_field_subscript(res, "attname");
		i_atttypname = dbms_md_get_field_subscript(res, "atttypname");
		i_atttypmod = dbms_md_get_field_subscript(res, "atttypmod");
		i_attstattarget = dbms_md_get_field_subscript(res, "attstattarget");
		i_attstorage = dbms_md_get_field_subscript(res, "attstorage");
		i_typstorage = dbms_md_get_field_subscript(res, "typstorage");
		i_attnotnull = dbms_md_get_field_subscript(res, "attnotnull");
		i_atthasdef = dbms_md_get_field_subscript(res, "atthasdef");
		i_attidentity = dbms_md_get_field_subscript(res, "attidentity");
		i_attisdropped = dbms_md_get_field_subscript(res, "attisdropped");
		i_attlen = dbms_md_get_field_subscript(res, "attlen");
		i_attalign = dbms_md_get_field_subscript(res, "attalign");
		i_attislocal = dbms_md_get_field_subscript(res, "attislocal");
		i_attoptions = dbms_md_get_field_subscript(res, "attoptions");
		i_attcollation = dbms_md_get_field_subscript(res, "attcollation");
		i_attfdwoptions = dbms_md_get_field_subscript(res, "attfdwoptions");

		tbinfo->numatts = ntups;
		tbinfo->attnames = (char **) dbms_md_malloc0(fout->memory_ctx, ntups * sizeof(char *));
		tbinfo->atttypnames = (char **) dbms_md_malloc0(fout->memory_ctx, ntups * sizeof(char *));
		tbinfo->atttypmod = (int *) dbms_md_malloc0(fout->memory_ctx, ntups * sizeof(int));
		tbinfo->attstattarget = (int *) dbms_md_malloc0(fout->memory_ctx, ntups * sizeof(int));
		tbinfo->attstorage = (char *) dbms_md_malloc0(fout->memory_ctx, ntups * sizeof(char));
		tbinfo->typstorage = (char *) dbms_md_malloc0(fout->memory_ctx, ntups * sizeof(char));
		tbinfo->attidentity = (char *) dbms_md_malloc0(fout->memory_ctx, ntups * sizeof(char));
		tbinfo->attisdropped = (bool *) dbms_md_malloc0(fout->memory_ctx, ntups * sizeof(bool));
		tbinfo->attlen = (int *) dbms_md_malloc0(fout->memory_ctx, ntups * sizeof(int));
		tbinfo->attalign = (char *) dbms_md_malloc0(fout->memory_ctx, ntups * sizeof(char));
		tbinfo->attislocal = (bool *) dbms_md_malloc0(fout->memory_ctx, ntups * sizeof(bool));
		tbinfo->attoptions = (char **) dbms_md_malloc0(fout->memory_ctx, ntups * sizeof(char *));
		tbinfo->attcollation = (Oid *) dbms_md_malloc0(fout->memory_ctx, ntups * sizeof(Oid));
		tbinfo->attfdwoptions = (char **) dbms_md_malloc0(fout->memory_ctx, ntups * sizeof(char *));
		tbinfo->notnull = (bool *) dbms_md_malloc0(fout->memory_ctx, ntups * sizeof(bool));
		tbinfo->inhNotNull = (bool *) dbms_md_malloc0(fout->memory_ctx, ntups * sizeof(bool));
		tbinfo->attrdefs = (AttrDefInfo **) dbms_md_malloc0(fout->memory_ctx, ntups * sizeof(AttrDefInfo *));
		hasdefaults = false;

		for (j = 0; j < ntups; j++)
		{
			if (j + 1 != atoi(dbms_md_get_field_value(res, j, i_attnum)))
				elog(ERROR,
							  "invalid column numbering in table \"%s\"\n",
							  tbinfo->dobj.name);
			tbinfo->attnames[j] = dbms_md_strdup(dbms_md_get_field_value(res, j, i_attname));
			tbinfo->atttypnames[j] = dbms_md_strdup(dbms_md_get_field_value(res, j, i_atttypname));
			tbinfo->atttypmod[j] = atoi(dbms_md_get_field_value(res, j, i_atttypmod));
			tbinfo->attstattarget[j] = atoi(dbms_md_get_field_value(res, j, i_attstattarget));
			tbinfo->attstorage[j] = *(dbms_md_get_field_value(res, j, i_attstorage));
			tbinfo->typstorage[j] = *(dbms_md_get_field_value(res, j, i_typstorage));
			tbinfo->attidentity[j] = (i_attidentity >= 0 ? *(dbms_md_get_field_value(res, j, i_attidentity)) : '\0');
			tbinfo->needs_override = tbinfo->needs_override || (tbinfo->attidentity[j] == ATTRIBUTE_IDENTITY_ALWAYS);
			tbinfo->attisdropped[j] = (dbms_md_get_field_value(res, j, i_attisdropped)[0] == 't');
			tbinfo->attlen[j] = atoi(dbms_md_get_field_value(res, j, i_attlen));
			tbinfo->attalign[j] = *(dbms_md_get_field_value(res, j, i_attalign));
			tbinfo->attislocal[j] = (dbms_md_get_field_value(res, j, i_attislocal)[0] == 't');
			tbinfo->notnull[j] = (dbms_md_get_field_value(res, j, i_attnotnull)[0] == 't');
			tbinfo->attoptions[j] = dbms_md_strdup(dbms_md_get_field_value(res, j, i_attoptions));
			tbinfo->attcollation[j] = dbms_md_str_to_oid(dbms_md_get_field_value(res, j, i_attcollation));
			tbinfo->attfdwoptions[j] = dbms_md_strdup(dbms_md_get_field_value(res, j, i_attfdwoptions));
			tbinfo->attrdefs[j] = NULL; /* fix below */
			if (dbms_md_get_field_value(res, j, i_atthasdef)[0] == 't')
				hasdefaults = true;
			/* these flags will be set in flagInhAttrs() */
			tbinfo->inhNotNull[j] = false;
		}

		dbms_md_free_tuples(res);

		/*
		 * Get info about column defaults
		 */
		if (hasdefaults)
		{
			AttrDefInfo *attrdefs;
			int			numDefaults;

			write_msg(NULL, "finding default expressions of table \"%s.%s\"\n",
						  tbinfo->dobj.namespace->dobj.name,
						  tbinfo->dobj.name);

			printfStringInfo(q, "SELECT tableoid, oid, adnum, "
							  "pg_catalog.pg_get_expr(adbin, adrelid) AS adsrc "
							  "FROM pg_catalog.pg_attrdef "
							  "WHERE adrelid = '%u'::pg_catalog.oid",
							  tbinfo->dobj.catId.oid);

			res = ExecuteSqlQuery(fout, q->data);

			numDefaults = dbms_md_get_tuple_num(res);
			attrdefs = (AttrDefInfo *) dbms_md_malloc0(fout->memory_ctx, numDefaults * sizeof(AttrDefInfo));

			for (j = 0; j < numDefaults; j++)
			{
				int			adnum;

				adnum = atoi(dbms_md_get_field_value(res, j, SPI_RES_FIRST_FIELD_NO + 2));

				if (adnum <= 0 || adnum > ntups)
					elog(ERROR,
								  "invalid adnum value %d for table \"%s\"\n",
								  adnum, tbinfo->dobj.name);

				/*
				 * dropped columns shouldn't have defaults, but just in case,
				 * ignore 'em
				 */
				if (tbinfo->attisdropped[adnum - 1])
					continue;

				attrdefs[j].dobj.objType = DO_ATTRDEF;
				attrdefs[j].dobj.catId.tableoid = dbms_md_str_to_oid(dbms_md_get_field_value(res, j, SPI_RES_FIRST_FIELD_NO + 0));
				attrdefs[j].dobj.catId.oid = dbms_md_str_to_oid(dbms_md_get_field_value(res, j, SPI_RES_FIRST_FIELD_NO + 1));
				AssignDumpId(fout, &attrdefs[j].dobj);
				attrdefs[j].adtable = tbinfo;
				attrdefs[j].adnum = adnum;
				attrdefs[j].adef_expr = dbms_md_strdup(dbms_md_get_field_value(res, j, SPI_RES_FIRST_FIELD_NO + 3));

				attrdefs[j].dobj.name = dbms_md_strdup(tbinfo->dobj.name);
				attrdefs[j].dobj.namespace = tbinfo->dobj.namespace;

				attrdefs[j].dobj.dump = tbinfo->dobj.dump;

				/*
				 * Defaults on a VIEW must always be dumped as separate ALTER
				 * TABLE commands.  Defaults on regular tables are dumped as
				 * part of the CREATE TABLE if possible, which it won't be if
				 * the column is not going to be emitted explicitly.
				 */
				if (tbinfo->relkind == RELKIND_VIEW)
				{
					attrdefs[j].separate = true;
				}
				else if (!shouldPrintColumn(dopt, tbinfo, adnum - 1))
				{
					/* column will be suppressed, print default separately */
					attrdefs[j].separate = true;
				}
				else
				{
					attrdefs[j].separate = false;

					/*
					 * Mark the default as needing to appear before the table,
					 * so that any dependencies it has must be emitted before
					 * the CREATE TABLE.  If this is not possible, we'll
					 * change to "separate" mode while sorting dependencies.
					 */
					addObjectDependency(fout, &tbinfo->dobj,
										attrdefs[j].dobj.dumpId);
				}

				tbinfo->attrdefs[adnum - 1] = &attrdefs[j];
			}
			dbms_md_free_tuples(res);
		}

		/*
		 * Get info about table CHECK constraints
		 */
		if (tbinfo->ncheck > 0)
		{
			ConstraintInfo *constrs;
			int			numConstrs;

			write_msg(NULL, "finding check constraints for table \"%s.%s\"\n",
						  tbinfo->dobj.namespace->dobj.name,
						  tbinfo->dobj.name);

			resetStringInfo(q);
			if (fout->remoteVersion >= 90200)
			{
				/*
				 * convalidated is new in 9.2 (actually, it is there in 9.1,
				 * but it wasn't ever false for check constraints until 9.2).
				 */
				appendStringInfo(q, "SELECT tableoid, oid, conname, "
								  "pg_catalog.pg_get_constraintdef(oid) AS consrc, "
								  "conislocal, convalidated "
								  "FROM pg_catalog.pg_constraint "
								  "WHERE conrelid = '%u'::pg_catalog.oid "
								  "   AND contype = 'c' "
								  "ORDER BY conname",
								  tbinfo->dobj.catId.oid);
			}
			else if (fout->remoteVersion >= 80400)
			{
				/* conislocal is new in 8.4 */
				appendStringInfo(q, "SELECT tableoid, oid, conname, "
								  "pg_catalog.pg_get_constraintdef(oid) AS consrc, "
								  "conislocal, true AS convalidated "
								  "FROM pg_catalog.pg_constraint "
								  "WHERE conrelid = '%u'::pg_catalog.oid "
								  "   AND contype = 'c' "
								  "ORDER BY conname",
								  tbinfo->dobj.catId.oid);
			}
			else
			{
				appendStringInfo(q, "SELECT tableoid, oid, conname, "
								  "pg_catalog.pg_get_constraintdef(oid) AS consrc, "
								  "true AS conislocal, true AS convalidated "
								  "FROM pg_catalog.pg_constraint "
								  "WHERE conrelid = '%u'::pg_catalog.oid "
								  "   AND contype = 'c' "
								  "ORDER BY conname",
								  tbinfo->dobj.catId.oid);
			}

			res = ExecuteSqlQuery(fout, q->data);

			numConstrs = dbms_md_get_tuple_num(res);
			if (numConstrs != tbinfo->ncheck)
			{
				elog(WARNING, ngettext("expected %d check constraint on table \"%s\" but found %d\n",
										 "expected %d check constraints on table \"%s\" but found %d\n",
										 tbinfo->ncheck),
						  tbinfo->ncheck, tbinfo->dobj.name, numConstrs);
				elog(ERROR, "(The system catalogs might be corrupted.)\n");
				return ;
			}

			constrs = (ConstraintInfo *) dbms_md_malloc0(fout->memory_ctx, numConstrs * sizeof(ConstraintInfo));
			tbinfo->checkexprs = constrs;

			for (j = 0; j < numConstrs; j++)
			{
				bool		validated = dbms_md_get_field_value(res, j, SPI_RES_FIRST_FIELD_NO + 5)[0] == 't';

				constrs[j].dobj.objType = DO_CONSTRAINT;
				constrs[j].dobj.catId.tableoid = dbms_md_str_to_oid(dbms_md_get_field_value(res, j, SPI_RES_FIRST_FIELD_NO + 0));
				constrs[j].dobj.catId.oid = dbms_md_str_to_oid(dbms_md_get_field_value(res, j, SPI_RES_FIRST_FIELD_NO + 1));
				AssignDumpId(fout, &constrs[j].dobj);
				constrs[j].dobj.name = dbms_md_strdup(dbms_md_get_field_value(res, j, SPI_RES_FIRST_FIELD_NO + 2));
				constrs[j].dobj.namespace = tbinfo->dobj.namespace;
				constrs[j].contable = tbinfo;
				constrs[j].condomain = NULL;
				constrs[j].contype = 'c';
				constrs[j].condef = dbms_md_strdup(dbms_md_get_field_value(res, j, SPI_RES_FIRST_FIELD_NO + 3));
				constrs[j].confrelid = InvalidOid;
				constrs[j].conindex = 0;
				constrs[j].condeferrable = false;
				constrs[j].condeferred = false;
				constrs[j].conislocal = (dbms_md_get_field_value(res, j, SPI_RES_FIRST_FIELD_NO + 4)[0] == 't');

				/*
				 * An unvalidated constraint needs to be dumped separately, so
				 * that potentially-violating existing data is loaded before
				 * the constraint.
				 */
				constrs[j].separate = !validated;

				constrs[j].dobj.dump = tbinfo->dobj.dump;

				/*
				 * Mark the constraint as needing to appear before the table
				 * --- this is so that any other dependencies of the
				 * constraint will be emitted before we try to create the
				 * table.  If the constraint is to be dumped separately, it
				 * will be dumped after data is loaded anyway, so don't do it.
				 * (There's an automatic dependency in the opposite direction
				 * anyway, so don't need to add one manually here.)
				 */
				if (!constrs[j].separate)
					addObjectDependency(fout, &tbinfo->dobj,
										constrs[j].dobj.dumpId);

				/*
				 * If the constraint is inherited, this will be detected later
				 * (in pre-8.4 databases).  We also detect later if the
				 * constraint must be split out from the table definition.
				 */
			}
			dbms_md_free_tuples(res);
		}
	}

	destroyStringInfo(q);
	pfree(q);
}

/*
 * Test whether a column should be printed as part of table's CREATE TABLE.
 * Column number is zero-based.
 *
 * Normally this is always true, but it's false for dropped columns, as well
 * as those that were inherited without any local definition.  (If we print
 * such a column it will mistakenly get pg_attribute.attislocal set to true.)
 * However, in binary_upgrade mode, we must print all such columns anyway and
 * fix the attislocal/attisdropped state later, so as to keep control of the
 * physical column order.
 *
 * This function exists because there are scattered nonobvious places that
 * must be kept in sync with this decision.
 */
bool
shouldPrintColumn(DumpOptions *dopt, TableInfo *tbinfo, int colno)
{
	if (dopt->binary_upgrade)
		return true;
	return (tbinfo->attislocal[colno] && !tbinfo->attisdropped[colno]);
}


/*
 * getTSParsers:
 *	  read all text search parsers in the system catalogs and return them
 *	  in the TSParserInfo* structure
 *
 *	numTSParsers is set to the number of parsers read in
 */
TSParserInfo *
getTSParsers(Archive *fout, int *numTSParsers)
{
	SPITupleTable   *res;
	int			ntups;
	int			i;
	StringInfo query;
	TSParserInfo *prsinfo;
	int			i_tableoid;
	int			i_oid;
	int			i_prsname;
	int			i_prsnamespace;
	int			i_prsstart;
	int			i_prstoken;
	int			i_prsend;
	int			i_prsheadline;
	int			i_prslextype;

	/* Before 8.3, there is no built-in text search support */
	if (fout->remoteVersion < 80300)
	{
		*numTSParsers = 0;
		return NULL;
	}

	query = createStringInfo();

	/*
	 * find all text search objects, including builtin ones; we filter out
	 * system-defined objects at dump-out time.
	 */

	/* Make sure we are in proper schema */
	selectSourceSchema(fout, "pg_catalog");

	appendStringInfoString(query, "SELECT tableoid, oid, prsname, prsnamespace, "
						 "prsstart::oid, prstoken::oid, "
						 "prsend::oid, prsheadline::oid, prslextype::oid "
						 "FROM pg_ts_parser");

	res = ExecuteSqlQuery(fout, query->data);

	ntups = dbms_md_get_tuple_num(res);
	*numTSParsers = ntups;

	prsinfo = (TSParserInfo *) dbms_md_malloc0(fout->memory_ctx, ntups * sizeof(TSParserInfo));

	i_tableoid = dbms_md_get_field_subscript(res, "tableoid");
	i_oid = dbms_md_get_field_subscript(res, "oid");
	i_prsname = dbms_md_get_field_subscript(res, "prsname");
	i_prsnamespace = dbms_md_get_field_subscript(res, "prsnamespace");
	i_prsstart = dbms_md_get_field_subscript(res, "prsstart");
	i_prstoken = dbms_md_get_field_subscript(res, "prstoken");
	i_prsend = dbms_md_get_field_subscript(res, "prsend");
	i_prsheadline = dbms_md_get_field_subscript(res, "prsheadline");
	i_prslextype = dbms_md_get_field_subscript(res, "prslextype");

	for (i = 0; i < ntups; i++)
	{
		prsinfo[i].dobj.objType = DO_TSPARSER;
		prsinfo[i].dobj.catId.tableoid = dbms_md_str_to_oid(dbms_md_get_field_value(res, i, i_tableoid));
		prsinfo[i].dobj.catId.oid = dbms_md_str_to_oid(dbms_md_get_field_value(res, i, i_oid));
		AssignDumpId(fout, &prsinfo[i].dobj);
		prsinfo[i].dobj.name = dbms_md_strdup(dbms_md_get_field_value(res, i, i_prsname));
		prsinfo[i].dobj.namespace =
			findNamespace(fout,
						  dbms_md_str_to_oid(dbms_md_get_field_value(res, i, i_prsnamespace)));
		prsinfo[i].prsstart = dbms_md_str_to_oid(dbms_md_get_field_value(res, i, i_prsstart));
		prsinfo[i].prstoken = dbms_md_str_to_oid(dbms_md_get_field_value(res, i, i_prstoken));
		prsinfo[i].prsend = dbms_md_str_to_oid(dbms_md_get_field_value(res, i, i_prsend));
		prsinfo[i].prsheadline = dbms_md_str_to_oid(dbms_md_get_field_value(res, i, i_prsheadline));
		prsinfo[i].prslextype = dbms_md_str_to_oid(dbms_md_get_field_value(res, i, i_prslextype));

		/* Decide whether we want to dump it */
		selectDumpableObject(&(prsinfo[i].dobj), fout, DBMS_MD_DOBJ_BUTT);

		/* Text Search Parsers do not currently have ACLs. */
		prsinfo[i].dobj.dump &= ~DUMP_COMPONENT_ACL;
	}

	dbms_md_free_tuples(res);

	destroyStringInfo(query);
	pfree(query);

	return prsinfo;
}

/*
 * getTSDictionaries:
 *	  read all text search dictionaries in the system catalogs and return them
 *	  in the TSDictInfo* structure
 *
 *	numTSDicts is set to the number of dictionaries read in
 */
TSDictInfo *
getTSDictionaries(Archive *fout, int *numTSDicts)
{
	SPITupleTable   *res;
	int			ntups;
	int			i;
	StringInfo query;
	TSDictInfo *dictinfo;
	int			i_tableoid;
	int			i_oid;
	int			i_dictname;
	int			i_dictnamespace;
	int			i_rolname;
	int			i_dicttemplate;
	int			i_dictinitoption;

	/* Before 8.3, there is no built-in text search support */
	if (fout->remoteVersion < 80300)
	{
		*numTSDicts = 0;
		return NULL;
	}

	query = createStringInfo();

	/* Make sure we are in proper schema */
	selectSourceSchema(fout, "pg_catalog");

	appendStringInfo(query, "SELECT tableoid, oid, dictname, "
					  "dictnamespace, (%s dictowner) AS rolname, "
					  "dicttemplate, dictinitoption "
					  "FROM pg_ts_dict",
					  username_subquery);

	res = ExecuteSqlQuery(fout, query->data);

	ntups = dbms_md_get_tuple_num(res);
	*numTSDicts = ntups;

	dictinfo = (TSDictInfo *) dbms_md_malloc0(fout->memory_ctx, ntups * sizeof(TSDictInfo));

	i_tableoid = dbms_md_get_field_subscript(res, "tableoid");
	i_oid = dbms_md_get_field_subscript(res, "oid");
	i_dictname = dbms_md_get_field_subscript(res, "dictname");
	i_dictnamespace = dbms_md_get_field_subscript(res, "dictnamespace");
	i_rolname = dbms_md_get_field_subscript(res, "rolname");
	i_dictinitoption = dbms_md_get_field_subscript(res, "dictinitoption");
	i_dicttemplate = dbms_md_get_field_subscript(res, "dicttemplate");

	for (i = 0; i < ntups; i++)
	{
		dictinfo[i].dobj.objType = DO_TSDICT;
		dictinfo[i].dobj.catId.tableoid = dbms_md_str_to_oid(dbms_md_get_field_value(res, i, i_tableoid));
		dictinfo[i].dobj.catId.oid = dbms_md_str_to_oid(dbms_md_get_field_value(res, i, i_oid));
		AssignDumpId(fout, &dictinfo[i].dobj);
		dictinfo[i].dobj.name = dbms_md_strdup(dbms_md_get_field_value(res, i, i_dictname));
		dictinfo[i].dobj.namespace =
			findNamespace(fout,
						  dbms_md_str_to_oid(dbms_md_get_field_value(res, i, i_dictnamespace)));
		dictinfo[i].rolname = dbms_md_strdup(dbms_md_get_field_value(res, i, i_rolname));
		dictinfo[i].dicttemplate = dbms_md_str_to_oid(dbms_md_get_field_value(res, i, i_dicttemplate));
		if (dbms_md_is_tuple_field_null(res, i, i_dictinitoption))
			dictinfo[i].dictinitoption = NULL;
		else
			dictinfo[i].dictinitoption = dbms_md_strdup(dbms_md_get_field_value(res, i, i_dictinitoption));

		/* Decide whether we want to dump it */
		selectDumpableObject(&(dictinfo[i].dobj), fout, DBMS_MD_DOBJ_BUTT);

		/* Text Search Dictionaries do not currently have ACLs. */
		dictinfo[i].dobj.dump &= ~DUMP_COMPONENT_ACL;
	}

	dbms_md_free_tuples(res);

	destroyStringInfo(query);
	pfree(query);

	return dictinfo;
}

/*
 * getTSTemplates:
 *	  read all text search templates in the system catalogs and return them
 *	  in the TSTemplateInfo* structure
 *
 *	numTSTemplates is set to the number of templates read in
 */
TSTemplateInfo *
getTSTemplates(Archive *fout, int *numTSTemplates)
{
	SPITupleTable   *res;
	int			ntups;
	int			i;
	StringInfo query;
	TSTemplateInfo *tmplinfo;
	int			i_tableoid;
	int			i_oid;
	int			i_tmplname;
	int			i_tmplnamespace;
	int			i_tmplinit;
	int			i_tmpllexize;

	/* Before 8.3, there is no built-in text search support */
	if (fout->remoteVersion < 80300)
	{
		*numTSTemplates = 0;
		return NULL;
	}

	query = createStringInfo();

	/* Make sure we are in proper schema */
	selectSourceSchema(fout, "pg_catalog");

	appendStringInfoString(query, "SELECT tableoid, oid, tmplname, "
						 "tmplnamespace, tmplinit::oid, tmpllexize::oid "
						 "FROM pg_ts_template");

	res = ExecuteSqlQuery(fout, query->data);

	ntups = dbms_md_get_tuple_num(res);
	*numTSTemplates = ntups;

	tmplinfo = (TSTemplateInfo *) dbms_md_malloc0(fout->memory_ctx, ntups * sizeof(TSTemplateInfo));

	i_tableoid = dbms_md_get_field_subscript(res, "tableoid");
	i_oid = dbms_md_get_field_subscript(res, "oid");
	i_tmplname = dbms_md_get_field_subscript(res, "tmplname");
	i_tmplnamespace = dbms_md_get_field_subscript(res, "tmplnamespace");
	i_tmplinit = dbms_md_get_field_subscript(res, "tmplinit");
	i_tmpllexize = dbms_md_get_field_subscript(res, "tmpllexize");

	for (i = 0; i < ntups; i++)
	{
		tmplinfo[i].dobj.objType = DO_TSTEMPLATE;
		tmplinfo[i].dobj.catId.tableoid = dbms_md_str_to_oid(dbms_md_get_field_value(res, i, i_tableoid));
		tmplinfo[i].dobj.catId.oid = dbms_md_str_to_oid(dbms_md_get_field_value(res, i, i_oid));
		AssignDumpId(fout, &tmplinfo[i].dobj);
		tmplinfo[i].dobj.name = dbms_md_strdup(dbms_md_get_field_value(res, i, i_tmplname));
		tmplinfo[i].dobj.namespace =
			findNamespace(fout,
						  dbms_md_str_to_oid(dbms_md_get_field_value(res, i, i_tmplnamespace)));
		tmplinfo[i].tmplinit = dbms_md_str_to_oid(dbms_md_get_field_value(res, i, i_tmplinit));
		tmplinfo[i].tmpllexize = dbms_md_str_to_oid(dbms_md_get_field_value(res, i, i_tmpllexize));

		/* Decide whether we want to dump it */
		selectDumpableObject(&(tmplinfo[i].dobj), fout, DBMS_MD_DOBJ_BUTT);

		/* Text Search Templates do not currently have ACLs. */
		tmplinfo[i].dobj.dump &= ~DUMP_COMPONENT_ACL;
	}

	dbms_md_free_tuples(res);

	destroyStringInfo(query);
	pfree(query);

	return tmplinfo;
}

/*
 * getTSConfigurations:
 *	  read all text search configurations in the system catalogs and return
 *	  them in the TSConfigInfo* structure
 *
 *	numTSConfigs is set to the number of configurations read in
 */
TSConfigInfo *
getTSConfigurations(Archive *fout, int *numTSConfigs)
{
	SPITupleTable   *res;
	int			ntups;
	int			i;
	StringInfo query;
	TSConfigInfo *cfginfo;
	int			i_tableoid;
	int			i_oid;
	int			i_cfgname;
	int			i_cfgnamespace;
	int			i_rolname;
	int			i_cfgparser;

	/* Before 8.3, there is no built-in text search support */
	if (fout->remoteVersion < 80300)
	{
		*numTSConfigs = 0;
		return NULL;
	}

	query = createStringInfo();

	/* Make sure we are in proper schema */
	selectSourceSchema(fout, "pg_catalog");

	appendStringInfo(query, "SELECT tableoid, oid, cfgname, "
					  "cfgnamespace, (%s cfgowner) AS rolname, cfgparser "
					  "FROM pg_ts_config",
					  username_subquery);

	res = ExecuteSqlQuery(fout, query->data);

	ntups = dbms_md_get_tuple_num(res);
	*numTSConfigs = ntups;

	cfginfo = (TSConfigInfo *) dbms_md_malloc0(fout->memory_ctx, ntups * sizeof(TSConfigInfo));

	i_tableoid = dbms_md_get_field_subscript(res, "tableoid");
	i_oid = dbms_md_get_field_subscript(res, "oid");
	i_cfgname = dbms_md_get_field_subscript(res, "cfgname");
	i_cfgnamespace = dbms_md_get_field_subscript(res, "cfgnamespace");
	i_rolname = dbms_md_get_field_subscript(res, "rolname");
	i_cfgparser = dbms_md_get_field_subscript(res, "cfgparser");

	for (i = 0; i < ntups; i++)
	{
		cfginfo[i].dobj.objType = DO_TSCONFIG;
		cfginfo[i].dobj.catId.tableoid = dbms_md_str_to_oid(dbms_md_get_field_value(res, i, i_tableoid));
		cfginfo[i].dobj.catId.oid = dbms_md_str_to_oid(dbms_md_get_field_value(res, i, i_oid));
		AssignDumpId(fout, &cfginfo[i].dobj);
		cfginfo[i].dobj.name = dbms_md_strdup(dbms_md_get_field_value(res, i, i_cfgname));
		cfginfo[i].dobj.namespace =
			findNamespace(fout,
						  dbms_md_str_to_oid(dbms_md_get_field_value(res, i, i_cfgnamespace)));
		cfginfo[i].rolname = dbms_md_strdup(dbms_md_get_field_value(res, i, i_rolname));
		cfginfo[i].cfgparser = dbms_md_str_to_oid(dbms_md_get_field_value(res, i, i_cfgparser));

		/* Decide whether we want to dump it */
		selectDumpableObject(&(cfginfo[i].dobj), fout, DBMS_MD_DOBJ_BUTT);

		/* Text Search Configurations do not currently have ACLs. */
		cfginfo[i].dobj.dump &= ~DUMP_COMPONENT_ACL;
	}

	dbms_md_free_tuples(res);

	destroyStringInfo(query);
	pfree(query);

	return cfginfo;
}

/*
 * getForeignDataWrappers:
 *	  read all foreign-data wrappers in the system catalogs and return
 *	  them in the FdwInfo* structure
 *
 *	numForeignDataWrappers is set to the number of fdws read in
 */
FdwInfo *
getForeignDataWrappers(Archive *fout, int *numForeignDataWrappers)
{
	DumpOptions *dopt = fout->dopt;
	SPITupleTable   *res;
	int			ntups;
	int			i;
	StringInfo query;
	FdwInfo    *fdwinfo;
	int			i_tableoid;
	int			i_oid;
	int			i_fdwname;
	int			i_rolname;
	int			i_fdwhandler;
	int			i_fdwvalidator;
	int			i_fdwacl;
	int			i_rfdwacl;
	int			i_initfdwacl;
	int			i_initrfdwacl;
	int			i_fdwoptions;

	/* Before 8.4, there are no foreign-data wrappers */
	if (fout->remoteVersion < 80400)
	{
		*numForeignDataWrappers = 0;
		return NULL;
	}

	query = createStringInfo();

	/* Make sure we are in proper schema */
	selectSourceSchema(fout, "pg_catalog");

	if (fout->remoteVersion >= 90600)
	{
		StringInfo acl_subquery = createStringInfo();
		StringInfo racl_subquery = createStringInfo();
		StringInfo initacl_subquery = createStringInfo();
		StringInfo initracl_subquery = createStringInfo();

		buildACLQueries(acl_subquery, racl_subquery, initacl_subquery,
						initracl_subquery, "f.fdwacl", "f.fdwowner", "'F'",
						dopt->binary_upgrade);

		appendStringInfo(query, "SELECT f.tableoid, f.oid, f.fdwname, "
						  "(%s f.fdwowner) AS rolname, "
						  "f.fdwhandler::pg_catalog.regproc, "
						  "f.fdwvalidator::pg_catalog.regproc, "
						  "%s AS fdwacl, "
						  "%s AS rfdwacl, "
						  "%s AS initfdwacl, "
						  "%s AS initrfdwacl, "
						  "array_to_string(ARRAY("
						  "SELECT quote_ident(option_name) || ' ' || "
						  "quote_literal(option_value) "
						  "FROM pg_options_to_table(f.fdwoptions) "
						  "ORDER BY option_name"
						  "), E',\n    ') AS fdwoptions "
						  "FROM pg_foreign_data_wrapper f "
						  "LEFT JOIN pg_init_privs pip ON "
						  "(f.oid = pip.objoid "
						  "AND pip.classoid = 'pg_foreign_data_wrapper'::regclass "
						  "AND pip.objsubid = 0) ",
						  username_subquery,
						  acl_subquery->data,
						  racl_subquery->data,
						  initacl_subquery->data,
						  initracl_subquery->data);

		destroyStringInfo(acl_subquery);
		pfree(acl_subquery);
		destroyStringInfo(racl_subquery);
		pfree(racl_subquery);
		destroyStringInfo(initacl_subquery);
		pfree(initacl_subquery);
		destroyStringInfo(initracl_subquery);
		pfree(initracl_subquery);
	}
	else if (fout->remoteVersion >= 90100)
	{
		appendStringInfo(query, "SELECT tableoid, oid, fdwname, "
						  "(%s fdwowner) AS rolname, "
						  "fdwhandler::pg_catalog.regproc, "
						  "fdwvalidator::pg_catalog.regproc, fdwacl, "
						  "NULL as rfdwacl, "
						  "NULL as initfdwacl, NULL AS initrfdwacl, "
						  "array_to_string(ARRAY("
						  "SELECT quote_ident(option_name) || ' ' || "
						  "quote_literal(option_value) "
						  "FROM pg_options_to_table(fdwoptions) "
						  "ORDER BY option_name"
						  "), E',\n    ') AS fdwoptions "
						  "FROM pg_foreign_data_wrapper",
						  username_subquery);
	}
	else
	{
		appendStringInfo(query, "SELECT tableoid, oid, fdwname, "
						  "(%s fdwowner) AS rolname, "
						  "'-' AS fdwhandler, "
						  "fdwvalidator::pg_catalog.regproc, fdwacl, "
						  "NULL as rfdwacl, "
						  "NULL as initfdwacl, NULL AS initrfdwacl, "
						  "array_to_string(ARRAY("
						  "SELECT quote_ident(option_name) || ' ' || "
						  "quote_literal(option_value) "
						  "FROM pg_options_to_table(fdwoptions) "
						  "ORDER BY option_name"
						  "), E',\n    ') AS fdwoptions "
						  "FROM pg_foreign_data_wrapper",
						  username_subquery);
	}

	res = ExecuteSqlQuery(fout, query->data);

	ntups = dbms_md_get_tuple_num(res);
	*numForeignDataWrappers = ntups;

	fdwinfo = (FdwInfo *) dbms_md_malloc0(fout->memory_ctx, ntups * sizeof(FdwInfo));

	i_tableoid = dbms_md_get_field_subscript(res, "tableoid");
	i_oid = dbms_md_get_field_subscript(res, "oid");
	i_fdwname = dbms_md_get_field_subscript(res, "fdwname");
	i_rolname = dbms_md_get_field_subscript(res, "rolname");
	i_fdwhandler = dbms_md_get_field_subscript(res, "fdwhandler");
	i_fdwvalidator = dbms_md_get_field_subscript(res, "fdwvalidator");
	i_fdwacl = dbms_md_get_field_subscript(res, "fdwacl");
	i_rfdwacl = dbms_md_get_field_subscript(res, "rfdwacl");
	i_initfdwacl = dbms_md_get_field_subscript(res, "initfdwacl");
	i_initrfdwacl = dbms_md_get_field_subscript(res, "initrfdwacl");
	i_fdwoptions = dbms_md_get_field_subscript(res, "fdwoptions");

	for (i = 0; i < ntups; i++)
	{
		fdwinfo[i].dobj.objType = DO_FDW;
		fdwinfo[i].dobj.catId.tableoid = dbms_md_str_to_oid(dbms_md_get_field_value(res, i, i_tableoid));
		fdwinfo[i].dobj.catId.oid = dbms_md_str_to_oid(dbms_md_get_field_value(res, i, i_oid));
		AssignDumpId(fout, &fdwinfo[i].dobj);
		fdwinfo[i].dobj.name = dbms_md_strdup(dbms_md_get_field_value(res, i, i_fdwname));
		fdwinfo[i].dobj.namespace = NULL;
		fdwinfo[i].rolname = dbms_md_strdup(dbms_md_get_field_value(res, i, i_rolname));
		fdwinfo[i].fdwhandler = dbms_md_strdup(dbms_md_get_field_value(res, i, i_fdwhandler));
		fdwinfo[i].fdwvalidator = dbms_md_strdup(dbms_md_get_field_value(res, i, i_fdwvalidator));
		fdwinfo[i].fdwoptions = dbms_md_strdup(dbms_md_get_field_value(res, i, i_fdwoptions));
		fdwinfo[i].fdwacl = dbms_md_strdup(dbms_md_get_field_value(res, i, i_fdwacl));
		fdwinfo[i].rfdwacl = dbms_md_strdup(dbms_md_get_field_value(res, i, i_rfdwacl));
		fdwinfo[i].initfdwacl = dbms_md_strdup(dbms_md_get_field_value(res, i, i_initfdwacl));
		fdwinfo[i].initrfdwacl = dbms_md_strdup(dbms_md_get_field_value(res, i, i_initrfdwacl));

		/* Decide whether we want to dump it */
		selectDumpableObject(&(fdwinfo[i].dobj), fout, DBMS_MD_DOBJ_BUTT);

		/* Do not try to dump ACL if no ACL exists. */
		if (dbms_md_is_tuple_field_null(res, i, i_fdwacl) 
			&& dbms_md_is_tuple_field_null(res, i, i_rfdwacl) 
			&& dbms_md_is_tuple_field_null(res, i, i_initfdwacl) 
			&& dbms_md_is_tuple_field_null(res, i, i_initrfdwacl))
			fdwinfo[i].dobj.dump &= ~DUMP_COMPONENT_ACL;
	}

	dbms_md_free_tuples(res);

	destroyStringInfo(query);
	pfree(query);

	return fdwinfo;
}

/*
 * getForeignServers:
 *	  read all foreign servers in the system catalogs and return
 *	  them in the ForeignServerInfo * structure
 *
 *	numForeignServers is set to the number of servers read in
 */
ForeignServerInfo *
getForeignServers(Archive *fout, int *numForeignServers)
{
	DumpOptions *dopt = fout->dopt;
	SPITupleTable   *res;
	int			ntups;
	int			i;
	StringInfo query;
	ForeignServerInfo *srvinfo;
	int			i_tableoid;
	int			i_oid;
	int			i_srvname;
	int			i_rolname;
	int			i_srvfdw;
	int			i_srvtype;
	int			i_srvversion;
	int			i_srvacl;
	int			i_rsrvacl;
	int			i_initsrvacl;
	int			i_initrsrvacl;
	int			i_srvoptions;

	/* Before 8.4, there are no foreign servers */
	if (fout->remoteVersion < 80400)
	{
		*numForeignServers = 0;
		return NULL;
	}

	query = createStringInfo();

	/* Make sure we are in proper schema */
	selectSourceSchema(fout, "pg_catalog");

	if (fout->remoteVersion >= 90600)
	{
		StringInfo acl_subquery = createStringInfo();
		StringInfo racl_subquery = createStringInfo();
		StringInfo initacl_subquery = createStringInfo();
		StringInfo initracl_subquery = createStringInfo();

		buildACLQueries(acl_subquery, racl_subquery, initacl_subquery,
						initracl_subquery, "f.srvacl", "f.srvowner", "'S'",
						dopt->binary_upgrade);

		appendStringInfo(query, "SELECT f.tableoid, f.oid, f.srvname, "
						  "(%s f.srvowner) AS rolname, "
						  "f.srvfdw, f.srvtype, f.srvversion, "
						  "%s AS srvacl, "
						  "%s AS rsrvacl, "
						  "%s AS initsrvacl, "
						  "%s AS initrsrvacl, "
						  "array_to_string(ARRAY("
						  "SELECT quote_ident(option_name) || ' ' || "
						  "quote_literal(option_value) "
						  "FROM pg_options_to_table(f.srvoptions) "
						  "ORDER BY option_name"
						  "), E',\n    ') AS srvoptions "
						  "FROM pg_foreign_server f "
						  "LEFT JOIN pg_init_privs pip "
						  "ON (f.oid = pip.objoid "
						  "AND pip.classoid = 'pg_foreign_server'::regclass "
						  "AND pip.objsubid = 0) ",
						  username_subquery,
						  acl_subquery->data,
						  racl_subquery->data,
						  initacl_subquery->data,
						  initracl_subquery->data);

		destroyStringInfo(acl_subquery);
		pfree(acl_subquery);
		destroyStringInfo(racl_subquery);
		pfree(racl_subquery);
		destroyStringInfo(initacl_subquery);
		pfree(initacl_subquery);
		destroyStringInfo(initracl_subquery);
		pfree(initracl_subquery);
	}
	else
	{
		appendStringInfo(query, "SELECT tableoid, oid, srvname, "
						  "(%s srvowner) AS rolname, "
						  "srvfdw, srvtype, srvversion, srvacl, "
						  "NULL AS rsrvacl, "
						  "NULL AS initsrvacl, NULL AS initrsrvacl, "
						  "array_to_string(ARRAY("
						  "SELECT quote_ident(option_name) || ' ' || "
						  "quote_literal(option_value) "
						  "FROM pg_options_to_table(srvoptions) "
						  "ORDER BY option_name"
						  "), E',\n    ') AS srvoptions "
						  "FROM pg_foreign_server",
						  username_subquery);
	}

	res = ExecuteSqlQuery(fout, query->data);

	ntups = dbms_md_get_tuple_num(res);
	*numForeignServers = ntups;

	srvinfo = (ForeignServerInfo *) dbms_md_malloc0(fout->memory_ctx, ntups * sizeof(ForeignServerInfo));

	i_tableoid = dbms_md_get_field_subscript(res, "tableoid");
	i_oid = dbms_md_get_field_subscript(res, "oid");
	i_srvname = dbms_md_get_field_subscript(res, "srvname");
	i_rolname = dbms_md_get_field_subscript(res, "rolname");
	i_srvfdw = dbms_md_get_field_subscript(res, "srvfdw");
	i_srvtype = dbms_md_get_field_subscript(res, "srvtype");
	i_srvversion = dbms_md_get_field_subscript(res, "srvversion");
	i_srvacl = dbms_md_get_field_subscript(res, "srvacl");
	i_rsrvacl = dbms_md_get_field_subscript(res, "rsrvacl");
	i_initsrvacl = dbms_md_get_field_subscript(res, "initsrvacl");
	i_initrsrvacl = dbms_md_get_field_subscript(res, "initrsrvacl");
	i_srvoptions = dbms_md_get_field_subscript(res, "srvoptions");

	for (i = 0; i < ntups; i++)
	{
		srvinfo[i].dobj.objType = DO_FOREIGN_SERVER;
		srvinfo[i].dobj.catId.tableoid = dbms_md_str_to_oid(dbms_md_get_field_value(res, i, i_tableoid));
		srvinfo[i].dobj.catId.oid = dbms_md_str_to_oid(dbms_md_get_field_value(res, i, i_oid));
		AssignDumpId(fout, &srvinfo[i].dobj);
		srvinfo[i].dobj.name = dbms_md_strdup(dbms_md_get_field_value(res, i, i_srvname));
		srvinfo[i].dobj.namespace = NULL;
		srvinfo[i].rolname = dbms_md_strdup(dbms_md_get_field_value(res, i, i_rolname));
		srvinfo[i].srvfdw = dbms_md_str_to_oid(dbms_md_get_field_value(res, i, i_srvfdw));
		srvinfo[i].srvtype = dbms_md_strdup(dbms_md_get_field_value(res, i, i_srvtype));
		srvinfo[i].srvversion = dbms_md_strdup(dbms_md_get_field_value(res, i, i_srvversion));
		srvinfo[i].srvoptions = dbms_md_strdup(dbms_md_get_field_value(res, i, i_srvoptions));
		srvinfo[i].srvacl = dbms_md_strdup(dbms_md_get_field_value(res, i, i_srvacl));
		srvinfo[i].rsrvacl = dbms_md_strdup(dbms_md_get_field_value(res, i, i_rsrvacl));
		srvinfo[i].initsrvacl = dbms_md_strdup(dbms_md_get_field_value(res, i, i_initsrvacl));
		srvinfo[i].initrsrvacl = dbms_md_strdup(dbms_md_get_field_value(res, i, i_initrsrvacl));

		/* Decide whether we want to dump it */
		selectDumpableObject(&(srvinfo[i].dobj), fout, DBMS_MD_DOBJ_BUTT);

		/* Do not try to dump ACL if no ACL exists. */
		if (dbms_md_is_tuple_field_null(res, i, i_srvacl) && dbms_md_is_tuple_field_null(res, i, i_rsrvacl) &&
			dbms_md_is_tuple_field_null(res, i, i_initsrvacl) &&
			dbms_md_is_tuple_field_null(res, i, i_initrsrvacl))
			srvinfo[i].dobj.dump &= ~DUMP_COMPONENT_ACL;
	}

	dbms_md_free_tuples(res);

	destroyStringInfo(query);
	pfree(query);

	return srvinfo;
}

/*
 * getDefaultACLs:
 *	  read all default ACL information in the system catalogs and return
 *	  them in the DefaultACLInfo structure
 *
 *	numDefaultACLs is set to the number of ACLs read in
 */
DefaultACLInfo *
getDefaultACLs(Archive *fout, int *numDefaultACLs)
{
	DumpOptions *dopt = fout->dopt;
	DefaultACLInfo *daclinfo;
	StringInfo query;
	SPITupleTable   *res;
	int			i_oid;
	int			i_tableoid;
	int			i_defaclrole;
	int			i_defaclnamespace;
	int			i_defaclobjtype;
	int			i_defaclacl;
	int			i_rdefaclacl;
	int			i_initdefaclacl;
	int			i_initrdefaclacl;
	int			i,
				ntups;

	if (fout->remoteVersion < 90000)
	{
		*numDefaultACLs = 0;
		return NULL;
	}

	query = createStringInfo();

	/* Make sure we are in proper schema */
	selectSourceSchema(fout, "pg_catalog");

	if (fout->remoteVersion >= 90600)
	{
		StringInfo acl_subquery = createStringInfo();
		StringInfo racl_subquery = createStringInfo();
		StringInfo initacl_subquery = createStringInfo();
		StringInfo initracl_subquery = createStringInfo();

		buildACLQueries(acl_subquery, racl_subquery, initacl_subquery,
						initracl_subquery, "defaclacl", "defaclrole",
						"CASE WHEN defaclobjtype = 'S' THEN 's' ELSE defaclobjtype END::\"char\"",
						dopt->binary_upgrade);

		appendStringInfo(query, "SELECT d.oid, d.tableoid, "
						  "(%s d.defaclrole) AS defaclrole, "
						  "d.defaclnamespace, "
						  "d.defaclobjtype, "
						  "%s AS defaclacl, "
						  "%s AS rdefaclacl, "
						  "%s AS initdefaclacl, "
						  "%s AS initrdefaclacl "
						  "FROM pg_default_acl d "
						  "LEFT JOIN pg_init_privs pip ON "
						  "(d.oid = pip.objoid "
						  "AND pip.classoid = 'pg_default_acl'::regclass "
						  "AND pip.objsubid = 0) ",
						  username_subquery,
						  acl_subquery->data,
						  racl_subquery->data,
						  initacl_subquery->data,
						  initracl_subquery->data);
		
		destroyStringInfo(acl_subquery);
		pfree(acl_subquery);
		destroyStringInfo(racl_subquery);
		pfree(racl_subquery);
		destroyStringInfo(initacl_subquery);
		pfree(initacl_subquery);
		destroyStringInfo(initracl_subquery);
		pfree(initracl_subquery);
	}
	else
	{
		appendStringInfo(query, "SELECT oid, tableoid, "
						  "(%s defaclrole) AS defaclrole, "
						  "defaclnamespace, "
						  "defaclobjtype, "
						  "defaclacl, "
						  "NULL AS rdefaclacl, "
						  "NULL AS initdefaclacl, "
						  "NULL AS initrdefaclacl "
						  "FROM pg_default_acl",
						  username_subquery);
	}

	res = ExecuteSqlQuery(fout, query->data);

	ntups = dbms_md_get_tuple_num(res);
	*numDefaultACLs = ntups;

	daclinfo = (DefaultACLInfo *) dbms_md_malloc0(fout->memory_ctx, ntups * sizeof(DefaultACLInfo));

	i_oid = dbms_md_get_field_subscript(res, "oid");
	i_tableoid = dbms_md_get_field_subscript(res, "tableoid");
	i_defaclrole = dbms_md_get_field_subscript(res, "defaclrole");
	i_defaclnamespace = dbms_md_get_field_subscript(res, "defaclnamespace");
	i_defaclobjtype = dbms_md_get_field_subscript(res, "defaclobjtype");
	i_defaclacl = dbms_md_get_field_subscript(res, "defaclacl");
	i_rdefaclacl = dbms_md_get_field_subscript(res, "rdefaclacl");
	i_initdefaclacl = dbms_md_get_field_subscript(res, "initdefaclacl");
	i_initrdefaclacl = dbms_md_get_field_subscript(res, "initrdefaclacl");

	for (i = 0; i < ntups; i++)
	{
		Oid			nspid = dbms_md_str_to_oid(dbms_md_get_field_value(res, i, i_defaclnamespace));

		daclinfo[i].dobj.objType = DO_DEFAULT_ACL;
		daclinfo[i].dobj.catId.tableoid = dbms_md_str_to_oid(dbms_md_get_field_value(res, i, i_tableoid));
		daclinfo[i].dobj.catId.oid = dbms_md_str_to_oid(dbms_md_get_field_value(res, i, i_oid));
		AssignDumpId(fout, &daclinfo[i].dobj);
		/* cheesy ... is it worth coming up with a better object name? */
		daclinfo[i].dobj.name = dbms_md_strdup(dbms_md_get_field_value(res, i, i_defaclobjtype));

		if (nspid != InvalidOid)
			daclinfo[i].dobj.namespace = findNamespace(fout, nspid);
		else
			daclinfo[i].dobj.namespace = NULL;

		daclinfo[i].defaclrole = dbms_md_strdup(dbms_md_get_field_value(res, i, i_defaclrole));
		daclinfo[i].defaclobjtype = *(dbms_md_get_field_value(res, i, i_defaclobjtype));
		daclinfo[i].defaclacl = dbms_md_strdup(dbms_md_get_field_value(res, i, i_defaclacl));
		daclinfo[i].rdefaclacl = dbms_md_strdup(dbms_md_get_field_value(res, i, i_rdefaclacl));
		daclinfo[i].initdefaclacl = dbms_md_strdup(dbms_md_get_field_value(res, i, i_initdefaclacl));
		daclinfo[i].initrdefaclacl = dbms_md_strdup(dbms_md_get_field_value(res, i, i_initrdefaclacl));

		/* Decide whether we want to dump it */
		selectDumpableDefaultACL(&(daclinfo[i]), dopt);
	}

	dbms_md_free_tuples(res);

	destroyStringInfo(query);
	pfree(query);

	return daclinfo;
}

/*
 * dumpComment --
 *
 * This routine is used to dump any comments associated with the
 * object handed to this routine. The routine takes a constant character
 * string for the target part of the comment-creation command, plus
 * the namespace and owner of the object (for labeling the ArchiveEntry),
 * plus catalog ID and subid which are the lookup key for pg_description,
 * plus the dump ID for the object (for setting a dependency).
 * If a matching pg_description entry is found, it is dumped.
 *
 * Note: although this routine takes a dumpId for dependency purposes,
 * that purpose is just to mark the dependency in the emitted dump file
 * for possible future use by pg_restore.  We do NOT use it for determining
 * ordering of the comment in the dump file, because this routine is called
 * after dependency sorting occurs.  This routine should be called just after
 * calling ArchiveEntry() for the specified object.
 */
static void
dumpComment(Archive *fout, const char *target,
			const char *namespace, const char *owner,
			CatalogId catalogId, int subid, DumpId dumpId)
{
	DumpOptions *dopt = fout->dopt;
	CommentItem *comments;
	int			ncomments;

	/* Comments are schema not data ... except blob comments are data */
	if (strncmp(target, "LARGE OBJECT ", 13) != 0)
	{
		if (dopt->dataOnly)
			return;
	}
	else
	{
		/* We do dump blob comments in binary-upgrade mode */
		if (dopt->schemaOnly && !dopt->binary_upgrade)
			return;
	}

	/* Search for comments associated with catalogId, using table */
	ncomments = findComments(fout, catalogId.tableoid, catalogId.oid,
							 &comments);

	/* Is there one matching the subid? */
	while (ncomments > 0)
	{
		if (comments->objsubid == subid)
			break;
		comments++;
		ncomments--;
	}

	/* If a comment exists, build COMMENT ON statement */
	if (ncomments > 0)
	{
		StringInfo query = createStringInfo();

		appendStringInfo(query, "COMMENT ON %s IS ", target);
		appendStringLiteralAH(query, comments->descr, fout);
		appendStringInfoString(query, ";\n");

		/*
		 * We mark comments as SECTION_NONE because they really belong in the
		 * same section as their parent, whether that is pre-data or
		 * post-data.
		 */
		ArchiveEntry(fout, nilCatalogId, createDumpId(fout),
					 target, namespace, NULL, owner,
					 false, "COMMENT", SECTION_NONE,
					 query->data, "", NULL,
					 &(dumpId), 1,
					 NULL, NULL);

		destroyStringInfo(query);
		pfree(query);
	}
}

/*
 * dumpTableComment --
 *
 * As above, but dump comments for both the specified table (or view)
 * and its columns.
 */
static void
dumpTableComment(Archive *fout, TableInfo *tbinfo,
				 const char *reltypename)
{
	DumpOptions *dopt = fout->dopt;
	CommentItem *comments;
	int			ncomments;
	StringInfo query;
	StringInfo target;

	/* Comments are SCHEMA not data */
	if (dopt->dataOnly)
		return;

	/* Search for comments associated with relation, using table */
	ncomments = findComments(fout,
							 tbinfo->dobj.catId.tableoid,
							 tbinfo->dobj.catId.oid,
							 &comments);

	/* If comments exist, build COMMENT ON statements */
	if (ncomments <= 0)
		return;

	query = createStringInfo();
	target = createStringInfo();

	while (ncomments > 0)
	{
		const char *descr = comments->descr;
		int			objsubid = comments->objsubid;

		if (objsubid == 0)
		{
			resetStringInfo(target);
			appendStringInfo(target, "%s %s", reltypename,
							  dbms_md_fmtId(tbinfo->dobj.name));

			resetStringInfo(query);
			appendStringInfo(query, "COMMENT ON %s IS ", target->data);
			appendStringLiteralAH(query, descr, fout);
			appendStringInfoString(query, ";\n");

			ArchiveEntry(fout, nilCatalogId, createDumpId(fout),
						 target->data,
						 tbinfo->dobj.namespace->dobj.name,
						 NULL, tbinfo->rolname,
						 false, "COMMENT", SECTION_NONE,
						 query->data, "", NULL,
						 &(tbinfo->dobj.dumpId), 1,
						 NULL, NULL);
		}
		else if (objsubid > 0 && objsubid <= tbinfo->numatts)
		{
			resetStringInfo(target);
			appendStringInfo(target, "COLUMN %s.",
							  dbms_md_fmtId(tbinfo->dobj.name));
			appendStringInfoString(target, dbms_md_fmtId(tbinfo->attnames[objsubid - 1]));

			resetStringInfo(query);
			appendStringInfo(query, "COMMENT ON %s IS ", target->data);
			appendStringLiteralAH(query, descr, fout);
			appendStringInfoString(query, ";\n");

			ArchiveEntry(fout, nilCatalogId, createDumpId(fout),
						 target->data,
						 tbinfo->dobj.namespace->dobj.name,
						 NULL, tbinfo->rolname,
						 false, "COMMENT", SECTION_NONE,
						 query->data, "", NULL,
						 &(tbinfo->dobj.dumpId), 1,
						 NULL, NULL);
		}

		comments++;
		ncomments--;
	}

	destroyStringInfo(query);
	pfree(query);
	destroyStringInfo(target);
	pfree(target);
}

/*
 * findComments --
 *
 * Find the comment(s), if any, associated with the given object.  All the
 * objsubid values associated with the given classoid/objoid are found with
 * one search.
 */
static int
findComments(Archive *fout, Oid classoid, Oid objoid,
			 CommentItem **items)
{
	CommentItem *middle = NULL;
	CommentItem *low;
	CommentItem *high;
	int			nmatch;

	/*
	 * Do binary search to find some item matching the object.
	 */
	low = &comments[0];
	high = &comments[ncomments - 1];
	while (low <= high)
	{
		middle = low + (high - low) / 2;

		if (classoid < middle->classoid)
			high = middle - 1;
		else if (classoid > middle->classoid)
			low = middle + 1;
		else if (objoid < middle->objoid)
			high = middle - 1;
		else if (objoid > middle->objoid)
			low = middle + 1;
		else
			break;				/* found a match */
	}

	if (low > high)				/* no matches */
	{
		*items = NULL;
		return 0;
	}

	/*
	 * Now determine how many items match the object.  The search loop
	 * invariant still holds: only items between low and high inclusive could
	 * match.
	 */
	nmatch = 1;
	while (middle > low)
	{
		if (classoid != middle[-1].classoid ||
			objoid != middle[-1].objoid)
			break;
		middle--;
		nmatch++;
	}

	*items = middle;

	middle += nmatch;
	while (middle <= high)
	{
		if (classoid != middle->classoid ||
			objoid != middle->objoid)
			break;
		middle++;
		nmatch++;
	}

	return nmatch;
}

/*
 * collectComments --
 *
 * Construct a table of all comments available for database objects.
 * We used to do per-object queries for the comments, but it's much faster
 * to pull them all over at once, and on most databases the memory cost
 * isn't high.
 *
 * The table is sorted by classoid/objid/objsubid for speed in lookup.
 */
static void
collectComments(Archive *fout)
{
	SPITupleTable   *res;
	StringInfo query;
	int			i_description;
	int			i_classoid;
	int			i_objoid;
	int			i_objsubid;
	int			ntups;
	int			i;

	/*
	 * Note we do NOT change source schema here; preserve the caller's
	 * setting, instead.
	 */

	query = createStringInfo();

	appendStringInfoString(query, "SELECT description, classoid, objoid, objsubid "
						 "FROM pg_catalog.pg_description "
						 "ORDER BY classoid, objoid, objsubid");

	res = ExecuteSqlQuery(fout, query->data);

	/* Construct lookup table containing OIDs in numeric form */

	i_description = dbms_md_get_field_subscript(res, "description");
	i_classoid = dbms_md_get_field_subscript(res, "classoid");
	i_objoid = dbms_md_get_field_subscript(res, "objoid");
	i_objsubid = dbms_md_get_field_subscript(res, "objsubid");

	ntups = dbms_md_get_tuple_num(res);

	comments = (CommentItem *) dbms_md_malloc0(fout->memory_ctx, ntups * sizeof(CommentItem));
	ncomments = 0;

	for (i = 0; i < ntups; i++)
	{
		comments[ncomments].descr = dbms_md_get_field_value(res, i, i_description);
		comments[ncomments].classoid = dbms_md_str_to_oid(dbms_md_get_field_value(res, i, i_classoid));
		comments[ncomments].objoid = dbms_md_str_to_oid(dbms_md_get_field_value(res, i, i_objoid));
		comments[ncomments].objsubid = atoi(dbms_md_get_field_value(res, i, i_objsubid));
		ncomments++;
	}

	dbms_md_free_tuples(res);

	/* Do NOT free the SPITupleTable since we are keeping pointers into it */
	destroyStringInfo(query);
	pfree(query);
}

/*
 * dumpDumpableObject
 *
 * This routine and its subsidiaries are responsible for creating
 * ArchiveEntries (TOC objects) for each object to be dumped.
 */
static void
dumpDumpableObject(Archive *fout, DumpableObject *dobj)
{
	switch (dobj->objType)
	{
		case DO_NAMESPACE:
			dumpNamespace(fout, (NamespaceInfo *) dobj);
			break;
		case DO_EXTENSION:
			dumpExtension(fout, (ExtensionInfo *) dobj);
			break;
		case DO_TYPE:
			dumpType(fout, (TypeInfo *) dobj);
			break;
		case DO_SHELL_TYPE:
			dumpShellType(fout, (ShellTypeInfo *) dobj);
			break;
		case DO_FUNC:
			dumpFunc(fout, (FuncInfo *) dobj);
			break;
		case DO_AGG:
			dumpAgg(fout, (AggInfo *) dobj);
			break;
		case DO_OPERATOR:
			dumpOpr(fout, (OprInfo *) dobj);
			break;
		case DO_ACCESS_METHOD:
			dumpAccessMethod(fout, (AccessMethodInfo *) dobj);
			break;
		case DO_OPCLASS:
			dumpOpclass(fout, (OpclassInfo *) dobj);
			break;
		case DO_OPFAMILY:
			dumpOpfamily(fout, (OpfamilyInfo *) dobj);
			break;
		case DO_COLLATION:
			dumpCollation(fout, (CollInfo *) dobj);
			break;
		case DO_CONVERSION:
			dumpConversion(fout, (ConvInfo *) dobj);
			break;
		case DO_TABLE:
			dumpTable(fout, (TableInfo *) dobj);
			break;
		case DO_ATTRDEF:
			dumpAttrDef(fout, (AttrDefInfo *) dobj);
			break;
		case DO_INDEX:
			dumpIndex(fout, (IndxInfo *) dobj);
			break;
		case DO_INDEX_ATTACH:
			dumpIndexAttach(fout, (IndexAttachInfo *) dobj);
			break;
		case DO_STATSEXT:
			dumpStatisticsExt(fout, (StatsExtInfo *) dobj);
			break;
		case DO_REFRESH_MATVIEW:
			/*refreshMatViewData(fout, (TableDataInfo *) dobj);*/
			break;
		case DO_RULE:
			dumpRule(fout, (RuleInfo *) dobj);
			break;
		case DO_TRIGGER:
			dumpTrigger(fout, (TriggerInfo *) dobj);
			break;
		case DO_EVENT_TRIGGER:
			dumpEventTrigger(fout, (EventTriggerInfo *) dobj);
			break;
		case DO_CONSTRAINT:
			dumpConstraint(fout, (ConstraintInfo *) dobj);
			break;
		case DO_FK_CONSTRAINT:
			dumpConstraint(fout, (ConstraintInfo *) dobj);
			break;
		case DO_PROCLANG:
			dumpProcLang(fout, (ProcLangInfo *) dobj);
			break;
		case DO_CAST:
			dumpCast(fout, (CastInfo *) dobj);
			break;
		case DO_TRANSFORM:
			dumpTransform(fout, (TransformInfo *) dobj);
			break;
		case DO_SEQUENCE_SET:
			dumpSequenceData(fout, (TableDataInfo *) dobj);
			break;
		case DO_TABLE_DATA:			
			//dumpTableData(fout, (TableDataInfo *) dobj);
			break;
		case DO_DUMMY_TYPE:
			/* table rowtypes and array types are never dumped separately */
			break;
		case DO_TSPARSER:
			dumpTSParser(fout, (TSParserInfo *) dobj);
			break;
		case DO_TSDICT:
			dumpTSDictionary(fout, (TSDictInfo *) dobj);
			break;
		case DO_TSTEMPLATE:
			dumpTSTemplate(fout, (TSTemplateInfo *) dobj);
			break;
		case DO_TSCONFIG:
			dumpTSConfig(fout, (TSConfigInfo *) dobj);
			break;
		case DO_FDW:
			dumpForeignDataWrapper(fout, (FdwInfo *) dobj);
			break;
		case DO_FOREIGN_SERVER:
			dumpForeignServer(fout, (ForeignServerInfo *) dobj);
			break;
		case DO_DEFAULT_ACL:
			dumpDefaultACL(fout, (DefaultACLInfo *) dobj);
			break;
		case DO_BLOB:
			/*dumpBlob(fout, (BlobInfo *) dobj);*/
			break;
		case DO_BLOB_DATA:
#if 0			
			if (dobj->dump & DUMP_COMPONENT_DATA)
				ArchiveEntry(fout, dobj->catId, dobj->dumpId,
							 dobj->name, NULL, NULL, "",
							 false, "BLOBS", SECTION_DATA,
							 "", "", NULL,
							 NULL, 0,
							 dumpBlobs, NULL);
#endif			
			break;
		case DO_POLICY:
			dumpPolicy(fout, (PolicyInfo *) dobj);
			break;
		case DO_PUBLICATION:
			dumpPublication(fout, (PublicationInfo *) dobj);
			break;
		case DO_PUBLICATION_REL:
			dumpPublicationTable(fout, (PublicationRelInfo *) dobj);
			break;
		case DO_SUBSCRIPTION:
			dumpSubscription(fout, (SubscriptionInfo *) dobj);
			break;
		case DO_PRE_DATA_BOUNDARY:
		case DO_POST_DATA_BOUNDARY:
			/* never dumped, nothing to do */
			break;
	}
}

/*
 * dumpNamespace
 *	  writes out to fout the queries to recreate a user-defined namespace
 */
static void
dumpNamespace(Archive *fout, NamespaceInfo *nspinfo)
{
	DumpOptions *dopt = fout->dopt;
	StringInfo q;
	StringInfo delq;
	StringInfo labelq;
	char	   *qnspname;

	/* Skip if not to be dumped */
	if (!nspinfo->dobj.dump || dopt->dataOnly)
		return;

	q = createStringInfo();
	delq = createStringInfo();
	labelq = createStringInfo();

	qnspname = dbms_md_strdup(dbms_md_fmtId(nspinfo->dobj.name));

	appendStringInfo(delq, "DROP SCHEMA %s;\n", qnspname);

	appendStringInfo(q, "CREATE SCHEMA %s;\n", qnspname);

	appendStringInfo(labelq, "SCHEMA %s", qnspname);

#if 0
	if (dopt->binary_upgrade)
		binary_upgrade_extension_member(q, &nspinfo->dobj, labelq->data);
#endif

	if (nspinfo->dobj.dump & DUMP_COMPONENT_DEFINITION)
		ArchiveEntry(fout, nspinfo->dobj.catId, nspinfo->dobj.dumpId,
					 nspinfo->dobj.name,
					 NULL, NULL,
					 nspinfo->rolname,
					 false, "SCHEMA", SECTION_PRE_DATA,
					 q->data, delq->data, NULL,
					 NULL, 0,
					 NULL, NULL);

	/* Dump Schema Comments and Security Labels */
	if (nspinfo->dobj.dump & DUMP_COMPONENT_COMMENT)
		dumpComment(fout, labelq->data,
					NULL, nspinfo->rolname,
					nspinfo->dobj.catId, 0, nspinfo->dobj.dumpId);

	if (nspinfo->dobj.dump & DUMP_COMPONENT_SECLABEL)
		dumpSecLabel(fout, labelq->data,
					 NULL, nspinfo->rolname,
					 nspinfo->dobj.catId, 0, nspinfo->dobj.dumpId);

	if (nspinfo->dobj.dump & DUMP_COMPONENT_ACL)
		dumpACL(fout, nspinfo->dobj.catId, nspinfo->dobj.dumpId, "SCHEMA",
				qnspname, NULL, nspinfo->dobj.name, NULL,
				nspinfo->rolname, nspinfo->nspacl, nspinfo->rnspacl,
				nspinfo->initnspacl, nspinfo->initrnspacl);

	free(qnspname);

	destroyStringInfo(q);
	pfree(q);
	destroyStringInfo(delq);
	pfree(delq);
	destroyStringInfo(labelq);
	pfree(labelq);
}

/*
 * dumpExtension
 *	  writes out to fout the queries to recreate an extension
 */
static void
dumpExtension(Archive *fout, ExtensionInfo *extinfo)
{
	DumpOptions *dopt = fout->dopt;
	StringInfo q;
	StringInfo delq;
	StringInfo labelq;
	char	   *qextname;

	/* Skip if not to be dumped */
	if (!extinfo->dobj.dump || dopt->dataOnly)
		return;

	q = createStringInfo();
	delq = createStringInfo();
	labelq = createStringInfo();

	qextname = dbms_md_strdup(dbms_md_fmtId_internal(extinfo->dobj.name, true));

	appendStringInfo(delq, "DROP EXTENSION %s;\n", qextname);

#if 0
	if (!dopt->binary_upgrade)
	{
		/*
		 * In a regular dump, we use IF NOT EXISTS so that there isn't a
		 * problem if the extension already exists in the target database;
		 * this is essential for installed-by-default extensions such as
		 * plpgsql.
		 *
		 * In binary-upgrade mode, that doesn't work well, so instead we skip
		 * built-in extensions based on their OIDs; see
		 * selectDumpableExtension.
		 */
		appendStringInfo(q, "CREATE EXTENSION IF NOT EXISTS %s WITH SCHEMA %s;\n",
						  qextname, dbms_md_fmtId(extinfo->namespace));
	}
	else
#endif
	{
		int			i;
		int			n;

		appendStringInfoString(q, "-- For binary upgrade, create an empty extension and insert objects into it\n");

		/*
		 * We unconditionally create the extension, so we must drop it if it
		 * exists.  This could happen if the user deleted 'plpgsql' and then
		 * readded it, causing its oid to be greater than g_last_builtin_oid.
		 * The g_last_builtin_oid test was kept to avoid repeatedly dropping
		 * and recreating extensions like 'plpgsql'.
		 */
		appendStringInfo(q, "DROP EXTENSION IF EXISTS %s;\n", qextname);

		appendStringInfoString(q,
							 "SELECT pg_catalog.binary_upgrade_create_empty_extension(");
		appendStringLiteralAH(q, extinfo->dobj.name, fout);
		appendStringInfoString(q, ", ");
		appendStringLiteralAH(q, extinfo->namespace, fout);
		appendStringInfoString(q, ", ");
		appendStringInfo(q, "%s, ", extinfo->relocatable ? "true" : "false");
		appendStringLiteralAH(q, extinfo->extversion, fout);
		appendStringInfoString(q, ", ");

		/*
		 * Note that we're pushing extconfig (an OID array) back into
		 * pg_extension exactly as-is.  This is OK because pg_class OIDs are
		 * preserved in binary upgrade.
		 */
		if (NULL != extinfo->extconfig && strlen(extinfo->extconfig) > 2)
			appendStringLiteralAH(q, extinfo->extconfig, fout);
		else
			appendStringInfoString(q, "NULL");
		appendStringInfoString(q, ", ");
		if (NULL != extinfo->extcondition && strlen(extinfo->extcondition) > 2)
			appendStringLiteralAH(q, extinfo->extcondition, fout);
		else
			appendStringInfoString(q, "NULL");
		appendStringInfoString(q, ", ");
		appendStringInfoString(q, "ARRAY[");
		n = 0;
		for (i = 0; i < extinfo->dobj.nDeps; i++)
		{
			DumpableObject *extobj;

			extobj = findObjectByDumpId(fout, extinfo->dobj.dependencies[i]);
			if (extobj && extobj->objType == DO_EXTENSION)
			{
				if (n++ > 0)
					appendStringInfoChar(q, ',');
				appendStringLiteralAH(q, extobj->name, fout);
			}
		}
		appendStringInfoString(q, "]::pg_catalog.text[]");
		appendStringInfoString(q, ");\n");
	}

	appendStringInfo(labelq, "EXTENSION %s", qextname);

	if (extinfo->dobj.dump & DUMP_COMPONENT_DEFINITION)
		ArchiveEntry(fout, extinfo->dobj.catId, extinfo->dobj.dumpId,
					 extinfo->dobj.name,
					 NULL, NULL,
					 "",
					 false, "EXTENSION", SECTION_PRE_DATA,
					 q->data, delq->data, NULL,
					 NULL, 0,
					 NULL, NULL);

	/* Dump Extension Comments and Security Labels */
	if (extinfo->dobj.dump & DUMP_COMPONENT_COMMENT)
		dumpComment(fout, labelq->data,
					NULL, "",
					extinfo->dobj.catId, 0, extinfo->dobj.dumpId);

	if (extinfo->dobj.dump & DUMP_COMPONENT_SECLABEL)
		dumpSecLabel(fout, labelq->data,
					 NULL, "",
					 extinfo->dobj.catId, 0, extinfo->dobj.dumpId);

	free(qextname);

	destroyStringInfo(q);
	pfree(q);
	destroyStringInfo(delq);
	pfree(delq);
	destroyStringInfo(labelq);
	pfree(labelq);
}

/*
 * dumpType
 *	  writes out to fout the queries to recreate a user-defined type
 */
static void
dumpType(Archive *fout, TypeInfo *tyinfo)
{
	DumpOptions *dopt = fout->dopt;

	/* Skip if not to be dumped */
	if (!tyinfo->dobj.dump || dopt->dataOnly)
		return;

	/* Dump out in proper style */
	if (tyinfo->typtype == TYPTYPE_BASE)
		dumpBaseType(fout, tyinfo);
	else if (tyinfo->typtype == TYPTYPE_DOMAIN)
		dumpDomain(fout, tyinfo);
	else if (tyinfo->typtype == TYPTYPE_COMPOSITE)
		dumpCompositeType(fout, tyinfo);
	else if (tyinfo->typtype == TYPTYPE_ENUM)
		dumpEnumType(fout, tyinfo);
	else if (tyinfo->typtype == TYPTYPE_RANGE)
		dumpRangeType(fout, tyinfo);
	else if (tyinfo->typtype == TYPTYPE_PSEUDO && !tyinfo->isDefined)
		dumpUndefinedType(fout, tyinfo);
	else
		write_msg(NULL, "WARNING: typtype of data type \"%s\" appears to be invalid\n",
				  tyinfo->dobj.name);
}

/*
 * dumpEnumType
 *	  writes out to fout the queries to recreate a user-defined enum type
 */
static void
dumpEnumType(Archive *fout, TypeInfo *tyinfo)
{
	// DumpOptions *dopt = fout->dopt;
	StringInfo q = createStringInfo();
	StringInfo delq = createStringInfo();
	StringInfo labelq = createStringInfo();
	StringInfo query = createStringInfo();
	SPITupleTable   *res;
	//int			num,
	//			i;
	//Oid			enum_oid;
	char	   *qtypname;
	//char	   *label;

	/* Set proper schema search path */
	selectSourceSchema(fout, "pg_catalog");

	if (fout->remoteVersion >= 90100)
		appendStringInfo(query, "SELECT oid, enumlabel "
						  "FROM pg_catalog.pg_enum "
						  "WHERE enumtypid = '%u'"
						  "ORDER BY enumsortorder",
						  tyinfo->dobj.catId.oid);
	else
		appendStringInfo(query, "SELECT oid, enumlabel "
						  "FROM pg_catalog.pg_enum "
						  "WHERE enumtypid = '%u'"
						  "ORDER BY oid",
						  tyinfo->dobj.catId.oid);

	res = ExecuteSqlQuery(fout, query->data);

	//num = dbms_md_get_tuple_num(res);

	qtypname = dbms_md_strdup(dbms_md_fmtId(tyinfo->dobj.name));

	/*
	 * DROP must be fully qualified in case same name appears in pg_catalog.
	 * CASCADE shouldn't be required here as for normal types since the I/O
	 * functions are generic and do not get dropped.
	 */
	appendStringInfo(delq, "DROP TYPE %s.",
					  dbms_md_fmtId(tyinfo->dobj.namespace->dobj.name));
	appendStringInfo(delq, "%s;\n",
					  qtypname);

#if 0
	if (dopt->binary_upgrade)
		binary_upgrade_set_type_oids_by_type_oid(fout, q,
												 tyinfo->dobj.catId.oid);
#endif

	appendStringInfo(q, "CREATE TYPE %s AS ENUM (",
					  qtypname);

#if 0
	if (!dopt->binary_upgrade)
	{
		/* Labels with server-assigned oids */
		for (i = 0; i < num; i++)
		{
			label = dbms_md_get_field_value(res, i, dbms_md_get_field_subscript(res, "enumlabel"));
			if (i > 0)
				appendStringInfoChar(q, ',');
			appendStringInfoString(q, "\n    ");
			appendStringLiteralAH(q, label, fout);
		}
	}
#endif
	appendStringInfoString(q, "\n);\n");

#if 0
	if (dopt->binary_upgrade)
	{
		/* Labels with dump-assigned (preserved) oids */
		for (i = 0; i < num; i++)
		{
			enum_oid = dbms_md_str_to_oid(dbms_md_get_field_value(res, i, dbms_md_get_field_subscript(res, "oid")));
			label = dbms_md_get_field_value(res, i, dbms_md_get_field_subscript(res, "enumlabel"));

			if (i == 0)
				appendStringInfoString(q, "\n-- For binary upgrade, must preserve pg_enum oids\n");
			appendStringInfo(q,
							  "SELECT pg_catalog.binary_upgrade_set_next_pg_enum_oid('%u'::pg_catalog.oid);\n",
							  enum_oid);
			appendStringInfo(q, "ALTER TYPE %s.",
							  dbms_md_fmtId(tyinfo->dobj.namespace->dobj.name));
			appendStringInfo(q, "%s ADD VALUE ",
							  qtypname);
			appendStringLiteralAH(q, label, fout);
			appendStringInfoString(q, ";\n\n");
		}
	}
#endif
	appendStringInfo(labelq, "TYPE %s", qtypname);

#if 0
	if (dopt->binary_upgrade)
		binary_upgrade_extension_member(q, &tyinfo->dobj, labelq->data);
#endif

	if (tyinfo->dobj.dump & DUMP_COMPONENT_DEFINITION)
		ArchiveEntry(fout, tyinfo->dobj.catId, tyinfo->dobj.dumpId,
					 tyinfo->dobj.name,
					 tyinfo->dobj.namespace->dobj.name,
					 NULL,
					 tyinfo->rolname, false,
					 "TYPE", SECTION_PRE_DATA,
					 q->data, delq->data, NULL,
					 NULL, 0,
					 NULL, NULL);

	/* Dump Type Comments and Security Labels */
	if (tyinfo->dobj.dump & DUMP_COMPONENT_COMMENT)
		dumpComment(fout, labelq->data,
					tyinfo->dobj.namespace->dobj.name, tyinfo->rolname,
					tyinfo->dobj.catId, 0, tyinfo->dobj.dumpId);

	if (tyinfo->dobj.dump & DUMP_COMPONENT_SECLABEL)
		dumpSecLabel(fout, labelq->data,
					 tyinfo->dobj.namespace->dobj.name, tyinfo->rolname,
					 tyinfo->dobj.catId, 0, tyinfo->dobj.dumpId);

	if (tyinfo->dobj.dump & DUMP_COMPONENT_ACL)
		dumpACL(fout, tyinfo->dobj.catId, tyinfo->dobj.dumpId, "TYPE",
				qtypname, NULL, tyinfo->dobj.name,
				tyinfo->dobj.namespace->dobj.name,
				tyinfo->rolname, tyinfo->typacl, tyinfo->rtypacl,
				tyinfo->inittypacl, tyinfo->initrtypacl);

	dbms_md_free_tuples(res);
	destroyStringInfo(q);
	pfree(q);
	destroyStringInfo(delq);
	pfree(delq);
	destroyStringInfo(labelq);
	pfree(labelq);
	destroyStringInfo(query);
	pfree(query);
}

/*
 * dumpRangeType
 *	  writes out to fout the queries to recreate a user-defined range type
 */
static void
dumpRangeType(Archive *fout, TypeInfo *tyinfo)
{
//	DumpOptions *dopt = fout->dopt;
	StringInfo q = createStringInfo();
	StringInfo delq = createStringInfo();
	StringInfo labelq = createStringInfo();
	StringInfo query = createStringInfo();
	SPITupleTable   *res;
	Oid			collationOid;
	char	   *qtypname;
	char	   *procname;

	/*
	 * select appropriate schema to ensure names in CREATE are properly
	 * qualified
	 */
	selectSourceSchema(fout, tyinfo->dobj.namespace->dobj.name);

	appendStringInfo(query,
					  "SELECT pg_catalog.format_type(rngsubtype, NULL) AS rngsubtype, "
					  "opc.opcname AS opcname, "
					  "(SELECT nspname FROM pg_catalog.pg_namespace nsp "
					  "  WHERE nsp.oid = opc.opcnamespace) AS opcnsp, "
					  "opc.opcdefault, "
					  "CASE WHEN rngcollation = st.typcollation THEN 0 "
					  "     ELSE rngcollation END AS collation, "
					  "rngcanonical, rngsubdiff "
					  "FROM pg_catalog.pg_range r, pg_catalog.pg_type st, "
					  "     pg_catalog.pg_opclass opc "
					  "WHERE st.oid = rngsubtype AND opc.oid = rngsubopc AND "
					  "rngtypid = '%u'",
					  tyinfo->dobj.catId.oid);

	res = ExecuteSqlQueryForSingleRow(fout, query->data);

	qtypname = dbms_md_strdup(dbms_md_fmtId(tyinfo->dobj.name));

	/*
	 * DROP must be fully qualified in case same name appears in pg_catalog.
	 * CASCADE shouldn't be required here as for normal types since the I/O
	 * functions are generic and do not get dropped.
	 */
	appendStringInfo(delq, "DROP TYPE %s.",
					  dbms_md_fmtId(tyinfo->dobj.namespace->dobj.name));
	appendStringInfo(delq, "%s;\n",
					  qtypname);

#if 0
	if (dopt->binary_upgrade)
		binary_upgrade_set_type_oids_by_type_oid(fout,
												 q, tyinfo->dobj.catId.oid);
#endif

	appendStringInfo(q, "CREATE TYPE %s AS RANGE (",
					  qtypname);

	appendStringInfo(q, "\n    subtype = %s",
					  dbms_md_get_field_value(res, 0, dbms_md_get_field_subscript(res, "rngsubtype")));

	/* print subtype_opclass only if not default for subtype */
	if (dbms_md_get_field_value(res, 0, dbms_md_get_field_subscript(res, "opcdefault"))[0] != 't')
	{
		char	   *opcname = dbms_md_get_field_value(res, 0, dbms_md_get_field_subscript(res, "opcname"));
		char	   *nspname = dbms_md_get_field_value(res, 0, dbms_md_get_field_subscript(res, "opcnsp"));

		/* always schema-qualify, don't try to be smart */
		appendStringInfo(q, ",\n    subtype_opclass = %s.",
						  dbms_md_fmtId(nspname));
		appendStringInfoString(q, dbms_md_fmtId_internal(opcname, true));
	}

	collationOid = dbms_md_str_to_oid(dbms_md_get_field_value(res, 0, dbms_md_get_field_subscript(res, "collation")));
	if (OidIsValid(collationOid))
	{
		CollInfo   *coll = findCollationByOid(fout, collationOid);

		if (coll)
		{
			/* always schema-qualify, don't try to be smart */
			appendStringInfo(q, ",\n    collation = %s.",
							  dbms_md_fmtId(coll->dobj.namespace->dobj.name));
			appendStringInfoString(q, dbms_md_fmtId_internal(coll->dobj.name, true));
		}
	}

	procname = dbms_md_get_field_value(res, 0, dbms_md_get_field_subscript(res, "rngcanonical"));
	if (strcmp(procname, "-") != 0)
		appendStringInfo(q, ",\n    canonical = %s", procname);

	procname = dbms_md_get_field_value(res, 0, dbms_md_get_field_subscript(res, "rngsubdiff"));
	if (strcmp(procname, "-") != 0)
		appendStringInfo(q, ",\n    subtype_diff = %s", procname);

	appendStringInfoString(q, "\n);\n");

	appendStringInfo(labelq, "TYPE %s", qtypname);

#if 0
	if (dopt->binary_upgrade)
		binary_upgrade_extension_member(q, &tyinfo->dobj, labelq->data);
#endif

	if (tyinfo->dobj.dump & DUMP_COMPONENT_DEFINITION)
		ArchiveEntry(fout, tyinfo->dobj.catId, tyinfo->dobj.dumpId,
					 tyinfo->dobj.name,
					 tyinfo->dobj.namespace->dobj.name,
					 NULL,
					 tyinfo->rolname, false,
					 "TYPE", SECTION_PRE_DATA,
					 q->data, delq->data, NULL,
					 NULL, 0,
					 NULL, NULL);

	/* Dump Type Comments and Security Labels */
	if (tyinfo->dobj.dump & DUMP_COMPONENT_COMMENT)
		dumpComment(fout, labelq->data,
					tyinfo->dobj.namespace->dobj.name, tyinfo->rolname,
					tyinfo->dobj.catId, 0, tyinfo->dobj.dumpId);

	if (tyinfo->dobj.dump & DUMP_COMPONENT_SECLABEL)
		dumpSecLabel(fout, labelq->data,
					 tyinfo->dobj.namespace->dobj.name, tyinfo->rolname,
					 tyinfo->dobj.catId, 0, tyinfo->dobj.dumpId);

	if (tyinfo->dobj.dump & DUMP_COMPONENT_ACL)
		dumpACL(fout, tyinfo->dobj.catId, tyinfo->dobj.dumpId, "TYPE",
				qtypname, NULL, tyinfo->dobj.name,
				tyinfo->dobj.namespace->dobj.name,
				tyinfo->rolname, tyinfo->typacl, tyinfo->rtypacl,
				tyinfo->inittypacl, tyinfo->initrtypacl);

	dbms_md_free_tuples(res);
	destroyStringInfo(q);
	pfree(q);
	destroyStringInfo(delq);
	pfree(delq);
	destroyStringInfo(labelq);
	pfree(labelq);
	destroyStringInfo(query);
	pfree(query);
}

/*
 * dumpUndefinedType
 *	  writes out to fout the queries to recreate a !typisdefined type
 *
 * This is a shell type, but we use different terminology to distinguish
 * this case from where we have to emit a shell type definition to break
 * circular dependencies.  An undefined type shouldn't ever have anything
 * depending on it.
 */
static void
dumpUndefinedType(Archive *fout, TypeInfo *tyinfo)
{
	//DumpOptions *dopt = fout->dopt;
	StringInfo q = createStringInfo();
	StringInfo delq = createStringInfo();
	StringInfo labelq = createStringInfo();
	char	   *qtypname;

	qtypname = dbms_md_strdup(dbms_md_fmtId(tyinfo->dobj.name));

	/*
	 * DROP must be fully qualified in case same name appears in pg_catalog.
	 */
	appendStringInfo(delq, "DROP TYPE %s.",
					  dbms_md_fmtId(tyinfo->dobj.namespace->dobj.name));
	appendStringInfo(delq, "%s;\n",
					  qtypname);

#if 0
	if (dopt->binary_upgrade)
		binary_upgrade_set_type_oids_by_type_oid(fout,
												 q, tyinfo->dobj.catId.oid);
#endif

	appendStringInfo(q, "CREATE TYPE %s;\n",
					  qtypname);

	appendStringInfo(labelq, "TYPE %s", qtypname);

#if 0
	if (dopt->binary_upgrade)
		binary_upgrade_extension_member(q, &tyinfo->dobj, labelq->data);
#endif

	if (tyinfo->dobj.dump & DUMP_COMPONENT_DEFINITION)
		ArchiveEntry(fout, tyinfo->dobj.catId, tyinfo->dobj.dumpId,
					 tyinfo->dobj.name,
					 tyinfo->dobj.namespace->dobj.name,
					 NULL,
					 tyinfo->rolname, false,
					 "TYPE", SECTION_PRE_DATA,
					 q->data, delq->data, NULL,
					 NULL, 0,
					 NULL, NULL);

	/* Dump Type Comments and Security Labels */
	if (tyinfo->dobj.dump & DUMP_COMPONENT_COMMENT)
		dumpComment(fout, labelq->data,
					tyinfo->dobj.namespace->dobj.name, tyinfo->rolname,
					tyinfo->dobj.catId, 0, tyinfo->dobj.dumpId);

	if (tyinfo->dobj.dump & DUMP_COMPONENT_SECLABEL)
		dumpSecLabel(fout, labelq->data,
					 tyinfo->dobj.namespace->dobj.name, tyinfo->rolname,
					 tyinfo->dobj.catId, 0, tyinfo->dobj.dumpId);

	if (tyinfo->dobj.dump & DUMP_COMPONENT_ACL)
		dumpACL(fout, tyinfo->dobj.catId, tyinfo->dobj.dumpId, "TYPE",
				qtypname, NULL, tyinfo->dobj.name,
				tyinfo->dobj.namespace->dobj.name,
				tyinfo->rolname, tyinfo->typacl, tyinfo->rtypacl,
				tyinfo->inittypacl, tyinfo->initrtypacl);

	destroyStringInfo(q);
	pfree(q);
	destroyStringInfo(delq);
	pfree(delq);
	destroyStringInfo(labelq);
	pfree(labelq);
}

/*
 * dumpBaseType
 *	  writes out to fout the queries to recreate a user-defined base type
 */
static void
dumpBaseType(Archive *fout, TypeInfo *tyinfo)
{
	//DumpOptions *dopt = fout->dopt;
	StringInfo q = createStringInfo();
	StringInfo delq = createStringInfo();
	StringInfo labelq = createStringInfo();
	StringInfo query = createStringInfo();
	SPITupleTable   *res;
	char	   *qtypname;
	char	   *typlen;
	char	   *typinput;
	char	   *typoutput;
	char	   *typreceive;
	char	   *typsend;
	char	   *typmodin;
	char	   *typmodout;
	char	   *typanalyze;
	Oid			typreceiveoid;
	Oid			typsendoid;
	Oid			typmodinoid;
	Oid			typmodoutoid;
	Oid			typanalyzeoid;
	char	   *typcategory;
	char	   *typispreferred;
	char	   *typdelim;
	char	   *typbyval;
	char	   *typalign;
	char	   *typstorage;
	char	   *typcollatable;
	char	   *typdefault;
	bool		typdefault_is_literal = false;

	/* Set proper schema search path so regproc references list correctly */
	selectSourceSchema(fout, tyinfo->dobj.namespace->dobj.name);

	/* Fetch type-specific details */
	if (fout->remoteVersion >= 90100)
	{
		appendStringInfo(query, "SELECT typlen, "
						  "typinput, typoutput, typreceive, typsend, "
						  "typmodin, typmodout, typanalyze, "
						  "typreceive::pg_catalog.oid AS typreceiveoid, "
						  "typsend::pg_catalog.oid AS typsendoid, "
						  "typmodin::pg_catalog.oid AS typmodinoid, "
						  "typmodout::pg_catalog.oid AS typmodoutoid, "
						  "typanalyze::pg_catalog.oid AS typanalyzeoid, "
						  "typcategory, typispreferred, "
						  "typdelim, typbyval, typalign, typstorage, "
						  "(typcollation <> 0) AS typcollatable, "
						  "pg_catalog.pg_get_expr(typdefaultbin, 0) AS typdefaultbin, typdefault "
						  "FROM pg_catalog.pg_type "
						  "WHERE oid = '%u'::pg_catalog.oid",
						  tyinfo->dobj.catId.oid);
	}
	else if (fout->remoteVersion >= 80400)
	{
		appendStringInfo(query, "SELECT typlen, "
						  "typinput, typoutput, typreceive, typsend, "
						  "typmodin, typmodout, typanalyze, "
						  "typreceive::pg_catalog.oid AS typreceiveoid, "
						  "typsend::pg_catalog.oid AS typsendoid, "
						  "typmodin::pg_catalog.oid AS typmodinoid, "
						  "typmodout::pg_catalog.oid AS typmodoutoid, "
						  "typanalyze::pg_catalog.oid AS typanalyzeoid, "
						  "typcategory, typispreferred, "
						  "typdelim, typbyval, typalign, typstorage, "
						  "false AS typcollatable, "
						  "pg_catalog.pg_get_expr(typdefaultbin, 0) AS typdefaultbin, typdefault "
						  "FROM pg_catalog.pg_type "
						  "WHERE oid = '%u'::pg_catalog.oid",
						  tyinfo->dobj.catId.oid);
	}
	else if (fout->remoteVersion >= 80300)
	{
		/* Before 8.4, pg_get_expr does not allow 0 for its second arg */
		appendStringInfo(query, "SELECT typlen, "
						  "typinput, typoutput, typreceive, typsend, "
						  "typmodin, typmodout, typanalyze, "
						  "typreceive::pg_catalog.oid AS typreceiveoid, "
						  "typsend::pg_catalog.oid AS typsendoid, "
						  "typmodin::pg_catalog.oid AS typmodinoid, "
						  "typmodout::pg_catalog.oid AS typmodoutoid, "
						  "typanalyze::pg_catalog.oid AS typanalyzeoid, "
						  "'U' AS typcategory, false AS typispreferred, "
						  "typdelim, typbyval, typalign, typstorage, "
						  "false AS typcollatable, "
						  "pg_catalog.pg_get_expr(typdefaultbin, 'pg_catalog.pg_type'::pg_catalog.regclass) AS typdefaultbin, typdefault "
						  "FROM pg_catalog.pg_type "
						  "WHERE oid = '%u'::pg_catalog.oid",
						  tyinfo->dobj.catId.oid);
	}
	else
	{
		appendStringInfo(query, "SELECT typlen, "
						  "typinput, typoutput, typreceive, typsend, "
						  "'-' AS typmodin, '-' AS typmodout, "
						  "typanalyze, "
						  "typreceive::pg_catalog.oid AS typreceiveoid, "
						  "typsend::pg_catalog.oid AS typsendoid, "
						  "0 AS typmodinoid, 0 AS typmodoutoid, "
						  "typanalyze::pg_catalog.oid AS typanalyzeoid, "
						  "'U' AS typcategory, false AS typispreferred, "
						  "typdelim, typbyval, typalign, typstorage, "
						  "false AS typcollatable, "
						  "pg_catalog.pg_get_expr(typdefaultbin, 'pg_catalog.pg_type'::pg_catalog.regclass) AS typdefaultbin, typdefault "
						  "FROM pg_catalog.pg_type "
						  "WHERE oid = '%u'::pg_catalog.oid",
						  tyinfo->dobj.catId.oid);
	}

	res = ExecuteSqlQueryForSingleRow(fout, query->data);

	typlen = dbms_md_get_field_value(res, 0, dbms_md_get_field_subscript(res, "typlen"));
	typinput = dbms_md_get_field_value(res, 0, dbms_md_get_field_subscript(res, "typinput"));
	typoutput = dbms_md_get_field_value(res, 0, dbms_md_get_field_subscript(res, "typoutput"));
	typreceive = dbms_md_get_field_value(res, 0, dbms_md_get_field_subscript(res, "typreceive"));
	typsend = dbms_md_get_field_value(res, 0, dbms_md_get_field_subscript(res, "typsend"));
	typmodin = dbms_md_get_field_value(res, 0, dbms_md_get_field_subscript(res, "typmodin"));
	typmodout = dbms_md_get_field_value(res, 0, dbms_md_get_field_subscript(res, "typmodout"));
	typanalyze = dbms_md_get_field_value(res, 0, dbms_md_get_field_subscript(res, "typanalyze"));
	typreceiveoid = dbms_md_str_to_oid(dbms_md_get_field_value(res, 0, dbms_md_get_field_subscript(res, "typreceiveoid")));
	typsendoid = dbms_md_str_to_oid(dbms_md_get_field_value(res, 0, dbms_md_get_field_subscript(res, "typsendoid")));
	typmodinoid = dbms_md_str_to_oid(dbms_md_get_field_value(res, 0, dbms_md_get_field_subscript(res, "typmodinoid")));
	typmodoutoid = dbms_md_str_to_oid(dbms_md_get_field_value(res, 0, dbms_md_get_field_subscript(res, "typmodoutoid")));
	typanalyzeoid = dbms_md_str_to_oid(dbms_md_get_field_value(res, 0, dbms_md_get_field_subscript(res, "typanalyzeoid")));
	typcategory = dbms_md_get_field_value(res, 0, dbms_md_get_field_subscript(res, "typcategory"));
	typispreferred = dbms_md_get_field_value(res, 0, dbms_md_get_field_subscript(res, "typispreferred"));
	typdelim = dbms_md_get_field_value(res, 0, dbms_md_get_field_subscript(res, "typdelim"));
	typbyval = dbms_md_get_field_value(res, 0, dbms_md_get_field_subscript(res, "typbyval"));
	typalign = dbms_md_get_field_value(res, 0, dbms_md_get_field_subscript(res, "typalign"));
	typstorage = dbms_md_get_field_value(res, 0, dbms_md_get_field_subscript(res, "typstorage"));
	typcollatable = dbms_md_get_field_value(res, 0, dbms_md_get_field_subscript(res, "typcollatable"));
	if (!dbms_md_is_tuple_field_null(res, 0, dbms_md_get_field_subscript(res, "typdefaultbin")))
		typdefault = dbms_md_get_field_value(res, 0, dbms_md_get_field_subscript(res, "typdefaultbin"));
	else if (!dbms_md_is_tuple_field_null(res, 0, dbms_md_get_field_subscript(res, "typdefault")))
	{
		typdefault = dbms_md_get_field_value(res, 0, dbms_md_get_field_subscript(res, "typdefault"));
		typdefault_is_literal = true;	/* it needs quotes */
	}
	else
		typdefault = NULL;

	qtypname = dbms_md_strdup(dbms_md_fmtId(tyinfo->dobj.name));

	/*
	 * DROP must be fully qualified in case same name appears in pg_catalog.
	 * The reason we include CASCADE is that the circular dependency between
	 * the type and its I/O functions makes it impossible to drop the type any
	 * other way.
	 */
	appendStringInfo(delq, "DROP TYPE %s.",
					  dbms_md_fmtId(tyinfo->dobj.namespace->dobj.name));
	appendStringInfo(delq, "%s CASCADE;\n",
					  qtypname);

#if 0
	/* We might already have a shell type, but setting pg_type_oid is harmless */
	if (dopt->binary_upgrade)
		binary_upgrade_set_type_oids_by_type_oid(fout, q,
												 tyinfo->dobj.catId.oid);
#endif

	appendStringInfo(q,
					  "CREATE TYPE %s (\n"
					  "    INTERNALLENGTH = %s",
					  qtypname,
					  (strcmp(typlen, "-1") == 0) ? "variable" : typlen);

	/* regproc result is sufficiently quoted already */
	appendStringInfo(q, ",\n    INPUT = %s", typinput);
	appendStringInfo(q, ",\n    OUTPUT = %s", typoutput);
	if (OidIsValid(typreceiveoid))
		appendStringInfo(q, ",\n    RECEIVE = %s", typreceive);
	if (OidIsValid(typsendoid))
		appendStringInfo(q, ",\n    SEND = %s", typsend);
	if (OidIsValid(typmodinoid))
		appendStringInfo(q, ",\n    TYPMOD_IN = %s", typmodin);
	if (OidIsValid(typmodoutoid))
		appendStringInfo(q, ",\n    TYPMOD_OUT = %s", typmodout);
	if (OidIsValid(typanalyzeoid))
		appendStringInfo(q, ",\n    ANALYZE = %s", typanalyze);

	if (strcmp(typcollatable, "t") == 0)
		appendStringInfoString(q, ",\n    COLLATABLE = true");

	if (typdefault != NULL)
	{
		appendStringInfoString(q, ",\n    DEFAULT = ");
		if (typdefault_is_literal)
			appendStringLiteralAH(q, typdefault, fout);
		else
			appendStringInfoString(q, typdefault);
	}

	if (OidIsValid(tyinfo->typelem))
	{
		char	   *elemType;

		/* reselect schema in case changed by function dump */
		selectSourceSchema(fout, tyinfo->dobj.namespace->dobj.name);
		elemType = getFormattedTypeName(fout, tyinfo->typelem, zeroAsOpaque);
		appendStringInfo(q, ",\n    ELEMENT = %s", elemType);
		free(elemType);
	}

	if (strcmp(typcategory, "U") != 0)
	{
		appendStringInfoString(q, ",\n    CATEGORY = ");
		appendStringLiteralAH(q, typcategory, fout);
	}

	if (strcmp(typispreferred, "t") == 0)
		appendStringInfoString(q, ",\n    PREFERRED = true");

	if (typdelim && strcmp(typdelim, ",") != 0)
	{
		appendStringInfoString(q, ",\n    DELIMITER = ");
		appendStringLiteralAH(q, typdelim, fout);
	}

	if (strcmp(typalign, "c") == 0)
		appendStringInfoString(q, ",\n    ALIGNMENT = char");
	else if (strcmp(typalign, "s") == 0)
		appendStringInfoString(q, ",\n    ALIGNMENT = int2");
	else if (strcmp(typalign, "i") == 0)
		appendStringInfoString(q, ",\n    ALIGNMENT = int4");
	else if (strcmp(typalign, "d") == 0)
		appendStringInfoString(q, ",\n    ALIGNMENT = double");

	if (strcmp(typstorage, "p") == 0)
		appendStringInfoString(q, ",\n    STORAGE = plain");
	else if (strcmp(typstorage, "e") == 0)
		appendStringInfoString(q, ",\n    STORAGE = external");
	else if (strcmp(typstorage, "x") == 0)
		appendStringInfoString(q, ",\n    STORAGE = extended");
	else if (strcmp(typstorage, "m") == 0)
		appendStringInfoString(q, ",\n    STORAGE = main");

	if (strcmp(typbyval, "t") == 0)
		appendStringInfoString(q, ",\n    PASSEDBYVALUE");

	appendStringInfoString(q, "\n);\n");

	appendStringInfo(labelq, "TYPE %s", qtypname);

#if 0
	if (dopt->binary_upgrade)
		binary_upgrade_extension_member(q, &tyinfo->dobj, labelq->data);
#endif

	if (tyinfo->dobj.dump & DUMP_COMPONENT_DEFINITION)
		ArchiveEntry(fout, tyinfo->dobj.catId, tyinfo->dobj.dumpId,
					 tyinfo->dobj.name,
					 tyinfo->dobj.namespace->dobj.name,
					 NULL,
					 tyinfo->rolname, false,
					 "TYPE", SECTION_PRE_DATA,
					 q->data, delq->data, NULL,
					 NULL, 0,
					 NULL, NULL);

	/* Dump Type Comments and Security Labels */
	if (tyinfo->dobj.dump & DUMP_COMPONENT_COMMENT)
		dumpComment(fout, labelq->data,
					tyinfo->dobj.namespace->dobj.name, tyinfo->rolname,
					tyinfo->dobj.catId, 0, tyinfo->dobj.dumpId);

	if (tyinfo->dobj.dump & DUMP_COMPONENT_SECLABEL)
		dumpSecLabel(fout, labelq->data,
					 tyinfo->dobj.namespace->dobj.name, tyinfo->rolname,
					 tyinfo->dobj.catId, 0, tyinfo->dobj.dumpId);

	if (tyinfo->dobj.dump & DUMP_COMPONENT_ACL)
		dumpACL(fout, tyinfo->dobj.catId, tyinfo->dobj.dumpId, "TYPE",
				qtypname, NULL, tyinfo->dobj.name,
				tyinfo->dobj.namespace->dobj.name,
				tyinfo->rolname, tyinfo->typacl, tyinfo->rtypacl,
				tyinfo->inittypacl, tyinfo->initrtypacl);

	dbms_md_free_tuples(res);
	destroyStringInfo(q);
	pfree(q);
	destroyStringInfo(delq);
	pfree(delq);
	destroyStringInfo(labelq);
	pfree(labelq);
	destroyStringInfo(query);
	pfree(query);
}

/*
 * dumpDomain
 *	  writes out to fout the queries to recreate a user-defined domain
 */
static void
dumpDomain(Archive *fout, TypeInfo *tyinfo)
{
	//DumpOptions *dopt = fout->dopt;
	StringInfo q = createStringInfo();
	StringInfo delq = createStringInfo();
	StringInfo labelq = createStringInfo();
	StringInfo query = createStringInfo();
	SPITupleTable   *res;
	int			i;
	char	   *qtypname;
	char	   *typnotnull;
	char	   *typdefn;
	char	   *typdefault;
	Oid			typcollation;
	bool		typdefault_is_literal = false;

	/* Set proper schema search path so type references list correctly */
	selectSourceSchema(fout, tyinfo->dobj.namespace->dobj.name);

	/* Fetch domain specific details */
	if (fout->remoteVersion >= 90100)
	{
		/* typcollation is new in 9.1 */
		appendStringInfo(query, "SELECT t.typnotnull, "
						  "pg_catalog.format_type(t.typbasetype, t.typtypmod) AS typdefn, "
						  "pg_catalog.pg_get_expr(t.typdefaultbin, 'pg_catalog.pg_type'::pg_catalog.regclass) AS typdefaultbin, "
						  "t.typdefault, "
						  "CASE WHEN t.typcollation <> u.typcollation "
						  "THEN t.typcollation ELSE 0 END AS typcollation "
						  "FROM pg_catalog.pg_type t "
						  "LEFT JOIN pg_catalog.pg_type u ON (t.typbasetype = u.oid) "
						  "WHERE t.oid = '%u'::pg_catalog.oid",
						  tyinfo->dobj.catId.oid);
	}
	else
	{
		appendStringInfo(query, "SELECT typnotnull, "
						  "pg_catalog.format_type(typbasetype, typtypmod) AS typdefn, "
						  "pg_catalog.pg_get_expr(typdefaultbin, 'pg_catalog.pg_type'::pg_catalog.regclass) AS typdefaultbin, "
						  "typdefault, 0 AS typcollation "
						  "FROM pg_catalog.pg_type "
						  "WHERE oid = '%u'::pg_catalog.oid",
						  tyinfo->dobj.catId.oid);
	}

	res = ExecuteSqlQueryForSingleRow(fout, query->data);

	typnotnull = dbms_md_get_field_value(res, 0, dbms_md_get_field_subscript(res, "typnotnull"));
	typdefn = dbms_md_get_field_value(res, 0, dbms_md_get_field_subscript(res, "typdefn"));
	if (!dbms_md_is_tuple_field_null(res, 0, dbms_md_get_field_subscript(res, "typdefaultbin")))
		typdefault = dbms_md_get_field_value(res, 0, dbms_md_get_field_subscript(res, "typdefaultbin"));
	else if (!dbms_md_is_tuple_field_null(res, 0, dbms_md_get_field_subscript(res, "typdefault")))
	{
		typdefault = dbms_md_get_field_value(res, 0, dbms_md_get_field_subscript(res, "typdefault"));
		typdefault_is_literal = true;	/* it needs quotes */
	}
	else
		typdefault = NULL;
	typcollation = dbms_md_str_to_oid(dbms_md_get_field_value(res, 0, dbms_md_get_field_subscript(res, "typcollation")));

#if 0
	if (dopt->binary_upgrade)
		binary_upgrade_set_type_oids_by_type_oid(fout, q,
												 tyinfo->dobj.catId.oid);
#endif

	qtypname = dbms_md_strdup(dbms_md_fmtId(tyinfo->dobj.name));

	appendStringInfo(q,
					  "CREATE DOMAIN %s AS %s",
					  qtypname,
					  typdefn);

	/* Print collation only if different from base type's collation */
	if (OidIsValid(typcollation))
	{
		CollInfo   *coll;

		coll = findCollationByOid(fout, typcollation);
		if (coll)
		{
			/* always schema-qualify, don't try to be smart */
			appendStringInfo(q, " COLLATE %s.",
							  dbms_md_fmtId(coll->dobj.namespace->dobj.name));
			appendStringInfoString(q, dbms_md_fmtId_internal(coll->dobj.name, true));
		}
	}

	if (typnotnull[0] == 't')
		appendStringInfoString(q, " NOT NULL");

	if (typdefault != NULL)
	{
		appendStringInfoString(q, " DEFAULT ");
		if (typdefault_is_literal)
			appendStringLiteralAH(q, typdefault, fout);
		else
			appendStringInfoString(q, typdefault);
	}

	dbms_md_free_tuples(res);

	/*
	 * Add any CHECK constraints for the domain
	 */
	for (i = 0; i < tyinfo->nDomChecks; i++)
	{
		ConstraintInfo *domcheck = &(tyinfo->domChecks[i]);

		if (!domcheck->separate)
			appendStringInfo(q, "\n\tCONSTRAINT %s %s",
							  dbms_md_fmtId(domcheck->dobj.name), domcheck->condef);
	}

	appendStringInfoString(q, ";\n");

	/*
	 * DROP must be fully qualified in case same name appears in pg_catalog
	 */
	appendStringInfo(delq, "DROP DOMAIN %s.",
					  dbms_md_fmtId(tyinfo->dobj.namespace->dobj.name));
	appendStringInfo(delq, "%s;\n",
					  qtypname);

	appendStringInfo(labelq, "DOMAIN %s", qtypname);

#if 0
	if (dopt->binary_upgrade)
		binary_upgrade_extension_member(q, &tyinfo->dobj, labelq->data);
#endif

	if (tyinfo->dobj.dump & DUMP_COMPONENT_DEFINITION)
		ArchiveEntry(fout, tyinfo->dobj.catId, tyinfo->dobj.dumpId,
					 tyinfo->dobj.name,
					 tyinfo->dobj.namespace->dobj.name,
					 NULL,
					 tyinfo->rolname, false,
					 "DOMAIN", SECTION_PRE_DATA,
					 q->data, delq->data, NULL,
					 NULL, 0,
					 NULL, NULL);

	/* Dump Domain Comments and Security Labels */
	if (tyinfo->dobj.dump & DUMP_COMPONENT_COMMENT)
		dumpComment(fout, labelq->data,
					tyinfo->dobj.namespace->dobj.name, tyinfo->rolname,
					tyinfo->dobj.catId, 0, tyinfo->dobj.dumpId);

	if (tyinfo->dobj.dump & DUMP_COMPONENT_SECLABEL)
		dumpSecLabel(fout, labelq->data,
					 tyinfo->dobj.namespace->dobj.name, tyinfo->rolname,
					 tyinfo->dobj.catId, 0, tyinfo->dobj.dumpId);

	if (tyinfo->dobj.dump & DUMP_COMPONENT_ACL)
		dumpACL(fout, tyinfo->dobj.catId, tyinfo->dobj.dumpId, "TYPE",
				qtypname, NULL, tyinfo->dobj.name,
				tyinfo->dobj.namespace->dobj.name,
				tyinfo->rolname, tyinfo->typacl, tyinfo->rtypacl,
				tyinfo->inittypacl, tyinfo->initrtypacl);

	/* Dump any per-constraint comments */
	for (i = 0; i < tyinfo->nDomChecks; i++)
	{
		ConstraintInfo *domcheck = &(tyinfo->domChecks[i]);
		StringInfo labelq = createStringInfo();

		appendStringInfo(labelq, "CONSTRAINT %s ",
						  dbms_md_fmtId(domcheck->dobj.name));
		appendStringInfo(labelq, "ON DOMAIN %s",
						  qtypname);

		if (tyinfo->dobj.dump & DUMP_COMPONENT_COMMENT)
			dumpComment(fout, labelq->data,
						tyinfo->dobj.namespace->dobj.name,
						tyinfo->rolname,
						domcheck->dobj.catId, 0, tyinfo->dobj.dumpId);

		destroyStringInfo(labelq);
	}

	destroyStringInfo(q);
	pfree(q);
	destroyStringInfo(delq);
	pfree(delq);
	destroyStringInfo(labelq);
	pfree(labelq);
	destroyStringInfo(query);
	pfree(query);
}

/*
 * dumpCompositeType
 *	  writes out to fout the queries to recreate a user-defined stand-alone
 *	  composite type
 */
static void
dumpCompositeType(Archive *fout, TypeInfo *tyinfo)
{
	DumpOptions *dopt = fout->dopt;
	StringInfo q = createStringInfo();
	StringInfo dropped = createStringInfo();
	StringInfo delq = createStringInfo();
	StringInfo labelq = createStringInfo();
	StringInfo query = createStringInfo();
	SPITupleTable   *res;
	char	   *qtypname;
	int			ntups;
	int			i_attname;
	int			i_atttypdefn;
	int			i_attlen;
	int			i_attalign;
	int			i_attisdropped;
	int			i_attcollation;
	int			i;
	int			actual_atts;

	/* Set proper schema search path so type references list correctly */
	selectSourceSchema(fout, tyinfo->dobj.namespace->dobj.name);

	/* Fetch type specific details */
	if (fout->remoteVersion >= 90100)
	{
		/*
		 * attcollation is new in 9.1.  Since we only want to dump COLLATE
		 * clauses for attributes whose collation is different from their
		 * type's default, we use a CASE here to suppress uninteresting
		 * attcollations cheaply.  atttypid will be 0 for dropped columns;
		 * collation does not matter for those.
		 */
		appendStringInfo(query, "SELECT a.attname, "
						  "pg_catalog.format_type(a.atttypid, a.atttypmod) AS atttypdefn, "
						  "a.attlen, a.attalign, a.attisdropped, "
						  "CASE WHEN a.attcollation <> at.typcollation "
						  "THEN a.attcollation ELSE 0 END AS attcollation "
						  "FROM pg_catalog.pg_type ct "
						  "JOIN pg_catalog.pg_attribute a ON a.attrelid = ct.typrelid "
						  "LEFT JOIN pg_catalog.pg_type at ON at.oid = a.atttypid "
						  "WHERE ct.oid = '%u'::pg_catalog.oid "
						  "ORDER BY a.attnum ",
						  tyinfo->dobj.catId.oid);
	}
	else
	{
		/*
		 * Since ALTER TYPE could not drop columns until 9.1, attisdropped
		 * should always be false.
		 */
		appendStringInfo(query, "SELECT a.attname, "
						  "pg_catalog.format_type(a.atttypid, a.atttypmod) AS atttypdefn, "
						  "a.attlen, a.attalign, a.attisdropped, "
						  "0 AS attcollation "
						  "FROM pg_catalog.pg_type ct, pg_catalog.pg_attribute a "
						  "WHERE ct.oid = '%u'::pg_catalog.oid "
						  "AND a.attrelid = ct.typrelid "
						  "ORDER BY a.attnum ",
						  tyinfo->dobj.catId.oid);
	}

	res = ExecuteSqlQuery(fout, query->data);

	ntups = dbms_md_get_tuple_num(res);

	i_attname = dbms_md_get_field_subscript(res, "attname");
	i_atttypdefn = dbms_md_get_field_subscript(res, "atttypdefn");
	i_attlen = dbms_md_get_field_subscript(res, "attlen");
	i_attalign = dbms_md_get_field_subscript(res, "attalign");
	i_attisdropped = dbms_md_get_field_subscript(res, "attisdropped");
	i_attcollation = dbms_md_get_field_subscript(res, "attcollation");

#if 0
	if (dopt->binary_upgrade)
	{
		binary_upgrade_set_type_oids_by_type_oid(fout, q,
												 tyinfo->dobj.catId.oid);
		binary_upgrade_set_pg_class_oids(fout, q, tyinfo->typrelid, false);
	}
#endif

	qtypname = dbms_md_strdup(dbms_md_fmtId(tyinfo->dobj.name));

	appendStringInfo(q, "CREATE TYPE %s AS (",
					  qtypname);

	actual_atts = 0;
	for (i = 0; i < ntups; i++)
	{
		char	   *attname;
		char	   *atttypdefn;
		char	   *attlen;
		char	   *attalign;
		bool		attisdropped;
		Oid			attcollation;

		attname = dbms_md_get_field_value(res, i, i_attname);
		atttypdefn = dbms_md_get_field_value(res, i, i_atttypdefn);
		attlen = dbms_md_get_field_value(res, i, i_attlen);
		attalign = dbms_md_get_field_value(res, i, i_attalign);
		attisdropped = (dbms_md_get_field_value(res, i, i_attisdropped)[0] == 't');
		attcollation = dbms_md_str_to_oid(dbms_md_get_field_value(res, i, i_attcollation));

		if (attisdropped && !dopt->binary_upgrade)
			continue;

		/* Format properly if not first attr */
		if (actual_atts++ > 0)
			appendStringInfoChar(q, ',');
		appendStringInfoString(q, "\n\t");

		if (!attisdropped)
		{
			appendStringInfo(q, "%s %s", dbms_md_fmtId(attname), atttypdefn);

			/* Add collation if not default for the column type */
			if (OidIsValid(attcollation))
			{
				CollInfo   *coll;

				coll = findCollationByOid(fout, attcollation);
				if (coll)
				{
					/* always schema-qualify, don't try to be smart */
					appendStringInfo(q, " COLLATE %s.",
									  dbms_md_fmtId(coll->dobj.namespace->dobj.name));
					appendStringInfoString(q, dbms_md_fmtId_internal(coll->dobj.name, true));
				}
			}
		}
		else
		{
			/*
			 * This is a dropped attribute and we're in binary_upgrade mode.
			 * Insert a placeholder for it in the CREATE TYPE command, and set
			 * length and alignment with direct UPDATE to the catalogs
			 * afterwards. See similar code in dumpTableSchema().
			 */
			appendStringInfo(q, "%s INTEGER /* dummy */", dbms_md_fmtId(attname));

			/* stash separately for insertion after the CREATE TYPE */
			appendStringInfoString(dropped,
								 "\n-- For binary upgrade, recreate dropped column.\n");
			appendStringInfo(dropped, "UPDATE pg_catalog.pg_attribute\n"
							  "SET attlen = %s, "
							  "attalign = '%s', attbyval = false\n"
							  "WHERE attname = ", attlen, attalign);
			appendStringLiteralAH(dropped, attname, fout);
			appendStringInfoString(dropped, "\n  AND attrelid = ");
			appendStringLiteralAH(dropped, qtypname, fout);
			appendStringInfoString(dropped, "::pg_catalog.regclass;\n");

			appendStringInfo(dropped, "ALTER TYPE %s ",
							  qtypname);
			appendStringInfo(dropped, "DROP ATTRIBUTE %s;\n",
							  dbms_md_fmtId(attname));
		}
	}
	appendStringInfoString(q, "\n);\n");
	appendStringInfoString(q, dropped->data);

	/*
	 * DROP must be fully qualified in case same name appears in pg_catalog
	 */
	appendStringInfo(delq, "DROP TYPE %s.",
					  dbms_md_fmtId(tyinfo->dobj.namespace->dobj.name));
	appendStringInfo(delq, "%s;\n",
					  qtypname);

	appendStringInfo(labelq, "TYPE %s", qtypname);

#if 0
	if (dopt->binary_upgrade)
		binary_upgrade_extension_member(q, &tyinfo->dobj, labelq->data);
#endif

	if (tyinfo->dobj.dump & DUMP_COMPONENT_DEFINITION)
		ArchiveEntry(fout, tyinfo->dobj.catId, tyinfo->dobj.dumpId,
					 tyinfo->dobj.name,
					 tyinfo->dobj.namespace->dobj.name,
					 NULL,
					 tyinfo->rolname, false,
					 "TYPE", SECTION_PRE_DATA,
					 q->data, delq->data, NULL,
					 NULL, 0,
					 NULL, NULL);


	/* Dump Type Comments and Security Labels */
	if (tyinfo->dobj.dump & DUMP_COMPONENT_COMMENT)
		dumpComment(fout, labelq->data,
					tyinfo->dobj.namespace->dobj.name, tyinfo->rolname,
					tyinfo->dobj.catId, 0, tyinfo->dobj.dumpId);

	if (tyinfo->dobj.dump & DUMP_COMPONENT_SECLABEL)
		dumpSecLabel(fout, labelq->data,
					 tyinfo->dobj.namespace->dobj.name, tyinfo->rolname,
					 tyinfo->dobj.catId, 0, tyinfo->dobj.dumpId);

	if (tyinfo->dobj.dump & DUMP_COMPONENT_ACL)
		dumpACL(fout, tyinfo->dobj.catId, tyinfo->dobj.dumpId, "TYPE",
				qtypname, NULL, tyinfo->dobj.name,
				tyinfo->dobj.namespace->dobj.name,
				tyinfo->rolname, tyinfo->typacl, tyinfo->rtypacl,
				tyinfo->inittypacl, tyinfo->initrtypacl);

	dbms_md_free_tuples(res);
	
	destroyStringInfo(q);	
	destroyStringInfo(delq);
	destroyStringInfo(labelq);
	destroyStringInfo(query);
	destroyStringInfo(dropped);

	pfree(q);
	pfree(delq);
	pfree(labelq);
	pfree(query);
	pfree(dropped);

	/* Dump any per-column comments */
	if (tyinfo->dobj.dump & DUMP_COMPONENT_COMMENT)
		dumpCompositeTypeColComments(fout, tyinfo);
}

/*
 * dumpCompositeTypeColComments
 *	  writes out to fout the queries to recreate comments on the columns of
 *	  a user-defined stand-alone composite type
 */
static void
dumpCompositeTypeColComments(Archive *fout, TypeInfo *tyinfo)
{
	CommentItem *comments;
	int			ncomments;
	SPITupleTable   *res;
	StringInfo query;
	StringInfo target;
	Oid			pgClassOid;
	int			i;
	int			ntups;
	int			i_attname;
	int			i_attnum;

	query = createStringInfo();

	appendStringInfo(query,
					  "SELECT c.tableoid, a.attname, a.attnum "
					  "FROM pg_catalog.pg_class c, pg_catalog.pg_attribute a "
					  "WHERE c.oid = '%u' AND c.oid = a.attrelid "
					  "  AND NOT a.attisdropped "
					  "ORDER BY a.attnum ",
					  tyinfo->typrelid);

	/* Fetch column attnames */
	res = ExecuteSqlQuery(fout, query->data);

	ntups = dbms_md_get_tuple_num(res);
	if (ntups < 1)
	{
		dbms_md_free_tuples(res);
		destroyStringInfo(query);
		return;
	}

	pgClassOid = dbms_md_str_to_oid(dbms_md_get_field_value(res, 0, dbms_md_get_field_subscript(res, "tableoid")));

	/* Search for comments associated with type's pg_class OID */
	ncomments = findComments(fout,
							 pgClassOid,
							 tyinfo->typrelid,
							 &comments);

	/* If no comments exist, we're done */
	if (ncomments <= 0)
	{
		dbms_md_free_tuples(res);
		destroyStringInfo(query);
		return;
	}

	/* Build COMMENT ON statements */
	target = createStringInfo();

	i_attnum = dbms_md_get_field_subscript(res, "attnum");
	i_attname = dbms_md_get_field_subscript(res, "attname");
	while (ncomments > 0)
	{
		const char *attname;

		attname = NULL;
		for (i = 0; i < ntups; i++)
		{
			if (atoi(dbms_md_get_field_value(res, i, i_attnum)) == comments->objsubid)
			{
				attname = dbms_md_get_field_value(res, i, i_attname);
				break;
			}
		}
		if (attname)			/* just in case we don't find it */
		{
			const char *descr = comments->descr;

			resetStringInfo(target);
			appendStringInfo(target, "COLUMN %s.",
							  dbms_md_fmtId(tyinfo->dobj.name));
			appendStringInfoString(target, dbms_md_fmtId(attname));

			resetStringInfo(query);
			appendStringInfo(query, "COMMENT ON %s IS ", target->data);
			appendStringLiteralAH(query, descr, fout);
			appendStringInfoString(query, ";\n");

			ArchiveEntry(fout, nilCatalogId, createDumpId(fout),
						 target->data,
						 tyinfo->dobj.namespace->dobj.name,
						 NULL, tyinfo->rolname,
						 false, "COMMENT", SECTION_NONE,
						 query->data, "", NULL,
						 &(tyinfo->dobj.dumpId), 1,
						 NULL, NULL);
		}

		comments++;
		ncomments--;
	}

	dbms_md_free_tuples(res);
	
	destroyStringInfo(query);	
	destroyStringInfo(target);

	pfree(query);
	pfree(target);
}

/*
 * dumpShellType
 *	  writes out to fout the queries to create a shell type
 *
 * We dump a shell definition in advance of the I/O functions for the type.
 */
static void
dumpShellType(Archive *fout, ShellTypeInfo *stinfo)
{
	DumpOptions *dopt = fout->dopt;
	StringInfo q;

	/* Skip if not to be dumped */
	if (!stinfo->dobj.dump || dopt->dataOnly)
		return;

	q = createStringInfo();

	/*
	 * Note the lack of a DROP command for the shell type; any required DROP
	 * is driven off the base type entry, instead.  This interacts with
	 * _printTocEntry()'s use of the presence of a DROP command to decide
	 * whether an entry needs an ALTER OWNER command.  We don't want to alter
	 * the shell type's owner immediately on creation; that should happen only
	 * after it's filled in, otherwise the backend complains.
	 */
#if 0
	if (dopt->binary_upgrade)
		binary_upgrade_set_type_oids_by_type_oid(fout, q,
												 stinfo->baseType->dobj.catId.oid);
#endif

	appendStringInfo(q, "CREATE TYPE %s;\n",
					  dbms_md_fmtId(stinfo->dobj.name));

	if (stinfo->dobj.dump & DUMP_COMPONENT_DEFINITION)
		ArchiveEntry(fout, stinfo->dobj.catId, stinfo->dobj.dumpId,
					 stinfo->dobj.name,
					 stinfo->dobj.namespace->dobj.name,
					 NULL,
					 stinfo->baseType->rolname, false,
					 "SHELL TYPE", SECTION_PRE_DATA,
					 q->data, "", NULL,
					 NULL, 0,
					 NULL, NULL);

	destroyStringInfo(q);
	pfree(q);
}

/*
 * dumpProcLang
 *		  writes out to fout the queries to recreate a user-defined
 *		  procedural language
 */
static void
dumpProcLang(Archive *fout, ProcLangInfo *plang)
{
	DumpOptions *dopt = fout->dopt;
	StringInfo defqry;
	StringInfo delqry;
	StringInfo labelq;
	bool		useParams;
	char	   *qlanname;
	char	   *lanschema;
	FuncInfo   *funcInfo;
	FuncInfo   *inlineInfo = NULL;
	FuncInfo   *validatorInfo = NULL;

	/* Skip if not to be dumped */
	if (!plang->dobj.dump || dopt->dataOnly)
		return;

	/*
	 * Try to find the support function(s).  It is not an error if we don't
	 * find them --- if the functions are in the pg_catalog schema, as is
	 * standard in 8.1 and up, then we won't have loaded them. (In this case
	 * we will emit a parameterless CREATE LANGUAGE command, which will
	 * require PL template knowledge in the backend to reload.)
	 */

	funcInfo = findFuncByOid(fout, plang->lanplcallfoid);
	if (funcInfo != NULL && !funcInfo->dobj.dump)
		funcInfo = NULL;		/* treat not-dumped same as not-found */

	if (OidIsValid(plang->laninline))
	{
		inlineInfo = findFuncByOid(fout, plang->laninline);
		if (inlineInfo != NULL && !inlineInfo->dobj.dump)
			inlineInfo = NULL;
	}

	if (OidIsValid(plang->lanvalidator))
	{
		validatorInfo = findFuncByOid(fout, plang->lanvalidator);
		if (validatorInfo != NULL && !validatorInfo->dobj.dump)
			validatorInfo = NULL;
	}

	/*
	 * If the functions are dumpable then emit a traditional CREATE LANGUAGE
	 * with parameters.  Otherwise, we'll write a parameterless command, which
	 * will rely on data from pg_pltemplate.
	 */
	useParams = (funcInfo != NULL &&
				 (inlineInfo != NULL || !OidIsValid(plang->laninline)) &&
				 (validatorInfo != NULL || !OidIsValid(plang->lanvalidator)));

	defqry = createStringInfo();
	delqry = createStringInfo();
	labelq = createStringInfo();

	qlanname = dbms_md_strdup(dbms_md_fmtId_internal(plang->dobj.name, true));

	/*
	 * If dumping a HANDLER clause, treat the language as being in the handler
	 * function's schema; this avoids cluttering the HANDLER clause. Otherwise
	 * it doesn't really have a schema.
	 */
	if (useParams)
		lanschema = funcInfo->dobj.namespace->dobj.name;
	else
		lanschema = NULL;

	appendStringInfo(delqry, "DROP PROCEDURAL LANGUAGE %s;\n",
					  qlanname);

	if (useParams)
	{
		appendStringInfo(defqry, "CREATE %sPROCEDURAL LANGUAGE %s",
						  plang->lanpltrusted ? "TRUSTED " : "",
						  qlanname);
		appendStringInfo(defqry, " HANDLER %s",
						  dbms_md_fmtId(funcInfo->dobj.name));
		if (OidIsValid(plang->laninline))
		{
			appendStringInfoString(defqry, " INLINE ");
			/* Cope with possibility that inline is in different schema */
			if (inlineInfo->dobj.namespace != funcInfo->dobj.namespace)
				appendStringInfo(defqry, "%s.",
								  dbms_md_fmtId(inlineInfo->dobj.namespace->dobj.name));
			appendStringInfoString(defqry, dbms_md_fmtId(inlineInfo->dobj.name));
		}
		if (OidIsValid(plang->lanvalidator))
		{
			appendStringInfoString(defqry, " VALIDATOR ");
			/* Cope with possibility that validator is in different schema */
			if (validatorInfo->dobj.namespace != funcInfo->dobj.namespace)
				appendStringInfo(defqry, "%s.",
								  dbms_md_fmtId(validatorInfo->dobj.namespace->dobj.name));
			appendStringInfoString(defqry, dbms_md_fmtId(validatorInfo->dobj.name));
		}
	}
	else
	{
		/*
		 * If not dumping parameters, then use CREATE OR REPLACE so that the
		 * command will not fail if the language is preinstalled in the target
		 * database.  We restrict the use of REPLACE to this case so as to
		 * eliminate the risk of replacing a language with incompatible
		 * parameter settings: this command will only succeed at all if there
		 * is a pg_pltemplate entry, and if there is one, the existing entry
		 * must match it too.
		 */
		appendStringInfo(defqry, "CREATE OR REPLACE PROCEDURAL LANGUAGE %s",
						  qlanname);
	}
	appendStringInfoString(defqry, ";\n");

	appendStringInfo(labelq, "LANGUAGE %s", qlanname);

#if 0
	if (dopt->binary_upgrade)
		binary_upgrade_extension_member(defqry, &plang->dobj, labelq->data);
#endif

	if (plang->dobj.dump & DUMP_COMPONENT_DEFINITION)
		ArchiveEntry(fout, plang->dobj.catId, plang->dobj.dumpId,
					 plang->dobj.name,
					 lanschema, NULL, plang->lanowner,
					 false, "PROCEDURAL LANGUAGE", SECTION_PRE_DATA,
					 defqry->data, delqry->data, NULL,
					 NULL, 0,
					 NULL, NULL);

	/* Dump Proc Lang Comments and Security Labels */
	if (plang->dobj.dump & DUMP_COMPONENT_COMMENT)
		dumpComment(fout, labelq->data,
					lanschema, plang->lanowner,
					plang->dobj.catId, 0, plang->dobj.dumpId);

	if (plang->dobj.dump & DUMP_COMPONENT_SECLABEL)
		dumpSecLabel(fout, labelq->data,
					 lanschema, plang->lanowner,
					 plang->dobj.catId, 0, plang->dobj.dumpId);

	if (plang->lanpltrusted && plang->dobj.dump & DUMP_COMPONENT_ACL)
		dumpACL(fout, plang->dobj.catId, plang->dobj.dumpId, "LANGUAGE",
				qlanname, NULL, plang->dobj.name,
				lanschema,
				plang->lanowner, plang->lanacl, plang->rlanacl,
				plang->initlanacl, plang->initrlanacl);

	free(qlanname);

	destroyStringInfo(defqry);	
	destroyStringInfo(delqry);
	destroyStringInfo(labelq);
	
	pfree(defqry);
	pfree(delqry);
	pfree(labelq);
}

/*
 * format_function_arguments: generate function name and argument list
 *
 * This is used when we can rely on pg_get_function_arguments to format
 * the argument list.  Note, however, that pg_get_function_arguments
 * does not special-case zero-argument aggregates.
 */
static char *
format_function_arguments(FuncInfo *finfo, char *funcargs, bool is_agg)
{
	StringInfoData fn;
	char *res = NULL;

	initStringInfo(&fn);
	appendStringInfoString(&fn, dbms_md_fmtId(finfo->dobj.name));
	if (is_agg && finfo->nargs == 0)
		appendStringInfoString(&fn, "(*)");
	else
		appendStringInfo(&fn, "(%s)", funcargs);

	res = dbms_md_strdup(fn.data);
	pfree(fn.data);
	return res;
}

/*
 * format_function_arguments_old: generate function name and argument list
 *
 * The argument type names are qualified if needed.  The function name
 * is never qualified.
 *
 * This is used only with pre-8.4 servers, so we aren't expecting to see
 * VARIADIC or TABLE arguments, nor are there any defaults for arguments.
 *
 * Any or all of allargtypes, argmodes, argnames may be NULL.
 */
static char *
format_function_arguments_old(Archive *fout,
							  FuncInfo *finfo, int nallargs,
							  char **allargtypes,
							  char **argmodes,
							  char **argnames)
{
	StringInfoData fn;
	int			j;
	char *res = NULL;

	initStringInfo(&fn);
	appendStringInfo(&fn, "%s(", dbms_md_fmtId(finfo->dobj.name));
	for (j = 0; j < nallargs; j++)
	{
		Oid			typid;
		char	   *typname;
		const char *argmode;
		const char *argname;

		typid = allargtypes ? dbms_md_str_to_oid(allargtypes[j]) : finfo->argtypes[j];
		typname = getFormattedTypeName(fout, typid, zeroAsOpaque);

		if (argmodes)
		{
			switch (argmodes[j][0])
			{
				case PROARGMODE_IN:
					argmode = "";
					break;
				case PROARGMODE_OUT:
					argmode = "OUT ";
					break;
				case PROARGMODE_INOUT:
					argmode = "INOUT ";
					break;
				default:
					write_msg(NULL, "WARNING: bogus value in proargmodes array\n");
					argmode = "";
					break;
			}
		}
		else
			argmode = "";

		argname = argnames ? argnames[j] : (char *) NULL;
		if (argname && argname[0] == '\0')
			argname = NULL;

		appendStringInfo(&fn, "%s%s%s%s%s",
						  (j > 0) ? ", " : "",
						  argmode,
						  argname ? dbms_md_fmtId(argname) : "",
						  argname ? " " : "",
						  typname);
		free(typname);
	}
	appendStringInfoChar(&fn, ')');

	res = dbms_md_strdup(fn.data);
	pfree(fn.data);
	
	return res;
}

/*
 * format_function_signature: generate function name and argument list
 *
 * This is like format_function_arguments_old except that only a minimal
 * list of input argument types is generated; this is sufficient to
 * reference the function, but not to define it.
 *
 * If honor_quotes is false then the function name is never quoted.
 * This is appropriate for use in TOC tags, but not in SQL commands.
 */
static char *
format_function_signature(Archive *fout, FuncInfo *finfo, bool honor_quotes)
{
	StringInfoData fn;
	int			j;
	char *res = NULL;

	initStringInfo(&fn);
	if (honor_quotes)
		appendStringInfo(&fn, "%s(", dbms_md_fmtId(finfo->dobj.name));
	else
		appendStringInfo(&fn, "%s(", finfo->dobj.name);
	for (j = 0; j < finfo->nargs; j++)
	{
		char	   *typname;

		if (j > 0)
			appendStringInfoString(&fn, ", ");

		typname = getFormattedTypeName(fout, finfo->argtypes[j],
									   zeroAsOpaque);
		appendStringInfoString(&fn, typname);
		free(typname);
	}
	appendStringInfoChar(&fn, ')');
	res = dbms_md_strdup(fn.data);
	pfree(fn.data);
	
	return res;
}


/*
 * dumpFunc:
 *	  dump out one function
 */
static void
dumpFunc(Archive *fout, FuncInfo *finfo)
{
	DumpOptions *dopt = fout->dopt;
	StringInfo query;
	StringInfo q;
	StringInfo delqry;
	StringInfo labelq;
	StringInfo asPart;
	SPITupleTable   *res;
	char	   *funcsig;		/* identity signature */
	char	   *funcfullsig = NULL; /* full signature */
	char	   *funcsig_tag;
	char	   *proretset;
	char	   *prosrc;
	char	   *probin;
	char	   *funcargs;
	char	   *funciargs;
	char	   *funcresult;
	char	   *proallargtypes;
	char	   *proargmodes;
	char	   *proargnames;
	char	   *protrftypes;
	char	   *prokind;
	char	   *provolatile;
	char	   *proisstrict;
	char	   *prosecdef;
	char	   *proleakproof;
	char	   *proconfig;
	char	   *procost;
	char	   *prorows;
	char	   *proparallel;
	char	   *lanname;
	char	   *rettypename;
	int			nallargs;
	char	  **allargtypes = NULL;
	char	  **argmodes = NULL;
	char	  **argnames = NULL;
	char	  **configitems = NULL;
	int			nconfigitems = 0;
	const char *keyword;
	int			i;

	/* Skip if not to be dumped */
	if (!finfo->dobj.dump || dopt->dataOnly)
		return;

	query = createStringInfo();
	q = createStringInfo();
	delqry = createStringInfo();
	labelq = createStringInfo();
	asPart = createStringInfo();

	/* Set proper schema search path so type references list correctly */
	selectSourceSchema(fout, finfo->dobj.namespace->dobj.name);

	/* Fetch function-specific details */
    if (fout->remoteVersion >= 100000)
    {
         /*
          * prokind was added in 11
          */
         appendStringInfo(query,
                           "SELECT proretset, prosrc, probin, "
                           "pg_catalog.pg_get_function_arguments(oid) AS funcargs, "
                           "pg_catalog.pg_get_function_identity_arguments(oid) AS funciargs, "
                           "pg_catalog.pg_get_function_result(oid) AS funcresult, "
                           "array_to_string(protrftypes, ' ') AS protrftypes, "
                           "prokind, provolatile, proisstrict, prosecdef, "
                           "proleakproof, proconfig, procost, prorows, "
                           "proparallel, "
                           "(SELECT lanname FROM pg_catalog.pg_language WHERE oid = prolang) AS lanname "
                           "FROM pg_catalog.pg_proc "
                           "WHERE oid = '%u'::pg_catalog.oid",
                           finfo->dobj.catId.oid);
    }
    else if (fout->remoteVersion >= 90600)
	{
		/*
		 * proparallel was added in 9.6
		 */
		appendStringInfo(query,
						  "SELECT proretset, prosrc, probin, "
						  "pg_catalog.pg_get_function_arguments(oid) AS funcargs, "
						  "pg_catalog.pg_get_function_identity_arguments(oid) AS funciargs, "
						  "pg_catalog.pg_get_function_result(oid) AS funcresult, "
						  "array_to_string(protrftypes, ' ') AS protrftypes, "
						  "CASE WHEN proiswindow THEN 'w' ELSE 'f' END AS prokind, "
						  "provolatile, proisstrict, prosecdef, "
						  "proleakproof, proconfig, procost, prorows, "
						  "proparallel, "
						  "(SELECT lanname FROM pg_catalog.pg_language WHERE oid = prolang) AS lanname "
						  "FROM pg_catalog.pg_proc "
						  "WHERE oid = '%u'::pg_catalog.oid",
						  finfo->dobj.catId.oid);
	}
	else if (fout->remoteVersion >= 90500)
	{
		/*
		 * protrftypes was added in 9.5
		 */
		appendStringInfo(query,
						  "SELECT proretset, prosrc, probin, "
						  "pg_catalog.pg_get_function_arguments(oid) AS funcargs, "
						  "pg_catalog.pg_get_function_identity_arguments(oid) AS funciargs, "
						  "pg_catalog.pg_get_function_result(oid) AS funcresult, "
						  "array_to_string(protrftypes, ' ') AS protrftypes, "
						  "CASE WHEN proiswindow THEN 'w' ELSE 'f' END AS prokind, "
						  "provolatile, proisstrict, prosecdef, "
						  "proleakproof, proconfig, procost, prorows, "
						  "(SELECT lanname FROM pg_catalog.pg_language WHERE oid = prolang) AS lanname "
						  "FROM pg_catalog.pg_proc "
						  "WHERE oid = '%u'::pg_catalog.oid",
						  finfo->dobj.catId.oid);
	}
	else if (fout->remoteVersion >= 90200)
	{
		/*
		 * proleakproof was added in 9.2
		 */
		appendStringInfo(query,
						  "SELECT proretset, prosrc, probin, "
						  "pg_catalog.pg_get_function_arguments(oid) AS funcargs, "
						  "pg_catalog.pg_get_function_identity_arguments(oid) AS funciargs, "
						  "pg_catalog.pg_get_function_result(oid) AS funcresult, "
						  "CASE WHEN proiswindow THEN 'w' ELSE 'f' END AS prokind, "
						  "provolatile, proisstrict, prosecdef, "
						  "proleakproof, proconfig, procost, prorows, "
						  "(SELECT lanname FROM pg_catalog.pg_language WHERE oid = prolang) AS lanname "
						  "FROM pg_catalog.pg_proc "
						  "WHERE oid = '%u'::pg_catalog.oid",
						  finfo->dobj.catId.oid);
	}
	else if (fout->remoteVersion >= 80400)
	{
		/*
		 * In 8.4 and up we rely on pg_get_function_arguments and
		 * pg_get_function_result instead of examining proallargtypes etc.
		 */
		appendStringInfo(query,
						  "SELECT proretset, prosrc, probin, "
						  "pg_catalog.pg_get_function_arguments(oid) AS funcargs, "
						  "pg_catalog.pg_get_function_identity_arguments(oid) AS funciargs, "
						  "pg_catalog.pg_get_function_result(oid) AS funcresult, "
						  "CASE WHEN proiswindow THEN 'w' ELSE 'f' END AS prokind, "
						  "provolatile, proisstrict, prosecdef, "
						  "false AS proleakproof, "
						  " proconfig, procost, prorows, "
						  "(SELECT lanname FROM pg_catalog.pg_language WHERE oid = prolang) AS lanname "
						  "FROM pg_catalog.pg_proc "
						  "WHERE oid = '%u'::pg_catalog.oid",
						  finfo->dobj.catId.oid);
	}
	else if (fout->remoteVersion >= 80300)
	{
		appendStringInfo(query,
						  "SELECT proretset, prosrc, probin, "
						  "proallargtypes, proargmodes, proargnames, "
						  "'f' AS prokind, "
						  "provolatile, proisstrict, prosecdef, "
						  "false AS proleakproof, "
						  "proconfig, procost, prorows, "
						  "(SELECT lanname FROM pg_catalog.pg_language WHERE oid = prolang) AS lanname "
						  "FROM pg_catalog.pg_proc "
						  "WHERE oid = '%u'::pg_catalog.oid",
						  finfo->dobj.catId.oid);
	}
	else if (fout->remoteVersion >= 80100)
	{
		appendStringInfo(query,
						  "SELECT proretset, prosrc, probin, "
						  "proallargtypes, proargmodes, proargnames, "
						  "'f' AS prokind, "
						  "provolatile, proisstrict, prosecdef, "
						  "false AS proleakproof, "
						  "null AS proconfig, 0 AS procost, 0 AS prorows, "
						  "(SELECT lanname FROM pg_catalog.pg_language WHERE oid = prolang) AS lanname "
						  "FROM pg_catalog.pg_proc "
						  "WHERE oid = '%u'::pg_catalog.oid",
						  finfo->dobj.catId.oid);
	}
	else
	{
		appendStringInfo(query,
						  "SELECT proretset, prosrc, probin, "
						  "null AS proallargtypes, "
						  "null AS proargmodes, "
						  "proargnames, "
						  "'f' AS prokind, "
						  "provolatile, proisstrict, prosecdef, "
						  "false AS proleakproof, "
						  "null AS proconfig, 0 AS procost, 0 AS prorows, "
						  "(SELECT lanname FROM pg_catalog.pg_language WHERE oid = prolang) AS lanname "
						  "FROM pg_catalog.pg_proc "
						  "WHERE oid = '%u'::pg_catalog.oid",
						  finfo->dobj.catId.oid);
	}

	res = ExecuteSqlQueryForSingleRow(fout, query->data);

	proretset = dbms_md_get_field_value(res, 0, dbms_md_get_field_subscript(res, "proretset"));
	prosrc = dbms_md_get_field_value(res, 0, dbms_md_get_field_subscript(res, "prosrc"));
	probin = dbms_md_get_field_value(res, 0, dbms_md_get_field_subscript(res, "probin"));
	if (fout->remoteVersion >= 80400)
	{
		funcargs = dbms_md_get_field_value(res, 0, dbms_md_get_field_subscript(res, "funcargs"));
		funciargs = dbms_md_get_field_value(res, 0, dbms_md_get_field_subscript(res, "funciargs"));
		funcresult = dbms_md_get_field_value(res, 0, dbms_md_get_field_subscript(res, "funcresult"));
		proallargtypes = proargmodes = proargnames = NULL;
	}
	else
	{
		proallargtypes = dbms_md_get_field_value(res, 0, dbms_md_get_field_subscript(res, "proallargtypes"));
		proargmodes = dbms_md_get_field_value(res, 0, dbms_md_get_field_subscript(res, "proargmodes"));
		proargnames = dbms_md_get_field_value(res, 0, dbms_md_get_field_subscript(res, "proargnames"));
		funcargs = funciargs = funcresult = NULL;
	}

	if (dbms_md_get_field_subscript(res, "protrftypes") != -1)
		protrftypes = dbms_md_get_field_value(res, 0, dbms_md_get_field_subscript(res, "protrftypes"));
	else
		protrftypes = NULL;
	prokind = dbms_md_get_field_value(res, 0, dbms_md_get_field_subscript(res, "prokind"));
	provolatile = dbms_md_get_field_value(res, 0, dbms_md_get_field_subscript(res, "provolatile"));
	proisstrict = dbms_md_get_field_value(res, 0, dbms_md_get_field_subscript(res, "proisstrict"));
	prosecdef = dbms_md_get_field_value(res, 0, dbms_md_get_field_subscript(res, "prosecdef"));
	proleakproof = dbms_md_get_field_value(res, 0, dbms_md_get_field_subscript(res, "proleakproof"));
	proconfig = dbms_md_get_field_value(res, 0, dbms_md_get_field_subscript(res, "proconfig"));
	procost = dbms_md_get_field_value(res, 0, dbms_md_get_field_subscript(res, "procost"));
	prorows = dbms_md_get_field_value(res, 0, dbms_md_get_field_subscript(res, "prorows"));

	if (dbms_md_get_field_subscript(res, "proparallel") != -1)
		proparallel = dbms_md_get_field_value(res, 0, dbms_md_get_field_subscript(res, "proparallel"));
	else
		proparallel = NULL;

	lanname = dbms_md_get_field_value(res, 0, dbms_md_get_field_subscript(res, "lanname"));

	/*
	 * See backend/commands/functioncmds.c for details of how the 'AS' clause
	 * is used.  In 8.4 and up, an unused probin is NULL (here ""); previous
	 * versions would set it to "-".  There are no known cases in which prosrc
	 * is unused, so the tests below for "-" are probably useless.
	 */
	if (NULL != probin && probin[0] != '\0' && strcmp(probin, "-") != 0)
	{
		appendStringInfoString(asPart, "AS ");
		appendStringLiteralAH(asPart, probin, fout);
		if (strcmp(prosrc, "-") != 0)
		{
			appendStringInfoString(asPart, ", ");

			/*
			 * where we have bin, use dollar quoting if allowed and src
			 * contains quote or backslash; else use regular quoting.
			 */
			if (dopt->disable_dollar_quoting ||
				(strchr(prosrc, '\'') == NULL && strchr(prosrc, '\\') == NULL))
				appendStringLiteralAH(asPart, prosrc, fout);
			else
				dbms_md_append_string_literal_dq(asPart, prosrc, NULL);
		}
	}
	else
	{
		if (NULL!= prosrc)
		{
			if (strcmp(prosrc, "-") != 0)
			{
				appendStringInfoString(asPart, "AS ");
				/* with no bin, dollar quote src unconditionally if allowed */
				if (dopt->disable_dollar_quoting)
					appendStringLiteralAH(asPart, prosrc, fout);
				else
					dbms_md_append_string_literal_dq(asPart, prosrc, NULL);
			}
		}
	}

	nallargs = finfo->nargs;	/* unless we learn different from allargs */

	if (proallargtypes && *proallargtypes)
	{
		int			nitems = 0;

		if (!dbms_md_parse_array(proallargtypes, &allargtypes, &nitems) ||
			nitems < finfo->nargs)
		{
			write_msg(NULL, "WARNING: could not parse proallargtypes array\n");
			if (allargtypes)
				pfree(allargtypes);
			allargtypes = NULL;
		}
		else
			nallargs = nitems;
	}

	if (proargmodes && *proargmodes)
	{
		int			nitems = 0;

		if (!dbms_md_parse_array(proargmodes, &argmodes, &nitems) ||
			nitems != nallargs)
		{
			write_msg(NULL, "WARNING: could not parse proargmodes array\n");
			if (argmodes)
				pfree(argmodes);
			argmodes = NULL;
		}
	}

	if (proargnames && *proargnames)
	{
		int			nitems = 0;

		if (!dbms_md_parse_array(proargnames, &argnames, &nitems) ||
			nitems != nallargs)
		{
			write_msg(NULL, "WARNING: could not parse proargnames array\n");
			if (argnames)
				pfree(argnames);
			argnames = NULL;
		}
	}

	if (proconfig && *proconfig)
	{
		if (!dbms_md_parse_array(proconfig, &configitems, &nconfigitems))
		{
			write_msg(NULL, "WARNING: could not parse proconfig array\n");
			if (configitems)
				pfree(configitems);
			configitems = NULL;
			nconfigitems = 0;
		}
	}

	if (funcargs)
	{
		/* 8.4 or later; we rely on server-side code for most of the work */
		funcfullsig = format_function_arguments(finfo, funcargs, false);
		funcsig = format_function_arguments(finfo, funciargs, false);
	}
	else
		/* pre-8.4, do it ourselves */
		funcsig = format_function_arguments_old(fout,
												finfo, nallargs, allargtypes,
												argmodes, argnames);

	funcsig_tag = format_function_signature(fout, finfo, false);

	if (prokind[0] == PROKIND_PROCEDURE)
       		keyword = "PROCEDURE";
	else
		keyword = "FUNCTION";   /* works for window functions too */

	/*
	 * DROP must be fully qualified in case same name appears in pg_catalog
	 */
	appendStringInfo(delqry, "DROP %s %s.%s;\n",
					  keyword,
					  dbms_md_fmtId(finfo->dobj.namespace->dobj.name),
					  funcsig);

	appendStringInfo(q, "CREATE %s %s",
					  keyword,
					  funcfullsig ? funcfullsig :
					  funcsig);

    if (prokind[0] == PROKIND_PROCEDURE)
        /* no result type to output */ ;
	else if (funcresult)
		appendStringInfo(q, " RETURNS %s", funcresult);
	else
	{
		rettypename = getFormattedTypeName(fout, finfo->prorettype,
										   zeroAsOpaque);
		appendStringInfo(q, " RETURNS %s%s",
						  (proretset[0] == 't') ? "SETOF " : "",
						  rettypename);
		free(rettypename);
	}

	appendStringInfo(q, "\n    LANGUAGE %s", dbms_md_fmtId_internal(lanname, true));

	if (protrftypes != NULL && strcmp(protrftypes, "") != 0)
	{
		Oid		   *typeids = palloc(FUNC_MAX_ARGS * sizeof(Oid));
		int			i;

		appendStringInfoString(q, " TRANSFORM ");
		parseOidArray(protrftypes, typeids, FUNC_MAX_ARGS);
		for (i = 0; typeids[i]; i++)
		{
			if (i != 0)
				appendStringInfoString(q, ", ");
			appendStringInfo(q, "FOR TYPE %s",
							  getFormattedTypeName(fout, typeids[i], zeroAsNone));
		}
	}

	if (prokind[0] == PROKIND_WINDOW)
		appendStringInfoString(q, " WINDOW");

	if (provolatile[0] != PROVOLATILE_VOLATILE)
	{
		if (provolatile[0] == PROVOLATILE_IMMUTABLE)
			appendStringInfoString(q, " IMMUTABLE");
		else if (provolatile[0] == PROVOLATILE_STABLE)
			appendStringInfoString(q, " STABLE");
		else if (provolatile[0] == PROVOLATILE_CACHABLE)
			appendStringInfoString(q, " RESULT_CACHE");
		else if (provolatile[0] != PROVOLATILE_VOLATILE)
			elog(ERROR, "unrecognized provolatile value for function \"%s\"\n",
						  finfo->dobj.name);
	}

	if (proisstrict[0] == 't')
		appendStringInfoString(q, " STRICT");

	if (prosecdef[0] == 't')
		appendStringInfoString(q, " SECURITY DEFINER");

	if (proleakproof[0] == 't')
		appendStringInfoString(q, " LEAKPROOF");

	/*
	 * COST and ROWS are emitted only if present and not default, so as not to
	 * break backwards-compatibility of the dump without need.  Keep this code
	 * in sync with the defaults in functioncmds.c.
	 */
    if(procost[0] == '-')
    {
        char* temp;
        int   len;

        appendStringInfoString(q, " PUSHDOWN");
        len = strlen(procost);
        temp = dbms_md_palloc(len + 1);
        strcpy(temp, procost+1);
        temp[len-1] = '\0';
        strcpy(procost, temp);
        procost[len-1] = 0;
        dbms_md_free(temp);
        temp = NULL;
    }
    
	if (strcmp(procost, "0") != 0)
	{
		if (strcmp(lanname, "internal") == 0 || strcmp(lanname, "c") == 0)
		{
			/* default cost is 1 */
			if (strcmp(procost, "1") != 0)
				appendStringInfo(q, " COST %s", procost);
		}
		else
		{
			/* default cost is 100 */
			if (strcmp(procost, "100") != 0)
				appendStringInfo(q, " COST %s", procost);
		}
	}
	if (proretset[0] == 't' &&
		strcmp(prorows, "0") != 0 && strcmp(prorows, "1000") != 0)
		appendStringInfo(q, " ROWS %s", prorows);

	if (proparallel != NULL && proparallel[0] != PROPARALLEL_UNSAFE)
	{
		if (proparallel[0] == PROPARALLEL_SAFE)
			appendStringInfoString(q, " PARALLEL SAFE");
		else if (proparallel[0] == PROPARALLEL_RESTRICTED)
			appendStringInfoString(q, " PARALLEL RESTRICTED");
		else if (proparallel[0] != PROPARALLEL_UNSAFE)
			elog(ERROR, "unrecognized proparallel value for function \"%s\"\n",
						  finfo->dobj.name);
	}

	for (i = 0; i < nconfigitems; i++)
	{
		/* we feel free to scribble on configitems[] here */
		char	   *configitem = configitems[i];
		char	   *pos;

		pos = strchr(configitem, '=');
		if (pos == NULL)
			continue;
		*pos++ = '\0';
		appendStringInfo(q, "\n    SET %s TO ", dbms_md_fmtId_internal(configitem, true));

		/*
		 * Some GUC variable names are 'LIST' type and hence must not be
		 * quoted.
		 */
		if (pg_strcasecmp(configitem, "DateStyle") == 0
			|| pg_strcasecmp(configitem, "search_path") == 0)
			appendStringInfoString(q, pos);
		else
			appendStringLiteralAH(q, pos, fout);
	}

	appendStringInfo(q, "\n    %s;\n", asPart->data);

	appendStringInfo(labelq, "%s %s", keyword, funcsig);

#if 0
	if (dopt->binary_upgrade)
		binary_upgrade_extension_member(q, &finfo->dobj, labelq->data);
#endif

	if (finfo->dobj.dump & DUMP_COMPONENT_DEFINITION)
		ArchiveEntry(fout, finfo->dobj.catId, finfo->dobj.dumpId,
					 funcsig_tag,
					 finfo->dobj.namespace->dobj.name,
					 NULL,
					 finfo->rolname, false,
					 keyword, SECTION_PRE_DATA,
					 q->data, delqry->data, NULL,
					 NULL, 0,
					 NULL, NULL);

	/* Dump Function Comments and Security Labels */
	if (finfo->dobj.dump & DUMP_COMPONENT_COMMENT)
		dumpComment(fout, labelq->data,
					finfo->dobj.namespace->dobj.name, finfo->rolname,
					finfo->dobj.catId, 0, finfo->dobj.dumpId);

	if (finfo->dobj.dump & DUMP_COMPONENT_SECLABEL)
		dumpSecLabel(fout, labelq->data,
					 finfo->dobj.namespace->dobj.name, finfo->rolname,
					 finfo->dobj.catId, 0, finfo->dobj.dumpId);

	if (finfo->dobj.dump & DUMP_COMPONENT_ACL)
		dumpACL(fout, finfo->dobj.catId, finfo->dobj.dumpId, keyword,
				funcsig, NULL, funcsig_tag,
				finfo->dobj.namespace->dobj.name,
				finfo->rolname, finfo->proacl, finfo->rproacl,
				finfo->initproacl, finfo->initrproacl);

	dbms_md_free_tuples(res);

	destroyStringInfo(query);
	destroyStringInfo(q);
	destroyStringInfo(delqry);
	destroyStringInfo(labelq);
	destroyStringInfo(asPart);

	pfree(query);
	pfree(q);
	pfree(delqry);
	pfree(labelq);
	pfree(asPart);
	
	free(funcsig);
	if (funcfullsig)
		free(funcfullsig);
	free(funcsig_tag);
	if (allargtypes)
		pfree(allargtypes);
	if (argmodes)
		pfree(argmodes);
	if (argnames)
		pfree(argnames);
	if (configitems)
		pfree(configitems);
}


/*
 * Dump a user-defined cast
 */
static void
dumpCast(Archive *fout, CastInfo *cast)
{
	DumpOptions *dopt = fout->dopt;
	StringInfo defqry;
	StringInfo delqry;
	StringInfo labelq;
	FuncInfo   *funcInfo = NULL;
	char	   *sourceType;
	char	   *targetType;

	/* Skip if not to be dumped */
	if (!cast->dobj.dump || dopt->dataOnly)
		return;

	/* Cannot dump if we don't have the cast function's info */
	if (OidIsValid(cast->castfunc))
	{
		funcInfo = findFuncByOid(fout, cast->castfunc);
		if (funcInfo == NULL)
			elog(ERROR, "could not find function definition for function with OID %u\n",
						  cast->castfunc);
	}

	/*
	 * Make sure we are in proper schema (needed for getFormattedTypeName).
	 * Casts don't have a schema of their own, so use pg_catalog.
	 */
	selectSourceSchema(fout, "pg_catalog");

	defqry = createStringInfo();
	delqry = createStringInfo();
	labelq = createStringInfo();

	sourceType = getFormattedTypeName(fout, cast->castsource, zeroAsNone);
	targetType = getFormattedTypeName(fout, cast->casttarget, zeroAsNone);
	appendStringInfo(delqry, "DROP CAST (%s AS %s);\n",
					  sourceType, targetType);

	appendStringInfo(defqry, "CREATE CAST (%s AS %s) ",
					  sourceType, targetType);

	switch (cast->castmethod)
	{
		case COERCION_METHOD_BINARY:
			appendStringInfoString(defqry, "WITHOUT FUNCTION");
			break;
		case COERCION_METHOD_INOUT:
			appendStringInfoString(defqry, "WITH INOUT");
			break;
		case COERCION_METHOD_FUNCTION:
			if (funcInfo)
			{
				char	   *fsig = format_function_signature(fout, funcInfo, true);

				/*
				 * Always qualify the function name, in case it is not in
				 * pg_catalog schema (format_function_signature won't qualify
				 * it).
				 */
				appendStringInfo(defqry, "WITH FUNCTION %s.%s",
								  dbms_md_fmtId(funcInfo->dobj.namespace->dobj.name), fsig);
				free(fsig);
			}
			else
				write_msg(NULL, "WARNING: bogus value in pg_cast.castfunc or pg_cast.castmethod field\n");
			break;
		default:
			write_msg(NULL, "WARNING: bogus value in pg_cast.castmethod field\n");
	}

	if (cast->castcontext == 'a')
		appendStringInfoString(defqry, " AS ASSIGNMENT");
	else if (cast->castcontext == 'i')
		appendStringInfoString(defqry, " AS IMPLICIT");
	appendStringInfoString(defqry, ";\n");

	appendStringInfo(labelq, "CAST (%s AS %s)",
					  sourceType, targetType);

#if 0
	if (dopt->binary_upgrade)
		binary_upgrade_extension_member(defqry, &cast->dobj, labelq->data);
#endif

	if (cast->dobj.dump & DUMP_COMPONENT_DEFINITION)
		ArchiveEntry(fout, cast->dobj.catId, cast->dobj.dumpId,
					 labelq->data,
					 "pg_catalog", NULL, "",
					 false, "CAST", SECTION_PRE_DATA,
					 defqry->data, delqry->data, NULL,
					 NULL, 0,
					 NULL, NULL);

	/* Dump Cast Comments */
	if (cast->dobj.dump & DUMP_COMPONENT_COMMENT)
		dumpComment(fout, labelq->data,
					"pg_catalog", "",
					cast->dobj.catId, 0, cast->dobj.dumpId);

	free(sourceType);
	free(targetType);

	destroyStringInfo(defqry);
	destroyStringInfo(delqry);
	destroyStringInfo(labelq);

	pfree(defqry);
	pfree(delqry);
	pfree(labelq);
}

/*
 * Dump a transform
 */
static void
dumpTransform(Archive *fout, TransformInfo *transform)
{
	DumpOptions *dopt = fout->dopt;
	StringInfo defqry;
	StringInfo delqry;
	StringInfo labelq;
	FuncInfo   *fromsqlFuncInfo = NULL;
	FuncInfo   *tosqlFuncInfo = NULL;
	char	   *lanname;
	char	   *transformType;

	/* Skip if not to be dumped */
	if (!transform->dobj.dump || dopt->dataOnly)
		return;

	/* Cannot dump if we don't have the transform functions' info */
	if (OidIsValid(transform->trffromsql))
	{
		fromsqlFuncInfo = findFuncByOid(fout, transform->trffromsql);
		if (fromsqlFuncInfo == NULL)
			elog(ERROR, "could not find function definition for function with OID %u\n",
						  transform->trffromsql);
	}
	if (OidIsValid(transform->trftosql))
	{
		tosqlFuncInfo = findFuncByOid(fout, transform->trftosql);
		if (tosqlFuncInfo == NULL)
			elog(ERROR, "could not find function definition for function with OID %u\n",
						  transform->trftosql);
	}

	/* Make sure we are in proper schema (needed for getFormattedTypeName) */
	selectSourceSchema(fout, "pg_catalog");

	defqry = createStringInfo();
	delqry = createStringInfo();
	labelq = createStringInfo();

	lanname = get_language_name(fout, transform->trflang);
	transformType = getFormattedTypeName(fout, transform->trftype, zeroAsNone);

	appendStringInfo(delqry, "DROP TRANSFORM FOR %s LANGUAGE %s;\n",
					  transformType, lanname);

	appendStringInfo(defqry, "CREATE TRANSFORM FOR %s LANGUAGE %s (",
					  transformType, lanname);

	if (!transform->trffromsql && !transform->trftosql)
		write_msg(NULL, "WARNING: bogus transform definition, at least one of trffromsql and trftosql should be nonzero\n");

	if (transform->trffromsql)
	{
		if (fromsqlFuncInfo)
		{
			char	   *fsig = format_function_signature(fout, fromsqlFuncInfo, true);

			/*
			 * Always qualify the function name, in case it is not in
			 * pg_catalog schema (format_function_signature won't qualify it).
			 */
			appendStringInfo(defqry, "FROM SQL WITH FUNCTION %s.%s",
							  dbms_md_fmtId(fromsqlFuncInfo->dobj.namespace->dobj.name), fsig);
			free(fsig);
		}
		else
			write_msg(NULL, "WARNING: bogus value in pg_transform.trffromsql field\n");
	}

	if (transform->trftosql)
	{
		if (transform->trffromsql)
			appendStringInfo(defqry, ", ");

		if (tosqlFuncInfo)
		{
			char	   *fsig = format_function_signature(fout, tosqlFuncInfo, true);

			/*
			 * Always qualify the function name, in case it is not in
			 * pg_catalog schema (format_function_signature won't qualify it).
			 */
			appendStringInfo(defqry, "TO SQL WITH FUNCTION %s.%s",
							  dbms_md_fmtId(tosqlFuncInfo->dobj.namespace->dobj.name), fsig);
			free(fsig);
		}
		else
			write_msg(NULL, "WARNING: bogus value in pg_transform.trftosql field\n");
	}

	appendStringInfo(defqry, ");\n");

	appendStringInfo(labelq, "TRANSFORM FOR %s LANGUAGE %s",
					  transformType, lanname);

#if 0
	if (dopt->binary_upgrade)
		binary_upgrade_extension_member(defqry, &transform->dobj, labelq->data);
#endif

	if (transform->dobj.dump & DUMP_COMPONENT_DEFINITION)
		ArchiveEntry(fout, transform->dobj.catId, transform->dobj.dumpId,
					 labelq->data,
					 "pg_catalog", NULL, "",
					 false, "TRANSFORM", SECTION_PRE_DATA,
					 defqry->data, delqry->data, NULL,
					 transform->dobj.dependencies, transform->dobj.nDeps,
					 NULL, NULL);

	/* Dump Transform Comments */
	if (transform->dobj.dump & DUMP_COMPONENT_COMMENT)
		dumpComment(fout, labelq->data,
					"pg_catalog", "",
					transform->dobj.catId, 0, transform->dobj.dumpId);

	free(lanname);
	free(transformType);
	destroyStringInfo(defqry);
	destroyStringInfo(delqry);
	destroyStringInfo(labelq);

	pfree(defqry);
	pfree(delqry);
	pfree(labelq);
}


/*
 * dumpOpr
 *	  write out a single operator definition
 */
static void
dumpOpr(Archive *fout, OprInfo *oprinfo)
{
	DumpOptions *dopt = fout->dopt;
	StringInfo query;
	StringInfo q;
	StringInfo delq;
	StringInfo labelq;
	StringInfo oprid;
	StringInfo details;
	SPITupleTable   *res;
	int			i_oprkind;
	int			i_oprcode;
	int			i_oprleft;
	int			i_oprright;
	int			i_oprcom;
	int			i_oprnegate;
	int			i_oprrest;
	int			i_oprjoin;
	int			i_oprcanmerge;
	int			i_oprcanhash;
	char	   *oprkind;
	char	   *oprcode;
	char	   *oprleft;
	char	   *oprright;
	char	   *oprcom;
	char	   *oprnegate;
	char	   *oprrest;
	char	   *oprjoin;
	char	   *oprcanmerge;
	char	   *oprcanhash;
	char	   *oprregproc;
	char	   *oprref;

	/* Skip if not to be dumped */
	if (!oprinfo->dobj.dump || dopt->dataOnly)
		return;

	/*
	 * some operators are invalid because they were the result of user
	 * defining operators before commutators exist
	 */
	if (!OidIsValid(oprinfo->oprcode))
		return;

	query = createStringInfo();
	q = createStringInfo();
	delq = createStringInfo();
	labelq = createStringInfo();
	oprid = createStringInfo();
	details = createStringInfo();

	/* Make sure we are in proper schema so regoperator works correctly */
	selectSourceSchema(fout, oprinfo->dobj.namespace->dobj.name);

	if (fout->remoteVersion >= 80300)
	{
		appendStringInfo(query, "SELECT oprkind, "
						  "oprcode::pg_catalog.regprocedure, "
						  "oprleft::pg_catalog.regtype, "
						  "oprright::pg_catalog.regtype, "
						  "oprcom::pg_catalog.regoperator, "
						  "oprnegate::pg_catalog.regoperator, "
						  "oprrest::pg_catalog.regprocedure, "
						  "oprjoin::pg_catalog.regprocedure, "
						  "oprcanmerge, oprcanhash "
						  "FROM pg_catalog.pg_operator "
						  "WHERE oid = '%u'::pg_catalog.oid",
						  oprinfo->dobj.catId.oid);
	}
	else
	{
		appendStringInfo(query, "SELECT oprkind, "
						  "oprcode::pg_catalog.regprocedure, "
						  "oprleft::pg_catalog.regtype, "
						  "oprright::pg_catalog.regtype, "
						  "oprcom::pg_catalog.regoperator, "
						  "oprnegate::pg_catalog.regoperator, "
						  "oprrest::pg_catalog.regprocedure, "
						  "oprjoin::pg_catalog.regprocedure, "
						  "(oprlsortop != 0) AS oprcanmerge, "
						  "oprcanhash "
						  "FROM pg_catalog.pg_operator "
						  "WHERE oid = '%u'::pg_catalog.oid",
						  oprinfo->dobj.catId.oid);
	}

	res = ExecuteSqlQueryForSingleRow(fout, query->data);

	i_oprkind = dbms_md_get_field_subscript(res, "oprkind");
	i_oprcode = dbms_md_get_field_subscript(res, "oprcode");
	i_oprleft = dbms_md_get_field_subscript(res, "oprleft");
	i_oprright = dbms_md_get_field_subscript(res, "oprright");
	i_oprcom = dbms_md_get_field_subscript(res, "oprcom");
	i_oprnegate = dbms_md_get_field_subscript(res, "oprnegate");
	i_oprrest = dbms_md_get_field_subscript(res, "oprrest");
	i_oprjoin = dbms_md_get_field_subscript(res, "oprjoin");
	i_oprcanmerge = dbms_md_get_field_subscript(res, "oprcanmerge");
	i_oprcanhash = dbms_md_get_field_subscript(res, "oprcanhash");

	oprkind = dbms_md_get_field_value(res, 0, i_oprkind);
	oprcode = dbms_md_get_field_value(res, 0, i_oprcode);
	oprleft = dbms_md_get_field_value(res, 0, i_oprleft);
	oprright = dbms_md_get_field_value(res, 0, i_oprright);
	oprcom = dbms_md_get_field_value(res, 0, i_oprcom);
	oprnegate = dbms_md_get_field_value(res, 0, i_oprnegate);
	oprrest = dbms_md_get_field_value(res, 0, i_oprrest);
	oprjoin = dbms_md_get_field_value(res, 0, i_oprjoin);
	oprcanmerge = dbms_md_get_field_value(res, 0, i_oprcanmerge);
	oprcanhash = dbms_md_get_field_value(res, 0, i_oprcanhash);

	oprregproc = convertRegProcReference(fout, oprcode);
	if (oprregproc)
	{
		appendStringInfo(details, "    FUNCTION = %s", oprregproc);
		free(oprregproc);
	}

	appendStringInfo(oprid, "%s (",
					  oprinfo->dobj.name);

	/*
	 * right unary means there's a left arg and left unary means there's a
	 * right arg
	 */
	if (strcmp(oprkind, "r") == 0 ||
		strcmp(oprkind, "b") == 0)
	{
		appendStringInfo(details, ",\n    LEFTARG = %s", oprleft);
		appendStringInfoString(oprid, oprleft);
	}
	else
		appendStringInfoString(oprid, "NONE");

	if (strcmp(oprkind, "l") == 0 ||
		strcmp(oprkind, "b") == 0)
	{
		appendStringInfo(details, ",\n    RIGHTARG = %s", oprright);
		appendStringInfo(oprid, ", %s)", oprright);
	}
	else
		appendStringInfoString(oprid, ", NONE)");

	oprref = convertOperatorReference(fout, oprcom);
	if (oprref)
	{
		appendStringInfo(details, ",\n    COMMUTATOR = %s", oprref);
		free(oprref);
	}

	oprref = convertOperatorReference(fout, oprnegate);
	if (oprref)
	{
		appendStringInfo(details, ",\n    NEGATOR = %s", oprref);
		free(oprref);
	}

	if (strcmp(oprcanmerge, "t") == 0)
		appendStringInfoString(details, ",\n    MERGES");

	if (strcmp(oprcanhash, "t") == 0)
		appendStringInfoString(details, ",\n    HASHES");

	oprregproc = convertRegProcReference(fout, oprrest);
	if (oprregproc)
	{
		appendStringInfo(details, ",\n    RESTRICT = %s", oprregproc);
		free(oprregproc);
	}

	oprregproc = convertRegProcReference(fout, oprjoin);
	if (oprregproc)
	{
		appendStringInfo(details, ",\n    JOIN = %s", oprregproc);
		free(oprregproc);
	}

	/*
	 * DROP must be fully qualified in case same name appears in pg_catalog
	 */
	appendStringInfo(delq, "DROP OPERATOR %s.%s;\n",
					  dbms_md_fmtId(oprinfo->dobj.namespace->dobj.name),
					  oprid->data);

	appendStringInfo(q, "CREATE OPERATOR %s (\n%s\n);\n",
					  oprinfo->dobj.name, details->data);

	appendStringInfo(labelq, "OPERATOR %s", oprid->data);

#if 0
	if (dopt->binary_upgrade)
		binary_upgrade_extension_member(q, &oprinfo->dobj, labelq->data);
#endif

	if (oprinfo->dobj.dump & DUMP_COMPONENT_DEFINITION)
		ArchiveEntry(fout, oprinfo->dobj.catId, oprinfo->dobj.dumpId,
					 oprinfo->dobj.name,
					 oprinfo->dobj.namespace->dobj.name,
					 NULL,
					 oprinfo->rolname,
					 false, "OPERATOR", SECTION_PRE_DATA,
					 q->data, delq->data, NULL,
					 NULL, 0,
					 NULL, NULL);

	/* Dump Operator Comments */
	if (oprinfo->dobj.dump & DUMP_COMPONENT_COMMENT)
		dumpComment(fout, labelq->data,
					oprinfo->dobj.namespace->dobj.name, oprinfo->rolname,
					oprinfo->dobj.catId, 0, oprinfo->dobj.dumpId);

	dbms_md_free_tuples(res);

	destroyStringInfo(query);
	destroyStringInfo(q);
	destroyStringInfo(delq);
	destroyStringInfo(labelq);
	destroyStringInfo(oprid);
	destroyStringInfo(details);

	pfree(query);
	pfree(q);
	pfree(delq);
	pfree(labelq);
	pfree(oprid);
	pfree(details);
}

/*
 * Convert a function reference obtained from pg_operator
 *
 * Returns allocated string of what to print, or NULL if function references
 * is InvalidOid. Returned string is expected to be free'd by the caller.
 *
 * The input is a REGPROCEDURE display; we have to strip the argument-types
 * part.
 */
static char *
convertRegProcReference(Archive *fout, const char *proc)
{
	char	   *name;
	char	   *paren;
	bool		inquote;

	/* In all cases "-" means a null reference */
	if (strcmp(proc, "-") == 0)
		return NULL;

	name = dbms_md_strdup(proc);
	/* find non-double-quoted left paren */
	inquote = false;
	for (paren = name; *paren; paren++)
	{
		if (*paren == '(' && !inquote)
		{
			*paren = '\0';
			break;
		}
		if (*paren == '"')
			inquote = !inquote;
	}
	return name;
}

/*
 * Convert an operator cross-reference obtained from pg_operator
 *
 * Returns an allocated string of what to print, or NULL to print nothing.
 * Caller is responsible for free'ing result string.
 *
 * The input is a REGOPERATOR display; we have to strip the argument-types
 * part, and add OPERATOR() decoration if the name is schema-qualified.
 */
static char *
convertOperatorReference(Archive *fout, const char *opr)
{
	char	   *name;
	char	   *oname;
	char	   *ptr;
	bool		inquote;
	bool		sawdot;

	/* In all cases "0" means a null reference */
	if (strcmp(opr, "0") == 0)
		return NULL;

	name = dbms_md_strdup(opr);
	/* find non-double-quoted left paren, and check for non-quoted dot */
	inquote = false;
	sawdot = false;
	for (ptr = name; *ptr; ptr++)
	{
		if (*ptr == '"')
			inquote = !inquote;
		else if (*ptr == '.' && !inquote)
			sawdot = true;
		else if (*ptr == '(' && !inquote)
		{
			*ptr = '\0';
			break;
		}
	}
	/* If not schema-qualified, don't need to add OPERATOR() */
	if (!sawdot)
		return name;
	oname = psprintf("OPERATOR(%s)", name);
	free(name);
	return oname;
}

/*
 * Convert a function OID obtained from pg_ts_parser or pg_ts_template
 *
 * It is sufficient to use REGPROC rather than REGPROCEDURE, since the
 * argument lists of these functions are predetermined.  Note that the
 * caller should ensure we are in the proper schema, because the results
 * are search path dependent!
 */
static char *
convertTSFunction(Archive *fout, Oid funcOid)
{
	char	   *result;
	char		query[128];
	SPITupleTable   *res;

	snprintf(query, sizeof(query),
			 "SELECT '%u'::pg_catalog.regproc", funcOid);
	res = ExecuteSqlQueryForSingleRow(fout, query);

	result = dbms_md_strdup(dbms_md_get_field_value(res, 0, SPI_RES_FIRST_FIELD_NO));

	dbms_md_free_tuples(res);

	return result;
}

/*
 * dumpAccessMethod
 *	  write out a single access method definition
 */
static void
dumpAccessMethod(Archive *fout, AccessMethodInfo *aminfo)
{
	DumpOptions *dopt = fout->dopt;
	StringInfo q;
	StringInfo delq;
	StringInfo labelq;
	char	   *qamname;

	/* Skip if not to be dumped */
	if (!aminfo->dobj.dump || dopt->dataOnly)
		return;

	q = createStringInfo();
	delq = createStringInfo();
	labelq = createStringInfo();

	qamname = dbms_md_strdup(dbms_md_fmtId_internal(aminfo->dobj.name, true));

	appendStringInfo(q, "CREATE ACCESS METHOD %s ", qamname);

	switch (aminfo->amtype)
	{
		case AMTYPE_INDEX:
			appendStringInfo(q, "TYPE INDEX ");
			break;
		default:
			write_msg(NULL, "WARNING: invalid type \"%c\" of access method \"%s\"\n",
					  aminfo->amtype, qamname);
			dbms_md_free(qamname);
			destroyStringInfo(q);
			destroyStringInfo(delq);
			destroyStringInfo(labelq);
			pfree(q);
			pfree(delq);
			pfree(labelq);
			return;
	}

	appendStringInfo(q, "HANDLER %s;\n", aminfo->amhandler);

	appendStringInfo(delq, "DROP ACCESS METHOD %s;\n",
					  qamname);

	appendStringInfo(labelq, "ACCESS METHOD %s",
					  qamname);

#if 0
	if (dopt->binary_upgrade)
		binary_upgrade_extension_member(q, &aminfo->dobj, labelq->data);
#endif

	if (aminfo->dobj.dump & DUMP_COMPONENT_DEFINITION)
		ArchiveEntry(fout, aminfo->dobj.catId, aminfo->dobj.dumpId,
					 aminfo->dobj.name,
					 NULL,
					 NULL,
					 "",
					 false, "ACCESS METHOD", SECTION_PRE_DATA,
					 q->data, delq->data, NULL,
					 NULL, 0,
					 NULL, NULL);

	/* Dump Access Method Comments */
	if (aminfo->dobj.dump & DUMP_COMPONENT_COMMENT)
		dumpComment(fout, labelq->data,
					NULL, "",
					aminfo->dobj.catId, 0, aminfo->dobj.dumpId);

	dbms_md_free(qamname);

	destroyStringInfo(q);
	destroyStringInfo(delq);
	destroyStringInfo(labelq);

	pfree(q);
	pfree(delq);
	pfree(labelq);
}

/*
 * dumpOpclass
 *	  write out a single operator class definition
 */
static void
dumpOpclass(Archive *fout, OpclassInfo *opcinfo)
{
	DumpOptions *dopt = fout->dopt;
	StringInfo query;
	StringInfo q;
	StringInfo delq;
	StringInfo labelq;
	SPITupleTable   *res;
	int			ntups;
	int			i_opcintype;
	int			i_opckeytype;
	int			i_opcdefault;
	int			i_opcfamily;
	int			i_opcfamilyname;
	int			i_opcfamilynsp;
	int			i_amname;
	int			i_amopstrategy;
	int			i_amopreqcheck;
	int			i_amopopr;
	int			i_sortfamily;
	int			i_sortfamilynsp;
	int			i_amprocnum;
	int			i_amproc;
	int			i_amproclefttype;
	int			i_amprocrighttype;
	char	   *opcintype;
	char	   *opckeytype;
	char	   *opcdefault;
	char	   *opcfamily;
	char	   *opcfamilyname;
	char	   *opcfamilynsp;
	char	   *amname;
	char	   *amopstrategy;
	char	   *amopreqcheck;
	char	   *amopopr;
	char	   *sortfamily;
	char	   *sortfamilynsp;
	char	   *amprocnum;
	char	   *amproc;
	char	   *amproclefttype;
	char	   *amprocrighttype;
	bool		needComma;
	int			i;

	/* Skip if not to be dumped */
	if (!opcinfo->dobj.dump || dopt->dataOnly)
		return;

	query = createStringInfo();
	q = createStringInfo();
	delq = createStringInfo();
	labelq = createStringInfo();

	/* Make sure we are in proper schema so regoperator works correctly */
	selectSourceSchema(fout, opcinfo->dobj.namespace->dobj.name);

	/* Get additional fields from the pg_opclass row */
	if (fout->remoteVersion >= 80300)
	{
		appendStringInfo(query, "SELECT opcintype::pg_catalog.regtype, "
						  "opckeytype::pg_catalog.regtype, "
						  "opcdefault, opcfamily, "
						  "opfname AS opcfamilyname, "
						  "nspname AS opcfamilynsp, "
						  "(SELECT amname FROM pg_catalog.pg_am WHERE oid = opcmethod) AS amname "
						  "FROM pg_catalog.pg_opclass c "
						  "LEFT JOIN pg_catalog.pg_opfamily f ON f.oid = opcfamily "
						  "LEFT JOIN pg_catalog.pg_namespace n ON n.oid = opfnamespace "
						  "WHERE c.oid = '%u'::pg_catalog.oid",
						  opcinfo->dobj.catId.oid);
	}
	else
	{
		appendStringInfo(query, "SELECT opcintype::pg_catalog.regtype, "
						  "opckeytype::pg_catalog.regtype, "
						  "opcdefault, NULL AS opcfamily, "
						  "NULL AS opcfamilyname, "
						  "NULL AS opcfamilynsp, "
						  "(SELECT amname FROM pg_catalog.pg_am WHERE oid = opcamid) AS amname "
						  "FROM pg_catalog.pg_opclass "
						  "WHERE oid = '%u'::pg_catalog.oid",
						  opcinfo->dobj.catId.oid);
	}

	res = ExecuteSqlQueryForSingleRow(fout, query->data);

	i_opcintype = dbms_md_get_field_subscript(res, "opcintype");
	i_opckeytype = dbms_md_get_field_subscript(res, "opckeytype");
	i_opcdefault = dbms_md_get_field_subscript(res, "opcdefault");
	i_opcfamily = dbms_md_get_field_subscript(res, "opcfamily");
	i_opcfamilyname = dbms_md_get_field_subscript(res, "opcfamilyname");
	i_opcfamilynsp = dbms_md_get_field_subscript(res, "opcfamilynsp");
	i_amname = dbms_md_get_field_subscript(res, "amname");

	/* opcintype may still be needed after we dbms_md_free_tuples res */
	opcintype = dbms_md_strdup(dbms_md_get_field_value(res, 0, i_opcintype));
	opckeytype = dbms_md_get_field_value(res, 0, i_opckeytype);
	opcdefault = dbms_md_get_field_value(res, 0, i_opcdefault);
	/* opcfamily will still be needed after we dbms_md_free_tuples res */
	opcfamily = dbms_md_strdup(dbms_md_get_field_value(res, 0, i_opcfamily));
	opcfamilyname = dbms_md_get_field_value(res, 0, i_opcfamilyname);
	opcfamilynsp = dbms_md_get_field_value(res, 0, i_opcfamilynsp);
	/* amname will still be needed after we dbms_md_free_tuples res */
	amname = dbms_md_strdup(dbms_md_get_field_value(res, 0, i_amname));

	/*
	 * DROP must be fully qualified in case same name appears in pg_catalog
	 */
	appendStringInfo(delq, "DROP OPERATOR CLASS %s",
					  dbms_md_fmtId(opcinfo->dobj.namespace->dobj.name));
	appendStringInfo(delq, ".%s",
					  dbms_md_fmtId_internal(opcinfo->dobj.name, true));
	appendStringInfo(delq, " USING %s;\n",
					  dbms_md_fmtId_internal(amname, true));

	/* Build the fixed portion of the CREATE command */
	appendStringInfo(q, "CREATE OPERATOR CLASS %s\n    ",
					  dbms_md_fmtId_internal(opcinfo->dobj.name, true));
	if (strcmp(opcdefault, "t") == 0)
		appendStringInfoString(q, "DEFAULT ");
	appendStringInfo(q, "FOR TYPE %s USING %s",
					  opcintype,
					  dbms_md_fmtId_internal(amname, true));
	if (strlen(opcfamilyname) > 0)
	{
		appendStringInfoString(q, " FAMILY ");
		if (strcmp(opcfamilynsp, opcinfo->dobj.namespace->dobj.name) != 0)
			appendStringInfo(q, "%s.", dbms_md_fmtId(opcfamilynsp));
		appendStringInfoString(q, dbms_md_fmtId_internal(opcfamilyname, true));
	}
	appendStringInfoString(q, " AS\n    ");

	needComma = false;

	if (strcmp(opckeytype, "-") != 0)
	{
		appendStringInfo(q, "STORAGE %s",
						  opckeytype);
		needComma = true;
	}

	dbms_md_free_tuples(res);

	/*
	 * Now fetch and print the OPERATOR entries (pg_amop rows).
	 *
	 * Print only those opfamily members that are tied to the opclass by
	 * pg_depend entries.
	 *
	 * XXX RECHECK is gone as of 8.4, but we'll still print it if dumping an
	 * older server's opclass in which it is used.  This is to avoid
	 * hard-to-detect breakage if a newer pg_dump is used to dump from an
	 * older server and then reload into that old version.  This can go away
	 * once 8.3 is so old as to not be of interest to anyone.
	 */
	resetStringInfo(query);

	if (fout->remoteVersion >= 90100)
	{
		appendStringInfo(query, "SELECT amopstrategy, false AS amopreqcheck, "
						  "amopopr::pg_catalog.regoperator, "
						  "opfname AS sortfamily, "
						  "nspname AS sortfamilynsp "
						  "FROM pg_catalog.pg_amop ao JOIN pg_catalog.pg_depend ON "
						  "(classid = 'pg_catalog.pg_amop'::pg_catalog.regclass AND objid = ao.oid) "
						  "LEFT JOIN pg_catalog.pg_opfamily f ON f.oid = amopsortfamily "
						  "LEFT JOIN pg_catalog.pg_namespace n ON n.oid = opfnamespace "
						  "WHERE refclassid = 'pg_catalog.pg_opclass'::pg_catalog.regclass "
						  "AND refobjid = '%u'::pg_catalog.oid "
						  "AND amopfamily = '%s'::pg_catalog.oid "
						  "ORDER BY amopstrategy",
						  opcinfo->dobj.catId.oid,
						  opcfamily);
	}
	else if (fout->remoteVersion >= 80400)
	{
		appendStringInfo(query, "SELECT amopstrategy, false AS amopreqcheck, "
						  "amopopr::pg_catalog.regoperator, "
						  "NULL AS sortfamily, "
						  "NULL AS sortfamilynsp "
						  "FROM pg_catalog.pg_amop ao, pg_catalog.pg_depend "
						  "WHERE refclassid = 'pg_catalog.pg_opclass'::pg_catalog.regclass "
						  "AND refobjid = '%u'::pg_catalog.oid "
						  "AND classid = 'pg_catalog.pg_amop'::pg_catalog.regclass "
						  "AND objid = ao.oid "
						  "ORDER BY amopstrategy",
						  opcinfo->dobj.catId.oid);
	}
	else if (fout->remoteVersion >= 80300)
	{
		appendStringInfo(query, "SELECT amopstrategy, amopreqcheck, "
						  "amopopr::pg_catalog.regoperator, "
						  "NULL AS sortfamily, "
						  "NULL AS sortfamilynsp "
						  "FROM pg_catalog.pg_amop ao, pg_catalog.pg_depend "
						  "WHERE refclassid = 'pg_catalog.pg_opclass'::pg_catalog.regclass "
						  "AND refobjid = '%u'::pg_catalog.oid "
						  "AND classid = 'pg_catalog.pg_amop'::pg_catalog.regclass "
						  "AND objid = ao.oid "
						  "ORDER BY amopstrategy",
						  opcinfo->dobj.catId.oid);
	}
	else
	{
		/*
		 * Here, we print all entries since there are no opfamilies and hence
		 * no loose operators to worry about.
		 */
		appendStringInfo(query, "SELECT amopstrategy, amopreqcheck, "
						  "amopopr::pg_catalog.regoperator, "
						  "NULL AS sortfamily, "
						  "NULL AS sortfamilynsp "
						  "FROM pg_catalog.pg_amop "
						  "WHERE amopclaid = '%u'::pg_catalog.oid "
						  "ORDER BY amopstrategy",
						  opcinfo->dobj.catId.oid);
	}

	res = ExecuteSqlQuery(fout, query->data);

	ntups = dbms_md_get_tuple_num(res);

	i_amopstrategy = dbms_md_get_field_subscript(res, "amopstrategy");
	i_amopreqcheck = dbms_md_get_field_subscript(res, "amopreqcheck");
	i_amopopr = dbms_md_get_field_subscript(res, "amopopr");
	i_sortfamily = dbms_md_get_field_subscript(res, "sortfamily");
	i_sortfamilynsp = dbms_md_get_field_subscript(res, "sortfamilynsp");

	for (i = 0; i < ntups; i++)
	{
		amopstrategy = dbms_md_get_field_value(res, i, i_amopstrategy);
		amopreqcheck = dbms_md_get_field_value(res, i, i_amopreqcheck);
		amopopr = dbms_md_get_field_value(res, i, i_amopopr);
		sortfamily = dbms_md_get_field_value(res, i, i_sortfamily);
		sortfamilynsp = dbms_md_get_field_value(res, i, i_sortfamilynsp);

		if (needComma)
			appendStringInfoString(q, " ,\n    ");

		appendStringInfo(q, "OPERATOR %s %s",
						  amopstrategy, amopopr);

		if (strlen(sortfamily) > 0)
		{
			appendStringInfoString(q, " FOR ORDER BY ");
			if (strcmp(sortfamilynsp, opcinfo->dobj.namespace->dobj.name) != 0)
				appendStringInfo(q, "%s.", dbms_md_fmtId(sortfamilynsp));
			appendStringInfoString(q, dbms_md_fmtId_internal(sortfamily, true));
		}

		if (strcmp(amopreqcheck, "t") == 0)
			appendStringInfoString(q, " RECHECK");

		needComma = true;
	}

	dbms_md_free_tuples(res);

	/*
	 * Now fetch and print the FUNCTION entries (pg_amproc rows).
	 *
	 * Print only those opfamily members that are tied to the opclass by
	 * pg_depend entries.
	 *
	 * We print the amproclefttype/amprocrighttype even though in most cases
	 * the backend could deduce the right values, because of the corner case
	 * of a btree sort support function for a cross-type comparison.  That's
	 * only allowed in 9.2 and later, but for simplicity print them in all
	 * versions that have the columns.
	 */
	resetStringInfo(query);

	if (fout->remoteVersion >= 80300)
	{
		appendStringInfo(query, "SELECT amprocnum, "
						  "amproc::pg_catalog.regprocedure, "
						  "amproclefttype::pg_catalog.regtype, "
						  "amprocrighttype::pg_catalog.regtype "
						  "FROM pg_catalog.pg_amproc ap, pg_catalog.pg_depend "
						  "WHERE refclassid = 'pg_catalog.pg_opclass'::pg_catalog.regclass "
						  "AND refobjid = '%u'::pg_catalog.oid "
						  "AND classid = 'pg_catalog.pg_amproc'::pg_catalog.regclass "
						  "AND objid = ap.oid "
						  "ORDER BY amprocnum",
						  opcinfo->dobj.catId.oid);
	}
	else
	{
		appendStringInfo(query, "SELECT amprocnum, "
						  "amproc::pg_catalog.regprocedure, "
						  "'' AS amproclefttype, "
						  "'' AS amprocrighttype "
						  "FROM pg_catalog.pg_amproc "
						  "WHERE amopclaid = '%u'::pg_catalog.oid "
						  "ORDER BY amprocnum",
						  opcinfo->dobj.catId.oid);
	}

	res = ExecuteSqlQuery(fout, query->data);

	ntups = dbms_md_get_tuple_num(res);

	i_amprocnum = dbms_md_get_field_subscript(res, "amprocnum");
	i_amproc = dbms_md_get_field_subscript(res, "amproc");
	i_amproclefttype = dbms_md_get_field_subscript(res, "amproclefttype");
	i_amprocrighttype = dbms_md_get_field_subscript(res, "amprocrighttype");

	for (i = 0; i < ntups; i++)
	{
		amprocnum = dbms_md_get_field_value(res, i, i_amprocnum);
		amproc = dbms_md_get_field_value(res, i, i_amproc);
		amproclefttype = dbms_md_get_field_value(res, i, i_amproclefttype);
		amprocrighttype = dbms_md_get_field_value(res, i, i_amprocrighttype);

		if (needComma)
			appendStringInfoString(q, " ,\n    ");

		appendStringInfo(q, "FUNCTION %s", amprocnum);

		if (*amproclefttype && *amprocrighttype)
			appendStringInfo(q, " (%s, %s)", amproclefttype, amprocrighttype);

		appendStringInfo(q, " %s", amproc);

		needComma = true;
	}

	dbms_md_free_tuples(res);

	/*
	 * If needComma is still false it means we haven't added anything after
	 * the AS keyword.  To avoid printing broken SQL, append a dummy STORAGE
	 * clause with the same datatype.  This isn't sanctioned by the
	 * documentation, but actually DefineOpClass will treat it as a no-op.
	 */
	if (!needComma)
		appendStringInfo(q, "STORAGE %s", opcintype);

	appendStringInfoString(q, ";\n");

	appendStringInfo(labelq, "OPERATOR CLASS %s",
					  dbms_md_fmtId_internal(opcinfo->dobj.name, true));
	appendStringInfo(labelq, " USING %s",
					  dbms_md_fmtId_internal(amname, true));

#if 0
	if (dopt->binary_upgrade)
		binary_upgrade_extension_member(q, &opcinfo->dobj, labelq->data);
#endif

	if (opcinfo->dobj.dump & DUMP_COMPONENT_DEFINITION)
		ArchiveEntry(fout, opcinfo->dobj.catId, opcinfo->dobj.dumpId,
					 opcinfo->dobj.name,
					 opcinfo->dobj.namespace->dobj.name,
					 NULL,
					 opcinfo->rolname,
					 false, "OPERATOR CLASS", SECTION_PRE_DATA,
					 q->data, delq->data, NULL,
					 NULL, 0,
					 NULL, NULL);

	/* Dump Operator Class Comments */
	if (opcinfo->dobj.dump & DUMP_COMPONENT_COMMENT)
		dumpComment(fout, labelq->data,
					opcinfo->dobj.namespace->dobj.name, opcinfo->rolname,
					opcinfo->dobj.catId, 0, opcinfo->dobj.dumpId);

	free(opcintype);
	free(opcfamily);
	free(amname);
	destroyStringInfo(query);
	destroyStringInfo(q);
	destroyStringInfo(delq);
	destroyStringInfo(labelq);

	pfree(query);
	pfree(q);
	pfree(delq);
	pfree(labelq);
}

/*
 * dumpOpfamily
 *	  write out a single operator family definition
 *
 * Note: this also dumps any "loose" operator members that aren't bound to a
 * specific opclass within the opfamily.
 */
static void
dumpOpfamily(Archive *fout, OpfamilyInfo *opfinfo)
{
	DumpOptions *dopt = fout->dopt;
	StringInfo query;
	StringInfo q;
	StringInfo delq;
	StringInfo labelq;
	SPITupleTable   *res;
	SPITupleTable   *res_ops;
	SPITupleTable   *res_procs;
	int			ntups;
	int			i_amname;
	int			i_amopstrategy;
	int			i_amopreqcheck;
	int			i_amopopr;
	int			i_sortfamily;
	int			i_sortfamilynsp;
	int			i_amprocnum;
	int			i_amproc;
	int			i_amproclefttype;
	int			i_amprocrighttype;
	char	   *amname;
	char	   *amopstrategy;
	char	   *amopreqcheck;
	char	   *amopopr;
	char	   *sortfamily;
	char	   *sortfamilynsp;
	char	   *amprocnum;
	char	   *amproc;
	char	   *amproclefttype;
	char	   *amprocrighttype;
	bool		needComma;
	int			i;

	/* Skip if not to be dumped */
	if (!opfinfo->dobj.dump || dopt->dataOnly)
		return;

	query = createStringInfo();
	q = createStringInfo();
	delq = createStringInfo();
	labelq = createStringInfo();

	/* Make sure we are in proper schema so regoperator works correctly */
	selectSourceSchema(fout, opfinfo->dobj.namespace->dobj.name);

	/*
	 * Fetch only those opfamily members that are tied directly to the
	 * opfamily by pg_depend entries.
	 *
	 * XXX RECHECK is gone as of 8.4, but we'll still print it if dumping an
	 * older server's opclass in which it is used.  This is to avoid
	 * hard-to-detect breakage if a newer pg_dump is used to dump from an
	 * older server and then reload into that old version.  This can go away
	 * once 8.3 is so old as to not be of interest to anyone.
	 */
	if (fout->remoteVersion >= 90100)
	{
		appendStringInfo(query, "SELECT amopstrategy, false AS amopreqcheck, "
						  "amopopr::pg_catalog.regoperator, "
						  "opfname AS sortfamily, "
						  "nspname AS sortfamilynsp "
						  "FROM pg_catalog.pg_amop ao JOIN pg_catalog.pg_depend ON "
						  "(classid = 'pg_catalog.pg_amop'::pg_catalog.regclass AND objid = ao.oid) "
						  "LEFT JOIN pg_catalog.pg_opfamily f ON f.oid = amopsortfamily "
						  "LEFT JOIN pg_catalog.pg_namespace n ON n.oid = opfnamespace "
						  "WHERE refclassid = 'pg_catalog.pg_opfamily'::pg_catalog.regclass "
						  "AND refobjid = '%u'::pg_catalog.oid "
						  "AND amopfamily = '%u'::pg_catalog.oid "
						  "ORDER BY amopstrategy",
						  opfinfo->dobj.catId.oid,
						  opfinfo->dobj.catId.oid);
	}
	else if (fout->remoteVersion >= 80400)
	{
		appendStringInfo(query, "SELECT amopstrategy, false AS amopreqcheck, "
						  "amopopr::pg_catalog.regoperator, "
						  "NULL AS sortfamily, "
						  "NULL AS sortfamilynsp "
						  "FROM pg_catalog.pg_amop ao, pg_catalog.pg_depend "
						  "WHERE refclassid = 'pg_catalog.pg_opfamily'::pg_catalog.regclass "
						  "AND refobjid = '%u'::pg_catalog.oid "
						  "AND classid = 'pg_catalog.pg_amop'::pg_catalog.regclass "
						  "AND objid = ao.oid "
						  "ORDER BY amopstrategy",
						  opfinfo->dobj.catId.oid);
	}
	else
	{
		appendStringInfo(query, "SELECT amopstrategy, amopreqcheck, "
						  "amopopr::pg_catalog.regoperator, "
						  "NULL AS sortfamily, "
						  "NULL AS sortfamilynsp "
						  "FROM pg_catalog.pg_amop ao, pg_catalog.pg_depend "
						  "WHERE refclassid = 'pg_catalog.pg_opfamily'::pg_catalog.regclass "
						  "AND refobjid = '%u'::pg_catalog.oid "
						  "AND classid = 'pg_catalog.pg_amop'::pg_catalog.regclass "
						  "AND objid = ao.oid "
						  "ORDER BY amopstrategy",
						  opfinfo->dobj.catId.oid);
	}

	res_ops = ExecuteSqlQuery(fout, query->data);

	resetStringInfo(query);

	appendStringInfo(query, "SELECT amprocnum, "
					  "amproc::pg_catalog.regprocedure, "
					  "amproclefttype::pg_catalog.regtype, "
					  "amprocrighttype::pg_catalog.regtype "
					  "FROM pg_catalog.pg_amproc ap, pg_catalog.pg_depend "
					  "WHERE refclassid = 'pg_catalog.pg_opfamily'::pg_catalog.regclass "
					  "AND refobjid = '%u'::pg_catalog.oid "
					  "AND classid = 'pg_catalog.pg_amproc'::pg_catalog.regclass "
					  "AND objid = ap.oid "
					  "ORDER BY amprocnum",
					  opfinfo->dobj.catId.oid);

	res_procs = ExecuteSqlQuery(fout, query->data);

	/* Get additional fields from the pg_opfamily row */
	resetStringInfo(query);

	appendStringInfo(query, "SELECT "
					  "(SELECT amname FROM pg_catalog.pg_am WHERE oid = opfmethod) AS amname "
					  "FROM pg_catalog.pg_opfamily "
					  "WHERE oid = '%u'::pg_catalog.oid",
					  opfinfo->dobj.catId.oid);

	res = ExecuteSqlQueryForSingleRow(fout, query->data);

	i_amname = dbms_md_get_field_subscript(res, "amname");

	/* amname will still be needed after we dbms_md_free_tuples res */
	amname = dbms_md_strdup(dbms_md_get_field_value(res, 0, i_amname));

	/*
	 * DROP must be fully qualified in case same name appears in pg_catalog
	 */
	appendStringInfo(delq, "DROP OPERATOR FAMILY %s",
					  dbms_md_fmtId(opfinfo->dobj.namespace->dobj.name));
	appendStringInfo(delq, ".%s",
					  dbms_md_fmtId_internal(opfinfo->dobj.name, true));
	appendStringInfo(delq, " USING %s;\n",
					  dbms_md_fmtId_internal(amname, true));

	/* Build the fixed portion of the CREATE command */
	appendStringInfo(q, "CREATE OPERATOR FAMILY %s",
					  dbms_md_fmtId_internal(opfinfo->dobj.name, true));
	appendStringInfo(q, " USING %s;\n",
					  dbms_md_fmtId_internal(amname, true));

	dbms_md_free_tuples(res);

	/* Do we need an ALTER to add loose members? */
	if (dbms_md_get_tuple_num(res_ops) > 0 || dbms_md_get_tuple_num(res_procs) > 0)
	{
		appendStringInfo(q, "ALTER OPERATOR FAMILY %s",
						  dbms_md_fmtId_internal(opfinfo->dobj.name, true));
		appendStringInfo(q, " USING %s ADD\n    ",
						  dbms_md_fmtId_internal(amname, true));

		needComma = false;

		/*
		 * Now fetch and print the OPERATOR entries (pg_amop rows).
		 */
		ntups = dbms_md_get_tuple_num(res_ops);

		i_amopstrategy = dbms_md_get_field_subscript(res_ops, "amopstrategy");
		i_amopreqcheck = dbms_md_get_field_subscript(res_ops, "amopreqcheck");
		i_amopopr = dbms_md_get_field_subscript(res_ops, "amopopr");
		i_sortfamily = dbms_md_get_field_subscript(res_ops, "sortfamily");
		i_sortfamilynsp = dbms_md_get_field_subscript(res_ops, "sortfamilynsp");

		for (i = 0; i < ntups; i++)
		{
			amopstrategy = dbms_md_get_field_value(res_ops, i, i_amopstrategy);
			amopreqcheck = dbms_md_get_field_value(res_ops, i, i_amopreqcheck);
			amopopr = dbms_md_get_field_value(res_ops, i, i_amopopr);
			sortfamily = dbms_md_get_field_value(res_ops, i, i_sortfamily);
			sortfamilynsp = dbms_md_get_field_value(res_ops, i, i_sortfamilynsp);

			if (needComma)
				appendStringInfoString(q, " ,\n    ");

			appendStringInfo(q, "OPERATOR %s %s",
							  amopstrategy, amopopr);

			if (strlen(sortfamily) > 0)
			{
				appendStringInfoString(q, " FOR ORDER BY ");
				if (strcmp(sortfamilynsp, opfinfo->dobj.namespace->dobj.name) != 0)
					appendStringInfo(q, "%s.", dbms_md_fmtId(sortfamilynsp));
				appendStringInfoString(q, dbms_md_fmtId_internal(sortfamily, true));
			}

			if (strcmp(amopreqcheck, "t") == 0)
				appendStringInfoString(q, " RECHECK");

			needComma = true;
		}

		/*
		 * Now fetch and print the FUNCTION entries (pg_amproc rows).
		 */
		ntups = dbms_md_get_tuple_num(res_procs);

		i_amprocnum = dbms_md_get_field_subscript(res_procs, "amprocnum");
		i_amproc = dbms_md_get_field_subscript(res_procs, "amproc");
		i_amproclefttype = dbms_md_get_field_subscript(res_procs, "amproclefttype");
		i_amprocrighttype = dbms_md_get_field_subscript(res_procs, "amprocrighttype");

		for (i = 0; i < ntups; i++)
		{
			amprocnum = dbms_md_get_field_value(res_procs, i, i_amprocnum);
			amproc = dbms_md_get_field_value(res_procs, i, i_amproc);
			amproclefttype = dbms_md_get_field_value(res_procs, i, i_amproclefttype);
			amprocrighttype = dbms_md_get_field_value(res_procs, i, i_amprocrighttype);

			if (needComma)
				appendStringInfoString(q, " ,\n    ");

			appendStringInfo(q, "FUNCTION %s (%s, %s) %s",
							  amprocnum, amproclefttype, amprocrighttype,
							  amproc);

			needComma = true;
		}

		appendStringInfoString(q, ";\n");
	}

	appendStringInfo(labelq, "OPERATOR FAMILY %s",
					  dbms_md_fmtId_internal(opfinfo->dobj.name, true));
	appendStringInfo(labelq, " USING %s",
					  dbms_md_fmtId_internal(amname, true));

#if 0
	if (dopt->binary_upgrade)
		binary_upgrade_extension_member(q, &opfinfo->dobj, labelq->data);
#endif

	if (opfinfo->dobj.dump & DUMP_COMPONENT_DEFINITION)
		ArchiveEntry(fout, opfinfo->dobj.catId, opfinfo->dobj.dumpId,
					 opfinfo->dobj.name,
					 opfinfo->dobj.namespace->dobj.name,
					 NULL,
					 opfinfo->rolname,
					 false, "OPERATOR FAMILY", SECTION_PRE_DATA,
					 q->data, delq->data, NULL,
					 NULL, 0,
					 NULL, NULL);

	/* Dump Operator Family Comments */
	if (opfinfo->dobj.dump & DUMP_COMPONENT_COMMENT)
		dumpComment(fout, labelq->data,
					opfinfo->dobj.namespace->dobj.name, opfinfo->rolname,
					opfinfo->dobj.catId, 0, opfinfo->dobj.dumpId);

	free(amname);
	dbms_md_free_tuples(res_ops);
	dbms_md_free_tuples(res_procs);
	destroyStringInfo(query);
	destroyStringInfo(q);
	destroyStringInfo(delq);
	destroyStringInfo(labelq);

	pfree(query);
	pfree(q);
	pfree(delq);
	pfree(labelq);
}

/*
 * dumpCollation
 *	  write out a single collation definition
 */
static void
dumpCollation(Archive *fout, CollInfo *collinfo)
{
	DumpOptions *dopt = fout->dopt;
	StringInfo query;
	StringInfo q;
	StringInfo delq;
	StringInfo labelq;
	SPITupleTable   *res;
	int			i_collprovider;
	int			i_collcollate;
	int			i_collctype;
	const char *collprovider;
	const char *collcollate;
	const char *collctype;

	/* Skip if not to be dumped */
	if (!collinfo->dobj.dump || dopt->dataOnly)
		return;

	query = createStringInfo();
	q = createStringInfo();
	delq = createStringInfo();
	labelq = createStringInfo();

	/* Make sure we are in proper schema */
	selectSourceSchema(fout, collinfo->dobj.namespace->dobj.name);

	/* Get collation-specific details */
	if (fout->remoteVersion >= 100000)
		appendStringInfo(query, "SELECT "
						  "collprovider, "
						  "collcollate, "
						  "collctype, "
						  "collversion "
						  "FROM pg_catalog.pg_collation c "
						  "WHERE c.oid = '%u'::pg_catalog.oid",
						  collinfo->dobj.catId.oid);
	else
		appendStringInfo(query, "SELECT "
						  "'c'::char AS collprovider, "
						  "collcollate, "
						  "collctype, "
						  "NULL AS collversion "
						  "FROM pg_catalog.pg_collation c "
						  "WHERE c.oid = '%u'::pg_catalog.oid",
						  collinfo->dobj.catId.oid);

	res = ExecuteSqlQueryForSingleRow(fout, query->data);

	i_collprovider = dbms_md_get_field_subscript(res, "collprovider");
	i_collcollate = dbms_md_get_field_subscript(res, "collcollate");
	i_collctype = dbms_md_get_field_subscript(res, "collctype");

	collprovider = dbms_md_get_field_value(res, 0, i_collprovider);
	collcollate = dbms_md_get_field_value(res, 0, i_collcollate);
	collctype = dbms_md_get_field_value(res, 0, i_collctype);

	/*
	 * DROP must be fully qualified in case same name appears in pg_catalog
	 */
	appendStringInfo(delq, "DROP COLLATION %s",
					  dbms_md_fmtId(collinfo->dobj.namespace->dobj.name));
	appendStringInfo(delq, ".%s;\n",
					  dbms_md_fmtId_internal(collinfo->dobj.name, true));

	appendStringInfo(q, "CREATE COLLATION %s (",
					  dbms_md_fmtId_internal(collinfo->dobj.name, true));

	appendStringInfoString(q, "provider = ");
	if (collprovider[0] == 'c')
		appendStringInfoString(q, "libc");
	else if (collprovider[0] == 'i')
		appendStringInfoString(q, "icu");
	else if (collprovider[0] == 'd')
		/* to allow dumping pg_catalog; not accepted on input */
		appendStringInfoString(q, "default");
	else
		elog(ERROR,
					  "unrecognized collation provider: %s\n",
					  collprovider);

	if (strcmp(collcollate, collctype) == 0)
	{
		appendStringInfoString(q, ", locale = ");
		appendStringLiteralAH(q, collcollate, fout);
	}
	else
	{
		appendStringInfoString(q, ", lc_collate = ");
		appendStringLiteralAH(q, collcollate, fout);
		appendStringInfoString(q, ", lc_ctype = ");
		appendStringLiteralAH(q, collctype, fout);
	}

	/*
	 * For binary upgrade, carry over the collation version.  For normal
	 * dump/restore, omit the version, so that it is computed upon restore.
	 */
	if (dopt->binary_upgrade)
	{
		int			i_collversion;

		i_collversion = dbms_md_get_field_subscript(res, "collversion");
		if (!dbms_md_is_tuple_field_null(res, 0, i_collversion))
		{
			appendStringInfoString(q, ", version = ");
			appendStringLiteralAH(q,
								  dbms_md_get_field_value(res, 0, i_collversion),
								  fout);
		}
	}

	appendStringInfoString(q, ");\n");

	appendStringInfo(labelq, "COLLATION %s", dbms_md_fmtId_internal(collinfo->dobj.name, true));

#if 0
	if (dopt->binary_upgrade)
		binary_upgrade_extension_member(q, &collinfo->dobj, labelq->data);
#endif

	if (collinfo->dobj.dump & DUMP_COMPONENT_DEFINITION)
		ArchiveEntry(fout, collinfo->dobj.catId, collinfo->dobj.dumpId,
					 collinfo->dobj.name,
					 collinfo->dobj.namespace->dobj.name,
					 NULL,
					 collinfo->rolname,
					 false, "COLLATION", SECTION_PRE_DATA,
					 q->data, delq->data, NULL,
					 NULL, 0,
					 NULL, NULL);

	/* Dump Collation Comments */
	if (collinfo->dobj.dump & DUMP_COMPONENT_COMMENT)
		dumpComment(fout, labelq->data,
					collinfo->dobj.namespace->dobj.name, collinfo->rolname,
					collinfo->dobj.catId, 0, collinfo->dobj.dumpId);

	dbms_md_free_tuples(res);

	destroyStringInfo(query);
	destroyStringInfo(q);
	destroyStringInfo(delq);
	destroyStringInfo(labelq);

	pfree(query);
	pfree(q);
	pfree(delq);
	pfree(labelq);
}

/*
 * dumpConversion
 *	  write out a single conversion definition
 */
static void
dumpConversion(Archive *fout, ConvInfo *convinfo)
{
	DumpOptions *dopt = fout->dopt;
	StringInfo query;
	StringInfo q;
	StringInfo delq;
	StringInfo labelq;
	SPITupleTable   *res;
	int			i_conforencoding;
	int			i_contoencoding;
	int			i_conproc;
	int			i_condefault;
	const char *conforencoding;
	const char *contoencoding;
	const char *conproc;
	bool		condefault;

	/* Skip if not to be dumped */
	if (!convinfo->dobj.dump || dopt->dataOnly)
		return;

	query = createStringInfo();
	q = createStringInfo();
	delq = createStringInfo();
	labelq = createStringInfo();

	/* Make sure we are in proper schema */
	selectSourceSchema(fout, convinfo->dobj.namespace->dobj.name);

	/* Get conversion-specific details */
	appendStringInfo(query, "SELECT "
					  "pg_catalog.pg_encoding_to_char(conforencoding) AS conforencoding, "
					  "pg_catalog.pg_encoding_to_char(contoencoding) AS contoencoding, "
					  "conproc, condefault "
					  "FROM pg_catalog.pg_conversion c "
					  "WHERE c.oid = '%u'::pg_catalog.oid",
					  convinfo->dobj.catId.oid);

	res = ExecuteSqlQueryForSingleRow(fout, query->data);

	i_conforencoding = dbms_md_get_field_subscript(res, "conforencoding");
	i_contoencoding = dbms_md_get_field_subscript(res, "contoencoding");
	i_conproc = dbms_md_get_field_subscript(res, "conproc");
	i_condefault = dbms_md_get_field_subscript(res, "condefault");

	conforencoding = dbms_md_get_field_value(res, 0, i_conforencoding);
	contoencoding = dbms_md_get_field_value(res, 0, i_contoencoding);
	conproc = dbms_md_get_field_value(res, 0, i_conproc);
	condefault = (dbms_md_get_field_value(res, 0, i_condefault)[0] == 't');

	/*
	 * DROP must be fully qualified in case same name appears in pg_catalog
	 */
	appendStringInfo(delq, "DROP CONVERSION %s",
					  dbms_md_fmtId(convinfo->dobj.namespace->dobj.name));
	appendStringInfo(delq, ".%s;\n",
					  dbms_md_fmtId(convinfo->dobj.name));

	appendStringInfo(q, "CREATE %sCONVERSION %s FOR ",
					  (condefault) ? "DEFAULT " : "",
					  dbms_md_fmtId(convinfo->dobj.name));
	appendStringLiteralAH(q, conforencoding, fout);
	appendStringInfoString(q, " TO ");
	appendStringLiteralAH(q, contoencoding, fout);
	/* regproc output is already sufficiently quoted */
	appendStringInfo(q, " FROM %s;\n", conproc);

	appendStringInfo(labelq, "CONVERSION %s", dbms_md_fmtId(convinfo->dobj.name));

#if 0
	if (dopt->binary_upgrade)
		binary_upgrade_extension_member(q, &convinfo->dobj, labelq->data);
#endif

	if (convinfo->dobj.dump & DUMP_COMPONENT_DEFINITION)
		ArchiveEntry(fout, convinfo->dobj.catId, convinfo->dobj.dumpId,
					 convinfo->dobj.name,
					 convinfo->dobj.namespace->dobj.name,
					 NULL,
					 convinfo->rolname,
					 false, "CONVERSION", SECTION_PRE_DATA,
					 q->data, delq->data, NULL,
					 NULL, 0,
					 NULL, NULL);

	/* Dump Conversion Comments */
	if (convinfo->dobj.dump & DUMP_COMPONENT_COMMENT)
		dumpComment(fout, labelq->data,
					convinfo->dobj.namespace->dobj.name, convinfo->rolname,
					convinfo->dobj.catId, 0, convinfo->dobj.dumpId);

	dbms_md_free_tuples(res);

	destroyStringInfo(query);
	destroyStringInfo(q);
	destroyStringInfo(delq);
	destroyStringInfo(labelq);

	pfree(query);
	pfree(q);
	pfree(delq);
	pfree(labelq);
}

/*
 * format_aggregate_signature: generate aggregate name and argument list
 *
 * The argument type names are qualified if needed.  The aggregate name
 * is never qualified.
 */
static char *
format_aggregate_signature(AggInfo *agginfo, Archive *fout, bool honor_quotes)
{
	StringInfoData buf;
	int			j;

	initStringInfo(&buf);
	if (honor_quotes)
		appendStringInfoString(&buf, dbms_md_fmtId(agginfo->aggfn.dobj.name));
	else
		appendStringInfoString(&buf, agginfo->aggfn.dobj.name);

	if (agginfo->aggfn.nargs == 0)
		appendStringInfo(&buf, "(*)");
	else
	{
		appendStringInfoChar(&buf, '(');
		for (j = 0; j < agginfo->aggfn.nargs; j++)
		{
			char	   *typname;

			typname = getFormattedTypeName(fout, agginfo->aggfn.argtypes[j],
										   zeroAsOpaque);

			appendStringInfo(&buf, "%s%s",
							  (j > 0) ? ", " : "",
							  typname);
			free(typname);
		}
		appendStringInfoChar(&buf, ')');
	}
	return buf.data;
}

/*
 * dumpAgg
 *	  write out a single aggregate definition
 */
static void
dumpAgg(Archive *fout, AggInfo *agginfo)
{
	DumpOptions *dopt = fout->dopt;
	StringInfo query;
	StringInfo q;
	StringInfo delq;
	StringInfo labelq;
	StringInfo details;
	char	   *aggsig;			/* identity signature */
	char	   *aggfullsig = NULL;	/* full signature */
	char	   *aggsig_tag;
	SPITupleTable   *res;
	int			i_aggtransfn;
	int			i_aggfinalfn;
	int			i_aggcombinefn;
	int			i_aggserialfn;
	int			i_aggdeserialfn;
	int			i_aggmtransfn;
	int			i_aggminvtransfn;
	int			i_aggmfinalfn;
	int			i_aggfinalextra;
	int			i_aggmfinalextra;
	int			i_aggsortop;
	int			i_hypothetical;
	int			i_aggtranstype;
	int			i_aggtransspace;
	int			i_aggmtranstype;
	int			i_aggmtransspace;
	int			i_agginitval;
	int			i_aggminitval;
	int			i_convertok;
	int			i_proparallel;
	const char *aggtransfn;
	const char *aggfinalfn;
	const char *aggcombinefn;
	const char *aggserialfn;
	const char *aggdeserialfn;
	const char *aggmtransfn;
	const char *aggminvtransfn;
	const char *aggmfinalfn;
	bool		aggfinalextra;
	bool		aggmfinalextra;
	const char *aggsortop;
	char	   *aggsortconvop;
	bool		hypothetical;
	const char *aggtranstype;
	const char *aggtransspace;
	const char *aggmtranstype;
	const char *aggmtransspace;
	const char *agginitval;
	const char *aggminitval;
	bool		convertok;
	const char *proparallel;

	/* Skip if not to be dumped */
	if (!agginfo->aggfn.dobj.dump || dopt->dataOnly)
		return;

	query = createStringInfo();
	q = createStringInfo();
	delq = createStringInfo();
	labelq = createStringInfo();
	details = createStringInfo();

	/* Make sure we are in proper schema */
	selectSourceSchema(fout, agginfo->aggfn.dobj.namespace->dobj.name);

	/* Get aggregate-specific details */
	if (fout->remoteVersion >= 90600)
	{
		appendStringInfo(query, "SELECT aggtransfn, "
						  "aggfinalfn, aggtranstype::pg_catalog.regtype, "
						  "aggcombinefn, aggserialfn, aggdeserialfn, aggmtransfn, "
						  "aggminvtransfn, aggmfinalfn, aggmtranstype::pg_catalog.regtype, "
						  "aggfinalextra, aggmfinalextra, "
						  "aggsortop::pg_catalog.regoperator, "
						  "(aggkind = 'h') AS hypothetical, "
						  "aggtransspace, agginitval, "
						  "aggmtransspace, aggminitval, "
						  "true AS convertok, "
						  "pg_catalog.pg_get_function_arguments(p.oid) AS funcargs, "
						  "pg_catalog.pg_get_function_identity_arguments(p.oid) AS funciargs, "
						  "p.proparallel "
						  "FROM pg_catalog.pg_aggregate a, pg_catalog.pg_proc p "
						  "WHERE a.aggfnoid = p.oid "
						  "AND p.oid = '%u'::pg_catalog.oid",
						  agginfo->aggfn.dobj.catId.oid);
	}
	else if (fout->remoteVersion >= 90400)
	{
		appendStringInfo(query, "SELECT aggtransfn, "
						  "aggfinalfn, aggtranstype::pg_catalog.regtype, "
						  "'-' AS aggcombinefn, '-' AS aggserialfn, "
						  "'-' AS aggdeserialfn, aggmtransfn, aggminvtransfn, "
						  "aggmfinalfn, aggmtranstype::pg_catalog.regtype, "
						  "aggfinalextra, aggmfinalextra, "
						  "aggsortop::pg_catalog.regoperator, "
						  "(aggkind = 'h') AS hypothetical, "
						  "aggtransspace, agginitval, "
						  "aggmtransspace, aggminitval, "
						  "true AS convertok, "
						  "pg_catalog.pg_get_function_arguments(p.oid) AS funcargs, "
						  "pg_catalog.pg_get_function_identity_arguments(p.oid) AS funciargs "
						  "FROM pg_catalog.pg_aggregate a, pg_catalog.pg_proc p "
						  "WHERE a.aggfnoid = p.oid "
						  "AND p.oid = '%u'::pg_catalog.oid",
						  agginfo->aggfn.dobj.catId.oid);
	}
	else if (fout->remoteVersion >= 80400)
	{
		appendStringInfo(query, "SELECT aggtransfn, "
						  "aggfinalfn, aggtranstype::pg_catalog.regtype, "
						  "'-' AS aggcombinefn, '-' AS aggserialfn, "
						  "'-' AS aggdeserialfn, '-' AS aggmtransfn, "
						  "'-' AS aggminvtransfn, '-' AS aggmfinalfn, "
						  "0 AS aggmtranstype, false AS aggfinalextra, "
						  "false AS aggmfinalextra, "
						  "aggsortop::pg_catalog.regoperator, "
						  "false AS hypothetical, "
						  "0 AS aggtransspace, agginitval, "
						  "0 AS aggmtransspace, NULL AS aggminitval, "
						  "true AS convertok, "
						  "pg_catalog.pg_get_function_arguments(p.oid) AS funcargs, "
						  "pg_catalog.pg_get_function_identity_arguments(p.oid) AS funciargs "
						  "FROM pg_catalog.pg_aggregate a, pg_catalog.pg_proc p "
						  "WHERE a.aggfnoid = p.oid "
						  "AND p.oid = '%u'::pg_catalog.oid",
						  agginfo->aggfn.dobj.catId.oid);
	}
	else if (fout->remoteVersion >= 80100)
	{
		appendStringInfo(query, "SELECT aggtransfn, "
						  "aggfinalfn, aggtranstype::pg_catalog.regtype, "
						  "'-' AS aggcombinefn, '-' AS aggserialfn, "
						  "'-' AS aggdeserialfn, '-' AS aggmtransfn, "
						  "'-' AS aggminvtransfn, '-' AS aggmfinalfn, "
						  "0 AS aggmtranstype, false AS aggfinalextra, "
						  "false AS aggmfinalextra, "
						  "aggsortop::pg_catalog.regoperator, "
						  "false AS hypothetical, "
						  "0 AS aggtransspace, agginitval, "
						  "0 AS aggmtransspace, NULL AS aggminitval, "
						  "true AS convertok "
						  "FROM pg_catalog.pg_aggregate a, pg_catalog.pg_proc p "
						  "WHERE a.aggfnoid = p.oid "
						  "AND p.oid = '%u'::pg_catalog.oid",
						  agginfo->aggfn.dobj.catId.oid);
	}
	else
	{
		appendStringInfo(query, "SELECT aggtransfn, "
						  "aggfinalfn, aggtranstype::pg_catalog.regtype, "
						  "'-' AS aggcombinefn, '-' AS aggserialfn, "
						  "'-' AS aggdeserialfn, '-' AS aggmtransfn, "
						  "'-' AS aggminvtransfn, '-' AS aggmfinalfn, "
						  "0 AS aggmtranstype, false AS aggfinalextra, "
						  "false AS aggmfinalextra, 0 AS aggsortop, "
						  "false AS hypothetical, "
						  "0 AS aggtransspace, agginitval, "
						  "0 AS aggmtransspace, NULL AS aggminitval, "
						  "true AS convertok "
						  "FROM pg_catalog.pg_aggregate a, pg_catalog.pg_proc p "
						  "WHERE a.aggfnoid = p.oid "
						  "AND p.oid = '%u'::pg_catalog.oid",
						  agginfo->aggfn.dobj.catId.oid);
	}

	res = ExecuteSqlQueryForSingleRow(fout, query->data);

	i_aggtransfn = dbms_md_get_field_subscript(res, "aggtransfn");
	i_aggfinalfn = dbms_md_get_field_subscript(res, "aggfinalfn");
	i_aggcombinefn = dbms_md_get_field_subscript(res, "aggcombinefn");
	i_aggserialfn = dbms_md_get_field_subscript(res, "aggserialfn");
	i_aggdeserialfn = dbms_md_get_field_subscript(res, "aggdeserialfn");
	i_aggmtransfn = dbms_md_get_field_subscript(res, "aggmtransfn");
	i_aggminvtransfn = dbms_md_get_field_subscript(res, "aggminvtransfn");
	i_aggmfinalfn = dbms_md_get_field_subscript(res, "aggmfinalfn");
	i_aggfinalextra = dbms_md_get_field_subscript(res, "aggfinalextra");
	i_aggmfinalextra = dbms_md_get_field_subscript(res, "aggmfinalextra");
	i_aggsortop = dbms_md_get_field_subscript(res, "aggsortop");
	i_hypothetical = dbms_md_get_field_subscript(res, "hypothetical");
	i_aggtranstype = dbms_md_get_field_subscript(res, "aggtranstype");
	i_aggtransspace = dbms_md_get_field_subscript(res, "aggtransspace");
	i_aggmtranstype = dbms_md_get_field_subscript(res, "aggmtranstype");
	i_aggmtransspace = dbms_md_get_field_subscript(res, "aggmtransspace");
	i_agginitval = dbms_md_get_field_subscript(res, "agginitval");
	i_aggminitval = dbms_md_get_field_subscript(res, "aggminitval");
	i_convertok = dbms_md_get_field_subscript(res, "convertok");
	i_proparallel = dbms_md_get_field_subscript(res, "proparallel");

	aggtransfn = dbms_md_get_field_value(res, 0, i_aggtransfn);
	aggfinalfn = dbms_md_get_field_value(res, 0, i_aggfinalfn);
	aggcombinefn = dbms_md_get_field_value(res, 0, i_aggcombinefn);
	aggserialfn = dbms_md_get_field_value(res, 0, i_aggserialfn);
	aggdeserialfn = dbms_md_get_field_value(res, 0, i_aggdeserialfn);
	aggmtransfn = dbms_md_get_field_value(res, 0, i_aggmtransfn);
	aggminvtransfn = dbms_md_get_field_value(res, 0, i_aggminvtransfn);
	aggmfinalfn = dbms_md_get_field_value(res, 0, i_aggmfinalfn);
	aggfinalextra = (dbms_md_get_field_value(res, 0, i_aggfinalextra)[0] == 't');
	aggmfinalextra = (dbms_md_get_field_value(res, 0, i_aggmfinalextra)[0] == 't');
	aggsortop = dbms_md_get_field_value(res, 0, i_aggsortop);
	hypothetical = (dbms_md_get_field_value(res, 0, i_hypothetical)[0] == 't');
	aggtranstype = dbms_md_get_field_value(res, 0, i_aggtranstype);
	aggtransspace = dbms_md_get_field_value(res, 0, i_aggtransspace);
	aggmtranstype = dbms_md_get_field_value(res, 0, i_aggmtranstype);
	aggmtransspace = dbms_md_get_field_value(res, 0, i_aggmtransspace);
	agginitval = dbms_md_get_field_value(res, 0, i_agginitval);
	aggminitval = dbms_md_get_field_value(res, 0, i_aggminitval);
	convertok = (dbms_md_get_field_value(res, 0, i_convertok)[0] == 't');

	if (fout->remoteVersion >= 80400)
	{
		/* 8.4 or later; we rely on server-side code for most of the work */
		char	   *funcargs;
		char	   *funciargs;

		funcargs = dbms_md_get_field_value(res, 0, dbms_md_get_field_subscript(res, "funcargs"));
		funciargs = dbms_md_get_field_value(res, 0, dbms_md_get_field_subscript(res, "funciargs"));
		aggfullsig = format_function_arguments(&agginfo->aggfn, funcargs, true);
		aggsig = format_function_arguments(&agginfo->aggfn, funciargs, true);
	}
	else
		/* pre-8.4, do it ourselves */
		aggsig = format_aggregate_signature(agginfo, fout, true);

	aggsig_tag = format_aggregate_signature(agginfo, fout, false);

	if (i_proparallel != -1)
		proparallel = dbms_md_get_field_value(res, 0, dbms_md_get_field_subscript(res, "proparallel"));
	else
		proparallel = NULL;

	if (!convertok)
	{
		write_msg(NULL, "WARNING: aggregate function %s could not be dumped correctly for this database version; ignored\n",
				  aggsig);

		if (aggfullsig)
			free(aggfullsig);

		free(aggsig);

		return;
	}

	/* regproc and regtype output is already sufficiently quoted */
	appendStringInfo(details, "    SFUNC = %s,\n    STYPE = %s",
					  aggtransfn, aggtranstype);

	if (strcmp(aggtransspace, "0") != 0)
	{
		appendStringInfo(details, ",\n    SSPACE = %s",
						  aggtransspace);
	}

	if (!dbms_md_is_tuple_field_null(res, 0, i_agginitval))
	{
		appendStringInfoString(details, ",\n    INITCOND = ");
		appendStringLiteralAH(details, agginitval, fout);
	}

	if (strcmp(aggfinalfn, "-") != 0)
	{
		appendStringInfo(details, ",\n    FINALFUNC = %s",
						  aggfinalfn);
		if (aggfinalextra)
			appendStringInfoString(details, ",\n    FINALFUNC_EXTRA");
	}

	if (strcmp(aggcombinefn, "-") != 0)
		appendStringInfo(details, ",\n    COMBINEFUNC = %s", aggcombinefn);

	if (strcmp(aggserialfn, "-") != 0)
		appendStringInfo(details, ",\n    SERIALFUNC = %s", aggserialfn);

	if (strcmp(aggdeserialfn, "-") != 0)
		appendStringInfo(details, ",\n    DESERIALFUNC = %s", aggdeserialfn);

	if (strcmp(aggmtransfn, "-") != 0)
	{
		appendStringInfo(details, ",\n    MSFUNC = %s,\n    MINVFUNC = %s,\n    MSTYPE = %s",
						  aggmtransfn,
						  aggminvtransfn,
						  aggmtranstype);
	}

	if (strcmp(aggmtransspace, "0") != 0)
	{
		appendStringInfo(details, ",\n    MSSPACE = %s",
						  aggmtransspace);
	}

	if (!dbms_md_is_tuple_field_null(res, 0, i_aggminitval))
	{
		appendStringInfoString(details, ",\n    MINITCOND = ");
		appendStringLiteralAH(details, aggminitval, fout);
	}

	if (strcmp(aggmfinalfn, "-") != 0)
	{
		appendStringInfo(details, ",\n    MFINALFUNC = %s",
						  aggmfinalfn);
		if (aggmfinalextra)
			appendStringInfoString(details, ",\n    MFINALFUNC_EXTRA");
	}

	aggsortconvop = convertOperatorReference(fout, aggsortop);
	if (aggsortconvop)
	{
		appendStringInfo(details, ",\n    SORTOP = %s",
						  aggsortconvop);
		free(aggsortconvop);
	}

	if (hypothetical)
		appendStringInfoString(details, ",\n    HYPOTHETICAL");

	if (proparallel != NULL && proparallel[0] != PROPARALLEL_UNSAFE)
	{
		if (proparallel[0] == PROPARALLEL_SAFE)
			appendStringInfoString(details, ",\n    PARALLEL = safe");
		else if (proparallel[0] == PROPARALLEL_RESTRICTED)
			appendStringInfoString(details, ",\n    PARALLEL = restricted");
		else if (proparallel[0] != PROPARALLEL_UNSAFE)
			elog(ERROR, "unrecognized proparallel value for function \"%s\"\n",
						  agginfo->aggfn.dobj.name);
	}

	/*
	 * DROP must be fully qualified in case same name appears in pg_catalog
	 */
	appendStringInfo(delq, "DROP AGGREGATE %s.%s;\n",
					  dbms_md_fmtId(agginfo->aggfn.dobj.namespace->dobj.name),
					  aggsig);

	appendStringInfo(q, "CREATE AGGREGATE %s (\n%s\n);\n",
					  aggfullsig ? aggfullsig : aggsig, details->data);

	appendStringInfo(labelq, "AGGREGATE %s", aggsig);

#if 0
	if (dopt->binary_upgrade)
		binary_upgrade_extension_member(q, &agginfo->aggfn.dobj, labelq->data);
#endif

	if (agginfo->aggfn.dobj.dump & DUMP_COMPONENT_DEFINITION)
		ArchiveEntry(fout, agginfo->aggfn.dobj.catId,
					 agginfo->aggfn.dobj.dumpId,
					 aggsig_tag,
					 agginfo->aggfn.dobj.namespace->dobj.name,
					 NULL,
					 agginfo->aggfn.rolname,
					 false, "AGGREGATE", SECTION_PRE_DATA,
					 q->data, delq->data, NULL,
					 NULL, 0,
					 NULL, NULL);

	/* Dump Aggregate Comments */
	if (agginfo->aggfn.dobj.dump & DUMP_COMPONENT_COMMENT)
		dumpComment(fout, labelq->data,
					agginfo->aggfn.dobj.namespace->dobj.name,
					agginfo->aggfn.rolname,
					agginfo->aggfn.dobj.catId, 0, agginfo->aggfn.dobj.dumpId);

	if (agginfo->aggfn.dobj.dump & DUMP_COMPONENT_SECLABEL)
		dumpSecLabel(fout, labelq->data,
					 agginfo->aggfn.dobj.namespace->dobj.name,
					 agginfo->aggfn.rolname,
					 agginfo->aggfn.dobj.catId, 0, agginfo->aggfn.dobj.dumpId);

	/*
	 * Since there is no GRANT ON AGGREGATE syntax, we have to make the ACL
	 * command look like a function's GRANT; in particular this affects the
	 * syntax for zero-argument aggregates and ordered-set aggregates.
	 */
	free(aggsig);
	pfree(aggsig_tag);

	aggsig = format_function_signature(fout, &agginfo->aggfn, true);
	aggsig_tag = format_function_signature(fout, &agginfo->aggfn, false);

	if (agginfo->aggfn.dobj.dump & DUMP_COMPONENT_ACL)
		dumpACL(fout, agginfo->aggfn.dobj.catId, agginfo->aggfn.dobj.dumpId,
				"FUNCTION",
				aggsig, NULL, aggsig_tag,
				agginfo->aggfn.dobj.namespace->dobj.name,
				agginfo->aggfn.rolname, agginfo->aggfn.proacl,
				agginfo->aggfn.rproacl,
				agginfo->aggfn.initproacl, agginfo->aggfn.initrproacl);

	free(aggsig);
	if (aggfullsig)
		free(aggfullsig);
	free(aggsig_tag);

	dbms_md_free_tuples(res);

	destroyStringInfo(query);
	destroyStringInfo(q);
	destroyStringInfo(delq);
	destroyStringInfo(labelq);
	destroyStringInfo(details);

	pfree(query);
	pfree(q);
	pfree(delq);
	pfree(labelq);
	pfree(details);
}

/*
 * dumpTSParser
 *	  write out a single text search parser
 */
static void
dumpTSParser(Archive *fout, TSParserInfo *prsinfo)
{
	DumpOptions *dopt = fout->dopt;
	StringInfo q;
	StringInfo delq;
	StringInfo labelq;

	/* Skip if not to be dumped */
	if (!prsinfo->dobj.dump || dopt->dataOnly)
		return;

	q = createStringInfo();
	delq = createStringInfo();
	labelq = createStringInfo();

	/* Make sure we are in proper schema */
	selectSourceSchema(fout, prsinfo->dobj.namespace->dobj.name);

	appendStringInfo(q, "CREATE TEXT SEARCH PARSER %s (\n",
					  dbms_md_fmtId_internal(prsinfo->dobj.name, true));

	appendStringInfo(q, "    START = %s,\n",
					  convertTSFunction(fout, prsinfo->prsstart));
	appendStringInfo(q, "    GETTOKEN = %s,\n",
					  convertTSFunction(fout, prsinfo->prstoken));
	appendStringInfo(q, "    END = %s,\n",
					  convertTSFunction(fout, prsinfo->prsend));
	if (prsinfo->prsheadline != InvalidOid)
		appendStringInfo(q, "    HEADLINE = %s,\n",
						  convertTSFunction(fout, prsinfo->prsheadline));
	appendStringInfo(q, "    LEXTYPES = %s );\n",
					  convertTSFunction(fout, prsinfo->prslextype));

	/*
	 * DROP must be fully qualified in case same name appears in pg_catalog
	 */
	appendStringInfo(delq, "DROP TEXT SEARCH PARSER %s",
					  dbms_md_fmtId(prsinfo->dobj.namespace->dobj.name));
	appendStringInfo(delq, ".%s;\n",
					  dbms_md_fmtId_internal(prsinfo->dobj.name, true));

	appendStringInfo(labelq, "TEXT SEARCH PARSER %s",
					  dbms_md_fmtId_internal(prsinfo->dobj.name, true));

#if 0
	if (dopt->binary_upgrade)
		binary_upgrade_extension_member(q, &prsinfo->dobj, labelq->data);
#endif

	if (prsinfo->dobj.dump & DUMP_COMPONENT_DEFINITION)
		ArchiveEntry(fout, prsinfo->dobj.catId, prsinfo->dobj.dumpId,
					 prsinfo->dobj.name,
					 prsinfo->dobj.namespace->dobj.name,
					 NULL,
					 "",
					 false, "TEXT SEARCH PARSER", SECTION_PRE_DATA,
					 q->data, delq->data, NULL,
					 NULL, 0,
					 NULL, NULL);

	/* Dump Parser Comments */
	if (prsinfo->dobj.dump & DUMP_COMPONENT_COMMENT)
		dumpComment(fout, labelq->data,
					prsinfo->dobj.namespace->dobj.name, "",
					prsinfo->dobj.catId, 0, prsinfo->dobj.dumpId);

	destroyStringInfo(q);
	destroyStringInfo(delq);
	destroyStringInfo(labelq);

	pfree(q);
	pfree(delq);
	pfree(labelq);
}

/*
 * dumpTSDictionary
 *	  write out a single text search dictionary
 */
static void
dumpTSDictionary(Archive *fout, TSDictInfo *dictinfo)
{
	DumpOptions *dopt = fout->dopt;
	StringInfo q;
	StringInfo delq;
	StringInfo labelq;
	StringInfo query;
	SPITupleTable   *res;
	char	   *nspname;
	char	   *tmplname;

	/* Skip if not to be dumped */
	if (!dictinfo->dobj.dump || dopt->dataOnly)
		return;

	q = createStringInfo();
	delq = createStringInfo();
	labelq = createStringInfo();
	query = createStringInfo();

	/* Fetch name and namespace of the dictionary's template */
	selectSourceSchema(fout, "pg_catalog");
	appendStringInfo(query, "SELECT nspname, tmplname "
					  "FROM pg_ts_template p, pg_namespace n "
					  "WHERE p.oid = '%u' AND n.oid = tmplnamespace",
					  dictinfo->dicttemplate);
	res = ExecuteSqlQueryForSingleRow(fout, query->data);
	nspname = dbms_md_get_field_value(res, 0, SPI_RES_FIRST_FIELD_NO);
	tmplname = dbms_md_get_field_value(res, 0, SPI_RES_FIRST_FIELD_NO + 1);

	/* Make sure we are in proper schema */
	selectSourceSchema(fout, dictinfo->dobj.namespace->dobj.name);

	appendStringInfo(q, "CREATE TEXT SEARCH DICTIONARY %s (\n",
					  dbms_md_fmtId_internal(dictinfo->dobj.name, true));

	appendStringInfoString(q, "    TEMPLATE = ");
	if (strcmp(nspname, dictinfo->dobj.namespace->dobj.name) != 0)
		appendStringInfo(q, "%s.", dbms_md_fmtId(nspname));
	appendStringInfoString(q, dbms_md_fmtId_internal(tmplname, true));

	dbms_md_free_tuples(res);

	/* the dictinitoption can be dumped straight into the command */
	if (dictinfo->dictinitoption)
		appendStringInfo(q, ",\n    %s", dictinfo->dictinitoption);

	appendStringInfoString(q, " );\n");

	/*
	 * DROP must be fully qualified in case same name appears in pg_catalog
	 */
	appendStringInfo(delq, "DROP TEXT SEARCH DICTIONARY %s",
					  dbms_md_fmtId(dictinfo->dobj.namespace->dobj.name));
	appendStringInfo(delq, ".%s;\n",
					  dbms_md_fmtId_internal(dictinfo->dobj.name, true));

	appendStringInfo(labelq, "TEXT SEARCH DICTIONARY %s",
					  dbms_md_fmtId_internal(dictinfo->dobj.name, true));

#if 0
	if (dopt->binary_upgrade)
		binary_upgrade_extension_member(q, &dictinfo->dobj, labelq->data);
#endif

	if (dictinfo->dobj.dump & DUMP_COMPONENT_DEFINITION)
		ArchiveEntry(fout, dictinfo->dobj.catId, dictinfo->dobj.dumpId,
					 dictinfo->dobj.name,
					 dictinfo->dobj.namespace->dobj.name,
					 NULL,
					 dictinfo->rolname,
					 false, "TEXT SEARCH DICTIONARY", SECTION_PRE_DATA,
					 q->data, delq->data, NULL,
					 NULL, 0,
					 NULL, NULL);

	/* Dump Dictionary Comments */
	if (dictinfo->dobj.dump & DUMP_COMPONENT_COMMENT)
		dumpComment(fout, labelq->data,
					dictinfo->dobj.namespace->dobj.name, dictinfo->rolname,
					dictinfo->dobj.catId, 0, dictinfo->dobj.dumpId);

	destroyStringInfo(q);
	destroyStringInfo(delq);
	destroyStringInfo(labelq);
	destroyStringInfo(query);

	pfree(query);
	pfree(q);
	pfree(delq);
	pfree(labelq);
}

/*
 * dumpTSTemplate
 *	  write out a single text search template
 */
static void
dumpTSTemplate(Archive *fout, TSTemplateInfo *tmplinfo)
{
	DumpOptions *dopt = fout->dopt;
	StringInfo q;
	StringInfo delq;
	StringInfo labelq;

	/* Skip if not to be dumped */
	if (!tmplinfo->dobj.dump || dopt->dataOnly)
		return;

	q = createStringInfo();
	delq = createStringInfo();
	labelq = createStringInfo();

	/* Make sure we are in proper schema */
	selectSourceSchema(fout, tmplinfo->dobj.namespace->dobj.name);

	appendStringInfo(q, "CREATE TEXT SEARCH TEMPLATE %s (\n",
					  dbms_md_fmtId_internal(tmplinfo->dobj.name, true));

	if (tmplinfo->tmplinit != InvalidOid)
		appendStringInfo(q, "    INIT = %s,\n",
						  convertTSFunction(fout, tmplinfo->tmplinit));
	appendStringInfo(q, "    LEXIZE = %s );\n",
					  convertTSFunction(fout, tmplinfo->tmpllexize));

	/*
	 * DROP must be fully qualified in case same name appears in pg_catalog
	 */
	appendStringInfo(delq, "DROP TEXT SEARCH TEMPLATE %s",
					  dbms_md_fmtId(tmplinfo->dobj.namespace->dobj.name));
	appendStringInfo(delq, ".%s;\n",
					  dbms_md_fmtId_internal(tmplinfo->dobj.name, true));

	appendStringInfo(labelq, "TEXT SEARCH TEMPLATE %s",
					  dbms_md_fmtId_internal(tmplinfo->dobj.name, true));

#if 0
	if (dopt->binary_upgrade)
		binary_upgrade_extension_member(q, &tmplinfo->dobj, labelq->data);
#endif

	if (tmplinfo->dobj.dump & DUMP_COMPONENT_DEFINITION)
		ArchiveEntry(fout, tmplinfo->dobj.catId, tmplinfo->dobj.dumpId,
					 tmplinfo->dobj.name,
					 tmplinfo->dobj.namespace->dobj.name,
					 NULL,
					 "",
					 false, "TEXT SEARCH TEMPLATE", SECTION_PRE_DATA,
					 q->data, delq->data, NULL,
					 NULL, 0,
					 NULL, NULL);

	/* Dump Template Comments */
	if (tmplinfo->dobj.dump & DUMP_COMPONENT_COMMENT)
		dumpComment(fout, labelq->data,
					tmplinfo->dobj.namespace->dobj.name, "",
					tmplinfo->dobj.catId, 0, tmplinfo->dobj.dumpId);

	destroyStringInfo(q);
	destroyStringInfo(delq);
	destroyStringInfo(labelq);

	pfree(q);
	pfree(delq);
	pfree(labelq);
}

/*
 * dumpTSConfig
 *	  write out a single text search configuration
 */
static void
dumpTSConfig(Archive *fout, TSConfigInfo *cfginfo)
{
	DumpOptions *dopt = fout->dopt;
	StringInfo q;
	StringInfo delq;
	StringInfo labelq;
	StringInfo query;
	SPITupleTable   *res;
	char	   *nspname;
	char	   *prsname;
	int			ntups,
				i;
	int			i_tokenname;
	int			i_dictname;

	/* Skip if not to be dumped */
	if (!cfginfo->dobj.dump || dopt->dataOnly)
		return;

	q = createStringInfo();
	delq = createStringInfo();
	labelq = createStringInfo();
	query = createStringInfo();

	/* Fetch name and namespace of the config's parser */
	selectSourceSchema(fout, "pg_catalog");
	appendStringInfo(query, "SELECT nspname, prsname "
					  "FROM pg_ts_parser p, pg_namespace n "
					  "WHERE p.oid = '%u' AND n.oid = prsnamespace",
					  cfginfo->cfgparser);
	res = ExecuteSqlQueryForSingleRow(fout, query->data);
	nspname = dbms_md_get_field_value(res, 0, SPI_RES_FIRST_FIELD_NO);
	prsname = dbms_md_get_field_value(res, 0, SPI_RES_FIRST_FIELD_NO + 1);

	/* Make sure we are in proper schema */
	selectSourceSchema(fout, cfginfo->dobj.namespace->dobj.name);

	appendStringInfo(q, "CREATE TEXT SEARCH CONFIGURATION %s (\n",
					  dbms_md_fmtId_internal(cfginfo->dobj.name, true));

	appendStringInfoString(q, "    PARSER = ");
	if (strcmp(nspname, cfginfo->dobj.namespace->dobj.name) != 0)
		appendStringInfo(q, "%s.", dbms_md_fmtId(nspname));
	appendStringInfo(q, "%s );\n", dbms_md_fmtId_internal(prsname, true));

	dbms_md_free_tuples(res);

	resetStringInfo(query);
	appendStringInfo(query,
					  "SELECT\n"
					  "  ( SELECT alias FROM pg_catalog.ts_token_type('%u'::pg_catalog.oid) AS t\n"
					  "    WHERE t.tokid = m.maptokentype ) AS tokenname,\n"
					  "  m.mapdict::pg_catalog.regdictionary AS dictname\n"
					  "FROM pg_catalog.pg_ts_config_map AS m\n"
					  "WHERE m.mapcfg = '%u'\n"
					  "ORDER BY m.mapcfg, m.maptokentype, m.mapseqno",
					  cfginfo->cfgparser, cfginfo->dobj.catId.oid);

	res = ExecuteSqlQuery(fout, query->data);
	ntups = dbms_md_get_tuple_num(res);

	i_tokenname = dbms_md_get_field_subscript(res, "tokenname");
	i_dictname = dbms_md_get_field_subscript(res, "dictname");

	for (i = 0; i < ntups; i++)
	{
		char	   *tokenname = dbms_md_get_field_value(res, i, i_tokenname);
		char	   *dictname = dbms_md_get_field_value(res, i, i_dictname);

		if (i == 0 ||
			strcmp(tokenname, dbms_md_get_field_value(res, i - 1, i_tokenname)) != 0)
		{
			/* starting a new token type, so start a new command */
			if (i > 0)
				appendStringInfoString(q, ";\n");
			appendStringInfo(q, "\nALTER TEXT SEARCH CONFIGURATION %s\n",
							  dbms_md_fmtId_internal(cfginfo->dobj.name, true));
			/* tokenname needs quoting, dictname does NOT */
			appendStringInfo(q, "    ADD MAPPING FOR %s WITH %s",
							  dbms_md_fmtId_internal(tokenname, true), dictname);
		}
		else
			appendStringInfo(q, ", %s", dictname);
	}

	if (ntups > 0)
		appendStringInfoString(q, ";\n");

	dbms_md_free_tuples(res);

	/*
	 * DROP must be fully qualified in case same name appears in pg_catalog
	 */
	appendStringInfo(delq, "DROP TEXT SEARCH CONFIGURATION %s",
					  dbms_md_fmtId(cfginfo->dobj.namespace->dobj.name));
	appendStringInfo(delq, ".%s;\n",
					  dbms_md_fmtId_internal(cfginfo->dobj.name, true));

	appendStringInfo(labelq, "TEXT SEARCH CONFIGURATION %s",
					  dbms_md_fmtId_internal(cfginfo->dobj.name, true));

#if 0
	if (dopt->binary_upgrade)
		binary_upgrade_extension_member(q, &cfginfo->dobj, labelq->data);
#endif

	if (cfginfo->dobj.dump & DUMP_COMPONENT_DEFINITION)
		ArchiveEntry(fout, cfginfo->dobj.catId, cfginfo->dobj.dumpId,
					 cfginfo->dobj.name,
					 cfginfo->dobj.namespace->dobj.name,
					 NULL,
					 cfginfo->rolname,
					 false, "TEXT SEARCH CONFIGURATION", SECTION_PRE_DATA,
					 q->data, delq->data, NULL,
					 NULL, 0,
					 NULL, NULL);

	/* Dump Configuration Comments */
	if (cfginfo->dobj.dump & DUMP_COMPONENT_COMMENT)
		dumpComment(fout, labelq->data,
					cfginfo->dobj.namespace->dobj.name, cfginfo->rolname,
					cfginfo->dobj.catId, 0, cfginfo->dobj.dumpId);

	destroyStringInfo(q);
	destroyStringInfo(delq);
	destroyStringInfo(labelq);
	destroyStringInfo(query);

	pfree(query);
	pfree(q);
	pfree(delq);
	pfree(labelq);
}

/*
 * dumpForeignDataWrapper
 *	  write out a single foreign-data wrapper definition
 */
static void
dumpForeignDataWrapper(Archive *fout, FdwInfo *fdwinfo)
{
	DumpOptions *dopt = fout->dopt;
	StringInfo q;
	StringInfo delq;
	StringInfo labelq;
	char	   *qfdwname;

	/* Skip if not to be dumped */
	if (!fdwinfo->dobj.dump || dopt->dataOnly)
		return;

	q = createStringInfo();
	delq = createStringInfo();
	labelq = createStringInfo();

	qfdwname = dbms_md_strdup(dbms_md_fmtId(fdwinfo->dobj.name));

	appendStringInfo(q, "CREATE FOREIGN DATA WRAPPER %s",
					  qfdwname);

	if (strcmp(fdwinfo->fdwhandler, "-") != 0)
		appendStringInfo(q, " HANDLER %s", fdwinfo->fdwhandler);

	if (strcmp(fdwinfo->fdwvalidator, "-") != 0)
		appendStringInfo(q, " VALIDATOR %s", fdwinfo->fdwvalidator);

	if (strlen(fdwinfo->fdwoptions) > 0)
		appendStringInfo(q, " OPTIONS (\n    %s\n)", fdwinfo->fdwoptions);

	appendStringInfoString(q, ";\n");

	appendStringInfo(delq, "DROP FOREIGN DATA WRAPPER %s;\n",
					  qfdwname);

	appendStringInfo(labelq, "FOREIGN DATA WRAPPER %s",
					  qfdwname);

#if 0
	if (dopt->binary_upgrade)
		binary_upgrade_extension_member(q, &fdwinfo->dobj, labelq->data);
#endif

	if (fdwinfo->dobj.dump & DUMP_COMPONENT_DEFINITION)
		ArchiveEntry(fout, fdwinfo->dobj.catId, fdwinfo->dobj.dumpId,
					 fdwinfo->dobj.name,
					 NULL,
					 NULL,
					 fdwinfo->rolname,
					 false, "FOREIGN DATA WRAPPER", SECTION_PRE_DATA,
					 q->data, delq->data, NULL,
					 NULL, 0,
					 NULL, NULL);

	/* Handle the ACL */
	if (fdwinfo->dobj.dump & DUMP_COMPONENT_ACL)
		dumpACL(fout, fdwinfo->dobj.catId, fdwinfo->dobj.dumpId,
				"FOREIGN DATA WRAPPER",
				qfdwname, NULL, fdwinfo->dobj.name,
				NULL, fdwinfo->rolname,
				fdwinfo->fdwacl, fdwinfo->rfdwacl,
				fdwinfo->initfdwacl, fdwinfo->initrfdwacl);

	/* Dump Foreign Data Wrapper Comments */
	if (fdwinfo->dobj.dump & DUMP_COMPONENT_COMMENT)
		dumpComment(fout, labelq->data,
					NULL, fdwinfo->rolname,
					fdwinfo->dobj.catId, 0, fdwinfo->dobj.dumpId);

	free(qfdwname);

	destroyStringInfo(q);
	destroyStringInfo(delq);
	destroyStringInfo(labelq);

	pfree(q);
	pfree(delq);
	pfree(labelq);
}

/*
 * dumpForeignServer
 *	  write out a foreign server definition
 */
static void
dumpForeignServer(Archive *fout, ForeignServerInfo *srvinfo)
{
	DumpOptions *dopt = fout->dopt;
	StringInfo q;
	StringInfo delq;
	StringInfo labelq;
	StringInfo query;
	SPITupleTable   *res;
	char	   *qsrvname;
	char	   *fdwname;

	/* Skip if not to be dumped */
	if (!srvinfo->dobj.dump || dopt->dataOnly)
		return;

	q = createStringInfo();
	delq = createStringInfo();
	labelq = createStringInfo();
	query = createStringInfo();

	qsrvname = dbms_md_strdup(dbms_md_fmtId(srvinfo->dobj.name));

	/* look up the foreign-data wrapper */
	selectSourceSchema(fout, "pg_catalog");
	appendStringInfo(query, "SELECT fdwname "
					  "FROM pg_foreign_data_wrapper w "
					  "WHERE w.oid = '%u'",
					  srvinfo->srvfdw);
	res = ExecuteSqlQueryForSingleRow(fout, query->data);
	fdwname = dbms_md_get_field_value(res, 0, SPI_RES_FIRST_FIELD_NO);

	appendStringInfo(q, "CREATE SERVER %s", qsrvname);
	if (srvinfo->srvtype && strlen(srvinfo->srvtype) > 0)
	{
		appendStringInfoString(q, " TYPE ");
		appendStringLiteralAH(q, srvinfo->srvtype, fout);
	}
	if (srvinfo->srvversion && strlen(srvinfo->srvversion) > 0)
	{
		appendStringInfoString(q, " VERSION ");
		appendStringLiteralAH(q, srvinfo->srvversion, fout);
	}

	appendStringInfoString(q, " FOREIGN DATA WRAPPER ");
	appendStringInfoString(q, dbms_md_fmtId(fdwname));

	if (srvinfo->srvoptions && strlen(srvinfo->srvoptions) > 0)
		appendStringInfo(q, " OPTIONS (\n    %s\n)", srvinfo->srvoptions);

	appendStringInfoString(q, ";\n");

	appendStringInfo(delq, "DROP SERVER %s;\n",
					  qsrvname);

	appendStringInfo(labelq, "SERVER %s", qsrvname);

#if 0
	if (dopt->binary_upgrade)
		binary_upgrade_extension_member(q, &srvinfo->dobj, labelq->data);
#endif

	if (srvinfo->dobj.dump & DUMP_COMPONENT_DEFINITION)
		ArchiveEntry(fout, srvinfo->dobj.catId, srvinfo->dobj.dumpId,
					 srvinfo->dobj.name,
					 NULL,
					 NULL,
					 srvinfo->rolname,
					 false, "SERVER", SECTION_PRE_DATA,
					 q->data, delq->data, NULL,
					 NULL, 0,
					 NULL, NULL);

	/* Handle the ACL */
	if (srvinfo->dobj.dump & DUMP_COMPONENT_ACL)
		dumpACL(fout, srvinfo->dobj.catId, srvinfo->dobj.dumpId,
				"FOREIGN SERVER",
				qsrvname, NULL, srvinfo->dobj.name,
				NULL, srvinfo->rolname,
				srvinfo->srvacl, srvinfo->rsrvacl,
				srvinfo->initsrvacl, srvinfo->initrsrvacl);

	/* Dump user mappings */
	if (srvinfo->dobj.dump & DUMP_COMPONENT_USERMAP)
		dumpUserMappings(fout,
						 srvinfo->dobj.name, NULL,
						 srvinfo->rolname,
						 srvinfo->dobj.catId, srvinfo->dobj.dumpId);

	/* Dump Foreign Server Comments */
	if (srvinfo->dobj.dump & DUMP_COMPONENT_COMMENT)
		dumpComment(fout, labelq->data,
					NULL, srvinfo->rolname,
					srvinfo->dobj.catId, 0, srvinfo->dobj.dumpId);

	free(qsrvname);

	destroyStringInfo(q);
	destroyStringInfo(delq);
	destroyStringInfo(labelq);
	destroyStringInfo(query);

	pfree(query);
	pfree(q);
	pfree(delq);
	pfree(labelq);
}

/*
 * dumpUserMappings
 *
 * This routine is used to dump any user mappings associated with the
 * server handed to this routine. Should be called after ArchiveEntry()
 * for the server.
 */
static void
dumpUserMappings(Archive *fout,
				 const char *servername, const char *namespace,
				 const char *owner,
				 CatalogId catalogId, DumpId dumpId)
{
	StringInfo q;
	StringInfo delq;
	StringInfo query;
	StringInfo tag;
	SPITupleTable   *res;
	int			ntups;
	int			i_usename;
	int			i_umoptions;
	int			i;

	q = createStringInfo();
	tag = createStringInfo();
	delq = createStringInfo();
	query = createStringInfo();

	/*
	 * We read from the publicly accessible view pg_user_mappings, so as not
	 * to fail if run by a non-superuser.  Note that the view will show
	 * umoptions as null if the user hasn't got privileges for the associated
	 * server; this means that pg_dump will dump such a mapping, but with no
	 * OPTIONS clause.  A possible alternative is to skip such mappings
	 * altogether, but it's not clear that that's an improvement.
	 */
	selectSourceSchema(fout, "pg_catalog");

	appendStringInfo(query,
					  "SELECT usename, "
					  "array_to_string(ARRAY("
					  "SELECT quote_ident(option_name) || ' ' || "
					  "quote_literal(option_value) "
					  "FROM pg_options_to_table(umoptions) "
					  "ORDER BY option_name"
					  "), E',\n    ') AS umoptions "
					  "FROM pg_user_mappings "
					  "WHERE srvid = '%u' "
					  "ORDER BY usename",
					  catalogId.oid);

	res = ExecuteSqlQuery(fout, query->data);

	ntups = dbms_md_get_tuple_num(res);
	i_usename = dbms_md_get_field_subscript(res, "usename");
	i_umoptions = dbms_md_get_field_subscript(res, "umoptions");

	for (i = 0; i < ntups; i++)
	{
		char	   *usename;
		char	   *umoptions;

		usename = dbms_md_get_field_value(res, i, i_usename);
		umoptions = dbms_md_get_field_value(res, i, i_umoptions);

		resetStringInfo(q);
		appendStringInfo(q, "CREATE USER MAPPING FOR %s", dbms_md_fmtId(usename));
		appendStringInfo(q, " SERVER %s", dbms_md_fmtId(servername));

		if (umoptions && strlen(umoptions) > 0)
			appendStringInfo(q, " OPTIONS (\n    %s\n)", umoptions);

		appendStringInfoString(q, ";\n");

		resetStringInfo(delq);
		appendStringInfo(delq, "DROP USER MAPPING FOR %s", dbms_md_fmtId(usename));
		appendStringInfo(delq, " SERVER %s;\n", dbms_md_fmtId(servername));

		resetStringInfo(tag);
		appendStringInfo(tag, "USER MAPPING %s SERVER %s",
						  usename, servername);

		ArchiveEntry(fout, nilCatalogId, createDumpId(fout),
					 tag->data,
					 namespace,
					 NULL,
					 owner, false,
					 "USER MAPPING", SECTION_PRE_DATA,
					 q->data, delq->data, NULL,
					 &dumpId, 1,
					 NULL, NULL);
	}

	dbms_md_free_tuples(res);

	destroyStringInfo(query);
	destroyStringInfo(delq);
	destroyStringInfo(tag);
	destroyStringInfo(q);

	pfree(query);
	pfree(q);
	pfree(delq);
	pfree(tag);
}

/*
 * Write out default privileges information
 */
static void
dumpDefaultACL(Archive *fout, DefaultACLInfo *daclinfo)
{
	DumpOptions *dopt = fout->dopt;
	StringInfo q;
	StringInfo tag;
	const char *type;

	/* Skip if not to be dumped */
	if (!daclinfo->dobj.dump || dopt->dataOnly || dopt->aclsSkip)
		return;

	q = createStringInfo();
	tag = createStringInfo();

	switch (daclinfo->defaclobjtype)
	{
		case DEFACLOBJ_RELATION:
			type = "TABLES";
			break;
		case DEFACLOBJ_SEQUENCE:
			type = "SEQUENCES";
			break;
		case DEFACLOBJ_FUNCTION:
			type = "FUNCTIONS";
			break;
		case DEFACLOBJ_TYPE:
			type = "TYPES";
			break;
		case DEFACLOBJ_NAMESPACE:
			type = "SCHEMAS";
			break;
		default:
			/* shouldn't get here */
			elog(ERROR,
						  "unrecognized object type in default privileges: %d\n",
						  (int) daclinfo->defaclobjtype);
			type = "";			/* keep compiler quiet */
	}

	appendStringInfo(tag, "DEFAULT PRIVILEGES FOR %s", type);

	/* build the actual command(s) for this tuple */
	if (!buildDefaultACLCommands(type,
								 daclinfo->dobj.namespace != NULL ?
								 daclinfo->dobj.namespace->dobj.name : NULL,
								 daclinfo->defaclacl,
								 daclinfo->rdefaclacl,
								 daclinfo->initdefaclacl,
								 daclinfo->initrdefaclacl,
								 daclinfo->defaclrole,
								 fout->remoteVersion,
								 q))
		elog(ERROR, "could not parse default ACL list (%s)\n",
					  daclinfo->defaclacl);

	if (daclinfo->dobj.dump & DUMP_COMPONENT_ACL)
		ArchiveEntry(fout, daclinfo->dobj.catId, daclinfo->dobj.dumpId,
					 tag->data,
					 daclinfo->dobj.namespace ? daclinfo->dobj.namespace->dobj.name : NULL,
					 NULL,
					 daclinfo->defaclrole,
					 false, "DEFAULT ACL", SECTION_POST_DATA,
					 q->data, "", NULL,
					 NULL, 0,
					 NULL, NULL);

	destroyStringInfo(tag);
	destroyStringInfo(q);

	pfree(tag);
	pfree(q);
}

/*----------
 * Write out grant/revoke information
 *
 * 'objCatId' is the catalog ID of the underlying object.
 * 'objDumpId' is the dump ID of the underlying object.
 * 'type' must be one of
 *		TABLE, SEQUENCE, FUNCTION, LANGUAGE, SCHEMA, DATABASE, TABLESPACE,
 *		FOREIGN DATA WRAPPER, SERVER, or LARGE OBJECT.
 * 'name' is the formatted name of the object.  Must be quoted etc. already.
 * 'subname' is the formatted name of the sub-object, if any.  Must be quoted.
 * 'tag' is the tag for the archive entry (typ. unquoted name of object).
 * 'nspname' is the namespace the object is in (NULL if none).
 * 'owner' is the owner, NULL if there is no owner (for languages).
 * 'acls' contains the ACL string of the object from the appropriate system
 * 		catalog field; it will be passed to buildACLCommands for building the
 * 		appropriate GRANT commands.
 * 'racls' contains the ACL string of any initial-but-now-revoked ACLs of the
 * 		object; it will be passed to buildACLCommands for building the
 * 		appropriate REVOKE commands.
 * 'initacls' In binary-upgrade mode, ACL string of the object's initial
 * 		privileges, to be recorded into pg_init_privs
 * 'initracls' In binary-upgrade mode, ACL string of the object's
 * 		revoked-from-default privileges, to be recorded into pg_init_privs
 *
 * NB: initacls/initracls are needed because extensions can set privileges on
 * an object during the extension's script file and we record those into
 * pg_init_privs as that object's initial privileges.
 *----------
 */
static void
dumpACL(Archive *fout, CatalogId objCatId, DumpId objDumpId,
		const char *type, const char *name, const char *subname,
		const char *tag, const char *nspname, const char *owner,
		const char *acls, const char *racls,
		const char *initacls, const char *initracls)
{
	DumpOptions *dopt = fout->dopt;
	StringInfo sql;

	/* Do nothing if ACL dump is not enabled */
	if (dopt->aclsSkip)
		return;

	/* --data-only skips ACLs *except* BLOB ACLs */
	if (dopt->dataOnly && strcmp(type, "LARGE OBJECT") != 0)
		return;

	sql = createStringInfo();

	/*
	 * Check to see if this object has had any initial ACLs included for it.
	 * If so, we are in binary upgrade mode and these are the ACLs to turn
	 * into GRANT and REVOKE statements to set and record the initial
	 * privileges for an extension object.  Let the backend know that these
	 * are to be recorded by calling binary_upgrade_set_record_init_privs()
	 * before and after.
	 */
	if (strlen(initacls) != 0 || strlen(initracls) != 0)
	{
		appendStringInfo(sql, "SELECT pg_catalog.binary_upgrade_set_record_init_privs(true);\n");
		if (!buildACLCommands(name, subname, type, initacls, initracls, owner,
							  "", fout->remoteVersion, sql))
			elog(ERROR,
						  "could not parse initial GRANT ACL list (%s) or initial REVOKE ACL list (%s) for object \"%s\" (%s)\n",
						  initacls, initracls, name, type);
		appendStringInfo(sql, "SELECT pg_catalog.binary_upgrade_set_record_init_privs(false);\n");
	}

	if (!buildACLCommands(name, subname, type, acls, racls, owner,
						  "", fout->remoteVersion, sql))
		elog(ERROR,
					  "could not parse GRANT ACL list (%s) or REVOKE ACL list (%s) for object \"%s\" (%s)\n",
					  acls, racls, name, type);

	if (sql->len > 0)
		ArchiveEntry(fout, nilCatalogId, createDumpId(fout),
					 tag, nspname,
					 NULL,
					 owner ? owner : "",
					 false, "ACL", SECTION_NONE,
					 sql->data, "", NULL,
					 &(objDumpId), 1,
					 NULL, NULL);

	destroyStringInfo(sql);
	pfree(sql);
}

/*
 * dumpSecLabel
 *
 * This routine is used to dump any security labels associated with the
 * object handed to this routine. The routine takes a constant character
 * string for the target part of the security-label command, plus
 * the namespace and owner of the object (for labeling the ArchiveEntry),
 * plus catalog ID and subid which are the lookup key for pg_seclabel,
 * plus the dump ID for the object (for setting a dependency).
 * If a matching pg_seclabel entry is found, it is dumped.
 *
 * Note: although this routine takes a dumpId for dependency purposes,
 * that purpose is just to mark the dependency in the emitted dump file
 * for possible future use by pg_restore.  We do NOT use it for determining
 * ordering of the label in the dump file, because this routine is called
 * after dependency sorting occurs.  This routine should be called just after
 * calling ArchiveEntry() for the specified object.
 */
static void
dumpSecLabel(Archive *fout, const char *target,
			 const char *namespace, const char *owner,
			 CatalogId catalogId, int subid, DumpId dumpId)
{
	DumpOptions *dopt = fout->dopt;
	SecLabelItem *labels;
	int			nlabels;
	int			i;
	StringInfo query;

	/* do nothing, if --no-security-labels is supplied */
	if (dopt->no_security_labels)
		return;

	/* Comments are schema not data ... except blob comments are data */
	if (strncmp(target, "LARGE OBJECT ", 13) != 0)
	{
		if (dopt->dataOnly)
			return;
	}
	else
	{
		/* We do dump blob security labels in binary-upgrade mode */
		if (dopt->schemaOnly && !dopt->binary_upgrade)
			return;
	}

	/* Search for security labels associated with catalogId, using table */
	nlabels = findSecLabels(fout, catalogId.tableoid, catalogId.oid, &labels);

	query = createStringInfo();

	for (i = 0; i < nlabels; i++)
	{
		/*
		 * Ignore label entries for which the subid doesn't match.
		 */
		if (labels[i].objsubid != subid)
			continue;

		appendStringInfo(query,
						  "SECURITY LABEL FOR %s ON %s IS ",
						  dbms_md_fmtId_internal(labels[i].provider, true), target);
		appendStringLiteralAH(query, labels[i].label, fout);
		appendStringInfoString(query, ";\n");
	}

	if (query->len > 0)
	{
		ArchiveEntry(fout, nilCatalogId, createDumpId(fout),
					 target, namespace, NULL, owner,
					 false, "SECURITY LABEL", SECTION_NONE,
					 query->data, "", NULL,
					 &(dumpId), 1,
					 NULL, NULL);
	}
	
	destroyStringInfo(query);
	pfree(query);
}

/*
 * dumpTableSecLabel
 *
 * As above, but dump security label for both the specified table (or view)
 * and its columns.
 */
static void
dumpTableSecLabel(Archive *fout, TableInfo *tbinfo, const char *reltypename)
{
	DumpOptions *dopt = fout->dopt;
	SecLabelItem *labels;
	int			nlabels;
	int			i;
	StringInfo query;
	StringInfo target;

	/* do nothing, if --no-security-labels is supplied */
	if (dopt->no_security_labels)
		return;

	/* SecLabel are SCHEMA not data */
	if (dopt->dataOnly)
		return;

	/* Search for comments associated with relation, using table */
	nlabels = findSecLabels(fout,
							tbinfo->dobj.catId.tableoid,
							tbinfo->dobj.catId.oid,
							&labels);

	/* If security labels exist, build SECURITY LABEL statements */
	if (nlabels <= 0)
		return;

	query = createStringInfo();
	target = createStringInfo();

	for (i = 0; i < nlabels; i++)
	{
		const char *colname;
		const char *provider = labels[i].provider;
		const char *label = labels[i].label;
		int			objsubid = labels[i].objsubid;

		resetStringInfo(target);
		if (objsubid == 0)
		{
			appendStringInfo(target, "%s %s", reltypename,
							  dbms_md_fmtId(tbinfo->dobj.name));
		}
		else
		{
			colname = getAttrName(objsubid, tbinfo);
			/* first dbms_md_fmtId result must be consumed before calling it again */
			appendStringInfo(target, "COLUMN %s", dbms_md_fmtId(tbinfo->dobj.name));
			appendStringInfo(target, ".%s", dbms_md_fmtId(colname));
		}
		appendStringInfo(query, "SECURITY LABEL FOR %s ON %s IS ",
						  dbms_md_fmtId_internal(provider, true), target->data);
		appendStringLiteralAH(query, label, fout);
		appendStringInfoString(query, ";\n");
	}
	if (query->len > 0)
	{
		resetStringInfo(target);
		appendStringInfo(target, "%s %s", reltypename,
						  dbms_md_fmtId(tbinfo->dobj.name));
		ArchiveEntry(fout, nilCatalogId, createDumpId(fout),
					 target->data,
					 tbinfo->dobj.namespace->dobj.name,
					 NULL, tbinfo->rolname,
					 false, "SECURITY LABEL", SECTION_NONE,
					 query->data, "", NULL,
					 &(tbinfo->dobj.dumpId), 1,
					 NULL, NULL);
	}

	destroyStringInfo(query);
	destroyStringInfo(target);
	pfree(query);
	pfree(target);
	
}

/*
 * findSecLabels
 *
 * Find the security label(s), if any, associated with the given object.
 * All the objsubid values associated with the given classoid/objoid are
 * found with one search.
 */
static int
findSecLabels(Archive *fout, Oid classoid, Oid objoid, SecLabelItem **items)
{
	/* static storage for table of security labels */
	static SecLabelItem *labels = NULL;
	static int	nlabels = -1;

	SecLabelItem *middle = NULL;
	SecLabelItem *low;
	SecLabelItem *high;
	int			nmatch;

	/* Get security labels if we didn't already */
	if (nlabels < 0)
		nlabels = collectSecLabels(fout, &labels);

	if (nlabels <= 0)			/* no labels, so no match is possible */
	{
		*items = NULL;
		return 0;
	}

	/*
	 * Do binary search to find some item matching the object.
	 */
	low = &labels[0];
	high = &labels[nlabels - 1];
	while (low <= high)
	{
		middle = low + (high - low) / 2;

		if (classoid < middle->classoid)
			high = middle - 1;
		else if (classoid > middle->classoid)
			low = middle + 1;
		else if (objoid < middle->objoid)
			high = middle - 1;
		else if (objoid > middle->objoid)
			low = middle + 1;
		else
			break;				/* found a match */
	}

	if (low > high)				/* no matches */
	{
		*items = NULL;
		return 0;
	}

	/*
	 * Now determine how many items match the object.  The search loop
	 * invariant still holds: only items between low and high inclusive could
	 * match.
	 */
	nmatch = 1;
	while (middle > low)
	{
		if (classoid != middle[-1].classoid ||
			objoid != middle[-1].objoid)
			break;
		middle--;
		nmatch++;
	}

	*items = middle;

	middle += nmatch;
	while (middle <= high)
	{
		if (classoid != middle->classoid ||
			objoid != middle->objoid)
			break;
		middle++;
		nmatch++;
	}

	return nmatch;
}

/*
 * collectSecLabels
 *
 * Construct a table of all security labels available for database objects.
 * It's much faster to pull them all at once.
 *
 * The table is sorted by classoid/objid/objsubid for speed in lookup.
 */
static int
collectSecLabels(Archive *fout, SecLabelItem **items)
{
	SPITupleTable   *res;
	StringInfo query;
	int			i_label;
	int			i_provider;
	int			i_classoid;
	int			i_objoid;
	int			i_objsubid;
	int			ntups;
	int			i;
	SecLabelItem *labels;

	query = createStringInfo();

	appendStringInfoString(query,
						 "SELECT label, provider, classoid, objoid, objsubid "
						 "FROM pg_catalog.pg_seclabel "
						 "ORDER BY classoid, objoid, objsubid");

	res = ExecuteSqlQuery(fout, query->data);

	/* Construct lookup table containing OIDs in numeric form */
	i_label = dbms_md_get_field_subscript(res, "label");
	i_provider = dbms_md_get_field_subscript(res, "provider");
	i_classoid = dbms_md_get_field_subscript(res, "classoid");
	i_objoid = dbms_md_get_field_subscript(res, "objoid");
	i_objsubid = dbms_md_get_field_subscript(res, "objsubid");

	ntups = dbms_md_get_tuple_num(res);

	labels = (SecLabelItem *) dbms_md_malloc0(fout->memory_ctx, ntups * sizeof(SecLabelItem));

	for (i = 0; i < ntups; i++)
	{
		labels[i].label = dbms_md_get_field_value(res, i, i_label);
		labels[i].provider = dbms_md_get_field_value(res, i, i_provider);
		labels[i].classoid = dbms_md_str_to_oid(dbms_md_get_field_value(res, i, i_classoid));
		labels[i].objoid = dbms_md_str_to_oid(dbms_md_get_field_value(res, i, i_objoid));
		labels[i].objsubid = atoi(dbms_md_get_field_value(res, i, i_objsubid));
	}

	/* Do NOT free the SPITupleTable since we are keeping pointers into it */
	destroyStringInfo(query);
	pfree(query);

	*items = labels;
	return ntups;
}

/*
 * dumpTable
 *	  write out to fout the declarations (not data) of a user-defined table
 */
static void
dumpTable(Archive *fout, TableInfo *tbinfo)
{
	DumpOptions *dopt = fout->dopt;
	char	   *namecopy;

	/*
	 * noop if we are not dumping anything about this table, or if we are
	 * doing a data-only dump
	 */
	if (!tbinfo->dobj.dump || dopt->dataOnly)
		return;

	if (tbinfo->relkind == RELKIND_SEQUENCE)
		dumpSequence(fout, tbinfo);
	else
#ifdef __OPENTENBASE__
	{
		if(tbinfo->parttype == 'c')
		{
			return;
		}
#endif
		dumpTableSchema(fout, tbinfo);
#ifdef __OPENTENBASE__
	}
#endif
	/* Handle the ACL here */
	namecopy = dbms_md_strdup(dbms_md_fmtId(tbinfo->dobj.name));
	if (tbinfo->dobj.dump & DUMP_COMPONENT_ACL)
		dumpACL(fout, tbinfo->dobj.catId, tbinfo->dobj.dumpId,
				(tbinfo->relkind == RELKIND_SEQUENCE) ? "SEQUENCE" :
				"TABLE",
				namecopy, NULL, tbinfo->dobj.name,
				tbinfo->dobj.namespace->dobj.name, tbinfo->rolname,
				tbinfo->relacl, tbinfo->rrelacl,
				tbinfo->initrelacl, tbinfo->initrrelacl);

	/*
	 * Handle column ACLs, if any.  Note: we pull these with a separate query
	 * rather than trying to fetch them during getTableAttrs, so that we won't
	 * miss ACLs on system columns.
	 */
	if (fout->remoteVersion >= 80400 && tbinfo->dobj.dump & DUMP_COMPONENT_ACL)
	{
		StringInfo query = createStringInfo();
		SPITupleTable   *res;
		int			i;

		if (fout->remoteVersion >= 90600)
		{
			StringInfo acl_subquery = createStringInfo();
			StringInfo racl_subquery = createStringInfo();
			StringInfo initacl_subquery = createStringInfo();
			StringInfo initracl_subquery = createStringInfo();

			buildACLQueries(acl_subquery, racl_subquery, initacl_subquery,
							initracl_subquery, "at.attacl", "c.relowner", "'c'",
							dopt->binary_upgrade);

			appendStringInfo(query,
							  "SELECT at.attname, "
							  "%s AS attacl, "
							  "%s AS rattacl, "
							  "%s AS initattacl, "
							  "%s AS initrattacl "
							  "FROM pg_catalog.pg_attribute at "
							  "JOIN pg_catalog.pg_class c ON (at.attrelid = c.oid) "
							  "LEFT JOIN pg_catalog.pg_init_privs pip ON "
							  "(at.attrelid = pip.objoid "
							  "AND pip.classoid = 'pg_catalog.pg_class'::pg_catalog.regclass "
							  "AND at.attnum = pip.objsubid) "
							  "WHERE at.attrelid = '%u'::pg_catalog.oid AND "
							  "NOT at.attisdropped "
							  "AND ("
							  "%s IS NOT NULL OR "
							  "%s IS NOT NULL OR "
							  "%s IS NOT NULL OR "
							  "%s IS NOT NULL)"
							  "ORDER BY at.attnum",
							  acl_subquery->data,
							  racl_subquery->data,
							  initacl_subquery->data,
							  initracl_subquery->data,
							  tbinfo->dobj.catId.oid,
							  acl_subquery->data,
							  racl_subquery->data,
							  initacl_subquery->data,
							  initracl_subquery->data);

			destroyStringInfo(acl_subquery);
			destroyStringInfo(racl_subquery);
			destroyStringInfo(initacl_subquery);
			destroyStringInfo(initracl_subquery);

			pfree(acl_subquery);
			pfree(racl_subquery);
			pfree(initacl_subquery);
			pfree(initracl_subquery);
		}
		else
		{
			appendStringInfo(query,
							  "SELECT attname, attacl, NULL as rattacl, "
							  "NULL AS initattacl, NULL AS initrattacl "
							  "FROM pg_catalog.pg_attribute "
							  "WHERE attrelid = '%u'::pg_catalog.oid AND NOT attisdropped "
							  "AND attacl IS NOT NULL "
							  "ORDER BY attnum",
							  tbinfo->dobj.catId.oid);
		}

		res = ExecuteSqlQuery(fout, query->data);

		for (i = 0; i < dbms_md_get_tuple_num(res); i++)
		{
			char	   *attname = dbms_md_get_field_value(res, i, SPI_RES_FIRST_FIELD_NO + 0);
			char	   *attacl = dbms_md_get_field_value(res, i, SPI_RES_FIRST_FIELD_NO + 1);
			char	   *rattacl = dbms_md_get_field_value(res, i, SPI_RES_FIRST_FIELD_NO + 2);
			char	   *initattacl = dbms_md_get_field_value(res, i, SPI_RES_FIRST_FIELD_NO + 3);
			char	   *initrattacl = dbms_md_get_field_value(res, i, SPI_RES_FIRST_FIELD_NO + 4);
			char	   *attnamecopy;
			char	   *acltag;

			attnamecopy = dbms_md_strdup(dbms_md_fmtId(attname));
			acltag = psprintf("%s.%s", tbinfo->dobj.name, attname);
			/* Column's GRANT type is always TABLE */
			dumpACL(fout, tbinfo->dobj.catId, tbinfo->dobj.dumpId, "TABLE",
					namecopy, attnamecopy, acltag,
					tbinfo->dobj.namespace->dobj.name, tbinfo->rolname,
					attacl, rattacl, initattacl, initrattacl);
			free(attnamecopy);
		}
		dbms_md_free_tuples(res);
		destroyStringInfo(query);
		pfree(query);
	}

	free(namecopy);

	return;
}

/*
 * Create the AS clause for a view or materialized view. The semicolon is
 * stripped because a materialized view must add a WITH NO DATA clause.
 *
 * This returns a new buffer which must be freed by the caller.
 */
static StringInfo
createViewAsClause(Archive *fout, TableInfo *tbinfo)
{
	StringInfo query = createStringInfo();
	StringInfo result = createStringInfo();
	SPITupleTable   *res;
	int			len;
	char *viewdef = NULL;

	/* Fetch the view definition */
	appendStringInfo(query,
					  "SELECT pg_catalog.pg_get_viewdef('%u'::pg_catalog.oid) AS viewdef",
					  tbinfo->dobj.catId.oid);

	res = ExecuteSqlQuery(fout, query->data);

	if (dbms_md_get_tuple_num(res) != 1)
	{
		if (dbms_md_get_tuple_num(res) < 1)
		{
			elog(INFO, "query to obtain definition of view \"%s\" returned no data\n",
						  tbinfo->dobj.name);
			return result;
		}
		else
		{
			elog(INFO, "query to obtain definition of view \"%s\" returned more than one definition\n",
						  tbinfo->dobj.name);
			return result;
		}
	}

	len = dbms_md_get_field_val_len(res, 0, SPI_RES_FIRST_FIELD_NO);

	if (len == 0)
	{
		elog(INFO, "definition of view \"%s\" appears to be empty (length zero)\n",
					  tbinfo->dobj.name);
		return result;
	}

	/* Strip off the trailing semicolon so that other things may follow. */
	viewdef = dbms_md_get_field_value(res, 0, SPI_RES_FIRST_FIELD_NO);
	Assert(viewdef[len - 1] == ';');
	appendBinaryStringInfo(result, viewdef, len - 1);

	dbms_md_free_tuples(res);
	destroyStringInfo(query);
	pfree(query);

	return result;
}

/*
 * Create a dummy AS clause for a view.  This is used when the real view
 * definition has to be postponed because of circular dependencies.
 * We must duplicate the view's external properties -- column names and types
 * (including collation) -- so that it works for subsequent references.
 *
 * This returns a new buffer which must be freed by the caller.
 */
static StringInfo
createDummyViewAsClause(Archive *fout, TableInfo *tbinfo)
{
	StringInfo result = createStringInfo();
	int			j;

	appendStringInfoString(result, "SELECT");

	for (j = 0; j < tbinfo->numatts; j++)
	{
		if (j > 0)
			appendStringInfoChar(result, ',');
		appendStringInfoString(result, "\n    ");

		appendStringInfo(result, "NULL::%s", tbinfo->atttypnames[j]);

		/*
		 * Must add collation if not default for the type, because CREATE OR
		 * REPLACE VIEW won't change it
		 */
		if (OidIsValid(tbinfo->attcollation[j]))
		{
			CollInfo   *coll;

			coll = findCollationByOid(fout, tbinfo->attcollation[j]);
			if (coll)
			{
				/* always schema-qualify, don't try to be smart */
				appendStringInfo(result, " COLLATE %s.",
								  dbms_md_fmtId(coll->dobj.namespace->dobj.name));
				appendStringInfoString(result, dbms_md_fmtId_internal(coll->dobj.name, true));
			}
		}

		appendStringInfo(result, " AS %s", dbms_md_fmtId(tbinfo->attnames[j]));
	}

	return result;
}

/*
 * dumpTableSchema
 *	  write the declaration (not data) of one user-defined table or view
 */
static void
dumpTableSchema(Archive *fout, TableInfo *tbinfo)
{
	DumpOptions *dopt = fout->dopt;
	DBMS_MD_DUMP_GVAR_ST *dump_gvar = &(dopt->dump_gvar);
	StringInfo q = createStringInfo();
	StringInfo delq = createStringInfo();
	StringInfo labelq = createStringInfo();
	int			numParents;
	TableInfo **parents;
	int			actual_atts;	/* number of attrs in this CREATE statement */
	const char *reltypename;
	char	   *storage;
	char	   *srvname;
	char	   *ftoptions;
	int			j,
				k;

	/* Make sure we are in proper schema */
	selectSourceSchema(fout, tbinfo->dobj.namespace->dobj.name);

#if 0
	if (dopt->binary_upgrade)
		binary_upgrade_set_type_oids_by_rel_oid(fout, q,
												tbinfo->dobj.catId.oid);
#endif

	/* Is it a table or a view? */
	if (tbinfo->relkind == RELKIND_VIEW)
	{
		StringInfo result;

		/*
		 * Note: keep this code in sync with the is_view case in dumpRule()
		 */

		reltypename = "VIEW";

		/*
		 * DROP must be fully qualified in case same name appears in
		 * pg_catalog
		 */
		appendStringInfo(delq, "DROP VIEW %s.",
						  dbms_md_fmtId(tbinfo->dobj.namespace->dobj.name));
		appendStringInfo(delq, "%s;\n",
						  dbms_md_fmtId(tbinfo->dobj.name));

#if 0
		if (dopt->binary_upgrade)
			binary_upgrade_set_pg_class_oids(fout, q,
											 tbinfo->dobj.catId.oid, false);
#endif

		appendStringInfo(q, "CREATE VIEW %s", dbms_md_fmtId(tbinfo->dobj.name));
		if (tbinfo->dummy_view)
			result = createDummyViewAsClause(fout, tbinfo);
		else
		{
			if (nonemptyReloptions(tbinfo->reloptions))
			{
				appendStringInfoString(q, " WITH (");
				appendReloptionsArrayAH(q, tbinfo->reloptions, "", fout);
				appendStringInfoChar(q, ')');
			}
			result = createViewAsClause(fout, tbinfo);
		}
		appendStringInfo(q, " AS\n%s", result->data);
		destroyStringInfo(result);

		if (tbinfo->checkoption != NULL && !tbinfo->dummy_view)
			appendStringInfo(q, "\n  WITH %s CHECK OPTION", tbinfo->checkoption);
		appendStringInfoString(q, ";\n");

		appendStringInfo(labelq, "VIEW %s",
						  dbms_md_fmtId(tbinfo->dobj.name));
	}
	else
	{
		char *table_type = NULL;

		switch (tbinfo->relkind)
		{
			case RELKIND_FOREIGN_TABLE:
				{
					StringInfo query = createStringInfo();
					SPITupleTable   *res;
					int			i_srvname;
					int			i_ftoptions;

					reltypename = "FOREIGN TABLE";

					/* retrieve name of foreign server and generic options */
					appendStringInfo(query,
									  "SELECT fs.srvname, "
									  "pg_catalog.array_to_string(ARRAY("
									  "SELECT pg_catalog.quote_ident(option_name) || "
									  "' ' || pg_catalog.quote_literal(option_value) "
									  "FROM pg_catalog.pg_options_to_table(ftoptions) "
									  "ORDER BY option_name"
									  "), E',\n    ') AS ftoptions "
									  "FROM pg_catalog.pg_foreign_table ft "
									  "JOIN pg_catalog.pg_foreign_server fs "
									  "ON (fs.oid = ft.ftserver) "
									  "WHERE ft.ftrelid = '%u'",
									  tbinfo->dobj.catId.oid);
					res = ExecuteSqlQueryForSingleRow(fout, query->data);
					i_srvname = dbms_md_get_field_subscript(res, "srvname");
					i_ftoptions = dbms_md_get_field_subscript(res, "ftoptions");
					srvname = dbms_md_strdup(dbms_md_get_field_value(res, 0, i_srvname));
					ftoptions = dbms_md_strdup(dbms_md_get_field_value(res, 0, i_ftoptions));
					dbms_md_free_tuples(res);
					destroyStringInfo(query);
					break;
				}
			case RELKIND_MATVIEW:
				reltypename = "MATERIALIZED VIEW";
				srvname = NULL;
				ftoptions = NULL;
				break;
			default:
				reltypename = "TABLE";
				srvname = NULL;
				ftoptions = NULL;
		}

		numParents = tbinfo->numParents;
		parents = tbinfo->parents;

		/*
		 * DROP must be fully qualified in case same name appears in
		 * pg_catalog
		 */
		appendStringInfo(delq, "DROP %s %s.", reltypename,
						  dbms_md_fmtId(tbinfo->dobj.namespace->dobj.name));
		appendStringInfo(delq, "%s;\n",
						  dbms_md_fmtId(tbinfo->dobj.name));

		appendStringInfo(labelq, "%s %s", reltypename,
						  dbms_md_fmtId(tbinfo->dobj.name));

#if 0
		if (dopt->binary_upgrade)
			binary_upgrade_set_pg_class_oids(fout, q,
											 tbinfo->dobj.catId.oid, false);
#endif

		if (tbinfo->relpersistence == RELPERSISTENCE_UNLOGGED)
			table_type = "UNLOGGED ";
		else if (tbinfo->relpersistence == RELPERSISTENCE_GLOBAL_TEMP)
			table_type = "GLOBAL TEMPORARY ";
		else
			table_type = "";

		appendStringInfo(q, "CREATE %s%s %s",
						  table_type,
						  reltypename,
						  dbms_md_fmtId(tbinfo->dobj.name));

		/*
		 * Attach to type, if reloftype; except in case of a binary upgrade,
		 * we dump the table normally and attach it to the type afterward.
		 */
		if (tbinfo->reloftype)
			appendStringInfo(q, " OF %s", tbinfo->reloftype);

		/*
		 * If the table is a partition, dump it as such; except in the case of
		 * a binary upgrade, we dump the table normally and attach it to the
		 * parent afterward.
		 */
		if (tbinfo->ispartition)
		{
			TableInfo  *parentRel = tbinfo->parents[0];

			/*
			 * With partitions, unlike inheritance, there can only be one
			 * parent.
			 */
			if (tbinfo->numParents != 1)
				elog(ERROR, "invalid number of parents %d for table \"%s\"\n",
							  tbinfo->numParents, tbinfo->dobj.name);

			appendStringInfo(q, " PARTITION OF ");
			if (parentRel->dobj.namespace != tbinfo->dobj.namespace)
				appendStringInfo(q, "%s.",
								  dbms_md_fmtId(parentRel->dobj.namespace->dobj.name));
			appendStringInfoString(q, dbms_md_fmtId(parentRel->dobj.name));
		}

		if (tbinfo->relkind != RELKIND_MATVIEW)
		{
			/* Dump the attributes */
			actual_atts = 0;
			for (j = 0; j < tbinfo->numatts; j++)
			{
				/*
				 * Normally, dump if it's locally defined in this table, and
				 * not dropped.  But for binary upgrade, we'll dump all the
				 * columns, and then fix up the dropped and nonlocal cases
				 * below.
				 */
#ifdef __OPENTENBASE__
				/* when set with_dropped_column, also dump dropped columns */
				if (shouldPrintColumn(dopt, tbinfo, j) || 
				    (dump_gvar->with_dropped_column 
				    && !tbinfo->ispartition 
				    && (tbinfo->attisdropped[j] || numParents)))
#else
				if (shouldPrintColumn(dopt, tbinfo, j))
#endif
				{
					/*
					 * Default value --- suppress if to be printed separately.
					 */
					bool		has_default = (tbinfo->attrdefs[j] != NULL &&
											   !tbinfo->attrdefs[j]->separate);

					/*
					 * Not Null constraint --- suppress if inherited, except
					 * in binary-upgrade case where that won't work.
					 */
					bool		has_notnull = (tbinfo->notnull[j] &&
											   (!tbinfo->inhNotNull[j] ||
												dopt->binary_upgrade));

					/*
					 * Skip column if fully defined by reloftype or the
					 * partition parent.
					 */
					if ((tbinfo->reloftype || tbinfo->ispartition) &&
						!has_default && !has_notnull)
						continue;

					/* Format properly if not first attr */
					if (actual_atts == 0)
						appendStringInfoString(q, " (");
					else
						appendStringInfoChar(q, ',');
					appendStringInfoString(q, "\n    ");
					actual_atts++;

					/* Attribute name */
					appendStringInfoString(q, dbms_md_fmtId(tbinfo->attnames[j]));

					if (tbinfo->attisdropped[j])
					{
						/*
						 * ALTER TABLE DROP COLUMN clears
						 * pg_attribute.atttypid, so we will not have gotten a
						 * valid type name; insert INTEGER as a stopgap. We'll
						 * clean things up later.
						 */
						appendStringInfoString(q, " INTEGER /* dummy */");
						/* Skip all the rest, too */
						continue;
					}

					/*
					 * Attribute type
					 *
					 * In binary-upgrade mode, we always include the type. If
					 * we aren't in binary-upgrade mode, then we skip the type
					 * when creating a typed table ('OF type_name') or a
					 * partition ('PARTITION OF'), since the type comes from
					 * the parent/partitioned table.
					 */
					if ((!tbinfo->reloftype && !tbinfo->ispartition))
					{
						appendStringInfo(q, " %s",
										  tbinfo->atttypnames[j]);
					}

					/* Add collation if not default for the type */
					if (OidIsValid(tbinfo->attcollation[j]))
					{
						CollInfo   *coll;

						coll = findCollationByOid(fout, tbinfo->attcollation[j]);
						if (coll)
						{
							/* always schema-qualify, don't try to be smart */
							appendStringInfo(q, " COLLATE %s.",
											  dbms_md_fmtId(coll->dobj.namespace->dobj.name));
							appendStringInfoString(q, dbms_md_fmtId_internal(coll->dobj.name, true));
						}
					}

					if (has_default)
						appendStringInfo(q, " DEFAULT %s",
										  tbinfo->attrdefs[j]->adef_expr);

					if (has_notnull)
						appendStringInfoString(q, " NOT NULL");
				}
			}

			/*
			 * Add non-inherited CHECK constraints, if any.
			 */
			for (j = 0; j < tbinfo->ncheck; j++)
			{
				ConstraintInfo *constr = &(tbinfo->checkexprs[j]);

				if (constr->separate || !constr->conislocal || (tbinfo->relkind == RELKIND_RELATION && tbinfo->parttype == 'p'))
					continue;

				if (actual_atts == 0)
					appendStringInfoString(q, " (\n    ");
				else
					appendStringInfoString(q, ",\n    ");

				appendStringInfo(q, "CONSTRAINT %s ",
								  dbms_md_fmtId(constr->dobj.name));
				appendStringInfoString(q, constr->condef);

				actual_atts++;
			}

			if (actual_atts)
				appendStringInfoString(q, "\n)");
			else if (!((tbinfo->reloftype || tbinfo->ispartition)))
			{
				/*
				 * We must have a parenthesized attribute list, even though
				 * empty, when not using the OF TYPE or PARTITION OF syntax.
				 */
				appendStringInfoString(q, " (\n)");
			}

			if (tbinfo->ispartition)
			{
				appendStringInfoString(q, "\n");
				appendStringInfoString(q, tbinfo->partbound);
			}

			/* Emit the INHERITS clause, except if this is a partition. */
			if (numParents > 0 &&
				!tbinfo->ispartition &&
#ifdef __OPENTENBASE__
				!dump_gvar->with_dropped_column &&
#endif
				!dopt->binary_upgrade)
			{
				appendStringInfoString(q, "\nINHERITS (");
				for (k = 0; k < numParents; k++)
				{
					TableInfo  *parentRel = parents[k];

					if (k > 0)
						appendStringInfoString(q, ", ");
					if (parentRel->dobj.namespace != tbinfo->dobj.namespace)
						appendStringInfo(q, "%s.",
										  dbms_md_fmtId(parentRel->dobj.namespace->dobj.name));
					appendStringInfoString(q, dbms_md_fmtId(parentRel->dobj.name));
				}
				appendStringInfoChar(q, ')');
			}

			if (tbinfo->relkind == RELKIND_PARTITIONED_TABLE)
				appendStringInfo(q, "\nPARTITION BY %s", tbinfo->partkeydef);
#ifdef __OPENTENBASE__
			if (tbinfo->relkind == RELKIND_RELATION)
			{
				if (tbinfo->parttype == 'p')
				{
					appendStringInfo(q,"\nPARTITION BY RANGE (%s) BEGIN (%s) STEP (%s) PARTITIONS (%d) ",
					dbms_md_fmtId(tbinfo->attnames[tbinfo->partattnum - 1]),
					tbinfo->partstartvalue,
					tbinfo->partinterval,
					tbinfo->nparts);
				}
			}
#endif
			if (tbinfo->relkind == RELKIND_FOREIGN_TABLE)
				appendStringInfo(q, "\nSERVER %s", dbms_md_fmtId(srvname));
		}

		if (nonemptyReloptions(tbinfo->reloptions) ||
			nonemptyReloptions(tbinfo->toast_reloptions))
		{
			bool		addcomma = false;

			appendStringInfoString(q, "\nWITH (");
			if (nonemptyReloptions(tbinfo->reloptions))
			{
				addcomma = true;
				appendReloptionsArrayAH(q, tbinfo->reloptions, "", fout);
			}
			if (nonemptyReloptions(tbinfo->toast_reloptions))
			{
				if (addcomma)
					appendStringInfoString(q, ", ");
				appendReloptionsArrayAH(q, tbinfo->toast_reloptions, "toast.",
										fout);
			}
			appendStringInfoChar(q, ')');
		}

#ifdef PGXC
		if (fout->isPostgresXL)
		{
			/* Add the grammar extension linked to PGXC depending on data got from pgxc_class */
			if (tbinfo->pgxclocatortype != 'E')
			{
				/* N: DISTRIBUTE BY ROUNDROBIN */
				if (tbinfo->pgxclocatortype == 'N')
				{
					appendStringInfo(q, "\nDISTRIBUTE BY ROUNDROBIN");
				}
				/* R: DISTRIBUTE BY REPLICATED */
				else if (tbinfo->pgxclocatortype == 'R' && !tbinfo->ispartition)
				{
					if (tbinfo->groupname)
					{
						appendStringInfo(q, "\nDISTRIBUTE BY REPLICATION to GROUP %s", tbinfo->groupname);
					}
					else
					{
						appendStringInfo(q, "\nDISTRIBUTE BY REPLICATION");
					}
				}
				/* H: DISTRIBUTE BY HASH  */
				else if (tbinfo->pgxclocatortype == 'H')
				{
					int hashkey = tbinfo->pgxcattnum;
					appendStringInfo(q, "\nDISTRIBUTE BY HASH (%s)",
									  dbms_md_fmtId(tbinfo->attnames[hashkey - 1]));
				}
				else if (tbinfo->pgxclocatortype == 'M')
				{
					int hashkey = tbinfo->pgxcattnum;
					appendStringInfo(q, "\nDISTRIBUTE BY MODULO (%s)",
									  dbms_md_fmtId(tbinfo->attnames[hashkey - 1]));
				}
#ifdef __OPENTENBASE__
				else if(tbinfo->pgxclocatortype == 'S' && !tbinfo->ispartition)
				{
					int hashkey = tbinfo->pgxcattnum;
					int sechashkey = tbinfo->pgxcsecattnum;

					if (sechashkey)
					{
						appendStringInfo(q, "\nDISTRIBUTE BY SHARD (%s,",
									  	dbms_md_fmtId(tbinfo->attnames[hashkey - 1]));
						appendStringInfo(q, "%s)",
									  	dbms_md_fmtId(tbinfo->attnames[sechashkey - 1]));
					}
					else
						appendStringInfo(q, "\nDISTRIBUTE BY SHARD (%s)",
									  	dbms_md_fmtId(tbinfo->attnames[hashkey - 1]));

					if (tbinfo->coldgroupname)
						appendStringInfo(q, " to GROUP %s %s", tbinfo->groupname, tbinfo->coldgroupname);
					else
						appendStringInfo(q, " to GROUP %s", tbinfo->groupname);
				}
#endif
			}

			if (dump_gvar->include_nodes 
				&&	tbinfo->pgxc_node_names != NULL 
				&&	tbinfo->pgxc_node_names[0] != '\0')
			{
				appendStringInfo(q, "\nTO NODE (%s)", tbinfo->pgxc_node_names);
			}
		}
#endif
		/* Dump generic options if any */
		if (ftoptions && ftoptions[0])
			appendStringInfo(q, "\nOPTIONS (\n    %s\n)", ftoptions);

		/*
		 * For materialized views, create the AS clause just like a view. At
		 * this point, we always mark the view as not populated.
		 */
		if (tbinfo->relkind == RELKIND_MATVIEW)
		{
			StringInfo result;

			result = createViewAsClause(fout, tbinfo);
			appendStringInfo(q, " AS\n%s\n  WITH NO DATA;\n",
							  result->data);
			destroyStringInfo(result);
		}
		else
			appendStringInfoString(q, ";\n");


#ifdef __OPENTENBASE__
		/* dump table keyvalues */
		if (tbinfo->nKeys)
		{
			int i;
			for (i = 0; i < tbinfo->nKeys; i++)
			{
				if (tbinfo->keyvalue[i].colddisgroup)
				{					
					appendStringInfo(q, "CREATE KEY VALUES '%s' FOR TABLE %s.%s TO GROUP %s %s;\n", 
											tbinfo->keyvalue[i].keyvalue,
											tbinfo->keyvalue[i].schema,
											tbinfo->keyvalue[i].table,
											tbinfo->keyvalue[i].hotdisgroup,
											tbinfo->keyvalue[i].colddisgroup);
				}
				else
				{				
					appendStringInfo(q, "CREATE KEY VALUES '%s' FOR TABLE %s.%s TO GROUP %s;\n", 
											tbinfo->keyvalue[i].keyvalue,
											tbinfo->keyvalue[i].schema,
											tbinfo->keyvalue[i].table,
											tbinfo->keyvalue[i].hotdisgroup);
				}
			}
			appendStringInfo(q, "\n");
		}
#endif
		/*
		 * To create binary-compatible heap files, we have to ensure the same
		 * physical column order, including dropped columns, as in the
		 * original.  Therefore, we create dropped columns above and drop them
		 * here, also updating their attlen/attalign values so that the
		 * dropped column can be skipped properly.  (We do not bother with
		 * restoring the original attbyval setting.)  Also, inheritance
		 * relationships are set up by doing ALTER TABLE INHERIT rather than
		 * using an INHERITS clause --- the latter would possibly mess up the
		 * column order.  That also means we have to take care about setting
		 * attislocal correctly, plus fix up any inherited CHECK constraints.
		 * Analogously, we set up typed tables using ALTER TABLE / OF here.
		 */
		if (dopt->binary_upgrade &&
			(tbinfo->relkind == RELKIND_RELATION ||
			 tbinfo->relkind == RELKIND_FOREIGN_TABLE ||
			 tbinfo->relkind == RELKIND_PARTITIONED_TABLE))
		{
			for (j = 0; j < tbinfo->numatts; j++)
			{
				if (tbinfo->attisdropped[j])
				{
					appendStringInfoString(q, "\n-- For binary upgrade, recreate dropped column.\n");
					appendStringInfo(q, "UPDATE pg_catalog.pg_attribute\n"
									  "SET attlen = %d, "
									  "attalign = '%c', attbyval = false\n"
									  "WHERE attname = ",
									  tbinfo->attlen[j],
									  tbinfo->attalign[j]);
					appendStringLiteralAH(q, tbinfo->attnames[j], fout);
					appendStringInfoString(q, "\n  AND attrelid = ");
					appendStringLiteralAH(q, dbms_md_fmtId(tbinfo->dobj.name), fout);
					appendStringInfoString(q, "::pg_catalog.regclass;\n");

					if (tbinfo->relkind == RELKIND_RELATION ||
						tbinfo->relkind == RELKIND_PARTITIONED_TABLE)
						appendStringInfo(q, "ALTER TABLE ONLY %s ",
										  dbms_md_fmtId(tbinfo->dobj.name));
					else
						appendStringInfo(q, "ALTER FOREIGN TABLE ONLY %s ",
										  dbms_md_fmtId(tbinfo->dobj.name));
					appendStringInfo(q, "DROP COLUMN %s;\n",
									  dbms_md_fmtId(tbinfo->attnames[j]));
				}
				else if (!tbinfo->attislocal[j])
				{
					appendStringInfoString(q, "\n-- For binary upgrade, recreate inherited column.\n");
					appendStringInfoString(q, "UPDATE pg_catalog.pg_attribute\n"
										 "SET attislocal = false\n"
										 "WHERE attname = ");
					appendStringLiteralAH(q, tbinfo->attnames[j], fout);
					appendStringInfoString(q, "\n  AND attrelid = ");
					appendStringLiteralAH(q, dbms_md_fmtId(tbinfo->dobj.name), fout);
					appendStringInfoString(q, "::pg_catalog.regclass;\n");
				}
			}

			for (k = 0; k < tbinfo->ncheck; k++)
			{
				ConstraintInfo *constr = &(tbinfo->checkexprs[k]);

				if (constr->separate || constr->conislocal)
					continue;

				appendStringInfoString(q, "\n-- For binary upgrade, set up inherited constraint.\n");
				appendStringInfo(q, "ALTER TABLE ONLY %s ",
								  dbms_md_fmtId(tbinfo->dobj.name));
				appendStringInfo(q, " ADD CONSTRAINT %s ",
								  dbms_md_fmtId(constr->dobj.name));
				appendStringInfo(q, "%s;\n", constr->condef);
				appendStringInfoString(q, "UPDATE pg_catalog.pg_constraint\n"
									 "SET conislocal = false\n"
									 "WHERE contype = 'c' AND conname = ");
				appendStringLiteralAH(q, constr->dobj.name, fout);
				appendStringInfoString(q, "\n  AND conrelid = ");
				appendStringLiteralAH(q, dbms_md_fmtId(tbinfo->dobj.name), fout);
				appendStringInfoString(q, "::pg_catalog.regclass;\n");
			}

			if (numParents > 0)
			{
				appendStringInfoString(q, "\n-- For binary upgrade, set up inheritance and partitioning this way.\n");
				for (k = 0; k < numParents; k++)
				{
					TableInfo  *parentRel = parents[k];
					StringInfo parentname = createStringInfo();

					/* Schema-qualify the parent table, if necessary */
					if (parentRel->dobj.namespace != tbinfo->dobj.namespace)
						appendStringInfo(parentname, "%s.",
										  dbms_md_fmtId(parentRel->dobj.namespace->dobj.name));

					appendStringInfo(parentname, "%s",
									  dbms_md_fmtId(parentRel->dobj.name));

					/* In the partitioning case, we alter the parent */
					if (tbinfo->ispartition)
						appendStringInfo(q,
										  "ALTER TABLE ONLY %s ATTACH PARTITION ",
										  parentname->data);
					else
						appendStringInfo(q, "ALTER TABLE ONLY %s INHERIT ",
										  dbms_md_fmtId(tbinfo->dobj.name));

					/* Partition needs specifying the bounds */
					if (tbinfo->ispartition)
						appendStringInfo(q, "%s %s;\n",
										  dbms_md_fmtId(tbinfo->dobj.name),
										  tbinfo->partbound);
					else
						appendStringInfo(q, "%s;\n", parentname->data);

					destroyStringInfo(parentname);
				}
			}

			if (tbinfo->reloftype)
			{
				appendStringInfoString(q, "\n-- For binary upgrade, set up typed tables this way.\n");
				appendStringInfo(q, "ALTER TABLE ONLY %s OF %s;\n",
								  dbms_md_fmtId(tbinfo->dobj.name),
								  tbinfo->reloftype);
			}

			appendStringInfoString(q, "\n-- For binary upgrade, set heap's relfrozenxid and relminmxid\n");
			appendStringInfo(q, "UPDATE pg_catalog.pg_class\n"
							  "SET relfrozenxid = '%u', relminmxid = '%u'\n"
							  "WHERE oid = ",
							  tbinfo->frozenxid, tbinfo->minmxid);
			appendStringLiteralAH(q, dbms_md_fmtId(tbinfo->dobj.name), fout);
			appendStringInfoString(q, "::pg_catalog.regclass;\n");

			if (tbinfo->toast_oid)
			{
				/* We preserve the toast oids, so we can use it during restore */
				appendStringInfoString(q, "\n-- For binary upgrade, set toast's relfrozenxid and relminmxid\n");
				appendStringInfo(q, "UPDATE pg_catalog.pg_class\n"
								  "SET relfrozenxid = '%u', relminmxid = '%u'\n"
								  "WHERE oid = '%u';\n",
								  tbinfo->toast_frozenxid,
								  tbinfo->toast_minmxid, tbinfo->toast_oid);
			}
		}

		/*
		 * In binary_upgrade mode, restore matviews' populated status by
		 * poking pg_class directly.  This is pretty ugly, but we can't use
		 * REFRESH MATERIALIZED VIEW since it's possible that some underlying
		 * matview is not populated even though this matview is.
		 */
		if (dopt->binary_upgrade && tbinfo->relkind == RELKIND_MATVIEW &&
			tbinfo->relispopulated)
		{
			appendStringInfoString(q, "\n-- For binary upgrade, mark materialized view as populated\n");
			appendStringInfoString(q, "UPDATE pg_catalog.pg_class\n"
								 "SET relispopulated = 't'\n"
								 "WHERE oid = ");
			appendStringLiteralAH(q, dbms_md_fmtId(tbinfo->dobj.name), fout);
			appendStringInfoString(q, "::pg_catalog.regclass;\n");
		}

		/*
		 * Dump additional per-column properties that we can't handle in the
		 * main CREATE TABLE command.
		 */
		for (j = 0; j < tbinfo->numatts; j++)
		{
			/* None of this applies to dropped columns */
			if (tbinfo->attisdropped[j])
				continue;

			/*
			 * If we didn't dump the column definition explicitly above, and
			 * it is NOT NULL and did not inherit that property from a parent,
			 * we have to mark it separately.
			 */
			if (!shouldPrintColumn(dopt, tbinfo, j) &&
				tbinfo->notnull[j] && !tbinfo->inhNotNull[j])
			{
				appendStringInfo(q, "ALTER TABLE ONLY %s ",
								  dbms_md_fmtId(tbinfo->dobj.name));
				appendStringInfo(q, "ALTER COLUMN %s SET NOT NULL;\n",
								  dbms_md_fmtId(tbinfo->attnames[j]));
			}

			/*
			 * Dump per-column statistics information. We only issue an ALTER
			 * TABLE statement if the attstattarget entry for this column is
			 * non-negative (i.e. it's not the default value)
			 */
			if (tbinfo->attstattarget[j] >= 0)
			{
				appendStringInfo(q, "ALTER TABLE ONLY %s ",
								  dbms_md_fmtId(tbinfo->dobj.name));
				appendStringInfo(q, "ALTER COLUMN %s ",
								  dbms_md_fmtId(tbinfo->attnames[j]));
				appendStringInfo(q, "SET STATISTICS %d;\n",
								  tbinfo->attstattarget[j]);
			}

			/*
			 * Dump per-column storage information.  The statement is only
			 * dumped if the storage has been changed from the type's default.
			 */
			if (tbinfo->attstorage[j] != tbinfo->typstorage[j])
			{
				switch (tbinfo->attstorage[j])
				{
					case 'p':
						storage = "PLAIN";
						break;
					case 'e':
						storage = "EXTERNAL";
						break;
					case 'm':
						storage = "MAIN";
						break;
					case 'x':
						storage = "EXTENDED";
						break;
					default:
						storage = NULL;
				}

				/*
				 * Only dump the statement if it's a storage type we recognize
				 */
				if (storage != NULL)
				{
					appendStringInfo(q, "ALTER TABLE ONLY %s ",
									  dbms_md_fmtId(tbinfo->dobj.name));
					appendStringInfo(q, "ALTER COLUMN %s ",
									  dbms_md_fmtId(tbinfo->attnames[j]));
					appendStringInfo(q, "SET STORAGE %s;\n",
									  storage);
				}
			}

			/*
			 * Dump per-column attributes.
			 */
			if (tbinfo->attoptions[j] && tbinfo->attoptions[j][0] != '\0')
			{
				appendStringInfo(q, "ALTER TABLE ONLY %s ",
								  dbms_md_fmtId(tbinfo->dobj.name));
				appendStringInfo(q, "ALTER COLUMN %s ",
								  dbms_md_fmtId(tbinfo->attnames[j]));
				appendStringInfo(q, "SET (%s);\n",
								  tbinfo->attoptions[j]);
			}

			/*
			 * Dump per-column fdw options.
			 */
			if (tbinfo->relkind == RELKIND_FOREIGN_TABLE &&
				tbinfo->attfdwoptions[j] &&
				tbinfo->attfdwoptions[j][0] != '\0')
			{
				appendStringInfo(q, "ALTER FOREIGN TABLE %s ",
								  dbms_md_fmtId(tbinfo->dobj.name));
				appendStringInfo(q, "ALTER COLUMN %s ",
								  dbms_md_fmtId(tbinfo->attnames[j]));
				appendStringInfo(q, "OPTIONS (\n    %s\n);\n",
								  tbinfo->attfdwoptions[j]);
			}
		}
#ifdef __OPENTENBASE__
		/* add alter table....drop column to dumped table while with_dropped_column is set */
		if (dump_gvar->with_dropped_column &&  !tbinfo->ispartition &&
			(tbinfo->relkind == RELKIND_RELATION || tbinfo->relkind == RELKIND_PARTITIONED_TABLE))
		{
			for (j = 0; j < tbinfo->numatts; j++)
			{
				if (tbinfo->attisdropped[j])
				{
					appendStringInfo(q, "ALTER TABLE %s ",
										  dbms_md_fmtId(tbinfo->dobj.name));
					appendStringInfo(q, "DROP COLUMN %s;\n",
									  dbms_md_fmtId(tbinfo->attnames[j]));
				}
			}

			/* inherit */
			if (numParents > 0)
			{
				for (k = 0; k < numParents; k++)
				{
					TableInfo  *parentRel = parents[k];
					StringInfo parentname = createStringInfo();

					/* Schema-qualify the parent table, if necessary */
					if (parentRel->dobj.namespace != tbinfo->dobj.namespace)
						appendStringInfo(parentname, "%s.",
										  dbms_md_fmtId(parentRel->dobj.namespace->dobj.name));

					appendStringInfo(parentname, "%s",
									  dbms_md_fmtId(parentRel->dobj.name));

					appendStringInfo(q, "ALTER TABLE ONLY %s INHERIT ",
									  dbms_md_fmtId(tbinfo->dobj.name));

					appendStringInfo(q, "%s;\n", parentname->data);

					destroyStringInfo(parentname);
				}
			}
		}
#endif
	}
	/*
	 * dump properties we only have ALTER TABLE syntax for
	 */
	if ((tbinfo->relkind == RELKIND_RELATION ||
		 tbinfo->relkind == RELKIND_PARTITIONED_TABLE ||
		 tbinfo->relkind == RELKIND_MATVIEW) &&
		tbinfo->relreplident != REPLICA_IDENTITY_DEFAULT)
	{
		if (tbinfo->relreplident == REPLICA_IDENTITY_INDEX)
		{
			/* nothing to do, will be set when the index is dumped */
		}
		else if (tbinfo->relreplident == REPLICA_IDENTITY_NOTHING)
		{
			appendStringInfo(q, "\nALTER TABLE ONLY %s REPLICA IDENTITY NOTHING;\n",
							  dbms_md_fmtId(tbinfo->dobj.name));
		}
		else if (tbinfo->relreplident == REPLICA_IDENTITY_FULL)
		{
			appendStringInfo(q, "\nALTER TABLE ONLY %s REPLICA IDENTITY FULL;\n",
							  dbms_md_fmtId(tbinfo->dobj.name));
		}
	}

	if (tbinfo->relkind == RELKIND_FOREIGN_TABLE && tbinfo->hasoids)
		appendStringInfo(q, "\nALTER TABLE ONLY %s SET WITH OIDS;\n",
						  dbms_md_fmtId(tbinfo->dobj.name));

	if (tbinfo->forcerowsec)
		appendStringInfo(q, "\nALTER TABLE ONLY %s FORCE ROW LEVEL SECURITY;\n",
						  dbms_md_fmtId(tbinfo->dobj.name));

#if 0
	if (dopt->binary_upgrade)
		binary_upgrade_extension_member(q, &tbinfo->dobj, labelq->data);
#endif

#ifdef __OPENTENBASE__
	if (tbinfo->relkind == RELKIND_RELATION && tbinfo->parttype == 'p')
	{
		for (j = 0; j < tbinfo->ncheck; j++)
		{
			ConstraintInfo *constr = &(tbinfo->checkexprs[j]);

			if (constr->separate || !constr->conislocal)
				continue;

			appendStringInfo(q, "\nALTER TABLE %s ",
							  dbms_md_fmtId(tbinfo->dobj.name));
			appendStringInfo(q, "ADD CONSTRAINT %s ",
							  dbms_md_fmtId(constr->dobj.name));
			appendStringInfo(q, "%s;\n",
							  constr->condef);
		}
	}
#endif

	if (tbinfo->dobj.dump & DUMP_COMPONENT_DEFINITION)
		ArchiveEntry(fout, tbinfo->dobj.catId, tbinfo->dobj.dumpId,
					 tbinfo->dobj.name,
					 tbinfo->dobj.namespace->dobj.name,
					 (tbinfo->relkind == RELKIND_VIEW) ? NULL : tbinfo->reltablespace,
					 tbinfo->rolname,
					 (strcmp(reltypename, "TABLE") == 0) ? tbinfo->hasoids : false,
					 reltypename,
					 tbinfo->postponed_def ?
					 SECTION_POST_DATA : SECTION_PRE_DATA,
					 q->data, delq->data, NULL,
					 NULL, 0,
					 NULL, NULL);

	/* Dump Table Comments */
	if (tbinfo->dobj.dump & DUMP_COMPONENT_COMMENT)
		dumpTableComment(fout, tbinfo, reltypename);

	/* Dump Table Security Labels */
	if (tbinfo->dobj.dump & DUMP_COMPONENT_SECLABEL)
		dumpTableSecLabel(fout, tbinfo, reltypename);

	/* Dump comments on inlined table constraints */
	for (j = 0; j < tbinfo->ncheck; j++)
	{
		ConstraintInfo *constr = &(tbinfo->checkexprs[j]);

		if (constr->separate || !constr->conislocal)
			continue;

		if (tbinfo->dobj.dump & DUMP_COMPONENT_COMMENT)
			dumpTableConstraintComment(fout, constr);
	}

	destroyStringInfo(q);
	destroyStringInfo(delq);
	destroyStringInfo(labelq);

	pfree(q);
	pfree(delq);
	pfree(labelq);
}

/*
 * dumpAttrDef --- dump an attribute's default-value declaration
 */
static void
dumpAttrDef(Archive *fout, AttrDefInfo *adinfo)
{
	DumpOptions *dopt = fout->dopt;
	TableInfo  *tbinfo = adinfo->adtable;
	int			adnum = adinfo->adnum;
	StringInfo q;
	StringInfo delq;
	char	   *tag = NULL;

	/* Skip if table definition not to be dumped */
	if (!tbinfo->dobj.dump || dopt->dataOnly)
		return;

	/* Skip if not "separate"; it was dumped in the table's definition */
	if (!adinfo->separate)
		return;

	q = createStringInfo();
	delq = createStringInfo();

	appendStringInfo(q, "ALTER TABLE ONLY %s ",
					  dbms_md_fmtId(tbinfo->dobj.name));
	appendStringInfo(q, "ALTER COLUMN %s SET DEFAULT %s;\n",
					  dbms_md_fmtId(tbinfo->attnames[adnum - 1]),
					  adinfo->adef_expr);

	/*
	 * DROP must be fully qualified in case same name appears in pg_catalog
	 */
	appendStringInfo(delq, "ALTER TABLE %s.",
					  dbms_md_fmtId(tbinfo->dobj.namespace->dobj.name));
	appendStringInfo(delq, "%s ",
					  dbms_md_fmtId(tbinfo->dobj.name));
	appendStringInfo(delq, "ALTER COLUMN %s DROP DEFAULT;\n",
					  dbms_md_fmtId(tbinfo->attnames[adnum - 1]));

	tag = psprintf("%s %s", tbinfo->dobj.name, tbinfo->attnames[adnum - 1]);
	Assert(tag != NULL);

	if (adinfo->dobj.dump & DUMP_COMPONENT_DEFINITION)
		ArchiveEntry(fout, adinfo->dobj.catId, adinfo->dobj.dumpId,
					 tag,
					 tbinfo->dobj.namespace->dobj.name,
					 NULL,
					 tbinfo->rolname,
					 false, "DEFAULT", SECTION_PRE_DATA,
					 q->data, delq->data, NULL,
					 NULL, 0,
					 NULL, NULL);

	Assert(tag != NULL);
	pfree(tag);

	destroyStringInfo(q);
	destroyStringInfo(delq);

	pfree(q);
	pfree(delq);
}

/*
 * getAttrName: extract the correct name for an attribute
 *
 * The array tblInfo->attnames[] only provides names of user attributes;
 * if a system attribute number is supplied, we have to fake it.
 * We also do a little bit of bounds checking for safety's sake.
 */
static const char *
getAttrName(int attrnum, TableInfo *tblInfo)
{
	if (attrnum > 0 && attrnum <= tblInfo->numatts)
		return tblInfo->attnames[attrnum - 1];
	switch (attrnum)
	{
		case SelfItemPointerAttributeNumber:
			return "ctid";
		case ObjectIdAttributeNumber:
			return "oid";
		case MinTransactionIdAttributeNumber:
			return "xmin";
		case MinCommandIdAttributeNumber:
			return "cmin";
		case MaxTransactionIdAttributeNumber:
			return "xmax";
		case MaxCommandIdAttributeNumber:
			return "cmax";
		case TableOidAttributeNumber:
			return "tableoid";
#ifdef PGXC
		case XC_NodeIdAttributeNumber:
			return "xc_node_id";
#endif
	}
	elog(ERROR, "invalid column number %d for table \"%s\"\n",
				  attrnum, tblInfo->dobj.name);
	return NULL;				/* keep compiler quiet */
}

/*
 * dumpIndex
 *	  write out to fout a user-defined index
 */
static void
dumpIndex(Archive *fout, IndxInfo *indxinfo)
{
	DumpOptions *dopt = fout->dopt;
	TableInfo  *tbinfo = indxinfo->indextable;
	bool		is_constraint = (indxinfo->indexconstraint != 0);
	StringInfo q;
	StringInfo delq;
	StringInfo labelq;

	if (dopt->dataOnly)
		return;

#ifdef __OPENTENBASE__
	if(tbinfo->parttype == 'c')
	{
		return;
	}
#endif

	q = createStringInfo();
	delq = createStringInfo();
	labelq = createStringInfo();

	appendStringInfo(labelq, "INDEX %s",
					  dbms_md_fmtId(indxinfo->dobj.name));

	/*
	 * If there's an associated constraint, don't dump the index per se, but
	 * do dump any comment for it.  (This is safe because dependency ordering
	 * will have ensured the constraint is emitted first.)	Note that the
	 * emitted comment has to be shown as depending on the constraint, not the
	 * index, in such cases.
	 */
	if (!is_constraint)
	{
#if 0
		if (dopt->binary_upgrade)
			binary_upgrade_set_pg_class_oids(fout, q,
											 indxinfo->dobj.catId.oid, true);
#endif

		/* Plain secondary index */
		appendStringInfo(q, "%s;\n", indxinfo->indexdef);

		/* If the index is clustered, we need to record that. */
		if (indxinfo->indisclustered)
		{
			appendStringInfo(q, "\nALTER TABLE %s CLUSTER",
							  dbms_md_fmtId(tbinfo->dobj.name));
			appendStringInfo(q, " ON %s;\n",
							  dbms_md_fmtId(indxinfo->dobj.name));
		}

		/* If the index defines identity, we need to record that. */
		if (indxinfo->indisreplident)
		{
			appendStringInfo(q, "\nALTER TABLE ONLY %s REPLICA IDENTITY USING",
							  dbms_md_fmtId(tbinfo->dobj.name));
			appendStringInfo(q, " INDEX %s;\n",
							  dbms_md_fmtId(indxinfo->dobj.name));
		}

		/*
		 * DROP must be fully qualified in case same name appears in
		 * pg_catalog
		 */
		appendStringInfo(delq, "DROP INDEX %s.",
						  dbms_md_fmtId(tbinfo->dobj.namespace->dobj.name));
		appendStringInfo(delq, "%s;\n",
						  dbms_md_fmtId(indxinfo->dobj.name));

		if (indxinfo->dobj.dump & DUMP_COMPONENT_DEFINITION)
			ArchiveEntry(fout, indxinfo->dobj.catId, indxinfo->dobj.dumpId,
						 indxinfo->dobj.name,
						 tbinfo->dobj.namespace->dobj.name,
						 indxinfo->tablespace,
						 tbinfo->rolname, false,
						 "INDEX", SECTION_POST_DATA,
						 q->data, delq->data, NULL,
						 NULL, 0,
						 NULL, NULL);
	}

	/* Dump Index Comments */
	if (indxinfo->dobj.dump & DUMP_COMPONENT_COMMENT)
		dumpComment(fout, labelq->data,
					tbinfo->dobj.namespace->dobj.name,
					tbinfo->rolname,
					indxinfo->dobj.catId, 0,
					is_constraint ? indxinfo->indexconstraint :
					indxinfo->dobj.dumpId);

	destroyStringInfo(q);
	destroyStringInfo(delq);
	destroyStringInfo(labelq);

	pfree(q);
	pfree(delq);
	pfree(labelq);
}

/*
 * dumpIndexAttach
 *	  write out to fout a partitioned-index attachment clause
 */
void
dumpIndexAttach(Archive *fout, IndexAttachInfo *attachinfo)
{
	if (fout->dopt->dataOnly)
		return;

	if (attachinfo->partitionIdx->dobj.dump & DUMP_COMPONENT_DEFINITION)
	{
		StringInfo	q = createStringInfo();

		appendStringInfo(q, "\nALTER INDEX %s ",
						  dbms_md_fmt_qualified_id(fout->remoteVersion,
										 attachinfo->parentIdx->dobj.namespace->dobj.name,
										 attachinfo->parentIdx->dobj.name));
		appendStringInfo(q, "ATTACH PARTITION %s;\n",
						  dbms_md_fmt_qualified_id(fout->remoteVersion,
										 attachinfo->partitionIdx->dobj.namespace->dobj.name,
										 attachinfo->partitionIdx->dobj.name));

		ArchiveEntry(fout, attachinfo->dobj.catId, attachinfo->dobj.dumpId,
					 attachinfo->dobj.name,
					 NULL, NULL,
					 "",
					 false, "INDEX ATTACH", SECTION_POST_DATA,
					 q->data, "", NULL,
					 NULL, 0,
					 NULL, NULL);

		destroyStringInfo(q);
		pfree(q);
	}
}

/*
 * dumpStatisticsExt
 *	  write out to fout an extended statistics object
 */
static void
dumpStatisticsExt(Archive *fout, StatsExtInfo *statsextinfo)
{
	DumpOptions *dopt = fout->dopt;
	TableInfo  *tbinfo = statsextinfo->statsexttable;
	StringInfo q;
	StringInfo delq;
	StringInfo labelq;

	if (dopt->dataOnly)
		return;

	q = createStringInfo();
	delq = createStringInfo();
	labelq = createStringInfo();

	appendStringInfo(labelq, "STATISTICS %s",
					  dbms_md_fmtId(statsextinfo->dobj.name));

	appendStringInfo(q, "%s;\n", statsextinfo->statsextdef);

	appendStringInfo(delq, "DROP STATISTICS %s.",
					  dbms_md_fmtId(tbinfo->dobj.namespace->dobj.name));
	appendStringInfo(delq, "%s;\n",
					  dbms_md_fmtId(statsextinfo->dobj.name));

	if (statsextinfo->dobj.dump & DUMP_COMPONENT_DEFINITION)
		ArchiveEntry(fout, statsextinfo->dobj.catId,
					 statsextinfo->dobj.dumpId,
					 statsextinfo->dobj.name,
					 tbinfo->dobj.namespace->dobj.name,
					 NULL,
					 tbinfo->rolname, false,
					 "STATISTICS", SECTION_POST_DATA,
					 q->data, delq->data, NULL,
					 NULL, 0,
					 NULL, NULL);

	/* Dump Statistics Comments */
	if (statsextinfo->dobj.dump & DUMP_COMPONENT_COMMENT)
		dumpComment(fout, labelq->data,
					tbinfo->dobj.namespace->dobj.name,
					tbinfo->rolname,
					statsextinfo->dobj.catId, 0,
					statsextinfo->dobj.dumpId);

	destroyStringInfo(q);
	destroyStringInfo(delq);
	destroyStringInfo(labelq);

	pfree(q);
	pfree(delq);
	pfree(labelq);
}

/*
 * dumpConstraint
 *	  write out to fout a user-defined constraint
 */
static void
dumpConstraint(Archive *fout, ConstraintInfo *coninfo)
{
	DumpOptions *dopt = fout->dopt;
	TableInfo  *tbinfo = coninfo->contable;
	StringInfo q;
	StringInfo delq;
	char	   *tag = NULL;

	/* Skip if not to be dumped */
	if (!coninfo->dobj.dump || dopt->dataOnly)
		return;

	q = createStringInfo();
	delq = createStringInfo();

	if (coninfo->contype == 'p' ||
		coninfo->contype == 'u' ||
		coninfo->contype == 'x')
	{
		/* Index-related constraint */
		IndxInfo   *indxinfo;
		int			k;

		indxinfo = (IndxInfo *) findObjectByDumpId(fout, coninfo->conindex);

		if (indxinfo == NULL)
			elog(ERROR, "missing index for constraint \"%s\"\n",
						  coninfo->dobj.name);

#if 0
		if (dopt->binary_upgrade)
			binary_upgrade_set_pg_class_oids(fout, q,
											 indxinfo->dobj.catId.oid, true);
#endif

		appendStringInfo(q, "ALTER TABLE ONLY %s\n",
						  dbms_md_fmtId(tbinfo->dobj.name));
		appendStringInfo(q, "    ADD CONSTRAINT %s ",
						  dbms_md_fmtId(coninfo->dobj.name));

		if (coninfo->condef)
		{
			/* pg_get_constraintdef should have provided everything */
			appendStringInfo(q, "%s;\n", coninfo->condef);
		}
		else
		{
			appendStringInfo(q, "%s (",
							  coninfo->contype == 'p' ? "PRIMARY KEY" : "UNIQUE");
			for (k = 0; k < indxinfo->indnkeys; k++)
			{
				int			indkey = (int) indxinfo->indkeys[k];
				const char *attname;

				if (indkey == InvalidAttrNumber)
					break;
				attname = getAttrName(indkey, tbinfo);

				appendStringInfo(q, "%s%s",
								  (k == 0) ? "" : ", ",
								  dbms_md_fmtId(attname));
			}

			appendStringInfoChar(q, ')');

			if (nonemptyReloptions(indxinfo->indreloptions))
			{
				appendStringInfoString(q, " WITH (");
				appendReloptionsArrayAH(q, indxinfo->indreloptions, "", fout);
				appendStringInfoChar(q, ')');
			}

			if (coninfo->condeferrable)
			{
				appendStringInfoString(q, " DEFERRABLE");
				if (coninfo->condeferred)
					appendStringInfoString(q, " INITIALLY DEFERRED");
			}

			appendStringInfoString(q, ";\n");
		}

		/* If the index is clustered, we need to record that. */
		if (indxinfo->indisclustered)
		{
			appendStringInfo(q, "\nALTER TABLE %s CLUSTER",
							  dbms_md_fmtId(tbinfo->dobj.name));
			appendStringInfo(q, " ON %s;\n",
							  dbms_md_fmtId(indxinfo->dobj.name));
		}

		/*
		 * DROP must be fully qualified in case same name appears in
		 * pg_catalog
		 */
		appendStringInfo(delq, "ALTER TABLE ONLY %s.",
						  dbms_md_fmtId(tbinfo->dobj.namespace->dobj.name));
		appendStringInfo(delq, "%s ",
						  dbms_md_fmtId(tbinfo->dobj.name));
		appendStringInfo(delq, "DROP CONSTRAINT %s;\n",
						  dbms_md_fmtId(coninfo->dobj.name));

		Assert(tag == NULL);
		tag = psprintf("%s %s", tbinfo->dobj.name, coninfo->dobj.name);
		Assert(tag != NULL);

		if (coninfo->dobj.dump & DUMP_COMPONENT_DEFINITION)
			ArchiveEntry(fout, coninfo->dobj.catId, coninfo->dobj.dumpId,
						 tag,
						 tbinfo->dobj.namespace->dobj.name,
						 indxinfo->tablespace,
						 tbinfo->rolname, false,
						 "CONSTRAINT", SECTION_POST_DATA,
						 q->data, delq->data, NULL,
						 NULL, 0,
						 NULL, NULL);
	}
	else if (coninfo->contype == 'f')
	{
		/*
		 * XXX Potentially wrap in a 'SET CONSTRAINTS OFF' block so that the
		 * current table data is not processed
		 */
		appendStringInfo(q, "ALTER TABLE ONLY %s\n",
						  dbms_md_fmtId(tbinfo->dobj.name));
		appendStringInfo(q, "    ADD CONSTRAINT %s %s;\n",
						  dbms_md_fmtId(coninfo->dobj.name),
						  coninfo->condef);

		/*
		 * DROP must be fully qualified in case same name appears in
		 * pg_catalog
		 */
		appendStringInfo(delq, "ALTER TABLE ONLY %s.",
						  dbms_md_fmtId(tbinfo->dobj.namespace->dobj.name));
		appendStringInfo(delq, "%s ",
						  dbms_md_fmtId(tbinfo->dobj.name));
		appendStringInfo(delq, "DROP CONSTRAINT %s;\n",
						  dbms_md_fmtId(coninfo->dobj.name));

		Assert(tag == NULL);
		tag = psprintf("%s %s", tbinfo->dobj.name, coninfo->dobj.name);
		Assert(tag != NULL);

		if (coninfo->dobj.dump & DUMP_COMPONENT_DEFINITION)
			ArchiveEntry(fout, coninfo->dobj.catId, coninfo->dobj.dumpId,
						 tag,
						 tbinfo->dobj.namespace->dobj.name,
						 NULL,
						 tbinfo->rolname, false,
						 "FK CONSTRAINT", SECTION_POST_DATA,
						 q->data, delq->data, NULL,
						 NULL, 0,
						 NULL, NULL);
	}
	else if (coninfo->contype == 'c' && tbinfo)
	{
		/* CHECK constraint on a table */

		/* Ignore if not to be dumped separately, or if it was inherited */
		if (coninfo->separate && coninfo->conislocal)
		{
			/* not ONLY since we want it to propagate to children */
			appendStringInfo(q, "ALTER TABLE %s\n",
							  dbms_md_fmtId(tbinfo->dobj.name));
			appendStringInfo(q, "    ADD CONSTRAINT %s %s;\n",
							  dbms_md_fmtId(coninfo->dobj.name),
							  coninfo->condef);

			/*
			 * DROP must be fully qualified in case same name appears in
			 * pg_catalog
			 */
			appendStringInfo(delq, "ALTER TABLE %s.",
							  dbms_md_fmtId(tbinfo->dobj.namespace->dobj.name));
			appendStringInfo(delq, "%s ",
							  dbms_md_fmtId(tbinfo->dobj.name));
			appendStringInfo(delq, "DROP CONSTRAINT %s;\n",
							  dbms_md_fmtId(coninfo->dobj.name));

			Assert(tag == NULL);
			tag = psprintf("%s %s", tbinfo->dobj.name, coninfo->dobj.name);
			Assert(tag != NULL);

			if (coninfo->dobj.dump & DUMP_COMPONENT_DEFINITION)
				ArchiveEntry(fout, coninfo->dobj.catId, coninfo->dobj.dumpId,
							 tag,
							 tbinfo->dobj.namespace->dobj.name,
							 NULL,
							 tbinfo->rolname, false,
							 "CHECK CONSTRAINT", SECTION_POST_DATA,
							 q->data, delq->data, NULL,
							 NULL, 0,
							 NULL, NULL);
		}
	}
	else if (coninfo->contype == 'c' && tbinfo == NULL)
	{
		/* CHECK constraint on a domain */
		TypeInfo   *tyinfo = coninfo->condomain;

		/* Ignore if not to be dumped separately */
		if (coninfo->separate)
		{
			appendStringInfo(q, "ALTER DOMAIN %s\n",
							  dbms_md_fmtId(tyinfo->dobj.name));
			appendStringInfo(q, "    ADD CONSTRAINT %s %s;\n",
							  dbms_md_fmtId(coninfo->dobj.name),
							  coninfo->condef);

			/*
			 * DROP must be fully qualified in case same name appears in
			 * pg_catalog
			 */
			appendStringInfo(delq, "ALTER DOMAIN %s.",
							  dbms_md_fmtId(tyinfo->dobj.namespace->dobj.name));
			appendStringInfo(delq, "%s ",
							  dbms_md_fmtId(tyinfo->dobj.name));
			appendStringInfo(delq, "DROP CONSTRAINT %s;\n",
							  dbms_md_fmtId(coninfo->dobj.name));

			Assert(tag == NULL);
			tag = psprintf("%s %s", tyinfo->dobj.name, coninfo->dobj.name);
			Assert(tag != NULL);

			if (coninfo->dobj.dump & DUMP_COMPONENT_DEFINITION)
				ArchiveEntry(fout, coninfo->dobj.catId, coninfo->dobj.dumpId,
							 tag,
							 tyinfo->dobj.namespace->dobj.name,
							 NULL,
							 tyinfo->rolname, false,
							 "CHECK CONSTRAINT", SECTION_POST_DATA,
							 q->data, delq->data, NULL,
							 NULL, 0,
							 NULL, NULL);
		}
	}
	else
	{
		elog(ERROR, "unrecognized constraint type: %c\n",
					  coninfo->contype);
	}

	/* Dump Constraint Comments --- only works for table constraints */
	if (tbinfo && coninfo->separate &&
		coninfo->dobj.dump & DUMP_COMPONENT_COMMENT)
		dumpTableConstraintComment(fout, coninfo);

	if (tag)
		pfree(tag);

	destroyStringInfo(q);
	destroyStringInfo(delq);

	pfree(q);
	pfree(delq);
}

/*
 * dumpTableConstraintComment --- dump a constraint's comment if any
 *
 * This is split out because we need the function in two different places
 * depending on whether the constraint is dumped as part of CREATE TABLE
 * or as a separate ALTER command.
 */
static void
dumpTableConstraintComment(Archive *fout, ConstraintInfo *coninfo)
{
	TableInfo  *tbinfo = coninfo->contable;
	StringInfo labelq = createStringInfo();

	appendStringInfo(labelq, "CONSTRAINT %s ",
					  dbms_md_fmtId(coninfo->dobj.name));
	appendStringInfo(labelq, "ON %s",
					  dbms_md_fmtId(tbinfo->dobj.name));

	if (coninfo->dobj.dump & DUMP_COMPONENT_COMMENT)
		dumpComment(fout, labelq->data,
					tbinfo->dobj.namespace->dobj.name,
					tbinfo->rolname,
					coninfo->dobj.catId, 0,
					coninfo->separate ? coninfo->dobj.dumpId : tbinfo->dobj.dumpId);

	destroyStringInfo(labelq);
	pfree(labelq);
}

/*
 * dumpSequence
 *	  write the declaration (not data) of one user-defined sequence
 */
static void
dumpSequence(Archive *fout, TableInfo *tbinfo)
{
	//DumpOptions *dopt = fout->dopt;
	SPITupleTable   *res;
	char	   *startv,
			   *incby,
			   *maxv,
			   *minv,
			   *cache,
			   *seqtype;
	bool		cycled;
	bool		is_ascending;
	StringInfo query = createStringInfo();
	StringInfo delqry = createStringInfo();
	StringInfo labelq = createStringInfo();

	if (fout->remoteVersion >= 100000)
	{
		/* Make sure we are in proper schema */
		selectSourceSchema(fout, "pg_catalog");

		/* Sequence metadata is stored in pg_ora_sequence in opentenbase_ora mode */
		appendStringInfo(query,
						  "SELECT format_type(seqtypid, NULL), "
						  "seqstart, seqincrement, "
						  "seqmax, seqmin, "
						  "seqcache, seqcycle "
						  "FROM pg_class c "
						  "JOIN %s s ON (s.seqrelid = c.oid) "
						  "WHERE c.oid = '%u'::oid",
						  ORA_MODE ? "pg_ora_sequence" : "pg_sequence",
						  tbinfo->dobj.catId.oid);
	}
	else if (fout->remoteVersion >= 80400)
	{
		/*
		 * Before PostgreSQL 10, sequence metadata is in the sequence itself,
		 * so switch to the sequence's schema instead of pg_catalog.
		 */

		/* Make sure we are in proper schema */
		selectSourceSchema(fout, tbinfo->dobj.namespace->dobj.name);

		appendStringInfo(query,
						  "SELECT 'bigint'::name AS sequence_type, "
						  "start_value, increment_by, max_value, min_value, "
						  "cache_value, is_cycled FROM %s",
						  dbms_md_fmtId(tbinfo->dobj.name));
	}
	else
	{
		/* Make sure we are in proper schema */
		selectSourceSchema(fout, tbinfo->dobj.namespace->dobj.name);

		appendStringInfo(query,
						  "SELECT 'bigint'::name AS sequence_type, "
						  "0 AS start_value, increment_by, max_value, min_value, "
						  "cache_value, is_cycled FROM %s",
						  dbms_md_fmtId(tbinfo->dobj.name));
	}

	res = ExecuteSqlQuery(fout, query->data);

	if (dbms_md_get_tuple_num(res) != 1)
	{
		elog(ERROR, ngettext("query to get data of sequence \"%s\" returned %d row (expected 1)\n",
								 "query to get data of sequence \"%s\" returned %d rows (expected 1)\n",
								 dbms_md_get_tuple_num(res)),
				  tbinfo->dobj.name, dbms_md_get_tuple_num(res));
		return ;
	}

	seqtype = dbms_md_get_field_value(res, 0, SPI_RES_FIRST_FIELD_NO);
	startv = dbms_md_get_field_value(res, 0, SPI_RES_FIRST_FIELD_NO+1);
	incby = dbms_md_get_field_value(res, 0, SPI_RES_FIRST_FIELD_NO+2);
	maxv = dbms_md_get_field_value(res, 0, SPI_RES_FIRST_FIELD_NO+3);
	minv = dbms_md_get_field_value(res, 0, SPI_RES_FIRST_FIELD_NO+4);
	cache = dbms_md_get_field_value(res, 0, SPI_RES_FIRST_FIELD_NO+5);
	cycled = (strcmp(dbms_md_get_field_value(res, 0, SPI_RES_FIRST_FIELD_NO+6), "t") == 0);

	is_ascending = incby[0] != '-';

	if (is_ascending && atoi(minv) == 1)
		minv = NULL;
	if (!is_ascending && atoi(maxv) == -1)
		maxv = NULL;

	if (strcmp(seqtype, "smallint") == 0)
	{
		if (!is_ascending && atoi(minv) == PG_INT16_MIN)
			minv = NULL;
		if (is_ascending && atoi(maxv) == PG_INT16_MAX)
			maxv = NULL;
	}
	else if (strcmp(seqtype, "integer") == 0)
	{
		if (!is_ascending && atoi(minv) == PG_INT32_MIN)
			minv = NULL;
		if (is_ascending && atoi(maxv) == PG_INT32_MAX)
			maxv = NULL;
	}
	else if (strcmp(seqtype, "bigint") == 0)
	{
		char		bufm[100],
					bufx[100];

		snprintf(bufm, sizeof(bufm), INT64_FORMAT, PG_INT64_MIN);
		snprintf(bufx, sizeof(bufx), INT64_FORMAT, PG_INT64_MAX);

		if (!is_ascending && strcmp(minv, bufm) == 0)
			minv = NULL;
		if (is_ascending && strcmp(maxv, bufx) == 0)
			maxv = NULL;
	}

	/*
	 * DROP must be fully qualified in case same name appears in pg_catalog
	 */
	if (!tbinfo->is_identity_sequence)
	{
		appendStringInfo(delqry, "DROP SEQUENCE %s.",
						  dbms_md_fmtId(tbinfo->dobj.namespace->dobj.name));
		appendStringInfo(delqry, "%s;\n",
						  dbms_md_fmtId(tbinfo->dobj.name));
	}

	resetStringInfo(query);

#if 0
	if (dopt->binary_upgrade)
	{
		binary_upgrade_set_pg_class_oids(fout, query,
										 tbinfo->dobj.catId.oid, false);
		binary_upgrade_set_type_oids_by_rel_oid(fout, query,
												tbinfo->dobj.catId.oid);
	}
#endif

	if (tbinfo->is_identity_sequence)
	{
		TableInfo  *owning_tab = findTableByOid(fout, tbinfo->owning_tab);

		appendStringInfo(query,
						  "ALTER TABLE %s ",
						  dbms_md_fmtId(owning_tab->dobj.name));
		appendStringInfo(query,
						  "ALTER COLUMN %s ADD GENERATED ",
						  dbms_md_fmtId(owning_tab->attnames[tbinfo->owning_col - 1]));
		if (owning_tab->attidentity[tbinfo->owning_col - 1] == ATTRIBUTE_IDENTITY_ALWAYS)
			appendStringInfo(query, "ALWAYS");
		else if (owning_tab->attidentity[tbinfo->owning_col - 1] == ATTRIBUTE_IDENTITY_BY_DEFAULT)
			appendStringInfo(query, "BY DEFAULT");
		appendStringInfo(query, " AS IDENTITY (\n    SEQUENCE NAME %s\n",
						  dbms_md_fmtId(tbinfo->dobj.name));
	}
	else
	{
		appendStringInfo(query,
						  "CREATE SEQUENCE %s\n",
						  dbms_md_fmtId(tbinfo->dobj.name));

		if (strcmp(seqtype, "bigint") != 0)
			appendStringInfo(query, "    AS %s\n", seqtype);
	}

	if (fout->remoteVersion >= 80400)
		appendStringInfo(query, "    START WITH %s\n", startv);

	appendStringInfo(query, "    INCREMENT BY %s\n", incby);

	if (minv)
		appendStringInfo(query, "    MINVALUE %s\n", minv);
	else
		appendStringInfoString(query, "    NO MINVALUE\n");

	if (maxv)
		appendStringInfo(query, "    MAXVALUE %s\n", maxv);
	else
		appendStringInfoString(query, "    NO MAXVALUE\n");

	appendStringInfo(query,
					  "    CACHE %s%s",
					  cache, (cycled ? "\n    CYCLE" : ""));

	if (tbinfo->is_identity_sequence)
		appendStringInfoString(query, "\n);\n");
	else
		appendStringInfoString(query, ";\n");

	appendStringInfo(labelq, "SEQUENCE %s", dbms_md_fmtId(tbinfo->dobj.name));

	/* binary_upgrade:	no need to clear TOAST table oid */
#if 0
	if (dopt->binary_upgrade)
		binary_upgrade_extension_member(query, &tbinfo->dobj,
										labelq->data);
#endif

	if (tbinfo->dobj.dump & DUMP_COMPONENT_DEFINITION)
		ArchiveEntry(fout, tbinfo->dobj.catId, tbinfo->dobj.dumpId,
					 tbinfo->dobj.name,
					 tbinfo->dobj.namespace->dobj.name,
					 NULL,
					 tbinfo->rolname,
					 false, "SEQUENCE", SECTION_PRE_DATA,
					 query->data, delqry->data, NULL,
					 NULL, 0,
					 NULL, NULL);

	/*
	 * If the sequence is owned by a table column, emit the ALTER for it as a
	 * separate TOC entry immediately following the sequence's own entry. It's
	 * OK to do this rather than using full sorting logic, because the
	 * dependency that tells us it's owned will have forced the table to be
	 * created first.  We can't just include the ALTER in the TOC entry
	 * because it will fail if we haven't reassigned the sequence owner to
	 * match the table's owner.
	 *
	 * We need not schema-qualify the table reference because both sequence
	 * and table must be in the same schema.
	 */
	if (OidIsValid(tbinfo->owning_tab) && !tbinfo->is_identity_sequence)
	{
		TableInfo  *owning_tab = findTableByOid(fout, tbinfo->owning_tab);

		if (owning_tab == NULL)
			elog(ERROR, "failed sanity check, parent table with OID %u of sequence with OID %u not found\n",
						  tbinfo->owning_tab, tbinfo->dobj.catId.oid);

		if (owning_tab->dobj.dump & DUMP_COMPONENT_DEFINITION)
		{
			resetStringInfo(query);
			appendStringInfo(query, "ALTER SEQUENCE %s",
							  dbms_md_fmtId(tbinfo->dobj.name));
			appendStringInfo(query, " OWNED BY %s",
							  dbms_md_fmtId(owning_tab->dobj.name));
			appendStringInfo(query, ".%s;\n",
							  dbms_md_fmtId(owning_tab->attnames[tbinfo->owning_col - 1]));

			if (tbinfo->dobj.dump & DUMP_COMPONENT_DEFINITION)
				ArchiveEntry(fout, nilCatalogId, createDumpId(fout),
							 tbinfo->dobj.name,
							 tbinfo->dobj.namespace->dobj.name,
							 NULL,
							 tbinfo->rolname,
							 false, "SEQUENCE OWNED BY", SECTION_PRE_DATA,
							 query->data, "", NULL,
							 &(tbinfo->dobj.dumpId), 1,
							 NULL, NULL);
		}
	}

	/* Dump Sequence Comments and Security Labels */
	if (tbinfo->dobj.dump & DUMP_COMPONENT_COMMENT)
		dumpComment(fout, labelq->data,
					tbinfo->dobj.namespace->dobj.name, tbinfo->rolname,
					tbinfo->dobj.catId, 0, tbinfo->dobj.dumpId);

	if (tbinfo->dobj.dump & DUMP_COMPONENT_SECLABEL)
		dumpSecLabel(fout, labelq->data,
					 tbinfo->dobj.namespace->dobj.name, tbinfo->rolname,
					 tbinfo->dobj.catId, 0, tbinfo->dobj.dumpId);

	dbms_md_free_tuples(res);

	destroyStringInfo(query);
	destroyStringInfo(delqry);
	destroyStringInfo(labelq);

	pfree(query);
	pfree(delqry);
	pfree(labelq);
}

/*
 * dumpSequenceData
 *	  write the data of one user-defined sequence
 */
static void
dumpSequenceData(Archive *fout, TableDataInfo *tdinfo)
{
	TableInfo  *tbinfo = tdinfo->tdtable;
	SPITupleTable   *res;
	char	   *last;
	bool		called;
	StringInfo query = createStringInfo();

	/* Make sure we are in proper schema */
	selectSourceSchema(fout, tbinfo->dobj.namespace->dobj.name);

	appendStringInfo(query,
					  "SELECT last_value, is_called FROM %s",
					  dbms_md_fmtId(tbinfo->dobj.name));

	res = ExecuteSqlQuery(fout, query->data);

	if (dbms_md_get_tuple_num(res) != 1)
	{
		elog(ERROR, ngettext("query to get data of sequence \"%s\" returned %d row (expected 1)\n",
								 "query to get data of sequence \"%s\" returned %d rows (expected 1)\n",
								 dbms_md_get_tuple_num(res)),
				  tbinfo->dobj.name, dbms_md_get_tuple_num(res));
		return;
	}

	last = dbms_md_get_field_value(res, 0, SPI_RES_FIRST_FIELD_NO);
	called = (strcmp(dbms_md_get_field_value(res, 0, SPI_RES_FIRST_FIELD_NO + 1), "t") == 0);
#ifdef PGXC
	if (fout->isPostgresXL)
	{
		/* 
		 * In Postgres-XC it is possible that the current value of a
		 * sequence cached on each node is different as several sessions
		 * might use the sequence on different nodes. So what we do here
		 * to get a consistent dump is to get the next value of sequence.
		 * This insures that sequence value is unique as nextval is directly
		 * obtained from GTM.
		 */
		resetStringInfo(query);
		appendStringInfoString(query, "SELECT pg_catalog.nextval(");
		appendStringLiteralAH(query, dbms_md_fmtId(tbinfo->dobj.name), fout);
		appendStringInfo(query, ");\n");
		res = ExecuteSqlQuery(fout, query->data);

		if (dbms_md_get_tuple_num(res) != 1)
		{
			elog(ERROR, ngettext("query to get nextval of sequence \"%s\" "
									 "returned %d rows (expected 1)\n",
										"query to get nextval of sequence \"%s\" "
									 "returned %d rows (expected 1)\n",
									 dbms_md_get_tuple_num(res)),
					tbinfo->dobj.name, dbms_md_get_tuple_num(res));
			return ;
		}

		last = dbms_md_get_field_value(res, 0, SPI_RES_FIRST_FIELD_NO);
	}
#endif

	resetStringInfo(query);
	appendStringInfoString(query, "SELECT pg_catalog.setval(");
	appendStringLiteralAH(query, dbms_md_fmtId(tbinfo->dobj.name), fout);
	appendStringInfo(query, ", %s, %s);\n",
					  last, (called ? "true" : "false"));

	if (tdinfo->dobj.dump & DUMP_COMPONENT_DATA)
		ArchiveEntry(fout, nilCatalogId, createDumpId(fout),
					 tbinfo->dobj.name,
					 tbinfo->dobj.namespace->dobj.name,
					 NULL,
					 tbinfo->rolname,
					 false, "SEQUENCE SET", SECTION_DATA,
					 query->data, "", NULL,
					 &(tbinfo->dobj.dumpId), 1,
					 NULL, NULL);

	dbms_md_free_tuples(res);

	destroyStringInfo(query);
	pfree(query);
}

/*
 * dumpTrigger
 *	  write the declaration of one user-defined table trigger
 */
static void
dumpTrigger(Archive *fout, TriggerInfo *tginfo)
{
	DumpOptions *dopt = fout->dopt;
	TableInfo  *tbinfo = tginfo->tgtable;
	StringInfo query;
	StringInfo delqry;
	StringInfo labelq;
	char	   *tgargs;
	size_t		lentgargs;
	const char *p;
	int			findx;
	char	   *tag = NULL;

	/*
	 * we needn't check dobj.dump because TriggerInfo wouldn't have been
	 * created in the first place for non-dumpable triggers
	 */
	if (dopt->dataOnly)
		return;

	query = createStringInfo();
	delqry = createStringInfo();
	labelq = createStringInfo();

	/*
	 * DROP must be fully qualified in case same name appears in pg_catalog
	 */
	appendStringInfo(delqry, "DROP TRIGGER %s ",
					  dbms_md_fmtId(tginfo->dobj.name));
	appendStringInfo(delqry, "ON %s.",
					  dbms_md_fmtId(tbinfo->dobj.namespace->dobj.name));
	appendStringInfo(delqry, "%s;\n",
					  dbms_md_fmtId(tbinfo->dobj.name));

	if (tginfo->tgdef)
	{
		appendStringInfo(query, "%s;\n", tginfo->tgdef);
	}
	else
	{
		if (tginfo->tgisconstraint)
		{
			appendStringInfoString(query, "CREATE CONSTRAINT TRIGGER ");
			appendStringInfoString(query, dbms_md_fmtId(tginfo->tgconstrname));
		}
		else
		{
			appendStringInfoString(query, "CREATE TRIGGER ");
			appendStringInfoString(query, dbms_md_fmtId(tginfo->dobj.name));
		}
		appendStringInfoString(query, "\n    ");

		/* Trigger type */
		if (TRIGGER_FOR_BEFORE(tginfo->tgtype))
			appendStringInfoString(query, "BEFORE");
		else if (TRIGGER_FOR_AFTER(tginfo->tgtype))
			appendStringInfoString(query, "AFTER");
		else if (TRIGGER_FOR_INSTEAD(tginfo->tgtype))
			appendStringInfoString(query, "INSTEAD OF");
		else
		{
			elog(ERROR, "unexpected tgtype value: %d\n", tginfo->tgtype);
			return ;
		}

		findx = 0;
		if (TRIGGER_FOR_INSERT(tginfo->tgtype))
		{
			appendStringInfoString(query, " INSERT");
			findx++;
		}
		if (TRIGGER_FOR_DELETE(tginfo->tgtype))
		{
			if (findx > 0)
				appendStringInfoString(query, " OR DELETE");
			else
				appendStringInfoString(query, " DELETE");
			findx++;
		}
		if (TRIGGER_FOR_UPDATE(tginfo->tgtype))
		{
			if (findx > 0)
				appendStringInfoString(query, " OR UPDATE");
			else
				appendStringInfoString(query, " UPDATE");
			findx++;
		}
		if (TRIGGER_FOR_TRUNCATE(tginfo->tgtype))
		{
			if (findx > 0)
				appendStringInfoString(query, " OR TRUNCATE");
			else
				appendStringInfoString(query, " TRUNCATE");
			findx++;
		}
		appendStringInfo(query, " ON %s\n",
						  dbms_md_fmtId(tbinfo->dobj.name));

		if (tginfo->tgisconstraint)
		{
			if (OidIsValid(tginfo->tgconstrrelid))
			{
				/* regclass output is already quoted */
				appendStringInfo(query, "    FROM %s\n    ",
								  tginfo->tgconstrrelname);
			}
			if (!tginfo->tgdeferrable)
				appendStringInfoString(query, "NOT ");
			appendStringInfoString(query, "DEFERRABLE INITIALLY ");
			if (tginfo->tginitdeferred)
				appendStringInfoString(query, "DEFERRED\n");
			else
				appendStringInfoString(query, "IMMEDIATE\n");
		}

		if (TRIGGER_FOR_ROW(tginfo->tgtype))
			appendStringInfoString(query, "    FOR EACH ROW\n    ");
		else
			appendStringInfoString(query, "    FOR EACH STATEMENT\n    ");

		/* regproc output is already sufficiently quoted */
		appendStringInfo(query, "EXECUTE PROCEDURE %s(",
						  tginfo->tgfname);

		tgargs = (char *) dbms_md_escape_bytea((unsigned char *) tginfo->tgargs,
										  &lentgargs);
		p = tgargs;
		for (findx = 0; findx < tginfo->tgnargs; findx++)
		{
			/* find the embedded null that terminates this trigger argument */
			size_t		tlen = strlen(p);

			if (p + tlen >= tgargs + lentgargs)
			{
				/* hm, not found before end of bytea value... */
				elog(ERROR, "invalid argument string (%s) for trigger \"%s\" on table \"%s\"\n",
						  tginfo->tgargs,
						  tginfo->dobj.name,
						  tbinfo->dobj.name);
				return ;
			}

			if (findx > 0)
				appendStringInfoString(query, ", ");
			appendStringLiteralAH(query, p, fout);
			p += tlen + 1;
		}
		pfree(tgargs);
		appendStringInfoString(query, ");\n");
	}

	if (tginfo->tgenabled != 't' && tginfo->tgenabled != 'O')
	{
		appendStringInfo(query, "\nALTER TABLE %s ",
						  dbms_md_fmtId(tbinfo->dobj.name));
		switch (tginfo->tgenabled)
		{
			case 'D':
			case 'f':
				appendStringInfoString(query, "DISABLE");
				break;
			case 'A':
				appendStringInfoString(query, "ENABLE ALWAYS");
				break;
			case 'R':
				appendStringInfoString(query, "ENABLE REPLICA");
				break;
			default:
				appendStringInfoString(query, "ENABLE");
				break;
		}
		appendStringInfo(query, " TRIGGER %s;\n",
						  dbms_md_fmtId(tginfo->dobj.name));
	}

	appendStringInfo(labelq, "TRIGGER %s ",
					  dbms_md_fmtId(tginfo->dobj.name));
	appendStringInfo(labelq, "ON %s",
					  dbms_md_fmtId(tbinfo->dobj.name));

	tag = psprintf("%s %s", tbinfo->dobj.name, tginfo->dobj.name);
	Assert(tag != NULL);

	if (tginfo->dobj.dump & DUMP_COMPONENT_DEFINITION)
		ArchiveEntry(fout, tginfo->dobj.catId, tginfo->dobj.dumpId,
					 tag,
					 tbinfo->dobj.namespace->dobj.name,
					 NULL,
					 tbinfo->rolname, false,
					 "TRIGGER", SECTION_POST_DATA,
					 query->data, delqry->data, NULL,
					 NULL, 0,
					 NULL, NULL);

	if (tginfo->dobj.dump & DUMP_COMPONENT_COMMENT)
		dumpComment(fout, labelq->data,
					tbinfo->dobj.namespace->dobj.name, tbinfo->rolname,
					tginfo->dobj.catId, 0, tginfo->dobj.dumpId);

	Assert(tag != NULL);
	pfree(tag);

	destroyStringInfo(query);
	destroyStringInfo(delqry);
	destroyStringInfo(labelq);

	pfree(query);
	pfree(delqry);
	pfree(labelq);
}

/*
 * dumpEventTrigger
 *	  write the declaration of one user-defined event trigger
 */
static void
dumpEventTrigger(Archive *fout, EventTriggerInfo *evtinfo)
{
	DumpOptions *dopt = fout->dopt;
	StringInfo query;
	StringInfo delqry;
	StringInfo labelq;

	/* Skip if not to be dumped */
	if (!evtinfo->dobj.dump || dopt->dataOnly)
		return;

	query = createStringInfo();
	delqry = createStringInfo();
	labelq = createStringInfo();

	appendStringInfoString(query, "CREATE EVENT TRIGGER ");
	appendStringInfoString(query, dbms_md_fmtId(evtinfo->dobj.name));
	appendStringInfoString(query, " ON ");
	appendStringInfoString(query, dbms_md_fmtId(evtinfo->evtevent));

	if (strcmp("", evtinfo->evttags) != 0)
	{
		appendStringInfoString(query, "\n         WHEN TAG IN (");
		appendStringInfoString(query, evtinfo->evttags);
		appendStringInfoChar(query, ')');
	}

	appendStringInfoString(query, "\n   EXECUTE PROCEDURE ");
	appendStringInfoString(query, evtinfo->evtfname);
	appendStringInfoString(query, "();\n");

	if (evtinfo->evtenabled != 'O')
	{
		appendStringInfo(query, "\nALTER EVENT TRIGGER %s ",
						  dbms_md_fmtId(evtinfo->dobj.name));
		switch (evtinfo->evtenabled)
		{
			case 'D':
				appendStringInfoString(query, "DISABLE");
				break;
			case 'A':
				appendStringInfoString(query, "ENABLE ALWAYS");
				break;
			case 'R':
				appendStringInfoString(query, "ENABLE REPLICA");
				break;
			default:
				appendStringInfoString(query, "ENABLE");
				break;
		}
		appendStringInfoString(query, ";\n");
	}

	appendStringInfo(delqry, "DROP EVENT TRIGGER %s;\n",
					  dbms_md_fmtId(evtinfo->dobj.name));

	appendStringInfo(labelq, "EVENT TRIGGER %s",
					  dbms_md_fmtId(evtinfo->dobj.name));

	if (evtinfo->dobj.dump & DUMP_COMPONENT_DEFINITION)
		ArchiveEntry(fout, evtinfo->dobj.catId, evtinfo->dobj.dumpId,
					 evtinfo->dobj.name, NULL, NULL,
					 evtinfo->evtowner, false,
					 "EVENT TRIGGER", SECTION_POST_DATA,
					 query->data, delqry->data, NULL,
					 NULL, 0,
					 NULL, NULL);

	if (evtinfo->dobj.dump & DUMP_COMPONENT_COMMENT)
		dumpComment(fout, labelq->data,
					NULL, evtinfo->evtowner,
					evtinfo->dobj.catId, 0, evtinfo->dobj.dumpId);

	destroyStringInfo(query);
	destroyStringInfo(delqry);
	destroyStringInfo(labelq);

	pfree(query);
	pfree(delqry);
	pfree(labelq);
}

/*
 * dumpRule
 *		Dump a rule
 */
static void
dumpRule(Archive *fout, RuleInfo *rinfo)
{
	DumpOptions *dopt = fout->dopt;
	TableInfo  *tbinfo = rinfo->ruletable;
	bool		is_view;
	StringInfo query;
	StringInfo cmd;
	StringInfo delcmd;
	StringInfo labelq;
	SPITupleTable   *res;
	char	   *tag = NULL;

	/* Skip if not to be dumped */
	if (!rinfo->dobj.dump || dopt->dataOnly)
		return;

	/*
	 * If it is an ON SELECT rule that is created implicitly by CREATE VIEW,
	 * we do not want to dump it as a separate object.
	 */
	if (!rinfo->separate)
		return;

	/*
	 * If it's an ON SELECT rule, we want to print it as a view definition,
	 * instead of a rule.
	 */
	is_view = (rinfo->ev_type == '1' && rinfo->is_instead);

	/*
	 * Make sure we are in proper schema.
	 */
	selectSourceSchema(fout, tbinfo->dobj.namespace->dobj.name);

	query = createStringInfo();
	cmd = createStringInfo();
	delcmd = createStringInfo();
	labelq = createStringInfo();

	if (is_view)
	{
		StringInfo result;

		/*
		 * We need OR REPLACE here because we'll be replacing a dummy view.
		 * Otherwise this should look largely like the regular view dump code.
		 */
		appendStringInfo(cmd, "CREATE OR REPLACE VIEW %s",
						  dbms_md_fmtId(tbinfo->dobj.name));
		if (nonemptyReloptions(tbinfo->reloptions))
		{
			appendStringInfoString(cmd, " WITH (");
			appendReloptionsArrayAH(cmd, tbinfo->reloptions, "", fout);
			appendStringInfoChar(cmd, ')');
		}
		result = createViewAsClause(fout, tbinfo);
		appendStringInfo(cmd, " AS\n%s", result->data);
		destroyStringInfo(result);
		if (tbinfo->checkoption != NULL)
			appendStringInfo(cmd, "\n  WITH %s CHECK OPTION",
							  tbinfo->checkoption);
		appendStringInfoString(cmd, ";\n");
	}
	else
	{
		/* In the rule case, just print pg_get_ruledef's result verbatim */
		appendStringInfo(query,
						  "SELECT pg_catalog.pg_get_ruledef('%u'::pg_catalog.oid)",
						  rinfo->dobj.catId.oid);

		res = ExecuteSqlQuery(fout, query->data);

		if (dbms_md_get_tuple_num(res) != 1)
		{
			elog(ERROR, "query to get rule \"%s\" for table \"%s\" failed: wrong number of rows returned\n",
					  rinfo->dobj.name, tbinfo->dobj.name);
			return ;
		}

		printfStringInfo(cmd, "%s\n", dbms_md_get_field_value(res, 0, SPI_RES_FIRST_FIELD_NO));

		dbms_md_free_tuples(res);
	}

	/*
	 * Add the command to alter the rules replication firing semantics if it
	 * differs from the default.
	 */
	if (rinfo->ev_enabled != 'O')
	{
		appendStringInfo(cmd, "ALTER TABLE %s ", dbms_md_fmtId(tbinfo->dobj.name));
		switch (rinfo->ev_enabled)
		{
			case 'A':
				appendStringInfo(cmd, "ENABLE ALWAYS RULE %s;\n",
								  dbms_md_fmtId(rinfo->dobj.name));
				break;
			case 'R':
				appendStringInfo(cmd, "ENABLE REPLICA RULE %s;\n",
								  dbms_md_fmtId(rinfo->dobj.name));
				break;
			case 'D':
				appendStringInfo(cmd, "DISABLE RULE %s;\n",
								  dbms_md_fmtId(rinfo->dobj.name));
				break;
		}
	}

	/*
	 * DROP must be fully qualified in case same name appears in pg_catalog
	 */
	if (is_view)
	{
		/*
		 * We can't DROP a view's ON SELECT rule.  Instead, use CREATE OR
		 * REPLACE VIEW to replace the rule with something with minimal
		 * dependencies.
		 */
		StringInfo result;

		appendStringInfo(delcmd, "CREATE OR REPLACE VIEW %s.",
						  dbms_md_fmtId(tbinfo->dobj.namespace->dobj.name));
		appendStringInfo(delcmd, "%s",
						  dbms_md_fmtId(tbinfo->dobj.name));
		result = createDummyViewAsClause(fout, tbinfo);
		appendStringInfo(delcmd, " AS\n%s;\n", result->data);
		destroyStringInfo(result);
	}
	else
	{
		appendStringInfo(delcmd, "DROP RULE %s ",
						  dbms_md_fmtId(rinfo->dobj.name));
		appendStringInfo(delcmd, "ON %s.",
						  dbms_md_fmtId(tbinfo->dobj.namespace->dobj.name));
		appendStringInfo(delcmd, "%s;\n",
						  dbms_md_fmtId(tbinfo->dobj.name));
	}

	appendStringInfo(labelq, "RULE %s",
					  dbms_md_fmtId(rinfo->dobj.name));
	appendStringInfo(labelq, " ON %s",
					  dbms_md_fmtId(tbinfo->dobj.name));

	tag = psprintf("%s %s", tbinfo->dobj.name, rinfo->dobj.name);
	Assert(tag != NULL);

	if (rinfo->dobj.dump & DUMP_COMPONENT_DEFINITION)
		ArchiveEntry(fout, rinfo->dobj.catId, rinfo->dobj.dumpId,
					 tag,
					 tbinfo->dobj.namespace->dobj.name,
					 NULL,
					 tbinfo->rolname, false,
					 "RULE", SECTION_POST_DATA,
					 cmd->data, delcmd->data, NULL,
					 NULL, 0,
					 NULL, NULL);

	/* Dump rule comments */
	if (rinfo->dobj.dump & DUMP_COMPONENT_COMMENT)
		dumpComment(fout, labelq->data,
					tbinfo->dobj.namespace->dobj.name,
					tbinfo->rolname,
					rinfo->dobj.catId, 0, rinfo->dobj.dumpId);

	Assert(tag != NULL);
	pfree(tag);

	destroyStringInfo(query);
	destroyStringInfo(cmd);
	destroyStringInfo(delcmd);
	destroyStringInfo(labelq);

	pfree(query);
	pfree(cmd);
	pfree(delcmd);
	pfree(labelq);
}

/*
 * getExtensionMembership --- obtain extension membership data
 *
 * We need to identify objects that are extension members as soon as they're
 * loaded, so that we can correctly determine whether they need to be dumped.
 * Generally speaking, extension member objects will get marked as *not* to
 * be dumped, as they will be recreated by the single CREATE EXTENSION
 * command.  However, in binary upgrade mode we still need to dump the members
 * individually.
 */
void
getExtensionMembership(Archive *fout, ExtensionInfo extinfo[],
					   int numExtensions)
{
	StringInfo query;
	SPITupleTable   *res;
	int			ntups,
				nextmembers,
				i;
	int			i_classid,
				i_objid,
				i_refobjid;
	ExtensionMemberId *extmembers;
	ExtensionInfo *ext;

	/* Nothing to do if no extensions */
	if (numExtensions == 0)
		return;

	/* Make sure we are in proper schema */
	selectSourceSchema(fout, "pg_catalog");

	query = createStringInfo();

	/* refclassid constraint is redundant but may speed the search */
	appendStringInfo(query, "SELECT "
						 "classid, objid, refobjid "
						 "FROM pg_depend "
						 "WHERE refclassid = %u"
						 "AND deptype = 'e' "
						 "ORDER BY 3", ExtensionRelationId);

	res = ExecuteSqlQuery(fout, query->data);

	ntups = dbms_md_get_tuple_num(res);

	i_classid = dbms_md_get_field_subscript(res, "classid");
	i_objid = dbms_md_get_field_subscript(res, "objid");
	i_refobjid = dbms_md_get_field_subscript(res, "refobjid");

	extmembers = (ExtensionMemberId *) dbms_md_malloc0(fout->memory_ctx, 
												ntups * sizeof(ExtensionMemberId));
	nextmembers = 0;

	/*
	 * Accumulate data into extmembers[].
	 *
	 * Since we ordered the SELECT by referenced ID, we can expect that
	 * multiple entries for the same extension will appear together; this
	 * saves on searches.
	 */
	ext = NULL;

	for (i = 0; i < ntups; i++)
	{
		CatalogId	objId;
		Oid			extId;

		objId.tableoid = dbms_md_str_to_oid(dbms_md_get_field_value(res, i, i_classid));
		objId.oid = dbms_md_str_to_oid(dbms_md_get_field_value(res, i, i_objid));
		extId = dbms_md_str_to_oid(dbms_md_get_field_value(res, i, i_refobjid));

		if (ext == NULL ||
			ext->dobj.catId.oid != extId)
			ext = findExtensionByOid(fout, extId);

		if (ext == NULL)
		{
			/* shouldn't happen */
			fprintf(stderr, "could not find referenced extension %u\n", extId);
			continue;
		}

		extmembers[nextmembers].catId = objId;
		extmembers[nextmembers].ext = ext;
		nextmembers++;
	}

	dbms_md_free_tuples(res);

	/* Remember the data for use later */
	setExtensionMembership(fout, extmembers, nextmembers);

	destroyStringInfo(query);
	pfree(query);
}

/*
 * processExtensionTables --- deal with extension configuration tables
 *
 * There are two parts to this process:
 *
 * 1. Identify and create dump records for extension configuration tables.
 *
 *	  Extensions can mark tables as "configuration", which means that the user
 *	  is able and expected to modify those tables after the extension has been
 *	  loaded.  For these tables, we dump out only the data- the structure is
 *	  expected to be handled at CREATE EXTENSION time, including any indexes or
 *	  foreign keys, which brings us to-
 *
 * 2. Record FK dependencies between configuration tables.
 *
 *	  Due to the FKs being created at CREATE EXTENSION time and therefore before
 *	  the data is loaded, we have to work out what the best order for reloading
 *	  the data is, to avoid FK violations when the tables are restored.  This is
 *	  not perfect- we can't handle circular dependencies and if any exist they
 *	  will cause an invalid dump to be produced (though at least all of the data
 *	  is included for a user to manually restore).  This is currently documented
 *	  but perhaps we can provide a better solution in the future.
 */
void
processExtensionTables(Archive *fout, ExtensionInfo extinfo[],
					   int numExtensions)
{
	DumpOptions *dopt = fout->dopt;
	DBMS_MD_DUMP_GVAR_ST *dump_gvar = &(dopt->dump_gvar);
	StringInfo query;
	SPITupleTable   *res;
	int			ntups,
				i;
	int			i_conrelid,
				i_confrelid;

	/* Nothing to do if no extensions */
	if (numExtensions == 0)
		return;

	/*
	 * Identify extension configuration tables and create TableDataInfo
	 * objects for them, ensuring their data will be dumped even though the
	 * tables themselves won't be.
	 *
	 * Note that we create TableDataInfo objects even in schemaOnly mode, ie,
	 * user data in a configuration table is treated like schema data. This
	 * seems appropriate since system data in a config table would get
	 * reloaded by CREATE EXTENSION.
	 */
	for (i = 0; i < numExtensions; i++)
	{
		ExtensionInfo *curext = &(extinfo[i]);
		char	   *extconfig = curext->extconfig;
		char	   *extcondition = curext->extcondition;
		char	  **extconfigarray = NULL;
		char	  **extconditionarray = NULL;
		int			nconfigitems;
		int			nconditionitems;

		if (dbms_md_parse_array(extconfig, &extconfigarray, &nconfigitems) &&
			dbms_md_parse_array(extcondition, &extconditionarray, &nconditionitems) &&
			nconfigitems == nconditionitems)
		{
			int			j;

			for (j = 0; j < nconfigitems; j++)
			{
				TableInfo  *configtbl;
				Oid			configtbloid = dbms_md_str_to_oid(extconfigarray[j]);
				bool		dumpobj =
				curext->dobj.dump & DUMP_COMPONENT_DEFINITION;

				configtbl = findTableByOid(fout, configtbloid);
				if (configtbl == NULL)
					continue;

				/*
				 * Tables of not-to-be-dumped extensions shouldn't be dumped
				 * unless the table or its schema is explicitly included
				 */
				if (!(curext->dobj.dump & DUMP_COMPONENT_DEFINITION))
				{
					/* check table explicitly requested */
					if (dump_gvar->table_include_oids.head != NULL &&
						dbms_md_oid_list_member(&dump_gvar->table_include_oids,
											   configtbloid))
						dumpobj = true;

					/* check table's schema explicitly requested */
					if (configtbl->dobj.namespace->dobj.dump &
						DUMP_COMPONENT_DATA)
						dumpobj = true;
				}

				/* check table excluded by an exclusion switch */
				/*if (table_exclude_oids.head != NULL &&
					dbms_md_oid_list_member(&table_exclude_oids,
										   configtbloid))
					dumpobj = false;*/

				/* check schema excluded by an exclusion switch */
				/*if (dbms_md_oid_list_member(&schema_exclude_oids,
										   configtbl->dobj.namespace->dobj.catId.oid))
					dumpobj = false;*/

				if (dumpobj)
				{
					/*
					 * Note: config tables are dumped without OIDs regardless
					 * of the --oids setting.  This is because row filtering
					 * conditions aren't compatible with dumping OIDs.
					 */
					makeTableDataInfo(fout, configtbl, false);
					if (configtbl->dataObj != NULL)
					{
						if (strlen(extconditionarray[j]) > 0)
							configtbl->dataObj->filtercond = dbms_md_strdup(extconditionarray[j]);
					}
				}
			}
		}
		if (extconfigarray)
			pfree(extconfigarray);
		if (extconditionarray)
			pfree(extconditionarray);
	}

	/*
	 * Now that all the TableInfoData objects have been created for all the
	 * extensions, check their FK dependencies and register them to try and
	 * dump the data out in an order that they can be restored in.
	 *
	 * Note that this is not a problem for user tables as their FKs are
	 * recreated after the data has been loaded.
	 */

	/* Make sure we are in proper schema */
	selectSourceSchema(fout, "pg_catalog");

	query = createStringInfo();

	printfStringInfo(query,
					  "SELECT conrelid, confrelid "
					  "FROM pg_constraint "
					  "JOIN pg_depend ON (objid = confrelid) "
					  "WHERE contype = 'f' "
					  "AND refclassid = 'pg_extension'::regclass "
					  "AND classid = 'pg_class'::regclass;");

	res = ExecuteSqlQuery(fout, query->data);
	ntups = dbms_md_get_tuple_num(res);

	i_conrelid = dbms_md_get_field_subscript(res, "conrelid");
	i_confrelid = dbms_md_get_field_subscript(res, "confrelid");

	/* Now get the dependencies and register them */
	for (i = 0; i < ntups; i++)
	{
		Oid			conrelid,
					confrelid;
		TableInfo  *reftable,
				   *contable;

		conrelid = dbms_md_str_to_oid(dbms_md_get_field_value(res, i, i_conrelid));
		confrelid = dbms_md_str_to_oid(dbms_md_get_field_value(res, i, i_confrelid));
		contable = findTableByOid(fout, conrelid);
		reftable = findTableByOid(fout, confrelid);

		if (reftable == NULL ||
			reftable->dataObj == NULL ||
			contable == NULL ||
			contable->dataObj == NULL)
			continue;

		/*
		 * Make referencing TABLE_DATA object depend on the referenced table's
		 * TABLE_DATA object.
		 */
		addObjectDependency(fout, &contable->dataObj->dobj,
							reftable->dataObj->dobj.dumpId);
	}
	
	dbms_md_free_tuples(res);
	destroyStringInfo(query);
	pfree(query);
}

/*
 * getDependencies --- obtain available dependency data
 */
static void
getDependencies(Archive *fout)
{
	StringInfo query;
	SPITupleTable   *res;
	int			ntups,
				i;
	int			i_classid,
				i_objid,
				i_refclassid,
				i_refobjid,
				i_deptype;
	DumpableObject *dobj,
			   *refdobj;

	write_msg(NULL, "reading dependency data\n");

	/* Make sure we are in proper schema */
	selectSourceSchema(fout, "pg_catalog");

	query = createStringInfo();

	/*
	 * PIN dependencies aren't interesting, and EXTENSION dependencies were
	 * already processed by getExtensionMembership.
	 */
	appendStringInfoString(query, "SELECT "
						 "classid, objid, refclassid, refobjid, deptype "
						 "FROM pg_depend "
						 "WHERE deptype != 'p' AND deptype != 'e' "
						 "ORDER BY 1,2");

	res = ExecuteSqlQuery(fout, query->data);

	ntups = dbms_md_get_tuple_num(res);

	i_classid = dbms_md_get_field_subscript(res, "classid");
	i_objid = dbms_md_get_field_subscript(res, "objid");
	i_refclassid = dbms_md_get_field_subscript(res, "refclassid");
	i_refobjid = dbms_md_get_field_subscript(res, "refobjid");
	i_deptype = dbms_md_get_field_subscript(res, "deptype");

	/*
	 * Since we ordered the SELECT by referencing ID, we can expect that
	 * multiple entries for the same object will appear together; this saves
	 * on searches.
	 */
	dobj = NULL;

	for (i = 0; i < ntups; i++)
	{
		CatalogId	objId;
		CatalogId	refobjId;
		char		deptype;

		objId.tableoid = dbms_md_str_to_oid(dbms_md_get_field_value(res, i, i_classid));
		objId.oid = dbms_md_str_to_oid(dbms_md_get_field_value(res, i, i_objid));
		refobjId.tableoid = dbms_md_str_to_oid(dbms_md_get_field_value(res, i, i_refclassid));
		refobjId.oid = dbms_md_str_to_oid(dbms_md_get_field_value(res, i, i_refobjid));
		deptype = *(dbms_md_get_field_value(res, i, i_deptype));

		if (dobj == NULL ||
			dobj->catId.tableoid != objId.tableoid ||
			dobj->catId.oid != objId.oid)
			dobj = findObjectByCatalogId(fout, objId);

		/*
		 * Failure to find objects mentioned in pg_depend is not unexpected,
		 * since for example we don't collect info about TOAST tables.
		 */
		if (dobj == NULL)
		{
#ifdef NOT_USED
			fprintf(stderr, "no referencing object %u %u\n",
					objId.tableoid, objId.oid);
#endif
			continue;
		}

		refdobj = findObjectByCatalogId(fout, refobjId);

		if (refdobj == NULL)
		{
#ifdef NOT_USED
			fprintf(stderr, "no referenced object %u %u\n",
					refobjId.tableoid, refobjId.oid);
#endif
			continue;
		}

		/*
		 * Ordinarily, table rowtypes have implicit dependencies on their
		 * tables.  However, for a composite type the implicit dependency goes
		 * the other way in pg_depend; which is the right thing for DROP but
		 * it doesn't produce the dependency ordering we need. So in that one
		 * case, we reverse the direction of the dependency.
		 */
		if (deptype == 'i' &&
			dobj->objType == DO_TABLE &&
			refdobj->objType == DO_TYPE)
			addObjectDependency(fout, refdobj, dobj->dumpId);
		else
			/* normal case */
			addObjectDependency(fout, dobj, refdobj->dumpId);
	}

	dbms_md_free_tuples(res);

	destroyStringInfo(query);
	pfree(query);
}


/*
 * createBoundaryObjects - create dummy DumpableObjects to represent
 * dump section boundaries.
 */
static DumpableObject *
createBoundaryObjects(Archive *fout)
{
	DumpableObject *dobjs;

	dobjs = (DumpableObject *) dbms_md_malloc0(fout->memory_ctx, 2 * sizeof(DumpableObject));

	dobjs[0].objType = DO_PRE_DATA_BOUNDARY;
	dobjs[0].catId = nilCatalogId;
	AssignDumpId(fout, dobjs + 0);
	dobjs[0].name = dbms_md_strdup("PRE-DATA BOUNDARY");

	dobjs[1].objType = DO_POST_DATA_BOUNDARY;
	dobjs[1].catId = nilCatalogId;
	AssignDumpId(fout, dobjs + 1);
	dobjs[1].name = dbms_md_strdup("POST-DATA BOUNDARY");

	return dobjs;
}

/*
 * addBoundaryDependencies - add dependencies as needed to enforce the dump
 * section boundaries.
 */
static void
addBoundaryDependencies(Archive *fout, DumpableObject **dobjs, int numObjs,
						DumpableObject *boundaryObjs)
{
	DumpableObject *preDataBound = boundaryObjs + 0;
	DumpableObject *postDataBound = boundaryObjs + 1;
	int			i;

	for (i = 0; i < numObjs; i++)
	{
		DumpableObject *dobj = dobjs[i];

		/*
		 * The classification of object types here must match the SECTION_xxx
		 * values assigned during subsequent ArchiveEntry calls!
		 */
		switch (dobj->objType)
		{
			case DO_NAMESPACE:
			case DO_EXTENSION:
			case DO_TYPE:
			case DO_SHELL_TYPE:
			case DO_FUNC:
			case DO_AGG:
			case DO_OPERATOR:
			case DO_ACCESS_METHOD:
			case DO_OPCLASS:
			case DO_OPFAMILY:
			case DO_COLLATION:
			case DO_CONVERSION:
			case DO_TABLE:
			case DO_ATTRDEF:
			case DO_PROCLANG:
			case DO_CAST:
			case DO_DUMMY_TYPE:
			case DO_TSPARSER:
			case DO_TSDICT:
			case DO_TSTEMPLATE:
			case DO_TSCONFIG:
			case DO_FDW:
			case DO_FOREIGN_SERVER:
			case DO_TRANSFORM:
			case DO_BLOB:
				/* Pre-data objects: must come before the pre-data boundary */
				addObjectDependency(fout, preDataBound, dobj->dumpId);
				break;
			case DO_TABLE_DATA:
			case DO_SEQUENCE_SET:
			case DO_BLOB_DATA:
				/* Data objects: must come between the boundaries */
				addObjectDependency(fout, dobj, preDataBound->dumpId);
				addObjectDependency(fout, postDataBound, dobj->dumpId);
				break;
			case DO_INDEX:
			case DO_INDEX_ATTACH:
			case DO_STATSEXT:
			case DO_REFRESH_MATVIEW:
			case DO_TRIGGER:
			case DO_EVENT_TRIGGER:
			case DO_DEFAULT_ACL:
			case DO_POLICY:
			case DO_PUBLICATION:
			case DO_PUBLICATION_REL:
			case DO_SUBSCRIPTION:
				/* Post-data objects: must come after the post-data boundary */
				addObjectDependency(fout, dobj, postDataBound->dumpId);
				break;
			case DO_RULE:
				/* Rules are post-data, but only if dumped separately */
				if (((RuleInfo *) dobj)->separate)
					addObjectDependency(fout, dobj, postDataBound->dumpId);
				break;
			case DO_CONSTRAINT:
			case DO_FK_CONSTRAINT:
				/* Constraints are post-data, but only if dumped separately */
				if (((ConstraintInfo *) dobj)->separate)
					addObjectDependency(fout, dobj, postDataBound->dumpId);
				break;
			case DO_PRE_DATA_BOUNDARY:
				/* nothing to do */
				break;
			case DO_POST_DATA_BOUNDARY:
				/* must come after the pre-data boundary */
				addObjectDependency(fout, dobj, preDataBound->dumpId);
				break;
		}
	}
}


/*
 * buildArchiveDependencies - create dependency data for archive TOC entries
 *
 * The raw dependency data obtained by getDependencies() is not terribly
 * useful in an archive dump, because in many cases there are dependency
 * chains linking through objects that don't appear explicitly in the dump.
 * For example, a view will depend on its _RETURN rule while the _RETURN rule
 * will depend on other objects --- but the rule will not appear as a separate
 * object in the dump.  We need to adjust the view's dependencies to include
 * whatever the rule depends on that is included in the dump.
 *
 * Just to make things more complicated, there are also "special" dependencies
 * such as the dependency of a TABLE DATA item on its TABLE, which we must
 * not rearrange because pg_restore knows that TABLE DATA only depends on
 * its table.  In these cases we must leave the dependencies strictly as-is
 * even if they refer to not-to-be-dumped objects.
 *
 * To handle this, the convention is that "special" dependencies are created
 * during ArchiveEntry calls, and an archive TOC item that has any such
 * entries will not be touched here.  Otherwise, we recursively search the
 * DumpableObject data structures to build the correct dependencies for each
 * archive TOC item.
 */
static void
buildArchiveDependencies(Archive *fout)
{
	ArchiveHandle *AH = (ArchiveHandle *) fout;
	TocEntry   *te;

	/* Scan all TOC entries in the archive */
	for (te = AH->toc->next; te != AH->toc; te = te->next)
	{
		DumpableObject *dobj;
		DumpId	   *dependencies;
		int			nDeps;
		int			allocDeps;

		/* No need to process entries that will not be dumped */
		if (te->reqs == 0)
			continue;
		/* Ignore entries that already have "special" dependencies */
		if (te->nDeps > 0)
			continue;
		/* Otherwise, look up the item's original DumpableObject, if any */
		dobj = findObjectByDumpId(fout, te->dumpId);
		if (dobj == NULL)
			continue;
		/* No work if it has no dependencies */
		if (dobj->nDeps <= 0)
			continue;
		/* Set up work array */
		allocDeps = 64;
		dependencies = (DumpId *) dbms_md_malloc0(fout->memory_ctx, allocDeps * sizeof(DumpId));
		nDeps = 0;
		/* Recursively find all dumpable dependencies */
		findDumpableDependencies(AH, dobj,
								 &dependencies, &nDeps, &allocDeps);
		/* And save 'em ... */
		if (nDeps > 0)
		{
			dependencies = (DumpId *) dbms_md_realloc(dependencies,
												 nDeps * sizeof(DumpId));
			te->dependencies = dependencies;
			te->nDeps = nDeps;
		}
		else
			free(dependencies);
	}
}

/* Recursive search subroutine for buildArchiveDependencies */
static void
findDumpableDependencies(ArchiveHandle *AH, DumpableObject *dobj,
						 DumpId **dependencies, int *nDeps, int *allocDeps)
{
	int			i;
	Archive *fout = (Archive*)AH;

	/*
	 * Ignore section boundary objects: if we search through them, we'll
	 * report lots of bogus dependencies.
	 */
	if (dobj->objType == DO_PRE_DATA_BOUNDARY ||
		dobj->objType == DO_POST_DATA_BOUNDARY)
		return;

	for (i = 0; i < dobj->nDeps; i++)
	{
		DumpId		depid = dobj->dependencies[i];

		if (TocIDRequired(AH, depid) != 0)
		{
			/* Object will be dumped, so just reference it as a dependency */
			if (*nDeps >= *allocDeps)
			{
				*allocDeps *= 2;
				*dependencies = (DumpId *) dbms_md_realloc(*dependencies,
													  *allocDeps * sizeof(DumpId));
			}
			(*dependencies)[*nDeps] = depid;
			(*nDeps)++;
		}
		else
		{
			/*
			 * Object will not be dumped, so recursively consider its deps. We
			 * rely on the assumption that sortDumpableObjects already broke
			 * any dependency loops, else we might recurse infinitely.
			 */
			DumpableObject *otherdobj = findObjectByDumpId(fout, depid);

			if (otherdobj)
				findDumpableDependencies(AH, otherdobj,
										 dependencies, nDeps, allocDeps);
		}
	}
}

/*
 * selectSourceSchema - make the specified schema the active search path
 * in the source database.
 *
 * NB: pg_catalog is explicitly searched after the specified schema;
 * so user names are only qualified if they are cross-schema references,
 * and system names are only qualified if they conflict with a user name
 * in the current schema.
 *
 * Whenever the selected schema is not pg_catalog, be careful to qualify
 * references to system catalogs and types in our emitted commands!
 *
 * This function is called only from selectSourceSchemaOnAH and
 * selectSourceSchema.
 */
static void
selectSourceSchema(Archive *fout, const char *schemaName)
{
	StringInfoData query;

	/* This is checked by the callers already */
	Assert(schemaName != NULL && *schemaName != '\0');

	initStringInfo(&query);
	appendStringInfo(&query, "SET search_path = %s",
					  dbms_md_fmtId(schemaName));
	if (strcmp(schemaName, "pg_catalog") != 0)
		appendStringInfoString(&query, ", pg_catalog");

	dbms_md_exe_sql_no_result(fout, query.data);
	/*ExecuteSqlStatement(fout, query.data);*/

	pfree(query.data);
}

/*
 * getFormattedTypeName - retrieve a nicely-formatted type name for the
 * given type name.
 *
 * NB: the result may depend on the currently-selected search_path; this is
 * why we don't try to cache the names.
 */
static char *
getFormattedTypeName(Archive *fout, Oid oid, OidOptions opts)
{
	char	   *result = NULL;
	StringInfo query;
	SPITupleTable   *res;

	if (oid == 0)
	{
		if ((opts & zeroAsOpaque) != 0)
			return dbms_md_strdup(g_opaque_type);
		else if ((opts & zeroAsAny) != 0)
			return dbms_md_strdup("'any'");
		else if ((opts & zeroAsStar) != 0)
			return dbms_md_strdup("*");
		else if ((opts & zeroAsNone) != 0)
			return dbms_md_strdup("NONE");
	}

	query = createStringInfo();
	appendStringInfo(query, "SELECT pg_catalog.format_type('%u'::pg_catalog.oid, NULL)",
					  oid);

	res = ExecuteSqlQueryForSingleRow(fout, query->data);

	/* result of format_type is already quoted */
	result = dbms_md_strdup(dbms_md_get_field_strval(res, 0, SPI_RES_FIRST_FIELD_NO));

	dbms_md_free_tuples(res);
	destroyStringInfo(query);
	pfree(query);

	return result;
}

/*
 * Return a column list clause for the given relation.
 *
 * Special case: if there are no undropped columns in the relation, return
 * "", not an invalid "()" column list.
 */
#if 0 
static const char *
fmtCopyColumnList(const TableInfo *ti, StringInfo buffer)
{
	int			numatts = ti->numatts;
	char	  **attnames = ti->attnames;
	bool	   *attisdropped = ti->attisdropped;
	bool		needComma;
	int			i;

	appendStringInfoChar(buffer, '(');
	needComma = false;
	for (i = 0; i < numatts; i++)
	{
		if (attisdropped[i])
			continue;
		if (needComma)
			appendStringInfoString(buffer, ", ");
		appendStringInfoString(buffer, dbms_md_fmtId(attnames[i]));
		needComma = true;
	}

	if (!needComma)
		return "";				/* no undropped columns */

	appendStringInfoChar(buffer, ')');
	return buffer->data;
}
#endif
/*
 * Check if a reloptions array is nonempty.
 */
static bool
nonemptyReloptions(const char *reloptions)
{
	/* Don't want to print it if it's just "{}" */
	return (reloptions != NULL && strlen(reloptions) > 2);
}

/*
 * Format a reloptions array and append it to the given buffer.
 *
 * "prefix" is prepended to the option names; typically it's "" or "toast.".
 */

static void
appendReloptionsArrayAH(StringInfo buffer, const char *reloptions,
						const char *prefix, Archive *fout)
{
	bool		res;

	res = dbms_append_reloptions_array(buffer, reloptions, prefix, fout->encoding,
								fout->std_strings);
	if (!res)
		elog(WARNING, "WARNING: could not parse reloptions array");
}


#ifdef _SHARDING_
void
dbms_md_shard_list_append(Archive *fout, DbmsMdOidList *list, const char *val)
{
	DbmsMdOidListCell *cell;

	/* this calculation correctly accounts for the null trailing byte */
	cell = (DbmsMdOidListCell *)dbms_md_malloc0(fout->memory_ctx, sizeof(DbmsMdOidListCell));

	cell->next = NULL;
	cell->val = (Oid)dbms_md_str_to_oid(val);

	if(!ShardIDIsValid(cell->val))
	{
		elog(ERROR, _("shardid %d is invalid\n"), cell->val);
		return;
	}

	if (list->tail)
		list->tail->next = cell;
	else
		list->head = cell;
	list->tail = cell;
}
#endif

static void 
init_dump_gobal_var(DumpOptions *dopt)
{
	DBMS_MD_DUMP_GVAR_ST *dump_gvar = &(dopt->dump_gvar);
	init_md_common_gvar(&(dopt->comm_gvar));

	dump_gvar->schema_include_patterns.head = NULL;
	dump_gvar->schema_include_patterns.tail = NULL;
	
	dump_gvar->schema_include_oids.head = NULL;
	dump_gvar->schema_include_oids.tail = NULL;

	dump_gvar->table_include_patterns.head = NULL;
	dump_gvar->table_include_patterns.tail = NULL;

	dump_gvar->table_include_oids.head = NULL;
	dump_gvar->table_include_oids.tail =NULL;
	
#ifdef _SHARDING_
	dump_gvar->dump_shards.head = NULL;
	dump_gvar->dump_shards.tail =NULL;
	dump_gvar->shardstring = NULL;
	dump_gvar->with_dropped_column = false;
#endif
	
#ifdef PGXC
	dump_gvar->include_nodes = 0;
#endif
}

static void 
clean_dump_gobal_var(DumpOptions*dopt)
{
	DBMS_MD_DUMP_GVAR_ST *dump_gvar = &(dopt->dump_gvar);
	clean_md_common_gvar(&(dopt->comm_gvar));

	dbms_md_string_list_clear(&(dump_gvar->schema_include_patterns));
	dump_gvar->schema_include_patterns.head = NULL;
	dump_gvar->schema_include_patterns.tail = NULL;

	dbms_md_oid_list_clear(&(dump_gvar->schema_include_oids));
	dump_gvar->schema_include_oids.head = NULL;
	dump_gvar->schema_include_oids.tail = NULL;

	dbms_md_string_list_clear(&(dump_gvar->table_include_patterns));
	dump_gvar->table_include_patterns.head = NULL;
	dump_gvar->table_include_patterns.tail = NULL;

	dbms_md_oid_list_clear(&(dump_gvar->table_include_oids));
	dump_gvar->table_include_oids.head = NULL;
	dump_gvar->table_include_oids.tail =NULL;
	
#ifdef _SHARDING_
	dbms_md_oid_list_clear(&(dump_gvar->dump_shards));
	dump_gvar->dump_shards.head = NULL;
	dump_gvar->dump_shards.tail =NULL;
	dump_gvar->shardstring = NULL;
	dump_gvar->with_dropped_column = false;
#endif
	
#ifdef PGXC
	dump_gvar->include_nodes = 0;
#endif
}

static const char*
dbms_md_get_current_schema(Archive *fout)
{
	const char *cs = NULL;	
	SPITupleTable *res = ExecuteSqlQuery(fout,  "SELECT current_schema() as cschema");
	cs = dbms_md_strdup(dbms_md_get_field_strval(res, 0, SPI_RES_FIRST_FIELD_NO));
	dbms_md_free_tuples(res);
	return cs;
}
