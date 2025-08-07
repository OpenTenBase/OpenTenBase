/*-------------------------------------------------------------------------
 *
 * dbms_md_backup.h
 *
 *	Public interface to the pg_dump archiver routines.
 *
 *-------------------------------------------------------------------------
 */

#ifndef DBMS_MD_BACKUP_H
#define DBMS_MD_BACKUP_H

#include "dbms_md_itf.h"

typedef enum trivalue
{
	TRI_DEFAULT,
	TRI_NO,
	TRI_YES
} trivalue;

typedef enum _archiveFormat
{
	archUnknown = 0,
	archCustom = 1,
	archTar = 3,
	archString = 4,
	archDirectory = 5
} ArchiveFormat;

typedef enum _dbms_md_archiveMode
{
	archModeAppend,
	archModeWrite,
	archModeRead
} DBMS_MD_ArchiveMode;

typedef enum _teSection
{
	SECTION_NONE = 1,			/* COMMENTs, ACLs, etc; can be anywhere */
	SECTION_PRE_DATA,			/* stuff to be processed before data */
	SECTION_DATA,				/* TABLE DATA, BLOBS, BLOB COMMENTS */
	SECTION_POST_DATA			/* stuff to be processed after data */
} teSection;

#define oidcmp(x,y) ( ((x) < (y) ? -1 : ((x) > (y)) ?  1 : 0) )
#define oideq(x,y) ( (x) == (y) )
#define oidle(x,y) ( (x) <= (y) )
#define oidge(x,y) ( (x) >= (y) )
#define oidzero(x) ( (x) == 0 )

/*
 * The data structures used to store system catalog information.  Every
 * dumpable object is a subclass of DumpableObject.
 *
 * NOTE: the structures described here live for the entire pg_dump run;
 * and in most cases we make a struct for every object we can find in the
 * catalogs, not only those we are actually going to dump.  Hence, it's
 * best to store a minimal amount of per-object info in these structs,
 * and retrieve additional per-object info when and if we dump a specific
 * object.  In particular, try to avoid retrieving expensive-to-compute
 * information until it's known to be needed.  We do, however, have to
 * store enough info to determine whether an object should be dumped and
 * what order to dump in.
 */

typedef enum
{
	/* When modifying this enum, update priority tables in pg_dump_sort.c! */
	DO_NAMESPACE,
	DO_EXTENSION,
	DO_TYPE,
	DO_SHELL_TYPE,
	DO_FUNC,
	DO_AGG,
	DO_OPERATOR,
	DO_ACCESS_METHOD,
	DO_OPCLASS,
	DO_OPFAMILY,
	DO_COLLATION,
	DO_CONVERSION,
	DO_TABLE,
	DO_ATTRDEF,
	DO_INDEX,
	DO_INDEX_ATTACH,
	DO_STATSEXT,
	DO_RULE,
	DO_TRIGGER,
	DO_CONSTRAINT,
	DO_FK_CONSTRAINT,			/* see note for ConstraintInfo */
	DO_PROCLANG,
	DO_CAST,
	DO_TABLE_DATA,
	DO_SEQUENCE_SET,
	DO_DUMMY_TYPE,
	DO_TSPARSER,
	DO_TSDICT,
	DO_TSTEMPLATE,
	DO_TSCONFIG,
	DO_FDW,
	DO_FOREIGN_SERVER,
	DO_DEFAULT_ACL,
	DO_TRANSFORM,
	DO_BLOB,
	DO_BLOB_DATA,
	DO_PRE_DATA_BOUNDARY,
	DO_POST_DATA_BOUNDARY,
	DO_EVENT_TRIGGER,
	DO_REFRESH_MATVIEW,
	DO_POLICY,
	DO_PUBLICATION,
	DO_PUBLICATION_REL,
	DO_SUBSCRIPTION
} DumpableObjectType;

/* component types of an object which can be selected for dumping */
typedef uint32 DumpComponents;	/* a bitmask of dump object components */
#define DUMP_COMPONENT_NONE			(0)
#define DUMP_COMPONENT_DEFINITION	(1 << 0)
#define DUMP_COMPONENT_DATA			(1 << 1)
#define DUMP_COMPONENT_COMMENT		(1 << 2)
#define DUMP_COMPONENT_SECLABEL		(1 << 3)
#define DUMP_COMPONENT_ACL			(1 << 4)
#define DUMP_COMPONENT_POLICY		(1 << 5)
#define DUMP_COMPONENT_USERMAP		(1 << 6)
#define DUMP_COMPONENT_ALL			(0xFFFF)

/*
 * component types which require us to obtain a lock on the table
 *
 * Note that some components only require looking at the information
 * in the pg_catalog tables and, for those components, we do not need
 * to lock the table.  Be careful here though- some components use
 * server-side functions which pull the latest information from
 * SysCache and in those cases we *do* need to lock the table.
 *
 * We do not need locks for the COMMENT and SECLABEL components as
 * those simply query their associated tables without using any
 * server-side functions.  We do not need locks for the ACL component
 * as we pull that information from pg_class without using any
 * server-side functions that use SysCache.  The USERMAP component
 * is only relevant for FOREIGN SERVERs and not tables, so no sense
 * locking a table for that either (that can happen if we are going
 * to dump "ALL" components for a table).
 *
 * We DO need locks for DEFINITION, due to various server-side
 * functions that are used and POLICY due to pg_get_expr().  We set
 * this up to grab the lock except in the cases we know to be safe.
 */
#define DUMP_COMPONENTS_REQUIRING_LOCK (\
		DUMP_COMPONENT_DEFINITION |\
		DUMP_COMPONENT_DATA |\
		DUMP_COMPONENT_POLICY)

/*
 * pg_dump uses two different mechanisms for identifying database objects:
 *
 * CatalogId represents an object by the tableoid and oid of its defining
 * entry in the system catalogs.  We need this to interpret pg_depend entries,
 * for instance.
 *
 * DumpId is a simple sequential integer counter assigned as dumpable objects
 * are identified during a pg_dump run.  We use DumpId internally in preference
 * to CatalogId for two reasons: it's more compact, and we can assign DumpIds
 * to "objects" that don't have a separate CatalogId.  For example, it is
 * convenient to consider a table, its data, and its ACL as three separate
 * dumpable "objects" with distinct DumpIds --- this lets us reason about the
 * order in which to dump these things.
 */
typedef struct
{
	Oid			tableoid;
	Oid			oid;
} CatalogId;

typedef int DumpId;


typedef struct _dumpableObject
{
	DumpableObjectType objType;
	CatalogId	catId;			/* zero if not a cataloged object */
	DumpId		dumpId;			/* assigned by AssignDumpId() */
	char	   *name;			/* object name (should never be NULL) */
	struct _namespaceInfo *namespace;	/* containing namespace, or NULL */
	DumpComponents dump;		/* bitmask of components to dump */
	DumpComponents dump_contains;	/* as above, but for contained objects */
	bool		ext_member;		/* true if object is member of extension */
	DumpId	   *dependencies;	/* dumpIds of objects this one depends on */
	int			nDeps;			/* number of valid dependencies */
	int			allocDeps;		/* allocated size of dependencies[] */
} DumpableObject;

typedef struct _namespaceInfo
{
	DumpableObject dobj;
	char	   *rolname;		/* name of owner, or empty string */
	char	   *nspacl;
	char	   *rnspacl;
	char	   *initnspacl;
	char	   *initrnspacl;
} NamespaceInfo;

typedef struct _extensionInfo
{
	DumpableObject dobj;
	char	   *namespace;		/* schema containing extension's objects */
	bool		relocatable;
	char	   *extversion;
	char	   *extconfig;		/* info about configuration tables */
	char	   *extcondition;
} ExtensionInfo;

typedef struct _typeInfo
{
	DumpableObject dobj;

	/*
	 * Note: dobj.name is the pg_type.typname entry.  format_type() might
	 * produce something different than typname
	 */
	char	   *rolname;		/* name of owner, or empty string */
	char	   *typacl;
	char	   *rtypacl;
	char	   *inittypacl;
	char	   *initrtypacl;
	Oid			typelem;
	Oid			typrelid;
	char		typrelkind;		/* 'r', 'v', 'c', etc */
	char		typtype;		/* 'b', 'c', etc */
	bool		isArray;		/* true if auto-generated array type */
	bool		isDefined;		/* true if typisdefined */
	/* If needed, we'll create a "shell type" entry for it; link that here: */
	struct _shellTypeInfo *shellType;	/* shell-type entry, or NULL */
	/* If it's a domain, we store links to its constraints here: */
	int			nDomChecks;
	struct _constraintInfo *domChecks;
} TypeInfo;

typedef struct _shellTypeInfo
{
	DumpableObject dobj;

	TypeInfo   *baseType;		/* back link to associated base type */
} ShellTypeInfo;

typedef struct _funcInfo
{
	DumpableObject dobj;
	char	   *rolname;		/* name of owner, or empty string */
	Oid			lang;
	int			nargs;
	Oid		   *argtypes;
	Oid			prorettype;
	char	   *proacl;
	char	   *rproacl;
	char	   *initproacl;
	char	   *initrproacl;
} FuncInfo;

/* AggInfo is a superset of FuncInfo */
typedef struct _aggInfo
{
	FuncInfo	aggfn;
	/* we don't require any other fields at the moment */
} AggInfo;

typedef struct _oprInfo
{
	DumpableObject dobj;
	char	   *rolname;
	char		oprkind;
	Oid			oprcode;
} OprInfo;

typedef struct _accessMethodInfo
{
	DumpableObject dobj;
	char		amtype;
	char	   *amhandler;
} AccessMethodInfo;

typedef struct _opclassInfo
{
	DumpableObject dobj;
	char	   *rolname;
} OpclassInfo;

typedef struct _opfamilyInfo
{
	DumpableObject dobj;
	char	   *rolname;
} OpfamilyInfo;

typedef struct _collInfo
{
	DumpableObject dobj;
	char	   *rolname;
} CollInfo;

typedef struct _convInfo
{
	DumpableObject dobj;
	char	   *rolname;
} ConvInfo;

#ifdef __OPENTENBASE__
typedef struct _keyValueInfo
{
	char *schema;
	char *table;
	char *keyvalue;
	char *hotdisgroup;
	char *colddisgroup;
}KeyValueInfo;
#endif

typedef struct _tableInfo
{
	/*
	 * These fields are collected for every table in the database.
	 */
	DumpableObject dobj;
	char	   *rolname;		/* name of owner, or empty string */
	char	   *relacl;
	char	   *rrelacl;
	char	   *initrelacl;
	char	   *initrrelacl;
	char		relkind;
	char		relpersistence; /* relation persistence */
	bool		relispopulated; /* relation is populated */
	char		relreplident;	/* replica identifier */
	char	   *reltablespace;	/* relation tablespace */
	char	   *reloptions;		/* options specified by WITH (...) */
	char	   *checkoption;	/* WITH CHECK OPTION, if any */
	char	   *toast_reloptions;	/* WITH options for the TOAST table */
	bool		hasindex;		/* does it have any indexes? */
	bool		hasrules;		/* does it have any rules? */
	bool		hastriggers;	/* does it have any triggers? */
	bool		rowsec;			/* is row security enabled? */
	bool		forcerowsec;	/* is row security forced? */
	bool		hasoids;		/* does it have OIDs? */
	uint32		frozenxid;		/* table's relfrozenxid */
	uint32		minmxid;		/* table's relminmxid */
	Oid			toast_oid;		/* toast table's OID, or 0 if none */
	uint32		toast_frozenxid;	/* toast table's relfrozenxid, if any */
	uint32		toast_minmxid;	/* toast table's relminmxid */
	int			ncheck;			/* # of CHECK expressions */
	char	   *reloftype;		/* underlying type for typed table */
	/* these two are set only if table is a sequence owned by a column: */
	Oid			owning_tab;		/* OID of table owning sequence */
	int			owning_col;		/* attr # of column owning sequence */
	bool		is_identity_sequence;
	int			relpages;		/* table's size in pages (from pg_class) */

	bool		interesting;	/* true if need to collect more data */
	bool		dummy_view;		/* view's real definition must be postponed */
	bool		postponed_def;	/* matview must be postponed into post-data */
	bool		ispartition;	/* is table a partition? */

#ifdef PGXC
	/* PGXC table locator Data */
	char		pgxclocatortype;	/* Type of PGXC table locator */
	int			pgxcattnum;		/* Number of the attribute the table is partitioned with */
	int         pgxcsecattnum;  /* Number of the second attribute the table is partitioned with */
	char		*pgxc_node_names;	/* List of node names where this table is distributed */
#endif
#ifdef __OPENTENBASE__
	char		parttype;
	int 		partattnum;
	char		*partstartvalue;
	char		*partinterval;
	int 		nparts;
	char        *groupname;
	char        *coldgroupname;
	int32         nKeys;		/* Number of keyvalues */
	KeyValueInfo *keyvalue;		/* Key values*/
#endif

	/*
	 * These fields are computed only if we decide the table is interesting
	 * (it's either a table to dump, or a direct parent of a dumpable table).
	 */
	int			numatts;		/* number of attributes */
	char	  **attnames;		/* the attribute names */
	char	  **atttypnames;	/* attribute type names */
	int		   *atttypmod;		/* type-specific type modifiers */
	int		   *attstattarget;	/* attribute statistics targets */
	char	   *attstorage;		/* attribute storage scheme */
	char	   *typstorage;		/* type storage scheme */
	bool	   *attisdropped;	/* true if attr is dropped; don't dump it */
	char	   *attidentity;
	int		   *attlen;			/* attribute length, used by binary_upgrade */
	char	   *attalign;		/* attribute align, used by binary_upgrade */
	bool	   *attislocal;		/* true if attr has local definition */
	char	  **attoptions;		/* per-attribute options */
	Oid		   *attcollation;	/* per-attribute collation selection */
	char	  **attfdwoptions;	/* per-attribute fdw options */
	bool	   *notnull;		/* NOT NULL constraints on attributes */
	bool	   *inhNotNull;		/* true if NOT NULL is inherited */
	struct _attrDefInfo **attrdefs; /* DEFAULT expressions */
	struct _constraintInfo *checkexprs; /* CHECK constraints */
	char	   *partkeydef;		/* partition key definition */
	char	   *partbound;		/* partition bound definition */
	bool		needs_override; /* has GENERATED ALWAYS AS IDENTITY */

	/*
	 * Stuff computed only for dumpable tables.
	 */
	int			numParents;		/* number of (immediate) parent tables */
	struct _tableInfo **parents;	/* TableInfos of immediate parents */
	int			numIndexes;		/* number of indexes */
	struct _indxInfo *indexes;	/* indexes */
	struct _tableDataInfo *dataObj; /* TableDataInfo, if dumping its data */
	int			numTriggers;	/* number of triggers for table */
	struct _triggerInfo *triggers;	/* array of TriggerInfo structs */
} TableInfo;

typedef struct _attrDefInfo
{
	DumpableObject dobj;		/* note: dobj.name is name of table */
	TableInfo  *adtable;		/* link to table of attribute */
	int			adnum;
	char	   *adef_expr;		/* decompiled DEFAULT expression */
	bool		separate;		/* TRUE if must dump as separate item */
} AttrDefInfo;

typedef struct _tableDataInfo
{
	DumpableObject dobj;
	TableInfo  *tdtable;		/* link to table to dump */
	bool		oids;			/* include OIDs in data? */
	char	   *filtercond;		/* WHERE condition to limit rows dumped */
} TableDataInfo;

typedef struct _indxInfo
{
	DumpableObject dobj;
	TableInfo  *indextable;		/* link to table the index is for */
	char	   *indexdef;
	char	   *tablespace;		/* tablespace in which index is stored */
	char	   *indreloptions;	/* options specified by WITH (...) */
	int			indnkeys;
	Oid		   *indkeys;
	bool		indisclustered;
	bool		indisreplident;
	Oid			parentidx;		/* if partitioned, parent index OID */
	/* if there is an associated constraint object, its dumpId: */
	DumpId		indexconstraint;
	int			relpages;		/* relpages of the underlying table */
} IndxInfo;

typedef struct _indexAttachInfo
{
	DumpableObject dobj;
	IndxInfo   *parentIdx;		/* link to index on partitioned table */
	IndxInfo   *partitionIdx;	/* link to index on partition */
} IndexAttachInfo;

typedef struct _statsExtInfo
{
	DumpableObject dobj;
	TableInfo  *statsexttable;	/* link to table the stats ext is for */
	char	   *statsextdef;
} StatsExtInfo;

typedef struct _ruleInfo
{
	DumpableObject dobj;
	TableInfo  *ruletable;		/* link to table the rule is for */
	char		ev_type;
	bool		is_instead;
	char		ev_enabled;
	bool		separate;		/* TRUE if must dump as separate item */
	/* separate is always true for non-ON SELECT rules */
} RuleInfo;

typedef struct _triggerInfo
{
	DumpableObject dobj;
	TableInfo  *tgtable;		/* link to table the trigger is for */
	char	   *tgfname;
	int			tgtype;
	int			tgnargs;
	char	   *tgargs;
	bool		tgisconstraint;
	char	   *tgconstrname;
	Oid			tgconstrrelid;
	char	   *tgconstrrelname;
	char		tgenabled;
	bool		tgdeferrable;
	bool		tginitdeferred;
	char	   *tgdef;
} TriggerInfo;

typedef struct _evttriggerInfo
{
	DumpableObject dobj;
	char	   *evtname;
	char	   *evtevent;
	char	   *evtowner;
	char	   *evttags;
	char	   *evtfname;
	char		evtenabled;
} EventTriggerInfo;

/*
 * struct ConstraintInfo is used for all constraint types.  However we
 * use a different objType for foreign key constraints, to make it easier
 * to sort them the way we want.
 *
 * Note: condeferrable and condeferred are currently only valid for
 * unique/primary-key constraints.  Otherwise that info is in condef.
 */
typedef struct _constraintInfo
{
	DumpableObject dobj;
	TableInfo  *contable;		/* NULL if domain constraint */
	TypeInfo   *condomain;		/* NULL if table constraint */
	char		contype;
	char	   *condef;			/* definition, if CHECK or FOREIGN KEY */
	Oid			confrelid;		/* referenced table, if FOREIGN KEY */
	DumpId		conindex;		/* identifies associated index if any */
	bool		condeferrable;	/* TRUE if constraint is DEFERRABLE */
	bool		condeferred;	/* TRUE if constraint is INITIALLY DEFERRED */
	bool		conislocal;		/* TRUE if constraint has local definition */
	bool		separate;		/* TRUE if must dump as separate item */
} ConstraintInfo;

typedef struct _procLangInfo
{
	DumpableObject dobj;
	bool		lanpltrusted;
	Oid			lanplcallfoid;
	Oid			laninline;
	Oid			lanvalidator;
	char	   *lanacl;
	char	   *rlanacl;
	char	   *initlanacl;
	char	   *initrlanacl;
	char	   *lanowner;		/* name of owner, or empty string */
} ProcLangInfo;

typedef struct _castInfo
{
	DumpableObject dobj;
	Oid			castsource;
	Oid			casttarget;
	Oid			castfunc;
	char		castcontext;
	char		castmethod;
} CastInfo;

typedef struct _transformInfo
{
	DumpableObject dobj;
	Oid			trftype;
	Oid			trflang;
	Oid			trffromsql;
	Oid			trftosql;
} TransformInfo;

/* InhInfo isn't a DumpableObject, just temporary state */
typedef struct _inhInfo
{
	Oid			inhrelid;		/* OID of a child table */
	Oid			inhparent;		/* OID of its parent */
} InhInfo;

typedef struct _prsInfo
{
	DumpableObject dobj;
	Oid			prsstart;
	Oid			prstoken;
	Oid			prsend;
	Oid			prsheadline;
	Oid			prslextype;
} TSParserInfo;

typedef struct _dictInfo
{
	DumpableObject dobj;
	char	   *rolname;
	Oid			dicttemplate;
	char	   *dictinitoption;
} TSDictInfo;

typedef struct _tmplInfo
{
	DumpableObject dobj;
	Oid			tmplinit;
	Oid			tmpllexize;
} TSTemplateInfo;

typedef struct _cfgInfo
{
	DumpableObject dobj;
	char	   *rolname;
	Oid			cfgparser;
} TSConfigInfo;

typedef struct _fdwInfo
{
	DumpableObject dobj;
	char	   *rolname;
	char	   *fdwhandler;
	char	   *fdwvalidator;
	char	   *fdwoptions;
	char	   *fdwacl;
	char	   *rfdwacl;
	char	   *initfdwacl;
	char	   *initrfdwacl;
} FdwInfo;

typedef struct _foreignServerInfo
{
	DumpableObject dobj;
	char	   *rolname;
	Oid			srvfdw;
	char	   *srvtype;
	char	   *srvversion;
	char	   *srvacl;
	char	   *rsrvacl;
	char	   *initsrvacl;
	char	   *initrsrvacl;
	char	   *srvoptions;
} ForeignServerInfo;

typedef struct _defaultACLInfo
{
	DumpableObject dobj;
	char	   *defaclrole;
	char		defaclobjtype;
	char	   *defaclacl;
	char	   *rdefaclacl;
	char	   *initdefaclacl;
	char	   *initrdefaclacl;
} DefaultACLInfo;

typedef struct _blobInfo
{
	DumpableObject dobj;
	char	   *rolname;
	char	   *blobacl;
	char	   *rblobacl;
	char	   *initblobacl;
	char	   *initrblobacl;
} BlobInfo;

/*
 * The PolicyInfo struct is used to represent policies on a table and
 * to indicate if a table has RLS enabled (ENABLE ROW SECURITY).  If
 * polname is NULL, then the record indicates ENABLE ROW SECURITY, while if
 * it's non-NULL then this is a regular policy definition.
 */
typedef struct _policyInfo
{
	DumpableObject dobj;
	TableInfo  *poltable;
	char	   *polname;		/* null indicates RLS is enabled on rel */
	char		polcmd;
	bool		polpermissive;
	char	   *polroles;
	char	   *polqual;
	char	   *polwithcheck;
} PolicyInfo;

/*
 * The PublicationInfo struct is used to represent publications.
 */
typedef struct _PublicationInfo
{
	DumpableObject dobj;
	char	   *rolname;
	bool		puballtables;
	bool		pubinsert;
	bool		pubupdate;
	bool		pubdelete;
} PublicationInfo;

/*
 * The PublicationRelInfo struct is used to represent publication table
 * mapping.
 */
typedef struct _PublicationRelInfo
{
	DumpableObject dobj;
	TableInfo  *pubtable;
	char	   *pubname;
} PublicationRelInfo;

/*
 * The SubscriptionInfo struct is used to represent subscription.
 */
typedef struct _SubscriptionInfo
{
	DumpableObject dobj;
	char	   *rolname;
	char	   *subconninfo;
	char	   *subslotname;
	char	   *subsynccommit;
	char	   *subpublications;
} SubscriptionInfo;

/*
 * We build an array of these with an entry for each object that is an
 * extension member according to pg_depend.
 */
typedef struct _extensionMemberId
{
	CatalogId	catId;			/* tableoid+oid of some member object */
	ExtensionInfo *ext;			/* owning extension */
} ExtensionMemberId;

typedef struct 
{
	
	/*
	 * Variables for mapping DumpId to DumpableObject
	 */
	DumpableObject **dumpIdMap;
	int	allocedDumpIds;
	DumpId lastDumpId;
	
	/*
	 * Variables for mapping CatalogId to DumpableObject
	 */
	bool catalogIdMapValid;
	DumpableObject **catalogIdMap;
	int	numCatalogIds;
	
	/*
	 * These variables are static to avoid the notational cruft of having to pass
	 * them into findTableByOid() and friends.	For each of these arrays, we build
	 * a sorted-by-OID index array immediately after the objects are fetched,
	 * and then we use binary search in findTableByOid() and friends.  (qsort'ing
	 * the object arrays themselves would be simpler, but it doesn't work because
	 * pg_dump.c may have already established pointers between items.)
	 */
	DumpableObject **tblinfoindex;
	DumpableObject **typinfoindex;
	DumpableObject **funinfoindex;
	DumpableObject **oprinfoindex;
	DumpableObject **collinfoindex;
	DumpableObject **nspinfoindex;
	DumpableObject **extinfoindex;
	int	numTables;
	int	numTypes;
	int	numFuncs;
	int	numOperators;
	int	numCollations;
	int	numNamespaces;
	int	numExtensions;
	
	/* This is an array of object identities, not actual DumpableObjects */
	ExtensionMemberId *extmembers;
	int	numextmembers;

}DBMS_MD_DUMP_COMMON_ST;

typedef struct {

	/*
	 * Object inclusion/exclusion lists
	 *
	 * The string lists record the patterns given by command-line switches,
	 * which we then convert to lists of OIDs of matching objects.
	 */
	DbmsMdStringList schema_include_patterns;
	DbmsMdOidList schema_include_oids;
	//static DbmsMdStringList schema_exclude_patterns = {NULL, NULL};
	//static DbmsMdOidList schema_exclude_oids = {NULL, NULL};

	DbmsMdStringList table_include_patterns;
	DbmsMdOidList table_include_oids;
//static DbmsMdStringList table_exclude_patterns = {NULL, NULL};
//static DbmsMdOidList table_exclude_oids = {NULL, NULL};
//static DbmsMdStringList tabledata_exclude_patterns = {NULL, NULL};
//static DbmsMdOidList tabledata_exclude_oids = {NULL, NULL};
#ifdef _SHARDING_
	DbmsMdOidList  dump_shards;
	char * shardstring;
	bool with_dropped_column;
#endif




#ifdef PGXC
	int	include_nodes;
#endif
}DBMS_MD_DUMP_GVAR_ST;


typedef struct _restoreOptions
{
	uint64 		dump_obj_bm;
	int			createDB;		/* Issue commands to create the database */
	int			noOwner;		/* Don't try to match original object owner */
	int			noTablespace;	/* Don't issue tablespace-related commands */
	int			disable_triggers;	/* disable triggers during data-only
									 * restore */
	int			use_setsessauth;	/* Use SET SESSION AUTHORIZATION commands
									 * instead of OWNER TO */
	char	   		*superuser;		/* Username to use as superuser */
	char	   		*use_role;		/* Issue SET ROLE to this */
	int			dropSchema;
	int			disable_dollar_quoting;
	int			dump_inserts;
	int			column_inserts;
	int			if_exists;
	int			no_publications;	/* Skip publication entries */
	int			no_security_labels; /* Skip security label entries */
	int			no_subscriptions;	/* Skip subscription entries */
	int			strict_names;

	const char *filename;
	int			dataOnly;
	int			schemaOnly;
	int			dumpSections;
	int			verbose;
	int			aclsSkip;
	const char *lockWaitTimeout;
	int			include_everything;

	int			tocSummary;
	char	   *tocFile;
	int			format;
	char	   *formatName;

	int			selTypes;
	int			selIndex;
	int			selFunction;
	int			selTrigger;
	int			selTable;
	DbmsMdStringList indexNames;
	DbmsMdStringList functionNames;
	DbmsMdStringList schemaNames;
	DbmsMdStringList schemaExcludeNames;
	DbmsMdStringList triggerNames;
	DbmsMdStringList tableNames;

	int			useDB;
	char	   *dbname;			/* subject to expand_dbname */
	char	   *pgport;
	char	   *pghost;
	char	   *username;
	int			noDataForFailedTables;
	trivalue	promptPassword;
	int			exit_on_error;
	int			compression;
	int			suppressDumpWarnings;	/* Suppress output of WARNING entries
										 * to stderr */
	bool		single_txn;

	bool	   *idWanted;		/* array showing which dump IDs to emit */
	int			enable_row_security;
	int			sequence_data;	/* dump sequence data even in schema-only mode */
	//int			binary_upgrade;
} RestoreOptions;

typedef struct _dumpOptions
{
	uint64 dump_obj_bm;
	const char *dbname;			/* subject to expand_dbname */
	const char *pghost;
	const char *pgport;
	const char *username;
	bool		oids;

	int			binary_upgrade;

	/* various user-settable parameters */
	bool		schemaOnly;
	bool		dataOnly;
	int			dumpSections;	/* bitmask of chosen sections */
	bool		aclsSkip;
	const char *lockWaitTimeout;

	/* flags for various command-line long options */
	int			disable_dollar_quoting;
	int			dump_inserts;
	int			column_inserts;
	int			if_exists;
	int			no_security_labels;
	int			no_publications;
	int			no_subscriptions;
	int			no_synchronized_snapshots;
	int			no_unlogged_table_data;
	int			serializable_deferrable;
	int			quote_all_identifiers;
	int			disable_triggers;
	int			outputNoTablespaces;
	int			use_setsessauth;
	int			enable_row_security;

	/* default, if no "inclusion" switches appear, is to dump everything */
	bool		include_everything;

	int			outputClean;
	int			outputCreateDB;
	bool		outputBlobs;
	bool		dontOutputBlobs;
	int			outputNoOwner;
	char	   *outputSuperuser;

	int			sequence_data;	/* dump sequence data even in schema-only mode */

	DBMS_MD_DUMP_COMMON_ST comm_gvar;
	DBMS_MD_DUMP_GVAR_ST dump_gvar;
} DumpOptions;

/*
 *	We may want to have some more user-readable data, but in the mean
 *	time this gives us some abstraction and type checking.
 */
typedef struct Archive
{
	DumpOptions *dopt;			/* options, if dumping */
	RestoreOptions *ropt;		/* options, if restoring */

	MemoryContext	memory_ctx;
	int			verbose;
	char	   *remoteVersionStr;	/* server's version string */
	int			remoteVersion;	/* same in numeric form */
	bool		isStandby;		/* is server a standby node */
	bool		isPostgresXL;	/* is server a Postgres-XL node */

	int			minRemoteVersion;	/* allowable range */
	int			maxRemoteVersion;

	int			numWorkers;		/* number of parallel processes */
	char	   *sync_snapshot_id;	/* sync snapshot id for parallel operation */

	/* info needed for string escaping */
	int			encoding;		/* libpq code for client_encoding */
	bool		std_strings;	/* standard_conforming_strings */
	char	   *use_role;		/* Issue SET ROLE to this */

	/* error handling */
	bool		exit_on_error;	/* whether to exit on SQL errors... */
	int			n_errors;		/* number of errors (if no die) */

	StringInfo dump_out_buf;

	DumpId preDataBoundId;
	DumpId postDataBoundId;
	/* The rest is private */
} Archive;

typedef int (*DataDumperPtr) (Archive *AH, void *userArg);

typedef void (*SetupWorkerPtrType) (Archive *AH);

/*
 * Main archiver interface.
 */
extern void ConnectDatabase(Archive *AH,
				const char *dbname,
				const char *pghost,
				const char *pgport,
				const char *username,
				trivalue prompt_password);

/* Called to add a TOC entry */
extern void ArchiveEntry(Archive *AHX,
			 CatalogId catalogId, DumpId dumpId,
			 const char *tag,
			 const char *namespace, const char *tablespace,
			 const char *owner, bool withOids,
			 const char *desc, teSection section,
			 const char *defn,
			 const char *dropStmt, const char *copyStmt,
			 const DumpId *deps, int nDeps,
			 DataDumperPtr dumpFn, void *dumpArg);

/* Called to write *data* to the archive */
extern void WriteData(Archive *AH, const void *data, size_t dLen);

extern int	StartBlob(Archive *AH, Oid oid);
extern int	EndBlob(Archive *AH, Oid oid);

extern void CloseArchive(Archive *AH);

extern void SetArchiveOptions(Archive *AH, DumpOptions *dopt, RestoreOptions *ropt);

extern void ProcessArchiveRestoreOptions(Archive *AH);

extern void RestoreArchive(Archive *AH);

/* Open an existing archive */
extern Archive *OpenArchive(const char *FileSpec, const ArchiveFormat fmt);

/* Create a new archive */
extern Archive *
CreateArchive(const char *FileSpec, const ArchiveFormat fmt,
				  const int compression, DBMS_MD_ArchiveMode mode,
				  SetupWorkerPtrType setupDumpWorker, MemoryContext mem_ctx);

/* The --list option */
extern void PrintTOCSummary(Archive *AH);

extern RestoreOptions *NewRestoreOptions(void);

extern DumpOptions *NewDumpOptions(void);
extern void InitDumpOptions(DumpOptions *opts);
extern DumpOptions *dumpOptionsFromRestoreOptions(RestoreOptions *ropt);

/* Rearrange and filter TOC entries */
extern void SortTocFromFile(Archive *AHX);

/* Convenience functions used only when writing DATA */
extern void archputs(const char *s, Archive *AH);
extern int	archprintf(Archive *AH, const char *fmt,...) pg_attribute_printf(2, 3);

#endif							/* DBMS_MD_BACKUP_H*/
