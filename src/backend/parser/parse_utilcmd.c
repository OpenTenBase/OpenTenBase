/*-------------------------------------------------------------------------
 *
 * parse_utilcmd.c
 *	  Perform parse analysis work for various utility commands
 *
 * Formerly we did this work during parse_analyze() in analyze.c.  However
 * that is fairly unsafe in the presence of querytree caching, since any
 * database state that we depend on in making the transformations might be
 * obsolete by the time the utility command is executed; and utility commands
 * have no infrastructure for holding locks or rechecking plan validity.
 * Hence these functions are now called at the start of execution of their
 * respective utility commands.
 *
 * NOTE: in general we must avoid scribbling on the passed-in raw parse
 * tree, since it might be in a plan cache.  The simplest solution is
 * a quick copyObject() call before manipulating the query tree.
 *
 *
 * Portions Copyright (c) 2012-2014, TransLattice, Inc.
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 *
 *	src/backend/parser/parse_utilcmd.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/amapi.h"
#include "access/htup_details.h"
#include "access/reloptions.h"
#include "access/tupdesc.h"
#include "catalog/dependency.h"
#include "catalog/heap.h"
#include "catalog/index.h"
#include "catalog/namespace.h"
#include "catalog/partition.h"
#include "catalog/pg_am.h"
#include "catalog/pg_collation.h"
#include "catalog/pg_constraint.h"
#include "catalog/pg_constraint_fn.h"
#include "catalog/pg_opclass.h"
#include "catalog/pg_operator.h"
#include "catalog/pg_type.h"
#include "commands/comment.h"
#include "commands/defrem.h"
#include "commands/sequence.h"
#include "commands/tablecmds.h"
#include "commands/tablespace.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "optimizer/planner.h"
#include "parser/analyze.h"
#include "parser/parse_clause.h"
#include "parser/parse_coerce.h"
#include "parser/parse_collate.h"
#include "parser/parse_expr.h"
#include "parser/parse_relation.h"
#include "parser/parse_target.h"
#include "parser/parse_type.h"
#include "parser/parse_utilcmd.h"
#include "parser/parser.h"
#include "parser/scansup.h"
#include "rewrite/rewriteManip.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/orcl_datetime.h"
#include "utils/partcache.h"
#include "utils/rel.h"
#include "utils/ruleutils.h"
#include "utils/syscache.h"
#include "utils/typcache.h"
#ifdef PGXC
#include "pgxc/execRemote.h"
#include "pgxc/groupmgr.h"
#include "pgxc/locator.h"
#include "pgxc/pgxc.h"
#include "pgxc/planner.h"
#endif
#ifdef XCP
#include "catalog/pgxc_node.h"
#endif
#ifdef __OPENTENBASE__
#include "catalog/pgxc_class.h"
#include "utils/fmgroids.h"
#include "utils/inval.h"
#endif
#ifdef _PG_ORCL_
#include "catalog/pg_inherits_fn.h"
#include "utils/datum.h"
#include "utils/guc.h"
#endif
#ifdef __OPENTENBASE_C__
#include "utils/memutils.h"
#include "opentenbase_ora/opentenbase_ora.h"

#endif


#ifdef XCP
/*
 * Sources to make decision about distribution column, in order of preceedence
 */
typedef enum
{
	FBS_NONE,		/* no fallback columns */
	FBS_COLDEF, 	/* column definition, if no constraints defined */
	FBS_UIDX,		/* unique key definition, if no PK defined */
	FBS_PKEY,		/* primary key definition */
	FBS_REPLICATE	/* constraint definitions require to replicate table */
} FallbackSrc;
#endif

/* State shared by transformCreateStmt and its subroutines */
typedef struct
{
	ParseState *pstate;			/* overall parser state */
	const char *stmtType;		/* "CREATE [FOREIGN] TABLE" or "ALTER TABLE" */
	RangeVar   *relation;		/* relation to create */
	Relation	rel;			/* opened/locked rel, if ALTER */
	List	   *inhRelations;	/* relations to inherit from */
	bool		isforeign;		/* true if CREATE/ALTER FOREIGN TABLE */
	bool		isalter;		/* true if altering existing table */
	bool		hasoids;		/* does relation have an OID column? */
	List	   *columns;		/* ColumnDef items */
	List	   *ckconstraints;	/* CHECK constraints */
	List	   *fkconstraints;	/* FOREIGN KEY constraints */
	List	   *ixconstraints;	/* index-creating constraints */
	List	   *clusterconstraints;  /* PARTIAL CLUSTER KEY constraints */
	List	   *inh_indexes;	/* cloned indexes from INCLUDING INDEXES */
	List	   *blist;			/* "before list" of things to do before
								 * creating the table */
	List	   *alist;			/* "after list" of things to do after creating
								 * the table */
	IndexStmt  *pkey;			/* PRIMARY KEY index, if any */
#ifdef PGXC
	FallbackSrc fallback_source;
	List	   *fallback_dist_cols;
	DistributeBy	*distributeby;		/* original distribute by column of CREATE TABLE */
	PGXCSubCluster	*subcluster;		/* original subcluster option of CREATE TABLE */
#endif
	bool		ispartitioned;	/* true if table is partitioned */
	PartitionBoundSpec *partbound;	/* transformed FOR VALUES */
	bool            ofType;                 /* true if statement contains OF typename */
	List		*attr_encodings; /* List of ColumnReferenceStorageDirectives */
	MemoryContext tempCtx;
	int           longtype_colnum;
} CreateStmtContext;

/* State shared by transformCreateSchemaStmt and its subroutines */
typedef struct
{
	const char *stmtType;		/* "CREATE SCHEMA" or "ALTER SCHEMA" */
	char	   *schemaname;		/* name of schema */
	RoleSpec   *authrole;		/* owner of schema */
	List	   *sequences;		/* CREATE SEQUENCE items */
	List	   *tables;			/* CREATE TABLE items */
	List	   *views;			/* CREATE VIEW items */
	List	   *indexes;		/* CREATE INDEX items */
	List	   *triggers;		/* CREATE TRIGGER items */
	List	   *grants;			/* GRANT items */
} CreateSchemaStmtContext;

#ifdef XCP
bool loose_constraints = false;
#endif

static bool transformColumnDefinition(CreateStmtContext *cxt,
						  ColumnDef *column);
static void transformTableConstraint(CreateStmtContext *cxt,
						 Constraint *constraint);
static void transformTableLikeClause(CreateStmtContext *cxt,
						 TableLikeClause *table_like_clause, bool forceBareCol);
static void transformOfType(CreateStmtContext *cxt,
				TypeName *ofTypename);
static List *get_collation(Oid collation, Oid actual_datatype);
static List *get_opclass(Oid opclass, Oid actual_datatype);
static void transformIndexConstraints(CreateStmtContext *cxt);
static IndexStmt *transformIndexConstraint(Constraint *constraint,
						 CreateStmtContext *cxt);
static void transformFKConstraints(CreateStmtContext *cxt,
					   bool skipValidation,
					   bool isAddConstraint);
static void transformCheckConstraints(CreateStmtContext *cxt,
						  bool skipValidation);
static void transformPartialClusterConstraints(CreateStmtContext *cxt);
static void transformConstraintAttrs(CreateStmtContext *cxt,
						 List *constraintList);
static void transformColumnType(CreateStmtContext *cxt, ColumnDef *column);
static void setSchemaName(char *context_schema, char **stmt_schema_name);
static void transformPartitionCmd(CreateStmtContext *cxt, PartitionCmd *cmd);
static void validateInfiniteBounds(ParseState *pstate, List *blist);
static Const *transformPartitionBoundValue(ParseState *pstate, Node *node,
							 const char *colName, Oid colType, int32 colTypmod);

#ifdef PGXC
static void checkLocalFKConstraints(CreateStmtContext *cxt);
#endif
#ifdef XCP
static List *transformSubclusterNodes(PGXCSubCluster *subcluster);
static PGXCSubCluster *makeSubCluster(List *nodelist);
#endif
#ifdef __OPENTENBASE__
static char * ChooseSerialName(const char *relname, const char *colname,
                               const char *label, Oid namespaceid);
#endif

static List *addCreatePartitionStmts(List *stmts, CreateStmt *p_stmt);

static bool
HasNonNullConstraint(ColumnDef *column)
{
	if (column->constraints)
	{
		ListCell *lc;
		foreach(lc, column->constraints)
		{
			Constraint *con = (Constraint *) lfirst(lc);
			if (con->contype != CONSTR_NULL && con->contype != CONSTR_NOTNULL &&
				con->contype != CONSTR_CHECK)
			{
				return true;
			}	
		}
	}
	return false;
}

static void
CheckLongTypeSetDefault(Oid relid, AlterTableCmd *cmd)
{
	if (!ORA_MODE)
		return;

	if (cmd->subtype == AT_ColumnDefault)
	{
		char *name = cmd->name;
		Oid atttypid = InvalidOid;
		AttrNumber attnum = InvalidAttrNumber;

		if (name)
		{
			attnum = get_attnum(relid, name);

			if (attnum != InvalidAttrNumber)
			{
				atttypid = get_atttype(relid, attnum);

				if (LONGOID == atttypid || LONGARROID == atttypid)
					elog(ERROR, "\"long\" type can not have default constraint");
			}
		}
	}
}

/*
 * transformCreateStmt -
 *	  parse analysis for CREATE TABLE
 *
 * Returns a List of utility commands to be done in sequence.  One of these
 * will be the transformed CreateStmt, but there may be additional actions
 * to be done before and after the actual DefineRelation() call.
 *
 * SQL allows constraints to be scattered all over, so thumb through
 * the columns and collect all constraints into one place.
 * If there are any implied indices (e.g. UNIQUE or PRIMARY KEY)
 * then expand those into multiple IndexStmt blocks.
 *	  - thomas 1997-12-02
 */
#ifdef XCP
List *
transformCreateStmt(CreateStmt *stmt, const char *queryString,
					bool is_local, bool sentToRemote)
#else
List *
transformCreateStmt(CreateStmt *stmt, const char *queryString)
#endif
{
	ParseState *pstate;
	CreateStmtContext cxt = {0};
	List	   *result;
	List	   *save_alist;
	ListCell   *elements;
	Oid			namespaceid;
	Oid			existing_relid;
	ParseCallbackState pcbstate;
	bool		is_foreign_table = IsA(stmt, CreateForeignTableStmt);
	bool        autodistribute;
	bool        is_temp;
	bool		bfile_exist = false;

#ifdef __OPENTENBASE__
	stmt->like_found = false;

	/*local temp table need autodistribute*/
	is_temp = stmt->relation->relpersistence == RELPERSISTENCE_TEMP;
	autodistribute = ((!is_local) && (!sentToRemote)) || (is_local && is_temp);
#endif

#ifdef _PG_ORCL_
	/* always generate distribution information for foreign table */
	if (is_foreign_table)
		autodistribute = true;
	/*
	 * For merge/split partition, we use PARTITION OF to create the newly added
	 * partition, and make clear that should generate distribution information
	 * in this transformation.
	 */
	if (stmt->inhRelations && stmt->distributeby == NULL && stmt->istransformed)
		autodistribute = true;
#endif

	/*
	 * We must not scribble on the passed-in CreateStmt, so copy it.  (This is
	 * overkill, but easy.)
	 */
	stmt = copyObject(stmt);

	/* Set up pstate */
	pstate = make_parsestate(NULL);
	pstate->p_sourcetext = queryString;

	/*
	 * Look up the creation namespace.  This also checks permissions on the
	 * target namespace, locks it against concurrent drops, checks for a
	 * preexisting relation in that namespace with the same name, and updates
	 * stmt->relation->relpersistence if the selected namespace is temporary.
	 */
	setup_parser_errposition_callback(&pcbstate, pstate,
									  stmt->relation->location);
	namespaceid =
		RangeVarGetAndCheckCreationNamespace(stmt->relation, NoLock,
											 &existing_relid);
	cancel_parser_errposition_callback(&pcbstate);

	/*
	 * If the relation already exists and the user specified "IF NOT EXISTS",
	 * bail out with a NOTICE.
	 */
	if (stmt->if_not_exists && OidIsValid(existing_relid))
	{
		/*
		 * If we are in an extension script, insist that the pre-existing
		 * object be a member of the extension, to avoid security risks.
		 */
		ObjectAddress address;

		ObjectAddressSet(address, RelationRelationId, existing_relid);
		checkMembershipInCurrentExtension(&address);

		/* OK to skip */
		ereport(NOTICE,
				(errcode(ERRCODE_DUPLICATE_TABLE),
				 errmsg("relation \"%s\" already exists, skipping",
						stmt->relation->relname)));
		return NIL;
	}

	/*
	 * If the target relation name isn't schema-qualified, make it so.  This
	 * prevents some corner cases in which added-on rewritten commands might
	 * think they should apply to other relations that have the same name and
	 * are earlier in the search path.  But a local temp table is effectively
	 * specified to be in pg_temp, so no need for anything extra in that case.
	 */
	if (stmt->relation->schemaname == NULL
		&& stmt->relation->relpersistence != RELPERSISTENCE_TEMP)
		stmt->relation->schemaname = get_namespace_name(namespaceid);

	/* Set up CreateStmtContext */
	cxt.pstate = pstate;
	if (IsA(stmt, CreateForeignTableStmt))
	{
		CreateForeignTableStmt *fstmt = (CreateForeignTableStmt *) stmt;
		ListCell    *optcell;
		char		*datasourceVal = NULL;
		
		cxt.stmtType = "CREATE FOREIGN TABLE";
		cxt.isforeign = true;

		foreach(optcell, fstmt->options)
		{
			DefElem *def = (DefElem *)lfirst(optcell);

			if (pg_strcasecmp(def->defname, "datasource") == 0)
			{
				datasourceVal = defGetString(def);
				break;
			}
		}

		if (datasourceVal && pg_strcasecmp(datasourceVal, "hudi") == 0)
			cxt.pstate->isHudiForeign = true;
		else
			cxt.pstate->isHudiForeign = false;
	}
	else
	{
		cxt.stmtType = "CREATE TABLE";
		cxt.isforeign = false;
		cxt.pstate->isHudiForeign = false;
	}
	cxt.relation = stmt->relation;
	cxt.rel = NULL;
	cxt.inhRelations = stmt->inhRelations;
	cxt.isalter = false;
	cxt.columns = NIL;
	cxt.ckconstraints = NIL;
	cxt.fkconstraints = NIL;
	cxt.ixconstraints = NIL;
	cxt.clusterconstraints = NIL;
	cxt.inh_indexes = NIL;
	cxt.blist = NIL;
	cxt.alist = NIL;
	cxt.pkey = NULL;
#ifdef PGXC
	cxt.fallback_source = FBS_NONE;
	cxt.fallback_dist_cols = NIL;
	cxt.distributeby = stmt->distributeby;
	cxt.subcluster = stmt->subcluster;
#endif
	cxt.ispartitioned = stmt->partspec != NULL;
	cxt.partbound = stmt->partbound;
	cxt.ofType = (stmt->ofTypename != NULL);
	/*
	 * Notice that we allow OIDs here only for plain tables, even though
	 * foreign tables also support them.  This is necessary because the
	 * default_with_oids GUC must apply only to plain tables and not any other
	 * relkind; doing otherwise would break existing pg_dump files.  We could
	 * allow explicit "WITH OIDS" while not allowing default_with_oids to
	 * affect other relkinds, but it would complicate interpretOidsOption(),
	 * and right now there's no WITH OIDS option in CREATE FOREIGN TABLE
	 * anyway.
	 */
	cxt.hasoids = interpretOidsOption(stmt->options, !cxt.isforeign);


	Assert(!stmt->ofTypename || !stmt->inhRelations);	/* grammar enforces */

	if (stmt->ofTypename)
		transformOfType(&cxt, stmt->ofTypename);

	if (stmt->partspec)
	{
		if (stmt->inhRelations && !stmt->partbound)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
					 errmsg("cannot create partitioned table as inheritance child")));
	}

	/*
	 * Run through each primary element in the table creation clause. Separate
	 * column defs from constraints, and do preliminary analysis.
	 */
	foreach(elements, stmt->tableElts)
	{
		Node	   *element = lfirst(elements);

		switch (nodeTag(element))
		{
			case T_ColumnDef:
				{
					if (ORA_MODE)
					{
						ColumnDef	*column = (ColumnDef *) element;
						Oid			typeOid = InvalidOid;

						if (column->typeName)
						{
							typeOid = LookupTypeNameOid(pstate, column->typeName, true);

							if (BFILEOID == typeOid)
								bfile_exist = true;
						}
					}
					(void) transformColumnDefinition(&cxt, (ColumnDef *) element);
				}
				break;

			case T_Constraint:
				transformTableConstraint(&cxt, (Constraint *) element);
				break;

			case T_TableLikeClause:
				stmt->like_found = true;
				transformTableLikeClause(&cxt, (TableLikeClause *) element, false);

				/* Check if the original table columns contain the bfile type */
				if (!bfile_exist && cxt.columns && list_length(cxt.columns) > 0)
				{
					ListCell *lc;
					foreach (lc, cxt.columns)
					{
						ColumnDef *def = lfirst(lc);
						if (def->typeName)
						{
							Oid typeOid = LookupTypeNameOid(pstate, def->typeName, true);

							if (typeOid == BFILEOID)
							{
								bfile_exist = true;
								break;
							}
						}
					}
				}

				break;

			default:
				elog(ERROR, "unrecognized node type: %d",
					 (int) nodeTag(element));
				break;
		}
	}

	/*
	 * If we had any LIKE tables, they may require creation of an OID column
	 * even though the command's own WITH clause didn't ask for one (or,
	 * perhaps, even specifically rejected having one).  Insert a WITH option
	 * to ensure that happens.  We prepend to the list because the first oid
	 * option will be honored, and we want to override anything already there.
	 * (But note that DefineRelation will override this again to add an OID
	 * column if one appears in an inheritance parent table.)
	 */
	if (stmt->like_found && cxt.hasoids)
		stmt->options = lcons(makeDefElem("oids",
										  (Node *) makeInteger(true), -1),
							  stmt->options);

	/*
	 * If the table is inherited then use the distribution strategy of the
	 * parent. We must have already checked for multiple parents and raised an
	 * ERROR since Postgres-XL does not support inheriting from multiple
	 * parents.
	 */
#ifdef __OPENTENBASE__
	/* add distribute info on both datanode and coordinator */
	if (stmt->inhRelations && IsPostmasterEnvironment && autodistribute)
#else
	if (stmt->inhRelations && IS_PGXC_COORDINATOR && autodistribute)
#endif
	{
		RangeVar   *inh = (RangeVar *) linitial(stmt->inhRelations);
		Relation	rel;

		Assert(IsA(inh, RangeVar));
		rel = heap_openrv(inh, AccessShareLock);
#ifdef __FDW__		
		if ((rel->rd_rel->relkind != RELKIND_RELATION) &&
			(rel->rd_rel->relkind != RELKIND_PARTITIONED_TABLE) &&
			(rel->rd_rel->relkind != RELKIND_FOREIGN_TABLE))
			ereport(ERROR,
					(errcode(ERRCODE_WRONG_OBJECT_TYPE),
					 errmsg("inherited relation \"%s\" is not a table or foreign table",
							inh->relname)));
#else
        if ((rel->rd_rel->relkind != RELKIND_RELATION) &&
            (rel->rd_rel->relkind != RELKIND_PARTITIONED_TABLE))
            ereport(ERROR,
                    (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                     errmsg("inherited relation \"%s\" is not a table",
                            inh->relname)));
#endif
		if (stmt->distributeby)
		{
			if (!rel->rd_locator_info)
				ereport(ERROR,
					(errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
					 errmsg("parent table \"%s\" is not distributed, but "
						 "distribution is specified for the child table \"%s\"",
						 RelationGetRelationName(rel),
						 stmt->relation->relname)));
			ereport(WARNING,
				(errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
				 errmsg("Inherited/partition tables inherit"
					 " distribution from the parent"),
				 errdetail("Explicitly specified distribution will be ignored")));
		}
		stmt->distributeby = makeNode(DistributeBy);

		/* Table includes bfile only exists in coordinator */
		if (ORA_MODE && bfile_exist && stmt->distributeby)
		{
			pfree(stmt->distributeby);
			stmt->distributeby = NULL;
		}

		if (rel->rd_locator_info && stmt->distributeby)
		{
			char *colname = NULL;
			switch (rel->rd_locator_info->locatorType)
			{
				case LOCATOR_TYPE_HASH:
					colname = rel->rd_locator_info->nDisAttrs > 0 ?
							get_attname(rel->rd_locator_info->relid, rel->rd_locator_info->disAttrNums[0]) : NULL;
					stmt->distributeby->disttype = DISTTYPE_HASH;
#ifdef __OPENTENBASE_C__
					stmt->distributeby->colname =
							list_make1(makeString(pstrdup(colname)));
#else
					stmt->distributeby->colname =
							pstrdup(rel->rd_locator_info->partAttrName);
#endif
					break;
#ifdef _SHARDING_
				case LOCATOR_TYPE_SHARD:
					stmt->distributeby->disttype = DISTTYPE_SHARD;
					if (rel->rd_locator_info->nDisAttrs)
					{
						int i = 0;
						for (i = 0; i < rel->rd_locator_info->nDisAttrs; i++)
						{
							colname = get_attname(rel->rd_locator_info->relid, rel->rd_locator_info->disAttrNums[i]);
							if (!colname)
							{
								elog(ERROR, "could not get attribute(%d) name for rel %s", rel->rd_locator_info->disAttrNums[i],
									        get_rel_name(rel->rd_locator_info->relid));
							}
							stmt->distributeby->colname = lappend(stmt->distributeby->colname, makeString(colname));
						}
					}
					break;
#endif
				case LOCATOR_TYPE_MODULO:
					colname = rel->rd_locator_info->nDisAttrs > 0 ?
							get_attname(rel->rd_locator_info->relid, rel->rd_locator_info->disAttrNums[0]) : NULL;
					stmt->distributeby->disttype = DISTTYPE_MODULO;
#ifdef __OPENTENBASE_C__
					stmt->distributeby->colname =
							list_make1(makeString(pstrdup(colname)));
#else
					stmt->distributeby->colname =
							pstrdup(rel->rd_locator_info->partAttrName);
#endif
					break;
				case LOCATOR_TYPE_RROBIN:
					stmt->distributeby->disttype = DISTTYPE_ROUNDROBIN;
					break;
				case LOCATOR_TYPE_REPLICATED:
					stmt->distributeby->disttype = DISTTYPE_REPLICATION;
					break;
				default:
				{
					if (is_foreign_table)
						stmt->distributeby->disttype = DISTTYPE_ROUNDROBIN;
					else
					stmt->distributeby->disttype = DISTTYPE_REPLICATION;
					break;
			}

			}
#ifdef _SHARDING_
			if (LOCATOR_TYPE_SHARD == rel->rd_locator_info->locatorType ||
				(LOCATOR_TYPE_REPLICATED == rel->rd_locator_info->locatorType && OidIsValid(rel->rd_locator_info->groupId)))
			{
				stmt->subcluster = makeShardSubCluster(rel->rd_locator_info->groupId);
			}
			else
			{
#endif
			stmt->subcluster = makeSubCluster(rel->rd_locator_info->rl_nodeList);
#ifdef _SHARDING_
			}
#endif
		}
		else if (rel->rd_locator_info == NULL)
		{
			pfree(stmt->distributeby);
			cxt.distributeby = NULL;
			stmt->distributeby = NULL;
			autodistribute = false;
		}

		heap_close(rel, NoLock);
	}

	/*
	 * transformIndexConstraints wants cxt.alist to contain only index
	 * statements, so transfer anything we already have into save_alist.
	 */
	save_alist = cxt.alist;
	cxt.alist = NIL;

	Assert(stmt->constraints == NIL);

	/*
	 * Postprocess constraints that give rise to index definitions.
	 */
	transformIndexConstraints(&cxt);


	/*
	 * Postprocess foreign-key constraints.
	 */
	transformFKConstraints(&cxt, true, false);

	/*
	 * Postprocess check constraints.
	 */
	transformCheckConstraints(&cxt, !is_foreign_table ? true : false);

	/*
	 * Postprocess partial cluster constraints.
	 */
	transformPartialClusterConstraints(&cxt);

	/*
	 * Output results.
	 */
	stmt->tableElts = cxt.columns;
	stmt->constraints = cxt.ckconstraints;

	result = lappend(cxt.blist, stmt);
	result = list_concat(result, cxt.alist);
	result = list_concat(result, save_alist);

#ifdef PGXC
	/*
	 * If the user did not specify any distribution clause and there is no
	 * inherits clause, try and use PK or unique index
	 */
#ifdef __OPENTENBASE__
	/* add distribute info on both datanode and coordinator */
	if (IsPostmasterEnvironment && autodistribute && !stmt->distributeby)
#else
	if (IS_PGXC_COORDINATOR && autodistribute && !stmt->distributeby)
#endif
	{
		/* always apply suggested subcluster */
		stmt->subcluster = copyObject(cxt.subcluster);
		if (cxt.distributeby)
		{
			/* Table includes bfile only exists in coordinator */
			if (ORA_MODE && bfile_exist)
			{
				stmt->distributeby = NULL;
				return addCreatePartitionStmts(result, stmt);
			}
			stmt->distributeby = copyObject(cxt.distributeby);
			return addCreatePartitionStmts(result, stmt);
		}
		/*
		 * If constraints require replicated table set it replicated
		 */
		if (ORA_MODE && bfile_exist)
			stmt->distributeby = NULL;
		else
			stmt->distributeby = makeNode(DistributeBy);

		if (stmt->distributeby != NULL)
		{
			if (cxt.fallback_source == FBS_REPLICATE)
			{
				stmt->distributeby->disttype = DISTTYPE_REPLICATION;
				stmt->distributeby->colname = NULL;
			}
	#ifdef __OPENTENBASE__
			/*
			* If there are columns suitable for shard/[hash for regress only] distribution distribute on
			* first of them. Use shard first.
			*/
			else if (cxt.fallback_dist_cols)
			{
				stmt->distributeby->disttype = get_default_distype();
	#ifdef __OPENTENBASE_C__
				stmt->distributeby->colname = list_make1(makeString((char *) linitial(cxt.fallback_dist_cols)));
	#else
				stmt->distributeby->colname = (char *) linitial(cxt.fallback_dist_cols);
	#endif
			}
	#endif
			/*
				* If none of above applies distribute by replication
			*/
			else
			{
				if (is_foreign_table)
					stmt->distributeby->disttype = DISTTYPE_ROUNDROBIN;
				else
					stmt->distributeby->disttype = DISTTYPE_REPLICATION;
				stmt->distributeby->colname = NULL;
			}
		}
	}
#endif

	/* Add create partition table statements in the end */
	return addCreatePartitionStmts(result, stmt);
}

#ifdef __OPENTENBASE__
/*
 *  * Check relation exists before choose sequence name, if
 *   * the relation already exists, no need to create sequence
 *    * and relation.
 *     */
static char *
ChooseSerialName(const char *relname, const char *colname,
					const char *curlabel, Oid namespaceid)
{
	int		pass = 0;
	char	modlabel[NAMEDATALEN];
	char	*sqname;
	const char *label = curlabel;
	Oid		seqoid;

	if (ORA_MODE)
		label = upcase_identifier(curlabel, strlen(curlabel));

	/* try the unmodified label first */
	StrNCpy(modlabel, label, sizeof(modlabel));

	for (;;)
	{
		sqname = makeObjectName(relname, colname, modlabel);

		AcceptInvalidationMessages();
		seqoid = get_relname_relid(sqname, namespaceid);
		if (OidIsValid(seqoid))
		{
			Relation rel = heap_open(seqoid, AccessShareLock);
			if (OidIsValid(get_relname_relid(relname, namespaceid)))
			{
				heap_close(rel, AccessShareLock);
				elog(ERROR, "relation \"%s\" already exists", relname);
			}
			heap_close(rel, AccessShareLock);

			/* found a conflict, so try a new name component */
			pfree(sqname);
			snprintf(modlabel, sizeof(modlabel), "%s%d", label, ++pass);
		}
		else
			break;
	}

	return sqname;
}
#endif

/*
 * generateSerialExtraStmts
 *		Generate CREATE SEQUENCE and ALTER SEQUENCE ... OWNED BY statements
 *		to create the sequence for a serial or identity column.
 *
 * This includes determining the name the sequence will have.  The caller
 * can ask to get back the name components by passing non-null pointers
 * for snamespace_p and sname_p.
 */
static void
generateSerialExtraStmts(CreateStmtContext *cxt, ColumnDef *column,
						 Oid seqtypid, List *seqoptions, bool for_identity,
						 char **snamespace_p, char **sname_p)
{
	ListCell   *option;
	DefElem    *nameEl = NULL;
	Oid			snamespaceid;
	char	   *snamespace;
	char	   *sname;
	CreateSeqStmt *seqstmt;
	AlterSeqStmt *altseqstmt;
	List	   *attnamelist;

	if (cxt->relation->relpersistence == RELPERSISTENCE_GLOBAL_TEMP)
	{
		ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("Global temp table does not yet support serial column")));
	}

	/*
	 * Determine namespace and name to use for the sequence.
	 *
	 * First, check if a sequence name was passed in as an option.  This is
	 * used by pg_dump.  Else, generate a name.
	 *
	 * Although we use ChooseRelationName, it's not guaranteed that the
	 * selected sequence name won't conflict; given sufficiently long field
	 * names, two different serial columns in the same table could be assigned
	 * the same sequence name, and we'd not notice since we aren't creating
	 * the sequence quite yet.  In practice this seems quite unlikely to be a
	 * problem, especially since few people would need two serial columns in
	 * one table.
	 */
	foreach(option, seqoptions)
	{
		DefElem    *defel = lfirst_node(DefElem, option);

		if (strcmp(defel->defname, "sequence_name") == 0)
		{
			if (nameEl)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options")));
			nameEl = defel;
		}
	}

	if (nameEl)
	{
		RangeVar   *rv = makeRangeVarFromNameList(castNode(List, nameEl->arg));

		snamespace = rv->schemaname;
		if (!snamespace)
		{
			/* Given unqualified SEQUENCE NAME, select namespace */
			if (cxt->rel)
				snamespaceid = RelationGetNamespace(cxt->rel);
			else
				snamespaceid = RangeVarGetCreationNamespace(cxt->relation);
			snamespace = get_namespace_name(snamespaceid);
		}
		sname = rv->relname;
		/* Remove the SEQUENCE NAME item from seqoptions */
		seqoptions = list_delete_ptr(seqoptions, nameEl);
	}
	else
	{
		if (cxt->rel)
			snamespaceid = RelationGetNamespace(cxt->rel);
		else
		{
			snamespaceid = RangeVarGetCreationNamespace(cxt->relation);
			RangeVarAdjustRelationPersistence(cxt->relation, snamespaceid);
		}
		snamespace = get_namespace_name(snamespaceid);
#ifdef __OPENTENBASE__
		if (strcmp("CREATE TABLE", cxt->stmtType) == 0)
			sname = ChooseSerialName(cxt->relation->relname,
									column->colname,
									"seq",
									snamespaceid);
		else
#endif
		sname = ChooseRelationName(cxt->relation->relname,
								   column->colname,
								   "seq",
								   snamespaceid);
	}

	ereport(DEBUG1,
			(errmsg("%s will create implicit sequence \"%s\" for serial column \"%s.%s\"",
					cxt->stmtType, sname,
					cxt->relation->relname, column->colname)));

	/*
	 * Build a CREATE SEQUENCE command to create the sequence object, and add
	 * it to the list of things to be done before this CREATE/ALTER TABLE.
	 */
	seqstmt = makeNode(CreateSeqStmt);
	seqstmt->for_identity = for_identity;
	seqstmt->sequence = makeRangeVar(snamespace, sname, -1);
	seqstmt->options = seqoptions;

	/*
	 * If a sequence data type was specified, add it to the options.  Prepend
	 * to the list rather than append; in case a user supplied their own AS
	 * clause, the "redundant options" error will point to their occurrence,
	 * not our synthetic one.
	 */
	if (seqtypid)
		seqstmt->options = lcons(makeDefElem("as",
											 (Node *) makeTypeNameFromOid(seqtypid, -1),
											 -1),
								 seqstmt->options);

	/*
	 * If this is ALTER ADD COLUMN, make sure the sequence will be owned by
	 * the table's owner.  The current user might be someone else (perhaps a
	 * superuser, or someone who's only a member of the owning role), but the
	 * SEQUENCE OWNED BY mechanisms will bleat unless table and sequence have
	 * exactly the same owning role.
	 */
	if (cxt->rel)
		seqstmt->ownerId = cxt->rel->rd_rel->relowner;
	else
		seqstmt->ownerId = InvalidOid;

	cxt->blist = lappend(cxt->blist, seqstmt);

	/*
	 * Build an ALTER SEQUENCE ... OWNED BY command to mark the sequence as
	 * owned by this column, and add it to the list of things to be done after
	 * this CREATE/ALTER TABLE.
	 */
	altseqstmt = makeNode(AlterSeqStmt);
	altseqstmt->sequence = makeRangeVar(snamespace, sname, -1);
	attnamelist = list_make3(makeString(snamespace),
							 makeString(cxt->relation->relname),
							 makeString(column->colname));
	altseqstmt->options = list_make1(makeDefElem("owned_by",
												 (Node *) attnamelist, -1));
	altseqstmt->for_identity = for_identity;

	cxt->alist = lappend(cxt->alist, altseqstmt);

	if (snamespace_p)
		*snamespace_p = snamespace;
	if (sname_p)
		*sname_p = sname;
}

/*
 * transformColumnDefinition -
 *		transform a single ColumnDef within CREATE TABLE
 *		Also used in ALTER TABLE ADD COLUMN
 */
static bool
transformColumnDefinition(CreateStmtContext *cxt, ColumnDef *column)
{
	bool		is_serial;
	bool		saw_nullable;
	bool		saw_default;
	bool		saw_identity;
	ListCell   *clist;

	cxt->columns = lappend(cxt->columns, column);
	/* Check for SERIAL pseudo-types */
	is_serial = false;
	if (column->typeName
		&& list_length(column->typeName->names) == 1
		&& !column->typeName->pct_type)
	{
		char *typname = strVal(linitial(column->typeName->names));
		int (*strcmpfunc)(const char *, const char *) = GETSTRCMPFUNC;

		if (strcmpfunc(typname, "smallserial") == 0 ||
			strcmpfunc(typname, "serial2") == 0)
		{
			is_serial = true;
			column->typeName->names = NIL;
			column->typeName->typeOid = INT2OID;
		}
		else if (strcmpfunc(typname, "serial") == 0 ||
				 strcmpfunc(typname, "serial4") == 0)
		{
			is_serial = true;
			column->typeName->names = NIL;
			column->typeName->typeOid = INT4OID;
		}
		else if (strcmpfunc(typname, "bigserial") == 0 ||
				 strcmpfunc(typname, "serial8") == 0)
		{
			is_serial = true;
			column->typeName->names = NIL;
			column->typeName->typeOid = INT8OID;
		}
		/*
		 * We have to reject "serial[]" explicitly, because once we've set
		 * typeid, LookupTypeName won't notice arrayBounds.  We don't need any
		 * special coding for serial(typmod) though.
		 */
		if (is_serial && column->typeName->arrayBounds != NIL)
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("array of serial is not implemented"),
					 parser_errposition(cxt->pstate,
										column->typeName->location)));
	}

	if (ORA_MODE && column->typeName)
	{
		Oid typeoid = LookupTypeNameOid(cxt->pstate, column->typeName, true);
		if (typeoid == LRAWOID || typeoid == LONGOID)
		{
			cxt->longtype_colnum++;
			if (cxt->longtype_colnum > 1)
				elog(ERROR, "Only one \"long\" type is allowed in a table");
		}
		else if (typeoid == RAWOID && column->typeName->typemod < 0 &&
					column->typeName->typmods == NIL)
		{
			/* opentenbase_ora: Please add typmods while creating a new table structure of type RAW. */
			elog(ERROR, "missing left parenthesis.");
		}
	}

	/* Do necessary work on the column type declaration */
	if (column->typeName)
		transformColumnType(cxt, column);

	/* Special actions for SERIAL pseudo-types */
	if (is_serial)
	{
		char	   *snamespace;
		char	   *sname;
		char	   *qstring;
		A_Const	   *snamenode;
		TypeCast   *castnode;
		FuncCall   *funccallnode;
		Constraint *constraint;

		/* XXX XL 9.6 was setting stmt->is_serial. CHECK */
		generateSerialExtraStmts(cxt, column,
								 column->typeName->typeOid, NIL, false,
								 &snamespace, &sname);

		/*
		 * Create appropriate constraints for SERIAL.  We do this in full,
		 * rather than shortcutting, so that we will detect any conflicting
		 * constraints the user wrote (like a different DEFAULT).
		 *
		 * Create an expression tree representing the function call
		 * nextval('sequencename').  We cannot reduce the raw tree to cooked
		 * form until after the sequence is created, but there's no need to do
		 * so.
		 */
		qstring = quote_qualified_identifier(snamespace, sname);
		snamenode = makeNode(A_Const);
		snamenode->val.type = T_String;
		snamenode->val.val.str = qstring;
		snamenode->location = -1;
		castnode = makeNode(TypeCast);
		castnode->typeName = SystemTypeName("regclass");
		castnode->arg = (Node *) snamenode;
		castnode->location = -1;
		funccallnode = makeFuncCall(SystemFuncName("nextval"),
									list_make1(castnode),
									-1);
		constraint = makeNode(Constraint);
		constraint->contype = CONSTR_DEFAULT;
		constraint->location = -1;
		constraint->raw_expr = (Node *) funccallnode;
		constraint->cooked_expr = NULL;
		column->constraints = lappend(column->constraints, constraint);

		constraint = makeNode(Constraint);
		constraint->contype = CONSTR_NOTNULL;
		constraint->location = -1;
		column->constraints = lappend(column->constraints, constraint);
	}

	/* Process column constraints, if any... */
	transformConstraintAttrs(cxt, column->constraints);

	saw_nullable = false;
	saw_default = false;
	saw_identity = false;

	foreach(clist, column->constraints)
	{
		Constraint *constraint = lfirst_node(Constraint, clist);

		switch (constraint->contype)
		{
			case CONSTR_NULL:
				if (saw_nullable && column->is_not_null)
					ereport(ERROR,
							(errcode(ERRCODE_SYNTAX_ERROR),
							 errmsg("conflicting NULL/NOT NULL declarations for column \"%s\" of table \"%s\"",
									column->colname, cxt->relation->relname),
							 parser_errposition(cxt->pstate,
												constraint->location)));
				column->is_not_null = FALSE;
				saw_nullable = true;
				break;

			case CONSTR_NOTNULL:
				if (saw_nullable && !column->is_not_null)
					ereport(ERROR,
							(errcode(ERRCODE_SYNTAX_ERROR),
							 errmsg("conflicting NULL/NOT NULL declarations for column \"%s\" of table \"%s\"",
									column->colname, cxt->relation->relname),
							 parser_errposition(cxt->pstate,
												constraint->location)));
				column->is_not_null = TRUE;

				saw_nullable = true;
				break;

			case CONSTR_DEFAULT:
				if (saw_default)
					ereport(ERROR,
							(errcode(ERRCODE_SYNTAX_ERROR),
							 errmsg("multiple default values specified for column \"%s\" of table \"%s\"",
									column->colname, cxt->relation->relname),
							 parser_errposition(cxt->pstate,
												constraint->location)));
				column->raw_default = constraint->raw_expr;
				Assert(constraint->cooked_expr == NULL);
				if (column->raw_default && column->raw_default->type == T_FuncCall)
				{
					FuncCall *fc = (FuncCall *) column->raw_default;

					if (list_length(fc->funcname) == 1 &&
					    downcase_ora_ident_strcmp(strVal(linitial(fc->funcname)), "nextval") == 0 &&
					    list_length(fc->args) == 1 && fc->agg_order == NIL &&
					    fc->agg_filter == NULL && !fc->agg_star && !fc->agg_distinct &&
					    !fc->func_variadic && fc->over == NULL)
					{
						/* this default is actually a "nextval" function call */
						is_serial = true;
					}
				}
				saw_default = true;
				break;

			case CONSTR_IDENTITY:
				{
					Type		ctype;
					Oid			typeOid;

					if (cxt->ofType)
						ereport(ERROR,
									(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
									 errmsg("identity colums are not supported on typed tables")));
					if (cxt->partbound)
						ereport(ERROR,
								(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
								 errmsg("identify columns are not supported on partitions")));

					ctype = typenameType(cxt->pstate, column->typeName, NULL);
					typeOid = HeapTupleGetOid(ctype);
					ReleaseSysCache(ctype);

					if (saw_identity)
						ereport(ERROR,
								(errcode(ERRCODE_SYNTAX_ERROR),
								 errmsg("multiple identity specifications for column \"%s\" of table \"%s\"",
										column->colname, cxt->relation->relname),
								 parser_errposition(cxt->pstate,
													constraint->location)));

					generateSerialExtraStmts(cxt, column,
											 typeOid, constraint->options, true,
											 NULL, NULL);

					column->identity = constraint->generated_when;
					saw_identity = true;
					column->is_not_null = TRUE;
					break;
				}

			case CONSTR_CHECK:
				cxt->ckconstraints = lappend(cxt->ckconstraints, constraint);
				break;

			case CONSTR_PRIMARY:
				if (cxt->isforeign)
					ereport(ERROR,
							(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							 errmsg("primary key constraints are not supported on foreign tables"),
							 parser_errposition(cxt->pstate,
												constraint->location)));
				/* FALL THRU */

			case CONSTR_UNIQUE:
				if (cxt->isforeign)
					ereport(ERROR,
							(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							 errmsg("unique constraints are not supported on foreign tables"),
							 parser_errposition(cxt->pstate,
												constraint->location)));
				if (constraint->keys == NIL)
					constraint->keys = list_make1(makeString(column->colname));
				cxt->ixconstraints = lappend(cxt->ixconstraints, constraint);
				break;

			case CONSTR_EXCLUSION:
				/* grammar does not allow EXCLUDE as a column constraint */
				elog(ERROR, "column exclusion constraints are not supported");
				break;

			case CONSTR_FOREIGN:
				if (cxt->isforeign)
					ereport(ERROR,
							(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							 errmsg("foreign key constraints are not supported on foreign tables"),
							 parser_errposition(cxt->pstate,
												constraint->location)));
				if (cxt->ispartitioned)
					ereport(ERROR,
							(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							 errmsg("foreign key constraints are not supported on partitioned tables"),
							 parser_errposition(cxt->pstate,
												constraint->location)));

				/*
				 * Fill in the current attribute's name and throw it into the
				 * list of FK constraints to be processed later.
				 */
				constraint->fk_attrs = list_make1(makeString(column->colname));
				cxt->fkconstraints = lappend(cxt->fkconstraints, constraint);
				break;

			case CONSTR_ATTR_DEFERRABLE:
			case CONSTR_ATTR_NOT_DEFERRABLE:
			case CONSTR_ATTR_DEFERRED:
			case CONSTR_ATTR_IMMEDIATE:
				/* transformConstraintAttrs took care of these */
				break;

			default:
				elog(ERROR, "unrecognized constraint type: %d",
					 constraint->contype);
				break;
		}

		if (saw_default && saw_identity)
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					 errmsg("both default and identity specified for column \"%s\" of table \"%s\"",
							column->colname, cxt->relation->relname),
					 parser_errposition(cxt->pstate,
										constraint->location)));
	}

	/*
	 * If needed, generate ALTER FOREIGN TABLE ALTER COLUMN statement to add
	 * per-column foreign data wrapper options to this column after creation.
	 */
	if (column->fdwoptions != NIL)
	{
		AlterTableStmt *stmt;
		AlterTableCmd *cmd;

		cmd = makeNode(AlterTableCmd);
		cmd->subtype = AT_AlterColumnGenericOptions;
		cmd->name = column->colname;
		cmd->def = (Node *) column->fdwoptions;
		cmd->behavior = DROP_RESTRICT;
		cmd->missing_ok = false;

		stmt = makeNode(AlterTableStmt);
		stmt->relation = cxt->relation;
		stmt->cmds = NIL;
		stmt->relkind = OBJECT_FOREIGN_TABLE;
		stmt->cmds = lappend(stmt->cmds, cmd);

		cxt->alist = lappend(cxt->alist, stmt);
	}

	return is_serial;
}

/*
 * transformTableConstraint
 *		transform a Constraint node within CREATE TABLE or ALTER TABLE
 */
static void
transformTableConstraint(CreateStmtContext *cxt, Constraint *constraint)
{
	switch (constraint->contype)
	{
		case CONSTR_PRIMARY:
			if (cxt->isforeign)
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("primary key constraints are not supported on foreign tables"),
						 parser_errposition(cxt->pstate,
											constraint->location)));
			cxt->ixconstraints = lappend(cxt->ixconstraints, constraint);
			break;

		case CONSTR_UNIQUE:
			if (cxt->isforeign)
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("unique constraints are not supported on foreign tables"),
						 parser_errposition(cxt->pstate,
											constraint->location)));
			cxt->ixconstraints = lappend(cxt->ixconstraints, constraint);
			break;

		case CONSTR_EXCLUSION:
			if (cxt->isforeign)
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("exclusion constraints are not supported on foreign tables"),
						 parser_errposition(cxt->pstate,
											constraint->location)));
			if (cxt->ispartitioned)
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("exclusion constraints are not supported on partitioned tables"),
						 parser_errposition(cxt->pstate,
											constraint->location)));
			cxt->ixconstraints = lappend(cxt->ixconstraints, constraint);
			break;

		case CONSTR_CHECK:
			cxt->ckconstraints = lappend(cxt->ckconstraints, constraint);
			break;

		case CONSTR_FOREIGN:
			if (cxt->isforeign)
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("foreign key constraints are not supported on foreign tables"),
						 parser_errposition(cxt->pstate,
											constraint->location)));
			if (cxt->ispartitioned)
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("foreign key constraints are not supported on partitioned tables"),
						 parser_errposition(cxt->pstate,
											constraint->location)));
			cxt->fkconstraints = lappend(cxt->fkconstraints, constraint);
			break;

		case CONSTR_NULL:
		case CONSTR_NOTNULL:
		case CONSTR_DEFAULT:
		case CONSTR_ATTR_DEFERRABLE:
		case CONSTR_ATTR_NOT_DEFERRABLE:
		case CONSTR_ATTR_DEFERRED:
		case CONSTR_ATTR_IMMEDIATE:
			elog(ERROR, "invalid context for constraint type %d",
				 constraint->contype);
			break;

		default:
			elog(ERROR, "unrecognized constraint type: %d",
				 constraint->contype);
			break;
	}
}

/*
 * transformTableLikeClause
 *
 * Change the LIKE <srctable> portion of a CREATE TABLE statement into
 * column definitions which recreate the user defined column portions of
 * <srctable>.
 *
 * If forceBareCol is true we disallow inheriting any indexes/constr/defaults.
 */
static void
transformTableLikeClause(CreateStmtContext *cxt, TableLikeClause *table_like_clause, bool forceBareCol)
{
	AttrNumber	parent_attno;
	Relation	relation;
	TupleDesc	tupleDesc;
	TupleConstr *constr;
	AttrNumber *attmap;
	AclResult	aclresult;
	char	   *comment;
	ParseCallbackState pcbstate;
	bool		activatinggtt = false;

	setup_parser_errposition_callback(&pcbstate, cxt->pstate,
									  table_like_clause->relation->location);


	/* we could support LIKE in many cases, but worry about it another day */
	if (cxt->isforeign)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("LIKE is not supported for creating foreign tables")));

	relation = relation_openrv(table_like_clause->relation, AccessShareLock);

	if (relation->rd_rel->relkind != RELKIND_RELATION &&
		relation->rd_rel->relkind != RELKIND_VIEW &&
		relation->rd_rel->relkind != RELKIND_MATVIEW &&
		relation->rd_rel->relkind != RELKIND_COMPOSITE_TYPE &&
		relation->rd_rel->relkind != RELKIND_FOREIGN_TABLE &&
		relation->rd_rel->relkind != RELKIND_PARTITIONED_TABLE)
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("\"%s\" is not a table, view, materialized view, composite type, or foreign table",
						RelationGetRelationName(relation))));

	cancel_parser_errposition_callback(&pcbstate);

#ifdef PGXC
	/*
	 * Block the creation of tables using views in their LIKE clause.
	 * Views are not created on Datanodes, so this will result in an error
	 * PGXCTODO: In order to fix this problem, it will be necessary to
	 * transform the query string of CREATE TABLE into something not using
	 * the view definition. Now Postgres-XC only uses the raw string...
	 * There is some work done with event triggers in 9.3, so it might
	 * be possible to use that code to generate the SQL query to be sent to
	 * remote nodes. When this is done, this error will be removed.
	 */
	if (relation->rd_rel->relkind == RELKIND_VIEW)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("Postgres-XL does not support VIEW in LIKE clauses"),
				 errdetail("The feature is not currently supported")));
#endif

	/*
	 * Check for privileges
	 */
	if (relation->rd_rel->relkind == RELKIND_COMPOSITE_TYPE)
	{
		aclresult = pg_type_aclcheck(relation->rd_rel->reltype, GetUserId(),
									 ACL_USAGE);
		if (aclresult != ACLCHECK_OK)
			aclcheck_error(aclresult, ACL_KIND_TYPE,
						   RelationGetRelationName(relation));
	}
	else
	{
		aclresult = pg_class_aclcheck(RelationGetRelid(relation), GetUserId(),
									  ACL_SELECT);
		if (aclresult != ACLCHECK_OK)
			aclcheck_error(aclresult, ACL_KIND_CLASS,
						   RelationGetRelationName(relation));
	}

	tupleDesc = RelationGetDescr(relation);
	constr = tupleDesc->constr;

	if (forceBareCol && table_like_clause->options != 0)
		ereport(ERROR,
		        (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				        errmsg("LIKE INCLUDING may not be used with this kind of relation")));
	
	/*
	 * Initialize column number map for map_variable_attnos().  We need this
	 * since dropped columns in the source table aren't copied, so the new
	 * table can have different column numbers.
	 */
	attmap = (AttrNumber *) palloc0(sizeof(AttrNumber) * tupleDesc->natts);

	/*
	 * Insert the copied attributes into the cxt for the new table definition.
	 */
	for (parent_attno = 1; parent_attno <= tupleDesc->natts;
		 parent_attno++)
	{
		Form_pg_attribute attribute = TupleDescAttr(tupleDesc,
													parent_attno - 1);
		char	   *attributeName = NameStr(attribute->attname);
		ColumnDef  *def;

		/*
		 * Ignore dropped columns in the parent.  attmap entry is left zero.
		 */
		if (attribute->attisdropped && !activatinggtt)
			continue;

		/*
		 * Create a new column, which is marked as NOT inherited.
		 *
		 * For constraints, ONLY the NOT NULL constraint is inherited by the
		 * new column definition per SQL99.
		 */
		def = makeNode(ColumnDef);
		def->colname = pstrdup(attributeName);
		def->typeName = makeTypeNameFromOid(attribute->atttypid,
											attribute->atttypmod);
		def->inhcount = 0;
		def->is_local = true;
		def->is_not_null = (forceBareCol ? false : attribute->attnotnull);
		def->is_from_type = false;
		def->storage = 0;
		def->raw_default = NULL;
		def->cooked_default = NULL;
		def->collClause = NULL;
		def->collOid = attribute->attcollation;
		def->constraints = NIL;
		def->location = -1;
		def->ptr = NULL;
		def->is_dropped = attribute->attisdropped;

		/* 
		 * keep the columns of an active global table as same as the meta 
		 * global table 
		 */
		if (attribute->attisdropped && activatinggtt)
		{
			def->ptr = (Form_pg_attribute) palloc0(ATTRIBUTE_FIXED_PART_SIZE);
			memcpy(def->ptr, attribute, ATTRIBUTE_FIXED_PART_SIZE);
		}

		/*
		 * Add to column list
		 */
		cxt->columns = lappend(cxt->columns, def);

#ifdef XCP
		/*
		 * If the distribution is not defined yet by a priority source add it
		 * to the list of possible fallbacks
		 */
#ifdef __OPENTENBASE__
		/* we need distribution info on both coordinator and datanode */
		if (IsPostmasterEnvironment && cxt->distributeby == NULL && !cxt->isalter &&
				cxt->fallback_source <= FBS_COLDEF &&
				IsTypeHashDistributable(attribute->atttypid))
#else
		if (IS_PGXC_COORDINATOR && cxt->distributeby == NULL && !cxt->isalter &&
				cxt->fallback_source <= FBS_COLDEF &&
				IsTypeHashDistributable(attribute->atttypid))
#endif
		{
			cxt->fallback_dist_cols = lappend(cxt->fallback_dist_cols,
											  pstrdup(attributeName));
			cxt->fallback_source = FBS_COLDEF;
		}

		if (IsPostmasterEnvironment && cxt->distributeby == NULL && relation->rd_locator_info && 
			(table_like_clause->options == CREATE_TABLE_LIKE_ALL))
		{
			char *colname = NULL;

			cxt->distributeby = makeNode(DistributeBy);
			switch (relation->rd_locator_info->locatorType)
			{
				case LOCATOR_TYPE_HASH:
					colname = relation->rd_locator_info->nDisAttrs > 0 ?
							get_attname(relation->rd_locator_info->relid, relation->rd_locator_info->disAttrNums[0]) : NULL;
					cxt->distributeby->disttype = DISTTYPE_HASH;
#ifdef __OPENTENBASE_C__
					cxt->distributeby->colname =
							list_make1(makeString(pstrdup(colname)));
#else
					cxt->distributeby->colname =
							pstrdup(relation->rd_locator_info->partAttrName);
#endif
					break;
				case LOCATOR_TYPE_SHARD:
					cxt->distributeby->disttype = DISTTYPE_SHARD;
					if (relation->rd_locator_info->nDisAttrs)
					{
						int i = 0;
						for (i = 0; i < relation->rd_locator_info->nDisAttrs; i++)
						{
							colname = get_attname(relation->rd_locator_info->relid, relation->rd_locator_info->disAttrNums[i]);
							if (!colname)
							{
								elog(ERROR, "could not get attribute(%d) name for relation %s", relation->rd_locator_info->disAttrNums[i],
											get_rel_name(relation->rd_locator_info->relid));
							}
							cxt->distributeby->colname = lappend(cxt->distributeby->colname, makeString(colname));
						}
					}
					break;
				case LOCATOR_TYPE_RROBIN:
					cxt->distributeby->disttype = DISTTYPE_ROUNDROBIN;
					break;
				case LOCATOR_TYPE_REPLICATED:
				default:
					cxt->distributeby->disttype = DISTTYPE_REPLICATION;
					break;
			}
		}
#endif
		attmap[parent_attno - 1] = list_length(cxt->columns);

		/*
		 * Copy default, if present and the default has been requested
		 */
		if (attribute->atthasdef &&
			(table_like_clause->options & CREATE_TABLE_LIKE_DEFAULTS))
		{
			Node	   *this_default = NULL;
			AttrDefault *attrdef;
			int			i;

			/* Find default in constraint structure */
			Assert(constr != NULL);
			attrdef = constr->defval;
			for (i = 0; i < constr->num_defval; i++)
			{
				if (attrdef[i].adnum == parent_attno)
				{
					this_default = stringToNode(attrdef[i].adbin);
					break;
				}
			}
			Assert(this_default != NULL);

			/*
			 * If default expr could contain any vars, we'd need to fix 'em,
			 * but it can't; so default is ready to apply to child.
			 */

			def->cooked_default = this_default;
		}

		/*
		 * Copy identity if requested
		 */
		if (attribute->attidentity &&
			(table_like_clause->options & CREATE_TABLE_LIKE_IDENTITY))
		{
			Oid			seq_relid;
			List	   *seq_options;

			/*
			 * find sequence owned by old column; extract sequence parameters;
			 * build new create sequence command
			 */
			seq_relid = getOwnedSequence(RelationGetRelid(relation), attribute->attnum);
			seq_options = sequence_options(seq_relid);
			generateSerialExtraStmts(cxt, def,
									 InvalidOid, seq_options, true,
									 NULL, NULL);
			def->identity = attribute->attidentity;
		}

		/* Likewise, copy storage if requested */
		if (table_like_clause->options & CREATE_TABLE_LIKE_STORAGE)
		{
			def->storage = attribute->attstorage;
		}
			
		else
			def->storage = 0;

		/* TRS:2c21fed902fe7549611c0ad8bff4f85e9291d9c5 */

		/* Likewise, copy comment if requested */
		if ((table_like_clause->options & CREATE_TABLE_LIKE_COMMENTS) &&
			(comment = GetComment(attribute->attrelid,
								  RelationRelationId,
								  attribute->attnum)) != NULL)
		{
			CommentStmt *stmt = makeNode(CommentStmt);

			stmt->objtype = OBJECT_COLUMN;
			stmt->object = (Node *) list_make3(makeString(cxt->relation->schemaname),
											   makeString(cxt->relation->relname),
											   makeString(def->colname));
			stmt->comment = comment;

			cxt->alist = lappend(cxt->alist, stmt);
		}
	}

	/* We use oids if at least one LIKE'ed table has oids. */
	cxt->hasoids |= relation->rd_rel->relhasoids;


	/*
	 * Copy CHECK constraints if requested, being careful to adjust attribute
	 * numbers so they match the child.
	 */
	if ((table_like_clause->options & CREATE_TABLE_LIKE_CONSTRAINTS) &&
		tupleDesc->constr)
	{
		int			ccnum;

		for (ccnum = 0; ccnum < tupleDesc->constr->num_check; ccnum++)
		{
			char	   *ccname = tupleDesc->constr->check[ccnum].ccname;
			char	   *ccbin = tupleDesc->constr->check[ccnum].ccbin;
			Constraint *n = makeNode(Constraint);
			Node	   *ccbin_node;
			bool		found_whole_row;

			ccbin_node = map_variable_attnos(stringToNode(ccbin),
											 1, 0,
											 attmap, tupleDesc->natts,
											 InvalidOid, &found_whole_row);

			/*
			 * We reject whole-row variables because the whole point of LIKE
			 * is that the new table's rowtype might later diverge from the
			 * parent's.  So, while translation might be possible right now,
			 * it wouldn't be possible to guarantee it would work in future.
			 */
			if (found_whole_row)
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("cannot convert whole-row table reference"),
						 errdetail("Constraint \"%s\" contains a whole-row reference to table \"%s\".",
								   ccname,
								   RelationGetRelationName(relation))));

			n->contype = CONSTR_CHECK;
			n->location = -1;
			n->conname = pstrdup(ccname);
			n->raw_expr = NULL;
			n->cooked_expr = nodeToString(ccbin_node);
			cxt->ckconstraints = lappend(cxt->ckconstraints, n);

			/* Copy comment on constraint */
			if ((table_like_clause->options & CREATE_TABLE_LIKE_COMMENTS) &&
				(comment = GetComment(get_relation_constraint_oid(RelationGetRelid(relation),
																  n->conname, false),
									  ConstraintRelationId,
									  0)) != NULL)
			{
				CommentStmt *stmt = makeNode(CommentStmt);

				stmt->objtype = OBJECT_TABCONSTRAINT;
				stmt->object = (Node *) list_make3(makeString(cxt->relation->schemaname),
												   makeString(cxt->relation->relname),
												   makeString(n->conname));
				stmt->comment = comment;

				cxt->alist = lappend(cxt->alist, stmt);
			}
		}

	}

	/*
	 * Likewise, copy indexes if requested
	 */
	if ((table_like_clause->options & CREATE_TABLE_LIKE_INDEXES) &&
		relation->rd_rel->relhasindex)
	{
		List	   *parent_indexes;
		ListCell   *l;

		parent_indexes = RelationGetIndexList(relation);

		foreach(l, parent_indexes)
		{
			Oid			parent_index_oid = lfirst_oid(l);
			Relation	parent_index;
			IndexStmt  *index_stmt;

			parent_index = index_open(parent_index_oid, AccessShareLock);

			/* Build CREATE INDEX statement to recreate the parent_index */
                        index_stmt = generateClonedIndexStmt(cxt->relation, InvalidOid,
                                                                                                 parent_index,
												 attmap, tupleDesc->natts, NULL);

			/* Copy comment on index, if requested */
			if (table_like_clause->options & CREATE_TABLE_LIKE_COMMENTS)
			{
				comment = GetComment(parent_index_oid, RelationRelationId, 0);

				/*
				 * We make use of IndexStmt's idxcomment option, so as not to
				 * need to know now what name the index will have.
				 */
				index_stmt->idxcomment = comment;
			}

			/* Save it in the inh_indexes list for the time being */
			cxt->inh_indexes = lappend(cxt->inh_indexes, index_stmt);

			index_close(parent_index, AccessShareLock);
		}
	}

	/*
	 * Close the parent rel, but keep our AccessShareLock on it until xact
	 * commit.  That will prevent someone else from deleting or ALTERing the
	 * parent before the child is committed.
	 */
	heap_close(relation, NoLock);
}

static void
transformOfType(CreateStmtContext *cxt, TypeName *ofTypename)
{
	HeapTuple	tuple;
	TupleDesc	tupdesc;
	int			i;
	Oid			ofTypeId;

	AssertArg(ofTypename);

	tuple = typenameType(NULL, ofTypename, NULL);
	check_of_type(tuple);
	ofTypeId = HeapTupleGetOid(tuple);
	ofTypename->typeOid = ofTypeId; /* cached for later */

	tupdesc = lookup_rowtype_tupdesc(ofTypeId, -1);
	for (i = 0; i < tupdesc->natts; i++)
	{
		Form_pg_attribute attr = TupleDescAttr(tupdesc, i);
		ColumnDef  *n;

		if (attr->attisdropped)
			continue;

		n = makeNode(ColumnDef);
		n->colname = pstrdup(NameStr(attr->attname));
		n->typeName = makeTypeNameFromOid(attr->atttypid, attr->atttypmod);
		n->inhcount = 0;
		n->is_local = true;
		n->is_not_null = false;
		n->is_from_type = true;
		n->storage = 0;
		n->raw_default = NULL;
		n->cooked_default = NULL;
		n->collClause = NULL;
		n->collOid = attr->attcollation;
		n->constraints = NIL;
		n->location = -1;
		cxt->columns = lappend(cxt->columns, n);
	}
	DecrTupleDescRefCount(tupdesc);

	ReleaseSysCache(tuple);
}

/*
 * Generate an IndexStmt node using information from an already existing index
 * "source_idx", for the rel identified either by heapRel or heapRelid.
 *
 * Attribute numbers should be adjusted according to attmap.
 */
IndexStmt *
generateClonedIndexStmt(RangeVar *heapRel, Oid heapRelid, Relation source_idx,
						const AttrNumber *attmap, int attmap_length, Oid *constraintOid)
{
	Oid			source_relid = RelationGetRelid(source_idx);
	HeapTuple	ht_idxrel;
	HeapTuple	ht_idx;
	HeapTuple	ht_am;
	Form_pg_class idxrelrec;
	Form_pg_index idxrec;
	Form_pg_am	amrec;
	oidvector  *indcollation;
	oidvector  *indclass;
	IndexStmt  *index;
	List	   *indexprs;
	ListCell   *indexpr_item;
	Oid			indrelid;
	int			keyno;
	Oid			keycoltype;
	Datum		datum;
	bool		isnull;

	Assert((heapRel == NULL && OidIsValid(heapRelid)) ||
		   (heapRel != NULL && !OidIsValid(heapRelid)));

	/*
	 * Fetch pg_class tuple of source index.  We can't use the copy in the
	 * relcache entry because it doesn't include optional fields.
	 */
	ht_idxrel = SearchSysCache1(RELOID, ObjectIdGetDatum(source_relid));
	if (!HeapTupleIsValid(ht_idxrel))
		elog(ERROR, "cache lookup failed for relation %u", source_relid);
	idxrelrec = (Form_pg_class) GETSTRUCT(ht_idxrel);

	/* Fetch pg_index tuple for source index from relcache entry */
	ht_idx = source_idx->rd_indextuple;
	idxrec = (Form_pg_index) GETSTRUCT(ht_idx);
	indrelid = idxrec->indrelid;

	/* Fetch the pg_am tuple of the index' access method */
	ht_am = SearchSysCache1(AMOID, ObjectIdGetDatum(idxrelrec->relam));
	if (!HeapTupleIsValid(ht_am))
		elog(ERROR, "cache lookup failed for access method %u",
			 idxrelrec->relam);
	amrec = (Form_pg_am) GETSTRUCT(ht_am);

	/* Extract indcollation from the pg_index tuple */
	datum = SysCacheGetAttr(INDEXRELID, ht_idx,
							Anum_pg_index_indcollation, &isnull);
	Assert(!isnull);
	indcollation = (oidvector *) DatumGetPointer(datum);

	/* Extract indclass from the pg_index tuple */
	datum = SysCacheGetAttr(INDEXRELID, ht_idx,
							Anum_pg_index_indclass, &isnull);
	Assert(!isnull);
	indclass = (oidvector *) DatumGetPointer(datum);

	/* Begin building the IndexStmt */
	index = makeNode(IndexStmt);
	index->relation = heapRel;
	index->relationId = heapRelid;
	index->accessMethod = pstrdup(NameStr(amrec->amname));
	if (OidIsValid(idxrelrec->reltablespace))
		index->tableSpace = get_tablespace_name(idxrelrec->reltablespace);
	else
		index->tableSpace = NULL;
	index->excludeOpNames = NIL;
	index->idxcomment = NULL;
	index->indexOid = InvalidOid;
	index->oldNode = InvalidOid;
	index->unique = idxrec->indisunique;
	index->primary = idxrec->indisprimary;
	index->transformed = true;	/* don't need transformIndexStmt */
	index->concurrent = false;
	index->if_not_exists = false;
	index->reset_default_tblspc = false;

	/*
	 * We don't try to preserve the name of the source index; instead, just
	 * let DefineIndex() choose a reasonable name.  (If we tried to preserve
	 * the name, we'd get duplicate-relation-name failures unless the source
	 * table was in a different schema.)
	 * Note: we have to use the same index name with meta global temp table 
	 * when activating the global temp table
	 */
		index->idxname = NULL;

	/*
	 * If the index is marked PRIMARY or has an exclusion condition, it's
	 * certainly from a constraint; else, if it's not marked UNIQUE, it
	 * certainly isn't.  If it is or might be from a constraint, we have to
	 * fetch the pg_constraint record.
	 */
	if (index->primary || index->unique || idxrec->indisexclusion)
	{
		Oid			constraintId = get_index_constraint(source_relid);

		if (OidIsValid(constraintId))
		{
			HeapTuple	ht_constr;
			Form_pg_constraint conrec;

			if (constraintOid)
				*constraintOid = constraintId;

			ht_constr = SearchSysCache1(CONSTROID,
										ObjectIdGetDatum(constraintId));
			if (!HeapTupleIsValid(ht_constr))
				elog(ERROR, "cache lookup failed for constraint %u",
					 constraintId);
			conrec = (Form_pg_constraint) GETSTRUCT(ht_constr);

			index->isconstraint = true;
			index->deferrable = conrec->condeferrable;
			index->initdeferred = conrec->condeferred;

			/* If it's an exclusion constraint, we need the operator names */
			if (idxrec->indisexclusion)
			{
				Datum	   *elems;
				int			nElems;
				int			i;

				Assert(conrec->contype == CONSTRAINT_EXCLUSION);
				/* Extract operator OIDs from the pg_constraint tuple */
				datum = SysCacheGetAttr(CONSTROID, ht_constr,
										Anum_pg_constraint_conexclop,
										&isnull);
				if (isnull)
					elog(ERROR, "null conexclop for constraint %u",
						 constraintId);

				deconstruct_array(DatumGetArrayTypeP(datum),
								  OIDOID, sizeof(Oid), true, 'i',
								  &elems, NULL, &nElems);

				for (i = 0; i < nElems; i++)
				{
					Oid			operid = DatumGetObjectId(elems[i]);
					HeapTuple	opertup;
					Form_pg_operator operform;
					char	   *oprname;
					char	   *nspname;
					List	   *namelist;

					opertup = SearchSysCache1(OPEROID,
											  ObjectIdGetDatum(operid));
					if (!HeapTupleIsValid(opertup))
						elog(ERROR, "cache lookup failed for operator %u",
							 operid);
					operform = (Form_pg_operator) GETSTRUCT(opertup);
					oprname = pstrdup(NameStr(operform->oprname));
					/* For simplicity we always schema-qualify the op name */
					nspname = get_namespace_name(operform->oprnamespace);
					namelist = list_make2(makeString(nspname),
										  makeString(oprname));
					index->excludeOpNames = lappend(index->excludeOpNames,
													namelist);
					ReleaseSysCache(opertup);
				}
			}

			ReleaseSysCache(ht_constr);
		}
		else
			index->isconstraint = false;
	}
	else
		index->isconstraint = false;

	/* Get the index expressions, if any */
	datum = SysCacheGetAttr(INDEXRELID, ht_idx,
							Anum_pg_index_indexprs, &isnull);
	if (!isnull)
	{
		char	   *exprsString;

		exprsString = TextDatumGetCString(datum);
		indexprs = (List *) stringToNode(exprsString);
	}
	else
		indexprs = NIL;

	/* Build the list of IndexElem */
	index->indexParams = NIL;

	indexpr_item = list_head(indexprs);
	for (keyno = 0; keyno < idxrec->indnatts; keyno++)
	{
		IndexElem  *iparam;
		AttrNumber	attnum = idxrec->indkey.values[keyno];
		Form_pg_attribute attr = TupleDescAttr(RelationGetDescr(source_idx),
											   keyno);
		int16		opt = source_idx->rd_indoption[keyno];

		iparam = makeNode(IndexElem);

		if (AttributeNumberIsValid(attnum))
		{
			/* Simple index column */
			char	   *attname;

			attname = get_relid_attribute_name(indrelid, attnum);
			keycoltype = get_atttype(indrelid, attnum);

			iparam->name = attname;
			iparam->expr = NULL;
		}
		else
		{
			/* Expressional index */
			Node	   *indexkey;
			bool		found_whole_row;

			if (indexpr_item == NULL)
				elog(ERROR, "too few entries in indexprs list");
			indexkey = (Node *) lfirst(indexpr_item);
			indexpr_item = lnext(indexpr_item);

			/* Adjust Vars to match new table's column numbering */
			indexkey = map_variable_attnos(indexkey,
										   1, 0,
										   attmap, attmap_length,
										   InvalidOid, &found_whole_row);

			/* As in transformTableLikeClause, reject whole-row variables */
			if (found_whole_row)
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("cannot convert whole-row table reference"),
						 errdetail("Index \"%s\" contains a whole-row table reference.",
								   RelationGetRelationName(source_idx))));

			iparam->name = NULL;
			iparam->expr = indexkey;

			keycoltype = exprType(indexkey);
		}

		/* Copy the original index column name */
		iparam->indexcolname = pstrdup(NameStr(attr->attname));

		/* Add the collation name, if non-default */
		iparam->collation = get_collation(indcollation->values[keyno], keycoltype);

		/* Add the operator class name, if non-default */
		iparam->opclass = get_opclass(indclass->values[keyno], keycoltype);

		iparam->ordering = SORTBY_DEFAULT;
		iparam->nulls_ordering = SORTBY_NULLS_DEFAULT;

		/* Adjust options if necessary */
		if (source_idx->rd_amroutine->amcanorder)
		{
			/*
			 * If it supports sort ordering, copy DESC and NULLS opts. Don't
			 * set non-default settings unnecessarily, though, so as to
			 * improve the chance of recognizing equivalence to constraint
			 * indexes.
			 */
			if (opt & INDOPTION_DESC)
			{
				iparam->ordering = SORTBY_DESC;
				if ((opt & INDOPTION_NULLS_FIRST) == 0)
					iparam->nulls_ordering = SORTBY_NULLS_LAST;
			}
			else
			{
				if (opt & INDOPTION_NULLS_FIRST)
					iparam->nulls_ordering = SORTBY_NULLS_FIRST;
			}
		}

		index->indexParams = lappend(index->indexParams, iparam);
	}

	/* Copy reloptions if any */
	datum = SysCacheGetAttr(RELOID, ht_idxrel,
							Anum_pg_class_reloptions, &isnull);
	if (!isnull)
		index->options = untransformRelOptions(datum);

	/* If it's a partial index, decompile and append the predicate */
	datum = SysCacheGetAttr(INDEXRELID, ht_idx,
							Anum_pg_index_indpred, &isnull);
	if (!isnull)
	{
		char	   *pred_str;
		Node	   *pred_tree;
		bool		found_whole_row;

		/* Convert text string to node tree */
		pred_str = TextDatumGetCString(datum);
		pred_tree = (Node *) stringToNode(pred_str);

		/* Adjust Vars to match new table's column numbering */
		pred_tree = map_variable_attnos(pred_tree,
										1, 0,
										attmap, attmap_length,
										InvalidOid, &found_whole_row);

		/* As in transformTableLikeClause, reject whole-row variables */
		if (found_whole_row)
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("cannot convert whole-row table reference"),
					 errdetail("Index \"%s\" contains a whole-row table reference.",
							   RelationGetRelationName(source_idx))));

		index->whereClause = pred_tree;
	}

	/* Clean up */
	ReleaseSysCache(ht_idxrel);
	ReleaseSysCache(ht_am);

	return index;
}

/*
 * get_collation		- fetch qualified name of a collation
 *
 * If collation is InvalidOid or is the default for the given actual_datatype,
 * then the return value is NIL.
 */
static List *
get_collation(Oid collation, Oid actual_datatype)
{
	List	   *result;
	HeapTuple	ht_coll;
	Form_pg_collation coll_rec;
	char	   *nsp_name;
	char	   *coll_name;

	if (!OidIsValid(collation))
		return NIL;				/* easy case */
	if (collation == get_typcollation(actual_datatype))
		return NIL;				/* just let it default */

	ht_coll = SearchSysCache1(COLLOID, ObjectIdGetDatum(collation));
	if (!HeapTupleIsValid(ht_coll))
		elog(ERROR, "cache lookup failed for collation %u", collation);
	coll_rec = (Form_pg_collation) GETSTRUCT(ht_coll);

	/* For simplicity, we always schema-qualify the name */
	nsp_name = get_namespace_name(coll_rec->collnamespace);
	coll_name = pstrdup(NameStr(coll_rec->collname));
	result = list_make2(makeString(nsp_name), makeString(coll_name));

	ReleaseSysCache(ht_coll);
	return result;
}

/*
 * get_opclass			- fetch qualified name of an index operator class
 *
 * If the opclass is the default for the given actual_datatype, then
 * the return value is NIL.
 */
static List *
get_opclass(Oid opclass, Oid actual_datatype)
{
	List	   *result = NIL;
	HeapTuple	ht_opc;
	Form_pg_opclass opc_rec;

	ht_opc = SearchSysCache1(CLAOID, ObjectIdGetDatum(opclass));
	if (!HeapTupleIsValid(ht_opc))
		elog(ERROR, "cache lookup failed for opclass %u", opclass);
	opc_rec = (Form_pg_opclass) GETSTRUCT(ht_opc);

	if (GetDefaultOpClass(actual_datatype, opc_rec->opcmethod) != opclass)
	{
		/* For simplicity, we always schema-qualify the name */
		char	   *nsp_name = get_namespace_name(opc_rec->opcnamespace);
		char	   *opc_name = pstrdup(NameStr(opc_rec->opcname));

		result = list_make2(makeString(nsp_name), makeString(opc_name));
	}

	ReleaseSysCache(ht_opc);
	return result;
}


List *
transformCreateExternalStmt(CreateExternalStmt *stmt, const char *queryString)
{
	ParseState *pstate;
	CreateStmtContext cxt = {0};
	List	   *result;
	ListCell   *elements;
	Oid                existing_relid;
	ParseCallbackState pcbstate;
	Oid                namespaceid;

	/* Set up pstate */
	pstate = make_parsestate(NULL);
	pstate->p_sourcetext = queryString;

	/*
	 * Look up the creation namespace.  This also checks permissions on the
	 * target namespace, locks it against concurrent drops, checks for a
	 * preexisting relation in that namespace with the same name, and updates
	 * stmt->relation->relpersistence if the selected namespace is temporary.
	 */
	setup_parser_errposition_callback(&pcbstate, pstate,
									  stmt->relation->location);
	namespaceid =
			RangeVarGetAndCheckCreationNamespace(stmt->relation, NoLock,
												 &existing_relid);
	cancel_parser_errposition_callback(&pcbstate);

	/*
	 * If the relation already exists and the user specified "IF NOT EXISTS",
	 * bail out with a NOTICE.
	 */
	if (stmt->if_not_exists && OidIsValid(existing_relid))
	{
		ereport(NOTICE,
				(errcode(ERRCODE_DUPLICATE_TABLE),
						errmsg("relation \"%s\" already exists, skipping",
							   stmt->relation->relname)));
		return NIL;
	}

	/*
	 * If the target relation name isn't schema-qualified, make it so.  This
	 * prevents some corner cases in which added-on rewritten commands might
	 * think they should apply to other relations that have the same name and
	 * are earlier in the search path.  But a local temp table is effectively
	 * specified to be in pg_temp, so no need for anything extra in that case.
	 */
	if (stmt->relation->schemaname == NULL
		&& stmt->relation->relpersistence != RELPERSISTENCE_TEMP)
		stmt->relation->schemaname = get_namespace_name(namespaceid);

	memset(&cxt, 0, sizeof(CreateStmtContext));

	/*
	 * Create a temporary context in order to confine memory leaks due
	 * to expansions within a short lived context
	 */
	cxt.tempCtx = AllocSetContextCreate(CurrentMemoryContext,
							  "CreateExteranlStmt analyze context",
							  ALLOCSET_DEFAULT_SIZES);

	/*
	 * There exist transformations that might write on the passed on stmt.
	 * Create a copy of it to both protect from (un)intentional writes and be
	 * a bit more explicit of the intended ownership.
	 */
	stmt = (CreateExternalStmt *)copyObject(stmt);

	cxt.pstate = pstate;
	cxt.stmtType = "CREATE EXTERNAL TABLE";
	cxt.relation = stmt->relation;
	cxt.inhRelations = NIL;
	cxt.isalter = false;
	cxt.columns = NIL;
	cxt.ckconstraints = NIL;
	cxt.fkconstraints = NIL;
	cxt.ixconstraints = NIL;
	cxt.attr_encodings = NIL;
	cxt.pkey = NULL;
	cxt.rel = NULL;

	cxt.blist = NIL;
	cxt.alist = NIL;

	/*
	 * Run through each primary element in the table creation clause. Separate
	 * column defs from constraints, and do preliminary analysis.
	 */
	foreach(elements, stmt->tableElts)
	{
		Node	   *element = lfirst(elements);

		switch (nodeTag(element))
		{
			case T_ColumnDef:
				(void)transformColumnDefinition(&cxt, (ColumnDef *) element);
				break;

			case T_Constraint:
				/* should never happen. If it does fix gram.y */
				elog(ERROR, "node type %d not supported for external tables",
					 (int) nodeTag(element));
				break;

			case T_TableLikeClause:
				{
					/* LIKE */
					transformTableLikeClause(&cxt, (TableLikeClause *) element, true);
				}
				break;

			default:
				elog(ERROR, "unrecognized node type: %d",
					 (int) nodeTag(element));
				break;
		}
	}

	if (!stmt->distributeby)
	{
		/*
		 * If constraints require replicated table set it replicated
		 */
		stmt->distributeby = makeNode(DistributeBy);
		if (cxt.fallback_source == FBS_REPLICATE)
		{
			stmt->distributeby->disttype = DISTTYPE_REPLICATION;
			stmt->distributeby->colname = NULL;
		}
		/*
		 * If there are columns suitable for shard/[hash for regress only] distribution distribute on
		 * first of them. Use shard first.
		 */
		else if (cxt.fallback_dist_cols)
		{
			stmt->distributeby->disttype = get_default_distype();
#ifdef __OPENTENBASE_C__
			stmt->distributeby->colname = list_make1(makeString((char *) linitial(cxt.fallback_dist_cols)));
#else
			stmt->distributeby->colname = (char *) linitial(cxt.fallback_dist_cols);
#endif
		}
		/*
		 * If none of above applies distribute by round robin
		 */
		else
		{
			stmt->distributeby->disttype = DISTTYPE_ROUNDROBIN;
			stmt->distributeby->colname = NULL;
		}
	}

	Assert(cxt.ckconstraints == NIL);
	Assert(cxt.fkconstraints == NIL);
	Assert(cxt.ixconstraints == NIL);

	/*
	 * Output results.
	 */
	stmt->tableElts = cxt.columns;

	result = lappend(cxt.blist, stmt);
	result = list_concat(result, cxt.alist);

	MemoryContextDelete(cxt.tempCtx);

	return result;
}


/*
 * transformIndexConstraints
 *		Handle UNIQUE, PRIMARY KEY, EXCLUDE constraints, which create indexes.
 *		We also merge in any index definitions arising from
 *		LIKE ... INCLUDING INDEXES.
 */
static void
transformIndexConstraints(CreateStmtContext *cxt)
{
	IndexStmt  *index;
	List	   *indexlist = NIL;
	ListCell   *lc;

	/*
	 * Run through the constraints that need to generate an index. For PRIMARY
	 * KEY, mark each column as NOT NULL and create an index. For UNIQUE or
	 * EXCLUDE, create an index as for PRIMARY KEY, but do not insist on NOT
	 * NULL.
	 */
	foreach(lc, cxt->ixconstraints)
	{
		Constraint *constraint = lfirst_node(Constraint, lc);

		Assert(constraint->contype == CONSTR_PRIMARY ||
			   constraint->contype == CONSTR_UNIQUE ||
			   constraint->contype == CONSTR_EXCLUSION);

		index = transformIndexConstraint(constraint, cxt);

		indexlist = lappend(indexlist, index);
	}

	/* Add in any indexes defined by LIKE ... INCLUDING INDEXES */
	foreach(lc, cxt->inh_indexes)
	{
		index = (IndexStmt *) lfirst(lc);

		if (index->primary)
		{
			if (cxt->pkey != NULL)
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_TABLE_DEFINITION),
						 errmsg("multiple primary keys for table \"%s\" are not allowed",
								cxt->relation->relname)));
			cxt->pkey = index;
		}

		indexlist = lappend(indexlist, index);
	}

	/*
	 * Scan the index list and remove any redundant index specifications. This
	 * can happen if, for instance, the user writes UNIQUE PRIMARY KEY. A
	 * strict reading of SQL would suggest raising an error instead, but that
	 * strikes me as too anal-retentive. - tgl 2001-02-14
	 *
	 * XXX in ALTER TABLE case, it'd be nice to look for duplicate
	 * pre-existing indexes, too.
	 */
	Assert(cxt->alist == NIL);
	if (cxt->pkey != NULL)
	{
		/* Make sure we keep the PKEY index in preference to others... */
		cxt->alist = list_make1(cxt->pkey);
	}

	foreach(lc, indexlist)
	{
		bool		keep = true;
		ListCell   *k;

		index = lfirst(lc);

		/* if it's pkey, it's already in cxt->alist */
		if (index == cxt->pkey)
			continue;

		foreach(k, cxt->alist)
		{
			IndexStmt  *priorindex = lfirst(k);

			if (equal(index->indexParams, priorindex->indexParams) &&
				equal(index->whereClause, priorindex->whereClause) &&
				equal(index->excludeOpNames, priorindex->excludeOpNames) &&
				strcmp(index->accessMethod, priorindex->accessMethod) == 0 &&
				index->deferrable == priorindex->deferrable &&
				index->initdeferred == priorindex->initdeferred)
			{
				priorindex->unique |= index->unique;

				/*
				 * If the prior index is as yet unnamed, and this one is
				 * named, then transfer the name to the prior index. This
				 * ensures that if we have named and unnamed constraints,
				 * we'll use (at least one of) the names for the index.
				 */
				if (priorindex->idxname == NULL)
					priorindex->idxname = index->idxname;
				keep = false;
				break;
			}
		}

		if (keep)
			cxt->alist = lappend(cxt->alist, index);
	}
}


/*
 * transformIndexConstraint
 *		Transform one UNIQUE, PRIMARY KEY, or EXCLUDE constraint for
 *		transformIndexConstraints.
 */
static IndexStmt *
transformIndexConstraint(Constraint *constraint, CreateStmtContext *cxt)
{
	IndexStmt  *index;
#ifdef PGXC
	bool		isLocalSafe = false;
#endif
#ifdef XCP
	List	   *fallback_cols = NIL;
#endif
	ListCell   *lc;

	index = makeNode(IndexStmt);

	index->unique = (constraint->contype != CONSTR_EXCLUSION);
	index->primary = (constraint->contype == CONSTR_PRIMARY);
	if (index->primary)
	{
		if (cxt->pkey != NULL)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_TABLE_DEFINITION),
					 errmsg("multiple primary keys for table \"%s\" are not allowed",
							cxt->relation->relname),
					 parser_errposition(cxt->pstate, constraint->location)));
		cxt->pkey = index;

		/*
		 * In ALTER TABLE case, a primary index might already exist, but
		 * DefineIndex will check for it.
		 */
	}
	index->isconstraint = true;
	index->deferrable = constraint->deferrable;
	index->initdeferred = constraint->initdeferred;

	if (constraint->conname != NULL)
		index->idxname = pstrdup(constraint->conname);
	else
		index->idxname = NULL;	/* DefineIndex will choose name */

	index->relation = cxt->relation;
	index->accessMethod = constraint->access_method ? constraint->access_method : DEFAULT_INDEX_TYPE;
	index->options = constraint->options;
	index->tableSpace = constraint->indexspace;
	index->whereClause = constraint->where_clause;
	index->indexParams = NIL;
	index->excludeOpNames = NIL;
	index->idxcomment = NULL;
	index->indexOid = InvalidOid;
	index->oldNode = InvalidOid;
	index->transformed = false;
	index->concurrent = false;
	index->if_not_exists = false;
	index->reset_default_tblspc = constraint->reset_default_tblspc;

	/*
	 * If it's ALTER TABLE ADD CONSTRAINT USING INDEX, look up the index and
	 * verify it's usable, then extract the implied column name list.  (We
	 * will not actually need the column name list at runtime, but we need it
	 * now to check for duplicate column entries below.)
	 */
	if (constraint->indexname != NULL)
	{
		char	   *index_name = constraint->indexname;
		Relation	heap_rel = cxt->rel;
		Oid			index_oid;
		Relation	index_rel;
		Form_pg_index index_form;
		oidvector  *indclass;
		Datum		indclassDatum;
		bool		isnull;
		int			i;

		/* Grammar should not allow this with explicit column list */
		Assert(constraint->keys == NIL);

		/* Grammar should only allow PRIMARY and UNIQUE constraints */
		Assert(constraint->contype == CONSTR_PRIMARY ||
			   constraint->contype == CONSTR_UNIQUE);

		/* Must be ALTER, not CREATE, but grammar doesn't enforce that */
		if (!cxt->isalter)
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("cannot use an existing index in CREATE TABLE"),
					 parser_errposition(cxt->pstate, constraint->location)));

		/* Look for the index in the same schema as the table */
		index_oid = get_relname_relid(index_name, RelationGetNamespace(heap_rel));

		if (!OidIsValid(index_oid))
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_OBJECT),
					 errmsg("index \"%s\" does not exist", index_name),
					 parser_errposition(cxt->pstate, constraint->location)));

		/* Open the index (this will throw an error if it is not an index) */
		index_rel = index_open(index_oid, AccessShareLock);
		index_form = index_rel->rd_index;

		/* Check that it does not have an associated constraint already */
		if (OidIsValid(get_index_constraint(index_oid)))
			ereport(ERROR,
					(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
					 errmsg("index \"%s\" is already associated with a constraint",
							index_name),
					 parser_errposition(cxt->pstate, constraint->location)));

		/* Perform validity checks on the index */
		if (index_form->indrelid != RelationGetRelid(heap_rel))
			ereport(ERROR,
					(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
					 errmsg("index \"%s\" does not belong to table \"%s\"",
							index_name, RelationGetRelationName(heap_rel)),
					 parser_errposition(cxt->pstate, constraint->location)));

		if (!IndexIsValid(index_form))
			ereport(ERROR,
					(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
					 errmsg("index \"%s\" is not valid", index_name),
					 parser_errposition(cxt->pstate, constraint->location)));

		if (!index_form->indisunique)
			ereport(ERROR,
					(errcode(ERRCODE_WRONG_OBJECT_TYPE),
					 errmsg("\"%s\" is not a unique index", index_name),
					 errdetail("Cannot create a primary key or unique constraint using such an index."),
					 parser_errposition(cxt->pstate, constraint->location)));

		if (RelationGetIndexExpressions(index_rel) != NIL)
			ereport(ERROR,
					(errcode(ERRCODE_WRONG_OBJECT_TYPE),
					 errmsg("index \"%s\" contains expressions", index_name),
					 errdetail("Cannot create a primary key or unique constraint using such an index."),
					 parser_errposition(cxt->pstate, constraint->location)));

		if (RelationGetIndexPredicate(index_rel) != NIL)
			ereport(ERROR,
					(errcode(ERRCODE_WRONG_OBJECT_TYPE),
					 errmsg("\"%s\" is a partial index", index_name),
					 errdetail("Cannot create a primary key or unique constraint using such an index."),
					 parser_errposition(cxt->pstate, constraint->location)));

		/*
		 * It's probably unsafe to change a deferred index to non-deferred. (A
		 * non-constraint index couldn't be deferred anyway, so this case
		 * should never occur; no need to sweat, but let's check it.)
		 */
		if (!index_form->indimmediate && !constraint->deferrable)
			ereport(ERROR,
					(errcode(ERRCODE_WRONG_OBJECT_TYPE),
					 errmsg("\"%s\" is a deferrable index", index_name),
					 errdetail("Cannot create a non-deferrable constraint using a deferrable index."),
					 parser_errposition(cxt->pstate, constraint->location)));

		/*
		 * Insist on it being a btree.  That's the only kind that supports
		 * uniqueness at the moment anyway; but we must have an index that
		 * exactly matches what you'd get from plain ADD CONSTRAINT syntax,
		 * else dump and reload will produce a different index (breaking
		 * pg_upgrade in particular).
		 */
		if (index_rel->rd_rel->relam != get_index_am_oid(DEFAULT_INDEX_TYPE, false))
			ereport(ERROR,
					(errcode(ERRCODE_WRONG_OBJECT_TYPE),
					 errmsg("index \"%s\" is not a btree", index_name),
					 parser_errposition(cxt->pstate, constraint->location)));

		/* Must get indclass the hard way */
		indclassDatum = SysCacheGetAttr(INDEXRELID, index_rel->rd_indextuple,
										Anum_pg_index_indclass, &isnull);
		Assert(!isnull);
		indclass = (oidvector *) DatumGetPointer(indclassDatum);

		for (i = 0; i < index_form->indnatts; i++)
		{
			int16		attnum = index_form->indkey.values[i];
			Form_pg_attribute attform;
			char	   *attname;
			Oid			defopclass;

			/*
			 * We shouldn't see attnum == 0 here, since we already rejected
			 * expression indexes.  If we do, SystemAttributeDefinition will
			 * throw an error.
			 */
			if (attnum > 0)
			{
				Assert(attnum <= heap_rel->rd_att->natts);
				attform = TupleDescAttr(heap_rel->rd_att, attnum - 1);
			}
			else
				attform = SystemAttributeDefinition(attnum,
													heap_rel->rd_rel->relhasoids
													);
			attname = pstrdup(NameStr(attform->attname));

			/*
			 * Insist on default opclass and sort options.  While the index
			 * would still work as a constraint with non-default settings, it
			 * might not provide exactly the same uniqueness semantics as
			 * you'd get from a normally-created constraint; and there's also
			 * the dump/reload problem mentioned above.
			 */
			defopclass = GetDefaultOpClass(attform->atttypid,
										   index_rel->rd_rel->relam);
			if (indclass->values[i] != defopclass ||
				index_rel->rd_indoption[i] != 0)
				ereport(ERROR,
						(errcode(ERRCODE_WRONG_OBJECT_TYPE),
						 errmsg("index \"%s\" does not have default sorting behavior", index_name),
						 errdetail("Cannot create a primary key or unique constraint using such an index."),
						 parser_errposition(cxt->pstate, constraint->location)));

			constraint->keys = lappend(constraint->keys, makeString(attname));
		}

		/* Close the index relation but keep the lock */
		relation_close(index_rel, NoLock);

		index->indexOid = index_oid;
	}

	/*
	 * If it's an EXCLUDE constraint, the grammar returns a list of pairs of
	 * IndexElems and operator names.  We have to break that apart into
	 * separate lists.
	 */
	if (constraint->contype == CONSTR_EXCLUSION)
	{
		foreach(lc, constraint->exclusions)
		{
			List	   *pair = (List *) lfirst(lc);
			IndexElem  *elem;
			List	   *opname;

			Assert(list_length(pair) == 2);
			elem = linitial_node(IndexElem, pair);
			opname = lsecond_node(List, pair);

			index->indexParams = lappend(index->indexParams, elem);
			index->excludeOpNames = lappend(index->excludeOpNames, opname);
		}

		return index;
	}

	/*
	 * For UNIQUE and PRIMARY KEY, we just have a list of column names.
	 *
	 * Make sure referenced keys exist.  If we are making a PRIMARY KEY index,
	 * also make sure they are NOT NULL, if possible. (Although we could leave
	 * it to DefineIndex to mark the columns NOT NULL, it's more efficient to
	 * get it right the first time.)
	 */
	foreach(lc, constraint->keys)
	{
		char	   *key = strVal(lfirst(lc));
		bool		found = false;
		ColumnDef  *column = NULL;
		ListCell   *columns;
		IndexElem  *iparam;

		foreach(columns, cxt->columns)
		{
			column = lfirst_node(ColumnDef, columns);
			if (strcmp(column->colname, key) == 0)
			{
				found = true;
				break;
			}
		}
		if (found)
		{
			/* found column in the new table; force it to be NOT NULL */
			if (constraint->contype == CONSTR_PRIMARY)
				column->is_not_null = TRUE;
		}
		else if (SystemAttributeByName(key, cxt->hasoids
										) != NULL)
		{
			/*
			 * column will be a system column in the new table, so accept it.
			 * System columns can't ever be null, so no need to worry about
			 * PRIMARY/NOT NULL constraint.
			 */
			found = true;
		}
		else if (cxt->inhRelations)
		{
			/* try inherited tables */
			ListCell   *inher;

			foreach(inher, cxt->inhRelations)
			{
				RangeVar   *inh = lfirst_node(RangeVar, inher);
				Relation	rel;
				int			count;

				rel = heap_openrv(inh, AccessShareLock);
				/* check user requested inheritance from valid relkind */
				if (rel->rd_rel->relkind != RELKIND_RELATION &&
					rel->rd_rel->relkind != RELKIND_FOREIGN_TABLE &&
					rel->rd_rel->relkind != RELKIND_PARTITIONED_TABLE)
					ereport(ERROR,
							(errcode(ERRCODE_WRONG_OBJECT_TYPE),
							 errmsg("inherited relation \"%s\" is not a table or foreign table",
									inh->relname)));
				for (count = 0; count < rel->rd_att->natts; count++)
				{
					Form_pg_attribute inhattr = TupleDescAttr(rel->rd_att,
															  count);
					char	   *inhname = NameStr(inhattr->attname);

					if (inhattr->attisdropped)
						continue;
					if (strcmp(key, inhname) == 0)
					{
						found = true;
#ifdef XCP
						/*
						 * We should add the column to the fallback list now,
						 * so it could be found there, because inherited
						 * columns are not normally added.
						 * Do not modify the list if it is set from a priority
						 * source.
						 */
						if (IS_PGXC_COORDINATOR &&
								cxt->distributeby == NULL && !cxt->isalter &&
								cxt->fallback_source <= FBS_COLDEF &&
								IsTypeHashDistributable(inhattr->atttypid))
						{
							cxt->fallback_dist_cols =
									lappend(cxt->fallback_dist_cols,
											pstrdup(inhname));
							cxt->fallback_source = FBS_COLDEF;
						}
#endif

						/*
						 * We currently have no easy way to force an inherited
						 * column to be NOT NULL at creation, if its parent
						 * wasn't so already. We leave it to DefineIndex to
						 * fix things up in this case.
						 */
						break;
					}
				}
				heap_close(rel, NoLock);
				if (found)
					break;
			}
		}

		/*
		 * In the ALTER TABLE case, don't complain about index keys not
		 * created in the command; they may well exist already. DefineIndex
		 * will complain about them if not, and will also take care of marking
		 * them NOT NULL.
		 */
		if (!found && !cxt->isalter)
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_COLUMN),
					 errmsg("column \"%s\" named in key does not exist", key),
					 parser_errposition(cxt->pstate, constraint->location)));

		/* Check for PRIMARY KEY(foo, foo) */
		foreach(columns, index->indexParams)
		{
			iparam = (IndexElem *) lfirst(columns);
			if (iparam->name && strcmp(key, iparam->name) == 0)
			{
				if (index->primary)
					ereport(ERROR,
							(errcode(ERRCODE_DUPLICATE_COLUMN),
							 errmsg("column \"%s\" appears twice in primary key constraint",
									key),
							 parser_errposition(cxt->pstate, constraint->location)));
				else
					ereport(ERROR,
							(errcode(ERRCODE_DUPLICATE_COLUMN),
							 errmsg("column \"%s\" appears twice in unique constraint",
									key),
							 parser_errposition(cxt->pstate, constraint->location)));
			}
		}

#ifdef PGXC
#ifdef __OPENTENBASE__
		if (IsPostmasterEnvironment)
#else
		if (IS_PGXC_COORDINATOR)
#endif
		{
			/*
			 * Check if index can be enforced locally
			 */
			if (!isLocalSafe)
			{
				ListCell *lc;
				/*
				 * If distribution is defined check current column against
				 * the distribution.
				 */
				if (cxt->distributeby)
#ifdef __OPENTENBASE_C__
				{
					char *partcolname = NULL;

					if (cxt->distributeby->colname)
					{
						partcolname = strVal(list_nth(cxt->distributeby->colname, 0));
					}
					
					isLocalSafe = CheckLocalIndexColumn (
							ConvertToLocatorType(cxt->distributeby->disttype),
							partcolname, key);
				}
#else
					isLocalSafe = CheckLocalIndexColumn (
							ConvertToLocatorType(cxt->distributeby->disttype),
							cxt->distributeby->colname, key);
#endif
				/*
				 * Similar, if altering existing table check against target
				 * table distribution
				 */
				if (cxt->isalter)
					isLocalSafe = cxt->rel->rd_locator_info == NULL ||
							CheckLocalIndexColumn (
									cxt->rel->rd_locator_info->locatorType,
									cxt->rel->rd_locator_info->nDisAttrs == 0 ? NULL :
									get_attname(cxt->rel->rd_locator_info->relid,
									            cxt->rel->rd_locator_info->disAttrNums[0]),
									key);

				/*
				 * Check if it is possible to distribute table by this column
				 * If yes, save it, and replace the fallback list when done
				 */
				foreach (lc, cxt->fallback_dist_cols)
				{
					char *col = (char *) lfirst(lc);

					if (strcmp(key, col) == 0)
					{
						fallback_cols = lappend(fallback_cols, pstrdup(key));
						break;
					}
				}
			}
		}
#endif

		/* OK, add it to the index definition */
		iparam = makeNode(IndexElem);
		iparam->name = pstrdup(key);
		iparam->expr = NULL;
		iparam->indexcolname = NULL;
		iparam->collation = NIL;
		iparam->opclass = NIL;
		iparam->ordering = SORTBY_DEFAULT;
		iparam->nulls_ordering = SORTBY_NULLS_DEFAULT;
		index->indexParams = lappend(index->indexParams, iparam);
	}
#ifdef PGXC
#ifdef __OPENTENBASE__
	if (IsPostmasterEnvironment && !isLocalSafe)
#else
	if (IS_PGXC_COORDINATOR && !isLocalSafe)
#endif
	{
		if ((cxt->distributeby || cxt->isalter) && !IS_CENTRALIZED_MODE)
		{
			/*
			 * Index is not safe for defined distribution; since for replicated
			 * distribution any index is safe and for round robin none, but
			 * this case bombs out immediately, so that is incompatible
			 * HASH or MODULO. Report the problem.
			 */
			if (loose_constraints && cxt->isalter && index->unique)
				ereport(WARNING,
					(errcode(ERRCODE_INVALID_COLUMN_REFERENCE),
					 errmsg("Unique index of distributed table must contain the "
							"distribution column.")));
			else
				ereport(ERROR,
					(errcode(ERRCODE_INVALID_COLUMN_REFERENCE),
					 errmsg("Unique index of distributed table must contain the "
							"distribution column.")));
		}
		else
		{
			/*
			 * fallback_dist_cols saves the columns that can be used for distribution
			 * key, fallback_cols saves current index columns which are same with
			 * colunms in fallback_dist_cols, In usual case, use fallback_cols to
			 * replace fallback_dist_cols. However when there are more than one index,
			 * we will enter the function more than once. The first time we enter the
			 * function, replace fallback_dist_cols with fallback_cols, then the second
			 * time we enter the function fallback_cols will be null, and then we will
			 * enter else branch to set fallback_source to be FBS_REPLICATE, which
			 * will set the distribution type to be replication. For centralized mode,
			 * adjust fallback_dist_cols with index columns in the head of list instead
			 * of replacing it with fallback_cols so that we won't set fallback_source to
			 * be FBS_REPLICATE.
			 */
			if (IS_CENTRALIZED_MODE)
			{
				ListCell	*lc = NULL;
				ListCell	*lc1 = NULL;
				List		*tmp_fallback_cols = list_copy(fallback_cols);
				/*
					* put key in indexs in the head of fallback_dist_cols, so
					* that the key can used to be distribution key.
					*/
				foreach (lc, cxt->fallback_dist_cols)
				{
					bool exists = false;
					char *col = (char *) lfirst(lc);
					foreach (lc1, tmp_fallback_cols)
					{
						char *key = (char *) lfirst(lc1);
						if (strcmp(key, col) == 0)
						{
							exists = true;
							break;
						}
					}
					if (!exists)
					{
						fallback_cols = lappend(fallback_cols, pstrdup(col));
					}
				}
				list_free(tmp_fallback_cols);
			}

			if (fallback_cols)
			{
				list_free_deep(cxt->fallback_dist_cols);
				cxt->fallback_dist_cols = fallback_cols;
				if (index->primary)
					cxt->fallback_source = FBS_PKEY;
				else if (cxt->fallback_source < FBS_PKEY)
					cxt->fallback_source = FBS_UIDX;
			}
			else
			{
				if (cxt->fallback_dist_cols)
				{
					list_free_deep(cxt->fallback_dist_cols);
					cxt->fallback_dist_cols = NIL;
				}
				cxt->fallback_source = FBS_REPLICATE;
			}
		}
	}
#endif

	return index;
}

/*
 * transformCheckConstraints
 *		handle CHECK constraints
 *
 * Right now, there's nothing to do here when called from ALTER TABLE,
 * but the other constraint-transformation functions are called in both
 * the CREATE TABLE and ALTER TABLE paths, so do the same here, and just
 * don't do anything if we're not authorized to skip validation.
 */
static void
transformCheckConstraints(CreateStmtContext *cxt, bool skipValidation)
{
	ListCell   *ckclist;

	if (cxt->ckconstraints == NIL)
		return;

	/*
	 * If creating a new table (but not a foreign table), we can safely skip
	 * validation of check constraints, and nonetheless mark them valid. (This
	 * will override any user-supplied NOT VALID flag.)
	 */
	if (skipValidation)
	{
		foreach(ckclist, cxt->ckconstraints)
		{
			Constraint *constraint = (Constraint *) lfirst(ckclist);

			constraint->skip_validation = true;
			constraint->initially_valid = true;
		}
	}
}

/*
 * transformFKConstraints
 *		handle FOREIGN KEY constraints
 */
static void
transformFKConstraints(CreateStmtContext *cxt,
					   bool skipValidation, bool isAddConstraint)
{
	ListCell   *fkclist;

	if (cxt->fkconstraints == NIL)
		return;

	/*
	 * If CREATE TABLE or adding a column with NULL default, we can safely
	 * skip validation of FK constraints, and nonetheless mark them valid.
	 * (This will override any user-supplied NOT VALID flag.)
	 */
	if (skipValidation)
	{
		foreach(fkclist, cxt->fkconstraints)
		{
			Constraint *constraint = (Constraint *) lfirst(fkclist);

			constraint->skip_validation = true;
			constraint->initially_valid = true;
		}
	}

#ifdef PGXC
	/* Only allow constraints that are locally enforceable - no distributed ones */
	checkLocalFKConstraints(cxt);
#endif

	/*
	 * For CREATE TABLE or ALTER TABLE ADD COLUMN, gin up an ALTER TABLE ADD
	 * CONSTRAINT command to execute after the basic command is complete. (If
	 * called from ADD CONSTRAINT, that routine will add the FK constraints to
	 * its own subcommand list.)
	 *
	 * Note: the ADD CONSTRAINT command must also execute after any index
	 * creation commands.  Thus, this should run after
	 * transformIndexConstraints, so that the CREATE INDEX commands are
	 * already in cxt->alist.
	 */
	if (!isAddConstraint)
	{
		AlterTableStmt *alterstmt = makeNode(AlterTableStmt);

		alterstmt->relation = cxt->relation;
		alterstmt->cmds = NIL;
		alterstmt->relkind = OBJECT_TABLE;

		foreach(fkclist, cxt->fkconstraints)
		{
			Constraint *constraint = (Constraint *) lfirst(fkclist);
			AlterTableCmd *altercmd = makeNode(AlterTableCmd);

			altercmd->subtype = AT_ProcessedConstraint;
			altercmd->name = NULL;
			altercmd->def = (Node *) constraint;
			alterstmt->cmds = lappend(alterstmt->cmds, altercmd);
		}

		cxt->alist = lappend(cxt->alist, alterstmt);
	}
}


/*
 * transformPartialClusterConstraints
 *		handle partial cluster constraints
 *
 * Right now, there's nothing to do here in terms of creation, as partial
 * cluster key is mostly a marker. However we need to perform necessary
 * checks here.
 */
static void
transformPartialClusterConstraints(CreateStmtContext *cxt)
{
	ListCell *clusterconstraint = NULL;
	ListCell *column1 = NULL;
	ListCell *column2 = NULL;
	char	 *key1 = NULL;
	char	 *key2 = NULL;
	Constraint *constraint = NULL;

	if (cxt->columns == NIL)
	{
		/* Partial cluster constraints will always have columns in their statements */
		return;
	}

	/* Check duplicated columns */
	foreach(clusterconstraint, cxt->clusterconstraints)
	{
		constraint = (Constraint *) lfirst(clusterconstraint);
		foreach(column1, constraint->keys)
		{
			key1 = strVal(lfirst(column1));

			column2 = lnext(column1);
			while (column2 != NULL)
			{
				key2 = strVal(lfirst(column2));
				if (strcasecmp(key1, key2) == 0)
				{
					ereport(ERROR, (errcode(ERRCODE_DUPLICATE_OBJECT),
								errmsg("partial cluster constraint contains multiple reference of \"%s\" column",
								key1)));
				}

				/*
				 * We dont allow duplicate in any circumstances. Even though
				 * containing multiple entry of the same key probably will not yield us
				 * any different results, it violate the principle
				 * of data constraints, so check all insteads.
				 */
				column2 = lnext(column2);
			}
		}
	}

}

/*
 * transformIndexStmt - parse analysis for CREATE INDEX and ALTER TABLE
 *
 * Note: this is a no-op for an index not using either index expressions or
 * a predicate expression.  There are several code paths that create indexes
 * without bothering to call this, because they know they don't have any
 * such expressions to deal with.
 *
 * To avoid race conditions, it's important that this function rely only on
 * the passed-in relid (and not on stmt->relation) to determine the target
 * relation.
 */
IndexStmt *
transformIndexStmt(Oid relid, IndexStmt *stmt, const char *queryString)
{
	ParseState *pstate;
	RangeTblEntry *rte;
	ListCell   *l;
	Relation	rel;

	/* Nothing to do if statement already transformed. */
	if (stmt->transformed)
		return stmt;

	/*
	 * We must not scribble on the passed-in IndexStmt, so copy it.  (This is
	 * overkill, but easy.)
	 */
	stmt = copyObject(stmt);

	/* Set up pstate */
	pstate = make_parsestate(NULL);
	pstate->p_sourcetext = queryString;

	/*
	 * Put the parent table into the rtable so that the expressions can refer
	 * to its fields without qualification.  Caller is responsible for locking
	 * relation, but we still need to open it.
	 */
	rel = relation_open(relid, NoLock);
	rte = addRangeTableEntryForRelation(pstate, rel, NULL, false, true);

	/* no to join list, yes to namespaces */
	addRTEtoQuery(pstate, rte, false, true, true);

	/* take care of the where clause */
	if (stmt->whereClause)
	{
		stmt->whereClause = transformWhereClause(pstate,
												 stmt->whereClause,
												 EXPR_KIND_INDEX_PREDICATE,
												 "WHERE");
		/* we have to fix its collations too */
		assign_expr_collations(pstate, stmt->whereClause);
	}

	/* take care of any index expressions */
	foreach(l, stmt->indexParams)
	{
		IndexElem  *ielem = (IndexElem *) lfirst(l);

		if (ielem->expr)
		{
			/* Extract preliminary index col name before transforming expr */
			if (ielem->indexcolname == NULL)
				ielem->indexcolname = FigureIndexColname(ielem->expr);

			/* Now do parse transformation of the expression */
			ielem->expr = transformExpr(pstate, ielem->expr,
										EXPR_KIND_INDEX_EXPRESSION);

			/* We have to fix its collations too */
			assign_expr_collations(pstate, ielem->expr);

			/*
			 * transformExpr() should have already rejected subqueries,
			 * aggregates, window functions, and SRFs, based on the EXPR_KIND_
			 * for an index expression.
			 *
			 * DefineIndex() will make more checks.
			 */
		}
	}

	/*
	 * Check that only the base rel is mentioned.  (This should be dead code
	 * now that add_missing_from is history.)
	 */
	if (list_length(pstate->p_rtable) != 1)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_COLUMN_REFERENCE),
				 errmsg("index expressions and predicates can refer only to the table being indexed")));

	free_parsestate(pstate);

	/* Close relation */
	heap_close(rel, NoLock);

	/* Mark statement as successfully transformed */
	stmt->transformed = true;

	return stmt;
}


/*
 * transformRuleStmt -
 *	  transform a CREATE RULE Statement. The action is a list of parse
 *	  trees which is transformed into a list of query trees, and we also
 *	  transform the WHERE clause if any.
 *
 * actions and whereClause are output parameters that receive the
 * transformed results.
 *
 * Note that we must not scribble on the passed-in RuleStmt, so we do
 * copyObject() on the actions and WHERE clause.
 */
void
transformRuleStmt(RuleStmt *stmt, const char *queryString,
				  List **actions, Node **whereClause)
{
	Relation	rel;
	ParseState *pstate;
	RangeTblEntry *oldrte;
	RangeTblEntry *newrte;

	/*
	 * To avoid deadlock, make sure the first thing we do is grab
	 * AccessExclusiveLock on the target relation.  This will be needed by
	 * DefineQueryRewrite(), and we don't want to grab a lesser lock
	 * beforehand.
	 */
	rel = heap_openrv(stmt->relation, AccessExclusiveLock);

	if (rel->rd_rel->relkind == RELKIND_MATVIEW)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("rules on materialized views are not supported")));

	/* Set up pstate */
	pstate = make_parsestate(NULL);
	pstate->p_sourcetext = queryString;

	/*
	 * NOTE: 'OLD' must always have a varno equal to 1 and 'NEW' equal to 2.
	 * Set up their RTEs in the main pstate for use in parsing the rule
	 * qualification.
	 */
	oldrte = addRangeTableEntryForRelation(pstate, rel,
										   makeAlias((ORA_MODE)? "OLD" : "old", NIL),
										   false, false);
	newrte = addRangeTableEntryForRelation(pstate, rel,
										   makeAlias((ORA_MODE)? "NEW" : "new", NIL),
										   false, false);
	/* Must override addRangeTableEntry's default access-check flags */
	oldrte->requiredPerms = 0;
	newrte->requiredPerms = 0;

	/*
	 * They must be in the namespace too for lookup purposes, but only add the
	 * one(s) that are relevant for the current kind of rule.  In an UPDATE
	 * rule, quals must refer to OLD.field or NEW.field to be unambiguous, but
	 * there's no need to be so picky for INSERT & DELETE.  We do not add them
	 * to the joinlist.
	 */
	switch (stmt->event)
	{
		case CMD_SELECT:
			addRTEtoQuery(pstate, oldrte, false, true, true);
			break;
		case CMD_UPDATE:
			addRTEtoQuery(pstate, oldrte, false, true, true);
			addRTEtoQuery(pstate, newrte, false, true, true);
			break;
		case CMD_INSERT:
			addRTEtoQuery(pstate, newrte, false, true, true);
			break;
		case CMD_DELETE:
			addRTEtoQuery(pstate, oldrte, false, true, true);
			break;
		default:
			elog(ERROR, "unrecognized event type: %d",
				 (int) stmt->event);
			break;
	}

	/* take care of the where clause */
	*whereClause = transformWhereClause(pstate,
										(Node *) copyObject(stmt->whereClause),
										EXPR_KIND_WHERE,
										"WHERE");
	/* we have to fix its collations too */
	assign_expr_collations(pstate, *whereClause);

	/* this is probably dead code without add_missing_from: */
	if (list_length(pstate->p_rtable) != 2) /* naughty, naughty... */
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
				 errmsg("rule WHERE condition cannot contain references to other relations")));

	/*
	 * 'instead nothing' rules with a qualification need a query rangetable so
	 * the rewrite handler can add the negated rule qualification to the
	 * original query. We create a query with the new command type CMD_NOTHING
	 * here that is treated specially by the rewrite system.
	 */
	if (stmt->actions == NIL)
	{
		Query	   *nothing_qry = makeNode(Query);

		nothing_qry->commandType = CMD_NOTHING;
		nothing_qry->rtable = pstate->p_rtable;
		nothing_qry->jointree = makeFromExpr(NIL, NULL);	/* no join wanted */

		*actions = list_make1(nothing_qry);
	}
	else
	{
		ListCell   *l;
		List	   *newactions = NIL;

		/*
		 * transform each statement, like parse_sub_analyze()
		 */
		foreach(l, stmt->actions)
		{
			Node	   *action = (Node *) lfirst(l);
			ParseState *sub_pstate = make_parsestate(NULL);
			Query	   *sub_qry,
					   *top_subqry;
			bool		has_old,
						has_new;

#ifdef PGXC
			if (IsA(action, NotifyStmt))
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
						 errmsg("Rule may not use NOTIFY, it is not yet supported")));
#endif
			/*
			 * Since outer ParseState isn't parent of inner, have to pass down
			 * the query text by hand.
			 */
			sub_pstate->p_sourcetext = queryString;

			/*
			 * Set up OLD/NEW in the rtable for this statement.  The entries
			 * are added only to relnamespace, not varnamespace, because we
			 * don't want them to be referred to by unqualified field names
			 * nor "*" in the rule actions.  We decide later whether to put
			 * them in the joinlist.
			 */
			oldrte = addRangeTableEntryForRelation(sub_pstate, rel,
												   makeAlias((ORA_MODE)? "OLD" : "old", NIL),
												   false, false);
			newrte = addRangeTableEntryForRelation(sub_pstate, rel,
												   makeAlias((ORA_MODE)? "NEW" : "new", NIL),
												   false, false);
			oldrte->requiredPerms = 0;
			newrte->requiredPerms = 0;
			addRTEtoQuery(sub_pstate, oldrte, false, true, false);
			addRTEtoQuery(sub_pstate, newrte, false, true, false);

			/* Transform the rule action statement */
			top_subqry = transformStmt(sub_pstate,
									   (Node *) copyObject(action));

			/*
			 * We cannot support utility-statement actions (eg NOTIFY) with
			 * nonempty rule WHERE conditions, because there's no way to make
			 * the utility action execute conditionally.
			 */
			if (top_subqry->commandType == CMD_UTILITY &&
				*whereClause != NULL)
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
						 errmsg("rules with WHERE conditions can only have SELECT, INSERT, UPDATE, or DELETE actions")));

			/*
			 * If the action is INSERT...SELECT, OLD/NEW have been pushed down
			 * into the SELECT, and that's what we need to look at. (Ugly
			 * kluge ... try to fix this when we redesign querytrees.)
			 */
			sub_qry = getInsertSelectQuery(top_subqry, NULL);

			/*
			 * If the sub_qry is a setop, we cannot attach any qualifications
			 * to it, because the planner won't notice them.  This could
			 * perhaps be relaxed someday, but for now, we may as well reject
			 * such a rule immediately.
			 */
			if (sub_qry->setOperations != NULL && *whereClause != NULL)
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("conditional UNION/INTERSECT/EXCEPT statements are not implemented")));

			/*
			 * Validate action's use of OLD/NEW, qual too
			 */
			has_old =
				rangeTableEntry_used((Node *) sub_qry, PRS2_OLD_VARNO, 0) ||
				rangeTableEntry_used(*whereClause, PRS2_OLD_VARNO, 0);
			has_new =
				rangeTableEntry_used((Node *) sub_qry, PRS2_NEW_VARNO, 0) ||
				rangeTableEntry_used(*whereClause, PRS2_NEW_VARNO, 0);

			switch (stmt->event)
			{
				case CMD_SELECT:
					if (has_old)
						ereport(ERROR,
								(errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
								 errmsg("ON SELECT rule cannot use OLD")));
					if (has_new)
						ereport(ERROR,
								(errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
								 errmsg("ON SELECT rule cannot use NEW")));
					break;
				case CMD_UPDATE:
					/* both are OK */
					break;
				case CMD_INSERT:
					if (has_old)
						ereport(ERROR,
								(errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
								 errmsg("ON INSERT rule cannot use OLD")));
					break;
				case CMD_DELETE:
					if (has_new)
						ereport(ERROR,
								(errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
								 errmsg("ON DELETE rule cannot use NEW")));
					break;
				default:
					elog(ERROR, "unrecognized event type: %d",
						 (int) stmt->event);
					break;
			}

			/*
			 * OLD/NEW are not allowed in WITH queries, because they would
			 * amount to outer references for the WITH, which we disallow.
			 * However, they were already in the outer rangetable when we
			 * analyzed the query, so we have to check.
			 *
			 * Note that in the INSERT...SELECT case, we need to examine the
			 * CTE lists of both top_subqry and sub_qry.
			 *
			 * Note that we aren't digging into the body of the query looking
			 * for WITHs in nested sub-SELECTs.  A WITH down there can
			 * legitimately refer to OLD/NEW, because it'd be an
			 * indirect-correlated outer reference.
			 */
			if (rangeTableEntry_used((Node *) top_subqry->cteList,
									 PRS2_OLD_VARNO, 0) ||
				rangeTableEntry_used((Node *) sub_qry->cteList,
									 PRS2_OLD_VARNO, 0))
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("cannot refer to OLD within WITH query")));
			if (rangeTableEntry_used((Node *) top_subqry->cteList,
									 PRS2_NEW_VARNO, 0) ||
				rangeTableEntry_used((Node *) sub_qry->cteList,
									 PRS2_NEW_VARNO, 0))
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("cannot refer to NEW within WITH query")));

			/*
			 * For efficiency's sake, add OLD to the rule action's jointree
			 * only if it was actually referenced in the statement or qual.
			 *
			 * For INSERT, NEW is not really a relation (only a reference to
			 * the to-be-inserted tuple) and should never be added to the
			 * jointree.
			 *
			 * For UPDATE, we treat NEW as being another kind of reference to
			 * OLD, because it represents references to *transformed* tuples
			 * of the existing relation.  It would be wrong to enter NEW
			 * separately in the jointree, since that would cause a double
			 * join of the updated relation.  It's also wrong to fail to make
			 * a jointree entry if only NEW and not OLD is mentioned.
			 */
			if (has_old || (has_new && stmt->event == CMD_UPDATE))
			{
				/*
				 * If sub_qry is a setop, manipulating its jointree will do no
				 * good at all, because the jointree is dummy. (This should be
				 * a can't-happen case because of prior tests.)
				 */
				if (sub_qry->setOperations != NULL)
					ereport(ERROR,
							(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							 errmsg("conditional UNION/INTERSECT/EXCEPT statements are not implemented")));
				/* hack so we can use addRTEtoQuery() */
				sub_pstate->p_rtable = sub_qry->rtable;
				sub_pstate->p_joinlist = sub_qry->jointree->fromlist;
				addRTEtoQuery(sub_pstate, oldrte, true, false, false);
				sub_qry->jointree->fromlist = sub_pstate->p_joinlist;
			}

			newactions = lappend(newactions, top_subqry);

			free_parsestate(sub_pstate);
		}

		*actions = newactions;
	}

	free_parsestate(pstate);

	/* Close relation, but keep the exclusive lock */
	heap_close(rel, NoLock);
}

#ifdef _PG_ORCL_
static List *
make_detach_partition_stmt(Relation root, List *relids)
{
	ListCell	*l;
	List		*at_cmds = NIL;

	foreach(l, relids)
	{
		Oid	part_relid;
		Oid	parent;
		Oid	parent_nspid;
		Oid	part_nspid;
		AlterTableStmt	*n;
		AlterTableCmd	*d;
		PartitionCmd	*cmd;
		RangeVar	*parent_rv;
		RangeVar	*part_rv;

		part_relid = lfirst_oid(l);
		parent = get_partition_parent(part_relid);
		parent_nspid = get_rel_namespace(parent);
		part_nspid = get_rel_namespace(part_relid);

		Assert(OidIsValid(parent_nspid) && OidIsValid(part_nspid));

		parent_rv = makeNode(RangeVar);
		parent_rv->schemaname = get_namespace_name(parent_nspid);
		parent_rv->relname = get_rel_name(parent);

		n = makeNode(AlterTableStmt);
		d = makeNode(AlterTableCmd);
		n->relkind = OBJECT_TABLE;
		n->missing_ok = false;
		n->relation = parent_rv;
		n->cmds = list_make1(d);

		cmd = makeNode(PartitionCmd);
		d->subtype = AT_DetachPartition;
		d->def = (Node *) cmd;

		part_rv = makeNode(RangeVar);
		part_rv->schemaname = get_namespace_name(part_nspid);
		part_rv->relname = get_rel_name(part_relid);

		Assert(part_rv->relname != NULL && part_rv->schemaname != NULL);
		cmd->name = part_rv;
		cmd->bound = NULL;

		/*
		 * When running this subcommond, should recurse into ProcessUtility.
		 */
		at_cmds = lappend(at_cmds, n);
	}

	return at_cmds;
}

static bool
partition_key_isequal(PartitionKey k1, PartitionKey k2)
{
#define PointerCMP(p1, p2, sz)	\
	do	{	\
		if ((p1) != NULL && (p2) != NULL)	\
		{	\
			if (memcmp(p1, p2, sz) != 0)	\
				return false;		\
		}	\
		else if (p1 != NULL || p2 != NULL)	\
			return false;	\
	} while (0)

	if (k1 == k2)
		return true;

	if (k1 == NULL || k2 == NULL)
		return false;

	if (k1->strategy != k2->strategy)
		return false;
	if (k1->partnatts != k2->partnatts)
		return false;

	PointerCMP(k1->partattrs, k2->partattrs, sizeof(AttrNumber) * k1->partnatts);
	if (!equal(k1->partexprs, k2->partexprs))
		return false;

	PointerCMP(k1->partcollation, k2->partcollation, sizeof(Oid) * k1->partnatts);
	PointerCMP(k1->parttypid, k2->parttypid, sizeof(Oid) * k1->partnatts);
	PointerCMP(k1->parttypmod, k2->parttypmod, sizeof(int32) * k1->partnatts);
	PointerCMP(k1->parttyplen, k2->parttyplen, sizeof(int16) * k1->partnatts);
	PointerCMP(k1->parttypbyval, k2->parttypbyval, sizeof(bool) * k1->partnatts);
	PointerCMP(k1->parttypalign, k2->parttypalign, sizeof(char) * k1->partnatts);
	PointerCMP(k1->parttypcoll, k2->parttypcoll, sizeof(Oid) * k1->partnatts);

	PointerCMP(k1->partopfamily, k2->partopfamily, sizeof(Oid) * k1->partnatts);
	PointerCMP(k1->partopcintype, k2->partopcintype, sizeof(Oid) * k1->partnatts);

	/* Above comparisons maybe enough */
	if (k1->partsupfunc != NULL && k2->partsupfunc != NULL)
	{
		if (k1->partsupfunc->fn_oid != k2->partsupfunc->fn_oid)
			return false;
	}
	else if(k1->partsupfunc != NULL || k2->partsupfunc != NULL)
		return false;

	return true;
}

/*
 * Check if the merged partition trees have the same partition specification at the
 * same level.
 */
static void
check_partition_spec(List *suboids)
{
	Relation	part_rel;
	ListCell	*l;
	PartitionKey	key;

	if (list_length(suboids) <= 1)
		return;

	part_rel = heap_open(linitial_oid(suboids), NoLock);
	key = RelationGetPartitionKey(part_rel);

	/*
	 * Keep the relation opened to check if the merged partition's
	 * specification.
	 */
	l = lnext(list_head(suboids));
	while (l != NULL)
	{
		Oid		rid = lfirst_oid(l);
		Relation	cur_rel;
		PartitionKey	cur_key;

		cur_rel = heap_open(rid, NoLock);
		cur_key = RelationGetPartitionKey(cur_rel);
		if (!partition_key_isequal(cur_key, key))
			elog(ERROR, "merged partitions should have the same partition specification");

		heap_close(cur_rel, NoLock);
		l = lnext(l);
	}

	heap_close(part_rel, NoLock);
}

/*
 * make_partition_createstmt
 *    Make a CREATE TABLE...PARTITION OF.. to create a partition or partitioned table.
 */
static List *
make_partition_createstmt(Relation rel, char *inhrel, PartitionMergeSpec *part_spc,
							PartitionBoundSpec *bound_spc, char *relname)
{
	RangeVar	*rv;
	CreateStmt	*createpart;
	char	*schname;

	createpart = makeNode(CreateStmt);
	schname = get_namespace_name(RelationGetNamespace(rel));

	createpart->relation = makeNode(RangeVar);
	createpart->relation->schemaname = schname;
	createpart->relation->relname = relname;
	createpart->relation->relpersistence = rel->rd_rel->relpersistence;
	createpart->tableElts = NULL;

	createpart->constraints = NULL;
	createpart->distributeby = NULL;
	createpart->subcluster = NULL;
	createpart->if_not_exists = false;

	rv = makeNode(RangeVar);
	rv->schemaname = schname;
	rv->relname = inhrel;
	rv->relpersistence = rel->rd_rel->relpersistence;
	createpart->inhRelations = list_make1(rv);
	createpart->ofTypename = NULL;
	createpart->oncommit = ONCOMMIT_NOOP;
	createpart->options = NULL;
	createpart->tablespacename = get_tablespace_name(rel->rd_rel->reltablespace);
	createpart->partspec = NULL;

	/* Merge partition */
	createpart->istransformed = true;
	createpart->merge_partspec = part_spc;
	createpart->partbound = bound_spc;

	return list_make1(createpart);
}

/*
 * Get merged partititons from `mbound'
 */
static List *
get_merged_subpartitions(List *mbound, List *part_bounds, List *suboids)
{
	ListCell	*l;
	List	*merge_rels = NIL;

	Assert(list_length(part_bounds) == list_length(suboids));
	foreach(l, mbound)
	{
		ListCell	*ll;
		int		i = 0;

		foreach(ll, part_bounds)
		{
			if (lfirst(l) == lfirst(ll))
			{
				merge_rels = lappend_oid(merge_rels, list_nth_oid(suboids, i));
				break;
			}
			i++;
		}

		if (ll == NULL)
			elog(ERROR, "not found merged partition");
	}

	Assert(list_length(merge_rels) == list_length(mbound));
	return merge_rels;
}

/*
 * make_merged_subpartition_stmt
 *     Create subpartition relations. It is similar as make_merged_partition_stmt(),
 *  except mering the overlapping PartitionBoundSpec.
 */
static List *
make_merged_subpartition_stmt(Relation rel, List *oids, int level,
								char *inhrel, int *seqrelid)
{
	PartitionMergeSpec	*part_spc;
	ListCell	*l;
	Relation	prel;
	List	*suboids = NIL;
	List	*part_bounds;
	List	*merged_bounds;
	List	*copied_bounds;
	Oid		parent_relid;

	List	*stmts = NIL;
	List	*copied_relids = NIL;

	foreach(l, oids)
	{
		Oid	relid = lfirst_oid(l);
		List	*subrelid;

		subrelid = find_inheritance_children(relid, AccessExclusiveLock);
		if (subrelid)
			suboids = list_concat(suboids, subrelid);
	}

	if (list_length(suboids) == 0)
		return NULL;

	/*
	 * Still check the lower subpartitions have the same partition specification.
	 * It is not critical restrictions, but can simplify the merge logic, and
	 * it is the same with opentenbase_ora.
	 */
	check_partition_spec(suboids);
	part_spc = get_partition_spec(linitial_oid(suboids));
	part_bounds = get_partitions_bound(suboids, true, &copied_relids);
	copied_bounds = list_copy(part_bounds);

	Assert(list_length(part_bounds) != 0);

	/* Merge overlapping part_bounds */
	parent_relid = get_partition_parent(linitial_oid(suboids));
	prel = heap_open(parent_relid, NoLock);
	merged_bounds = get_overlap_partition_bound(part_bounds,
												RelationGetPartitionKey(prel));

	Assert(list_length(merged_bounds) != 0);

	/* Each bound for creating one subpartition */
	foreach(l, merged_bounds)
	{
		PartitionBoundSpec	*bound_spc;
		List	*mbound = lfirst(l);
		char	prefix[16];
		char	*relname; /* Get relation name at `rel' namespace */
		List	*merge_subrels;

		*seqrelid = *seqrelid + 1;

		snprintf(prefix, sizeof(prefix), "p%d_%d", level, *seqrelid);
		relname = ChooseRelationName(get_rel_name(linitial_oid(suboids)), NULL,
													prefix,
													RelationGetNamespace(rel));

		bound_spc = merge_partition_bound(mbound, RelationGetPartitionKey(prel));
		stmts = list_concat(stmts,
							make_partition_createstmt(rel, inhrel, part_spc,
														bound_spc, relname));

		/* Deep down to create the child partition */
		merge_subrels = get_merged_subpartitions(mbound, copied_bounds, copied_relids);
		stmts = list_concat(stmts,
							make_merged_subpartition_stmt(rel, merge_subrels,
															level + 1, relname,
															seqrelid));
	}
	heap_close(prel, NoLock);

	return stmts;
}

/*
 * make_merged_partition_stmt
 *    oids: the root partition tree should be merged
 *    relname: the merged partition name
 * Combine the parition bounds of root partition into one and create the new partition.
 * Then recurse into child partitions to merge the overlap bounds as one child
 * subpartition of the newly create partition, until to the leaf partition.
 */
static List *
make_merged_partition_stmt(Relation rel, List *oids, char *relname)
{
	PartitionBoundSpec	*bound_spc;
	PartitionMergeSpec	*part_spc;
	Relation	partrel;
	List	*stmts = NIL;
	List	*part_bounds;
	Oid		parent_relid;
	int		seq_relid = 0;

	/*
	 * Subpartitions have the same partition specification or there is no way to
	 * merge them togather.
	 */
	check_partition_spec(oids);
	part_spc = get_partition_spec(linitial_oid(oids));
	part_bounds = get_partitions_bound(oids, true, NULL);

	parent_relid = get_partition_parent(linitial_oid(oids));
	partrel = heap_open(parent_relid, NoLock);
	bound_spc = merge_partition_bound(part_bounds, RelationGetPartitionKey(partrel));

	stmts = list_concat(stmts, make_partition_createstmt(rel,
						pstrdup(RelationGetRelationName(partrel)), part_spc,
						bound_spc, relname));
	heap_close(partrel, NoLock);

	/* Create the lower partitions */
	stmts = list_concat(stmts, make_merged_subpartition_stmt(rel, oids, 0,
											relname, &seq_relid));
	return stmts;
}

static AlterTableStmt *
make_move_parttuple_stmt(Relation rel, RangeVar *into_rv, List *from_rels)
{
	PartitionMergeSplit	*cmd;
	PartitionInto	*partinto;
	AlterTableStmt	*n;
	AlterTableCmd	*d;
	RangeVar	*rv;

	cmd = makeNode(PartitionMergeSplit);

	partinto = makeNode(PartitionInto);
	partinto->part_name = into_rv;

	rv = makeNode(RangeVar);
	rv->relname = pstrdup(RelationGetRelationName(rel));
	rv->schemaname = get_namespace_name(RelationGetNamespace(rel));

	n = makeNode(AlterTableStmt);
	d = makeNode(AlterTableCmd);
	n->relkind = OBJECT_TABLE;
	n->missing_ok = false;
	n->relation = rv;
	n->cmds = list_make1(d);

	d->subtype = AT_MergePartition;
	d->def = (Node *) cmd;

	cmd->into_parts = list_make1(partinto);
	cmd->is_merge = true;
	cmd->stp = MS_MoveData;
	cmd->from_rel = from_rels;

	return n;
}

/*
 * make_partition_dropstmt
 *    Make a list of DropStmt to remove the merge partition(s).
 */
static DropStmt *
make_partition_dropstmt(List *oids)
{
	ListCell	*l;
	DropStmt	*n;

	n = makeNode(DropStmt);
	n->removeType = OBJECT_TABLE;
	n->missing_ok = false;
	n->behavior = DROP_RESTRICT; /* careful */
	n->concurrent = false;
	foreach(l, oids)
	{
		Oid		rid = lfirst_oid(l);
		Oid		spcid;
		char	*relname;
		char	*spcname;
		List	*q_name;

		spcid = get_rel_namespace(rid);
		Assert(OidIsValid(spcid));
		relname = get_rel_name(rid);
		spcname = get_namespace_name(spcid);
		Assert(relname != NULL && spcname != NULL);

		q_name = list_make2(makeString(spcname), makeString(relname));
		n->objects = lappend(n->objects, q_name);
	}

	return n;
}

/*
 * Rename the merged partition to user supplied.
 */
static RenameStmt *
make_partition_renamestmt(Oid spcid, char *relname, char *to_name)
{
	RenameStmt	*n;
	RangeVar	*rv;

	n = makeNode(RenameStmt);
	n->renameType = OBJECT_TABLE;
	n->subname = NULL;
	n->missing_ok = false;

	rv = makeNode(RangeVar);
	rv->schemaname = get_namespace_name(spcid);
	Assert(rv->schemaname != NULL);
	rv->relname = relname;
	n->relation = rv;
	n->newname = to_name;

	return n;
}

/*
 * Check if the merged partitions are at the same partition level.
 */
static void
check_partition_level(Relation rel, List *relids)
{
	ListCell	*l;
	Oid		relid = linitial_oid(relids);
	List	*ancestors;
	int		level = 0;
	Oid		parent_oid;

	ancestors = get_partition_ancestors(relid);
	Assert(ancestors != NULL);
	if (!list_member_oid(ancestors, RelationGetRelid(rel)))
		elog(ERROR, "partition is not partition of table \"%s\"",
							RelationGetRelationName(rel));
	level = list_length(ancestors);
	parent_oid = linitial_oid(ancestors);

	l = lnext(list_head(relids));
	while (l != NULL)
	{
		relid = lfirst_oid(l);
		ancestors = get_partition_ancestors(relid);
		Assert(ancestors != NULL);
		if (level != list_length(ancestors)
					|| parent_oid != linitial_oid(ancestors))
			elog(ERROR, "the merged partitions are not at the same level");

		if (!list_member_oid(ancestors, RelationGetRelid(rel)))
			elog(ERROR, "partition is not partition of table \"%s\"",
							RelationGetRelationName(rel));
		l = lnext(l);
	}
}

/*
 * generate_partbound_from_list
 *    Get transformed values against partition key. If `error_dup' is true, error out
 *  when there are multiple values in 'list' for LIST partition strategy.
 */
static List *
generate_partbound_from_list(PartitionKey key, List *values,
								int len, List **remains)
{
	ListCell	*l;
	List	*listdatums = NIL;
	List	*rangebound = NIL;
	int		i = 0;

	if (remains)
		*remains = NIL;

	Assert(key->strategy != PARTITION_STRATEGY_RANGE || key->partnatts == len);
	Assert(len <= list_length(values));

	foreach(l, values)
	{
		A_Const    *con = castNode(A_Const, lfirst(l));
		Node	   *value;
		Oid		typeid;
		int		typmod;
		ParseState	*pstate;

		typeid = (key->strategy == PARTITION_STRATEGY_LIST) ? key->parttypid[0]:
					key->parttypid[i];
		typmod = (key->strategy == PARTITION_STRATEGY_LIST) ? key->parttypmod[0]:
					key->parttypmod[i];

		/* Make it into a Const */
		pstate = make_parsestate(NULL);
		value = (Node *) make_const(pstate, &con->val, con->location);

		/* Coerce to correct type */
		value = coerce_to_target_type(pstate, value, exprType(value),
									  typeid, typmod, COERCION_ASSIGNMENT,
									  COERCE_IMPLICIT_CAST, -1);

		if (value == NULL)
			ereport(ERROR,
					(errcode(ERRCODE_DATATYPE_MISMATCH),
					 errmsg("specified value cannot be cast to type %s",
							format_type_be(typeid))));

		/* Simplify the expression */
		if (!IsA(value, Const))
			value = (Node *) expression_planner((Expr *) value);

		/* Fail if we don't have a constant (i.e., non-immutable coercion) */
		if (!IsA(value, Const))
			ereport(ERROR,
					(errcode(ERRCODE_DATATYPE_MISMATCH),
					 errmsg("specified value cannot be cast to type %s",
							format_type_be(typeid)),
					 errdetail("The cast requires a non-immutable conversion.")));

		if (((Const *) value)->constisnull && key->strategy == PARTITION_STRATEGY_RANGE)
			ereport(ERROR,
							(errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
							 errmsg("cannot specify NULL in range bound")));

		if (key->strategy == PARTITION_STRATEGY_RANGE)
		{
			PartitionRangeDatum *n = makeNode(PartitionRangeDatum);

			n->kind = PARTITION_RANGE_DATUM_VALUE;
			n->value = value;
			n->location = -1;

			rangebound = lappend(rangebound, n);
		}
		else if (key->strategy == PARTITION_STRATEGY_LIST)
		{
			ListCell	*lc;

			foreach(lc, listdatums)
			{
				Const	*node = castNode(Const, lfirst(lc));

				if (node->constisnull)
				{
					if (((Const *) value)->constisnull)
						elog(ERROR, "multiple NULL found in partition values");

					continue;
				}

				if (((Const *) value)->constisnull)
					continue;

				/* Both are not null, compare actual datum */
				if (datumIsEqual(node->constvalue, ((Const *) value)->constvalue,
										key->parttypbyval[0], key->parttyplen[0]))
					break;
			}

			if (lc == NULL)
				listdatums = lappend(listdatums, value);
		}
		else
			elog(ERROR, "unsupported partition type to merge/split");

		i++;

		if (i == len)
			break;
	}

	if (remains)
	{
		l = lnext(l);
		while (l)
		{
			/* Put remains element into a sublist for next partition bound */
			*remains = lappend(*remains, lfirst(l));
			l = lnext(l);
		}
	}

	if (key->strategy == PARTITION_STRATEGY_RANGE)
		return rangebound;
	else
		return listdatums;
}

/*
 * transformSplitAt
 *    For range partition, the number of bound values should be equal to partition keys.
 *    For list partition, the any number of bound values is ok.
 */
static PartitionBoundSpec *
transformSplitAt(Oid relid, PartitionBoundSpec *bound)
{
	Oid		parentid;
	Relation	parentrel;
	PartitionKey	key;
	PartitionBoundSpec	*trans_bound;

	parentid = get_partition_parent(relid);
	Assert(parentid != InvalidOid);
	parentrel = heap_open(parentid, AccessExclusiveLock);

	key = RelationGetPartitionKey(parentrel);
	if (key->strategy != bound->strategy)
		elog(ERROR, "SPLIT type does not match splitted partition type");

	trans_bound = copyObject(bound);
	Assert(bound->strategy == key->strategy);
	if (key->strategy == PARTITION_STRATEGY_LIST)
	{
		trans_bound->listdatums
			= generate_partbound_from_list(key, bound->listdatums,
											list_length(bound->listdatums), NULL);
	}
	else if (key->strategy == PARTITION_STRATEGY_RANGE)
	{
		if (key->partnatts != list_length(bound->listdatums))
			elog(ERROR, "number of SPLIT point values is not equal to the number of partition keys");

		trans_bound->upperdatums = trans_bound->lowerdatums
					= generate_partbound_from_list(key, bound->listdatums,
													key->partnatts, NULL);
	}
	else
		elog(ERROR, "unsupported partition type to split/merge");

	heap_close(parentrel, NoLock);

	return trans_bound;
}

/*
 * Transform FOR... to qualify partitions.
 *
 * If ispartition is true, parsed PartitionBoundSpec for the 1st level partition.
 * Else parse PartitionBoundSpecs for the 1st and 2nd partitions, therefore return
 * a list.
 */
static List *
transformMergeSplitFor(Relation rel, PartitionBoundSpec *bound, bool ispartition)
{
	PartitionBoundSpec	*newbound;
	PartitionKey	key = RelationGetPartitionKey(rel);
	List	*values;
	List	*bounds = NIL;
	List	*inhrels;
	List	*sublist = NIL;
	Relation	subrel;

	if (key->partnatts > list_length(bound->listdatums) ||
			(ispartition && key->partnatts < list_length(bound->listdatums)))
		ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR),
				errmsg("FOR values is not equal number of partition keys"),
				errdetail("number of FOR list less than partition keys")));

	/* Always construnct the partition boundary for 1st partition level */
	values = generate_partbound_from_list(key, bound->listdatums, key->partnatts,
											&sublist);
	newbound = copyObject(bound);
	if (key->strategy == PARTITION_STRATEGY_RANGE)
		newbound->upperdatums = newbound->lowerdatums = values;
	else
		newbound->listdatums = values;

	newbound->strategy = key->strategy;
	bounds = lappend(bounds, newbound);

	if (ispartition)
		return bounds;

	/* Generate the 2nd partition bound */
	inhrels = find_inheritance_children(RelationGetRelid(rel),
											AccessExclusiveLock);
	if (list_length(inhrels) == 0 || list_length(sublist) == 0)
		ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR),
				errmsg("FOR values is not equal number of partition keys"),
				errdetail("zero subpartitions or less FOR values")));
	/*
	 * Assume partition specification styles are same at the 2nd level. If the
	 * qualified partitions are not the same partspec, check_partition_spec()
	 * has duty to check. Therefore pick up the partition specific of the first
	 * children.
	 */
	subrel = heap_open(linitial_oid(inhrels), AccessExclusiveLock);
	key = RelationGetPartitionKey(subrel);
	if (key->partnatts != list_length(sublist))
		elog(ERROR, "number of PARTITION keys is not equal to the number fo FOR values");

	values = generate_partbound_from_list(key, sublist, key->partnatts, NULL);

	newbound = copyObject(bound);
	newbound->strategy = key->strategy;
	if (key->strategy == PARTITION_STRATEGY_RANGE)
		newbound->upperdatums = newbound->lowerdatums = values;
	else
		newbound->listdatums = values;
	bounds = lappend(bounds, newbound);

	heap_close(subrel, NoLock);

	return bounds;
}

/*
 * partition_is_mergesplitted
 *    The split/merge partition only can be a PARTITION.
 */
static void
partition_is_mergesplitted(List *parts)
{
	ListCell	*l;

	foreach(l, parts)
	{
		Oid	split_part = lfirst_oid(l);
		Oid	parent_id;
		Relation	rel;
		PartitionKey	key;

		parent_id = get_partition_parent(split_part);
		if (!OidIsValid(parent_id))
			elog(ERROR, "can only split a partition or subpartition");

		/* Currently only support range/list partitioned table */
		rel = heap_open(parent_id, AccessExclusiveLock);
		key = RelationGetPartitionKey(rel);
		if (key == NULL || (key->strategy != PARTITION_STRATEGY_LIST &&
								key->strategy != PARTITION_STRATEGY_RANGE))
			elog(ERROR, "can only split range/list partition");

		heap_close(rel, NoLock);
	}
}

/*
 * transformMergePartition
 *    Merge partition will be transformed such subcommand:
 *    1. create table from PartitionInto if any, else create table with system
 *       defined name.
 *    2. alter table command (transformed PartitionMergeSplit) to move tuple
 *       from merged partition into newly created partition.
 *    3. Drop previous merged partition
 *    4. alter table command to rename newly create partition
 */
static List *
transformMergePartition(Relation main_rel, PartitionMergeSplit *merge_part)
{
	ListCell	*l;
	List		*merge_parts = NIL;
	Oid		main_nspid;
	List	*stmt = NIL;
	char	*relname;
	AlterTableStmt	*ats;

	main_nspid = RelationGetNamespace(main_rel);
	Assert(main_nspid != InvalidOid);
	foreach(l, merge_part->part_ext_names)
	{
		Node	*n = lfirst(l);

		if (IsA(n, RangeVar))
		{
			RangeVar	*rv = (RangeVar *) n;

			if (rv->schemaname == NULL)
			{
				/*
				 * Assume will each this partition under main relation's schema
				 * if any.
				 */
				rv->schemaname = get_namespace_name(main_nspid);
			}

			merge_parts
				= list_append_unique_oid(merge_parts,
											RangeVarGetRelid(rv,
															AccessExclusiveLock,
															false));
		}
		else
		{
			PartitionBoundSpec	*partspc;
			List	*new_partbound = NULL;
			bool	ispartition = merge_part->is_partition;
			Oid		qrelid;

			Assert(IsA(n, PartitionBoundSpec));
			partspc = (PartitionBoundSpec *) n;

			new_partbound = transformMergeSplitFor(main_rel, partspc, ispartition);
			qrelid = get_relid_from_partbound(main_rel, new_partbound,
												AccessExclusiveLock, ispartition);

			if (OidIsValid(qrelid))
				merge_parts = list_append_unique_oid(merge_parts, qrelid);
		}
	}

	if (list_length(merge_parts) <= 1)
		elog(ERROR, "merged partitions should be greater than 1");

	/*
	 * Check if the merged partitions are at the same level or list/range
	 * partition.
	 */
	check_partition_level(main_rel, merge_parts);
	partition_is_mergesplitted(merge_parts);

	/* 0. detach merge partitions from partition tree */
	stmt = list_concat(stmt, make_detach_partition_stmt(main_rel, merge_parts));

	/* 1. create newly added partition */
	relname = ChooseRelationName(get_rel_name(linitial_oid(merge_parts)),
												NULL, "m", main_nspid);
	stmt = list_concat(stmt, make_merged_partition_stmt(main_rel, merge_parts,
														relname));

	/* 2. make a alter table command to move tuple */
	ats = make_move_parttuple_stmt(main_rel,
									makeRangeVar(get_namespace_name(main_nspid),
													relname, -1), merge_parts);
	stmt = lappend(stmt, ats);

	/* 3. drop the merged partition */
	stmt = lappend(stmt, make_partition_dropstmt(merge_parts));

	/* 4. make a rename statement to rename newly added partition */
	if (merge_part->into_parts)
	{
		RangeVar	*rv;

		Assert(list_length(merge_part->into_parts) == 1);
		rv = (linitial_node(PartitionInto , merge_part->into_parts))->part_name;

		/*
		 * Will keep the system defined relation name if no INTO PARTITION.
		 */
		stmt = lappend(stmt, make_partition_renamestmt(main_nspid, relname,
														rv->relname));
	}

	return stmt;
}

/*
 * make_splited_subpartition_stmt
 *   Copy the subpartition tree of splitted partition `splitrel' to create the
 * same structure under newly created partition.
 */
static List *
make_splited_subpartition_stmt(Relation rel, Oid splitrel, int level,
										char *inrel, int *seqrelid)
{
	List	*subrelid;
	List	*stmts = NIL;
	ListCell	*l;

	subrelid = find_inheritance_children(splitrel, AccessExclusiveLock);
	foreach(l, subrelid)
	{
		Oid		relid = lfirst_oid(l);
		PartitionBoundSpec	*part_bound;
		PartitionMergeSpec	*part_spc;
		List	*list;
		char	prefix[16];
		char	*relname; /* Get relation name at `rel' namespace */

		/*
		 * Subpartitions of splitted partition can have different partition
		 * specifications and no need check like MERGE partition scenario.
		 */
		part_spc = get_partition_spec(relid);
		list = get_partitions_bound(list_make1_oid(relid), true, NULL);
		part_bound = linitial(list);

		/* Create the root relation of current subpartition tree */
		*seqrelid = *seqrelid + 1;

		snprintf(prefix, sizeof(prefix), "p%d_%d", level, *seqrelid);
		relname = ChooseRelationName(get_rel_name(relid),
										NULL, prefix, RelationGetNamespace(rel));

		stmts = list_concat(stmts,
							make_partition_createstmt(rel, inrel, part_spc,
														part_bound, relname));

		/* Recurse down for child subpartitions */
		stmts = list_concat(stmts,
							make_splited_subpartition_stmt(rel, relid, level + 1,
															relname, seqrelid));
	}

	return stmts;
}

/*
 * Create split partitions and their child partitions if any.
 */
static List *
make_splited_partition_stmt(Relation main_rel, Oid one_rel, char *relname1,
								char *relname2, PartitionBoundSpec *bound)
{
	PartitionMergeSpec	*part_spc;
	Relation	parent;
	Oid		parentid;
	List	*stmts = NIL;
	List	*tmp_stmt;
	char	*pname;
	List	*bounds;
	int		seq_relid = 0;

	parentid = get_partition_parent(one_rel);
	Assert(parentid != InvalidOid);

	parent = heap_open(parentid, AccessExclusiveLock);
	bounds = split_partition_bound(main_rel, parent, one_rel, bound);
	part_spc = get_partition_spec(one_rel);

	/* Create the lower partitions for the first splitting partition */
	pname = pstrdup(RelationGetRelationName(parent));
	tmp_stmt = make_partition_createstmt(main_rel, pname, part_spc,
											linitial(bounds), relname1);
	stmts = list_concat(stmts, tmp_stmt);
	stmts = list_concat(stmts,
						make_splited_subpartition_stmt(main_rel, one_rel, 0,
														relname1, &seq_relid));

	/* Create the lower partitions for the second splitting partition */
	pname = pstrdup(RelationGetRelationName(parent));
	tmp_stmt = make_partition_createstmt(main_rel, pname, part_spc,
											lsecond(bounds), relname2);
	stmts = list_concat(stmts, tmp_stmt);
	stmts = list_concat(stmts,
						make_splited_subpartition_stmt(main_rel, one_rel, 0,
														relname2, &seq_relid));
	heap_close(parent, NoLock);

	return stmts;
}

/*
 * transformSplitPartition
 *    Like transformMergePartition, but transform SPLIT PARTITION:
 */
static List *
transformSplitPartition(Relation main_rel, PartitionMergeSplit *split_part)
{
	Node	*n;
	List	*stmt = NIL;
	PartitionBoundSpec	*trans_bound;
	AlterTableStmt	*ats;
	Oid		main_nspid;
	Oid		one_rel;
	char	*relname1;
	char	*relname2;
	Oid		parentid;
	Oid		parentnspid;
	List	*ancestors;
	RangeVar	*into_rv;

	/* Qualify a splitted partition */
	if (list_length(split_part->part_ext_names) != 1)
		elog(ERROR, "can not split more than one partitions");

	main_nspid = RelationGetNamespace(main_rel),
	n = linitial(split_part->part_ext_names);
	if (IsA(n, RangeVar))
	{
		RangeVar	*rv = (RangeVar *) n;

		if (rv->schemaname == NULL)
		{
			/*
			 * Assume will each this partition under main relation's schema
			 * if any.
			 */
			rv->schemaname = get_namespace_name(main_nspid);
		}

		one_rel = RangeVarGetRelid(rv, AccessExclusiveLock, false);
	}
	else
	{
		PartitionBoundSpec	*partspc;
		List	*new_partbound = NULL;
		bool	ispartition = split_part->is_partition;

		Assert(IsA(n, PartitionBoundSpec));
		partspc = (PartitionBoundSpec *) n;

		new_partbound = transformMergeSplitFor(main_rel, partspc, ispartition);
		one_rel = get_relid_from_partbound(main_rel, new_partbound,
											AccessExclusiveLock, ispartition);
	}

	if (one_rel == InvalidOid)
		ereport(ERROR,
			(errcode(ERRCODE_UNDEFINED_TABLE),
				errmsg("split partition relation does not exist")));

	ancestors = get_partition_ancestors(one_rel);
	Assert(ancestors != NULL);
	if (!list_member_oid(ancestors, RelationGetRelid(main_rel)))
		elog(ERROR, "partition is not partition of table \"%s\"",
							RelationGetRelationName(main_rel));

	/* Generate many statements to rewrite partition definition. */
	partition_is_mergesplitted(list_make1_oid(one_rel));

	/* 0. detach the split partition */
	stmt = list_concat(stmt,
						make_detach_partition_stmt(main_rel,
													list_make1_oid(one_rel)));

	/* 1. create newly split partitions */
	relname1 = ChooseRelationName(get_rel_name(one_rel), NULL, "a", main_nspid);
	relname2 = ChooseRelationName(get_rel_name(one_rel), NULL, "b", main_nspid);

	/* Transform partition bound split point against `one_rel' */
	trans_bound = transformSplitAt(one_rel, split_part->bound);
	stmt = list_concat(stmt, make_splited_partition_stmt(main_rel, one_rel,
														relname1, relname2,
														trans_bound));
	/*
	 * 2. make a alter table command to move tuple into parent table of split
	 * partitions
	 */
	parentid = get_partition_parent(one_rel);
	Assert(parentid != InvalidOid);
	parentnspid = get_rel_namespace(parentid);
	Assert(parentnspid != InvalidOid);

	into_rv = makeRangeVar(get_namespace_name(parentnspid),
												get_rel_name(parentid), -1);
	ats = make_move_parttuple_stmt(main_rel, into_rv, list_make1_oid(one_rel));
	stmt = lappend(stmt, ats);

	/* 3. drop the splitted partition */
	stmt = lappend(stmt, make_partition_dropstmt(list_make1_oid(one_rel)));

	/* 4. make a rename statement to rename newly added partition */
	if (split_part->into_parts)
	{
		PartitionInto	*pinto;
		RangeVar	*rv;

		Assert(list_length(split_part->into_parts) == 2);

		pinto = linitial(split_part->into_parts);
		rv = pinto->part_name;
		stmt = lappend(stmt, make_partition_renamestmt(main_nspid, relname1,
											rv->relname));

		pinto = lsecond(split_part->into_parts);
		rv = pinto->part_name;
		stmt = lappend(stmt, make_partition_renamestmt(main_nspid, relname2,
											rv->relname));
	}

	return stmt;
}
#endif

static void
CheckLongTypeConstraintCore(TupleDesc rd_att, ColumnDef *column, AttrNumber alter_attidx,
                            AttrNumber attidx, Oid atttypid)
{
	Oid typeOid = InvalidOid;
	if (column->typeName &&
	    (atttypid == LRAWOID || atttypid == LONGOID))
	{
		typeOid = LookupTypeNameOid(NULL, column->typeName, true);
		if (typeOid == LRAWOID || typeOid == LONGOID)
		{
			if (InvalidAttrNumber == alter_attidx || alter_attidx != attidx)
				elog(ERROR, "Only one \"long\" type is allowed in a table");
			if (HasNonNullConstraint(column))
				elog(ERROR, "\"long\" type can not have non null constraint");
		}
	}
}

static void
CheckLongTypeConstraint(Relation rel, ColumnDef *column, AttrNumber alter_attidx)
{
	TupleDesc  rd_att = NULL;
	AttrNumber attnum = 0;
	AttrNumber attidx = InvalidAttrNumber;
	Oid        atttypid = InvalidOid;
	int        i = 0;

	rd_att = rel->rd_att;
	if (rd_att == NULL)
		return;

	attnum = rd_att->natts;
	for (i = 0; i < attnum; i++)
	{
		atttypid = rd_att->attrs[i].atttypid;
		attidx = rd_att->attrs[i].attnum;
		CheckLongTypeConstraintCore(rd_att, column, alter_attidx, attidx, atttypid);
	}
}

/*
 * transformAlterTableStmt -
 *		parse analysis for ALTER TABLE
 *
 * Returns a List of utility commands to be done in sequence.  One of these
 * will be the transformed AlterTableStmt, but there may be additional actions
 * to be done before and after the actual AlterTable() call.
 *
 * To avoid race conditions, it's important that this function rely only on
 * the passed-in relid (and not on stmt->relation) to determine the target
 * relation.
 */
List *
transformAlterTableStmt(Oid relid, AlterTableStmt *stmt,
						const char *queryString)
{
	Relation	rel;
	ParseState *pstate;
	CreateStmtContext cxt = {0};
	List	   *result;
	List	   *save_alist;
	ListCell   *lcmd,
			   *l;
	List	   *newcmds = NIL;
	bool		skipValidation = true;
	AlterTableCmd *newcmd;
	RangeTblEntry *rte;
	bool bfile_exist = false;
#ifdef __OPENTENBASE__
	List *createlist = NULL;
#endif
	/*
	 * We must not scribble on the passed-in AlterTableStmt, so copy it. (This
	 * is overkill, but easy.)
	 */
	stmt = copyObject(stmt);

	/* Caller is responsible for locking the relation */
	rel = relation_open(relid, NoLock);

	/* Set up pstate */
	pstate = make_parsestate(NULL);
	pstate->p_sourcetext = queryString;
	rte = addRangeTableEntryForRelation(pstate,
										rel,
										NULL,
										false,
										true);
	addRTEtoQuery(pstate, rte, false, true, true);

	/* Set up CreateStmtContext */
	cxt.pstate = pstate;
	if (stmt->relkind == OBJECT_FOREIGN_TABLE)
	{
		cxt.stmtType = "ALTER FOREIGN TABLE";
		cxt.isforeign = true;
	}
	else
	{
		cxt.stmtType = "ALTER TABLE";
		cxt.isforeign = false;
	}
	cxt.relation = stmt->relation;
	cxt.rel = rel;
	cxt.inhRelations = NIL;
	cxt.isalter = true;
	cxt.hasoids = false;		/* need not be right */
	cxt.columns = NIL;
	cxt.ckconstraints = NIL;
	cxt.fkconstraints = NIL;
	cxt.ixconstraints = NIL;
	cxt.clusterconstraints = NIL;
	cxt.inh_indexes = NIL;
	cxt.blist = NIL;
	cxt.alist = NIL;
	cxt.pkey = NULL;
#ifdef PGXC
	cxt.fallback_source = FBS_NONE;
	cxt.fallback_dist_cols = NIL;
	cxt.distributeby = NULL;
	cxt.subcluster = NULL;
#endif
	cxt.ispartitioned = (rel->rd_rel->relkind == RELKIND_PARTITIONED_TABLE);
	cxt.partbound = NULL;
	cxt.ofType = false;

	/*
	 * The only subtypes that currently require parse transformation handling
	 * are ADD COLUMN, ADD CONSTRAINT and SET DATA TYPE.  These largely re-use
	 * code from CREATE TABLE.
	 */
	foreach(lcmd, stmt->cmds)
	{
		AlterTableCmd *cmd = (AlterTableCmd *) lfirst(lcmd);

		switch (cmd->subtype)
		{
			case AT_AddColumn:
			case AT_AddColumnToView:
				{
					ColumnDef  *def = castNode(ColumnDef, cmd->def);
					Oid			typeOid = InvalidOid;

					if (def->typeName)
					{
						typeOid = LookupTypeNameOid(pstate, def->typeName, true);

						if (typeOid == BFILEOID)
							bfile_exist = true;
					}

					if (ORA_MODE)
						CheckLongTypeConstraint(rel, def, InvalidAttrNumber);
					transformColumnDefinition(&cxt, def);
					/*
					 * If the column has a non-null default, we can't skip
					 * validation of foreign keys.
					 */
					if (def->raw_default != NULL)
						skipValidation = false;

					/*
					 * All constraints are processed in other ways. Remove the
					 * original list
					 */
					def->constraints = NIL;

					newcmds = lappend(newcmds, cmd);
					break;
				}

			case AT_AddConstraint:

				/*
				 * The original AddConstraint cmd node doesn't go to newcmds
				 */
				if (IsA(cmd->def, Constraint))
				{
					transformTableConstraint(&cxt, (Constraint *) cmd->def);
					if (((Constraint *) cmd->def)->contype == CONSTR_FOREIGN)
					{
						skipValidation = false;
					}
				}
				else
					elog(ERROR, "unrecognized node type: %d",
						 (int) nodeTag(cmd->def));
				break;

			case AT_ProcessedConstraint:

				/*
				 * Already-transformed ADD CONSTRAINT, so just make it look
				 * like the standard case.
				 */
				cmd->subtype = AT_AddConstraint;
				newcmds = lappend(newcmds, cmd);
				break;

#ifdef _PG_ORCL_
			/* Merge partition */
			case AT_MergePartition:
				{
					PartitionMergeSplit	*n = (PartitionMergeSplit *) cmd->def;

					newcmds = lappend(newcmds, cmd);
					if (n->stp == MS_RAW)
					{
						List	*stmts;
						PartitionKey	key;

						if (rel->rd_rel->relkind != RELKIND_PARTITIONED_TABLE)
							ereport(ERROR,
										(errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
										 errmsg("table \"%s\" is not partitioned",
												RelationGetRelationName(rel))));

						key = RelationGetPartitionKey(rel);
						Assert(key != NULL);
						if (key->strategy != PARTITION_STRATEGY_RANGE &&
												key->strategy != PARTITION_STRATEGY_LIST)
							ereport(ERROR,
										(errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
										 errmsg("only can merge list/range partitioned")));
						if (n->is_merge)
							stmts = transformMergePartition(rel, n);
						else
							stmts = transformSplitPartition(rel, n);

						if (stmts != NULL)
							createlist  = list_concat(createlist, stmts);
					}
					break;
				}
#endif

			case AT_AlterColumnType:
				{
					ColumnDef  *def = (ColumnDef *) cmd->def;
					AttrNumber	attnum;
					Oid			typeOid = InvalidOid;

					if (def->typeName)
					{
						typeOid = LookupTypeNameOid(pstate, def->typeName, true);

						if (typeOid == BFILEOID)
							bfile_exist = true;
					}

					/*
					 * For ALTER COLUMN TYPE, transform the USING clause if
					 * one was specified.
					 */
					if (def->raw_default)
					{
						def->cooked_default =
							transformExpr(pstate, def->raw_default,
										  EXPR_KIND_ALTER_COL_TRANSFORM);
					}

					/*
					 * For identity column, create ALTER SEQUENCE command to
					 * change the data type of the sequence.
					 */
					attnum = get_attnum(relid, cmd->name);

					if (ORA_MODE)
					{

						CheckLongTypeConstraint(rel, def, attnum);
					}

					/*
					 * if attribute not found, something will error about it
					 * later
					 */
					if (attnum != InvalidAttrNumber && get_attidentity(relid, attnum))
					{
						Oid			seq_relid = getOwnedSequence(relid, attnum);
						Oid			typeOid = typenameTypeId(pstate, def->typeName);
						AlterSeqStmt *altseqstmt = makeNode(AlterSeqStmt);

						altseqstmt->sequence = makeRangeVar(get_namespace_name(get_rel_namespace(seq_relid)),
															get_rel_name(seq_relid),
															-1);
						altseqstmt->options = list_make1(makeDefElem("as", (Node *) makeTypeNameFromOid(typeOid, -1), -1));
						altseqstmt->for_identity = true;
						cxt.blist = lappend(cxt.blist, altseqstmt);
					}

					newcmds = lappend(newcmds, cmd);
					break;
				}

			case AT_AddIdentity:
				{
					Constraint *def = castNode(Constraint, cmd->def);
					ColumnDef  *newdef = makeNode(ColumnDef);
					AttrNumber	attnum;

					newdef->colname = cmd->name;
					newdef->identity = def->generated_when;
					cmd->def = (Node *) newdef;

					attnum = get_attnum(relid, cmd->name);

					/*
					 * if attribute not found, something will error about it
					 * later
					 */
					if (attnum != InvalidAttrNumber)
						generateSerialExtraStmts(&cxt, newdef,
												 get_atttype(relid, attnum),
												 def->options, true,
												 NULL, NULL);

					newcmds = lappend(newcmds, cmd);
					break;
				}

			case AT_SetIdentity:
				{
					/*
					 * Create an ALTER SEQUENCE statement for the internal
					 * sequence of the identity column.
					 */
					ListCell   *lc;
					List	   *newseqopts = NIL;
					List	   *newdef = NIL;
					List	   *seqlist;
					AttrNumber	attnum;

					/*
					 * Split options into those handled by ALTER SEQUENCE and
					 * those for ALTER TABLE proper.
					 */
					foreach(lc, castNode(List, cmd->def))
					{
						DefElem    *def = lfirst_node(DefElem, lc);

						if (strcmp(def->defname, "generated") == 0)
							newdef = lappend(newdef, def);
						else
							newseqopts = lappend(newseqopts, def);
					}

					attnum = get_attnum(relid, cmd->name);

					if (attnum)
					{
						seqlist = getOwnedSequences(relid, attnum);
						if (seqlist)
						{
							AlterSeqStmt *seqstmt;
							Oid			seq_relid;

							seqstmt = makeNode(AlterSeqStmt);
							seq_relid = linitial_oid(seqlist);
							seqstmt->sequence = makeRangeVar(get_namespace_name(get_rel_namespace(seq_relid)),
															 get_rel_name(seq_relid), -1);
							seqstmt->options = newseqopts;
							seqstmt->for_identity = true;
							seqstmt->missing_ok = false;

							cxt.alist = lappend(cxt.alist, seqstmt);
						}
					}

					/*
					 * If column was not found or was not an identity column,
					 * we just let the ALTER TABLE command error out later.
					 */

					cmd->def = (Node *) newdef;
					newcmds = lappend(newcmds, cmd);
					break;
				}

			case AT_AttachPartition:
			case AT_DetachPartition:
				{
					PartitionCmd *partcmd = (PartitionCmd *) cmd->def;

					transformPartitionCmd(&cxt, partcmd);
					/* assign transformed value of the partition bound */
					partcmd->bound = cxt.partbound;
				}

				newcmds = lappend(newcmds, cmd);
				break;
			default:
				{
					CheckLongTypeSetDefault(relid, cmd);
					newcmds = lappend(newcmds, cmd);
					break;
				}
		}
	}

	if (IS_PGXC_COORDINATOR && bfile_exist)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
				 errmsg("Distributed table do not support BFILE data type.")));

	/*
	 * transformIndexConstraints wants cxt.alist to contain only index
	 * statements, so transfer anything we already have into save_alist
	 * immediately.
	 */
	save_alist = cxt.alist;
	cxt.alist = NIL;

	/* Postprocess constraints */
	transformIndexConstraints(&cxt);
	transformFKConstraints(&cxt, skipValidation, true);
	transformCheckConstraints(&cxt, false);
	transformPartialClusterConstraints(&cxt);

	/*
	 * Push any index-creation commands into the ALTER, so that they can be
	 * scheduled nicely by tablecmds.c.  Note that tablecmds.c assumes that
	 * the IndexStmt attached to an AT_AddIndex or AT_AddIndexConstraint
	 * subcommand has already been through transformIndexStmt.
	 */
	foreach(l, cxt.alist)
	{
		IndexStmt  *idxstmt = lfirst_node(IndexStmt, l);

		idxstmt = transformIndexStmt(relid, idxstmt, queryString);
		newcmd = makeNode(AlterTableCmd);
		newcmd->subtype = OidIsValid(idxstmt->indexOid) ? AT_AddIndexConstraint : AT_AddIndex;
		newcmd->def = (Node *) idxstmt;
		newcmds = lappend(newcmds, newcmd);
	}
	cxt.alist = NIL;

	/* Append any CHECK or FK constraints to the commands list */
	foreach(l, cxt.ckconstraints)
	{
		newcmd = makeNode(AlterTableCmd);
		newcmd->subtype = AT_AddConstraint;
		newcmd->def = (Node *) lfirst(l);
		newcmds = lappend(newcmds, newcmd);
	}
	foreach(l, cxt.fkconstraints)
	{
		newcmd = makeNode(AlterTableCmd);
		newcmd->subtype = AT_AddConstraint;
		newcmd->def = (Node *) lfirst(l);
		newcmds = lappend(newcmds, newcmd);
	}
	foreach(l, cxt.clusterconstraints)
	{
		newcmd = makeNode(AlterTableCmd);
		newcmd->subtype = AT_AddConstraint;
		newcmd->def = (Node *) lfirst(l);
		newcmds = lappend(newcmds, newcmd);
	}

	/* Close rel */
	relation_close(rel, NoLock);

	/*
	 * Output results.
	 */
	stmt->cmds = newcmds;

	result = lappend(cxt.blist, stmt);
#ifdef __OPENTENBASE__
	if(createlist)
		result = list_concat(result, createlist);
#endif
	result = list_concat(result, cxt.alist);
	result = list_concat(result, save_alist);

	return result;
}


/*
 * Preprocess a list of column constraint clauses
 * to attach constraint attributes to their primary constraint nodes
 * and detect inconsistent/misplaced constraint attributes.
 *
 * NOTE: currently, attributes are only supported for FOREIGN KEY, UNIQUE,
 * EXCLUSION, and PRIMARY KEY constraints, but someday they ought to be
 * supported for other constraint types.
 */
static void
transformConstraintAttrs(CreateStmtContext *cxt, List *constraintList)
{
	Constraint *lastprimarycon = NULL;
	bool		saw_deferrability = false;
	bool		saw_initially = false;
	ListCell   *clist;

#define SUPPORTS_ATTRS(node)				\
	((node) != NULL &&						\
	 ((node)->contype == CONSTR_PRIMARY ||	\
	  (node)->contype == CONSTR_UNIQUE ||	\
	  (node)->contype == CONSTR_EXCLUSION || \
	  (node)->contype == CONSTR_FOREIGN))

	foreach(clist, constraintList)
	{
		Constraint *con = (Constraint *) lfirst(clist);

		if (!IsA(con, Constraint))
			elog(ERROR, "unrecognized node type: %d",
				 (int) nodeTag(con));
		switch (con->contype)
		{
			case CONSTR_ATTR_DEFERRABLE:
				if (!SUPPORTS_ATTRS(lastprimarycon))
					ereport(ERROR,
							(errcode(ERRCODE_SYNTAX_ERROR),
							 errmsg("misplaced DEFERRABLE clause"),
							 parser_errposition(cxt->pstate, con->location)));
				if (saw_deferrability)
					ereport(ERROR,
							(errcode(ERRCODE_SYNTAX_ERROR),
							 errmsg("multiple DEFERRABLE/NOT DEFERRABLE clauses not allowed"),
							 parser_errposition(cxt->pstate, con->location)));
				saw_deferrability = true;
				lastprimarycon->deferrable = true;
				break;

			case CONSTR_ATTR_NOT_DEFERRABLE:
				if (!SUPPORTS_ATTRS(lastprimarycon))
					ereport(ERROR,
							(errcode(ERRCODE_SYNTAX_ERROR),
							 errmsg("misplaced NOT DEFERRABLE clause"),
							 parser_errposition(cxt->pstate, con->location)));
				if (saw_deferrability)
					ereport(ERROR,
							(errcode(ERRCODE_SYNTAX_ERROR),
							 errmsg("multiple DEFERRABLE/NOT DEFERRABLE clauses not allowed"),
							 parser_errposition(cxt->pstate, con->location)));
				saw_deferrability = true;
				lastprimarycon->deferrable = false;
				if (saw_initially &&
					lastprimarycon->initdeferred)
					ereport(ERROR,
							(errcode(ERRCODE_SYNTAX_ERROR),
							 errmsg("constraint declared INITIALLY DEFERRED must be DEFERRABLE"),
							 parser_errposition(cxt->pstate, con->location)));
				break;

			case CONSTR_ATTR_DEFERRED:
				if (!SUPPORTS_ATTRS(lastprimarycon))
					ereport(ERROR,
							(errcode(ERRCODE_SYNTAX_ERROR),
							 errmsg("misplaced INITIALLY DEFERRED clause"),
							 parser_errposition(cxt->pstate, con->location)));
				if (saw_initially)
					ereport(ERROR,
							(errcode(ERRCODE_SYNTAX_ERROR),
							 errmsg("multiple INITIALLY IMMEDIATE/DEFERRED clauses not allowed"),
							 parser_errposition(cxt->pstate, con->location)));
				saw_initially = true;
				lastprimarycon->initdeferred = true;

				/*
				 * If only INITIALLY DEFERRED appears, assume DEFERRABLE
				 */
				if (!saw_deferrability)
					lastprimarycon->deferrable = true;
				else if (!lastprimarycon->deferrable)
					ereport(ERROR,
							(errcode(ERRCODE_SYNTAX_ERROR),
							 errmsg("constraint declared INITIALLY DEFERRED must be DEFERRABLE"),
							 parser_errposition(cxt->pstate, con->location)));
				break;

			case CONSTR_ATTR_IMMEDIATE:
				if (!SUPPORTS_ATTRS(lastprimarycon))
					ereport(ERROR,
							(errcode(ERRCODE_SYNTAX_ERROR),
							 errmsg("misplaced INITIALLY IMMEDIATE clause"),
							 parser_errposition(cxt->pstate, con->location)));
				if (saw_initially)
					ereport(ERROR,
							(errcode(ERRCODE_SYNTAX_ERROR),
							 errmsg("multiple INITIALLY IMMEDIATE/DEFERRED clauses not allowed"),
							 parser_errposition(cxt->pstate, con->location)));
				saw_initially = true;
				lastprimarycon->initdeferred = false;
				break;

			default:
				/* Otherwise it's not an attribute */
				lastprimarycon = con;
				/* reset flags for new primary node */
				saw_deferrability = false;
				saw_initially = false;
				break;
		}
	}
}

/*
 * Special handling of type definition for a column
 */
static void
transformColumnType(CreateStmtContext *cxt, ColumnDef *column)
{
	/*
	 * All we really need to do here is verify that the type is valid,
	 * including any collation spec that might be present.
	 */
	Type		ctype = typenameType(cxt->pstate, column->typeName, NULL);

	if (cxt->pstate && cxt->pstate->isHudiColumnTypeReset)
	{
		column->typeName->typmods = NIL; 	//date type no typmods
		cxt->pstate->isHudiColumnTypeReset = false;
	}

	if (column->collClause)
	{
		Form_pg_type typtup = (Form_pg_type) GETSTRUCT(ctype);

		LookupCollation(cxt->pstate,
						column->collClause->collname,
						column->collClause->location);
		/* Complain if COLLATE is applied to an uncollatable type */
		if (!OidIsValid(typtup->typcollation))
			ereport(ERROR,
					(errcode(ERRCODE_DATATYPE_MISMATCH),
					 errmsg("collations are not supported by type %s",
							format_type_be(HeapTupleGetOid(ctype))),
					 parser_errposition(cxt->pstate,
										column->collClause->location)));
	}
#ifdef XCP
	/*
	 * If the distribution is not defined yet by a priority source add it to the
	 * list of possible fallbacks
	 */
#ifdef __OPENTENBASE__
	/* we need distribution info on both coordinator and datanode */
	if (IsPostmasterEnvironment && cxt->distributeby == NULL && !cxt->isalter &&
			cxt->fallback_source <= FBS_COLDEF &&
			IsTypeHashDistributable(HeapTupleGetOid(ctype)))
#else
	if (IS_PGXC_COORDINATOR && cxt->distributeby == NULL && !cxt->isalter &&
			cxt->fallback_source <= FBS_COLDEF &&
			IsTypeHashDistributable(HeapTupleGetOid(ctype)))
#endif
	{
#ifdef __FDW__
        if (!cxt->isforeign)
        {
    		cxt->fallback_dist_cols = lappend(cxt->fallback_dist_cols,
    										  pstrdup(column->colname));
    		cxt->fallback_source = FBS_COLDEF;        
        }
#else
		cxt->fallback_dist_cols = lappend(cxt->fallback_dist_cols,
										  pstrdup(column->colname));
		cxt->fallback_source = FBS_COLDEF;
#endif		
	}
#endif

	ReleaseSysCache(ctype);
}


/*
 * transformCreateSchemaStmt -
 *	  analyzes the CREATE SCHEMA statement
 *
 * Split the schema element list into individual commands and place
 * them in the result list in an order such that there are no forward
 * references (e.g. GRANT to a table created later in the list). Note
 * that the logic we use for determining forward references is
 * presently quite incomplete.
 *
 * SQL also allows constraints to make forward references, so thumb through
 * the table columns and move forward references to a posterior alter-table
 * command.
 *
 * The result is a list of parse nodes that still need to be analyzed ---
 * but we can't analyze the later commands until we've executed the earlier
 * ones, because of possible inter-object references.
 *
 * Note: this breaks the rules a little bit by modifying schema-name fields
 * within passed-in structs.  However, the transformation would be the same
 * if done over, so it should be all right to scribble on the input to this
 * extent.
 */
List *
transformCreateSchemaStmt(CreateSchemaStmt *stmt)
{
	CreateSchemaStmtContext cxt;
	List	   *result;
	ListCell   *elements;

	cxt.stmtType = "CREATE SCHEMA";
	cxt.schemaname = stmt->schemaname;
	cxt.authrole = (RoleSpec *) stmt->authrole;
	cxt.sequences = NIL;
	cxt.tables = NIL;
	cxt.views = NIL;
	cxt.indexes = NIL;
	cxt.triggers = NIL;
	cxt.grants = NIL;

	/*
	 * Run through each schema element in the schema element list. Separate
	 * statements by type, and do preliminary analysis.
	 */
	foreach(elements, stmt->schemaElts)
	{
		Node	   *element = lfirst(elements);

		switch (nodeTag(element))
		{
			case T_CreateSeqStmt:
				{
					CreateSeqStmt *elp = (CreateSeqStmt *) element;

					setSchemaName(cxt.schemaname, &elp->sequence->schemaname);
					cxt.sequences = lappend(cxt.sequences, element);
				}
				break;

			case T_CreateStmt:
				{
					CreateStmt *elp = (CreateStmt *) element;

					setSchemaName(cxt.schemaname, &elp->relation->schemaname);

					/*
					 * XXX todo: deal with constraints
					 */
					cxt.tables = lappend(cxt.tables, element);
				}
				break;

			case T_ViewStmt:
				{
					ViewStmt   *elp = (ViewStmt *) element;

					setSchemaName(cxt.schemaname, &elp->view->schemaname);

					/*
					 * XXX todo: deal with references between views
					 */
					cxt.views = lappend(cxt.views, element);
				}
				break;

			case T_IndexStmt:
				{
					IndexStmt  *elp = (IndexStmt *) element;

					setSchemaName(cxt.schemaname, &elp->relation->schemaname);
					cxt.indexes = lappend(cxt.indexes, element);
				}
				break;

			case T_CreateTrigStmt:
				{
					CreateTrigStmt *elp = (CreateTrigStmt *) element;

					setSchemaName(cxt.schemaname, &elp->relation->schemaname);
					cxt.triggers = lappend(cxt.triggers, element);
				}
				break;

			case T_GrantStmt:
				cxt.grants = lappend(cxt.grants, element);
				break;

			default:
				elog(ERROR, "unrecognized node type: %d",
					 (int) nodeTag(element));
		}
	}

	result = NIL;
	result = list_concat(result, cxt.sequences);
	result = list_concat(result, cxt.tables);
	result = list_concat(result, cxt.views);
	result = list_concat(result, cxt.indexes);
	result = list_concat(result, cxt.triggers);
	result = list_concat(result, cxt.grants);

	return result;
}

/*
 * setSchemaName
 *		Set or check schema name in an element of a CREATE SCHEMA command
 */
static void
setSchemaName(char *context_schema, char **stmt_schema_name)
{
	if (*stmt_schema_name == NULL)
		*stmt_schema_name = context_schema;
	else if (strcmp(context_schema, *stmt_schema_name) != 0)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_SCHEMA_DEFINITION),
				 errmsg("CREATE specifies a schema (%s) "
						"different from the one being created (%s)",
						*stmt_schema_name, context_schema)));
}

#ifdef PGXC
/*
 * CheckLocalIndexColumn
 *
 * Checks whether or not the index can be safely enforced locally
 */
bool
CheckLocalIndexColumn (char loctype, char *partcolname, char *indexcolname)
{
	if (IsLocatorReplicated(loctype) || IS_CENTRALIZED_MODE)
	{
		/* always safe */
		return true;
	}
	
	if (loctype == LOCATOR_TYPE_RROBIN)
	{
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_COLUMN_REFERENCE),
				errmsg("Cannot locally enforce a unique index on round robin distributed table.")));
	}
	else if (loctype == LOCATOR_TYPE_HASH || loctype == LOCATOR_TYPE_MODULO
#ifdef _SHARDING_
					|| loctype == LOCATOR_TYPE_SHARD
#endif
			)
	{
		if (partcolname && indexcolname && strcmp(partcolname, indexcolname) == 0)
			return true;
	}
	return false;
}

/*
 * Given relation, find the index of the attribute in the primary key,
 * which is the distribution key. Returns -1 if table is not a Hash/Modulo
 * distributed, does not have a primary key or distribution key is not in the
 * primary key (last should not happen).
 */
static int
find_relation_pk_dist_index(Relation rel)
{
	int 		result = -1;
	List	   *indexoidlist;
	ListCell   *indexoidscan;
	int			partAttNum = InvalidAttrNumber;
	bool 		pk_found = false;

	if (rel->rd_locator_info && rel->rd_locator_info->nDisAttrs > 0)
		partAttNum = rel->rd_locator_info->disAttrNums[0];

	if (partAttNum == InvalidAttrNumber)
		return -1;

	/*
	 * Look up the primary key
	 */
	indexoidlist = RelationGetIndexList(rel);

	foreach(indexoidscan, indexoidlist)
	{
		Oid			indexoid = lfirst_oid(indexoidscan);
		HeapTuple	indexTuple;
		Form_pg_index indexForm;

		indexTuple = SearchSysCache1(INDEXRELID,
								 ObjectIdGetDatum(indexoid));
		if (!HeapTupleIsValid(indexTuple)) /* should not happen */
			elog(ERROR, "cache lookup failed for index %u", indexoid);
		indexForm = ((Form_pg_index) GETSTRUCT(indexTuple));
		if (indexForm->indisprimary)
		{
			int i;

			pk_found = true;

			/*
			 * Loop over index attributes to find
			 * the distribution key
			 */
			for (i = 0; i < indexForm->indnatts; i++)
			{
				if (indexForm->indkey.values[i] == partAttNum)
				{
					result = i;
					break;
				}
			}
		}
		ReleaseSysCache(indexTuple);
		if (pk_found)
			break;
	}

	list_free(indexoidlist);

	return result;
}

/*
 * check to see if the constraint can be enforced locally
 * if not, an error will be thrown
 * At present, the trigger processing of foreign keys is in CN, so theoretically
 * checklocalfkconstraints can not be called, because there are other checks in
 * this function, so this function is retained.
 */
static void
checkLocalFKConstraints(CreateStmtContext *cxt)
{
	ListCell   *fkclist;
	List 	   *nodelist = NIL;

	if (cxt->subcluster)
		nodelist = transformSubclusterNodes(cxt->subcluster);

	foreach(fkclist, cxt->fkconstraints)
	{
		Constraint *constraint;
		Oid pk_rel_id;
		RelationLocInfo *rel_loc_info;
		constraint = (Constraint *) lfirst(fkclist);

		/*
		 * If constraint references to the table itself, it is safe
		 * Check if relation name is the same
		 * XCTODO: NO! It is only safe if table is replicated
		 * or distributed on primary key
		 */
		if (constraint->pktable &&
			strcmp(constraint->pktable->relname,cxt->relation->relname) == 0)
		{
			/* Is namespace also the same ? */
			char *fkcon_schemaname = NULL;
			char	*snamespace = cxt->relation->schemaname;

			if (!snamespace && cxt->isalter)
			{
				Oid	snamespaceid;

				if (cxt->rel)
					snamespaceid = RelationGetNamespace(cxt->rel);
				else
					snamespaceid = RangeVarGetCreationNamespace(cxt->relation);

				snamespace = get_namespace_name(snamespaceid);
				Assert(snamespace != NULL);
			}

			if (!snamespace &&
					!constraint->pktable->schemaname)
				continue;

			if (!constraint->pktable->schemaname)
			{
				/* Schema name is not defined, look for current one */
				List   *search_path = fetch_search_path(false);
				fkcon_schemaname = get_namespace_name(linitial_oid(search_path));
				list_free(search_path);
			}
			else
				fkcon_schemaname = constraint->pktable->schemaname;

			/*
			 * If schema name and relation name are the same, table
			 * references to itself, so constraint is safe
			 */
			if (fkcon_schemaname &&
				strcmp(fkcon_schemaname, snamespace) == 0)
			{
				/* The foreign key reference itself is safe now. skip this check for shard table */
				if ((cxt->distributeby == NULL && !cxt->isalter) ||
				    (cxt->distributeby && cxt->distributeby->disttype == DISTTYPE_SHARD) ||
				    (cxt->isalter && cxt->rel->rd_locator_info != NULL &&
				     cxt->rel->rd_locator_info->locatorType == LOCATOR_TYPE_SHARD))
					continue;
				
				/* check if bad distribution is already defined */
				if ((cxt->distributeby &&
					 cxt->distributeby->disttype != DISTTYPE_REPLICATION) ||
					(cxt->isalter &&
					 cxt->rel->rd_locator_info != NULL &&
					 !IsLocatorReplicated(cxt->rel->rd_locator_info->locatorType) &&
					 !IS_CENTRALIZED_MODE))
					ereport(ERROR,
							(errcode(ERRCODE_SYNTAX_ERROR),
							 errmsg("only replicated table can reference itself")));
				/* Record that replication is required */
				cxt->fallback_source = FBS_REPLICATE;
				if (cxt->fallback_dist_cols)
				{
					list_free_deep(cxt->fallback_dist_cols);
					cxt->fallback_dist_cols = NULL;
				}
				continue;
			}
		}

		pk_rel_id = RangeVarGetRelid(constraint->pktable, NoLock, false);
		rel_loc_info = GetRelationLocInfo(pk_rel_id);
		/* If referenced table is replicated, the constraint is safe */
		if (rel_loc_info == NULL)
			nodelist = NIL;
		else if (IsLocatorReplicated(rel_loc_info->locatorType))
		{
			List *common;

			if (cxt->subcluster)
			{
				/*
				 * Distribution nodes are defined, they must be a subset of
				 * the referenced relation's nodes
				 */
				common = list_intersection_int(nodelist, rel_loc_info->rl_nodeList);
				if (list_length(common) < list_length(nodelist))
					ereport(ERROR,
							(errcode(ERRCODE_SYNTAX_ERROR),
							 errmsg("referenced table is not defined on all target nodes")));
				list_free(common);
			}
			else if (nodelist)
			{
				/* suggest distribution */
				common = list_intersection_int(nodelist, rel_loc_info->rl_nodeList);
				if (list_length(common) == 0)
					ereport(ERROR,
							(errcode(ERRCODE_SYNTAX_ERROR),
							 errmsg("referenced tables is defined on different nodes")));
				list_free(nodelist);
				nodelist = common;
			}
			else
				nodelist = list_copy(rel_loc_info->rl_nodeList);
		}
		else if (rel_loc_info->locatorType == LOCATOR_TYPE_RROBIN)
		{
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					 errmsg("Cannot reference a round robin table in a foreign key constraint")));
		}
		else if (IsLocatorDistributedByValue(rel_loc_info->locatorType))
		{
			ListCell   *fklc;
			ListCell   *pklc;
			char 	  	ltype;
			char	   *lattr = NULL;
			bool		found = false;
			List 	   *common;
			char        *colname = NULL;
			
			if (rel_loc_info->nDisAttrs > 0)
				colname = get_attname(rel_loc_info->relid, rel_loc_info->disAttrNums[0]);

			/*
			 * First check nodes, they must be the same as in
			 * the referenced relation
			 */
			if (cxt->subcluster)
			{
				common = list_intersection_int(nodelist, rel_loc_info->rl_nodeList);
				if (list_length(common) != list_length(rel_loc_info->rl_nodeList) ||
						list_length(common) != list_length(nodelist))
				{
					if (list_length(common) == 0)
						ereport(ERROR,
								(errcode(ERRCODE_SYNTAX_ERROR),
								 errmsg("referenced HASH/MODULO table must be defined on same nodes")));
				}
				list_free(common);
			}
			else
			{
				if (nodelist)
				{
					common = list_intersection_int(nodelist, rel_loc_info->rl_nodeList);
					if (list_length(common) != list_length(rel_loc_info->rl_nodeList))
						ereport(ERROR,
								(errcode(ERRCODE_SYNTAX_ERROR),
								 errmsg("referenced HASH/MODULO table must be defined on same nodes")));
					list_free(nodelist);
					nodelist = common;
				}
				else
					nodelist = list_copy(rel_loc_info->rl_nodeList);
				/* Now define the subcluster */
				cxt->subcluster = makeSubCluster(nodelist);
			}

			if (cxt->distributeby)
			{
				ltype = ConvertToLocatorType(cxt->distributeby->disttype);
#ifdef __OPENTENBASE_C__
				if (cxt->distributeby->colname)
				{
					lattr = strVal(list_nth(cxt->distributeby->colname, 0));
				}
				else
				{
					lattr = NULL;
				}
#else
				lattr = cxt->distributeby->colname;
#endif

#ifdef __OPENTENBASE__
				if (ltype == LOCATOR_TYPE_SHARD)
				{
					/* find the fk attribute matching the distribution column */
					lattr = NULL;
					if (list_length(constraint->pk_attrs) == 0)
					{
						/*
						 * PK attribute list may be missing, so FK must reference
						 * the primary table's primary key. The primary key may
						 * consist of multiple attributes, one of them is a
						 * distribution key. We should find the foreign attribute
						 * referencing that primary attribute and set it as the
						 * distribution key of the table.
						 */
						int 		pk_attr_idx;
						Relation	rel;

						rel = relation_open(pk_rel_id, AccessShareLock);
						pk_attr_idx = find_relation_pk_dist_index(rel);
						relation_close(rel, AccessShareLock);

						if (pk_attr_idx >= 0 &&
								pk_attr_idx < list_length(constraint->fk_attrs))
						{
							lattr = strVal(list_nth(constraint->fk_attrs, pk_attr_idx));
						}
					}
					else
					{
						/*
						 * One of the primary attributes must be the primary
						 * tabble's distribution key. We should find the foreign
						 * attribute referencing that primary attribute and set it
						 * as the distribution key of the table.
						 */
						forboth(fklc, constraint->fk_attrs,
								pklc, constraint->pk_attrs)
						{
							if (strcmp(colname,
									   strVal(lfirst(pklc))) == 0)
							{
								lattr = strVal(lfirst(fklc));
								break;
							}
						}
					}
					/* distribution column is not referenced? */
					if (lattr == NULL && !IS_CENTRALIZED_MODE)
						ereport(ERROR,
								(errcode(ERRCODE_SYNTAX_ERROR),
								 errmsg("Hash/Modulo distribution column does not refer"
										" to hash/modulo distribution column in referenced table.")));
				}
#endif
			}
			else if (cxt->isalter)
			{
				if (cxt->rel->rd_locator_info == NULL)
				{
					if (IS_CENTRALIZED_MODE)
					{
						continue;
					}
					else
					{
						ereport(ERROR,
							(errcode(ERRCODE_SYNTAX_ERROR),
							 errmsg("Hash/Modulo distribution column does not refer"
									" to hash/modulo distribution column in referenced table.")));
					}
				}

				ltype = cxt->rel->rd_locator_info->locatorType;
				if (cxt->rel->rd_locator_info->nDisAttrs > 0)
					lattr = get_attname(cxt->rel->rd_locator_info->relid, cxt->rel->rd_locator_info->disAttrNums[0]);
				else
					lattr = NULL;

#ifdef __OPENTENBASE__
				if (ltype == LOCATOR_TYPE_SHARD)
				{
					/* find the fk attribute matching the distribution column */
					lattr = NULL;
					if (list_length(constraint->pk_attrs) == 0)
					{
						/*
						 * PK attribute list may be missing, so FK must reference
						 * the primary table's primary key. The primary key may
						 * consist of multiple attributes, one of them is a
						 * distribution key. We should find the foreign attribute
						 * referencing that primary attribute and set it as the
						 * distribution key of the table.
						 */
						int 		pk_attr_idx;
						Relation	rel;

						rel = relation_open(pk_rel_id, AccessShareLock);
						pk_attr_idx = find_relation_pk_dist_index(rel);
						relation_close(rel, AccessShareLock);

						if (pk_attr_idx >= 0 &&
								pk_attr_idx < list_length(constraint->fk_attrs))
						{
							lattr = strVal(list_nth(constraint->fk_attrs, pk_attr_idx));
						}
					}
					else
					{
						/*
						 * One of the primary attributes must be the primary
						 * tabble's distribution key. We should find the foreign
						 * attribute referencing that primary attribute and set it
						 * as the distribution key of the table.
						 */
						forboth(fklc, constraint->fk_attrs,
								pklc, constraint->pk_attrs)
						{
							if (strcmp(colname,
									   strVal(lfirst(pklc))) == 0)
							{
								lattr = strVal(lfirst(fklc));
								break;
							}
						}
					}
					/* distribution column is not referenced? */
					if (lattr == NULL && !IS_CENTRALIZED_MODE)
						ereport(ERROR,
								(errcode(ERRCODE_SYNTAX_ERROR),
								 errmsg("Hash/Modulo distribution column does not refer"
										" to hash/modulo distribution column in referenced table.")));
				}
#endif

			}
			else
			{
				/*
				 * Not defined distribution, but we can define now.
				 * The distribution must be the same as in referenced table,
				 * distribution keys must be matching fk/pk
				 */
				/*
				 * Can not define distribution by value already
				 */
				if (cxt->fallback_source == FBS_REPLICATE && !IS_CENTRALIZED_MODE)
					ereport(ERROR,
							(errcode(ERRCODE_SYNTAX_ERROR),
							 errmsg("Hash/Modulo distribution column does not refer"
									" to hash/modulo distribution column in referenced table.")));
				/* find the fk attribute matching the distribution column */
				lattr = NULL;
				if (list_length(constraint->pk_attrs) == 0)
				{
					/*
					 * PK attribute list may be missing, so FK must reference
					 * the primary table's primary key. The primary key may
					 * consist of multiple attributes, one of them is a
					 * distribution key. We should find the foreign attribute
					 * referencing that primary attribute and set it as the
					 * distribution key of the table.
					 */
					int 		pk_attr_idx;
					Relation	rel;

					rel = relation_open(pk_rel_id, AccessShareLock);
					pk_attr_idx = find_relation_pk_dist_index(rel);
					relation_close(rel, AccessShareLock);

					if (pk_attr_idx >= 0 &&
							pk_attr_idx < list_length(constraint->fk_attrs))
					{
						lattr = strVal(list_nth(constraint->fk_attrs, pk_attr_idx));
					}
				}
				else
				{
					/*
					 * One of the primary attributes must be the primary
					 * table's distribution key. We should find the foreign
					 * attribute referencing that primary attribute and set it
					 * as the distribution key of the table.
					 */
					forboth(fklc, constraint->fk_attrs,
							pklc, constraint->pk_attrs)
					{
						if (strcmp(colname,
								   strVal(lfirst(pklc))) == 0)
						{
							lattr = strVal(lfirst(fklc));
							break;
						}
					}
				}
				/* distribution column is not referenced? */
				if (lattr == NULL)
				{
					/* single node mode allows distribution keys to be unassociated. */
					if (IS_CENTRALIZED_MODE)
					{
						continue;
					}
					else
					{
						ereport(ERROR,
							(errcode(ERRCODE_SYNTAX_ERROR),
							 errmsg("Hash/Modulo distribution column does not refer"
									" to hash/modulo distribution column in referenced table.")));
					}
				}

				foreach(fklc, cxt->fallback_dist_cols)
				{
					if (strcmp(lattr, (char *) lfirst(fklc)) == 0)
					{
						found = true;
						break;
					}
				}
				if (found)
				{
					list_free_deep(cxt->fallback_dist_cols);
					cxt->fallback_dist_cols = NIL;
					cxt->fallback_source = FBS_NONE;
					cxt->distributeby = makeNode(DistributeBy);
					switch (rel_loc_info->locatorType)
					{
						case LOCATOR_TYPE_HASH:
							cxt->distributeby->disttype = DISTTYPE_HASH;
#ifdef __OPENTENBASE_C__
							cxt->distributeby->colname = list_make1(makeString(pstrdup(lattr)));
#else
							cxt->distributeby->colname = pstrdup(lattr);
#endif
							break;
						case LOCATOR_TYPE_MODULO:
							cxt->distributeby->disttype = DISTTYPE_MODULO;
#ifdef __OPENTENBASE_C__
							cxt->distributeby->colname = list_make1(makeString(pstrdup(lattr)));
#else
							cxt->distributeby->colname = pstrdup(lattr);
#endif
							break;
						case LOCATOR_TYPE_SHARD:
							cxt->distributeby->disttype = DISTTYPE_SHARD;
							cxt->distributeby->colname = list_make1(makeString(pstrdup(lattr)));
							break;
						default:
							/* can not happen ?*/
							ereport(ERROR,
									(errcode(ERRCODE_SYNTAX_ERROR),
									 errmsg("Hash/Modulo distribution column does not refer"
											" to hash/modulo distribution column in referenced table.")));
					}
				}
				else if (!IS_CENTRALIZED_MODE) /* dist attr is not found */
					ereport(ERROR,
							(errcode(ERRCODE_SYNTAX_ERROR),
							 errmsg("Hash/Modulo distribution column does not refer"
									" to hash/modulo distribution column in referenced table.")));
				continue;
			}
			/*
			 * Here determine if already defined distribution is matching
			 * to distribution of primary table.
			 */
			if ((ltype != rel_loc_info->locatorType || lattr == NULL))
			{
				if (IS_CENTRALIZED_MODE)
				{
					continue;
				}
				else
				{
					ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("Hash/Modulo distribution column does not refer"
								" to hash/modulo distribution column in referenced table.")));
				}
			}
			if (list_length(constraint->pk_attrs) == 0)
			{
				/*
				 * PK attribute list may be missing, so FK must reference
				 * the primary table's primary key. The primary key may
				 * consist of multiple attributes, one of them is a
				 * distribution key. We should find the foreign attribute
				 * referencing that primary attribute and make sure it is a
				 * distribution key of the table.
				 */
				int 		pk_attr_idx;
				Relation	rel;

				rel = relation_open(pk_rel_id, AccessShareLock);
				pk_attr_idx = find_relation_pk_dist_index(rel);
				relation_close(rel, AccessShareLock);

				/*
				 * Two first conditions are just avoid assertion failure in
				 * list_nth. First should never happen, because the primary key
				 * of hash/modulo distributed table must contain distribution
				 * key. Second may only happen if list of foreign columns is
				 * shorter then the primary key. In that case statement would
				 * probably fail later, but no harm if it fails here.
				 */
				if (pk_attr_idx >= 0 &&
						pk_attr_idx < list_length(constraint->fk_attrs) &&
						strcmp(lattr, strVal(list_nth(constraint->fk_attrs,
													  pk_attr_idx))) == 0)
				{
					found = true;
				}
			}
			else
			{
				forboth(fklc, constraint->fk_attrs, pklc, constraint->pk_attrs)
				{
					if (strcmp(lattr, strVal(lfirst(fklc))) == 0)
					{
						found = true;
						if (strcmp(colname,
								   strVal(lfirst(pklc))) == 0)
							break;
						else if (!IS_CENTRALIZED_MODE)
							ereport(ERROR,
									(errcode(ERRCODE_SYNTAX_ERROR),
									 errmsg("Hash/Modulo distribution column does not refer"
											" to hash/modulo distribution column in referenced table.")));
					}
				}
			}
			if (!found && !IS_CENTRALIZED_MODE)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("Hash/Modulo distribution column does not refer"
								" to hash/modulo distribution column in referenced table.")));
		}
		else /* Unsupported distribution */
		{
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					 errmsg("Cannot reference a table with distribution type \"%c\"",
					 rel_loc_info->locatorType)));
		}
	}
	/*
	 * If presence of a foreign constraint suggested a set of nodes, fix it here
	 */
	if (nodelist && cxt->subcluster == NULL)
		cxt->subcluster = makeSubCluster(nodelist);
}
#endif


#ifdef XCP
/*
 * Convert SubCluster definition to a list of Datanode indexes, to compare to
 * relation nodes
 */
static List *
transformSubclusterNodes(PGXCSubCluster *subcluster)
{
	List   *result = NIL;
	Oid	   *nodeoids;
	int		numnodes;
	int 	i;
	char	nodetype = PGXC_NODE_DATANODE;

	nodeoids = GetRelationDistributionNodes(subcluster, &numnodes);
	for (i = 0; i < numnodes; i++)
		result = lappend_int(result, PGXCNodeGetNodeId(nodeoids[i], &nodetype));

	return result;
}


/*
 * Create a SubCluster definition from a list of node indexes.
 * Validate nodelist against default node group, error when
 * validation failed.
 */
static PGXCSubCluster *
makeSubCluster(List *nodelist)
{
	ListCell *lc;
	Oid group_oid;
	Oid *default_nodes = NULL;
	int node_number;
	int i;

	group_oid = GetDefaultGroup();
	node_number = get_pgxc_groupmembers(group_oid, &default_nodes);

	/* assume there are no duplicate members in both group */
	foreach (lc, nodelist)
	{
	    int nodeidx = lfirst_int(lc);
	    bool found = false;

	    for (i = 0; i < node_number; i++)
	    {
	        if (PGXCNodeGetNodeOid(nodeidx, PGXC_NODE_DATANODE) == default_nodes[i])
	        {
	            found = true;
	            break;
	        }
	    }

	    if (!found)
	    {
	        elog(ERROR, "Could not automatically determine target group,please manually specify one");
	    }
	}

	return makeShardSubCluster(group_oid);
}
#endif

#ifdef _SHARDING_
/*
 * Create a ShardSubCluster definition from given group.
 */
PGXCSubCluster *
makeShardSubCluster(Oid groupId)
{
	PGXCSubCluster *result = NULL;
	char   *groupName = NULL;
	
	result = makeNode(PGXCSubCluster);
	result->clustertype = SUBCLUSTER_GROUP;

	groupName = get_pgxc_groupname(groupId);
	
	result->members = lappend(result->members, makeString(groupName));

	return result;
}
#endif

/*
 * transformPartitionCmd
 *		Analyze the ATTACH/DETACH PARTITION command
 *
 * In case of the ATTACH PARTITION command, cxt->partbound is set to the
 * transformed value of cmd->bound.
 */
static void
transformPartitionCmd(CreateStmtContext *cxt, PartitionCmd *cmd)
{
	Relation	parentRel = cxt->rel;

	switch (parentRel->rd_rel->relkind)
	{
		case RELKIND_PARTITIONED_TABLE:
			/* transform the partition bound, if any */
			Assert(RelationGetPartitionKey(parentRel) != NULL);
			if (cmd->bound != NULL)
				cxt->partbound = transformPartitionBound(cxt->pstate, parentRel,
														 cmd->bound);
			break;
		case RELKIND_PARTITIONED_INDEX:
			/*
			 * A partitioned index cannot have a partition bound set.  ALTER
			 * INDEX prevents that with its grammar, but not ALTER TABLE.
			 */
			if (cmd->bound != NULL)
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
						 errmsg("\"%s\" is not a partitioned table",
								RelationGetRelationName(parentRel))));
			break;
		case RELKIND_RELATION:
			/* the table must be partitioned */
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
					 errmsg("table \"%s\" is not partitioned",
							RelationGetRelationName(parentRel))));
			break;
		case RELKIND_INDEX:
			/* the index must be partitioned */
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
					 errmsg("index \"%s\" is not partitioned",
							RelationGetRelationName(parentRel))));
			break;
		default:
			/* parser shouldn't let this case through */
			elog(ERROR, "\"%s\" is not a partitioned table or index",
				 RelationGetRelationName(parentRel));
			break;
	}
}

/*
 * transformPartitionBound
 *
 * Transform a partition bound specification
 */
PartitionBoundSpec *
transformPartitionBound(ParseState *pstate, Relation parent,
						PartitionBoundSpec *spec)
{
	PartitionBoundSpec *result_spec;
	PartitionKey key = RelationGetPartitionKey(parent);
	char		strategy = get_partition_strategy(key);
	int			partnatts = get_partition_natts(key);
	List	   *partexprs = get_partition_exprs(key);

	/* Avoid scribbling on input */
	result_spec = copyObject(spec);

	if (spec->is_default)
	{
		if (strategy == PARTITION_STRATEGY_HASH)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_TABLE_DEFINITION),
					 errmsg("a hash-partitioned table may not have a default partition")));

		/*
		 * In case of the default partition, parser had no way to identify the
		 * partition strategy. Assign the parent's strategy to the default
		 * partition bound spec.
		 */
		result_spec->strategy = strategy;

		return result_spec;
	}

	if (strategy == PARTITION_STRATEGY_HASH)
	{
		if (spec->strategy != PARTITION_STRATEGY_HASH)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_TABLE_DEFINITION),
					 errmsg("invalid bound specification for a hash partition"),
					 parser_errposition(pstate, exprLocation((Node *) spec))));

		if (spec->modulus <= 0)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_TABLE_DEFINITION),
					 errmsg("modulus for hash partition must be a positive integer")));

		Assert(spec->remainder >= 0);

		if (spec->remainder >= spec->modulus)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_TABLE_DEFINITION),
					 errmsg("remainder for hash partition must be less than modulus")));
	}
	else if (strategy == PARTITION_STRATEGY_LIST)
	{
		ListCell   *cell;
		char	   *colname;
		Oid			coltype;
		int32		coltypmod;

		if (spec->strategy != PARTITION_STRATEGY_LIST)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_TABLE_DEFINITION),
					 errmsg("invalid bound specification for a list partition"),
					 parser_errposition(pstate, exprLocation((Node *) spec))));

		/* Get the only column's name in case we need to output an error */
		if (key->partattrs[0] != 0)
			colname = get_relid_attribute_name(RelationGetRelid(parent),
											   key->partattrs[0]);
		else
			colname = deparse_expression((Node *) linitial(partexprs),
										 deparse_context_for(RelationGetRelationName(parent),
															 RelationGetRelid(parent)),
										 false, false);
		/* Need its type data too */
		coltype = get_partition_col_typid(key, 0);
		coltypmod = get_partition_col_typmod(key, 0);

		result_spec->listdatums = NIL;
		foreach(cell, spec->listdatums)
		{
			Const	   *value;
			ListCell   *cell2;
			bool		duplicate;

			value = transformPartitionBoundValue(pstate, (Node *) lfirst(cell),
												 colname, coltype, coltypmod);

			/* Don't add to the result if the value is a duplicate */
			duplicate = false;
			foreach(cell2, result_spec->listdatums)
			{
				Const	   *value2 = castNode(Const, lfirst(cell2));

				if (equal(value, value2))
				{
					duplicate = true;
					break;
				}
			}
			if (duplicate)
				continue;

			result_spec->listdatums = lappend(result_spec->listdatums,
											  value);
		}
	}
	else if (strategy == PARTITION_STRATEGY_RANGE)
	{
		ListCell   *cell1,
				   *cell2;
		int			i,
					j;

		if (spec->strategy != PARTITION_STRATEGY_RANGE)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_TABLE_DEFINITION),
					 errmsg("invalid bound specification for a range partition"),
					 parser_errposition(pstate, exprLocation((Node *) spec))));


		if (list_length(spec->lowerdatums) != partnatts)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_TABLE_DEFINITION),
					 errmsg("FROM must specify exactly one value per partitioning column")));
		if (list_length(spec->upperdatums) != partnatts)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_TABLE_DEFINITION),
					 errmsg("TO must specify exactly one value per partitioning column")));

		/*
		 * Once we see MINVALUE or MAXVALUE for one column, the remaining
		 * columns must be the same.
		 */
		validateInfiniteBounds(pstate, spec->lowerdatums);
		validateInfiniteBounds(pstate, spec->upperdatums);

		/* Transform all the constants */
		i = j = 0;
		result_spec->lowerdatums = result_spec->upperdatums = NIL;
		forboth(cell1, spec->lowerdatums, cell2, spec->upperdatums)
		{
			PartitionRangeDatum *ldatum = (PartitionRangeDatum *) lfirst(cell1);
			PartitionRangeDatum *rdatum = (PartitionRangeDatum *) lfirst(cell2);
			char	   *colname;
			Oid			coltype;
			int32		coltypmod;
			Const	   *value;

			/* Get the column's name in case we need to output an error */
			if (key->partattrs[i] != 0)
				colname = get_relid_attribute_name(RelationGetRelid(parent),
												   key->partattrs[i]);
			else
			{
				colname = deparse_expression((Node *) list_nth(partexprs, j),
											 deparse_context_for(RelationGetRelationName(parent),
																 RelationGetRelid(parent)),
											 false, false);
				++j;
			}
			/* Need its type data too */
			coltype = get_partition_col_typid(key, i);
			coltypmod = get_partition_col_typmod(key, i);

			if (ldatum->value)
			{
				value = transformPartitionBoundValue(pstate, ldatum->value,
													 colname,
													 coltype, coltypmod);
				if (value->constisnull)
					ereport(ERROR,
							(errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
							 errmsg("cannot specify NULL in range bound")));
				ldatum = copyObject(ldatum);	/* don't scribble on input */
				ldatum->value = (Node *) value;
			}

			if (rdatum->value)
			{
				value = transformPartitionBoundValue(pstate, rdatum->value,
													 colname,
													 coltype, coltypmod);
				if (value->constisnull)
					ereport(ERROR,
							(errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
							 errmsg("cannot specify NULL in range bound")));
				rdatum = copyObject(rdatum);	/* don't scribble on input */
				rdatum->value = (Node *) value;
			}

			result_spec->lowerdatums = lappend(result_spec->lowerdatums,
											   ldatum);
			result_spec->upperdatums = lappend(result_spec->upperdatums,
											   rdatum);

			++i;
		}
	}
	else
		elog(ERROR, "unexpected partition strategy: %d", (int) strategy);

	return result_spec;
}

/*
 * validateInfiniteBounds
 *
 * Check that a MAXVALUE or MINVALUE specification in a partition bound is
 * followed only by more of the same.
 */
static void
validateInfiniteBounds(ParseState *pstate, List *blist)
{
        ListCell   *lc;
	PartitionRangeDatumKind kind = PARTITION_RANGE_DATUM_VALUE;

	foreach(lc, blist)
	{
		PartitionRangeDatum *prd = castNode(PartitionRangeDatum, lfirst(lc));

		if (kind == prd->kind)
			continue;

		switch (kind)
		{
			case PARTITION_RANGE_DATUM_VALUE:
				kind = prd->kind;
				break;

			case PARTITION_RANGE_DATUM_MAXVALUE:
				ereport(ERROR,
						(errcode(ERRCODE_DATATYPE_MISMATCH),
						 errmsg("every bound following MAXVALUE must also be MAXVALUE"),
						 parser_errposition(pstate, exprLocation((Node *) prd))));

			case PARTITION_RANGE_DATUM_MINVALUE:
				ereport(ERROR,
						(errcode(ERRCODE_DATATYPE_MISMATCH),
						 errmsg("every bound following MINVALUE must also be MINVALUE"),
						 parser_errposition(pstate, exprLocation((Node *) prd))));
		}
	}
}

/*
 * Transform one constant in a partition bound spec
 */
static Const *
transformPartitionBoundValue(ParseState *pstate, Node *node,
							 const char *colName, Oid colType, int32 colTypmod)
{
	Node	   *value;
    A_Const     *con;

    /* node may Const or A_Const in raw tree */
    if (IsA(node, Const))
		return (Const *) copyObject(node);

    con = castNode(A_Const, node);

	/* Make it into a Const */
	value = (Node *) make_const(pstate, &con->val, con->location);

	/* Coerce to correct type */
	value = coerce_to_target_type(pstate,
								  value, exprType(value),
								  colType,
								  colTypmod,
								  COERCION_ASSIGNMENT,
								  COERCE_IMPLICIT_CAST,
								  -1);

	if (value == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_DATATYPE_MISMATCH),
				 errmsg("specified value cannot be cast to type %s for column \"%s\"",
						format_type_be(colType), colName),
				 parser_errposition(pstate, con->location)));

	/* Simplify the expression, in case we had a coercion */
	if (!IsA(value, Const))
		value = (Node *) expression_planner((Expr *) value);

	/* Fail if we don't have a constant (i.e., non-immutable coercion) */
	if (!IsA(value, Const))
		ereport(ERROR,
				(errcode(ERRCODE_DATATYPE_MISMATCH),
				 errmsg("specified value cannot be cast to type %s for column \"%s\"",
						format_type_be(colType), colName),
				 errdetail("The cast requires a non-immutable conversion."),
				 errhint("Try putting the literal value in single quotes."),
				 parser_errposition(pstate, con->location)));

	return (Const *) value;
}


/* Add create partition table statements after transform */
static List *
addCreatePartitionStmts(List *stmts, CreateStmt *p_stmt)
{
	return stmts;
}
