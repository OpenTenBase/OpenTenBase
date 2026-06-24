/*-------------------------------------------------------------------------
 *
 * packagecmds.c
 *
 *	  Routines for CREATE and DROP PACKAGE/PACKAGE BODY commands and DROP
 *	  PACKAGE commands.
 * Portions Copyright (c) 2020-2030, Tencent OpenTenBase Develop Team.
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/commands/packagecmds.c
 *
 * DESCRIPTION
 *	  These routines take the parse tree and pick out the
 *	  appropriate arguments/flags, and pass the results to the
 *	  corresponding "FooDefine" routines (in src/catalog) that do
 *	  the actual catalog-munging.  These routines also verify permission
 *	  of the user to execute the command.
 *
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/genam.h"
#include "access/heapam.h"
#include "access/htup_details.h"
#include "access/sysattr.h"
#include "access/xact.h"
#include "catalog/dependency.h"
#include "catalog/indexing.h"
#include "catalog/objectaccess.h"
#include "catalog/pg_aggregate.h"
#include "catalog/pg_cast.h"
#include "catalog/pg_language.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_proc_fn.h"
#include "catalog/pg_transform.h"
#include "catalog/pg_type.h"
#include "catalog/pg_type_fn.h"
#include "catalog/pg_package.h"
#include "catalog/pg_synonym.h"
#include "commands/alter.h"
#include "commands/defrem.h"
#include "commands/proclang.h"
#include "commands/schemacmds.h"
#include "commands/typecmds.h"
#include "nodes/makefuncs.h"
#include "miscadmin.h"
#include "optimizer/var.h"
#include "parser/parse_coerce.h"
#include "parser/parse_collate.h"
#include "parser/parse_expr.h"
#include "parser/parse_func.h"
#include "parser/parse_type.h"
#include "parser/parser.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/guc.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/syscache.h"
#include "utils/tqual.h"
#include "utils/hsearch.h"

#include "catalog/pg_compile.h"
#include "catalog/pg_collation.h"
#include "commands/extension.h"
#include "utils/memutils.h"
#include "parser/gramparse_extra.h"
#include "utils/memutils.h"

#define PSEUDO_PROC_IN_PKG "pseudo_proc_pkg"

static void updatePackagePublicFuncs(Oid pkgoid, List *funcs);
static void CompilePackage(PackageCompileContext *context, int comp_pkg);
static char *ChoosePackageCorrespondingNamespace(const char *name);

plpgsql_get_udt_typoid_hooks		get_udt_typoid;
plpgsql_get_udt_typname_hooks		get_udt_typname;
plpgsql_compile_pkg_hooks			compile_package;
plpgsql_post_columnref_hooks		plpgsql_post_column;
plpgsql_get_pkg_dot_udt_hooks		get_pkg_dot_udt;
plpgsql_get_pkg_var_typoid_hooks	get_pkg_var_typoid;

bool load_plpgsql = false;

/*
 * Dissect the list of options assembled in gram.y into function
 * attributes.
 */
static void
compute_pkg_attributes_sql_style(ParseState *pstate, List *options, List **as,
								char **sharing, bool *defcollation_p,
								char **authid, Node **accessible,
								float4 *procost, float4 *prorows) {
	ListCell	*option;
	DefElem		*as_item = NULL;
	DefElem		*sharing_item = NULL;
	DefElem		*authid_item = NULL;
	DefElem		*defaultcollation_item = NULL;
	DefElem		*accessible_item = NULL;
	DefElem		*cost_item = NULL;
	DefElem		*rows_item = NULL;

	foreach(option, options)
	{
		DefElem *defel = (DefElem *) lfirst(option);

		if (strcmp(defel->defname, "as") == 0)
		{
			if (as_item)
				ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR),
						errmsg("conflicting or redundant options"),
						parser_errposition(pstate, defel->location)));
			as_item = defel;
		}
		else if (strcmp(defel->defname, "sharing") == 0)
		{
			if (sharing_item)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options"),
						 parser_errposition(pstate, defel->location)));
			sharing_item = defel;
		}
		else if (strcmp(defel->defname, "authid") == 0)
		{
			if (authid_item)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options"),
						 parser_errposition(pstate, defel->location)));
			authid_item = defel;
		}
		else if (strcmp(defel->defname, "defcollation") == 0)
		{
			if (defaultcollation_item)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options"),
						 parser_errposition(pstate, defel->location)));
			defaultcollation_item = defel;
		}
		else if (strcmp(defel->defname, "accessible") == 0)
		{
			if (accessible_item)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						errmsg("conflicting or redundant options"),
						parser_errposition(pstate, defel->location)));
			accessible_item = defel;
		}
		else
			elog(ERROR, "option \"%s\" not recognized", defel->defname);
	}

	/* process required items */
	if (as_item)
		*as = (List *) as_item->arg;
	else
	{
		ereport(ERROR,
					(errcode(ERRCODE_INVALID_FUNCTION_DEFINITION),
					errmsg("no function body specified")));
		*as = NIL; /* keep compiler quiet */
	}

	/* process optional items */
	if (sharing_item)
		*sharing = strVal(sharing_item->arg);
	if (authid_item)
		*authid = strVal(authid_item->arg);
	if (defaultcollation_item)
		*defcollation_p = intVal(defaultcollation_item->arg);
	if (accessible_item)
		*accessible = accessible_item->arg;

	if (cost_item)
	{
		*procost = defGetNumeric(cost_item);

		if (*procost <= 0)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("COST must be positive")));
	}

	if (rows_item)
	{
		*prorows = defGetNumeric(rows_item);

		if (*prorows <= 0)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					errmsg("ROWS must be positive")));
	}
}

static void
deleteFunctionsDepend(Oid pkgcrrspndingns, char* pkgName)
{
	HeapScanDesc	scandesc;
	Relation	rel;
	HeapTuple	tuple;
	ScanKeyData	entry[1];
	Oid			procoid;

	/*
	 * Find the target tuple
	 */
	rel = heap_open(ProcedureRelationId, RowExclusiveLock);

	ScanKeyInit(&entry[0], Anum_pg_proc_pronamespace, BTEqualStrategyNumber,
					F_OIDEQ, ObjectIdGetDatum(pkgcrrspndingns));

	scandesc = heap_beginscan_catalog(rel, 1, entry);
	while ((tuple = heap_getnext(scandesc, ForwardScanDirection)) != NULL)
	{
		procoid = HeapTupleGetOid(tuple);
		deleteDependencyRecordsFor(ProcedureRelationId, procoid, true);
	}

	heap_endscan(scandesc);

	/* release the lock. */
	heap_close(rel, RowExclusiveLock);

	/*
	 * CommandCounterIncrement here to ensure that preceding changes are all
	 * visible to the next deletion step.
	 */
	CommandCounterIncrement();
}

static void
removeFunctions(Oid pkgcrrspndingns, char* pkgName)
{
	HeapScanDesc	scandesc;
	Relation		rel;
	HeapTuple		tuple;
	ScanKeyData		entry[1];
	ObjectAddresses *objects;

	objects = new_object_addresses();

	/*
	 * Find the target tuple
	 */
	rel = heap_open(ProcedureRelationId, RowExclusiveLock);

	ScanKeyInit(&entry[0], Anum_pg_proc_pronamespace, BTEqualStrategyNumber,
							F_OIDEQ, ObjectIdGetDatum(pkgcrrspndingns));

	scandesc = heap_beginscan_catalog(rel, 1, entry);
	while ((tuple = heap_getnext(scandesc, ForwardScanDirection)) != NULL)
	{
		ObjectAddress obj;

		ObjectAddressSet(obj, ProcedureRelationId, HeapTupleGetOid(tuple));
		add_exact_object_address(&obj, objects);
	}

	performMultipleDeletions(objects, DROP_RESTRICT, 0);

	heap_endscan(scandesc);
	heap_close(rel, RowExclusiveLock);

	free_object_addresses(objects);
}

static void
validationFunctions(Oid pkgcrrspndingns, char* pkgName)
{
	HeapScanDesc	scandesc;
	Relation	rel;
	HeapTuple	tuple;
	ScanKeyData	entry[1];
	Datum		tmp;
	char		*funcname;
	char		*prosrc;
	bool		isnull;

	/*
	 * Find the target tuple
	 */
	rel = heap_open(ProcedureRelationId, RowExclusiveLock);

	ScanKeyInit(&entry[0], Anum_pg_proc_pronamespace, BTEqualStrategyNumber,
							F_OIDEQ, ObjectIdGetDatum(pkgcrrspndingns));

	scandesc = heap_beginscan_catalog(rel, 1, entry);
	while ((tuple = heap_getnext(scandesc, ForwardScanDirection)) != NULL)
	{
		funcname = NameStr(((Form_pg_proc) GETSTRUCT(tuple))->proname);

		tmp = SysCacheGetAttr(PROCOID, tuple, Anum_pg_proc_prosrc, &isnull);
		if (isnull)
			elog(ERROR, "null prosrc");
		prosrc = TextDatumGetCString(tmp);
		if (0 == strcmp(prosrc, PSEUDO_PROC_IN_PKG))
			elog(ERROR, "function %s is declared in a package specification and must be defined in the package body",
									funcname);
	}

	heap_endscan(scandesc);

	/* release the lock. */
	heap_close(rel, RowExclusiveLock);
}


static ObjectAddress
CreateOnePkgFunction(Oid pkgoid, CreateFunctionStmt *stmt, ParseState *pstate,
						Oid pkgcrrspndingns, bool replace, bool add_as, bool notice)
{
	List	*options = NULL;
	char	*funcname = NULL;
	ListCell	*l;
	DefElem		*defel;
	ObjectAddress	pkgaddr;
	ObjectAddress	funcaddr;

	/* Convert list of names to a name and namespace */
	QualifiedNameGetCreationNamespace(stmt->funcname, &funcname);
	if (add_as)
		options = list_make2(makeDefElem("as",
							(Node *) list_make1(makeString(PSEUDO_PROC_IN_PKG)), -1),
							makeDefElem("language", (Node *) makeString("internal"), -1));
	stmt->replace = replace;
	stmt->options = list_concat(stmt->options, options);

	funcaddr = CreateFuncByNamespaceAndName(pstate, stmt,
											pkgcrrspndingns, funcname,
											false, NULL, NULL, !add_as,
											pkgoid, notice);

	pkgaddr.classId = PackageRelationId;
	pkgaddr.objectId = pkgoid;
	pkgaddr.objectSubId = 0;
	/* function dependency on package */
	recordDependencyOn(&funcaddr, &pkgaddr, DEPENDENCY_INTERNAL);

	if (options != NULL)
	{
		foreach(l, options)
		{
			defel = (DefElem *)lfirst(l);
			if (strcmp(defel->defname, "as") == 0)
				list_free_deep((List*)defel->arg);
			if (strcmp(defel->defname, "language") == 0)
				pfree(defel->arg);
		}

		list_free_deep(options);
	}

	return funcaddr;
}

static void
CreatePackageFunctions(Oid pkgoid, const char *pkgsrc, Oid pkgcrrspndingns,
					   bool replace, bool secdef, bool add_as, List **funcs,
					   bool notice)
{
	ParseState	*pstate;
	List		*pkgContext;
	ListCell	*l;
	RawStmt		*rawStmt;

	if (funcs)
		*funcs = NIL;

	if (pkgsrc == NULL || pkgsrc[0] == '\0')
		return;

	pkgContext = raw_parser(pkgsrc);
	pstate = make_parsestate(NULL);
	pstate->p_sourcetext = pkgsrc;

	pstate->p_pkgoid = pkgoid;
	pstate->p_post_columnref_hook = plpgsql_post_column;

	foreach(l, pkgContext)
	{
		rawStmt = (RawStmt*)lfirst(l);
		switch (nodeTag(rawStmt->stmt))
		{
			case T_CreateFunctionStmt:
				{
					ObjectAddress	funcaddr;
					ListCell   *option;
					CreateFunctionStmt	*fstmt = (CreateFunctionStmt *) rawStmt->stmt;
					bool	tmp_add_as = add_as;
					bool	has_sec = false;

					foreach(option, fstmt->options)
					{
						DefElem    *defel = (DefElem *) lfirst(option);

						if (strcmp(defel->defname, "security") == 0)
						{
							if (!creating_extension)
								elog(ERROR, "AUTHID is only allowed at package level");
							has_sec = true;
						}
						else if (strcmp(defel->defname, "as") == 0)
							tmp_add_as = false;
					}

					if (!has_sec)
						fstmt->options
							= lappend(fstmt->options, makeDefElem("security",
											(Node *) makeInteger(secdef ? TRUE : FALSE), -1));

					funcaddr
						= CreateOnePkgFunction(pkgoid, fstmt, pstate,
											   pkgcrrspndingns, replace, tmp_add_as,
											   notice);
					if (funcs)
						*funcs = lappend_oid(*funcs, funcaddr.objectId);
				}
				break;
			default:
				ereport(ERROR, (errmsg("unsupport raw stmt:%d",
									rawStmt->stmt->type)));
			break;
		}
	}
}

/*
 * Update public function.
 */
static void
updatePackagePublicFuncs(Oid pkgoid, List *funcs)
{
	HeapTuple	oldtuple,
				tup;
	Datum		*func_d = NULL;
	ListCell	*l;
	ArrayType	*func_arr = NULL;
	Datum		values[Natts_pg_pkg];
	bool		nulls[Natts_pg_pkg];
	bool		replaces[Natts_pg_pkg];
	Relation	rel;
	int			cnt;
	int			i = 0;

	cnt = list_length(funcs);
	memset(values, 0, sizeof(values));
	memset(nulls, false, sizeof(nulls));
	memset(replaces, false, sizeof(replaces));

	if (cnt > 0)
	{
		func_d = palloc(sizeof(Datum) * list_length(funcs));
		foreach(l, funcs)
		{
			func_d[i] = ObjectIdGetDatum(lfirst_oid(l));
			i++;
		}

		func_arr = construct_array(func_d, cnt, OIDOID,
											 sizeof(Oid), true, 'i');
	}

	CommandCounterIncrement();

	rel = heap_open(PackageRelationId, RowExclusiveLock);

	oldtuple = SearchSysCache(PGPKGOID, ObjectIdGetDatum(pkgoid), 0, 0, 0);
	if (!HeapTupleIsValid(oldtuple))
		elog(ERROR, "failed to get package for %u", pkgoid);

	values[Anum_pg_pkg_func - 1] = PointerGetDatum(func_arr);
	nulls[Anum_pg_pkg_func - 1] = (func_arr == NULL ? true : false);
	replaces[Anum_pg_pkg_func - 1] = true;

	tup = heap_modify_tuple(oldtuple, RelationGetDescr(rel), values, nulls, replaces);
	CatalogTupleUpdate(rel, &tup->t_self, tup);

	ReleaseSysCache(oldtuple);
	heap_close(rel, RowExclusiveLock);

	CommandCounterIncrement();
}

static void
DropPackageFunctions(Oid pkgoid, Oid pkgcrrspndingns, char *pkgname)
{
	deleteFunctionsDepend(pkgcrrspndingns, pkgname);
	removeFunctions(pkgcrrspndingns, pkgname);
	updatePackagePublicFuncs(pkgoid, NULL);
}


void
GeneratePkgDependence(ObjectAddress pkgaddr, Oid classid, Oid objectid)
{
	ObjectAddress	referenced;

	referenced.classId = classid;
	referenced.objectId = objectid;
	referenced.objectSubId = 0;
	recordDependencyOn(&pkgaddr, &referenced, DEPENDENCY_NORMAL);
}

/*
 * Will parse the package body to pickup private section, function definition
 * and initialization part.
 */
static void
parse_plpgsql_body(char *body, CreatePackageBodyStmt *stmt)
{
	core_yyscan_t		yyscanner;
	base_yy_extra_type	yyextra;
	ora_yy_lookahead_type	la_tok;

	if (!stmt->reparse_body)
		return;

	yyscanner = scanner_init(body,
							 (core_yy_extra_type *) &yyextra,
							 ScanKeywords,
							 NumScanKeywords);

	la_tok.token = core_yylex(&la_tok.lval, &la_tok.loc, yyscanner);

	/* check return value for failure? */
	get_plpgsql_body(yyscanner, stmt->pkgname->relname, &la_tok, false, NULL, 0);
	if (la_tok.loc < strlen(body))
	{
		/*
		 * The extracted start position of the function in the package and the start
		 * position of the initialization block start from la.tok.token.
		 */
		extract_package_body_defs(stmt, pg_yyget_extra(yyscanner), body + la_tok.loc);
	}

	scanner_finish(yyscanner);
}

static void
parse_plpgsql_spec(char *body, CreatePackageStmt *stmt)
{
	core_yyscan_t		yyscanner;
	base_yy_extra_type	yyextra;
	ora_yy_lookahead_type	la_tok;

	if (!stmt->reparse_spec)
		return;

	yyscanner = scanner_init(body,
							 (core_yy_extra_type *) &yyextra,
							 ScanKeywords,
							 NumScanKeywords);

	la_tok.token = core_yylex(&la_tok.lval, &la_tok.loc, yyscanner);

	/* check return value for failure? */
	get_plpgsql_body(yyscanner, stmt->pkgname->relname, &la_tok, false, NULL, 0);
	stmt->func_spec = pg_yyget_extra(yyscanner)->func_spec;

	scanner_finish(yyscanner);
}

/*
 * CreatePackage
 *	 Execute a CREATE PACKAGE utility statement.
 */
ObjectAddress
CreatePackage(ParseState *pstate, CreatePackageStmt *stmt)
{
	char	*pkgname;
	char	*pkgcrrsname;
	Oid		namespaceId;

	/* pg_package members */
	Oid		pkgcrrspndingns;/* OID of namespace containing this package's members */
	Oid		pkgowner;		/* package owner,ahthor id */
	Oid		languageOid = 12;    /* package language */
	ArrayType	*pkgconfig = NULL;

	float4	pkgcost;			  /* estimated execution cost */
	float4	pkgrows;			  /* estimated # of rows out (if proretset) */
	bool	pkgseriallyreuseable; /* is pkg seriallyreuseable ? */
	bool	pkgissystem; 		  /* is pkg a system pkg? */

	char	*pkgcollation;        /* package collation */
	int16	pkgaccesstype;	     /* package access type, value of pkg_accesstype */
	Oid		pkgaccessobject;     /* package access object, if pkgaccesstype not specified will be set INVALID OID */

	List	*as_clause;
	char	*sharing;
	char	*authid = NULL;
	bool	defcollation_p;
	char	*pkgsrc;
	char	*func_def = NULL;
	Node	*accessibleDefElem = NULL;
	AclResult	aclresult;
	ObjectAddress pkgself;
	bool	issecurity;
	PackageCompileContext   comp_context;

	PLPGSQL_LOAD_FILE;

	/* Convert list of names to a name and namespace */
	namespaceId = RangeVarGetCreationNamespace(stmt->pkgname);

	/* set local package name.*/
	pkgname = stmt->pkgname->relname;

	/* override attributes from explicit list */
	compute_pkg_attributes_sql_style(pstate, stmt->options, &as_clause,
										&sharing, &defcollation_p, &authid,
										&accessibleDefElem, &pkgcost, &pkgrows);

	if (authid != NULL)
	{
		if (strcasecmp(authid, "definer") == 0)
			issecurity = true;
		else if (strcasecmp(authid, "invoker") == 0 ||
					strcasecmp(authid, "current_user") == 0)
			issecurity = false;
		else
			elog(ERROR, "unrecognized authid: %s", authid);
	}
	else
		issecurity = true;

	/* Check we have creation rights in target namespace */
	aclresult = pg_namespace_aclcheck(namespaceId, GetUserId(), ACL_CREATE);
	if (aclresult != ACLCHECK_OK)
	{
		aclcheck_error(aclresult, ACL_KIND_NAMESPACE,
					   get_namespace_name(namespaceId));
	}

	/* check whether the package already exists */
	if (!stmt->replace && package_exists(pkgname, namespaceId))
	{
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("package \"%s\" already exists", pkgname),
				 (errhint("please check the package catalog."))));
	}

	/* check default attributes */
	pkgowner 	= GetUserId();
	pkgconfig 	= NULL;
	pkgcost 	= -1;	/* indicates not set */
	pkgrows 	= -1;	/* indicates not set */
	pkgissystem = false;
	pkgseriallyreuseable = false;
	pkgcollation = "";
	pkgaccesstype = pkg_accesstype_all;
	pkgaccessobject = InvalidOid;

	/*
	 * (pkgname, namespaceId)  |  replace  |  pkgcrrspndingns
	 * 1.             existed       false            whatever  :  already handled
	 * 2.             existed        true             existed  :  get pkgcrrspndingns
	 * 3.             existed        true         not existed  :  should never happened(assert)
	 * 4.         not existed       false             existed  :  handle duplicate ns
	 * 5.         not existed       false         not existed  :  create ns
	 * 6.         not existed        true             existed  :  handle duplicate ns
	 * 7.         not existed        true         not existed  :  create ns
	 */
	pkgcrrspndingns = get_package_corresponding_namespace((const char *) pkgname, namespaceId);
	Assert(!((pkgcrrspndingns == InvalidOid) && package_exists(pkgname, namespaceId)));

	if (pkgcrrspndingns == InvalidOid)
	{
		pkgcrrsname = ChoosePackageCorrespondingNamespace((const char *) pkgname);
		Assert(!OidIsValid(get_namespace_oid(pkgcrrsname, true)));

		pkgcrrspndingns = NamespaceCreate((const char *) pkgcrrsname, pkgowner, false);
		stmt->replace = false;
	}

	pkgsrc = strVal(linitial(as_clause));

	parse_plpgsql_spec(pkgsrc, stmt);

	/* create the package it self */
	pkgself = PackageCreate(pkgname,
							namespaceId,
							pkgcrrspndingns,
							pkgowner,
							languageOid,
							stmt->edition,
							pkgseriallyreuseable,
							pkgcollation,
							pkgaccesstype,
							pkgaccessobject,
							pkgissystem,
							pkgsrc,
							stmt->func_spec,
							PointerGetDatum(pkgconfig),
							stmt->replace,
							pkgcost,
							pkgrows,
							issecurity);

	/*
	 * Drop all functions prevously created.
	 */
	if (stmt->replace)
	{
		/*
		 * Check whether the current package is dependent. If it 
		 * is dependent, it cannot be replaced.
		 */
		checkObjectDepency(&pkgself);

		func_def = get_package_func_def(pkgname, pkgself.objectId, true);
		DropPackageFunctions(pkgself.objectId, pkgcrrspndingns, pkgname);
	}

	memset((char *) &comp_context, 0, sizeof(comp_context));
	comp_context.ctxt
			= AllocSetContextCreate(CurrentMemoryContext,
									"create package compile context",
										ALLOCSET_DEFAULT_SIZES);
	comp_context.pkgid = pkgself.objectId;
	comp_context.pkgself = pkgself;
	CompilePackage(&comp_context, CompilePkgPubSection);
	CreateOrUpdatePkgCompile(pkgself.objectId, comp_context.udt, comp_context.base_types,
									CompilePkgPubSection);

	MemoryContextDelete(comp_context.ctxt);

	/*
	 * Recreate all by new source.
	 */
	if (stmt->func_spec)
	{
		List	*pubfuncs;

		CreatePackageFunctions(pkgself.objectId, stmt->func_spec,
									pkgcrrspndingns, stmt->replace, issecurity, true,
									&pubfuncs, true);
		updatePackagePublicFuncs(pkgself.objectId, pubfuncs);
	}

	/*
	 * If package has body, create the true function definition.
	 */
	if (func_def)
		CreatePackageFunctions(pkgself.objectId, func_def, pkgcrrspndingns,
								stmt->replace, issecurity, true, NULL, false);

	return pkgself;
}

/*
 * CreatePackageBody
 *	 Execute a CREATE PACKAGE BODY utility statement.
 */
ObjectAddress
CreatePackageBody(ParseState *pstate, CreatePackageBodyStmt *stmt)
{
	char	*pkgname;
	Oid		namespaceId;
	Oid		pkgid;
	PackageCompileContext	comp_context;

	/* pg_package members */
	float4		pkgcost;			  /* estimated execution cost */
	float4		pkgrows;			  /* estimated # of rows out (if proretset) */
	Oid			pkgcrrspndingns;/* OID of namespace containing this package's members */
	Oid			pkgowner;		/* package owner,ahthor id */
	Oid			languageOid = 12;	/* package language */
	ArrayType	*pkgconfig = NULL;
	AclResult	aclresult;

	ObjectAddress	pkgself;

	List	*as_clause;
	char	*sharing;
	char	*authid;
	bool	defcollation_p;
	Node	*accessibleDefElem = NULL;
	char	*pkgsrc;
	HeapTuple	tuple;
	bool		isnull;
	Form_pg_package	pkgform;
	char		*func_decl;

	PLPGSQL_LOAD_FILE;

	/* override attributes from explicit list */
	compute_pkg_attributes_sql_style(pstate, stmt->options, &as_clause,
										&sharing, &defcollation_p, &authid,
										&accessibleDefElem, &pkgcost, &pkgrows);

	pkgsrc = strVal(linitial(as_clause));
	if (pkgsrc)
		parse_plpgsql_body(pkgsrc, stmt);

	/* Convert list of names to a name and namespace */
	namespaceId = RangeVarGetCreationNamespace(stmt->pkgname);

	/* set local package name.*/
	pkgname = stmt->pkgname->relname;

	/* Check we have creation rights in target namespace */
	aclresult = pg_namespace_aclcheck(namespaceId, GetUserId(), ACL_CREATE);
	if (aclresult != ACLCHECK_OK)
	{
		aclcheck_error(aclresult, ACL_KIND_NAMESPACE,
					   get_namespace_name(namespaceId));
	}

	tuple = SearchSysCache2(PGPACKAGENMNS, CStringGetDatum(pkgname), ObjectIdGetDatum(namespaceId));
	if (!HeapTupleIsValid(tuple))
		ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT),
						errmsg("package \"%s\" does not exist", pkgname),
						(errhint("please create the package first."))));

	/* check whether the package already created */
	pkgid = HeapTupleGetOid(tuple);
	pkgform = (Form_pg_package) GETSTRUCT(tuple);
	SysCacheGetAttr(PGPACKAGENMNS, tuple, Anum_pg_pkg_bodysrc, &isnull);

	/* package body is created, and is not create or replace stmt, report error */
	if (!isnull && !stmt->replace)
		ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT),
						errmsg("package body \"%s\" already exists", pkgname),
						(errhint("please check the package catalog."))));

	pkgcrrspndingns = pkgform->pkgcrrspndingns;

	/* package body is created, and is create or replace stmt, clean funtion define body */
	if (!isnull && stmt->replace)
		DropPackageFunctions(pkgid, pkgcrrspndingns, pkgname);

	/* check default attributes */
	pkgowner = GetUserId();
	pkgconfig = NULL;

	/* create the package it self */
	pkgself = PackageBodyCreate(pkgname,
								namespaceId,
								pkgcrrspndingns,
								pkgowner,
								languageOid,
								pkgsrc,
								stmt->priv_decl,
								stmt->func_def,
								stmt->initial_part,
								PointerGetDatum(pkgconfig));

	/* create a compile for the package */
	memset((char *) &comp_context, 0, sizeof(comp_context));
	comp_context.ctxt
			= AllocSetContextCreate(CurrentMemoryContext,
									"create package compile context",
										ALLOCSET_DEFAULT_SIZES);
	comp_context.pkgid = pkgself.objectId;
	comp_context.pkgself = pkgself;
	CompilePackage(&comp_context, CompilePkgPrivSection);
	CreateOrUpdatePkgCompile(pkgself.objectId, comp_context.udt, comp_context.base_types,
								CompilePkgPrivSection);
	MemoryContextDelete(comp_context.ctxt);

	func_decl = get_package_func_decl(pkgname, namespaceId);
	if (func_decl && stmt->replace)
	{
		List	*pubfuncs;

		DropPackageFunctions(pkgself.objectId, pkgcrrspndingns, pkgname);
		CreatePackageFunctions(pkgid, func_decl, pkgcrrspndingns, TRUE,
								pkgform->pkgsecdef, true, &pubfuncs, false);

		updatePackagePublicFuncs(pkgself.objectId, pubfuncs);
	}

	/*
	 * Create related functions.
	 */
	CreatePackageFunctions(pkgid, stmt->func_def, pkgcrrspndingns, TRUE,
						   pkgform->pkgsecdef, true, NULL, true);

	validationFunctions(pkgcrrspndingns, pkgname);
	ReleaseSysCache(tuple);

	return pkgself;
}

/*
 * Drop a package.
 *
 */
static void
packageDrop(RangeVar *rangevar, Oid pkgnamens, Oid pkgcrrndsns,
					bool dropbody, DropBehavior behavior)
{
	HeapTuple	tuple;
	Oid			pkgoid;
	bool		isnull;
	Datum		prosrcdatum;
	char		*pkgheadersrc;
	char		*func_decl;
	DropStmt	*stmt = NULL;
	char		*pkgname = rangevar->relname;
	Form_pg_package pkgform;

	tuple = SearchSysCache2(PGPACKAGENMNS, CStringGetDatum(pkgname),
								ObjectIdGetDatum(pkgnamens));

	if (!HeapTupleIsValid(tuple))
		ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT),
						errmsg("package \"%s\" does not exist", pkgname)));

	pkgoid = HeapTupleGetOid(tuple);

	pkgform = (Form_pg_package) GETSTRUCT(tuple);
	prosrcdatum = SysCacheGetAttr(PGPACKAGENMNS, tuple,
									Anum_pg_pkg_headersrc, &isnull);
	if (isnull)
		elog(ERROR, "null pkgheadersrc");

	pkgheadersrc = TextDatumGetCString(prosrcdatum);
	if (((Form_pg_package) GETSTRUCT(tuple))->pkgissystem)
		ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT),
						errmsg("can not drop system package \"%s\"", pkgname)));

	pkgheadersrc = TextDatumGetCString(prosrcdatum);
	func_decl = get_package_func_decl(pkgname, pkgnamens);

	if (dropbody)
	{
		ObjectAddress	objaddr;

		DropPackageFunctions(pkgoid, pkgcrrndsns, rangevar->relname);

		/*
		 * Recreate the function declaration.
		 */
		objaddr = PackageCreate(rangevar->relname,
						pkgform->pkgnamespace,
						pkgform->pkgcrrspndingns,
						pkgform->pkgowner,
						pkgform->pkglang,
						pkgform->pkgeditionable,
						pkgform->pkgseriallyreuseable,
						NULL,
						pkgform->pkgaccesstype,
						pkgform->pkgaccessobject,
						pkgform->pkgissystem,
						pkgheadersrc,
						func_decl,
						PointerGetDatum(NULL),
						TRUE, 0, 0, pkgform->pkgsecdef);

		if (func_decl)
		{
			List	*pubfuncs;

			if (objaddr.objectId != pkgoid)
			{
				Assert(objaddr.objectId == pkgoid);
				elog(ERROR, "packageDrop: invalid args objectId[%u], pkgoid[%u]",
					objaddr.objectId, pkgoid);
			}

			DropPackageFunctions(pkgoid, pkgcrrndsns, pkgname);
			CreatePackageFunctions(pkgoid, func_decl, pkgcrrndsns, TRUE,
								   pkgform->pkgsecdef, true, &pubfuncs,
								   false);
			updatePackagePublicFuncs(pkgoid, pubfuncs);
		}

		/* Invalid the UDT in private section */
		DiscardPkgCompile(pkgoid, CompilePkgPrivSection);
	}
	else
	{
		stmt = makeNode(DropStmt);
		stmt->removeType = OBJECT_PACKAGE;
		stmt->behavior = behavior;
		stmt->objects = lappend(stmt->objects, rangevar);

		RemoveObjects(stmt);

		/* Functions dropped along with dependency */
		DropCompile(PackageRelationId, pkgoid);
	}

	ReleaseSysCache(tuple);
}

/*
 * DropPackage
 *	 Execute a DROP PACKAGE utility statement.
 */
void
DropPackage(ParseState *pstate, DropPackageStmt *stmt)
{
	char	*pkg_name;
	Oid		namespaceId;
	Oid		pkgcrrspndingns;
	AclResult	aclresult;

	/* Convert list of names to a name and namespace */
	namespaceId = RangeVarGetCreationNamespace(stmt->pkgname);

	/* set local package name.*/
	pkg_name = stmt->pkgname->relname;

	/* Check we have creation rights in target namespace */
	aclresult = pg_namespace_aclcheck(namespaceId, GetUserId(), ACL_CREATE);
	if (aclresult != ACLCHECK_OK)
		aclcheck_error(aclresult, ACL_KIND_NAMESPACE,
								get_namespace_name(namespaceId));

	/* get the corresponding namespace. */
	pkgcrrspndingns = get_package_corresponding_namespace((const char *) pkg_name, namespaceId);
	if (!OidIsValid(pkgcrrspndingns))
	{
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("package corresponding namespace \"%s\" does not exist", pkg_name),
				  errhint("Please check catalog.")));
	}

	if (stmt->removeType == OBJECT_PACKAGE_BODY)
		packageDrop(stmt->pkgname, namespaceId, pkgcrrspndingns, TRUE, stmt->behavior);
	else
		packageDrop(stmt->pkgname, namespaceId, pkgcrrspndingns, FALSE, stmt->behavior);
}

/*
 * AlterPackage
 *    Alter package does nothing but invalidate the package. A package is cached
 * in plpgsql.so, we delete it via change the xim of package tuple.
 */
ObjectAddress
AlterPackage(ParseState *pstate, AlterPackageStmt *stmt)
{
	char	*relname;
	Oid		namespaceId;
	bool		replaces[Natts_pg_pkg];
	int			i = 0;
	bool		nulls[Natts_pg_pkg];
	Datum		values[Natts_pg_pkg];
	HeapTuple	tup,
				newtup;
	TupleDesc	tupDesc;
	Relation	rel;
	ObjectAddress pkgself;
	Oid			pkgId;

	/* Convert list of names to a name and namespace */
	namespaceId = RangeVarGetCreationNamespace(stmt->pkgname);

	/* set local package name.*/
	relname = stmt->pkgname->relname;

	tup = SearchSysCache2(PGPACKAGENMNS, CStringGetDatum(relname),
							ObjectIdGetDatum(namespaceId));

	if (!HeapTupleIsValid(tup))
		ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT),
						errmsg("package \"%s\" does not exist", relname)));

	pkgId = HeapTupleGetOid(tup);

	if (!superuser())
	{
		if (!pg_pkg_ownercheck(HeapTupleGetOid(tup), GetUserId()))
			aclcheck_error(ACLCHECK_NOT_OWNER, ACL_KIND_PACKAGE, stmt->pkgname->relname);
	}

	for (i = 0; i < Natts_pg_pkg; ++i)
	{
		nulls[i] = false;
		values[i] = (Datum) 0;
		replaces[i] = false;
	}

	values[Anum_pg_pkg_namespace - 1]
			= ObjectIdGetDatum(((Form_pg_package) GETSTRUCT(tup))->pkgnamespace);
	nulls[Anum_pg_pkg_namespace - 1] = false;
	replaces[Anum_pg_pkg_namespace - 1] = true;

	rel = heap_open(PackageRelationId, RowExclusiveLock);
	tupDesc = RelationGetDescr(rel);
	newtup = heap_modify_tuple(tup, tupDesc, values, nulls, replaces);
	CatalogTupleUpdate(rel, &newtup->t_self, newtup);

	heap_close(rel, RowExclusiveLock);
	heap_freetuple(newtup);

	ReleaseSysCache(tup);

	pkgself.classId = PackageRelationId;
	pkgself.objectId = pkgId;
	pkgself.objectSubId = 0;

	return pkgself;
}

/*
 * RemovePackageById
 */
void
RemovePackageById(Oid pkgOid)
{
	Relation	rel;
	HeapTuple	tuple;

	/*
	 * Find the target tuple
	 */
	rel = heap_open(PackageRelationId, RowExclusiveLock);

	tuple = SearchSysCache1(PGPKGOID, ObjectIdGetDatum(pkgOid));
	if (!HeapTupleIsValid(tuple)) /* should not happen */
		elog(ERROR, "cache lookup failed for package %u", pkgOid);

	CatalogTupleDelete(rel, &tuple->t_self);

	ReleaseSysCache(tuple);
	heap_close(rel, RowExclusiveLock);
}

Oid
GetPackageOid(RangeVar *pkgname, bool missing_ok)
{
	char	*relname;
	Oid		namespaceId;
	Oid		pkgId = InvalidOid;

	/* Convert list of names to a name and namespace */
	namespaceId = RangeVarGetCreationNamespace(pkgname);

	/* set local package name.*/
	relname = pkgname->relname;

	pkgId = GetSysCacheOid2(PGPACKAGENMNS, CStringGetDatum(relname),
							ObjectIdGetDatum(namespaceId));
	if (!OidIsValid(pkgId) && !missing_ok)
		ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT),
						errmsg("package \"%s\" does not exist", relname)));

	return pkgId;
}

static char *
format_package_internal(Oid pkgoid, bool force_qualify)
{
	char		*result;
	HeapTuple	pkgtup;

	pkgtup = SearchSysCache1(PGPKGOID, ObjectIdGetDatum(pkgoid));

	if (HeapTupleIsValid(pkgtup))
	{
		Form_pg_package	pkgform = (Form_pg_package) GETSTRUCT(pkgtup);
		char		*pkgname = NameStr(pkgform->pkgname);
		char		*nspname;
		StringInfoData	buf;

		/* XXX no support here for bootstrap mode */
		Assert(!IsBootstrapProcessingMode());
		initStringInfo(&buf);

		/*
		 * Would this proc be found (given the right args) by regprocedurein?
		 * If not, or if caller requests it, we need to qualify it.
		 */
		if (!force_qualify && PackageIsVisible(pkgoid))
			nspname = NULL;
		else
			nspname = get_namespace_name(pkgform->pkgnamespace);

		appendStringInfo(&buf, "%s",
							quote_qualified_identifier(nspname, pkgname));
		result = buf.data;
		ReleaseSysCache(pkgtup);
	}
	else
	{
		/* If OID doesn't match any pg_proc entry, return it numerically */
		result = (char *) palloc(NAMEDATALEN);
		snprintf(result, NAMEDATALEN, "%u", pkgoid);
	}

	return result;
}

char *
format_package(Oid pkgoid)
{
	return format_package_internal(pkgoid, false);
}

List *
get_pkg_dot_udt_typename(Oid pkgoid, List *udt_name)
{
	if (get_pkg_dot_udt == NULL)
		return NIL;

	return get_pkg_dot_udt(pkgoid, udt_name);
}

Oid
get_package_var_typeoid(Oid pkgid, List *var_name, char **o_udt_type, bool pkg_func)
{
	PLPGSQL_LOAD_FILE;

	if (get_pkg_var_typoid == NULL)
		return InvalidOid;

	return get_pkg_var_typoid(pkgid, var_name, o_udt_type, pkg_func);
}

/*
 * Get a package defined UDT typename. Maybe return NULL if function call in a
 * SQL statement.
 */
char *
get_package_udt_typename(Param *p, void *p_state)
{
	if (get_udt_typname == NULL)
		return NULL;

	return get_udt_typname(p, p_state);
}

Oid
get_package_udt_typeoid(Param *p, void *p_state)
{
	if (get_udt_typoid == NULL)
		return InvalidOid;

	return get_udt_typoid(p, p_state);
}


static void
CompilePackage(PackageCompileContext *context, int comp_pkg)
{
	context->udt = NIL;

	if (compile_package == NULL)
		return;

	compile_package(context, comp_pkg);
}

/*
 * Select a nonconflicting name for a new package corresponding namespace.
 *
 * Returns a palloc'd string.
 */
static char *
ChoosePackageCorrespondingNamespace(const char *name)
{
	int			pass = 0;
	char		*nsname = NULL;
	char		pkgname[NAMEDATALEN];
	int			namepos = 1;

	StrNCpy(pkgname, name, sizeof(pkgname));

	for (;;)
	{
		nsname = makeObjectName(pkgname, NULL, NULL);

		if (!OidIsValid(get_namespace_oid(nsname, true)))
			break;

		if (strcasecmp(nsname, pkgname) == 0 && strlen(nsname) == NAMEDATALEN - 1)
		{
			if (pass > 0 && pass % 10 == 0)
				namepos++;

			nsname[NAMEDATALEN - 1 - namepos] = '\0';
			snprintf(pkgname, sizeof(pkgname), "%s%d", nsname, ++pass);

			pfree(nsname);
			if (namepos > NAMEDATALEN - 1)
				ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
								errmsg("Can't alloc schema name for package.")));
		}
		else
		{
			pfree(nsname);
			snprintf(pkgname, sizeof(pkgname), "%s%d", name, ++pass);
		}
	}

	return nsname;
}
