/*-------------------------------------------------------------------------
 *
 * pg_package.h
 *	  definition of the system "package" relation (pg_package)
 *	  along with the relation's initial contents.
 *
 * Portions Copyright (c) 2020-2030, Tencent.com.
 * Portions Copyright (c) 2012-2014, TransLattice, Inc.
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/catalog/pg_package.h
 *
 * NOTES
 *	  The script catalog/genbki.pl reads this file and generates .bki
 *	  information from the DATA() statements.  utils/Gen_fmgrtab.pl
 *	  generates fmgroids.h and fmgrtab.c the same way.
 *
 *	  XXX do NOT break up DATA() statements into multiple lines!
 *		  the scripts are not as smart as you might think...
 *	  XXX (eg. #if 0 #endif won't do what you think)
 *
 *-------------------------------------------------------------------------
 */
#ifndef PG_PACKAGE_H
#define PG_PACKAGE_H

#include "catalog/genbki.h"
#include "catalog/objectaddress.h"

#define PackageRelationId  9004

CATALOG(pg_package,9004)
{
	NameData	pkgname;		/* package name */
	Oid			pkgnamespace;	/* namespace that owns the package */
	Oid			pkgcrrspndingns;/* OID of namespace containing this package's members */
	Oid			pkgowner;		/* package owner,ahthor id */
	Oid			pkglang;		/* OID of pg_language entry */

	float4		pkgcost;			  /* estimated execution cost */
	float4		pkgrows;			  /* estimated # of rows out (if proretset) */
	bool        pkgeditionable;		  /* is pkg editionable ? */
	bool        pkgseriallyreuseable; /* is pkg seriallyreuseable ? */

	NameData    pkgcollation;   /* package collation */
	int16		pkgaccesstype;	/* package access type, value of pkg_accesstype */
	Oid			pkgaccessobject;/* package access object, if pkgaccesstype not specified will be set INVALID OID */
	bool        pkgissystem;
	bool		pkgsecdef;

	/*
	 * variable-length fields start here.
	 */
#ifdef CATALOG_VARLEN
	text		pkgheadersrc   BKI_FORCE_NOT_NULL; /* package header (specification) */
	text		pkgfuncspec;
	text		pkgbodysrc     BKI_FORCE_NOT_NULL; /* package body */

	text		pkgprivvarsrc;	/* package private variable source */
	text		pkgprocdefsrc;	/* package procedure definition source */
	text		pkginitsrc;	/* package initialization part */

	text		pkgconfig[1];	/* package-local GUC settings */
	aclitem		pkgacl[1];		/* access permissions */
	Oid			pkgfunc[1];
#endif
} FormData_pg_package;

/* ----------------
 *		Form_pg_package corresponds to a pointer to a tuple with
 *		the format of pg_package relation.
 * ----------------
 */
typedef FormData_pg_package *Form_pg_package;

/* ----------------
 *		compiler constants for pg_package
 * ----------------
 */
#define Natts_pg_pkg					23
#define Anum_pg_pkg_name			    1
#define Anum_pg_pkg_namespace		    2
#define Anum_pg_pkg_crrspndingns        3
#define Anum_pg_pkg_owner			    4
#define Anum_pg_pkg_lang			    5
#define Anum_pg_pkg_cost			    6
#define Anum_pg_pkg_rows			    7
#define Anum_pg_pkg_editionable		    8
#define Anum_pg_pkg_seriallyreuseable	9

#define Anum_pg_pkg_collation		    10
#define Anum_pg_pkg_accesstype		    11
#define Anum_pg_pkg_accessobject	    12
#define Anum_pg_pkg_issystem			13
#define Anum_pg_pkg_secdef				14
#define Anum_pg_pkg_headersrc		    15
#define Anum_pg_pkg_funcspec		    16
#define Anum_pg_pkg_bodysrc			    17
#define Anum_pg_pkg_privsrc		        18
#define Anum_pg_pkg_procdefsrc		    19
#define Anum_pg_pkg_initsrc		        20
#define Anum_pg_pkg_config		        21
#define Anum_pg_pkg_acl			        22
#define Anum_pg_pkg_func			    23


/* ----------------
 *		pg_package definition.  cpp turns this into
 *		typedef struct FormData_pg_package
 * ----------------
 */
typedef enum
{
	pkg_accesstype_all       = 0,
	pkg_accesstype_function  = 1,
	pkg_accesstype_procedure = 2,
	pkg_accesstype_package   = 3,
	pkg_accesstype_trigger   = 4,
	pkg_accesstype_type      = 5,
	pkg_accesstype_none
}pkg_accesstype;

typedef enum
{
	PACKAGE_LINK_DEPENDENT, /* default way */
	PACKAGE_LINK_DYNAMIC,
} PackageLinkMethod;

typedef enum CollectionCategory
{
	TYPE_RECORD = 1,
	TYPE_VARRAY,
	TYPE_N_TABLE,
	TYPE_A_ARRAY,
	TYPE_NONE
} CollectionCategory;

typedef struct CollectionType
{
	CollectionCategory  category;
	char                *typname;
	char                *elemtypname;
	char                *index;
	bool                pkg_elemtyp;
} CollectionType;
typedef struct
{
	MemoryContext	ctxt;
	ObjectAddress 	pkgself;
	Oid		pkgid;
	List	*udt;
	List	*base_types; /* arrayOid or pludtOid */
} PackageCompileContext;

typedef Oid (*plpgsql_get_udt_typoid_hooks)(Param *p, void *p_state);
typedef char *(*plpgsql_get_udt_typname_hooks)(Param *p, void *p_state);
typedef void (*plpgsql_compile_pkg_hooks)(PackageCompileContext *, int);
typedef Node *(*plpgsql_post_columnref_hooks)(ParseState *pstate, ColumnRef *cref, Node *var);
typedef List *(*plpgsql_get_pkg_dot_udt_hooks)(Oid pkgid, List *udt_name);
typedef Oid (*plpgsql_get_pkg_var_typoid_hooks)(Oid pkgid, List *var_name, char **o_udt_name, bool pkg_func);

extern int package_link_method;

extern char *get_package_body(const char *pkgname, Oid nspid);
extern char *get_package_spec(const char *pkgname, Oid nspid);
extern char *get_package_priv_spec(const char *pkgname, Oid nspid);
extern char *get_package_spec_tuple(HeapTuple tuple);
extern char *get_package_priv_spec_tuple(HeapTuple tuple);
extern bool package_exists(const char *pkgname, Oid nspid);
extern char *get_package_func_def(const char *pkgname, Oid nspid, bool missing_ok);
extern char *get_package_func_decl(const char *pkgname, Oid nspid);
extern char *get_package_init_block(Oid pkgId);
extern Oid get_pkgId_by_namespace(Oid ns);
extern Oid get_pkgId_by_function(Oid funcId);
extern bool type_belong_to_pkg(Oid typoid, Oid pkgoid);
extern Oid get_pkgId_by_typoid(Oid typoid);
extern char *get_package_name_by_id(Oid pkgId);
extern char *get_pkgspec_by_id(Oid pkgId);
extern char *get_pkg_privspec_by_id(Oid pkgId);

extern bool pg_pkg_ownercheck(Oid pkgoid, Oid roleid);
extern Oid get_package_cornsp(Oid pkgOid);
extern bool package_function_is_privated(Oid pkgId, Oid funcid);
extern char *get_package_func_def_by_id(Oid pkgId);
extern Oid get_pkgcrrspndingns_by_id(Oid pkgId);
extern char *get_package_ns_name_by_id(Oid pkgId);
extern Oid pg_pkg_secdef_ownerid(Oid pkgoid);

extern ObjectAddress
PackageCreate(const char *pkgname,
					Oid   pkgnamens,
					Oid   pkgcrrndsns,	
					Oid   pkgowner,	
					Oid   pkglanguage,	
					bool  pkgeditionable,
					bool  pkgseriallyreuseable,
					char *pkgcollation,
					int   pkgaccesstype,
					Oid	  pkgaccessobject,
					bool  pkgissystem,
					char *pkgheadersrc,
					char *pkgvarsrc,
					Datum pkgconfig,				
					bool  replace,
					float4 procost,
					float4 prorows,
					bool secdef);

extern ObjectAddress
PackageBodyCreate(const char *pkgname,
					Oid   pkgnamens,
					Oid   pkgcrrndsns,	
					Oid   pkgowner,	
					Oid   pkglanguage,	
					char *pkgbodysrc,
					char *pkgvarsrc,
					char *pkgprocsrc,
					char *pkginitsrc,
					Datum pkgconfig);
extern void PackageDrop(const char *pkgname, Oid pkgnamens, Oid pkgcrrndsns);
extern char *get_package_udt_typename(Param *p, void *p_state);
extern List *get_pkg_dot_udt_typename(Oid pkgoid, List *udt_name);
extern Oid get_package_udt_typeoid(Param *p, void *p_state);
extern Oid get_package_var_typeoid(Oid pkgid, List *var_name, char **o_udt_type, bool pkg_func);

extern void push_current_package_namespace(void);
extern void pop_current_package_namespace(void);
extern void GeneratePkgDependence(ObjectAddress pkgaddr, Oid classid, Oid objectid);

extern bool get_pkg_sec_def(Oid pkgId);

extern Oid	CallingPackageId;

extern plpgsql_get_udt_typoid_hooks		get_udt_typoid;
extern plpgsql_get_udt_typname_hooks	get_udt_typname;
extern plpgsql_compile_pkg_hooks		compile_package;
extern plpgsql_post_columnref_hooks		plpgsql_post_column;
extern plpgsql_get_pkg_dot_udt_hooks	get_pkg_dot_udt;
extern plpgsql_get_pkg_var_typoid_hooks	get_pkg_var_typoid;

extern bool load_plpgsql;

#define PLPGSQL_LOAD_FILE \
do \
{ \
	if (!load_plpgsql) \
	{ \
		load_file("$libdir/oraplsql", false); \
		load_plpgsql = true; \
	} \
} while (0)

#endif							/* PG_PACKAGE_H */
