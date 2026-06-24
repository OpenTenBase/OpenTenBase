/*-------------------------------------------------------------------------
 *
 * pg_package.c
 *	  routines to support manipulation of the pg_proc relation
 * Portions Copyright (c) 2020-2030, Tencent.com.
 * Portions Copyright (c) 2012-2014, TransLattice, Inc.
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/catalog/pg_proc.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/htup_details.h"
#include "access/xact.h"
#include "catalog/dependency.h"
#include "catalog/indexing.h"
#include "catalog/namespace.h"
#include "catalog/objectaccess.h"
#include "catalog/pg_language.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_proc_fn.h"
#include "catalog/pg_transform.h"
#include "catalog/pg_type.h"
#include "commands/defrem.h"
#include "executor/functions.h"
#include "funcapi.h"
#include "mb/pg_wchar.h"
#include "miscadmin.h"
#include "nodes/nodeFuncs.h"
#include "parser/parse_type.h"
#include "tcop/pquery.h"
#include "tcop/tcopprot.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/regproc.h"
#include "utils/rel.h"
#include "utils/syscache.h"
#include "utils/fmgroids.h"
#ifdef PGXC
#include "pgxc/execRemote.h"
#include "pgxc/pgxc.h"
#include "pgxc/planner.h"
#include "catalog/pg_package.h"
#endif

int package_link_method = PACKAGE_LINK_DEPENDENT;

/*
 * If current execution is in a package. It is Oid of the direct calling
 * package.
 *
 * InvalidOid is not in a package execution.
 */
Oid	CallingPackageId = InvalidOid;

/*
 * Package information interface. A package is defined using:
 *
 *  CREATE PACKAGE name AS
 *    << package public specification >>
 *      -- will extract func/proc declaration, can be returned by
 *         get_package_func_decl()
 *
 *    -- full specification can be returned by get_package_spec()
 *  END;
 *
 *  CREATE PACKAGE BODY name AS
 *    << package private specification >>
 *      -- can be returned by get_package_priv_spec()
 *
 *    << package function/procedure/cursor definition >>
 *      -- will extract func/proc definition, can be returned by
 *         get_package_func_def()
 *
 *  BEGIN
 *    << initialization part >>
 *      -- will be returned by get_package_init_block()
 *   -- full body can be returned by get_package_body()
 *  END;
 */

/*
 * Get a full string of package body.
 */
char *
get_package_body(const char *pkgname, Oid nspid)
{
	bool 		isNull = false;
	Datum		tmp;
	char*       src    = NULL;
	HeapTuple	tuple;

	tuple = SearchSysCache(PGPACKAGENMNS, PointerGetDatum(pkgname),
								ObjectIdGetDatum(nspid), 0, 0);
	if (!HeapTupleIsValid(tuple))
		return NULL;

	/*
	 * And of course we need the function body text.
	 */
	tmp = SysCacheGetAttr(PGPACKAGENMNS, tuple, Anum_pg_pkg_bodysrc, &isNull);
	if (isNull)
		elog(ERROR, "null package body for package %s", pkgname);

	src = TextDatumGetCString(tmp);

	ReleaseSysCache(tuple);
	return src;
}

/*
 * Get the package function definition section to recreate it.
 */
char *
get_package_func_def(const char *pkgname, Oid nspid, bool missing_ok)
{
	bool	isNull = false;
	Datum	tmp;
	char	*src = NULL;
	HeapTuple	tuple;

	tuple = SearchSysCache(PGPACKAGENMNS, PointerGetDatum(pkgname),
								ObjectIdGetDatum(nspid), 0, 0);
	if (!HeapTupleIsValid(tuple))
	{
		if (!missing_ok)
			elog(ERROR, "package does not exist");
	}
	else
	{
		tmp = SysCacheGetAttr(PGPACKAGENMNS, tuple, Anum_pg_pkg_procdefsrc, &isNull);
		if (!isNull)
			src = TextDatumGetCString(tmp);

		ReleaseSysCache(tuple);
	}

	return src;
}

/*
 * Get the package function specification.
 */
char *
get_package_func_decl(const char *pkgname, Oid nspid)
{
	bool	isNull = false;
	Datum	tmp;
	char	*src = NULL;
	HeapTuple	tuple;

	tuple = SearchSysCache(PGPACKAGENMNS, PointerGetDatum(pkgname),
								ObjectIdGetDatum(nspid), 0, 0);
	if (!HeapTupleIsValid(tuple))
		elog(ERROR, "package does not exist");

	tmp = SysCacheGetAttr(PGPACKAGENMNS, tuple, Anum_pg_pkg_funcspec, &isNull);
	if (!isNull)
		src = TextDatumGetCString(tmp);

	ReleaseSysCache(tuple);
	return src;
}

/*
 * Get package definition specification, ie. package public members.
 */
char *
get_package_spec(const char *pkgname, Oid relnamespace)
{
	bool 		isNull = false;
	Datum		tmp;
	char*		src = NULL;
	HeapTuple	tuple;

	tuple = SearchSysCache(PGPACKAGENMNS, PointerGetDatum(pkgname),
								ObjectIdGetDatum(relnamespace), 0, 0);
	if (!HeapTupleIsValid(tuple))
		return NULL;

	/*
	 * And of course we need the function header text.
	 */
	tmp = SysCacheGetAttr(PGPACKAGENMNS, tuple, Anum_pg_pkg_headersrc, &isNull);
	if (isNull)
		elog(ERROR, "null package specification for package %s", pkgname);

	src = TextDatumGetCString(tmp);

	ReleaseSysCache(tuple);
	return src;
}

/*
 * Get package corresponding namespace.
 */
Oid
get_package_cornsp(Oid pkgId)
{
	HeapTuple	tuple;
	Form_pg_package	pkgform;
	Oid		nspid;

	tuple = SearchSysCache(PGPKGOID, ObjectIdGetDatum(pkgId), 0, 0, 0);
	if (!HeapTupleIsValid(tuple))
		return InvalidOid;

	pkgform = (Form_pg_package) GETSTRUCT(tuple);
	nspid = pkgform->pkgcrrspndingns;

	ReleaseSysCache(tuple);

	return nspid;
}

/*
 * get_pkgspec_by_id
 */
char *
get_pkgspec_by_id(Oid pkgId)
{
	bool 		isNull = false;
	Datum		tmp;
	char*		src = NULL;
	HeapTuple	tuple;

	tuple = SearchSysCache(PGPKGOID, ObjectIdGetDatum(pkgId), 0, 0, 0);
	if (!HeapTupleIsValid(tuple))
		return NULL;

	tmp = SysCacheGetAttr(PGPACKAGENMNS, tuple, Anum_pg_pkg_headersrc, &isNull);
	if (isNull)
		src = NULL;
	else
		src = TextDatumGetCString(tmp);
	ReleaseSysCache(tuple);

	return src;
}

/*
 * get_pkgcrrspndingns_by_id
 */
Oid
get_pkgcrrspndingns_by_id(Oid pkgId)
{
	Oid			res = InvalidOid;
	HeapTuple	tuple;
	Form_pg_package	pkgdata;

	tuple = SearchSysCache(PGPKGOID, ObjectIdGetDatum(pkgId), 0, 0, 0);
	if (!HeapTupleIsValid(tuple))
		return res;

	pkgdata = (Form_pg_package) GETSTRUCT(tuple);
	res = pkgdata->pkgcrrspndingns;

	ReleaseSysCache(tuple);

	return res;
}

/*
 * get_pkg_privspec_by_id
 */
char *
get_pkg_privspec_by_id(Oid pkgId)
{
	bool 		isNull = false;
	Datum		tmp;
	char*		src = NULL;
	HeapTuple	tuple;

	tuple = SearchSysCache(PGPKGOID, ObjectIdGetDatum(pkgId), 0, 0, 0);
	if (!HeapTupleIsValid(tuple))
		return NULL;

	tmp = SysCacheGetAttr(PGPACKAGENMNS, tuple, Anum_pg_pkg_privsrc, &isNull);
	if (isNull)
		src = NULL;
	else
		src = TextDatumGetCString(tmp);
	ReleaseSysCache(tuple);

	return src;
}

/*
 * Get the package function definition source.
 */
char *
get_package_func_def_by_id(Oid pkgId)
{
	bool		isNull = false;
	Datum		tmp;
	char		*src = NULL;
	HeapTuple	tuple;

	tuple = SearchSysCache(PGPKGOID, ObjectIdGetDatum(pkgId), 0, 0, 0);
	if (!HeapTupleIsValid(tuple))
		return NULL;

	tmp = SysCacheGetAttr(PGPACKAGENMNS, tuple, Anum_pg_pkg_procdefsrc, &isNull);
	if (isNull)
		src = NULL;
	else
		src = TextDatumGetCString(tmp);
	ReleaseSysCache(tuple);

	return src;
}

/*
 * Get private section of a package body.
 */
char *
get_package_priv_spec(const char *pkgname, Oid relnamespace)
{
	bool 		isNull = false;
	Datum		tmp;
	char*		src = NULL;
	HeapTuple	tuple;

	tuple = SearchSysCache(PGPACKAGENMNS, PointerGetDatum(pkgname),
								ObjectIdGetDatum(relnamespace), 0, 0);
	if (!HeapTupleIsValid(tuple))
		return NULL;

	tmp = SysCacheGetAttr(PGPACKAGENMNS,
						  tuple,
						  Anum_pg_pkg_privsrc,
						  &isNull);
	if (isNull)
		src = NULL;
	else
		src = TextDatumGetCString(tmp);

	ReleaseSysCache(tuple);
	return src;
}

/*
 * get_package_spec_tuple
 */
char *
get_package_spec_tuple(HeapTuple tuple)
{
	bool 		isNull = false;
	Datum		datum;

	Assert(HeapTupleIsValid(tuple));

	/*
	 * And of course we need the function header text.
	 */
	datum = SysCacheGetAttr(PGPACKAGENMNS, tuple, Anum_pg_pkg_headersrc, &isNull);
	if (isNull)
		return NULL;

	return TextDatumGetCString(datum);
}

/*
 * get_package_priv_spec_tuple
 */
char *
get_package_priv_spec_tuple(HeapTuple tuple)
{
	bool 		isNull = false;
	Datum		datum;

	Assert(HeapTupleIsValid(tuple));

	datum = SysCacheGetAttr(PGPACKAGENMNS,
						  tuple,
						  Anum_pg_pkg_privsrc,
						  &isNull);
	if (isNull)
		return NULL;

	return TextDatumGetCString(datum);
}

/*
 * Get initialization part of a package.
 */
char *
get_package_init_block(Oid pkgId)
{
	bool 		isNull = false;
	Datum		tmp;
	char*		src = NULL;
	HeapTuple	tuple;

	tuple = SearchSysCache(PGPKGOID, ObjectIdGetDatum(pkgId), 0, 0, 0);
	if (!HeapTupleIsValid(tuple))
		return NULL;

	tmp = SysCacheGetAttr(PGPACKAGENMNS,
						  tuple,
						  Anum_pg_pkg_initsrc,
						  &isNull);
	if (isNull)
		src = NULL;
	else
		src = TextDatumGetCString(tmp);
	ReleaseSysCache(tuple);

	return src;
}

/*
 * If a package exists.
 */
bool
package_exists(const char *pkgname, Oid relnamespace)
{
	HeapTuple	tuple;

	tuple = SearchSysCache(PGPACKAGENMNS, CStringGetDatum(pkgname),
									ObjectIdGetDatum(relnamespace), 0, 0);
	if (HeapTupleIsValid(tuple))
		ReleaseSysCache(tuple);

	return HeapTupleIsValid(tuple);
}

/*
 * Get a package name
 */
char *
get_package_name_by_id(Oid pkgId)
{
	char*		pkgname = NULL;
	HeapTuple	tuple;
	Form_pg_package	pkgdata;

	tuple = SearchSysCache(PGPKGOID, ObjectIdGetDatum(pkgId), 0, 0, 0);
	if (!HeapTupleIsValid(tuple))
		elog(ERROR, "failed to get package name for %u", pkgId);

	pkgdata = (Form_pg_package) GETSTRUCT(tuple);
	pkgname = pstrdup(NameStr(pkgdata->pkgname));

	ReleaseSysCache(tuple);

	return pkgname;
}

/*
 * Get a package namespace
 */
char *
get_package_ns_name_by_id(Oid pkgId)
{
	char*		nsname = NULL;
	HeapTuple	tuple;
	Form_pg_package pkgdata;

	tuple = SearchSysCache(PGPKGOID, ObjectIdGetDatum(pkgId), 0, 0, 0);
	if (!HeapTupleIsValid(tuple))
		elog(ERROR, "failed to get package name for %u", pkgId);

	pkgdata = (Form_pg_package) GETSTRUCT(tuple);
	nsname = get_namespace_name(pkgdata->pkgnamespace);

	ReleaseSysCache(tuple);

	return nsname;
}
/*
 * get_pkgId_by_namespace
 *    Get package Oid for given namespace.
 */
Oid
get_pkgId_by_namespace(Oid ns)
{
	Oid		pkgId = InvalidOid;
	HeapTuple	tuple;

	tuple = SearchSysCache(PGPKGCRRNS, ObjectIdGetDatum(ns), 0, 0, 0);
	if (!HeapTupleIsValid(tuple))
		return InvalidOid;

	pkgId = HeapTupleGetOid(tuple);
	ReleaseSysCache(tuple);

	return pkgId;
}

bool
package_function_is_privated(Oid pkgId, Oid funcid)
{
	HeapTuple	tuple;
	bool	isNull;
	Datum	funcs;
	ArrayType  *arr;
	int		nfuncs;
	Oid		*oid_arr;
	int		i = 0;

	tuple = SearchSysCache(PGPKGOID, ObjectIdGetDatum(pkgId), 0, 0, 0);
	if (!HeapTupleIsValid(tuple))
		return false;

	funcs = SysCacheGetAttr(PGPKGOID, tuple,
									 Anum_pg_pkg_func,
									 &isNull);
	if (isNull)
	{
		/* No public procedure/function */
		ReleaseSysCache(tuple);
		return true;
	}

	arr = DatumGetArrayTypeP(funcs);
	nfuncs = ARR_DIMS(arr)[0];
	if (ARR_NDIM(arr) != 1 || nfuncs < 0 || ARR_HASNULL(arr) ||
			ARR_ELEMTYPE(arr) != OIDOID)
		elog(ERROR, "package public functions is not a 1-D Oid array");

	oid_arr = (Oid *) ARR_DATA_PTR(arr);
	for (; i < nfuncs; i++)
	{
		if (oid_arr[i] == funcid)
			break;
	}

	ReleaseSysCache(tuple);

	if (i == nfuncs) /* not found in public function */
		return true;
	return false;
}

/*
 * Return Oid of package a function contained in
 */
Oid
get_pkgId_by_function(Oid funcId)
{
	Oid		nspId;

	if (funcId == InvalidOid)
		return InvalidOid;

	nspId = get_func_namespace(funcId);
	if (nspId == InvalidOid)
		elog(ERROR, "failed to lookup function %u", funcId);

	return get_pkgId_by_namespace(nspId);
}

Oid
get_pkgId_by_typoid(Oid typoid)
{
	Oid			typnamespace = InvalidOid;
	HeapTuple	typtup;

	if (!OidIsValid(typoid))
		return InvalidOid;

	typtup = SearchSysCache1(TYPEOID, ObjectIdGetDatum(typoid));
	if (!HeapTupleIsValid(typtup))
		return InvalidOid;	/* keep quiet */
	typnamespace = ((Form_pg_type) GETSTRUCT(typtup))->typnamespace;
	ReleaseSysCache(typtup);

	return get_pkgId_by_namespace(typnamespace);
}

/*
 * Check whether the type belongs to the package
 */
bool
type_belong_to_pkg(Oid typoid, Oid pkgoid)
{
	Oid			typnamespace = InvalidOid;
	HeapTuple	typtup;

	if (!OidIsValid(typoid) || !OidIsValid(pkgoid))
		return false;

	typtup = SearchSysCache1(TYPEOID, ObjectIdGetDatum(typoid));
	if (!HeapTupleIsValid(typtup))
		return false;	/* keep quiet */
	typnamespace = ((Form_pg_type) GETSTRUCT(typtup))->typnamespace;
	ReleaseSysCache(typtup);

	if (get_pkgId_by_namespace(typnamespace) == pkgoid)
		return true;

	return false;
}

/*
 * pg_pkg_ownercheck
 */
bool
pg_pkg_ownercheck(Oid pkgoid, Oid roleid)
{
	HeapTuple	tuple;
	Oid			ownerId;

	/* Superusers bypass all permission checking. */
	if (superuser_arg(roleid))
		return true;

	tuple = SearchSysCache1(PGPKGOID, ObjectIdGetDatum(pkgoid));
	if (!HeapTupleIsValid(tuple))
		elog(ERROR, "package with OID %u does not exist", pkgoid);

	ownerId = ((Form_pg_package) GETSTRUCT(tuple))->pkgowner;

	ReleaseSysCache(tuple);

	return has_privs_of_role(roleid, ownerId);
}

Oid
pg_pkg_secdef_ownerid(Oid pkgoid)
{
	HeapTuple	tuple;
	Oid			ownerId;
	bool		secdef = false;

	if (!OidIsValid(pkgoid))
		return InvalidOid;

	tuple = SearchSysCache1(PGPKGOID, ObjectIdGetDatum(pkgoid));
	if (!HeapTupleIsValid(tuple))
		elog(ERROR, "package with OID %u does not exist", pkgoid);

	secdef = ((Form_pg_package) GETSTRUCT(tuple))->pkgsecdef;
	ownerId = ((Form_pg_package) GETSTRUCT(tuple))->pkgowner;

	ReleaseSysCache(tuple);

	if (secdef)
		return ownerId;
	
	return InvalidOid;
}

/*
 * Create a package specification.
 */
ObjectAddress
PackageCreate(const char *pkgname,
					Oid pkgnamens,
					Oid pkgcrrndsns,
					Oid pkgowner,
					Oid pkglanguage,
					bool pkgeditionable,
					bool pkgseriallyreuseable,
					char *pkgcollation,
					int pkgaccesstype,
					Oid pkgaccessobject,
					bool pkgissystem,
					char *pkgheadersrc,
					char *func_spec,
					Datum pkgconfig,
					bool replace,
					float4 procost,
					float4 prorows,
					bool secdef)

{
	Oid			retval;
	Acl		   *proacl = NULL;
	Relation	rel;
	HeapTuple	tup;
	HeapTuple	oldtup;

	bool		nulls[Natts_pg_pkg];
	Datum		values[Natts_pg_pkg];
	bool		replaces[Natts_pg_pkg];

	NameData	pkgdname;
	NameData	collation;

	TupleDesc	tupDesc;
	bool		is_update;

	ObjectAddress   myself;
	ObjectAddress	referenced;
	int				i;
	/*
	 * sanity checks
	 */
	Assert(PointerIsValid(pkgheadersrc));

	for (i = 0; i < Natts_pg_pkg; ++i)
	{
		nulls[i]    = false;
		values[i]   = (Datum) 0;
		replaces[i] = true;
	}

	namestrcpy(&pkgdname, pkgname);
	namestrcpy(&collation, pkgcollation);

	values[Anum_pg_pkg_name - 1] = NameGetDatum(&pkgdname);
	values[Anum_pg_pkg_namespace - 1] = ObjectIdGetDatum(pkgnamens);
	values[Anum_pg_pkg_crrspndingns - 1] = ObjectIdGetDatum(pkgcrrndsns);
	values[Anum_pg_pkg_owner - 1] = ObjectIdGetDatum(pkgowner);
	values[Anum_pg_pkg_lang - 1] = ObjectIdGetDatum(pkglanguage);
	values[Anum_pg_pkg_cost - 1] = Float4GetDatum(procost);
	values[Anum_pg_pkg_rows - 1] = Float4GetDatum(prorows);
	values[Anum_pg_pkg_editionable - 1] = BoolGetDatum(pkgeditionable);
	values[Anum_pg_pkg_seriallyreuseable - 1] = BoolGetDatum(pkgseriallyreuseable);
	values[Anum_pg_pkg_collation - 1] = NameGetDatum(&collation);
	values[Anum_pg_pkg_accesstype - 1] = UInt16GetDatum(pkgaccesstype);
	values[Anum_pg_pkg_accessobject - 1] = ObjectIdGetDatum(pkgaccessobject);
	values[Anum_pg_pkg_issystem - 1] = BoolGetDatum(pkgissystem);
	values[Anum_pg_pkg_secdef - 1] = BoolGetDatum(secdef);
	values[Anum_pg_pkg_headersrc - 1] = CStringGetTextDatum(pkgheadersrc);

	nulls[Anum_pg_pkg_bodysrc - 1] = true;
	nulls[Anum_pg_pkg_privsrc - 1] = true;
	nulls[Anum_pg_pkg_procdefsrc - 1] = true;
	nulls[Anum_pg_pkg_initsrc - 1] = true;

	if (pkgheadersrc)
		values[Anum_pg_pkg_headersrc - 1] = CStringGetTextDatum(pkgheadersrc);
	else
		nulls[Anum_pg_pkg_headersrc - 1] = true;

	if (func_spec)
		values[Anum_pg_pkg_funcspec - 1] = CStringGetTextDatum(func_spec);
	else
		nulls[Anum_pg_pkg_funcspec - 1] = true;

	if (pkgconfig != PointerGetDatum(NULL))
		values[Anum_pg_pkg_config - 1] = pkgconfig;
	else
		nulls[Anum_pg_pkg_config - 1] = true;

	/* Always update or set public function to NULL */
	nulls[Anum_pg_pkg_func - 1] = true;
	replaces[Anum_pg_pkg_func - 1] = true;

	/* pkg acl will be determined later */
	rel = heap_open(PackageRelationId, RowExclusiveLock);
	tupDesc = RelationGetDescr(rel);

	/* Check for pre-existing definition */
	oldtup = SearchSysCache2(PGPACKAGENMNS,
							 CStringGetDatum(pkgname),
							 ObjectIdGetDatum(pkgnamens));

	if (HeapTupleIsValid(oldtup))
	{
		if (!replace)
			ereport(ERROR,
					(errcode(ERRCODE_DUPLICATE_FUNCTION),
					 errmsg("package \"%s\" already exists with the same name",
							pkgname)));

		if (!pg_pkg_ownercheck(HeapTupleGetOid(oldtup), GetUserId()))
			aclcheck_error(ACLCHECK_NOT_OWNER, ACL_KIND_PACKAGE, pkgname);

		/*
		 * Do not change existing ownership or permissions, either.  Note
		 * dependency-update code below has to agree with this decision.
		 */
		replaces[Anum_pg_pkg_owner - 1] = false;
		replaces[Anum_pg_pkg_acl - 1]   = false;

		/* Okay, do it... */
		tup = heap_modify_tuple(oldtup, tupDesc, values, nulls, replaces);
		CatalogTupleUpdate(rel, &tup->t_self, tup);

		ReleaseSysCache(oldtup);
		is_update = true;
	}
	else
	{
		/* Creating a new procedure */

		/* First, get default permissions and set up proacl */
		proacl = get_user_default_acl(ACL_OBJECT_FUNCTION, pkgowner,
									  pkgnamens);
		if (proacl != NULL)
			values[Anum_pg_pkg_acl - 1] = PointerGetDatum(proacl);
		else
			nulls[Anum_pg_pkg_acl - 1] = true;

		tup = heap_form_tuple(tupDesc, values, nulls);
		CatalogTupleInsert(rel, tup);
		is_update = false;
	}

	retval = HeapTupleGetOid(tup);

	/*
	 * Create dependencies for the new package.  If we are updating an
	 * existing package, first delete any existing pg_depend entries.
	 * (However, since we are not changing ownership or permissions, the
	 * shared dependencies do *not* need to change, and we leave them alone.)
	 */
	if (is_update)
		deleteDependencyRecordsFor(PackageRelationId, retval, true);

	myself.classId = PackageRelationId;
	myself.objectId = retval;
	myself.objectSubId = 0;

	if (IsInplaceUpgrade && myself.objectId < FirstBootstrapObjectId && !is_update)
	{
		recordPinnedDependency(&myself);
	}
	else
	{
		/* dependency on namespace */
		referenced.classId = NamespaceRelationId;
		referenced.objectId = pkgnamens;
		referenced.objectSubId = 0;
		recordDependencyOn(&myself, &referenced, DEPENDENCY_NORMAL);

		/* dependency on implementation language */
		referenced.classId = LanguageRelationId;
		referenced.objectId = pkglanguage;
		referenced.objectSubId = 0;
		recordDependencyOn(&myself, &referenced, DEPENDENCY_NORMAL);

		/* corresponding namespace dependency on us*/
		referenced.classId = NamespaceRelationId;
		referenced.objectId = pkgcrrndsns;
		referenced.objectSubId = 0;
		recordDependencyOn(&referenced, &myself, DEPENDENCY_INTERNAL);

		/* dependency on owner */
		if (!is_update)
			recordDependencyOnOwner(PackageRelationId, retval, pkgowner);
	}

	/* dependency on any roles mentioned in ACL */
	if (!is_update && proacl != NULL)
	{
		int			nnewmembers;
		Oid		   *newmembers;

		nnewmembers = aclmembers(proacl, &newmembers);
		updateAclDependencies(PackageRelationId, retval, 0,
							  pkgowner,
							  0, NULL,
							  nnewmembers, newmembers);
	}

	/* dependency on extension */
	recordDependencyOnCurrentExtension(&myself, is_update);

	heap_freetuple(tup);

	/* Post creation hook for new function */
	InvokeObjectPostCreateHook(PackageRelationId, retval, 0);

	heap_close(rel, RowExclusiveLock);

	CommandCounterIncrement();

	return myself;
}

ObjectAddress
PackageBodyCreate(const char *pkgname, Oid pkgnamens, Oid pkgcrrndsns,
						Oid pkgowner, Oid pkglanguage, char *pkgbodysrc,
						char *pkgvarsrc, char *pkgprocsrc, char *pkginitsrc,
						Datum pkgconfig)

{
	Oid			retval;
	Relation	rel;
	HeapTuple	tup;
	HeapTuple	oldtup;

	bool		nulls[Natts_pg_pkg];
	Datum		values[Natts_pg_pkg];
	bool		replaces[Natts_pg_pkg];
	TupleDesc	tupDesc;

	ObjectAddress   myself;
	ObjectAddress	referenced;
	int				i;

	/*
	 * sanity checks
	 */
	Assert(PointerIsValid(pkgbodysrc));

	for (i = 0; i < Natts_pg_pkg; ++i)
	{
		nulls[i]    = false;
		values[i]   = (Datum) 0;
		replaces[i] = false;
	}

	/* need to update body src */
	values[Anum_pg_pkg_bodysrc - 1]   = CStringGetTextDatum(pkgbodysrc);
	replaces[Anum_pg_pkg_bodysrc - 1] = true;

	/* need to update var src */
	if (pkgvarsrc)
	{
		values[Anum_pg_pkg_privsrc - 1] = CStringGetTextDatum(pkgvarsrc);
		replaces[Anum_pg_pkg_privsrc - 1] = true;
	}
	else
		nulls[Anum_pg_pkg_privsrc - 1] = true;

	if (pkgprocsrc)
	{
		values[Anum_pg_pkg_procdefsrc - 1] = CStringGetTextDatum(pkgprocsrc);
		replaces[Anum_pg_pkg_procdefsrc - 1] = true;
	}
	else
		nulls[Anum_pg_pkg_procdefsrc - 1] = true;

	if (pkginitsrc)
	{
		values[Anum_pg_pkg_initsrc - 1] = CStringGetTextDatum(pkginitsrc);
		replaces[Anum_pg_pkg_initsrc - 1] = true;
	}
	else
		nulls[Anum_pg_pkg_initsrc - 1] = true;

	if (pkgconfig != PointerGetDatum(NULL))
	{
		values[Anum_pg_pkg_config - 1] = pkgconfig;
		replaces[Anum_pg_pkg_config - 1] = true;
	}
	else
		nulls[Anum_pg_pkg_config - 1] = true;

	/* pkg acl will be determined later */
	rel = heap_open(PackageRelationId, RowExclusiveLock);
	tupDesc = RelationGetDescr(rel);

	/* Check for pre-existing definition */
	oldtup = SearchSysCache2(PGPACKAGENMNS,
							 CStringGetDatum(pkgname),
							 ObjectIdGetDatum(pkgnamens));
	if (HeapTupleIsValid(oldtup))
	{
		if (!pg_pkg_ownercheck(HeapTupleGetOid(oldtup), GetUserId()))
			aclcheck_error(ACLCHECK_NOT_OWNER, ACL_KIND_PACKAGE, pkgname);

		/* Okay, do it... */
		tup = heap_modify_tuple(oldtup, tupDesc, values, nulls, replaces);
		CatalogTupleUpdate(rel, &tup->t_self, tup);

		ReleaseSysCache(oldtup);
	}
	else
		ereport(ERROR,
					(errcode(ERRCODE_DUPLICATE_FUNCTION),
					 errmsg("package does not exist with name \"%s\"",
							pkgname)));

	retval = HeapTupleGetOid(tup);

	/*
	 * Create dependencies for the new package.  If we are updating an
	 * existing package, first delete any existing pg_depend entries.
	 * (However, since we are not changing ownership or permissions, the
	 * shared dependencies do *not* need to change, and we leave them alone.)
	 */
	deleteDependencyRecordsFor(PackageRelationId, retval, true);

	myself.classId = PackageRelationId;
	myself.objectId = retval;
	myself.objectSubId = 0;

	/* dependency on namespace */
	referenced.classId = NamespaceRelationId;
	referenced.objectId = pkgnamens;
	referenced.objectSubId = 0;
	recordDependencyOn(&myself, &referenced, DEPENDENCY_NORMAL);

	/* dependency on implementation language */
	referenced.classId = LanguageRelationId;
	referenced.objectId = pkglanguage;
	referenced.objectSubId = 0;
	recordDependencyOn(&myself, &referenced, DEPENDENCY_NORMAL);

	/* corresponding namespace dependency on us*/
	referenced.classId = NamespaceRelationId;
	referenced.objectId = pkgcrrndsns;
	referenced.objectSubId = 0;
	recordDependencyOn(&referenced, &myself, DEPENDENCY_INTERNAL);


	/* dependency on extension */
	recordDependencyOnCurrentExtension(&myself, true);

	heap_freetuple(tup);

	/* Post creation hook for new function */
	InvokeObjectPostCreateHook(PackageRelationId, retval, 0);

	heap_close(rel, RowExclusiveLock);

	CommandCounterIncrement();

	return myself;
}

/*
 * Drop a package.
 */
void
PackageDrop(const char *pkgname, Oid pkgnamens, Oid pkgcrrndsns)
{
	HeapScanDesc scandesc;
	Relation	rel;
	HeapTuple	tuple;
	ScanKeyData entry[2];
	Oid			packaegoid;

	/*
	 * Find the target tuple
	 */
	rel = heap_open(PackageRelationId, RowExclusiveLock);

	ScanKeyInit(&entry[0],
				Anum_pg_pkg_name,
				BTEqualStrategyNumber, F_NAMEEQ,
				CStringGetDatum(pkgname));

	ScanKeyInit(&entry[1],
				Anum_pg_pkg_namespace,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(pkgnamens));

	scandesc = heap_beginscan_catalog(rel, 1, entry);
	tuple = heap_getnext(scandesc, ForwardScanDirection);

	if (!HeapTupleIsValid(tuple))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("package \"%s\" does not exist",
						pkgname)));

	packaegoid = HeapTupleGetOid(tuple);

	/* Must be tablespace owner */
	if (!pg_tablespace_ownercheck(packaegoid, GetUserId()))
		aclcheck_error(ACLCHECK_NOT_OWNER, ACL_KIND_PACKAGE,
					   pkgname);

	if (((Form_pg_package) GETSTRUCT(tuple))->pkgissystem)
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("can not drop system package \"%s\"",
						pkgname)));

	/*
	 * Remove the pg_tablespace tuple (this will roll back if we fail below)
	 */
	CatalogTupleDelete(rel, &tuple->t_self);

	heap_endscan(scandesc);

	/*
	 * Remove dependency on owner.
	 */
	deleteDependencyRecordsFor(PackageRelationId, packaegoid, true);

	/* remove types and functions inside the package. */

	/* release the lock. */
	heap_close(rel, RowExclusiveLock);
}

/*
 * Add namespace named by package name if any.
 */
void
push_current_package_namespace(void)
{
	OverrideSearchPath *overridePath = NULL;
	Oid		members_nsp;

	if (CallingPackageId == InvalidOid)
		return;

	members_nsp = get_package_cornsp(CallingPackageId);
	if (members_nsp != InvalidOid)
	{
		overridePath = GetOverrideSearchPath(CurrentMemoryContext);
		overridePath->schemas = lcons_oid(members_nsp, overridePath->schemas);
		PushOverrideSearchPath(overridePath);
	}
}

/*
 * Pop top namespace.
 */
void
pop_current_package_namespace(void)
{
	if (CallingPackageId == InvalidOid) /* If still in package calling */
		return;

	/* If empty stack, maybe rollback called */
	if (!NamespaceStackEmpty())
		PopOverrideSearchPath();
}

bool
get_pkg_sec_def(Oid pkgId)
{
	bool 		is_null = false;
	Datum		tmp;
	bool		sec = false;
	HeapTuple	tuple;

	tuple = SearchSysCache(PGPKGOID, ObjectIdGetDatum(pkgId), 0, 0, 0);

	if (!HeapTupleIsValid(tuple))
		return false;

	tmp = SysCacheGetAttr(PGPACKAGENMNS, tuple, Anum_pg_pkg_secdef, &is_null);

	if (is_null)
		sec = false;
	else
		sec = DatumGetBool(tmp);

	ReleaseSysCache(tuple);
	return sec;
}