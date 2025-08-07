/*-------------------------------------------------------------------------
 *
 * pg_compile.c
 *	  routines to support manipulation of the pg_compile relation
 * Portions Copyright (c) 2021, Tencent.com.
 *
 *
 * IDENTIFICATION
 *	  src/backend/catalog/pg_compile.c
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
#include "catalog/pg_compile.h"
#include "catalog/pg_package.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_type.h"
#include "utils/builtins.h"
#include "utils/rel.h"
#include "utils/syscache.h"

static void
CreateCompile(Oid clssid, Oid objid)
{
	bool	nulls[Natts_pg_compile];
	Datum	values[Natts_pg_compile];
	HeapTuple	tup;
	Relation	rel;
	int		i;

	for (i = 0; i < Natts_pg_compile; i++)
		nulls[i] = true;

	values[Natts_pg_compile_compclassid - 1] = clssid;
	nulls[Natts_pg_compile_compclassid - 1] = false;

	values[Natts_pg_compile_compobjid - 1] = objid;
	nulls[Natts_pg_compile_compobjid - 1] = false;

	values[Natts_pg_compile_comptype - 1] = PG_COMP_INTERPRETED;
	nulls[Natts_pg_compile_comptype - 1] = false;

	values[Natts_pg_compile_compvalid - 1] = BoolGetDatum(true);
	nulls[Natts_pg_compile_compvalid - 1] = false;

	tup = SearchSysCache2(COMPILEOBJNAME,
							 ObjectIdGetDatum(clssid),
							 ObjectIdGetDatum(objid));
	if (HeapTupleIsValid(tup))
	{
		ReleaseSysCache(tup);
		return;
	}

	rel = heap_open(CompileRelationId, RowExclusiveLock);

	tup = heap_form_tuple(RelationGetDescr(rel), values, nulls);
	CatalogTupleInsert(rel, tup);
	heap_freetuple(tup);

	heap_close(rel, NoLock);

	CommandCounterIncrement();
}

/*
 * Record package and base type dependencies.
 */
static void
record_type_dependency_for_pkg(Oid pkgid, List *typeIds)
{
	ObjectAddress	myself;
	ObjectAddress	referenced;
	ListCell		*l;

	myself.classId = PackageRelationId;
	myself.objectId = pkgid;
	myself.objectSubId = 0;

	referenced.classId = TypeRelationId;
	referenced.objectSubId = 0;
	foreach(l, typeIds)
	{
		referenced.objectId = lfirst_oid(l);
		recordDependencyOn(&myself, &referenced, DEPENDENCY_NORMAL);
	}
}

/*
 * CreateOrUpdateFuncArgCompile
 *   Save UDT type args into pg_compile.
 */
void
CreateOrUpdateFuncArgCompile(Oid clssid, Oid objid, Datum argtype)
{
	bool	nulls[Natts_pg_compile];
	Datum	values[Natts_pg_compile];
	bool	replaces[Natts_pg_compile];
	ArrayType	*array;
	int		i = 0;
	HeapTuple	oldtup;
	HeapTuple	tup;
	Relation	rel;
	int			cnt;

	CreateCompile(clssid, objid);

	for (; i < Natts_pg_compile; i++)
	{
		nulls[i] = true;
		replaces[i] = false;
	}

	array = DatumGetArrayTypeP(argtype);
	cnt = ArrayGetNItems(ARR_NDIM(array), ARR_DIMS(array));

	values[Natts_pg_compile_udnargs - 1] = Int32GetDatum(cnt);
	nulls[Natts_pg_compile_udnargs - 1] = false;
	replaces[Natts_pg_compile_udnargs - 1] = true;

	values[Natts_pg_compile_udargtype - 1] = argtype;
	nulls[Natts_pg_compile_udargtype - 1] = false;
	replaces[Natts_pg_compile_udargtype - 1] = true;

	oldtup = SearchSysCache2(COMPILEOBJNAME,
							 ObjectIdGetDatum(clssid),
							 ObjectIdGetDatum(objid));
	if (!HeapTupleIsValid(oldtup))
		elog(ERROR, "failed to lookup compile cache");

	rel = heap_open(CompileRelationId, RowExclusiveLock);
	tup = heap_modify_tuple(oldtup, RelationGetDescr(rel), values, nulls, replaces);

	CatalogTupleUpdate(rel, &tup->t_self, tup);
	ReleaseSysCache(oldtup);

	heap_close(rel, NoLock);

	CommandCounterIncrement();
}

void
CreateOrUpdateFuncRetCompile(Oid clssid, Oid objid, Datum type)
{
	bool	nulls[Natts_pg_compile];
	Datum	values[Natts_pg_compile];
	bool	replaces[Natts_pg_compile];
	int		i = 0;
	HeapTuple	oldtup;
	HeapTuple	tup;
	Relation	rel;

	CreateCompile(clssid, objid);

	for (; i < Natts_pg_compile; i++)
	{
		nulls[i] = true;
		replaces[i] = false;
	}

	values[Natts_pg_compile_udrettype - 1] = type;
	nulls[Natts_pg_compile_udrettype - 1] = false;
	replaces[Natts_pg_compile_udrettype - 1] = true;

	oldtup = SearchSysCache2(COMPILEOBJNAME,
							 ObjectIdGetDatum(clssid),
							 ObjectIdGetDatum(objid));
	if (!HeapTupleIsValid(oldtup))
		elog(ERROR, "failed to lookup compile cache");

	rel = heap_open(CompileRelationId, RowExclusiveLock);
	tup = heap_modify_tuple(oldtup, RelationGetDescr(rel), values, nulls, replaces);

	CatalogTupleUpdate(rel, &tup->t_self, tup);
	ReleaseSysCache(oldtup);

	heap_close(rel, NoLock);

	CommandCounterIncrement();
}

/*
 * ConstructFuncCompileTuple
 *
 *  Construct a tuple of pg_compile corresponding to function parameters and return value.
 */
HeapTuple
ConstructFuncCompileTuple(ArrayType *argtype, char *rettype)
{
	int			i = 0;
	bool		nulls[Natts_pg_compile];
	Datum		values[Natts_pg_compile];
	HeapTuple	tup;
	Relation	rel;

	for (; i < Natts_pg_compile; i++)
		nulls[i] = true;

	if (argtype != NULL)
	{
		int	cnt;

		cnt = ArrayGetNItems(ARR_NDIM(argtype), ARR_DIMS(argtype));

		values[Natts_pg_compile_udnargs - 1] = Int32GetDatum(cnt);
		nulls[Natts_pg_compile_udnargs - 1] = false;

		values[Natts_pg_compile_udargtype - 1] = PointerGetDatum(argtype);
		nulls[Natts_pg_compile_udargtype - 1] = false;
	}

	if (rettype != NULL)
	{
		values[Natts_pg_compile_udrettype - 1] = CStringGetTextDatum(rettype);
		nulls[Natts_pg_compile_udrettype - 1] = false;
	}

	rel = heap_open(CompileRelationId, AccessShareLock);
	tup = heap_form_tuple(RelationGetDescr(rel), values, nulls);
	heap_close(rel, NoLock);

	return tup;
}

/*
 * Save pacakge compiled UDT name
 */
void
CreateOrUpdatePkgCompile(Oid pkgid, List *udt, List *basetypid, int comp_pkg)
{
	bool	nulls[Natts_pg_compile];
	Datum	values[Natts_pg_compile];
	bool	replaces[Natts_pg_compile];
	int		i = 0;
	HeapTuple	oldtup;
	HeapTuple	tup;
	Relation	rel;
	ListCell	*l;
	Datum		*arr_datums;
	ArrayType	*udt_arr;
	ArrayType	*basetyp_arr;

	DiscardPkgCompile(pkgid, comp_pkg);

	if (list_length(udt) == 0)
		return;

	CreateCompile(PackageRelationId, pkgid);

	for (; i < Natts_pg_compile; i++)
	{
		nulls[i] = true;
		replaces[i] = false;
	}

	Assert(list_length(udt) == list_length(basetypid));

	/* Make a name list into vector */
	i = 0;
	arr_datums = palloc0(list_length(udt) * sizeof(Datum));
	foreach(l, udt)
	{
		arr_datums[i] = CStringGetTextDatum((char *) lfirst(l));
		i++;
	}
	udt_arr = construct_array(arr_datums, i, TEXTOID, -1, false, 'i');

	i = 0;
	foreach(l, basetypid)
	{
		arr_datums[i] = ObjectIdGetDatum(lfirst_oid(l));
		i++;
	}
	basetyp_arr = construct_array(arr_datums, i, OIDOID,
												sizeof(Oid), true, 'i');
	if (comp_pkg == CompilePkgPubSection)
	{
		values[Natts_pg_compile_compudt - 1] = PointerGetDatum(udt_arr);
		nulls[Natts_pg_compile_compudt - 1] = false;
		replaces[Natts_pg_compile_compudt - 1] = true;

		values[Natts_pg_compile_compudtbase - 1] = PointerGetDatum(basetyp_arr);
		nulls[Natts_pg_compile_compudtbase - 1] = false;
		replaces[Natts_pg_compile_compudtbase - 1] = true;
	}
	else
	{
		Assert(comp_pkg == CompilePkgPrivSection);

		values[Natts_pg_compile_compudt_priv - 1] = PointerGetDatum(udt_arr);
		nulls[Natts_pg_compile_compudt_priv - 1] = false;
		replaces[Natts_pg_compile_compudt_priv - 1] = true;

		values[Natts_pg_compile_compudopentenbase_priv - 1] = PointerGetDatum(basetyp_arr);
		nulls[Natts_pg_compile_compudopentenbase_priv - 1] = false;
		replaces[Natts_pg_compile_compudopentenbase_priv - 1] = true;
	}

	oldtup = SearchSysCache2(COMPILEOBJNAME,
							 ObjectIdGetDatum(PackageRelationId),
							 ObjectIdGetDatum(pkgid));
	if (!HeapTupleIsValid(oldtup))
		elog(ERROR, "failed to lookup compile cache");

	rel = heap_open(CompileRelationId, RowExclusiveLock);
	tup = heap_modify_tuple(oldtup, RelationGetDescr(rel), values, nulls, replaces);

	CatalogTupleUpdate(rel, &tup->t_self, tup);
	ReleaseSysCache(oldtup);

	record_type_dependency_for_pkg(pkgid, basetypid);

	heap_close(rel, NoLock);

	CommandCounterIncrement();
}

/*
 * Make compile information as NULL, but not delete the whole tuple.
 */
void
DiscardPkgCompile(Oid pkgoid, int comp_pkg)
{
	bool	nulls[Natts_pg_compile];
	Datum	values[Natts_pg_compile];
	bool	replaces[Natts_pg_compile];
	HeapTuple	oldtup;
	HeapTuple	tup;
	Relation	rel;
	int		i = 0;

	for (; i < Natts_pg_compile; i++)
	{
		nulls[i] = true;
		replaces[i] = false;
	}

	if (comp_pkg == CompilePkgPubSection)
	{
		values[Natts_pg_compile_compudt - 1] = (Datum) 0;
		nulls[Natts_pg_compile_compudt - 1] = true;
		replaces[Natts_pg_compile_compudt - 1] = true;

		values[Natts_pg_compile_compudtbase - 1] = (Datum) 0;
		nulls[Natts_pg_compile_compudtbase - 1] = true;
		replaces[Natts_pg_compile_compudtbase - 1] = true;
	}
	else
	{
		values[Natts_pg_compile_compudt_priv - 1] = (Datum) 0;
		nulls[Natts_pg_compile_compudt_priv - 1] = true;
		replaces[Natts_pg_compile_compudt_priv - 1] = true;

		values[Natts_pg_compile_compudopentenbase_priv - 1] = (Datum) 0;
		nulls[Natts_pg_compile_compudopentenbase_priv - 1] = true;
		replaces[Natts_pg_compile_compudopentenbase_priv - 1] = true;
	}

	oldtup = SearchSysCache2(COMPILEOBJNAME,
							 ObjectIdGetDatum(PackageRelationId),
							 ObjectIdGetDatum(pkgoid));
	if (!HeapTupleIsValid(oldtup))
		return;

	rel = heap_open(CompileRelationId, RowExclusiveLock);
	tup = heap_modify_tuple(oldtup, RelationGetDescr(rel), values, nulls, replaces);

	CatalogTupleUpdate(rel, &tup->t_self, tup);
	ReleaseSysCache(oldtup);

	heap_close(rel, NoLock);

	CommandCounterIncrement();
}

void
DropCompile(Oid clssid, Oid objid)
{
	HeapTuple	tup;
	Relation	relation;

	tup = SearchSysCache2(COMPILEOBJNAME,
							 ObjectIdGetDatum(clssid),
							 ObjectIdGetDatum(objid));
	if (!HeapTupleIsValid(tup))
		return;

	relation = heap_open(CompileRelationId, RowExclusiveLock);
	CatalogTupleDelete(relation, &tup->t_self);
	ReleaseSysCache(tup);
	heap_close(relation, RowExclusiveLock);

	CommandCounterIncrement();
}

/*
 * Get actual UDT typename of a function.
 */
bool
GetFuncitonActualRetType(Oid funcid, char **udtname)
{
	HeapTuple	tup;
	Datum		val;
	bool		isnull;

	tup = SearchSysCache2(COMPILEOBJNAME,
							 ObjectIdGetDatum(ProcedureRelationId),
							 ObjectIdGetDatum(funcid));
	if (!HeapTupleIsValid(tup))
		return false;

	val = SysCacheGetAttr(COMPILEOBJNAME, tup, Natts_pg_compile_udrettype, &isnull);
	if (isnull)
	{
		ReleaseSysCache(tup);
		return false;
	}

	*udtname = pstrdup(TextDatumGetCString(val));

	ReleaseSysCache(tup);
	return true;
}

bool
GetFuncitonActualRetTypeByTuple(HeapTuple tup, char **udtname)
{
	Datum		val;
	bool		isnull;

	val = SysCacheGetAttr(COMPILEOBJNAME, tup, Natts_pg_compile_udrettype, &isnull);
	if (isnull)
		return false;
	*udtname = pstrdup(TextDatumGetCString(val));

	return true;
}

/*
 * Get actual function args typename of UDT.
 */
bool
GetFunctionActualUdtType(Oid funcid, int idx, char **udtname)
{
	HeapTuple	tup;
	Datum		val;
	bool		isnull;
#ifdef USE_ASSERT_CHECKING
	int			nargs;
#endif
	ArrayType	*type_arr;
	Datum		*argnames;
	int			i = 0;

	tup = SearchSysCache2(COMPILEOBJNAME,
							 ObjectIdGetDatum(ProcedureRelationId),
							 ObjectIdGetDatum(funcid));
	if (!HeapTupleIsValid(tup))
		return false;

	val = SysCacheGetAttr(COMPILEOBJNAME, tup, Natts_pg_compile_udnargs, &isnull);
	if (isnull || DatumGetInt32(val) == 0 || i >= DatumGetInt32(val))
	{
		ReleaseSysCache(tup);
		return false;
	}
#ifdef USE_ASSERT_CHECKING
	nargs = DatumGetInt32(val);
#endif

	val = SysCacheGetAttr(COMPILEOBJNAME, tup, Natts_pg_compile_udargtype, &isnull);
	if (isnull)
	{
		ReleaseSysCache(tup);
		return false;
	}

	type_arr = DatumGetArrayTypeP(val);
	deconstruct_array(type_arr, TEXTOID, -1, false, 'i', &argnames, NULL, &i);

	Assert(i == nargs);
	*udtname = pstrdup(TextDatumGetCString(argnames[idx]));

	ReleaseSysCache(tup);

	return true;
}

bool
GetFunctionActualUdtTypeByTuple(HeapTuple tup, int idx, char **udtname)
{
	Datum		val;
	bool		isnull;
#ifdef USE_ASSERT_CHECKING
	int			nargs;
#endif
	ArrayType	*type_arr;
	Datum		*argnames;
	int			i = 0;

	val = SysCacheGetAttr(COMPILEOBJNAME, tup, Natts_pg_compile_udnargs, &isnull);
	if (isnull || DatumGetInt32(val) == 0 || i >= DatumGetInt32(val))
		return false;
#ifdef USE_ASSERT_CHECKING
	nargs = DatumGetInt32(val);
#endif

	val = SysCacheGetAttr(COMPILEOBJNAME, tup, Natts_pg_compile_udargtype, &isnull);
	if (isnull)
		return false;

	type_arr = DatumGetArrayTypeP(val);
	deconstruct_array(type_arr, TEXTOID, -1, false, 'i', &argnames, NULL, &i);

	Assert(i == nargs);
	*udtname = pstrdup(TextDatumGetCString(argnames[idx]));

	return true;
}

/*
 * Get name list of UDT for function args.
 */
List *
GetFunctionUDTArgs(Oid funcid)
{
	HeapTuple	tup;
	Datum		val;
	bool		isnull;
	int			nargs;
	ArrayType	*type_arr;
	Datum		*argnames;
	List	*argname = NIL;
	int		i = 0;

	tup = SearchSysCache2(COMPILEOBJNAME,
							 ObjectIdGetDatum(ProcedureRelationId),
							 ObjectIdGetDatum(funcid));
	if (!HeapTupleIsValid(tup))
		return NIL;

	val = SysCacheGetAttr(COMPILEOBJNAME, tup, Natts_pg_compile_udnargs, &isnull);
	if (isnull || DatumGetInt32(val) == 0)
	{
		ReleaseSysCache(tup);
		return NIL;
	}
	nargs = DatumGetInt32(val);

	val = SysCacheGetAttr(COMPILEOBJNAME, tup, Natts_pg_compile_udargtype, &isnull);
	if (isnull)
		elog(ERROR, "empty UDT arguments in functino definition");

	type_arr = DatumGetArrayTypeP(val);
	deconstruct_array(type_arr, TEXTOID, -1, false, 'i', &argnames, NULL, &i);

	Assert(i == nargs);
	for (i = 0; i < nargs; i++)
	{
		StringInfoData	sinfo;

		initStringInfo(&sinfo);
		appendStringInfo(&sinfo, "%s", TextDatumGetCString(argnames[i]));

		argname = lappend(argname, sinfo.data);
	}
	ReleaseSysCache(tup);

	return argname;
}

List *
GetFunctionUDTArgsByTuple(HeapTuple tup)
{
	int			i = 0;
	int			nargs;
	bool		isnull;
	List		*argname = NIL;
	Datum		val;
	Datum		*argnames;
	ArrayType	*type_arr;

	val = SysCacheGetAttr(COMPILEOBJNAME, tup, Natts_pg_compile_udnargs, &isnull);
	if (isnull || DatumGetInt32(val) == 0)
		return NIL;
	nargs = DatumGetInt32(val);

	val = SysCacheGetAttr(COMPILEOBJNAME, tup, Natts_pg_compile_udargtype, &isnull);
	if (isnull)
		elog(ERROR, "empty UDT arguments in functino definition");

	type_arr = DatumGetArrayTypeP(val);
	deconstruct_array(type_arr, TEXTOID, -1, false, 'i', &argnames, NULL, &i);

	Assert(i == nargs);
	for (i = 0; i < nargs; i++)
	{
		StringInfoData	sinfo;

		initStringInfo(&sinfo);
		appendStringInfo(&sinfo, "%s", TextDatumGetCString(argnames[i]));

		argname = lappend(argname, sinfo.data);
	}

	return argname;
}

static Oid
find_udt_type_id(char *typename, Datum name, Datum type)
{
	Datum	*arr_name;
	Oid		*arr_oid;
	int		nname;
	int		i;
	ArrayType	*typ_arr;

	deconstruct_array(DatumGetArrayTypeP(name), TEXTOID, -1, false, 'i', &arr_name, NULL, &nname);

	typ_arr = DatumGetArrayTypeP(type);
	arr_oid = (Oid *) ARR_DATA_PTR(typ_arr);

	i = ArrayGetNItems(ARR_NDIM(typ_arr), ARR_DIMS(typ_arr));
	Assert(nname == i);
	for (i = 0; i < nname; i++)
	{
		if (strcasecmp(typename, TextDatumGetCString(arr_name[i])) == 0)
			return arr_oid[i];
	}

	return InvalidOid;
}

/*
 * GetPackageDefinedTypeId
 *    Get base of UDT defined in package. If 'search_all' is true, will search public
 *  and private definition section.
 */
Oid
GetPackageDefinedTypeId(Oid pkgId, char *typname, bool search_all)
{
	HeapTuple	tup;
	Datum		name_val;
	Datum		typid_val;
	Oid			base_id = InvalidOid;
	bool		isnull;

	tup = SearchSysCache2(COMPILEOBJNAME,
							 ObjectIdGetDatum(PackageRelationId),
							 ObjectIdGetDatum(pkgId));

	if (!HeapTupleIsValid(tup))
		return InvalidOid;

	name_val = SysCacheGetAttr(COMPILEOBJNAME, tup, Natts_pg_compile_compudt, &isnull);
	if (!isnull)
	{
		typid_val = SysCacheGetAttr(COMPILEOBJNAME, tup, Natts_pg_compile_compudtbase, &isnull);
		Assert(!isnull);
		base_id = find_udt_type_id(typname, name_val, typid_val);
	}

	if (base_id != InvalidOid)
	{
		ReleaseSysCache(tup);
		return base_id;
	}

	if (!search_all)
	{
		ReleaseSysCache(tup);
		return base_id;
	}

	name_val = SysCacheGetAttr(COMPILEOBJNAME, tup, Natts_pg_compile_compudt_priv, &isnull);
	if (!isnull)
	{
		typid_val = SysCacheGetAttr(COMPILEOBJNAME, tup, Natts_pg_compile_compudopentenbase_priv, &isnull);
		Assert(!isnull);
		base_id = find_udt_type_id(typname, name_val, typid_val);
	}

	ReleaseSysCache(tup);
	return base_id;
}

/*
 * If the input arguments match udt in package.
 */
bool
CompiledUDTMatched(Oid funcid, List *fargs, void *p_state)
{
	List	*udt_arg;
	List	*pkg_udt_arg;
	ListCell	*l;
	int		i = 0;

	udt_arg = GetFunctionUDTArgs(funcid);
	if (udt_arg == NIL || p_state == NULL)
		return true;

	pkg_udt_arg = get_pkg_dot_udt_typename(get_pkgId_by_function(funcid), udt_arg);
	if (pkg_udt_arg == NIL)
		return true;

	foreach(l, fargs)
	{
		Node	*n = (Node *) lfirst(l);
		Param	*p;
		char	*typename;

		if (!IsA(n, Param))
			continue;

		p = (Param *) n;
		if (p->paramtype != PLUDTOID)
			continue;

		typename = get_package_udt_typename(p, p_state);
		if (typename == NULL)
			return false;

		if (strcasecmp(typename, list_nth(pkg_udt_arg, i)) != 0)
			return false;
		i++;
	}

	return true;
}
