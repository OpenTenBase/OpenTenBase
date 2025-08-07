/*-------------------------------------------------------------------------
 *
 * parse_type.c
 *		handle type operations for parser
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/parser/parse_type.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/htup_details.h"
#include "catalog/namespace.h"
#include "catalog/pg_compile.h"
#include "catalog/pg_type.h"
#include "catalog/pg_class.h"
#include "catalog/pg_package.h"
#include "catalog/pg_synonym.h"
#include "catalog/pg_collation.h"
#include "lib/stringinfo.h"
#include "nodes/makefuncs.h"
#include "parser/parser.h"
#include "parser/parse_type.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"
#include "utils/datetime.h"
#include "utils/formatting.h"
#include "utils/orcl_datetime.h"
#include "nodes/nodeFuncs.h"

#ifdef __OPENTENBASE_C__
#include "parser/parse_coerce.h"
#endif
#include "tsearch/ts_locale.h"
#include "catalog/pg_synonym.h"

static int32 typenameTypeMod(ParseState *pstate, const TypeName *typeName,
				Type typ, bool isReset);
static int32 typenameTypeMods(const TypeName *typeName);

/*
 * For interval operator, pass arg typmod to result for output format
 */
int32
GetIntervalTypmodForOpenTenBaseOraOut(List *args, Oid typid, int32 src_typmod)
{
#define INTERVAL_FORMAT_NUM 3

	ListCell	*lc = NULL;
	Datum		*datums = NULL;
	ArrayType	*arrtypmod = NULL;
	Type		tup = NULL;
	Oid			typmodin = InvalidOid;
	char		*cstr = NULL;
	int			result = src_typmod;
	int			i = 0;

	if (!ORA_MODE || typid != INTERVALOID || src_typmod != -1)
	{
		return result;
	}

	datums = (Datum *) palloc(INTERVAL_FORMAT_NUM * sizeof(Datum));
	foreach(lc, args)
	{
		if (exprType(lfirst(lc)) == INTERVALOID)
		{
			int32 typmod = exprTypmod(lfirst(lc));
			int fields = INTERVAL_RANGE(typmod);
			int	year_mon = (INTERVAL_MASK(YEAR) | INTERVAL_MASK(MONTH));

			if (fields & year_mon)
			{
				cstr = psprintf("%d", INTERVAL_MASK(YEAR) |
											INTERVAL_MASK(MONTH));
				datums[i++] = CStringGetDatum(cstr);
				cstr = psprintf("%d", MAX_INTERVAL_YEAR_PRECISION);
				datums[i++] = CStringGetDatum(cstr);
				cstr = psprintf("%d", INTERVAL_MASK(YEAR));
				datums[i++] = CStringGetDatum(cstr);
				arrtypmod = construct_array(datums, i,
											CSTRINGOID,
											-2, false, 'c');
				tup = (Type) SearchSysCache1(TYPEOID, ObjectIdGetDatum(INTERVALOID));
				typmodin = ((Form_pg_type) GETSTRUCT(tup))->typmodin;
				ReleaseSysCache(tup);
				result = DatumGetInt32(OidFunctionCall1(typmodin,
									PointerGetDatum(arrtypmod)));
				pfree(datums);
				return result;
			}
			else
			{
				int	day_sec = (INTERVAL_MASK(DAY) | INTERVAL_MASK(HOUR) |
								INTERVAL_MASK(MINUTE) | INTERVAL_MASK(SECOND));
				if (!(fields & day_sec))
				{
					elog(ERROR, "Invalid interval format");
				}

				cstr = psprintf("%d", INTERVAL_MASK(DAY) |
										INTERVAL_MASK(HOUR) |
										INTERVAL_MASK(MINUTE) |
										INTERVAL_MASK(SECOND));
				datums[i++] = CStringGetDatum(cstr);
				cstr = psprintf("%d", INTERVAL_MIX_RREC(
											MAX_INTERVAL_DAY_PRECISION,
											MAX_INTERVAL_PRECISION));
				datums[i++] = CStringGetDatum(cstr);
				cstr = psprintf("%d", INTERVAL_MASK(DAY) |
											INTERVAL_MASK(SECOND));
				datums[i++] = CStringGetDatum(cstr);
				arrtypmod = construct_array(datums, i,
											CSTRINGOID,
											-2, false, 'c');
				tup = (Type) SearchSysCache1(TYPEOID, ObjectIdGetDatum(INTERVALOID));
				typmodin = ((Form_pg_type) GETSTRUCT(tup))->typmodin;
				ReleaseSysCache(tup);
				result = DatumGetInt32(OidFunctionCall1(typmodin,
									PointerGetDatum(arrtypmod)));
				pfree(datums);
				return result;
			}
		}
	}
	pfree(datums);
	return result;
}

/*
 * LookupPackageUDT
 *   If the typename is defined in package. Return true if it is a UDT type. Return false
 * if not a UDT type.
 */
bool
LookupPackageUDT(const TypeName *typeName, Oid *pkg_oid, char **udtname,
				 Oid p_pkgid, Oid *act_typid, Oid *o_synoid)
{
	char	*schemaname = NULL;
	char	*pkgname = NULL;
	char	*typname = NULL;
	Oid		pkgId = InvalidOid;
	List	*names = NULL;
	Oid		base_typid;
	Oid		syn_oid = InvalidOid;

	if (typeName->names == NIL || typeName->pct_type || typeName->pct_rowtype)
		return false;

	/* deconstruct the name list */
	names = typeName->names;
	switch (list_length(names))
	{
		case 1:
			/*
			 * If function is from package, use its package name, or return false.
			 */
			if (!OidIsValid(p_pkgid))
				return false;
			schemaname = get_package_ns_name_by_id(p_pkgid);
			pkgname = get_package_name_by_id(p_pkgid);
			typname = pstrdup(strVal(linitial(names)));
			break;
		case 2:
			pkgname = strVal(linitial(names));
			typname = strVal(lsecond(names));
			break;
		case 3:
			schemaname = strVal(linitial(names));
			pkgname = strVal(lsecond(names));
			typname = strVal(lthird(names));
			break;
		default:
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					 errmsg("improper qualified name (too many dotted names): %s",
							NameListToString(names))));
			break;
	}

	if (OidIsValid(p_pkgid) &&
		list_length(names) == 2 &&
		strcmp(pkgname, get_package_name_by_id(p_pkgid)) == 0)
	{
		/*
		 * If the package name of the type is the same as the package name
		 * being created, then matching takes precedence.
		 */
		pkgId = p_pkgid;
	}
	else
		pkgId = get_package_oid(schemaname, pkgname, &syn_oid);

	if (!OidIsValid(pkgId))
	{
		char    *objspc = NULL,
				*objname = NULL,
				*dblink = NULL;

		if (list_length(names) != 2)
			return false;

		/* check if first name is synonym. */
		syn_oid = resolve_synonym(NULL, strVal(linitial(names)), &objspc, &objname, &dblink);
		if (objname != NULL && dblink == NULL)
		{
			pkgId = get_package_oid(objspc, objname, NULL);
			if (!OidIsValid(pkgId))
				return false;
		}
		else
			return false;
	}

	/* Get actual type Oid in package definition */
	base_typid = GetPackageDefinedTypeId(pkgId, typname, p_pkgid != InvalidOid);
	if (base_typid == InvalidOid)
		return false;

	*pkg_oid = pkgId;
	*udtname = NameListToString(typeName->names);
	*act_typid = base_typid;

	if (o_synoid)
		*o_synoid = syn_oid;

	return true;
}

/*
 * LookupPackageVarType
 *
 *   If the typename is defined in package. Return true if it is a UDT type. Return false
 * if not a UDT type.
 */
bool
LookupPackageVarType(const TypeName *typeName, Oid *pkg_oid, char **o_udtname,
							Oid p_pkgid, Oid *act_typid, bool notice)
{
	Oid		base_typid;
	Oid		pkgid = InvalidOid;
	bool	pkg_func = false; /* the function in the package corresponding to p_pkgid */
	char	*udtname = NULL;
	List	*varname = NIL;
	List	*names = NULL;

	if (list_length(typeName->names) <= 0 || !typeName->pct_type)
		return false;

	/* deconstruct the name list */
	names = typeName->names;
	switch (list_length(names))
	{
		case 1:
			{
				/*
				 * If function is from package, use its package name, or return false.
				 */
				pkgid = p_pkgid;
				varname = names;
				break;
			}
		case 2:
			{
				char	*pkgname = NULL;

				/* TODO: Handle synonym.variable cases */
				pkgname = strVal(linitial(names));
				if (OidIsValid(p_pkgid) &&
					strcmp(pkgname, get_package_name_by_id(p_pkgid)) == 0)
				{
					/*
					 * If the package name of the type is the same as the package name
					 * being created, then matching takes precedence.
					 */
					pkgid = p_pkgid;
				}
				else
					pkgid = get_package_oid(NULL, pkgname, NULL);

				if (OidIsValid(pkgid))
				{
					/* package.variable */
					varname = list_copy_tail(names, 1);
				}
				else
				{
					if (!OidIsValid(p_pkgid))
						return false;

					/* variable.field */
					pkgid = p_pkgid;
					varname = names;
				}
				break;
			}
		default:
			{
				char	*name1;
				char	*name2;

				name1 = strVal(linitial(names));
				name2 = strVal(lsecond(names));

				/* try schema.package.variable */
				pkgid = get_package_oid(name1, name2, NULL);
				if (OidIsValid(pkgid))
					varname = list_copy_tail(names, 2);
				else
				{
					/* try package.variable */
					pkgid = get_package_oid(NULL, name1, NULL);

					if (OidIsValid(pkgid))
					{
						varname = list_copy_tail(names, 1);
					}
					else 
					{
						/* variable.field1.field2... */
						pkgid = p_pkgid;
						varname = names;
					}
				}

				break;
			}
	}

	if (!OidIsValid(pkgid))
		return false;
	else if (pkgid == p_pkgid)
		pkg_func = true;

	base_typid = get_package_var_typeoid(pkgid, varname, &udtname, pkg_func);
	if (base_typid == InvalidOid)
		return false;

	if (notice)
	{
		/*
		 * Print information only when notice is true, because
		 * printing some additional operation information will
		 * confuse the user.
		 */
		ereport(NOTICE,
				(errmsg("type reference %s converted to %s",
				 TypeNameToString(typeName),
				 udtname ? udtname : format_type_be(base_typid))));
	}

	*pkg_oid = pkgid;
	*act_typid = base_typid;
	*o_udtname = udtname;

	return true;
}

bool
LookupPackageAnyType(const TypeName *typeName, Oid *pkg_oid, char **o_udtname,
					 Oid p_pkgid, Oid *act_typid, bool notice, Oid *o_synoid)
{
	bool	found = false;

	if (typeName->pct_type)
	{
		/* Find var%type in packages */
		found = LookupPackageVarType(typeName, pkg_oid, o_udtname, p_pkgid, act_typid, notice);
	}
	else if (!typeName->pct_type && !typeName->pct_rowtype)
	{
		/* Find types in packages (PLUDT types) */
		found = LookupPackageUDT(typeName, pkg_oid, o_udtname, p_pkgid, act_typid, o_synoid);
		if (!found)
		{
			/* Find types in packages (non-PLUDT types) */
			found = LookupPackageType(typeName, p_pkgid, act_typid);
		}
	}

	return found;
}

bool
CheckTypeInPkg(const char *typeName, Oid pkgid)
{
	Oid		pkgcrrspndingns;
	char	*namespace_name;
	List	*new_idents;
	Type		typtup;

	pkgcrrspndingns = get_pkgcrrspndingns_by_id(pkgid);
	namespace_name = get_namespace_name(pkgcrrspndingns);

	new_idents = list_make2(makeString(namespace_name) ,makeString(str_toupper(typeName, strlen(typeName), DEFAULT_COLLATION_OID)));

	typtup = LookupTypeName(NULL, makeTypeNameFromNameList(new_idents), NULL, false);

	if (typtup)
	{
		ReleaseSysCache(typtup);
		return true;
	}

	return false;
}

/*
 * LookupPackageType
 *   Find a type that is a collection type in pacakge.
 */
bool
LookupPackageType(const TypeName *typeName, Oid p_pkgid, Oid *act_typid)
{
	char		*typname = NULL;
	List		*names = NULL;
	RangeVar	*pkgname = NULL;
	Oid			pkg_oid = InvalidOid;
	Oid			toid = InvalidOid;
	Type		typtup;

	if (typeName->names == NIL || typeName->pct_type || typeName->pct_rowtype)
		return false;

	/* deconstruct the name list */
	names = typeName->names;

	switch (list_length(names))
	{
		case 1:
			pkg_oid = p_pkgid;
			typname = strVal(linitial(names));
			break;
		case 2:
			pkgname = makeRangeVar(NULL, strVal(linitial(names)), -1);
			typname = strVal(lsecond(names));
			break;
		case 3:
			pkgname = makeRangeVar(strVal(linitial(names)),
								strVal(lsecond(names)), -1);
			typname = strVal(lthird(names));
			break;
		default:
			break;
	}

	if (pkg_oid == InvalidOid && pkgname != NULL)
		pkg_oid = GetPackageIdRangeVar(pkgname, NULL);

	if (pkg_oid != InvalidOid && typname)
	{
		Oid		pkgcrrspndingns;
		char	*namespace_name;
		List	*new_idents;

		pkgcrrspndingns = get_pkgcrrspndingns_by_id(pkg_oid);
		namespace_name = get_namespace_name(pkgcrrspndingns);
		if (namespace_name == NULL)
			elog(ERROR, "failed to found package schema for oid %u", pkg_oid);

		new_idents = list_make2(makeString(namespace_name), makeString(typname));

		typtup = LookupTypeName(NULL, makeTypeNameFromNameList(new_idents), NULL, false);

		if (typtup)
		{
			toid = typeTypeId(typtup);
			ReleaseSysCache(typtup);
		}
	}

	if (toid != InvalidOid)
	{
		*act_typid = toid;
		return true;
	}

	return false;
}

/*
 * LookupTypeName
 *		Wrapper for typical case.
 */
Type
LookupTypeName(ParseState *pstate, const TypeName *typeName, int32 *typmod_p,
			   bool missing_ok)
{
	return LookupTypeNameExtended(pstate, typeName, typmod_p, true, missing_ok);
}

/*
 * LookupTypeNameExtended
 *		Given a TypeName object, lookup the pg_type syscache entry of the type.
 *		Returns NULL if no such type can be found.  If the type is found,
 *		the typmod value represented in the TypeName struct is computed and
 *		stored into *typmod_p.
 *
 * NB: on success, the caller must ReleaseSysCache the type tuple when done
 * with it.
 *
 * NB: direct callers of this function MUST check typisdefined before assuming
 * that the type is fully valid.  Most code should go through typenameType
 * or typenameTypeId instead.
 *
 * typmod_p can be passed as NULL if the caller does not care to know the
 * typmod value, but the typmod decoration (if any) will be validated anyway,
 * except in the case where the type is not found.  Note that if the type is
 * found but is a shell, and there is typmod decoration, an error will be
 * thrown --- this is intentional.
 *
 * If temp_ok is false, ignore types in the temporary namespace.  Pass false
 * when the caller will decide, using goodness of fit criteria, whether the
 * typeName is actually a type or something else.  If typeName always denotes
 * a type (or denotes nothing), pass true.
 *
 * pstate is used for error location info and it is used to establish
 * dependencies on function parameters/return values in the package, and
 * may be NULL.
 */
Type
LookupTypeNameExtended(ParseState *pstate,
					   const TypeName *typeName, int32 *typmod_p,
					   bool temp_ok, bool missing_ok)
{
	Oid			typoid;
	HeapTuple	tup;
	int32		typmod;
	bool isHudiColumnTypeReset = false;

	if (typeName->names == NIL)
	{
		/* We have the OID already if it's an internally generated TypeName */
		typoid = typeName->typeOid;
	}
	else if (typeName->pct_type)
	{
		/* Handle %TYPE reference to type of an existing field */
		RangeVar   *rel = makeRangeVar(NULL, NULL, typeName->location);
		char	   *field = NULL;
		Oid			relid;
		AttrNumber	attnum;

		/* deconstruct the name list */
		switch (list_length(typeName->names))
		{
			case 1:
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("improper %%TYPE reference (too few dotted names): %s",
								NameListToString(typeName->names)),
						 parser_errposition(pstate, typeName->location)));
				break;
			case 2:
			{
				char *tname = strVal(linitial(typeName->names));

				rel->relname = tname;

				/* Check tname is synonym in opentenbase_ora mode */
				if (ORA_MODE)
				{
					char    *objspc = NULL,
							*objname = NULL,
							*dblink = NULL;

					if (RangeVarGetRelid(rel, NoLock, true) == InvalidOid)
					{
						resolve_synonym(NULL, tname, &objspc, &objname, &dblink);
						if (objname != NULL && dblink == NULL)
						{
							rel->schemaname = pstrdup(objspc);
							rel->relname = pstrdup(objname);
						}
					}
				}

				field = strVal(lsecond(typeName->names));
			}
				break;
			case 3:
				rel->schemaname = strVal(linitial(typeName->names));
				rel->relname = strVal(lsecond(typeName->names));
				field = strVal(lthird(typeName->names));
				break;
			case 4:
				rel->catalogname = strVal(linitial(typeName->names));
				rel->schemaname = strVal(lsecond(typeName->names));
				rel->relname = strVal(lthird(typeName->names));
				field = strVal(lfourth(typeName->names));
				break;
			default:
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("improper %%TYPE reference (too many dotted names): %s",
								NameListToString(typeName->names)),
						 parser_errposition(pstate, typeName->location)));
				break;
		}

		/*
		 * Look up the field.
		 *
		 * XXX: As no lock is taken here, this might fail in the presence of
		 * concurrent DDL.  But taking a lock would carry a performance
		 * penalty and would also require a permissions check.
		 */
		relid = RangeVarGetRelid(rel, NoLock, missing_ok);
		attnum = get_attnum(relid, field);
		if (attnum == InvalidAttrNumber)
		{
			if (missing_ok)
				typoid = InvalidOid;
			else
				ereport(ERROR,
						(errcode(ERRCODE_UNDEFINED_COLUMN),
						 errmsg("column \"%s\" of relation \"%s\" does not exist",
								field, rel->relname),
						 parser_errposition(pstate, typeName->location)));
		}
		else
		{
			typoid = get_atttype(relid, attnum);

			/* this construct should never have an array indicator */
			Assert(typeName->arrayBounds == NIL);

			if (pstate && pstate->parse_pkg_func_header && OidIsValid(pstate->p_pkgoid))
			{
				/* Currently parsing the parameter type or return value type of the function */
				Oid		reltypoid = InvalidOid;

				reltypoid = get_rel_type_id(relid);
				if (OidIsValid(reltypoid))
				{
					ObjectAddress	pkgaddr;

					pkgaddr.classId = PackageRelationId;
					pkgaddr.objectId = pstate->p_pkgoid;
					pkgaddr.objectSubId = InvalidOid;

					GeneratePkgDependence(pkgaddr, TypeRelationId, reltypoid);
				}
			}

			/* emit nuisance notice (intentionally not errposition'd) */
			ereport(NOTICE,
					(errmsg("type reference %s converted to %s",
							TypeNameToString(typeName),
							format_type_be(typoid))));
		}
	}
	else if (typeName->pct_rowtype)
	{
		/* Handle %ROWTYPE reference to type of an existing table/struct */
		RangeVar   *rel = makeRangeVar(NULL, NULL, typeName->location);
		//char	   *field = NULL;
		Oid			relid;
		HeapTuple	ht_rel;
		Form_pg_class rel_class;		

		/* deconstruct the name list */
		switch (list_length(typeName->names))
		{			
			case 1:
				rel->relname = strVal(linitial(typeName->names));
				//field = strVal(lsecond(typeName->names));
				break;
			case 2:
				rel->schemaname = strVal(linitial(typeName->names));
				rel->relname = strVal(lsecond(typeName->names));
				//field = strVal(lthird(typeName->names));
				break;
			case 3:
				rel->catalogname = strVal(linitial(typeName->names));
				rel->schemaname = strVal(lsecond(typeName->names));
				rel->relname = strVal(lthird(typeName->names));
				//field = strVal(lfourth(typeName->names));
				break;
			default:
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("improper %%TYPE reference (too many dotted names): %s",
								NameListToString(typeName->names)),
						 parser_errposition(pstate, typeName->location)));
				break;
		}
		

		/*
		 *for 'xxx table_name%rowtype', it meams the variable 'xxx' type is the table_name
		 *so typoid is relid;
		 *
		 * XXX: As no lock is taken here, this might fail in the presence of
		 * concurrent DDL.  But taking a lock would carry a performance
		 * penalty and would also require a permissions check.
		 */
		relid = RangeVarGetRelid(rel, NoLock, missing_ok);
		ht_rel = SearchSysCache1(RELOID, ObjectIdGetDatum(relid));
		if (!HeapTupleIsValid(ht_rel))
			elog(ERROR, "cache lookup failed for relation %u", relid);
		rel_class = (Form_pg_class) GETSTRUCT(ht_rel);
		typoid = rel_class->reltype;
		ReleaseSysCache(ht_rel);
	}
	else
	{
		/* Normal reference to a type name */
		char	   *schemaname;
		char	   *typname;
		int 	   typmods = -1;

		/* deconstruct the name list */
		DeconstructQualifiedName(typeName->names, &schemaname, &typname);

		/* 
		 * when enable_lightweight_ora_syntax is on, change date type to timestamp in gram.y, 
		 * but hudi foreign table can't change, so reset to date type
		 * */
		if (enable_lightweight_ora_syntax && pstate && pstate->isHudiForeign && pg_strcasecmp(typname, "timestamp") == 0)
		{
			typmods = typenameTypeMods(typeName);
			/* 
			 * LIGHTWEIGHT_ORA_CONVERS_DATE_TIMESTAMP_TAG is a tags to mark date convers to timestamp in gram.y   
			 */
			if (typmods == LIGHTWEIGHT_ORA_CONVERS_DATE_TIMESTAMP_TAG)
			{
				strcpy(typname, "date");	// strlen(date) < strlen(timestamp), so no need to extend space
				pstate->isHudiColumnTypeReset = true;
				isHudiColumnTypeReset = true;
			}
		}

		if (enable_lightweight_ora_syntax && strcmp(typname, "number") == 0)
			typname = "numeric"; /* replace number to numeric for lightweight_ora compatible */

		if (schemaname)
		{
			/* Look in specific schema only */
			Oid			namespaceId;
			ParseCallbackState pcbstate;

			setup_parser_errposition_callback(&pcbstate, pstate, typeName->location);

			if (PackageGetPkgid(schemaname) != InvalidOid)
				namespaceId = LookupExplicitNamespace(schemaname, true);
			else
			{
				if (!ORA_MODE)
					namespaceId = LookupExplicitNamespace(schemaname, missing_ok);
				else
					namespaceId = LookupExplicitNamespace(schemaname, true);
			}
			if (OidIsValid(namespaceId))
				typoid = GetSysCacheOid2(TYPENAMENSP,
										 PointerGetDatum(typname),
										 ObjectIdGetDatum(namespaceId));
			else
			{
				typoid = InvalidOid;
				/* check if schemaname is synonym  */
				if(!OidIsValid(typoid))
				{
					char    *objspc = NULL,
							*objname = NULL,
							*dblink = NULL;
					Oid		pkgId;
					char	*pkgnamespace = NULL;
					List	*names = typeName->names;

					if (list_length(names) == 2)
					{
						resolve_synonym(NULL, schemaname, &objspc, &objname, &dblink);
						if (objname != NULL && dblink == NULL)
						{
							if (objspc)
								pkgnamespace = pstrdup(objspc);

							pkgId = get_package_oid(pkgnamespace, objname, NULL);
							if (OidIsValid(pkgId))
								typoid = GetPackageDefinedTypeId(pkgId, typname, true);
						}
					}
				}
			}

			cancel_parser_errposition_callback(&pcbstate);
		}
		else
		{
			/* Unqualified type name, so search the search path */
			typoid = TypenameGetTypid(typname);
		}

		/* If an array reference, return the array type instead */
		if (typeName->arrayBounds != NIL)
			typoid = get_array_type(typoid);
	}

	if (!OidIsValid(typoid))
	{
		if (typmod_p)
			*typmod_p = -1;
		return NULL;
	}

	tup = SearchSysCache1(TYPEOID, ObjectIdGetDatum(typoid));
	if (!HeapTupleIsValid(tup)) /* should not happen */
		elog(ERROR, "cache lookup failed for type %u", typoid);

	typmod = typenameTypeMod(pstate, typeName, (Type) tup, isHudiColumnTypeReset) ;

	if (typmod_p)
		*typmod_p = typmod;

	return (Type) tup;
}

/*
 * LookupTypeNameOid
 *		Given a TypeName object, lookup the pg_type syscache entry of the type.
 *		Returns InvalidOid if no such type can be found.  If the type is found,
 *		return its Oid.
 *
 * NB: direct callers of this function need to be aware that the type OID
 * returned may correspond to a shell type.  Most code should go through
 * typenameTypeId instead.
 *
 * pstate is used for error location info and it is used to establish
 * dependencies on function parameters/return values in the package, 
 * and may be NULL.
 */
Oid
LookupTypeNameOid(ParseState *pstate, const TypeName *typeName, bool missing_ok)
{
	Oid			typoid;
	Type		tup;

	tup = LookupTypeName(pstate, typeName, NULL, missing_ok);
	if (tup == NULL)
	{
		if (!missing_ok)
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_OBJECT),
					 errmsg("type \"%s\" does not exist",
							TypeNameToString(typeName)),
					 parser_errposition(pstate, typeName->location)));

		return InvalidOid;
	}

	typoid = HeapTupleGetOid(tup);
	ReleaseSysCache(tup);

	return typoid;
}

/*
 * typenameType - given a TypeName, return a Type structure and typmod
 *
 * This is equivalent to LookupTypeName, except that this will report
 * a suitable error message if the type cannot be found or is not defined.
 * Callers of this can therefore assume the result is a fully valid type.
 */
Type
typenameType(ParseState *pstate, const TypeName *typeName, int32 *typmod_p)
{
	Type		tup;

	tup = LookupTypeName(pstate, typeName, typmod_p, false);
	if (tup == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("type \"%s\" does not exist",
						TypeNameToString(typeName)),
				 parser_errposition(pstate, typeName->location)));
	if (!((Form_pg_type) GETSTRUCT(tup))->typisdefined)
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("type \"%s\" is only a shell",
						TypeNameToString(typeName)),
				 parser_errposition(pstate, typeName->location)));
	return tup;
}

/*
 * typenameTypeId - given a TypeName, return the type's OID
 *
 * This is similar to typenameType, but we only hand back the type OID
 * not the syscache entry.
 */
Oid
typenameTypeId(ParseState *pstate, const TypeName *typeName)
{
	Oid			typoid;
	Type		tup;

	tup = typenameType(pstate, typeName, NULL);
	typoid = HeapTupleGetOid(tup);
	ReleaseSysCache(tup);

	return typoid;
}

/*
 * typenameTypeIdAndMod - given a TypeName, return the type's OID and typmod
 *
 * This is equivalent to typenameType, but we only hand back the type OID
 * and typmod, not the syscache entry.
 */
void
typenameTypeIdAndMod(ParseState *pstate, const TypeName *typeName,
					 Oid *typeid_p, int32 *typmod_p, bool is_column)
{
	Type		tup;

	tup = typenameType(pstate, typeName, typmod_p);
	*typeid_p = HeapTupleGetOid(tup);


	ReleaseSysCache(tup);
}

/*
 * typenameTypeMod - given a TypeName, return the internal typmod value
 *
 * This will throw an error if the TypeName includes type modifiers that are
 * illegal for the data type.
 *
 * The actual type OID represented by the TypeName must already have been
 * looked up, and is passed as "typ".
 *
 * pstate is used for error location info and it is used to establish
 * dependencies on function parameters/return values in the package, 
 * and may be NULL.
 */
static int32
typenameTypeMod(ParseState *pstate, const TypeName *typeName, Type typ, bool isReset)
{
	int32		result;
	Oid			typmodin;
	Datum	   *datums;
	int			n;
	ListCell   *l;
	ArrayType  *arrtypmod;
	ParseCallbackState pcbstate;

	/* Return prespecified typmod if no typmod expressions */
	if (typeName->typmods == NIL || isReset)
		return typeName->typemod;

	/*
	 * Else, type had better accept typmods.  We give a special error message
	 * for the shell-type case, since a shell couldn't possibly have a
	 * typmodin function.
	 */
	if (!((Form_pg_type) GETSTRUCT(typ))->typisdefined)
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("type modifier cannot be specified for shell type \"%s\"",
						TypeNameToString(typeName)),
				 parser_errposition(pstate, typeName->location)));

	typmodin = ((Form_pg_type) GETSTRUCT(typ))->typmodin;

	if (typmodin == InvalidOid)
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("type modifier is not allowed for type \"%s\"",
						TypeNameToString(typeName)),
				 parser_errposition(pstate, typeName->location)));

	/*
	 * Convert the list of raw-grammar-output expressions to a cstring array.
	 * Currently, we allow simple numeric constants, string literals, and
	 * identifiers; possibly this list could be extended.
	 */
	datums = (Datum *) palloc(list_length(typeName->typmods) * sizeof(Datum));
	n = 0;
	foreach(l, typeName->typmods)
	{
		Node	   *tm = (Node *) lfirst(l);
		char	   *cstr = NULL;

		if (IsA(tm, A_Const))
		{
			A_Const    *ac = (A_Const *) tm;

			if (IsA(&ac->val, Integer))
			{
				// LIGHTWEIGHT_ORA_CONVERS_DATE_TIMESTAMP_TAG is a tags to mark date convers to timestamp in gram.y, real data is 0;
				if (ac->val.val.ival == LIGHTWEIGHT_ORA_CONVERS_DATE_TIMESTAMP_TAG)
					ac->val.val.ival = 0;
				cstr = psprintf("%ld", (long) ac->val.val.ival);
			}
			else if (IsA(&ac->val, Float) ||
					 IsA(&ac->val, String))
			{
				/* we can just use the str field directly. */
				cstr = ac->val.val.str;
			}
		}
		else if (IsA(tm, ColumnRef))
		{
			ColumnRef  *cr = (ColumnRef *) tm;

			if (list_length(cr->fields) == 1 &&
				IsA(linitial(cr->fields), String))
				cstr = strVal(linitial(cr->fields));
		}
		if (!cstr)
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					 errmsg("type modifiers must be simple constants or identifiers"),
					 parser_errposition(pstate, typeName->location)));
		datums[n++] = CStringGetDatum(cstr);
	}

	/* hardwired knowledge about cstring's representation details here */
	arrtypmod = construct_array(datums, n, CSTRINGOID,
								-2, false, 'c');

	/* arrange to report location if type's typmodin function fails */
	setup_parser_errposition_callback(&pcbstate, pstate, typeName->location);

	result = DatumGetInt32(OidFunctionCall1(typmodin,
											PointerGetDatum(arrtypmod)));

	cancel_parser_errposition_callback(&pcbstate);

	pfree(datums);
	pfree(arrtypmod);

	return result;
}

/*
 * given a TypeName, return the internal typmod value from typeName->typmods
 */
static int32
typenameTypeMods(const TypeName *typeName)
{
	int ret_val = -1;

	/* Return prespecified typmod if no typmod expressions */
	if (typeName->typmods == NIL)
		return typeName->typemod;
	
	if (typeName->typmods->length == 1)
	{
		ListCell   *l = list_head(typeName->typmods);
		Node	   *tm = (Node *) lfirst(l);

		if (IsA(tm, A_Const))
		{
			A_Const    *ac = (A_Const *) tm;

			if (IsA(&ac->val, Integer))
			{
				ret_val = ac->val.val.ival;
			}
		}
	}

	return ret_val;
}

/*
 * appendTypeNameToBuffer
 *		Append a string representing the name of a TypeName to a StringInfo.
 *		This is the shared guts of TypeNameToString and TypeNameListToString.
 *
 * NB: this must work on TypeNames that do not describe any actual type;
 * it is mostly used for reporting lookup errors.
 */
static void
appendTypeNameToBuffer(const TypeName *typeName, StringInfo string)
{
	if (typeName->names != NIL)
	{
		/* Emit possibly-qualified name as-is */
		ListCell   *l;

		foreach(l, typeName->names)
		{
			if (l != list_head(typeName->names))
				appendStringInfoChar(string, '.');
			appendStringInfoString(string, strVal(lfirst(l)));
		}
	}
	else
	{
		/* Look up internally-specified type */
		appendStringInfoString(string, format_type_be(typeName->typeOid));
	}

	/*
	 * Add decoration as needed, but only for fields considered by
	 * LookupTypeName
	 */
	if (typeName->pct_type)
		appendStringInfoString(string, "%TYPE");

	if (typeName->arrayBounds != NIL)
		appendStringInfoString(string, "[]");
}

/*
 * TypeNameToString
 *		Produce a string representing the name of a TypeName.
 *
 * NB: this must work on TypeNames that do not describe any actual type;
 * it is mostly used for reporting lookup errors.
 */
char *
TypeNameToString(const TypeName *typeName)
{
	StringInfoData string;

	initStringInfo(&string);
	appendTypeNameToBuffer(typeName, &string);
	return string.data;
}

/*
 * TypeNameListToString
 *		Produce a string representing the name(s) of a List of TypeNames
 */
char *
TypeNameListToString(List *typenames)
{
	StringInfoData string;
	ListCell   *l;

	initStringInfo(&string);
	foreach(l, typenames)
	{
		TypeName   *typeName = lfirst_node(TypeName, l);

		if (l != list_head(typenames))
			appendStringInfoChar(&string, ',');
		appendTypeNameToBuffer(typeName, &string);
	}
	return string.data;
}

/*
 * LookupCollation
 *
 * Look up collation by name, return OID, with support for error location.
 */
Oid
LookupCollation(ParseState *pstate, List *collnames, int location)
{
	Oid			colloid;
	ParseCallbackState pcbstate;

	if (pstate)
		setup_parser_errposition_callback(&pcbstate, pstate, location);

	colloid = get_collation_oid(collnames, false);

	if (pstate)
		cancel_parser_errposition_callback(&pcbstate);

	return colloid;
}

/*
 * GetColumnDefCollation
 *
 * Get the collation to be used for a column being defined, given the
 * ColumnDef node and the previously-determined column type OID.
 *
 * pstate is only used for error location purposes, and can be NULL.
 */
Oid
GetColumnDefCollation(ParseState *pstate, ColumnDef *coldef, Oid typeOid)
{
	Oid			result;
	Oid			typcollation = get_typcollation(typeOid);
	int			location = coldef->location;

	if (coldef->collClause)
	{
		/* We have a raw COLLATE clause, so look up the collation */
		location = coldef->collClause->location;
		result = LookupCollation(pstate, coldef->collClause->collname,
								 location);
	}
	else if (OidIsValid(coldef->collOid))
	{
		/* Precooked collation spec, use that */
		result = coldef->collOid;
	}
	else
	{
		/* Use the type's default collation if any */
		result = typcollation;
	}

	/* Complain if COLLATE is applied to an uncollatable type */
	if (OidIsValid(result) && !OidIsValid(typcollation))
		ereport(ERROR,
				(errcode(ERRCODE_DATATYPE_MISMATCH),
				 errmsg("collations are not supported by type %s",
						format_type_be(typeOid)),
				 parser_errposition(pstate, location)));

	return result;
}

/* return a Type structure, given a type id */
/* NB: caller must ReleaseSysCache the type tuple when done with it */
Type
typeidType(Oid id)
{
	HeapTuple	tup;

	tup = SearchSysCache1(TYPEOID, ObjectIdGetDatum(id));
	if (!HeapTupleIsValid(tup))
		elog(ERROR, "cache lookup failed for type %u", id);
	return (Type) tup;
}

/* given type (as type struct), return the type OID */
Oid
typeTypeId(Type tp)
{
	if (tp == NULL)				/* probably useless */
		elog(ERROR, "typeTypeId() called with NULL type struct");
	return HeapTupleGetOid(tp);
}

/* given type (as type struct), return the length of type */
int16
typeLen(Type t)
{
	Form_pg_type typ;

	typ = (Form_pg_type) GETSTRUCT(t);
	return typ->typlen;
}

/* given type (as type struct), return its 'byval' attribute */
bool
typeByVal(Type t)
{
	Form_pg_type typ;

	typ = (Form_pg_type) GETSTRUCT(t);
	return typ->typbyval;
}

/* given type (as type struct), return the type's name */
char *
typeTypeName(Type t)
{
	Form_pg_type typ;

	typ = (Form_pg_type) GETSTRUCT(t);
	/* pstrdup here because result may need to outlive the syscache entry */
	return pstrdup(NameStr(typ->typname));
}

/* given type (as type struct), return its 'typrelid' attribute */
Oid
typeTypeRelid(Type typ)
{
	Form_pg_type typtup;

	typtup = (Form_pg_type) GETSTRUCT(typ);
	return typtup->typrelid;
}

/* given type (as type struct), return its 'typcollation' attribute */
Oid
typeTypeCollation(Type typ)
{
	Form_pg_type typtup;

	typtup = (Form_pg_type) GETSTRUCT(typ);
	return typtup->typcollation;
}

/*
 * Given a type structure and a string, returns the internal representation
 * of that string.  The "string" can be NULL to perform conversion of a NULL
 * (which might result in failure, if the input function rejects NULLs).
 */
Datum
stringTypeDatum(Type tp, char *string, int32 atttypmod)
{
	Form_pg_type typform = (Form_pg_type) GETSTRUCT(tp);
	Oid			typinput = typform->typinput;
	Oid			typioparam = getTypeIOParam(tp);

	return OidInputFunctionCall(typinput, string, typioparam, atttypmod);
}

/*
 * Given a typeid, return the type's typrelid (associated relation), if any.
 * Returns InvalidOid if type is not a composite type.
 */
Oid
typeidTypeRelid(Oid type_id)
{
	HeapTuple	typeTuple;
	Form_pg_type type;
	Oid			result;

	typeTuple = SearchSysCache1(TYPEOID, ObjectIdGetDatum(type_id));
	if (!HeapTupleIsValid(typeTuple))
		elog(ERROR, "cache lookup failed for type %u", type_id);
	type = (Form_pg_type) GETSTRUCT(typeTuple);
	result = type->typrelid;
	ReleaseSysCache(typeTuple);
	return result;
}

/*
 * Given a typeid, return the type's typrelid (associated relation), if any.
 * Returns InvalidOid if type is not a composite type or a domain over one.
 * This is the same as typeidTypeRelid(getBaseType(type_id)), but faster.
 */
Oid
typeOrDomainTypeRelid(Oid type_id)
{
	HeapTuple	typeTuple;
	Form_pg_type type;
	Oid			result;

	for (;;)
	{
		typeTuple = SearchSysCache1(TYPEOID, ObjectIdGetDatum(type_id));
		if (!HeapTupleIsValid(typeTuple))
			elog(ERROR, "cache lookup failed for type %u", type_id);
		type = (Form_pg_type) GETSTRUCT(typeTuple);
		if (type->typtype != TYPTYPE_DOMAIN)
		{
			/* Not a domain, so done looking through domains */
			break;
		}
		/* It is a domain, so examine the base type instead */
		type_id = type->typbasetype;
		ReleaseSysCache(typeTuple);
	}
	result = type->typrelid;
	ReleaseSysCache(typeTuple);
	return result;
}

/*
 * error context callback for parse failure during parseTypeString()
 */
static void
pts_error_callback(void *arg)
{
	const char *str = (const char *) arg;

	errcontext("invalid type name \"%s\"", str);

	/*
	 * Currently we just suppress any syntax error position report, rather
	 * than transforming to an "internal query" error.  It's unlikely that a
	 * type name is complex enough to need positioning.
	 */
	errposition(0);
}

/*
 * Given a string that is supposed to be a SQL-compatible type declaration,
 * such as "int4" or "integer" or "character varying(32)", parse
 * the string and return the result as a TypeName.
 * If the string cannot be parsed as a type, an error is raised.
 */
TypeName *
typeStringToTypeName(const char *str)
{
	StringInfoData buf;
	List	   *raw_parsetree_list;
	SelectStmt *stmt;
	ResTarget  *restarget;
	TypeCast   *typecast;
	TypeName   *typeName;
	ErrorContextCallback ptserrcontext;

	/* make sure we give useful error for empty input */
	if (strspn(str, " \t\n\r\f") == strlen(str))
		goto fail;

	initStringInfo(&buf);
	appendStringInfo(&buf, "SELECT NULL::%s", str);

	/*
	 * Setup error traceback support in case of ereport() during parse
	 */
	ptserrcontext.callback = pts_error_callback;
	ptserrcontext.arg = (void *) str;
	ptserrcontext.previous = error_context_stack;
	error_context_stack = &ptserrcontext;

	raw_parsetree_list = raw_parser(buf.data);

	error_context_stack = ptserrcontext.previous;

	/*
	 * Make sure we got back exactly what we expected and no more; paranoia is
	 * justified since the string might contain anything.
	 */
	if (list_length(raw_parsetree_list) != 1)
		goto fail;
	stmt = (SelectStmt *) linitial_node(RawStmt, raw_parsetree_list)->stmt;
	if (stmt == NULL ||
		!IsA(stmt, SelectStmt) ||
		stmt->distinctClause != NIL ||
		stmt->intoClause != NULL ||
		stmt->fromClause != NIL ||
		stmt->whereClause != NULL ||
		stmt->groupClause != NIL ||
		stmt->havingClause != NULL ||
		stmt->windowClause != NIL ||
		stmt->valuesLists != NIL ||
		stmt->sortClause != NIL ||
		stmt->limitOffset != NULL ||
		stmt->limitCount != NULL ||
		stmt->lockingClause != NIL ||
		stmt->withClause != NULL ||
		stmt->op != SETOP_NONE)
		goto fail;
	if (list_length(stmt->targetList) != 1)
		goto fail;
	restarget = (ResTarget *) linitial(stmt->targetList);
	if (restarget == NULL ||
		!IsA(restarget, ResTarget) ||
		restarget->name != NULL ||
		restarget->indirection != NIL)
		goto fail;
	typecast = (TypeCast *) restarget->val;
	if (typecast == NULL ||
		!IsA(typecast, TypeCast) ||
		typecast->arg == NULL ||
		!IsA(typecast->arg, A_Const))
		goto fail;

	typeName = typecast->typeName;
	if (typeName == NULL ||
		!IsA(typeName, TypeName))
		goto fail;
	if (typeName->setof)
		goto fail;

	pfree(buf.data);

	return typeName;

fail:
	ereport(ERROR,
			(errcode(ERRCODE_SYNTAX_ERROR),
			 errmsg("invalid type name \"%s\"", str)));
	return NULL;				/* keep compiler quiet */
}

/*
 * Given a string that is supposed to be a SQL-compatible type declaration,
 * such as "int4" or "integer" or "character varying(32)", parse
 * the string and convert it to a type OID and type modifier.
 * If missing_ok is true, InvalidOid is returned rather than raising an error
 * when the type name is not found.
 */
void
parseTypeString(const char *str, Oid *typeid_p, int32 *typmod_p, bool missing_ok)
{
	TypeName   *typeName;
	Type		tup;

	typeName = typeStringToTypeName(str);

	tup = LookupTypeName(NULL, typeName, typmod_p, missing_ok);
	if (tup == NULL)
	{
		if (!missing_ok)
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_OBJECT),
					 errmsg("type \"%s\" does not exist",
							TypeNameToString(typeName)),
					 parser_errposition(NULL, typeName->location)));
		*typeid_p = InvalidOid;
	}
	else
	{
		if (!((Form_pg_type) GETSTRUCT(tup))->typisdefined)
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_OBJECT),
					 errmsg("type \"%s\" is only a shell",
							TypeNameToString(typeName)),
					 parser_errposition(NULL, typeName->location)));
		*typeid_p = HeapTupleGetOid(tup);
		ReleaseSysCache(tup);
	}
}

#ifdef __OPENTENBASE_C__
/*
 * only interger or float or text can be type of column table
 */
bool typeSupportColTable(Oid type_id)
{
	return (typeIsColDatumType(type_id) ||
			IsBinaryCoercible(type_id, TEXTOID));
}

/*
 * type is integer or float ?
 */
bool typeIsColDatumType(Oid type_id)
{
	Type tp = typeidType(type_id);
	bool byVal = typeByVal(tp);

	ReleaseSysCache(tp);

	return byVal;
}

#endif
bool
is_integer_type(char *typname)
{
	if (0 == strcmp("PLS_INTEGER", typname) || 0 == strcmp("BINARY_INTEGER", typname))
		return true;
	else
		return false;
}

bool
is_numericfd_type(Oid type_oid)
{
	if (type_oid == NUMERICFOID || type_oid == NUMERICDOID)
		return true;

	return false;
}
