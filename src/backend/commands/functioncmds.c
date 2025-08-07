/*-------------------------------------------------------------------------
 *
 * functioncmds.c
 *
 *	  Routines for CREATE and DROP FUNCTION commands and CREATE and DROP
 *	  CAST commands.
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/commands/functioncmds.c
 *
 * DESCRIPTION
 *	  These routines take the parse tree and pick out the
 *	  appropriate arguments/flags, and pass the results to the
 *	  corresponding "FooDefine" routines (in src/catalog) that do
 *	  the actual catalog-munging.  These routines also verify permission
 *	  of the user to execute the command.
 *
 * NOTES
 *	  These things must be defined and committed in the following order:
 *		"create function":
 *				input/output, recv/send procedures
 *		"create type":
 *				type
 *		"create operator":
 *				operators
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/genam.h"
#include "access/heapam.h"
#include "access/htup_details.h"
#include "access/sysattr.h"
#include "catalog/dependency.h"
#include "catalog/indexing.h"
#include "catalog/objectaccess.h"
#include "catalog/pg_aggregate.h"
#include "catalog/pg_cast.h"
#include "catalog/pg_language.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_package.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_proc_fn.h"
#include "catalog/pg_transform.h"
#include "catalog/pg_trigger.h"
#include "catalog/pg_type.h"
#include "catalog/pg_type_fn.h"
#include "catalog/pg_aggregate.h"
#include "catalog/pg_synonym.h"
#include "commands/alter.h"
#include "commands/defrem.h"
#include "commands/proclang.h"
#include "commands/extension.h"
#include "executor/execdesc.h"
#include "executor/executor.h"
#include "executor/resultCache.h"
#include "executor/spi.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "optimizer/clauses.h"
#include "optimizer/var.h"
#include "parser/parse_coerce.h"
#include "parser/parse_collate.h"
#include "parser/parse_expr.h"
#include "parser/parse_func.h"
#include "parser/parse_type.h"
#include "pgstat.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/guc.h"
#include "utils/inval.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/syscache.h"
#include "utils/typcache.h"
#include "utils/tqual.h"

#include "catalog/pg_compile.h"

/* When the pushdown attribute is specified when creating function, there is
 * no need to automatically pushdown the function.
 */
static bool set_pushdown_attribute = false;

/*
 *	 Examine the RETURNS clause of the CREATE FUNCTION statement
 *	 and return information about it as *prorettype_p and *returnsSet.
 *
 * This is more complex than the average typename lookup because we want to
 * allow a shell type to be used, or even created if the specified return type
 * doesn't exist yet.  (Without this, there's no way to define the I/O procs
 * for a new type.)  But SQL function creation won't cope, so error out if
 * the target language is SQL.  (We do this here, not in the SQL-function
 * validator, so as not to produce a NOTICE and then an ERROR for the same
 * condition.)
 */
static void
compute_return_type(ParseState *pstate, TypeName *returnType,
					Oid languageOid, Oid *prorettype_p,
					bool *returnsSet_p, Oid pkgOid, bool iswith,
					char **o_udtname)
{
	Oid			rettype = InvalidOid;
	Type		typtup;
	AclResult	aclresult;

	if (ORA_MODE)
		LookupPackageType(returnType, pkgOid, &rettype);

	if (rettype == InvalidOid)
	{
		if (OidIsValid(pstate->p_pkgoid) && !iswith)
			pstate->parse_pkg_func_header = true;

		typtup = LookupTypeName(pstate, returnType, NULL, false);

		if (OidIsValid(pstate->p_pkgoid) && !iswith)
			pstate->parse_pkg_func_header = false;

		if (typtup)
		{
			if (!((Form_pg_type) GETSTRUCT(typtup))->typisdefined)
			{
				if (languageOid == SQLlanguageId)
					ereport(ERROR,
							(errcode(ERRCODE_INVALID_FUNCTION_DEFINITION),
							errmsg("SQL function cannot return shell type %s",
									TypeNameToString(returnType))));
				else
					ereport(NOTICE,
							(errcode(ERRCODE_WRONG_OBJECT_TYPE),
							errmsg("return type %s is only a shell",
									TypeNameToString(returnType))));
			}
			rettype = typeTypeId(typtup);
			ReleaseSysCache(typtup);
		}
	}

	if (!OidIsValid(rettype))
	{
		if (check_function_pludt)
		{
			char	   *typnam = TypeNameToString(returnType);
			Oid			namespaceId;
			AclResult	aclresult;
			char	   *typname;
			ObjectAddress address;

			/*
			* Only C-coded functions can be I/O functions.  We enforce this
			* restriction here mainly to prevent littering the catalogs with
			* shell types due to simple typos in user-defined function
			* definitions.
			*/
			if (languageOid != INTERNALlanguageId &&
				languageOid != ClanguageId)
				ereport(ERROR,
						(errcode(ERRCODE_UNDEFINED_OBJECT),
						errmsg("type \"%s\" does not exist", typnam)));

			/* Reject if there's typmod decoration, too */
			if (returnType->typmods != NIL)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						errmsg("type modifier cannot be specified for shell type \"%s\"",
								typnam)));

			/* Otherwise, go ahead and make a shell type */
			ereport(NOTICE,
					(errcode(ERRCODE_UNDEFINED_OBJECT),
					errmsg("type \"%s\" is not yet defined", typnam),
					errdetail("Creating a shell type definition.")));
			namespaceId = QualifiedNameGetCreationNamespace(returnType->names,
															&typname);
			aclresult = pg_namespace_aclcheck(namespaceId, GetUserId(),
											ACL_CREATE);
			if (aclresult != ACLCHECK_OK)
				aclcheck_error(aclresult, ACL_KIND_NAMESPACE,
							get_namespace_name(namespaceId));
			address = TypeShellMake(typname, namespaceId, GetUserId());
			rettype = address.objectId;
			Assert(OidIsValid(rettype));
		}
		else
		{
			/*
			 * When check_function_pludt is off (usually when pg_dump is called),
			 * the type cannot be found, so the type is treated as a PLUDT in
			 * the package.
			 */
			rettype = PLUDTOID;
			if (o_udtname && returnType)
				*o_udtname = TypeNameToString(returnType);
		}
	}

	aclresult = pg_type_aclcheck(rettype, GetUserId(), ACL_USAGE);
	if (aclresult != ACLCHECK_OK)
		aclcheck_error_type(aclresult, rettype);

	*prorettype_p = rettype;
	*returnsSet_p = returnType->setof;
}

/*
 * Interpret the function parameter list of a CREATE FUNCTION or
 * CREATE AGGREGATE statement.
 *
 * Input parameters:
 * parameters: list of FunctionParameter structs
 * languageOid: OID of function language (InvalidOid if it's CREATE AGGREGATE)
 * is_aggregate: needed only to determine error handling
 *
 * Results are stored into output parameters.  parameterTypes must always
 * be created, but the other arrays are set to NULL if not needed.
 * variadicArgType is set to the variadic array type if there's a VARIADIC
 * parameter (there can be only one); or to InvalidOid if not.
 * requiredResultType is set to InvalidOid if there are no OUT parameters,
 * else it is set to the OID of the implied result type.
 */
void
interpret_function_parameter_list(ParseState *pstate,
								  List *parameters,
								  Oid languageOid,
								  ObjectType objtype,
								  oidvector **parameterTypes,
								  ArrayType **allParameterTypes,
								  ArrayType **parameterModes,
								  ArrayType **parameterNames,
#ifdef _PG_ORCL_
								  ArrayType **parameterUdtNames,
								  Oid p_pkgid,
								  List **syn_oids,
#endif
								  List **parameterDefaults,
								  Oid *variadicArgType,
								  Oid *requiredResultType,
								  bool iswith,
								  bool notice)
{
	int			parameterCount = list_length(parameters);
	Oid		   *sigArgTypes;
	int			sigArgCount = 0;
	Datum	   *allTypes;
	Datum	   *paramModes;
	Datum	   *paramNames;
#ifdef _PG_ORCL_
	Datum	   *udtnames;
	int			nudt = 0;
#endif
	int			outCount = 0;
	int			varCount = 0;
	bool		have_names = false;
	bool		have_defaults = false;
	ListCell   *x;
	int			i;

	*variadicArgType = InvalidOid;	/* default result */
	*requiredResultType = InvalidOid;	/* default result */

	sigArgTypes = (Oid *) palloc(parameterCount * sizeof(Oid));
	allTypes = (Datum *) palloc(parameterCount * sizeof(Datum));
	paramModes = (Datum *) palloc(parameterCount * sizeof(Datum));
	paramNames = (Datum *) palloc0(parameterCount * sizeof(Datum));
#ifdef _PG_ORCL_
	udtnames = (Datum *) palloc0(parameterCount * sizeof(Datum));
#endif
	*parameterDefaults = NIL;

	/* Scan the list and extract data into work arrays */
	i = 0;
	foreach(x, parameters)
	{
		FunctionParameter *fp = (FunctionParameter *) lfirst(x);
		TypeName   *t = fp->argType;
		bool		isinput = false;
		Oid			toid = InvalidOid;
		Type		typtup;
		AclResult	aclresult;

		if (ORA_MODE)
		{
			Oid				syn_oid = InvalidOid;
			Oid				pkg_oid;
			char			*udtname = NULL;
			ObjectAddress	pkgaddr;

			pkgaddr.objectId = InvalidOid;

			/*
			 * When the package and table have the same name, the package will
			 * be matched first.
			 */
			LookupPackageAnyType(t, &pkg_oid, &udtname, p_pkgid, &toid, notice, &syn_oid);
			if (toid == PLUDTOID)
			{
				if (udtname)
					udtnames[nudt] = CStringGetTextDatum(udtname);

				if (OidIsValid(pstate->p_pkgoid) && !iswith &&
					OidIsValid(pkg_oid) && pstate->p_pkgoid != pkg_oid)
				{
					if (!OidIsValid(pkgaddr.objectId))
					{
						pkgaddr.classId = PackageRelationId;
						pkgaddr.objectId = pstate->p_pkgoid;
						pkgaddr.objectSubId = InvalidOid;
					}

					/* Depend on other package types */
					GeneratePkgDependence(pkgaddr, PackageRelationId, pkg_oid);
				}

				nudt++;
			}

			if (OidIsValid(syn_oid) && OidIsValid(toid) &&
				!iswith)
			{
				if (OidIsValid(pstate->p_pkgoid))
				{
					if (!OidIsValid(pkgaddr.objectId))
					{
						pkgaddr.classId = PackageRelationId;
						pkgaddr.objectId = pstate->p_pkgoid;
						pkgaddr.objectSubId = InvalidOid;
					}

					/*
					 * If the current function is within a package, add
					 * the package's dependency on the synonym.
					 */
					GeneratePkgDependence(pkgaddr, SynonymRelationId, syn_oid);
				}
				else if (syn_oids)
				{
					/* Record it and add the dependency after creating the function */
					if (!list_member_oid(*syn_oids, syn_oid))
						*syn_oids = lappend_oid(*syn_oids, syn_oid);
				}
			}
		}

		if (toid == InvalidOid)
		{
			if (OidIsValid(pstate->p_pkgoid) && !iswith)
				pstate->parse_pkg_func_header = true;

			typtup = LookupTypeName(pstate, t, NULL, false);

			if (OidIsValid(pstate->p_pkgoid) && !iswith)
				pstate->parse_pkg_func_header = false;

			if (typtup)
			{
				if (!((Form_pg_type) GETSTRUCT(typtup))->typisdefined)
				{
					/* As above, hard error if language is SQL */
					if (languageOid == SQLlanguageId)
						ereport(ERROR,
								(errcode(ERRCODE_INVALID_FUNCTION_DEFINITION),
								errmsg("SQL function cannot accept shell type %s",
										TypeNameToString(t))));
					/* We don't allow creating aggregates on shell types either */
					else if (objtype == OBJECT_AGGREGATE)
						ereport(ERROR,
								(errcode(ERRCODE_INVALID_FUNCTION_DEFINITION),
								errmsg("aggregate cannot accept shell type %s",
										TypeNameToString(t))));
					else
						ereport(NOTICE,
								(errcode(ERRCODE_WRONG_OBJECT_TYPE),
								errmsg("argument type %s is only a shell",
										TypeNameToString(t))));
				}
				toid = typeTypeId(typtup);
				ReleaseSysCache(typtup);
			}
			else
			{
				if (check_function_pludt)
				{
					ereport(ERROR,
						(errcode(ERRCODE_UNDEFINED_OBJECT),
						 errmsg("type %s does not exist",
								TypeNameToString(t))));
					toid = InvalidOid;	/* keep compiler quiet */
				}
				else
				{
					/*
					 * When check_function_pludt is off (usually when pg_dump is
					 * called), the type cannot be found, so the type is treated
					 * as a PLUDT in the package.
					 */
					toid = PLUDTOID;
					udtnames[nudt] = CStringGetTextDatum(TypeNameToString(t));
					nudt++;
				}

			}
		}

		aclresult = pg_type_aclcheck(toid, GetUserId(), ACL_USAGE);
		if (aclresult != ACLCHECK_OK)
			aclcheck_error_type(aclresult, toid);

		if (t->setof)
		{
			if (objtype == OBJECT_AGGREGATE)
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_FUNCTION_DEFINITION),
						 errmsg("aggregates cannot accept set arguments")));
			else if (objtype == OBJECT_PROCEDURE)
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_FUNCTION_DEFINITION),
						 errmsg("procedures cannot accept set arguments")));
			else
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_FUNCTION_DEFINITION),
						 errmsg("functions cannot accept set arguments")));
		}

		/* handle input parameters */
		if (fp->mode != FUNC_PARAM_OUT && fp->mode != FUNC_PARAM_TABLE)
			isinput = true;

		/* handle signature parameters */
		if (fp->mode == FUNC_PARAM_IN || fp->mode == FUNC_PARAM_INOUT ||
			(objtype == OBJECT_PROCEDURE && fp->mode == FUNC_PARAM_OUT) ||
#ifdef _PG_ORCL_
			(ORA_MODE && IsUnderPostmaster && (!creating_extension || different_func_argret) && objtype == OBJECT_FUNCTION
					&& fp->mode == FUNC_PARAM_OUT) ||
#endif
			fp->mode == FUNC_PARAM_VARIADIC)
		{
			/* other signature parameters can't follow a VARIADIC parameter */
			if (varCount > 0)
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_FUNCTION_DEFINITION),
						 errmsg("VARIADIC parameter must be the last signature parameter")));
			sigArgTypes[sigArgCount++] = toid;
		}

		/* handle output parameters */
		if (fp->mode != FUNC_PARAM_IN && fp->mode != FUNC_PARAM_VARIADIC)
		{
			if (objtype == OBJECT_PROCEDURE)
				*requiredResultType = RECORDOID;
			else if (outCount == 0)	/* save first output param's type */
				*requiredResultType = toid;
			outCount++;
		}

		if (fp->mode == FUNC_PARAM_VARIADIC)
		{
			*variadicArgType = toid;
			varCount++;
			/* validate variadic parameter type */
			switch (toid)
			{
				case ANYARRAYOID:
				case ANYOID:
					/* okay */
					break;
				default:
					if (!OidIsValid(get_element_type(toid)))
						ereport(ERROR,
								(errcode(ERRCODE_INVALID_FUNCTION_DEFINITION),
								 errmsg("VARIADIC parameter must be an array")));
					break;
			}
		}

		allTypes[i] = ObjectIdGetDatum(toid);

		paramModes[i] = CharGetDatum(fp->mode);

		if (fp->name && fp->name[0])
		{
			ListCell   *px;

			/*
			 * As of Postgres 9.0 we disallow using the same name for two
			 * input or two output function parameters.  Depending on the
			 * function's language, conflicting input and output names might
			 * be bad too, but we leave it to the PL to complain if so.
			 */
			foreach(px, parameters)
			{
				FunctionParameter *prevfp = (FunctionParameter *) lfirst(px);

				if (prevfp == fp)
					break;
				/* pure in doesn't conflict with pure out */
				if ((fp->mode == FUNC_PARAM_IN ||
					 fp->mode == FUNC_PARAM_VARIADIC) &&
					(prevfp->mode == FUNC_PARAM_OUT ||
					 prevfp->mode == FUNC_PARAM_TABLE))
					continue;
				if ((prevfp->mode == FUNC_PARAM_IN ||
					 prevfp->mode == FUNC_PARAM_VARIADIC) &&
					(fp->mode == FUNC_PARAM_OUT ||
					 fp->mode == FUNC_PARAM_TABLE))
					continue;
				if (prevfp->name && prevfp->name[0] &&
					strcmp(prevfp->name, fp->name) == 0)
					ereport(ERROR,
							(errcode(ERRCODE_INVALID_FUNCTION_DEFINITION),
							 errmsg("parameter name \"%s\" used more than once",
									fp->name)));
			}

			paramNames[i] = CStringGetTextDatum(fp->name);
			have_names = true;
		}

		if (fp->defexpr)
		{
			Node	   *def;

			if (!isinput)
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_FUNCTION_DEFINITION),
						 errmsg("only input parameters can have default values")));

			def = transformExpr(pstate, fp->defexpr,
								EXPR_KIND_FUNCTION_DEFAULT);

			def = coerce_to_specific_type(pstate, def, toid, "DEFAULT");
			assign_expr_collations(pstate, def);

			/*
			 * Make sure no variables are referred to (this is probably dead
			 * code now that add_missing_from is history).
			 */
			if (list_length(pstate->p_rtable) != 0 ||
				contain_var_clause(def))
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_COLUMN_REFERENCE),
						 errmsg("cannot use table references in parameter default value")));

			/*
			 * transformExpr() should have already rejected subqueries,
			 * aggregates, and window functions, based on the EXPR_KIND_ for a
			 * default expression.
			 *
			 * It can't return a set either --- but coerce_to_specific_type
			 * already checked that for us.
			 *
			 * Note: the point of these restrictions is to ensure that an
			 * expression that, on its face, hasn't got subplans, aggregates,
			 * etc cannot suddenly have them after function default arguments
			 * are inserted.
			 */

			*parameterDefaults = lappend(*parameterDefaults, def);
			have_defaults = true;
		}
		else
		{
			if (ORA_MODE)
			{
				/* only 'In' parameters can have default values */
				// TODO: why append a list? NIL is an empty list.
				//       list_concat(*parameterDefaults, NIL)???
				if (isinput)
					*parameterDefaults = lappend(*parameterDefaults, NIL);
			}
			else
			{
				if (isinput && have_defaults)
					ereport(ERROR,
							(errcode(ERRCODE_INVALID_FUNCTION_DEFINITION),
							 errmsg("input parameters after one with a default value must also have defaults")));
			}
		}

		i++;
	}

	/* Now construct the proper outputs as needed */
	*parameterTypes = buildoidvector(sigArgTypes, sigArgCount);

	if (outCount > 0 || varCount > 0)
	{
		*allParameterTypes = construct_array(allTypes, parameterCount, OIDOID,
											 sizeof(Oid), true, 'i');
		*parameterModes = construct_array(paramModes, parameterCount, CHAROID,
										  1, true, 'c');
		if (outCount > 1)
			*requiredResultType = RECORDOID;
		/* otherwise we set requiredResultType correctly above */
	}
	else
	{
		*allParameterTypes = NULL;
		*parameterModes = NULL;
	}

	if (have_names)
	{
		for (i = 0; i < parameterCount; i++)
		{
			if (paramNames[i] == PointerGetDatum(NULL))
				paramNames[i] = CStringGetTextDatum("");
		}
		*parameterNames = construct_array(paramNames, parameterCount, TEXTOID,
										  -1, false, 'i');
	}
	else
		*parameterNames = NULL;

#ifdef _PG_ORCL_
	if (ORA_MODE && nudt)
	{
		if (parameterUdtNames)
			*parameterUdtNames = construct_array(udtnames, nudt, TEXTOID,
													-1, false, 'i');
	}
#endif
}


/*
 * Recognize one of the options that can be passed to both CREATE
 * FUNCTION and ALTER FUNCTION and return it via one of the out
 * parameters. Returns true if the passed option was recognized. If
 * the out parameter we were going to assign to points to non-NULL,
 * raise a duplicate-clause error.  (We don't try to detect duplicate
 * SET parameters though --- if you're redundant, the last one wins.)
 */
static bool
compute_common_attribute(ParseState *pstate,
						 bool is_procedure,
						 DefElem *defel,
						 DefElem **volatility_item,
						 DefElem **strict_item,
						 DefElem **security_item,
						 DefElem **leakproof_item,
                         DefElem **pushdow_item,
						 List **set_items,
						 DefElem **cost_item,
						 DefElem **rows_item,
						 DefElem **parallel_item,
						 DefElem **pipelined_item)
{
	if (strcmp(defel->defname, "volatility") == 0)
	{
		if (is_procedure)
			goto procedure_error;
		if (*volatility_item)
			goto duplicate_error;

		*volatility_item = defel;
	}
	else if (strcmp(defel->defname, "strict") == 0)
	{
		if (is_procedure)
			goto procedure_error;
		if (*strict_item)
			goto duplicate_error;

		*strict_item = defel;
	}
	else if (strcmp(defel->defname, "security") == 0)
	{
		if (*security_item)
			goto duplicate_error;

		*security_item = defel;
	}
	else if (strcmp(defel->defname, "leakproof") == 0)
	{
		if (is_procedure)
			goto procedure_error;
		if (*leakproof_item)
			goto duplicate_error;

		*leakproof_item = defel;
	}
    else if (strcmp(defel->defname, "pushdown") == 0)
    {
        if (*pushdow_item)
            goto duplicate_error;
        
        *pushdow_item = defel;
    }
	else if (strcmp(defel->defname, "set") == 0)
	{
		*set_items = lappend(*set_items, defel->arg);
	}
	else if (strcmp(defel->defname, "cost") == 0)
	{
		if (is_procedure)
			goto procedure_error;
		if (*cost_item)
			goto duplicate_error;

		*cost_item = defel;
	}
	else if (strcmp(defel->defname, "rows") == 0)
	{
		if (is_procedure)
			goto procedure_error;
		if (*rows_item)
			goto duplicate_error;

		*rows_item = defel;
	}
	else if (strcmp(defel->defname, "parallel") == 0)
	{
		if (is_procedure)
			goto procedure_error;
		if (*parallel_item)
			goto duplicate_error;

		*parallel_item = defel;
	}
	else if (strcmp(defel->defname, "pipelined") == 0)
	{
		if (is_procedure)
			goto procedure_error;
		if (*pipelined_item)
			goto duplicate_error;
		
		*pipelined_item = defel;
	}
	else
		return false;

	/* Recognized an option */
	return true;

duplicate_error:
	ereport(ERROR,
			(errcode(ERRCODE_SYNTAX_ERROR),
			 errmsg("conflicting or redundant options"),
			 parser_errposition(pstate, defel->location)));
	return false;				/* keep compiler quiet */

procedure_error:
	ereport(ERROR,
			(errcode(ERRCODE_INVALID_FUNCTION_DEFINITION),
			 errmsg("invalid attribute in procedure definition"),
			 parser_errposition(pstate, defel->location)));
	return false;
}

static char
interpret_func_volatility(DefElem *defel)
{
	char	   *str = strVal(defel->arg);

	if (strcmp(str, "immutable") == 0)
		return PROVOLATILE_IMMUTABLE;
	else if (strcmp(str, "stable") == 0)
		return PROVOLATILE_STABLE;
	else if (strcmp(str, "volatile") == 0)
		return PROVOLATILE_VOLATILE;
	else if (strcmp(str, "result_cache") == 0)
		return PROVOLATILE_CACHABLE;
	else
	{
		elog(ERROR, "invalid volatility \"%s\"", str);
		return 0;				/* keep compiler quiet */
	}
}

static char
interpret_func_parallel(DefElem *defel)
{
	char	   *str = strVal(defel->arg);

	if (strcmp(str, "safe") == 0)
		return PROPARALLEL_SAFE;
	else if (strcmp(str, "unsafe") == 0)
		return PROPARALLEL_UNSAFE;
	else if (strcmp(str, "restricted") == 0)
		return PROPARALLEL_RESTRICTED;
	else
	{
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("parameter \"parallel\" must be SAFE, RESTRICTED, or UNSAFE")));
		return PROPARALLEL_UNSAFE;	/* keep compiler quiet */
	}
}

/*
 * Update a proconfig value according to a list of VariableSetStmt items.
 *
 * The input and result may be NULL to signify a null entry.
 */
static ArrayType *
update_proconfig_value(ArrayType *a, List *set_items)
{
	ListCell   *l;

	foreach(l, set_items)
	{
		VariableSetStmt *sstmt = lfirst_node(VariableSetStmt, l);

		if (sstmt->kind == VAR_RESET_ALL)
			a = NULL;
		else
		{
			char	   *valuestr = ExtractSetVariableArgs(sstmt);

			if (valuestr)
				a = GUCArrayAdd(a, sstmt->name, valuestr);
			else				/* RESET */
				a = GUCArrayDelete(a, sstmt->name);
		}
	}

	return a;
}


/*
 * Dissect the list of options assembled in gram.y into function
 * attributes.
 */
static void
compute_attributes_sql_style(ParseState *pstate,
							 bool is_procedure,
							 List *options,
							 List **as,
							 char **language,
							 Node **transform,
							 bool *windowfunc_p,
							 char *volatility_p,
							 bool *strict_p,
							 bool *security_definer,
							 bool *leakproof_p,
                             bool *pushable_p,
							 ArrayType **proconfig,
							 float4 *procost,
							 float4 *prorows,
							 char *parallel_p)
{
	ListCell   *option;
	DefElem    *as_item = NULL;
	DefElem    *language_item = NULL;
	DefElem    *transform_item = NULL;
	DefElem    *windowfunc_item = NULL;
	DefElem    *volatility_item = NULL;
	DefElem    *strict_item = NULL;
	DefElem    *security_item = NULL;
	DefElem    *leakproof_item = NULL;
    DefElem    *pushdown_item = NULL;
	List	   *set_items = NIL;
	DefElem    *cost_item = NULL;
	DefElem    *rows_item = NULL;
	DefElem    *parallel_item = NULL;
	DefElem    *pipelined_item = NULL;

	foreach(option, options)
	{
		DefElem    *defel = (DefElem *) lfirst(option);

		if (strcmp(defel->defname, "as") == 0)
		{
			if (as_item)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options"),
						 parser_errposition(pstate, defel->location)));
			as_item = defel;
		}
		else if (strcmp(defel->defname, "language") == 0)
		{
			if (language_item)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options"),
						 parser_errposition(pstate, defel->location)));
			language_item = defel;
		}
		else if (strcmp(defel->defname, "transform") == 0)
		{
			if (transform_item)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options"),
						 parser_errposition(pstate, defel->location)));
			transform_item = defel;
		}
		else if (strcmp(defel->defname, "window") == 0)
		{
			if (windowfunc_item)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options"),
						 parser_errposition(pstate, defel->location)));
			if (is_procedure)
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_FUNCTION_DEFINITION),
						 errmsg("invalid attribute in procedure definition"),
						 parser_errposition(pstate, defel->location)));
			windowfunc_item = defel;
		}
		else if (compute_common_attribute(pstate,
										  is_procedure,
										  defel,
										  &volatility_item,
										  &strict_item,
										  &security_item,
										  &leakproof_item,
                                          &pushdown_item,
										  &set_items,
										  &cost_item,
										  &rows_item,
										  &parallel_item,
										  &pipelined_item))
		{
			/* recognized common option */
			continue;
		}
		else
			elog(ERROR, "option \"%s\" not recognized",
				 defel->defname);
	}

	/* process required items */
	if (as_item)
		*as = (List *) as_item->arg;
	else
	{
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_FUNCTION_DEFINITION),
				 errmsg("no function body specified")));
		*as = NIL;				/* keep compiler quiet */
	}

	if (language_item)
	{
		*language = strVal(language_item->arg);

		/* To compatible existed test case and open source community */
		if (strcasecmp("default_plsql", *language) == 0)
		{
			if (ORA_MODE)
				*language = pstrdup("oraplsql");
			else
				*language = pstrdup("plpgsql");
		}
	}
	else if (ORA_MODE)
		*language = "oraplsql";
	else
	{
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_FUNCTION_DEFINITION),
				 errmsg("no language specified")));
		*language = NULL;		/* keep compiler quiet */
	}

	/* process optional items */
	if (transform_item)
		*transform = transform_item->arg;
	if (windowfunc_item)
		*windowfunc_p = intVal(windowfunc_item->arg);
	if (volatility_item)
		*volatility_p = interpret_func_volatility(volatility_item);
	if (strict_item)
		*strict_p = intVal(strict_item->arg);
	if (security_item)
		*security_definer = intVal(security_item->arg);
	if (leakproof_item)
		*leakproof_p = intVal(leakproof_item->arg);
	if (pushdown_item)
	{
		*pushable_p = intVal(pushdown_item->arg);
		set_pushdown_attribute = true;
	}
	if (set_items)
		*proconfig = update_proconfig_value(NULL, set_items);
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
	if (parallel_item)
		*parallel_p = interpret_func_parallel(parallel_item);
}


/*-------------
 *	 Interpret the parameters *parameters and return their contents via
 *	 *isStrict_p and *volatility_p.
 *
 *	These parameters supply optional information about a function.
 *	All have defaults if not specified. Parameters:
 *
 *	 * isStrict means the function should not be called when any NULL
 *	   inputs are present; instead a NULL result value should be assumed.
 *
 *	 * volatility tells the optimizer whether the function's result can
 *	   be assumed to be repeatable over multiple evaluations.
 *------------
 */
static void
compute_attributes_with_style(ParseState *pstate, bool is_procedure, List *parameters, bool *isStrict_p, char *volatility_p)
{
	ListCell   *pl;

	foreach(pl, parameters)
	{
		DefElem    *param = (DefElem *) lfirst(pl);

		if (pg_strcasecmp(param->defname, "isstrict") == 0)
		{
			if (is_procedure)
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_FUNCTION_DEFINITION),
						 errmsg("invalid attribute in procedure definition"),
						 parser_errposition(pstate, param->location)));
			*isStrict_p = defGetBoolean(param);
		}
		else if (pg_strcasecmp(param->defname, "iscachable") == 0)
		{
			/* obsolete spelling of isImmutable */
			if (is_procedure)
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_FUNCTION_DEFINITION),
						 errmsg("invalid attribute in procedure definition"),
						 parser_errposition(pstate, param->location)));
			if (defGetBoolean(param))
				*volatility_p = PROVOLATILE_IMMUTABLE;
		}
		else
			ereport(WARNING,
					(errcode(ERRCODE_SYNTAX_ERROR),
					 errmsg("unrecognized function attribute \"%s\" ignored",
							param->defname),
					 parser_errposition(pstate, param->location)));
	}
}


/*
 * For a dynamically linked C language object, the form of the clause is
 *
 *	   AS <object file name> [, <link symbol name> ]
 *
 * In all other cases
 *
 *	   AS <object reference, or sql code>
 */
static void
interpret_AS_clause(Oid languageOid, const char *languageName,
					char *funcname, List *as,
					char **prosrc_str_p, char **probin_str_p)
{
	Assert(as != NIL);

	if (languageOid == ClanguageId)
	{
		/*
		 * For "C" language, store the file name in probin and, when given,
		 * the link symbol name in prosrc.  If link symbol is omitted,
		 * substitute procedure name.  We also allow link symbol to be
		 * specified as "-", since that was the habit in PG versions before
		 * 8.4, and there might be dump files out there that don't translate
		 * that back to "omitted".
		 */
		*probin_str_p = strVal(linitial(as));
		if (list_length(as) == 1)
			*prosrc_str_p = get_c_function_symbol_name(funcname);
		else
		{
			*prosrc_str_p = strVal(lsecond(as));
			if (strcmp(*prosrc_str_p, "-") == 0)
				*prosrc_str_p = get_c_function_symbol_name(funcname);
		}
	}
	else
	{
		/* Everything else wants the given string in prosrc. */
		*prosrc_str_p = strVal(linitial(as));
		*probin_str_p = NULL;

		if (list_length(as) != 1)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_FUNCTION_DEFINITION),
					 errmsg("only one AS item needed for language \"%s\"",
							languageName)));

		if (languageOid == INTERNALlanguageId)
		{
			/*
			 * In PostgreSQL versions before 6.5, the SQL name of the created
			 * function could not be different from the internal name, and
			 * "prosrc" wasn't used.  So there is code out there that does
			 * CREATE FUNCTION xyz AS '' LANGUAGE internal. To preserve some
			 * modicum of backwards compatibility, accept an empty "prosrc"
			 * value as meaning the supplied SQL function name.
			 */
			if (strlen(*prosrc_str_p) == 0)
				*prosrc_str_p = get_c_function_symbol_name(funcname);
		}
	}
}

ObjectAddress
CreateFuncByNamespaceAndName(ParseState *pstate, CreateFunctionStmt *stmt,
							 Oid namespaceId, char* funcname,
							 bool iswith, HeapTuple *tuple, HeapTuple *compile_tuple,
							 bool check_internal, Oid p_pkgid, bool notice)
{
	char	   *probin_str;
	char	   *prosrc_str;
	Oid			prorettype = InvalidOid;
	bool		returnsSet;
	char	   *language;
	Oid			languageOid;
	Oid			languageValidator;
	Node	   *transformDefElem = NULL;
	oidvector  *parameterTypes;
	ArrayType  *allParameterTypes;
	ArrayType  *parameterModes;
	ArrayType  *parameterNames;
#ifdef _PG_ORCL_
	ArrayType	*parameterUdtNames = NULL;
	Oid			pkg_oid = InvalidOid;
	char		*udtname = NULL;
#endif
	List	   *parameterDefaults;
	Oid			variadicArgType;
	List	   *trftypes_list = NIL;
	ArrayType  *trftypes;
	Oid			requiredResultType;
	bool		isWindowFunc,
				isStrict,
				security,
				isLeakProof,
                isPushdown;
	char		volatility;
	ArrayType  *proconfig;
	float4		procost;
	float4		prorows;
	HeapTuple	languageTuple;
	Form_pg_language languageStruct;
	List	   *as_clause;
	char		parallel;
	List	   *syn_oids = NIL;
	ObjectAddress	funcaddr;

	/* default attributes */
	isWindowFunc = false;
	isStrict = false;
	security = false;
	isLeakProof = false;
    isPushdown = false;
	volatility = PROVOLATILE_VOLATILE;
	proconfig = NULL;
	procost = -1;				/* indicates not set */
	prorows = -1;				/* indicates not set */
	parallel = PROPARALLEL_UNSAFE;

	/* override attributes from explicit list */
	compute_attributes_sql_style(pstate,
								 stmt->is_procedure,
								 stmt->options,
								 &as_clause, &language, &transformDefElem,
								 &isWindowFunc, &volatility,
								 &isStrict, &security, &isLeakProof, &isPushdown,
								 &proconfig, &procost, &prorows, &parallel);

	/* Look up the language and validate permissions */
	languageTuple = SearchSysCache1(LANGNAME, PointerGetDatum(language));
	if (!HeapTupleIsValid(languageTuple))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("language \"%s\" does not exist", language),
				 (PLTemplateExists(language) ?
				  errhint("Use CREATE LANGUAGE to load the language into the database.") : 0)));

	languageOid = HeapTupleGetOid(languageTuple);
	languageStruct = (Form_pg_language) GETSTRUCT(languageTuple);

	if (languageStruct->lanpltrusted)
	{
		/* if trusted language, need USAGE privilege */
		AclResult	aclresult;

		aclresult = pg_language_aclcheck(languageOid, GetUserId(), ACL_USAGE);
		if (aclresult != ACLCHECK_OK)
			aclcheck_error(aclresult, ACL_KIND_LANGUAGE,
						   NameStr(languageStruct->lanname));
	}
	else
	{
		/* if untrusted language, must be superuser */
#ifdef _PG_ORCL_
		if (check_internal)
#endif
		if (!superuser())
			aclcheck_error(ACLCHECK_NO_PRIV, ACL_KIND_LANGUAGE,
						   NameStr(languageStruct->lanname));
	}

	pstate->langValidatorOid = languageStruct->lanvalidator;
	/* type object member function will be validated outside */
	if (stmt->info == OBJ_FUNC_OTHER)
		languageValidator = languageStruct->lanvalidator;
	else
		languageValidator = InvalidOid;

	ReleaseSysCache(languageTuple);

	/*
	 * Only superuser is allowed to create leakproof functions because
	 * leakproof functions can see tuples which have not yet been filtered out
	 * by security barrier views or row level security policies.
	 */
	if (isLeakProof && !superuser())
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("only superuser can define a leakproof function")));

	if (transformDefElem)
	{
		ListCell   *lc;

		foreach(lc, castNode(List, transformDefElem))
		{
			Oid			typeid = typenameTypeId(NULL,
												lfirst_node(TypeName, lc));
			Oid			elt = get_base_element_type(typeid);

			typeid = elt ? elt : typeid;

			get_transform_oid(typeid, languageOid, false);
			trftypes_list = lappend_oid(trftypes_list, typeid);
		}
	}

	/*
	 * Convert remaining parameters of CREATE to form wanted by
	 * ProcedureCreate.
	 */
	interpret_function_parameter_list(pstate,
									  stmt->parameters,
									  languageOid,
									  stmt->is_procedure ? OBJECT_PROCEDURE : OBJECT_FUNCTION,
									  &parameterTypes,
									  &allParameterTypes,
									  &parameterModes,
									  &parameterNames,
#ifdef _PG_ORCL_
									  &parameterUdtNames, p_pkgid,
									  &syn_oids,
#endif
									  &parameterDefaults,
									  &variadicArgType,
									  &requiredResultType,
									  iswith,
									  notice);

	if (stmt->is_procedure)
	{
		Assert(!stmt->returnType);
		prorettype = requiredResultType ? requiredResultType : VOIDOID;
		returnsSet = false;
	}
	else if (stmt->returnType)
	{
		returnsSet = false;
		if (ORA_MODE && stmt->info == OBJ_FUNC_OTHER)
		{
			Oid		syn_oid = InvalidOid;
			bool	found = false;

			if (stmt->returnType->pct_type)
			{
				/* Find var%type in packages */
				found = LookupPackageVarType(stmt->returnType, &pkg_oid, &udtname, p_pkgid, &prorettype, notice);
			}
			else if (!stmt->returnType->pct_type && !stmt->returnType->pct_rowtype)
				found = LookupPackageUDT(stmt->returnType, &pkg_oid, &udtname, p_pkgid, &prorettype, &syn_oid);

			if (found)
			{
				ObjectAddress	pkgaddr;

				if (prorettype != PLUDTOID)
				{
					/* Disable save up package UDT if it is a SQL base type */
					udtname = NULL;
				}
				else if (OidIsValid(pkg_oid) && OidIsValid(p_pkgid) && p_pkgid != pkg_oid)
				{
					pkgaddr.classId = PackageRelationId;
					pkgaddr.objectId = p_pkgid;
					pkgaddr.objectSubId = InvalidOid;
					GeneratePkgDependence(pkgaddr, PackageRelationId, pkg_oid);
				}

				if (OidIsValid(syn_oid))
				{
					if (OidIsValid(p_pkgid))
					{
						pkgaddr.classId = PackageRelationId;
						pkgaddr.objectId = p_pkgid;
						pkgaddr.objectSubId = InvalidOid;

						/*
						 * If the current function is within a package, add
						 * the package's dependency on the synonym.
						 */
						GeneratePkgDependence(pkgaddr, SynonymRelationId, syn_oid);
					}
					else
					{
						/* Record it and add the dependency after creating the function */
						if (!list_member_oid(syn_oids, syn_oid))
							syn_oids = lappend_oid(syn_oids, syn_oid);
					}
				}
			}
		}

		if (!OidIsValid(prorettype))
		{
			/* explicit RETURNS clause */
			compute_return_type(pstate, stmt->returnType, languageOid,
								&prorettype, &returnsSet, p_pkgid,
								iswith, &udtname);
		}

		if (ORA_MODE && (LONGOID == prorettype || LONGARROID == prorettype))
		{
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_FUNCTION_DEFINITION),
					 errmsg("function output type must not be \"long\" type")));
		}

		if (OidIsValid(requiredResultType) && prorettype != requiredResultType)
		{
#ifdef _PG_ORCL_
			if (!ORA_MODE)
#endif
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_FUNCTION_DEFINITION),
					 errmsg("function result type must be %s because of OUT parameters",
							format_type_be(requiredResultType))));
		}
	}
	else if (OidIsValid(requiredResultType))
	{
		/* default RETURNS clause from OUT parameters */
		prorettype = requiredResultType;
		returnsSet = false;
	}
	else
	{
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_FUNCTION_DEFINITION),
				 errmsg("function result type must be specified")));
		/* Alternative possibility: default to RETURNS VOID */
		prorettype = VOIDOID;
		returnsSet = false;
	}

	if (list_length(trftypes_list) > 0)
	{
		ListCell   *lc;
		Datum	   *arr;
		int			i;

		arr = palloc(list_length(trftypes_list) * sizeof(Datum));
		i = 0;
		foreach(lc, trftypes_list)
			arr[i++] = ObjectIdGetDatum(lfirst_oid(lc));
		trftypes = construct_array(arr, list_length(trftypes_list),
								   OIDOID, sizeof(Oid), true, 'i');
	}
	else
	{
		/* store SQL NULL instead of empty array */
		trftypes = NULL;
	}

	compute_attributes_with_style(pstate, stmt->is_procedure, stmt->withClause, &isStrict, &volatility);

	/* Treat ODCIAggregateIterate as Strict function (ignore null input) */
	if (ORA_MODE && stmt->info == OBJ_FUNC_MEMBER &&
		(pg_strcasecmp(funcname, ODCIFunctionNames[ODCI_AGGREGATE_ITERATE]) == 0))
		isStrict = true;

	interpret_AS_clause(languageOid, language, funcname, as_clause,
						&prosrc_str, &probin_str);

	/*
	 * Set default values for COST and ROWS depending on other parameters;
	 * reject ROWS if it's not returnsSet.  NB: pg_dump knows these default
	 * values, keep it in sync if you change them.
	 */
	if (procost < 0)
	{
		/* SQL and PL-language functions are assumed more expensive */
		if (languageOid == INTERNALlanguageId ||
			languageOid == ClanguageId)
			procost = 1;
		else
			procost = 100;
	}
	if(isPushdown)
	    procost = -procost;
	
	if (prorows < 0)
	{
		if (returnsSet)
			prorows = 1000;
		else
			prorows = 0;		/* dummy value if not returnsSet */
	}
	else if (!returnsSet)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("ROWS is not applicable when function does not return a set")));

#ifdef _PG_ORCL_
	if (!ORA_MODE && iswith)
	{
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_FUNCTION_DEFINITION),
					 errmsg("only opentenbase_ora mode support WITH FUNCTION feature")));
	}
	else
#endif

	/*
	 * And now that we have all the parameters, and know we're permitted to do
	 * so, go ahead and create the function.
	 */
	funcaddr = ProcedureCreate(funcname,
							   namespaceId,
							   stmt->replace,
							   returnsSet,
							   prorettype,
							   GetUserId(),
							   languageOid,
							   languageValidator,
							   prosrc_str,	/* converted to text later */
							   probin_str,	/* converted to text later */
							   stmt->is_procedure ? PROKIND_PROCEDURE : (isWindowFunc ? PROKIND_WINDOW : PROKIND_FUNCTION),
							   security,
							   isLeakProof,
							   isStrict,
							   volatility,
							   parallel,
							   parameterTypes,
							   PointerGetDatum(allParameterTypes),
							   PointerGetDatum(parameterModes),
							   PointerGetDatum(parameterNames),
							   parameterDefaults,
							   PointerGetDatum(trftypes),
							   PointerGetDatum(proconfig),
							   procost,
							   prorows,
							   iswith,
							   tuple,
							   compile_tuple,
							   parameterUdtNames,
							   udtname);

	if (ORA_MODE && OidIsValid(funcaddr.objectId))
	{
		ListCell	*lc;

		foreach(lc, syn_oids)
		{
			Oid				syn_oid;
			ObjectAddress	referenced;

			syn_oid = lfirst_oid(lc);
			referenced.classId = SynonymRelationId;
			referenced.objectId = syn_oid;
			referenced.objectSubId = 0;
			recordDependencyOn(&funcaddr, &referenced, DEPENDENCY_NORMAL);
		}
	}

	if (syn_oids)
		list_free(syn_oids);

	return funcaddr;
}

/*
 * CreateFunction
 *	 Execute a CREATE FUNCTION utility statement.
 */
ObjectAddress
CreateFunction(ParseState *pstate, CreateFunctionStmt *stmt)
{
    char	   *funcname;
    Oid			namespaceId;
    AclResult	aclresult;
    char        functoken[9] = {0};
    int         i = 0;
	ObjectAddress myself;

   if (ORA_MODE)
   {
        PLPGSQL_LOAD_FILE;
        pstate->p_post_columnref_hook = plpgsql_post_column;
   }

    if (stmt->pkginner)
    {
        for (i = 0; i < sizeof(functoken) - 1; ++i)
        {
            functoken[i] = pstate->p_sourcetext[i];
        }
        functoken[sizeof(functoken) - 1] = '\0';

        ereport(ERROR,
                (errcode(ERRCODE_SYNTAX_ERROR),
                        errmsg("syntax error at or near \"%s\"", functoken),
                        parser_errposition(pstate, 0)));
    }

	if (ORA_MODE && (list_length(stmt->funcname) > 1 && CheckIsPackageFunction(stmt->funcname, &funcname)))
	{
		ereport(ERROR,
				(errcode(ERRCODE_DUPLICATE_FUNCTION),
				 errmsg("direct creation of package functions %s is not supported.",
					 	funcname)));
	}

	/* Convert list of names to a name and namespace */
    namespaceId = QualifiedNameGetCreationNamespaceImpl(stmt->funcname,
                                                    &funcname, false);

    /* Check we have creation rights in target namespace */
    aclresult = pg_namespace_aclcheck(namespaceId, GetUserId(), ACL_CREATE);
    if (aclresult != ACLCHECK_OK)
	{
        aclcheck_error(aclresult, ACL_KIND_NAMESPACE,
                       get_namespace_name(namespaceId));
	}

	set_pushdown_attribute = false;

	myself = CreateFuncByNamespaceAndName(pstate, stmt, namespaceId, funcname,
										  false, NULL, NULL, true, InvalidOid,
										  true);

	if (!IS_CENTRALIZED_DATANODE && !set_pushdown_attribute && enable_auto_pushdown_function &&
		myself.objectId >= FirstNormalObjectId &&
		get_func_prokind(myself.objectId) == PROKIND_FUNCTION)
	{
		FuncCheckData ckdata = {.type = T_FuncCheckData,
								.pullup = false,
								.relid = InvalidOid,
								.tgtype = 0};
		(void)CheckFuncPushdown(myself.objectId, (Node *)&ckdata);
	}

	return myself;
}

static bool
check_output_argument(Oid funcoid)
{
	HeapTuple tuple;
	int numargs;
	Oid *argtypes;
	char **argnames;
	char *argmodes;
	int i;

	tuple = SearchSysCache1(PROCOID, funcoid);
	if (!HeapTupleIsValid(tuple))
		elog(ERROR, "cache lookup failed for function %u", funcoid);
	numargs = get_func_arg_info(tuple, &argtypes, &argnames, &argmodes);
	ReleaseSysCache(tuple);

	for (i = 0; i < numargs; i++)
	{
		if (argmodes && (argmodes[i] == PROARGMODE_INOUT || argmodes[i] == PROARGMODE_OUT))
		{
			return true;
		}
	}

	return false;
}

/*
 * Check whether the function can be pushed down, and alter its corresponding
 * attribute if it can be pushed down, i.e., change its cost to negative number.
 */
bool
CheckFuncPushdown(Oid funcoid, Node *chkdata)
{
	FunctionCallInfoData fcinfo;
	FmgrInfo flinfo;
	char *langname;
	char *funcname;
	char *namespace;
	float4 cost;
	Oid rettypeid;
	bool check_pullup;
	MemoryContext old_context;
	ResourceOwner old_owner;
	ResourceOwner func_check_owner;
	Oid *argtypes = NULL;
	int nargs = 0;
	int i;
	bool is_for_trigger = ((FuncCheckData *)chkdata)->tgtype > 0;

	Assert(chkdata != NULL && IsA(chkdata, FuncCheckData));

	cost = get_func_cost_with_sign(funcoid);
	if (!is_for_trigger && cost < 0)
	{
		return true;
	}

	langname = get_language_name(get_func_lang(funcoid), false);
	if (strcmp(langname, "plpgsql") != 0 && strcmp(langname, "oraplsql") != 0)
	{
		return cost < 0;
	}

	rettypeid = get_func_signature(funcoid, &argtypes, &nargs);

	/* Trigger function will be pushdown when creating trigger. */
	if (!is_for_trigger && (rettypeid == TRIGGEROID || rettypeid == EVTTRIGGEROID))
	{
		return cost < 0;
	}
	/*
	 * If the function has polymorphic type, we need to replace any polymorphic types
	 * (ANYELEMENT etc) with correct data types deduced from the input arguments (see
	 * plpgsql_resolve_polymorphic_argtypes) when compiling the function, but we have
	 * no input parameters when creating function, so just return here.
	 */
	if (IsPolymorphicType(rettypeid))
	{
		return cost < 0;
	}
	for (i = 0; i < nargs; i++)
	{
		if (IsPolymorphicType(argtypes[i]))
		{
			return cost < 0;
		}
	}

	/*
	 * If the function has input of anonymous composite type, with arguments like "(INOUT, ...)"
	 * or "(OUT, ...)", we won't pushdown it automatically. Because when it is called by other
	 * functions nestedly, if it is pushed down, it is not supported and an error "input of 
	 * anonymous composite types is not implemented" will be reported.
	 */
	if (rettypeid == RECORDOID && check_output_argument(funcoid))
	{
		return cost < 0;
	}

	fmgr_info(funcoid, &flinfo);
	flinfo.fn_mcxt = AllocSetContextCreate(CurrentMemoryContext,
										   "check function pushdown",
										   ALLOCSET_DEFAULT_SIZES);
	old_context = MemoryContextSwitchTo(flinfo.fn_mcxt);
	old_owner = CurrentResourceOwner;
	func_check_owner = ResourceOwnerCreate(CurrentResourceOwner, "FunctionCheckOwner");
	CurrentResourceOwner = func_check_owner;

	/*
	 * Call the function, passing no arguments but setting a context.
	 */
	InitFunctionCallInfoData(fcinfo, &flinfo, 0,
							 InvalidOid, chkdata, NULL);

	PG_TRY();
	{
		FunctionCallInvoke(&fcinfo);
		check_pullup = ((FuncCheckData *)chkdata)->pullup;
	}
	PG_CATCH();
	{
		ErrorData *edata = NULL;

		(void)MemoryContextSwitchTo(old_context);
		SPI_finish();

		HOLD_INTERRUPTS();
		edata = CopyErrorData();
		FlushErrorState();
		RESUME_INTERRUPTS();

		elog(is_for_trigger ? WARNING : LOG, "Evaluate trigger function failed, "
											 "function if forced to pull up reason: %s",
			 edata->message);

		check_pullup = true;
		FreeErrorData(edata);
	}
	PG_END_TRY();

	(void)MemoryContextSwitchTo(old_context);
	MemoryContextDelete(flinfo.fn_mcxt);
	CurrentResourceOwner = old_owner;
	ResourceOwnerRelease(func_check_owner, RESOURCE_RELEASE_BEFORE_LOCKS, false, false);
	ResourceOwnerRelease(func_check_owner, RESOURCE_RELEASE_LOCKS, false, false);
	ResourceOwnerRelease(func_check_owner, RESOURCE_RELEASE_AFTER_LOCKS, false, false);
	ResourceOwnerDelete(func_check_owner);

	funcname = get_func_name(funcoid);
	namespace = get_namespace_name(get_func_namespace(funcoid));
	if (cost < 0 && check_pullup)
	{
		ereport(WARNING,
				(errcode(ERRCODE_E_R_I_E_TRIGGER_PROTOCOL_VIOLATED),
				 errmsg("function %s is forced to push down while it should not",
						funcname),
				 errhint("if you want to change this behavior, use "
						 "ALTER FUNCTION %s NOT PUSHDOWN "
						 "otherwise you may LOSE DATA !!",
						 funcname)));
	}
	else if (!check_pullup)
	{
		ereport(is_for_trigger ? NOTICE : LOG,
				(errcode(ERRCODE_E_R_I_E_TRIGGER_PROTOCOL_VIOLATED),
				 errmsg("function %s is evaluated to push down to DN for trigger execution",
						funcname),
				 errhint("if you want to change this behavior, use "
						 "ALTER FUNCTION %s NOT PUSHDOWN",
						 funcname)));

		/* func is now set as "pullup", alter it by constructing a stmt */
		if (cost >= 0)
		{
			ParseState *pstate = make_parsestate(NULL);
			AlterFunctionStmt *afstmt = makeNode(AlterFunctionStmt);
			cost = -cost;

			afstmt->objtype = OBJECT_FUNCTION;
			afstmt->func = makeNode(ObjectWithArgs);
			afstmt->func->objname = list_make2(makeString(namespace),
											   makeString(funcname));
			if (is_for_trigger)
			{
				/* trigger function has no args; */
				afstmt->func->args_unspecified = true;
				afstmt->func->objargs = NIL;
			}
			else
			{
				for (i = 0; i < nargs; i++)
				{
					afstmt->func->objargs = lappend(afstmt->func->objargs, (void *)makeTypeNameFromOid(argtypes[i], -1));
				}
			}
			afstmt->actions = list_make1((void *)makeDefElem("pushdown", (Node *)makeInteger(TRUE), -1));

			AlterFunction(pstate, afstmt);
			CommandCounterIncrement();
		}
	}

	return cost < 0;
}

/*
 * Guts of function deletion.
 *
 * Note: this is also used for aggregate deletion, since the OIDs of
 * both functions and aggregates point to pg_proc.
 */
void
RemoveFunctionById(Oid funcOid)
{
	Relation	relation;
	HeapTuple	tup;
	char		prokind;

	/*
	 * Delete the pg_proc tuple.
	 */
	relation = heap_open(ProcedureRelationId, RowExclusiveLock);

	/*
	 * if the function is a builtin function, its Oid is less than 10000.
	 * we can't allow removing the builtin functions
	 */
	if (IsSystemObjOid(funcOid) && IsInplaceUpgrade == false)
		ereport(ERROR,
			(errcode(ERRCODE_INVALID_FUNCTION_DEFINITION),
			errmsg("the builtin function can not be removed,its function oid is \"%u\"",
			funcOid)));

	tup = SearchSysCache1(PROCOID, ObjectIdGetDatum(funcOid));
	if (!HeapTupleIsValid(tup)) /* should not happen */
		elog(ERROR, "cache lookup failed for function %u", funcOid);

	prokind = ((Form_pg_proc) GETSTRUCT(tup))->prokind;

	CatalogTupleDelete(relation, &tup->t_self);

	ReleaseSysCache(tup);

	heap_close(relation, RowExclusiveLock);

	/*
	 * If there's a pg_aggregate tuple, delete that too.
	 */
	if (prokind == PROKIND_AGGREGATE)
	{
		relation = heap_open(AggregateRelationId, RowExclusiveLock);

		tup = SearchSysCache1(AGGFNOID, ObjectIdGetDatum(funcOid));
		if (!HeapTupleIsValid(tup)) /* should not happen */
			elog(ERROR, "cache lookup failed for pg_aggregate tuple for function %u", funcOid);

		CatalogTupleDelete(relation, &tup->t_self);

		ReleaseSysCache(tup);

		heap_close(relation, RowExclusiveLock);
	}

#ifdef _PG_ORCL_
	DropCompile(ProcedureRelationId, funcOid);
#endif
}

/*
 * Implements the ALTER FUNCTION utility command (except for the
 * RENAME and OWNER clauses, which are handled as part of the generic
 * ALTER framework).
 */
ObjectAddress
AlterFunction(ParseState *pstate, AlterFunctionStmt *stmt)
{
	HeapTuple	tup;
	Oid			funcOid;
	Form_pg_proc procForm;
	bool		is_procedure;
	Relation	rel;
	ListCell   *l;
	DefElem    *volatility_item = NULL;
	DefElem    *strict_item = NULL;
	DefElem    *security_def_item = NULL;
	DefElem    *leakproof_item = NULL;
    DefElem    *pushdown_item = NULL;
	List	   *set_items = NIL;
	DefElem    *cost_item = NULL;
	DefElem    *rows_item = NULL;
	DefElem    *parallel_item = NULL;
	DefElem    *pipelined_item = NULL;
	ObjectAddress address;

	rel = heap_open(ProcedureRelationId, RowExclusiveLock);

	funcOid = LookupFuncWithArgs(stmt->objtype, stmt->func, false);

	/*
	 * if the function is a builtin function, its Oid is less than 10000.
	 * we can't allow alter the builtin functions
	 */
	if (IsSystemObjOid(funcOid) && IsInplaceUpgrade == false)
	{
		ereport(ERROR,
			(errcode(ERRCODE_INVALID_FUNCTION_DEFINITION),
				errmsg("function \"%s\" is a builtin function,it can not be altered",
						NameListToString(stmt->func->objname))));
	}

	tup = SearchSysCacheCopy1(PROCOID, ObjectIdGetDatum(funcOid));
	if (!HeapTupleIsValid(tup)) /* should not happen */
		elog(ERROR, "cache lookup failed for function %u", funcOid);

	procForm = (Form_pg_proc) GETSTRUCT(tup);

	/* Permission check: must own function */
	if (!pg_proc_ownercheck(funcOid, GetUserId()))
		aclcheck_error(ACLCHECK_NOT_OWNER, ACL_KIND_PROC,
					   NameListToString(stmt->func->objname));

	if (procForm->prokind == PROKIND_AGGREGATE)
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("\"%s\" is an aggregate function",
						NameListToString(stmt->func->objname))));

	is_procedure = (procForm->prokind == PROKIND_PROCEDURE);

	/* Examine requested actions. */
	foreach(l, stmt->actions)
	{
		DefElem    *defel = (DefElem *) lfirst(l);

		if (compute_common_attribute(pstate,
									 is_procedure,
									 defel,
									 &volatility_item,
									 &strict_item,
									 &security_def_item,
									 &leakproof_item,
                                     &pushdown_item,
									 &set_items,
									 &cost_item,
									 &rows_item,
									 &parallel_item,
									 &pipelined_item) == false)
			elog(ERROR, "option \"%s\" not recognized", defel->defname);
	}

	if (volatility_item)
	{
		procForm->provolatile = interpret_func_volatility(volatility_item);
		if (funcOid > FirstNormalObjectId)
			CheckFuncResultCachable(procForm->provolatile, procForm->proretset, 
									procForm->pronargs, &procForm->proargtypes);
		else if (procForm->provolatile == PROVOLATILE_CACHABLE)
			elog(ERROR, "System built-in function cannot altered to be cachable");
	}

	if (strict_item)
		procForm->proisstrict = intVal(strict_item->arg);
	if (security_def_item)
		procForm->prosecdef = intVal(security_def_item->arg);
	if (leakproof_item)
	{
		procForm->proleakproof = intVal(leakproof_item->arg);
		if (procForm->proleakproof && !superuser())
			ereport(ERROR,
					(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
					 errmsg("only superuser can define a leakproof function")));
	}
	if (cost_item)
	{
		procForm->procost = defGetNumeric(cost_item);
		if (procForm->procost <= 0)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("COST must be positive")));
	}
    if (pushdown_item)
    {
        bool pushdown = intVal(pushdown_item->arg);
        if (pushdown && procForm->procost > 0)
            procForm->procost = -procForm->procost;
        if ((!pushdown) && procForm->procost < 0)
            procForm->procost = -procForm->procost;
    }
	if (rows_item)
	{
		procForm->prorows = defGetNumeric(rows_item);
		if (procForm->prorows <= 0)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("ROWS must be positive")));
		if (!procForm->proretset)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("ROWS is not applicable when function does not return a set")));
	}
	if (set_items)
	{
		Datum		datum;
		bool		isnull;
		ArrayType  *a;
		Datum		repl_val[Natts_pg_proc];
		bool		repl_null[Natts_pg_proc];
		bool		repl_repl[Natts_pg_proc];

		/* extract existing proconfig setting */
		datum = SysCacheGetAttr(PROCOID, tup, Anum_pg_proc_proconfig, &isnull);
		a = isnull ? NULL : DatumGetArrayTypeP(datum);

		/* update according to each SET or RESET item, left to right */
		a = update_proconfig_value(a, set_items);

		/* update the tuple */
		memset(repl_repl, false, sizeof(repl_repl));
		repl_repl[Anum_pg_proc_proconfig - 1] = true;

		if (a == NULL)
		{
			repl_val[Anum_pg_proc_proconfig - 1] = (Datum) 0;
			repl_null[Anum_pg_proc_proconfig - 1] = true;
		}
		else
		{
			repl_val[Anum_pg_proc_proconfig - 1] = PointerGetDatum(a);
			repl_null[Anum_pg_proc_proconfig - 1] = false;
		}

		tup = heap_modify_tuple(tup, RelationGetDescr(rel),
								repl_val, repl_null, repl_repl);
	}
	if (parallel_item)
		procForm->proparallel = interpret_func_parallel(parallel_item);

	/* Do the update */
	CatalogTupleUpdate(rel, &tup->t_self, tup);

	InvokeObjectPostAlterHook(ProcedureRelationId, funcOid, 0);

	ObjectAddressSet(address, ProcedureRelationId, funcOid);

	/*
	 * A "pushdown" config item may affect a trigger on relcache we already built,
	 * invalidate it by looking through catalog.
	 */
	if (procForm->prorettype == TRIGGEROID && pushdown_item)
	{
		Relation    tgrel = heap_open(TriggerRelationId, AccessShareLock);
		ScanKeyData tgkey;
		SysScanDesc tgscan;
		HeapTuple   tgtuple;
		
		ScanKeyInit(&tgkey,
		            Anum_pg_trigger_tgfoid,
		            BTEqualStrategyNumber, F_OIDEQ,
		            ObjectIdGetDatum(funcOid));
		
		tgscan = systable_beginscan(tgrel, InvalidOid, false, NULL, 1, &tgkey);
		
		while (HeapTupleIsValid(tgtuple = systable_getnext(tgscan)))
		{
			Form_pg_trigger tgform = (Form_pg_trigger) GETSTRUCT(tgtuple);
			
			if (OidIsValid(tgform->tgrelid))
			{
				CacheInvalidateRelcacheByRelid(tgform->tgrelid);
			}
		}
		
		systable_endscan(tgscan);
		heap_close(tgrel, AccessShareLock);
	}

	heap_close(rel, NoLock);
	heap_freetuple(tup);

	return address;
}

/*
 * SetFunctionReturnType - change declared return type of a function
 *
 * This is presently only used for adjusting legacy functions that return
 * OPAQUE to return whatever we find their correct definition should be.
 * The caller should emit a suitable warning explaining what we did.
 */
void
SetFunctionReturnType(Oid funcOid, Oid newRetType)
{
	Relation	pg_proc_rel;
	HeapTuple	tup;
	Form_pg_proc procForm;
	ObjectAddress func_address;
	ObjectAddress type_address;

	/*
	 * if the function is a builtin function, its Oid is less than 10000.
	 * we can't allow setting return type for the builtin functions
	 */
	if (IsSystemObjOid(funcOid) && !IsInplaceUpgrade)
		ereport(ERROR,
			(errcode(ERRCODE_INVALID_FUNCTION_DEFINITION),
				errmsg("set return type failed for function %u, because it is a builtin function.",
						funcOid)));

	pg_proc_rel = heap_open(ProcedureRelationId, RowExclusiveLock);

	tup = SearchSysCacheCopy1(PROCOID, ObjectIdGetDatum(funcOid));
	if (!HeapTupleIsValid(tup)) /* should not happen */
		elog(ERROR, "cache lookup failed for function %u", funcOid);
	procForm = (Form_pg_proc) GETSTRUCT(tup);

	if (procForm->prorettype != OPAQUEOID)	/* caller messed up */
		elog(ERROR, "function %u doesn't return OPAQUE", funcOid);

	/* okay to overwrite copied tuple */
	procForm->prorettype = newRetType;

	/* update the catalog and its indexes */
	CatalogTupleUpdate(pg_proc_rel, &tup->t_self, tup);

	heap_close(pg_proc_rel, RowExclusiveLock);

	/*
	 * Also update the dependency to the new type. Opaque is a pinned type, so
	 * there is no old dependency record for it that we would need to remove.
	 */
	ObjectAddressSet(type_address, TypeRelationId, newRetType);
	ObjectAddressSet(func_address, ProcedureRelationId, funcOid);
	recordDependencyOn(&func_address, &type_address, DEPENDENCY_NORMAL);
}


/*
 * SetFunctionArgType - change declared argument type of a function
 *
 * As above, but change an argument's type.
 */
void
SetFunctionArgType(Oid funcOid, int argIndex, Oid newArgType)
{
	Relation	pg_proc_rel;
	HeapTuple	tup;
	Form_pg_proc procForm;
	ObjectAddress func_address;
	ObjectAddress type_address;

	/*
	 * if the function is a builtin function, its Oid is less than 10000.
	 * we can't allow setting argument type for the builtin functions
	 */
	if (IsSystemObjOid(funcOid) && IsInplaceUpgrade == false)
		ereport(ERROR,
			(errcode(ERRCODE_INVALID_FUNCTION_DEFINITION),
				errmsg("set argument type failed for function %u, because it is a builtin function.",
						funcOid)));

	pg_proc_rel = heap_open(ProcedureRelationId, RowExclusiveLock);

	tup = SearchSysCacheCopy1(PROCOID, ObjectIdGetDatum(funcOid));
	if (!HeapTupleIsValid(tup)) /* should not happen */
		elog(ERROR, "cache lookup failed for function %u", funcOid);
	procForm = (Form_pg_proc) GETSTRUCT(tup);

	if (argIndex < 0 || argIndex >= procForm->pronargs ||
		procForm->proargtypes.values[argIndex] != OPAQUEOID)
		elog(ERROR, "function %u doesn't take OPAQUE", funcOid);

	/* okay to overwrite copied tuple */
	procForm->proargtypes.values[argIndex] = newArgType;

	/* update the catalog and its indexes */
	CatalogTupleUpdate(pg_proc_rel, &tup->t_self, tup);

	heap_close(pg_proc_rel, RowExclusiveLock);

	/*
	 * Also update the dependency to the new type. Opaque is a pinned type, so
	 * there is no old dependency record for it that we would need to remove.
	 */
	ObjectAddressSet(type_address, TypeRelationId, newArgType);
	ObjectAddressSet(func_address, ProcedureRelationId, funcOid);
	recordDependencyOn(&func_address, &type_address, DEPENDENCY_NORMAL);
}



/*
 * CREATE CAST
 */
ObjectAddress
CreateCast(CreateCastStmt *stmt)
{
	Oid			sourcetypeid;
	Oid			targettypeid;
	char		sourcetyptype;
	char		targettyptype;
	Oid			funcid;
	Oid			castid;
	int			nargs;
	char		castcontext;
	char		castmethod;
	Relation	relation;
	HeapTuple	tuple;
	Datum		values[Natts_pg_cast];
	bool		nulls[Natts_pg_cast];
	ObjectAddress myself,
				referenced;
	AclResult	aclresult;

	sourcetypeid = typenameTypeId(NULL, stmt->sourcetype);
	targettypeid = typenameTypeId(NULL, stmt->targettype);
	sourcetyptype = get_typtype(sourcetypeid);
	targettyptype = get_typtype(targettypeid);

	/* No pseudo-types allowed */
	if (sourcetyptype == TYPTYPE_PSEUDO)
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("source data type %s is a pseudo-type",
						TypeNameToString(stmt->sourcetype))));

	if (targettyptype == TYPTYPE_PSEUDO)
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("target data type %s is a pseudo-type",
						TypeNameToString(stmt->targettype))));

	/* Permission check */
	if (!pg_type_ownercheck(sourcetypeid, GetUserId())
		&& !pg_type_ownercheck(targettypeid, GetUserId()))
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("must be owner of type %s or type %s",
						format_type_be(sourcetypeid),
						format_type_be(targettypeid))));

	aclresult = pg_type_aclcheck(sourcetypeid, GetUserId(), ACL_USAGE);
	if (aclresult != ACLCHECK_OK)
		aclcheck_error_type(aclresult, sourcetypeid);

	aclresult = pg_type_aclcheck(targettypeid, GetUserId(), ACL_USAGE);
	if (aclresult != ACLCHECK_OK)
		aclcheck_error_type(aclresult, targettypeid);

	/* Domains are allowed for historical reasons, but we warn */
	if (sourcetyptype == TYPTYPE_DOMAIN)
		ereport(WARNING,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("cast will be ignored because the source data type is a domain")));

	else if (targettyptype == TYPTYPE_DOMAIN)
		ereport(WARNING,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("cast will be ignored because the target data type is a domain")));

	/* Determine the cast method */
	if (stmt->func != NULL)
		castmethod = COERCION_METHOD_FUNCTION;
	else if (stmt->inout)
		castmethod = COERCION_METHOD_INOUT;
	else
		castmethod = COERCION_METHOD_BINARY;

	if (castmethod == COERCION_METHOD_FUNCTION)
	{
		Form_pg_proc procstruct;

		funcid = LookupFuncWithArgs(OBJECT_FUNCTION, stmt->func, false);

		tuple = SearchSysCache1(PROCOID, ObjectIdGetDatum(funcid));
		if (!HeapTupleIsValid(tuple))
			elog(ERROR, "cache lookup failed for function %u", funcid);

		procstruct = (Form_pg_proc) GETSTRUCT(tuple);
		nargs = procstruct->pronargs;
		if (nargs < 1 || nargs > 3)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
					 errmsg("cast function must take one to three arguments")));
		if (!IsBinaryCoercible(sourcetypeid, procstruct->proargtypes.values[0]))
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
					 errmsg("argument of cast function must match or be binary-coercible from source data type")));
		if (nargs > 1 && procstruct->proargtypes.values[1] != INT4OID)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
					 errmsg("second argument of cast function must be type %s",
							"integer")));
		if (nargs > 2 && procstruct->proargtypes.values[2] != BOOLOID)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
					 errmsg("third argument of cast function must be type %s",
							"boolean")));
		if (!IsBinaryCoercible(procstruct->prorettype, targettypeid))
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
					 errmsg("return data type of cast function must match or be binary-coercible to target data type")));

		/*
		 * Restricting the volatility of a cast function may or may not be a
		 * good idea in the abstract, but it definitely breaks many old
		 * user-defined types.  Disable this check --- tgl 2/1/03
		 */
#ifdef NOT_USED
		if (procstruct->provolatile == PROVOLATILE_VOLATILE)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
					 errmsg("cast function must not be volatile")));
#endif
		if (procstruct->prokind != PROKIND_FUNCTION)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
					 errmsg("cast function must be a normal function")));
		if (procstruct->proretset)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
					 errmsg("cast function must not return a set")));

		ReleaseSysCache(tuple);
	}
	else
	{
		funcid = InvalidOid;
		nargs = 0;
	}

	if (castmethod == COERCION_METHOD_BINARY)
	{
		int16		typ1len;
		int16		typ2len;
		bool		typ1byval;
		bool		typ2byval;
		char		typ1align;
		char		typ2align;

		/*
		 * Must be superuser to create binary-compatible casts, since
		 * erroneous casts can easily crash the backend.
		 */
		if (!superuser())
			ereport(ERROR,
					(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
					 errmsg("must be superuser to create a cast WITHOUT FUNCTION")));

		/*
		 * Also, insist that the types match as to size, alignment, and
		 * pass-by-value attributes; this provides at least a crude check that
		 * they have similar representations.  A pair of types that fail this
		 * test should certainly not be equated.
		 */
		get_typlenbyvalalign(sourcetypeid, &typ1len, &typ1byval, &typ1align);
		get_typlenbyvalalign(targettypeid, &typ2len, &typ2byval, &typ2align);
		if (typ1len != typ2len ||
			typ1byval != typ2byval ||
			typ1align != typ2align)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
					 errmsg("source and target data types are not physically compatible")));

		/*
		 * We know that composite, enum and array types are never binary-
		 * compatible with each other.  They all have OIDs embedded in them.
		 *
		 * Theoretically you could build a user-defined base type that is
		 * binary-compatible with a composite, enum, or array type.  But we
		 * disallow that too, as in practice such a cast is surely a mistake.
		 * You can always work around that by writing a cast function.
		 */
		if (sourcetyptype == TYPTYPE_COMPOSITE ||
			targettyptype == TYPTYPE_COMPOSITE)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
					 errmsg("composite data types are not binary-compatible")));

		if (sourcetyptype == TYPTYPE_ENUM ||
			targettyptype == TYPTYPE_ENUM)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
					 errmsg("enum data types are not binary-compatible")));

		if (OidIsValid(get_element_type(sourcetypeid)) ||
			OidIsValid(get_element_type(targettypeid)))
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
					 errmsg("array data types are not binary-compatible")));

		/*
		 * We also disallow creating binary-compatibility casts involving
		 * domains.  Casting from a domain to its base type is already
		 * allowed, and casting the other way ought to go through domain
		 * coercion to permit constraint checking.  Again, if you're intent on
		 * having your own semantics for that, create a no-op cast function.
		 *
		 * NOTE: if we were to relax this, the above checks for composites
		 * etc. would have to be modified to look through domains to their
		 * base types.
		 */
		if (sourcetyptype == TYPTYPE_DOMAIN ||
			targettyptype == TYPTYPE_DOMAIN)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
					 errmsg("domain data types must not be marked binary-compatible")));
	}

	/*
	 * Allow source and target types to be same only for length coercion
	 * functions.  We assume a multi-arg function does length coercion.
	 */
	if (sourcetypeid == targettypeid && nargs < 2)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
				 errmsg("source data type and target data type are the same")));

	/* convert CoercionContext enum to char value for castcontext */
	switch (stmt->context)
	{
		case COERCION_IMPLICIT:
			castcontext = COERCION_CODE_IMPLICIT;
			break;
		case COERCION_ASSIGNMENT:
			castcontext = COERCION_CODE_ASSIGNMENT;
			break;
		case COERCION_EXPLICIT:
			castcontext = COERCION_CODE_EXPLICIT;
			break;
		default:
			elog(ERROR, "unrecognized CoercionContext: %d", stmt->context);
			castcontext = 0;	/* keep compiler quiet */
			break;
	}

	relation = heap_open(CastRelationId, RowExclusiveLock);

	/*
	 * Check for duplicate.  This is just to give a friendly error message,
	 * the unique index would catch it anyway (so no need to sweat about race
	 * conditions).
	 */
	tuple = SearchSysCache2(CASTSOURCETARGET,
							ObjectIdGetDatum(sourcetypeid),
							ObjectIdGetDatum(targettypeid));
	if (HeapTupleIsValid(tuple))
		ereport(ERROR,
				(errcode(ERRCODE_DUPLICATE_OBJECT),
				 errmsg("cast from type %s to type %s already exists",
						format_type_be(sourcetypeid),
						format_type_be(targettypeid))));

	/* ready to go */
	values[Anum_pg_cast_castsource - 1] = ObjectIdGetDatum(sourcetypeid);
	values[Anum_pg_cast_casttarget - 1] = ObjectIdGetDatum(targettypeid);
	values[Anum_pg_cast_castfunc - 1] = ObjectIdGetDatum(funcid);
	values[Anum_pg_cast_castcontext - 1] = CharGetDatum(castcontext);
	values[Anum_pg_cast_castmethod - 1] = CharGetDatum(castmethod);

	MemSet(nulls, false, sizeof(nulls));

	tuple = heap_form_tuple(RelationGetDescr(relation), values, nulls);

	castid = CatalogTupleInsert(relation, tuple);

	/* make dependency entries */
	myself.classId = CastRelationId;
	myself.objectId = castid;
	myself.objectSubId = 0;

	/* dependency on source type */
	referenced.classId = TypeRelationId;
	referenced.objectId = sourcetypeid;
	referenced.objectSubId = 0;
	recordDependencyOn(&myself, &referenced, DEPENDENCY_NORMAL);

	/* dependency on target type */
	referenced.classId = TypeRelationId;
	referenced.objectId = targettypeid;
	referenced.objectSubId = 0;
	recordDependencyOn(&myself, &referenced, DEPENDENCY_NORMAL);

	/* dependency on function */
	if (OidIsValid(funcid))
	{
		referenced.classId = ProcedureRelationId;
		referenced.objectId = funcid;
		referenced.objectSubId = 0;
		recordDependencyOn(&myself, &referenced, DEPENDENCY_NORMAL);
	}

	/* dependency on extension */
	recordDependencyOnCurrentExtension(&myself, false);

	/* Post creation hook for new cast */
	InvokeObjectPostCreateHook(CastRelationId, castid, 0);

	heap_freetuple(tuple);

	heap_close(relation, RowExclusiveLock);

	return myself;
}

/*
 * get_cast_oid - given two type OIDs, look up a cast OID
 *
 * If missing_ok is false, throw an error if the cast is not found.  If
 * true, just return InvalidOid.
 */
Oid
get_cast_oid(Oid sourcetypeid, Oid targettypeid, bool missing_ok)
{
	Oid			oid;

	oid = GetSysCacheOid2(CASTSOURCETARGET,
						  ObjectIdGetDatum(sourcetypeid),
						  ObjectIdGetDatum(targettypeid));
	if (!OidIsValid(oid) && !missing_ok)
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("cast from type %s to type %s does not exist",
						format_type_be(sourcetypeid),
						format_type_be(targettypeid))));
	return oid;
}

void
DropCastById(Oid castOid)
{
	Relation	relation;
	ScanKeyData scankey;
	SysScanDesc scan;
	HeapTuple	tuple;

	relation = heap_open(CastRelationId, RowExclusiveLock);

	ScanKeyInit(&scankey,
				ObjectIdAttributeNumber,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(castOid));
	scan = systable_beginscan(relation, CastOidIndexId, true,
							  NULL, 1, &scankey);

	tuple = systable_getnext(scan);
	if (!HeapTupleIsValid(tuple))
		elog(ERROR, "could not find tuple for cast %u", castOid);
	CatalogTupleDelete(relation, &tuple->t_self);

	systable_endscan(scan);
	heap_close(relation, RowExclusiveLock);
}


static void
check_transform_function(Form_pg_proc procstruct)
{
	if (procstruct->provolatile == PROVOLATILE_VOLATILE)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
				 errmsg("transform function must not be volatile")));
	if (procstruct->prokind != PROKIND_FUNCTION)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
				 errmsg("transform function must be a normal function")));
	if (procstruct->proretset)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
				 errmsg("transform function must not return a set")));
	if (procstruct->pronargs != 1)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
				 errmsg("transform function must take one argument")));
	if (procstruct->proargtypes.values[0] != INTERNALOID)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
				 errmsg("first argument of transform function must be type %s",
						"internal")));
}


/*
 * CREATE TRANSFORM
 */
ObjectAddress
CreateTransform(CreateTransformStmt *stmt)
{
	Oid			typeid;
	char		typtype;
	Oid			langid;
	Oid			fromsqlfuncid;
	Oid			tosqlfuncid;
	AclResult	aclresult;
	Form_pg_proc procstruct;
	Datum		values[Natts_pg_transform];
	bool		nulls[Natts_pg_transform];
	bool		replaces[Natts_pg_transform];
	Oid			transformid;
	HeapTuple	tuple;
	HeapTuple	newtuple;
	Relation	relation;
	ObjectAddress myself,
				referenced;
	bool		is_replace;

	/*
	 * Get the type
	 */
	typeid = typenameTypeId(NULL, stmt->type_name);
	typtype = get_typtype(typeid);

	if (typtype == TYPTYPE_PSEUDO)
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("data type %s is a pseudo-type",
						TypeNameToString(stmt->type_name))));

	if (typtype == TYPTYPE_DOMAIN)
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("data type %s is a domain",
						TypeNameToString(stmt->type_name))));

	if (!pg_type_ownercheck(typeid, GetUserId()))
		aclcheck_error_type(ACLCHECK_NOT_OWNER, typeid);

	aclresult = pg_type_aclcheck(typeid, GetUserId(), ACL_USAGE);
	if (aclresult != ACLCHECK_OK)
		aclcheck_error_type(aclresult, typeid);

	/*
	 * Get the language
	 */
	langid = get_language_oid(stmt->lang, false);

	aclresult = pg_language_aclcheck(langid, GetUserId(), ACL_USAGE);
	if (aclresult != ACLCHECK_OK)
		aclcheck_error(aclresult, ACL_KIND_LANGUAGE, stmt->lang);

	/*
	 * Get the functions
	 */
	if (stmt->fromsql)
	{
		fromsqlfuncid = LookupFuncWithArgs(OBJECT_FUNCTION, stmt->fromsql, false);

		if (!pg_proc_ownercheck(fromsqlfuncid, GetUserId()))
			aclcheck_error(ACLCHECK_NOT_OWNER, ACL_KIND_PROC, NameListToString(stmt->fromsql->objname));

		aclresult = pg_proc_aclcheck(fromsqlfuncid, GetUserId(), ACL_EXECUTE);
		if (aclresult != ACLCHECK_OK)
			aclcheck_error(aclresult, ACL_KIND_PROC, NameListToString(stmt->fromsql->objname));

		tuple = SearchSysCache1(PROCOID, ObjectIdGetDatum(fromsqlfuncid));
		if (!HeapTupleIsValid(tuple))
			elog(ERROR, "cache lookup failed for function %u", fromsqlfuncid);
		procstruct = (Form_pg_proc) GETSTRUCT(tuple);
		if (procstruct->prorettype != INTERNALOID)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
					 errmsg("return data type of FROM SQL function must be %s",
							"internal")));
		check_transform_function(procstruct);
		ReleaseSysCache(tuple);
	}
	else
		fromsqlfuncid = InvalidOid;

	if (stmt->tosql)
	{
		tosqlfuncid = LookupFuncWithArgs(OBJECT_FUNCTION, stmt->tosql, false);

		if (!pg_proc_ownercheck(tosqlfuncid, GetUserId()))
			aclcheck_error(ACLCHECK_NOT_OWNER, ACL_KIND_PROC, NameListToString(stmt->tosql->objname));

		aclresult = pg_proc_aclcheck(tosqlfuncid, GetUserId(), ACL_EXECUTE);
		if (aclresult != ACLCHECK_OK)
			aclcheck_error(aclresult, ACL_KIND_PROC, NameListToString(stmt->tosql->objname));

		tuple = SearchSysCache1(PROCOID, ObjectIdGetDatum(tosqlfuncid));
		if (!HeapTupleIsValid(tuple))
			elog(ERROR, "cache lookup failed for function %u", tosqlfuncid);
		procstruct = (Form_pg_proc) GETSTRUCT(tuple);
		if (procstruct->prorettype != typeid)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
					 errmsg("return data type of TO SQL function must be the transform data type")));
		check_transform_function(procstruct);
		ReleaseSysCache(tuple);
	}
	else
		tosqlfuncid = InvalidOid;

	/*
	 * Ready to go
	 */
	values[Anum_pg_transform_trftype - 1] = ObjectIdGetDatum(typeid);
	values[Anum_pg_transform_trflang - 1] = ObjectIdGetDatum(langid);
	values[Anum_pg_transform_trffromsql - 1] = ObjectIdGetDatum(fromsqlfuncid);
	values[Anum_pg_transform_trftosql - 1] = ObjectIdGetDatum(tosqlfuncid);

	MemSet(nulls, false, sizeof(nulls));

	relation = heap_open(TransformRelationId, RowExclusiveLock);

	tuple = SearchSysCache2(TRFTYPELANG,
							ObjectIdGetDatum(typeid),
							ObjectIdGetDatum(langid));
	if (HeapTupleIsValid(tuple))
	{
		if (!stmt->replace)
			ereport(ERROR,
					(errcode(ERRCODE_DUPLICATE_OBJECT),
					 errmsg("transform for type %s language \"%s\" already exists",
							format_type_be(typeid),
							stmt->lang)));

		MemSet(replaces, false, sizeof(replaces));
		replaces[Anum_pg_transform_trffromsql - 1] = true;
		replaces[Anum_pg_transform_trftosql - 1] = true;

		newtuple = heap_modify_tuple(tuple, RelationGetDescr(relation), values, nulls, replaces);
		CatalogTupleUpdate(relation, &newtuple->t_self, newtuple);

		transformid = HeapTupleGetOid(tuple);
		ReleaseSysCache(tuple);
		is_replace = true;
	}
	else
	{
		newtuple = heap_form_tuple(RelationGetDescr(relation), values, nulls);
		transformid = CatalogTupleInsert(relation, newtuple);
		is_replace = false;
	}

	if (is_replace)
		deleteDependencyRecordsFor(TransformRelationId, transformid, true);

	/* make dependency entries */
	myself.classId = TransformRelationId;
	myself.objectId = transformid;
	myself.objectSubId = 0;

	/* dependency on language */
	referenced.classId = LanguageRelationId;
	referenced.objectId = langid;
	referenced.objectSubId = 0;
	recordDependencyOn(&myself, &referenced, DEPENDENCY_NORMAL);

	/* dependency on type */
	referenced.classId = TypeRelationId;
	referenced.objectId = typeid;
	referenced.objectSubId = 0;
	recordDependencyOn(&myself, &referenced, DEPENDENCY_NORMAL);

	/* dependencies on functions */
	if (OidIsValid(fromsqlfuncid))
	{
		referenced.classId = ProcedureRelationId;
		referenced.objectId = fromsqlfuncid;
		referenced.objectSubId = 0;
		recordDependencyOn(&myself, &referenced, DEPENDENCY_NORMAL);
	}
	if (OidIsValid(tosqlfuncid))
	{
		referenced.classId = ProcedureRelationId;
		referenced.objectId = tosqlfuncid;
		referenced.objectSubId = 0;
		recordDependencyOn(&myself, &referenced, DEPENDENCY_NORMAL);
	}

	/* dependency on extension */
	recordDependencyOnCurrentExtension(&myself, is_replace);

	/* Post creation hook for new transform */
	InvokeObjectPostCreateHook(TransformRelationId, transformid, 0);

	heap_freetuple(newtuple);

	heap_close(relation, RowExclusiveLock);

	return myself;
}


/*
 * get_transform_oid - given type OID and language OID, look up a transform OID
 *
 * If missing_ok is false, throw an error if the transform is not found.  If
 * true, just return InvalidOid.
 */
Oid
get_transform_oid(Oid type_id, Oid lang_id, bool missing_ok)
{
	Oid			oid;

	oid = GetSysCacheOid2(TRFTYPELANG,
						  ObjectIdGetDatum(type_id),
						  ObjectIdGetDatum(lang_id));
	if (!OidIsValid(oid) && !missing_ok)
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("transform for type %s language \"%s\" does not exist",
						format_type_be(type_id),
						get_language_name(lang_id, false))));
	return oid;
}


void
DropTransformById(Oid transformOid)
{
	Relation	relation;
	ScanKeyData scankey;
	SysScanDesc scan;
	HeapTuple	tuple;

	relation = heap_open(TransformRelationId, RowExclusiveLock);

	ScanKeyInit(&scankey,
				ObjectIdAttributeNumber,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(transformOid));
	scan = systable_beginscan(relation, TransformOidIndexId, true,
							  NULL, 1, &scankey);

	tuple = systable_getnext(scan);
	if (!HeapTupleIsValid(tuple))
		elog(ERROR, "could not find tuple for transform %u", transformOid);
	CatalogTupleDelete(relation, &tuple->t_self);

	systable_endscan(scan);
	heap_close(relation, RowExclusiveLock);
}


/*
 * Subroutine for ALTER FUNCTION/AGGREGATE SET SCHEMA/RENAME
 *
 * Is there a function with the given name and signature already in the given
 * namespace?  If so, raise an appropriate error message.
 */
void
IsThereFunctionInNamespace(const char *proname, int pronargs,
						   oidvector *proargtypes, Oid nspOid)
{
	/* check for duplicate name (more friendly than unique-index failure) */
	if (SearchSysCacheExists3(PROCNAMEARGSNSP,
							  CStringGetDatum(proname),
							  PointerGetDatum(proargtypes),
							  ObjectIdGetDatum(nspOid)))
		ereport(ERROR,
				(errcode(ERRCODE_DUPLICATE_FUNCTION),
				 errmsg("function %s already exists in schema \"%s\"",
						funcname_signature_string(proname, pronargs,
												  NIL, proargtypes->values),
						get_namespace_name(nspOid))));
}

/*
 * ExecuteDoStmt
 *		Execute inline procedural-language code
 *
 * See at ExecuteCallStmt() about the atomic argument.
 */
void
ExecuteDoStmt(DoStmt *stmt, bool atomic, ProcessUtilityContext context)
{
	InlineCodeBlock *codeblock = makeNode(InlineCodeBlock);
	ListCell   *arg;
	DefElem    *as_item = NULL;
	DefElem    *language_item = NULL;
	char	   *language;
	Oid			laninline;
	HeapTuple	languageTuple;
	Form_pg_language languageStruct;

	/* Process options we got from gram.y */
	foreach(arg, stmt->args)
	{
		DefElem    *defel = (DefElem *) lfirst(arg);

		if (strcmp(defel->defname, "as") == 0)
		{
			if (as_item)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options")));
			as_item = defel;
		}
		else if (strcmp(defel->defname, "language") == 0)
		{
			if (language_item)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options")));
			language_item = defel;
		}
		else
			elog(ERROR, "option \"%s\" not recognized",
				 defel->defname);
	}

	if (as_item)
		codeblock->source_text = strVal(as_item->arg);
	else
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("no inline code specified")));

	/* if LANGUAGE option wasn't specified, use the default */
	if (language_item)
	{
		language = strVal(language_item->arg);
		
		if (strcmp("default_plsql", language) == 0)
		{
			if (ORA_MODE)
				language = "oraplsql";
			else
				language = "plpgsql";
		}
	}
	else if (ORA_MODE)
		language = "oraplsql";
	else
		language = "plpgsql";

	/* Look up the language and validate permissions */
	languageTuple = SearchSysCache1(LANGNAME, PointerGetDatum(language));
	if (!HeapTupleIsValid(languageTuple))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("language \"%s\" does not exist", language),
				 (PLTemplateExists(language) ?
				  errhint("Use CREATE LANGUAGE to load the language into the database.") : 0)));

	codeblock->langOid = HeapTupleGetOid(languageTuple);
	languageStruct = (Form_pg_language) GETSTRUCT(languageTuple);
	codeblock->langIsTrusted = languageStruct->lanpltrusted;
	codeblock->atomic = atomic;
	if (ORA_MODE && strcmp(language, "oraplsql") == 0)
	{
		if (!(context == PROCESS_UTILITY_TOPLEVEL ||
			  context == PROCESS_UTILITY_QUERY_NONATOMIC))
			codeblock->atomic = true;
		else
			codeblock->atomic = false;
	}

	if (languageStruct->lanpltrusted)
	{
		/* if trusted language, need USAGE privilege */
		AclResult	aclresult;

		aclresult = pg_language_aclcheck(codeblock->langOid, GetUserId(),
										 ACL_USAGE);
		if (aclresult != ACLCHECK_OK)
			aclcheck_error(aclresult, ACL_KIND_LANGUAGE,
						   NameStr(languageStruct->lanname));
	}
	else
	{
		/* if untrusted language, must be superuser */
		if (!superuser())
			aclcheck_error(ACLCHECK_NO_PRIV, ACL_KIND_LANGUAGE,
						   NameStr(languageStruct->lanname));
	}

	/* get the handler function's OID */
	laninline = languageStruct->laninline;
	if (!OidIsValid(laninline))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("language \"%s\" does not support inline code execution",
						NameStr(languageStruct->lanname))));

	ReleaseSysCache(languageTuple);

	/* execute the inline handler */
	OidFunctionCall1(laninline, PointerGetDatum(codeblock));
}

/*
 * Execute CALL statement
 *
 * Inside a top-level CALL statement, transaction-terminating commands such as
 * COMMIT or a PL-specific equivalent are allowed.  The terminology in the SQL
 * standard is that CALL establishes a non-atomic execution context.  Most
 * other commands establish an atomic execution context, in which transaction
 * control actions are not allowed.  If there are nested executions of CALL,
 * we want to track the execution context recursively, so that the nested
 * CALLs can also do transaction control.  Note, however, that for example in
 * CALL -> SELECT -> CALL, the second call cannot do transaction control,
 * because the SELECT in between establishes an atomic execution context.
 *
 * So when ExecuteCallStmt() is called from the top level, we pass in atomic =
 * false (recall that that means transactions = yes).  We then create a
 * CallContext node with content atomic = false, which is passed in the
 * fcinfo->context field to the procedure invocation.  The language
 * implementation should then take appropriate measures to allow or prevent
 * transaction commands based on that information, e.g., call
 * SPI_connect_ext(SPI_OPT_NONATOMIC).  The language should also pass on the
 * atomic flag to any nested invocations to CALL.
 *
 * The expression data structures and execution context that we create
 * within this function are children of the portalContext of the Portal
 * that the CALL utility statement runs in.  Therefore, any pass-by-ref
 * values that we're passing to the procedure will survive transaction
 * commits that might occur inside the procedure.
 *
 * context is used only in opentenbase_ora mode, because its caller doest't know which
 * language the function belong to, so we move it here.
 */
void
ExecuteCallStmt(CallStmt *stmt, ParamListInfo params, bool atomic, DestReceiver *dest, ProcessUtilityContext context)
{
	ListCell   *lc;
	FuncExpr   *fexpr;
	int			nargs;
	int			i;
	AclResult   aclresult;
	Oid		   *argtypes;
	char	  **argnames;
	char	   *argmodes;
	FmgrInfo	flinfo;
	FunctionCallInfoData fcinfo;
	CallContext *callcontext;
	EState	   *estate;
	ExprContext *econtext;
	HeapTuple	tp;
	PgStat_FunctionCallUsage fcusage;
	Datum		retval;
	Form_pg_proc procedureStruct;
	char		*language = NULL;
	bool		islocal_proc = false;

	fexpr = stmt->funcexpr;
	Assert(fexpr);

	if (ORA_MODE)
		islocal_proc = is_local_func_proc((fmNodePtr) fexpr);

	if (!islocal_proc)
	{
		/* Check permission to call procedure */
		aclresult = pg_proc_aclcheck(fexpr->funcid, GetUserId(), ACL_EXECUTE);
		if (aclresult != ACLCHECK_OK)
			aclcheck_error(aclresult, ACL_KIND_PROC, get_func_name(fexpr->funcid));
	}

	/* Prep the context object we'll pass to the procedure */
	callcontext = makeNode(CallContext);
	callcontext->atomic = atomic;

	if (fexpr->withfuncnsp != NULL)
		tp = GetWithFunctionTupleById(fexpr->withfuncnsp, fexpr->withfuncid);
	else
		tp = SearchSysCache1(PROCOID, ObjectIdGetDatum(fexpr->funcid));

	if (!HeapTupleIsValid(tp))
		elog(ERROR, "cache lookup failed for function %u", fexpr->funcid);
	procedureStruct = (Form_pg_proc) GETSTRUCT(tp);
	language = get_language_name(procedureStruct->prolang, false);
	if (ORA_MODE && strcmp(language, "oraplsql") == 0)
	{
		if (!(context == PROCESS_UTILITY_TOPLEVEL ||
			  context == PROCESS_UTILITY_QUERY_NONATOMIC))
			callcontext->atomic = true;
		else
			callcontext->atomic = false;
	}

	/*
	 * If proconfig is set we can't allow transaction commands because of the
	 * way the GUC stacking works: The transaction boundary would have to pop
	 * the proconfig setting off the stack.  That restriction could be lifted
	 * by redesigning the GUC nesting mechanism a bit.
	 */
	if (!heap_attisnull(tp, Anum_pg_proc_proconfig, NULL))
		callcontext->atomic = true;

    /*
     * In security definer procedures, we can't allow transaction commands.
     * StartTransaction() insists that the security context stack is empty,
     * and AbortTransaction() resets the security context.  This could be
     * reorganized, but right now it doesn't work.
     */
    if (((Form_pg_proc )GETSTRUCT(tp))->prosecdef &&
		(!ORA_MODE || !enable_secfunc_xact))
        callcontext->atomic = true;

    /*
     * Expand named arguments, defaults, etc.
     */
    fexpr->args = expand_function_arguments(fexpr->args, fexpr->funcresulttype, tp);
    nargs = list_length(fexpr->args);

	get_func_arg_info(tp, &argtypes, &argnames, &argmodes);

	if (fexpr->withfuncnsp == NULL)
		ReleaseSysCache(tp);

	/* safety check; see ExecInitFunc() */
	if (nargs > FUNC_MAX_ARGS)
		ereport(ERROR,
				(errcode(ERRCODE_TOO_MANY_ARGUMENTS),
				errmsg_plural("cannot pass more than %d argument to a procedure",
					"cannot pass more than %d arguments to a procedure",
						FUNC_MAX_ARGS,
						FUNC_MAX_ARGS)));

	/* Initialize function call structure */
	InvokeFunctionExecuteHook(fexpr->funcid);
	if (fexpr->withfuncnsp != NULL)
		fmgr_info_withfuncs(InvalidOid, &flinfo, (fmNodePtr) fexpr);
	else
		fmgr_info(fexpr->funcid, &flinfo);
	fmgr_info_set_expr((Node *) fexpr, &flinfo);
	InitFunctionCallInfoData(fcinfo, &flinfo, nargs, fexpr->inputcollid, (Node *) callcontext, NULL);

	/*
	* Evaluate procedure arguments inside a suitable execution context.  Note
	* we can't free this context till the procedure returns.
	*/
	estate = CreateExecutorState();
	estate->es_param_list_info = params;
	econtext = CreateExprContext(estate);

	i = 0;
	foreach (lc, fexpr->args)
	{
		/* Assign the param value when the paramtype is cursor whatever the argmode is. */
		if (argmodes && argmodes[i] == PROARGMODE_OUT &&
			!(ORA_MODE && IsA(lfirst(lc), Param) && ((Param *)(lfirst(lc)))->paramtype == REFCURSOROID))
		{
			fcinfo.arg[i] = 0;
			fcinfo.argnull[i] = true;
		}
		else
		{
			ExprState  *exprstate;
			Datum		val;
			bool		isnull;

			exprstate = ExecPrepareExpr(lfirst(lc), estate);

			val = ExecEvalExprSwitchContext(exprstate, econtext, &isnull);

			fcinfo.arg[i] = val;
			fcinfo.argnull[i] = isnull;
		}

		i++;
	}

	pgstat_init_function_usage(&fcinfo, &fcusage);
	retval = FunctionCallInvoke(&fcinfo);
	pgstat_end_function_usage(&fcusage, true);

	if (fexpr->funcresulttype == VOIDOID)
	{
		/* do nothing */
	}
	else if (fexpr->funcresulttype == RECORDOID)
	{
		/*
		 * send tuple to client
		 */

		HeapTupleHeader td;
		Oid			tupType;
		int32		tupTypmod;
		TupleDesc	retdesc;
		HeapTupleData rettupdata;
		TupOutputState *tstate;
		TupleTableSlot *slot;

		if (fcinfo.isnull)
			elog(ERROR, "procedure returned null record");

		td = DatumGetHeapTupleHeader(retval);
		tupType = HeapTupleHeaderGetTypeId(td);
		tupTypmod = HeapTupleHeaderGetTypMod(td);
		retdesc = lookup_rowtype_tupdesc(tupType, tupTypmod);

		tstate = begin_tup_output_tupdesc(dest, retdesc);

		rettupdata.t_len = HeapTupleHeaderGetDatumLength(td);
		ItemPointerSetInvalid(&(rettupdata.t_self));
		rettupdata.t_tableOid = InvalidOid;
		rettupdata.t_data = td;

		slot = ExecStoreHeapTuple(&rettupdata, tstate->slot, false);
		tstate->dest->receiveSlot(slot, tstate->dest);

		end_tup_output(tstate);

		ReleaseTupleDesc(retdesc);
	}
	else
		elog(ERROR, "unexpected result type for procedure: %u",
			 fexpr->funcresulttype);

	FreeExecutorState(estate);
}

/*
 * Construct the tuple descriptor for a CALL statement return
 */
TupleDesc
CallStmtResultDesc(CallStmt *stmt)
{
	FuncExpr   *fexpr;
	HeapTuple   tuple;
	TupleDesc   tupdesc;

	fexpr = stmt->funcexpr;

	if (fexpr->withfuncnsp != NULL)
		tuple = GetWithFunctionTupleById(fexpr->withfuncnsp, fexpr->withfuncid);
	else
		tuple = SearchSysCache1(PROCOID, ObjectIdGetDatum(fexpr->funcid));
	if (!HeapTupleIsValid(tuple))
		elog(ERROR, "cache lookup failed for procedure %u", fexpr->funcid);

	tupdesc = build_function_result_tupdesc_t(tuple);

	if (fexpr->withfuncnsp == NULL)
		ReleaseSysCache(tuple);

	return tupdesc;
}
