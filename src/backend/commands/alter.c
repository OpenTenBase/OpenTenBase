/*-------------------------------------------------------------------------
 *
 * alter.c
 *	  Drivers for generic alter commands
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/commands/alter.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/htup_details.h"
#include "access/sysattr.h"
#include "catalog/dependency.h"
#include "catalog/indexing.h"
#include "catalog/namespace.h"
#include "catalog/objectaccess.h"
#include "catalog/pg_collation.h"
#include "catalog/pg_conversion.h"
#include "catalog/pg_event_trigger.h"
#include "catalog/pg_extprotocol.h"
#include "catalog/pg_foreign_data_wrapper.h"
#include "catalog/pg_foreign_server.h"
#include "catalog/pg_language.h"
#include "catalog/pg_largeobject.h"
#include "catalog/pg_largeobject_metadata.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_opclass.h"
#include "catalog/pg_opfamily.h"
#include "catalog/pg_package.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_subscription.h"
#include "catalog/pg_statistic_ext.h"
#include "catalog/pg_ts_config.h"
#include "catalog/pg_ts_dict.h"
#include "catalog/pg_ts_parser.h"
#include "catalog/pg_ts_template.h"
#include "commands/alter.h"
#include "commands/collationcmds.h"
#include "commands/conversioncmds.h"
#include "commands/dbcommands.h"
#include "commands/defrem.h"
#include "commands/event_trigger.h"
#include "commands/extension.h"
#include "commands/policy.h"
#include "commands/proclang.h"
#include "commands/publicationcmds.h"
#include "commands/schemacmds.h"
#include "commands/subscriptioncmds.h"
#include "commands/tablecmds.h"
#include "commands/tablespace.h"
#include "commands/trigger.h"
#include "commands/typecmds.h"
#include "commands/user.h"
#include "parser/parse_func.h"
#include "miscadmin.h"
#include "rewrite/rewriteDefine.h"
#include "tcop/utility.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/syscache.h"
#include "utils/tqual.h"


static Oid	AlterObjectNamespace_internal(Relation rel, Oid objid, Oid nspOid);

/*
 * Raise an error to the effect that an object of the given name is already
 * present in the given namespace.
 */
static void
report_name_conflict(Oid classId, const char *name)
{
	char	   *msgfmt;

	switch (classId)
	{
		case EventTriggerRelationId:
			msgfmt = gettext_noop("event trigger \"%s\" already exists");
			break;
		case ForeignDataWrapperRelationId:
			msgfmt = gettext_noop("foreign-data wrapper \"%s\" already exists");
			break;
		case ForeignServerRelationId:
			msgfmt = gettext_noop("server \"%s\" already exists");
			break;
		case LanguageRelationId:
			msgfmt = gettext_noop("language \"%s\" already exists");
			break;
		case ExtprotocolRelationId:
			msgfmt = gettext_noop("protocol \"%s\" already exists");
			break;
		case PublicationRelationId:
			msgfmt = gettext_noop("publication \"%s\" already exists");
			break;
		case SubscriptionRelationId:
			msgfmt = gettext_noop("subscription \"%s\" already exists");
			break;
		default:
			elog(ERROR, "unsupported object class %u", classId);
			break;
	}

	ereport(ERROR,
			(errcode(ERRCODE_DUPLICATE_OBJECT),
			 errmsg(msgfmt, name)));
}

static void
report_namespace_conflict(Oid classId, const char *name, Oid nspOid)
{
	char	   *msgfmt;

	Assert(OidIsValid(nspOid));

	switch (classId)
	{
		case ConversionRelationId:
			Assert(OidIsValid(nspOid));
			msgfmt = gettext_noop("conversion \"%s\" already exists in schema \"%s\"");
			break;
		case StatisticExtRelationId:
			Assert(OidIsValid(nspOid));
			msgfmt = gettext_noop("statistics object \"%s\" already exists in schema \"%s\"");
			break;
		case TSParserRelationId:
			Assert(OidIsValid(nspOid));
			msgfmt = gettext_noop("text search parser \"%s\" already exists in schema \"%s\"");
			break;
		case TSDictionaryRelationId:
			Assert(OidIsValid(nspOid));
			msgfmt = gettext_noop("text search dictionary \"%s\" already exists in schema \"%s\"");
			break;
		case TSTemplateRelationId:
			Assert(OidIsValid(nspOid));
			msgfmt = gettext_noop("text search template \"%s\" already exists in schema \"%s\"");
			break;
		case TSConfigRelationId:
			Assert(OidIsValid(nspOid));
			msgfmt = gettext_noop("text search configuration \"%s\" already exists in schema \"%s\"");
			break;
		default:
			elog(ERROR, "unsupported object class %u", classId);
			break;
	}

	ereport(ERROR,
			(errcode(ERRCODE_DUPLICATE_OBJECT),
			 errmsg(msgfmt, name, get_namespace_name(nspOid))));
}

/*
 * AlterObjectRename_internal
 *
 * Generic function to rename the given object, for simple cases (won't
 * work for tables, nor other cases where we need to do more than change
 * the name column of a single catalog entry).
 *
 * rel: catalog relation containing object (RowExclusiveLock'd by caller)
 * objectId: OID of object to be renamed
 * new_name: CString representation of new name
 */
static void
AlterObjectRename_internal(Relation rel, Oid objectId, const char *new_name)
{
	Oid			classId = RelationGetRelid(rel);
	int			oidCacheId = get_object_catcache_oid(classId);
	int			nameCacheId = get_object_catcache_name(classId);
	AttrNumber	Anum_name = get_object_attnum_name(classId);
	AttrNumber	Anum_namespace = get_object_attnum_namespace(classId);
	AttrNumber	Anum_owner = get_object_attnum_owner(classId);
	AclObjectKind acl_kind = get_object_aclkind(classId);
	HeapTuple	oldtup;
	HeapTuple	newtup;
	Datum		datum;
	bool		isnull;
	Oid			namespaceId;
	Oid			ownerId;
	char	   *old_name;
	AclResult	aclresult;
	Datum	   *values;
	bool	   *nulls;
	bool	   *replaces;
	NameData	nameattrdata;

	oldtup = SearchSysCache1(oidCacheId, ObjectIdGetDatum(objectId));
	if (!HeapTupleIsValid(oldtup))
		elog(ERROR, "cache lookup failed for object %u of catalog \"%s\"",
			 objectId, RelationGetRelationName(rel));

	datum = heap_getattr(oldtup, Anum_name,
						 RelationGetDescr(rel), &isnull);
	Assert(!isnull);
	old_name = NameStr(*(DatumGetName(datum)));

	/* Get OID of namespace */
	if (Anum_namespace > 0)
	{
		datum = heap_getattr(oldtup, Anum_namespace,
							 RelationGetDescr(rel), &isnull);
		Assert(!isnull);
		namespaceId = DatumGetObjectId(datum);
	}
	else
		namespaceId = InvalidOid;

	/* Permission checks ... superusers can always do it */
	if (!superuser())
	{
		/* Fail if object does not have an explicit owner */
		if (Anum_owner <= 0)
			ereport(ERROR,
					(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
					 (errmsg("must be superuser to rename %s",
							 getObjectDescriptionOids(classId, objectId)))));

		/* Otherwise, must be owner of the existing object */
		datum = heap_getattr(oldtup, Anum_owner,
							 RelationGetDescr(rel), &isnull);
		Assert(!isnull);
		ownerId = DatumGetObjectId(datum);

		if (!has_privs_of_role(GetUserId(), DatumGetObjectId(ownerId)))
			aclcheck_error(ACLCHECK_NOT_OWNER, acl_kind, old_name);

		/* User must have CREATE privilege on the namespace */
		if (OidIsValid(namespaceId))
		{
			aclresult = pg_namespace_aclcheck(namespaceId, GetUserId(),
											  ACL_CREATE);
			if (aclresult != ACLCHECK_OK)
				aclcheck_error(aclresult, ACL_KIND_NAMESPACE,
							   get_namespace_name(namespaceId));
		}
	}

	/*
	 * Check for duplicate name (more friendly than unique-index failure).
	 * Since this is just a friendliness check, we can just skip it in cases
	 * where there isn't suitable support.
	 */
	if (classId == ProcedureRelationId)
	{
		Form_pg_proc proc = (Form_pg_proc) GETSTRUCT(oldtup);

		/*
		 * if the function is a builtin function, its Oid is less than 10000.
		 * we can't allow renaming the builtin functions
		 */
		if (IsSystemObjOid(objectId) && !IsInplaceUpgrade)
			ereport(ERROR,
				(errcode(ERRCODE_INVALID_FUNCTION_DEFINITION),
		    		errmsg("the builtin function can not be renamed,its function oid is \"%u\"", objectId)));

		IsThereFunctionInNamespace(new_name, proc->pronargs,
								   &proc->proargtypes, proc->pronamespace);
	}
	else if (classId == CollationRelationId)
	{
		Form_pg_collation coll = (Form_pg_collation) GETSTRUCT(oldtup);

		IsThereCollationInNamespace(new_name, coll->collnamespace);
	}
	else if (classId == OperatorClassRelationId)
	{
		Form_pg_opclass opc = (Form_pg_opclass) GETSTRUCT(oldtup);

		IsThereOpClassInNamespace(new_name, opc->opcmethod,
								  opc->opcnamespace);
	}
	else if (classId == OperatorFamilyRelationId)
	{
		Form_pg_opfamily opf = (Form_pg_opfamily) GETSTRUCT(oldtup);

		IsThereOpFamilyInNamespace(new_name, opf->opfmethod,
								   opf->opfnamespace);
	}
	else if (classId == SubscriptionRelationId)
	{
		if (SearchSysCacheExists2(SUBSCRIPTIONNAME, MyDatabaseId,
								  CStringGetDatum(new_name)))
			report_name_conflict(classId, new_name);
	}
	else if (nameCacheId >= 0)
	{
		if (OidIsValid(namespaceId))
		{
			if (SearchSysCacheExists2(nameCacheId,
									  CStringGetDatum(new_name),
									  ObjectIdGetDatum(namespaceId)))
				report_namespace_conflict(classId, new_name, namespaceId);
		}
		else
		{
			if (SearchSysCacheExists1(nameCacheId,
									  CStringGetDatum(new_name)))
				report_name_conflict(classId, new_name);
		}
	}

	/* Build modified tuple */
	values = palloc0(RelationGetNumberOfAttributes(rel) * sizeof(Datum));
	nulls = palloc0(RelationGetNumberOfAttributes(rel) * sizeof(bool));
	replaces = palloc0(RelationGetNumberOfAttributes(rel) * sizeof(bool));
	namestrcpy(&nameattrdata, new_name);
	values[Anum_name - 1] = NameGetDatum(&nameattrdata);
	replaces[Anum_name - 1] = true;
	newtup = heap_modify_tuple(oldtup, RelationGetDescr(rel),
							   values, nulls, replaces);

	/* Perform actual update */
	CatalogTupleUpdate(rel, &oldtup->t_self, newtup);

	InvokeObjectPostAlterHook(classId, objectId, 0);

	/* Release memory */
	pfree(values);
	pfree(nulls);
	pfree(replaces);
	heap_freetuple(newtup);

	ReleaseSysCache(oldtup);
}

/*
 * Executes an ALTER OBJECT / RENAME TO statement.  Based on the object
 * type, the function appropriate to that type is executed.
 *
 * Return value is the address of the renamed object.
 */
ObjectAddress
ExecRenameStmt(RenameStmt *stmt)
{
	switch (stmt->renameType)
	{
		case OBJECT_TABCONSTRAINT:
		case OBJECT_DOMCONSTRAINT:
			return RenameConstraint(stmt);

		case OBJECT_DATABASE:
			return RenameDatabase(stmt->subname, stmt->newname);

		case OBJECT_ROLE:
			return RenameRole(stmt->subname, stmt->newname);

		case OBJECT_SCHEMA:
			return RenameSchema(stmt->subname, stmt->newname);

		case OBJECT_TABLESPACE:
			return RenameTableSpace(stmt->subname, stmt->newname);

		case OBJECT_TABLE:
		case OBJECT_SEQUENCE:
		case OBJECT_VIEW:
		case OBJECT_MATVIEW:
		case OBJECT_INDEX:
		case OBJECT_FOREIGN_TABLE:
			return RenameRelation(stmt);

		case OBJECT_COLUMN:
		case OBJECT_ATTRIBUTE:
			return renameatt(stmt);

		case OBJECT_RULE:
			return RenameRewriteRule(stmt->relation, stmt->subname,
									 stmt->newname);

		case OBJECT_TRIGGER:
			return renametrig(stmt);

		case OBJECT_POLICY:
			return rename_policy(stmt);

		case OBJECT_DOMAIN:
			return RenameType(stmt);
		case OBJECT_TYPE:
			return RenameType(stmt);

		case OBJECT_AGGREGATE:
		case OBJECT_COLLATION:
		case OBJECT_CONVERSION:
		case OBJECT_EVENT_TRIGGER:
		case OBJECT_FDW:
		case OBJECT_FOREIGN_SERVER:
		case OBJECT_FUNCTION:
		case OBJECT_OPCLASS:
		case OBJECT_OPFAMILY:
		case OBJECT_LANGUAGE:
		case OBJECT_PROCEDURE:
		case OBJECT_ROUTINE:
		case OBJECT_STATISTIC_EXT:
		case OBJECT_TSCONFIGURATION:
		case OBJECT_TSDICTIONARY:
		case OBJECT_TSPARSER:
		case OBJECT_TSTEMPLATE:
		case OBJECT_PUBLICATION:
		case OBJECT_SUBSCRIPTION:
			{
				ObjectAddress address;
				Relation	catalog;
				Relation	relation;

				address = get_object_address(stmt->renameType,
											 stmt->object,
											 &relation,
											 AccessExclusiveLock, false);
				Assert(relation == NULL);

				catalog = heap_open(address.classId, RowExclusiveLock);
				AlterObjectRename_internal(catalog,
										   address.objectId,
										   stmt->newname);
				heap_close(catalog, RowExclusiveLock);

				return address;
			}

		default:
			elog(ERROR, "unrecognized rename stmt type: %d",
				 (int) stmt->renameType);
			return InvalidObjectAddress;	/* keep compiler happy */
	}
}

/*
 * Executes an ALTER OBJECT / DEPENDS ON [EXTENSION] statement.
 *
 * Return value is the address of the altered object.  refAddress is an output
 * argument which, if not null, receives the address of the object that the
 * altered object now depends on.
 */
ObjectAddress
ExecAlterObjectDependsStmt(AlterObjectDependsStmt *stmt, ObjectAddress *refAddress)
{
	ObjectAddress address;
	ObjectAddress refAddr;
	Relation	rel;
	List   *currexts;

	address =
		get_object_address_rv(stmt->objectType, stmt->relation, (List *) stmt->object,
							  &rel, AccessExclusiveLock, false);

	/*
	 * If a relation was involved, it would have been opened and locked. We
	 * don't need the relation here, but we'll retain the lock until commit.
	 */
	if (rel)
		heap_close(rel, NoLock);

	refAddr = get_object_address(OBJECT_EXTENSION, (Node *) stmt->extname,
								 &rel, AccessExclusiveLock, false);
	Assert(rel == NULL);
	if (refAddress)
		*refAddress = refAddr;

	/* Avoid duplicates */
	currexts = getAutoExtensionsOfObject(address.classId,
										 address.objectId);
	if (!list_member_oid(currexts, refAddr.objectId))
		recordDependencyOn(&address, &refAddr, DEPENDENCY_AUTO_EXTENSION);

	return address;
}

/*
 * Executes an ALTER OBJECT / SET SCHEMA statement.  Based on the object
 * type, the function appropriate to that type is executed.
 *
 * Return value is that of the altered object.
 *
 * oldSchemaAddr is an output argument which, if not NULL, is set to the object
 * address of the original schema.
 */
ObjectAddress
ExecAlterObjectSchemaStmt(AlterObjectSchemaStmt *stmt,
						  ObjectAddress *oldSchemaAddr)
{
	ObjectAddress address;
	Oid			oldNspOid;

	switch (stmt->objectType)
	{
		case OBJECT_EXTENSION:
			address = AlterExtensionNamespace(strVal((Value *) stmt->object), stmt->newschema,
											  oldSchemaAddr ? &oldNspOid : NULL);
			break;

		case OBJECT_FOREIGN_TABLE:
		case OBJECT_SEQUENCE:
		case OBJECT_TABLE:
		case OBJECT_VIEW:
		case OBJECT_MATVIEW:
			address = AlterTableNamespace(stmt,
										  oldSchemaAddr ? &oldNspOid : NULL);
			break;

		case OBJECT_DOMAIN:
		case OBJECT_TYPE:
			address = AlterTypeNamespace(castNode(List, stmt->object), stmt->newschema,
										 stmt->objectType,
										 oldSchemaAddr ? &oldNspOid : NULL);
			break;

			/* generic code path */
		case OBJECT_AGGREGATE:
		case OBJECT_COLLATION:
		case OBJECT_CONVERSION:
		case OBJECT_FUNCTION:
		case OBJECT_OPERATOR:
		case OBJECT_OPCLASS:
		case OBJECT_OPFAMILY:
		case OBJECT_PROCEDURE:
		case OBJECT_ROUTINE:
		case OBJECT_STATISTIC_EXT:
		case OBJECT_TSCONFIGURATION:
		case OBJECT_TSDICTIONARY:
		case OBJECT_TSPARSER:
		case OBJECT_TSTEMPLATE:
			{
				Relation	catalog;
				Relation	relation;
				Oid			classId;
				Oid			nspOid;

				address = get_object_address(stmt->objectType,
											 stmt->object,
											 &relation,
											 AccessExclusiveLock,
											 false);
				Assert(relation == NULL);
				classId = address.classId;
				catalog = heap_open(classId, RowExclusiveLock);
				nspOid = LookupCreationNamespace(stmt->newschema);

				oldNspOid = AlterObjectNamespace_internal(catalog, address.objectId,
														  nspOid);
				heap_close(catalog, RowExclusiveLock);
			}
			break;

		default:
			elog(ERROR, "unrecognized AlterObjectSchemaStmt type: %d",
				 (int) stmt->objectType);
			return InvalidObjectAddress;	/* keep compiler happy */
	}

	if (oldSchemaAddr)
		ObjectAddressSet(*oldSchemaAddr, NamespaceRelationId, oldNspOid);

	return address;
}

/*
 * Change an object's namespace given its classOid and object Oid.
 *
 * Objects that don't have a namespace should be ignored.
 *
 * This function is currently used only by ALTER EXTENSION SET SCHEMA,
 * so it only needs to cover object types that can be members of an
 * extension, and it doesn't have to deal with certain special cases
 * such as not wanting to process array types --- those should never
 * be direct members of an extension anyway.  Nonetheless, we insist
 * on listing all OCLASS types in the switch.
 *
 * Returns the OID of the object's previous namespace, or InvalidOid if
 * object doesn't have a schema.
 */
Oid
AlterObjectNamespace_oid(Oid classId, Oid objid, Oid nspOid,
						 ObjectAddresses *objsMoved)
{
	Oid			oldNspOid = InvalidOid;
	ObjectAddress dep;

	dep.classId = classId;
	dep.objectId = objid;
	dep.objectSubId = 0;

	switch (getObjectClass(&dep))
	{
		case OCLASS_CLASS:
			{
				Relation	rel;

				rel = relation_open(objid, AccessExclusiveLock);
				oldNspOid = RelationGetNamespace(rel);

				AlterTableNamespaceInternal(rel, oldNspOid, nspOid, objsMoved, NULL);

				relation_close(rel, NoLock);
				break;
			}

		case OCLASS_TYPE:
			oldNspOid = AlterTypeNamespace_oid(objid, nspOid, objsMoved);
			break;

		case OCLASS_PROC:
		case OCLASS_COLLATION:
		case OCLASS_CONVERSION:
		case OCLASS_OPERATOR:
		case OCLASS_OPCLASS:
		case OCLASS_OPFAMILY:
		case OCLASS_STATISTIC_EXT:
		case OCLASS_TSPARSER:
		case OCLASS_TSDICT:
		case OCLASS_TSTEMPLATE:
		case OCLASS_TSCONFIG:
		case OCLASS_SYNONYM:
		case OCLASS_PACKAGE:
		case OCLASS_SPMPLAN:
		case OCLASS_SPMPLAN_HISTORY:
		case OCLASS_CHILDALIAS:
			{
				Relation	catalog;

				catalog = heap_open(classId, RowExclusiveLock);

				oldNspOid = AlterObjectNamespace_internal(catalog, objid,
														  nspOid);

				heap_close(catalog, RowExclusiveLock);
			}
			break;

		case OCLASS_CAST:
		case OCLASS_CONSTRAINT:
		case OCLASS_DEFAULT:
		case OCLASS_LANGUAGE:
		case OCLASS_LARGEOBJECT:
		case OCLASS_AM:
		case OCLASS_AMOP:
		case OCLASS_AMPROC:
		case OCLASS_REWRITE:
		case OCLASS_TRIGGER:
		case OCLASS_SCHEMA:
		case OCLASS_ROLE:
		case OCLASS_DATABASE:
		case OCLASS_TBLSPACE:
		case OCLASS_FDW:
		case OCLASS_FOREIGN_SERVER:
		case OCLASS_USER_MAPPING:
		case OCLASS_DEFACL:
		case OCLASS_EXTENSION:
		case OCLASS_EVENT_TRIGGER:
		case OCLASS_POLICY:
		case OCLASS_PUBLICATION:
		case OCLASS_PUBLICATION_REL:
		case OCLASS_SUBSCRIPTION:
		case OCLASS_TRANSFORM:
		case OCLASS_EXTPROTOCOL:
		case OCLASS_PGXC_NODE:
		case OCLASS_PGXC_GROUP:
		case OCLASS_PGXC_CLASS:
#ifdef __AUDIT__
		case OCLASS_AUDIT_STMT:
		case OCLASS_AUDIT_USER:
		case OCLASS_AUDIT_OBJ:
		case OCLASS_AUDIT_OBJDEFAULT:
#endif
#ifdef __STORAGE_SCALABLE__
		case OCLASS_PUBLICATION_SHARD:
		case OCLASS_SUBSCRIPTION_SHARD:
		case OCLASS_SUBSCRIPTION_TABLE:
#endif
		case OCLASS_TYPE_OBJECT:
			/* ignore object types that don't have schema-qualified names */
			break;

			/*
			 * There's intentionally no default: case here; we want the
			 * compiler to warn if a new OCLASS hasn't been handled above.
			 */
	}

	return oldNspOid;
}

/*
 * Generic function to change the namespace of a given object, for simple
 * cases (won't work for tables, nor other cases where we need to do more
 * than change the namespace column of a single catalog entry).
 *
 * rel: catalog relation containing object (RowExclusiveLock'd by caller)
 * objid: OID of object to change the namespace of
 * nspOid: OID of new namespace
 *
 * Returns the OID of the object's previous namespace.
 */
static Oid
AlterObjectNamespace_internal(Relation rel, Oid objid, Oid nspOid)
{
	Oid			classId = RelationGetRelid(rel);
	int			oidCacheId = get_object_catcache_oid(classId);
	int			nameCacheId = get_object_catcache_name(classId);
	AttrNumber	Anum_name = get_object_attnum_name(classId);
	AttrNumber	Anum_namespace = get_object_attnum_namespace(classId);
	AttrNumber	Anum_owner = get_object_attnum_owner(classId);
	AclObjectKind acl_kind = get_object_aclkind(classId);
	Oid			oldNspOid;
	Datum		name,
				namespace;
	bool		isnull;
	HeapTuple	tup,
				newtup;
	Datum	   *values;
	bool	   *nulls;
	bool	   *replaces;

	tup = SearchSysCacheCopy1(oidCacheId, ObjectIdGetDatum(objid));
	if (!HeapTupleIsValid(tup)) /* should not happen */
		elog(ERROR, "cache lookup failed for object %u of catalog \"%s\"",
			 objid, RelationGetRelationName(rel));

	name = heap_getattr(tup, Anum_name, RelationGetDescr(rel), &isnull);
	Assert(!isnull);
	namespace = heap_getattr(tup, Anum_namespace, RelationGetDescr(rel),
							 &isnull);
	Assert(!isnull);
	oldNspOid = DatumGetObjectId(namespace);

	/*
	 * If the object is already in the correct namespace, we don't need to do
	 * anything except fire the object access hook.
	 */
	if (oldNspOid == nspOid)
	{
		InvokeObjectPostAlterHook(classId, objid, 0);
		return oldNspOid;
	}

	/* Check basic namespace related issues */
	CheckSetNamespace(oldNspOid, nspOid);

	/* Permission checks ... superusers can always do it */
	if (!superuser())
	{
		Datum		owner;
		Oid			ownerId;
		AclResult	aclresult;

		/* Fail if object does not have an explicit owner */
		if (Anum_owner <= 0)
			ereport(ERROR,
					(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
					 (errmsg("must be superuser to set schema of %s",
							 getObjectDescriptionOids(classId, objid)))));

		/* Otherwise, must be owner of the existing object */
		owner = heap_getattr(tup, Anum_owner, RelationGetDescr(rel), &isnull);
		Assert(!isnull);
		ownerId = DatumGetObjectId(owner);

		if (!has_privs_of_role(GetUserId(), ownerId))
			aclcheck_error(ACLCHECK_NOT_OWNER, acl_kind,
						   NameStr(*(DatumGetName(name))));

		/* User must have CREATE privilege on new namespace */
		aclresult = pg_namespace_aclcheck(nspOid, GetUserId(), ACL_CREATE);
		if (aclresult != ACLCHECK_OK)
			aclcheck_error(aclresult, ACL_KIND_NAMESPACE,
						   get_namespace_name(nspOid));
	}

	/*
	 * Check for duplicate name (more friendly than unique-index failure).
	 * Since this is just a friendliness check, we can just skip it in cases
	 * where there isn't suitable support.
	 */
	if (classId == ProcedureRelationId)
	{
		Form_pg_proc proc = (Form_pg_proc) GETSTRUCT(tup);

		/*
		 * if the function is a builtin function, its Oid is less than 10000.
		 * we can't allow change the namespace of the builtin functions
		 */
		if (IsSystemObjOid(objid) && !IsInplaceUpgrade)
			ereport(ERROR,
				(errcode(ERRCODE_INVALID_FUNCTION_DEFINITION),
					errmsg("namespace change failed for function %u, because it is a builtin function.",
							objid)));

		IsThereFunctionInNamespace(NameStr(proc->proname), proc->pronargs,
								   &proc->proargtypes, nspOid);
	}
	else if (classId == CollationRelationId)
	{
		Form_pg_collation coll = (Form_pg_collation) GETSTRUCT(tup);

		IsThereCollationInNamespace(NameStr(coll->collname), nspOid);
	}
	else if (classId == OperatorClassRelationId)
	{
		Form_pg_opclass opc = (Form_pg_opclass) GETSTRUCT(tup);

		IsThereOpClassInNamespace(NameStr(opc->opcname),
								  opc->opcmethod, nspOid);
	}
	else if (classId == OperatorFamilyRelationId)
	{
		Form_pg_opfamily opf = (Form_pg_opfamily) GETSTRUCT(tup);

		IsThereOpFamilyInNamespace(NameStr(opf->opfname),
								   opf->opfmethod, nspOid);
	}
	else if (nameCacheId >= 0 &&
			 SearchSysCacheExists2(nameCacheId, name,
								   ObjectIdGetDatum(nspOid)))
		report_namespace_conflict(classId,
								  NameStr(*(DatumGetName(name))),
								  nspOid);

	/* Build modified tuple */
	values = palloc0(RelationGetNumberOfAttributes(rel) * sizeof(Datum));
	nulls = palloc0(RelationGetNumberOfAttributes(rel) * sizeof(bool));
	replaces = palloc0(RelationGetNumberOfAttributes(rel) * sizeof(bool));
	values[Anum_namespace - 1] = ObjectIdGetDatum(nspOid);
	replaces[Anum_namespace - 1] = true;
	newtup = heap_modify_tuple(tup, RelationGetDescr(rel),
							   values, nulls, replaces);

	/* Perform actual update */
	CatalogTupleUpdate(rel, &tup->t_self, newtup);

	/* Release memory */
	pfree(values);
	pfree(nulls);
	pfree(replaces);

	/* update dependencies to point to the new schema */
	changeDependencyFor(classId, objid,
						NamespaceRelationId, oldNspOid, nspOid);

	InvokeObjectPostAlterHook(classId, objid, 0);

	return oldNspOid;
}

/*
 * Executes an ALTER OBJECT / OWNER TO statement.  Based on the object
 * type, the function appropriate to that type is executed.
 */
ObjectAddress
ExecAlterOwnerStmt(AlterOwnerStmt *stmt)
{
	Oid			newowner = get_rolespec_oid(stmt->newowner, false);

	switch (stmt->objectType)
	{
		case OBJECT_DATABASE:
			return AlterDatabaseOwner(strVal((Value *) stmt->object), newowner);

		case OBJECT_SCHEMA:
			return AlterSchemaOwner(strVal((Value *) stmt->object), newowner);

		case OBJECT_TYPE:
		case OBJECT_DOMAIN:		/* same as TYPE */
			return AlterTypeOwner(castNode(List, stmt->object), newowner, stmt->objectType);
			break;

		case OBJECT_FDW:
			return AlterForeignDataWrapperOwner(strVal((Value *) stmt->object),
												newowner);

		case OBJECT_FOREIGN_SERVER:
			return AlterForeignServerOwner(strVal((Value *) stmt->object),
										   newowner);

		case OBJECT_EVENT_TRIGGER:
			return AlterEventTriggerOwner(strVal((Value *) stmt->object),
										  newowner);

		case OBJECT_PUBLICATION:
			return AlterPublicationOwner(strVal((Value *) stmt->object),
										 newowner);

		case OBJECT_SUBSCRIPTION:
			return AlterSubscriptionOwner(strVal((Value *) stmt->object),
										  newowner);

			/* Generic cases */
		case OBJECT_AGGREGATE:
		case OBJECT_COLLATION:
		case OBJECT_CONVERSION:
		case OBJECT_FUNCTION:
		case OBJECT_LANGUAGE:
		case OBJECT_LARGEOBJECT:
		case OBJECT_OPERATOR:
		case OBJECT_OPCLASS:
		case OBJECT_OPFAMILY:
		case OBJECT_PROCEDURE:
		case OBJECT_ROUTINE:
		case OBJECT_STATISTIC_EXT:
		case OBJECT_TABLESPACE:
		case OBJECT_TSDICTIONARY:
		case OBJECT_SYNONYM:
		case OBJECT_TSCONFIGURATION:
			{
				Relation	catalog;
				Relation	relation;
				Oid			classId;
				ObjectAddress address;

				address = get_object_address(stmt->objectType,
											 stmt->object,
											 &relation,
											 AccessExclusiveLock,
											 false);
				Assert(relation == NULL);
				classId = address.classId;

				/*
				 * XXX - get_object_address returns Oid of pg_largeobject
				 * catalog for OBJECT_LARGEOBJECT because of historical
				 * reasons.  Fix up it here.
				 */
				if (classId == LargeObjectRelationId)
					classId = LargeObjectMetadataRelationId;

				catalog = heap_open(classId, RowExclusiveLock);
				if (classId == ProcedureRelationId)
				{
					/*
					 * if the function is a builtin function, its Oid is less than 10000.
					 * we can't allow change the ownerId of the builtin functions
					 */
					if (IsSystemObjOid(address.objectId) && !IsInplaceUpgrade)
						ereport(ERROR,
							(errcode(ERRCODE_INVALID_FUNCTION_DEFINITION),
								errmsg("function \"%s\" is a builtin function, "
										"its owner can not be changed",
										strVal((Value *) stmt->object))));
				}
				AlterObjectOwner_internal(catalog, address.objectId, newowner);
				heap_close(catalog, RowExclusiveLock);

				return address;
			}
			break;
		case OBJECT_PACKAGE:
			{
				Oid			pkg_namespaceid;
				Oid			pkg_crrsid;
				char		*pkg_name;
				Relation	pkg_catalog;
				Relation	pkg_relation;
				ObjectAddress pkg_address;

				Relation		proc_catalog;
				Oid				proc_oid;
				HeapTuple		proc_tuple;
				HeapScanDesc	proc_scandesc;
				ScanKeyData		proc_entry[1];

				Relation		type_catalog;
				Oid				type_oid;
				HeapTuple		type_tuple;
				HeapScanDesc	type_scandesc;
				ScanKeyData		type_entry[1];

				Assert(IsA(stmt->object, RangeVar));

				/* 1. processing package owner in pg_package */
				pkg_address = get_object_address(stmt->objectType,
												 stmt->object,
												 &pkg_relation,
												 AccessExclusiveLock,
												 false);
				Assert(pkg_relation == NULL);
				Assert(pkg_address.classId == PackageRelationId);

				pkg_catalog = heap_open(pkg_address.classId, RowExclusiveLock);
				AlterObjectOwner_internal(pkg_catalog, pkg_address.objectId, newowner);
				heap_close(pkg_catalog, RowExclusiveLock);

				/* 2. processing package schema owner in pg_namespace */
				pkg_namespaceid = RangeVarGetCreationNamespace((RangeVar *) stmt->object);
				Assert(OidIsValid(pkg_namespaceid));

				pkg_name = ((RangeVar *) stmt->object)->relname;
				pkg_crrsid = get_package_corresponding_namespace((const char *) pkg_name, pkg_namespaceid);
				Assert(OidIsValid(pkg_crrsid));

				AlterSchemaOwner_oid(pkg_crrsid, newowner);

				/* should be aware of package namespace changes */
				CommandCounterIncrement();

				/* 3. processing package functions owner in pg_proc */
				proc_catalog = heap_open(ProcedureRelationId, RowExclusiveLock);

				ScanKeyInit(&proc_entry[0], Anum_pg_proc_pronamespace, BTEqualStrategyNumber,
							F_OIDEQ, ObjectIdGetDatum(pkg_crrsid));

				proc_scandesc = heap_beginscan_catalog(proc_catalog, 1, proc_entry);

				while ((proc_tuple = heap_getnext(proc_scandesc, ForwardScanDirection)) != NULL)
				{
					proc_oid = HeapTupleGetOid(proc_tuple);
					Assert(OidIsValid(proc_oid));
					AlterObjectOwner_internal(proc_catalog, proc_oid, newowner);
				}

				heap_endscan(proc_scandesc);
				heap_close(proc_catalog, RowExclusiveLock);

				/* 4. processing package collection types owner in pg_type */
				type_catalog = heap_open(TypeRelationId, RowExclusiveLock);

				ScanKeyInit(&type_entry[0], Anum_pg_type_typnamespace, BTEqualStrategyNumber,
							F_OIDEQ, ObjectIdGetDatum(pkg_crrsid));

				type_scandesc = heap_beginscan_catalog(type_catalog, 1, type_entry);

				while ((type_tuple = heap_getnext(type_scandesc, ForwardScanDirection)) != NULL)
				{
					type_oid = HeapTupleGetOid(type_tuple);
					Assert(OidIsValid(type_oid));
					AlterTypeOwner_oid(type_oid, newowner, true);
				}

				heap_endscan(type_scandesc);
				heap_close(type_catalog, RowExclusiveLock);

				return pkg_address;
			}
			break;
		default:
			elog(ERROR, "unrecognized AlterOwnerStmt type: %d",
				 (int) stmt->objectType);
			return InvalidObjectAddress;	/* keep compiler happy */
	}
}

/*
 * Generic function to change the ownership of a given object, for simple
 * cases (won't work for tables, nor other cases where we need to do more than
 * change the ownership column of a single catalog entry).
 *
 * rel: catalog relation containing object (RowExclusiveLock'd by caller)
 * objectId: OID of object to change the ownership of
 * new_ownerId: OID of new object owner
 */
void
AlterObjectOwner_internal(Relation rel, Oid objectId, Oid new_ownerId)
{
	Oid			classId = RelationGetRelid(rel);
	AttrNumber	Anum_owner = get_object_attnum_owner(classId);
	AttrNumber	Anum_namespace = get_object_attnum_namespace(classId);
	AttrNumber	Anum_acl = get_object_attnum_acl(classId);
	AttrNumber	Anum_name = get_object_attnum_name(classId);
	HeapTuple	oldtup;
	Datum		datum;
	bool		isnull;
	Oid			old_ownerId;
	Oid			namespaceId = InvalidOid;

	oldtup = get_catalog_object_by_oid(rel, objectId);
	if (oldtup == NULL)
		elog(ERROR, "cache lookup failed for object %u of catalog \"%s\"",
			 objectId, RelationGetRelationName(rel));

	datum = heap_getattr(oldtup, Anum_owner,
						 RelationGetDescr(rel), &isnull);
	Assert(!isnull);
	old_ownerId = DatumGetObjectId(datum);

	if (Anum_namespace != InvalidAttrNumber)
	{
		datum = heap_getattr(oldtup, Anum_namespace,
							 RelationGetDescr(rel), &isnull);
		Assert(!isnull);
		namespaceId = DatumGetObjectId(datum);
	}

	if (old_ownerId != new_ownerId)
	{
		AttrNumber	nattrs;
		HeapTuple	newtup;
		Datum	   *values;
		bool	   *nulls;
		bool	   *replaces;

		/* Superusers can bypass permission checks */
		if (!superuser())
		{
			AclObjectKind aclkind = get_object_aclkind(classId);

			/* must be owner */
			if (!has_privs_of_role(GetUserId(), old_ownerId))
			{
				char	   *objname;
				char		namebuf[NAMEDATALEN];

				if (Anum_name != InvalidAttrNumber)
				{
					datum = heap_getattr(oldtup, Anum_name,
										 RelationGetDescr(rel), &isnull);
					Assert(!isnull);
					objname = NameStr(*DatumGetName(datum));
				}
				else
				{
					snprintf(namebuf, sizeof(namebuf), "%u",
							 HeapTupleGetOid(oldtup));
					objname = namebuf;
				}
				aclcheck_error(ACLCHECK_NOT_OWNER, aclkind, objname);
			}
			/* Must be able to become new owner */
			check_is_member_of_role(GetUserId(), new_ownerId);

			/* New owner must have CREATE privilege on namespace */
			if (OidIsValid(namespaceId))
			{
				AclResult	aclresult;

				aclresult = pg_namespace_aclcheck(namespaceId, new_ownerId,
												  ACL_CREATE);
				if (aclresult != ACLCHECK_OK)
					aclcheck_error(aclresult, ACL_KIND_NAMESPACE,
								   get_namespace_name(namespaceId));
			}
		}

		/* untrusted? don't allow ALTER OWNER to non-super user */
		if(classId == ExtprotocolRelationId) 
		{
			char *old_name;
			bool is_trusted;

			datum = heap_getattr(oldtup, Anum_name,
                                                 RelationGetDescr(rel), &isnull);

			Assert(!isnull);
		
			old_name = NameStr(*(DatumGetName(datum)));
		
			datum = heap_getattr(oldtup, Anum_pg_extprotocol_ptctrusted,
						 RelationGetDescr(rel), &isnull);
			if (isnull)
				ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_OBJECT),
				 	errmsg("internal error: protocol \"%s\" has no trust attribute defined", old_name)));
			
			is_trusted = DatumGetBool(datum);
			
			if(!is_trusted && !superuser_arg(new_ownerId))
				ereport(ERROR,
					(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				errmsg("untrusted protocol \"%s\" can't be owned by non superuser", old_name)));
		}

		/* Build a modified tuple */
		nattrs = RelationGetNumberOfAttributes(rel);
		values = palloc0(nattrs * sizeof(Datum));
		nulls = palloc0(nattrs * sizeof(bool));
		replaces = palloc0(nattrs * sizeof(bool));
		values[Anum_owner - 1] = ObjectIdGetDatum(new_ownerId);
		replaces[Anum_owner - 1] = true;

		/*
		 * Determine the modified ACL for the new owner.  This is only
		 * necessary when the ACL is non-null.
		 */
		if (Anum_acl != InvalidAttrNumber)
		{
			datum = heap_getattr(oldtup,
								 Anum_acl, RelationGetDescr(rel), &isnull);
			if (!isnull)
			{
				Acl		   *newAcl;

				newAcl = aclnewowner(DatumGetAclP(datum),
									 old_ownerId, new_ownerId);
				values[Anum_acl - 1] = PointerGetDatum(newAcl);
				replaces[Anum_acl - 1] = true;
			}
		}

		newtup = heap_modify_tuple(oldtup, RelationGetDescr(rel),
								   values, nulls, replaces);

		/* Perform actual update */
		CatalogTupleUpdate(rel, &newtup->t_self, newtup);

		/* Update owner dependency reference */
		if (classId == LargeObjectMetadataRelationId)
			classId = LargeObjectRelationId;
		changeDependencyOnOwner(classId, HeapTupleGetOid(newtup), new_ownerId);

		/* Release memory */
		pfree(values);
		pfree(nulls);
		pfree(replaces);
	}

	InvokeObjectPostAlterHook(classId, objectId, 0);
}
