/*-------------------------------------------------------------------------
 *
 * syscache.c
 *	  System cache management routines
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 *
 *
 * IDENTIFICATION
 *	  src/backend/utils/cache/syscache.c
 *
 * NOTES
 *	  These routines allow the parser/planner/executor to perform
 *	  rapid lookups on the contents of the system catalogs.
 *
 *	  see utils/syscache.h for a list of the cache IDs
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/htup_details.h"
#include "access/sysattr.h"
#include "catalog/indexing.h"
#include "catalog/pg_aggregate.h"
#include "catalog/pg_dbms_job.h"
#include "catalog/pg_am.h"
#include "catalog/pg_amop.h"
#include "catalog/pg_amproc.h"
#include "catalog/pg_auth_members.h"
#include "catalog/pg_authid.h"
#include "catalog/pg_cast.h"
#include "catalog/pg_collation.h"
#include "catalog/pg_constraint.h"
#include "catalog/pg_conversion.h"
#include "catalog/pg_database.h"
#include "catalog/pg_db_role_setting.h"
#include "catalog/pg_default_acl.h"
#include "catalog/pg_depend.h"
#include "catalog/pg_description.h"
#include "catalog/pg_enum.h"
#include "catalog/pg_event_trigger.h"
#include "catalog/pg_extprotocol_d.h"
#include "catalog/pg_foreign_data_wrapper.h"
#include "catalog/pg_foreign_server.h"
#include "catalog/pg_foreign_table.h"
#include "catalog/pg_language.h"
#ifdef _MLS_
#include "catalog/pg_mls.h"
#endif
#include "catalog/pg_namespace.h"
#include "catalog/pg_opclass.h"
#include "catalog/pg_operator.h"
#include "catalog/pg_opfamily.h"
#include "catalog/pg_partitioned_table.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_publication.h"
#include "catalog/pg_publication_rel.h"
#include "catalog/pg_range.h"
#include "catalog/pg_rewrite.h"
#include "catalog/pg_seclabel.h"
#include "catalog/pg_sequence.h"
#include "catalog/pg_ora_sequence.h"
#include "catalog/pg_shdepend.h"
#include "catalog/pg_shdescription.h"
#include "catalog/pg_shseclabel.h"
#include "catalog/pg_term.h"
#include "catalog/pg_replication_origin.h"
#include "catalog/pg_statistic.h"
#include "catalog/pg_statistic_ext.h"
#include "catalog/pg_subscription.h"
#include "catalog/pg_subscription_rel.h"
#include "catalog/pg_synonym.h"
#include "catalog/pg_spm_plan.h"
#include "catalog/pg_spm_plan_history.h"
#include "catalog/pg_tablespace.h"
#include "catalog/pg_transform.h"
#include "catalog/pg_ts_config.h"
#include "catalog/pg_ts_config_map.h"
#include "catalog/pg_ts_dict.h"
#include "catalog/pg_ts_parser.h"
#include "catalog/pg_ts_template.h"
#include "catalog/pg_type.h"
#include "catalog/pg_user_mapping.h"
#ifdef __AUDIT__
#include "catalog/pg_audit.h"
#endif
#ifdef PGXC
#include "catalog/pgxc_class.h"
#include "catalog/pgxc_node.h"
#include "catalog/pgxc_group.h"
#endif
#include "utils/rel.h"
#include "utils/catcache.h"
#include "utils/syscache.h"
#ifdef __STORAGE_SCALABLE__
#include "catalog/pg_publication_shard.h"
#include "catalog/pg_subscription_shard.h"
#include "catalog/pg_subscription_table.h"
#endif
#ifdef _PG_ORCL_
#include "catalog/pg_compile.h"
#include "catalog/pg_package.h"
#include "catalog/pg_childalias.h"
#endif

#include "catalog/pg_resgroup.h"
#include "catalog/pg_directory.h"
#include "catalog/directory.h"
#include "catalog/pg_type_object.h"

/*---------------------------------------------------------------------------

	Adding system caches:

	Add your new cache to the list in include/utils/syscache.h.
	Keep the list sorted alphabetically.

	Add your entry to the cacheinfo[] array below. All cache lists are
	alphabetical, so add it in the proper place.  Specify the relation OID,
	index OID, number of keys, key attribute numbers, and initial number of
	hash buckets.

	The number of hash buckets must be a power of 2.  It's reasonable to
	set this to the number of entries that might be in the particular cache
	in a medium-size database.

	There must be a unique index underlying each syscache (ie, an index
	whose key is the same as that of the cache).  If there is not one
	already, add definitions for it to include/catalog/indexing.h: you need
	to add a DECLARE_UNIQUE_INDEX macro and a #define for the index OID.
	(Adding an index requires a catversion.h update, while simply
	adding/deleting caches only requires a recompile.)

	Finally, any place your relation gets heap_insert() or
	heap_update() calls, use CatalogTupleInsert() or CatalogTupleUpdate()
	instead, which also update indexes.  The heap_* calls do not do that.

*---------------------------------------------------------------------------
*/

/*
 *		struct cachedesc: information defining a single syscache
 */
struct cachedesc
{
	Oid			reloid;			/* OID of the relation being cached */
	Oid			indoid;			/* OID of index relation for this cache */
	int			nkeys;			/* # of keys needed for cache lookup */
	int			key[4];			/* attribute numbers of key attrs */
	int			nbuckets;		/* number of hash buckets for this cache */
};

static const struct cachedesc cacheinfo[] = {
	{AggregateRelationId,		/* AGGFNOID */
		AggregateFnoidIndexId,
		1,
		{
			Anum_pg_aggregate_aggfnoid,
			0,
			0,
			0
		},
		16
	},
	{AccessMethodRelationId,	/* AMNAME */
		AmNameIndexId,
		1,
		{
			Anum_pg_am_amname,
			0,
			0,
			0
		},
		4
	},
	{AccessMethodRelationId,	/* AMOID */
		AmOidIndexId,
		1,
		{
			ObjectIdAttributeNumber,
			0,
			0,
			0
		},
		4
	},
	{AccessMethodOperatorRelationId,	/* AMOPOPID */
		AccessMethodOperatorIndexId,
		3,
		{
			Anum_pg_amop_amopopr,
			Anum_pg_amop_amoppurpose,
			Anum_pg_amop_amopfamily,
			0
		},
		64
	},
	{AccessMethodOperatorRelationId,	/* AMOPSTRATEGY */
		AccessMethodStrategyIndexId,
		4,
		{
			Anum_pg_amop_amopfamily,
			Anum_pg_amop_amoplefttype,
			Anum_pg_amop_amoprighttype,
			Anum_pg_amop_amopstrategy
		},
		64
	},
	{AccessMethodProcedureRelationId,	/* AMPROCNUM */
		AccessMethodProcedureIndexId,
		4,
		{
			Anum_pg_amproc_amprocfamily,
			Anum_pg_amproc_amproclefttype,
			Anum_pg_amproc_amprocrighttype,
			Anum_pg_amproc_amprocnum
		},
		16
	},
	{AttributeRelationId,		/* ATTNAME */
		AttributeRelidNameIndexId,
		2,
		{
			Anum_pg_attribute_attrelid,
			Anum_pg_attribute_attname,
			0,
			0
		},
		32
	},
	{AttributeRelationId,		/* ATTNUM */
		AttributeRelidNumIndexId,
		2,
		{
			Anum_pg_attribute_attrelid,
			Anum_pg_attribute_attnum,
			0,
			0
		},
		128
	},
#ifdef __AUDIT__
	{PgAuditStmtConfRelationId, 	/* AUDITSTMTCONF */
		AuditStmtConfIndexID,
		1,
		{
			Anum_audit_stmt_conf_action_id,
			0,
			0,
			0
		},
		64
	},
	{PgAuditStmtConfRelationId,		/* AUDITSTMTCONFOID */
		AuditStmtConfOidIndexId,
		1,
		{
			ObjectIdAttributeNumber,
			0,
			0,
			0
		},
		4
	},
	{PgAuditUserConfRelationId, 	/* AUDITUSERCONF */
		AuditUserConfIndexID,
		2,
		{
			Anum_audit_user_conf_user_id,
			Anum_audit_user_conf_action_id,
			0,
			0
		},
		64
	},
	{PgAuditUserConfRelationId,		/* AUDITUSERCONFOID */
		AuditUserConfOidIndexId,
		1,
		{
			ObjectIdAttributeNumber,
			0,
			0,
			0
		},
		4
	},
	{PgAuditObjConfRelationId,		/* AUDITOBJCONF */
		AuditObjConfIndexID,
		4,
		{
			Anum_audit_obj_conf_class_id,
			Anum_audit_obj_conf_object_id,
			Anum_audit_obj_conf_object_sub_id,
			Anum_audit_obj_conf_action_id
		},
		64
	},
	{PgAuditObjConfRelationId,		/* AUDITOBJCONFOID */
		AuditObjConfOidIndexId,
		1,
		{
			ObjectIdAttributeNumber,
			0,
			0,
			0
		},
		4
	},
	{PgAuditObjDefOptsRelationId,		/* AUDITOBJDEFAULT */
		AuditObjDefOptsIndexID,
		1,
		{
			Anum_audit_obj_def_opts_action_id,
			0,
			0,
			0
		},
		16
	},
	{PgAuditObjDefOptsRelationId,		/* AUDITOBJDEFAULTOID */
		AuditObjDefOptsOidIndexId,
		1,
		{
			ObjectIdAttributeNumber,
			0,
			0,
			0
		},
		4
	},
	{PgAuditFgaConfRelationId,		/* AUDITFGAPOLICYCONF */
		AuditFgaConfPolicyNameIndexID,
		1,
		{
			Anum_audit_fga_conf_policy_name,
			0,
			0,
			0
		},
		32
	},
	{PgAuditFgaConfRelationId,		/* AUDITFGAOBJPOLICYCONF */
		AuditFgaConfObjschOjbPolicyNameIndexID,
		3,
		{
			Anum_audit_fga_conf_object_schema,
			Anum_audit_fga_conf_object_id,
			Anum_audit_fga_conf_policy_name,
			0
		},
		64
	},
#endif
	{AuthMemRelationId,			/* AUTHMEMMEMROLE */
		AuthMemMemRoleIndexId,
		2,
		{
			Anum_pg_auth_members_member,
			Anum_pg_auth_members_roleid,
			0,
			0
		},
		8
	},
	{AuthMemRelationId,			/* AUTHMEMROLEMEM */
		AuthMemRoleMemIndexId,
		2,
		{
			Anum_pg_auth_members_roleid,
			Anum_pg_auth_members_member,
			0,
			0
		},
		8
	},
	{AuthIdRelationId,			/* AUTHNAME */
		AuthIdRolnameIndexId,
		1,
		{
			Anum_pg_authid_rolname,
			0,
			0,
			0
		},
		8
	},
	{AuthIdRelationId,			/* AUTHOID */
		AuthIdOidIndexId,
		1,
		{
			ObjectIdAttributeNumber,
			0,
			0,
			0
		},
		8
	},
	{DirectoryId,		/* BFILEDIRECTORY */
		PgDirectoryIndex,
		1,
		{
			Anum_directory_name,
			0,
			0,
			0
		},
		128
	},
	{
		CastRelationId,			/* CASTSOURCETARGET */
		CastSourceTargetIndexId,
		2,
		{
			Anum_pg_cast_castsource,
			Anum_pg_cast_casttarget,
			0,
			0
		},
		256
	},
	{OperatorClassRelationId,	/* CLAAMNAMENSP */
		OpclassAmNameNspIndexId,
		3,
		{
			Anum_pg_opclass_opcmethod,
			Anum_pg_opclass_opcname,
			Anum_pg_opclass_opcnamespace,
			0
		},
		8
	},
	{OperatorClassRelationId,	/* CLAOID */
		OpclassOidIndexId,
		1,
		{
			ObjectIdAttributeNumber,
			0,
			0,
			0
		},
		8
	},
#ifdef _MLS_
	{ClsLabelRelationId,	/* CLSLABELOID */
		PgClsLabelPolidLabelidIndexId,
		2,
		{
			Anum_pg_cls_label_polid,
			Anum_pg_cls_label_labelid,
			0,
			0
		},
		128
	},
#endif
	{TypeObjectRelationId,				/* TYPEOBJNMNSSPEC */
		PgTypeObjectNameNsSpecIndexId,
		3,
		{
			Anum_pg_type_object_name,
			Anum_pg_type_object_namespace,
			Anum_pg_type_object_isspec,
			0
		},
		32
	},
	{TypeObjectRelationId,				/* TYPEOBJOID */
		PgTypeOidIndexId,
		1,
		{
			ObjectIdAttributeNumber,
			0,
			0,
			0
		},
		32
	},
	{TypeObjectRelationId,				/* TYPECREATENSOID */
		PgTypeCreateNsOidSpecIndexId,
		2,
		{
			Anum_pg_type_object_creatensoid,
			Anum_pg_type_object_isspec,
			0,
			0
		},
		32
	},
	{CollationRelationId,		/* COLLNAMEENCNSP */
		CollationNameEncNspIndexId,
		3,
		{
			Anum_pg_collation_collname,
			Anum_pg_collation_collencoding,
			Anum_pg_collation_collnamespace,
			0
		},
		8
	},
	{CollationRelationId,		/* COLLOID */
		CollationOidIndexId,
		1,
		{
			ObjectIdAttributeNumber,
			0,
			0,
			0
		},
		8
	},
	{ConversionRelationId,		/* CONDEFAULT */
		ConversionDefaultIndexId,
		4,
		{
			Anum_pg_conversion_connamespace,
			Anum_pg_conversion_conforencoding,
			Anum_pg_conversion_contoencoding,
			ObjectIdAttributeNumber,
		},
		8
	},
	{ConversionRelationId,		/* CONNAMENSP */
		ConversionNameNspIndexId,
		2,
		{
			Anum_pg_conversion_conname,
			Anum_pg_conversion_connamespace,
			0,
			0
		},
		8
	},
	{ConstraintRelationId,		/* CONSTROID */
		ConstraintOidIndexId,
		1,
		{
			ObjectIdAttributeNumber,
			0,
			0,
			0
		},
		16
	},
	{ConversionRelationId,		/* CONVOID */
		ConversionOidIndexId,
		1,
		{
			ObjectIdAttributeNumber,
			0,
			0,
			0
		},
		8
	},
	{DatabaseRelationId,		/* DATABASEOID */
		DatabaseOidIndexId,
		1,
		{
			ObjectIdAttributeNumber,
			0,
			0,
			0
		},
		4
	},
#ifdef _MLS_	
	{DataMaskMapRelationId,		/* DATAMASKOID */
		PgDataMaskMapIndexId,
		2,
		{
			Anum_pg_data_mask_map_relid,
			Anum_pg_data_mask_map_attnum,
			0,
			0
		},
		128
	},
	{DataMaskUserRelationId,		/* DATAMASKUSEROID */
		PgDataMaskUserIndexId,
		3,
		{
			Anum_pg_data_mask_user_relid,
			Anum_pg_data_mask_user_userid,
			Anum_pg_data_mask_user_attnum,
			0
		},
		128
	},
#endif	
	{DefaultAclRelationId,		/* DEFACLROLENSPOBJ */
		DefaultAclRoleNspObjIndexId,
		3,
		{
			Anum_pg_default_acl_defaclrole,
			Anum_pg_default_acl_defaclnamespace,
			Anum_pg_default_acl_defaclobjtype,
			0
		},
		8
	},
	{EnumRelationId,			/* ENUMOID */
		EnumOidIndexId,
		1,
		{
			ObjectIdAttributeNumber,
			0,
			0,
			0
		},
		8
	},
	{EnumRelationId,			/* ENUMTYPOIDNAME */
		EnumTypIdLabelIndexId,
		2,
		{
			Anum_pg_enum_enumtypid,
			Anum_pg_enum_enumlabel,
			0,
			0
		},
		8
	},
	{EventTriggerRelationId,	/* EVENTTRIGGERNAME */
		EventTriggerNameIndexId,
		1,
		{
			Anum_pg_event_trigger_evtname,
			0,
			0,
			0
		},
		8
	},
	{EventTriggerRelationId,	/* EVENTTRIGGEROID */
		EventTriggerOidIndexId,
		1,
		{
			ObjectIdAttributeNumber,
			0,
			0,
			0
		},
		8
	},
	{ForeignDataWrapperRelationId,	/* FOREIGNDATAWRAPPERNAME */
		ForeignDataWrapperNameIndexId,
		1,
		{
			Anum_pg_foreign_data_wrapper_fdwname,
			0,
			0,
			0
		},
		2
	},
	{ForeignDataWrapperRelationId,	/* FOREIGNDATAWRAPPEROID */
		ForeignDataWrapperOidIndexId,
		1,
		{
			ObjectIdAttributeNumber,
			0,
			0,
			0
		},
		2
	},
	{ForeignServerRelationId,	/* FOREIGNSERVERNAME */
		ForeignServerNameIndexId,
		1,
		{
			Anum_pg_foreign_server_srvname,
			0,
			0,
			0
		},
		2
	},
	{ForeignServerRelationId,	/* FOREIGNSERVEROID */
		ForeignServerOidIndexId,
		1,
		{
			ObjectIdAttributeNumber,
			0,
			0,
			0
		},
		2
	},
	{ForeignTableRelationId,	/* FOREIGNTABLEREL */
		ForeignTableRelidIndexId,
		1,
		{
			Anum_pg_foreign_table_ftrelid,
			0,
			0,
			0
		},
		4
	},
	{IndexRelationId,			/* INDEXRELID */
		IndexRelidIndexId,
		1,
		{
			Anum_pg_index_indexrelid,
			0,
			0,
			0
		},
		64
	},
	{LanguageRelationId,		/* LANGNAME */
		LanguageNameIndexId,
		1,
		{
			Anum_pg_language_lanname,
			0,
			0,
			0
		},
		4
	},
	{LanguageRelationId,		/* LANGOID */
		LanguageOidIndexId,
		1,
		{
			ObjectIdAttributeNumber,
			0,
			0,
			0
		},
		4
	},
	{NamespaceRelationId,		/* NAMESPACENAME */
		NamespaceNameIndexId,
		1,
		{
			Anum_pg_namespace_nspname,
			0,
			0,
			0
		},
		4
	},
	{NamespaceRelationId,		/* NAMESPACEOID */
		NamespaceOidIndexId,
		1,
		{
			ObjectIdAttributeNumber,
			0,
			0,
			0
		},
		16
	},
	{OperatorRelationId,		/* OPERNAMENSP */
		OperatorNameNspIndexId,
		4,
		{
			Anum_pg_operator_oprname,
			Anum_pg_operator_oprleft,
			Anum_pg_operator_oprright,
			Anum_pg_operator_oprnamespace
		},
		256
	},
	{OperatorRelationId,		/* OPEROID */
		OperatorOidIndexId,
		1,
		{
			ObjectIdAttributeNumber,
			0,
			0,
			0
		},
		32
	},
	{OperatorFamilyRelationId,	/* OPFAMILYAMNAMENSP */
		OpfamilyAmNameNspIndexId,
		3,
		{
			Anum_pg_opfamily_opfmethod,
			Anum_pg_opfamily_opfname,
			Anum_pg_opfamily_opfnamespace,
			0
		},
		8
	},
	{OperatorFamilyRelationId,	/* OPFAMILYOID */
		OpfamilyOidIndexId,
		1,
		{
			ObjectIdAttributeNumber,
			0,
			0,
			0
		},
		8
	},
	{PgDbmsJobRelationId,  /* PGDBMSJOBID */
		PgDbmsJobIdIndexId,
		1,
		{
			Anum_pg_dbms_job_job_id,
			0,
			0,
			0
		},
		2048
	},
#ifdef _PG_ORCL_
	{PackageRelationId,	/* PGPACKAGENMNS */
		PgPackageNameNsIndexId,
		2,
		{
			Anum_pg_pkg_name,
			Anum_pg_pkg_namespace,
			0,
			0
		},
		4
	},
	{PackageRelationId,	/* PGPKGCRRNS */
		PgPackageCorrespondNsIndexId,
		1,
		{
			Anum_pg_pkg_crrspndingns,
			0,
			0,
			0
		},
		4
	},
    {PackageRelationId,	/* PGPKGOID */
     PgPackageOidIndexId,
     1,
     {
             ObjectIdAttributeNumber,
             0,
             0,
             0
     },
     4
    },
	{SynonymRelationId,	/* SYNONYMNAME */
		SynonymNameIndexId,
		2,
		{
			Anum_pg_synonym_synname,
			Anum_pg_synonym_synnamespace,
			0,
			0
		},
		64
	},
	{SynonymRelationId,	/* SYNONYMOID */
		SynonymOidIndexId,
		1,
		{
			ObjectIdAttributeNumber,
			0,
			0,
			0
		},
		64
	},
	{CompileRelationId,	/* COMPILEOBJNAME */
		CompileObjNameIndexId,
		2,
		{
			Natts_pg_compile_compclassid,
			Natts_pg_compile_compobjid,
			0,
			0
		},
		64
	},
    {ChildAliasRelationId,	/* CHIILDASOID */
     PgChildAliasOidIndexId,
     1,
     {
             ObjectIdAttributeNumber,
             0,
             0,
             0
     },
     4
    },
    {ChildAliasRelationId,	/* CHIILDASNAME */
     PgChildAliasNameIndexId,
     2,
     {
             Anum_pg_childas_poid,
             Anum_pg_childas_aname,
             0,
             0
     },
     8
    },
#endif
	{SPMPlanRelationId,	/* SPMPLANOID */
		SPMPlanOidIndexId,
		1,
		{
			ObjectIdAttributeNumber,
			0,
			0,
			0
		},
		1024
	},
	{SPMPlanRelationId,	/* SPMPLANSQLID */
		SPMPlanSqlidPlanidIndexId,
		2,
		{
			Anum_pg_spm_plan_sql_id,
			Anum_pg_spm_plan_plan_id,
			0,
			0
		},
		1024
	},
#ifdef PGXC
	{PgxcClassRelationId,	/* PGXCCLASSRELID */
		PgxcClassPgxcRelIdIndexId,
		1,
		{
			Anum_pgxc_class_pcrelid,
			0,
			0,
			0
		},
		1024
	},
	{PgxcGroupRelationId,	/* PGXCGROUPNAME */
		PgxcGroupGroupNameIndexId,
		1,
		{
			Anum_pgxc_group_name,
			0,
			0,
			0
		},
		256
	},
	{PgxcGroupRelationId,	/* PGXCGROUPOID */
		PgxcGroupOidIndexId,
		1,
		{
			ObjectIdAttributeNumber,
			0,
			0,
			0
		},
		256
	},
	{PgxcNodeRelationId,	/* PGXCNODENAME */
		PgxcNodeNodeNameIndexId,
		2,
		{
			Anum_pgxc_node_name,
			Anum_pgxc_node_plane_name,
			0,
			0
		},
		256
	},
	{PgxcNodeRelationId,	/* PGXCNODEOID */
		PgxcNodeOidIndexId,
		1,
		{
			ObjectIdAttributeNumber,
			0,
			0,
			0
		},
		256
	},
	{PgxcNodeRelationId,	/* PGXCNODEIDENTIFIER */
		PgxcNodeNodeIdIndexId,
		1,
		{
			Anum_pgxc_node_id,
			0,
			0,
			0
		},
		256
	},			
#endif
	{PartitionedRelationId,		/* PARTRELID */
		PartitionedRelidIndexId,
		1,
		{
			Anum_pg_partitioned_table_partrelid,
			0,
			0,
			0
		},
		32
	},
	{ProcedureRelationId,		/* PROCNAMEARGSNSP */
		ProcedureNameArgsNspIndexId,
		3,
		{
			Anum_pg_proc_proname,
			Anum_pg_proc_proargtypes,
			Anum_pg_proc_pronamespace,
			0
		},
		128
	},
	{ProcedureRelationId,		/* PROCOID */
		ProcedureOidIndexId,
		1,
		{
			ObjectIdAttributeNumber,
			0,
			0,
			0
		},
		128
	},
	{PublicationRelationId,		/* PUBLICATIONNAME */
		PublicationNameIndexId,
		1,
		{
			Anum_pg_publication_pubname,
			0,
			0,
			0
		},
		8
	},
	{PublicationRelationId,		/* PUBLICATIONOID */
		PublicationObjectIndexId,
		1,
		{
			ObjectIdAttributeNumber,
			0,
			0,
			0
		},
		8
	},
	{PublicationRelRelationId,	/* PUBLICATIONREL */
		PublicationRelObjectIndexId,
		1,
		{
			ObjectIdAttributeNumber,
			0,
			0,
			0
		},
		64
	},
	{PublicationRelRelationId,	/* PUBLICATIONRELMAP */
		PublicationRelPrrelidPrpubidIndexId,
		2,
		{
			Anum_pg_publication_rel_prrelid,
			Anum_pg_publication_rel_prpubid,
			0,
			0
		},
		64
	},
#ifdef __STORAGE_SCALABLE__
	{PublicationShardRelationId,	/* PUBLICATIONSHARD */
		PublicationShardObjectIndexId,
		1,
		{
			ObjectIdAttributeNumber,
			0,
			0,
			0
		},
		64
	},
	{PublicationShardRelationId,	/* PUBLICATIONRELMAP */
		PublicationShardPrshardidPrpubidIndexId,
		2,
		{
			Anum_pg_publication_shard_prshardid,
			Anum_pg_publication_shard_prpubid,
			0,
			0
		},
		64
	},
#endif
	{RangeRelationId,			/* RANGETYPE */
		RangeTypidIndexId,
		1,
		{
			Anum_pg_range_rngtypid,
			0,
			0,
			0
		},
		4
	},
	{RelationRelationId,		/* RELNAMENSP */
		ClassNameNspIndexId,
		2,
		{
			Anum_pg_class_relname,
			Anum_pg_class_relnamespace,
			0,
			0
		},
		128
	},
	{RelationRelationId,		/* RELOID */
		ClassOidIndexId,
		1,
		{
			ObjectIdAttributeNumber,
			0,
			0,
			0
		},
		128
	},
	{ReplicationOriginRelationId,	/* REPLORIGIDENT */
		ReplicationOriginIdentIndex,
		1,
		{
			Anum_pg_replication_origin_roident,
			0,
			0,
			0
		},
		16
	},
	{ReplicationOriginRelationId,	/* REPLORIGNAME */
		ReplicationOriginNameIndex,
		1,
		{
			Anum_pg_replication_origin_roname,
			0,
			0,
			0
		},
		16
	},
	{ResGroupRelationId,		/* RESGROUPOID */
		ResGroupOidIndexId,
		1,
		{
			ObjectIdAttributeNumber,
			0,
			0,
			0
		},
		128
	},
	{ResGroupRelationId,		/* RESGROUPNAME */
		ResGroupRsgnameIndexId,
		1,
		{
			Anum_pg_resgroup_rsgname,
			0,
			0,
			0
		},
		128
	},
	{RewriteRelationId,			/* RULERELNAME */
		RewriteRelRulenameIndexId,
		2,
		{
			Anum_pg_rewrite_ev_class,
			Anum_pg_rewrite_rulename,
			0,
			0
		},
		8
	},
	{SequenceRelationId,		/* SEQRELID */
		SequenceRelidIndexId,
		1,
		{
			Anum_pg_sequence_seqrelid,
			0,
			0,
			0
		},
		32
	},
	{StatisticExtRelationId,	/* STATEXTNAMENSP */
		StatisticExtNameIndexId,
		2,
		{
			Anum_pg_statistic_ext_stxname,
			Anum_pg_statistic_ext_stxnamespace,
			0,
			0
		},
		4
	},
	{StatisticExtRelationId,	/* STATEXTOID */
		StatisticExtOidIndexId,
		1,
		{
			ObjectIdAttributeNumber,
			0,
			0,
			0
		},
		4
	},
	{StatisticRelationId,		/* STATRELATTINH */
		StatisticRelidAttnumInhIndexId,
		3,
		{
			Anum_pg_statistic_starelid,
			Anum_pg_statistic_staattnum,
			Anum_pg_statistic_stainherit,
			0
		},
		128
	},
	{SubscriptionRelationId,	/* SUBSCRIPTIONNAME */
		SubscriptionNameIndexId,
		2,
		{
			Anum_pg_subscription_subdbid,
			Anum_pg_subscription_subname,
			0,
			0
		},
		4
	},
	{SubscriptionRelationId,	/* SUBSCRIPTIONOID */
		SubscriptionObjectIndexId,
		1,
		{
			ObjectIdAttributeNumber,
			0,
			0,
			0
		},
		4
	},
	{SubscriptionRelRelationId, /* SUBSCRIPTIONRELMAP */
		SubscriptionRelSrrelidSrsubidIndexId,
		2,
		{
			Anum_pg_subscription_rel_srrelid,
			Anum_pg_subscription_rel_srsubid,
			0,
			0
		},
		64
	},
#ifdef __STORAGE_SCALABLE__
	{SubscriptionShardRelationId,	/* SUBSCRIPTIONSHARD */
		SubscriptionShardObjectIndexId,
		1,
		{
			ObjectIdAttributeNumber,
			0,
			0,
			0
		},
		64
	},
	{SubscriptionShardRelationId, /* SUBSCRIPTIONSHARDMAP */
		SubscriptionShardSrshardidSrsubidSrpnameIndexId,
		3,
		{
			Anum_pg_subscription_shard_srsubid,
			Anum_pg_subscription_shard_srshardid,
			Anum_pg_subscription_shard_pubname,
			0
		},
		64
	},
	{SubscriptionShardRelationId, /* SUBSCRIPTIONSHARDPUBNAME */
		SubscriptionShardSrsubidSrpnameIndexId,
		2,
		{
			Anum_pg_subscription_shard_srsubid,
			Anum_pg_subscription_shard_pubname,
			0,
			0
		},
		64
	},
	{SubscriptionTableRelationId, /* SUBSCRIPTIONTABLE */
		SubscriptionTableSrsubidSrrelidIndexId,
		2,
		{
			Anum_pg_subscription_table_srsubid,
			Anum_pg_subscription_table_srrelid,
			0,
			0
		},
		64
	},
	{SubscriptionTableRelationId, /* SUBSCRIPTIONTABLEMAP */
		SubscriptionTableSrsubidSrrelidSrpnameIndexId,
		3,
		{
			Anum_pg_subscription_table_srsubid,
			Anum_pg_subscription_table_srrelid,
			Anum_pg_subscription_table_pubname,
			0
		},
		64
	},
	{SubscriptionTableRelationId, /* SUBSCRIPTIONTABLEOID */
		SubscriptionTableObjectIndexId,
		1,
		{
			ObjectIdAttributeNumber,
			0,
			0,
			0
		},
		64
	},
#endif
	{TableSpaceRelationId,		/* TABLESPACEOID */
		TablespaceOidIndexId,
		1,
		{
			ObjectIdAttributeNumber,
			0,
			0,
			0,
		},
		4
	},
	{TermRelationId,			/* TERMID */
		TermIdIndexId,
		1,
		{
			Anum_pg_term_id,
			0,
			0,
			0
		},
		8
	},
	{TransformRelationId,		/* TRFOID */
		TransformOidIndexId,
		1,
		{
			ObjectIdAttributeNumber,
			0,
			0,
			0,
		},
		16
	},
	{TransformRelationId,		/* TRFTYPELANG */
		TransformTypeLangIndexId,
		2,
		{
			Anum_pg_transform_trftype,
			Anum_pg_transform_trflang,
			0,
			0,
		},
		16
	},
	{TSConfigMapRelationId,		/* TSCONFIGMAP */
		TSConfigMapIndexId,
		3,
		{
			Anum_pg_ts_config_map_mapcfg,
			Anum_pg_ts_config_map_maptokentype,
			Anum_pg_ts_config_map_mapseqno,
			0
		},
		2
	},
	{TSConfigRelationId,		/* TSCONFIGNAMENSP */
		TSConfigNameNspIndexId,
		2,
		{
			Anum_pg_ts_config_cfgname,
			Anum_pg_ts_config_cfgnamespace,
			0,
			0
		},
		2
	},
	{TSConfigRelationId,		/* TSCONFIGOID */
		TSConfigOidIndexId,
		1,
		{
			ObjectIdAttributeNumber,
			0,
			0,
			0
		},
		2
	},
	{TSDictionaryRelationId,	/* TSDICTNAMENSP */
		TSDictionaryNameNspIndexId,
		2,
		{
			Anum_pg_ts_dict_dictname,
			Anum_pg_ts_dict_dictnamespace,
			0,
			0
		},
		2
	},
	{TSDictionaryRelationId,	/* TSDICTOID */
		TSDictionaryOidIndexId,
		1,
		{
			ObjectIdAttributeNumber,
			0,
			0,
			0
		},
		2
	},
	{TSParserRelationId,		/* TSPARSERNAMENSP */
		TSParserNameNspIndexId,
		2,
		{
			Anum_pg_ts_parser_prsname,
			Anum_pg_ts_parser_prsnamespace,
			0,
			0
		},
		2
	},
	{TSParserRelationId,		/* TSPARSEROID */
		TSParserOidIndexId,
		1,
		{
			ObjectIdAttributeNumber,
			0,
			0,
			0
		},
		2
	},
	{TSTemplateRelationId,		/* TSTEMPLATENAMENSP */
		TSTemplateNameNspIndexId,
		2,
		{
			Anum_pg_ts_template_tmplname,
			Anum_pg_ts_template_tmplnamespace,
			0,
			0
		},
		2
	},
	{TSTemplateRelationId,		/* TSTEMPLATEOID */
		TSTemplateOidIndexId,
		1,
		{
			ObjectIdAttributeNumber,
			0,
			0,
			0
		},
		2
	},
	{TypeRelationId,			/* TYPENAMENSP */
		TypeNameNspIndexId,
		2,
		{
			Anum_pg_type_typname,
			Anum_pg_type_typnamespace,
			0,
			0
		},
		64
	},
	{TypeRelationId,			/* TYPEOID */
		TypeOidIndexId,
		1,
		{
			ObjectIdAttributeNumber,
			0,
			0,
			0
		},
		64
	},
	{UserMappingRelationId,		/* USERMAPPINGOID */
		UserMappingOidIndexId,
		1,
		{
			ObjectIdAttributeNumber,
			0,
			0,
			0
		},
		2
	},
	{UserMappingRelationId,		/* USERMAPPINGUSERSERVER */
		UserMappingUserServerIndexId,
		2,
		{
			Anum_pg_user_mapping_umuser,
			Anum_pg_user_mapping_umserver,
			0,
			0
		},
		2
	},
	{OraSequenceRelationId,		/* ORASEQRELID */
		OraSequenceRelidIndexId,
		1,
		{
			Anum_pg_ora_sequence_seqrelid,
			0,
			0,
			0
		},
		32
	}
};

static CatCache *SysCache[SysCacheSize];

static bool CacheInitialized = false;

/* Sorted array of OIDs of tables that have caches on them */
static Oid	SysCacheRelationOid[SysCacheSize];
static int	SysCacheRelationOidSize;

/* Sorted array of OIDs of tables and indexes used by caches */
static Oid	SysCacheSupportingRelOid[SysCacheSize * 2];
static int	SysCacheSupportingRelOidSize;

static int	oid_compare(const void *a, const void *b);


/*
 * InitCatalogCache - initialize the caches
 *
 * Note that no database access is done here; we only allocate memory
 * and initialize the cache structure.  Interrogation of the database
 * to complete initialization of a cache happens upon first use
 * of that cache.
 */
void
InitCatalogCache(void)
{
	int			cacheId;
	int			i,
				j;

	StaticAssertStmt(SysCacheSize == (int) lengthof(cacheinfo),
					 "SysCacheSize does not match syscache.c's array");

	Assert(!CacheInitialized);

	SysCacheRelationOidSize = SysCacheSupportingRelOidSize = 0;

	for (cacheId = 0; cacheId < SysCacheSize; cacheId++)
	{
		SysCache[cacheId] = InitCatCache(cacheId,
										 cacheinfo[cacheId].reloid,
										 cacheinfo[cacheId].indoid,
										 cacheinfo[cacheId].nkeys,
										 cacheinfo[cacheId].key,
										 cacheinfo[cacheId].nbuckets);
		if (!PointerIsValid(SysCache[cacheId]))
			elog(ERROR, "could not initialize cache %u (%d)",
				 cacheinfo[cacheId].reloid, cacheId);
		/* Accumulate data for OID lists, too */
		SysCacheRelationOid[SysCacheRelationOidSize++] =
			cacheinfo[cacheId].reloid;
		SysCacheSupportingRelOid[SysCacheSupportingRelOidSize++] =
			cacheinfo[cacheId].reloid;
		SysCacheSupportingRelOid[SysCacheSupportingRelOidSize++] =
			cacheinfo[cacheId].indoid;
		/* see comments for RelationInvalidatesSnapshotsOnly */
		Assert(!RelationInvalidatesSnapshotsOnly(cacheinfo[cacheId].reloid));
	}

	Assert(SysCacheRelationOidSize <= lengthof(SysCacheRelationOid));
	Assert(SysCacheSupportingRelOidSize <= lengthof(SysCacheSupportingRelOid));

	/* Sort and de-dup OID arrays, so we can use binary search. */
	pg_qsort(SysCacheRelationOid, SysCacheRelationOidSize,
			 sizeof(Oid), oid_compare);
	for (i = 1, j = 0; i < SysCacheRelationOidSize; i++)
	{
		if (SysCacheRelationOid[i] != SysCacheRelationOid[j])
			SysCacheRelationOid[++j] = SysCacheRelationOid[i];
	}
	SysCacheRelationOidSize = j + 1;

	pg_qsort(SysCacheSupportingRelOid, SysCacheSupportingRelOidSize,
			 sizeof(Oid), oid_compare);
	for (i = 1, j = 0; i < SysCacheSupportingRelOidSize; i++)
	{
		if (SysCacheSupportingRelOid[i] != SysCacheSupportingRelOid[j])
			SysCacheSupportingRelOid[++j] = SysCacheSupportingRelOid[i];
	}
	SysCacheSupportingRelOidSize = j + 1;

	CacheInitialized = true;
}

/*
 * InitCatalogCachePhase2 - finish initializing the caches
 *
 * Finish initializing all the caches, including necessary database
 * access.
 *
 * This is *not* essential; normally we allow syscaches to be initialized
 * on first use.  However, it is useful as a mechanism to preload the
 * relcache with entries for the most-commonly-used system catalogs.
 * Therefore, we invoke this routine when we need to write a new relcache
 * init file.
 */
void
InitCatalogCachePhase2(void)
{
	int			cacheId;

	Assert(CacheInitialized);

	for (cacheId = 0; cacheId < SysCacheSize; cacheId++)
		InitCatCachePhase2(SysCache[cacheId], true);
}


/*
 * SearchSysCache
 *
 *	A layer on top of SearchCatCache that does the initialization and
 *	key-setting for you.
 *
 *	Returns the cache copy of the tuple if one is found, NULL if not.
 *	The tuple is the 'cache' copy and must NOT be modified!
 *
 *	When the caller is done using the tuple, call ReleaseSysCache()
 *	to release the reference count grabbed by SearchSysCache().  If this
 *	is not done, the tuple will remain locked in cache until end of
 *	transaction, which is tolerable but not desirable.
 *
 *	CAUTION: The tuple that is returned must NOT be freed by the caller!
 */
HeapTuple
SearchSysCache(int cacheId,
			   Datum key1,
			   Datum key2,
			   Datum key3,
			   Datum key4)
{
	Assert(cacheId >= 0 && cacheId < SysCacheSize &&
		   PointerIsValid(SysCache[cacheId]));

	return SearchCatCache(SysCache[cacheId], key1, key2, key3, key4);
}

HeapTuple
SearchSysCache1(int cacheId,
				Datum key1)
{
	Assert(cacheId >= 0 && cacheId < SysCacheSize &&
		   PointerIsValid(SysCache[cacheId]));
	Assert(SysCache[cacheId]->cc_nkeys == 1);

	return SearchCatCache1(SysCache[cacheId], key1);
}

HeapTuple
SearchSysCache1OnlyCache(int cacheId,
				Datum key1)
{
	Assert(cacheId >= 0 && cacheId < SysCacheSize &&
		   PointerIsValid(SysCache[cacheId]));
	Assert(SysCache[cacheId]->cc_nkeys == 1);

	return SearchCatCache1OnlyCache(SysCache[cacheId], key1);
}

HeapTuple
SearchSysCache2(int cacheId,
				Datum key1, Datum key2)
{
	Assert(cacheId >= 0 && cacheId < SysCacheSize &&
		   PointerIsValid(SysCache[cacheId]));
	Assert(SysCache[cacheId]->cc_nkeys == 2);

	return SearchCatCache2(SysCache[cacheId], key1, key2);
}

HeapTuple
SearchSysCache3(int cacheId,
				Datum key1, Datum key2, Datum key3)
{
	Assert(cacheId >= 0 && cacheId < SysCacheSize &&
		   PointerIsValid(SysCache[cacheId]));
	Assert(SysCache[cacheId]->cc_nkeys == 3);

	return SearchCatCache3(SysCache[cacheId], key1, key2, key3);
}

HeapTuple
SearchSysCache4(int cacheId,
				Datum key1, Datum key2, Datum key3, Datum key4)
{
	Assert(cacheId >= 0 && cacheId < SysCacheSize &&
		   PointerIsValid(SysCache[cacheId]));
	Assert(SysCache[cacheId]->cc_nkeys == 4);

	return SearchCatCache4(SysCache[cacheId], key1, key2, key3, key4);
}

/*
 * ReleaseSysCache
 *		Release previously grabbed reference count on a tuple
 */
void
ReleaseSysCache(HeapTuple tuple)
{
	ReleaseCatCache(tuple);
}

/*
 * SearchSysCacheCopy
 *
 * A convenience routine that does SearchSysCache and (if successful)
 * returns a modifiable copy of the syscache entry.  The original
 * syscache entry is released before returning.  The caller should
 * heap_freetuple() the result when done with it.
 */
HeapTuple
SearchSysCacheCopy(int cacheId,
				   Datum key1,
				   Datum key2,
				   Datum key3,
				   Datum key4)
{
	HeapTuple	tuple,
				newtuple;

	tuple = SearchSysCache(cacheId, key1, key2, key3, key4);
	if (!HeapTupleIsValid(tuple))
		return tuple;
	newtuple = heap_copytuple(tuple);
	ReleaseSysCache(tuple);
	return newtuple;
}

/*
 * SearchSysCacheExists
 *
 * A convenience routine that just probes to see if a tuple can be found.
 * No lock is retained on the syscache entry.
 */
bool
SearchSysCacheExists(int cacheId,
					 Datum key1,
					 Datum key2,
					 Datum key3,
					 Datum key4)
{
	HeapTuple	tuple;

	tuple = SearchSysCache(cacheId, key1, key2, key3, key4);
	if (!HeapTupleIsValid(tuple))
		return false;
	ReleaseSysCache(tuple);
	return true;
}

/*
 * GetSysCacheOid
 *
 * A convenience routine that does SearchSysCache and returns the OID
 * of the found tuple, or InvalidOid if no tuple could be found.
 * No lock is retained on the syscache entry.
 */
Oid
GetSysCacheOid(int cacheId,
			   Datum key1,
			   Datum key2,
			   Datum key3,
			   Datum key4)
{
	HeapTuple	tuple;
	Oid			result;

	tuple = SearchSysCache(cacheId, key1, key2, key3, key4);
	if (!HeapTupleIsValid(tuple))
		return InvalidOid;
	result = HeapTupleGetOid(tuple);
	ReleaseSysCache(tuple);
	return result;
}


/*
 * SearchSysCacheAttName
 *
 * This routine is equivalent to SearchSysCache on the ATTNAME cache,
 * except that it will return NULL if the found attribute is marked
 * attisdropped.  This is convenient for callers that want to act as
 * though dropped attributes don't exist.
 */
HeapTuple
SearchSysCacheAttName(Oid relid, const char *attname)
{
	HeapTuple	tuple;

	tuple = SearchSysCache2(ATTNAME,
							ObjectIdGetDatum(relid),
							CStringGetDatum(attname));
	if (!HeapTupleIsValid(tuple))
		return NULL;
	if (((Form_pg_attribute) GETSTRUCT(tuple))->attisdropped)
	{
		ReleaseSysCache(tuple);
		return NULL;
	}
	return tuple;
}

/*
 * SearchSysCacheCopyAttName
 *
 * As above, an attisdropped-aware version of SearchSysCacheCopy.
 */
HeapTuple
SearchSysCacheCopyAttName(Oid relid, const char *attname)
{
	HeapTuple	tuple,
				newtuple;

	tuple = SearchSysCacheAttName(relid, attname);
	if (!HeapTupleIsValid(tuple))
		return tuple;
	newtuple = heap_copytuple(tuple);
	ReleaseSysCache(tuple);
	return newtuple;
}

/*
 * SearchSysCacheExistsAttName
 *
 * As above, an attisdropped-aware version of SearchSysCacheExists.
 */
bool
SearchSysCacheExistsAttName(Oid relid, const char *attname)
{
	HeapTuple	tuple;

	tuple = SearchSysCacheAttName(relid, attname);
	if (!HeapTupleIsValid(tuple))
		return false;
	ReleaseSysCache(tuple);
	return true;
}


/*
 * SysCacheGetAttr
 *
 *		Given a tuple previously fetched by SearchSysCache(),
 *		extract a specific attribute.
 *
 * This is equivalent to using heap_getattr() on a tuple fetched
 * from a non-cached relation.  Usually, this is only used for attributes
 * that could be NULL or variable length; the fixed-size attributes in
 * a system table are accessed just by mapping the tuple onto the C struct
 * declarations from include/catalog/.
 *
 * As with heap_getattr(), if the attribute is of a pass-by-reference type
 * then a pointer into the tuple data area is returned --- the caller must
 * not modify or pfree the datum!
 *
 * Note: it is legal to use SysCacheGetAttr() with a cacheId referencing
 * a different cache for the same catalog the tuple was fetched from.
 */
Datum
SysCacheGetAttr(int cacheId, HeapTuple tup,
				AttrNumber attributeNumber,
				bool *isNull)
{
	/*
	 * We just need to get the TupleDesc out of the cache entry, and then we
	 * can apply heap_getattr().  Normally the cache control data is already
	 * valid (because the caller recently fetched the tuple via this same
	 * cache), but there are cases where we have to initialize the cache here.
	 */
	if (cacheId < 0 || cacheId >= SysCacheSize ||
		!PointerIsValid(SysCache[cacheId]))
		elog(ERROR, "invalid cache ID: %d", cacheId);
	if (!PointerIsValid(SysCache[cacheId]->cc_tupdesc))
	{
		InitCatCachePhase2(SysCache[cacheId], false);
		Assert(PointerIsValid(SysCache[cacheId]->cc_tupdesc));
	}

	return heap_getattr(tup, attributeNumber,
						SysCache[cacheId]->cc_tupdesc,
						isNull);
}

/*
 * GetSysCacheHashValue
 *
 * Get the hash value that would be used for a tuple in the specified cache
 * with the given search keys.
 *
 * The reason for exposing this as part of the API is that the hash value is
 * exposed in cache invalidation operations, so there are places outside the
 * catcache code that need to be able to compute the hash values.
 */
uint32
GetSysCacheHashValue(int cacheId,
					 Datum key1,
					 Datum key2,
					 Datum key3,
					 Datum key4)
{
	if (cacheId < 0 || cacheId >= SysCacheSize ||
		!PointerIsValid(SysCache[cacheId]))
		elog(ERROR, "invalid cache ID: %d", cacheId);

	return GetCatCacheHashValue(SysCache[cacheId], key1, key2, key3, key4);
}

/*
 * List-search interface
 */
struct catclist *
SearchSysCacheList(int cacheId, int nkeys,
				   Datum key1, Datum key2, Datum key3)
{
	if (cacheId < 0 || cacheId >= SysCacheSize ||
		!PointerIsValid(SysCache[cacheId]))
		elog(ERROR, "invalid cache ID: %d", cacheId);

	return SearchCatCacheList(SysCache[cacheId], nkeys,
							  key1, key2, key3);
}

/*
 * SysCacheInvalidate
 *
 *	Invalidate entries in the specified cache, given a hash value.
 *	See CatCacheInvalidate() for more info.
 *
 *	This routine is only quasi-public: it should only be used by inval.c.
 */
void
SysCacheInvalidate(int cacheId, uint32 hashValue)
{
	if (cacheId < 0 || cacheId >= SysCacheSize)
		elog(ERROR, "invalid cache ID: %d", cacheId);

	/* if this cache isn't initialized yet, no need to do anything */
	if (!PointerIsValid(SysCache[cacheId]))
		return;

	CatCacheInvalidate(SysCache[cacheId], hashValue);
}

/*
 * Certain relations that do not have system caches send snapshot invalidation
 * messages in lieu of catcache messages.  This is for the benefit of
 * GetCatalogSnapshot(), which can then reuse its existing MVCC snapshot
 * for scanning one of those catalogs, rather than taking a new one, if no
 * invalidation has been received.
 *
 * Relations that have syscaches need not (and must not) be listed here.  The
 * catcache invalidation messages will also flush the snapshot.  If you add a
 * syscache for one of these relations, remove it from this list.
 */
bool
RelationInvalidatesSnapshotsOnly(Oid relid)
{
	switch (relid)
	{
		case DbRoleSettingRelationId:
		case DependRelationId:
		case SharedDependRelationId:
		case DescriptionRelationId:
		case SharedDescriptionRelationId:
		case SecLabelRelationId:
		case SharedSecLabelRelationId:
			return true;
		default:
			break;
	}

	return false;
}

/*
 * Test whether a relation has a system cache.
 */
bool
RelationHasSysCache(Oid relid)
{
	int			low = 0,
				high = SysCacheRelationOidSize - 1;

	while (low <= high)
	{
		int			middle = low + (high - low) / 2;

		if (SysCacheRelationOid[middle] == relid)
			return true;
		if (SysCacheRelationOid[middle] < relid)
			low = middle + 1;
		else
			high = middle - 1;
	}

	return false;
}

/*
 * Test whether a relation supports a system cache, ie it is either a
 * cached table or the index used for a cache.
 */
bool
RelationSupportsSysCache(Oid relid)
{
	int			low = 0,
				high = SysCacheSupportingRelOidSize - 1;

	while (low <= high)
	{
		int			middle = low + (high - low) / 2;

		if (SysCacheSupportingRelOid[middle] == relid)
			return true;
		if (SysCacheSupportingRelOid[middle] < relid)
			low = middle + 1;
		else
			high = middle - 1;
	}

	return false;
}


/*
 * OID comparator for pg_qsort
 */
static int
oid_compare(const void *a, const void *b)
{
	Oid			oa = *((const Oid *) a);
	Oid			ob = *((const Oid *) b);

	if (oa == ob)
		return 0;
	return (oa > ob) ? 1 : -1;
}

#ifdef __AUDIT__
void GetSysCacheInfo(int32 cacheid,
					 Oid * reloid,
					 Oid * indoid,
					 int * nkeys)
{
	if (cacheid < 0 || cacheid >= SysCacheSize)
	{
		elog(FATAL, "invalid cache ID: %d", cacheid);
	}

	if (reloid != NULL)
	{
		*reloid = cacheinfo[cacheid].reloid;
	}

	if (indoid != NULL)
	{
		*indoid = cacheinfo[cacheid].indoid;
	}

	if (nkeys != NULL)
	{
		*nkeys = cacheinfo[cacheid].nkeys;
	}
}
#endif
