#ifndef __PGXC_AUDIT__H
#define __PGXC_AUDIT__H

#include "nodes/nodes.h"
#include "nodes/parsenodes.h"

extern bool enable_audit;
extern bool enable_audit_warning;
extern bool enable_audit_depend;

typedef enum AuditSQL
{
	AuditSql_All				= 0,		/* AUDIT ALL [ {BY xxx} | {ON xxx} ]*/

	AuditSql_ShortcutBegin		= 1,		/* SQL Statement Shortcuts for Auditing	*/
											/* Used for AUDIT xxx [BY xxx] */
	AuditSql_AlterSystem,

	AuditSql_Database,
	AuditSql_Database_Alter,
	AuditSql_Database_Create,
	AuditSql_Directory_Create,
	AuditSql_Database_Drop,
	AuditSql_Directory_Drop,

	AuditSql_Extension			= 100,
	AuditSql_Extension_Alter,
	AuditSql_Extension_Create,
	AuditSql_Extension_Drop,

	AuditSql_Function			= 200,
	AuditSql_Function_Alter,
	AuditSql_Function_Create,
	AuditSql_Function_Drop,

	AuditSql_Group				= 300,
	AuditSql_Group_CreateNodeGroup,
	AuditSql_Group_CreateShardingGroup,
	AuditSql_Group_DropNodeGroup,
	AuditSql_Group_DropShardingInGroup,

	AuditSql_Index				= 400,
	AuditSql_Index_Alter,
	AuditSql_Index_Create,
	AuditSql_Index_Drop,

	AuditSql_MaterializedView	= 500,
	AuditSql_MaterializedView_Alter,
	AuditSql_MaterializedView_Create,
	AuditSql_MaterializedView_Drop,

	AuditSql_Node				= 600,
	AuditSql_Node_Alter,
	AuditSql_Node_Create,
	AuditSql_Node_Drop,

	AuditSql_Role				= 800,
	AuditSql_Role_Alter,
	AuditSql_Role_Create,
	AuditSql_Role_Drop,
	AuditSql_Role_Set,

	AuditSql_Schema,
	AuditSql_Schema_Alter,
	AuditSql_Schema_Create,
	AuditSql_Schema_Drop,

	AuditSql_Sequence			= 900,
	AuditSql_Sequence_Create,
	AuditSql_Sequence_Drop,

	AuditSql_Table				= 1000,
	AuditSql_Table_Create,
	AuditSql_Table_Drop,
	AuditSql_Table_Truncate,

	AuditSql_Tablespace			= 1100,
	AuditSql_Tablespace_Alter,
	AuditSql_Tablespace_Create,
	AuditSql_Tablespace_Drop,

	AuditSql_Trigger			= 1200,
	AuditSql_Trigger_Alter,
	AuditSql_Trigger_Create,
	AuditSql_Trigger_Drop,
	AuditSql_Trigger_Disable,
	AuditSql_Trigger_Enable,

	AuditSql_Type				= 1300,
	AuditSql_Type_Alter,
	AuditSql_Type_Create,
	AuditSql_Type_Drop,

	AuditSql_User				= 1400,
	AuditSql_User_Alter,
	AuditSql_User_Create,
	AuditSql_User_Drop,

	AuditSql_View				= 1500,
	AuditSql_View_Alter,
	AuditSql_View_Create,
	AuditSql_View_Drop,
#ifdef _PG_ORCL_	
	AuditSql_Package			= 1600,
	AuditSql_Package_Create,
	AuditSql_PackageBody_Create,
	AuditSql_Package_Drop,
	AuditSql_Package_Alter,

	AuditSql_Synonym			= 1700,
	AuditSql_Synonym_Create,
	AuditSql_Synonym_Drop,
	AuditSql_Synonym_Alter,
#endif

	AuditSql_ShortcutEnd 		= 30000,	/* SQL Statement Shortcuts for Auditing	*/

	AuditSql_AdditionalBegin 	= 30001,	/* Additional SQL Statement Shortcuts for Auditing */
											/* Used for AUDIT xxx [BY xxx] */

	AuditSql_AlterSequence,					/* ALTER ON SEQUENCE */
	AuditSql_AlterTable,					/* ALTER ON TABLE */
	AuditSql_CommentTable,					/* COMMENT ON TABLE[.COLUMN] */
											/* COMMENT ON VIEW[.COLUMN] */
											/* COMMENT ON MATERIALIZED VIEW[.COLUMN] */
	AuditSql_DeleteTable,					/* DELETE FROM TABLE */
											/* DELETE FROM VIEW */
	AuditSql_GrantFunction,					/* GRANT ON FUNCTION */
											/* REOVKE ON FUNCTION */
	AuditSql_GrantSequence,					/* GRANT ON SEQUENCE */
											/* REVOKE ON SEQUENCE */
	AuditSql_GrantTable,					/* GRANT ON TABLE/VIEW/MVIEW */
											/* REVOKE ON TABLE/VIEW/MVIEW */
	AuditSql_GrantType,						/* GRANT ON TYPE */
											/* REVOKE ON TYPE */
	AuditSql_InsertTable,					/* INSERT INTO TABLE */
											/* INSERT INTO VIEW */
	AuditSql_LockTable,						/* LOCK TABLE */
	AuditSql_SelectSequence,				/* SELECT SEQUENCE.nextval */
											/* SELECT SEQUENCE.currval */
	AuditSql_SelectTable,					/* SELECT FROM table */
											/* SELECT FROM view */
											/* SELECT FROM materialized view */
	AuditSql_SystemAudit,					/* AUDIT */
											/* NOAUDIT */
	AuditSql_SystemGrant,					/* GRANT */
											/* REVOKE */
	AuditSql_UpdateTable,					/* UPDATE TABLE */
											/* UPDATE VIEW */

	AuditSql_AdditionalEnd		= 60000,	/* Additional SQL Statement Shortcuts for Auditing */

	AuditSql_SchemaObjectBegin	= 60001,	/* Schema Object Auditing Options */
											/* Used for AUDIT xxx ON xxx */

	AuditSql_Alter,							/* ALTER TABLE */
											/* ALTER VIEW */
											/* ALTER MATERIALIZED VIEW */
											/* ALTER SEQUENCE */
	AuditSql_Audit,							/* AUDIT ON TABLE */
											/* NOAUDIT ON TABLE */
											/* AUDIT ON VIEW */
											/* NOAUDIT ON VIEW */
											/* AUDIT ON MATERIALIZED VIEW */
											/* NOAUDIT ON MATERIALIZED VIEW */
											/* AUDIT ON SEQUENCE */
											/* NOAUDIT ON SEQUENCE */
											/* AUDIT ON FUNCTION */
											/* NOAUDIT ON FUNCTION */
	AuditSql_Comment,						/* COMMENT ON TABLE */
											/* COMMENT ON VIEW */
											/* COMMENT ON MATERIALIZED VIEW */
	AuditSql_Delete,						/* DELETE FROM TABLE */
											/* DELETE FROM VIEW */
	AuditSql_Grant,							/* GRANT ON TABLE */
											/* REVOKE ON TABLE */
											/* GRANT ON VIEW */
											/* REVOKE ON VIEW */
											/* GRANT ON MATERIALIZED VIEW */
											/* REVOKE ON MATERIALIZED VIEW */
											/* GRANT ON SEQUENCE */
											/* REVOKE ON SEQUENCE */
											/* GRANT ON FUNCTION */
											/* REVOKE ON FUNCTION */
	/* AuditSql_Index, */					/* Audit/NoAudit Index On Table/MView is not supported in current version */
											/* CREATE INDEX ON TABLE */
											/* DROP INDEX ON TABLE */
											/* ALTER INDEX ON TABLE */
											/* CREATE INDEX ON MATERIALIZED VIEW */
											/* DROP INDEX ON MATERIALIZED VIEW */
											/* ALTER INDEX ON MATERIALIZED VIEW */
	AuditSql_Insert,						/* INSERT INTO TABLE */
											/* INSERT INTO VIEW */
	AuditSql_Lock,							/* LOCK ON TABLE */
	AuditSql_Rename,						/* RENAME ON TABLE */
											/* RENAME ON VIEW */
											/* RENAME ON MATERIALIZED VIEW */
	AuditSql_Select,						/* SELECT FROM TABLE */
											/* SELECT FROM VIEW */
											/* SELECT FROM MATERIALIZED VIEW */
											/* SELECT ON SEQUENCE */
	AuditSql_Update,						/* UPDATE TABLE */
											/* UPDATE VIEW */
											/* UPDATE MATERIALIZED VIEW */

	AuditSql_SchemaObjectEnd	= 90000,	/* Schema Object Auditing Options */

	AuditSQL_Ivalid				= 99999,	/* Ivalid AuditSQL */
} AuditSQL;

extern void AuditInitEnv(void);
extern void AuditDefine(AuditStmt * stmt, const char * queryString, char ** auditString);
extern void AuditClean(CleanAuditStmt * stmt, const char * queryString, char ** cleanString);
extern void AuditClearResultInfo(void);
extern void AuditReadQueryList(const char * query_sring, List * l_parsetree);
extern void AuditProcessResultInfo(bool is_success);
extern void AuditCheckPerms(Oid table_oid, Oid roleid, AclMode mask);

extern void RemoveStmtAuditById(Oid audit_id);
extern void RemoveUserAuditById(Oid audit_id);
extern void RemoveObjectAuditById(Oid audit_id);
extern void RemoveObjectDefaultAuditById(Oid audit_id);

extern char * get_stmt_audit_desc(Oid audit_id, bool just_identity);
extern char * get_user_audit_desc(Oid audit_id, bool just_identity);
extern char * get_obj_audit_desc(Oid audit_id, bool just_identity);
extern char * get_obj_default_audit_desc(Oid audit_id, bool just_identity);

extern bool audituser(void);
extern bool audituser_arg(Oid roleid);

#endif

