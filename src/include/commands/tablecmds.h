/*-------------------------------------------------------------------------
 *
 * tablecmds.h
 *	  prototypes for tablecmds.c.
 *
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/commands/tablecmds.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef TABLECMDS_H
#define TABLECMDS_H

#include "access/htup.h"
#include "catalog/dependency.h"
#include "catalog/objectaddress.h"
#include "nodes/parsenodes.h"
#include "storage/lock.h"
#include "utils/relcache.h"

#include "access/heapam.h"


extern void	DefineExternalRelation(CreateExternalStmt *stmt);

extern ObjectAddress DefineRelation(CreateStmt *stmt, char relkind, Oid ownerId,
			   ObjectAddress *typaddress, const char *queryString, Oid viewId);
#ifdef __OPENTENBASE__
extern int RemoveRelations(DropStmt *drop, char* queryString);
#else
extern void RemoveRelations(DropStmt *drop);
#endif

extern Oid	AlterTableLookupRelation(AlterTableStmt *stmt, LOCKMODE lockmode);

extern void AlterTable(Oid relid, LOCKMODE lockmode, AlterTableStmt *stmt);

extern LOCKMODE AlterTableGetLockLevel(List *cmds);

extern void ATExecChangeOwner(Oid relationOid, Oid newOwnerId, bool recursing, LOCKMODE lockmode, void *context);

extern void AlterTableInternal(Oid relid, List *cmds, bool recurse);

extern Oid	AlterTableMoveAll(AlterTableMoveAllStmt *stmt);

extern ObjectAddress AlterTableNamespace(AlterObjectSchemaStmt *stmt,
					Oid *oldschema);

extern void AlterTableNamespaceInternal(Relation rel, Oid oldNspOid,
							Oid nspOid, ObjectAddresses *objsMoved
#ifdef _MLS_
                            , const char * newschemaname
#endif
							);

extern void AlterRelationNamespaceInternal(Relation classRel, Oid relOid,
							   Oid oldNspOid, Oid newNspOid,
							   bool hasDependEntry,
							   ObjectAddresses *objsMoved);

extern bool ConstraintSatisfyAutoIncrement(HeapTuple tuple, TupleDesc desc, 
										   AttrNumber attrnum, char contype);

extern void CheckTableNotInUse(Relation rel, const char *stmt);

extern void ExecuteTruncate(TruncateStmt *stmt);

extern void SetRelationHasSubclass(Oid relationId, bool relhassubclass);

extern ObjectAddress renameatt(RenameStmt *stmt);

extern ObjectAddress renameatt_type(RenameStmt *stmt);

extern ObjectAddress RenameConstraint(RenameStmt *stmt);

extern ObjectAddress RenameRelation(RenameStmt *stmt);

extern void RenameRelationInternal(Oid myrelid,
					   const char *newrelname, bool is_internal);

extern void find_composite_type_dependencies(Oid typeOid,
								 Relation origRelation,
								 const char *origTypeName);

extern void check_of_type(HeapTuple typetuple);

extern void register_on_commit_action(Oid relid, OnCommitAction action);
extern void remove_on_commit_action(Oid relid);

extern void PreCommit_on_commit_actions(void);
extern void AtEOXact_on_commit_actions(bool isCommit);
extern void AtEOSubXact_on_commit_actions(bool isCommit,
							  SubTransactionId mySubid,
							  SubTransactionId parentSubid);
#ifdef PGXC
extern bool IsShardRelOrMainTableIsShardRelOrRepRel(Oid relid);
extern bool IsTempTable(Oid relid);
extern bool IsLocalTempTable(Oid relid);
extern bool IsIndexUsingTempTable(Oid relid);
extern bool IsIndexUsingGlobalTempTable(Oid relid);
extern bool IsOnCommitActions(void);
extern void DropTableThrowErrorExternal(RangeVar *relation,
										ObjectType removeType,
										bool missing_ok);
#endif

extern void RangeVarCallbackOwnsTable(const RangeVar *relation,
						  Oid relId, Oid oldRelId, void *arg);

extern bool has_partition_ancestor_privs(Oid relid, Oid userid, AclMode acl);

extern void RangeVarCallbackOwnsRelation(const RangeVar *relation,
							 Oid relId, Oid oldRelId, void *noCatalogs);
extern bool PartConstraintImpliedByRelConstraint(Relation scanrel,
                                    List *partConstraint);

#ifdef _MIGRATE_
extern bool oidarray_contian_oid(Oid *old_oids, int old_num, Oid new_oid);
extern Oid *add_node_list(Oid *old_oids, int old_num, Oid *add_oids, int add_num, int *new_num);
#endif

#ifdef _PG_ORCL_
extern void MoveDefaultPartitionRows(Relation def_rel, Relation newRel,
						HeapTuple tuple, int hi_options, BulkInsertState bistate,
						CommandId mycid);
#endif

extern List *MergeAttributes(List *schema, List *supers, char relpersistence,
				bool is_partition, List **supconstr,
				int *supOidCount
				);

extern void report_utility_time(RangeVar* relation, Relation objRel, Oid parentOid);
extern void record_acitve_temp_table(Oid oid);

#endif							/* TABLECMDS_H */
