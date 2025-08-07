/*-------------------------------------------------------------------------
 *
 * index.h
 *	  prototypes for catalog/index.c.
 *
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/catalog/index.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef INDEX_H
#define INDEX_H

#include "catalog/objectaddress.h"
#include "nodes/execnodes.h"


#define DEFAULT_INDEX_TYPE	"btree"

/* Typedef for callback function for IndexBuildHeapScan */
typedef void (*IndexBuildCallback) (Relation index,
									HeapTuple htup,
									Datum *values,
									bool *isnull,
									bool tupleIsAlive,
									void *state);

typedef void (*IndexBuildVectorCallback)(Relation index, ItemPointer tid, 
											Datum *values, bool *isnull, void *state);

/* Action code for index_set_state_flags */
typedef enum
{
	INDEX_CREATE_SET_READY,
	INDEX_CREATE_SET_VALID,
	INDEX_DROP_CLEAR_VALID,
	INDEX_DROP_SET_DEAD,
#ifdef __OPENTENBASE__
	INDEX_ENABLE_SET_ALIVE,
    INDEX_DISABLE_SET_DEAD,
	INDEX_CREATE_SET_VALID_WITH_UPDATE,   /* do not use inplace update, use simple update */
#endif
} IndexStateFlagsAction;



extern void index_check_primary_key(Relation heapRel,
						IndexInfo *indexInfo,
						bool is_alter_table,
                                                IndexStmt *stmt);

#define    INDEX_CREATE_IS_PRIMARY             (1 << 0)
#define    INDEX_CREATE_ADD_CONSTRAINT         (1 << 1)
#define    INDEX_CREATE_SKIP_BUILD             (1 << 2)
#define    INDEX_CREATE_CONCURRENT             (1 << 3)
#define    INDEX_CREATE_IF_NOT_EXISTS          (1 << 4)
#define	INDEX_CREATE_PARTITIONED			(1 << 5)
#define INDEX_CREATE_INVALID				(1 << 6)

extern Oid index_create(Relation heapRelation,
			 char *indexRelationName,
			 Oid indexRelationId,
			 Oid parentIndexRelid,
			 Oid parentConstraintId,
			 Oid relFileNode,
			 IndexInfo *indexInfo,
			 List *indexColNames,
			 Oid accessMethodObjectId,
			 Oid tableSpaceId,
			 Oid *collationObjectId,
			 Oid *classObjectId,
			 int16 *coloptions,
			 Datum reloptions,
			 bits16 flags,
			 bits16 constr_flags,
			 bool allow_system_table_mods,
			 bool is_internal,
			 Oid *constraintId,
			 IndexStmt *indexstmt);

#define	INDEX_CONSTR_CREATE_MARK_AS_PRIMARY	(1 << 0)
#define	INDEX_CONSTR_CREATE_DEFERRABLE		(1 << 1)
#define	INDEX_CONSTR_CREATE_INIT_DEFERRED	(1 << 2)
#define	INDEX_CONSTR_CREATE_UPDATE_INDEX	(1 << 3)
#define	INDEX_CONSTR_CREATE_REMOVE_OLD_DEPS	(1 << 4)

#define CNI_SHARD_ID "cni_shardid"
#define CNI_TABLE_OID "cni_table_oid"
#define CNI_CTID "cni_ctid"
#define CNI_EXTRA "cni_extra"
#define CNI_META_NUM 4

extern ObjectAddress index_constraint_create(Relation heapRelation,
						Oid indexRelationId,
						Oid parentConstraintId,
						IndexInfo *indexInfo,
						const char *constraintName,
						char constraintType,
						bits16 constr_flags,
						bool allow_system_table_mods,
						bool is_internal);

extern void index_drop(Oid indexId, bool concurrent);

extern IndexInfo *BuildIndexInfo(Relation index);

extern bool CompareIndexInfo(IndexInfo *info1, IndexInfo *info2,
				 Oid *collations1, Oid *collations2,
				 Oid *opfamilies1, Oid *opfamilies2,
				 AttrNumber *attmap, int maplen);
extern IndexInfo *BuildDummyIndexInfo(Relation index);

extern void BuildSpeculativeIndexInfo(Relation index, IndexInfo *ii);

extern void FormIndexDatum(IndexInfo *indexInfo,
			   TupleTableSlot *slot,
			   EState *estate,
			   Datum *values,
			   bool *isnull);

extern void index_build(Relation heapRelation,
			Relation indexRelation,
			IndexInfo *indexInfo,
			bool isprimary,
			bool isreindex);

extern double IndexBuildHeapScan(Relation heapRelation,
				   Relation indexRelation,
				   IndexInfo *indexInfo,
				   bool allow_sync,
				   IndexBuildCallback callback,
				   void *callback_state);
extern double IndexBuildHeapRangeScan(Relation heapRelation,
						Relation indexRelation,
						IndexInfo *indexInfo,
						bool allow_sync,
						bool anyvisible,
						BlockNumber start_blockno,
						BlockNumber end_blockno,
						IndexBuildCallback callback,
						void *callback_state);

extern void validate_index(Oid heapId, Oid indexId, Snapshot snapshot);

extern void index_set_state_flags(Oid indexId, IndexStateFlagsAction action);

extern Oid	IndexGetRelation(Oid indexId, bool missing_ok);

extern void reindex_index(Oid indexId, bool skip_constraint_checks,
			  char relpersistence, int options);

/* Flag bits for reindex_relation(): */
#define REINDEX_REL_PROCESS_TOAST			0x01
#define REINDEX_REL_SUPPRESS_INDEX_USE		0x02
#define REINDEX_REL_CHECK_CONSTRAINTS		0x04
#define REINDEX_REL_FORCE_INDEXES_UNLOGGED	0x08
#define REINDEX_REL_FORCE_INDEXES_PERMANENT 0x10

extern bool reindex_relation(Oid relid, int flags, int options);

extern bool ReindexIsProcessingHeap(Oid heapOid);
extern bool ReindexIsProcessingIndex(Oid indexOid);
extern void ResetReindexState(int nestLevel);

extern void IndexSetParentIndex(Relation idx, Oid parentOid);

extern void index_table_create(Relation heapRelation,
							TupleDesc index_key_desc,
							char *indexTableName,
							bool unique,
							Oid tableSpaceId,
							Oid nameSpaceId,
							ObjectAddress *relation,
							ObjectAddress *index,
							IndexStmt *originIndexStmt);
extern void cross_node_index_drop(Oid indexId, bool concurrent);
extern bool relid_has_cross_node_index(Oid relid);
extern bool relation_has_cross_node_index(Relation rel);
extern List *RelationBuildCrossNodeIndexList(Relation relation, bool copy_data);
extern bool RelationOidIsCrossNodeIndex(Oid oid);
#define IS_GLOBAL_INDEX_PATH(path) (bool) ((path) && IsA((path), IndexPath) && \
    ((IndexPath*)(path))->indexinfo && (RelationOidIsCrossNodeIndex(((IndexPath*)(path))->indexinfo->indexoid)))
extern AttrNumber* cross_node_index_get_distribute_attnum_map(RelationLocInfo *rel_loc_info);
extern void IndexCreateSetValid(Oid index, Oid rel);

#endif							/* INDEX_H */
