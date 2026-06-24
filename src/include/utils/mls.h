#ifndef _MLS_H_
#define _MLS_H_

#include "nodes/parsenodes.h"
#include "nodes/pg_list.h"
#include "utils/relcache.h"
#include "access/transam.h"
#include "access/heapam.h"
#include "executor/executor.h"
#include "postmaster/postmaster.h"
#include "storage/relfilenode.h"
#include "utils/cls.h"

#define MARK(X) 1

/*
 * this macro seems redundant, cause IsSystemRelation has already done a more elegant work,
 * while, we just keep a sample priciple, that is tables created in initdb procedure, we just skip them.
 */
#define IS_SYSTEM_REL(_relid)       ((_relid) < FirstNormalObjectId)

extern bool g_enable_cls;

extern List * FetchAllParitionList(Oid relid);
extern void MlsExecCheck(ScanState *node, TupleTableSlot *slot);
extern bool mls_check_relation_permission(Oid relid, bool * schema_bound);
extern bool mls_check_schema_permission(Oid schemaoid);
extern bool mls_check_column_permission(Oid relid, int attnum);
extern bool mls_check_role_permission(Oid roleid);
extern int transfer_rol_kind_ext(Oid rolid);
extern int transfer_rel_kind_ext(Oid relid);
extern bool mls_user(void);
extern Oid mls_get_parent_oid(Relation rel);
extern void CacheInvalidateRelcacheAllPartitions(Oid databaseid, Oid relid);
extern bool is_mls_or_audit_user(void);
extern bool mls_support_data_type(Oid typid);
extern Oid mls_get_parent_oid_by_relid(Oid relid);
extern const char * mls_query_string_prune(const char * querystring);
extern bool is_mls_user(void);

extern void mls_start_encrypt_parellel_workers(void);
extern List* mls_encrypt_buf_parellel(List * buf_id_list, int16 algo_id, int buf_id, int status);
extern List * SyncBufidListAppend(List * buf_id_list, int buf_id, int status, int slot_id, int worker_id, char* bufToWrite);
extern void mls_log_crypt_worker_detail(void);
extern void mls_check_datamask_need_passby(ScanState * scanstate, Oid relid);
extern bool mls_check_inner_conn(const char * cmd_option);

#endif /*_MLS_H_*/
