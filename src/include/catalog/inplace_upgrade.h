/*-------------------------------------------------------------------------
 *
 * inplace_upgrade.h
 *	  variables used for binary upgrades
 *
 * Portions Copyright (c) 2018, Tencent OpenTenBase-C group.
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/catalog/inplace_upgrade.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef INPLACE_UPGRADE_H
#define INPLACE_UPGRADE_H

extern Oid inplace_upgrade_next_heap_pg_class_oid;
extern Oid inplace_upgrade_next_toast_pg_class_oid;
extern Oid inplace_upgrade_next_index_pg_class_oid;
extern Oid inplace_upgrade_next_pg_type_oid;
extern Oid inplace_upgrade_next_array_pg_type_oid;
extern Oid inplace_upgrade_next_pg_proc_oid;
extern Oid inplace_upgrade_next_pg_namespace_oid;
extern Oid inplace_upgrade_next_general_oid;

extern bool new_catalog_isshared;
extern bool new_catalog_need_storage;

extern List* new_shared_catalog_list; 

extern char TypeCreateType;

#endif							/* INPLACE_UPGRADE_H */
