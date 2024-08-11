/*-------------------------------------------------------------------------
 *
 * builtins.h
 *      Declarations for operations on built-in types.
 *
 *
 * Portions Copyright (c) 2012-2014, TransLattice, Inc.
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * This source code file contains modifications made by THL A29 Limited ("Tencent Modifications").
 * All Tencent Modifications are Copyright (C) 2023 THL A29 Limited.
 *
 * src/include/utils/builtins.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef BUILTINS_H
#define BUILTINS_H

#include "fmgr.h"
#include "nodes/parsenodes.h"
#ifdef PGXC
#include "lib/stringinfo.h"
#endif
#include "nodes/nodes.h"
#include "utils/fmgrprotos.h"

/* bool.c */
extern bool parse_bool(const char *value, bool *result);
extern bool parse_bool_with_len(const char *value, size_t len, bool *result);

/* domains.c */
extern void domain_check(Datum value, bool isnull, Oid domainType,
             void **extra, MemoryContext mcxt);
extern int    errdatatype(Oid datatypeOid);
extern int    errdomainconstraint(Oid datatypeOid, const char *conname);

/* encode.c */
extern unsigned hex_encode(const char *src, unsigned len, char *dst);
extern unsigned hex_decode(const char *src, unsigned len, char *dst);

/* int.c */
extern int2vector *buildint2vector(const int16 *int2s, int n);

/* name.c */
extern int    namecpy(Name n1, Name n2);
extern int    namestrcpy(Name name, const char *str);
extern int    namestrcmp(Name name, const char *str);

/* numutils.c */
extern int32 pg_atoi(const char *s, int size, int c);
extern void pg_itoa(int16 i, char *a);
extern void pg_ltoa(int32 l, char *a);
extern void pg_lltoa(int64 ll, char *a);
extern char *pg_ltostr_zeropad(char *str, int32 value, int32 minwidth);
extern char *pg_ltostr(char *str, int32 value);
extern uint64 pg_strtouint64(const char *str, char **endptr, int base);

/* float.c */
extern PGDLLIMPORT int extra_float_digits;

extern double get_float8_infinity(void);
extern float get_float4_infinity(void);
extern double get_float8_nan(void);
extern float get_float4_nan(void);
extern int    is_infinite(double val);
extern double float8in_internal(char *num, char **endptr_p,
                  const char *type_name, const char *orig_string);
extern char *float8out_internal(double num);
extern int    float4_cmp_internal(float4 a, float4 b);
extern int    float8_cmp_internal(float8 a, float8 b);

/* oid.c */
extern oidvector *buildoidvector(const Oid *oids, int n);
extern Oid    oidparse(Node *node);
#ifdef PGXC
extern Datum pgxc_node_str (PG_FUNCTION_ARGS);
extern Datum pgxc_lock_for_backup (PG_FUNCTION_ARGS);
#endif
extern int    oid_cmp(const void *p1, const void *p2);

/* regexp.c */
extern char *regexp_fixed_prefix(text *text_re, bool case_insensitive,
                    Oid collation, bool *exact);

/* ruleutils.c */
extern bool quote_all_identifiers;
#ifdef PGXC
extern void get_query_def_from_valuesList(Query *query, StringInfo buf);
extern void deparse_query(Query *query, StringInfo buf, List *parentnamespace,
        bool finalise_aggs, bool sortgroup_colno);
#endif
#ifdef PGXC
extern List *deparse_context_for_plan(Node *plan, List *ancestors,
                              List *rtable);
#endif
extern const char *quote_identifier(const char *ident);
extern char *quote_qualified_identifier(const char *qualifier,
                           const char *ident);

/* varchar.c */
extern int    bpchartruelen(char *s, int len);

/* popular functions from varlena.c */
extern text *cstring_to_text(const char *s);
extern text *cstring_to_text_with_len(const char *s, int len);
extern char *text_to_cstring(const text *t);
extern void text_to_cstring_buffer(const text *src, char *dst, size_t dst_len);

#define CStringGetTextDatum(s) PointerGetDatum(cstring_to_text(s))
#define TextDatumGetCString(d) text_to_cstring((text *) DatumGetPointer(d))

/* xid.c */
extern int    xidComparator(const void *arg1, const void *arg2);

/* inet_cidr_ntop.c */
extern char *inet_cidr_ntop(int af, const void *src, int bits,
               char *dst, size_t size);

/* inet_net_pton.c */
extern int inet_net_pton(int af, const char *src,
              void *dst, size_t size);

/* network.c */
extern double convert_network_to_scalar(Datum value, Oid typid);
extern Datum network_scan_first(Datum in);
extern Datum network_scan_last(Datum in);
extern void clean_ipv6_addr(int addr_family, char *addr);

/* numeric.c */
extern Datum numeric_float8_no_overflow(PG_FUNCTION_ARGS);

/* format_type.c */
extern char *format_type_be(Oid type_oid);
extern char *format_type_be_qualified(Oid type_oid);
extern char *format_type_with_typemod(Oid type_oid, int32 typemod);
extern char *format_type_with_typemod_qualified(Oid type_oid, int32 typemod);
extern int32 type_maximum_size(Oid type_oid, int32 typemod);

/* quote.c */
extern char *quote_literal_cstr(const char *rawstr);

#ifdef PGXC
/* backend/pgxc/pool/poolutils.c */
extern Datum pgxc_pool_check(PG_FUNCTION_ARGS);
extern Datum pgxc_pool_reload(PG_FUNCTION_ARGS);
extern Datum pgxc_rda_reload(PG_FUNCTION_ARGS);
extern Datum pgxc_pool_disconnect(PG_FUNCTION_ARGS);

/* backend/access/transam/transam.c */
extern Datum pgxc_is_committed(PG_FUNCTION_ARGS);
extern Datum pgxc_is_inprogress(PG_FUNCTION_ARGS);
extern Datum opentenbase_version(PG_FUNCTION_ARGS);
#endif

extern Datum pg_msgmodule_set(PG_FUNCTION_ARGS);
extern Datum pg_msgmodule_change(PG_FUNCTION_ARGS);
extern Datum pg_msgmodule_enable(PG_FUNCTION_ARGS);
extern Datum pg_msgmodule_disable(PG_FUNCTION_ARGS);
extern Datum pg_msgmodule_enable_all(PG_FUNCTION_ARGS);
extern Datum pg_msgmodule_disable_all(PG_FUNCTION_ARGS);

#ifdef _SHARDING_
extern oidvector *oidvector_append(oidvector *oldoids, Oid newOid);
extern oidvector *oidvector_remove(oidvector *oldoids, Oid toremove);
extern bool  oidvector_member(oidvector *oids, Oid oid);
extern Datum pg_extent_info_oid(PG_FUNCTION_ARGS);
extern Datum pg_extent_info_relname(PG_FUNCTION_ARGS);
extern Datum pg_shard_scan_list_oid(PG_FUNCTION_ARGS);
extern Datum pg_shard_scan_list_relname(PG_FUNCTION_ARGS);
extern Datum pg_shard_alloc_list_oid(PG_FUNCTION_ARGS);
extern Datum pg_shard_alloc_list_relname(PG_FUNCTION_ARGS);
extern Datum pg_shard_anchor_oid(PG_FUNCTION_ARGS);
extern Datum pg_shard_anchor_relname(PG_FUNCTION_ARGS);
extern Datum pg_check_extent(PG_FUNCTION_ARGS);
extern Datum pg_stat_barrier_shards(PG_FUNCTION_ARGS);

extern Datum pg_stat_table_shard(PG_FUNCTION_ARGS);
extern Datum  pg_stat_all_shard(PG_FUNCTION_ARGS);
#endif
/*mls.c*/
extern Datum pg_cls_check(PG_FUNCTION_ARGS);
extern Datum pg_execute_query_on_all_nodes(PG_FUNCTION_ARGS);
extern Datum pg_get_table_oid_by_name(PG_FUNCTION_ARGS);
extern Datum pg_get_role_oid_by_name(PG_FUNCTION_ARGS);
extern Datum pg_get_function_oid_by_name(PG_FUNCTION_ARGS);
extern Datum pg_get_schema_oid_by_name(PG_FUNCTION_ARGS);
extern Datum pg_get_tablespace_oid_by_tablename(PG_FUNCTION_ARGS);
extern Datum pg_get_tablespace_name_by_tablename(PG_FUNCTION_ARGS);
extern Datum pg_get_current_database_oid(PG_FUNCTION_ARGS);
extern Datum pg_trsprt_crypt_support_datatype(PG_FUNCTION_ARGS);
extern Datum pg_rel_crypt_hash_dump(PG_FUNCTION_ARGS);
extern Datum pg_crypt_key_hash_dump(PG_FUNCTION_ARGS);


#endif                            /* BUILTINS_H */
