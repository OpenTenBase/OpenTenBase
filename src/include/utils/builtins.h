/*-------------------------------------------------------------------------
 *
 * builtins.h
 *	  Declarations for operations on built-in types.
 *
 *
 * Portions Copyright (c) 2012-2014, TransLattice, Inc.
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
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
typedef struct BoolAggState
{
	int64		aggcount;		/* number of non-null values aggregated */
	int64		aggtrue;		/* number of values aggregated that are true */
} BoolAggState;
extern BoolAggState *makeBoolAggState(FunctionCallInfo fcinfo);
extern bool parse_bool(const char *value, bool *result);
extern bool parse_bool_with_len(const char *value, size_t len, bool *result);

/* domains.c */
extern void domain_check(Datum value, bool isnull, Oid domainType,
			 void **extra, MemoryContext mcxt);
extern int	errdatatype(Oid datatypeOid);
extern int	errdomainconstraint(Oid datatypeOid, const char *conname);

/* encode.c */
extern unsigned hex_encode(const char *src, unsigned len, char *dst);
extern unsigned hex_decode(const char *src, unsigned len, char *dst);
extern unsigned b64_encode_safe(const char *src, unsigned srclen, char *dst, unsigned dstlen);
extern unsigned b64_decode_safe(const char *src, unsigned srclen, char *dst, unsigned dstlen);
#ifdef _PG_ORCL_
extern unsigned b64_enc_len(const char *src, unsigned srclen);
#endif
/* int.c */
extern int2vector *buildint2vector(const int16 *int2s, int n);

/* name.c */
extern int	namecpy(Name n1, Name n2);
extern int	namestrcpy(Name name, const char *str);
extern int	namestrcmp(Name name, const char *str);

/* numutils.c */
extern int32 pg_atoi(const char *s, int size, int c);
extern int16 pg_strtoint16(const char *s);
extern int32 pg_strtoint32(const char *s);
extern int64 pg_strtoint64(const char *s);
extern void pg_itoa(int16 i, char *a);
extern void pg_ltoa(int32 l, char *a);
extern void pg_lltoa(int64 ll, char *a);
extern char *pg_ltostr_zeropad(char *str, int32 value, int32 minwidth);
extern char *pg_ltostr(char *str, int32 value);
extern uint64 pg_strtouint64(const char *str, char **endptr, int base);
extern void pg_i128toa(int128 value, char *a, int length);
extern double pg_banker_round(double number);
/* float.c */
extern PGDLLIMPORT int extra_float_digits;
/* for deparse, convert float to string with max digits*/
#define MAX_EXTRA_FLOAT_DIGITS 4

extern double get_float8_infinity(void);
extern float get_float4_infinity(void);
extern double get_float8_nan(void);
extern float get_float4_nan(void);
extern int	is_infinite(double val);
extern double float8in_internal(char *num, char **endptr_p,
				  const char *type_name, const char *orig_string);
extern double float8in_internal_opt_error(char *num, char **endptr_p,
					const char *type_name, const char *orig_string,
					bool *have_error);
extern char *float8out_internal(double num);
extern int	float4_cmp_internal(float4 a, float4 b);
extern int	float8_cmp_internal(float8 a, float8 b);
extern void float_overflow_error(void) pg_attribute_noreturn();
extern void float_underflow_error(void) pg_attribute_noreturn();

/* oid.c */
extern oidvector *buildoidvector(const Oid *oids, int n);
extern Oid	oidparse(Node *node);
#ifdef PGXC
extern Datum pgxc_node_str (PG_FUNCTION_ARGS);
extern Datum pgxc_lock_for_backup (PG_FUNCTION_ARGS);
#endif
extern int	oid_cmp(const void *p1, const void *p2);

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
extern char *quote_qualified_pkgobj_identifier(const char *qualifier,
											   const char *pkgname,
											   const char *object);

extern const char *quote_identifier_as_pg(const char *ident);
extern char *quote_qualified_identifier_as_pg(const char *qualifier, const char *ident);

/* varchar.c */
extern int	bpchartruelen(char *s, int len);

/* popular functions from varlena.c */
extern text *cstring_to_text(const char *s);
extern text *cstring_to_text_with_len(const char *s, int len);
extern text *cstring_to_text_upper(const char *s);
extern text *cstring_to_text_upper_with_len(const char *s, int len);
extern char *text_to_cstring(const text *t);
extern void text_to_cstring_buffer(const text *src, char *dst, size_t dst_len);

#define CStringGetTextDatum(s) PointerGetDatum(cstring_to_text(s))
#define TextDatumGetCString(d) text_to_cstring((text *) DatumGetPointer(d))

/* xid.c */
extern int	xidComparator(const void *arg1, const void *arg2);

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
extern Datum opentenbase_table_shard_stat_internal(PG_FUNCTION_ARGS);
extern Datum opentenbase_shard_stat_internal(PG_FUNCTION_ARGS);
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
extern Datum pg_transparent_crypt_support_datatype(PG_FUNCTION_ARGS);

/* new hash function */
extern Datum hashbpcharnew(PG_FUNCTION_ARGS);
extern Datum uuid_hash_new(PG_FUNCTION_ARGS);
extern Datum timestamp_hash_new(PG_FUNCTION_ARGS);
extern Datum interval_hash_new(PG_FUNCTION_ARGS);
extern Datum timetz_hash_new(PG_FUNCTION_ARGS);
extern Datum time_hash_new(PG_FUNCTION_ARGS);
extern Datum jsonb_hash_new(PG_FUNCTION_ARGS);
extern Datum hash_numeric_new(PG_FUNCTION_ARGS);
extern Datum hashvarlenanew(PG_FUNCTION_ARGS);
extern Datum hashtextnew(PG_FUNCTION_ARGS);
extern Datum hashnamenew(PG_FUNCTION_ARGS);
extern Datum hashcharnew(PG_FUNCTION_ARGS);
extern Datum hashoidnew(PG_FUNCTION_ARGS);
extern Datum hashfloat8new(PG_FUNCTION_ARGS);
extern Datum hashfloat4new(PG_FUNCTION_ARGS);
extern Datum hashint8new(PG_FUNCTION_ARGS);
extern Datum hashint4new(PG_FUNCTION_ARGS);
extern Datum hashint2new(PG_FUNCTION_ARGS);

#ifdef _PG_ORCL_
/* rowid.c */
extern Datum rowid_in(PG_FUNCTION_ARGS);
extern Datum rowid_out(PG_FUNCTION_ARGS);
extern Datum rowid_recv(PG_FUNCTION_ARGS);
extern Datum rowid_send(PG_FUNCTION_ARGS);
extern Datum rowid_eq(PG_FUNCTION_ARGS);
extern Datum rowid_ne(PG_FUNCTION_ARGS);
extern Datum rowid_lt(PG_FUNCTION_ARGS);
extern Datum rowid_le(PG_FUNCTION_ARGS);
extern Datum rowid_gt(PG_FUNCTION_ARGS);
extern Datum rowid_ge(PG_FUNCTION_ARGS);
extern Datum rowid_larger(PG_FUNCTION_ARGS);
extern Datum rowid_smaller(PG_FUNCTION_ARGS);
extern Datum btrowidcmp(PG_FUNCTION_ARGS);
extern Datum rowid_hash(PG_FUNCTION_ARGS);
extern Datum rowid_hash_new(PG_FUNCTION_ARGS);
extern Datum rowid_to_text(PG_FUNCTION_ARGS);
extern Datum text_to_rowid(PG_FUNCTION_ARGS);
#endif

#ifdef _PG_ORCL_
extern Datum rowid_to_raw_text(PG_FUNCTION_ARGS);
extern Datum orcl_regexp_count(PG_FUNCTION_ARGS);
extern Datum orcl_regexp_count_2(PG_FUNCTION_ARGS);
extern Datum orcl_regexp_count_3(PG_FUNCTION_ARGS);
extern Datum orcl_regexp_replace(PG_FUNCTION_ARGS);
extern Datum orcl_regexp_replace_2(PG_FUNCTION_ARGS);
extern Datum orcl_regexp_replace_3(PG_FUNCTION_ARGS);
extern Datum orcl_regexp_replace_4(PG_FUNCTION_ARGS);
extern Datum orcl_regexp_replace_5(PG_FUNCTION_ARGS);
extern Datum orcl_regexp_substr(PG_FUNCTION_ARGS);
extern Datum orcl_regexp_substr_2(PG_FUNCTION_ARGS);
extern Datum orcl_regexp_substr_3(PG_FUNCTION_ARGS);
extern Datum orcl_regexp_substr_4(PG_FUNCTION_ARGS);
extern Datum orcl_regexp_substr_5(PG_FUNCTION_ARGS);
extern Datum regexp_substr(PG_FUNCTION_ARGS);
extern Datum orcl_regexp_instr(PG_FUNCTION_ARGS);
extern Datum orcl_regexp_instr_2(PG_FUNCTION_ARGS);
extern Datum orcl_regexp_instr_3(PG_FUNCTION_ARGS);
extern Datum orcl_regexp_instr_4(PG_FUNCTION_ARGS);
extern Datum orcl_regexp_instr_5(PG_FUNCTION_ARGS);
extern Datum orcl_regexp_instr_6(PG_FUNCTION_ARGS);
extern Datum orcl_regexp_like(PG_FUNCTION_ARGS);
extern Datum orcl_regexp_like_2(PG_FUNCTION_ARGS);

extern Datum orcl_nchr(PG_FUNCTION_ARGS);
extern Datum orcl_substrb_2(PG_FUNCTION_ARGS);
extern Datum orcl_substrb_3(PG_FUNCTION_ARGS);
extern Datum orcl_substrb(PG_FUNCTION_ARGS);
extern Datum orcl_chr(PG_FUNCTION_ARGS);
extern Datum chr_encoding(uint32 cvalue);
extern Datum orcl_to_single_byte(PG_FUNCTION_ARGS);

/*adapt a's empty_blob*/
extern Datum get_empty_blob(PG_FUNCTION_ARGS);

extern Datum wm_concat_text_transfn(PG_FUNCTION_ARGS);
extern Datum wm_concat_int_transfn(PG_FUNCTION_ARGS);
extern Datum wm_concat_numeric_transfn(PG_FUNCTION_ARGS);
extern Datum numeric_to_char_nls(PG_FUNCTION_ARGS);
extern Datum numeric_to_number_nls(PG_FUNCTION_ARGS);
extern Datum numeric_lt_zero(PG_FUNCTION_ARGS);
/* int8.c */
extern Datum in_range_int8_int8(PG_FUNCTION_ARGS);
/* int.c */
extern Datum in_range_int4_int4(PG_FUNCTION_ARGS);
extern Datum in_range_int4_int2(PG_FUNCTION_ARGS);
extern Datum in_range_int4_int8(PG_FUNCTION_ARGS);
extern Datum in_range_int2_int4(PG_FUNCTION_ARGS);
extern Datum in_range_int2_int2(PG_FUNCTION_ARGS);
extern Datum in_range_int2_int8(PG_FUNCTION_ARGS);
/* float.c */
extern Datum in_range_float8_float8(PG_FUNCTION_ARGS);
extern Datum in_range_float4_float8(PG_FUNCTION_ARGS);
#endif


#endif							/* BUILTINS_H */
