/*-------------------------------------------------------------------------
 *
 * dbms_md_string_utils.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef DBMS_MD_STRING_UTILS_H
#define DBMS_MD_STRING_UTILS_H

#include "postgres.h"
#include "lib/stringinfo.h"
#include "common/opentenbase_ora.h"


//extern StringInfo createStringInfo(MemoryContext *mem_ctx);
extern Oid dbms_md_str_to_oid(const char* str);
extern void SetLocalStringBuffer(StringInfo buf);
extern StringInfo createStringInfo(void);
extern void destroyStringInfo(StringInfo output);
extern void printfStringInfo(StringInfo str, const char *fmt,...)__attribute__ ((format (gnu_printf, 2, 3)));
extern const char *dbms_md_fmtId_internal(const char *rawid, bool force_pg_mode);
extern const char *dbms_md_fmt_qualified_id(int remoteVersion, const char *schema, const char *id);
extern bool dbms_md_parse_array(const char *atext, char ***itemarray, int *nitems);
extern char *
dbms_md_fmt_version_number(MemoryContext mem_ctx, int version_number, bool include_minor);

extern bool
dbms_process_sql_name_pattern(int encoding, 
										      int server_version, 
										      bool std_string,
										      StringInfo buf, 
										      const char *pattern,
										      bool have_where, 
										      bool force_escape,
										      const char *schemavar, 
										      const char *namevar,
										      const char *altnamevar, 
										      const char *visibilityrule);
extern bool
dbms_append_reloptions_array(StringInfo buffer, const char *reloptions,
					  const char *prefix, int encoding, bool std_strings);

extern size_t
dbms_md_escape_string_conn(int encoding, bool std_strings,
				   char *to, const char *from, size_t length,
				   int *error);
extern size_t
dbms_md_escape_string_internal(char *to, const char *from, size_t length,
					   int *error,
					   int encoding, bool std_strings);
extern void
dbms_md_append_psql_meta_connect(StringInfo buf, const char *dbname);

extern void
dbms_md_append_conn_str_val(StringInfo buf, const char *str);

extern void
dbms_md_append_bytea_literal(StringInfo buf, const unsigned char *str, size_t length,
				   bool std_strings);
extern void
dbms_md_append_string_literal_dq(StringInfo buf, const char *str, const char *dqprefix);

extern void
dbms_md_append_string_literal_conn(StringInfo buf, 
												const char *str,  
												int server_version,
												int encoding, 
												bool std_strings);
extern void
dbms_md_append_string_literal(StringInfo buf, const char *str,
					int encoding, bool std_strings);
extern unsigned char *
dbms_md_escape_bytea(const unsigned char *strtext, size_t *retbuflen);

#define appendStringLiteralAH(buf,str,AH) \
	dbms_md_append_string_literal(buf, str, (AH)->encoding, (AH)->std_strings)	

#define appendStringLiteralAHX(buf,str,AH) \
	dbms_md_append_string_literal(buf, str, (AH)->public.encoding, (AH)->public.std_strings)

#define appendByteaLiteralAHX(buf,str,len,AH) \
	dbms_md_append_bytea_literal(buf, str, len, (AH)->public.std_strings)

#define dbms_md_fmtId(rawid) dbms_md_fmtId_internal(rawid, false)

#endif							/* DBMS_MD_STRING_UTILS_H */
