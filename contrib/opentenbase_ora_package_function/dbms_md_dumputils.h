/*-------------------------------------------------------------------------
 *
 * dbms_md_dumputils.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef DBMS_MD_DUMPUTILS_H
#define DBMS_MD_DUMPUTILS_H

#include "postgres.h"
#include "lib/stringinfo.h"
#include "executor/spi.h"

typedef enum					/* bits returned by set_dump_section */
{
	DUMP_PRE_DATA = 0x01,
	DUMP_DATA = 0x02,
	DUMP_POST_DATA = 0x04,
	DUMP_UNSECTIONED = 0xff
} DumpSections;


/*
 * Preferred strftime(3) format specifier for printing timestamps in pg_dump
 * and friends.
 *
 * We don't print the timezone on Windows, because the names are long and
 * localized, which means they may contain characters in various random
 * encodings; this has been seen to cause encoding errors when reading the
 * dump script.  Think not to get around that by using %z, because
 * (1) %z is not portable to pre-C99 systems, and
 * (2) %z doesn't actually act differently from %Z on Windows anyway.
 */
#ifndef WIN32
#define PGDUMP_STRFTIME_FMT  "%Y-%m-%d %H:%M:%S %Z"
#else
#define PGDUMP_STRFTIME_FMT  "%Y-%m-%d %H:%M:%S"
#endif

typedef struct DbmsMdOidListCell
{
	struct DbmsMdOidListCell *next;
	Oid			val;
}DbmsMdOidListCell;

typedef struct DbmsMdOidList
{
	DbmsMdOidListCell *head;
	DbmsMdOidListCell *tail;
}DbmsMdOidList;

typedef struct DbmsMdStringListCell
{
	struct DbmsMdStringListCell *next;
	bool		touched;		/* true, when this string was searched and
								 * touched */
	char		val[FLEXIBLE_ARRAY_MEMBER]; /* null-terminated string here */
} DbmsMdStringListCell;

typedef struct  DbmsMdStringList
{
	 DbmsMdStringListCell *head;
	 DbmsMdStringListCell *tail;
}  DbmsMdStringList;

extern const char *dbms_md_string_list_not_touched(DbmsMdStringList *list);
extern void dbms_md_oid_list_clear(DbmsMdOidList *list);
extern void dbms_md_oid_list_append(MemoryContext mem_ctx, DbmsMdOidList *list, Oid val);
extern bool dbms_md_oid_list_member(DbmsMdOidList *list, Oid val);
extern void dbms_md_string_list_clear(DbmsMdStringList *list);
extern void dbms_md_string_list_append(MemoryContext mem_ctx, DbmsMdStringList *list, const char *val);
extern bool dbms_md_string_list_member(DbmsMdStringList *list, const char *val);


/*extern StringInfo createStringInfo();
extern void destroyStringInfo(StringInfo output);
extern void printfStringInfo(StringInfo str, const char *fmt,...);
extern const char * dbms_md_fmtId(const char *rawid);
extern const char *
dbms_md_fmt_qualified_id(int remoteVersion, const char *schema, const char *id);
*/
extern bool
dbms_md_parse_array(const char *atext, char ***itemarray, int *nitems);

extern bool buildACLCommands(const char *name, const char *subname,
				 const char *type, const char *acls, const char *racls,
				 const char *owner, const char *prefix, int remoteVersion,
				 StringInfo sql);
extern bool buildDefaultACLCommands(const char *type, const char *nspname,
						const char *acls, const char *racls,
						const char *initacls, const char *initracls,
						const char *owner,
						int remoteVersion,
						StringInfo sql);
/*extern void buildShSecLabelQuery(PGconn *conn, const char *catalog_name,
					 uint32 objectId, StringInfo sql);
extern void emitShSecLabels(PGconn *conn, PGresult *res,
				StringInfo buffer, const char *target, const char *objname);*/

extern void buildACLQueries(StringInfo acl_subquery, StringInfo racl_subquery,
				StringInfo init_acl_subquery, StringInfo init_racl_subquery,
				const char *acl_column, const char *acl_owner,
				const char *obj_kind, bool binary_upgrade);


extern char *dbms_md_strdup(const char *in);
extern void *dbms_md_malloc0(MemoryContext mem_ctx, size_t len);
extern void *dbms_md_palloc(size_t len);
extern void *dbms_md_realloc(void *ptr, size_t len);
extern void dbms_md_free(void *ptr);

extern void set_dump_section(const char *arg, int *dumpSections);
extern void write_msg(const char *modulename, const char *fmt,...) pg_attribute_printf(2, 3);
extern void vwrite_msg(const char *modulename, const char *fmt, va_list ap) pg_attribute_printf(2, 0);

#endif							/* DBMS_MD_DUMPUTILS_H */
