/*
 *	Definitions for dbms_md_backup_db.c
 *
 *	IDENTIFICATION
 *		dbms_md_backup_db.h
 */

#ifndef DBMS_MD_BACKUP_DB_H
#define DBMS_MD_BACKUP_DB_H


#include "dbms_md_backup.h"

#define DBMS_MD_MAX_TUPS (10000)
void DisconnectDatabase(Archive *AHX);
//extern int	ExecuteSqlCommandBuf(Archive *AHX, const char *buf, size_t bufLen);
extern void dbms_md_exe_sql_no_result(Archive *AHX, const char* query);
extern void ExecuteSqlStatement(Archive *AHX, const char *query);
extern SPITupleTable *ExecuteSqlQuery(Archive *AHX, const char *query);
extern SPITupleTable *ExecuteSqlQueryForSingleRow(Archive *fout, char *query);
extern int dbms_md_get_field_val_len(SPITupleTable *tups, int tup_idx, int field_idx);
extern char* dbms_md_get_field_strval(SPITupleTable *tups, int tup_idx, int field_idx);
extern char* dbms_md_get_field_value(SPITupleTable *tups, int tup_idx, int field_idx);
extern void dbms_md_free_tuples(SPITupleTable *tups);
extern int dbms_md_get_field_subscript(SPITupleTable *tups, const char* field_name);
extern const char* dbms_md_get_field_name(SPITupleTable *tups, int field_idx);

extern int dbms_md_get_tuple_num(SPITupleTable *tups);
extern int dbms_md_is_tuple_field_null(SPITupleTable *tups, int tup_idx, int field_idx);

#endif		/*DBMS_MD_BACKUP_DB_H*/
