#ifndef PG_CLS_TABLE_H
#define PG_CLS_TABLE_H

#include "catalog/genbki.h"

#define ClsTableRelationId  8174

CATALOG(pg_cls_table,8174) BKI_WITHOUT_OIDS 
{
    int16       polid;
    int16       attnum;
    Oid         relid;
    bool        enable;
    NameData    nspname;
    NameData    tblname;
    text        reloptions[1];
}FormData_cls_table;

typedef FormData_cls_table *    Form_pg_cls_table;

#define Natts_pg_cls_table              7

#define Anum_pg_cls_table_polid         1
#define Anum_pg_cls_table_attnum        2
#define Anum_pg_cls_table_relid         3
#define Anum_pg_cls_table_enable        4
#define Anum_pg_cls_table_nspname       5
#define Anum_pg_cls_table_tblname       6
#define Anum_pg_cls_table_reloptions    7

#endif   /* PG_CLS_TABLE_H */
