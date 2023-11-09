/*
 * Copyright (c) 2023 THL A29 Limited, a Tencent company.
 *
 * This source code file is licensed under the BSD 3-Clause License,
 * you may obtain a copy of the License at http://opensource.org/license/bsd-3-clause/
 */
#ifndef PG_CLS_TABLE_H
#define PG_CLS_TABLE_H

#include "catalog/genbki.h"
/* oid */
#define ClsTableRelationId  4581

CATALOG(pg_cls_table,4581) BKI_WITHOUT_OIDS 
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
/* the numbers of attribute  */
#define Natts_pg_cls_table              7
/* the number of attribute  */
#define Anum_pg_cls_table_polid         1
#define Anum_pg_cls_table_attnum        2
#define Anum_pg_cls_table_relid         3
#define Anum_pg_cls_table_enable        4
#define Anum_pg_cls_table_nspname       5
#define Anum_pg_cls_table_tblname       6
#define Anum_pg_cls_table_reloptions    7

#endif   /* PG_CLS_TABLE_H */
