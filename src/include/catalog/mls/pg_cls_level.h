/*
 * Copyright (c) 2023 THL A29 Limited, a Tencent company.
 *
 * This source code file is licensed under the BSD 3-Clause License,
 * you may obtain a copy of the License at http://opensource.org/license/bsd-3-clause/
 */
#ifndef PG_CLS_LEVEL_H
#define PG_CLS_LEVEL_H

#include "catalog/genbki.h"
/* oid */
#define ClsLevelRelationId  4585

CATALOG(pg_cls_level,4585) BKI_WITHOUT_OIDS 
{
    int16       polid;
    int16       levelid;
    NameData    shortname;
    text        longname;
}FormData_cls_level;

typedef FormData_cls_level *    Form_pg_cls_level;
/* the numbers of attribute  */
#define Natts_pg_cls_level              4

#define Anum_pg_cls_level_polid         1
#define Anum_pg_cls_level_levelid       2
#define Anum_pg_cls_level_shortname     3
#define Anum_pg_cls_level_longname      4

#endif   /* PG_CLS_LEVEL_H */
