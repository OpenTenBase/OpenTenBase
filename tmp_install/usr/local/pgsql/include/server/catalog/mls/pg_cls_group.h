/*
 * Copyright (c) 2023 THL A29 Limited, a Tencent company.
 *
 * This source code file is licensed under the BSD 3-Clause License,
 * you may obtain a copy of the License at http://opensource.org/license/bsd-3-clause/
 */
#ifndef PG_CLS_GROUP_H
#define PG_CLS_GROUP_H

#include "catalog/genbki.h"
/* oid */
#define ClsGroupRelationId  4589

CATALOG(pg_cls_group,4589) BKI_WITHOUT_OIDS 
{
    int16       polid;
    int16       groupid;
    int16       parentid;
    NameData    shortname;
    text        longname;
}FormData_cls_group;

typedef FormData_cls_group *    Form_pg_cls_group;
/* the numbers of attribute  */
#define Natts_pg_cls_group              5

#define Anum_pg_cls_group_polid         1
#define Anum_pg_cls_group_groupid       2
#define Anum_pg_cls_group_parentid      3
#define Anum_pg_cls_group_shortname     4
#define Anum_pg_cls_group_longname      5


#endif   /* PG_CLS_GROUP_H */
