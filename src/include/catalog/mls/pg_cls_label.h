/*
 * Copyright (c) 2023 THL A29 Limited, a Tencent company.
 *
 * This source code file is licensed under the BSD 3-Clause License,
 * you may obtain a copy of the License at http://opensource.org/license/bsd-3-clause/
 */
#ifndef PG_CLS_LABEL_H
#define PG_CLS_LABEL_H

#include "catalog/genbki.h"
/* oid */
#define ClsLabelRelationId 4583

CATALOG(pg_cls_label,4583) BKI_WITHOUT_OIDS 
{
    int16   polid;
    int16   labelid;
    int16   levelid;
    int16   compartmentid[1];
    int16   groupid[1];
}FormData_cls_label;

typedef FormData_cls_label *    Form_pg_cls_label;
/* the numbers of attribute  */
#define Natts_pg_cls_label              5

#define Anum_pg_cls_label_polid         1
#define Anum_pg_cls_label_labelid       2
#define Anum_pg_cls_label_levelid       3
#define Anum_pg_cls_label_compartmentid 4
#define Anum_pg_cls_label_groupid       5

#endif   /* PG_CLS_LABEL_H */
