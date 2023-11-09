/*
 * Copyright (c) 2023 THL A29 Limited, a Tencent company.
 *
 * This source code file is licensed under the BSD 3-Clause License,
 * you may obtain a copy of the License at http://opensource.org/license/bsd-3-clause/
 */
#ifndef PG_CLS_COMPARTMENT_H
#define PG_CLS_COMPARTMENT_H

#include "catalog/genbki.h"
/* oid */
#define ClsCompartmentRelationId  4587

CATALOG(pg_cls_compartment,4587) BKI_WITHOUT_OIDS 
{
    int16       polid;
    int16       compartmentid;
    NameData    shortname;
    text        longname;
}FormData_cls_compartment;

typedef FormData_cls_compartment *    Form_pg_cls_compartment;
/* the numbers of attribute  */
#define Natts_pg_cls_compartment              4

#define Anum_pg_cls_compartment_polid         1
#define Anum_pg_cls_compartment_compartmentid 2
#define Anum_pg_cls_compartment_shortname     3
#define Anum_pg_cls_compartment_longname      4

#endif   /* PG_CLS_COMPARTMENT_H */
