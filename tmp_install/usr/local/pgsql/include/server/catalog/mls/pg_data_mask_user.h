/*
 * Copyright (c) 2023 THL A29 Limited, a Tencent company.
 *
 * This source code file is licensed under the BSD 3-Clause License,
 * you may obtain a copy of the License at http://opensource.org/license/bsd-3-clause/
 */
#ifndef PG_DATA_USER_H
#define PG_DATA_USER_H

#include "catalog/genbki.h"
/* oid */
#define DataMaskUserRelationId  6123

CATALOG(pg_data_mask_user,6123) BKI_WITHOUT_OIDS 
{
    Oid         relid;
    Oid         userid;
    int16       attnum;
    bool        enable;
    NameData    username;
    NameData    nspname;
    NameData    tblname;
}FormData_data_mask_user;

typedef FormData_data_mask_user *    Form_pg_data_mask_user;
/* the numbers of attribute  */
#define Natts_pg_data_mask_user                     7
/* the number of attribute  */
#define Anum_pg_data_mask_user_relid                1
#define Anum_pg_data_mask_user_userid               2
#define Anum_pg_data_mask_user_attnum               3
#define Anum_pg_data_mask_user_enable               4
#define Anum_pg_data_mask_user_username             5
#define Anum_pg_data_mask_user_nspname              6
#define Anum_pg_data_mask_user_tblname              7


#endif   /* PG_DATA_USER_H */
