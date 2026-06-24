#ifndef PG_DATA_USER_H
#define PG_DATA_USER_H

#include "catalog/genbki.h"

#define DataMaskUserRelationId  8194
    
CATALOG(pg_data_mask_user,8194) BKI_WITHOUT_OIDS 
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

#define Natts_pg_data_mask_user                     7

#define Anum_pg_data_mask_user_relid                1
#define Anum_pg_data_mask_user_userid               2
#define Anum_pg_data_mask_user_attnum               3
#define Anum_pg_data_mask_user_enable               4
#define Anum_pg_data_mask_user_username             5
#define Anum_pg_data_mask_user_nspname              6
#define Anum_pg_data_mask_user_tblname              7


#endif   /* PG_DATA_USER_H */
