#ifndef PG_CLS_USER_H
#define PG_CLS_USER_H

#include "catalog/genbki.h"

#define ClsUserRelationId  8184
    
CATALOG(pg_cls_user,8184) BKI_WITHOUT_OIDS 
{
    int16       polid;
    int16       privilege;
    Oid         userid;
    int16       max_read_label;
    int16       max_write_label;
    int16       min_write_label;
    int16       default_read_label;
    int16       default_write_label;
    int16       default_row_label;
    NameData    username;
}FormData_cls_user;

typedef FormData_cls_user *    Form_pg_cls_user;

#define Natts_pg_cls_user                     10

#define Anum_pg_cls_user_polid                1
#define Anum_pg_cls_user_privilege            2
#define Anum_pg_cls_user_userid               3
#define Anum_pg_cls_user_max_read_label       4
#define Anum_pg_cls_user_max_write_label      5
#define Anum_pg_cls_user_min_write_label      6
#define Anum_pg_cls_user_default_read_label   7
#define Anum_pg_cls_user_default_write_label  8
#define Anum_pg_cls_user_default_row_label    9
#define Anum_pg_cls_user_name                 10

#define CLS_USER_PRIV_READ          0x0001
#define CLS_USER_PRIV_FULL          0x0002
#define CLS_USER_PRIV_COMPACCESS    0x0004
#define CLS_USER_PRIV_WRITEUP       0x0008
#define CLS_USER_PRIV_WRITEDOWN     0x0010
#define CLS_USER_PRIV_WRITEACROSS   0x0020

#endif   /* PG_CLS_USER_H */
