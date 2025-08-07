#ifndef PG_DATA_MASK_H
#define PG_DATA_MASK_H

#include "catalog/genbki.h"

#define DataMaskMapRelationId  8192
    
CATALOG(pg_data_mask_map,8192) BKI_WITHOUT_OIDS 
{
    Oid         relid;
    int16       attnum;
    bool        enable;
    int32       option;     /*see DATAMASK_KIND_VALUE, DATAMASK_KIND_STR_PREFIX, DATAMASK_KIND_STR_POSTFIX */
    int64       datamask;
    NameData    nspname;
    NameData    tblname;
    Oid         maskfunc;   /* use for user define object types */
    text        defaultval; /* keep default val */
}FormData_data_mask_map;

typedef FormData_data_mask_map *    Form_pg_data_mask_map;

#define Natts_pg_data_mask_map                     9

#define Anum_pg_data_mask_map_relid                1
#define Anum_pg_data_mask_map_attnum               2
#define Anum_pg_data_mask_map_enable               3
#define Anum_pg_data_mask_map_option               4
#define Anum_pg_data_mask_map_datamask             5
#define Anum_pg_data_mask_map_nspname              6
#define Anum_pg_data_mask_map_tblname              7
#define Anum_pg_data_mask_map_maskfunc_oid         8
#define Anum_pg_data_mask_map_defaultval           9


#endif   /* PG_DATA_MASK_H */
