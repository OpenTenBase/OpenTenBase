#ifndef PG_CLS_LABEL_H
#define PG_CLS_LABEL_H

#include "catalog/genbki.h"

#define ClsLabelRelationId 8176

CATALOG(pg_cls_label,8176) BKI_WITHOUT_OIDS 
{
    int16   polid;
    int16   labelid;
    int16   levelid;
    int16   compartmentid[1];
    int16   groupid[1];
}FormData_cls_label;

typedef FormData_cls_label *    Form_pg_cls_label;

#define Natts_pg_cls_label              5

#define Anum_pg_cls_label_polid         1
#define Anum_pg_cls_label_labelid       2
#define Anum_pg_cls_label_levelid       3
#define Anum_pg_cls_label_compartmentid 4
#define Anum_pg_cls_label_groupid       5

#endif   /* PG_CLS_LABEL_H */
