#ifndef PG_CLS_GROUP_H
#define PG_CLS_GROUP_H

#include "catalog/genbki.h"

#define ClsGroupRelationId  8182

CATALOG(pg_cls_group,8182) BKI_WITHOUT_OIDS 
{
    int16       polid;
    int16       groupid;
    int16       parentid;
    NameData    shortname;
    text        longname;
}FormData_cls_group;

typedef FormData_cls_group *    Form_pg_cls_group;

#define Natts_pg_cls_group              5

#define Anum_pg_cls_group_polid         1
#define Anum_pg_cls_group_groupid       2
#define Anum_pg_cls_group_parentid      3
#define Anum_pg_cls_group_shortname     4
#define Anum_pg_cls_group_longname      5


#endif   /* PG_CLS_GROUP_H */
