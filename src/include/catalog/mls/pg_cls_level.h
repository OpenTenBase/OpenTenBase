#ifndef PG_CLS_LEVEL_H
#define PG_CLS_LEVEL_H

#include "catalog/genbki.h"

#define ClsLevelRelationId  8178

CATALOG(pg_cls_level,8178) BKI_WITHOUT_OIDS 
{
    int16       polid;
    int16       levelid;
    NameData    shortname;
    text        longname;
}FormData_cls_level;

typedef FormData_cls_level *    Form_pg_cls_level;

#define Natts_pg_cls_level              4

#define Anum_pg_cls_level_polid         1
#define Anum_pg_cls_level_levelid       2
#define Anum_pg_cls_level_shortname     3
#define Anum_pg_cls_level_longname      4

#endif   /* PG_CLS_LEVEL_H */
