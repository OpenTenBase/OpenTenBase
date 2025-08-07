#ifndef PG_CLS_POLICY_H
#define PG_CLS_POLICY_H

#include "catalog/genbki.h"

#define ClsPolicyRelationId  8172

CATALOG(pg_cls_policy,8172) BKI_WITHOUT_OIDS 
{
    int16       polid;
    bool        enable;
    NameData    polname;
    text        reloptions[1];
}FormData_cls_policy;

typedef FormData_cls_policy * Form_pg_cls_policy;

#define Natts_pg_cls_policy             4

#define Anum_pg_cls_policy_polid        1
#define Anum_pg_cls_policy_enable       2
#define Anum_pg_cls_policy_polname      3
#define Anum_pg_cls_policy_reloption    4

#endif   /* PG_CLS_POLICY_H */


