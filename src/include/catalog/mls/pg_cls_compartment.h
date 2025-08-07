#ifndef PG_CLS_COMPARTMENT_H
#define PG_CLS_COMPARTMENT_H

#include "catalog/genbki.h"

#define ClsCompartmentRelationId  8180

CATALOG(pg_cls_compartment,8180) BKI_WITHOUT_OIDS 
{
    int16       polid;
    int16       compartmentid;
    NameData    shortname;
    text        longname;
}FormData_cls_compartment;

typedef FormData_cls_compartment *    Form_pg_cls_compartment;

#define Natts_pg_cls_compartment              4

#define Anum_pg_cls_compartment_polid         1
#define Anum_pg_cls_compartment_compartmentid 2
#define Anum_pg_cls_compartment_shortname     3
#define Anum_pg_cls_compartment_longname      4

#endif   /* PG_CLS_COMPARTMENT_H */
