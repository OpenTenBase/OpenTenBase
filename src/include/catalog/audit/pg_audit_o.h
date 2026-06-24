#ifndef PGXC_AUDIT_OBJ_H
#define PGXC_AUDIT_OBJ_H

#include "catalog/genbki.h"

#define PgAuditObjConfRelationId  8160

CATALOG(pg_audit_obj_conf,8160)
{
	Oid			auditor_id;			/* who write this conf */
	Oid			class_id;			/* classId of ObjectAddress */
	Oid			object_id;			/* objectId of ObjectAddress */
	int32		object_sub_id;		/* objectSubId of ObjectAddress */
	int32		action_id;			/* which action to be audited */   
	char		action_mode;		/* when to audit: successful, not successful or all */	 
	bool		action_ison;		/* turn on or power off this audit configure */
}FormData_audit_obj_conf;

typedef FormData_audit_obj_conf *Form_audit_obj_conf;

#define Natts_pg_audit_o					7
#define Natts_audit_obj_conf				Natts_pg_audit_o

#define Anum_audit_obj_conf_auditor_id		1
#define Anum_audit_obj_conf_class_id		2
#define Anum_audit_obj_conf_object_id		3
#define Anum_audit_obj_conf_object_sub_id	4
#define Anum_audit_obj_conf_action_id		5
#define Anum_audit_obj_conf_action_mode		6
#define Anum_audit_obj_conf_action_ison		7

/* DATA(insert OID = 5105 ( 10 		1259	11610 	3	1	a	t)); */
/* DATA(insert OID = 5106 ( 3373 	1259	11610	0	1	f 	f)); */
/* DATA(insert OID = 5107 ( 3373 	1259	11632	2	3	f 	f)); */
/* DATA(insert OID = 5108 ( 3374 	1247	114		0	3 	s 	t)); */
/* DATA(insert OID = 5109 ( 3375 	1247	3361	0	4 	n 	f)); */
/* DATA(insert OID = 5110 ( 3377 	1255	1242	0	5 	n 	t)); */
/* DATA(insert OID = 5111 ( 4200 	2617	95		0	6 	n 	f)); */

#endif	 /* PGXC_AUDIT_OBJ_H */


