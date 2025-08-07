#ifndef PGXC_AUDIT_USER_H
#define PGXC_AUDIT_USER_H

#include "catalog/genbki.h"

#define PgAuditUserConfRelationId  8163

CATALOG(pg_audit_user_conf,8163)
{
	Oid			auditor_id;			/* who write this conf */
	Oid			user_id;			/* which user to be audited*/
	int32		action_id;			/* which action to be audited */  
	char		action_mode;		/* when to audit: successful, not successful or all */	 
	bool		action_ison;		/* turn on or power off this audit configure */
}FormData_audit_user_conf;

typedef FormData_audit_user_conf *Form_audit_user_conf;

#define Natts_pg_audit_u					5
#define Natts_audit_user_conf				Natts_pg_audit_u

#define Anum_audit_user_conf_auditor_id		1
#define Anum_audit_user_conf_user_id		2
#define Anum_audit_user_conf_action_id		3
#define Anum_audit_user_conf_action_mode	4
#define Anum_audit_user_conf_action_ison	5

/* DATA(insert OID = 5120 ( 10 		10    1 a t)); */
/* DATA(insert OID = 5121 ( 10 		10    2 a t)); */
/* DATA(insert OID = 5122 ( 10 		10    3 a t)); */
/* DATA(insert OID = 5123 ( 3373 	3374  2 f f)); */
/* DATA(insert OID = 5124 ( 3373 	3374  3 f f)); */
/* DATA(insert OID = 5125 ( 3373 	3374  4 f f)); */
/* DATA(insert OID = 5126 ( 3373 	3374  5 f f)); */
/* DATA(insert OID = 5127 ( 3374 	3373  3 s t)); */
/* DATA(insert OID = 5128 ( 3375 	3375  4 n f)); */
/* DATA(insert OID = 5129 ( 3377 	4200  5 n t)); */
/* DATA(insert OID = 5130 ( 4200 	3377  6 n f)); */

#endif	 /* PGXC_AUDIT_USER_H */


