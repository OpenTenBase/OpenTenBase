#ifndef PGXC_AUDIT_DEFAULT_H
#define PGXC_AUDIT_DEFAULT_H

#include "catalog/genbki.h"

#define PgAuditObjDefOptsRelationId  8166

CATALOG(pg_audit_obj_def_opts,8166)
{
	Oid			auditor_id;			/* who write this conf */
	int32		action_id;			/* which action to be audited */   
	char		action_mode;		/* when to audit: successful, not successful, all or none */ 
	bool		action_ison;		/* turn on or power off this audit configure */
}FormData_audit_obj_def_opts;

typedef FormData_audit_obj_def_opts *Form_audit_obj_def_opts;

#define Natts_pg_audit_d  					4
#define Natts_audit_obj_def_opts			Natts_pg_audit_d

#define Anum_audit_obj_def_opts_auditor_id	1
#define Anum_audit_obj_def_opts_action_id	2
#define Anum_audit_obj_def_opts_action_mode	3
#define Anum_audit_obj_def_opts_action_ison	4


/* DATA(insert OID = 5100 (10 1 a t)); */
/* DATA(insert OID = 5101 (10 2 f f)); */
/* DATA(insert OID = 5103 (10 3 s t)); */
/* DATA(insert OID = 5104 (10 4 n f)); */

#endif	 /* PGXC_AUDIT_DEFAULT_H */


