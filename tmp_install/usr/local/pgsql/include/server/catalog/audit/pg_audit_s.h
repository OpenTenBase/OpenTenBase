/*
 * Copyright (c) 2023 THL A29 Limited, a Tencent company.
 *
 * This source code file is licensed under the BSD 3-Clause License,
 * you may obtain a copy of the License at http://opensource.org/license/bsd-3-clause/
 */
#ifndef PGXC_AUDIT_STMT_H
#define PGXC_AUDIT_STMT_H

#include "catalog/genbki.h"

#define PgAuditStmtConfRelationId  5017

CATALOG(pg_audit_stmt_conf,5017)
{
    Oid            auditor_id;            /* who write this conf */
    int32        action_id;            /* which action to be audited */  
    char        action_mode;        /* when to audit: successful, not successful or all */     
    bool        action_ison;        /* turn on or power off this audit configure */
}FormData_audit_stmt_conf;

typedef FormData_audit_stmt_conf *Form_audit_stmt_conf;

#define Natts_pg_audit_s                    4
#define Natts_audit_stmt_conf                Natts_pg_audit_s

#define Anum_audit_stmt_conf_auditor_id        1
#define Anum_audit_stmt_conf_action_id        2
#define Anum_audit_stmt_conf_action_mode    3
#define Anum_audit_stmt_conf_action_ison    4

/* DATA(insert  OID = 5112 ( 10      1 a t)); */
/* DATA(insert  OID = 5113 ( 10      2 a t)); */
/* DATA(insert  OID = 5114 ( 10      3 a t)); */
/* DATA(insert  OID = 5115 ( 3373      7 f f)); */
/* DATA(insert  OID = 5116 ( 3374      8 s t)); */
/* DATA(insert  OID = 5117 ( 3375      4 n f)); */
/* DATA(insert  OID = 5118 ( 3377      5 n t)); */
/* DATA(insert  OID = 5119 ( 4200      6 n f)); */

#endif     /* PGXC_AUDIT_STMT_H */


