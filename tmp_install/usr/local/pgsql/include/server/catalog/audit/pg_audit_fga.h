/*
 * Copyright (c) 2023 THL A29 Limited, a Tencent company.
 *
 * This source code file is licensed under the BSD 3-Clause License,
 * you may obtain a copy of the License at http://opensource.org/license/bsd-3-clause/
 */
#ifndef PGXC_AUDIT_FGA_H
#define PGXC_AUDIT_FGA_H

#include "catalog/genbki.h"

#define PgAuditFgaConfRelationId  5050

CATALOG(pg_audit_fga_conf,5050) BKI_WITHOUT_OIDS
{
    Oid                auditor_id;            /* who write this conf */
    Oid             object_schema;      /* object schema oid */
    Oid                object_id;            /* which object to be audited, include view and table */
    NameData        policy_name;        /* the unique name of the policy */
    oidvector       audit_column_ids;   /* the column oids to be checked for access */
    text            audit_columns;      /*  the columns to be checked for access */
#ifdef CATALOG_VARLEN    
    pg_node_tree    audit_condition;    /*  a condition in a row that indicates a monitoring condition */
#endif
    text            audit_condition_str;/*  a condition in a row that indicates a monitoring condition */
    Oid             handler_schema;     /* the schema that contains the event handler */
    Oid             handler_module;     /* the function name of the event handler */
    bool            audit_enable;       /* enables the policy if TRUE, which is the default */
    NameData        statement_types;    /* the SQL statement types to which this policy is applicable: INSERT, UPDATE, DELETE, or SELECT only */
    bool            audit_column_opts;  /* audited when the query references any column specified in the audit_column parameter or only when all such columns are referenced. 0: any column, 1: all column */
}FormData_audit_fga_conf;

typedef FormData_audit_fga_conf *Form_audit_fga_conf;

#define Natts_pg_audit_fga                    13
#define Natts_audit_fga_conf                Natts_pg_audit_fga

#define Anum_audit_fga_conf_auditor_id                1
#define Anum_audit_fga_conf_object_schema            2
#define Anum_audit_fga_conf_object_id                3
#define Anum_audit_fga_conf_policy_name                4
#define Anum_audit_fga_conf_audit_column_ids        5
#define Anum_audit_fga_conf_audit_columns            6
#define Anum_audit_fga_conf_audit_condition            7
#define Anum_audit_fga_conf_audit_condition_str        8
#define Anum_audit_fga_conf_handler_schema            9
#define Anum_audit_fga_conf_handler_module            10
#define Anum_audit_fga_conf_audit_enable            11
#define Anum_audit_fga_conf_statement_types            12
#define Anum_audit_fga_conf_audit_column_opts        13

#endif     /* PGXC_AUDIT_FGA_H */


