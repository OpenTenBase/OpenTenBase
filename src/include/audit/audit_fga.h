/*
 * Copyright (c) 2023 THL A29 Limited, a Tencent company.
 *
 * This source code file is licensed under the BSD 3-Clause License,
 * you may obtain a copy of the License at http://opensource.org/license/bsd-3-clause
 * 
 */
#ifndef __PGXC_AUDIT_FGA__H
#define __PGXC_AUDIT_FGA__H

#include "postgres.h"
#include "nodes/nodes.h"
#include "nodes/parsenodes.h"
#include "nodes/execnodes.h"

#define AUDIT_FGA_SQL_LEN   4096
#define AUDIT_TRIGGER_FEEDBACK_LEN  256

extern bool enable_fga;
extern const char *g_commandTag;


/* simple list of strings */
typedef struct _stringlist
{
    char       *str;
    struct _stringlist *next;
} _stringlist;

typedef struct AuditFgaPolicy
{
    NodeTag     type;
    char       *policy_name;    /* Name of the policy */
    //Expr       *qual;            /* Expression to audit condition */
    List       *qual;            /* Expression to audit condition */
    char       *query_string;
} AuditFgaPolicy;

typedef struct audit_fga_policy_state
{
    char       *policy_name;    /* Name of the policy */
    ExprState  *qual;            /* Expression to audit condition */
    char       *query_string;
} audit_fga_policy_state;

typedef enum exec_status
{
    FGA_STATUS_INIT = 0,
    FGA_STATUS_OK,
    FGA_STATUS_DOING,
    FGA_STATUS_FAIL
} exec_status;


typedef struct audit_fga_tigger_info
{
    int     backend_pid;
    char    user_name[NAMEDATALEN];
    char    db_name[NAMEDATALEN];
    char    host[32];
    char    port[32];    
    Oid     handler_module;
    char    func_name[NAMEDATALEN];
    int     status;
    char    exec_feedback[AUDIT_TRIGGER_FEEDBACK_LEN];
} audit_fga_tigger_info;

/*related function declaration*/
extern Oid schema_name_2_oid(text *in_string);
extern Oid object_name_2_oid(text *in_string, Oid schema_oid);
extern Oid function_name_2_oid(text *in_string, Oid schema_oid);
extern Datum text_2_namedata_datum(text *in_string);
extern void exec_policy_funct_on_other_node(char *query_string);

extern bool has_policy_matched_cmd(char * cmd_type, Datum statement_types_datum, bool is_null);
extern bool has_policy_matched_columns(List * tlist, oidvector *audit_column_oids, bool audit_column_opts);
extern bool get_audit_fga_quals(Oid rel, char * cmd_type, List *tlist, List **audit_fga_policy_list);
extern void audit_fga_log_policy_info(AuditFgaPolicy *policy_s, char * cmd_type);
extern void audit_fga_log_policy_info_2(audit_fga_policy_state *policy_s, char * cmd_type);

extern void audit_fga_log_prefix(StringInfoData *buf);
extern void audit_fga_log_prefix_json(StringInfoData *buf);

extern void ApplyAuditFgaMain(Datum main_arg);
extern void ApplyAuditFgaRegister(void);
extern Size AuditFgaShmemSize(void);
extern void AuditFgaShmemInit(void);
extern void write_trigger_handle_to_shmem(Oid func);


#endif  /*AUDIT_FGA_H*/

