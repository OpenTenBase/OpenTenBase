/*
 * Copyright (c) 2023 THL A29 Limited, a Tencent company.
 *
 * This source code file is licensed under the BSD 3-Clause License,
 * you may obtain a copy of the License at http://opensource.org/license/bsd-3-clause/
 */
#ifndef _CLS_H_
#define _CLS_H_


typedef enum ClsCmdType
{
    CLS_CMD_UNKNOWN,

    CLS_CMD_READ,                   /* SELECT/COPY TO/SELECT FOR UPDATE/SELECT FOR SHARE */
    CLS_CMD_WRITE,                  /* UPDATE/DELETE/INSERT ON CONFILCT UPDATE  */
    CLS_CMD_ROW,                    /* INSERT/COPY FROM */
    
    CLS_CMD_BUTT
} ClsCmdType;


/* extern api for mls main */
extern void mls_create_cls_check_expr(Relation rel);
extern int mls_command_tag_switch_to(int tag);
extern void mls_reset_command_tag(void);
extern void mls_assign_user_clsitem(void);
extern bool mls_cls_check_row_validation_in_cp(Datum datum);
extern void mls_update_cls_with_current_user(TupleTableSlot *slot);
extern bool mls_cls_column_add_check(char * colname, Oid typoid);
extern bool mls_cls_column_drop_check(char * name);
extern bool cls_check_table_col_has_policy(Oid relid, int attnum);
extern bool cls_check_table_has_policy(Oid relid);
extern bool cls_check_user_has_policy(Oid relid);

#endif
