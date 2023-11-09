/*
 * Copyright (c) 2023 THL A29 Limited, a Tencent company.
 *
 * This source code file is licensed under the BSD 3-Clause License,
 * you may obtain a copy of the License at http://opensource.org/license/bsd-3-clause/
 */
#ifndef DATAMASK_H
#define DATAMASK_H

#include "nodes/execnodes.h"

/*DATA_MASK_SKIP_ENUM*/
typedef enum
{
    DATA_MASK_SKIP_ALL_INIT = 0,
    DATA_MASK_SKIP_ALL_FALSE= 1,
    DATA_MASK_SKIP_ALL_TRUE = 2,
    DATA_MASK_SKIP_ALL_BUTT
} DATA_MASK_SKIP_ENUM;
/*related function declaration*/
extern bool g_enable_data_mask;
extern bool datamask_check_column_in_expr(Node * node, void * context);
extern void datamask_alloc_and_copy(TupleDesc dst, TupleDesc src);
extern void dmask_assgin_relat_tupledesc_fld(Relation relation);
extern bool datamask_check_datamask_equal(Datamask * dm1, Datamask * dm2);
extern void datamask_free_datamask_struct(Datamask *datamask);
extern void dmask_exchg_all_cols_value_copy(TupleDesc tupleDesc, Datum    *tuple_values, bool*tuple_isnull, Oid relid);
extern bool datamask_check_table_has_datamask(Oid relid);
extern bool dmask_check_table_col_has_dmask(Oid relid, int attnum);
extern bool datamask_check_user_in_white_list(Oid userid);
extern List * datamask_get_user_in_white_list(void);
extern bool dmask_chk_usr_and_col_in_whit_list(Oid relid, Oid userid, int16 attnum);
extern void datamask_exchange_all_cols_value(Node *node, TupleTableSlot *slot);
extern bool datamask_scan_key_contain_mask(ScanState *state);
extern DataMaskState *init_datamask_desc(Oid relid, Form_pg_attribute *attrs, Datamask *datamask);


#endif /*DATAMASK_H*/
