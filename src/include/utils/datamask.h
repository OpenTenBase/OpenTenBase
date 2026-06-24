#ifndef DATAMASK_H
#define DATAMASK_H

typedef enum
{
    DATA_MASK_SKIP_ALL_INIT = 0,
    DATA_MASK_SKIP_ALL_FALSE= 1,
    DATA_MASK_SKIP_ALL_TRUE = 2,
    DATA_MASK_SKIP_ALL_BUTT
} DATA_MASK_SKIP_ENUM;

extern bool g_enable_data_mask;
extern bool datamask_check_column_in_expr(Node * node, void * context);
extern void datamask_alloc_and_copy(TupleDesc dst, TupleDesc src);
extern void datamask_assign_relation_tupledesc_field(Relation relation);
extern bool datamask_check_datamask_equal(Datamask * dm1, Datamask * dm2);
extern void datamask_free_datamask_struct(Datamask *datamask);
extern void datamask_exchange_all_cols_value_copy(TupleDesc tupleDesc, Datum	*tuple_values, bool*tuple_isnull, Oid relid);
extern bool datamask_check_table_has_datamask(Oid relid);
extern bool datamask_check_table_col_has_datamask(Oid relid, int attnum);
extern bool datamask_check_user_in_white_list(Oid userid);
extern List * datamask_get_user_in_white_list(void);
extern bool datamask_check_user_and_column_in_white_list(Oid relid, Oid userid, int16 attnum);
extern void datamask_exchange_all_cols_value(Node *node, TupleTableSlot *slot, Oid relid);

#endif /*DATAMASK_H*/
