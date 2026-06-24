#ifndef TDX_LOGICAL_PROTO_H_
#define TDX_LOGICAL_PROTO_H_
#include "lib/stringinfo.h"

void logical_write_delete_op(StringInfo out, char **values, bool *isnull, int nfields, bool only_key);
void logical_write_insert_op(StringInfo out, char **values, bool *isnull, int nfields);
void logical_write_update_op(StringInfo out, char **old_values, bool *old_isnull, char **new_values, bool *new_isnull, int nfields, bool only_key);
#endif 
