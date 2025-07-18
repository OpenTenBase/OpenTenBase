#include "postgres.h"
#include "hdfs_fdw.h"
#include "hdfs_query.h"
#include "hdfs_client.h"
#include "utils/builtins.h"

/*
 * Build a tuple from current result row
 */
HeapTuple
hdfs_build_tuple_from_result_row(int con_index, AttInMetadata *attinmeta)
{
    int column_count;
    char **values;
    HeapTuple tuple;
    int i;

    /* Get column count */
    column_count = hdfs_get_column_count(con_index);
    
    /* Allocate value array */
    values = (char **) palloc(sizeof(char *) * column_count);
    
    /* Get each field value */
    for (i = 0; i < column_count; i++)
    {
        bool is_null;
        char *value = hdfs_get_field_as_cstring(con_index, i, &is_null);
        values[i] = is_null ? NULL : value;
    }
    
    /* Build the tuple */
    tuple = BuildTupleFromCStrings(attinmeta, values);
    
    /* Clean up */
    pfree(values);
    
    return tuple;
}
