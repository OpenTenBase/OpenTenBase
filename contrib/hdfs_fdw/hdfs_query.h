#ifndef HDFS_QUERY_H
#define HDFS_QUERY_H

#include "hdfs_fdw.h"

/* Query processing functions */
extern HeapTuple hdfs_build_tuple_from_result_row(int con_index, AttInMetadata *attinmeta);

#endif /* HDFS_QUERY_H */
