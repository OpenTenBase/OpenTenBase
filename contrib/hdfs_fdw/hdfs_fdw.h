#ifndef HDFS_FDW_H
#define HDFS_FDW_H

#include "foreign/foreign.h"
#include "lib/stringinfo.h"
#include "nodes/relation.h"
#include "utils/rel.h"
#include "funcapi.h"

/* Connection options structure */
typedef struct hdfs_opt
{
    char       *host;
    int         port;
    char       *dbname;
    char       *username;
    char       *password;
    char       *table_name;
    int         connect_timeout;
    int         receive_timeout;
    int         fetch_size;
    bool        log_remote_sql;
    int         client_type;
    char       *auth_type;
} hdfs_opt;

/* FDW execution state structure */
typedef struct hdfsFdwExecutionState
{
    char       *query;             /* Remote SQL query */
    MemoryContext batch_cxt;       /* Batch processing memory context */
    bool        query_executed;    /* Whether query has been executed */
    int         con_index;         /* Connection handle */
    Relation    rel;               /* Foreign table relation info */
    List       *retrieved_attrs;   /* Retrieved attributes list */
    AttInMetadata *attinmeta;      /* Attribute metadata */
    hdfs_opt   *options;           /* Connection options */
} hdfsFdwExecutionState;

/* Function declarations */
extern hdfs_opt *hdfs_get_options(Oid foreigntableid);
extern int hdfs_get_connection(ForeignServer *server, hdfs_opt *opt);
extern void hdfs_rel_connection(int con_index);

/* Client functions */
extern bool hdfs_query_execute(int con_index, hdfs_opt *opt, char *query);
extern int hdfs_fetch(int con_index);
extern int hdfs_get_column_count(int con_index);
extern char *hdfs_get_field_as_cstring(int con_index, int idx, bool *is_null);

#endif /* HDFS_FDW_H */
