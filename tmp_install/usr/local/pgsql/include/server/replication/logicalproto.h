/*-------------------------------------------------------------------------
 *
 * logicalproto.h
 *        logical replication protocol
 *
 * Copyright (c) 2015-2017, PostgreSQL Global Development Group
 *
 * This source code file contains modifications made by THL A29 Limited ("Tencent Modifications").
 * All Tencent Modifications are Copyright (C) 2023 THL A29 Limited.
 *
 * IDENTIFICATION
 *        src/include/replication/logicalproto.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef LOGICAL_PROTO_H
#define LOGICAL_PROTO_H

#include "replication/reorderbuffer.h"
#include "utils/rel.h"

/*
 * Protocol capabilities
 *
 * LOGICAL_PROTO_VERSION_NUM is our native protocol and the greatest version
 * we can support. PGLOGICAL_PROTO_MIN_VERSION_NUM is the oldest version we
 * have backwards compatibility for. The client requests protocol version at
 * connect time.
 */
#define LOGICALREP_PROTO_MIN_VERSION_NUM 1
#define LOGICALREP_PROTO_VERSION_NUM 1

/* Tuple coming via logical replication. */
typedef struct LogicalRepTupleData
{
    /* column values in text format, or NULL for a null value: */
    char       *values[MaxTupleAttributeNumber];
    /* markers for changed/unchanged column values: */
    bool        changed[MaxTupleAttributeNumber];

#ifdef __SUBSCRIPTION__
    int32        tuple_hash;        /* hash value of this tuple */
#endif
} LogicalRepTupleData;

typedef uint32 LogicalRepRelId;

/* Relation information */
typedef struct LogicalRepRelation
{
    /* Info coming from the remote side. */
    LogicalRepRelId remoteid;    /* unique id of the relation */
    char       *nspname;        /* schema name */
    char       *relname;        /* relation name */
    int            natts;            /* number of columns */
    char      **attnames;        /* column names */
    Oid           *atttyps;        /* column types */
    char        replident;        /* replica identity */
    Bitmapset  *attkeys;        /* Bitmap of key columns */
} LogicalRepRelation;

/* Type mapping info */
typedef struct LogicalRepTyp
{
    Oid            remoteid;        /* unique id of the type */
    char       *nspname;        /* schema name */
    char       *typname;        /* name of the type */
    Oid            typoid;            /* local type Oid */
} LogicalRepTyp;

/* Transaction info */
typedef struct LogicalRepBeginData
{
    XLogRecPtr    final_lsn;
    TimestampTz committime;
    TransactionId xid;
} LogicalRepBeginData;

typedef struct LogicalRepCommitData
{
    XLogRecPtr    commit_lsn;
    XLogRecPtr    end_lsn;
    TimestampTz committime;
} LogicalRepCommitData;

extern void logicalrep_write_begin(StringInfo out, ReorderBufferTXN *txn);
extern void logicalrep_read_begin(StringInfo in,
                      LogicalRepBeginData *begin_data);
extern void logicalrep_write_commit(StringInfo out, ReorderBufferTXN *txn,
                        XLogRecPtr commit_lsn);
extern void logicalrep_read_commit(StringInfo in,
                       LogicalRepCommitData *commit_data);
extern void logicalrep_write_origin(StringInfo out, const char *origin,
                        XLogRecPtr origin_lsn);
extern char *logicalrep_read_origin(StringInfo in, XLogRecPtr *origin_lsn);
extern void logicalrep_write_insert(StringInfo out, Relation rel,
#ifdef __SUBSCRIPTION__
                        int32 tuple_hash,
#endif
                        HeapTuple newtuple);
extern LogicalRepRelId logicalrep_read_insert(StringInfo in, 
#ifdef __SUBSCRIPTION__
                        char **nspname, char **relname, char *replident,
#endif
                        LogicalRepTupleData *newtup);
extern void logicalrep_write_update(StringInfo out, Relation rel, 
#ifdef __SUBSCRIPTION__
                        int32 tuple_hash,
#endif
                        HeapTuple oldtuple,
                        HeapTuple newtuple);
extern LogicalRepRelId logicalrep_read_update(StringInfo in,
#ifdef __SUBSCRIPTION__
                       char **nspname, char **relname, char *replident,
#endif
                       bool *has_oldtuple, LogicalRepTupleData *oldtup,
                       LogicalRepTupleData *newtup);
extern void logicalrep_write_delete(StringInfo out, Relation rel,
#ifdef __SUBSCRIPTION__
                        int32 tuple_hash,
#endif
                        HeapTuple oldtuple);
extern LogicalRepRelId logicalrep_read_delete(StringInfo in,
#ifdef __SUBSCRIPTION__
                       char **nspname, char **relname, char *replident,
#endif
                       LogicalRepTupleData *oldtup);
extern void logicalrep_write_rel(StringInfo out, Relation rel);
extern LogicalRepRelation *logicalrep_read_rel(StringInfo in);
extern void logicalrep_write_typ(StringInfo out, Oid typoid);
extern void logicalrep_read_typ(StringInfo out, LogicalRepTyp *ltyp);

#ifdef __SUBSCRIPTION__
extern void logicalrep_dml_set_hashmod(int32 sub_parallel_number);
extern void logicalrep_dml_set_hashvalue(int32 sub_parallel_index);
extern void logicalrep_dml_set_send_all(bool send_all);

extern int32 logicalrep_dml_get_hashmod(void);
extern int32 logicalrep_dml_get_hashvalue(void);
extern bool  logicalrep_dml_get_send_all(void);
extern int32 logicalrep_dml_calc_hash(Relation rel, HeapTuple tuple);
extern void	logicalrep_relation_free(LogicalRepRelation * rel);
#endif

#endif                            /* LOGICALREP_PROTO_H */
