/*-------------------------------------------------------------------------
 *
 *     dds_dependency.h
 *
 *      Function definitions for DDS dependency
 *
 *
 * Copyright (c) 2019-2024 TeleDB Development Group
 *
 * src/include/storage/dds_dependency.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef DDS_DEPENDENCY_H
#define DDS_DEPENDENCY_H

#include "storage/s_lock.h"
#include "utils/timestamp.h"

#define BUCKET_NUM 16
#define BUCKET_CAPACITY 1000
#define PER_BUCKET_AVERAGE 100
// #define PADDING_SIZE (256 - (offsetof(Dependent_Trans, Dependent_Trans->dep_gxid) + sizeof(Dependent_Trans->dep_gxid)))
#define PADDING_SIZE 116
#define ALIGNMENT 512

#define GetDependentBucket(id) (&DependentBucketEntry[(id)])   /* get the bucket */
#define listnext(lc)                ((lc)->next)
#define foreach_cell(cell, l)    \
    for ((cell) = dep_list_head(l); (cell) != NULL; (cell) = listnext(cell))

/* used in backend */
typedef struct Dependent_Gxid
{
    char waiter_gxid[NAMEDATALEN];
    char holder_gxid[NAMEDATALEN];
}Dependent_Gxid;

typedef struct Dependent_Cell Dependent_Cell;
typedef slock_t pg_spin_lock;

/* used in backend */
struct Dependent_Cell
{
    Dependent_Gxid gxid_data;
    TimestampTz waiter_xact_st_data;
    struct Dependent_Cell *next;
};

/* used in backend */
typedef struct Dependent_List
{
    int                  length;
    Dependent_Cell    *head;
    Dependent_Cell    *tail;
}Dependent_List;

/* use in shmem bucket */
typedef enum
{
    ADD,
    DELETE
}DepType;

/* use in shmem bucket */

typedef struct Dependent_Trans
{
    bool valid;                        /* judge whether the conflict is valid */
    DepType dep_type;    /* tag whether the conflict is ADD or DETELE */
    Dependent_Gxid dep_gxid;
    TimestampTz waiter_xact_start_time;
    char pad[PADDING_SIZE];
}Dependent_Trans;

typedef struct Dependent_Bucket
{
    Dependent_Trans *head;
    Dependent_Trans *tail;
    Dependent_Trans *begin;
    Dependent_Trans *end;
    pg_spin_lock head_lock;  
    pg_spin_lock tail_lock;
}Dependent_Bucket;

extern Dependent_Bucket *DependentBucketEntry;    /* bucket in shmem */

typedef struct DDSElem DDSElem;
struct DDSElem
{
    void *data;
    DDSElem *next;
};

typedef struct DDSQueue
{
    DDSElem *head;
    DDSElem *tail;
    pg_spin_lock lock;
}DDSQueue;


#endif