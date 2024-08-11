/*-------------------------------------------------------------------------
 *
 *     dds_dependency.c
 *
 *      Function to manage the dependency information of the current backend
 *
 *
 * Copyright (c) 2019-2024 TeleDB Development Group
 *
 * src/backend/storage/bucket/dds_dependency.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "storage/shmem.h"
#include "storage/spin.h"
#include "pgxc/dds.h"
#include "port/atomics.h"

static Dependent_Trans *PerBucketArray;
Dependent_Bucket *DependentBucketEntry;

static Dependent_Trans *get_and_promote_head(Dependent_Bucket *dep_bucket, int len);
static inline Dependent_Cell *dep_list_head(Dependent_List *dep_list);
static inline int dep_list_length(Dependent_List *dep_list);
static void push_dep_list(Dependent_List *dep_list, Dependent_Cell *dep_cell);
static Dependent_Trans *align_ptr(Dependent_Trans *ptr);
static bool compare_dep_gxid(Dependent_Gxid *cur, Dependent_Gxid *last);

static bool 
compare_dep_gxid(Dependent_Gxid *cur, Dependent_Gxid *last)
{
    return ((0 == strcmp(cur->waiter_gxid, last->waiter_gxid))
            && (0 == strcmp(cur->holder_gxid, last->holder_gxid)));
}

/* copy dependent cell to dependent trans */
static void
copy_cell_to_trans(Dependent_Cell *cell, Dependent_Trans *trans, DepType res)
{
    trans->dep_type = res;
    memcpy(trans->dep_gxid.waiter_gxid, cell->gxid_data.waiter_gxid, NAMEDATALEN);
    memcpy(trans->dep_gxid.holder_gxid, cell->gxid_data.holder_gxid, NAMEDATALEN);
    trans->waiter_xact_start_time = cell->waiter_xact_st_data;
    trans->valid = true;
}

/* 
    For each new dependency pair, traverse the previous dependency information of the current backend, 
    with the same set as ADD and different sets as DELETE
*/
void 
check_dep_in_list(Dependent_List *add_list, Dependent_List *del_list, Dependent_Cell *cell)
{
    bool found;
    Dependent_Cell *cur;
    Dependent_Cell *pre;
    found = false;
    pre = NULL;
    

    if(NULL == add_list || NULL == del_list)
    {
        return;
    }

    /* del_list's head is NULL(have 0 elem) */
    if(dep_list_head(del_list) == NULL)
    {
        push_dep_list(add_list, cell);
        return;
    }

    cur = dep_list_head(del_list);

    while(cur != NULL)
    {
        if(compare_dep_gxid(&cell->gxid_data, &cur->gxid_data))
        {
            found = true;
            if(cur == dep_list_head(del_list))
            {
                del_list->head = del_list->head->next;
                cur->next = NULL;
            }
            else
            {
                pre->next = cur->next;
                cur->next = NULL;
            }
            push_dep_list(add_list, cur);
            /* find same cell, need pfree incoming cell */
            pfree(cell);
            break;
        }
        else
        {
            pre = cur;
            cur = cur->next;
        }
    }
    if(!found)
    {
        push_dep_list(add_list, cell);
    }
}

/* 
    check_dep_in_list the entire Dependent_ Trans_ List, put every piece of information into shared memory
*/
void
push_dep_to_bucket(Dependent_List *list, DepType res, int *bucket_index){
    int len;
    Dependent_Trans *local_head;
    Dependent_Bucket *cur_bucket;
    Dependent_Cell *cell;
    local_head  = NULL;
    cur_bucket = NULL;
    
    if(NULL == list)
    {
        return;
    }

    len = dep_list_length(list);
    if(len == 0)
        return;

    
    cell = dep_list_head(list);

    /* deal with the situation: len > BUCKET_CAPACITY */
    while (len > 0)
    {
        int data_len = len > PER_BUCKET_AVERAGE ? PER_BUCKET_AVERAGE : len;
        len -= data_len;
        while(!local_head)
        {
            cur_bucket = GetDependentBucket(*(bucket_index) % BUCKET_NUM);
            *bucket_index += 1;
            local_head = get_and_promote_head(cur_bucket, data_len);
        }
        while(data_len)
        {
            local_head = get_true_location(cur_bucket, local_head);
            if(!local_head->valid)
            {
                copy_cell_to_trans(cell, local_head, res);
                local_head++;
                cell = cell->next;
                data_len--;
            }
        }
        local_head = NULL;
    }
}

/* 
    Adjust the queue to only retain Elem of type ADD
*/
void 
put_add_to_delete(Dependent_List *add_list, Dependent_List *del_list)
{
    Dependent_Cell *cell;
    Dependent_Cell *tmp;

    if(NULL == add_list || NULL == del_list)
    {
        return;
    }
    cell = dep_list_head(del_list);
    while(cell != NULL)
    {
        tmp = cell;
        cell = cell->next;
        pfree(tmp);
    }
    del_list->head = add_list->head;
    del_list->tail = add_list->tail;
    del_list->length = add_list->length;
    add_list->head = NULL;
    add_list->tail = NULL;
    add_list->length = 0;
}

Size
DependentShmemSize(void)
{
    Size size = 0;
    int i;
    size = mul_size(BUCKET_NUM, sizeof(Dependent_Bucket));
    for(i = 0; i < BUCKET_NUM; i++)
    {
        size = add_size(size, mul_size(sizeof(Dependent_Trans), BUCKET_CAPACITY));
    }
    return size;
}

/* 
 *  Initialize bucket in shared memory 
 */
void 
InitDependentShmem(void){
    Size size = 0;
    bool found;
    int i, j;
    Dependent_Trans *buffer;
    
    size = mul_size(BUCKET_NUM, sizeof(Dependent_Bucket));
    DependentBucketEntry = (Dependent_Bucket *)ShmemInitStruct("Dependent Trans Bucket Entry", size, &found);

    if(!found)
    {
        MemSet(DependentBucketEntry, 0, size);
    }

    size = 0;
    for(i = 0; i < BUCKET_NUM; i++)
    {
        size = add_size(size, mul_size(sizeof(Dependent_Trans), BUCKET_CAPACITY));
    }

    PerBucketArray = (Dependent_Trans *)ShmemInitStruct("Dependent Trans Array", size, &found);
    if(!found)
    {
        PerBucketArray = align_ptr(PerBucketArray);
        
        MemSet(PerBucketArray, 0, size);
        buffer = PerBucketArray;
        for(i = 0; i < BUCKET_NUM; i++)
        {
            DependentBucketEntry[i].head = buffer;
            DependentBucketEntry[i].tail = buffer;
            DependentBucketEntry[i].begin = buffer;                    /* get begin address */
            DependentBucketEntry[i].end = buffer + BUCKET_CAPACITY;    /* get end address */
            SpinLockInit(&(DependentBucketEntry[i].head_lock));
            SpinLockInit(&(DependentBucketEntry[i].tail_lock));
            for(j = 0; j < BUCKET_CAPACITY; j++)
            {
                buffer->valid = false;
                buffer++;
            }
        }
    }
}

Dependent_Trans *
align_ptr(Dependent_Trans *ptr)
{
    uint8_t *aligned_address = (uint8_t *)ptr + ALIGNMENT;
    aligned_address = (uint8_t *)(((uintptr_t)aligned_address + ALIGNMENT - 1) & ~(ALIGNMENT - 1));
    ptr = (Dependent_Trans *)aligned_address;
    return ptr;
}

bool
is_empty_bucket(Dependent_Bucket *dep_bucket)
{
    return dep_bucket->tail == dep_bucket->head;
}

/* Determine if there is still enough len data left in the bucket */
static bool
have_enough_len(Dependent_Bucket *dep_bucket, int len)
{
    return (len  + get_bucket_len(dep_bucket)) <= BUCKET_CAPACITY;
}

/* Because the head and tail are constantly increasing
 * it is necessary to calculate the true position based on the position of the origin 
 */
Dependent_Trans *
get_true_location(Dependent_Bucket *dep_bucket, Dependent_Trans *cur_loc)
{
    return dep_bucket->begin + ((cur_loc - dep_bucket->begin) % BUCKET_CAPACITY);
}

/* 
 * Before inserting data, first obtain the initial position of the head, and then push the head backwards
 * so that the head is always greater than the tail
 */
Dependent_Trans * 
get_and_promote_head(Dependent_Bucket *dep_bucket, int len)
{ 
    Dependent_Trans *local_head = NULL;
    SpinLockAcquire(&dep_bucket->head_lock);
    if(have_enough_len(dep_bucket, len))
    {
        local_head = dep_bucket->head;
        dep_bucket->head += len;
    }
    SpinLockRelease(&dep_bucket->head_lock);
    return local_head;
}

/* Calculate how much data is in the current bucket */
inline int 
get_bucket_len(Dependent_Bucket *dep_bucket)
{
    return dep_bucket->head - dep_bucket->tail;
}

Dependent_List *
new_dep_list(void)
{
    Dependent_List *new_list = (Dependent_List *) palloc(sizeof(Dependent_List));
    if(NULL == new_list)
    {
        return NULL;
    }
    new_list->length = 0;
    new_list->head = NULL;
    new_list->tail = NULL;
    return new_list;
}

void
free_dep_list(Dependent_List *dep_list)
{
    Dependent_Cell *cell;
    Dependent_Cell *tmp;
    if(dep_list == NULL)
        return;
    cell = dep_list->head;
    while (cell != NULL)
    {
        tmp = cell;
        cell = cell->next;
        pfree(tmp);
    }
    dep_list->length = 0;
    pfree(dep_list);
    dep_list = NULL;
}

inline Dependent_Cell *
dep_list_head(Dependent_List *dep_list)
{
    return dep_list->head;
}

inline int 
dep_list_length(Dependent_List *dep_list)
{
    return dep_list->length;
}

void 
push_dep_list(Dependent_List *dep_list, Dependent_Cell *dep_cell)
{   
    if(NULL == dep_list)
    {
        return;
    }
    if (dep_list->head == NULL)
    {
        dep_list->head = dep_cell;
        dep_list->tail = dep_cell;
    }
    else
    {
        dep_list->tail->next = dep_cell;
        dep_list->tail = dep_cell;
    }
    dep_list->length++;
}

DDSQueue *
new_ddsqueue(void)
{
    DDSQueue *dds_q;
    dds_q = (DDSQueue *)malloc(sizeof(DDSQueue));
    if(NULL == dds_q)
    {
        return NULL;
    }
    dds_q->head = NULL;
    dds_q->tail = NULL;
    SpinLockInit(&(dds_q->lock));
    return dds_q;
}

void
free_ddsqueue(DDSQueue *ddsq)
{
    DDSElem *dds_e, *dds_n;
    if (ddsq)
    {
        SpinLockAcquire(&(ddsq->lock));
        dds_e = ddsq->head;
        while (dds_e != NULL)
        {
            if (dds_e->data != NULL)
            {
                free(dds_e->data);
            }
            dds_n = dds_e->next;
            free(dds_e);
            dds_e = dds_n;
        }
        SpinLockRelease(&(ddsq->lock));
        free(ddsq);
    }
}

DDSElem *
pop_ddsqueue(DDSQueue *dds_q)
{
    DDSElem *head;
    if (dds_q)
    {
        SpinLockAcquire(&(dds_q->lock));
        head = dds_q->head;
        if (head)
        {
            dds_q->head = head->next;
        }
        SpinLockRelease(&(dds_q->lock));
        return head;
    }
    return NULL;
}

void 
push_ddsqueue(DDSQueue *dds_q, DDSElem *dds_e)
{
    if (dds_q)
    {
        SpinLockAcquire(&(dds_q->lock));
        if (dds_q->head == NULL)
        {
            dds_q->head = dds_e;
            dds_q->tail = dds_e;
        }
        else
        {
            dds_q->tail->next = dds_e;
            dds_q->tail = dds_e;
        }
        SpinLockRelease(&(dds_q->lock));
    }
}
