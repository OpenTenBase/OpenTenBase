/*-------------------------------------------------------------------------
 *
 * gtm_lock.h
 *
 *
 * Portions Copyright (c) 1996-2009, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 *
 * This source code file contains modifications made by THL A29 Limited ("Tencent Modifications").
 * All Tencent Modifications are Copyright (C) 2023 THL A29 Limited.
 *
 * $PostgreSQL$
 *
 *-------------------------------------------------------------------------
 */

#ifndef GTM_LOCK_H
#define GTM_LOCK_H


#include <pthread.h>

#define GTM_RWLOCK_FLAG_STORE 0x01

typedef struct GTM_RWLock
{
#ifdef __XLOG__
    int              lock_flag;
#endif
    pthread_rwlock_t lk_lock;
#ifdef GTM_LOCK_DEBUG
#define GTM_LOCK_DEBUG_MAX_READ_TRACKERS    1024
    pthread_mutex_t    lk_debug_mutex;
    int                wr_waiters_count;
    pthread_t        wr_waiters[GTM_LOCK_DEBUG_MAX_READ_TRACKERS];
    bool            wr_granted;
    pthread_t        wr_owner;
    int                rd_holders_count;
    bool            rd_holders_overflow;
    pthread_t        rd_holders[GTM_LOCK_DEBUG_MAX_READ_TRACKERS];
    int                rd_waiters_count;
    bool            rd_waiters_overflow;
    pthread_t        rd_waiters[GTM_LOCK_DEBUG_MAX_READ_TRACKERS];
#endif
} GTM_RWLock;

typedef struct GTM_MutexLock
{
    pthread_mutex_t lk_lock;
} GTM_MutexLock;

typedef enum GTM_LockMode
{
    GTM_LOCKMODE_WRITE,
    GTM_LOCKMODE_READ
} GTM_LockMode;

typedef struct GTM_CV
{
    pthread_cond_t    cv_condvar;
} GTM_CV;

extern bool GTM_RWLockAcquire(GTM_RWLock *lock, GTM_LockMode mode);
extern bool GTM_RWLockRelease(GTM_RWLock *lock);
#ifdef __XLOG__
extern int GTM_RWLockInit(GTM_RWLock *lock);
#else
extern int GTM_RWLockInit(GTM_RWLock *lock);
#endif
extern int GTM_RWLockDestroy(GTM_RWLock *lock);
extern bool GTM_RWLockConditionalAcquire(GTM_RWLock *lock, GTM_LockMode mode);

extern bool GTM_MutexLockAcquire(GTM_MutexLock *lock);
extern bool GTM_MutexLockRelease(GTM_MutexLock *lock);
extern int GTM_MutexLockInit(GTM_MutexLock *lock);
extern int GTM_MutexLockDestroy(GTM_MutexLock *lock);
extern bool GTM_MutexLockConditionalAcquire(GTM_MutexLock *lock);

extern int GTM_CVInit(GTM_CV *cv);
extern int GTM_CVDestroy(GTM_CV *cv);
extern int GTM_CVSignal(GTM_CV *cv);
extern int GTM_CVBcast(GTM_CV *cv);
extern int GTM_CVWait(GTM_CV *cv, GTM_MutexLock *lock);
extern int GTM_CVTimeWait(GTM_CV *cv, GTM_MutexLock *lock,int micro_seconds);

typedef int s_lock_t;
extern void SpinLockInit(s_lock_t *lock);

extern void SpinLockAcquire(s_lock_t *lock);

extern void SpinLockRelease(s_lock_t *lock);


typedef struct 
{
    void                 **q_list; 
    uint32               q_length; 
    s_lock_t              q_lock;  
    volatile uint32      q_head;   
    volatile uint32      q_tail;  
}GTM_Queue;
extern GTM_Queue* CreateQueue(uint32 size);
extern void    DestoryQueue(GTM_Queue *queue);
extern void    *QueuePop(GTM_Queue *queue);
extern int     QueueEnq(GTM_Queue *queue, void *p);
extern bool    QueueIsFull(GTM_Queue *queue);
extern bool    QueueIsEmpty(GTM_Queue *queue);
extern int        QueueLength(GTM_Queue *queue);

#ifdef __OPENTENBASE__
extern void    RWLockCleanUp(void);
#endif
#endif
