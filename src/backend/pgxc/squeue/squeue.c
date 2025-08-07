/*-------------------------------------------------------------------------
 *
 * squeue.c
 *
 *	  Shared queue is for data exchange in shared memory between sessions,
 * one of which is a producer, providing data rows. Others are consumer agents -
 * sessions initiated from other datanodes, the main purpose of them is to read
 * rows from the shared queue and send then to the parent data node.
 *    The producer is usually a consumer at the same time, it sends back tuples
 * to the parent node without putting it to the queue.
 *
 * Copyright (c) 2012-2014, TransLattice, Inc.
 *
 * IDENTIFICATION
 *	  $$
 *
 *
 *-------------------------------------------------------------------------
 */

#include <pthread.h>
#include <sys/stat.h>
#include <sys/time.h>

#include "postgres.h"

#include "access/gtm.h"
#include "pgxc/squeue.h"
#include "libpq/pqsignal.h"

void
ThreadSemaInit(ThreadSema *sema, int32 init, bool timeout)
{
	if (sema)
	{
		sema->m_cnt = init;
		pthread_mutex_init(&sema->m_mutex, 0);
		pthread_cond_init(&sema->m_cond, 0);
		sema->m_timeout = timeout;
	}
}

int
ThreadSemaDown(ThreadSema *sema)
{
	int rc = 0;

	if (sema)
	{
		struct timeval tp;
		struct timespec ts;
		
		if (sema->m_timeout)
		{
			gettimeofday(&tp, NULL);
			/* Convert from timeval to timespec */
			ts.tv_sec  = tp.tv_sec;
			ts.tv_nsec = tp.tv_usec * 1000 + 1000000; /* 1ms */
		}

		(void)pthread_mutex_lock(&sema->m_mutex);

		if (--(sema->m_cnt) < 0) 
		{
			Assert(sema->m_cnt == -1);
			/* thread goes to sleep */
			if (sema->m_timeout)
			{
				rc = pthread_cond_timedwait(&sema->m_cond, &sema->m_mutex, &ts);
				if (rc != 0)
					sema->m_cnt++;
			}
			else
			{
				rc = pthread_cond_wait(&sema->m_cond, &sema->m_mutex);
			}
		}

		(void)pthread_mutex_unlock(&sema->m_mutex);
	}

	return rc;
}

void
ThreadSemaUp(ThreadSema *sema)
{
	if (sema)
	{
		(void)pthread_mutex_lock(&sema->m_mutex);

		if ((sema->m_cnt)++ < 0) 
		{
			/*wake up sleeping thread*/
			(void)pthread_cond_signal(&sema->m_cond);
		}

		(void)pthread_mutex_unlock(&sema->m_mutex);
	}
}

pthread_t
CreateThread(void *(*f) (void *), void *arg, int32 mode, int32 *ret)
{

	pthread_attr_t attr;
	pthread_t	   threadid;

    pthread_attr_init(&attr);
    switch (mode) 
    {
        case MT_THR_JOINABLE:
            {
                pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_JOINABLE);
                break;
            }
        case MT_THR_DETACHED:
            {
                pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED);
                break;
            }        
        default:
            {
                elog(ERROR, "invalid thread mode %d\n", mode);
            }
    }
    *ret = pthread_create(&threadid, &attr, f, arg);
    return threadid;
}

PGPipe *
CreatePipe(uint32 size)
{
	PGPipe *pPipe = NULL;
	pPipe = palloc0(sizeof(PGPipe));
	pPipe->m_List = (void **) palloc0(sizeof(void*) * size);
	pPipe->m_Length = size;
	pPipe->m_Head   = 0;
	pPipe->m_Tail   = 0;
	SpinLockInit(&(pPipe->m_lock));
	return pPipe;
}

void
DestoryPipe(PGPipe *pPipe)
{
	if (pPipe)
	{
		pfree(pPipe->m_List);
		pfree(pPipe);
	}
}

/*
 * Take an elem from the queue for use,
 * and return a null pointer if the queue is empty.
 */
void *
PipeGet(PGPipe *pPipe)
{
    void *ptr = NULL;
	SpinLockAcquire(&(pPipe->m_lock));
    if (pPipe->m_Head == pPipe->m_Tail)
    {
        SpinLockRelease(&(pPipe->m_lock));
        return NULL;                
    }            
    ptr            				 = pPipe->m_List[pPipe->m_Head];
    pPipe->m_List[pPipe->m_Head] = NULL;                
    pPipe->m_Head  				 = (pPipe->m_Head  + 1) % pPipe->m_Length;  
    SpinLockRelease(&(pPipe->m_lock));
    return ptr;
}

/* Puts an elem into the queue, returns non-zero if the queue is full */
int PipePut(PGPipe *pPipe, void *p)
{
    SpinLockAcquire(&(pPipe->m_lock));
    if ((pPipe->m_Tail + 1) % pPipe->m_Length == pPipe->m_Head)
    {
        SpinLockRelease(&(pPipe->m_lock));
        return -1;
    }
    pPipe->m_List[pPipe->m_Tail] = p;
    pPipe->m_Tail = (pPipe->m_Tail  + 1) % pPipe->m_Length;  
    SpinLockRelease(&(pPipe->m_lock));    
    return 0;
}

bool
PipeIsFull(PGPipe *pPipe)
{
    SpinLockAcquire(&(pPipe->m_lock));
    if ((pPipe->m_Tail + 1) % pPipe->m_Length == pPipe->m_Head)
    {
        SpinLockRelease(&(pPipe->m_lock));
        return true;
    }
    else
    {
        SpinLockRelease(&(pPipe->m_lock));
        return false;
    }
}

bool
IsEmpty(PGPipe *pPipe)
{
    SpinLockAcquire(&(pPipe->m_lock));
    if (pPipe->m_Tail == pPipe->m_Head)
    {
        SpinLockRelease(&(pPipe->m_lock));
        return true;
    }
    else
    {
        SpinLockRelease(&(pPipe->m_lock));
        return false;
    }
}

int
PipeLength(PGPipe *pPipe)
{
	int len = -1;
	SpinLockAcquire(&(pPipe->m_lock));
	len = (pPipe->m_Tail - pPipe->m_Head + pPipe->m_Length) % pPipe->m_Length;
	SpinLockRelease(&(pPipe->m_lock));
	return len;
}

#if 0
/*
 * AtXact_SaveSqueueProducer
 */
void
AtXact_SaveSqueueProducer(TransactionExecuteContext *e)
{
	e->dist_query._share_sq = share_sq;
}

void
AtXact_ResetSqueueProducer(void)
{
	share_sq = NULL;
}

/*
 * AtXact_RestoreSqueueProducer
 */
void
AtXact_RestoreSqueueProducer(TransactionExecuteContext *e)
{
	share_sq = e->dist_query._share_sq;
}
#endif

/* ignore all signals, leave signal for main thread to process */
void
ThreadSigmask(void)
{
	sigset_t  new_mask;
	sigemptyset(&new_mask);
	(void) sigaddset(&new_mask, SIGQUIT);
	(void) sigaddset(&new_mask, SIGALRM);
	(void) sigaddset(&new_mask, SIGHUP);
	(void) sigaddset(&new_mask, SIGINT);
	(void) sigaddset(&new_mask, SIGTERM);
	(void) sigaddset(&new_mask, SIGQUIT);
	(void) sigaddset(&new_mask, SIGPIPE);
	(void) sigaddset(&new_mask, SIGUSR1);
	(void) sigaddset(&new_mask, SIGUSR2);
	(void) sigaddset(&new_mask, SIGFPE);
	(void) sigaddset(&new_mask, SIGCHLD);

	/* ignore signals*/
	(void) pthread_sigmask(SIG_BLOCK, &new_mask, NULL);
}
