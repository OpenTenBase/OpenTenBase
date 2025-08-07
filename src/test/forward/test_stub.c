#include "postgres.h"
#include <stdlib.h>
#include "pgxc/squeue.h"
#include <pthread.h>
#include "forward/fnutils.h"
#include "postmaster/forwardmgr.h"
#include "pgxc/nodemgr.h"
#include "storage/proc.h"

int	fn_max_server_count		= 1000;		/* max forward machine count per cluster */
int	fn_max_parallel_query	= 1000;		/* max parallel query per cluster */


void *
fn_malloc(size_t size)
{
	return malloc(size);
}

void *
fn_malloc0(size_t size)
{
    void *p = malloc(size);
    memset(p, 0, size);
    return p;
}


void
fn_free(void *p)
{
	free(p);
}

void
elog_start(const char *filename, int lineno,
#ifdef USE_MODULE_MSGIDS
		int moduleid, int fileid, int msgid,
#endif
		const char *funcname)
{
}

void
elog_finish(int elevel, const char *fmt,...)
{
}


void ThreadSemaInit(ThreadSema *sema, int32 init)
{
	if (sema)
	{
		sema->m_cnt = init;
		pthread_mutex_init(&sema->m_mutex, 0);
		pthread_cond_init(&sema->m_cond, 0);
	}
}

void ThreadSemaDown(ThreadSema *sema)
{            
	if (sema)
	{
		(void)pthread_mutex_lock(&sema->m_mutex);
#if 1
		if (--(sema->m_cnt) < 0) 
		{
			/* thread goes to sleep */

			(void)pthread_cond_wait(&sema->m_cond, &sema->m_mutex);
		}
#endif
		(void)pthread_mutex_unlock(&sema->m_mutex);
	}
}

void ThreadSemaUp(ThreadSema *sema)
{
	if (sema)
	{
		(void)pthread_mutex_lock(&sema->m_mutex);
#if 1
		if ((sema->m_cnt)++ < 0) 
		{
			/*wake up sleeping thread*/                
			(void)pthread_cond_signal(&sema->m_cond);
		}
#endif

		(void)pthread_mutex_unlock(&sema->m_mutex);
	}
}

int32 CreateThread(void *(*f) (void *), void *arg, int32 mode)
{

    pthread_attr_t attr;
    pthread_t      threadid;
    int            ret = 0;

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
    ret = pthread_create(&threadid, &attr, f, arg);
    return ret;
}

void ForwardMgrPrintLog(int level, const char *fmt,...)
{

}

void
PgxcNodeCopyDefs(char node_type, NodeDefinition *fns, int * num_of_fn)
{
}

bool
errstart(int elevel, const char *filename, int lineno,
#ifdef USE_MODULE_MSGIDS
		int moduleid, int fileid, int msgid,
#endif
		 const char *funcname, const char *domain)
{
	return false;
}

int
errmsg(const char *fmt,...)
{
	return 0;
}

int
errcode(int sqlerrcode)
{
	return 0;
}

void
errfinish(int dummy,...)
{
}

PGPROC	   *MyProc = NULL;
int			PGXCNodeId = 0;
bool isPGXCCoordinator = false;
void PgxcNodeCopyfnDefs(NodeDefinition * fns, int * num_of_fn, NodeDefinition * myself)
{
}
