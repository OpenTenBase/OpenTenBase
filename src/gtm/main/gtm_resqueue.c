/*-------------------------------------------------------------------------
 *
 * gtm_resqueue.c
 *	ResQueue handling on GTM
 *
 * Portions Copyright (c) 2012-2014, TransLattice, Inc.
 * Portions Copyright (c) 1996-2009, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 *
 *
 * IDENTIFICATION
 *	  $PostgreSQL$
 *
 *-------------------------------------------------------------------------
 */
#include <unistd.h>
#include <sys/epoll.h>

#include <gtm/gtm_xlog.h>

#include "gtm/assert.h"
#include "gtm/elog.h"
#include "gtm/gtm.h"
#include "gtm/gtm_client.h"
#include "gtm/gtm_resqueue.h"
#include "gtm/gtm_serialize.h"
#include "gtm/gtm_standby.h"
#include "gtm/standby_utils.h"
#include "gtm/libpq.h"
#include "gtm/libpq-int.h"
#include "gtm/pqformat.h"
#include "gtm/gtm_backup.h"
#include "gtm/gtm_store.h"

typedef struct GTM_ResQInfoHashBucket
{
	gtm_List   *rhb_list;
	GTM_RWLock	rhb_lock;
} GTM_ResQueueInfoHashBucket;

extern GTM_ThreadInfo	*g_resq_manager_thread;

#define RESQUEUE_HASH_TABLE_SIZE	GTM_STORED_HASH_TABLE_NBUCKET
static GTM_ResQueueInfoHashBucket GTMResQueues[RESQUEUE_HASH_TABLE_SIZE];

static GTM_ResQueueInfo GTMDefaultResQueue;

static uint32 resq_gethash(GTM_ResQueueKey key);
static bool resq_keys_equal(GTM_ResQueueKey key1, GTM_ResQueueKey key2);
static void resq_lock_HashBucket(GTM_ResQueueKey resqkey, GTM_LockMode mode);
static void resq_lock_HashBucket_write(GTM_ResQueueKey resqkey);
static void resq_unlock_HashBucket(GTM_ResQueueKey resqkey);
static GTM_ResQueueInfo *resq_find_resqinfo(GTM_ResQueueKey resqkey);
static int resq_release_resqinfo(GTM_ResQueueInfo *resqinfo);
static int resq_add_resqinfo(GTM_ResQueueInfo *resqinfo);
static int resq_remove_resqinfo(GTM_ResQueueInfo *resqinfo);
static void resq_free_resqinfo(GTM_ResQueueInfo *resqinfo);
static GTM_ResQueueKey resq_copy_key(GTM_ResQueueKey key_from, GTM_ResQueueKey key_to);
static int resq_acquire_resource(Port *myport,
						GTM_ResQueueInfo *resqinfo,
						GTM_ResQRequestData * request);
static int resq_release_resource(Port *myport,
						GTM_ResQueueInfo *resqinfo,
						GTM_ResQRequestData * request);
static int resq_clean_resource(Port *myport, 
						GTM_ResQueueInfo *resqinfo);
static int resq_ack_client(Port *myport, GTM_ResQueueInfo *resqinfo,
							GTM_ResultType result, int32 errcode);

/*
 * Get the hash value given the resquence key
 *
 * XXX This should probably be replaced by a better hash function.
 */
static uint32
resq_gethash(GTM_ResQueueKey key)
{
	uint32 total = 0;
	int ii = 0;

	for (ii = 0; ii < strnlen(key->data, NAMEDATALEN); ii++)
		total += key->data[ii];
	return (total % RESQUEUE_HASH_TABLE_SIZE);
}

/*
 * Return true if both keys are equal, else return false
 */
static bool
resq_keys_equal(GTM_ResQueueKey key1, GTM_ResQueueKey key2)
{
	size_t	len1 = 0;
	size_t	len2 = 0;

	Assert(key1);
	Assert(key2);

	len1 = strnlen(key1->data, NAMEDATALEN);
	len2 = strnlen(key2->data, NAMEDATALEN);

	if (len1 != len2) return false;

	return (strncmp(key1->data, key2->data, Min(len1, len2)) == 0);
}

static void resq_lock_HashBucket(GTM_ResQueueKey resqkey, GTM_LockMode mode)
{
	uint32 hash = 0;
	GTM_ResQueueInfoHashBucket *bucket = NULL;

	hash = resq_gethash(resqkey);
	bucket = &GTMResQueues[hash];

	GTM_RWLockAcquire(&bucket->rhb_lock, mode);
}

static void resq_lock_HashBucket_write(GTM_ResQueueKey resqkey)
{
	resq_lock_HashBucket(resqkey, GTM_LOCKMODE_WRITE);
}

static void resq_unlock_HashBucket(GTM_ResQueueKey resqkey)
{
	uint32 hash = 0;
	GTM_ResQueueInfoHashBucket *bucket = NULL;

	hash = resq_gethash(resqkey);
	bucket = &GTMResQueues[hash];

	GTM_RWLockRelease(&bucket->rhb_lock);
}

/*
 * Find the resqinfo structure for the given key. The reference count is
 * incremented before structure is returned. The caller must release the
 * reference to the structure when done with it
 */
static GTM_ResQueueInfo *
resq_find_resqinfo(GTM_ResQueueKey resqkey)
{
	uint32 hash = resq_gethash(resqkey);
	GTM_ResQueueInfoHashBucket *bucket = NULL;
	gtm_ListCell *elem = NULL;
	GTM_ResQueueInfo *curr_resqinfo = NULL;

	bucket = &GTMResQueues[hash];

	gtm_foreach(elem, bucket->rhb_list)
	{
		curr_resqinfo = (GTM_ResQueueInfo *) gtm_lfirst(elem);
		if (resq_keys_equal(&curr_resqinfo->resq_name, resqkey))
			break;
		curr_resqinfo = NULL;
	}

	if (curr_resqinfo != NULL)
	{
		GTM_RWLockAcquire(&curr_resqinfo->resq_lock, GTM_LOCKMODE_WRITE);
		if (curr_resqinfo->resq_state != RESQUEUE_STATE_ACTIVE)
		{
			GTM_RWLockRelease(&curr_resqinfo->resq_lock);
			elog(LOG, "Resource Queue not active");
			return NULL;
		}

		Assert(curr_resqinfo->resq_ref_count != RESQUEUE_MAX_REFCOUNT);
		curr_resqinfo->resq_ref_count++;

		GTM_RWLockRelease(&curr_resqinfo->resq_lock);
	}

	if (enable_gtm_resqueue_debug)
	{
		if (curr_resqinfo)
		{
			elog(LOG, "resq_find_resqinfo resq: %s resq:%d  resq_ref_count:%d done",
				NameStr(curr_resqinfo->resq_name),
				curr_resqinfo->resq_store_handle,
				curr_resqinfo->resq_ref_count);
		}
	}

	return curr_resqinfo;
}

/*
 * Release previously grabbed reference to the structure. If the structure is
 * marked for deletion, it will be removed from the global array and released
 */
static int
resq_release_resqinfo(GTM_ResQueueInfo *resqinfo)
{
	bool remove = false;

	if (enable_gtm_resqueue_debug)
	{
		elog(LOG, "resq_release_resqinfo resq: %s resq:%d resq_ref_count:%d begin",
			NameStr(resqinfo->resq_name),
			resqinfo->resq_store_handle,
			resqinfo->resq_ref_count);
	}

	GTM_RWLockAcquire(&resqinfo->resq_lock, GTM_LOCKMODE_WRITE);
	Assert(resqinfo->resq_ref_count > 0);
	resqinfo->resq_ref_count--;

	if ((resqinfo->resq_state == RESQUEUE_STATE_DELETED) &&
		(resqinfo->resq_ref_count == 0))
	{
		remove = true;
	}

	GTM_RWLockRelease(&resqinfo->resq_lock);

	if (enable_gtm_resqueue_debug)
	{
		elog(LOG, "resq_release_resqinfo resq: %s resq:%d, resq_ref_count:%d remove:%d done",
			NameStr(resqinfo->resq_name),
			resqinfo->resq_store_handle,
			resqinfo->resq_ref_count,
			remove);
	}

	/*
	 * Remove the structure from the global hash table
	 */
	if (remove) 
	{
		resq_remove_resqinfo(resqinfo);
	}
	
	return 0;
}

/*
 * Add a resqinfo structure to the global hash table.
 */
static int
resq_add_resqinfo(GTM_ResQueueInfo *resqinfo)
{
	uint32 hash = resq_gethash(&resqinfo->resq_name);
	GTM_ResQueueInfoHashBucket	*bucket = NULL;
	gtm_ListCell *elem = NULL;

	bucket = &GTMResQueues[hash];

	gtm_foreach(elem, bucket->rhb_list)
	{
		GTM_ResQueueInfo *curr_resqinfo = NULL;
		curr_resqinfo = (GTM_ResQueueInfo *) gtm_lfirst(elem);

		if (resq_keys_equal(&curr_resqinfo->resq_name, &resqinfo->resq_name))
		{
			ereport(LOG,
					(EEXIST,
					 errmsg("Resource Queue with the given key already exists")));
			return EEXIST;
		}
	}

	/*
	 * Safe to add the structure to the list
	 */
	bucket->rhb_list = gtm_lappend(bucket->rhb_list, resqinfo);
	
	if (enable_gtm_resqueue_debug)
	{
		elog(LOG, "resq_add_resqinfo resq:%s add to bucket:%d, resq:%d",
			NameStr(resqinfo->resq_name), hash, resqinfo->resq_store_handle);
	}

	return 0;
}

/*
 * Remove the resqinfo structure from the global hash table. If the structure is
 * currently referenced by some other thread, just mark the structure for
 * deletion and it will be deleted by the final reference is released.
 */
static int
resq_remove_resqinfo(GTM_ResQueueInfo *resqinfo)
{
	int32 ret = 0;
	uint32 hash = resq_gethash(&resqinfo->resq_name);
	GTM_ResQueueInfoHashBucket	*bucket = NULL;

	bucket = &GTMResQueues[hash];

	if (enable_gtm_resqueue_debug)
	{
		elog(LOG, "resq_remove_resqinfo remove resq:%s from bucket:%d, resq:%d, resq_ref_count:%d, begin", 
			NameStr(resqinfo->resq_name), hash, resqinfo->resq_store_handle, resqinfo->resq_ref_count);
	}
	
	GTM_RWLockAcquire(&resqinfo->resq_lock, GTM_LOCKMODE_WRITE);

	if (resqinfo->resq_ref_count > 1)
	{
		resqinfo->resq_state = RESQUEUE_STATE_DELETED;
		GTM_RWLockRelease(&resqinfo->resq_lock);
		return EBUSY;
	}

	bucket->rhb_list = gtm_list_delete(bucket->rhb_list, resqinfo);
	GTM_RWLockRelease(&resqinfo->resq_lock);
	
	if (enable_gtm_resqueue_debug)
	{
		elog(LOG, "resq_remove_resqinfo remove resq:%s from bucket:%d, resq:%d, resq_ref_count:%d, done",
			NameStr(resqinfo->resq_name), hash, resqinfo->resq_store_handle, resqinfo->resq_ref_count);
	}

	ret = GTM_StoreDropResQueue(resqinfo->resq_store_handle);
	if (ret)
	{
		ereport(LOG, (ENOMEM, errmsg("resq_remove_resqinfo GTM_StoreDropResQueue %s failed", NameStr(resqinfo->resq_name))));
	}			

	return 0;
}

static int 
resq_ack_client(Port *myport, GTM_ResQueueInfo *resqinfo, GTM_ResultType result, int32 errcode)
{	
	StringInfoData buf;
	MemSet(&buf, 0, sizeof(buf));

	/* Respond to the client */
	pq_beginmessage(&buf, 'S');
	pq_sendint(&buf, result, 4);
	if (myport->remote_type == GTM_NODE_GTM_PROXY)
	{
		GTM_ProxyMsgHeader proxyhdr;
		proxyhdr.ph_conid = myport->conn_id;
		pq_sendbytes(&buf, (char *)&proxyhdr, sizeof (GTM_ProxyMsgHeader));
	}

	pq_sendbytes(&buf, resqinfo->resq_name.data, NAMEDATALEN);
	pq_sendbytes(&buf, resqinfo->resq_group.data, NAMEDATALEN);
	pq_sendint64(&buf, resqinfo->resq_memory_limit);
	pq_sendint64(&buf, resqinfo->resq_network_limit);
	pq_sendint(&buf, resqinfo->resq_active_stmts, 4);
	pq_sendint(&buf, resqinfo->resq_wait_overload, 2);
	pq_sendint(&buf, resqinfo->resq_priority, 2);
	pq_sendint(&buf, errcode, 4);
	pq_endmessage(myport, &buf);

	if (myport->remote_type != GTM_NODE_GTM_PROXY)
	{
		pq_flush(myport);
	}

	return errcode;
}

static void 
resq_free_resqinfo(GTM_ResQueueInfo *resqinfo)
{
	if (resqinfo)
	{
		pfree(resqinfo);
	}
}

/*
 * Copy resquence key
 */
static GTM_ResQueueKey
resq_copy_key(GTM_ResQueueKey key_from, GTM_ResQueueKey key_to)
{
	memcpy(key_to->data, key_from->data, NAMEDATALEN);

	return key_to;
}

/*
 * Acquire resources from the resource queue
 */
static int
resq_acquire_resource(Port *myport,
						GTM_ResQueueInfo *resqinfo,
						GTM_ResQRequestData * request)
{
	int64 	resq_used_memory = 0;
	int64	resq_used_stmts = 0;
	int 	errcode = GTM_RESQ_RETCODE_OK;

	GTM_RWLockAcquire(&resqinfo->resq_lock, GTM_LOCKMODE_WRITE);

	resq_used_memory = resqinfo->resq_used_memory;
	if (resqinfo->resq_users != gtm_NIL)
	{
		resq_used_stmts = gtm_list_length(resqinfo->resq_users);
	}

	/*
	 * Apply for memory that exceeds the total memory of the resource queue
	 */
	if (resqinfo->resq_memory_limit > 0 && 
		request->memoryBytes > resqinfo->resq_memory_limit)
	{
		GTM_RWLockRelease(&resqinfo->resq_lock);

		errcode = GTM_RESQ_RETCODE_LACK_OF_RESOURCES;
		return resq_ack_client(myport, resqinfo, RESQUEUE_ACQUIRE_RESULT, errcode);
	}

	if (resq_used_stmts + 1 > resqinfo->resq_active_stmts ||
		(resqinfo->resq_memory_limit > 0 && 
		 request->memoryBytes + resq_used_memory > resqinfo->resq_memory_limit))
	{
		/*
		 * 1. The number of tasks exceeds the total number of tasks in the resource queue.
		 * 2. The requested memory resource is larger than the remaining memory of the resource queue.
		 */
		if (resqinfo->resq_wait_overload)
		{
			GTM_ResQWaiter * resq_waiter = NULL;
			MemoryContext oldContext = NULL;
			GTM_ConnectionInfo * conn = NULL;

			oldContext = MemoryContextSwitchTo(TopMostMemoryContext);

			resq_waiter = palloc0(sizeof(GTM_ResQWaiter));
			resq_waiter->myport = myport;
			resq_waiter->request = *request;

			resqinfo->resq_waiters = gtm_lappend(resqinfo->resq_waiters, resq_waiter);

			MemoryContextSwitchTo(oldContext);

			conn = (GTM_ConnectionInfo *) myport->conn;
			conn->con_resq_info = resqinfo;

			GTM_RWLockRelease(&resqinfo->resq_lock);

			errcode = GTM_RESQ_RETCODE_WAIT_OVERLOAD;
			return errcode;
		}
		else
		{
			GTM_RWLockRelease(&resqinfo->resq_lock);

			errcode = GTM_RESQ_RETCODE_LACK_OF_RESOURCES;
			return resq_ack_client(myport, resqinfo, RESQUEUE_ACQUIRE_RESULT, errcode);
		}
	}
	else
	{
		GTM_ResQUser * resq_user = NULL;
		MemoryContext oldContext = NULL;
		GTM_ConnectionInfo * conn = NULL;

		resqinfo->resq_used_memory += request->memoryBytes;
		Assert(resqinfo->resq_used_memory >= 0);
		if (resqinfo->resq_memory_limit > 0)
		{
			Assert(resqinfo->resq_used_memory <= resqinfo->resq_memory_limit);
		}

		oldContext = MemoryContextSwitchTo(TopMostMemoryContext);

		resq_user = palloc0(sizeof(GTM_ResQUser));
		resq_user->myport = myport;
		resq_user->request = *request;

		resqinfo->resq_users = gtm_lappend(resqinfo->resq_users, resq_user);

		MemoryContextSwitchTo(oldContext);

		conn = (GTM_ConnectionInfo *) myport->conn;
		conn->con_resq_info = resqinfo;

		GTM_RWLockRelease(&resqinfo->resq_lock);

		errcode = GTM_RESQ_RETCODE_OK;
		return resq_ack_client(myport, resqinfo, RESQUEUE_ACQUIRE_RESULT, errcode);
	}
}

/*
 * Release resources back to the resource queue
 */
static int
resq_release_resource(Port *myport,
						GTM_ResQueueInfo *resqinfo,
						GTM_ResQRequestData * request)
{
	gtm_ListCell   *cell = NULL;
	gtm_ListCell   *prev = NULL;
 
	GTM_RWLockAcquire(&resqinfo->resq_lock, GTM_LOCKMODE_WRITE);

	/*
	 * First release resource back to the resource queue
	 */
	cell = prev = NULL;
	gtm_foreach(cell, resqinfo->resq_users)
	{
		GTM_ResQUser * resq_user = NULL;

		resq_user = gtm_lfirst(cell);
		if (resq_user->myport == myport)
		{
			GTM_ConnectionInfo * conn = NULL;

			resqinfo->resq_users = gtm_list_delete_cell(resqinfo->resq_users, cell, prev);
			resqinfo->resq_used_memory -= resq_user->request.memoryBytes;

			Assert(resqinfo->resq_used_memory >= 0);
			Assert(resq_user->request.memoryBytes == request->memoryBytes);
			if (resqinfo->resq_memory_limit > 0)
			{
				Assert(resqinfo->resq_used_memory <= resqinfo->resq_memory_limit);
			}

			conn = (GTM_ConnectionInfo *) myport->conn;
			Assert(conn->con_resq_info == resqinfo);

			conn->con_resq_info = NULL;
			pfree(resq_user);
			break;
		}

		prev = cell;
	}

	/*
	 * Then wake up the resource request task in the waiting queue
	 */
	if (resqinfo->resq_waiters != gtm_NIL)
	{
		MemoryContext oldContext = NULL;
		gtm_List *new_waiters = gtm_NIL;

		oldContext = MemoryContextSwitchTo(TopMostMemoryContext);

		while (resqinfo->resq_waiters != gtm_NIL)
		{
			GTM_ResQWaiter * resq_waiter = NULL;
			int64 resq_used_memory = 0;
			int64 resq_used_stmts = 0;

			GTM_ConnectionInfo * conn = NULL;

			resq_waiter = gtm_linitial(resqinfo->resq_waiters);
			resqinfo->resq_waiters = gtm_list_delete_first(resqinfo->resq_waiters);

			conn = (GTM_ConnectionInfo *) resq_waiter->myport->conn;
			Assert(conn->con_resq_info == resqinfo);

			resq_used_memory = resqinfo->resq_used_memory;
			if (resqinfo->resq_users != gtm_NIL)
			{
				resq_used_stmts = gtm_list_length(resqinfo->resq_users);
			}

			/*
			 * Apply for memory that exceeds the total memory of the resource queue
			 */
			if (resqinfo->resq_memory_limit > 0 && 
				resq_waiter->request.memoryBytes > resqinfo->resq_memory_limit)
			{
				resq_ack_client(resq_waiter->myport, resqinfo, 
								RESQUEUE_ACQUIRE_RESULT, GTM_RESQ_RETCODE_LACK_OF_RESOURCES);
				conn->con_resq_info = NULL;
				pfree(resq_waiter);
				continue;
			}

			if (resq_used_stmts + 1 > resqinfo->resq_active_stmts ||
				(resqinfo->resq_memory_limit > 0 && 
				 resq_waiter->request.memoryBytes + resq_used_memory > resqinfo->resq_memory_limit))
			{
				/*
				 * 1. The number of tasks exceeds the total number of tasks in the resource queue.
				 * 2. The requested memory resource is larger than the remaining memory of the resource queue.
				 */
				if (resqinfo->resq_wait_overload)
				{
					new_waiters = gtm_lappend(new_waiters, resq_waiter);
				}
				else
				{
					resq_ack_client(resq_waiter->myport, resqinfo, 
								RESQUEUE_ACQUIRE_RESULT, GTM_RESQ_RETCODE_LACK_OF_RESOURCES);
					conn->con_resq_info = NULL;
					pfree(resq_waiter);
				}
			}
			else
			{
				resqinfo->resq_used_memory += resq_waiter->request.memoryBytes;
				Assert(resqinfo->resq_used_memory >= 0);
				if (resqinfo->resq_memory_limit > 0)
				{
					Assert(resqinfo->resq_used_memory <= resqinfo->resq_memory_limit);
				}

				resqinfo->resq_users = gtm_lappend(resqinfo->resq_users, resq_waiter);
				resq_ack_client(resq_waiter->myport, resqinfo, RESQUEUE_ACQUIRE_RESULT, GTM_RESQ_RETCODE_OK);
			}
		}

		MemoryContextSwitchTo(oldContext);
		resqinfo->resq_waiters = new_waiters;
	}

	GTM_RWLockRelease(&resqinfo->resq_lock);
	return resq_ack_client(myport, resqinfo, RESQUEUE_RELEASE_RESULT, GTM_RESQ_RETCODE_OK);
}

/*
 * clean resources, release resource back to the resource queue if needed
 */
static int
resq_clean_resource(Port *myport, GTM_ResQueueInfo *resqinfo)
{
	gtm_ListCell   *cell = NULL;
	gtm_ListCell   *prev = NULL;
 
	GTM_RWLockAcquire(&resqinfo->resq_lock, GTM_LOCKMODE_WRITE);

	/*
	 * If i am a user, then release resource back to the resource queue
	 */
	cell = prev = NULL;
	gtm_foreach(cell, resqinfo->resq_users)
	{
		GTM_ResQUser * resq_user = NULL;

		resq_user = gtm_lfirst(cell);
		if (resq_user->myport == myport)
		{
			GTM_ConnectionInfo * conn = NULL;

			resqinfo->resq_users = gtm_list_delete_cell(resqinfo->resq_users, cell, prev);
			resqinfo->resq_used_memory -= resq_user->request.memoryBytes;

			Assert(resqinfo->resq_used_memory >= 0);
			if (resqinfo->resq_memory_limit > 0)
			{
				Assert(resqinfo->resq_used_memory <= resqinfo->resq_memory_limit);
			}

			conn = (GTM_ConnectionInfo *) myport->conn;
			Assert(conn->con_resq_info == resqinfo);

			conn->con_resq_info = NULL;
			pfree(resq_user);

			GTM_RWLockRelease(&resqinfo->resq_lock);
			return 0;
		}

		prev = cell;
	}

	/*
	 * remove me from waiter list
	 */
	cell = prev = NULL;
	if (resqinfo->resq_waiters != gtm_NIL)
	{
		GTM_ResQWaiter * resq_waiter = NULL;

		resq_waiter = gtm_lfirst(cell);
		if (resq_waiter->myport == myport)
		{
			GTM_ConnectionInfo * conn = NULL;

			resqinfo->resq_waiters = gtm_list_delete_cell(resqinfo->resq_waiters, cell, prev);

			conn = (GTM_ConnectionInfo *) myport->conn;
			Assert(conn->con_resq_info == resqinfo);

			conn->con_resq_info = NULL;
			pfree(resq_waiter);

			GTM_RWLockRelease(&resqinfo->resq_lock);
			return 0;
		}

		prev = cell;
	}

	GTM_RWLockRelease(&resqinfo->resq_lock);

	return 0;
}

/*
 * Initialize the defaule resquence
 */
int
GTM_ResourceQueueOpenDefault(void)
{
	MemSet(&GTMDefaultResQueue, 0, sizeof(GTMDefaultResQueue));

	snprintf(NameStr(GTMDefaultResQueue.resq_name), NAMEDATALEN, "%s", DEFAULT_RESQUEUE_NAME);
	snprintf(NameStr(GTMDefaultResQueue.resq_group), NAMEDATALEN, "%s", "NoneGroup");
	GTMDefaultResQueue.resq_memory_limit = -1;		/* The default resource queue does not set the memory usage limit */
	GTMDefaultResQueue.resq_network_limit = -1;		/* The default resource queue does not set the network usage limit */
	GTMDefaultResQueue.resq_active_stmts = 20;		/* The default resource queue maximum concurrency is 20 */
	GTMDefaultResQueue.resq_wait_overload = 1;
	GTMDefaultResQueue.resq_priority = 3;			/* Tasks in the default resource queue have medium priority: PriorityVal_Medium */

	GTMDefaultResQueue.resq_ref_count = 0;
	GTMDefaultResQueue.resq_state = RESQUEUE_STATE_ACTIVE;
	GTM_RWLockInit(&GTMDefaultResQueue.resq_lock);
	GTMDefaultResQueue.resq_store_handle = INVALID_STORAGE_HANDLE;

	GTMDefaultResQueue.resq_used_memory = 0;
	GTMDefaultResQueue.resq_users = gtm_NIL;
	GTMDefaultResQueue.resq_waiters = gtm_NIL;

	return resq_add_resqinfo(&GTMDefaultResQueue);
}

/*
 * Initialize a new resquence. Optionally set the initial value of the resquence.
 */
int
GTM_ResourceQueueOpen(GTM_ResQueueInfo * resq)
{
	GTMStorageHandle	  resq_handle  = INVALID_STORAGE_HANDLE;
	GTM_ResQueueInfo	  *resqinfo = NULL;
	int          		  errcode = 0;

	if (enable_gtm_resqueue_debug)
	{
		elog(LOG, "GTM_ResourceQueueOpen resq:%s enter.", NameStr(resq->resq_name));
	}

	resq_lock_HashBucket_write(&resq->resq_name);

	resqinfo = resq_find_resqinfo(&resq->resq_name);
	if (resqinfo)
	{
		resq_release_resqinfo(resqinfo);
		resq_unlock_HashBucket(&resq->resq_name);
		ereport(LOG,
				(EEXIST,
				 errmsg("GTM_ResourceQueueOpen ResourceQueue with key:%s found in hashtab", NameStr(resq->resq_name))));
		return EEXIST;
	}

	GTM_FormResQueueOfStore(&resq->resq_name);
	resqinfo = resq_find_resqinfo(&resq->resq_name);
	if (resqinfo)
	{
		resq_release_resqinfo(resqinfo);
		resq_unlock_HashBucket(&resq->resq_name);
		ereport(LOG,
				(EEXIST,
				 errmsg("GTM_ResourceQueueOpen ResourceQueue with key:%s found in store", NameStr(resq->resq_name))));
		return EEXIST;
	}

	resqinfo = (GTM_ResQueueInfo *) palloc0(sizeof (GTM_ResQueueInfo));

	if (resqinfo == NULL)
	{
		resq_unlock_HashBucket(&resq->resq_name);
		ereport(ERROR, (ENOMEM, errmsg("GTM_ResourceQueueOpen Out of memory")));
		return EINVAL;
	}

	resq_copy_key(&resq->resq_name, &resqinfo->resq_name);
	resq_copy_key(&resq->resq_group, &resqinfo->resq_group);
	resqinfo->resq_memory_limit = resq->resq_memory_limit;
	resqinfo->resq_network_limit = resq->resq_network_limit;
	resqinfo->resq_active_stmts = resq->resq_active_stmts;
	resqinfo->resq_wait_overload = resq->resq_wait_overload;
	resqinfo->resq_priority = resq->resq_priority;

	resqinfo->resq_ref_count = 0;
	resqinfo->resq_state = RESQUEUE_STATE_ACTIVE;
	GTM_RWLockInit(&resqinfo->resq_lock);
	resqinfo->resq_store_handle = INVALID_STORAGE_HANDLE;

	resqinfo->resq_used_memory = 0;
	resqinfo->resq_users = gtm_NIL;
	resqinfo->resq_waiters = gtm_NIL;

	if ((errcode = resq_add_resqinfo(resqinfo)))
	{
		GTM_RWLockDestroy(&resqinfo->resq_lock);
		resq_free_resqinfo(resqinfo);

		resq_unlock_HashBucket(&resq->resq_name);
		return errcode;
	}
	else
	{
		/* open the resquence and remember the newly created resquence. */
		resqinfo = resq_find_resqinfo(&resqinfo->resq_name);
	}

    resq_handle = GTM_StoreResQueueCreate(resqinfo);
	if (INVALID_STORAGE_HANDLE == resq_handle)
	{
		resq_release_resqinfo(resqinfo);
		if (!resq_remove_resqinfo(resqinfo))
		{
			resq_free_resqinfo(resqinfo);
		}

		resq_unlock_HashBucket(&resq->resq_name);
		ereport(ERROR,
				(ERANGE,
				 errmsg("GTM_StoreSeqCreate for resq:%s failed", NameStr(resqinfo->resq_name))));

		return errcode;
	}

	resqinfo->resq_store_handle = resq_handle;

	if (enable_gtm_resqueue_debug)
	{
		elog(LOG, "GTM_ResourceQueueOpen resq:%s, handle:%d done.", NameStr(resqinfo->resq_name), resq_handle);
	}

	resq_unlock_HashBucket(&resq->resq_name);

	return errcode;
}

/*
 * Alter a resquence
 *
 * We don't track altered resquences because changes to resquence values are not
 * transactional and must not be rolled back if the transaction aborts.
 */
int GTM_ResourceQueueAlter(GTM_ResQueueInfo * resq, GTM_ResQAlterInfo * resq_alter)
{
	int32 ret = 0;
	GTM_ResQueueInfo *resqinfo = NULL;

	resq_lock_HashBucket_write(&resq->resq_name);

	resqinfo = resq_find_resqinfo(&resq->resq_name);
	if (NULL == resqinfo)
	{
		GTM_FormResQueueOfStore(&resq->resq_name);
		resqinfo = resq_find_resqinfo(&resq->resq_name);
	}

	if (resqinfo == NULL)
	{
		resq_unlock_HashBucket(&resq->resq_name);
		ereport(LOG,
				(EINVAL,
				 errmsg("The resquence %s does not exist", NameStr(resq->resq_name))));
		return EINVAL;
	}

	GTM_RWLockAcquire(&resqinfo->resq_lock, GTM_LOCKMODE_WRITE);

	if (resqinfo->resq_users != gtm_NIL)
	{
		GTM_RWLockRelease(&resqinfo->resq_lock);
		resq_release_resqinfo(resqinfo);
		resq_unlock_HashBucket(&resq->resq_name);
		ereport(LOG,
				(ERANGE,
				 errmsg("GTM_ResourceQueueAlter Cannot perform Alter operations on the resource queue %s who is being used",
				 		NameStr(resq->resq_name))));
		return EBUSY;
	}

	ret = GTM_StoreResQueueAlter(resq, resq_alter, resqinfo->resq_store_handle);
	if (ret)
	{
		GTM_RWLockRelease(&resqinfo->resq_lock);
		resq_release_resqinfo(resqinfo);
		resq_unlock_HashBucket(&resq->resq_name);
		ereport(LOG,
				(ERANGE,
				 errmsg("GTM_ResourceQueueAlter GTM_StoreResQueueAlter failed to alter resquence")));
		return ret;
	}

	/* Modify the data if necessary */
	if (resq_alter->alter_memory_limit)
		resqinfo->resq_memory_limit = resq->resq_memory_limit;

	if (resq_alter->alter_network_limit)
		resqinfo->resq_network_limit = resq->resq_network_limit;

	if (resq_alter->alter_active_stmts)
		resqinfo->resq_active_stmts = resq->resq_active_stmts;

	if (resq_alter->alter_wait_overload)
		resqinfo->resq_wait_overload = resq->resq_wait_overload;

	if (resq_alter->alter_priority)
		resqinfo->resq_priority = resq->resq_priority;

	GTM_RWLockRelease(&resqinfo->resq_lock);
	resq_release_resqinfo(resqinfo);

	resq_unlock_HashBucket(&resq->resq_name);

	return 0;
}

/*
 * Destroy the given resquence depending on type of given key
 */
int
GTM_ResourceQueueClose(GTM_ResQueueKey resqkey)
{
	int res = 0;
	GTM_ResQueueInfo *resqinfo = NULL;

	resq_lock_HashBucket_write(resqkey);

	resqinfo = resq_find_resqinfo(resqkey);
	if (NULL == resqinfo)
	{
		GTM_FormResQueueOfStore(resqkey);
		resqinfo = resq_find_resqinfo(resqkey);
	}

	/*
	 * If the resquence by created by the same transaction, then just
	 * drop it completely
	 */
	if (resqinfo != NULL)
	{
		resq_release_resqinfo(resqinfo);

		if (resqinfo->resq_users != gtm_NIL)
		{
			resq_unlock_HashBucket(resqkey);
			ereport(LOG,
					(ERANGE,
					 errmsg("GTM_ResourceQueueClose Cannot perform Drop operations on the resource queue %s who is being used",
					 		resqkey->data)));
			return EBUSY;
		}

		if (!resq_remove_resqinfo(resqinfo))
		{
			resq_free_resqinfo(resqinfo);
		}
	}
	else if (resqinfo == NULL)
	{
		res = EINVAL;
	}

	resq_unlock_HashBucket(resqkey);

	return res;
}

/*
 * Check whether the resource queue exists
 */
int
GTM_ResourceQueueIfExists(GTM_ResQueueKey resqkey)
{
	int res = 0;
	GTM_ResQueueInfo *resqinfo = NULL;

	resq_lock_HashBucket_write(resqkey);

	resqinfo = resq_find_resqinfo(resqkey);
	if (NULL == resqinfo)
	{
		GTM_FormResQueueOfStore(resqkey);
		resqinfo = resq_find_resqinfo(resqkey);
	}

	if (resqinfo != NULL)
	{
		resq_release_resqinfo(resqinfo);
		res = GTM_RESQ_RETCODE_EXISTS;
	}
	else if (resqinfo == NULL)
	{
		res = GTM_RESQ_RETCODE_NOT_EXISTS;
	}

	resq_unlock_HashBucket(resqkey);

	return res;
}

int
GTM_ResourceQueueAcqurie(Port *myport, GTM_ResQRequestData * request)
{
	int res = 0;
	GTM_ResQueueInfo *resqinfo = NULL;
	GTM_ResQueueKey resqkey = NULL;

	resqkey = &request->resqName;
	resq_lock_HashBucket_write(resqkey);

	resqinfo = resq_find_resqinfo(resqkey);
	if (NULL == resqinfo)
	{
		GTM_FormResQueueOfStore(resqkey);
		resqinfo = resq_find_resqinfo(resqkey);
	}

	if (resqinfo != NULL)
	{
		resq_release_resqinfo(resqinfo);
		res = resq_acquire_resource(myport, resqinfo, request);
	}
	else if (resqinfo == NULL)
	{
		GTM_ResQueueInfo	nullResqInfo;
		MemSet(&nullResqInfo, 0, sizeof(nullResqInfo));

		res = GTM_RESQ_RETCODE_NOT_EXISTS;
		resq_ack_client(myport, &nullResqInfo, RESQUEUE_ACQUIRE_RESULT, res);
	}

	resq_unlock_HashBucket(resqkey);

	return res;
}

int
GTM_ResourceQueueRelease(Port *myport, GTM_ResQRequestData * request)
{
	int res = 0;
	GTM_ResQueueInfo *resqinfo = NULL;
	GTM_ResQueueKey resqkey = NULL;

	resqkey = &request->resqName;
	resq_lock_HashBucket_write(resqkey);

	resqinfo = resq_find_resqinfo(resqkey);
	if (NULL == resqinfo)
	{
		GTM_FormResQueueOfStore(resqkey);
		resqinfo = resq_find_resqinfo(resqkey);
	}

	if (resqinfo != NULL)
	{
		resq_release_resqinfo(resqinfo);
		res = resq_release_resource(myport, resqinfo, request);
	}
	else if (resqinfo == NULL)
	{
		GTM_ResQueueInfo	nullResqInfo;
		MemSet(&nullResqInfo, 0, sizeof(nullResqInfo));

		res = GTM_RESQ_RETCODE_NOT_EXISTS;
		resq_ack_client(myport, &nullResqInfo, RESQUEUE_RELEASE_RESULT, res);
	}

	resq_unlock_HashBucket(resqkey);

	return res;
}

int
GTM_ResourceQueueClean(Port *myport)
{
	int res = 0;
	GTM_ConnectionInfo *conn = NULL;

	conn = (GTM_ConnectionInfo *) myport->conn;

	if (conn != NULL && conn->con_resq_info != NULL)
	{
		GTM_ResQueueInfo *resqinfo = NULL;
		GTM_ResQueueKey resqkey = NULL;
		
		resqinfo = (GTM_ResQueueInfo *)conn->con_resq_info;

		resqkey = &resqinfo->resq_name;
		resq_lock_HashBucket_write(resqkey);

		res = resq_clean_resource(myport, resqinfo);

		resq_unlock_HashBucket(resqkey);
	}

	return res;
}

void
GTM_InitResourceQueueManager(void)
{
	int ii = 0;
	for (ii = 0; ii < RESQUEUE_HASH_TABLE_SIZE; ii++)
	{
		GTMResQueues[ii].rhb_list = gtm_NIL;
		GTM_RWLockInit(&GTMResQueues[ii].rhb_lock);
	}
}

/*
 * Process MSG_RESQUEUE_INIT/MSG_BKUP_RESQUEUE_INIT message
 *
 * is_backup indicates the message is MSG_BKUP_RESQUEUE_INIT
 */
void
ProcessResourceQueueInitCommand(Port *myport, StringInfo message, bool is_backup)
{
	GTM_ResQueueInfo resq;
	StringInfoData buf;
	int errcode = 0;
	MemoryContext oldContext = NULL;

	MemSet(&resq, 0, sizeof(resq));
	MemSet(&buf, 0, sizeof(buf));

	if (Recovery_IsStandby())
	{
		if (myport->remote_type != GTM_NODE_GTM)
		{
			elog(ERROR, "gtm standby can't provide resquence to datanodes or coordinators.");
		}
	}	
	
	/*
	 * Get the resquence info
	 */
	memcpy(resq.resq_name.data, pq_getmsgbytes(message, NAMEDATALEN), NAMEDATALEN);
	memcpy(resq.resq_group.data, pq_getmsgbytes(message, NAMEDATALEN), NAMEDATALEN);
	resq.resq_memory_limit = pq_getmsgint64(message);
	resq.resq_network_limit = pq_getmsgint64(message);
	resq.resq_active_stmts = pq_getmsgint(message, 4);
	resq.resq_wait_overload = pq_getmsgint(message, 2);
	resq.resq_priority = pq_getmsgint(message, 2);
	pq_getmsgend(message);

	/*
	 * We must use the TopMostMemoryContext because the resquence information is
	 * not bound to a thread and can outlive any of the thread specific
	 * contextes.
	 */
	oldContext = MemoryContextSwitchTo(TopMostMemoryContext);	
	errcode    = GTM_ResourceQueueOpen(&resq);
	if (errcode)
	{
		MemoryContextSwitchTo(oldContext);
		ereport(ERROR,
				(errcode,
				 errmsg("Failed to create new resquence:%s for:%s", NameStr(resq.resq_name),strerror(errcode))));
	}

	MemoryContextSwitchTo(oldContext);

	elog(DEBUG1, "Opening resquence %s", NameStr(resq.resq_name));

	if (!is_backup)
	{        
        BeforeReplyToClientXLogTrigger();
		/*
		 * Send a SUCCESS message back to the client
		 */
		pq_beginmessage(&buf, 'S');
		pq_sendint(&buf, RESQUEUE_INIT_RESULT, 4);
		if (myport->remote_type == GTM_NODE_GTM_PROXY)
		{
			GTM_ProxyMsgHeader proxyhdr;
			proxyhdr.ph_conid = myport->conn_id;
			pq_sendbytes(&buf, (char *)&proxyhdr, sizeof (GTM_ProxyMsgHeader));
		}
		pq_sendbytes(&buf, resq.resq_name.data, NAMEDATALEN);
		pq_endmessage(myport, &buf);

		if (myport->remote_type != GTM_NODE_GTM_PROXY)
		{
			pq_flush(myport);
		}
	}
	else
        BeforeReplyToClientXLogTrigger();
	/* FIXME: need to check errors */
}

/*
 * Process MSG_RESQUEUE_ALTER/MSG_BKUP_RESQUEUE_ALTER message
 *
 * is_backup indicates the message is MSG_BKUP_RESQUEUE_ALTER
 */
void
ProcessResourceQueueAlterCommand(Port *myport, StringInfo message, bool is_backup)
{
	GTM_ResQueueInfo resq;
	GTM_ResQAlterInfo resq_alter;
	StringInfoData buf;
	int errcode = 0;
	MemoryContext oldContext = NULL;

	MemSet(&resq, 0, sizeof(resq));
	MemSet(&resq_alter, 0, sizeof(resq_alter));
	MemSet(&buf, 0, sizeof(buf));

	if (Recovery_IsStandby())
	{
		if (myport->remote_type != GTM_NODE_GTM)
		{
			elog(ERROR, "gtm standby can't provide resquence to datanodes or coordinators.");
		}
	}	

	/*
	 * Get the resquence key
	 */
	memcpy(resq.resq_name.data, pq_getmsgbytes(message, NAMEDATALEN), NAMEDATALEN);
	memcpy(resq.resq_group.data, pq_getmsgbytes(message, NAMEDATALEN), NAMEDATALEN);
	resq.resq_memory_limit = pq_getmsgint64(message);
	resq.resq_network_limit = pq_getmsgint64(message);
	resq.resq_active_stmts = pq_getmsgint(message, 4);
	resq.resq_wait_overload = pq_getmsgint(message, 2);
	resq.resq_priority = pq_getmsgint(message, 2);

	resq_alter.alter_name = pq_getmsgbyte(message);
	resq_alter.alter_group = pq_getmsgbyte(message);
	resq_alter.alter_memory_limit = pq_getmsgbyte(message);
	resq_alter.alter_network_limit = pq_getmsgbyte(message);
	resq_alter.alter_active_stmts = pq_getmsgbyte(message);
	resq_alter.alter_wait_overload = pq_getmsgbyte(message);
	resq_alter.alter_priority = pq_getmsgbyte(message);

	pq_getmsgend(message);

	/*
	 * We must use the TopMostMemoryContext because the resquence information is
	 * not bound to a thread and can outlive any of the thread specific
	 * contextes.
	 */
	oldContext = MemoryContextSwitchTo(TopMostMemoryContext);

	elog(DEBUG1, "Altering resquence key %s", NameStr(resq.resq_name));

	if ((errcode = GTM_ResourceQueueAlter(&resq, &resq_alter)))
	{
		MemoryContextSwitchTo(oldContext);
		ereport(ERROR,
				(errcode,
				 errmsg("Failed to alter resquence:%s", NameStr(resq.resq_name))));
	}

	MemoryContextSwitchTo(oldContext);

	if (!is_backup)
	{
        BeforeReplyToClientXLogTrigger();

		pq_beginmessage(&buf, 'S');
		pq_sendint(&buf, RESQUEUE_ALTER_RESULT, 4);
		if (myport->remote_type == GTM_NODE_GTM_PROXY)
		{
			GTM_ProxyMsgHeader proxyhdr;
			proxyhdr.ph_conid = myport->conn_id;
			pq_sendbytes(&buf, (char *)&proxyhdr, sizeof (GTM_ProxyMsgHeader));
		}
		pq_sendbytes(&buf, resq.resq_name.data, NAMEDATALEN);
		pq_endmessage(myport, &buf);

		if (myport->remote_type != GTM_NODE_GTM_PROXY)
		{
			pq_flush(myport);
		}

		/* FIXME: need to check errors */
	}
	else
        BeforeReplyToClientXLogTrigger();
}

/*
 * Process MSG_RESQUEUE_LIST message
 */
void
ProcessResourceQueueListCommand(Port *myport, StringInfo message)
{
	StringInfoData buf;
	int resq_count = 0;
	int resq_maxcount = 0;
	GTM_ResQueueInfo **resq_list = NULL;
	int i = 0;

	MemSet(&buf, 0, sizeof(buf));
	pq_getmsgend(message);

	if (Recovery_IsStandby())
		ereport(ERROR,
			(EPERM,
			 errmsg("Operation not permitted under the standby mode.")));

	resq_count = 0;
	resq_maxcount = MAX_RESQUEUE_NUMBER;
	resq_list = (GTM_ResQueueInfo **) palloc0(resq_maxcount * sizeof(GTM_ResQueueInfo *));

	/*
	 * Store pointers to all GTM_ResQueueInfo in the hash buckets into an array.
	 */
	for (i = 0; i < RESQUEUE_HASH_TABLE_SIZE ; i++)
	{
		GTM_ResQueueInfoHashBucket *b = NULL;
		gtm_ListCell *elem = NULL;

		b = &GTMResQueues[i];

		GTM_RWLockAcquire(&b->rhb_lock, GTM_LOCKMODE_READ);

		gtm_foreach(elem, b->rhb_list)
		{
			/* Allocate larger array if required */
			if (resq_count == resq_maxcount)
			{
				int 			newcount = 0;
				GTM_ResQueueInfo   **newlist = NULL;

				newcount = 2 * resq_maxcount;
				newlist = (GTM_ResQueueInfo **) repalloc(resq_list, newcount * sizeof(GTM_ResQueueInfo *));
				/*
				 * If failed try to get less. It is unlikely to happen, but
				 * let's be safe.
				 */
				while (newlist == NULL)
				{
					newcount = resq_maxcount + (newcount - resq_maxcount) / 2 - 1;
					if (newcount <= resq_maxcount)
					{
						/* give up */
						ereport(ERROR,
								(ERANGE,
								 errmsg("Can not list all the resquences")));
					}
					newlist = (GTM_ResQueueInfo **) repalloc(resq_list, newcount * sizeof(GTM_ResQueueInfo *));
				}
				resq_maxcount = newcount;
				resq_list = newlist;
			}
			resq_list[resq_count] = (GTM_ResQueueInfo *) gtm_lfirst(elem);
			resq_count++;
		}

		GTM_RWLockRelease(&b->rhb_lock);
	}

    BeforeReplyToClientXLogTrigger();

	pq_beginmessage(&buf, 'S');
	pq_sendint(&buf, RESQUEUE_LIST_RESULT, 4);

	if (myport->remote_type == GTM_NODE_GTM_PROXY)
	{
		GTM_ProxyMsgHeader proxyhdr;
		proxyhdr.ph_conid = myport->conn_id;
		pq_sendbytes(&buf, (char *)&proxyhdr, sizeof (GTM_ProxyMsgHeader));
	}

	/* Send a number of resquences */
	pq_sendint(&buf, resq_count, 4);

	/*
	 * Send resquences from the array
	 */
	{
		/*
		 * TODO set initial size big enough to fit any resquence, and avoid
		 * reallocations.
		 */
		size_t resq_maxlen = 256;
		char *resq_buf = (char *) palloc0(resq_maxlen);

		for (i = 0 ; i < resq_count ; i++)
		{
			size_t resq_buflen = gtm_get_resqueue_size(resq_list[i]);
			if (resq_buflen > resq_maxlen)
			{
				resq_maxlen = resq_buflen;
				resq_buf = (char *)repalloc(resq_buf, resq_maxlen);
			}

			gtm_serialize_resqueue(resq_list[i], resq_buf, resq_buflen);

			elog(DEBUG1, "resq_buflen = %ld", resq_buflen);

			pq_sendint(&buf, resq_buflen, 4);
			pq_sendbytes(&buf, resq_buf, resq_buflen);
		}

		pfree(resq_buf);
	}

	pq_endmessage(myport, &buf);

	elog(DEBUG1, "ProcessResourceQueueListCommand() done.");

	if (myport->remote_type != GTM_NODE_GTM_PROXY)
		/* Don't flush to the backup because this does not change the internal status */
		pq_flush(myport);
}

/*
 * Process MSG_RESQUEUE_CLOSE/MSG_BKUP_RESQUEUE_CLOSE message
 *
 * is_backup indicates the message is MSG_BKUP_RESQUEUE_CLOSE
 */
void
ProcessResourceQueueCloseCommand(Port *myport, StringInfo message, bool is_backup)
{
	GTM_ResQueueKeyData resqkey;
	StringInfoData buf;
	int errcode = 0;

	MemSet(&resqkey, 0, sizeof(resqkey));
	MemSet(&buf, 0, sizeof(buf));

	if (Recovery_IsStandby())
	{
		if (myport->remote_type != GTM_NODE_GTM)
		{
			elog(ERROR, "gtm standby can't provide resquence to datanodes or coordinators.");
		}
	}

	/*
	 * Get the resquence key
	 */
	memcpy(resqkey.data, pq_getmsgbytes(message, NAMEDATALEN), NAMEDATALEN);
	pq_getmsgend(message);

	elog(DEBUG1, "Closing resquence %s", resqkey.data);

	if ((errcode = GTM_ResourceQueueClose(&resqkey)))
		ereport(ERROR,
				(errcode,
				 errmsg("Can not close the resquence %s", resqkey.data)));

	if (!is_backup)
	{
        BeforeReplyToClientXLogTrigger();

		/* Respond to the client */
		pq_beginmessage(&buf, 'S');
		pq_sendint(&buf, RESQUEUE_CLOSE_RESULT, 4);
		if (myport->remote_type == GTM_NODE_GTM_PROXY)
		{
			GTM_ProxyMsgHeader proxyhdr;
			proxyhdr.ph_conid = myport->conn_id;
			pq_sendbytes(&buf, (char *)&proxyhdr, sizeof (GTM_ProxyMsgHeader));
		}

		pq_sendbytes(&buf, resqkey.data, NAMEDATALEN);
		pq_endmessage(myport, &buf);

		if (myport->remote_type != GTM_NODE_GTM_PROXY)
		{
			pq_flush(myport);
		}

		/* FIXME: need to check errors */
	}
	else 
    	BeforeReplyToClientXLogTrigger();
}

/* Move resq_conn to ResourceQueueManager */
void ProcessResourceQueueMoveConnCommand(Port *myport, StringInfo message)
{
	StringInfoData buf;
    struct epoll_event event;
	int32 move_success = 0;

	NameData nodeName;
	int32	procPid = 0;
	int32 	 backendId = 0;

	MemSet(&buf, 0, sizeof(buf));
	MemSet(&nodeName, 0, sizeof(nodeName));

	if (Recovery_IsStandby())
	{
		if (myport->remote_type != GTM_NODE_GTM)
		{
			elog(ERROR, "gtm standby can't provide resquence to datanodes or coordinators.");
		}
	}

	memcpy(nodeName.data, pq_getmsgbytes(message, NAMEDATALEN), NAMEDATALEN);
	procPid = pq_getmsgint(message, 4);
	backendId = pq_getmsgint(message, 4);
	pq_getmsgend(message);

	elog(LOG, "GTM start move conn to ResourceQueueManager: nodeName: %s, procPid: %d, backendId: %d",
			NameStr(nodeName), procPid, backendId);

	/* disconnect from current thread */
	if(epoll_ctl(GetMyThreadInfo->thr_efd, EPOLL_CTL_DEL, myport->sock, NULL) != 0)
	{
		elog(LOG,"epoll delete fails %s",strerror(errno));
        move_success = -1;
	}

	event.data.ptr = GetMyThreadInfo->thr_conn;
	event.events = EPOLLIN | EPOLLERR | EPOLLHUP | EPOLLRDHUP;

	if(epoll_ctl(g_resq_manager_thread->thr_efd, EPOLL_CTL_ADD, myport->sock, &event) != 0)
	{
		elog(LOG,"epoll add fails %s", strerror(errno));
		move_success = -1;
	}

	elog(LOG, "GTM finish move conn to ResourceQueueManager: nodeName: %s, procPid: %d, backendId: %d, ret: %s",
			NameStr(nodeName), procPid, backendId, move_success ? "failed" : "succeed");
	
	pq_beginmessage(&buf, 'S');
	pq_sendint(&buf, RESQUEUE_MOVE_CONN_RESULT, 4);
	pq_sendint(&buf, move_success, 4);
	pq_endmessage(myport, &buf);
	pq_flush(myport);
}

/* Acqurie resource from resqueue */
void ProcessResourceQueueAcquireCommand(Port *myport, StringInfo message)
{
	GTM_ResQRequestData request;

	if (Recovery_IsStandby())
	{
		if (myport->remote_type != GTM_NODE_GTM)
		{
			elog(ERROR, "gtm standby can't provide resquence to datanodes or coordinators.");
		}
	}

	MemSet(&request, 0, sizeof(request));

	memcpy(request.nodeName.data, pq_getmsgbytes(message, NAMEDATALEN), NAMEDATALEN);
	request.nodeProcPid = pq_getmsgint(message, 4);
	request.nodeBackendId = pq_getmsgint(message, 4);
	memcpy(request.resqName.data, pq_getmsgbytes(message, NAMEDATALEN), NAMEDATALEN);
	request.memoryBytes = pq_getmsgint64(message);
	pq_getmsgend(message);

	if (enable_gtm_resqueue_debug)
	{
		elog(LOG, "ProcessResourceQueueAcquireCommand receive resource acquire request, "
					"nodeName: %s, nodeProcPid: %d, nodeBackendId: %d, resqName: %s, memoryBytes: "INT64_FORMAT,
					NameStr(request.nodeName), request.nodeProcPid, request.nodeBackendId,
					NameStr(request.resqName), request.memoryBytes);
	}

	GTM_ResourceQueueAcqurie(myport, &request);
}

/* Release resource back to resqueue */
void ProcessResourceQueueReleaseCommand(Port *myport, StringInfo message)
{
	GTM_ResQRequestData request;

	if (Recovery_IsStandby())
	{
		if (myport->remote_type != GTM_NODE_GTM)
		{
			elog(ERROR, "gtm standby can't provide resquence to datanodes or coordinators.");
		}
	}

	MemSet(&request, 0, sizeof(request));

	memcpy(request.nodeName.data, pq_getmsgbytes(message, NAMEDATALEN), NAMEDATALEN);
	request.nodeProcPid = pq_getmsgint(message, 4);
	request.nodeBackendId = pq_getmsgint(message, 4);
	memcpy(request.resqName.data, pq_getmsgbytes(message, NAMEDATALEN), NAMEDATALEN);
	request.memoryBytes = pq_getmsgint64(message);
	pq_getmsgend(message);

	if (enable_gtm_resqueue_debug)
	{
		elog(LOG, "ProcessResourceQueueReleaseCommand receive resource release request, "
					"nodeName: %s, nodeProcPid: %d, nodeBackendId: %d, resqName: %s, memoryBytes: "INT64_FORMAT,
					NameStr(request.nodeName), request.nodeProcPid, request.nodeBackendId,
					NameStr(request.resqName), request.memoryBytes);
	}

	GTM_ResourceQueueRelease(myport, &request);
}

/* Check whether the resource queue exists */
void ProcessResourceQueueCheckIfExistsCommand(Port *myport, StringInfo message)
{
	GTM_ResQueueKeyData resqkey;
	StringInfoData buf;
	int errcode = 0;

	MemSet(&resqkey, 0, sizeof(resqkey));
	MemSet(&buf, 0, sizeof(buf));

	if (Recovery_IsStandby())
	{
		if (myport->remote_type != GTM_NODE_GTM)
		{
			elog(ERROR, "gtm standby can't provide resquence to datanodes or coordinators.");
		}
	}

	/*
	 * Get the resquence key
	 */
	memcpy(resqkey.data, pq_getmsgbytes(message, NAMEDATALEN), NAMEDATALEN);
	pq_getmsgend(message);

	elog(DEBUG1, "Check if exists resquence %s", resqkey.data);

	errcode = GTM_ResourceQueueIfExists(&resqkey);

	/* Respond to the client */
	pq_beginmessage(&buf, 'S');
	pq_sendint(&buf, RESQUEUE_IF_EXISTS_RESULT, 4);
	if (myport->remote_type == GTM_NODE_GTM_PROXY)
	{
		GTM_ProxyMsgHeader proxyhdr;
		proxyhdr.ph_conid = myport->conn_id;
		pq_sendbytes(&buf, (char *)&proxyhdr, sizeof (GTM_ProxyMsgHeader));
	}

	pq_sendbytes(&buf, resqkey.data, NAMEDATALEN);
	pq_sendint(&buf, errcode, 4);
	pq_endmessage(myport, &buf);

	if (myport->remote_type != GTM_NODE_GTM_PROXY)
	{
		pq_flush(myport);
	}
}

void ProcessResourceQueueListUsageCommand(Port *myport, StringInfo message)
{
	GTM_ResQueueKeyData resqkey;
	StringInfoData buf;
	GTM_ResQueueInfo *resqinfo = NULL;
	GTM_ResQUsageInfo *usage = NULL;
	int32	usage_count = 0, i = 0;
	gtm_ListCell   *cell = NULL;

	MemSet(&resqkey, 0, sizeof(resqkey));
	MemSet(&buf, 0, sizeof(buf));

	if (Recovery_IsStandby())
	{
		if (myport->remote_type != GTM_NODE_GTM)
		{
			elog(ERROR, "gtm standby can't provide resquence to datanodes or coordinators.");
		}
	}

	/*
	 * Get the resquence key
	 */
	memcpy(resqkey.data, pq_getmsgbytes(message, NAMEDATALEN), NAMEDATALEN);
	pq_getmsgend(message);

	elog(DEBUG1, "List usage info of resquence %s", resqkey.data);

	resq_lock_HashBucket_write(&resqkey);

	resqinfo = resq_find_resqinfo(&resqkey);
	if (NULL == resqinfo)
	{
		GTM_FormResQueueOfStore(&resqkey);
		resqinfo = resq_find_resqinfo(&resqkey);
	}

	if (resqinfo != NULL)
	{
		resq_release_resqinfo(resqinfo);
	}
	else if (resqinfo == NULL)
	{
		resq_unlock_HashBucket(&resqkey);
		elog(ERROR, "Can not list usage info of unexist resquence %s", resqkey.data);
		return;
	}

	GTM_RWLockAcquire(&resqinfo->resq_lock, GTM_LOCKMODE_READ);

	usage_count = gtm_list_length(resqinfo->resq_users);
	usage_count += gtm_list_length(resqinfo->resq_waiters);

	if (usage_count > 0)
	{
		usage = (GTM_ResQUsageInfo *) palloc0(usage_count * sizeof(GTM_ResQUsageInfo));
		if (usage == NULL)
		{
			GTM_RWLockRelease(&resqinfo->resq_lock);
			resq_unlock_HashBucket(&resqkey);
			elog(ERROR, "Can not alloc usage info memory for resquence %s", resqkey.data);
			return;
		}

		cell = NULL;
		gtm_foreach(cell, resqinfo->resq_users)
		{
			GTM_ResQUser * resq_user = NULL;

			resq_user = gtm_lfirst(cell);

			usage[i].usage = resq_user->request;
			usage[i].isuser = true;
			i += 1;
		}

		cell = NULL;
		gtm_foreach(cell, resqinfo->resq_waiters)
		{
			GTM_ResQWaiter* resq_waiter = NULL;

			resq_waiter = gtm_lfirst(cell);

			usage[i].usage = resq_waiter->request;
			usage[i].isuser = false;
			i += 1;
		}

		Assert(i == usage_count);
	}

	GTM_RWLockRelease(&resqinfo->resq_lock);

	resq_unlock_HashBucket(&resqkey);

	pq_beginmessage(&buf, 'S');
	pq_sendint(&buf, MSG_LIST_RESQUEUE_USAGE_RESULT, 4);

	if (myport->remote_type == GTM_NODE_GTM_PROXY)
	{
		GTM_ProxyMsgHeader proxyhdr;
		proxyhdr.ph_conid = myport->conn_id;
		pq_sendbytes(&buf, (char *)&proxyhdr, sizeof (GTM_ProxyMsgHeader));
	}

	/* Send a number of user */
	pq_sendint(&buf, usage_count, sizeof(usage_count));

	/*
	 * Send usage info from the array
	 */			
	for (i = 0 ; i < usage_count ; i++)
	{		
		pq_sendbytes(&buf, (char*)&(usage[i]), sizeof(GTM_ResQUsageInfo));
	}
	pq_endmessage(myport, &buf);

	if (usage != NULL)
		pfree(usage);

	if (myport->remote_type != GTM_NODE_GTM_PROXY)
	{
		/* Don't flush to the backup because this does not change the internal status */
		pq_flush(myport);
	}

	if (enable_gtm_resqueue_debug)
	{
		elog(LOG, "ProcessResourceQueueListUsageCommand done");
	}
}

/*
 * Form the resquence from GTM storage.
 */
GTM_ResQueueInfo *
GTM_FormResQueueOfStore(GTM_ResQueueKey resqkey)
{
	int32			 ret		= -1;
	GTMStorageHandle resq_handle = INVALID_STORAGE_HANDLE;
	GTM_ResQueueInfo     *resqinfo    = NULL;	
	MemoryContext    old_memorycontext = NULL;

	old_memorycontext = MemoryContextSwitchTo(TopMostMemoryContext);
	resqinfo = (GTM_ResQueueInfo *) palloc0(sizeof(GTM_ResQueueInfo));
	if (resqinfo == NULL)
	{
		MemoryContextSwitchTo(old_memorycontext);
		ereport(ERROR, (ENOMEM, errmsg("GTM_FormResQueueOfStore Out of memory")));
	}

	GTM_RWLockInit(&resqinfo->resq_lock);

	resqinfo->resq_ref_count = 0;
	resq_copy_key(resqkey, &resqinfo->resq_name);
	resqinfo->resq_state = RESQUEUE_STATE_ACTIVE;

	resqinfo->resq_used_memory = 0;
	resqinfo->resq_users = gtm_NIL;
	resqinfo->resq_waiters = gtm_NIL;

	resq_handle = GTM_StoreLoadResQueue(resqinfo);
	if (INVALID_STORAGE_HANDLE == resq_handle)
	{
		GTM_RWLockDestroy(&resqinfo->resq_lock);
		resq_free_resqinfo(resqinfo);
		MemoryContextSwitchTo(old_memorycontext);
		return NULL;
	}

	ret = resq_add_resqinfo(resqinfo);
	if (ret)
	{
		GTM_RWLockDestroy(&resqinfo->resq_lock);
		resq_free_resqinfo(resqinfo);
		MemoryContextSwitchTo(old_memorycontext);
		ereport(LOG, (ENOMEM, errmsg("GTM_FormResQueueOfStore resq_add_resqinfo failed")));
		return NULL;
	}

	resqinfo->resq_store_handle = resq_handle;
	MemoryContextSwitchTo(old_memorycontext);
	
	if (enable_gtm_resqueue_debug)
	{
		elog(LOG, "GTM_FormSeqOfStore resq: %s found in store, resq:%d",
			NameStr(resqinfo->resq_name), resq_handle);
	}
	return resqinfo;
}

