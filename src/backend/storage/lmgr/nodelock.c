/*-------------------------------------------------------------------------
 *
 * nodeLock.c
 *	 lock node to reject query
 *
 *
 *
 * IDENTIFICATION
 *	  src/backend/storage/nodeLock.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include <unistd.h>
#include <dirent.h>
#include <sys/stat.h>
#include <fcntl.h>
#include "miscadmin.h"
#include "funcapi.h"
#include "storage/lwlock.h"
#include "storage/nodelock.h"
#include "storage/fd.h"
#include "catalog/namespace.h"
#include "utils/lsyscache.h"
#include "storage/procarray.h"
#include "utils/builtins.h"
#include "access/htup_details.h"
#include "tcop/utility.h"
#include "catalog/pg_type.h"
#include "catalog/pg_package.h"
#include "utils/formatting.h"
#include "utils/dynahash.h"


/*
  *    lock content : DML/SELECT only
  *   tables are locked for DML/SELECT in table
  *   shards are locked for DML/SELECT in shard
  */
#define MAX_TABLE_NUM 1024
#define MAX_SHARD_NUM MAX_SHARDS

#define SHARD_BITMAP_SIZE \
	(BITMAPSET_SIZE(WORDNUM(MAX_SHARDS) + 1))

#define SHARD_CLUSTER_BITMAP_SIZE \
	(BITMAPSET_SIZE(WORDNUM(MAX_SHARD_CLUSTER_NUM) + 1))

typedef struct DMLLockContent
{
	int nTables;
	Oid table[MAX_TABLE_NUM + 1];
	int nShards[MAX_SHARDS];
	char shard[SHARD_BITMAP_SIZE];
#ifdef __OPENTENBASE_C__
	int nColShards[MAX_SHARDS];
	char colShard[SHARD_BITMAP_SIZE];
#endif
	int nShardClusters[MAX_SHARD_CLUSTER_NUM];        /* for LOCK_SC object lock type */
	char shardCluster[SHARD_CLUSTER_BITMAP_SIZE]; /* for LOCK_SC object lock type */
} DMLLockContent;

/*
  * flags to indicate which action is not permitted or All is not permitted
  */
#define ALL      ((1 << CMD_NOTHING) - 1)          /* we can do nothing */
#define DDL      (1 << CMD_UTILITY)          /* can not do DDL, such create, drop */
#define UPDATE   (1 << CMD_UPDATE)           /* update is not permitted */
#define INSERT   (1 << CMD_INSERT)           /* insert is not permitted */
#define DELETE   (1 << CMD_DELETE)           /* delete is not permitted */
#define SELECT   (1 << CMD_SELECT)           /* select is not permitted */

/* event lock flags */
#define STORAGE_EXTENSION_SOURCE (1 << 0)
#define STORAGE_EXTENSION_DEST   (1 << 1)
#define COLD_HOT                 (1 << 2)
#define PROMOTION                (1 << 3)

static char *event_message[] = {"storage extension source",
								"storage extension dest",
								"cold hot",
								"promotion"
								};

#define MAX_HEAVY_LOCK 5

#define NUM_LIGHT_LOCK 4
#define OFFSET CMD_UPDATE

#define LOCK_TABLE 'T'
#define LOCK_SHARD 'S'
#define LOCK_EVENT 'E'
#define LOCK_NONE_OBJECT  'N'
#ifdef __OPENTENBASE_C__
#define LOCK_COLUMN 'C'
#endif
#define LOCK_SC 'R'

/* file to write node locks information */
static char *controlFile = "nodeLock.control";


/* lock data in share memory */
typedef struct NodeLockData
{
	int16 flags;
	int16 nHeavyLocks[CMD_NOTHING];
#ifdef __OPENTENBASE_C__
	int16 colFlags;
#endif
	int16 eventLocks;
	int16 nEventLocks;
	DMLLockContent lock[NUM_LIGHT_LOCK];
} NodeLockData;


static char *LockMessages[NUM_LIGHT_LOCK] =    {"UPDATE",
                                                "INSERT",
                                                "DELETE",
												"SELECT"};


/* hash search type */
typedef enum hashAction
{
    FIND,
    ENTER
}   hashAction;

#define NODELOCKSIZE sizeof(NodeLockData)

static NodeLockData *nodelock = NULL;

static NodeLockData nodelock_Copy;

Size NodeLockShmemSize(void)
{
    Size size = 0;

    size = add_size(size, NODELOCKSIZE);

    return size;
}

void NodeLockShmemInit(void)
{
	int i = 0;
    bool found = false;
	Bitmapset *shardbitmap = NULL;

    nodelock = (NodeLockData *)ShmemInitStruct("Node Locks",
                                             NODELOCKSIZE,
                                             &found);
	if(found)
		return;

	memset(nodelock, 0, NODELOCKSIZE);

	/* shard bitmap init */
	for (i = 0; i < NUM_LIGHT_LOCK; i++)
	{
		shardbitmap = (Bitmapset *)nodelock->lock[i].shard;

		shardbitmap = bms_make((char *)shardbitmap, MAX_SHARD_NUM);
	}
#ifdef __OPENTENBASE_C__
	/* shard bitmap init */
	for (i = 0; i < NUM_LIGHT_LOCK; i++)
	{
		shardbitmap = (Bitmapset *)nodelock->lock[i].colShard;

		shardbitmap = bms_make((char *)shardbitmap, MAX_SHARD_NUM);
	}
#endif
	/* shardcluster bitmap init */
	for (i = 0; i < NUM_LIGHT_LOCK; i++)
	{
		shardbitmap = (Bitmapset *)nodelock->lock[i].shardCluster;
		
		shardbitmap = bms_make((char *)shardbitmap, MAX_SHARD_CLUSTER_NUM);
	}
}

/* write node locks information into file, we can recover from file if crash */
static bool 
WriteNodeLockFile(void)
{
    int fd;
    int ret;
    
    fd = BasicOpenFile(controlFile, O_RDWR | O_TRUNC | O_CREAT);
	if (fd < 0)
	{   
        elog(ERROR, "could not open control file \"%s\"", controlFile);
        return false;
	}

    ret = write(fd, (char *)nodelock, NODELOCKSIZE);
    
    if(ret != NODELOCKSIZE)
    {
        close(fd);
        elog(ERROR, "could not write into control file \"%s\"", controlFile);      
        return false;
    }
    
    close(fd);

    return true;
}

/* block all ddls/dmls */
static bool
SetHeavyLock(char *lockActions, bool lock)
{
	int i;
	int nIgnored = 0;

	/* loop to set lock actions. if all is specified, set action and return;
	 *  else set every legal action
	 */
	for(i = 0; i < strlen(lockActions); i++)
	{
		switch(lockActions[i])
		{
			case 'A':
			case 'a':
				{
					if (lock)
					{
						nodelock->flags |= ALL;
						nodelock->nHeavyLocks[CMD_NOTHING -1]++;
					}
					else
					{
						if (nodelock->flags & ALL)
						{
							nodelock->nHeavyLocks[CMD_NOTHING -1]--;
							if (nodelock->nHeavyLocks[CMD_NOTHING -1] == 0)
							{
								nodelock->flags &= ~ALL;
							}
						}
					}
					return true;
				}
			case 'S':
			case 's':
				{
					if (lock)
					{
						nodelock->flags |= SELECT;
						nodelock->nHeavyLocks[CMD_SELECT - 1]++;
					}
					else
					{
						if (nodelock->flags & SELECT)
						{
							nodelock->nHeavyLocks[CMD_SELECT - 1]--;
							if (nodelock->nHeavyLocks[CMD_SELECT - 1] == 0)
							{
								nodelock->flags &= ~SELECT;
							}
						}
					}
					break;
				}
			case 'U':
			case 'u':
				{
					if (lock)
					{
						nodelock->flags |= UPDATE;
						nodelock->nHeavyLocks[CMD_UPDATE - 1]++;
					}
					else
					{
						if (nodelock->flags & UPDATE)
						{
							nodelock->nHeavyLocks[CMD_UPDATE - 1]--;
							if (nodelock->nHeavyLocks[CMD_UPDATE - 1] == 0)
							{
								nodelock->flags &= ~UPDATE;
							}
						}
					}
					break;
				}
			case 'I':
			case 'i':
				{
					if (lock)
					{
						nodelock->flags |= INSERT;
						nodelock->nHeavyLocks[CMD_INSERT - 1]++;
					}
					else
					{
						if (nodelock->flags & INSERT)
						{
							nodelock->nHeavyLocks[CMD_INSERT - 1]--;
							if (nodelock->nHeavyLocks[CMD_INSERT - 1] == 0)
							{
								nodelock->flags &= ~INSERT;
							}
						}
					}
					break;
				}
			case 'D':
			case 'd':
				{
					if (lock)
					{
						nodelock->flags |= DELETE;
						nodelock->nHeavyLocks[CMD_DELETE - 1]++;
					}
					else
					{
						if (nodelock->flags & DELETE)
						{
							nodelock->nHeavyLocks[CMD_DELETE - 1]--;
							if (nodelock->nHeavyLocks[CMD_DELETE - 1] == 0)
							{
								nodelock->flags &= ~DELETE;
							}
						}
					}
					break;
				}
			case 'C':
			case 'c':
				{
					if (lock)
					{
						nodelock->flags |= DDL;
						nodelock->nHeavyLocks[CMD_UTILITY - 1]++;
					}
					else
					{
						if (nodelock->flags & DDL)
						{
							nodelock->nHeavyLocks[CMD_UTILITY - 1]--;
							if (nodelock->nHeavyLocks[CMD_UTILITY - 1] == 0)
							{
								nodelock->flags &= ~DDL;
							}
						}
					}
					break;
				}
			default:
				{
					nIgnored++;
					elog(NOTICE, "%c is unknown lock action, ignored here.", lockActions[i]);
				}
		}
	}

	/* if all actions are illlegal, set is failure, return false */
	if(nIgnored == strlen(lockActions))
	{
		elog(NOTICE, "all lock actions %s are illlegal.", lockActions);
		return false;
	}

	return true;
}

/* 
  * search tableOids to find whether the given Oid is already exist or not.
  */
static int32 HashSearch(DMLLockContent *lock, Oid table, hashAction action)
{
    int32 idx;
    int32 idx2;
    int32 end_idx;
    
    if(action == ENTER)
    {
        if(lock->nTables >= MAX_TABLE_NUM)
        {
            elog(NOTICE, "exceed max table number, could not lock more tables.");

			return 0;
        }
    }

    idx = (table % MAX_TABLE_NUM) + 1;

    if(lock->table[idx])
    {
        if(action == FIND && lock->table[idx] == table)
            return idx;
        else if(action == ENTER && lock->table[idx] == table)
        {
			return -1;
        }
    }
    else
    {
        if(action == ENTER)
            return idx;
        else
            return 0;
    }

    idx2 = 1 + table % (MAX_TABLE_NUM - 2);

    end_idx = idx;

    do
    {
        if(idx <= idx2)
            idx = MAX_TABLE_NUM + idx - idx2;
        else
            idx -= idx2;

        if(idx == end_idx)
            break;

        if(lock->table[idx])
        {
            if(action == FIND && lock->table[idx] == table)
                return idx;
            else if(action == ENTER && lock->table[idx] == table)
            {
				return -1;
            }
        }
        
    }while(lock->table[idx]);

    if(action == ENTER)
    {
        if(lock->table[idx] == InvalidOid)
            return idx;
    }

    return 0;
}


/* block dmls on specified table or shard */
static bool
SetLightLock(char *lockActions, char objectType, char *param1, char *param2, bool lock)
{
	bool setlock = false;
	int i = 0;
	int nIgnored = 0;
	Oid schema = InvalidOid;
	Oid table = InvalidOid;
	CmdType cmd = CMD_NOTHING;
	const char *delim = ",";
	char *buf = NULL;
	int shard = 0;

	/* sanity check */
	if (objectType == LOCK_TABLE)
	{
	    schema = get_namespace_oid(param1, false);
	    if(InvalidOid == schema)
	    {
	        ereport(NOTICE,
					(errcode(ERRCODE_UNDEFINED_SCHEMA),
					 errmsg("schema \"%s\" does not exist.", param1)));

			return false;
	    }

	    table = get_relname_relid(param2, schema);
	    if(InvalidOid == table)
	    {
	        ereport(NOTICE,
					(errcode(ERRCODE_UNDEFINED_TABLE),
					 errmsg("table \"%s\" does not exist.", param2)));

			return false;
	    }
	}

	/* 
	 *    loop to set lock actions. if all is specified, set action and return;
	 *	else set every legal action
	 */
	for(i = 0; i < strlen(lockActions); i++)
	{
		setlock = false;
		switch(lockActions[i])
		{
			case 'A':
			case 'a':
			case 'C':
			case 'c':
				break;
			case 'S':
			case 's':
				setlock = true;
				cmd = CMD_SELECT + 4;
				break;
			case 'U':
			case 'u':
				setlock = true;
				cmd = CMD_UPDATE;
				break;
			case 'I':
			case 'i':
				setlock = true;
				cmd = CMD_INSERT;
				break;
			case 'D':
			case 'd':
				setlock = true;
				cmd = CMD_DELETE;
				break;
			default:
				{
					nIgnored++;
					elog(NOTICE, "%c is unknown lock action, ignored here.", lockActions[i]);
				}
		}

		if (setlock)
		{
			if (objectType == LOCK_TABLE)
			{
				if (lock)
				{
					int idx = HashSearch(&nodelock->lock[cmd - OFFSET], table, ENTER);

					/* no space left */
					if (idx == 0)
					{
						return false;
					}

					if (idx == -1)
					{
						elog(NOTICE, "table %s is already locked on %s.", param2, LockMessages[cmd - OFFSET]);
					}

					/* set lock */
					if (idx > 0)
					{
						nodelock->lock[cmd - OFFSET].table[idx] = table;
						nodelock->lock[cmd - OFFSET].nTables++;
					}
				}
				else
				{
					int idx = HashSearch(&nodelock->lock[cmd - OFFSET], table, FIND);

					if (idx == 0)
					{
						elog(NOTICE, "table %s is not locked on %s before.", param2, LockMessages[cmd - OFFSET]);
					}

					if (idx > 0)
					{
						nodelock->lock[cmd - OFFSET].table[idx] = InvalidOid;
						nodelock->lock[cmd - OFFSET].nTables--;
					}
				}
			}
			else if (objectType == LOCK_SC)
			{
				ShardID shard_ids[MAX_SHARDS] = {0};
				Bitmapset *sc_lock_bitmap = NULL;
				Bitmapset *shard_lock_bitmap = NULL;
				char *sc_str = NULL;
				Bitmapset *sc_bms = NULL;
				int nShards = 0;
				int idx = 0;
				
				sc_str = (char *) palloc0(sizeof(char) * (strlen(param1) + 1));
				sc_bms = (Bitmapset *) palloc0(sizeof(char) * SHARD_CLUSTER_BITMAP_SIZE);
				
				/* 1. add shardclusterid into Bitmapset. */
				sc_lock_bitmap = (Bitmapset *) nodelock->lock[cmd - OFFSET].shardCluster;
				shard_lock_bitmap = (Bitmapset *) nodelock->lock[cmd - OFFSET].shard;
				
				memcpy(sc_str, param1, strlen(param1));
				
				buf = strtok(sc_str, delim);
				while (buf)
				{
					int sc = pg_atoi(buf, sizeof(int), '\0');
					
					if (lock)
					{
						if (!bms_is_member(sc, sc_lock_bitmap))
						{
							bms_add_member(sc_lock_bitmap, sc);
						}
						
						nodelock->lock[cmd - OFFSET].nShardClusters[sc]++;
					}
					else
					{
						if (bms_is_member(sc, sc_lock_bitmap))
						{
							nodelock->lock[cmd - OFFSET].nShardClusters[sc]--;
							
							if (nodelock->lock[cmd - OFFSET].nShardClusters[sc] == 0)
							{
								bms_del_member(sc_lock_bitmap, sc);
							}
						}
						else
						{
							elog(NOTICE, "shardcluster %d is not locked before, no need to unlock.", sc);
						}
					}
					
					/* Initialize the shardcluster bitmap to find all shardids. */
					if (!bms_is_member(sc, sc_bms))
						bms_add_member(sc_bms, sc);
					
					buf = strtok(NULL, delim);
				}
				pfree(sc_str);
				
				/* 2. find all shardids, add them into bitmapset. */
				GetShardIdsByShardClusterId(sc_bms, shard_ids, &nShards);
				bms_free(sc_bms);
				
				for (idx = 0; idx < nShards; ++idx)
				{
					shard = shard_ids[idx];
					if (!ShardIDIsValid(shard))
					{
						elog(NOTICE, "shard id %d is invalid.", shard);
						return false;
					}
					
					if (lock)
					{
						if (!bms_is_member(shard, shard_lock_bitmap))
						{
							bms_add_member(shard_lock_bitmap, shard);
						}
						
						nodelock->lock[cmd - OFFSET].nShards[shard]++;
					}
					else
					{
						if (shard >= MAX_SHARD_NUM)
						{
							elog(NOTICE, "shard %d exceed max shard number, no need to unlock.", shard);
						}
						if (bms_is_member(shard, shard_lock_bitmap))
						{
							nodelock->lock[cmd - OFFSET].nShards[shard]--;
							
							if (nodelock->lock[cmd - OFFSET].nShards[shard] == 0)
							{
								bms_del_member(shard_lock_bitmap, shard);
							}
						}
						else
						{
							elog(NOTICE, "shard %d is not locked before, no need to unlock.", shard);
						}
					}
				}
			}
			else
			{
				Bitmapset *shardbitmap = (Bitmapset *)nodelock->lock[cmd - OFFSET].shard;

				char *shards = (char *)palloc0(sizeof(char) * (strlen(param1)+ 1));

				memcpy(shards, param1, strlen(param1));
				
			    buf = strtok(shards, delim);
				
			    while(buf)
			    {
			        shard = pg_atoi(buf, sizeof(int), '\0');

					if (!ShardIDIsValid(shard))
					{
						elog(NOTICE, "shard id %d is invalid.", shard);
						return false;
					}

					if (lock)
					{
						if (!bms_is_member(shard, shardbitmap))
						{
							bms_add_member(shardbitmap, shard);
						}

						nodelock->lock[cmd - OFFSET].nShards[shard]++;
					}
					else
					{
						if (shard >= MAX_SHARD_NUM)
						{
							elog(NOTICE, "shard %d exceed max shard number, no need to unlock.", shard);
						}
						if (bms_is_member(shard, shardbitmap))
						{
							nodelock->lock[cmd - OFFSET].nShards[shard]--;

							if (nodelock->lock[cmd - OFFSET].nShards[shard] == 0)
							{
								bms_del_member(shardbitmap, shard);
							}
						}
						else
						{
							elog(NOTICE, "shard %d is not locked before, no need to unlock.", shard);
						}
					}
				
			    	buf = strtok(NULL, delim);
			    } 

				pfree(shards);
			}
		}
	}

	/* if all actions are illlegal, set is failure, return false */
	if(nIgnored == strlen(lockActions))
	{
		elog(NOTICE, "all lock actions %s are illlegal.", lockActions);
		return false;
	}

	return true;
}

static bool
SetEventLock(int event, bool lock)
{
	bool result = false;
	
	switch(event)
	{
		case STORAGE_EXTENSION_SOURCE:
		case COLD_HOT:
		case PROMOTION:
		case STORAGE_EXTENSION_DEST:
			{
				LWLockAcquire(NodeLockMgrLock, LW_EXCLUSIVE);

				if (lock)
				{
					if (nodelock->eventLocks && (event != STORAGE_EXTENSION_DEST && event != STORAGE_EXTENSION_SOURCE))
					{
						elog(NOTICE, "node is already locked with event \"%s\"", event_message[my_log2(nodelock->eventLocks)]);
						result = false;
					}
					else if (nodelock->eventLocks && ((nodelock->eventLocks & event) == 0))
					{
						elog(NOTICE, "node is already locked with event \"%s\"", event_message[my_log2(nodelock->eventLocks)]);
						result = false;
					}
					else
					{
						nodelock->nEventLocks++;

						nodelock->eventLocks |= event;

						WriteNodeLockFile();
						
						result = true;
					}
				}
				else
				{
					if (nodelock->eventLocks && (nodelock->eventLocks & event))
					{
						if (--nodelock->nEventLocks == 0)
						{
							nodelock->eventLocks &= ~event;
						}
						WriteNodeLockFile();

						result = true;
					}
					else if (nodelock->eventLocks && !(nodelock->eventLocks & event))
					{
						elog(NOTICE, "node is locked with other event \"%s\"", event_message[my_log2(nodelock->eventLocks)]);

						result = false;
					}
					else if (!nodelock->eventLocks)
					{
						elog(NOTICE, "node is not locked with any event");

						result = true;
					}
				}

				LWLockRelease(NodeLockMgrLock);
				
				break;
			}
		default:
			{
				elog(NOTICE, "unknow event lock type %d", event);
				result = false;
				break;
			}
	}

	return result;
}

#ifdef __OPENTENBASE_C__
static bool 
SetColstoreLock(char *lockActions, char objectType, char *param1, char *param2, bool lock)
{
	int i;
	int nIgnored = 0;
	
	if (strlen(param1) == 0)
	{
		/* loop to set lock actions. if all is specified, set action and return;
		 *	else set every legal action
		 */
		for(i = 0; i < strlen(lockActions); i++)
		{
			switch(lockActions[i])
			{
				case 'U':
				case 'u':
					{
						if (lock)
						{
							nodelock->colFlags |= UPDATE;
						}
						else
						{
							nodelock->colFlags &= ~UPDATE;
						}
						break;
					}
				case 'I':
				case 'i':
					{
						if (lock)
						{
							nodelock->colFlags |= INSERT;
						}
						else
						{
							nodelock->colFlags &= ~INSERT;
						}
						break;
					}
				case 'D':
				case 'd':
					{
						if (lock)
						{
							nodelock->colFlags |= DELETE;
						}
						else
						{
							nodelock->colFlags &= ~DELETE;
						}
						break;
					}
				default:
					{
						nIgnored++;
						elog(NOTICE, "%c is unknown lock action, ignored here.", lockActions[i]);
					}
			}
		}
		
		/* if all actions are illlegal, set is failure, return false */
		if(nIgnored == strlen(lockActions))
		{
			elog(NOTICE, "all lock actions %s are illlegal.", lockActions);
			return false;
		}
		
		return true;
	}
	else 
	{
		bool setlock = false;
		CmdType cmd = CMD_NOTHING;
		const char *delim = ",";
		char *buf = NULL;
		int shard = 0;
		
		for(i = 0; i < strlen(lockActions); i++)
		{
			setlock = false;
			switch(lockActions[i])
			{
				case 'A':
				case 'a':
				case 'S':
				case 's':
				case 'C':
				case 'c':
					break;
				case 'U':
				case 'u':
					setlock = true;
					cmd = CMD_UPDATE;
					break;
				case 'I':
				case 'i':
					setlock = true;
					cmd = CMD_INSERT;
					break;
				case 'D':
				case 'd':
					setlock = true;
					cmd = CMD_DELETE;
					break;
				default:
					{
						nIgnored++;
						elog(NOTICE, "%c is unknown lock action, ignored here.", lockActions[i]);
					}
			}
		
			if (setlock)
			{
				Bitmapset *shardbitmap = (Bitmapset *)nodelock->lock[cmd - OFFSET].colShard;
	
				char *shards = (char *)palloc0(sizeof(char) * (strlen(param1)+ 1));
	
				memcpy(shards, param1, strlen(param1));
				
				buf = strtok(shards, delim);
				
				while(buf)
				{
					shard = pg_atoi(buf, sizeof(int), '\0');
	
					if (!ShardIDIsValid(shard))
					{
						elog(NOTICE, "shard id %d is invalid.", shard);
						return false;
					}
	
					if (lock)
					{
						if (!bms_is_member(shard, shardbitmap))
						{
							bms_add_member(shardbitmap, shard);
						}
	
						nodelock->lock[cmd - OFFSET].nColShards[shard]++;
					}
					else
					{
						if (shard >= MAX_SHARD_NUM)
						{
							elog(NOTICE, "shard %d exceed max shard number, no need to unlock.", shard);
						}
						if (bms_is_member(shard, shardbitmap))
						{
							nodelock->lock[cmd - OFFSET].nColShards[shard]--;
	
							if (nodelock->lock[cmd - OFFSET].nColShards[shard] == 0)
							{
								bms_del_member(shardbitmap, shard);
							}
						}
						else
						{
							elog(NOTICE, "shard %d is not locked before, no need to unlock.", shard);
						}
					}
				
					buf = strtok(NULL, delim);
				} 
	
				pfree(shards);
			}
		}
		
		/* if all actions are illlegal, set is failure, return false */
		if(nIgnored == strlen(lockActions))
		{
			elog(NOTICE, "all lock actions %s are illlegal.", lockActions);
			return false;
		}
		
		return true;

	}
}

#endif

/* check if transactions in xid array finished or not
  */
static void checkTransFinished(TransactionId *xids, int *nxid, int *nxidLeft, RunningTransactions runTrans)
{
    int i = 0;
    int j = 0;
    bool exist = false;
    
    if(*nxidLeft == 0)
        return;

    /* traverse transactions in xids and current running transactions to find if transactions in xids 
     * are still running. if not, remove transaction xid from xids. 
     */
    for(i = 0; i < *nxid; i++)
    {
        exist = false;
        if(xids[i] != 0)
        {
            for(j = 0; j < runTrans->xcnt; j++)
            {
                if(xids[i] == runTrans->xids[j])
                {
                    exist = true;
                    break;
                }
            }

            if(!exist)
            {
                xids[i] = 0;
                
                (*nxidLeft)--;
                
                if(*nxidLeft == 0)
                    return;
            }
        }
    }
}

/* lock node to reject query */
bool NodeLock(char *lockActions, char objectType, char *param1, char *param2, int loopTimes)
{
	bool ret;

	if (loopTimes <= 0)
	{
		elog(NOTICE, "loopTimes %d must be greater than 0.", loopTimes);
		return false;
	}

	/*
	  * TODO!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
	  * Pre-check:if there are long transactions in system?
	  */
	  
	objectType = pg_ascii_toupper(objectType);
	
	if (objectType == LOCK_NONE_OBJECT)
	{
		LWLockAcquire(NodeLockMgrLock, LW_EXCLUSIVE); 

		/* 
		  * make copy before modify lock data 
		  * if lock failed, set lock data back
		  */
		memcpy((char *)&nodelock_Copy, (char *)nodelock, NODELOCKSIZE);

		ret = SetHeavyLock(lockActions, true);

		if (!ret)
		{
			memcpy((char *)nodelock, (char *)&nodelock_Copy, NODELOCKSIZE);
		}

		LWLockRelease(NodeLockMgrLock);
	}
	else if (objectType == LOCK_EVENT)
	{
		if (strcasecmp(lockActions, "storage extension source") == 0)
		{
			ret = SetEventLock(STORAGE_EXTENSION_SOURCE, true);
		}
		else if	(strcasecmp(lockActions, "cold hot") == 0)
		{
			ret = SetEventLock(COLD_HOT, true);
		}
		else if (strcasecmp(lockActions, "promotion") == 0)
		{
			ret = SetEventLock(PROMOTION, true);
		}
		else if (strcasecmp(lockActions, "storage extension dest") == 0)
		{
			ret = SetEventLock(STORAGE_EXTENSION_DEST, true);
		}
		else
		{
			elog(NOTICE, "unknow event lock type %s.", lockActions);

			ret = false;
		}

		return ret;
	}
#ifdef __OPENTENBASE_C__
	else if (objectType == LOCK_COLUMN)
	{
		LWLockAcquire(NodeLockMgrLock, LW_EXCLUSIVE);

		/* 
		  * make copy before modify lock data 
		  * if lock failed, set lock data back
		  */
		memcpy((char *)&nodelock_Copy, (char *)nodelock, NODELOCKSIZE);
		
		ret = SetColstoreLock(lockActions, objectType, param1, param2, true);

		if (!ret)
		{
			memcpy((char *)nodelock, (char *)&nodelock_Copy, NODELOCKSIZE);
		}

		LWLockRelease(NodeLockMgrLock);

	}
#endif
	else
	{
		if (objectType != LOCK_TABLE && objectType != LOCK_SHARD && objectType != LOCK_SC)
		{
			elog(NOTICE, "unknow lock object %c.", objectType);

			return false;
		}
		
		LWLockAcquire(NodeLockMgrLock, LW_EXCLUSIVE);

		/* 
		  * make copy before modify lock data 
		  * if lock failed, set lock data back
		  */
		memcpy((char *)&nodelock_Copy, (char *)nodelock, NODELOCKSIZE);
		
		ret = SetLightLock(lockActions, objectType, param1, param2, true);

		if (!ret)
		{
			memcpy((char *)nodelock, (char *)&nodelock_Copy, NODELOCKSIZE);
		}

		LWLockRelease(NodeLockMgrLock);
	}

    /* check to see whether running transactions exist or not.
         * if checkTimes is given, we will wait for checkTimes seconds at most.
         * before time's up, if no running transactions, keep going; else fail to
         * lock node
         */
    if (ret)
	{
		RunningTransactions running = NULL;

		running = GetCurrentRunningTransaction();

		LWLockRelease(XidGenLock);

		if (running->xcnt)
		{
		    TransactionId *xids;
		    int nxid;
		    int nxidLeft;
			
	        xids = (TransactionId *)malloc(sizeof(TransactionId) * running->xcnt);

	        memcpy(xids, running->xids, sizeof(TransactionId) * running->xcnt);

	        nxid = running->xcnt;

	        nxidLeft = nxid;
		
			while(loopTimes--)
	        {
	        	pg_usleep(1000000L); /* sleep one second */
				
	            running = GetCurrentRunningTransaction();

	        	/* GetRunningTransactionData() acquired XidGenLock, we must release it */
	        	LWLockRelease(XidGenLock);

				checkTransFinished(xids, &nxid, &nxidLeft, running);
	            
	            if(nxidLeft == 0)
	            {
	                break;
	            }
	        }

			free(xids);
	            
	        if(loopTimes == -1)
	        {
	            LWLockAcquire(NodeLockMgrLock, LW_EXCLUSIVE);

	            memcpy((char *)nodelock, (char *)&nodelock_Copy, NODELOCKSIZE);

	            LWLockRelease(NodeLockMgrLock);

	            elog(NOTICE, "Failed to lock node: There are running transactions.");
	            
	            ret = false;
	        }
		}
	}

	/* lock succeed, write control file */
	if (ret)
	{
		LWLockAcquire(NodeLockMgrLock, LW_EXCLUSIVE);
		WriteNodeLockFile();
		LWLockRelease(NodeLockMgrLock);
	}
	
	return ret;
}

bool NodeUnLock(char *lockActions, char objectType, char *param1, char *param2)
{
	bool ret;

	objectType = pg_ascii_toupper(objectType);
	
	if (objectType == LOCK_NONE_OBJECT)
	{
		LWLockAcquire(NodeLockMgrLock, LW_EXCLUSIVE); 

		/* 
		  * make copy before modify lock data 
		  * if lock failed, set lock data back
		  */
		memcpy((char *)&nodelock_Copy, (char *)nodelock, NODELOCKSIZE);

		ret = SetHeavyLock(lockActions, false);

		if (!ret)
		{
			memcpy((char *)nodelock, (char *)&nodelock_Copy, NODELOCKSIZE);
		}

		if (ret)
		{
			WriteNodeLockFile();
		}

		LWLockRelease(NodeLockMgrLock);
	}
	else if (objectType == LOCK_EVENT)
	{
		if (strcasecmp(lockActions, "storage extension source") == 0)
		{
			ret = SetEventLock(STORAGE_EXTENSION_SOURCE, false);
		}
		else if	(strcasecmp(lockActions, "cold hot") == 0)
		{
			ret = SetEventLock(COLD_HOT, false);
		}
		else if (strcasecmp(lockActions, "promotion") == 0)
		{
			ret = SetEventLock(PROMOTION, false);
		}
		else if (strcasecmp(lockActions, "storage extension dest") == 0)
		{
			ret = SetEventLock(STORAGE_EXTENSION_DEST, false);
		}
		else
		{
			elog(NOTICE, "unknow event lock type %s.", lockActions);

			ret = false;
		}

		return ret;
	}
#ifdef __OPENTENBASE_C__
	else if (objectType == LOCK_COLUMN)
	{
		LWLockAcquire(NodeLockMgrLock, LW_EXCLUSIVE);

		/* 
		  * make copy before modify lock data 
		  * if lock failed, set lock data back
		  */
		memcpy((char *)&nodelock_Copy, (char *)nodelock, NODELOCKSIZE);
		
		ret = SetColstoreLock(lockActions, objectType, param1, param2, false);

		if (!ret)
		{
			memcpy((char *)nodelock, (char *)&nodelock_Copy, NODELOCKSIZE);
		}
		
		if (ret)
		{
			WriteNodeLockFile();
		}

		LWLockRelease(NodeLockMgrLock);

	}
#endif
	else
	{
		if (objectType != LOCK_TABLE && objectType != LOCK_SHARD && objectType != LOCK_SC)
		{
			elog(NOTICE, "unknow lock object %c.", objectType);

			return false;
		}
		
		LWLockAcquire(NodeLockMgrLock, LW_EXCLUSIVE);

		/* 
		  * make copy before modify lock data 
		  * if lock failed, set lock data back
		  */
		memcpy((char *)&nodelock_Copy, (char *)nodelock, NODELOCKSIZE);
		
		ret = SetLightLock(lockActions, objectType, param1, param2, false);

		if (!ret)
		{
			memcpy((char *)nodelock, (char *)&nodelock_Copy, NODELOCKSIZE);
		}

		if (ret)
		{
			WriteNodeLockFile();
		}

		LWLockRelease(NodeLockMgrLock);
	}

	return ret;
}

/* query is DDL or not */
static bool
isDDL(void *parsetree)
{
	if (!parsetree)
		return false;
	
	switch (nodeTag((Node *)parsetree))
	{
		case T_AlterDatabaseStmt:
		case T_AlterDatabaseSetStmt:
		case T_AlterDomainStmt:
		case T_AlterFunctionStmt:
		case T_AlterRoleStmt:
		case T_AlterRoleSetStmt:
		case T_AlterObjectSchemaStmt:
		case T_AlterOwnerStmt:
		case T_AlterSeqStmt:
		case T_AlterTableStmt:
		case T_RenameStmt:
		case T_CommentStmt:
		case T_DefineStmt:
		case T_CreateCastStmt:
		case T_CreateConversionStmt:
		case T_CreatedbStmt:
		case T_CreateDirStmt:
		case T_DropDirStmt:
		case T_CreateDomainStmt:
		case T_CreateFunctionStmt:
		case T_CreateRoleStmt:
		case T_IndexStmt:
		case T_CreatePLangStmt:
		case T_CreateOpClassStmt:
		case T_CreateOpFamilyStmt:
		case T_AlterOpFamilyStmt:
		case T_RuleStmt:
		case T_CreateSchemaStmt:
		case T_CreateSeqStmt:
		case T_CreateStmt:
		case T_CreateExternalStmt:
		case T_CreateTableAsStmt:
		case T_CreateTableSpaceStmt:
		case T_CreateTrigStmt:
		case T_CompositeTypeStmt:
		case T_CreateEnumStmt:
		case T_CreateRangeStmt:
		case T_AlterEnumStmt:
		case T_ViewStmt:
		case T_DropStmt:
		case T_CreateSynonymStmt:
		case T_CreatePackageStmt:
		case T_CreatePackageBodyStmt:
		case T_AlterPackageStmt:
		case T_DropPackageStmt:
			{
				if (nodeTag((Node *) parsetree) == T_DropStmt)
				{
					DropStmt *stmt = (DropStmt *)parsetree;

					if (stmt->removeType == OBJECT_PUBLICATION)
						return false;
				}
			}
		case T_DropdbStmt:
		case T_DropTableSpaceStmt:
		case T_DropRoleStmt:
		case T_GrantStmt:
		case T_GrantRoleStmt:
		case T_AlterDefaultPrivilegesStmt:
		case T_TruncateStmt:
		case T_DropOwnedStmt:
		case T_ReassignOwnedStmt:
		case T_AlterTSDictionaryStmt:
		case T_AlterTSConfigurationStmt:
		case T_CreateExtensionStmt:
		case T_AlterExtensionStmt:
		case T_AlterExtensionContentsStmt:
		case T_CreateFdwStmt:
		case T_AlterFdwStmt:
		case T_CreateForeignServerStmt:
		case T_AlterForeignServerStmt:
		case T_CreateUserMappingStmt:
		case T_AlterUserMappingStmt:
		case T_DropUserMappingStmt:
		case T_AlterTableSpaceOptionsStmt:
		case T_CreateForeignTableStmt:
		case T_SecLabelStmt:
		case T_CreateShardStmt:
		//case T_CleanShardingStmt:
		case T_DropShardStmt:
		//case T_MoveDataStmt:
#ifdef __AUDIT__
		case T_AuditStmt:
		case T_CleanAuditStmt:
#endif
#ifdef _PG_ORCL_
		case T_CreateStatsStmt:
		case T_CreateProfileStmt:
		case T_AlterProfileStmt:
		case T_DropProfileStmt:
		case T_AlterResourceCostStmt:
		case T_CreateTypeObjectStmt:
		case T_CreateTypeObjectBodyStmt:
		case T_CreateNestedTableStmt:
#endif
#ifdef __RESOURCE_QUEUE__
		case T_CreateResourceQueueStmt:
		case T_AlterResourceQueueStmt:
		case T_DropResourceQueueStmt:
#endif
		case T_CreateResourceGroupStmt:
		case T_AlterResourceGroupStmt:
		case T_DropResourceGroupStmt:
			return true;
		case T_RawStmt:
			{
				bool ret;
				
				RawStmt *stmt = (RawStmt *)parsetree;

				ret = isDDL(stmt->stmt);

				if (ret)
					return true;
			}
			break;
		case T_VacuumStmt:
			{
				VacuumStmt *stmt = (VacuumStmt *)parsetree;

				if (stmt->options & VACOPT_FULL)
				{
					return true;
				}
			}
			break;
		default:
			/* do nothing */
			break;
	}
	return false;
}

/* if node is locked, we will show the messages about lock type and lock action 
  * one node may have multi different locks, all locks and corresponding lock actions
  * will show here
  */
void
LightLockCheck(CmdType cmd, Oid table, int shard, int8 shardcluster)
{
	DMLLockContent *lock = NULL;
	if (cmd < CMD_SELECT || cmd > CMD_DELETE)
	{
		return;
	}

	if (cmd == CMD_SELECT)
	{
		cmd = CMD_SELECT + 4;
	}

	lock = &nodelock->lock[cmd - OFFSET];

	/*
	 * For performance, remove the lock acquire, can not check the latest value,
	 * the effect is to delay the node lock effect or let the statement execute this time.
	 * LWLockAcquire(NodeLockMgrLock, LW_SHARED);
	 */

	if (lock->nTables > 0 && OidIsValid(table))
	{
		int idx = 0;

		idx = HashSearch(lock, table, FIND);

		if (idx > 0)
		{
			elog(ERROR, "%s on table %s is not permitted.", LockMessages[cmd - OFFSET], get_rel_name(table));
		}
	}
	
	if (ShardIDIsValid(shard) && lock->nShards[shard] > 0 )
	{
		Bitmapset *shardbitmap = (Bitmapset *)lock->shard;
		
		if (bms_is_member(shard, shardbitmap))
		{
			elog(ERROR, "%s on shard %d is not permitted.", LockMessages[cmd - OFFSET], shard);
		}
	}
}


/* node lock check
  * if current node has locks, we have to check whether query is permitted or not by locks
  * if query is not permitted, error messages will be shown, and transaction is aborted.
  */
void 
HeavyLockCheck(const char* cmdString, CmdType cmd, const char *query_string, void *parsetree)
{	
    char *lockFuncName = "pg_node_lock";
    char *unlockFuncName = "pg_node_unlock";
	char *statFuncName = "show_node_lock";
	char *poolReloadFunc = "pgxc_pool_reload";
	char *terminateFunc = "pg_terminate_backend";


	if ((nodelock->flags & SELECT) && ((cmdString && strcmp(cmdString, "SELECT") == 0) || cmd == CMD_SELECT))
	{
		if (query_string)
		{
		    char *query = asc_tolower(query_string, strlen(query_string));
		    
	        if(strstr(query, lockFuncName) != NULL || strstr(query, unlockFuncName) != NULL ||
				strstr(query, statFuncName) != NULL || strstr(query, poolReloadFunc) != NULL ||
				strstr(query, terminateFunc) != NULL)
	        {
	            pfree(query);
	            return;
	        }
				
		    pfree(query);
		}
	}


	/* if query is DDL, we'll check if current node has locks; 
	  * if so, and DDL is permitted, error messages will be shown.
	  * current transaction is aborted.
	  */
	if(parsetree && isDDL(parsetree))
	{
		if (nodelock->flags & DDL)
		{
			elog(ERROR, "%s is not permitted now.", CreateCommandTag((Node *)parsetree));
		}
	}
	/*
	  * query is DML (update, insert, delete) or select
	  */
	else
	{
		/* if query is select, we can not block the node lock and node unlock functions
		  * Here we make a check, if query is one of them, just go through, do not check.
		  */
		if((cmdString && strcmp(cmdString, "SELECT") == 0) || cmd == CMD_SELECT)
		{
			if (nodelock->flags & SELECT)
			{
				elog(ERROR, "SELECT is not permitted now.");
			}
		}
		else if((cmdString && strcmp(cmdString, "UPDATE") == 0) || cmd == CMD_UPDATE)
		{
			if (nodelock->flags & UPDATE)
			{
				elog(ERROR, "UPDATE is not permitted now.");
			}
		}
		else if((cmdString && strcmp(cmdString, "DELETE") == 0) || cmd == CMD_DELETE)
		{
			if (nodelock->flags & DELETE)
			{
				elog(ERROR, "DELETE is not permitted now.");
			}
		}
		else if((cmdString && strcmp(cmdString, "INSERT") == 0) || cmd == CMD_INSERT)
		{
			if (nodelock->flags & INSERT)
			{
				elog(ERROR, "INSERT is not permitted now.");
			}
		}
	}
}

typedef struct
{
	int currIdx;
    int pos;
} ShmMgr_State;

Datum show_node_lock(PG_FUNCTION_ARGS)
{
#define STAT_COLUMN_NUM 9

	FuncCallContext *funcctx = NULL;
	ShmMgr_State	*status  = NULL;
	Relation rel = NULL;

	if (SRF_IS_FIRSTCALL())
	{

		TupleDesc	tupdesc;
		MemoryContext oldcontext;

		LWLockAcquire(NodeLockMgrLock, LW_SHARED);

		memcpy((char *)&nodelock_Copy, (char *)nodelock, NODELOCKSIZE);
		
		/* create a function context for cross-call persistence */
		funcctx = SRF_FIRSTCALL_INIT();

		/*
		  * Switch to memory context appropriate for multiple function calls
		  */
		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

		/* build tupdesc for result tuples */
		tupdesc = CreateTemplateTupleDesc(STAT_COLUMN_NUM, false);
		TupleDescInitEntry(tupdesc, (AttrNumber) 1, "HeavyLock",
						 TEXTOID, -1, 0);

		TupleDescInitEntry(tupdesc, (AttrNumber) 2, "LightLock",
						 TEXTOID, -1, 0);

		TupleDescInitEntry(tupdesc, (AttrNumber) 3, "Schema",
						 TEXTOID, -1, 0);

		TupleDescInitEntry(tupdesc, (AttrNumber) 4, "Table",
						 TEXTOID, -1, 0);

		TupleDescInitEntry(tupdesc, (AttrNumber) 5, "Shard",
						 TEXTOID, -1, 0);
		
		TupleDescInitEntry(tupdesc, (AttrNumber) 6, "EventLock",
						 TEXTOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 7, "ColstoreLock",
						 TEXTOID, -1, 0);
						 
		TupleDescInitEntry(tupdesc, (AttrNumber) 8, "ColstoreShard",
						 TEXTOID, -1, 0);
		
		TupleDescInitEntry(tupdesc, (AttrNumber) 9, "ShardCluster",
		                   TEXTOID, -1, 0);

		funcctx->tuple_desc = BlessTupleDesc(tupdesc);

		status = (ShmMgr_State *) palloc0(sizeof(ShmMgr_State));
		funcctx->user_fctx = (void *) status;

		status->currIdx = 0;
		
		MemoryContextSwitchTo(oldcontext);
	}

	funcctx = SRF_PERCALL_SETUP();
	status	= (ShmMgr_State *) funcctx->user_fctx;

	/* show event locks */
	if (nodelock_Copy.eventLocks)
	{
		Datum		values[STAT_COLUMN_NUM];
		bool		nulls[STAT_COLUMN_NUM];
		HeapTuple	tuple;
		Datum		result;
			
		MemSet(values, 0, sizeof(values));
		MemSet(nulls,  0, sizeof(nulls));

		nulls[0] = true;
		nulls[1] = true;
		nulls[2] = true;
		nulls[3] = true;
		nulls[4] = true;
		nulls[6] = true;
		nulls[7] = true;
		nulls[8] = true;
		
		values[5] = CStringGetTextDatum(event_message[my_log2(nodelock_Copy.eventLocks)]);

		nodelock_Copy.eventLocks = 0;

		tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);
		result = HeapTupleGetDatum(tuple);
		SRF_RETURN_NEXT(funcctx, result);
	}

	/* show heavy locks */
	if (nodelock_Copy.flags)
	{
		Datum		values[STAT_COLUMN_NUM];
		bool		nulls[STAT_COLUMN_NUM];
		HeapTuple	tuple;
		Datum		result;
			
		MemSet(values, 0, sizeof(values));
		MemSet(nulls,  0, sizeof(nulls));

		nulls[1] = true;
		nulls[2] = true;
		nulls[3] = true;
		nulls[4] = true;
		nulls[5] = true;
		nulls[6] = true;
		nulls[7] = true;
		nulls[8] = true;

		if (nodelock_Copy.flags & DDL)
		{
			values[0] = CStringGetTextDatum("DDL");
			nodelock_Copy.flags = nodelock_Copy.flags & ~DDL;
		}
		else if (nodelock_Copy.flags & UPDATE)
		{
			values[0] = CStringGetTextDatum("UPDATE");
			nodelock_Copy.flags = nodelock_Copy.flags & ~UPDATE;
		}
		else if (nodelock_Copy.flags & INSERT)
		{
			values[0] = CStringGetTextDatum("INSERT");
			nodelock_Copy.flags = nodelock_Copy.flags & ~INSERT;
		}
		else if (nodelock_Copy.flags & DELETE)
		{
			values[0] = CStringGetTextDatum("DELETE");
			nodelock_Copy.flags = nodelock_Copy.flags & ~DELETE;
		}
		else if (nodelock_Copy.flags & SELECT)
		{
			values[0] = CStringGetTextDatum("SELECT");
			nodelock_Copy.flags = nodelock_Copy.flags & ~SELECT;
		}


		tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);
		result = HeapTupleGetDatum(tuple);
		SRF_RETURN_NEXT(funcctx, result);
	}
#ifdef __OPENTENBASE_C__
	/* show colstore locks */
	if (nodelock_Copy.colFlags)
	{
		Datum		values[STAT_COLUMN_NUM];
		bool		nulls[STAT_COLUMN_NUM];
		HeapTuple	tuple;
		Datum		result;
			
		MemSet(values, 0, sizeof(values));
		MemSet(nulls,  0, sizeof(nulls));

		nulls[0] = true;
		nulls[1] = true;
		nulls[2] = true;
		nulls[3] = true;
		nulls[4] = true;
		nulls[5] = true;
		nulls[7] = true;
		nulls[8] = true;
		
		if (nodelock_Copy.colFlags & UPDATE)
		{
			values[6] = CStringGetTextDatum("UPDATE");
			nodelock_Copy.colFlags = nodelock_Copy.colFlags & ~UPDATE;
		}
		else if (nodelock_Copy.colFlags & INSERT)
		{
			values[6] = CStringGetTextDatum("INSERT");
			nodelock_Copy.colFlags = nodelock_Copy.colFlags & ~INSERT;
		}
		else if (nodelock_Copy.colFlags & DELETE)
		{
			values[6] = CStringGetTextDatum("DELETE");
			nodelock_Copy.colFlags = nodelock_Copy.colFlags & ~DELETE;
		}
		else if (nodelock_Copy.colFlags & SELECT)
		{
			values[6] = CStringGetTextDatum("SELECT");
			nodelock_Copy.colFlags = nodelock_Copy.colFlags & ~SELECT;
		}

		tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);
		result = HeapTupleGetDatum(tuple);
		SRF_RETURN_NEXT(funcctx, result);
	}
#endif
	while (status->currIdx < NUM_LIGHT_LOCK)
	{
		Datum		values[STAT_COLUMN_NUM];
		bool		nulls[STAT_COLUMN_NUM];
		HeapTuple	tuple;
		Datum		result;
		Bitmapset *shardbitmap = (Bitmapset *)nodelock_Copy.lock[status->currIdx].shard;
		Bitmapset *sc_bitmap = (Bitmapset *)nodelock_Copy.lock[status->currIdx].shardCluster;
#ifdef __OPENTENBASE_C__
		Bitmapset *colbitmap = (Bitmapset *)nodelock_Copy.lock[status->currIdx].colShard;
#endif

		MemSet(values, 0, sizeof(values));
		MemSet(nulls,  0, sizeof(nulls));
		
		if (nodelock_Copy.lock[status->currIdx].nTables)
		{
			for (; status->pos < (MAX_TABLE_NUM + 1); status->pos++)
			{
				Oid table = nodelock_Copy.lock[status->currIdx].table[status->pos];
				
				if (OidIsValid(table))
				{
					nulls[0] = true;
					nulls[4] = true;
					nulls[5] = true;
					nulls[6] = true;
					nulls[7] = true;
					nulls[8] = true;

					values[1] = CStringGetTextDatum(LockMessages[status->currIdx]);

                    rel = heap_open(table, AccessShareLock);

                    values[2] = CStringGetTextDatum(get_namespace_name(rel->rd_rel->relnamespace));
                    values[3] = CStringGetTextDatum(NameStr(rel->rd_rel->relname));

                    heap_close(rel, AccessShareLock);

					nodelock_Copy.lock[status->currIdx].table[status->pos] = InvalidOid;

					nodelock_Copy.lock[status->currIdx].nTables--;

					tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);
					result = HeapTupleGetDatum(tuple);
					SRF_RETURN_NEXT(funcctx, result);
				}
			}
		}
		
		if (!bms_is_empty(sc_bitmap))
		{
			int shard = 0;
			int shardcluster = 0;
			int lineNum = 0;
			StringInfoData str;
			
			
			nulls[0] = true;
			nulls[2] = true;
			nulls[3] = true;
			nulls[5] = true;
			nulls[6] = true;
			nulls[7] = true;
			
			initStringInfo(&str);
			
			values[1] = CStringGetTextDatum(LockMessages[status->currIdx]);
			
			while ((shard = bms_first_member(shardbitmap)) >= 0)
			{
				appendStringInfo(&str, "%d ", shard);
				lineNum++;
				
				if (lineNum == 10)
				{
					lineNum = 0;
					appendStringInfoChar(&str, '\n');
				}
			}
			
			values[4] = CStringGetTextDatum(str.data);
			
			resetStringInfo(&str);
			while ((shardcluster = bms_first_member(sc_bitmap)) >= 0)
			{
				appendStringInfo(&str, "%d ", shardcluster);
			}
			
			values[8] = CStringGetTextDatum(str.data);
			
			
			tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);
			result = HeapTupleGetDatum(tuple);
			SRF_RETURN_NEXT(funcctx, result);
		}
		
		if (!bms_is_empty(shardbitmap))
		{
			int shard = 0;
			int lineNum = 0;
			StringInfoData str;
			
			
			nulls[0] = true;
			nulls[2] = true;
			nulls[3] = true;
			nulls[5] = true;
			nulls[6] = true;
			nulls[7] = true;
			nulls[8] = true;

			initStringInfo(&str);

			values[1] = CStringGetTextDatum(LockMessages[status->currIdx]);

			while ((shard = bms_first_member(shardbitmap)) >= 0)
			{
				appendStringInfo(&str, "%d ", shard);
				lineNum++;

				if (lineNum == 10)
				{
					lineNum = 0;
					appendStringInfoChar(&str, '\n');
				}
			}

			values[4] = CStringGetTextDatum(str.data);

			tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);
			result = HeapTupleGetDatum(tuple);
			SRF_RETURN_NEXT(funcctx, result);
		}
#ifdef __OPENTENBASE_C__
		if (!bms_is_empty(colbitmap))
		{
			int shard = 0;
			int lineNum = 0;
			StringInfoData str;
			
			
			nulls[0] = true;
			nulls[1] = true;
			nulls[2] = true;
			nulls[3] = true;
			nulls[4] = true;
			nulls[5] = true;
			nulls[8] = true;

			initStringInfo(&str);

			values[6] = CStringGetTextDatum(LockMessages[status->currIdx]);

			while ((shard = bms_first_member(colbitmap)) >= 0)
			{
				appendStringInfo(&str, "%d ", shard);
				lineNum++;

				if (lineNum == 10)
				{
					lineNum = 0;
					appendStringInfoChar(&str, '\n');
				}
			}

			values[7] = CStringGetTextDatum(str.data);

			tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);
			result = HeapTupleGetDatum(tuple);
			SRF_RETURN_NEXT(funcctx, result);
		}

#endif
		status->pos = 0;
		status->currIdx++;
	}
	
	LWLockRelease(NodeLockMgrLock);
	SRF_RETURN_DONE(funcctx);
}

Datum pg_node_lock(PG_FUNCTION_ARGS)
{
	bool ret;
	char *cmd_string = NULL;
	char lock_object;
	char *param1 = NULL;
	char *param2 = NULL;
	int32 loopTimes = 0;

	cmd_string = text_to_cstring(PG_GETARG_TEXT_P(0));

	lock_object = PG_GETARG_CHAR(1);

	param1 = text_to_cstring(PG_GETARG_TEXT_P(2));

	param2 = text_to_cstring(PG_GETARG_TEXT_P(3));

	loopTimes = PG_GETARG_INT32(4);

	ret = NodeLock(cmd_string, lock_object, param1, param2, loopTimes);

	PG_RETURN_BOOL(ret);
}
Datum pg_node_unlock(PG_FUNCTION_ARGS)
{
	bool ret;
	char *cmd_string = NULL;
	char lock_object;
	char *param1 = NULL;
	char *param2 = NULL;

	cmd_string = text_to_cstring(PG_GETARG_TEXT_P(0));

	lock_object = PG_GETARG_CHAR(1);

	param1 = text_to_cstring(PG_GETARG_TEXT_P(2));

	param2 = text_to_cstring(PG_GETARG_TEXT_P(3));

	ret = NodeUnLock(cmd_string, lock_object, param1, param2);

	PG_RETURN_BOOL(ret);
}
void nodeLockRecovery(void)
{
    char buf[NODELOCKSIZE + 1];
    int ret;
    int fd;
    
    if(access(controlFile, F_OK) == 0)
    {
        fd = BasicOpenFile(controlFile, O_RDONLY);
    	if (fd < 0)
    	{   
    		ereport(ERROR,
    			(errcode_for_file_access(),
    			errmsg("could not open control file \"%s\"", controlFile)));
    	}
        if((ret = read(fd, buf, NODELOCKSIZE)) == NODELOCKSIZE)
        {
            memcpy(nodelock, buf, NODELOCKSIZE);
        }
        else
        {
        	close(fd);
            ereport(ERROR,
                (errcode_for_file_access(),
                errmsg("could not read control file \"%s\"", controlFile)));
        }
		close(fd);
    }
}

typedef enum LockParam
{
  LOCK_ACTIONS,
  LOCK_OBJECT,
  LOCK_PARAM1,
  LOCK_PARAM2
} LockParam;

void
LockNode(LockNodeStmt *stmt)
{
	bool ret = false;
	Value *v = NULL;
	int loopTimes = 0;
	char *lockActions = NULL;
	char *objectType = NULL;
	char *param1 = "";
	char *param2 = "";
	
	if (list_length(stmt->params) < 3)
	{
		elog(ERROR, "Lock Node needs at least 3 parameters.");
	}

	v = (Value *)list_nth(stmt->params, LOCK_ACTIONS);

	if (v->type == T_String)
	{
		lockActions = v->val.str;
	}

	v = (Value *)list_nth(stmt->params, LOCK_OBJECT);

	if (v->type == T_String)
	{
		objectType = v->val.str;
	}

	if (list_length(stmt->params) - 1 >  LOCK_PARAM1)
	{
		v = (Value *)list_nth(stmt->params, LOCK_PARAM1);

		if (v->type == T_String)
		{
			param1 = v->val.str;
		}
	}

	if (list_length(stmt->params) - 1 >  LOCK_PARAM2)
	{
		v = (Value *)list_nth(stmt->params, LOCK_PARAM2);

		if (v->type == T_String)
		{
			param2 = v->val.str;
		}
	}

	v = (Value *)list_nth(stmt->params, list_length(stmt->params) - 1);

	if (v->type == T_Integer)
	{
		loopTimes = v->val.ival;
	}

	if (!lockActions)
		elog(ERROR, "you need to specify lock query type.");
	
	if (!objectType)
		elog(ERROR, "you need to specify lock object.");

	ret = NodeLock(lockActions, objectType[0], param1, param2, loopTimes);

	if (!ret)
		elog(ERROR, "Failed to lock node.");
}
void
UnLockNode(LockNodeStmt *stmt)
{
	bool ret = false;
	Value *v = NULL;
	char *lockActions = NULL;
	char *objectType = NULL;
	char *param1 = "";
	char *param2 = "";

	if (list_length(stmt->params) < 2)
	{
		elog(ERROR, "Lock Node needs at least 2 parameters.");
	}

	v = (Value *)list_nth(stmt->params, LOCK_ACTIONS);

	if (v->type == T_String)
	{
		lockActions = v->val.str;
	}

	v = (Value *)list_nth(stmt->params, LOCK_OBJECT);

	if (v->type == T_String)
	{
		objectType = v->val.str;
	}

	if (list_length(stmt->params) >  LOCK_PARAM1)
	{
		v = (Value *)list_nth(stmt->params, LOCK_PARAM1);

		if (v->type == T_String)
		{
			param1 = v->val.str;
		}
	}

	if (list_length(stmt->params) >  LOCK_PARAM2)
	{
		v = (Value *)list_nth(stmt->params, LOCK_PARAM2);

		if (v->type == T_String)
		{
			param2 = v->val.str;
		}
	}

	if (!lockActions)
		elog(ERROR, "you need to specify lock query type.");

	if (!objectType)
		elog(ERROR, "you need to specify lock object.");

	ret = NodeUnLock(lockActions, objectType[0], param1, param2);

	if (!ret)
		elog(ERROR, "Failed to unlock node.");
}
