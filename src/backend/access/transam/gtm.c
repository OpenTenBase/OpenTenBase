/*-------------------------------------------------------------------------
 *
 * gtm.c
 *
 *	  Module interfacing with GTM
 *
 *
 *-------------------------------------------------------------------------
 */

#include <sys/types.h>
#include <unistd.h>

#include "postgres.h"
#include "gtm/libpq-fe.h"
#include "gtm/gtm_client.h"
#include "access/gtm.h"
#include "access/mvccvars.h"
#include "access/transam.h"
#include "access/xact.h"

#include "utils/elog.h"
#include "miscadmin.h"
#include "pgxc/execRemote.h"
#include "pgxc/pgxc.h"
#include "gtm/gtm_c.h"
#include "postmaster/autovacuum.h"
#include "postmaster/clean2pc.h"
#include "postmaster/clustermon.h"
#include "storage/backendid.h"
#include "tcop/tcopprot.h"
#include "utils/guc.h"
#include "utils/lsyscache.h"
#include "utils/pg_rusage.h"
#ifdef __OPENTENBASE__
#include "access/htup_details.h"
#include "access/xlog.h"
#include "catalog/pg_type.h"
#include "catalog/pgxc_node.h"
#include "fmgr.h"
#include "funcapi.h"
#include "pgxc/nodemgr.h"
#include "storage/lmgr.h"
#include "utils/builtins.h"
#include "utils/memutils.h"
#include "utils/syscache.h"
#include "utils/tqual.h"
#include "utils/xact_whitebox.h"
#endif
#ifdef __OPENTENBASE_C__
#include "gtm/gtm_fid.h"
#include "pgxc/pgxcnode.h"
#endif

#ifdef __RESOURCE_QUEUE__
#include "commands/resqueue.h"
#endif

#ifdef __OPENTENBASE_C__
bool enable_gts_check;
#endif
int gts_cache_timeout;
bool feedbackGts = false;

/* Not supported in centralized mode, check and report error */
#define NOT_SUPPORTED_IN_CENTRALIZED_MODE \
do { \
	if (IS_CENTRALIZED_MODE) \
		elog(ERROR, "It is not supported in centralized mode"); \
} while (0)

#ifdef _PG_ORCL_
#include "access/atxact.h"
#endif

/* To access sequences */
#define GetMyCoordName \
	OidIsValid(MyCoordId) ? get_pgxc_nodename(MyCoordId) : ""
/* Configuration variables */
#ifdef __OPENTENBASE__
/*
 * This two value set while accept create/alter gtm node command
 */
char *NewGtmHost = NULL;
int	  NewGtmPort = -1;
bool  g_GTM_skip_catalog = false;
char *gtm_unix_socket_directory = DEFAULT_PGSOCKET_DIR;
#endif

int reconnect_gtm_retry_times = 3;
int reconnect_gtm_retry_interval = 500;

char *GtmHost = NULL;
int GtmPort = 0;
static int GtmConnectTimeout = 60;
bool IsXidFromGTM = false;
bool gtm_backup_barrier = false;
extern bool FirstSnapshotSet;
bool GTMDebugPrint = false;
static GTM_Conn *conn = NULL;

/* Used to check if needed to commit/abort at datanodes */
GlobalTransactionId currentGxid = InvalidGlobalTransactionId;

#ifdef __OPENTENBASE__
typedef struct
{
	int			currIdx;		/* current PROCLOCK index */
	int         totalIdx;
	char        *data;
} PG_Storage_status;

List *g_CreateSeqList = NULL;
List *g_DropSeqList   = NULL;
List *g_AlterSeqList  = NULL;
/* constant postfix for sequence to avoid same name */
#define GTM_SEQ_POSTFIX "_$OPENTENBASE$_sequence_temp_54312678712612"
static void CheckConnection(void);
static void ResetGTMConnection(void);
static int GetGTMStoreStatus(GTMStorageStatus *header);
static int GetGTMStoreSequence(GTM_StoredSeqInfo **store_seq);
static int GetGTMStoreTransaction(GTM_StoredTransactionInfo **store_txn);
static int CheckGTMStoreTransaction(GTMStorageTransactionStatus **store_txn, bool need_fix);
static int CheckGTMStoreSequence(GTMStorageSequneceStatus **store_seq, bool need_fix);

/* when modify this, had better modify GTMStorageError first*/
void
RestoreSeqCreate(CreateInfo *create_info, int32 count, bool iscommit)
{
	int32	ret = 0;
	int		i = 0;

	if (iscommit)
		return;

	for (i = 0; i < count; i++)
	{
		CreateInfo key = create_info[i];
		ret = DropSequenceGTM(key.name, key.gsk_type);
		if (ret)
		{
			elog(WARNING, "DropSequenceGTM failed for seq:%s type:%d commit:%d",
					key.name, key.gsk_type, iscommit);
		}
	}
}

void
RegisterSeqCreate(char *name, int32 type)
{
	GTM_SequenceKeyData *key 	 = NULL;
	MemoryContext 		 old_cxt = NULL;
	int 				 level	 = GetCurrentTransactionNestLevel();
	
	old_cxt = MemoryContextSwitchTo(TopMemoryContext);
	key = (GTM_SequenceKeyData*)palloc(sizeof(GTM_SequenceKeyData));
	key->gsk_keylen  = strlen(name);
	key->gsk_key     = pstrdup(name);
	key->gsk_type    = type;	
	key->nestLevel	 = level;
	g_CreateSeqList = lcons(key, g_CreateSeqList);
	MemoryContextSwitchTo(old_cxt);
}

void
RestoreSeqDrop(DropInfo *drop_info_array, int32 count, bool iscommit)
{
	int i = 0;
	int32 ret = 0;

	for (i = 0; i < count; i++)
	{
		DropInfo drop_info = drop_info_array[i];
		if (iscommit)
		{
			ret = DropSequenceGTM(drop_info.new_, drop_info.gsk_type);
			if (ret)
			{
				elog(WARNING, "DropSequenceGTM failed for seq:%s type:%d commit:%d",
						drop_info.new_, drop_info.gsk_type, iscommit);
			}
		}
		else
		{
			if (GTM_SEQ_FULL_NAME == drop_info.gsk_type)
			{
				ret = RenameSequenceGTM(drop_info.new_, drop_info.old);
				if (ret)
				{
					elog(WARNING, "RenameSequenceGTM seq:%s to:%s failed commit:%d",
							drop_info.new_, drop_info.old, iscommit);
				}
			}
		}
	}
}

void RegisterSeqDrop(char *name, int32 type)
{
	DropInfo 			 *drop_info = NULL;
	MemoryContext		 old_cxt   = NULL;
	struct timeval       tp;
	char				 temp[GTM_NAME_LEN];
	
	gettimeofday(&tp, NULL);
	if (GTM_SEQ_FULL_NAME == type)
	{
		/* Here we can only add postfix for the temp sequence, or drop database will fail. */
		snprintf(temp, GTM_NAME_LEN, "%s"GTM_SEQ_POSTFIX, name);
		if (RenameSequenceGTM((char *)name, temp))
		{
			elog(ERROR, "Deletion of sequences on database %s failed when backup old seq", name);
		}
	}
	else
	{
		snprintf(temp, GTM_NAME_LEN, "%s", name);
	}	
								
	old_cxt = MemoryContextSwitchTo(TopMemoryContext);
	drop_info = (DropInfo*)palloc(sizeof(DropInfo));
	snprintf(drop_info->new_, GTM_NAME_LEN, "%s", temp);
	snprintf(drop_info->old, GTM_NAME_LEN, "%s", name);
	drop_info->gsk_type = type;
	drop_info->nestLevel = GetCurrentTransactionNestLevel();
	g_DropSeqList		 = lcons(drop_info, g_DropSeqList);
	MemoryContextSwitchTo(old_cxt);
}

void
RestoreSeqRename(RenameInfo *rename_info_array, int32 count, bool iscommit)
{
	int i = 0;
	int32 ret = 0;

	if (iscommit)
		return;

	for (i = 0; i < count; i++)
	{
		RenameInfo rename_info = rename_info_array[i];
		ret = RenameSequenceGTM(rename_info.new_, rename_info.old);
		if (ret)
		{
			elog(WARNING, "RenameSequenceGTM seq:%s to:%s failed commit:%d",
					rename_info.new_, rename_info.old, iscommit);
		}
	}
}

void
RegisterSeqRename(char *new_, char *old)
{
	MemoryContext old_cxt;
	RenameInfo    *info = NULL;
	ListCell   	  *cell = NULL;
	RenameInfo    *rename_info = NULL;
	int			  level = GetCurrentTransactionNestLevel();

	/* combine the alter operation of the same sequence in the same transaction .
	 * EXAMPLE:
	 * RENAME SEQUENCE A TO B --\
	 *                           > RENAME SEQUENCE A TO C
	 * RENAME SEQUENCE B TO C --/
	 */
	foreach(cell, g_AlterSeqList)
	{
		rename_info = (RenameInfo *) lfirst(cell);			
		if (0 == strncmp(rename_info->new_, old, GTM_NAME_LEN) && rename_info->nestLevel == level)
		{			
			elog(LOG, "Combine requence seq:%s ->:%s, %s->%s to old:%s latest new_:%s", rename_info->new_, rename_info->old, new_, old, rename_info->old, new_);
			snprintf(rename_info->new_, GTM_NAME_LEN, "%s", new_);
			return;
		}		
	}
	
	old_cxt = MemoryContextSwitchTo(TopMemoryContext);
	info = palloc(sizeof(RenameInfo));
	snprintf(info->new_, GTM_NAME_LEN, "%s", new_);
	snprintf(info->old, GTM_NAME_LEN, "%s", old);
	info->nestLevel = level;
	g_AlterSeqList = lcons(info, g_AlterSeqList);
	MemoryContextSwitchTo(old_cxt);
}

void FinishSeqOp(bool commit)
{
	int32				 ret  		 = 0;
	ListCell   			*cell 		 = NULL;
	GTM_SequenceKeyData *key  		 = NULL;
	RenameInfo          *rename_info = NULL;
	DropInfo            *drop_info = NULL;
	int					nestLevel = 0;
	List				*delList = NIL;
	ListCell			*dellc = NULL;
	List				*renameList = NIL;
	ListCell			*renamelc = NULL;
	
	if (!IS_PGXC_LOCAL_COORDINATOR)
	{
		return;
	}

	nestLevel = GetCurrentTransactionNestLevel();
	if (commit)
	{
		/* do the drop for drop list */
		foreach(cell, g_DropSeqList)
		{
			drop_info = (DropInfo *) lfirst(cell);
			if (drop_info->nestLevel < nestLevel)
			{
				continue;
			}
			else
			{
				delList = lappend(delList, drop_info);
			}
		}
		/* rollback the created list */
		foreach(dellc, delList)
		{
			drop_info = (DropInfo *) lfirst(dellc);

			ret = DropSequenceGTM(drop_info->new_, drop_info->gsk_type);
			if (ret)
			{
				elog(LOG, "DropSequenceGTM failed for seq:%s type:%d commit:%d", drop_info->new_, drop_info->gsk_type, commit);
			}

			g_DropSeqList = list_delete_ptr(g_DropSeqList, drop_info);
		}
		list_free(delList);
		delList = NIL;

		/* Remove sequence from g_CreateSeqList for current nestlevel commit */
		foreach(cell, g_CreateSeqList)
		{
			key = (GTM_SequenceKeyData *) lfirst(cell);
			if (key->nestLevel < nestLevel)
			{
				continue;
			}
			else
			{
				delList = lappend(delList, key);
			}
		}
		foreach(dellc, delList)
		{
			key = (GTM_SequenceKeyData *) lfirst(dellc);
			pfree(key->gsk_key);
			g_CreateSeqList = list_delete_ptr(g_CreateSeqList, key);
		}
		list_free(delList);
		delList = NIL;

		/* do the alter for the alter list */
		foreach(cell, g_AlterSeqList)
		{
			rename_info = (RenameInfo *) lfirst(cell);
			if (rename_info->nestLevel < nestLevel)
			{
				continue;
			}
			else
			{
				renameList = lappend(renameList, rename_info);
			}
		}
		foreach(renamelc, renameList)
		{
			g_AlterSeqList = list_delete_ptr(g_AlterSeqList, rename_info);
		}
		list_free(renameList);
		renameList = NIL;
	}
	else
	{
		/* Get seq that need be rollbacked */
		foreach(cell, g_CreateSeqList)
		{
			key = (GTM_SequenceKeyData *) lfirst(cell);
			if (key->nestLevel < nestLevel)
			{
				continue;
			}
			else
			{
				delList = lappend(delList, key);
			}
		}
		/* rollback the created list */
		foreach(dellc, delList)
		{
			key = (GTM_SequenceKeyData *) lfirst(dellc);

			ret = DropSequenceGTM(key->gsk_key, key->gsk_type);
			if (ret)
			{
				elog(LOG, "DropSequenceGTM failed for seq:%s type:%d commit:%d", key->gsk_key, key->gsk_type, commit);
			}

			pfree(key->gsk_key);
			g_CreateSeqList = list_delete_ptr(g_CreateSeqList, key);
		}
		list_free(delList);
		delList = NIL;

		/* roll back the drop list */
		foreach(cell, g_DropSeqList)
		{
			drop_info = (DropInfo *) lfirst(cell);
			if (drop_info->nestLevel < nestLevel)
			{
				continue;
			}
			else
			{
				delList = lappend(delList, drop_info);
			}
		}
		foreach(dellc, delList)
		{
			drop_info = (DropInfo *) lfirst(dellc);
			/* No need to process GTM_SEQ_DB_NAME here. */
			if (GTM_SEQ_FULL_NAME == drop_info->gsk_type)
			{
				ret = RenameSequenceGTM(drop_info->new_, drop_info->old);
				if (ret)
				{
					elog(LOG, "RenameSequenceGTM seq:%s to:%s failed commit:%d", drop_info->new_, drop_info->old, commit);
				}
			}
			g_DropSeqList = list_delete_ptr(g_DropSeqList, drop_info);
		}
		list_free(delList);
		delList = NIL;
		
		/* rollback for the alter list */
		foreach(cell, g_AlterSeqList)
		{
			rename_info = (RenameInfo *) lfirst(cell);
			if (rename_info->nestLevel < nestLevel)
			{
				continue;
			}
			else
			{
				renameList = lappend(renameList, rename_info);
			}
		}
		foreach(renamelc, renameList)
		{
			rename_info = (RenameInfo *) lfirst(renamelc);
			ret = RenameSequenceGTM(rename_info->new_, rename_info->old);
			if (ret)
			{
				elog(LOG, "RenameSequenceGTM seq:%s to:%s failed commit:%d", rename_info->new_, rename_info->old, commit);
			}
			g_AlterSeqList = list_delete_ptr(g_AlterSeqList, rename_info);
		}
		list_free(renameList);
		renameList = NIL;
	}
}

/*
 * like AtSubCommit_smgr --- Take care of subtransaction commit.
 *
 * Reassign all items in the sequence lists to the parent transaction.
 */
void
AtSubCommit_seq(void)
{
	GTM_SequenceKeyData *key  		 = NULL;
	RenameInfo          *rename_info = NULL;
	DropInfo            *drop_info = NULL;
	int					nestLevel = GetCurrentTransactionNestLevel();
	ListCell   			*cell 		 = NULL;

	foreach(cell, g_CreateSeqList)
	{
		key = (GTM_SequenceKeyData *) lfirst(cell);
		if (key->nestLevel >= nestLevel)
		{
			key->nestLevel = nestLevel - 1;
		}
	}
	foreach(cell, g_DropSeqList)
	{
		drop_info = (DropInfo *) lfirst(cell);
		if (drop_info->nestLevel >= nestLevel)
		{
			drop_info->nestLevel = nestLevel - 1;
		}
	}
	foreach(cell, g_AlterSeqList)
	{
		rename_info = (RenameInfo *) lfirst(cell);
		if (rename_info->nestLevel >= nestLevel)
		{
			rename_info->nestLevel = nestLevel - 1;
		}
	}
}

/*
 * like PostPrepare_smgr -- Clean up after a successful PREPARE
 *
 * What we have to do here is throw away the in-memory state about
 * sequences lists.  It's all been recorded in the 2PC state file and
 * it's no longer FinishSeqOp to worry about it.
 */
void
PostPrepare_seq(void)
{
	ListCell   			*cell 		 = NULL;
	GTM_SequenceKeyData *key  		 = NULL;
	RenameInfo          *rename_info = NULL;
	DropInfo            *drop_info	 = NULL;

	if (g_CreateSeqList)
	{		
		foreach(cell, g_CreateSeqList)
		{
			key = (GTM_SequenceKeyData *) lfirst(cell);			
			pfree(key->gsk_key);
			pfree(key);
		}
		list_free(g_CreateSeqList);
		g_CreateSeqList = NULL;
	}

	if (g_DropSeqList)
	{
		foreach(cell, g_DropSeqList)
		{
			drop_info = (DropInfo *) lfirst(cell);
			pfree(drop_info);
		}
		list_free(g_DropSeqList);
		g_DropSeqList = NULL;
	}

	if (g_AlterSeqList)
	{
		foreach(cell, g_AlterSeqList)
		{
			rename_info = (RenameInfo *) lfirst(cell);
			pfree(rename_info);		
		}
		list_free(g_AlterSeqList);
		g_AlterSeqList = NULL;
	}
}

int32 GetGTMCreateSeq(char **create_info)
{
	int32                count   = 0;
	ListCell   			*cell 	 = NULL;
	GTM_SequenceKeyData *key  	 = NULL;	
	CreateInfo 			*content = NULL;
	int					nestLevel = GetCurrentTransactionNestLevel();

	content = (CreateInfo*)palloc0(MAXALIGN(sizeof(CreateInfo) * list_length(g_CreateSeqList)));
	foreach(cell, g_CreateSeqList)
	{
		key = (GTM_SequenceKeyData *) lfirst(cell);
		if (key->nestLevel >= nestLevel)
		{
			strncpy(content[count].name, key->gsk_key, GTM_NAME_LEN);
			content[count].gsk_type = key->gsk_type;
			count++;
		}
	}
	*create_info = (char*)content;
	return count;
}

int32 GetGTMDropSeq(char **drop_info)
{
	int32				 count	 = 0;
	ListCell			*cell	 = NULL;
	DropInfo 			*drop	 = NULL;	
	DropInfo			*content = NULL;
	int					nestLevel = GetCurrentTransactionNestLevel();

	content = (DropInfo*)palloc0(MAXALIGN(sizeof(DropInfo) * list_length(g_DropSeqList)));
	foreach(cell, g_DropSeqList)
	{
		drop = (DropInfo *) lfirst(cell);
		if (drop->nestLevel >= nestLevel)
		{
			strncpy(content[count].new_, drop->new_, GTM_NAME_LEN);
			strncpy(content[count].old, drop->old, GTM_NAME_LEN);
			content[count].gsk_type = drop->gsk_type;
			content[count].nestLevel = drop->nestLevel;
			count++;
		}
	}
	*drop_info = (char*)content;
	return count;
}

int32 GetGTMRenameSeq(char **rename_info)
{
	int32				 count	 = 0;
	ListCell			*cell	 = NULL;
	RenameInfo			*rename	 = NULL;	
	RenameInfo			*content = NULL;
	int					nestLevel = GetCurrentTransactionNestLevel();

	content = (RenameInfo*)palloc0(MAXALIGN(sizeof(RenameInfo) * list_length(g_AlterSeqList)));
	foreach(cell, g_AlterSeqList)
	{
		rename = (RenameInfo *) lfirst(cell);
		if (rename->nestLevel >= nestLevel)
		{
			strncpy(content[count].new_, rename->new_, GTM_NAME_LEN);
			strncpy(content[count].old, rename->old, GTM_NAME_LEN);
			content[count].nestLevel = rename->nestLevel;
			count++;
		}
	}
	*rename_info = (char*)content;
	return count;
}

/* Finish the transaction with gid. */
int FinishGIDGTM(char *gid)
{
	int ret = 0;

	CheckConnection();
	ret = -1;
	if (conn)
	{
		ret = finish_gid_gtm(conn, gid);
	}

	/*
	 * If something went wrong (timeout), try and reset GTM connection.
	 * We will abort the transaction locally anyway, and closing GTM will force
	 * it to end on GTM.
	 */
	if (ret < 0)
	{
		CloseGTM();
		InitGTM();
		if (conn)
		{
			ret = finish_gid_gtm(conn, gid);
		}
	}

	return ret;
}

/* Finish the transaction with gid. */
int GetGTMStoreStatus(GTMStorageStatus *header)
{
	int ret = 0;

	CheckConnection();
	ret = -1;
	if (conn)
	{
		ret = get_gtm_store_status(conn, header);
	}

	/*
	 * If something went wrong (timeout), try and reset GTM connection.
	 * We will abort the transaction locally anyway, and closing GTM will force
	 * it to end on GTM.
	 */
	if (ret < 0)
	{
		CloseGTM();
		InitGTM();
		if (conn)
		{
			ret = get_gtm_store_status(conn, header);
		}
	}

	return ret;
}


int GetGTMStoreSequence(GTM_StoredSeqInfo **store_seq)
{
	int ret = 0;

	CheckConnection();
	ret = -1;
	if (conn)
	{
		ret = get_storage_sequence_list(conn, store_seq);
	}

	/*
	 * If something went wrong (timeout), try and reset GTM connection.
	 * We will abort the transaction locally anyway, and closing GTM will force
	 * it to end on GTM.
	 */
	if (ret < 0)
	{
		CloseGTM();
		InitGTM();
		if (conn)
		{
			ret = get_storage_sequence_list(conn, store_seq);
		}
	}

	return ret;
}

int CheckGTMStoreTransaction(GTMStorageTransactionStatus **store_txn, bool need_fix)
{
	int ret = 0;

	CheckConnection();
	ret = -1;
	if (conn)
	{
		ret = check_storage_transaction(conn, store_txn, need_fix);
	}

	/*
	 * If something went wrong (timeout), try and reset GTM connection.
	 * We will abort the transaction locally anyway, and closing GTM will force
	 * it to end on GTM.
	 */
	if (ret < 0)
	{
		CloseGTM();
		InitGTM();
		if (conn)
		{
			ret = check_storage_transaction(conn, store_txn, need_fix);
		}
	}
	return ret;
}

int CheckGTMStoreSequence(GTMStorageSequneceStatus **store_seq, bool need_fix)
{
	int ret = 0;

	CheckConnection();
	ret = -1;
	if (conn)
	{
		ret = check_storage_sequence(conn, store_seq, need_fix);
	}

	/*
	 * If something went wrong (timeout), try and reset GTM connection.
	 * We will abort the transaction locally anyway, and closing GTM will force
	 * it to end on GTM.
	 */
	if (ret < 0)
	{
		CloseGTM();
		InitGTM();
		if (conn)
		{
			ret = check_storage_sequence(conn, store_seq, need_fix);
		}
	}

	return ret;
}

int GetGTMStoreTransaction(GTM_StoredTransactionInfo **store_txn)
{
	int ret = 0;

	CheckConnection();
	ret = -1;
	if (conn)
	{
		ret = get_storage_transaction_list(conn, store_txn);
	}

	/*
	 * If something went wrong (timeout), try and reset GTM connection.
	 * We will abort the transaction locally anyway, and closing GTM will force
	 * it to end on GTM.
	 */
	if (ret < 0)
	{
		CloseGTM();
		InitGTM();
		if (conn)
		{
			ret = get_storage_transaction_list(conn, store_txn);
		}
	}
	return ret;
}

/*
 * pg_list_gtm_store - produce a view with gtm store info.
 */
Datum
pg_list_gtm_store(PG_FUNCTION_ARGS)
{
#define  LIST_GTM_STORE_COLUMNS 16
	int32           ret = 0;
	TupleDesc		tupdesc;

	GTMStorageStatus gtm_status;

	Datum		values[LIST_GTM_STORE_COLUMNS];
	bool		nulls[LIST_GTM_STORE_COLUMNS];

	NOT_SUPPORTED_IN_CENTRALIZED_MODE;

	ret = GetGTMStoreStatus(&gtm_status);
	if (ret)
	{
		elog(ERROR, "get gtm info from gtm failed");
	}
   
	/* build tupdesc for result tuples */
	/* this had better match function's declaration in pg_proc.h */
	tupdesc = CreateTemplateTupleDesc(LIST_GTM_STORE_COLUMNS, false);
	

	TupleDescInitEntry(tupdesc, (AttrNumber) 1, "system_identifier",
					   INT8OID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) 2, "major_version",
					   INT4OID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) 3, "minor_version",
					   INT4OID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) 4, "gtm_status",
					   INT4OID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) 5, "global_time_stamp",
					   INT8OID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) 6, "global_xmin",
					   INT4OID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) 7, "next_gxid",
					   INT4OID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) 8, "seq_total",
					   INT4OID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) 9, "seq_used",
					   INT4OID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) 10, "seq_freelist",
					   INT4OID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) 11, "txn_total",
					   INT4OID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) 12, "txn_used",
					   INT4OID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) 13, "txn_freelist",
					   INT4OID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) 14, "system_lsn",
					   INT8OID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) 15, "last_update_time",
					   TIMESTAMPTZOID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) 16, "crc_check_value",
					   INT4OID, -1, 0);		
	BlessTupleDesc(tupdesc);
	MemSet(nulls, false, sizeof(nulls));
	
	/* Fill values and NULLs */
	values[0] = DatumGetInt32(gtm_status.header.m_identifier);
	values[1] = DatumGetInt32(gtm_status.header.m_major_version);
	values[2] = DatumGetInt32(gtm_status.header.m_minor_version);
	values[3] = DatumGetInt32(gtm_status.header.m_gtm_status);
	values[4] = DatumGetInt64(gtm_status.header.m_next_gts);
	values[5] = DatumGetInt32(gtm_status.header.m_global_xmin);;
	values[6] = DatumGetInt32(gtm_status.header.m_next_gxid);
	values[7] = DatumGetInt32(gtm_status.seq_total);
	values[8] = DatumGetInt32(gtm_status.seq_used);	
	values[9] = DatumGetInt32(gtm_status.header.m_seq_freelist);
	values[10] = DatumGetInt32(gtm_status.txn_total);
	values[11] = DatumGetInt32(gtm_status.txn_used);	
	values[12] = DatumGetInt32(gtm_status.header.m_txn_freelist);
	values[13] = DatumGetInt32(gtm_status.header.m_lsn);
	values[14] = TimestampGetDatum(gtm_status.header.m_last_update_time);
	values[15] = DatumGetInt32(gtm_status.header.m_crc);
	
	/* Returns the record as Datum */
	PG_RETURN_DATUM(HeapTupleGetDatum(heap_form_tuple(tupdesc, values, nulls)));
}


Datum
pg_list_storage_sequence(PG_FUNCTION_ARGS)
{
#define NUM_SEQ_STATUS_COLUMNS	15
	
	FuncCallContext   *funcctx;
	PG_Storage_status *mystatus;
	GTM_StoredSeqInfo *seqs;

	NOT_SUPPORTED_IN_CENTRALIZED_MODE;

	if (SRF_IS_FIRSTCALL())
	{
		TupleDesc	tupdesc;
		MemoryContext oldcontext;

		/* create a function context for cross-call persistence */
		funcctx = SRF_FIRSTCALL_INIT();
		
		/*
		 * switch to memory context appropriate for multiple function calls
		 */
		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);
		
		/* build tupdesc for result tuples */
		/* this had better match function's declaration in pg_proc.h */
		tupdesc = CreateTemplateTupleDesc(NUM_SEQ_STATUS_COLUMNS, false);
		TupleDescInitEntry(tupdesc, (AttrNumber) 1, "gsk_key",
						   TEXTOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 2, "gsk_type",
						   INT4OID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 3, "gs_value",
						   INT8OID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 4, "gs_init_value",
						   INT8OID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 5, "gs_increment_by",
						   INT8OID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 6, "gs_min_value",
						   INT8OID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 7, "gs_max_value",
						   INT8OID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 8, "gs_cycle",
						   BOOLOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 9, "gs_called",
						   BOOLOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 10, "gs_reserved",
						   BOOLOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 11, "gs_status",
						   INT4OID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 12, "gti_store_handle",
						   INT4OID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 13, "last_update_time",
						   TIMESTAMPTZOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 14, "gs_next",
						   INT4OID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 15, "gs_crc",
						   INT4OID, -1, 0);
		
		funcctx->tuple_desc = BlessTupleDesc(tupdesc);

		mystatus = (PG_Storage_status *) palloc0(sizeof(PG_Storage_status));
		funcctx->user_fctx = (void *) mystatus;

		MemoryContextSwitchTo(oldcontext);

		mystatus->totalIdx = GetGTMStoreSequence((GTM_StoredSeqInfo **)&mystatus->data);
		if (mystatus->totalIdx < 0)
		{
			elog(ERROR, "get sequence info from gtm failed");			
		}		
	}

	funcctx  = SRF_PERCALL_SETUP();
	mystatus = (PG_Storage_status *) funcctx->user_fctx;	
	seqs     = (GTM_StoredSeqInfo*)mystatus->data;
	while (mystatus->currIdx < mystatus->totalIdx)
	{
		Datum		values[NUM_SEQ_STATUS_COLUMNS];
		bool		nulls[NUM_SEQ_STATUS_COLUMNS];
		
		HeapTuple	tuple;
		Datum		result;
		GTM_StoredSeqInfo *instance;

		instance = &(seqs[mystatus->currIdx]);		

		/*
		 * Form tuple with appropriate data.
		 */
		MemSet(values, 0, sizeof(values));
		MemSet(nulls, false, sizeof(nulls));

		values[0] = CStringGetTextDatum(instance->gs_key.gsk_key);
		values[1] = Int32GetDatum(instance->gs_key.gsk_type);
		values[2] = Int64GetDatum(instance->gs_value);
		values[3] = Int64GetDatum(instance->gs_init_value);
		values[4] = Int64GetDatum(instance->gs_increment_by);
		values[5] = Int64GetDatum(instance->gs_min_value);
		values[6] = Int64GetDatum(instance->gs_max_value);
		values[7] = BoolGetDatum(instance->gs_cycle);
		values[8] = BoolGetDatum(instance->gs_called);
		values[9] = BoolGetDatum(instance->gs_reserved);
		values[10] = Int32GetDatum(instance->gs_status);
		values[11] = Int32GetDatum(instance->gti_store_handle);
		values[12] = TimestampTzGetDatum(instance->m_last_update_time);
		values[13] = Int32GetDatum(instance->gs_next);
		values[14] = UInt32GetDatum(instance->gs_crc);				
			

		tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);
		result = HeapTupleGetDatum(tuple);
		
		mystatus->currIdx++;
		SRF_RETURN_NEXT(funcctx, result);
	}	

	SRF_RETURN_DONE(funcctx);
}


Datum
pg_list_storage_transaction(PG_FUNCTION_ARGS)
{
#define NUM_TXN_STATUS_COLUMNS	7
	FuncCallContext   *funcctx;
	PG_Storage_status *mystatus;
	GTM_StoredTransactionInfo *txns;

	NOT_SUPPORTED_IN_CENTRALIZED_MODE;

	if (SRF_IS_FIRSTCALL())
	{
		TupleDesc	tupdesc;
		MemoryContext oldcontext;

		/* create a function context for cross-call persistence */
		funcctx = SRF_FIRSTCALL_INIT();
		
		/*
		 * switch to memory context appropriate for multiple function calls
		 */
		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);
		
		/* build tupdesc for result tuples */
		/* this had better match function's declaration in pg_proc.h */
		tupdesc = CreateTemplateTupleDesc(NUM_TXN_STATUS_COLUMNS, false);
		TupleDescInitEntry(tupdesc, (AttrNumber) 1, "gti_gid",
						   TEXTOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 2, "node_list",
						   TEXTOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 3, "gti_state",
						   INT4OID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 4, "gti_store_handle",
						   INT4OID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 5, "last_update_time",
						   TIMESTAMPTZOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 6, "gs_next",
						   INT4OID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 7, "gs_crc",
						   INT4OID, -1, 0);
		
		funcctx->tuple_desc = BlessTupleDesc(tupdesc);

		mystatus = (PG_Storage_status *) palloc0(sizeof(PG_Storage_status));
		funcctx->user_fctx = (void *) mystatus;

		MemoryContextSwitchTo(oldcontext);

		mystatus->totalIdx = GetGTMStoreTransaction((GTM_StoredTransactionInfo **)&mystatus->data);
		if (mystatus->totalIdx < 0)
		{
			elog(ERROR, "get transaction info from gtm failed");			
		}		
	}

	funcctx  = SRF_PERCALL_SETUP();
	mystatus = (PG_Storage_status*) funcctx->user_fctx;	
	txns     = (GTM_StoredTransactionInfo*)mystatus->data;
	while (mystatus->currIdx < mystatus->totalIdx)
	{
		Datum		values[NUM_TXN_STATUS_COLUMNS];
		bool		nulls[NUM_TXN_STATUS_COLUMNS];
		
		HeapTuple	tuple;
		Datum		result;
		GTM_StoredTransactionInfo *instance;

		instance = &(txns[mystatus->currIdx]);		

		/*
		 * Form tuple with appropriate data.
		 */
		MemSet(values, 0, sizeof(values));
		MemSet(nulls, false, sizeof(nulls));

		values[0] = CStringGetTextDatum(instance->gti_gid);
		values[1] = CStringGetTextDatum(instance->nodestring);
		values[2] = Int32GetDatum(instance->gti_state);
		values[3] = Int32GetDatum(instance->gti_store_handle);
		values[4] = TimestampTzGetDatum(instance->m_last_update_time);
		values[5] = Int32GetDatum(instance->gs_next);
		values[6] = Int32GetDatum(instance->gti_crc);

		tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);
		result = HeapTupleGetDatum(tuple);
		
		mystatus->currIdx++;
		SRF_RETURN_NEXT(funcctx, result);
	}	

	SRF_RETURN_DONE(funcctx);
}

Datum
pg_check_storage_sequence(PG_FUNCTION_ARGS)
{
#define NUM_CHECK_SEQ_STATUS_COLUMNS	17	
	FuncCallContext   *funcctx;
	PG_Storage_status *mystatus;
	GTMStorageSequneceStatus *seqs;

	NOT_SUPPORTED_IN_CENTRALIZED_MODE;

	if (SRF_IS_FIRSTCALL())
	{
		int32		  need_fix = false;
		TupleDesc	  tupdesc;
		MemoryContext oldcontext;

		/* create a function context for cross-call persistence */
		funcctx = SRF_FIRSTCALL_INIT();

		need_fix = PG_GETARG_BOOL(0);

		/*
		 * switch to memory context appropriate for multiple function calls
		 */
		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);
		
		/* build tupdesc for result tuples */
		/* this had better match function's declaration in pg_proc.h */
		tupdesc = CreateTemplateTupleDesc(NUM_CHECK_SEQ_STATUS_COLUMNS, false);
		TupleDescInitEntry(tupdesc, (AttrNumber) 1, "gsk_key",
						   TEXTOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 2, "gsk_type",
						   INT4OID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 3, "gs_value",
						   INT8OID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 4, "gs_init_value",
						   INT8OID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 5, "gs_increment_by",
						   INT8OID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 6, "gs_min_value",
						   INT8OID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 7, "gs_max_value",
						   INT8OID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 8, "gs_cycle",
						   BOOLOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 9, "gs_called",
						   BOOLOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 10, "gs_reserved",
						   BOOLOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 11, "gs_status",
						   INT4OID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 12, "gti_store_handle",
						   INT4OID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 13, "last_update_time",
						   TIMESTAMPTZOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 14, "gs_next",
						   INT4OID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 15, "gs_crc",
						   INT4OID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 16, "error_msg",
						   INT4OID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 17, "check_status",
						   INT4OID, -1, 0);
		
		funcctx->tuple_desc = BlessTupleDesc(tupdesc);

		mystatus = (PG_Storage_status *) palloc0(sizeof(PG_Storage_status));
		funcctx->user_fctx = (void *) mystatus;

		MemoryContextSwitchTo(oldcontext);

		mystatus->totalIdx = CheckGTMStoreSequence((GTMStorageSequneceStatus **)&mystatus->data, need_fix);
		if (mystatus->totalIdx < 0)
		{
			elog(ERROR, "get sequence info from gtm failed");			
		}		
	}

	funcctx  = SRF_PERCALL_SETUP();
	mystatus = (PG_Storage_status *) funcctx->user_fctx;	
	seqs     = (GTMStorageSequneceStatus*)mystatus->data;
	while (mystatus->currIdx < mystatus->totalIdx)
	{
		Datum		values[NUM_CHECK_SEQ_STATUS_COLUMNS];
		bool		nulls[NUM_CHECK_SEQ_STATUS_COLUMNS];
		
		HeapTuple	tuple;
		Datum		result;
		GTMStorageSequneceStatus *instance;

		instance = &(seqs[mystatus->currIdx]);		

		/*
		 * Form tuple with appropriate data.
		 */
		MemSet(values, 0, sizeof(values));
		MemSet(nulls, false, sizeof(nulls));

		values[0] = CStringGetTextDatum(instance->sequence.gs_key.gsk_key);
		values[1] = Int32GetDatum(instance->sequence.gs_key.gsk_type);
		values[2] = Int64GetDatum(instance->sequence.gs_value);
		values[3] = Int64GetDatum(instance->sequence.gs_init_value);
		values[4] = Int64GetDatum(instance->sequence.gs_increment_by);
		values[5] = Int64GetDatum(instance->sequence.gs_min_value);
		values[6] = Int64GetDatum(instance->sequence.gs_max_value);
		values[7] = BoolGetDatum(instance->sequence.gs_cycle);
		values[8] = BoolGetDatum(instance->sequence.gs_called);
		values[9] = BoolGetDatum(instance->sequence.gs_reserved);
		values[10] = Int32GetDatum(instance->sequence.gs_status);
		values[11] = Int32GetDatum(instance->sequence.gti_store_handle);
		values[12] = TimestampTzGetDatum(instance->sequence.m_last_update_time);
		values[13] = Int32GetDatum(instance->sequence.gs_next);
		values[14] = UInt32GetDatum(instance->sequence.gs_crc);						
		values[15] = Int32GetDatum(instance->error);
		values[16] = Int32GetDatum(instance->status);	

		tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);
		result = HeapTupleGetDatum(tuple);
		
		mystatus->currIdx++;
		SRF_RETURN_NEXT(funcctx, result);
	}	

	SRF_RETURN_DONE(funcctx);
}

Datum
pg_check_storage_transaction(PG_FUNCTION_ARGS)
{
#define NUM_CHECK_TXN_STATUS_COLUMNS	11
	FuncCallContext   *funcctx;
	PG_Storage_status *mystatus;
	GTMStorageTransactionStatus *txns;

	NOT_SUPPORTED_IN_CENTRALIZED_MODE;

	if (SRF_IS_FIRSTCALL())
	{
		TupleDesc	  tupdesc;
		MemoryContext oldcontext;
		int32		  need_fix = false;

		need_fix = PG_GETARG_BOOL(0);
		/* create a function context for cross-call persistence */
		funcctx = SRF_FIRSTCALL_INIT();
		
		/*
		 * switch to memory context appropriate for multiple function calls
		 */
		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);
		
		/* build tupdesc for result tuples */
		/* this had better match function's declaration in pg_proc.h */
		tupdesc = CreateTemplateTupleDesc(NUM_CHECK_TXN_STATUS_COLUMNS, false);
		TupleDescInitEntry(tupdesc, (AttrNumber) 1, "gti_gid",
						   TEXTOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 2, "node_list",
						   TEXTOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 3, "gti_state",
						   INT4OID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 4, "gti_store_handle",
						   INT4OID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 5, "last_update_time",
						   TIMESTAMPTZOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 6, "gs_next",
						   INT4OID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 7, "gs_crc",
						   INT4OID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 8, "error_msg",
						   INT4OID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 9, "check_status",
						   INT4OID, -1, 0);
		
		funcctx->tuple_desc = BlessTupleDesc(tupdesc);

		mystatus = (PG_Storage_status *) palloc0(sizeof(PG_Storage_status));
		funcctx->user_fctx = (void *) mystatus;

		MemoryContextSwitchTo(oldcontext);

		mystatus->totalIdx = CheckGTMStoreTransaction((GTMStorageTransactionStatus **)&mystatus->data, need_fix);
		if (mystatus->totalIdx < 0)
		{
			elog(ERROR, "get transaction info from gtm failed");			
		}		
	}

	funcctx  = SRF_PERCALL_SETUP();
	mystatus = (PG_Storage_status*) funcctx->user_fctx;	
	txns     = (GTMStorageTransactionStatus*)mystatus->data;
	while (mystatus->currIdx < mystatus->totalIdx)
	{
		Datum		values[NUM_CHECK_TXN_STATUS_COLUMNS];
		bool		nulls[NUM_CHECK_TXN_STATUS_COLUMNS];
		
		HeapTuple	tuple;
		Datum		result;
		GTMStorageTransactionStatus *instance;

		instance = &(txns[mystatus->currIdx]);		

		/*
		 * Form tuple with appropriate data.
		 */
		MemSet(values, 0, sizeof(values));
		MemSet(nulls, false, sizeof(nulls));

		values[0] = CStringGetTextDatum(instance->txn.gti_gid);
		values[1] = CStringGetTextDatum(instance->txn.nodestring);
		values[2] = Int32GetDatum(instance->txn.gti_state);
		values[3] = Int32GetDatum(instance->txn.gti_store_handle);
		values[4] = TimestampTzGetDatum(instance->txn.m_last_update_time);
		values[5] = Int32GetDatum(instance->txn.gs_next);
		values[6] = Int32GetDatum(instance->txn.gti_crc);
		values[7] = Int32GetDatum(instance->error);
		values[8] = Int32GetDatum(instance->status);

		tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);
		result = HeapTupleGetDatum(tuple);
		
		mystatus->currIdx++;
		SRF_RETURN_NEXT(funcctx, result);
	}	

	SRF_RETURN_DONE(funcctx);
}

#endif

bool
IsGTMConnected()
{
	return conn != NULL;
}

/*
 * get gtm info from pgxc_node wrap transaction
 */
static void
GetGtmInfoFromPgxcNode(bool *found)
{
	Relation	rel;
	HeapScanDesc scan;
	HeapTuple	gtmtup;
	Form_pgxc_node	nodeForm;
	bool need_abort = false;

	*found = false;

	if (IsTransactionIdle())
	{
		StartTransactionCommand();
		need_abort = true;
	}
	else if (!IsTransactionState() && !IsSubTransaction())
	{
		elog(WARNING, "unexpected transaction stat %s blockState %s",
			 CurrentTransactionTransStateAsString(),
			 CurrentTransactionBlockStateAsString());
	}
	
	rel = relation_open(PgxcNodeRelationId, AccessShareLock);
	scan = heap_beginscan_catalog(rel, 0, NULL);
	/* Only one record will match */
	while (HeapTupleIsValid(gtmtup = heap_getnext(scan, ForwardScanDirection)))
	{
		nodeForm = (Form_pgxc_node) GETSTRUCT(gtmtup);
		if (PGXC_NODE_GTM == nodeForm->node_type && nodeForm->nodeis_primary)
		{
			GtmHost = strdup(NameStr(nodeForm->node_host));
			GtmPort = nodeForm->node_port;
			*found = true;
			break;
		}
	}
	heap_endscan(scan);
	relation_close(rel, AccessShareLock);

	if (need_abort)
	{
		AbortCurrentTransaction();
	}

	return;
}

#ifdef __OPENTENBASE__
/*
 * Set gtm info with GtmHost and GtmPort.
 *
 * There are three cases:
 * 1.New gtm info from create/alter gtm node command
 * 2.Gtm info from pgxc_node
 * 3.Gtm info from recovery gtm host
 */
void
GetMasterGtmInfo(void)
{
	/* Check gtm host and port info */
	bool		found = false;

	/* reset gtm info */
	ResetGtmInfo();

	/* If NewGtmHost and NewGtmPort, just use it. */
	if (NewGtmHost && NewGtmPort != 0)
	{
		elog(LOG,
			"GetMasterGtmInfo: set master gtm info with NewGtmHost:%s NewGtmPort:%d",
			NewGtmHost, NewGtmPort);

		GtmHost = strdup(NewGtmHost);
		GtmPort = NewGtmPort;

		free(NewGtmHost);
		NewGtmHost = NULL;
		NewGtmPort = 0;

		return;
	}

	/* we have no recovery gtm host info, just read from heap. */
	if (!g_recovery_gtm_host->need_read)
	{
		GetGtmInfoFromPgxcNode(&found);
	}
	else
	{
		/* get the gtm host info  */
		GtmHost = strdup(NameStr(g_recovery_gtm_host->hostdata));
		GtmPort = g_recovery_gtm_host->port;	
		found = true;
	}	

	if (!found)
	{
		elog(LOG,
			"GetMasterGtmInfo: can not get master gtm info from pgxc_node");
	}
}
#endif

static void
CheckConnection(void)
{
	if (IS_CENTRALIZED_MODE)
		elog(PANIC, "Checking GTM connection in centralized mode is unexpected");

	/* Be sure that a backend does not use a postmaster connection */
	if (IsUnderPostmaster && GTMPQispostmaster(conn) == 1)
	{
		CloseGTM();
		InitGTM();
		return;
	}

	if (GTMPQstatus(conn) != CONNECTION_OK)
	{
		CloseGTM();
		InitGTM();
	}
}

static void
ResetGTMConnection(void)
{
	CloseGTM();

	if (!g_recovery_gtm_host->need_read)
	{
		char *tmp_host = NULL;
		int	  tmp_port = 0;
		bool  valid;
		
		SpinLockAcquire(&shm_gtm_primary->mutex);
		valid = shm_gtm_primary->valid;
		if (valid)
		{
			tmp_host = strdup(NameStr(shm_gtm_primary->host));
			tmp_port = shm_gtm_primary->port;
		}
		SpinLockRelease(&shm_gtm_primary->mutex);

		if (valid)
		{
			/* Only reset gtm info when shm_gtm_primary valid, which means 
			 * that gtm node info has been changed by create/alter node.
			 */
			ResetGtmInfo();
			GtmHost = tmp_host;
			GtmPort = tmp_port;
		}
		
		elog(LOG, "reset gtm conn use %s:%d shm info valid %d", GtmHost, GtmPort, valid);
	}

	InitGTM();
}

#ifdef HAVE_UNIX_SOCKETS
/*
 * gtm_unix_socket_file_exists()
 *
 * Checks whether the gtm unix domain socket file exists.
 */
static bool
gtm_unix_socket_file_exists(void)
{
    char		path[MAXGTMPATH];
    char		lockfile[MAXPGPATH];
    int			fd;

    UNIXSOCK_PATH(path, GtmPort, gtm_unix_socket_directory);
    snprintf(lockfile, sizeof(lockfile), "%s.lock", path);

    if ((fd = open(lockfile, O_RDONLY, 0)) < 0)
    {
        /* ENOTDIR means we will throw a more useful error later */
        if (errno != ENOENT && errno != ENOTDIR)
            elog(LOG, "could not open file \"%s\" for reading: %s\n",
                     lockfile, strerror(errno));

        return false;
    }

    close(fd);
    return true;
}
#endif

void
InitGTM(void)
{
#define  CONNECT_STR_LEN   256 /* 256 bytes should be enough */
	char conn_str[CONNECT_STR_LEN];
#ifdef __OPENTENBASE__
	int  try_cnt = 0;
	const int max_try_cnt = 1;
    bool  same_host = false;

	/*
	 * Only re-set gtm info in two cases:
	 * 1.No gtm info
	 * 2.New gtm info by create/alter gtm node command
	 */
	if ((GtmHost == NULL && GtmPort == 0) ||
		(NewGtmHost != NULL && NewGtmPort != 0))
	{
		GetMasterGtmInfo();
	}
	if (GtmHost == NULL && GtmPort == 0)
	{
		ereport(LOG,
                (errcode(ERRCODE_SYSTEM_ERROR),
                 errmsg("GtmHost and GtmPort are not set")));
		return;
	}

#ifdef HAVE_UNIX_SOCKETS
    if (GtmHost && (strcmp(PGXCNodeHost, GtmHost) == 0) && gtm_unix_socket_file_exists())
    {
        same_host = true;
    }
#endif
#endif

try_connect_gtm:
	/* If this thread is postmaster itself, it contacts gtm identifying itself */
	if (!IsUnderPostmaster)
	{
		GTM_PGXCNodeType remote_type = GTM_NODE_DEFAULT;

		if (IS_PGXC_COORDINATOR)
			remote_type = GTM_NODE_COORDINATOR;
		else if (IS_PGXC_DATANODE)
			remote_type = GTM_NODE_DATANODE;
#ifdef __OPENTENBASE__
        if (same_host)
        {
            /* Use 60s as connection timeout */
            snprintf(conn_str, CONNECT_STR_LEN, "host=%s port=%d node_name=%s remote_type=%d postmaster=1 connect_timeout=%d",
                     gtm_unix_socket_directory, GtmPort, PGXCNodeName, remote_type,
                     tcp_keepalives_idle > 0 ?
                     tcp_keepalives_idle : GtmConnectTimeout);
        }
        else
#endif
        {
            /* Use 60s as connection timeout */
            snprintf(conn_str, CONNECT_STR_LEN, "host=%s port=%d node_name=%s remote_type=%d postmaster=1 connect_timeout=%d",
                     GtmHost, GtmPort, PGXCNodeName, remote_type,
                     tcp_keepalives_idle > 0 ?
                     tcp_keepalives_idle : GtmConnectTimeout);
        }

		/* Log activity of GTM connections */
		if(GTMDebugPrint)
			elog(LOG, "Postmaster: connection established to GTM with string %s", conn_str);
	}
	else
	{
#ifdef __OPENTENBASE__
        if (same_host)
        {
            /* Use 60s as connection timeout */
            snprintf(conn_str, CONNECT_STR_LEN, "host=%s port=%d node_name=%s connect_timeout=%d",
                     gtm_unix_socket_directory, GtmPort, PGXCNodeName,
                     tcp_keepalives_idle > 0 ?
                     tcp_keepalives_idle : GtmConnectTimeout);
        }
        else
#endif
        {
            /* Use 60s as connection timeout */
            snprintf(conn_str, CONNECT_STR_LEN, "host=%s port=%d node_name=%s connect_timeout=%d",
                     GtmHost, GtmPort, PGXCNodeName,
                     tcp_keepalives_idle > 0 ?
                     tcp_keepalives_idle : GtmConnectTimeout);
        }

		/* Log activity of GTM connections */
		if (IsAutoVacuumWorkerProcess() && GTMDebugPrint)
			elog(LOG, "Autovacuum worker: connection established to GTM with string %s", conn_str);
		else if (IsAutoVacuumLauncherProcess() && GTMDebugPrint)
			elog(LOG, "Autovacuum launcher: connection established to GTM with string %s", conn_str);
		else if (IsClusterMonitorProcess() && GTMDebugPrint)
			elog(LOG, "Cluster monitor: connection established to GTM with string %s", conn_str);
		else if (IsClean2pcWorker() && GTMDebugPrint)
			elog(LOG, "Clean 2pc worker: connection established to GTM with string %s", conn_str);
		else if (IsClean2pcLauncher() && GTMDebugPrint)
			elog(LOG, "Clean 2pc launcher: connection established to GTM with string %s", conn_str);
		else if(GTMDebugPrint)
			elog(LOG, "Postmaster child: connection established to GTM with string %s", conn_str);
	}

	conn = PQconnectGTM(conn_str);
	if (GTMPQstatus(conn) != CONNECTION_OK)
	{
		int save_errno = errno;
		
#ifdef __OPENTENBASE__	
		if (try_cnt < max_try_cnt)
		{
			/* If connect gtm failed, get gtm info from syscache, and try again */
			GetMasterGtmInfo();
			if (GtmHost != NULL && GtmPort)
			{
				elog(DEBUG1, "[InitGTM] Get GtmHost:%s  GtmPort:%d try_cnt:%d max_try_cnt:%d", 
							 GtmHost, GtmPort, try_cnt, max_try_cnt);
			}
			CloseGTM();
			try_cnt++;

			/* if connect with unix domain socket failed */
			if (same_host)
            {
                same_host = false;
            }
			goto try_connect_gtm;
		}
		else
#endif		
		{
			ResetGtmInfo();

			/* Use LOG instead of ERROR to avoid error stack overflow. */
			if(conn)
			{
				ereport(LOG,
                        (errcode(ERRCODE_CONNECTION_FAILURE),
                         errmsg("can not connect to GTM: %s %m",
                                GTMPQerrorMessage(conn))));
			}
			else
			{
				ereport(LOG,
                        (errcode(ERRCODE_CONNECTION_DOES_NOT_EXIST),
                         errmsg("connection is null: %m")));
			}

			errno = save_errno;

			CloseGTM();
		}
		
	}
	else
	{
		if (!GTMSetSockKeepAlive(conn, tcp_keepalives_idle,
							tcp_keepalives_interval, tcp_keepalives_count))
		{
			ereport(LOG,
					(errcode(ERRCODE_CONNECTION_EXCEPTION),
					 errmsg("GTMSetSockKeepAlive failed: %m")));
		}

		/*
		 * Each process on CN or DN creates a separate socket to connect to GTM. Closing the socket
		 * upon process exit can easily result in a large number of TIME_WAIT state connections. Once
		 * the count exceeds 65535, connection refusal is likely. Here, we set ReuseAddr to quickly
		 * reuse TIME_WAIT state connections.
		 */
		if (!GTMSetSockReuseAddr(conn))
		{
			ereport(LOG,
			        (errcode(ERRCODE_CONNECTION_EXCEPTION),
			         errmsg("GTMSetSockReuseAddr failed: %m")));
		}

		if (IS_PGXC_COORDINATOR)
		{
			if(register_session(conn, PGXCNodeName, MyProcPid, MyBackendId))
			{
				ereport(LOG,
					(errcode(ERRCODE_CONNECTION_EXCEPTION),
					 errmsg("register_session failed: %m")));
				ResetGtmInfo();
				CloseGTM();
			}
		}
	}
}

void
CloseGTM(void)
{
	if (conn)
	{
		GTMPQfinish(conn);
		conn = NULL;
	}

	/* Log activity of GTM connections */
	if (!IsUnderPostmaster)
		elog(DEBUG1, "Postmaster: connection to GTM closed");
	else if (IsAutoVacuumWorkerProcess())
		elog(DEBUG1, "Autovacuum worker: connection to GTM closed");
	else if (IsAutoVacuumLauncherProcess())
		elog(DEBUG1, "Autovacuum launcher: connection to GTM closed");
	else if (IsClusterMonitorProcess())
		elog(DEBUG1, "Cluster monitor: connection to GTM closed");
	else if (IsClean2pcWorker())
		elog(DEBUG1, "Clean 2pc worker: connection to GTM closed");
	else if (IsClean2pcLauncher())
		elog(DEBUG1, "Clean 2pc launcher: connection to GTM closed");
	else
		elog(DEBUG1, "Postmaster child: connection to GTM closed");
}

#ifdef __SUPPORT_DISTRIBUTED_TRANSACTION__
GTM_Timestamp 
GetGlobalTimestampGTMDirectly(void)
{
	Get_GTS_Result gts_result = {InvalidGlobalTimestamp,false};
	GTM_Timestamp  latest_gts = InvalidGlobalTimestamp;
	struct rusage start_r;
	struct timeval start_t;
	int retry_cnt = 0;

	if (IS_CENTRALIZED_MODE)
	{
		return AdvanceSharedMaxCommitTs();
	}

	if (log_gtm_stats)
		ResetUsageCommon(&start_r, &start_t);

	CheckConnection();
	// TODO Isolation level
	if (conn)
	{
		gts_result =  get_global_timestamp(conn);
	}
	else if(GTMDebugPrint)
	{
		elog(LOG, "get global timestamp conn is null");
	}

	latest_gts = GetSharedLatestCommitTS();

	/* If something went wrong (timeout), try and reset GTM connection
	 * and retry. This is safe at the beginning of a transaction.
	 */
	while ((!GlobalTimestampIsValid(gts_result.gts) ||
			(latest_gts > (gts_result.gts + GTM_CHECK_DELTA))) &&
		retry_cnt < reconnect_gtm_retry_times)
	{
		if(GTMDebugPrint)
		{
			elog(LOG, "get global timestamp reconnect");
		}

		ResetGTMConnection();
		retry_cnt++;

		elog(DEBUG5, "reset gtm connection %d times", retry_cnt);

		if (conn)
		{
			gts_result = get_global_timestamp(conn);
			if (GlobalTimestampIsValid(gts_result.gts))
			{
				elog(DEBUG5, "retry get global timestamp gts " INT64_FORMAT,
					gts_result.gts);

				latest_gts = GetSharedLatestCommitTS();
				
				if (latest_gts <= (gts_result.gts + GTM_CHECK_DELTA))
					break;
			}
		}
		else if(GTMDebugPrint)
		{
			elog(LOG, "get global timestamp conn is null after retry %d times",
				retry_cnt);
		}

		if (retry_cnt < reconnect_gtm_retry_times)
		{
			pg_usleep(reconnect_gtm_retry_interval * 1000);
		}
	}
	elog(DEBUG7, "get global timestamp gts " INT64_FORMAT, gts_result.gts);

	if (!GlobalTimestampIsValid(gts_result.gts))
	{
		elog(WARNING, "retry %d times, get a invalid global timestamp, "
			"ResetGTMConnection", retry_cnt);
		ResetGTMConnection();
	}

#ifdef USE_WHITEBOX_INJECTION
		(void)whitebox_trigger_generic(GET_GTS_FAILED, WHITEBOX_TRIGGER_DEFAULT,
			INJECTION_LOCATION, g_twophase_state.gid, NULL);
#endif

	if (log_gtm_stats)
		ShowUsageCommon("BeginTranGTM", &start_r, &start_t);

	if (gts_result.gts != InvalidGlobalTimestamp && latest_gts > (gts_result.gts + GTM_CHECK_DELTA))
	{	
		elog(ERROR, "global gts:%lu is earlier than local gts:%lu, please check GTM status!", gts_result.gts + GTM_CHECK_DELTA, latest_gts);
	}

    SetSharedLatestCommitTS(gts_result.gts);
	/* if we are standby, use timestamp subtracting given interval */
	if (IsStandbyPostgres() && query_delay)
	{
		GTM_Timestamp  interval = query_delay * USECS_PER_SEC;

		gts_result.gts = gts_result.gts - interval;

		if (gts_result.gts < FirstGlobalTimestamp)
		{
			gts_result.gts = FirstGlobalTimestamp;
		}
	}

	GTM_ReadOnly = gts_result.gtm_readonly;
	
	return gts_result.gts;
}
#endif

static inline GTM_Timestamp
GetGlobalTimestampGTMInternal(bool isCommit, bool useLocalLatest)
{
	uint128_u compare;
	uint128_u exchange;
	uint128_u current;
	GTM_Timestamp gts = InvalidGTS;
	if (useLocalLatest)
	{
		if (isCommit)
			elog(ERROR, "can not use local latest gts for commit");
		else 
		{
			gts = (GTM_Timestamp) GetSharedLatestCommitTS() + 1;
			elog(DEBUG1, "use local latest gts: " INT64_FORMAT, gts);
			return gts;
		}
	}

	/* for not useLocalLatest */
	if (gts_cache_timeout <= 0)
	{
		gts = GetGlobalTimestampGTMDirectly();
		elog(DEBUG1, "use remote gts from gtm: " INT64_FORMAT, gts);
		return gts;
	}

	/* for gts_cache_optimize */
	/* localCacheGts.u64[0] is gts and localCacheGts.u64[1] is the time it was obtained */
	compare = pg_atomic_read_u128(&ShmemVariableCache->localCacheGts);
	if (!isCommit && GlobalTimestampIsValid(compare.u64[0]) &&
		!TimestampDifferenceExceeds(compare.u64[1], GetCurrentTimestamp(), gts_cache_timeout))
		return compare.u64[0];

	exchange.u64[0] = GetGlobalTimestampGTMDirectly();
	if (GlobalTimestampIsValid(exchange.u64[0]))
	{
		exchange.u64[1] = GetCurrentTimestamp();
loop:
		current = pg_atomic_compare_and_swap_u128(&ShmemVariableCache->localCacheGts,
				                                   compare, exchange);
		if (!UINT128_IS_EQUAL(compare, current))
		{
			if (!isCommit)
				return current.u64[0];
			else if (isCommit && (current.u64[0] < exchange.u64[0]))
			{
				UINT128_COPY(compare, current);
				goto loop;
			}
		}
		else if (isPGXCDataNode && isCommit)
			feedbackGts = true;
	}

	return exchange.u64[0];
}

void
UpdatelocalCacheGtsUseFeedbackGts(uint64 feedbackGts)
{
	uint128_u compare;
	uint128_u exchange;
	uint128_u current;

	if (gts_cache_timeout <= 0)
		return;

	/* localCacheGts.u64[0] is gts and localCacheGts.u64[1] is the time it was obtained */
	compare = pg_atomic_read_u128(&ShmemVariableCache->localCacheGts);
	if (compare.u64[0] < feedbackGts)
	{
		exchange.u64[0] = feedbackGts;
loop:
		exchange.u64[1] = compare.u64[1];
		current = pg_atomic_compare_and_swap_u128(&ShmemVariableCache->localCacheGts,
												  compare, exchange);
		if (!UINT128_IS_EQUAL(compare, current) && (current.u64[0] < exchange.u64[0]))
		{
			UINT128_COPY(compare, current);
			goto loop;
		}
	}

	return;
}

GTM_Timestamp
GetGlobalTimestampLocal(void)
{
	return GetGlobalTimestampGTMInternal(false, true);
}

GTM_Timestamp
GetGlobalTimestampGTM(void)
{
	return GetGlobalTimestampGTMInternal(false, false);
}

GTM_Timestamp
GetGlobalTimestampGTMForCommit(void)
{
	return GetGlobalTimestampGTMInternal(true, false);
}

GlobalTransactionId
BeginTranGTM(GTM_Timestamp *timestamp, const char *globalSession)
{
	GlobalTransactionId  xid = InvalidGlobalTransactionId;
	struct rusage start_r;
	struct timeval start_t;

	if (log_gtm_stats)
		ResetUsageCommon(&start_r, &start_t);

	CheckConnection();
	// TODO Isolation level
	if (conn)
		xid =  begin_transaction(conn, GTM_ISOLATION_RC, globalSession, timestamp);

	/* If something went wrong (timeout), try and reset GTM connection
	 * and retry. This is safe at the beginning of a transaction.
	 */
	if (!TransactionIdIsValid(xid))
	{
		CloseGTM();
		InitGTM();
		if (conn)
			xid = begin_transaction(conn, GTM_ISOLATION_RC, globalSession, timestamp);
	}
	if (xid)
		IsXidFromGTM = true;
	currentGxid = xid;

	elog(DEBUG2, "BeginTranGTM - session:%s, xid: %d", globalSession, xid);

	if (log_gtm_stats)
		ShowUsageCommon("BeginTranGTM", &start_r, &start_t);
	return xid;
}

GlobalTransactionId
BeginTranAutovacuumGTM(void)
{
	GlobalTransactionId  xid = InvalidGlobalTransactionId;

	CheckConnection();
	// TODO Isolation level
	if (conn)
		xid =  begin_transaction_autovacuum(conn, GTM_ISOLATION_RC);

	/*
	 * If something went wrong (timeout), try and reset GTM connection and retry.
	 * This is safe at the beginning of a transaction.
	 */
	if (!TransactionIdIsValid(xid))
	{
		CloseGTM();
		InitGTM();
		if (conn)
			xid =  begin_transaction_autovacuum(conn, GTM_ISOLATION_RC);
	}
	currentGxid = xid;

	elog(DEBUG3, "BeginTranGTM - %d", xid);
	return xid;
}

int
CommitTranGTM(GlobalTransactionId gxid, int waited_xid_count,
		GlobalTransactionId *waited_xids)
{
	int ret;
	struct rusage start_r;
	struct timeval start_t;

	if (!GlobalTransactionIdIsValid(gxid))
		return 0;

	if (log_gtm_stats)
		ResetUsageCommon(&start_r, &start_t);

	elog(DEBUG3, "CommitTranGTM: %d", gxid);

	CheckConnection();
	ret = -1;
	if (conn)
	ret = commit_transaction(conn, gxid, waited_xid_count, waited_xids);

	/*
	 * If something went wrong (timeout), try and reset GTM connection.
	 * We will close the transaction locally anyway, and closing GTM will force
	 * it to be closed on GTM.
	 */
	if (ret < 0)
	{
		CloseGTM();
		InitGTM();
		if (conn)
			ret = commit_transaction(conn, gxid, waited_xid_count, waited_xids);
	}

	/* Close connection in case commit is done by autovacuum worker or launcher */
	if (IsAutoVacuumWorkerProcess() || IsAutoVacuumLauncherProcess())
		CloseGTM();

	currentGxid = InvalidGlobalTransactionId;

	if (log_gtm_stats)
		ShowUsageCommon("CommitTranGTM", &start_r, &start_t);
	return ret;
}

/*
 * For a prepared transaction, commit the gxid used for PREPARE TRANSACTION
 * and for COMMIT PREPARED.
 */
int
CommitPreparedTranGTM(GlobalTransactionId gxid,
		GlobalTransactionId prepared_gxid, int waited_xid_count,
		GlobalTransactionId *waited_xids)
{
	int ret = 0;
	struct rusage start_r;
	struct timeval start_t;

	if (!GlobalTransactionIdIsValid(gxid) || !GlobalTransactionIdIsValid(prepared_gxid))
		return ret;

	if (log_gtm_stats)
		ResetUsageCommon(&start_r, &start_t);

	elog(DEBUG3, "CommitPreparedTranGTM: %d:%d", gxid, prepared_gxid);

	CheckConnection();
	ret = -1;
	if (conn)
		ret = commit_prepared_transaction(conn, gxid, prepared_gxid,
			waited_xid_count, waited_xids);

	/*
	 * If something went wrong (timeout), try and reset GTM connection.
	 * We will close the transaction locally anyway, and closing GTM will force
	 * it to be closed on GTM.
	 */

	if (ret < 0)
	{
		CloseGTM();
		InitGTM();
		if (conn)
			ret = commit_prepared_transaction(conn, gxid, prepared_gxid,
					waited_xid_count, waited_xids);
	}
	currentGxid = InvalidGlobalTransactionId;

	if (log_gtm_stats)
		ShowUsageCommon("CommitPreparedTranGTM", &start_r, &start_t);

	return ret;
}

int
RollbackTranGTM(GlobalTransactionId gxid)
{
	int ret = -1;

	if (!GlobalTransactionIdIsValid(gxid))
		return 0;
	CheckConnection();

	if (conn)
		ret = abort_transaction(conn, gxid);

	/*
	 * If something went wrong (timeout), try and reset GTM connection.
	 * We will abort the transaction locally anyway, and closing GTM will force
	 * it to end on GTM.
	 */
	if (ret < 0)
	{
		CloseGTM();
		InitGTM();
		if (conn)
			ret = abort_transaction(conn, gxid);
	}

	currentGxid = InvalidGlobalTransactionId;
	return ret;
}



/*
 * For CN, these timestamps are global ones issued by GTM
 * and are local timestamps on DNs.
 */
int
LogCommitTranGTM(GlobalTransactionId gxid,
					 const char *gid,
					 const char *nodestring,
					 int node_count,
					 bool isGlobal,
					 bool isCommit,
					 GlobalTimestamp prepare_timestamp,
					 GlobalTimestamp commit_timestamp) 
{
	int ret = 0;

	if (!GlobalTransactionIdIsValid(gxid))
		return 0;
	CheckConnection();

	ret = -1;
	if (conn)
		ret = log_commit_transaction(conn,
									 gxid,
									 gid,
									 nodestring,
									 node_count,
									 isGlobal,
									 isCommit,
									 prepare_timestamp,
									 commit_timestamp);

	/*
	 * If something went wrong (timeout), try and reset GTM connection.
	 * We will abort the transaction locally anyway, and closing GTM will force
	 * it to end on GTM.
	 */
	if (ret < 0)
	{
		CloseGTM();
		InitGTM();
		if (conn)
			ret = log_commit_transaction(conn,
										 gxid,
										 gid,
										 nodestring,
										 node_count,
										 isGlobal,
										 isCommit,
										 prepare_timestamp,
										 commit_timestamp);
	}

	
	return ret;
}

int
LogScanGTM( GlobalTransactionId gxid, 
 							 const char *node_string, 
 							 GlobalTimestamp	 start_ts,
 							 GlobalTimestamp	 local_start_ts,
 							 GlobalTimestamp	 local_complete_ts,
 							 int	scan_type,
 							 const char *rel_name,
							 int64  scan_number) 
{
	int ret = 0;

	if (!GlobalTransactionIdIsValid(gxid))
		return 0;
	CheckConnection();

	ret = -1;
	if (conn)
		ret = log_scan_transaction(conn,
								   gxid,
								   node_string,
								   start_ts,
								   local_start_ts,
								   local_complete_ts,
								   scan_type,
								   rel_name,
								   scan_number);

	/*
	 * If something went wrong (timeout), try and reset GTM connection.
	 * We will abort the transaction locally anyway, and closing GTM will force
	 * it to end on GTM.
	 */
	if (ret < 0)
	{
		CloseGTM();
		InitGTM();
		if (conn)
			ret = log_scan_transaction(conn,
									   gxid,
									   node_string,
									   start_ts,
									   local_start_ts,
									   local_complete_ts,
									   scan_type,
									   rel_name,
									   scan_number);
	}

	
	return ret;
}

int
StartPreparedTranGTM(GlobalTransactionId gxid,
					 char *gid,
					 char *nodestring)
{
	int ret = 0;

	if (!GlobalTransactionIdIsValid(gxid))
	{
		return 0;
	}
	CheckConnection();

	ret = -1;
	if (conn)
	{
		ret = start_prepared_transaction(conn, gxid, gid, nodestring);
	}
	/* Here, we should not reconnect, for sometime gtm raise an error, reconnect may skip the error. */
#ifndef __OPENTENBASE__
	
	/*
	 * If something went wrong (timeout), try and reset GTM connection.
	 * We will abort the transaction locally anyway, and closing GTM will force
	 * it to end on GTM.
	 */
	if (ret < 0)
	{
		CloseGTM();
		InitGTM();
		if (conn)
			ret = start_prepared_transaction(conn, gxid, gid, nodestring);
	}
#endif
	return ret;
}

int
PrepareTranGTM(GlobalTransactionId gxid)
{
	int ret;
	struct rusage start_r;
	struct timeval start_t;

	if (!GlobalTransactionIdIsValid(gxid))
		return 0;
	CheckConnection();

	if (log_gtm_stats)
		ResetUsageCommon(&start_r, &start_t);

	ret = -1;
	if (conn)
		ret = prepare_transaction(conn, gxid);

	/*
	 * If something went wrong (timeout), try and reset GTM connection.
	 * We will close the transaction locally anyway, and closing GTM will force
	 * it to be closed on GTM.
	 */
	if (ret < 0)
	{
		CloseGTM();
		InitGTM();
		if (conn)
			ret = prepare_transaction(conn, gxid);
	}
	currentGxid = InvalidGlobalTransactionId;

	if (log_gtm_stats)
		ShowUsageCommon("PrepareTranGTM", &start_r, &start_t);

	return ret;
}


int
GetGIDDataGTM(char *gid,
			  GlobalTransactionId *gxid,
			  GlobalTransactionId *prepared_gxid,
			  char **nodestring)
{
	int ret = 0;

	CheckConnection();
	ret = -1;
	if (conn)
		ret = get_gid_data(conn, GTM_ISOLATION_RC, gid, gxid,
					   prepared_gxid, nodestring);

	/*
	 * If something went wrong (timeout), try and reset GTM connection.
	 * We will abort the transaction locally anyway, and closing GTM will force
	 * it to end on GTM.
	 */
	if (ret < 0)
	{
		CloseGTM();
		InitGTM();
		if (conn)
			ret = get_gid_data(conn, GTM_ISOLATION_RC, gid, gxid,
							   prepared_gxid, nodestring);
	}

	return ret;
}

GTM_Snapshot
GetSnapshotGTM(GlobalTransactionId gxid, bool canbe_grouped)
{
	GTM_Snapshot ret_snapshot = NULL;
	struct rusage start_r;
	struct timeval start_t;

	CheckConnection();

	if (log_gtm_stats)
		ResetUsageCommon(&start_r, &start_t);

	if (conn)
		ret_snapshot = get_snapshot(conn, gxid, canbe_grouped);
	if (ret_snapshot == NULL)
	{
		CloseGTM();
		InitGTM();
		if (conn)
			ret_snapshot = get_snapshot(conn, gxid, canbe_grouped);
	}

	if (log_gtm_stats)
		ShowUsageCommon("GetSnapshotGTM", &start_r, &start_t);

	return ret_snapshot;
}


/*
 * Create a sequence on the GTM.
 */
int
CreateSequenceGTM(char *seqname, GTM_Sequence increment, GTM_Sequence minval,
				  GTM_Sequence maxval, GTM_Sequence startval, bool cycle, bool nocache, bool is_order)
{
	GTM_SequenceKeyData seqkey;
	CheckConnection();
	seqkey.gsk_keylen = strlen(seqname) + 1;
	seqkey.gsk_key = seqname;
	seqkey.gsk_type   = GTM_SEQ_FULL_NAME;

	return conn ? open_sequence(conn, &seqkey, increment, minval, maxval,
			startval, cycle, GetTopTransactionId(), nocache, is_order) : 0;
}

/*
 * Alter a sequence on the GTM
 */
int
AlterSequenceGTM(char *seqname, GTM_Sequence increment, GTM_Sequence minval,
				 GTM_Sequence maxval, GTM_Sequence startval, GTM_Sequence lastval, bool cycle, bool is_restart,
				 bool nocache, bool is_order)
{
	GTM_SequenceKeyData seqkey;
	CheckConnection();
	seqkey.gsk_keylen = strlen(seqname) + 1;
	seqkey.gsk_key = seqname;

	return conn ? alter_sequence(conn, &seqkey, increment, minval, maxval,
			startval, lastval, cycle, is_restart, nocache, is_order) : 0;
}

/*
 * get the current sequence value
 */

GTM_Sequence
GetCurrentValGTM(char *seqname)
{
	GTM_Sequence ret = -1;
	GTM_SequenceKeyData seqkey;
	char   *coordName = IS_PGXC_COORDINATOR ? PGXCNodeName : GetMyCoordName;
	int		coordPid = IS_PGXC_COORDINATOR ? MyProcPid : MyCoordPid;
	int		status;

	CheckConnection();
	seqkey.gsk_keylen = strlen(seqname) + 1;
	seqkey.gsk_key = seqname;

	if (conn)
		status = get_current(conn, &seqkey, coordName, coordPid, &ret);
	else
		status = GTM_RESULT_COMM_ERROR;

	/* retry once */
	if (status == GTM_RESULT_COMM_ERROR)
	{
		CloseGTM();
		InitGTM();
		if (conn)
			status = get_current(conn, &seqkey, coordName, coordPid, &ret);
	}
	if (status != GTM_RESULT_OK)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_TRANSACTION_STATE),
				 errmsg("%s", GTMPQerrorMessage(conn))));
	return ret;
}

/*
 * Get the next sequence value
 */
GTM_Sequence
GetNextValGTM(char *seqname, GTM_Sequence range, GTM_Sequence *rangemax)
{
	GTM_Sequence ret = -1;
	GTM_SequenceKeyData seqkey;
	char   *coordName = IS_PGXC_COORDINATOR ? PGXCNodeName : GetMyCoordName;
	int		coordPid = IS_PGXC_COORDINATOR ? MyProcPid : MyCoordPid;
	int		status;

	CheckConnection();
	seqkey.gsk_keylen = strlen(seqname) + 1;
	seqkey.gsk_key = seqname;

	if (conn)
		status = get_next(conn, &seqkey, coordName,
						  coordPid, range, &ret, rangemax);
	else
		status = GTM_RESULT_COMM_ERROR;

	/* retry once */
	if (status == GTM_RESULT_COMM_ERROR)
	{
		CloseGTM();
		InitGTM();
		if (conn)
			status = get_next(conn, &seqkey, coordName, coordPid,
							  range, &ret, rangemax);
	}
	if (status != GTM_RESULT_OK)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_TRANSACTION_STATE),
				 errmsg("%s", GTMPQerrorMessage(conn))));
	return ret;
}

/*
 * Set values for sequence
 */
int
SetValGTM(char *seqname, GTM_Sequence nextval, bool iscalled)
{
	GTM_SequenceKeyData seqkey;
	char   *coordName = IS_PGXC_COORDINATOR ? PGXCNodeName : GetMyCoordName;
	int		coordPid = IS_PGXC_COORDINATOR ? MyProcPid : MyCoordPid;

	CheckConnection();
	seqkey.gsk_keylen = strlen(seqname) + 1;
	seqkey.gsk_key = seqname;

	return conn ? set_val(conn, &seqkey, coordName, coordPid, nextval, iscalled) : -1;
}

/*
 * Drop the sequence depending the key type
 *
 * Type of Sequence name use in key;
 *		GTM_SEQ_FULL_NAME, full name of sequence
 *		GTM_SEQ_DB_NAME, DB name part of sequence key
 */
int
DropSequenceGTM(char *name, GTM_SequenceKeyType type)
{
	GTM_SequenceKeyData seqkey;
	CheckConnection();
	seqkey.gsk_keylen = strlen(name) + 1;
	seqkey.gsk_key = name;
	seqkey.gsk_type = type;

	return conn ? close_sequence(conn, &seqkey, GetTopTransactionId()) : -1;
}

/*
 * Rename the sequence
 */
int
RenameSequenceGTM(char *seqname, const char *newseqname)
{
	GTM_SequenceKeyData seqkey, newseqkey;
	CheckConnection();
	seqkey.gsk_keylen = strlen(seqname) + 1;
	seqkey.gsk_key = seqname;
	
	newseqkey.gsk_keylen = strlen(newseqname) + 1;
	newseqkey.gsk_key = (char *) newseqname;
	return conn ? rename_sequence(conn, &seqkey, &newseqkey,
			GetTopTransactionId()) : -1;
}

/*
 * Copy the database sequences from src database
 */
int
CopyDataBaseSequenceGTM(char *src_dbname, char *dest_dbname)
{
	GTM_SequenceKeyData src_seqkey, dest_seqkey;
	CheckConnection();
	src_seqkey.gsk_keylen = strlen(src_dbname) + 1;
	src_seqkey.gsk_key = src_dbname;

	dest_seqkey.gsk_keylen = strlen(dest_dbname) + 1;
	dest_seqkey.gsk_key = (char *) dest_dbname;
	return conn ? copy_database_sequence(conn, &src_seqkey, &dest_seqkey,
										 GetTopTransactionId()) : -1;
}

/*
 * Register Given Node
 * Connection for registering is just used once then closed
 */
int
RegisterGTM(GTM_PGXCNodeType type)
{
	int ret;

	CheckConnection();

	if (!conn)
		return EOF;

	ret = node_register(conn, type, 0, PGXCNodeName, "");
	elog(LOG, "node register %s", PGXCNodeName);
	/* If something went wrong, retry once */
	if (ret < 0)
	{
		CloseGTM();
		InitGTM();
		if (conn)
			ret = node_register(conn, type, 0, PGXCNodeName, "");
	}

	return ret;
}

/*
 * UnRegister Given Node
 * Connection for registering is just used once then closed
 */
int
UnregisterGTM(GTM_PGXCNodeType type)
{
	int ret;

	CheckConnection();

	if (!conn)
		return EOF;

	ret = node_unregister(conn, type, PGXCNodeName);

	/* If something went wrong, retry once */
	if (ret < 0)
	{
		CloseGTM();
		InitGTM();
		if (conn)
			ret = node_unregister(conn, type, PGXCNodeName);
	}

	/*
	 * If node is unregistered cleanly, cut the connection.
	 * and Node shuts down smoothly.
	 */
	CloseGTM();

	return ret;
}

/*
 * Report BARRIER
 */
int
ReportBarrierGTM(const char *barrier_id)
{
	if (!gtm_backup_barrier)
		return EINVAL;

	CheckConnection();

	if (!conn)
		return EOF;

	return(report_barrier(conn, barrier_id));
}

int
ReportGlobalXmin(GlobalTransactionId gxid, GlobalTransactionId *global_xmin,
		GlobalTransactionId *latest_completed_xid)
{
	int errcode = GTM_ERRCODE_UNKNOWN;

	CheckConnection();
	if (!conn)
		return EOF;
#ifdef __OPENTENBASE_C__
	report_global_xmin(conn, PGXCNodeName,
			IS_PGXC_COORDINATOR ?  GTM_NODE_COORDINATOR : ( IS_PGXC_DATANODE ? GTM_NODE_DATANODE : GTM_NODE_FORWARDNODE),
			gxid, global_xmin, latest_completed_xid, &errcode);
#else
	report_global_xmin(conn, PGXCNodeName,
			IS_PGXC_COORDINATOR ?  GTM_NODE_COORDINATOR : GTM_NODE_DATANODE,
			gxid, global_xmin, latest_completed_xid, &errcode);
#endif
	return errcode;
}


void 
CleanGTMSeq(void)
{
	char   *coordName = IS_PGXC_COORDINATOR ? PGXCNodeName : GetMyCoordName;
	int		coordPid = IS_PGXC_COORDINATOR ? MyProcPid : MyCoordPid;
	int		status;

	CheckConnection();

	if (conn)
		status = clean_session_sequence(conn, coordName, coordPid);
	else
		status = GTM_RESULT_COMM_ERROR;

	/* retry once */
	if (status == GTM_RESULT_COMM_ERROR)
	{
		CloseGTM();
		InitGTM();
		if (conn)
			status = clean_session_sequence(conn, coordName, coordPid);
	}
	if (status != GTM_RESULT_OK)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_TRANSACTION_STATE),
				 errmsg("%s", GTMPQerrorMessage(conn))));

	return;
}

#ifdef __OPENTENBASE__
void ResetGtmInfo(void)
{
	if (GtmHost)
	{
		free(GtmHost);
		GtmHost = NULL;
	}
	GtmPort = 0;
}

void
CheckGTMConnection(void)
{
	CheckConnection();
}

/*
 * Rename the sequence
 */
int
RenameDBSequenceGTM(const char *seqname, const char *newseqname)
{
	GTM_SequenceKeyData seqkey, newseqkey;
	CheckConnection();
	seqkey.gsk_keylen = strlen(seqname) + 1;
	seqkey.gsk_key = (char*)seqname;
	
	newseqkey.gsk_keylen = strlen(newseqname) + 1;
	newseqkey.gsk_key = (char *) newseqname;
	return conn ? rename_db_sequence(conn, &seqkey, &newseqkey,
			GetTopTransactionId()) : -1;
}
#endif

#define BitmapSet(bit, bitmap) \
({ \
	int index = bit >> BITMAP_RADIX_BIT; \
	int pos = bit & BITMAP_RADIX_MASK; \
	bitmap[index] |= (1 << pos); \
})

#ifdef __RESOURCE_QUEUE__

/*
 * Create a resqueue on the GTM.
 */
int
CreateResQueueGTM(ResQueueInfo * resq_info)
{
	CheckConnection();

	if (conn)
	{
		return resqueue_open(conn,
								&resq_info->resq_name,
								&resq_info->resq_group,
								resq_info->resq_memory_limit,
								resq_info->resq_network_limit,
								resq_info->resq_active_stmts,
								resq_info->resq_wait_overload,
								resq_info->resq_priority);
	}
	else
	{
		return -1;
	}
}

/*
 * Alter a resqueue on the GTM
 */
int
AlterResQueueGTM(ResQueueInfo * resq_info,
					char alter_name,
					char alter_group,
					char alter_memory_limit,
					char alter_network_limit,
					char alter_active_stmts,
					char alter_wait_overload,
					char alter_priority)
{
	CheckConnection();

	if (conn)
	{
		return resqueue_alter(conn,
								&resq_info->resq_name,
								&resq_info->resq_group,
								resq_info->resq_memory_limit,
								resq_info->resq_network_limit,
								resq_info->resq_active_stmts,
								resq_info->resq_wait_overload,
								resq_info->resq_priority,
								alter_name,
								alter_group,
								alter_memory_limit,
								alter_network_limit,
								alter_active_stmts,
								alter_wait_overload,
								alter_priority);
	}
	else
	{
		return -1;
	}
}

/*
 * Drop the resqueue
 */
int
DropResQueueGTM(NameData * resq_name)
{
	CheckConnection();

	if (conn)
	{
		return resqueue_close(conn, resq_name);
	}
	else
	{
		return -1;
	}
}

/*
 * Check whether the resource queue exists
 */
int
CheckIfExistsResQueueGTM(NameData * resq_name, bool * exists)
{
	CheckConnection();

	if (conn)
	{
		return resqueue_check_if_exists(conn, resq_name, exists);
	}
	else
	{
		return -1;
	}
}

static int GetGTMStoreResourceQueue(GTM_StoredResQueueDataInfo **store_resq)
{
	int ret = 0;

	CheckConnection();
	ret = -1;
	if (conn)
	{
		ret = get_storage_resqueue_list(conn, store_resq);
	}

	/*
	 * If something went wrong (timeout), try and reset GTM connection.
	 * We will abort the transaction locally anyway, and closing GTM will force
	 * it to end on GTM.
	 */
	if (ret < 0)
	{
		CloseGTM();
		InitGTM();
		if (conn)
		{
			ret = get_storage_resqueue_list(conn, store_resq);
		}
	}

	return ret;
}

static int CheckGTMStoreResourceQueue(GTMStorageResQueueStatus **store_resq, bool need_fix)
{
	int ret = 0;

	CheckConnection();
	ret = -1;
	if (conn)
	{
		ret = check_storage_resqueue(conn, store_resq, need_fix);
	}

	/*
	 * If something went wrong (timeout), try and reset GTM connection.
	 * We will abort the transaction locally anyway, and closing GTM will force
	 * it to end on GTM.
	 */
	if (ret < 0)
	{
		CloseGTM();
		InitGTM();
		if (conn)
		{
			ret = check_storage_resqueue(conn, store_resq, need_fix);
		}
	}

	return ret;
}

static int GetGTMResourceQueueList(GTM_ResQueueInfo **resq_list)
{
	int ret = 0;

	CheckConnection();
	ret = -1;
	if (conn)
	{
		ret = get_resqueue_list(conn, resq_list);
	}

	/*
	 * If something went wrong (timeout), try and reset GTM connection.
	 * We will abort the transaction locally anyway, and closing GTM will force
	 * it to end on GTM.
	 */
	if (ret < 0)
	{
		CloseGTM();
		InitGTM();
		if (conn)
		{
			ret = get_resqueue_list(conn, resq_list);
		}
	}

	return ret;
}

static int GetGTMResourceQueueUsage(NameData * resq_name, GTM_ResQUsageInfo **usages)
{
	int ret = 0;

	CheckConnection();
	ret = -1;
	if (conn)
	{
		ret = get_resqueue_usage(conn, resq_name, usages);
	}

	/*
	 * If something went wrong (timeout), try and reset GTM connection.
	 * We will abort the transaction locally anyway, and closing GTM will force
	 * it to end on GTM.
	 */
	if (ret < 0)
	{
		CloseGTM();
		InitGTM();
		if (conn)
		{
			ret = get_resqueue_usage(conn, resq_name, usages);
		}
	}

	return ret;
}

Datum
opentenbase_list_all_storage_resqueue(PG_FUNCTION_ARGS)
{
#define NUM_RESQ_STATUS_COLUMNS	10
	
	FuncCallContext   *funcctx = NULL;
	PG_Storage_status *mystatus = NULL;
	GTM_StoredResQueueDataInfo *resqs = NULL;

	NOT_SUPPORTED_IN_CENTRALIZED_MODE;

	if (SRF_IS_FIRSTCALL())
	{
		TupleDesc	tupdesc = NULL;
		MemoryContext oldcontext = NULL;

		/* create a function context for cross-call persistence */
		funcctx = SRF_FIRSTCALL_INIT();
		
		/*
		 * switch to memory context appropriate for multiple function calls
		 */
		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);
		
		/* build tupdesc for result tuples */
		/* this had better match function's declaration in pg_proc.h */
		tupdesc = CreateTemplateTupleDesc(NUM_RESQ_STATUS_COLUMNS, false);
		TupleDescInitEntry(tupdesc, (AttrNumber) 1, "resq_name",
						   NAMEOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 2, "resq_memory_limit_bytes",
						   INT8OID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 3, "resq_network_limit_bytes",
						   INT8OID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 4, "resq_active_stmts_limit",
						   INT4OID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 5, "resq_wait_overload",
						   INT2OID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 6, "resq_priority",
						   TEXTOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 7, "resq_status",
						   INT4OID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 8, "gti_store_handle",
						   INT4OID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 9, "gti_next",
						   INT4OID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 10, "resq_crc",
						   INT4OID, -1, 0);

		funcctx->tuple_desc = BlessTupleDesc(tupdesc);

		mystatus = (PG_Storage_status *) palloc0(sizeof(PG_Storage_status));
		funcctx->user_fctx = (void *) mystatus;

		MemoryContextSwitchTo(oldcontext);

		mystatus->totalIdx = GetGTMStoreResourceQueue((GTM_StoredResQueueDataInfo **)&mystatus->data);
		if (mystatus->totalIdx < 0)
		{
			elog(ERROR, "get resource queue info from gtm failed");	
		}		
	}

	funcctx  = SRF_PERCALL_SETUP();
	mystatus = (PG_Storage_status *) funcctx->user_fctx;	
	resqs     = (GTM_StoredResQueueDataInfo*)mystatus->data;
	while (mystatus->currIdx < mystatus->totalIdx)
	{
		Datum		values[NUM_RESQ_STATUS_COLUMNS] = { 0 };
		bool		nulls[NUM_RESQ_STATUS_COLUMNS] = { 0 };

		HeapTuple	tuple = NULL;
		Datum		result = 0;
		GTM_StoredResQueueDataInfo *instance = NULL;

		instance = &(resqs[mystatus->currIdx]);		

		/*
		 * Form tuple with appropriate data.
		 */
		MemSet(values, 0, sizeof(values));
		MemSet(nulls, false, sizeof(nulls));

		values[0] = NameGetDatum(&instance->resq_name);
		values[1] = Int64GetDatum(instance->resq_memory_limit);
		values[2] = Int64GetDatum(instance->resq_network_limit);
		values[3] = Int32GetDatum(instance->resq_active_stmts);
		values[4] = Int16GetDatum(instance->resq_wait_overload);
		values[5] = CStringGetTextDatum(ResQPriorityGetSetting(instance->resq_priority));
		values[6] = Int32GetDatum(instance->resq_status);
		values[7] = Int32GetDatum(instance->gti_store_handle);
		values[8] = Int32GetDatum(instance->resq_next);
		values[9] = UInt32GetDatum(instance->resq_crc);

		tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);
		result = HeapTupleGetDatum(tuple);
		
		mystatus->currIdx++;
		SRF_RETURN_NEXT(funcctx, result);
	}	

	SRF_RETURN_DONE(funcctx);
#undef NUM_RESQ_STATUS_COLUMNS
}

Datum
opentenbase_check_all_storage_resqueue(PG_FUNCTION_ARGS)
{
#define NUM_CHECK_RESQ_STATUS_COLUMNS	12	
	FuncCallContext   *funcctx = NULL;
	PG_Storage_status *mystatus = NULL;
	GTMStorageResQueueStatus *resqs = NULL;

	NOT_SUPPORTED_IN_CENTRALIZED_MODE;

	if (SRF_IS_FIRSTCALL())
	{
		int32		  need_fix = false;
		TupleDesc	  tupdesc = NULL;
		MemoryContext oldcontext = NULL;

		/* create a function context for cross-call persistence */
		funcctx = SRF_FIRSTCALL_INIT();

		need_fix = PG_GETARG_BOOL(0);

		/*
		 * switch to memory context appropriate for multiple function calls
		 */
		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);
		
		/* build tupdesc for result tuples */
		/* this had better match function's declaration in pg_proc.h */
		tupdesc = CreateTemplateTupleDesc(NUM_CHECK_RESQ_STATUS_COLUMNS, false);
		TupleDescInitEntry(tupdesc, (AttrNumber) 1, "resq_name",
						   NAMEOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 2, "resq_memory_limit_bytes",
						   INT8OID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 3, "resq_network_limit_bytes",
						   INT8OID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 4, "resq_active_stmts_limit",
						   INT4OID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 5, "resq_wait_overload",
						   INT2OID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 6, "resq_priority",
						   TEXTOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 7, "resq_status",
						   INT4OID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 8, "gti_store_handle",
						   INT4OID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 9, "gti_next",
						   INT4OID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 10, "resq_crc",
						   INT4OID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 11, "error_code",
						   INT4OID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 12, "check_status",
						   INT4OID, -1, 0);
		
		funcctx->tuple_desc = BlessTupleDesc(tupdesc);

		mystatus = (PG_Storage_status *) palloc0(sizeof(PG_Storage_status));
		funcctx->user_fctx = (void *) mystatus;

		MemoryContextSwitchTo(oldcontext);

		mystatus->totalIdx = CheckGTMStoreResourceQueue((GTMStorageResQueueStatus **)&mystatus->data, need_fix);
		if (mystatus->totalIdx < 0)
		{
			elog(ERROR, "get resource queue info from gtm failed");	
		}		
	}

	funcctx  = SRF_PERCALL_SETUP();
	mystatus = (PG_Storage_status *) funcctx->user_fctx;	
	resqs    = (GTMStorageResQueueStatus*)mystatus->data;
	while (mystatus->currIdx < mystatus->totalIdx)
	{
		Datum		values[NUM_CHECK_RESQ_STATUS_COLUMNS] = { 0 };
		bool		nulls[NUM_CHECK_RESQ_STATUS_COLUMNS] = { 0 };
		
		HeapTuple	tuple = NULL;
		Datum		result = 0;
		GTMStorageResQueueStatus *instance = NULL;

		instance = &(resqs[mystatus->currIdx]);		

		/*
		 * Form tuple with appropriate data.
		 */
		MemSet(values, 0, sizeof(values));
		MemSet(nulls, false, sizeof(nulls));

		values[0] = NameGetDatum(&instance->resqueue.resq_name);
		values[1] = Int64GetDatum(instance->resqueue.resq_memory_limit);
		values[2] = Int64GetDatum(instance->resqueue.resq_network_limit);
		values[3] = Int32GetDatum(instance->resqueue.resq_active_stmts);
		values[4] = Int16GetDatum(instance->resqueue.resq_wait_overload);
		values[5] = CStringGetTextDatum(ResQPriorityGetSetting(instance->resqueue.resq_priority));
		values[6] = Int32GetDatum(instance->resqueue.resq_status);
		values[7] = Int32GetDatum(instance->resqueue.gti_store_handle);
		values[8] = Int32GetDatum(instance->resqueue.resq_next);
		values[9] = UInt32GetDatum(instance->resqueue.resq_crc);
		values[10] = Int32GetDatum(instance->error);
		values[11] = Int32GetDatum(instance->status);	

		tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);
		result = HeapTupleGetDatum(tuple);
		
		mystatus->currIdx++;
		SRF_RETURN_NEXT(funcctx, result);
	}

	SRF_RETURN_DONE(funcctx);

#undef NUM_CHECK_RESQ_STATUS_COLUMNS
}

Datum
opentenbase_list_all_resqueue(PG_FUNCTION_ARGS)
{
#define NUM_RESQ_LIST_COLUMNS	9
	
	FuncCallContext   *funcctx = NULL;
	PG_Storage_status *mystatus = NULL;
	GTM_ResQueueInfo  *resqs = NULL;

	NOT_SUPPORTED_IN_CENTRALIZED_MODE;

	if (SRF_IS_FIRSTCALL())
	{
		TupleDesc	tupdesc = NULL;
		MemoryContext oldcontext = NULL;

		/* create a function context for cross-call persistence */
		funcctx = SRF_FIRSTCALL_INIT();
		
		/*
		 * switch to memory context appropriate for multiple function calls
		 */
		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);
		
		/* build tupdesc for result tuples */
		/* this had better match function's declaration in pg_proc.h */
		tupdesc = CreateTemplateTupleDesc(NUM_RESQ_LIST_COLUMNS, false);
		TupleDescInitEntry(tupdesc, (AttrNumber) 1, "resq_name",
						   NAMEOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 2, "resq_memory_limit_bytes",
						   INT8OID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 3, "resq_network_limit_bytes",
						   INT8OID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 4, "resq_active_stmts_limit",
						   INT4OID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 5, "resq_wait_overload",
						   INT2OID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 6, "resq_priority",
						   TEXTOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 7, "resq_state",
						   INT4OID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 8, "gti_store_handle",
						   INT4OID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 9, "resq_memory_used",
						   INT8OID, -1, 0);

		funcctx->tuple_desc = BlessTupleDesc(tupdesc);

		mystatus = (PG_Storage_status *) palloc0(sizeof(PG_Storage_status));
		funcctx->user_fctx = (void *) mystatus;

		MemoryContextSwitchTo(oldcontext);

		mystatus->totalIdx = GetGTMResourceQueueList((GTM_ResQueueInfo **)&mystatus->data);
		if (mystatus->totalIdx < 0)
		{
			elog(ERROR, "get resource queue info from gtm failed");	
		}		
	}

	funcctx  = SRF_PERCALL_SETUP();
	mystatus = (PG_Storage_status *) funcctx->user_fctx;	
	resqs     = (GTM_ResQueueInfo*)mystatus->data;
	while (mystatus->currIdx < mystatus->totalIdx)
	{
		Datum		values[NUM_RESQ_LIST_COLUMNS] = { 0 };
		bool		nulls[NUM_RESQ_LIST_COLUMNS] = { 0 };

		HeapTuple	tuple = NULL;
		Datum		result = 0;
		GTM_ResQueueInfo *instance = NULL;

		instance = &(resqs[mystatus->currIdx]);		

		/*
		 * Form tuple with appropriate data.
		 */
		MemSet(values, 0, sizeof(values));
		MemSet(nulls, false, sizeof(nulls));

		values[0] = NameGetDatum(&instance->resq_name);
		values[1] = Int64GetDatum(instance->resq_memory_limit);
		values[2] = Int64GetDatum(instance->resq_network_limit);
		values[3] = Int32GetDatum(instance->resq_active_stmts);
		values[4] = Int16GetDatum(instance->resq_wait_overload);
		values[5] = CStringGetTextDatum(ResQPriorityGetSetting(instance->resq_priority));
		values[6] = Int32GetDatum(instance->resq_state);
		values[7] = Int32GetDatum(instance->resq_store_handle);
		values[8] = Int64GetDatum(instance->resq_used_memory);

		tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);
		result = HeapTupleGetDatum(tuple);
		
		mystatus->currIdx++;
		SRF_RETURN_NEXT(funcctx, result);
	}	

	SRF_RETURN_DONE(funcctx);
#undef NUM_RESQ_LIST_COLUMNS
}

Datum
opentenbase_list_resqueue_usage(PG_FUNCTION_ARGS)
{
#define NUM_RESQ_USAGE_COLUMNS	6
	
	FuncCallContext   *funcctx = NULL;
	PG_Storage_status *mystatus = NULL;
	GTM_ResQUsageInfo *usages = NULL;

	NOT_SUPPORTED_IN_CENTRALIZED_MODE;

	if (SRF_IS_FIRSTCALL())
	{
		TupleDesc	tupdesc = NULL;
		MemoryContext oldcontext = NULL;

		Name resq_name = NULL;

		/* create a function context for cross-call persistence */
		funcctx = SRF_FIRSTCALL_INIT();

		resq_name = PG_GETARG_NAME(0);

		/*
		 * switch to memory context appropriate for multiple function calls
		 */
		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);
		
		/* build tupdesc for result tuples */
		/* this had better match function's declaration in pg_proc.h */
		tupdesc = CreateTemplateTupleDesc(NUM_RESQ_USAGE_COLUMNS, false);
		TupleDescInitEntry(tupdesc, (AttrNumber) 1, "resq_name",
						   NAMEOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 2, "node_name",
						   NAMEOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 3, "node_proc_pid",
						   INT4OID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 4, "node_backend_id",
						   INT4OID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 5, "memory_bytes",
						   INT8OID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 6, "role",
						   TEXTOID, -1, 0);

		funcctx->tuple_desc = BlessTupleDesc(tupdesc);

		mystatus = (PG_Storage_status *) palloc0(sizeof(PG_Storage_status));
		funcctx->user_fctx = (void *) mystatus;

		MemoryContextSwitchTo(oldcontext);

		mystatus->totalIdx = GetGTMResourceQueueUsage(resq_name, (GTM_ResQUsageInfo **)&mystatus->data);
		if (mystatus->totalIdx < 0)
		{
			elog(ERROR, "get resource queue usage info from gtm failed");	
		}		
	}

	funcctx  = SRF_PERCALL_SETUP();
	mystatus = (PG_Storage_status *) funcctx->user_fctx;	
	usages     = (GTM_ResQUsageInfo*)mystatus->data;
	while (mystatus->currIdx < mystatus->totalIdx)
	{
		Datum		values[NUM_RESQ_USAGE_COLUMNS] = { 0 };
		bool		nulls[NUM_RESQ_USAGE_COLUMNS] = { 0 };

		HeapTuple	tuple = NULL;
		Datum		result = 0;
		GTM_ResQUsageInfo *instance = NULL;

		instance = &(usages[mystatus->currIdx]);		

		/*
		 * Form tuple with appropriate data.
		 */
		MemSet(values, 0, sizeof(values));
		MemSet(nulls, false, sizeof(nulls));

		values[0] = NameGetDatum(&instance->usage.resqName);
		values[1] = NameGetDatum(&instance->usage.nodeName);
		values[2] = Int32GetDatum(instance->usage.nodeProcPid);
		values[3] = Int32GetDatum(instance->usage.nodeBackendId);
		values[4] = Int64GetDatum(instance->usage.memoryBytes);

		if (instance->isuser == true)
		{
			values[5] = CStringGetTextDatum("using");
		}
		else
		{
			values[5] = CStringGetTextDatum("waiting");
		}

		tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);
		result = HeapTupleGetDatum(tuple);
		
		mystatus->currIdx++;
		SRF_RETURN_NEXT(funcctx, result);
	}	

	SRF_RETURN_DONE(funcctx);
#undef NUM_RESQ_USAGE_COLUMNS
}

GlobalTimestamp 
GetSharedLatestCommitTS(void)
{
    GlobalTimestamp latest_gts;

    latest_gts = pg_atomic_read_u64(&ShmemVariableCache->latestGTS);
    
    return latest_gts;
}

void
SetLatestCommitTSInternal(GlobalTimestamp gts, const char *file, const char *func, int line)
{
    GlobalTimestamp latest_gts;
    GlobalTimestamp max_gts;

    latest_gts = pg_atomic_read_u64(&ShmemVariableCache->latestGTS);

    while (gts > latest_gts)
    {
        if (pg_atomic_compare_exchange_u64(&ShmemVariableCache->latestGTS,
                                           &latest_gts,
                                           gts))
            break;
    }

    if(enable_distri_print)
    {
        max_gts = pg_atomic_read_u64(&ShmemVariableCache->maxCommitTs);
        elog(LOG,
             "set latest committs "UINT64_FORMAT", maxCommitTs "UINT64_FORMAT" latestGTS "UINT64_FORMAT", %s %s:%d",
                gts, max_gts, latest_gts,func, file, line);
    }
}

GlobalTimestamp 
GetSharedMaxCommitTs(void)
{
    GlobalTimestamp gts;

    gts = pg_atomic_read_u64(&ShmemVariableCache->maxCommitTs);
    
    return gts;
}

GlobalTimestamp
AdvanceSharedMaxCommitTs(void)
{
    GlobalTimestamp gts;

    gts = pg_atomic_add_fetch_u64_impl(&ShmemVariableCache->maxCommitTs, 1);

    if(enable_distri_print)
    {
        elog(LOG, "advance max committs "UINT64_FORMAT, gts);
    }
    SetSharedLatestCommitTS(gts);
    return gts;
}

void
SetMaxCommitTsInternal(GlobalTimestamp gts, const char *file, const char *func, int line)
{
    GlobalTimestamp latest_gts;
    GlobalTimestamp max_gts;
    
    max_gts = pg_atomic_read_u64(&ShmemVariableCache->maxCommitTs);

    while (gts > max_gts)
    {
        if (pg_atomic_compare_exchange_u64(&ShmemVariableCache->maxCommitTs,
                                           &max_gts,
                                           gts))
            break;
    }
    
    if(enable_distri_print)
    {
        latest_gts = pg_atomic_read_u64(&ShmemVariableCache->latestGTS);
        elog(LOG,
             "set max committs "UINT64_FORMAT", maxCommitTs "UINT64_FORMAT" latestGTS "UINT64_FORMAT", %s %s:%d",
             gts, max_gts, latest_gts,func, file, line);
    }
}

void
SetAllCommitTsInternal(GlobalTimestamp gts, const char *file, const char *func, int line)
{
    GlobalTimestamp latest_gts;
    GlobalTimestamp max_gts;


    max_gts = pg_atomic_read_u64(&ShmemVariableCache->maxCommitTs);

    while (gts > max_gts)
    {
        if (pg_atomic_compare_exchange_u64(&ShmemVariableCache->maxCommitTs,
                                           &max_gts,
                                           gts))
            break;
    }

    latest_gts = pg_atomic_read_u64(&ShmemVariableCache->latestGTS);

    while (gts > latest_gts)
    {
        if (pg_atomic_compare_exchange_u64(&ShmemVariableCache->latestGTS,
                                           &latest_gts,
                                           gts))
            break;
    }
    
    if(enable_distri_print)
    {
        elog(LOG,
             "set latest committs "UINT64_FORMAT", maxCommitTs "UINT64_FORMAT" latestGTS "UINT64_FORMAT", %s %s:%d",
                gts, max_gts, latest_gts,func, file, line);
    }
}

TransactionId 
GetLatestCompletedXid(void)
{
    TransactionId xid;
    xid = pg_atomic_read_u32(&ShmemVariableCache->latestCompletedXid);
    return xid;
}

#endif
