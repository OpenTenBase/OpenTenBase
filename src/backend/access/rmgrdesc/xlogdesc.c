/*-------------------------------------------------------------------------
 *
 * xlogdesc.c
 *	  rmgr descriptor routines for access/transam/xlog.c
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/access/rmgrdesc/xlogdesc.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/csn.h"
#include "access/xlog.h"
#include "access/xlog_internal.h"
#include "catalog/pg_control.h"
#include "utils/guc.h"
#include "utils/timestamp.h"
/*
 * GUC support
 */
const struct config_enum_entry wal_level_options[] = {
	{"minimal", WAL_LEVEL_MINIMAL, false},
	{"replica", WAL_LEVEL_REPLICA, false},
	{"archive", WAL_LEVEL_REPLICA, true},	/* deprecated */
	{"hot_standby", WAL_LEVEL_REPLICA, true},	/* deprecated */
	{"logical", WAL_LEVEL_LOGICAL, false},
	{NULL, 0, false}
};

void
xlog_desc(StringInfo buf, XLogReaderState *record)
{
	char	   *rec = XLogRecGetData(record);
	uint8		info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;

	if (info == XLOG_CHECKPOINT_SHUTDOWN ||
		info == XLOG_CHECKPOINT_ONLINE)
	{
		CheckPoint *checkpoint = (CheckPoint *) rec;
#ifdef __SUPPORT_DISTRIBUTED_TRANSACTION__
			appendStringInfo(buf, "redo %X/%X; "
							 "tli %u; prev tli %u; fpw %s; xid %u:%u; latest commit timetamp " INT64_FORMAT "; latest global timetamp " INT64_FORMAT ";oid %u; multi %u; offset %u; "
							 "oldest xid %u in DB %u; oldest multi %u in DB %u; "
							 "oldest/newest commit timestamp xid: %u/%u; "
							 "oldest running xid %u; %s",
							 (uint32) (checkpoint->redo >> 32), (uint32) checkpoint->redo,
							 checkpoint->ThisTimeLineID,
							 checkpoint->PrevTimeLineID,
							 checkpoint->fullPageWrites ? "true" : "false",
							 checkpoint->nextXidEpoch, checkpoint->nextXid,
							 checkpoint->latestCommitTs,
							 checkpoint->latestGTS,
							 checkpoint->nextOid,
							 checkpoint->nextMulti,
							 checkpoint->nextMultiOffset,
							 checkpoint->oldestXid,
							 checkpoint->oldestXidDB,
							 checkpoint->oldestMulti,
							 checkpoint->oldestMultiDB,
							 checkpoint->oldestCommitTsXid,
							 checkpoint->newestCommitTsXid,
							 checkpoint->oldestActiveXid,
							 (info == XLOG_CHECKPOINT_SHUTDOWN) ? "shutdown" : "online");
#else

		appendStringInfo(buf, "redo %X/%X; "
						 "tli %u; prev tli %u; fpw %s; xid %u:%u; oid %u; multi %u; offset %u; "
						 "oldest xid %u in DB %u; oldest multi %u in DB %u; "
						 "oldest/newest commit timestamp xid: %u/%u; "
						 "oldest running xid %u; %s",
						 (uint32) (checkpoint->redo >> 32), (uint32) checkpoint->redo,
						 checkpoint->ThisTimeLineID,
						 checkpoint->PrevTimeLineID,
						 checkpoint->fullPageWrites ? "true" : "false",
						 checkpoint->nextXidEpoch, checkpoint->nextXid,
						 checkpoint->nextOid,
						 checkpoint->nextMulti,
						 checkpoint->nextMultiOffset,
						 checkpoint->oldestXid,
						 checkpoint->oldestXidDB,
						 checkpoint->oldestMulti,
						 checkpoint->oldestMultiDB,
						 checkpoint->oldestCommitTsXid,
						 checkpoint->newestCommitTsXid,
						 checkpoint->oldestActiveXid,
						 (info == XLOG_CHECKPOINT_SHUTDOWN) ? "shutdown" : "online");
#endif
	}
	else if (info == XLOG_CLEAN_2PC_FILE)
	{
		appendStringInfo(buf, "clean 2pc file %s", rec);
	}
	else if (info == XLOG_CREATE_2PC_FILE)
	{
		TransactionId xid;
		TransactionId startxid;
		char *gid;
		char *startnode;
		char *nodestring;
		char *pos;
		char *temp;
		uint8 version = 0;
		uint8 check_code = 0;
		GlobalTimestamp prepare_gts = InvalidGlobalTimestamp;
		char *traceid = NULL;
		char *database = NULL;
		char *user = NULL;

		gid = rec;
		pos = gid + strlen(gid) + 1;
		temp = pos;
		pos = pos + strlen(temp) + 1;

		if (0 != strcmp(temp, "readonly"))
		{
			/* compatible with XLOG_FMT_2PC_V1 and XLOG_FMT_2PC_V4 format */
			startnode = temp;
			memcpy(&startxid, pos, sizeof(TransactionId));
			pos = pos + sizeof(TransactionId);
			nodestring = pos;
			pos = pos + strlen(nodestring) + 1;
			memcpy(&xid, pos, sizeof(TransactionId));
			pos = pos + sizeof(TransactionId);
			memcpy(&version, pos, 1);
			pos = pos + 1;
			memcpy(&check_code, pos, 1);
			pos = pos + 1;

			if (check_code == XLOG_FMT_2PC_CHECK_CODE &&
				version == XLOG_FMT_2PC_V4)
			{
				/* compatible with XLOG_FMT_2PC_V4 format */
				memcpy(&prepare_gts, pos, sizeof(GlobalTimestamp));
				pos = pos + sizeof(GlobalTimestamp);
				database = pos;
				pos = pos + strlen(database) + 1;
				user = pos;
				pos = pos + strlen(user) + 1;
				traceid = pos;
				pos = pos + strlen(traceid) + 1;
			}
			else
			{
				/* compatible with XLOG_FMT_2PC_V1 format */
				prepare_gts = FrozenGlobalTimestamp + 1;
				traceid = "upgrade_from_fmt_v1";
				database = "upgrade_from_fmt_v1";
				user = "upgrade_from_fmt_v1";
			}

			appendStringInfo(buf, "create 2pc file %s 2pc file name: '%s', "
							"startnode: %s, startxid: %u, nodestring: %s, "
							"xid: %u, version: %u, check_code: %u, "
							"prepare_gts: %ld, database: %s, user: %s, traceid: %s",
							rec, gid, startnode, startxid, nodestring,
							xid, version, check_code,
							prepare_gts, database, user, traceid);
		}
		else
		{
			appendStringInfo(buf, "create 2pc file %s, readonly",rec);
		}
    }
    else if (info == XLOG_RECORD_2PC_TIMESTAMP)
    {
        appendStringInfo(buf, "record global commit timestamp in 2pc file %s", rec);
    }
    else if (info == XLOG_NEXTOID)
	{
		Oid			nextOid;

		memcpy(&nextOid, rec, sizeof(Oid));
		appendStringInfo(buf, "%u", nextOid);
	}
	else if (info == XLOG_RESTORE_POINT)
	{
		xl_restore_point *xlrec = (xl_restore_point *) rec;

		appendStringInfoString(buf, xlrec->rp_name);
	}
	else if (info == XLOG_FPI || info == XLOG_FPI_FOR_HINT)
	{
		/* no further information to print */
	}
	else if (info == XLOG_BACKUP_END)
	{
		XLogRecPtr	startpoint;

		memcpy(&startpoint, rec, sizeof(XLogRecPtr));
		appendStringInfo(buf, "%X/%X",
						 (uint32) (startpoint >> 32), (uint32) startpoint);
	}
	else if (info == XLOG_PARAMETER_CHANGE)
	{
		xl_parameter_change xlrec;
		const char *wal_level_str;
		const struct config_enum_entry *entry;

		memcpy(&xlrec, rec, sizeof(xl_parameter_change));

		/* Find a string representation for wal_level */
		wal_level_str = "?";
		for (entry = wal_level_options; entry->name; entry++)
		{
			if (entry->val == xlrec.wal_level)
			{
				wal_level_str = entry->name;
				break;
			}
		}

		appendStringInfo(buf, "max_connections=%d max_worker_processes=%d "
						 "max_prepared_xacts=%d max_locks_per_xact=%d "
						 "wal_level=%s wal_log_hints=%s "
						 "track_commit_timestamp=%s",
						 xlrec.MaxConnections,
						 xlrec.max_worker_processes,
						 xlrec.max_prepared_xacts,
						 xlrec.max_locks_per_xact,
						 wal_level_str,
						 xlrec.wal_log_hints ? "on" : "off",
						 xlrec.track_commit_timestamp ? "on" : "off");
	}
	else if (info == XLOG_FPW_CHANGE)
	{
		bool		fpw;

		memcpy(&fpw, rec, sizeof(bool));
		appendStringInfoString(buf, fpw ? "true" : "false");
	}
	else if (info == XLOG_END_OF_RECOVERY)
	{
		xl_end_of_recovery xlrec;

		memcpy(&xlrec, rec, sizeof(xl_end_of_recovery));
		appendStringInfo(buf, "tli %u; prev tli %u; time %s",
						 xlrec.ThisTimeLineID, xlrec.PrevTimeLineID,
						 timestamptz_to_str(xlrec.end_time));
	}
#ifdef __OPENTENBASE__
	else if (info == XLOG_MVCC)
	{
		int32	need_mvcc;

		memcpy(&need_mvcc, rec, sizeof(int32));
		appendStringInfo(buf, "%d", need_mvcc);
	}
#endif
}

const char *
xlog_identify(uint8 info)
{
	const char *id = NULL;

	switch (info & ~XLR_INFO_MASK)
	{
		case XLOG_CHECKPOINT_SHUTDOWN:
			id = "CHECKPOINT_SHUTDOWN";
			break;
		case XLOG_CHECKPOINT_ONLINE:
			id = "CHECKPOINT_ONLINE";
			break;
		case XLOG_NOOP:
			id = "NOOP";
			break;
		case XLOG_NEXTOID:
			id = "NEXTOID";
			break;
		case XLOG_SWITCH:
			id = "SWITCH";
			break;
		case XLOG_BACKUP_END:
			id = "BACKUP_END";
			break;
		case XLOG_PARAMETER_CHANGE:
			id = "PARAMETER_CHANGE";
			break;
		case XLOG_RESTORE_POINT:
			id = "RESTORE_POINT";
			break;
		case XLOG_FPW_CHANGE:
			id = "FPW_CHANGE";
			break;
		case XLOG_END_OF_RECOVERY:
			id = "END_OF_RECOVERY";
			break;
		case XLOG_FPI:
			id = "FPI";
			break;
		case XLOG_FPI_FOR_HINT:
			id = "FPI_FOR_HINT";
			break;
		case XLOG_CLEAN_2PC_FILE:
			id = "CLEAN_2PC_FILE";
			break;
        case XLOG_CREATE_2PC_FILE:
            id = "CREATE_2PC_FILE";
            break;
        case XLOG_RECORD_2PC_TIMESTAMP:
            id = "XLOG_RECORD_2PC_TIMESTAMP";
            break;
	    case XLOG_MVCC:
			id = "NEED_MVCC";
			break;

	}

	return id;
}
