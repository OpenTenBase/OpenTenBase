/*-------------------------------------------------------------------------
 *
 * replslotdesc.h
 *
 * Portions Copyright (c) 2019, OpenTenBase Development Group
 *
 * src/include/access/replslotdesc.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef REPL_SLOT_H
#define REPL_SLOT_H

#include "access/xlog.h"
#include "access/xlogdefs.h"

typedef struct xl_replication_slot_create
{
    int             slotid;

    /* in ReplicationSlotPersistentData */
    NameData	    slotname;
    Oid			    database;
    int             persistency;
    TransactionId   xmin;
    TransactionId   catalog_xmin;
	XLogRecPtr	    restart_lsn;
    XLogRecPtr	    confirmed_flush;
    NameData	    pluginname;

    /* in ReplicationSlot */
    TransactionId   effective_xmin;
	TransactionId   effective_catalog_xmin;
	bool            pgoutput;
    Oid             subid;      /* oid of subscription at remote */
    NameData	    subname;    /* name of subscription at remote */

	Oid             relid;

} xl_replication_slot_create;

typedef struct xl_replication_slot_drop
{
    int             slotid;
    NameData        slotname;
} xl_replication_slot_drop;

typedef struct xl_replication_slot_lsn_replica
{
    int             slotid;
	TransactionId   xmin;
    TransactionId   catalog_xmin;
    XLogRecPtr	    restart_lsn;
    XLogRecPtr	    confirmed_flush;
} xl_replication_slot_lsn_replica;

#define XLOG_REPLORIGIN_SLOT_CREATE         0x10
#define XLOG_REPLORIGIN_SLOT_LSN_REPLICA    0x20
#define XLOG_REPLORIGIN_SLOT_DROP           0x40

extern void replication_slot_redo(XLogReaderState *record);
extern void replication_slot_desc(StringInfo buf, XLogReaderState *record);
extern const char * replication_slot_identify(uint8 info);

#endif                          /* REPL_SLOT_H */
