/*-------------------------------------------------------------------------
 *
 * printtup.h
 *
 *
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * This source code file contains modifications made by THL A29 Limited ("Tencent Modifications").
 * All Tencent Modifications are Copyright (C) 2023 THL A29 Limited.
 *
 * src/include/access/printtup.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef PRINTTUP_H
#define PRINTTUP_H

#include "utils/portal.h"

extern DestReceiver *printtup_create_DR(CommandDest dest);

extern void SetRemoteDestReceiverParams(DestReceiver *self, Portal portal);

extern void SendRowDescriptionMessage(TupleDesc typeinfo, List *targetlist,
                          int16 *formats);
#ifdef __OPENTENBASE__
extern void FormRowDescriptionMessage(TupleDesc typeinfo, List *targetlist, 
                          int16 *formats, StringInfo buf);
#endif

extern void debugStartup(DestReceiver *self, int operation,
             TupleDesc typeinfo);
extern bool debugtup(TupleTableSlot *slot, DestReceiver *self);

/* XXX these are really in executor/spi.c */
extern void spi_dest_startup(DestReceiver *self, int operation,
                 TupleDesc typeinfo);
extern bool spi_printtup(TupleTableSlot *slot, DestReceiver *self);

#endif                            /* PRINTTUP_H */
