/*
 *
 * This source code file contains modifications made by THL A29 Limited ("Tencent Modifications").
 * All Tencent Modifications are Copyright (C) 2023 THL A29 Limited.
 * 
 * IDENTIFICATION
 *      src/include/gtm/gtm_gxid.h
 */
#ifndef _GTM_GXID_H
#define _GTM_GXID_H

/* ----------------
 *        Special transaction ID values
 *
 * BootstrapGlobalTransactionId is the XID for "bootstrap" operations, and
 * FrozenGlobalTransactionId is used for very old tuples.  Both should
 * always be considered valid.
 *
 * FirstNormalGlobalTransactionId is the first "normal" transaction id.
 * Note: if you need to change it, you must change pg_class.h as well.
 * ----------------
 */
#define BootstrapGlobalTransactionId        ((GlobalTransactionId) 1)
#define FrozenGlobalTransactionId            ((GlobalTransactionId) 2)
#define FirstNormalGlobalTransactionId    ((GlobalTransactionId) 3)
#define MaxGlobalTransactionId            ((GlobalTransactionId) 0xFFFFFFFF)
#define FirstGlobalTimestamp            ((GlobalTimestamp) 1000000000)

/* ----------------
 *        transaction ID manipulation macros
 * ----------------
 */
#define GlobalTransactionIdIsNormal(xid)        ((xid) >= FirstNormalGlobalTransactionId)
#define GlobalTransactionIdEquals(id1, id2)    ((id1) == (id2))
#define GlobalTransactionIdStore(xid, dest)    (*(dest) = (xid))
#define StoreInvalidGlobalTransactionId(dest) (*(dest) = InvalidGlobalTransactionId)

/* advance a transaction ID variable, handling wraparound correctly */
#define GlobalTransactionIdAdvance(dest)    \
    do { \
        (dest)++; \
        if ((dest) < FirstNormalGlobalTransactionId) \
            (dest) = FirstNormalGlobalTransactionId; \
    } while(0)

/* back up a transaction ID variable, handling wraparound correctly */
#define GlobalTransactionIdRetreat(dest)    \
    do { \
        (dest)--; \
    } while ((dest) < FirstNormalGlobalTransactionId)

extern bool GlobalTransactionIdPrecedes(GlobalTransactionId id1, GlobalTransactionId id2);
extern bool GlobalTransactionIdPrecedesOrEquals(GlobalTransactionId id1, GlobalTransactionId id2);
extern bool GlobalTransactionIdFollows(GlobalTransactionId id1, GlobalTransactionId id2);
extern bool GlobalTransactionIdFollowsOrEquals(GlobalTransactionId id1, GlobalTransactionId id2);
#endif
