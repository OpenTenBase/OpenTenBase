/*-------------------------------------------------------------------------
 *
 * csn.h
 *	  csn definitions
 *
 * Portions Copyright (c), OPENTENBASE Development Group
 *
 * src/include/access/csn.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef CTS_H
#define CTS_H

/* Original CLOG definition */
#define CSN_INPROGRESS	            UINT64CONST(0x0)
#define CSN_LOCAL           	    UINT64CONST(0x1)
#define CSN_FROZEN                  UINT64CONST(0x2)


#define CSN_ABORTED		            UINT64CONST(0x3)
#define CSN_STATUS_COMMITTED        UINT64CONST(0x4)
#define CSN_STATUS_SUB_COMMITTED    UINT64CONST(0x5)

#define CSN_COMMITTING	            UINT64CONST(0x6)
#define CSN_TRUNCATED	            UINT64CONST(0x7)

#define CSN_USE_LATEST              UINT64CONST(0x8)

#define CSN_SUBTRANS_BIT		    (UINT64CONST(1) << 62)
#define CSN_PREPARE_BIT			    (UINT64CONST(1) << 63)

#define CSN_FIRST_NORMAL            (UINT64CONST(1000000000)) /* 1000s */

#define CSN_IS_SUBTRANS(csn) ((csn) & CSN_SUBTRANS_BIT)
#define CSN_IS_PREPARED(csn) ((csn) & CSN_PREPARE_BIT)

#define MASK_PREPARE_BIT(csn) ((csn) | CSN_PREPARE_BIT)
#define UNMASK_PREPARE_BIT(csn) ((csn) & (~CSN_PREPARE_BIT))

#define MASK_SUBTRANS_BIT(xid) ((xid) | CSN_SUBTRANS_BIT)
#define UNMASK_SUBTRANS_BIT(csn) ((csn) & (~CSN_SUBTRANS_BIT))
#define CSN_SUBTRANS_PARENT_XID(csn) ((TransactionId)UNMASK_SUBTRANS_BIT(csn))

#define CSN_IS_INPROGRESS(csn) ((csn) == CSN_INPROGRESS)
#define CSN_IS_ABORTED(csn) ((csn) == CSN_ABORTED)
#define CSN_IS_LOCAL(csn) ((csn) == CSN_LOCAL)
#define CSN_IS_FROZEN(csn) ((csn) == CSN_FROZEN)
#define CSN_IS_USE_LATEST(csn) ((csn) == CSN_USE_LATEST)
#define CSN_IS_NORMAL(csn) ((csn) >= CSN_FIRST_NORMAL && !CSN_IS_SUBTRANS(csn) && !CSN_IS_PREPARED(csn))
#define CSN_IS_COMMITTED(csn) (CSN_IS_NORMAL(csn) || CSN_IS_FROZEN(csn) || CSN_IS_LOCAL(csn))


#define InvalidGlobalTimestamp  ((GlobalTimestamp) 0)
#define LocalCommitTimestamp	CSN_LOCAL
#define FrozenGlobalTimestamp   CSN_FROZEN
#define FirstGlobalTimestamp	CSN_FIRST_NORMAL

#define GlobalTimestampIsValid(timestamp)  (timestamp != InvalidGlobalTimestamp)
#define CommitTimestampIsLocal(timestamp)  (timestamp  == LocalCommitTimestamp)
#define GlobalTimestampIsFrozen(timestamp) (timestamp == FrozenGlobalTimestamp)

#define GTM_CHECK_DELTA  (UINT64CONST(10000000)) /* 10s */

#define TIMESTAMP_SHIFT (1000 * 1000L)
#define SHIFT_GC_INTERVAL(delta) ((uint64)(delta) * TIMESTAMP_SHIFT)
#define GET_REAL_CSN_VALUE(csn) ((csn) & ~(CSN_SUBTRANS_BIT | CSN_PREPARE_BIT))

#endif							/* CTS_H */
