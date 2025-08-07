/*-------------------------------------------------------------------------
 *
 * sysattr.h
 *	  POSTGRES system attribute definitions.
 *
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/access/sysattr.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef SYSATTR_H
#define SYSATTR_H


#define NODEID_OFFSET	48
#define TID_MASK		0x0000FFFFFFFFFFFF

#define FormGlobalTid(ctid) \
	(((uint64)(PGXCNodeId - 1) << NODEID_OFFSET) + (ctid))

#define GtidGetTid(gtid)	\
	((uint64)(gtid) & TID_MASK)

/*
 * Attribute numbers for the system-defined attributes
 */
#define SelfItemPointerAttributeNumber			(-1)
#define ObjectIdAttributeNumber					(-2)
#define MinTransactionIdAttributeNumber			(-3)
#define MinCommandIdAttributeNumber				(-4)
#define MaxTransactionIdAttributeNumber			(-5)
#define MaxCommandIdAttributeNumber				(-6)
#define TableOidAttributeNumber					(-7)
#define XC_NodeIdAttributeNumber				(-8)		/* PGXC*/
#define ShardIdAttributeNumber					(-9)		/* SHARD */
#define XmaxGTSIdAttributeNumber				(-10)		/* GTS */
#define XminGTSAttributeNumber					(-11)		/* GTS */
#define ORCL_RowIdAttributeNumber               (-12)		/* PG_ORCL */
#define GlobalTidAttributeNumber				(-13)		/* LateMaterial */
#define FirstLowInvalidHeapAttributeNumber		(-14)

#endif							/* SYSATTR_H */
