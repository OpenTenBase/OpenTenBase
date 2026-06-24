/*-------------------------------------------------------------------------
 *
 * tupdesc.h
 *	  POSTGRES tuple descriptor definitions.
 *
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/access/tupdesc.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef TUPDESC_H
#define TUPDESC_H

#include "access/attnum.h"
#include "catalog/pg_attribute.h"
#include "lib/stringinfo.h"
#include "nodes/pg_list.h"


#define MaxFixedDataLen 16 

#define AttrMaxLength(attr) \
		type_maximum_size((attr)->atttypid, (attr)->atttypmod) 

/* 
 * It aligns data size to a power of 2 
 * and is reentrant. 
 */
#define AttAligned(attlen) \
	(\
		((attlen) <= 2) ? (attlen):(fast_alignup_base_2(attlen))\
	)

static inline int fast_alignup_base_2(int num)
{
	num--;
	num |= num >> 1;
	num |= num >> 2;
	num |= num >> 4;
	num |= num >> 8;
	num |= num >> 16;
	num++;
	return num;
}

typedef uint32 Bitmap;
#define BITMAP_RADIX_BIT 5
#define BITMAP_RADIX_MASK 31
#define BITMAP_RADIX 32

typedef struct attrDefault
{
	AttrNumber	adnum;
	char	   *adbin;			/* nodeToString representation of expr */
} AttrDefault;

typedef struct constrCheck
{
	char	   *ccname;
	char	   *ccbin;			/* nodeToString representation of expr */
	bool		ccvalid;
	bool		ccnoinherit;	/* this is a non-inheritable constraint */
#ifdef __OPENTENBASE__
    bool        ccenable;       /* constraint is enabled */
#endif
} ConstrCheck;

#ifdef  _MLS_
typedef struct attrMissing *MissingPtr;
#endif

/* This structure contains constraints of a tuple */
typedef struct tupleConstr
{
	AttrDefault *defval;		/* array */
	ConstrCheck *check;			/* array */

#ifdef _MLS_
    MissingPtr	missing;		/* missing attributes values, NULL if none */
#endif

	uint16		num_defval;
	uint16		num_check;
	bool		has_not_null;
} TupleConstr;

#ifdef _MLS_

/* 
 * append the following structs into tupledesc 
 */
 
typedef struct datamask
{
    int     attmasknum;     /* a copy of nattr */ 
    bool   *mask_array;     /* to record datamask info separately */ 
} Datamask;
#endif

/*
 * This struct is passed around within the backend to describe the structure
 * of tuples.  For tuples coming from on-disk relations, the information is
 * collected from the pg_attribute, pg_attrdef, and pg_constraint catalogs.
 * Transient row types (such as the result of a join query) have anonymous
 * TupleDesc structs that generally omit any constraint info; therefore the
 * structure is designed to let the constraints be omitted efficiently.
 *
 * Note that only user attributes, not system attributes, are mentioned in
 * TupleDesc; with the exception that tdhasoid indicates if OID is present.
 *
 * If the tupdesc is known to correspond to a named rowtype (such as a table's
 * rowtype) then tdtypeid identifies that type and tdtypmod is -1.  Otherwise
 * tdtypeid is RECORDOID, and tdtypmod can be either -1 for a fully anonymous
 * row type, or a value >= 0 to allow the rowtype to be looked up in the
 * typcache.c type cache.
 *
 * Note that tdtypeid is never the OID of a domain over composite, even if
 * we are dealing with values that are known (at some higher level) to be of
 * a domain-over-composite type.  This is because tdtypeid/tdtypmod need to
 * match up with the type labeling of composite Datums, and those are never
 * explicitly marked as being of a domain type, either.
 *
 * Tuple descriptors that live in caches (relcache or typcache, at present)
 * are reference-counted: they can be deleted when their reference count goes
 * to zero.  Tuple descriptors created by the executor need no reference
 * counting, however: they are simply created in the appropriate memory
 * context and go away when the context is freed.  We set the tdrefcount
 * field of such a descriptor to -1, while reference-counted descriptors
 * always have tdrefcount >= 0.
 *
 * *************************************************
 *	Important ! remember to take care of FIELDNO_*
 * *************************************************
 */
typedef struct tupleDesc
{
	int			natts;			/* number of attributes in the tuple */
	Oid			tdtypeid;		/* composite type ID for tuple type */
	int32		tdtypmod;		/* typmod for tuple type */
	bool		tdhasoid;		/* tuple has oid attribute in its header */
	int			tdrefcount;		/* reference count, or -1 if not counting */
	bool		tdhasrowid;		/* tuple has a serial number column */
	bool		tdincl_nodeid;	/* if including node id in rowid */
#ifdef _MLS_
	Datamask   *tdatamask;		/* record those cols needing to be mask */
#endif
	bool        cn_penetrate_tuple;
	TupleConstr *constr;		/* constraints, or NULL if none */
	/* attrs[N] is the description of Attribute Number N+1 */
	FormData_pg_attribute attrs[FLEXIBLE_ARRAY_MEMBER];
}		   *TupleDesc;

/* Accessor for the i'th attribute of tupdesc. */
#define TupleDescAttr(tupdesc, i) (&(tupdesc)->attrs[(i)])

/*
 * Only data with fixed length between 0 and 16 
 * are considered as fixed length data.
 */
#define IsVarLenData(tup, attno) \
	(TupleDescAttr((tup), (attno))->attcollen 	< 0 ||\
	 TupleDescAttr((tup), (attno))->attcollen > MaxFixedDataLen)

#define AttrIsVarLenData(attr) \
	((attr)->attcollen 	< 0 ||\
	  (attr)->attcollen > MaxFixedDataLen)

#define IsColumnVarLenData(desc) \
	((desc).collen 	< 0 ||\
	  (desc).collen > 16)

extern TupleDesc CreateTemplateTupleDesc(int natts, bool hasoid);

extern TupleDesc CreateTupleDesc(int natts, bool hasoid,
				Form_pg_attribute *attrs);

extern TupleDesc CreateTupleDescCopy(TupleDesc tupdesc);

extern TupleDesc CreateTupleDescCopyConstr(TupleDesc tupdesc);

#define TupleDescSize(src) \
	(offsetof(struct tupleDesc, attrs) + \
	 (src)->natts * sizeof(FormData_pg_attribute))

extern void TupleDescCopy(TupleDesc dst, TupleDesc src);

extern void TupleDescCopyEntry(TupleDesc dst, AttrNumber dstAttno,
				   TupleDesc src, AttrNumber srcAttno);

extern void FreeTupleDesc(TupleDesc tupdesc);

extern void IncrTupleDescRefCount(TupleDesc tupdesc);
extern void DecrTupleDescRefCount(TupleDesc tupdesc);

#define PinTupleDesc(tupdesc) \
	do { \
		if ((tupdesc)->tdrefcount >= 0) \
			IncrTupleDescRefCount(tupdesc); \
	} while (0)

#define ReleaseTupleDesc(tupdesc) \
	do { \
		if ((tupdesc)->tdrefcount >= 0) \
			DecrTupleDescRefCount(tupdesc); \
	} while (0)

extern bool tupleDescIsSame(TupleDesc tupdesc1, TupleDesc tupdesc2, bool ignore_attname);
extern bool equalTupleDescs(TupleDesc tupdesc1, TupleDesc tupdesc2);

extern uint32 hashTupleDesc(TupleDesc tupdesc);

extern void TupleDescInitEntry(TupleDesc desc,
				   AttrNumber attributeNumber,
				   const char *attributeName,
				   Oid oidtypeid,
				   int32 typmod,
				   int attdim);

extern void TupleDescInitBuiltinEntry(TupleDesc desc,
						  AttrNumber attributeNumber,
						  const char *attributeName,
						  Oid oidtypeid,
						  int32 typmod,
						  int attdim);

extern void TupleDescInitEntryCollation(TupleDesc desc,
							AttrNumber attributeNumber,
							Oid collationid);

extern TupleDesc BuildDescForRelation(List *schema, char	relstore, int default_compress_id, bool inh);

extern TupleDesc BuildDescFromLists(List *names, List *types, List *typmods, List *collations);

extern bool GetColumnEncodingInfo(List *encoding, char *compression_id);

extern void BitmapClear(int bit, Bitmap *bitmap);

extern bool BitmapTest(int bit, Bitmap *bitmap);
extern void assign_attribute_alignedlen(TupleDesc tupleDesc, int attno);

extern void SerializeDescriptor(StringInfo buf, TupleDesc typeinfo);
extern TupleDesc DeserializeDescriptor(char *str, Size *len);

#endif							/* TUPDESC_H */
