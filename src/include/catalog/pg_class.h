/*-------------------------------------------------------------------------
 *
 * pg_class.h
 *	  definition of the system "relation" relation (pg_class)
 *	  along with the relation's initial contents.
 *
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/catalog/pg_class.h
 *
 * NOTES
 *	  the genbki.pl script reads this file and generates .bki
 *	  information from the DATA() statements.
 *
 *-------------------------------------------------------------------------
 */
#ifndef PG_CLASS_H
#define PG_CLASS_H

#include "catalog/genbki.h"

/* ----------------
 *		pg_class definition.  cpp turns this into
 *		typedef struct FormData_pg_class
 * ----------------
 */
#define RelationRelationId	1259
#define RelationRelation_Rowtype_Id  83

CATALOG(pg_class,1259) BKI_BOOTSTRAP BKI_ROWTYPE_OID(83) BKI_SCHEMA_MACRO
{
	NameData	relname;		/* class name */
	Oid			relnamespace;	/* OID of namespace containing this class */
	Oid			reltype;		/* OID of entry in pg_type for table's
								 * implicit row type */
	Oid			reloftype;		/* OID of entry in pg_type for underlying
								 * composite type */
	Oid			relowner;		/* class owner */
	Oid			relam;			/* index access method; 0 if not an index */
	Oid			relfilenode;	/* identifier of physical storage file */

	/* relfilenode == 0 means it is a "mapped" relation, see relmapper.c */
	Oid			reltablespace;	/* identifier of table space for relation */
	int32		relpages;		/* # of blocks (not always up-to-date) */
	float4		reltuples;		/* # of tuples (not always up-to-date; -1 means "unknown") */
	int32		relallvisible;	/* # of all-visible blocks (not always
								 * up-to-date) */
	Oid			reltoastrelid;	/* OID of toast table; 0 if none */
	Oid			relstashrelid;
	Oid			relstashidxid;
	Oid			relregistryrelid;
	Oid			relregistryidxid;

	bool		relhasindex;	/* T if has (or has had) any indexes */

	bool		relisshared;	/* T if shared across databases */
	char		relpersistence; /* see RELPERSISTENCE_xxx constants below */
	char		relkind;		/* see RELKIND_xxx constants below */
	int16		relnatts;		/* number of user attributes */
	/*
	 * Class pg_attribute must contain exactly "relnatts" user attributes
	 * (with attnums ranging from 1 to relnatts) for this class.  It may also
	 * contain entries with negative attnums for system attributes.
	 */
	int16		relchecks;		/* # of CHECK constraints for class */
	bool		relhasoids;		/* T if we generate OIDs for rows of rel */
	bool		relhaspkey;		/* has (or has had) PRIMARY KEY index */
	bool		relhasclusterkey;
	bool		relhasrules;	/* has (or has had) any rules */
	bool		relhastriggers; /* has (or has had) any TRIGGERs */
	bool		relhassubclass; /* has (or has had) derived classes */
	bool		relrowsecurity; /* row security is enabled or not */
	bool		relforcerowsecurity;	/* row security forced for owners or
										 * not */
	bool		relispopulated; /* matview currently holds query results */
	char		relreplident;	/* see REPLICA_IDENTITY_xxx constants  */
	bool		relispartition; /* is relation a partition? */

#ifdef _PG_ORCL_
	Oid			relrowidseq; /* RowId sequence generator */
#endif
#ifdef _SHARDING_
	bool		relhasextent;   /* T if organize with extent */
#endif
#ifdef __OPENTENBASE_C__
	char		relstore;		/* see RELSTORE_xxx constants below */
	int16		relflags;		/* see RELFLAGS_xxx constants below */
#endif

#ifdef __OPENTENBASE__
	/* used for interval partition */
	char        relpartkind;    /* partition parent or partition child or non-partition */
	Oid         relparent;      /* partition parent objectid if partition child */
#endif

	TransactionId relfrozenxid; /* all Xids < this are frozen in this rel */
	TransactionId relminmxid;	/* all multixacts in this rel are >= this.
								 * this is really a MultiXactId */
#ifdef CATALOG_VARLEN			/* variable-length fields start here */
	/* NOTE: These fields are not present in a relcache entry's rd_rel field. */
	aclitem		relacl[1];		/* access permissions */
	text		reloptions[1];	/* access-method-specific options */
	pg_node_tree relpartbound;	/* partition bound node tree */
#endif
} FormData_pg_class;

/* Size of fixed part of pg_class tuples, not counting var-length fields */
#define CLASS_TUPLE_SIZE \
	 (offsetof(FormData_pg_class,relminmxid) + sizeof(TransactionId))

#ifdef _PG_ORCL_
#define RelationIsPartition(rform) ((rform)->relispartition)
#endif

#define RelationIsIndex(rform) (RelkindIsIndex((rform->relkind)))
#define RelkindIsIndex(relkind) ((relkind) == RELKIND_INDEX || \
								 (relkind) == RELKIND_PARTITIONED_INDEX || \
								 (relkind) == RELKIND_GLOBAL_INDEX || \
								 (relkind) == RELKIND_PARTITIONED_GLOBAL_INDEX || \
								 (relkind) == RELKIND_GLOBAL_INDEX_OF_PARTITIONED)
#define RelationIsCrossNodeIndex(rform) (RelkindIsCrossNodeIndex((rform->relkind)))
#define RelkindIsCrossNodeIndex(relkind)                                                           \
	((relkind) == RELKIND_GLOBAL_INDEX || (relkind) == RELKIND_PARTITIONED_GLOBAL_INDEX)
#define RelationIsTemp(rel) ((rel)->relpersistence == RELPERSISTENCE_TEMP || \
								(rel)->relpersistence == RELPERSISTENCE_GLOBAL_TEMP)

#define RelationIsActivatingGlobalTemp(rel) \
		(((rel)->relpersistence == RELPERSISTENCE_GLOBAL_TEMP) && \
			(rel)->schemaname && \
			strcmp((rel)->schemaname, DUMMY_GTT_NAMESPACE) == 0)
/* ----------------
 *		Form_pg_class corresponds to a pointer to a tuple with
 *		the format of pg_class relation.
 * ----------------
 */
typedef FormData_pg_class *Form_pg_class;

/* ----------------
 *		compiler constants for pg_class
 * ----------------
 */


#define Natts_pg_class						44
#define Anum_pg_class_relname				1
#define Anum_pg_class_relnamespace			2
#define Anum_pg_class_reltype				3
#define Anum_pg_class_reloftype				4
#define Anum_pg_class_relowner				5
#define Anum_pg_class_relam					6
#define Anum_pg_class_relfilenode			7
#define Anum_pg_class_reltablespace			8
#define Anum_pg_class_relpages				9
#define Anum_pg_class_reltuples				10
#define Anum_pg_class_relallvisible			11
#define Anum_pg_class_reltoastrelid			12
#define Anum_pg_class_relstashrelid 		13
#define Anum_pg_class_relstashidx 			14
#define Anum_pg_class_relregistryrelid 		15
#define Anum_pg_class_relregistryidx 		16
#define Anum_pg_class_relhasindex			17
#define Anum_pg_class_relisshared			18
#define Anum_pg_class_relpersistence		19
#define Anum_pg_class_relkind				20
#define Anum_pg_class_relnatts				21
#define Anum_pg_class_relchecks				22
#define Anum_pg_class_relhasoids			23
#define Anum_pg_class_relhaspkey			24
#define Anum_pg_class_relhasclusterkey 		25
#define Anum_pg_class_relhasrules			26
#define Anum_pg_class_relhastriggers		27
#define Anum_pg_class_relhassubclass		28
#define Anum_pg_class_relrowsecurity		29
#define Anum_pg_class_relforcerowsecurity	30
#define Anum_pg_class_relispopulated		31
#define Anum_pg_class_relreplident			32
#define Anum_pg_class_relispartition		33
#ifdef _PG_ORCL_
#define Anum_pg_class_relrowidseq			34
#endif
#ifdef _SHARDING_
#define Anum_pg_class_relhasextent			35
#endif
#ifdef __OPENTENBASE_C__
#define Anum_pg_class_relstore				36
#define Anum_pg_class_relflags				37
#endif
#ifdef __OPENTENBASE__
#define Anum_pg_class_relpartkind			38
#define Anum_pg_class_relparent			    39
#endif
#define Anum_pg_class_relfrozenxid			40
#define Anum_pg_class_relminmxid			41

#define Anum_pg_class_relacl				42
#define Anum_pg_class_reloptions			43
#define Anum_pg_class_relpartbound			44

/* ----------------
 *		initial contents of pg_class
 *
 * NOTE: only "bootstrapped" relations need to be declared here.  Be sure that
 * the OIDs listed here match those given in their CATALOG macros, and that
 * the relnatts values are correct.
 * ----------------
 */

/*
 * Note: "3" in the relfrozenxid column stands for FirstNormalTransactionId;
 * similarly, "1" in relminmxid stands for FirstMultiXactId
 */
DATA(insert OID = 1247 (  pg_type       PGNSP 71 0 PGUID 0 0 0 0 0 0 0 0 0 0 0 f f p r 30 0 t f f f f f f f t n f 0 f r 0 n 0 3 1 _null_ _null_ _null_));
DESCR("");                                                                                                  
DATA(insert OID = 1249 (  pg_attribute  PGNSP 75 0 PGUID 0 0 0 0 0 0 0 0 0 0 0 f f p r 29 0 f f f f f f f f t n f 0 f r 0 n 0 3 1 _null_ _null_ _null_));
DESCR("");                                                                                                  
DATA(insert OID = 1255 (  pg_proc       PGNSP 81 0 PGUID 0 0 0 0 0 0 0 0 0 0 0 f f p r 28 0 t f f f f f f f t n f 0 f r 0 n 0 3 1 _null_ _null_ _null_));
DESCR("");
DATA(insert OID = 1259 (  pg_class      PGNSP 83 0 PGUID 0 0 0 0 0 0 0 0 0 0 0 f f p r 44 0 t f f f f f f f t n f 0 f r 0 n 0 3 1 _null_ _null_ _null_));
DESCR("");

#define		  RELKIND_RELATION		  'r'	/* ordinary table */
#define		  RELKIND_INDEX			  'i'	/* secondary index */
#define		  RELKIND_SEQUENCE		  'S'	/* sequence object */
#define		  RELKIND_TOASTVALUE	  't'	/* for out-of-line values */
#define		  RELKIND_VIEW			  'v'	/* view */
#define		  RELKIND_MATVIEW		  'm'	/* materialized view */
#define		  RELKIND_COMPOSITE_TYPE  'c'	/* composite type */
#define		  RELKIND_FOREIGN_TABLE   'f'	/* foreign table */
#define		  RELKIND_PARTITIONED_TABLE 'p' /* partitioned table */
#define		  RELKIND_PARTITIONED_INDEX 'I' /* partitioned index */
#define		  RELKIND_NESTED_TABLE_TYPE 'n' /* nested table type, as a array */
#define		  RELKIND_GLOBAL_INDEX		'g' /* global index, heap table */
#define		  RELKIND_PARTITIONED_GLOBAL_INDEX 'G' /* partitioned global index, heap table */
#define		  RELKIND_GLOBAL_INDEX_OF_PARTITIONED 'C' /* global index for partitioned table */
#define		  RELKIND_INVALID		  'x'

#define		  RELPERSISTENCE_PERMANENT	'p' /* regular table */
#define		  RELPERSISTENCE_UNLOGGED	'u' /* unlogged permanent table */
#define		  RELPERSISTENCE_TEMP		't' /* temporary table */
#define		  RELPERSISTENCE_GLOBAL_TEMP	'g' /* global temporary table */

#ifdef PGXC
#define		  RELPERSISTENCE_LOCAL_TEMP	'l'	/* local temp table */
#endif

/* default selection for replica identity (primary key or nothing) */
#define		  REPLICA_IDENTITY_DEFAULT	'd'
/* no replica identity is logged for this relation */
#define		  REPLICA_IDENTITY_NOTHING	'n'
/* all columns are logged as replica identity */
#define		  REPLICA_IDENTITY_FULL		'f'
/*
 * an explicitly chosen candidate key's columns are used as replica identity.
 * Note this will still be set if the index has been dropped; in that case it
 * has the same meaning as 'd'.
 */
#define		  REPLICA_IDENTITY_INDEX	'i'

/*
 * Relation kinds that have physical storage. These relations normally have
 * relfilenode set to non-zero, but it can also be zero if the relation is
 * mapped.
 */
#define RELKIND_CAN_HAVE_STORAGE(relkind) \
   ((relkind) == RELKIND_RELATION || \
    (relkind) == RELKIND_INDEX || \
    (relkind) == RELKIND_SEQUENCE || \
    (relkind) == RELKIND_TOASTVALUE || \
    (relkind) == RELKIND_MATVIEW)


#ifdef _MLS_
/* enum for relkindext column */
#define       RELKIND_AUDIT_SYS_TABLE   'a'
#define       RELKIND_MLS_SYS_TABLE     's'
#define       RELKIND_MLS_HAS_POLICY    'y'
#define       RELKIND_MLS_NO_POLICY     'n'
#define       RELKIND_SYS_TABLE         't'
#define       RELKIND_NORMAL_TABLE      'n'
#endif

#ifdef __OPENTENBASE_C__
/*
 * relstore describes how a relkind is physically stored in the database.
 *
 * RELSTORE_ROW     	- stored on disk using row store.
 */
#define		  RELSTORE_ROW			'r'

#define 	  RELFLAGS_NORMAL			0
#endif

#define REL_PARALLEL_DML_SAFE		"safe" /* can run in worker or master */
#define REL_PARALLEL_DML_RESTRICTED	"restricted" /* can run in parallel master only */
#define REL_PARALLEL_DML_UNSAFE		"unsafe" /* banned while in parallel mode */
#define REL_PARALLEL_DML_DEFAULT		"default" /* only used for parallel dml safety */

extern int errdetail_relkind_not_supported(char relkind);
#endif							/* PG_CLASS_H */
