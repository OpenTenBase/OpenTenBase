/*
 * Copyright (c) 2004-2007 EnterpriseDB Corporation. All Rights Reserved.
 */
#ifndef PGXC_CLASS_H
#define PGXC_CLASS_H

#include "nodes/parsenodes.h"

#define PgxcClassRelationId  9001

CATALOG(pgxc_class,9001) BKI_WITHOUT_OIDS
{
	Oid		pcrelid;		/* Table Oid */
	char		pclocatortype;		/* Type of distribution */
	int16 		pchashalgorithm;	/* Hashing algorithm */
	int16 		pchashbuckets;		/* Number of buckets */
#ifdef _MIGRATE_
	int16		psecondattnum;		/* Column number of secondary distribution */
	Oid 		pgroup; 			/* distribute group */
	Oid         pcoldgroup;			/* cold distribute group */
#endif
	/* VARIABLE LENGTH FIELDS: */
	oidvector	nodeoids;		    /* List of nodes used by table */
#ifdef CATALOG_VARLEN
#ifdef _MIGRATE_
	oidvector	coldnodeoids;	    /* List of code nodes used by table */
#endif
#ifdef __OPENTENBASE_C__
	int2vector  discolnums;         /* attr number of distributed columns(start from third if exists) */
#endif
#endif
} FormData_pgxc_class;

typedef FormData_pgxc_class *Form_pgxc_class;

#define Natts_pgxc_class					    10

#define Anum_pgxc_class_pcrelid				    1
#define Anum_pgxc_class_pclocatortype		    2
#define Anum_pgxc_class_pchashalgorithm		    3
#define Anum_pgxc_class_pchashbuckets		    4
#define Anum_pgxc_class_second_distribute	    5
#define Anum_pgxc_class_distribute_group	    6
#define Anum_pgxc_class_cold_distribute_group   7
#define Anum_pgxc_class_nodes				    8
#define Anum_pgxc_class_cold_nodes			    9
#define Anum_pgxc_class_discolnums			    10

typedef enum PgxcClassAlterType
{
	PGXC_CLASS_ALTER_DISTRIBUTION,
	PGXC_CLASS_ALTER_NODES,
	PGXC_CLASS_ALTER_ALL
} PgxcClassAlterType;

#ifdef _MIGRATE_

typedef enum PgxcClassModifyType
{
	PGXC_CLASS_ADD_NODE,
	PGXC_CLASS_DROP_NODE
	/* more */
} PgxcClassModifyType;

typedef struct PgxcClassModifyData
{
	Oid group;
	Oid node;
	/* more */
} PgxcClassModifyData;
extern void PgxcClassCreate(Oid pcrelid,
								char pclocatortype,
#ifdef __OPENTENBASE_C__
								int ndiscols,
								int *discolnums,
#endif
								int pchashalgorithm,
								int pchashbuckets,
								int numnodes,
								Oid *nodes,
								Oid group);
#else
extern void PgxcClassCreate(Oid pcrelid,
							char pclocatortype,
							int pcattnum,
							int pchashalgorithm,
							int pchashbuckets,
							int numnodes,
							Oid *nodes);
#endif

extern void PgxcClassAlter(Oid pcrelid,
							char pclocatortype,
							int ndiscols,
							AttrNumber *discolnums,
							int pchashalgorithm,
							int pchashbuckets,
							int numnodes,
							Oid *nodes,
							PgxcClassAlterType type);
extern void RemovePgxcClass(Oid pcrelid);


#ifdef _MIGRATE_
extern void RegisterDistributeKey(Oid pcrelid,
									char pclocatortype,
									int ndiscols,
									AttrNumber *discolnums,
									int pchashalgorithm,
									int pchashbuckets);
extern char GetRelLocatorType(Oid reloid);
extern Oid GetRelGroup(Oid reloid);
extern void GetNotShardRelations(bool is_contain_replic, 
										List **rellist, 
										List **nslist);

extern List * GetShardRelations(bool is_contain_replic);
extern void ModifyPgxcClass(PgxcClassModifyType type, PgxcClassModifyData *data);

extern void CheckPgxcClassGroupConfilct(Oid keyvaluehot);
#endif


#endif   /* PGXC_CLASS_H */
