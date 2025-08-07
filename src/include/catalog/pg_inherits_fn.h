/*-------------------------------------------------------------------------
 *
 * pg_inherits_fn.h
 *	  prototypes for functions in catalog/pg_inherits.c
 *
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/catalog/pg_inherits_fn.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef PG_INHERITS_FN_H
#define PG_INHERITS_FN_H

#include "nodes/pg_list.h"
#include "storage/lock.h"

extern List *find_inheritance_children(Oid parentrelId, LOCKMODE lockmode);
extern List *find_all_inheritors(Oid parentrelId, LOCKMODE lockmode,
					List **parents);
extern bool has_subclass(Oid relationId);
extern bool has_subclass_exact(Oid relationId);
extern bool has_superclass(Oid relationId);
extern bool typeInheritsFrom(Oid subclassTypeId, Oid superclassTypeId);
extern void StoreSingleInheritance(Oid relationId, Oid parentOid,
					   int32 seqNumber);
extern bool DeleteInheritsTuple(Oid inhrelid, Oid inhparent);
extern void find_all_children(Oid reloid, List **res);

#endif							/* PG_INHERITS_FN_H */
