/*-------------------------------------------------------------------------
 *
 * pg_type_object.h
 *	  definition of the system "type object" relation (pg_type_object)
 *	  along with the relation's initial contents.
 *
 * Portions Copyright (c) 2020-2030, Tencent.com.
 * Portions Copyright (c) 2012-2014, TransLattice, Inc.
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/catalog/pg_type_object.h
 *
 * NOTES
 *	  The script catalog/genbki.pl reads this file and generates .bki
 *	  information from the DATA() statements.  utils/Gen_fmgrtab.pl
 *	  generates fmgroids.h and fmgrtab.c the same way.
 *
 *	  XXX do NOT break up DATA() statements into multiple lines!
 *		  the scripts are not as smart as you might think...
 *	  XXX (eg. #if 0 #endif won't do what you think)
 *
 *-------------------------------------------------------------------------
 */
#ifndef PG_TYPE_OBJECT_H
#define PG_TYPE_OBJECT_H

#include "catalog/genbki.h"
#include "catalog/objectaddress.h"

#define TypeObjectRelationId  4192

CATALOG(pg_type_object,4192)
{
	NameData	objname;  /* type object name */
	Oid			objnamespace;	/* namespace that owns the type object */
	Oid			objcrrspndingns;/* OID of namespace containing this type object's members */
	Oid			objspecoid;		/* type object spec oid, -1 if this is spec */
	Oid			objcreatensoid;	/* this namespace is created by this type object */
	Oid			objowner;		/* type object owner,ahthor id */
	Oid			objlang;		/* OID of pg_language entry */
	Oid			objparent;		/* may inherit from other object, reserved for inherit, default is 0 */
	Oid			objchild;		/* reserverd for inherit, default is 0 */
	int64		objattr;		/* reserved for some type object attribute, default 0 */

	float4		objcost;			  /* estimated execution cost */
	float4		objrows;			  /* estimated # of rows out (if proretset) */
	bool        objeditionable;		  /* is pkg editionable ? */
	bool        objseriallyreuseable; /* is pkg seriallyreuseable ? */

	NameData    objcollation;   /* type object collation */
	int16		objaccesstype;	/* type object access type, value of obj_accesstype */
	Oid			objaccessobject;/* type object access object, if objaccesstype not specified will be set INVALID OID */
	bool		objissystem;
	bool		objsecdef;
	bool		objisspec;

#ifdef CATALOG_VARLEN
	text		objheadersrc   BKI_FORCE_NOT_NULL; /* type object header (specification) */
	text		objfuncspec;
	text		objbodysrc     BKI_FORCE_NOT_NULL; /* type object body */

	text		objprocdefsrc;	/* type object procedure definition source */

	text		objconfig[1];	/* type object-local GUC settings */
	aclitem		objacl[1];		/* access permissions */
	Oid			objfunc[1];
	int32		objfunctype[1];
#endif
} FormData_pg_type_object;

/* ----------------
 *		Form_pg_type_object corresponds to a pointer to a tuple with
 *		the format of pg_type_object relation.
 * ----------------
 */
typedef FormData_pg_type_object *Form_pg_type_object;

/* ----------------
 *		compiler constants for pg_type_object
 * ----------------
 */
#define Natts_pg_type_object					28
#define Anum_pg_type_object_name			    1
#define Anum_pg_type_object_namespace		    2
#define Anum_pg_type_object_crrspndingns        3
#define Anum_pg_type_object_specoid			    4
#define Anum_pg_type_object_creatensoid			5
#define Anum_pg_type_object_owner			    6
#define Anum_pg_type_object_lang			    7
#define Anum_pg_type_object_parent				8
#define Anum_pg_type_object_child				9
#define Anum_pg_type_object_attr				10
#define Anum_pg_type_object_cost			    11
#define Anum_pg_type_object_rows			    12
#define Anum_pg_type_object_editionable		    13
#define Anum_pg_type_object_seriallyreuseable	14
#define Anum_pg_type_object_collation		    15
#define Anum_pg_type_object_accesstype		    16
#define Anum_pg_type_object_accessobject		17
#define Anum_pg_type_object_issystem			18
#define Anum_pg_type_object_secdef				19
#define Anum_pg_type_object_isspec				20
#define Anum_pg_type_object_headersrc			21
#define Anum_pg_type_object_funcspec			22
#define Anum_pg_type_object_bodysrc				23
#define Anum_pg_type_object_procdefsrc			24
#define Anum_pg_type_object_config				25
#define Anum_pg_type_object_acl					26
#define Anum_pg_type_object_func				27
#define Anum_pg_type_object_func_type			28

#endif							/* PG_TYPE_OBJECT_H */
