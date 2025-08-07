/*-------------------------------------------------------------------------
 *
 * pg_compile.h
 *    Save the compile result of a plpgsql object.
 *
 * Portions Copyright (c) 2021, Tencent.com.
 *
 * src/include/catalog/pg_compile.h
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
#ifndef PG_COMPILE_H
#define PG_COMPILE_H

#include "catalog/genbki.h"
#include "catalog/objectaddress.h"

#define CompileRelationId  5008

CATALOG(pg_compile,5008) BKI_WITHOUT_OIDS
{
	Oid		compclassid;	/* classid of a object */
	Oid		compobjid;		/* Oid of a object */
	char	comptype;	/* compile type. See below. */
	bool	compvalid;

	int32	udnargs; /* proc: nargs with udt */
	Oid		udretpkg; /* proc: package Oid if return type is from it */

	/*
	 * variable-length fields start here.
	 */
#ifdef CATALOG_VARLEN
	/* package: compile related */
	text	compcode;
	text	compcode_priv;
	text	compudt[1]; /* compiled user-defined types */
	Oid		compudtbase[1];
	text	compudt_priv[1]; /* compiled user-defined types in private of package */
	Oid		compudopentenbase_priv[1];

	/* proc: dependent package */
	Oid		compdeppkg[1];

	/* proc: actual qualified typename if udt. */
	Oid		udargpkg[1];
	text	udargtype[1];

	/* proc: udretpkg's typename */
	text	udrettype;
#endif
} FormData_pg_compile;

/* ----------------
 *		Form_pg_compile corresponds to a pointer to a tuple with
 *		the format of pg_compile relation.
 * ----------------
 */
typedef FormData_pg_compile *Form_pg_compile;

/* ----------------
 *		compiler constants for pg_compile
 * ----------------
 */
#define Natts_pg_compile					16

#define Natts_pg_compile_compclassid		1
#define Natts_pg_compile_compobjid			2
#define Natts_pg_compile_comptype			3
#define Natts_pg_compile_compvalid			4
#define Natts_pg_compile_udnargs			5
#define Natts_pg_compile_udretpkg			6
#define Natts_pg_compile_compcode			7
#define Natts_pg_compile_compcode_priv		8
#define Natts_pg_compile_compudt			9
#define Natts_pg_compile_compudtbase		10
#define Natts_pg_compile_compudt_priv		11
#define Natts_pg_compile_compudopentenbase_priv	12
#define Natts_pg_compile_compdeppkg			13
#define Natts_pg_compile_udargpkg			14
#define Natts_pg_compile_udargtype			15
#define Natts_pg_compile_udrettype			16

#define PG_COMP_NONE		'n'
#define PG_COMP_INTERPRETED	'i'
#define PG_COMP_NATIVE		'm'

#define CompilePkgPubSection	1
#define CompilePkgPrivSection	2

extern void CreateOrUpdateFuncArgCompile(Oid clssid, Oid objid, Datum argtype);
extern void CreateOrUpdateFuncRetCompile(Oid clssid, Oid objid, Datum type);
extern HeapTuple ConstructFuncCompileTuple(ArrayType *argtype, char *rettype);
extern void CreateOrUpdatePkgCompile(Oid pkgid, List *udt, List *basetypid, int comp_pkg);
extern void DiscardPkgCompile(Oid pkgoid, int comp_pkg);
extern void DropCompile(Oid clssid, Oid objid);
extern bool CompiledUDTMatched(Oid funcid, List *fargs, void *p);
extern List *GetFunctionUDTArgs(Oid funcid);
extern List *GetFunctionUDTArgsByTuple(HeapTuple tup);
extern Oid GetPackageDefinedTypeId(Oid pkgId, char *typname, bool search_all);
extern bool GetFunctionActualUdtType(Oid funcid, int i, char **udtname);
extern bool GetFunctionActualUdtTypeByTuple(HeapTuple tup, int idx, char **udtname);
extern bool GetFuncitonActualRetType(Oid funcid, char **udtname);
extern bool GetFuncitonActualRetTypeByTuple(HeapTuple tup, char **udtname);

#endif							/* PG_COMPILE_H */
