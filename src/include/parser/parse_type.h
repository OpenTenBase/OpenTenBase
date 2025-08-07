/*-------------------------------------------------------------------------
 *
 * parse_type.h
 *		handle type operations for parser
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/parser/parse_type.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef PARSE_TYPE_H
#define PARSE_TYPE_H

#include "access/htup.h"
#include "parser/parse_node.h"


typedef HeapTuple Type;

extern Type LookupTypeName(ParseState *pstate, const TypeName *typeName,
			   int32 *typmod_p, bool missing_ok);
extern Type LookupTypeNameExtended(ParseState *pstate,
								   const TypeName *typeName, int32 *typmod_p,
								   bool temp_ok, bool missing_ok);
extern Oid LookupTypeNameOid(ParseState *pstate, const TypeName *typeName,
				  bool missing_ok);
extern Type typenameType(ParseState *pstate, const TypeName *typeName,
			 int32 *typmod_p);
extern Oid	typenameTypeId(ParseState *pstate, const TypeName *typeName);
extern void typenameTypeIdAndMod(ParseState *pstate, const TypeName *typeName,
					 Oid *typeid_p, int32 *typmod_p, bool is_column);

extern char *TypeNameToString(const TypeName *typeName);
extern char *TypeNameListToString(List *typenames);

extern Oid	LookupCollation(ParseState *pstate, List *collnames, int location);
extern Oid	GetColumnDefCollation(ParseState *pstate, ColumnDef *coldef, Oid typeOid);

extern Type typeidType(Oid id);

extern Oid	typeTypeId(Type tp);
extern int16 typeLen(Type t);
extern bool typeByVal(Type t);
extern char *typeTypeName(Type t);
extern Oid	typeTypeRelid(Type typ);
extern Oid	typeTypeCollation(Type typ);
extern Datum stringTypeDatum(Type tp, char *string, int32 atttypmod);

extern Oid	typeidTypeRelid(Oid type_id);
extern Oid	typeOrDomainTypeRelid(Oid type_id);

extern TypeName *typeStringToTypeName(const char *str);
extern void parseTypeString(const char *str, Oid *typeid_p, int32 *typmod_p, bool missing_ok);

/* true if typeid is composite, or domain over composite, but not RECORD */
#define ISCOMPLEX(typeid) (typeOrDomainTypeRelid(typeid) != InvalidOid)

#ifdef __OPENTENBASE_C__
extern bool typeSupportColTable(Oid type_id);
extern bool typeIsColDatumType(Oid type_id);
extern bool is_integer_type(char *typname);
extern bool is_numericfd_type(Oid type_oid);
extern bool CheckTypeInPkg(const char *typeName, Oid pkgid);
extern bool LookupPackageUDT(const TypeName *typeName, Oid *pkg_oid,
							 char **udtname, Oid p_pkgid, Oid *base_type,
							 Oid *o_synoid);
extern bool LookupPackageVarType(const TypeName *typeName, Oid *pkg_oid, char **o_udtname,
								 Oid p_pkgid, Oid *act_typid, bool notice);
extern bool LookupPackageAnyType(const TypeName *typeName, Oid *pkg_oid, char **o_udtname,
								 Oid p_pkgid, Oid *act_typid, bool notice, Oid *o_synoid);
extern bool LookupPackageType(const TypeName *typeName, Oid p_pkgid, Oid *act_typid);
extern bool CheckTypeInPkg(const char *typeName, Oid pkgid);
#endif

extern int32 GetIntervalTypmodForOpenTenBaseOraOut(List *args, Oid typid, int32 src_typmod);

#endif							/* PARSE_TYPE_H */
