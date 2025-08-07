/*-------------------------------------------------------------------------
 *
 * pg_dump.h
 *	  Common header file for the pg_dump utility
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/contrib/pg_dump/_dump.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef DBMS_MD_DUMP_H
#define DBMS_MD_DUMP_H

#include "dbms_md_backup.h"

/* global decls */
extern bool force_quotes;		/* double-quotes for identifiers flag */

/* placeholders for comment starting and ending delimiters */
extern char g_comment_start[10];
extern char g_comment_end[10];

extern char g_opaque_type[10];	/* name for the opaque type */

/*
 *	common utility functions
 */

extern TableInfo *dbms_md_get_schema_data(Archive *fout, int *numTablesPtr);

extern void AssignDumpId(Archive *fout, DumpableObject *dobj);
extern DumpId createDumpId(Archive *fout);
extern DumpId getMaxDumpId(Archive *fout);
extern DumpableObject *findObjectByDumpId(Archive *fout, DumpId dumpId);
extern DumpableObject *findObjectByCatalogId(Archive *fout, CatalogId catalogId);
extern void getDumpableObjects(Archive *fout, DumpableObject ***objs, int *numObjs);

extern void addObjectDependency(Archive *fout, DumpableObject *dobj, DumpId refId);
extern void removeObjectDependency(DumpableObject *dobj, DumpId refId);

extern TableInfo *findTableByOid(Archive *fout, Oid oid);
extern TypeInfo *findTypeByOid(Archive *fout, Oid oid);
extern FuncInfo *findFuncByOid(Archive *fout, Oid oid);
extern OprInfo *findOprByOid(Archive *fout, Oid oid);
extern CollInfo *findCollationByOid(Archive *fout, Oid oid);
extern NamespaceInfo *findNamespaceByOid(Archive *fout, Oid oid);
extern ExtensionInfo *findExtensionByOid(Archive *fout, Oid oid);

extern void setExtensionMembership(Archive *fout, ExtensionMemberId *extmems, int nextmems);
extern ExtensionInfo *findOwningExtension(Archive *fout, CatalogId catalogId);

extern void parseOidArray(const char *str, Oid *array, int arraysize);

extern void sortDumpableObjects(Archive *fout, DumpableObject **objs, int numObjs,
					DumpId preBoundaryId, DumpId postBoundaryId);
extern void sortDumpableObjectsByTypeName(Archive *fout, DumpableObject **objs, int numObjs);
extern void sortDataAndIndexObjectsBySize(DumpableObject **objs, int numObjs);

/*
 * version specific routines
 */
extern NamespaceInfo *getNamespaces(Archive *fout, int *numNamespaces);
extern ExtensionInfo *getExtensions(Archive *fout, int *numExtensions);
extern TypeInfo *getTypes(Archive *fout, int *numTypes);
extern FuncInfo *getFuncs(Archive *fout, int *numFuncs);
extern AggInfo *getAggregates(Archive *fout, int *numAggregates);
extern OprInfo *getOperators(Archive *fout, int *numOperators);
extern AccessMethodInfo *getAccessMethods(Archive *fout, int *numAccessMethods);
extern OpclassInfo *getOpclasses(Archive *fout, int *numOpclasses);
extern OpfamilyInfo *getOpfamilies(Archive *fout, int *numOpfamilies);
extern CollInfo *getCollations(Archive *fout, int *numCollations);
extern ConvInfo *getConversions(Archive *fout, int *numConversions);
extern TableInfo *getTables(Archive *fout, int *numTables);
extern void getOwnedSeqs(Archive *fout, TableInfo tblinfo[], int numTables);
extern InhInfo *getInherits(Archive *fout, int *numInherits);
extern void getIndexes(Archive *fout, TableInfo tblinfo[], int numTables);
extern void getExtendedStatistics(Archive *fout, TableInfo tblinfo[], int numTables);
extern void getConstraints(Archive *fout, TableInfo tblinfo[], int numTables);
extern RuleInfo *getRules(Archive *fout, int *numRules);
extern void getTriggers(Archive *fout, TableInfo tblinfo[], int numTables);
extern ProcLangInfo *getProcLangs(Archive *fout, int *numProcLangs);
extern CastInfo *getCasts(Archive *fout, int *numCasts);
extern TransformInfo *getTransforms(Archive *fout, int *numTransforms);
extern void getTableAttrs(Archive *fout, TableInfo *tbinfo, int numTables);
extern bool shouldPrintColumn(DumpOptions *dopt, TableInfo *tbinfo, int colno);
extern TSParserInfo *getTSParsers(Archive *fout, int *numTSParsers);
extern TSDictInfo *getTSDictionaries(Archive *fout, int *numTSDicts);
extern TSTemplateInfo *getTSTemplates(Archive *fout, int *numTSTemplates);
extern TSConfigInfo *getTSConfigurations(Archive *fout, int *numTSConfigs);
extern FdwInfo *getForeignDataWrappers(Archive *fout,
					   int *numForeignDataWrappers);
extern ForeignServerInfo *getForeignServers(Archive *fout,
				  int *numForeignServers);
extern DefaultACLInfo *getDefaultACLs(Archive *fout, int *numDefaultACLs);
extern void getExtensionMembership(Archive *fout, ExtensionInfo extinfo[],
					   int numExtensions);
extern void processExtensionTables(Archive *fout, ExtensionInfo extinfo[],
					   int numExtensions);
extern EventTriggerInfo *getEventTriggers(Archive *fout, int *numEventTriggers);
extern void getPolicies(Archive *fout, TableInfo tblinfo[], int numTables);
extern void getPublications(Archive *fout);
extern void getPublicationTables(Archive *fout, TableInfo tblinfo[],
					 int numTables);
extern void getSubscriptions(Archive *fout);
extern void clean_md_common_gvar(DBMS_MD_DUMP_COMMON_ST* gvar);
extern void init_md_common_gvar(DBMS_MD_DUMP_COMMON_ST* gvar);
extern bool dbms_md_check_dump_obj_valid(DBMS_MD_DUMPOBJ_BI oti);
extern bool dbms_md_is_dump_this_obj(DumpOptions *dopt, DBMS_MD_DUMPOBJ_BI oti);
#ifdef _SHARDING_
extern void dbms_md_shard_list_append(Archive *fout, DbmsMdOidList *list, const char *val);
#endif

#endif							/* DBMS_MD_DUMP_H */
