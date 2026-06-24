/*-------------------------------------------------------------------------
 *
 * outfuncs.c
 *	  Output functions for Postgres tree nodes.
 *
 * Portions Copyright (c) 2012-2014, TransLattice, Inc.
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/nodes/outfuncs.c
 *
 * NOTES
 *	  Every node type that can appear in stored rules' parsetrees *must*
 *	  have an output function defined here (as well as an input function
 *	  in readfuncs.c).  In addition, plan nodes should have input and
 *	  output functions so that they can be sent to parallel workers.
 *	  For use in debugging, we also provide output functions for nodes
 *	  that appear in raw parsetrees and path.  These nodes however need
 *	  not have input functions.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <ctype.h>

#include "lib/stringinfo.h"
#include "nodes/extensible.h"
#include "nodes/plannodes.h"
#include "nodes/relation.h"
#ifdef XCP
#include "fmgr.h"
#include "miscadmin.h"
#include "catalog/namespace.h"
#include "pgxc/execRemote.h"
#include "utils/lsyscache.h"
#endif
#include "utils/datum.h"
#ifdef PGXC
#include "pgxc/planner.h"
#endif

#ifdef __AUDIT_FGA__
#include "audit/audit_fga.h"
#endif


#ifdef __AUDIT__
#include "audit/audit.h"
#endif

#ifdef __OPENTENBASE__
#include "commands/defrem.h"
#endif
#include "foreign/fdwapi.h"

#include "foreign/foreign.h"

#ifdef XCP
/*
 * When we sending query plans between nodes we need to send OIDs of various
 * objects - relations, data types, functions, etc.
 * On different nodes OIDs of these objects may differ, so we need to send an
 * identifier, depending on object type, allowing to lookup OID on target node.
 * On the other hand we want to save space when storing rules, or in other cases
 * when we need to encode and decode nodes on the same node.
 * For now default format is not portable, as it is in original Postgres code.
 * Later we may want to add extra parameter in nodeToString() function
 */
bool portable_output = false;
void
set_portable_output(bool value)
{
	portable_output = value;
}
#endif
#include "utils/rel.h"

static void outChar(StringInfo str, char c);


/*
 * Macros to simplify output of different kinds of fields.  Use these
 * wherever possible to reduce the chance for silly typos.  Note that these
 * hard-wire conventions about the names of the local variables in an Out
 * routine.
 */

/* Write the label for the node type */
#define WRITE_NODE_TYPE(nodelabel) \
	appendStringInfoString(str, nodelabel)

/* Write an integer field (anything written as ":fldname %d") */
#define WRITE_INT_FIELD(fldname) \
	appendStringInfo(str, " :" CppAsString(fldname) " %d", node->fldname)

/* Write an unsigned integer field (anything written as ":fldname %u") */
#define WRITE_UINT_FIELD(fldname) \
	appendStringInfo(str, " :" CppAsString(fldname) " %u", node->fldname)

/* Write an unsigned long integer field (anything written as ":fldname %lu") */
#define WRITE_LONG_UINT_FIELD(fldname) \
	appendStringInfo(str, " :" CppAsString(fldname) " %lu", node->fldname)

#ifdef XCP
/* Only allow output OIDs in not portable mode */
#define WRITE_OID_FIELD(fldname) \
	(AssertMacro(!portable_output), \
	 appendStringInfo(str, " :" CppAsString(fldname) " %u", node->fldname))
#else
/* Write an OID field (don't hard-wire assumption that OID is same as uint) */
#define WRITE_OID_FIELD(fldname) \
	appendStringInfo(str, " :" CppAsString(fldname) " %u", node->fldname)
#endif

/* Write a long-integer field */
#define WRITE_LONG_FIELD(fldname) \
	appendStringInfo(str, " :" CppAsString(fldname) " %ld", node->fldname)

/* Write a char field (ie, one ascii character) */
#define WRITE_CHAR_FIELD(fldname) \
	(appendStringInfo(str, " :" CppAsString(fldname) " "), \
	 outChar(str, node->fldname))

/* Write an enumerated-type field as an integer code */
#define WRITE_ENUM_FIELD(fldname, enumtype) \
	appendStringInfo(str, " :" CppAsString(fldname) " %d", \
					 (int) node->fldname)

/* Write a float field --- caller must give format to define precision */
#define WRITE_FLOAT_FIELD(fldname,format) \
	appendStringInfo(str, " :" CppAsString(fldname) " " format, node->fldname)

/* Write a boolean field */
#define WRITE_BOOL_FIELD(fldname) \
	appendStringInfo(str, " :" CppAsString(fldname) " %s", \
					 booltostr(node->fldname))

/* Write a character-string (possibly NULL) field */
#define WRITE_STRING_FIELD(fldname) \
	(appendStringInfo(str, " :" CppAsString(fldname) " "), \
	 outToken(str, node->fldname))

/* Write a parse location field (actually same as INT case) */
#define WRITE_LOCATION_FIELD(fldname) \
	appendStringInfo(str, " :" CppAsString(fldname) " %d", node->fldname)

/* Write a Node field */
#define WRITE_NODE_FIELD(fldname) \
	do { \
		appendStringInfo(str, " :" CppAsString(fldname) " "); \
		outNode(str, node->fldname); \
	} while (0)

/* Write a Node field */
#define WRITE_NODE_FIELD_WITH_NODE(fldname, param_node) \
	do { \
		appendStringInfo(str, " :" CppAsString(fldname) " "); \
		outNode(str, (param_node)->fldname); \
	} while (0)

/* Write a bitmapset field */
#define WRITE_BITMAPSET_FIELD(fldname) \
	(appendStringInfo(str, " :" CppAsString(fldname) " "), \
	 outBitmapset(str, node->fldname))

#define WRITE_ATTRNUMBER_ARRAY(fldname, len) \
	do { \
		int i; \
		appendStringInfoString(str, " :" CppAsString(fldname) " "); \
		for (i = 0; i < len; i++) \
			appendStringInfo(str, " %d", node->fldname[i]); \
	} while(0)

#define WRITE_OID_ARRAY(fldname, len) \
	do { \
		int i; \
		appendStringInfoString(str, " :" CppAsString(fldname) " "); \
		for (i = 0; i < len; i++) \
			appendStringInfo(str, " %u", node->fldname[i]); \
	} while(0)

#define WRITE_INT_ARRAY(fldname, len) \
	do { \
		int i; \
		appendStringInfoString(str, " :" CppAsString(fldname) " "); \
		for (i = 0; i < len; i++) \
			appendStringInfo(str, " %d", node->fldname[i]); \
	} while(0)

#define WRITE_BOOL_ARRAY(fldname, len) \
	do { \
		int i; \
		appendStringInfoString(str, " :" CppAsString(fldname) " "); \
		for (i = 0; i < len; i++) \
			appendStringInfo(str, " %s", booltostr(node->fldname[i])); \
	} while(0)

#ifdef XCP
#define NSP_NAME(oid) \
	isTempNamespace(oid) ? "pg_temp" : get_namespace_name(oid)
/*
 * Macros to encode OIDs to send to other nodes. Objects on other nodes may have
 * different OIDs, so send instead an unique identifier allowing to lookup
 * the OID on target node. The identifier depends on object type.
 */

#define WRITE_RELID_INTERNAL(relid) \
	(outToken(str, OidIsValid((relid)) ? NSP_NAME(get_rel_namespace((relid))) : NULL), \
	 appendStringInfoChar(str, ' '), \
	 outToken(str, OidIsValid((relid)) ? get_rel_name((relid)) : NULL))

/* write an OID which is a relation OID */
#define WRITE_RELID_FIELD(fldname) \
	(appendStringInfo(str, " :" CppAsString(fldname) " "), \
	 WRITE_RELID_INTERNAL(node->fldname))

#define WRITE_RELID_LIST_FIELD(fldname) \
	do { \
		ListCell *lc; \
		char *sep = ""; \
		appendStringInfo(str, " :" CppAsString(fldname) " "); \
		if (node->fldname == NIL || list_length(node->fldname) == 0) \
			appendStringInfoString(str, "<>"); \
		else \
		{ \
			appendStringInfoChar(str, '('); \
			foreach (lc, node->fldname) \
			{ \
				Oid relid = lfirst_oid(lc); \
				appendStringInfoString(str, sep); \
				WRITE_RELID_INTERNAL(relid); \
				sep = " , "; \
			} \
			appendStringInfoString(str, " )"); \
		} \
	}  while (0)

#define WRITE_TYPID_INTERNAL(typid) \
	(outToken(str, OidIsValid(typid) ? NSP_NAME(get_typ_namespace(typid)) : NULL), \
	 appendStringInfoChar(str, ' '), \
	 outToken(str, OidIsValid(typid) ? get_typ_name(typid) : NULL))

/* write an oid which is a foreign server OID*/
#define WRITE_SERVERID_INTERNAL(serverid) \
	(outToken(str, OidIsValid((serverid)) ? get_foreign_server_name((serverid)) : NULL))

/* write an OID which is a relation OID */
#define WRITE_SERVERID_FIELD(fldname) \
	(appendStringInfo(str, " :" CppAsString(fldname) " "), \
	 WRITE_SERVERID_INTERNAL(node->fldname))

/* write an OID which is a data type OID */
#define WRITE_TYPID_FIELD(fldname) \
	(appendStringInfo(str, " :" CppAsString(fldname) " "), \
	 WRITE_TYPID_INTERNAL(node->fldname))

#define WRITE_TYPID_LIST_FIELD(fldname) \
	do { \
		ListCell *lc; \
		char *sep = ""; \
		appendStringInfo(str, " :" CppAsString(fldname) " "); \
		if (node->fldname == NIL || list_length(node->fldname) == 0) \
			appendStringInfoString(str, "<>"); \
		else \
		{ \
			appendStringInfoChar(str, '('); \
			foreach (lc, node->fldname) \
			{ \
				Oid typid = lfirst_oid(lc); \
				appendStringInfoString(str, sep); \
				WRITE_TYPID_INTERNAL(typid); \
				sep = " , "; \
			} \
			appendStringInfoString(str, " )"); \
		} \
	}  while (0)

/* write an OID which is a function OID */
#define WRITE_FUNCID_FIELD(fldname) \
	do { \
		appendStringInfo(str, " :" CppAsString(fldname) " "); \
		if (OidIsValid(node->fldname)) \
		{ \
			Oid *argtypes; \
			int i, nargs; \
			outToken(str, NSP_NAME(get_func_namespace(node->fldname))); \
			appendStringInfoChar(str, ' '); \
			outToken(str, get_func_name(node->fldname)); \
			appendStringInfoChar(str, ' '); \
			get_func_signature(node->fldname, &argtypes, &nargs); \
			appendStringInfo(str, "%d", nargs); \
			for (i = 0; i < nargs; i++) \
			{ \
				appendStringInfoChar(str, ' '); \
				outToken(str, NSP_NAME(get_typ_namespace(argtypes[i]))); \
				appendStringInfoChar(str, ' '); \
				outToken(str, get_typ_name(argtypes[i])); \
			} \
		} \
		else \
			appendStringInfo(str, "<> <> 0"); \
	} while (0)

/* write an OID which is an operator OID */
#define WRITE_OPERID_FIELD(fldname) \
	do { \
		appendStringInfo(str, " :" CppAsString(fldname) " "); \
		if (OidIsValid(node->fldname)) \
		{ \
			Oid oprleft, oprright; \
			outToken(str, NSP_NAME(get_opnamespace(node->fldname))); \
			appendStringInfoChar(str, ' '); \
			outToken(str, get_opname(node->fldname)); \
			appendStringInfoChar(str, ' '); \
			op_input_types(node->fldname, &oprleft, &oprright); \
			outToken(str, OidIsValid(oprleft) ? \
					NSP_NAME(get_typ_namespace(oprleft)) : NULL); \
			appendStringInfoChar(str, ' '); \
			outToken(str, OidIsValid(oprleft) ? get_typ_name(oprleft) : NULL); \
			appendStringInfoChar(str, ' '); \
			outToken(str, OidIsValid(oprright) ? \
					NSP_NAME(get_typ_namespace(oprright)) : NULL); \
			appendStringInfoChar(str, ' '); \
			outToken(str, OidIsValid(oprright) ? get_typ_name(oprright) : NULL); \
			appendStringInfoChar(str, ' '); \
		} \
		else \
			appendStringInfo(str, "<> <> <> <> <> <>"); \
	} while (0)

/* write an OID which is a collation OID */
#define WRITE_COLLID_FIELD(fldname) \
	do { \
		appendStringInfo(str, " :" CppAsString(fldname) " "); \
		if (OidIsValid(node->fldname)) \
		{ \
			outToken(str, NSP_NAME(get_collation_namespace(node->fldname))); \
			appendStringInfoChar(str, ' '); \
			outToken(str, get_collation_name(node->fldname)); \
			appendStringInfo(str, " %d", get_collation_encoding(node->fldname)); \
		} \
		else \
			appendStringInfo(str, "<> <> -1"); \
	} while (0)


#define WRITE_OPCLASS_FIELD(fldname) \
	do { \
		appendStringInfo(str, " :" CppAsString(fldname) " "); \
		if (OidIsValid(node->fldname)) \
		{ \
			char *name; \
			char *nspace; \
			get_opclass_name_namespace(node->fldname, &name, &nspace); \
			outToken(str, nspace); \
			appendStringInfoChar(str, ' '); \
			outToken(str, name); \
		} \
		else \
			appendStringInfo(str, "<> <>"); \
	} while (0)

#define WRITE_CONSTRAINT_FIELD(fldname) \
	do { \
		appendStringInfo(str, " :" CppAsString(fldname) " "); \
		if (OidIsValid(node->fldname)) \
		{ \
			char *name; \
			Oid  relid; \
			name = get_constraint_name_relid(node->fldname, &relid); \
			outToken(str, name); \
			appendStringInfoChar(str, ' '); \
			outToken(str, NSP_NAME(get_rel_namespace((relid)))); \
			appendStringInfoChar(str, ' '); \
			outToken(str, get_rel_name((relid))); \
		} \
		else \
			appendStringInfo(str, "<> <> <>"); \
	} while (0)
#endif

#define booltostr(x)  ((x) ? "true" : "false")


/*
 * outToken
 *	  Convert an ordinary string (eg, an identifier) into a form that
 *	  will be decoded back to a plain token by read.c's functions.
 *
 *	  If a null or empty string is given, it is encoded as "<>".
 */
void
outToken(StringInfo str, const char *s)
{
	if (s == NULL || *s == '\0')
	{
		appendStringInfoString(str, "<>");
		return;
	}

	/*
	 * Look for characters or patterns that are treated specially by read.c
	 * (either in pg_strtok() or in nodeRead()), and therefore need a
	 * protective backslash.
	 */
	/* These characters only need to be quoted at the start of the string */
	if (*s == '<' ||
		*s == '"' ||
		isdigit((unsigned char) *s) ||
		((*s == '+' || *s == '-') &&
		 (isdigit((unsigned char) s[1]) || s[1] == '.')))
		appendStringInfoChar(str, '\\');
	while (*s)
	{
		/* These chars must be backslashed anywhere in the string */
		if (*s == ' ' || *s == '\n' || *s == '\t' ||
			*s == '(' || *s == ')' || *s == '{' || *s == '}' ||
			*s == '\\')
			appendStringInfoChar(str, '\\');
		appendStringInfoChar(str, *s++);
	}
}

/*
 * Convert one char.  Goes through outToken() so that special characters are
 * escaped.
 */
static void
outChar(StringInfo str, char c)
{
	char		in[2];

	in[0] = c;
	in[1] = '\0';

	outToken(str, in);
}

static void
_outList(StringInfo str, const List *node)
{
	const ListCell *lc;

	appendStringInfoChar(str, '(');

	if (IsA(node, IntList))
		appendStringInfoChar(str, 'i');
	else if (IsA(node, OidList))
		appendStringInfoChar(str, 'o');

	foreach(lc, node)
	{
		/*
		 * For the sake of backward compatibility, we emit a slightly
		 * different whitespace format for lists of nodes vs. other types of
		 * lists. XXX: is this necessary?
		 */
		if (IsA(node, List))
		{
			outNode(str, lfirst(lc));
			if (lnext(lc))
				appendStringInfoChar(str, ' ');
		}
		else if (IsA(node, IntList))
			appendStringInfo(str, " %d", lfirst_int(lc));
		else if (IsA(node, OidList))
			appendStringInfo(str, " %u", lfirst_oid(lc));
		else
			elog(ERROR, "unrecognized list node type: %d",
				 (int) node->type);
	}

	appendStringInfoChar(str, ')');
}

/*
 * outBitmapset -
 *	   converts a bitmap set of integers
 *
 * Note: the output format is "(b int int ...)", similar to an integer List.
 */
void
outBitmapset(StringInfo str, const Bitmapset *bms)
{
	int			x;

	appendStringInfoChar(str, '(');
	appendStringInfoChar(str, 'b');
	x = -1;
	while ((x = bms_next_member(bms, x)) >= 0)
		appendStringInfo(str, " %d", x);
	appendStringInfoChar(str, ')');
}

/*
 * Print the value of a Datum given its type.
 */
void
outDatum(StringInfo str, Datum value, int typlen, bool typbyval)
{
	Size		length,
				i;
	char	   *s;

	length = datumGetSize(value, typbyval, typlen);

	if (typbyval)
	{
		s = (char *) (&value);
		appendStringInfo(str, "%u [ ", (unsigned int) length);
		for (i = 0; i < (Size) sizeof(Datum); i++)
			appendStringInfo(str, "%d ", (int) (s[i]));
		appendStringInfoChar(str, ']');
	}
	else
	{
		s = (char *) DatumGetPointer(value);
		if (!PointerIsValid(s))
			appendStringInfoString(str, "0 [ ]");
		else
		{
			appendStringInfo(str, "%u [ ", (unsigned int) length);
			for (i = 0; i < length; i++)
				appendStringInfo(str, "%d ", (int) (s[i]));
			appendStringInfoChar(str, ']');
		}
	}
}


#ifdef XCP
/*
 * Output value in text format
 */
static void
_printDatum(StringInfo str, Datum value, Oid typid)
{
	Oid 		typOutput;
	bool 		typIsVarlena;
	FmgrInfo    finfo;
	Datum		tmpval;
	char	   *textvalue;
	int			saveDateStyle;

	/* Get output function for the type */
	getTypeOutputInfo(typid, &typOutput, &typIsVarlena);
	fmgr_info(typOutput, &finfo);

	/* Detoast value if needed */
	if (typIsVarlena)
		tmpval = PointerGetDatum(PG_DETOAST_DATUM(value));
	else
		tmpval = value;

	/*
	 * It was found that if configuration setting for date style is
	 * "postgres,ymd" the output dates have format DD-MM-YYYY and they can not
	 * be parsed correctly by receiving party. So force ISO format YYYY-MM-DD
	 * in internal cluster communications, these values are always parsed
	 * correctly.
	 */
	saveDateStyle = DateStyle;
	DateStyle = USE_ISO_DATES;

	textvalue = DatumGetCString(FunctionCall1(&finfo, tmpval));
	outToken(str, textvalue);

	DateStyle = saveDateStyle;
}
#endif


/*
 *	Stuff from plannodes.h
 */

static void
_outPlannedStmt(StringInfo str, const PlannedStmt *node)
{
	WRITE_NODE_TYPE("PLANNEDSTMT");

	WRITE_ENUM_FIELD(commandType, CmdType);
	WRITE_UINT_FIELD(queryId);
	WRITE_BOOL_FIELD(hasReturning);
	WRITE_BOOL_FIELD(hasModifyingCTE);
	WRITE_BOOL_FIELD(canSetTag);
	WRITE_BOOL_FIELD(transientPlan);
	WRITE_BOOL_FIELD(dependsOnRole);
	WRITE_BOOL_FIELD(parallelModeNeeded);
    WRITE_BOOL_FIELD(canNextvalParallel);
	WRITE_BOOL_FIELD(remote);
	WRITE_INT_FIELD(jitFlags);
	WRITE_NODE_FIELD(planTree);
	WRITE_NODE_FIELD(rtable);
	WRITE_NODE_FIELD(resultRelations);
	WRITE_NODE_FIELD(nonleafResultRelations);
	WRITE_NODE_FIELD(rootResultRelations);
	WRITE_NODE_FIELD(subplans);
	WRITE_BITMAPSET_FIELD(rewindPlanIDs);
	WRITE_NODE_FIELD(rowMarks);
	WRITE_NODE_FIELD(relationOids);
	WRITE_NODE_FIELD(invalItems);
	WRITE_NODE_FIELD(paramExecTypes);
	WRITE_NODE_FIELD(utilityStmt);
	WRITE_LOCATION_FIELD(stmt_location);
	WRITE_LOCATION_FIELD(stmt_len);

#ifdef __AUDIT__
	WRITE_STRING_FIELD(queryString);
	WRITE_NODE_FIELD(parseTree);
#endif

#ifdef __OPENTENBASE_C__
	WRITE_NODE_FIELD(sharedCtePlans);
	WRITE_BITMAPSET_FIELD(sharedCtePlanIds);
	WRITE_INT_FIELD(planned_query_mem);
	WRITE_INT_FIELD(planned_work_mem);
	WRITE_INT_FIELD(fragmentNum);
#endif

	/* begin ora_compatible */
	WRITE_NODE_FIELD(multi_dist);
	WRITE_INT_FIELD(p_startno);
	WRITE_INT_FIELD(p_endno);
	WRITE_BOOL_FIELD(p_has_else);
	WRITE_BOOL_FIELD(p_has_val_func);
	/* end ora_compatible */
	WRITE_LONG_FIELD(queryid.timestamp_nodeid);
	WRITE_LONG_FIELD(queryid.sequence);
}

/*
 * print the basic stuff of all nodes that inherit from Plan
 */
static void
_outPlanInfo(StringInfo str, const Plan *node)
{
	WRITE_FLOAT_FIELD(startup_cost, "%.2f");
	WRITE_FLOAT_FIELD(total_cost, "%.2f");
	WRITE_FLOAT_FIELD(plan_rows, "%.0f");
	WRITE_INT_FIELD(plan_width);
	WRITE_BOOL_FIELD(parallel_aware);
	WRITE_BOOL_FIELD(parallel_safe);
	WRITE_INT_FIELD(plan_node_id);
	WRITE_NODE_FIELD(targetlist);
	WRITE_NODE_FIELD(qual);
	WRITE_NODE_FIELD(lefttree);
	WRITE_NODE_FIELD(righttree);
	WRITE_NODE_FIELD(initPlan);
	WRITE_NODE_FIELD(exec_subplan);
	WRITE_BITMAPSET_FIELD(extParam);
	WRITE_BITMAPSET_FIELD(allParam);
#ifdef __OPENTENBASE_C__
	WRITE_INT_FIELD(parallel_num);
	WRITE_BOOL_FIELD(is_subplan);
	WRITE_INT_FIELD(operator_mem);
#endif
#ifdef __OPENTENBASE__
	WRITE_INT_FIELD(remote_flag);
#endif
#ifdef __AUDIT_FGA__
	WRITE_NODE_FIELD(audit_fga_quals);
#endif
}

/*
 * print the basic stuff of all nodes that inherit from Scan
 */
static void
_outScanInfo(StringInfo str, const Scan *node)
{
	_outPlanInfo(str, (const Plan *) node);

	WRITE_UINT_FIELD(scanrelid);
	WRITE_INT_FIELD(leafNum);
	WRITE_ENUM_FIELD(partScanDirection, ScanDirection);

#ifdef XCP
	if (portable_output)
		WRITE_RELID_LIST_FIELD(partition_leaf_rels);
	else
	{
#endif
		WRITE_NODE_FIELD(partition_leaf_rels);
#ifdef XCP
	}
#endif
	WRITE_NODE_FIELD(partition_leaf_rels_idx);
	WRITE_INT_FIELD(partIterParamno);
	WRITE_BOOL_FIELD(isPartTbl);
}

/*
 * print the basic stuff of all nodes that inherit from Join
 */
static void
_outJoinPlanInfo(StringInfo str, const Join *node)
{
	_outPlanInfo(str, (const Plan *) node);

	WRITE_ENUM_FIELD(jointype, JoinType);
	WRITE_BOOL_FIELD(inner_unique);
	WRITE_NODE_FIELD(joinqual);
	WRITE_INT_FIELD(plannerinfo_id);
}


static void
_outPlan(StringInfo str, const Plan *node)
{
	WRITE_NODE_TYPE("PLAN");

	_outPlanInfo(str, (const Plan *) node);
}

static void
_outResult(StringInfo str, const Result *node)
{
	WRITE_NODE_TYPE("RESULT");

	_outPlanInfo(str, (const Plan *) node);

	WRITE_NODE_FIELD(resconstantqual);
	WRITE_BOOL_FIELD(fail_return);
}

static void
_outProjectSet(StringInfo str, const ProjectSet *node)
{
	WRITE_NODE_TYPE("PROJECTSET");

	_outPlanInfo(str, (const Plan *) node);
}

static void
_outModifyTable(StringInfo str, const ModifyTable *node)
{
	WRITE_NODE_TYPE("MODIFYTABLE");

	_outPlanInfo(str, (const Plan *) node);

	WRITE_ENUM_FIELD(operation, CmdType);
	WRITE_BOOL_FIELD(canSetTag);
	WRITE_UINT_FIELD(nominalRelation);
	WRITE_NODE_FIELD(partitioned_rels);
	WRITE_BOOL_FIELD(partColsUpdated);
	WRITE_NODE_FIELD(resultRelations);
	WRITE_INT_FIELD(resultRelIndex);
#ifdef _PG_ORCL_
	WRITE_INT_FIELD(rid_col_idx);
#endif
	WRITE_INT_FIELD(rootResultRelIndex);
	WRITE_NODE_FIELD(updateColnosLists);
	WRITE_NODE_FIELD(withCheckOptionLists);
	WRITE_NODE_FIELD(returningLists);
	WRITE_NODE_FIELD(fdwPrivLists);
	WRITE_BITMAPSET_FIELD(fdwDirectModifyPlans);
	WRITE_NODE_FIELD(rowMarks);
	WRITE_INT_FIELD(epqParam);
	WRITE_ENUM_FIELD(onConflictAction, OnConflictAction);
#ifdef XCP	
	if (portable_output)
		WRITE_RELID_LIST_FIELD(arbiterIndexes);
	else
	{
#endif
	WRITE_NODE_FIELD(arbiterIndexes);
#ifdef XCP
	}
#endif
	WRITE_NODE_FIELD(onConflictSet);
	WRITE_NODE_FIELD(onConflictWhere);
	WRITE_UINT_FIELD(exclRelRTI);
	WRITE_NODE_FIELD(exclRelTlist);
#ifdef __OPENTENBASE__
	WRITE_NODE_FIELD(remote_plans);
    WRITE_INT_FIELD(mergeTargetRelation);
    WRITE_NODE_FIELD(mergeActionLists);
	WRITE_BOOL_FIELD(global_index);
	WRITE_BOOL_FIELD(update_gindex_key);
#endif
	WRITE_BOOL_FIELD(is_submod);	/* ora_compatible */
}

static void
_outPartIterator(StringInfo str, const PartIterator *node)
{
	WRITE_NODE_TYPE("PARTITERATOR");

	_outPlanInfo(str, (const Plan *) node);

	WRITE_INT_FIELD(leafNum);
	WRITE_ENUM_FIELD(direction, ScanDirection);
	WRITE_INT_FIELD(partParamno);
	WRITE_NODE_FIELD(part_prune_info);
}

static void
_outAppend(StringInfo str, Append *node)
{
	WRITE_NODE_TYPE("APPEND");

	_outPlanInfo(str, (const Plan *) node);

	WRITE_NODE_FIELD(appendplans);
	WRITE_INT_FIELD(first_partial_plan);
	WRITE_NODE_FIELD(partitioned_rels);
	WRITE_NODE_FIELD(part_prune_info);
}

static void
_outMergeAppend(StringInfo str, MergeAppend *node)
{
	int			i;
	WRITE_NODE_TYPE("MERGEAPPEND");

	_outPlanInfo(str, (const Plan *) node);

	WRITE_NODE_FIELD(partitioned_rels);

	WRITE_NODE_FIELD(mergeplans);

	WRITE_INT_FIELD(numCols);
	WRITE_ATTRNUMBER_ARRAY(sortColIdx, node->numCols);

	appendStringInfoString(str, " :sortOperators");
	for (i = 0; i < node->numCols; i++)
#ifdef XCP
		if (portable_output)
		{
			Oid oper = node->sortOperators[i];
			Oid oprleft, oprright;
			/* Sort operator is always valid */
			Assert(OidIsValid(oper));
			appendStringInfoChar(str, ' ');
			outToken(str, NSP_NAME(get_opnamespace(oper)));
			appendStringInfoChar(str, ' ');
			outToken(str, get_opname(oper));
			appendStringInfoChar(str, ' ');
			op_input_types(oper, &oprleft, &oprright);
			outToken(str, OidIsValid(oprleft) ?
					NSP_NAME(get_typ_namespace(oprleft)) : NULL);
			appendStringInfoChar(str, ' ');
			outToken(str, OidIsValid(oprleft) ? get_typ_name(oprleft) : NULL);
			appendStringInfoChar(str, ' ');
			outToken(str, OidIsValid(oprright) ?
					NSP_NAME(get_typ_namespace(oprright)) : NULL);
			appendStringInfoChar(str, ' ');
			outToken(str, OidIsValid(oprright) ? get_typ_name(oprright) : NULL);
		}
		else
#endif
		appendStringInfo(str, " %u", node->sortOperators[i]);

	appendStringInfoString(str, " :collations");
	for (i = 0; i < node->numCols; i++)
#ifdef XCP
		if (portable_output)
		{
			Oid coll = node->collations[i];
			if (OidIsValid(coll))
			{
				appendStringInfoChar(str, ' ');
				outToken(str, NSP_NAME(get_collation_namespace(coll)));
				appendStringInfoChar(str, ' ');
				outToken(str, get_collation_name(coll));
				appendStringInfo(str, " %d", get_collation_encoding(coll));
			}
			else
				appendStringInfo(str, " <> <> -1");
		}
		else
#endif
		appendStringInfo(str, " %u", node->collations[i]);

	WRITE_BOOL_ARRAY(nullsFirst, node->numCols);
}

static void
_outRecursiveUnion(StringInfo str, const RecursiveUnion *node)
{
	int			i;
	WRITE_NODE_TYPE("RECURSIVEUNION");

	_outPlanInfo(str, (const Plan *) node);

	WRITE_INT_FIELD(wtParam);
	WRITE_INT_FIELD(numCols);
	WRITE_ATTRNUMBER_ARRAY(dupColIdx, node->numCols);

	appendStringInfoString(str, " :dupOperators");
	for (i = 0; i < node->numCols; i++)
#ifdef XCP
		if (portable_output)
		{
			Oid oper = node->dupOperators[i];
			Oid oprleft, oprright;
			/* Unique operator is always valid */
			Assert(OidIsValid(oper));
			appendStringInfoChar(str, ' ');
			outToken(str, NSP_NAME(get_opnamespace(oper)));
			appendStringInfoChar(str, ' ');
			outToken(str, get_opname(oper));
			appendStringInfoChar(str, ' ');
			op_input_types(oper, &oprleft, &oprright);
			outToken(str, OidIsValid(oprleft) ?
					NSP_NAME(get_typ_namespace(oprleft)) : NULL);
			appendStringInfoChar(str, ' ');
			outToken(str, OidIsValid(oprleft) ? get_typ_name(oprleft) : NULL);
			appendStringInfoChar(str, ' ');
			outToken(str, OidIsValid(oprright) ?
					NSP_NAME(get_typ_namespace(oprright)) : NULL);
			appendStringInfoChar(str, ' ');
			outToken(str, OidIsValid(oprright) ? get_typ_name(oprright) : NULL);
			appendStringInfoChar(str, ' ');
		}
		else
#endif
		appendStringInfo(str, " %u", node->dupOperators[i]);

	WRITE_LONG_FIELD(numGroups);
}

static void
_outBitmapAnd(StringInfo str, const BitmapAnd *node)
{
	WRITE_NODE_TYPE("BITMAPAND");

	_outPlanInfo(str, (const Plan *) node);

	WRITE_NODE_FIELD(bitmapplans);
	WRITE_BOOL_FIELD(isPartTbl);
}

static void
_outBitmapOr(StringInfo str, const BitmapOr *node)
{
	WRITE_NODE_TYPE("BITMAPOR");

	_outPlanInfo(str, (const Plan *) node);

	WRITE_BOOL_FIELD(isshared);
	WRITE_NODE_FIELD(bitmapplans);
	WRITE_BOOL_FIELD(isPartTbl);
}

static void
_outGather(StringInfo str, const Gather *node)
{
	WRITE_NODE_TYPE("GATHER");

	_outPlanInfo(str, (const Plan *) node);

	WRITE_INT_FIELD(num_workers);
	WRITE_INT_FIELD(rescan_param);
	WRITE_BOOL_FIELD(single_copy);
	WRITE_BOOL_FIELD(invisible);
	WRITE_BITMAPSET_FIELD(initParam);
}

static void
_outGatherMerge(StringInfo str, const GatherMerge *node)
{
	WRITE_NODE_TYPE("GATHERMERGE");

	_outPlanInfo(str, (const Plan *) node);

	WRITE_INT_FIELD(num_workers);
	WRITE_INT_FIELD(rescan_param);
	WRITE_INT_FIELD(numCols);
	WRITE_ATTRNUMBER_ARRAY(sortColIdx, node->numCols);
	WRITE_OID_ARRAY(sortOperators, node->numCols);
	WRITE_OID_ARRAY(collations, node->numCols);
	WRITE_BOOL_ARRAY(nullsFirst, node->numCols);
	WRITE_BITMAPSET_FIELD(initParam);
}

static void
_outScan(StringInfo str, const Scan *node)
{
	WRITE_NODE_TYPE("SCAN");

	_outScanInfo(str, node);
}

static void
_outSeqScan(StringInfo str, const SeqScan *node)
{
	WRITE_NODE_TYPE("SEQSCAN");

	_outScanInfo(str, (const Scan *) node);
}

static void
_outExternalScanInfo(StringInfo str, const ExternalScanInfo *node)
{
	WRITE_NODE_TYPE("EXTERNALSCANINFO");

	WRITE_NODE_FIELD(uriList);
	WRITE_CHAR_FIELD(fmtType);
	WRITE_BOOL_FIELD(isMasterOnly);
	WRITE_INT_FIELD(rejLimit);
	WRITE_BOOL_FIELD(rejLimitInRows);
	WRITE_CHAR_FIELD(logErrors);
	WRITE_INT_FIELD(encoding);
	WRITE_INT_FIELD(scancounter);
	WRITE_NODE_FIELD(extOptions);
}

static void
_outSampleScan(StringInfo str, const SampleScan *node)
{
	WRITE_NODE_TYPE("SAMPLESCAN");

	_outScanInfo(str, (const Scan *) node);

	WRITE_NODE_FIELD(tablesample);
}

static void
_outIndexScan(StringInfo str, const IndexScan *node)
{
	WRITE_NODE_TYPE("INDEXSCAN");

	_outScanInfo(str, (const Scan *) node);

#ifdef XCP
	if (portable_output)
		WRITE_RELID_FIELD(indexid);
	else
#endif
	WRITE_OID_FIELD(indexid);
	WRITE_NODE_FIELD(indexqual);
	WRITE_NODE_FIELD(indexqualorig);
	WRITE_NODE_FIELD(indexorderby);
	WRITE_NODE_FIELD(indexorderbyorig);
	WRITE_NODE_FIELD(indexorderbyops);
	WRITE_ENUM_FIELD(indexorderdir, ScanDirection);
}

#ifdef PGXC
static void
_outRemoteQuery(StringInfo str, const RemoteQuery *node)
{
	int			i;

	WRITE_NODE_TYPE("REMOTEQUERY");

	_outScanInfo(str, (Scan *) node);

	WRITE_ENUM_FIELD(exec_direct_type, ExecDirectType);
    WRITE_BOOL_FIELD(exec_direct_dn_allow);
	WRITE_STRING_FIELD(sql_statement);
	WRITE_NODE_FIELD(exec_nodes);
	WRITE_ENUM_FIELD(combine_type, CombineType);
	WRITE_NODE_FIELD(sort);
	WRITE_BOOL_FIELD(read_only);
	WRITE_BOOL_FIELD(force_autocommit);
	WRITE_STRING_FIELD(statement);
	WRITE_STRING_FIELD(cursor);
	WRITE_INT_FIELD(rq_num_params);

	if (node->rq_num_params > 0)
	{
		appendStringInfo(str, " :rq_param_types");
		for (i = 0; i < node->rq_num_params; i++)
			appendStringInfo(str, " %d", node->rq_param_types[i]);
	}

	WRITE_ENUM_FIELD(exec_type, RemoteQueryExecType);
	WRITE_INT_FIELD(reduce_level);
	WRITE_STRING_FIELD(outer_alias);
	WRITE_STRING_FIELD(inner_alias);
	WRITE_INT_FIELD(outer_reduce_level);
	WRITE_INT_FIELD(inner_reduce_level);
	WRITE_BITMAPSET_FIELD(outer_relids);
	WRITE_BITMAPSET_FIELD(inner_relids);
	WRITE_STRING_FIELD(inner_statement);
	WRITE_STRING_FIELD(outer_statement);
	WRITE_STRING_FIELD(join_condition);
	WRITE_BOOL_FIELD(has_row_marks);
	WRITE_BOOL_FIELD(has_ins_child_sel_parent);
	
	WRITE_BOOL_FIELD(rq_finalise_aggs);
	WRITE_BOOL_FIELD(rq_sortgroup_colno);
	WRITE_NODE_FIELD(remote_query);
	WRITE_NODE_FIELD(base_tlist);
	WRITE_NODE_FIELD(coord_var_tlist);
	WRITE_NODE_FIELD(query_var_tlist);
	WRITE_BOOL_FIELD(is_temp);
	WRITE_ENUM_FIELD(rcmdtype, RCmdType);
}

static void
_outExecNodes(StringInfo str, const ExecNodes *node)
{
	WRITE_NODE_TYPE("EXEC_NODES");

	WRITE_NODE_FIELD(primarynodelist);
	WRITE_NODE_FIELD(nodeList);
	WRITE_CHAR_FIELD(baselocatortype);
#ifdef __OPENTENBASE__
	if (portable_output)
	{
		WRITE_RELID_FIELD(en_relid);
	}
	else
#endif
	WRITE_OID_FIELD(en_relid);
	WRITE_STRING_FIELD(g_index_table_name);
	WRITE_ENUM_FIELD(accesstype, RelationAccessType);
}
#endif

#ifdef __AUDIT_FGA__
static void
_outAuditFgaStmt(StringInfo str, const AuditFgaPolicy *node)
    {
        WRITE_NODE_TYPE("AUDITFGAPOLICY");
    
        WRITE_STRING_FIELD(policy_name);
        WRITE_NODE_FIELD(qual);
        WRITE_STRING_FIELD(query_string);
    }

#endif

static void
_outIndexOnlyScan(StringInfo str, const IndexOnlyScan *node)
{
	WRITE_NODE_TYPE("INDEXONLYSCAN");

	_outScanInfo(str, (const Scan *) node);

#ifdef XCP
	if (portable_output)
		WRITE_RELID_FIELD(indexid);
	else
#endif
	WRITE_OID_FIELD(indexid);
	WRITE_NODE_FIELD(indexqual);
	WRITE_NODE_FIELD(indexorderby);
	WRITE_NODE_FIELD(indextlist);
	WRITE_ENUM_FIELD(indexorderdir, ScanDirection);
}

static void
_outBitmapIndexScan(StringInfo str, const BitmapIndexScan *node)
{
	WRITE_NODE_TYPE("BITMAPINDEXSCAN");

	_outScanInfo(str, (const Scan *) node);

#ifdef XCP
	if (portable_output)
		WRITE_RELID_FIELD(indexid);
	else
#endif
	WRITE_OID_FIELD(indexid);
	WRITE_BOOL_FIELD(isshared);
	WRITE_NODE_FIELD(indexqual);
	WRITE_NODE_FIELD(indexqualorig);
}

static void
_outBitmapHeapScan(StringInfo str, const BitmapHeapScan *node)
{
	WRITE_NODE_TYPE("BITMAPHEAPSCAN");

	_outScanInfo(str, (const Scan *) node);

	WRITE_NODE_FIELD(bitmapqualorig);
}

static void
_outTidScan(StringInfo str, const TidScan *node)
{
	WRITE_NODE_TYPE("TIDSCAN");

	_outScanInfo(str, (const Scan *) node);

	WRITE_NODE_FIELD(tidquals);
	WRITE_BOOL_FIELD(remote);
	WRITE_NODE_FIELD(remote_targets);
	WRITE_STRING_FIELD(remote_sql);
	WRITE_NODE_FIELD(remote_indexqual);
	WRITE_NODE_FIELD(remote_qual);
}

static void
_outSubqueryScan(StringInfo str, const SubqueryScan *node)
{
	WRITE_NODE_TYPE("SUBQUERYSCAN");

	_outScanInfo(str, (const Scan *) node);

	WRITE_NODE_FIELD(subplan);
}

static void
_outFunctionScan(StringInfo str, const FunctionScan *node)
{
	WRITE_NODE_TYPE("FUNCTIONSCAN");

	_outScanInfo(str, (const Scan *) node);

	WRITE_NODE_FIELD(functions);
	WRITE_BOOL_FIELD(funcordinality);
}

static void
_outTableFuncScan(StringInfo str, const TableFuncScan *node)
{
	WRITE_NODE_TYPE("TABLEFUNCSCAN");

	_outScanInfo(str, (const Scan *) node);

	WRITE_NODE_FIELD(tablefunc);
}

static void
_outValuesScan(StringInfo str, const ValuesScan *node)
{
	WRITE_NODE_TYPE("VALUESSCAN");

	_outScanInfo(str, (const Scan *) node);

	WRITE_NODE_FIELD(values_lists);
}

static void
_outCteScan(StringInfo str, const CteScan *node)
{
	WRITE_NODE_TYPE("CTESCAN");

	_outScanInfo(str, (const Scan *) node);

	WRITE_INT_FIELD(ctePlanId);
	WRITE_INT_FIELD(cteParam);
}

static void
_outNamedTuplestoreScan(StringInfo str, const NamedTuplestoreScan *node)
{
	WRITE_NODE_TYPE("NAMEDTUPLESTORESCAN");

	_outScanInfo(str, (const Scan *) node);

	WRITE_STRING_FIELD(enrname);
}

static void
_outWorkTableScan(StringInfo str, const WorkTableScan *node)
{
	WRITE_NODE_TYPE("WORKTABLESCAN");

	_outScanInfo(str, (const Scan *) node);

	WRITE_INT_FIELD(wtParam);
}

static void
_outForeignScan(StringInfo str, const ForeignScan *node)
{
	WRITE_NODE_TYPE("FOREIGNSCAN");

	_outScanInfo(str, (const Scan *) node);

	WRITE_ENUM_FIELD(operation, CmdType);
#ifdef XCP
	if (portable_output)
		WRITE_SERVERID_FIELD(fs_server);
	else
#endif
		WRITE_OID_FIELD(fs_server);
	WRITE_NODE_FIELD(fdw_exprs);
	WRITE_NODE_FIELD(fdw_private);
	WRITE_NODE_FIELD(fdw_scan_tlist);
	WRITE_NODE_FIELD(fdw_recheck_quals);
	WRITE_BITMAPSET_FIELD(fs_relids);
	WRITE_BOOL_FIELD(fsSystemCol);
}

static void
_outCustomScan(StringInfo str, const CustomScan *node)
{
	WRITE_NODE_TYPE("CUSTOMSCAN");

	_outScanInfo(str, (const Scan *) node);

	WRITE_UINT_FIELD(flags);
	WRITE_NODE_FIELD(custom_plans);
	WRITE_NODE_FIELD(custom_exprs);
	WRITE_NODE_FIELD(custom_private);
	WRITE_NODE_FIELD(custom_scan_tlist);
	WRITE_BITMAPSET_FIELD(custom_relids);
	/* CustomName is a key to lookup CustomScanMethods */
	appendStringInfoString(str, " :methods ");
	outToken(str, node->methods->CustomName);
}

static void
_outJoin(StringInfo str, const Join *node)
{
	WRITE_NODE_TYPE("JOIN");

	_outJoinPlanInfo(str, (const Join *) node);
}

static void
_outNestLoop(StringInfo str, const NestLoop *node)
{
	WRITE_NODE_TYPE("NESTLOOP");

	_outJoinPlanInfo(str, (const Join *) node);

	WRITE_NODE_FIELD(nestParams);
}

static void
_outMergeJoin(StringInfo str, const MergeJoin *node)
{
	int			numCols;
	int			i;

	WRITE_NODE_TYPE("MERGEJOIN");

	_outJoinPlanInfo(str, (const Join *) node);

	WRITE_BOOL_FIELD(skip_mark_restore);
	WRITE_NODE_FIELD(mergeclauses);

	numCols = list_length(node->mergeclauses);

	WRITE_OID_ARRAY(mergeFamilies, numCols);

	appendStringInfoString(str, " :mergeCollations");
	for (i = 0; i < numCols; i++)
#ifdef XCP
		if (portable_output)
		{
			Oid coll = node->mergeCollations[i];
			if (OidIsValid(coll))
			{
				appendStringInfoChar(str, ' ');
				outToken(str, NSP_NAME(get_collation_namespace(coll)));
				appendStringInfoChar(str, ' ');
				outToken(str, get_collation_name(coll));
				appendStringInfo(str, " %d", get_collation_encoding(coll));
			}
			else
				appendStringInfo(str, " <> <> -1");
		}
		else
#endif
		appendStringInfo(str, " %u", node->mergeCollations[i]);

	WRITE_INT_ARRAY(mergeStrategies, numCols);
	WRITE_BOOL_ARRAY(mergeNullsFirst, numCols);
	WRITE_BOOL_FIELD(is_not_full);
}

static void
_outHashJoin(StringInfo str, const HashJoin *node)
{
	WRITE_NODE_TYPE("HASHJOIN");

	_outJoinPlanInfo(str, (const Join *) node);

	WRITE_NODE_FIELD(hashclauses);
	WRITE_NODE_FIELD(hashoperators);
	WRITE_NODE_FIELD(hashcollations);
	WRITE_NODE_FIELD(hashkeys);
	WRITE_BOOL_FIELD(nonequijoin);

	WRITE_INT_FIELD(num_batches);

	WRITE_INT_FIELD(num_buckets);

	WRITE_FLOAT_FIELD(innerbucketsize, "%.2f");

	WRITE_FLOAT_FIELD(hashjointuples, "%.2f");
}

static void
_outAgg(StringInfo str, const Agg *node)
{
	int			i;

	WRITE_NODE_TYPE("AGG");

	_outPlanInfo(str, (const Plan *) node);

	WRITE_ENUM_FIELD(aggstrategy, AggStrategy);
	WRITE_ENUM_FIELD(aggsplit, AggSplit);
	WRITE_INT_FIELD(numCols);

	WRITE_ATTRNUMBER_ARRAY(grpColIdx, node->numCols);

	appendStringInfoString(str, " :grpOperators");
	for (i = 0; i < node->numCols; i++)
#ifdef XCP
		if (portable_output)
		{
			Oid oper = node->grpOperators[i];
			Oid oprleft, oprright;
			/* Group operator is always valid */
			Assert(OidIsValid(oper));
			appendStringInfoChar(str, ' ');
			outToken(str, NSP_NAME(get_opnamespace(oper)));
			appendStringInfoChar(str, ' ');
			outToken(str, get_opname(oper));
			appendStringInfoChar(str, ' ');
			op_input_types(oper, &oprleft, &oprright);
			outToken(str, OidIsValid(oprleft) ?
					NSP_NAME(get_typ_namespace(oprleft)) : NULL);
			appendStringInfoChar(str, ' ');
			outToken(str, OidIsValid(oprleft) ? get_typ_name(oprleft) : NULL);
			appendStringInfoChar(str, ' ');
			outToken(str, OidIsValid(oprright) ?
					NSP_NAME(get_typ_namespace(oprright)) : NULL);
			appendStringInfoChar(str, ' ');
			outToken(str, OidIsValid(oprright) ? get_typ_name(oprright) : NULL);
			appendStringInfoChar(str, ' ');
		}
		else
#endif
		appendStringInfo(str, " %u", node->grpOperators[i]);

	WRITE_LONG_FIELD(numGroups);
	WRITE_BITMAPSET_FIELD(aggParams);
	WRITE_NODE_FIELD(groupingSets);
	WRITE_NODE_FIELD(chain);
#ifdef __OPENTENBASE_C__
	WRITE_BOOL_FIELD(groupingFunc);
#endif
	WRITE_INT_FIELD(plannerinfo_seq_no);
	WRITE_ENUM_FIELD(agg_type, AggType);
}

static void
_outWindowAgg(StringInfo str, const WindowAgg *node)
{
	int			i;

	WRITE_NODE_TYPE("WINDOWAGG");

	_outPlanInfo(str, (const Plan *) node);

	WRITE_UINT_FIELD(winref);
	WRITE_INT_FIELD(partNumCols);
	WRITE_ATTRNUMBER_ARRAY(partColIdx, node->partNumCols);

	appendStringInfoString(str, " :partOperators");
	for (i = 0; i < node->partNumCols; i++)
#ifdef XCP
		if (portable_output)
		{
			Oid oper = node->partOperators[i];
			Oid oprleft, oprright;
			/* The operator is always valid */
			Assert(OidIsValid(oper));
			appendStringInfoChar(str, ' ');
			outToken(str, NSP_NAME(get_opnamespace(oper)));
			appendStringInfoChar(str, ' ');
			outToken(str, get_opname(oper));
			appendStringInfoChar(str, ' ');
			op_input_types(oper, &oprleft, &oprright);
			outToken(str, OidIsValid(oprleft) ?
					NSP_NAME(get_typ_namespace(oprleft)) : NULL);
			appendStringInfoChar(str, ' ');
			outToken(str, OidIsValid(oprleft) ? get_typ_name(oprleft) : NULL);
			appendStringInfoChar(str, ' ');
			outToken(str, OidIsValid(oprright) ?
					NSP_NAME(get_typ_namespace(oprright)) : NULL);
			appendStringInfoChar(str, ' ');
			outToken(str, OidIsValid(oprright) ? get_typ_name(oprright) : NULL);
			appendStringInfoChar(str, ' ');
		}
		else
#endif
		appendStringInfo(str, " %u", node->partOperators[i]);

	WRITE_INT_FIELD(ordNumCols);

	WRITE_ATTRNUMBER_ARRAY(ordColIdx, node->ordNumCols);

	appendStringInfoString(str, " :ordOperators");
	for (i = 0; i < node->ordNumCols; i++)
#ifdef XCP
		if (portable_output)
		{
			Oid oper = node->ordOperators[i];
			Oid oprleft, oprright;
			/* Group operator is always valid */
			Assert(OidIsValid(oper));
			appendStringInfoChar(str, ' ');
			outToken(str, NSP_NAME(get_opnamespace(oper)));
			appendStringInfoChar(str, ' ');
			outToken(str, get_opname(oper));
			appendStringInfoChar(str, ' ');
			op_input_types(oper, &oprleft, &oprright);
			outToken(str, OidIsValid(oprleft) ?
					NSP_NAME(get_typ_namespace(oprleft)) : NULL);
			appendStringInfoChar(str, ' ');
			outToken(str, OidIsValid(oprleft) ? get_typ_name(oprleft) : NULL);
			appendStringInfoChar(str, ' ');
			outToken(str, OidIsValid(oprright) ?
					NSP_NAME(get_typ_namespace(oprright)) : NULL);
			appendStringInfoChar(str, ' ');
			outToken(str, OidIsValid(oprright) ? get_typ_name(oprright) : NULL);
			appendStringInfoChar(str, ' ');
		}
		else
#endif
		appendStringInfo(str, " %u", node->ordOperators[i]);

	WRITE_INT_FIELD(frameOptions);
	WRITE_NODE_FIELD(startOffset);
	WRITE_NODE_FIELD(endOffset);
	if (portable_output)
	{
		WRITE_FUNCID_FIELD(startInRangeFunc);
		WRITE_FUNCID_FIELD(endInRangeFunc);
		WRITE_FUNCID_FIELD(inRangeColl);
	}
	else
	{
		WRITE_OID_FIELD(startInRangeFunc);
		WRITE_OID_FIELD(endInRangeFunc);
		WRITE_OID_FIELD(inRangeColl);
	}
	WRITE_BOOL_FIELD(inRangeAsc);
	WRITE_BOOL_FIELD(inRangeNullsFirst);
	WRITE_BOOL_FIELD(start_contain_vars);
	WRITE_BOOL_FIELD(end_contain_vars);
}

static void
_outGroup(StringInfo str, const Group *node)
{
	int			i;

	WRITE_NODE_TYPE("GROUP");

	_outPlanInfo(str, (const Plan *) node);

	WRITE_INT_FIELD(numCols);

	WRITE_ATTRNUMBER_ARRAY(grpColIdx, node->numCols);

	appendStringInfoString(str, " :grpOperators");
	for (i = 0; i < node->numCols; i++)
#ifdef XCP
		if (portable_output)
		{
			Oid oper = node->grpOperators[i];
			Oid oprleft, oprright;
			/* Group operator is always valid */
			Assert(OidIsValid(oper));
			appendStringInfoChar(str, ' ');
			outToken(str, NSP_NAME(get_opnamespace(oper)));
			appendStringInfoChar(str, ' ');
			outToken(str, get_opname(oper));
			appendStringInfoChar(str, ' ');
			op_input_types(oper, &oprleft, &oprright);
			outToken(str, OidIsValid(oprleft) ?
					NSP_NAME(get_typ_namespace(oprleft)) : NULL);
			appendStringInfoChar(str, ' ');
			outToken(str, OidIsValid(oprleft) ? get_typ_name(oprleft) : NULL);
			appendStringInfoChar(str, ' ');
			outToken(str, OidIsValid(oprright) ?
					NSP_NAME(get_typ_namespace(oprright)) : NULL);
			appendStringInfoChar(str, ' ');
			outToken(str, OidIsValid(oprright) ? get_typ_name(oprright) : NULL);
			appendStringInfoChar(str, ' ');
		}
		else
#endif
		appendStringInfo(str, " %u", node->grpOperators[i]);

	WRITE_INT_FIELD(plannerinfo_seq_no);
	WRITE_ENUM_FIELD(agg_type, AggType);
}

static void
_outMaterial(StringInfo str, const Material *node)
{
	WRITE_NODE_TYPE("MATERIAL");

	_outPlanInfo(str, (const Plan *) node);
}



static void
_outSort(StringInfo str, const Sort *node)
{
	int			i;

	WRITE_NODE_TYPE("SORT");

	_outPlanInfo(str, (const Plan *) node);

	WRITE_INT_FIELD(numCols);

	WRITE_ATTRNUMBER_ARRAY(sortColIdx, node->numCols);

	appendStringInfoString(str, " :sortOperators");
	for (i = 0; i < node->numCols; i++)
#ifdef XCP
		if (portable_output)
		{
			Oid oper = node->sortOperators[i];
			Oid oprleft, oprright;
			/* Sort operator is always valid */
			Assert(OidIsValid(oper));
			appendStringInfoChar(str, ' ');
			outToken(str, NSP_NAME(get_opnamespace(oper)));
			appendStringInfoChar(str, ' ');
			outToken(str, get_opname(oper));
			appendStringInfoChar(str, ' ');
			op_input_types(oper, &oprleft, &oprright);
			outToken(str, OidIsValid(oprleft) ?
					NSP_NAME(get_typ_namespace(oprleft)) : NULL);
			appendStringInfoChar(str, ' ');
			outToken(str, OidIsValid(oprleft) ? get_typ_name(oprleft) : NULL);
			appendStringInfoChar(str, ' ');
			outToken(str, OidIsValid(oprright) ?
					NSP_NAME(get_typ_namespace(oprright)) : NULL);
			appendStringInfoChar(str, ' ');
			outToken(str, OidIsValid(oprright) ? get_typ_name(oprright) : NULL);
		}
		else
#endif
		appendStringInfo(str, " %u", node->sortOperators[i]);

	appendStringInfoString(str, " :collations");
	for (i = 0; i < node->numCols; i++)
#ifdef XCP
		if (portable_output)
		{
			Oid coll = node->collations[i];
			if (OidIsValid(coll))
			{
				appendStringInfoChar(str, ' ');
				outToken(str, NSP_NAME(get_collation_namespace(coll)));
				appendStringInfoChar(str, ' ');
				outToken(str, get_collation_name(coll));
				appendStringInfo(str, " %d", get_collation_encoding(coll));
			}
			else
				appendStringInfo(str, " <> <> -1");
		}
		else
#endif
		appendStringInfo(str, " %u", node->collations[i]);

	WRITE_BOOL_ARRAY(nullsFirst, node->numCols);
}

static void
_outUnique(StringInfo str, const Unique *node)
{
	int			i;

	WRITE_NODE_TYPE("UNIQUE");

	_outPlanInfo(str, (const Plan *) node);

	WRITE_INT_FIELD(numCols);

	WRITE_ATTRNUMBER_ARRAY(uniqColIdx, node->numCols);

	appendStringInfoString(str, " :uniqOperators");
	for (i = 0; i < node->numCols; i++)
#ifdef XCP
		if (portable_output)
		{
			Oid oper = node->uniqOperators[i];
			Oid oprleft, oprright;
			/* Unique operator is always valid */
			Assert(OidIsValid(oper));
			appendStringInfoChar(str, ' ');
			outToken(str, NSP_NAME(get_opnamespace(oper)));
			appendStringInfoChar(str, ' ');
			outToken(str, get_opname(oper));
			appendStringInfoChar(str, ' ');
			op_input_types(oper, &oprleft, &oprright);
			outToken(str, OidIsValid(oprleft) ?
					NSP_NAME(get_typ_namespace(oprleft)) : NULL);
			appendStringInfoChar(str, ' ');
			outToken(str, OidIsValid(oprleft) ? get_typ_name(oprleft) : NULL);
			appendStringInfoChar(str, ' ');
			outToken(str, OidIsValid(oprright) ?
					NSP_NAME(get_typ_namespace(oprright)) : NULL);
			appendStringInfoChar(str, ' ');
			outToken(str, OidIsValid(oprright) ? get_typ_name(oprright) : NULL);
			appendStringInfoChar(str, ' ');
		}
		else
#endif
		appendStringInfo(str, " %u", node->uniqOperators[i]);

	WRITE_INT_FIELD(plannerinfo_seq_no);
	WRITE_ENUM_FIELD(agg_type, AggType);
}

static void
_outHash(StringInfo str, const Hash *node)
{
	WRITE_NODE_TYPE("HASH");

	_outPlanInfo(str, (const Plan *) node);

	WRITE_NODE_FIELD(hashkeys);
#ifdef XCP
	if (portable_output)
		WRITE_RELID_FIELD(skewTable);
	else
#endif
	WRITE_OID_FIELD(skewTable);
	WRITE_INT_FIELD(skewColumn);
	WRITE_BOOL_FIELD(skewInherit);
	WRITE_FLOAT_FIELD(rows_total, "%.0f");
}

static void
_outSetOp(StringInfo str, const SetOp *node)
{
	int			i;

	WRITE_NODE_TYPE("SETOP");

	_outPlanInfo(str, (const Plan *) node);

	WRITE_ENUM_FIELD(cmd, SetOpCmd);
	WRITE_ENUM_FIELD(strategy, SetOpStrategy);
	WRITE_INT_FIELD(numCols);

	WRITE_ATTRNUMBER_ARRAY(dupColIdx, node->numCols);

	appendStringInfoString(str, " :dupOperators");
	for (i = 0; i < node->numCols; i++)
#ifdef XCP
		if (portable_output)
		{
			Oid oper = node->dupOperators[i];
			Oid oprleft, oprright;
			/* Unique operator is always valid */
			Assert(OidIsValid(oper));
			appendStringInfoChar(str, ' ');
			outToken(str, NSP_NAME(get_opnamespace(oper)));
			appendStringInfoChar(str, ' ');
			outToken(str, get_opname(oper));
			appendStringInfoChar(str, ' ');
			op_input_types(oper, &oprleft, &oprright);
			outToken(str, OidIsValid(oprleft) ?
					NSP_NAME(get_typ_namespace(oprleft)) : NULL);
			appendStringInfoChar(str, ' ');
			outToken(str, OidIsValid(oprleft) ? get_typ_name(oprleft) : NULL);
			appendStringInfoChar(str, ' ');
			outToken(str, OidIsValid(oprright) ?
					NSP_NAME(get_typ_namespace(oprright)) : NULL);
			appendStringInfoChar(str, ' ');
			outToken(str, OidIsValid(oprright) ? get_typ_name(oprright) : NULL);
		}
		else
#endif
		appendStringInfo(str, " %u", node->dupOperators[i]);

	WRITE_INT_FIELD(flagColIdx);
	WRITE_INT_FIELD(firstFlag);
	WRITE_LONG_FIELD(numGroups);
}

static void
_outLockRows(StringInfo str, const LockRows *node)
{
	WRITE_NODE_TYPE("LOCKROWS");

	_outPlanInfo(str, (const Plan *) node);

	WRITE_NODE_FIELD(rowMarks);
	WRITE_INT_FIELD(epqParam);
}

static void
_outLimit(StringInfo str, const Limit *node)
{
	WRITE_NODE_TYPE("LIMIT");

	_outPlanInfo(str, (const Plan *) node);

	WRITE_NODE_FIELD(limitOffset);
	WRITE_NODE_FIELD(limitCount);
	WRITE_ENUM_FIELD(limitOption, LimitOption);
	WRITE_INT_FIELD(uniqNumCols);
	WRITE_ATTRNUMBER_ARRAY(uniqColIdx, node->uniqNumCols);
	WRITE_OID_ARRAY(uniqOperators, node->uniqNumCols);
	WRITE_OID_ARRAY(uniqCollations, node->uniqNumCols);
}

#ifdef __OPENTENBASE_C__


#endif

#ifdef XCP
static void
_outRemoteSubplan(StringInfo str, const RemoteSubplan *node)
{
	int i;
	
	WRITE_NODE_TYPE("REMOTESUBPLAN");

	_outScanInfo(str, (Scan *) node);
#ifdef __OPENTENBASE_C__
	WRITE_INT_FIELD(fid);
	WRITE_INT_FIELD(param_fid);
	WRITE_INT_FIELD(flevel);
	WRITE_NODE_FIELD(targetNodes);
	WRITE_INT_FIELD(targetNodeType);
	WRITE_INT_FIELD(protocol);
	WRITE_BOOL_FIELD(localSend);
	WRITE_BOOL_FIELD(under_subplan);
	WRITE_INT_FIELD(nparams);

	for (i = 0; i < node->nparams; i++)
	{
		RemoteParam *remoteparams = (RemoteParam *)node->remoteparams;
		RemoteParam *rparam = &(remoteparams[i]);
		appendStringInfo(str, " :paramkind");
		appendStringInfo(str, " %d", (int) rparam->paramkind);

		appendStringInfo(str, " :paramid");
		appendStringInfo(str, " %d", rparam->paramid);

		appendStringInfo(str, " :paramused");
		appendStringInfo(str, " %d", rparam->paramused);

		appendStringInfo(str, " :paramtype");
		if (portable_output)
		{
			Oid ptype = rparam->paramtype;
			Assert(OidIsValid(ptype));
			appendStringInfoChar(str, ' ');
			outToken(str, NSP_NAME(get_typ_namespace(ptype)));
			appendStringInfoChar(str, ' ');
			outToken(str, get_typ_name(ptype));
		}
		else
			appendStringInfo(str, " %u", rparam->paramtype);
	}
#endif
	WRITE_CHAR_FIELD(distributionType);
#ifdef __OPENTENBASE_C__
	WRITE_INT_FIELD(ndiskeys);
	for (i = 0; i < node->ndiskeys; i++)
	{
		appendStringInfo(str, " :diskey");
		appendStringInfo(str, " %d", (int) node->diskeys[i]);
	}
#endif
	WRITE_NODE_FIELD(distributionNodes);
	WRITE_NODE_FIELD(distributionRestrict);
	WRITE_NODE_FIELD(nodeList);
	WRITE_BOOL_FIELD(execOnAll);
	WRITE_NODE_FIELD(sort);
	WRITE_STRING_FIELD(cursor);
	WRITE_BOOL_FIELD(roundrobin_distributed);
	WRITE_BOOL_FIELD(roundrobin_replicate);
	WRITE_NODE_FIELD(retain_list);
	WRITE_NODE_FIELD(replicated_list);
	WRITE_INT_FIELD(num_workers);
	WRITE_INT_FIELD(num_virtualdop);
	WRITE_BOOL_FIELD(cacheSend);
	WRITE_BOOL_FIELD(transfer_datarow);
	WRITE_BITMAPSET_FIELD(initParam);
	WRITE_NODE_FIELD(dests);

	/* begin ora_compatible */
	WRITE_NODE_FIELD(multi_dist);
	WRITE_INT_FIELD(startno);
	WRITE_INT_FIELD(endno);
	WRITE_BOOL_FIELD(has_else);
	WRITE_BOOL_FIELD(only_one_send_tuple);
	/* end ora_compatible */
}

static void
_outRemoteStmt(StringInfo str, const RemoteStmt *node)
{
	int i;

	WRITE_NODE_TYPE("REMOTESTMT");

	WRITE_ENUM_FIELD(commandType, CmdType);
	WRITE_BOOL_FIELD(hasReturning);
	WRITE_INT_FIELD(jitFlags);
	WRITE_NODE_FIELD(planTree);
	WRITE_NODE_FIELD(rtable);
	WRITE_NODE_FIELD(resultRelations);
    WRITE_NODE_FIELD(nonleafResultRelations);
    WRITE_NODE_FIELD(rootResultRelations); 
	WRITE_NODE_FIELD(subplans);
	WRITE_BITMAPSET_FIELD(rewindPlanIDs);
	WRITE_NODE_FIELD(paramExecTypes);
	WRITE_INT_FIELD(nParamRemote);

	for (i = 0; i < node->nParamRemote; i++)
	{
		RemoteParam *rparam = &(node->remoteparams[i]);
		appendStringInfo(str, " :paramkind");
		appendStringInfo(str, " %d", (int) rparam->paramkind);

		appendStringInfo(str, " :paramid");
		appendStringInfo(str, " %d", rparam->paramid);

		appendStringInfo(str, " :paramused");
		appendStringInfo(str, " %d", rparam->paramused);

		appendStringInfo(str, " :paramtype");
		if (portable_output)
		{
			Oid ptype = rparam->paramtype;
			Assert(OidIsValid(ptype));
			appendStringInfoChar(str, ' ');
			outToken(str, NSP_NAME(get_typ_namespace(ptype)));
			appendStringInfoChar(str, ' ');
			outToken(str, get_typ_name(ptype));
		}
		else
			appendStringInfo(str, " %u", rparam->paramtype);
	}
	WRITE_NODE_FIELD(rowMarks);
#ifdef __OPENTENBASE_C__
	WRITE_INT_FIELD(ndiskeys);
	for (i = 0; i < node->ndiskeys; i++)
	{
		appendStringInfo(str, " :diskey");
		appendStringInfo(str, " %d", (int) node->diskeys[i]);
	}
#endif
#ifdef __OPENTENBASE__
	WRITE_BOOL_FIELD(parallelModeNeeded);
#endif
#ifdef __OPENTENBASE_C__
	WRITE_INT_FIELD(fragmentType);
	WRITE_BITMAPSET_FIELD(sharedCtePlanIds);
	WRITE_INT_FIELD(fragment_work_mem);
	WRITE_INT_FIELD(fragment_num);
#endif
#ifdef __AUDIT__
	WRITE_STRING_FIELD(queryString);
	WRITE_NODE_FIELD(parseTree);
#endif

	/* begin ora_compatible */
	WRITE_NODE_FIELD(multi_dist);
	WRITE_INT_FIELD(startno);
	WRITE_INT_FIELD(endno);
	WRITE_BOOL_FIELD(has_else);
	/* end ora_compatible */
	WRITE_LONG_FIELD(queryid.timestamp_nodeid);
	WRITE_LONG_FIELD(queryid.sequence);
}

static void
_outSimpleSort(StringInfo str, const SimpleSort *node)
{
	int			i;

	WRITE_NODE_TYPE("SIMPLESORT");

	WRITE_INT_FIELD(numCols);

	appendStringInfo(str, " :sortColIdx");
	for (i = 0; i < node->numCols; i++)
		appendStringInfo(str, " %d", node->sortColIdx[i]);

	appendStringInfo(str, " :sortOperators");
	for (i = 0; i < node->numCols; i++)
		if (portable_output)
		{
			Oid oper = node->sortOperators[i];
			Oid oprleft, oprright;
			/* Sort operator is always valid */
			Assert(OidIsValid(oper));
			appendStringInfoChar(str, ' ');
			outToken(str, NSP_NAME(get_opnamespace(oper)));
			appendStringInfoChar(str, ' ');
			outToken(str, get_opname(oper));
			appendStringInfoChar(str, ' ');
			op_input_types(oper, &oprleft, &oprright);
			outToken(str, OidIsValid(oprleft) ?
					NSP_NAME(get_typ_namespace(oprleft)) : NULL);
			appendStringInfoChar(str, ' ');
			outToken(str, OidIsValid(oprleft) ? get_typ_name(oprleft) : NULL);
			appendStringInfoChar(str, ' ');
			outToken(str, OidIsValid(oprright) ?
					NSP_NAME(get_typ_namespace(oprright)) : NULL);
			appendStringInfoChar(str, ' ');
			outToken(str, OidIsValid(oprright) ? get_typ_name(oprright) : NULL);
		}
		else
			appendStringInfo(str, " %u", node->sortOperators[i]);

	appendStringInfo(str, " :sortCollations");
	for (i = 0; i < node->numCols; i++)
		if (portable_output)
		{
			Oid coll = node->sortCollations[i];
			if (OidIsValid(coll))
			{
				appendStringInfoChar(str, ' ');
				outToken(str, NSP_NAME(get_collation_namespace(coll)));
				appendStringInfoChar(str, ' ');
				outToken(str, get_collation_name(coll));
				appendStringInfo(str, " %d", get_collation_encoding(coll));
			}
			else
				appendStringInfo(str, " <> <> -1");
		}
		else
			appendStringInfo(str, " %u", node->sortCollations[i]);

	appendStringInfo(str, " :nullsFirst");
	for (i = 0; i < node->numCols; i++)
		appendStringInfo(str, " %s", booltostr(node->nullsFirst[i]));
}
#endif

static void
_outNestLoopParam(StringInfo str, const NestLoopParam *node)
{
	WRITE_NODE_TYPE("NESTLOOPPARAM");

	WRITE_INT_FIELD(paramno);
	WRITE_NODE_FIELD(paramval);
}

static void
_outPlanRowMark(StringInfo str, const PlanRowMark *node)
{
	WRITE_NODE_TYPE("PLANROWMARK");

	WRITE_UINT_FIELD(rti);
	WRITE_UINT_FIELD(prti);
	WRITE_UINT_FIELD(rowmarkId);
	WRITE_ENUM_FIELD(markType, RowMarkType);
	WRITE_INT_FIELD(allMarkTypes);
	WRITE_ENUM_FIELD(strength, LockClauseStrength);
	WRITE_ENUM_FIELD(waitPolicy, LockWaitPolicy);
	WRITE_INT_FIELD(waitTimeout);
	WRITE_BOOL_FIELD(isParent);
}

static void
_outPartitionPruneStepOp(StringInfo str, const PartitionPruneStepOp *node)
{
	WRITE_NODE_TYPE("PARTITIONPRUNESTEPOP");

	WRITE_INT_FIELD(step.step_id);
	WRITE_INT_FIELD(opstrategy);
	WRITE_NODE_FIELD(exprs);
	WRITE_NODE_FIELD(cmpfns);
	WRITE_BITMAPSET_FIELD(nullkeys);
}

static void
_outPartitionPruneStepCombine(StringInfo str, const PartitionPruneStepCombine *node)
{
	WRITE_NODE_TYPE("PARTITIONPRUNESTEPCOMBINE");

	WRITE_INT_FIELD(step.step_id);
	WRITE_ENUM_FIELD(combineOp, PartitionPruneCombineOp);
	WRITE_NODE_FIELD(source_stepids);
}

static void
_outPartitionPruneInfo(StringInfo str, const PartitionPruneInfo *node)
{
	WRITE_NODE_TYPE("PARTITIONPRUNEINFO");

	WRITE_NODE_FIELD(prune_infos);
	WRITE_BITMAPSET_FIELD(other_subplans);
}

static void
_outPartitionedRelPruneInfo(StringInfo str, const PartitionedRelPruneInfo *node)
{
	WRITE_NODE_TYPE("PARTITIONEDRELPRUNEINFO");
#ifdef __OPENTENBASE__
	if (portable_output)
	{
		WRITE_RELID_FIELD(reloid);
	}
	else
#endif
		WRITE_OID_FIELD(reloid);

	WRITE_NODE_FIELD(pruning_steps);
	WRITE_BITMAPSET_FIELD(present_parts);
	WRITE_INT_FIELD(nparts);
	WRITE_INT_FIELD(nexprs);
	WRITE_INT_ARRAY(subplan_map, node->nparts);
	WRITE_INT_ARRAY(subpart_map, node->nparts);
	WRITE_BOOL_ARRAY(hasexecparam, node->nexprs);
	WRITE_BOOL_FIELD(do_initial_prune);
	WRITE_BOOL_FIELD(do_exec_prune);
	WRITE_BITMAPSET_FIELD(execparamids);
}

static void
_outPlanInvalItem(StringInfo str, const PlanInvalItem *node)
{
	WRITE_NODE_TYPE("PLANINVALITEM");

	WRITE_INT_FIELD(cacheId);
	WRITE_UINT_FIELD(hashValue);
}

/*****************************************************************************
 *
 *	Stuff from primnodes.h.
 *
 *****************************************************************************/

static void
_outAlias(StringInfo str, const Alias *node)
{
	WRITE_NODE_TYPE("ALIAS");

	WRITE_STRING_FIELD(aliasname);
	WRITE_NODE_FIELD(colnames);
}

static void
_outRangeVar(StringInfo str, const RangeVar *node)
{
	WRITE_NODE_TYPE("RANGEVAR");

	/*
	 * we deliberately ignore catalogname here, since it is presently not
	 * semantically meaningful
	 */
	WRITE_STRING_FIELD(schemaname);
	WRITE_STRING_FIELD(relname);
	WRITE_BOOL_FIELD(inh);
	WRITE_CHAR_FIELD(relpersistence);
	WRITE_NODE_FIELD(alias);
	WRITE_LOCATION_FIELD(location);
#ifdef __STORAGE_SCALABLE__
	WRITE_STRING_FIELD(pubname);
#endif
	WRITE_STRING_FIELD(childtablename);
}

static void
_outTableFunc(StringInfo str, const TableFunc *node)
{
	WRITE_NODE_TYPE("TABLEFUNC");

	WRITE_NODE_FIELD(ns_uris);
	WRITE_NODE_FIELD(ns_names);
	WRITE_NODE_FIELD(docexpr);
	WRITE_NODE_FIELD(rowexpr);
	WRITE_NODE_FIELD(colnames);
	WRITE_NODE_FIELD(coltypes);
	WRITE_NODE_FIELD(coltypmods);
	WRITE_NODE_FIELD(colcollations);
	WRITE_NODE_FIELD(colexprs);
	WRITE_NODE_FIELD(coldefexprs);
	WRITE_BITMAPSET_FIELD(notnulls);
	WRITE_INT_FIELD(ordinalitycol);
	WRITE_LOCATION_FIELD(location);
}

static void
_outIntoClause(StringInfo str, const IntoClause *node)
{
	WRITE_NODE_TYPE("INTOCLAUSE");

	WRITE_NODE_FIELD(rel);
	WRITE_NODE_FIELD(colNames);
	WRITE_NODE_FIELD(options);
	WRITE_ENUM_FIELD(onCommit, OnCommitAction);
	WRITE_STRING_FIELD(tableSpaceName);
	WRITE_NODE_FIELD(viewQuery);
	WRITE_BOOL_FIELD(skipData);
}

static void
_outVar(StringInfo str, const Var *node)
{
	WRITE_NODE_TYPE("VAR");

	WRITE_INT_FIELD(varno);
	WRITE_INT_FIELD(varattno);
#ifdef XCP
	if (portable_output)
		WRITE_TYPID_FIELD(vartype);
	else
#endif
	WRITE_OID_FIELD(vartype);
	WRITE_INT_FIELD(vartypmod);
#ifdef XCP
	if (portable_output)
		WRITE_COLLID_FIELD(varcollid);
	else
#endif
	WRITE_OID_FIELD(varcollid);
	WRITE_UINT_FIELD(varlevelsup);
	WRITE_UINT_FIELD(varnoold);
	WRITE_INT_FIELD(varoattno);
	WRITE_LOCATION_FIELD(location);
#ifdef __OPENTENBASE_C__
	WRITE_UINT_FIELD(varrelid);
	WRITE_INT_FIELD(varrelcolid);
	WRITE_UINT_FIELD(varseq);
#endif
}

static void
_outConst(StringInfo str, const Const *node)
{
	WRITE_NODE_TYPE("CONST");

#ifdef XCP
	if (portable_output)
		WRITE_TYPID_FIELD(consttype);
	else
#endif
	WRITE_OID_FIELD(consttype);
	WRITE_INT_FIELD(consttypmod);
#ifdef XCP
	if (portable_output)
		WRITE_COLLID_FIELD(constcollid);
	else
#endif
	WRITE_OID_FIELD(constcollid);
	WRITE_INT_FIELD(constlen);
	WRITE_BOOL_FIELD(constbyval);
	WRITE_BOOL_FIELD(constisnull);
	WRITE_LOCATION_FIELD(location);

	appendStringInfoString(str, " :constvalue ");
	if (node->constisnull)
		appendStringInfoString(str, "<>");
	else
#ifdef XCP
		if (portable_output)
			_printDatum(str, node->constvalue, node->consttype);
		else
#endif
		outDatum(str, node->constvalue, node->constlen, node->constbyval);
}

static void
_outParam(StringInfo str, const Param *node)
{
	WRITE_NODE_TYPE("PARAM");

	WRITE_ENUM_FIELD(paramkind, ParamKind);
	WRITE_INT_FIELD(paramid);
#ifdef XCP
	if (portable_output)
		WRITE_TYPID_FIELD(paramtype);
	else
#endif
	WRITE_OID_FIELD(paramtype);
	WRITE_INT_FIELD(paramtypmod);
#ifdef XCP
	if (portable_output)
		WRITE_COLLID_FIELD(paramcollid);
	else
#endif
	WRITE_OID_FIELD(paramcollid);
	WRITE_LOCATION_FIELD(location);
	WRITE_NODE_FIELD(paramindextype);
}

static void
_outAggref(StringInfo str, const Aggref *node)
{
	WRITE_NODE_TYPE("AGGREF");

#ifdef XCP
	if (portable_output)
		WRITE_FUNCID_FIELD(aggfnoid);
	else
#endif
	WRITE_OID_FIELD(aggfnoid);
#ifdef XCP
	if (portable_output)
		WRITE_TYPID_FIELD(aggtype);
	else
#endif
	WRITE_OID_FIELD(aggtype);
#ifdef XCP
	if (portable_output)
		WRITE_COLLID_FIELD(aggcollid);
	else
#endif
	WRITE_OID_FIELD(aggcollid);
#ifdef XCP
	if (portable_output)
		WRITE_COLLID_FIELD(inputcollid);
	else
#endif
	WRITE_OID_FIELD(inputcollid);
#ifdef XCP
	if (portable_output)
		WRITE_TYPID_FIELD(aggtranstype);
	else
#endif
	WRITE_OID_FIELD(aggtranstype);
#ifdef XCP
	if (portable_output)
		WRITE_TYPID_LIST_FIELD(aggargtypes);
	else
#endif
	WRITE_NODE_FIELD(aggargtypes);
	WRITE_NODE_FIELD(aggdirectargs);
	WRITE_NODE_FIELD(args);
	WRITE_NODE_FIELD(aggorder);
	WRITE_NODE_FIELD(aggdistinct);
	WRITE_NODE_FIELD(aggfilter);
	WRITE_BOOL_FIELD(aggstar);
	WRITE_BOOL_FIELD(aggvariadic);
	WRITE_CHAR_FIELD(aggkind);
	WRITE_UINT_FIELD(agglevelsup);
	WRITE_ENUM_FIELD(aggsplit, AggSplit);
	WRITE_LOCATION_FIELD(location);
#ifdef __OPENTENBASE_C__
	WRITE_NODE_FIELD(distinct_args);
	WRITE_LONG_FIELD(distinct_num);
#endif
	WRITE_BOOL_FIELD(aggkeep);
}

static void
_outGroupingFunc(StringInfo str, const GroupingFunc *node)
{
	WRITE_NODE_TYPE("GROUPINGFUNC");

	WRITE_NODE_FIELD(args);
	WRITE_NODE_FIELD(refs);
	WRITE_NODE_FIELD(cols);
	WRITE_UINT_FIELD(agglevelsup);
	WRITE_LOCATION_FIELD(location);
#ifdef __OPENTENBASE_C__
	WRITE_BOOL_FIELD(groupingSets);
#endif
	WRITE_ENUM_FIELD(kind, GroupingFuncKind);
}

static void
_outWindowFunc(StringInfo str, const WindowFunc *node)
{
	WRITE_NODE_TYPE("WINDOWFUNC");

#ifdef XCP
	if (portable_output)
		WRITE_FUNCID_FIELD(winfnoid);
	else
#endif
	WRITE_OID_FIELD(winfnoid);
#ifdef XCP
	if (portable_output)
		WRITE_TYPID_FIELD(wintype);
	else
#endif
	WRITE_OID_FIELD(wintype);
#ifdef XCP
	if (portable_output)
		WRITE_COLLID_FIELD(wincollid);
	else
#endif
	WRITE_OID_FIELD(wincollid);
#ifdef XCP
	if (portable_output)
		WRITE_COLLID_FIELD(inputcollid);
	else
#endif
	WRITE_OID_FIELD(inputcollid);
	WRITE_NODE_FIELD(args);
	WRITE_NODE_FIELD(aggfilter);
	WRITE_UINT_FIELD(winref);
	WRITE_BOOL_FIELD(winstar);
	WRITE_BOOL_FIELD(winagg);
	WRITE_BOOL_FIELD(windistinct);
#ifdef XCP
	if (portable_output)
		WRITE_TYPID_FIELD(winaggargtype);
	else
#endif
	WRITE_OID_FIELD(winaggargtype);
	WRITE_LOCATION_FIELD(location);
}

static void
_outArrayRef(StringInfo str, const ArrayRef *node)
{
	WRITE_NODE_TYPE("ARRAYREF");

#ifdef XCP
	if (portable_output)
		WRITE_TYPID_FIELD(refarraytype);
	else
#endif
	WRITE_OID_FIELD(refarraytype);
#ifdef XCP
	if (portable_output)
		WRITE_TYPID_FIELD(refelemtype);
	else
#endif
	WRITE_OID_FIELD(refelemtype);
	WRITE_INT_FIELD(reftypmod);
#ifdef XCP
	if (portable_output)
		WRITE_COLLID_FIELD(refcollid);
	else
#endif
	WRITE_OID_FIELD(refcollid);
	WRITE_NODE_FIELD(refupperindexpr);
	WRITE_NODE_FIELD(reflowerindexpr);
	WRITE_NODE_FIELD(refexpr);
	WRITE_NODE_FIELD(refassgnexpr);
}

static void
_outFuncExpr(StringInfo str, const FuncExpr *node)
{
	WRITE_NODE_TYPE("FUNCEXPR");

#ifdef XCP
	if (portable_output)
		WRITE_FUNCID_FIELD(funcid);
	else
#endif
	WRITE_OID_FIELD(funcid);
#ifdef XCP
	if (portable_output)
		WRITE_TYPID_FIELD(funcresulttype);
	else
#endif
	WRITE_OID_FIELD(funcresulttype);
	WRITE_BOOL_FIELD(funcretset);
	WRITE_BOOL_FIELD(funcvariadic);
	WRITE_ENUM_FIELD(funcformat, CoercionForm);
#ifdef XCP
	if (portable_output)
		WRITE_COLLID_FIELD(funccollid);
	else
#endif
	WRITE_OID_FIELD(funccollid);
#ifdef XCP
	if (portable_output)
		WRITE_COLLID_FIELD(inputcollid);
	else
#endif
	WRITE_OID_FIELD(inputcollid);
	WRITE_NODE_FIELD(args);
	WRITE_NODE_FIELD(withfuncnsp);
	WRITE_INT_FIELD(withfuncid);
	WRITE_LOCATION_FIELD(location);
}

static void
_outNamedArgExpr(StringInfo str, const NamedArgExpr *node)
{
	WRITE_NODE_TYPE("NAMEDARGEXPR");

	WRITE_NODE_FIELD(arg);
	WRITE_STRING_FIELD(name);
	WRITE_INT_FIELD(argnumber);
	WRITE_LOCATION_FIELD(location);
}

static void
_outOpExpr(StringInfo str, const OpExpr *node)
{
	WRITE_NODE_TYPE("OPEXPR");

#ifdef XCP
	if (portable_output)
		WRITE_OPERID_FIELD(opno);
	else
#endif
	WRITE_OID_FIELD(opno);
#ifdef XCP
	if (portable_output)
		WRITE_FUNCID_FIELD(opfuncid);
	else
#endif
	WRITE_OID_FIELD(opfuncid);
#ifdef XCP
	if (portable_output)
		WRITE_TYPID_FIELD(opresulttype);
	else
#endif
	WRITE_OID_FIELD(opresulttype);
	WRITE_BOOL_FIELD(opretset);
#ifdef XCP
	if (portable_output)
		WRITE_COLLID_FIELD(opcollid);
	else
#endif
	WRITE_OID_FIELD(opcollid);
#ifdef XCP
	if (portable_output)
		WRITE_COLLID_FIELD(inputcollid);
	else
#endif
	WRITE_OID_FIELD(inputcollid);
	WRITE_NODE_FIELD(args);
	WRITE_LOCATION_FIELD(location);
}

static void
_outDistinctExpr(StringInfo str, const DistinctExpr *node)
{
	WRITE_NODE_TYPE("DISTINCTEXPR");

#ifdef XCP
	if (portable_output)
		WRITE_OPERID_FIELD(opno);
	else
#endif
	WRITE_OID_FIELD(opno);
#ifdef XCP
	if (portable_output)
		WRITE_FUNCID_FIELD(opfuncid);
	else
#endif
	WRITE_OID_FIELD(opfuncid);
#ifdef XCP
	if (portable_output)
		WRITE_TYPID_FIELD(opresulttype);
	else
#endif
	WRITE_OID_FIELD(opresulttype);
	WRITE_BOOL_FIELD(opretset);
#ifdef XCP
	if (portable_output)
		WRITE_COLLID_FIELD(opcollid);
	else
#endif
	WRITE_OID_FIELD(opcollid);
#ifdef XCP
	if (portable_output)
		WRITE_COLLID_FIELD(inputcollid);
	else
#endif
	WRITE_OID_FIELD(inputcollid);
	WRITE_NODE_FIELD(args);
	WRITE_LOCATION_FIELD(location);
}

static void
_outNullIfExpr(StringInfo str, const NullIfExpr *node)
{
	WRITE_NODE_TYPE("NULLIFEXPR");

#ifdef XCP
	if (portable_output)
		WRITE_OPERID_FIELD(opno);
	else
#endif
	WRITE_OID_FIELD(opno);
#ifdef XCP
	if (portable_output)
		WRITE_FUNCID_FIELD(opfuncid);
	else
#endif
	WRITE_OID_FIELD(opfuncid);
#ifdef XCP
	if (portable_output)
		WRITE_TYPID_FIELD(opresulttype);
	else
#endif
	WRITE_OID_FIELD(opresulttype);
	WRITE_BOOL_FIELD(opretset);
#ifdef XCP
	if (portable_output)
		WRITE_COLLID_FIELD(opcollid);
	else
#endif
	WRITE_OID_FIELD(opcollid);
#ifdef XCP
	if (portable_output)
		WRITE_COLLID_FIELD(inputcollid);
	else
#endif
	WRITE_OID_FIELD(inputcollid);
	WRITE_NODE_FIELD(args);
	WRITE_LOCATION_FIELD(location);
}

static void
_outScalarArrayOpExpr(StringInfo str, const ScalarArrayOpExpr *node)
{
	WRITE_NODE_TYPE("SCALARARRAYOPEXPR");

#ifdef XCP
	if (portable_output)
		WRITE_OPERID_FIELD(opno);
	else
#endif
	WRITE_OID_FIELD(opno);
#ifdef XCP
	if (portable_output)
		WRITE_FUNCID_FIELD(opfuncid);
	else
#endif
	WRITE_OID_FIELD(opfuncid);
	WRITE_BOOL_FIELD(useOr);
#ifdef XCP
	if (portable_output)
		WRITE_COLLID_FIELD(inputcollid);
	else
#endif
	WRITE_OID_FIELD(inputcollid);
	WRITE_NODE_FIELD(args);
	WRITE_LOCATION_FIELD(location);
}

static void
_outBoolExpr(StringInfo str, const BoolExpr *node)
{
	char	   *opstr = NULL;

	WRITE_NODE_TYPE("BOOLEXPR");

	/* do-it-yourself enum representation */
	switch (node->boolop)
	{
		case AND_EXPR:
			opstr = "and";
			break;
		case OR_EXPR:
			opstr = "or";
			break;
		case NOT_EXPR:
			opstr = "not";
			break;
	}
	appendStringInfoString(str, " :boolop ");
	outToken(str, opstr);

	WRITE_NODE_FIELD(args);
	WRITE_LOCATION_FIELD(location);
}

static void
_outSubLink(StringInfo str, const SubLink *node)
{
	WRITE_NODE_TYPE("SUBLINK");

	WRITE_ENUM_FIELD(subLinkType, SubLinkType);
	WRITE_INT_FIELD(subLinkId);
	WRITE_NODE_FIELD(testexpr);
	WRITE_NODE_FIELD(operName);
	WRITE_NODE_FIELD(subselect);
	WRITE_LOCATION_FIELD(location);
}

static void
_outSubPlan(StringInfo str, const SubPlan *node)
{
	WRITE_NODE_TYPE("SUBPLAN");

	WRITE_ENUM_FIELD(subLinkType, SubLinkType);
	WRITE_NODE_FIELD(testexpr);
	WRITE_NODE_FIELD(paramIds);
	WRITE_INT_FIELD(plan_id);
	WRITE_STRING_FIELD(plan_name);
#ifdef XCP
	if (portable_output)
		WRITE_TYPID_FIELD(firstColType);
	else
#endif
	WRITE_OID_FIELD(firstColType);
	WRITE_INT_FIELD(firstColTypmod);
#ifdef XCP
	if (portable_output)
		WRITE_COLLID_FIELD(firstColCollation);
	else
#endif
	WRITE_OID_FIELD(firstColCollation);
	WRITE_BOOL_FIELD(useHashTable);
	WRITE_BOOL_FIELD(unknownEqFalse);
	WRITE_BOOL_FIELD(parallel_safe);
	WRITE_NODE_FIELD(setParam);
	WRITE_NODE_FIELD(parParam);
	WRITE_NODE_FIELD(args);
	WRITE_FLOAT_FIELD(startup_cost, "%.2f");
	WRITE_FLOAT_FIELD(per_call_cost, "%.2f");
#ifdef __OPENTENBASE_C__
	WRITE_BOOL_FIELD(isInitPlan);
#endif
}

static void
_outAlternativeSubPlan(StringInfo str, const AlternativeSubPlan *node)
{
	WRITE_NODE_TYPE("ALTERNATIVESUBPLAN");

	WRITE_NODE_FIELD(subplans);
}

static void
_outFieldSelect(StringInfo str, const FieldSelect *node)
{
	WRITE_NODE_TYPE("FIELDSELECT");

	WRITE_NODE_FIELD(arg);
	WRITE_INT_FIELD(fieldnum);
#ifdef XCP
	if (portable_output)
		WRITE_TYPID_FIELD(resulttype);
	else
#endif
	WRITE_OID_FIELD(resulttype);
	WRITE_INT_FIELD(resulttypmod);
#ifdef XCP
	if (portable_output)
		WRITE_COLLID_FIELD(resultcollid);
	else
#endif
	WRITE_OID_FIELD(resultcollid);
}

static void
_outFieldStore(StringInfo str, const FieldStore *node)
{
	WRITE_NODE_TYPE("FIELDSTORE");

	WRITE_NODE_FIELD(arg);
	WRITE_NODE_FIELD(newvals);
	WRITE_NODE_FIELD(fieldnums);
#ifdef XCP
	if (portable_output)
		WRITE_TYPID_FIELD(resulttype);
	else
#endif
	WRITE_OID_FIELD(resulttype);
}

static void
_outRelabelType(StringInfo str, const RelabelType *node)
{
	WRITE_NODE_TYPE("RELABELTYPE");

	WRITE_NODE_FIELD(arg);
#ifdef XCP
	if (portable_output)
		WRITE_TYPID_FIELD(resulttype);
	else
#endif
	WRITE_OID_FIELD(resulttype);
	WRITE_INT_FIELD(resulttypmod);
#ifdef XCP
	if (portable_output)
		WRITE_COLLID_FIELD(resultcollid);
	else
#endif
	WRITE_OID_FIELD(resultcollid);
	WRITE_ENUM_FIELD(relabelformat, CoercionForm);
	WRITE_LOCATION_FIELD(location);
}

static void
_outCoerceViaIO(StringInfo str, const CoerceViaIO *node)
{
	WRITE_NODE_TYPE("COERCEVIAIO");

	WRITE_NODE_FIELD(arg);
#ifdef XCP
	if (portable_output)
		WRITE_TYPID_FIELD(resulttype);
	else
#endif
	WRITE_OID_FIELD(resulttype);
    WRITE_INT_FIELD(resulttypmod);
#ifdef XCP
	if (portable_output)
		WRITE_COLLID_FIELD(resultcollid);
	else
#endif
	WRITE_OID_FIELD(resultcollid);
	WRITE_ENUM_FIELD(coerceformat, CoercionForm);
	WRITE_LOCATION_FIELD(location);
}

static void
_outArrayCoerceExpr(StringInfo str, const ArrayCoerceExpr *node)
{
	WRITE_NODE_TYPE("ARRAYCOERCEEXPR");

	WRITE_NODE_FIELD(arg);
	WRITE_NODE_FIELD(elemexpr);
#ifdef XCP
	if (portable_output)
		WRITE_TYPID_FIELD(resulttype);
	else
#endif
	WRITE_OID_FIELD(resulttype);
	WRITE_INT_FIELD(resulttypmod);
#ifdef XCP
	if (portable_output)
		WRITE_COLLID_FIELD(resultcollid);
	else
#endif
	WRITE_OID_FIELD(resultcollid);
	WRITE_ENUM_FIELD(coerceformat, CoercionForm);
	WRITE_LOCATION_FIELD(location);
}

static void
_outConvertRowtypeExpr(StringInfo str, const ConvertRowtypeExpr *node)
{
	WRITE_NODE_TYPE("CONVERTROWTYPEEXPR");

	WRITE_NODE_FIELD(arg);
#ifdef XCP
	if (portable_output)
		WRITE_TYPID_FIELD(resulttype);
	else
#endif
	WRITE_OID_FIELD(resulttype);
	WRITE_ENUM_FIELD(convertformat, CoercionForm);
	WRITE_LOCATION_FIELD(location);
}

static void
_outCollateExpr(StringInfo str, const CollateExpr *node)
{
	WRITE_NODE_TYPE("COLLATE");

	WRITE_NODE_FIELD(arg);
#ifdef __OPENTENBASE__
	if (portable_output)
	{
		WRITE_COLLID_FIELD(collOid);
	}
	else
#endif
	WRITE_OID_FIELD(collOid);
	WRITE_LOCATION_FIELD(location);
}

static void
_outCaseExpr(StringInfo str, const CaseExpr *node)
{
	WRITE_NODE_TYPE("CASE");

#ifdef XCP
	if (portable_output)
		WRITE_TYPID_FIELD(casetype);
	else
#endif
	WRITE_OID_FIELD(casetype);
#ifdef XCP
	if (portable_output)
		WRITE_COLLID_FIELD(casecollid);
	else
#endif
	WRITE_OID_FIELD(casecollid);
	WRITE_NODE_FIELD(arg);
	WRITE_NODE_FIELD(args);
	WRITE_NODE_FIELD(defresult);
	WRITE_BOOL_FIELD(isdecode);
	WRITE_LOCATION_FIELD(location);
}

static void
_outCaseWhen(StringInfo str, const CaseWhen *node)
{
	WRITE_NODE_TYPE("WHEN");

	WRITE_NODE_FIELD(expr);
	WRITE_NODE_FIELD(result);
	WRITE_LOCATION_FIELD(location);
}

static void
_outCaseTestExpr(StringInfo str, const CaseTestExpr *node)
{
	WRITE_NODE_TYPE("CASETESTEXPR");

#ifdef XCP
	if (portable_output)
		WRITE_TYPID_FIELD(typeId);
	else
#endif
	WRITE_OID_FIELD(typeId);
	WRITE_INT_FIELD(typeMod);
#ifdef XCP
	if (portable_output)
		WRITE_COLLID_FIELD(collation);
	else
#endif
	WRITE_OID_FIELD(collation);
}

static void
_outArrayExpr(StringInfo str, const ArrayExpr *node)
{
	WRITE_NODE_TYPE("ARRAY");

#ifdef XCP
	if (portable_output)
		WRITE_TYPID_FIELD(array_typeid);
	else
#endif
	WRITE_OID_FIELD(array_typeid);
#ifdef XCP
	if (portable_output)
		WRITE_COLLID_FIELD(array_collid);
	else
#endif
	WRITE_OID_FIELD(array_collid);
#ifdef XCP
	if (portable_output)
		WRITE_TYPID_FIELD(element_typeid);
	else
#endif
	WRITE_OID_FIELD(element_typeid);
	WRITE_NODE_FIELD(elements);
	WRITE_BOOL_FIELD(multidims);
	WRITE_LOCATION_FIELD(location);
}

static void
_outRowExpr(StringInfo str, const RowExpr *node)
{
	WRITE_NODE_TYPE("ROW");

	WRITE_NODE_FIELD(args);
#ifdef XCP
	if (portable_output)
		WRITE_TYPID_FIELD(row_typeid);
	else
#endif
	WRITE_OID_FIELD(row_typeid);
	WRITE_ENUM_FIELD(row_format, CoercionForm);
	WRITE_NODE_FIELD(colnames);
	WRITE_LOCATION_FIELD(location);
}

static void
_outRowCompareExpr(StringInfo str, const RowCompareExpr *node)
{
	WRITE_NODE_TYPE("ROWCOMPARE");

	WRITE_ENUM_FIELD(rctype, RowCompareType);
	WRITE_NODE_FIELD(opnos);
	WRITE_NODE_FIELD(opfamilies);
	WRITE_NODE_FIELD(inputcollids);
	WRITE_NODE_FIELD(largs);
	WRITE_NODE_FIELD(rargs);
}

static void
_outCoalesceExpr(StringInfo str, const CoalesceExpr *node)
{
	WRITE_NODE_TYPE("COALESCE");

#ifdef XCP
	if (portable_output)
		WRITE_TYPID_FIELD(coalescetype);
	else
#endif
	WRITE_OID_FIELD(coalescetype);
#ifdef XCP
	if (portable_output)
		WRITE_COLLID_FIELD(coalescecollid);
	else
#endif
	WRITE_OID_FIELD(coalescecollid);
	WRITE_NODE_FIELD(args);
	WRITE_LOCATION_FIELD(location);
}

static void
_outMinMaxExpr(StringInfo str, const MinMaxExpr *node)
{
	WRITE_NODE_TYPE("MINMAX");

#ifdef XCP
	if (portable_output)
		WRITE_TYPID_FIELD(minmaxtype);
	else
#endif
	WRITE_OID_FIELD(minmaxtype);
#ifdef XCP
	if (portable_output)
		WRITE_COLLID_FIELD(minmaxcollid);
	else
#endif
	WRITE_OID_FIELD(minmaxcollid);
#ifdef XCP
	if (portable_output)
		WRITE_COLLID_FIELD(inputcollid);
	else
#endif
	WRITE_OID_FIELD(inputcollid);
	WRITE_ENUM_FIELD(op, MinMaxOp);
	WRITE_NODE_FIELD(args);
	WRITE_LOCATION_FIELD(location);
}

static void
_outSQLValueFunction(StringInfo str, const SQLValueFunction *node)
{
	WRITE_NODE_TYPE("SQLVALUEFUNCTION");

	WRITE_ENUM_FIELD(op, SQLValueFunctionOp);
	if (portable_output)
		WRITE_TYPID_FIELD(type);
	else
		WRITE_OID_FIELD(type);
	WRITE_INT_FIELD(typmod);
	WRITE_LOCATION_FIELD(location);
}

static void
_outNextValueExpr(StringInfo str, const NextValueExpr *node)
{
	WRITE_NODE_TYPE("NEXTVALUEEXPR");

	if (portable_output)
	{
		WRITE_RELID_FIELD(seqid);
		WRITE_TYPID_FIELD(typeId);
	}
	else
	{
		WRITE_OID_FIELD(seqid);
		WRITE_OID_FIELD(typeId);
	}
	WRITE_INT_FIELD(typmod);
}
static void
_outXmlExpr(StringInfo str, const XmlExpr *node)
{
	WRITE_NODE_TYPE("XMLEXPR");

	WRITE_ENUM_FIELD(op, XmlExprOp);
	WRITE_STRING_FIELD(name);
	WRITE_NODE_FIELD(named_args);
	WRITE_NODE_FIELD(arg_names);
	WRITE_NODE_FIELD(args);
	WRITE_ENUM_FIELD(xmloption, XmlOptionType);
#ifdef XCP
	if (portable_output)
		WRITE_TYPID_FIELD(type);
	else
#endif
	WRITE_OID_FIELD(type);
	WRITE_INT_FIELD(typmod);
	WRITE_LOCATION_FIELD(location);
}

static void
_outNullTest(StringInfo str, const NullTest *node)
{
	WRITE_NODE_TYPE("NULLTEST");

	WRITE_NODE_FIELD(arg);
	WRITE_ENUM_FIELD(nulltesttype, NullTestType);
	WRITE_BOOL_FIELD(argisrow);
	WRITE_LOCATION_FIELD(location);
}

static void
_outBooleanTest(StringInfo str, const BooleanTest *node)
{
	WRITE_NODE_TYPE("BOOLEANTEST");

	WRITE_NODE_FIELD(arg);
	WRITE_ENUM_FIELD(booltesttype, BoolTestType);
	WRITE_LOCATION_FIELD(location);
}

static void
_outCoerceToDomain(StringInfo str, const CoerceToDomain *node)
{
	WRITE_NODE_TYPE("COERCETODOMAIN");

	WRITE_NODE_FIELD(arg);
#ifdef XCP
	if (portable_output)
		WRITE_TYPID_FIELD(resulttype);
	else
#endif
	WRITE_OID_FIELD(resulttype);
	WRITE_INT_FIELD(resulttypmod);
#ifdef XCP
	if (portable_output)
		WRITE_COLLID_FIELD(resultcollid);
	else
#endif
	WRITE_OID_FIELD(resultcollid);
	WRITE_ENUM_FIELD(coercionformat, CoercionForm);
	WRITE_LOCATION_FIELD(location);
}

static void
_outCoerceToDomainValue(StringInfo str, const CoerceToDomainValue *node)
{
	WRITE_NODE_TYPE("COERCETODOMAINVALUE");

#ifdef XCP
	if (portable_output)
		WRITE_TYPID_FIELD(typeId);
	else
#endif
	WRITE_OID_FIELD(typeId);
	WRITE_INT_FIELD(typeMod);
#ifdef XCP
	if (portable_output)
		WRITE_COLLID_FIELD(collation);
	else
#endif
	WRITE_OID_FIELD(collation);
	WRITE_LOCATION_FIELD(location);
}

static void
_outSetToDefault(StringInfo str, const SetToDefault *node)
{
	WRITE_NODE_TYPE("SETTODEFAULT");

#ifdef XCP
	if (portable_output)
		WRITE_TYPID_FIELD(typeId);
	else
#endif
	WRITE_OID_FIELD(typeId);
	WRITE_INT_FIELD(typeMod);
#ifdef XCP
	if (portable_output)
		WRITE_COLLID_FIELD(collation);
	else
#endif
	WRITE_OID_FIELD(collation);
	WRITE_LOCATION_FIELD(location);
}

static void
_outCurrentOfExpr(StringInfo str, const CurrentOfExpr *node)
{
	WRITE_NODE_TYPE("CURRENTOFEXPR");

	WRITE_UINT_FIELD(cvarno);
	WRITE_STRING_FIELD(cursor_name);
	WRITE_INT_FIELD(cursor_param);
}

static void
_outInferenceElem(StringInfo str, const InferenceElem *node)
{
	WRITE_NODE_TYPE("INFERENCEELEM");

	WRITE_NODE_FIELD(expr);
#ifdef __OPENTENBASE__
	if (portable_output)
	{
		WRITE_COLLID_FIELD(infercollid);
		WRITE_OPCLASS_FIELD(inferopclass);
	}
	else
	{
#endif
	WRITE_OID_FIELD(infercollid);
	WRITE_OID_FIELD(inferopclass);
#ifdef __OPENTENBASE__
	}
#endif
}

static void
_outTargetEntry(StringInfo str, const TargetEntry *node)
{
	WRITE_NODE_TYPE("TARGETENTRY");

	WRITE_NODE_FIELD(expr);
	WRITE_INT_FIELD(resno);
	WRITE_STRING_FIELD(resname);
	WRITE_UINT_FIELD(ressortgroupref);
#ifdef XCP
	if (portable_output)
		WRITE_RELID_FIELD(resorigtbl);
	else
#endif
	WRITE_OID_FIELD(resorigtbl);
	WRITE_INT_FIELD(resorigcol);
	WRITE_BOOL_FIELD(resjunk);
}

static void
_outRangeTblRef(StringInfo str, const RangeTblRef *node)
{
	WRITE_NODE_TYPE("RANGETBLREF");

	WRITE_INT_FIELD(rtindex);
}

static void
_outJoinExpr(StringInfo str, const JoinExpr *node)
{
	WRITE_NODE_TYPE("JOINEXPR");

	WRITE_ENUM_FIELD(jointype, JoinType);
	WRITE_BOOL_FIELD(isNatural);
	WRITE_NODE_FIELD(larg);
	WRITE_NODE_FIELD(rarg);
	WRITE_NODE_FIELD(usingClause);
	WRITE_NODE_FIELD(quals);
	WRITE_NODE_FIELD(alias);
	WRITE_INT_FIELD(rtindex);
}

static void
_outFromExpr(StringInfo str, const FromExpr *node)
{
	WRITE_NODE_TYPE("FROMEXPR");

	WRITE_NODE_FIELD(fromlist);
	WRITE_NODE_FIELD(quals);
}

static void
_outMergeAction(StringInfo str, const MergeAction* node)
{
	WRITE_NODE_TYPE("MERGEACTION");

	WRITE_BOOL_FIELD(matched);
	WRITE_ENUM_FIELD(commandType, CmdType);
	WRITE_ENUM_FIELD(override, OverridingKind);
	WRITE_NODE_FIELD(qual);
	WRITE_NODE_FIELD(targetList);
	WRITE_NODE_FIELD(updateColnos);
    WRITE_BOOL_FIELD(actionOnly);
	WRITE_NODE_FIELD(deleteAction);
}

static void
_outOnConflictExpr(StringInfo str, const OnConflictExpr *node)
{
	WRITE_NODE_TYPE("ONCONFLICTEXPR");

	WRITE_ENUM_FIELD(action, OnConflictAction);
	WRITE_NODE_FIELD(arbiterElems);
	WRITE_NODE_FIELD(arbiterWhere);
#ifdef __OPENTENBASE__
	if (portable_output)
	{
		WRITE_CONSTRAINT_FIELD(constraint);
	}
	else
#endif
	WRITE_OID_FIELD(constraint);
	WRITE_NODE_FIELD(onConflictSet);
	WRITE_NODE_FIELD(onConflictWhere);
	WRITE_INT_FIELD(exclRelIndex);
	WRITE_NODE_FIELD(exclRelTlist);
}


/*****************************************************************************
 *
 *	Stuff from relation.h.
 *
 *****************************************************************************/

/*
 * print the basic stuff of all nodes that inherit from Path
 *
 * Note we do NOT print the parent, else we'd be in infinite recursion.
 * We can print the parent's relids for identification purposes, though.
 * We print the pathtarget only if it's not the default one for the rel.
 * We also do not print the whole of param_info, since it's printed by
 * _outRelOptInfo; it's sufficient and less cluttering to print just the
 * required outer relids.
 */
static void
_outPathInfo(StringInfo str, const Path *node)
{
	WRITE_ENUM_FIELD(pathtype, NodeTag);
	appendStringInfoString(str, " :parent_relids ");
	outBitmapset(str, node->parent->relids);
	if (node->pathtarget != node->parent->reltarget)
		WRITE_NODE_FIELD(pathtarget);
	appendStringInfoString(str, " :required_outer ");
	if (node->param_info)
		outBitmapset(str, node->param_info->ppi_req_outer);
	else
		outBitmapset(str, NULL);
	WRITE_BOOL_FIELD(parallel_aware);
	WRITE_BOOL_FIELD(parallel_safe);
	WRITE_INT_FIELD(parallel_workers);
	WRITE_FLOAT_FIELD(rows, "%.0f");
	WRITE_FLOAT_FIELD(startup_cost, "%.2f");
	WRITE_FLOAT_FIELD(total_cost, "%.2f");
	WRITE_NODE_FIELD(pathkeys);
}

/*
 * print the basic stuff of all nodes that inherit from JoinPath
 */
static void
_outJoinPathInfo(StringInfo str, const JoinPath *node)
{
	_outPathInfo(str, (const Path *) node);

	WRITE_ENUM_FIELD(jointype, JoinType);
	WRITE_BOOL_FIELD(inner_unique);
	WRITE_NODE_FIELD(outerjoinpath);
	WRITE_NODE_FIELD(innerjoinpath);
	WRITE_NODE_FIELD(joinrestrictinfo);
	WRITE_NODE_FIELD(movedrestrictinfo);
}

static void
_outPath(StringInfo str, const Path *node)
{
	WRITE_NODE_TYPE("PATH");

	_outPathInfo(str, (const Path *) node);
}

static void
_outIndexPath(StringInfo str, const IndexPath *node)
{
	WRITE_NODE_TYPE("INDEXPATH");

	_outPathInfo(str, (const Path *) node);

	WRITE_NODE_FIELD(indexinfo);
	WRITE_NODE_FIELD(indexclauses);
	WRITE_NODE_FIELD(indexquals);
	WRITE_NODE_FIELD(indexqualcols);
	WRITE_NODE_FIELD(indexorderbys);
	WRITE_NODE_FIELD(indexorderbycols);
	WRITE_ENUM_FIELD(indexscandir, ScanDirection);
	WRITE_FLOAT_FIELD(indextotalcost, "%.2f");
	WRITE_FLOAT_FIELD(indexselectivity, "%.4f");
}

static void
_outBitmapHeapPath(StringInfo str, const BitmapHeapPath *node)
{
	WRITE_NODE_TYPE("BITMAPHEAPPATH");

	_outPathInfo(str, (const Path *) node);

	WRITE_NODE_FIELD(bitmapqual);
}

static void
_outBitmapAndPath(StringInfo str, const BitmapAndPath *node)
{
	WRITE_NODE_TYPE("BITMAPANDPATH");

	_outPathInfo(str, (const Path *) node);

	WRITE_NODE_FIELD(bitmapquals);
	WRITE_FLOAT_FIELD(bitmapselectivity, "%.4f");
}

static void
_outBitmapOrPath(StringInfo str, const BitmapOrPath *node)
{
	WRITE_NODE_TYPE("BITMAPORPATH");

	_outPathInfo(str, (const Path *) node);

	WRITE_NODE_FIELD(bitmapquals);
	WRITE_FLOAT_FIELD(bitmapselectivity, "%.4f");
}

static void
_outTidPath(StringInfo str, const TidPath *node)
{
	WRITE_NODE_TYPE("TIDPATH");

	_outPathInfo(str, (const Path *) node);

	WRITE_NODE_FIELD(tidquals);
}

static void
_outSubqueryScanPath(StringInfo str, const SubqueryScanPath *node)
{
	WRITE_NODE_TYPE("SUBQUERYSCANPATH");

	_outPathInfo(str, (const Path *) node);

	WRITE_NODE_FIELD(subpath);
}

static void
_outForeignPath(StringInfo str, const ForeignPath *node)
{
	WRITE_NODE_TYPE("FOREIGNPATH");

	_outPathInfo(str, (const Path *) node);

	WRITE_NODE_FIELD(fdw_outerpath);
	WRITE_NODE_FIELD(fdw_private);
}

static void
_outCustomPath(StringInfo str, const CustomPath *node)
{
	WRITE_NODE_TYPE("CUSTOMPATH");

	_outPathInfo(str, (const Path *) node);

	WRITE_UINT_FIELD(flags);
	WRITE_NODE_FIELD(custom_paths);
	WRITE_NODE_FIELD(custom_private);
	appendStringInfoString(str, " :methods ");
	outToken(str, node->methods->CustomName);
}

static void
_outPartIteratorPath(StringInfo str, const PartIteratorPath *node)
{
	WRITE_NODE_TYPE("PARTITERATORPATH");

	_outPathInfo(str, (const Path *) node);
	WRITE_NODE_FIELD(subPath);
	WRITE_INT_FIELD(leafNum);
	WRITE_ENUM_FIELD(direction, ScanDirection);
	WRITE_BOOL_FIELD(ispwj);
	WRITE_NODE_FIELD(partitioned_rels);
}

static void
_outAppendPath(StringInfo str, const AppendPath *node)
{
	WRITE_NODE_TYPE("APPENDPATH");

	_outPathInfo(str, (const Path *) node);

	WRITE_NODE_FIELD(partitioned_rels);
	WRITE_NODE_FIELD(subpaths);
#ifdef __OPENTENBASE_C__
	WRITE_BOOL_FIELD(grouping_append);
	WRITE_NODE_FIELD(sharedPath);
	WRITE_NODE_FIELD(sharedPlan);
	WRITE_NODE_FIELD(sharedCtePath);
#endif
}

static void
_outMergeAppendPath(StringInfo str, const MergeAppendPath *node)
{
	WRITE_NODE_TYPE("MERGEAPPENDPATH");

	_outPathInfo(str, (const Path *) node);

	WRITE_NODE_FIELD(partitioned_rels);
	WRITE_NODE_FIELD(subpaths);
	WRITE_FLOAT_FIELD(limit_tuples, "%.0f");
}

static void
_outResultPath(StringInfo str, const ResultPath *node)
{
	WRITE_NODE_TYPE("RESULTPATH");

	_outPathInfo(str, (const Path *) node);

	WRITE_NODE_FIELD(quals);
}

static void
_outMaterialPath(StringInfo str, const MaterialPath *node)
{
	WRITE_NODE_TYPE("MATERIALPATH");

	_outPathInfo(str, (const Path *) node);

	WRITE_NODE_FIELD(subpath);
}

static void
_outUniquePath(StringInfo str, const UniquePath *node)
{
	WRITE_NODE_TYPE("UNIQUEPATH");

	_outPathInfo(str, (const Path *) node);

	WRITE_NODE_FIELD(subpath);
	WRITE_ENUM_FIELD(umethod, UniquePathMethod);
	WRITE_NODE_FIELD(in_operators);
	WRITE_NODE_FIELD(uniq_exprs);
}

static void
_outGatherPath(StringInfo str, const GatherPath *node)
{
	WRITE_NODE_TYPE("GATHERPATH");

	_outPathInfo(str, (const Path *) node);

	WRITE_NODE_FIELD(subpath);
	WRITE_BOOL_FIELD(single_copy);
	WRITE_INT_FIELD(num_workers);
}

static void
_outProjectionPath(StringInfo str, const ProjectionPath *node)
{
	WRITE_NODE_TYPE("PROJECTIONPATH");

	_outPathInfo(str, (const Path *) node);

	WRITE_NODE_FIELD(subpath);
	WRITE_BOOL_FIELD(dummypp);
}

static void
_outProjectSetPath(StringInfo str, const ProjectSetPath *node)
{
	WRITE_NODE_TYPE("PROJECTSETPATH");

	_outPathInfo(str, (const Path *) node);

	WRITE_NODE_FIELD(subpath);
}

static void
_outQualPath(StringInfo str, const QualPath *node)
{
	WRITE_NODE_TYPE("QUALPATH");

	_outPathInfo(str, (const Path *) node);

	WRITE_NODE_FIELD(subpath);
	WRITE_NODE_FIELD(quals);
}

static void
_outSortPath(StringInfo str, const SortPath *node)
{
	WRITE_NODE_TYPE("SORTPATH");

	_outPathInfo(str, (const Path *) node);

	WRITE_NODE_FIELD(subpath);
#ifdef __OPENTENBASE_C__
	WRITE_NODE_FIELD(groupClause);
#endif
}

static void
_outGroupPath(StringInfo str, const GroupPath *node)
{
	WRITE_NODE_TYPE("GROUPPATH");

	_outPathInfo(str, (const Path *) node);

	WRITE_NODE_FIELD(subpath);
	WRITE_NODE_FIELD(groupClause);
	WRITE_NODE_FIELD(qual);
}

static void
_outUpperUniquePath(StringInfo str, const UpperUniquePath *node)
{
	WRITE_NODE_TYPE("UPPERUNIQUEPATH");

	_outPathInfo(str, (const Path *) node);

	WRITE_NODE_FIELD(subpath);
	WRITE_INT_FIELD(numkeys);
}

static void
_outAggPath(StringInfo str, const AggPath *node)
{
	WRITE_NODE_TYPE("AGGPATH");

	_outPathInfo(str, (const Path *) node);

	WRITE_NODE_FIELD(subpath);
	WRITE_ENUM_FIELD(aggstrategy, AggStrategy);
	WRITE_ENUM_FIELD(aggsplit, AggSplit);
	WRITE_FLOAT_FIELD(numGroups, "%.0f");
	WRITE_NODE_FIELD(groupClause);
	WRITE_NODE_FIELD(qual);
#ifdef __OPENTENBASE_C__
	WRITE_BOOL_FIELD(groupingFunc);
#endif
}

static void
_outRollupData(StringInfo str, const RollupData *node)
{
	WRITE_NODE_TYPE("ROLLUP");

	WRITE_NODE_FIELD(groupClause);
	WRITE_NODE_FIELD(gsets);
	WRITE_NODE_FIELD(gsets_data);
	WRITE_FLOAT_FIELD(numGroups, "%.0f");
	WRITE_BOOL_FIELD(hashable);
	WRITE_BOOL_FIELD(is_hashed);
}

static void
_outGroupingSetData(StringInfo str, const GroupingSetData *node)
{
	WRITE_NODE_TYPE("GSDATA");

	WRITE_NODE_FIELD(set);
	WRITE_FLOAT_FIELD(numGroups, "%.0f");
}

static void
_outGroupingSetsPath(StringInfo str, const GroupingSetsPath *node)
{
	WRITE_NODE_TYPE("GROUPINGSETSPATH");

	_outPathInfo(str, (const Path *) node);

	WRITE_NODE_FIELD(subpath);
	WRITE_ENUM_FIELD(aggstrategy, AggStrategy);
	WRITE_NODE_FIELD(rollups);
	WRITE_NODE_FIELD(qual);
}

static void
_outMinMaxAggPath(StringInfo str, const MinMaxAggPath *node)
{
	WRITE_NODE_TYPE("MINMAXAGGPATH");

	_outPathInfo(str, (const Path *) node);

	WRITE_NODE_FIELD(mmaggregates);
	WRITE_NODE_FIELD(quals);
}

static void
_outWindowAggPath(StringInfo str, const WindowAggPath *node)
{
	WRITE_NODE_TYPE("WINDOWAGGPATH");

	_outPathInfo(str, (const Path *) node);

	WRITE_NODE_FIELD(subpath);
	WRITE_NODE_FIELD(winclause);
}

static void
_outSetOpPath(StringInfo str, const SetOpPath *node)
{
	WRITE_NODE_TYPE("SETOPPATH");

	_outPathInfo(str, (const Path *) node);

	WRITE_NODE_FIELD(subpath);
	WRITE_ENUM_FIELD(cmd, SetOpCmd);
	WRITE_ENUM_FIELD(strategy, SetOpStrategy);
	WRITE_NODE_FIELD(distinctList);
	WRITE_INT_FIELD(flagColIdx);
	WRITE_INT_FIELD(firstFlag);
	WRITE_FLOAT_FIELD(numGroups, "%.0f");
}

static void
_outRecursiveUnionPath(StringInfo str, const RecursiveUnionPath *node)
{
	WRITE_NODE_TYPE("RECURSIVEUNIONPATH");

	_outPathInfo(str, (const Path *) node);

	WRITE_NODE_FIELD(leftpath);
	WRITE_NODE_FIELD(rightpath);
	WRITE_NODE_FIELD(distinctList);
	WRITE_INT_FIELD(wtParam);
	WRITE_FLOAT_FIELD(numGroups, "%.0f");
}

static void
_outLockRowsPath(StringInfo str, const LockRowsPath *node)
{
	WRITE_NODE_TYPE("LOCKROWSPATH");

	_outPathInfo(str, (const Path *) node);

	WRITE_NODE_FIELD(subpath);
	WRITE_NODE_FIELD(rowMarks);
	WRITE_INT_FIELD(epqParam);
}

static void
_outModifyTablePath(StringInfo str, const ModifyTablePath *node)
{
	WRITE_NODE_TYPE("MODIFYTABLEPATH");

	_outPathInfo(str, (const Path *) node);

	WRITE_NODE_FIELD(subpath);
	WRITE_ENUM_FIELD(operation, CmdType);
	WRITE_BOOL_FIELD(canSetTag);
	WRITE_UINT_FIELD(nominalRelation);
	WRITE_NODE_FIELD(partitioned_rels);
	WRITE_BOOL_FIELD(partColsUpdated);
	WRITE_NODE_FIELD(resultRelations);
	WRITE_NODE_FIELD(updateColnosLists);
	WRITE_NODE_FIELD(withCheckOptionLists);
	WRITE_NODE_FIELD(returningLists);
	WRITE_NODE_FIELD(rowMarks);
	WRITE_NODE_FIELD(onconflict);
	WRITE_INT_FIELD(epqParam);
}

static void
_outLimitPath(StringInfo str, const LimitPath *node)
{
	WRITE_NODE_TYPE("LIMITPATH");

	_outPathInfo(str, (const Path *) node);

	WRITE_NODE_FIELD(subpath);
	WRITE_NODE_FIELD(limitOffset);
	WRITE_NODE_FIELD(limitCount);
}

static void
_outGatherMergePath(StringInfo str, const GatherMergePath *node)
{
	WRITE_NODE_TYPE("GATHERMERGEPATH");

	_outPathInfo(str, (const Path *) node);

	WRITE_NODE_FIELD(subpath);
	WRITE_INT_FIELD(num_workers);
}

static void
_outNestPath(StringInfo str, const NestPath *node)
{
	WRITE_NODE_TYPE("NESTPATH");

	_outJoinPathInfo(str, (const JoinPath *) node);
}

static void
_outMergePath(StringInfo str, const MergePath *node)
{
	WRITE_NODE_TYPE("MERGEPATH");

	_outJoinPathInfo(str, (const JoinPath *) node);

	WRITE_NODE_FIELD(path_mergeclauses);
	WRITE_NODE_FIELD(outersortkeys);
	WRITE_NODE_FIELD(innersortkeys);
	WRITE_BOOL_FIELD(skip_mark_restore);
	WRITE_BOOL_FIELD(materialize_inner);
}

static void
_outHashPath(StringInfo str, const HashPath *node)
{
	WRITE_NODE_TYPE("HASHPATH");

	_outJoinPathInfo(str, (const JoinPath *) node);

	WRITE_NODE_FIELD(path_hashclauses);
	WRITE_INT_FIELD(num_batches);
}


static void
_outPlannerGlobal(StringInfo str, const PlannerGlobal *node)
{
	WRITE_NODE_TYPE("PLANNERGLOBAL");

	/* NB: this isn't a complete set of fields */
	WRITE_NODE_FIELD(subplans);
	WRITE_BITMAPSET_FIELD(rewindPlanIDs);
	WRITE_NODE_FIELD(finalrtable);
	WRITE_NODE_FIELD(finalrowmarks);
	WRITE_NODE_FIELD(resultRelations);
	WRITE_NODE_FIELD(nonleafResultRelations);
	WRITE_NODE_FIELD(rootResultRelations);
	WRITE_NODE_FIELD(relationOids);
	WRITE_NODE_FIELD(invalItems);
	WRITE_NODE_FIELD(paramExecTypes);
	WRITE_UINT_FIELD(lastPHId);
	WRITE_UINT_FIELD(lastRowMarkId);
	WRITE_INT_FIELD(lastPlanNodeId);
	WRITE_BOOL_FIELD(transientPlan);
	WRITE_BOOL_FIELD(dependsOnRole);
	WRITE_BOOL_FIELD(parallelModeOK);
	WRITE_BOOL_FIELD(parallelModeNeeded);
	WRITE_CHAR_FIELD(maxParallelHazard);
#ifdef __OPENTENBASE_C__
	WRITE_NODE_FIELD(sharedCtePlans);
	WRITE_BITMAPSET_FIELD(sharedCtePlanIds);
	WRITE_INT_FIELD(fragmentNum);
#endif
	/* begin ora_compatible */
	WRITE_NODE_FIELD(subInsertPlans);
	WRITE_BOOL_FIELD(is_multi_insert);
	/* end ora_compatible */
}

static void
_outPlannerInfo(StringInfo str, const PlannerInfo *node)
{
	WRITE_NODE_TYPE("PLANNERINFO");

	/* NB: this isn't a complete set of fields */
	WRITE_NODE_FIELD(parse);
	WRITE_NODE_FIELD(glob);
	WRITE_UINT_FIELD(query_level);
	WRITE_NODE_FIELD(plan_params);
	WRITE_BITMAPSET_FIELD(outer_params);
	WRITE_BITMAPSET_FIELD(all_baserels);
	WRITE_BITMAPSET_FIELD(nullable_baserels);
	WRITE_NODE_FIELD(join_rel_list);
	WRITE_INT_FIELD(join_cur_level);
	WRITE_NODE_FIELD(init_plans);
	WRITE_NODE_FIELD(cte_plan_ids);
	WRITE_NODE_FIELD(multiexpr_params);
	WRITE_NODE_FIELD(eq_classes);
	WRITE_NODE_FIELD(canon_pathkeys);
	WRITE_NODE_FIELD(left_join_clauses);
	WRITE_NODE_FIELD(right_join_clauses);
	WRITE_NODE_FIELD(full_join_clauses);
	WRITE_NODE_FIELD(join_info_list);
	WRITE_BITMAPSET_FIELD(all_result_relids);
	WRITE_BITMAPSET_FIELD(leaf_result_relids);
	WRITE_NODE_FIELD(append_rel_list);
	WRITE_NODE_FIELD(row_identity_vars);
	WRITE_NODE_FIELD(rowMarks);
	WRITE_NODE_FIELD(placeholder_list);
	WRITE_NODE_FIELD(fkey_list);
	WRITE_NODE_FIELD(query_pathkeys);
	WRITE_NODE_FIELD(group_pathkeys);
	WRITE_NODE_FIELD(window_pathkeys);
	WRITE_NODE_FIELD(distinct_pathkeys);
	WRITE_NODE_FIELD(sort_pathkeys);
	WRITE_NODE_FIELD(processed_tlist);
	WRITE_NODE_FIELD(update_colnos);
	WRITE_NODE_FIELD(minmax_aggs);
	WRITE_FLOAT_FIELD(total_table_pages, "%.0f");
	WRITE_FLOAT_FIELD(tuple_fraction, "%.4f");
	WRITE_FLOAT_FIELD(limit_tuples, "%.0f");
	WRITE_UINT_FIELD(qual_security_level);
	WRITE_BOOL_FIELD(hasJoinRTEs);
	WRITE_BOOL_FIELD(hasLateralRTEs);
	WRITE_BOOL_FIELD(hasDeletedRTEs);
	WRITE_BOOL_FIELD(hasHavingQual);
	WRITE_BOOL_FIELD(hasPseudoConstantQuals);
	WRITE_BOOL_FIELD(hasRecursion);
	WRITE_INT_FIELD(wt_param_id);
	WRITE_BITMAPSET_FIELD(curOuterRels);
	WRITE_NODE_FIELD(curOuterParams);
	WRITE_BOOL_FIELD(partColsUpdated);
}

static void
_outRelOptInfo(StringInfo str, const RelOptInfo *node)
{
	WRITE_NODE_TYPE("RELOPTINFO");

	/* NB: this isn't a complete set of fields */
	WRITE_ENUM_FIELD(reloptkind, RelOptKind);
	WRITE_BITMAPSET_FIELD(relids);
	WRITE_FLOAT_FIELD(rows, "%.0f");
	WRITE_BOOL_FIELD(consider_startup);
	WRITE_BOOL_FIELD(consider_param_startup);
	WRITE_BOOL_FIELD(consider_parallel);
	WRITE_NODE_FIELD(reltarget);
	WRITE_NODE_FIELD(pathlist);
	WRITE_NODE_FIELD(ppilist);
	WRITE_NODE_FIELD(partial_pathlist);
	WRITE_NODE_FIELD(cheapest_startup_path);
	WRITE_NODE_FIELD(cheapest_total_path);
	WRITE_NODE_FIELD(cheapest_unique_path);
	WRITE_NODE_FIELD(cheapest_parameterized_paths);
	WRITE_BITMAPSET_FIELD(direct_lateral_relids);
	WRITE_BITMAPSET_FIELD(lateral_relids);
	WRITE_UINT_FIELD(relid);
	WRITE_OID_FIELD(reltablespace);
	WRITE_ENUM_FIELD(rtekind, RTEKind);
	WRITE_INT_FIELD(min_attr);
	WRITE_INT_FIELD(max_attr);
	WRITE_NODE_FIELD(lateral_vars);
	WRITE_BITMAPSET_FIELD(lateral_referencers);
	WRITE_NODE_FIELD(indexlist);
	WRITE_NODE_FIELD(statlist);
	WRITE_UINT_FIELD(pages);
	WRITE_FLOAT_FIELD(tuples, "%.0f");
	WRITE_FLOAT_FIELD(allvisfrac, "%.6f");
	WRITE_NODE_FIELD(subroot);
	WRITE_NODE_FIELD(subplan_params);
	WRITE_INT_FIELD(rel_parallel_workers);
	WRITE_OID_FIELD(serverid);
	WRITE_OID_FIELD(userid);
	WRITE_BOOL_FIELD(useridiscurrent);
	/* we don't try to print fdwroutine or fdw_private */
	/* can't print unique_for_rels/non_unique_for_rels; BMSes aren't Nodes */
	WRITE_NODE_FIELD(baserestrictinfo);
	WRITE_UINT_FIELD(baserestrict_min_security);
	WRITE_NODE_FIELD(joininfo);
	WRITE_BOOL_FIELD(has_eclass_joins);
	WRITE_BOOL_FIELD(consider_partitionwise_join);
	WRITE_BITMAPSET_FIELD(top_parent_relids);
	WRITE_NODE_FIELD(partitioned_child_rels);
#ifdef __OPENTENBASE__
    WRITE_ENUM_FIELD(resultRelLoc, ResultRelLocation);
#endif
#ifdef __OPENTENBASE_C__
	WRITE_BOOL_FIELD(consider_latematerial);
	WRITE_NODE_FIELD(baserel_attr_masks);
#endif
}

static void
_outIndexOptInfo(StringInfo str, const IndexOptInfo *node)
{
	WRITE_NODE_TYPE("INDEXOPTINFO");

	/* NB: this isn't a complete set of fields */
	WRITE_OID_FIELD(indexoid);
	/* Do NOT print rel field, else infinite recursion */
	WRITE_UINT_FIELD(pages);
	WRITE_FLOAT_FIELD(tuples, "%.0f");
	WRITE_INT_FIELD(tree_height);
	WRITE_INT_FIELD(ncolumns);
	/* array fields aren't really worth the trouble to print */
	WRITE_OID_FIELD(relam);
	/* indexprs is redundant since we print indextlist */
	WRITE_NODE_FIELD(indpred);
	WRITE_NODE_FIELD(indextlist);
	WRITE_NODE_FIELD(indrestrictinfo);
	WRITE_BOOL_FIELD(predOK);
	WRITE_BOOL_FIELD(unique);
	WRITE_BOOL_FIELD(immediate);
	WRITE_BOOL_FIELD(hypothetical);
	/* we don't bother with fields copied from the index AM's API struct */
}

static void
_outForeignKeyOptInfo(StringInfo str, const ForeignKeyOptInfo *node)
{
	int			i;

	WRITE_NODE_TYPE("FOREIGNKEYOPTINFO");

	WRITE_UINT_FIELD(con_relid);
	WRITE_UINT_FIELD(ref_relid);
	WRITE_INT_FIELD(nkeys);
	WRITE_ATTRNUMBER_ARRAY(conkey, node->nkeys);
	WRITE_ATTRNUMBER_ARRAY(confkey, node->nkeys);
	WRITE_OID_ARRAY(conpfeqop, node->nkeys);
	WRITE_INT_FIELD(nmatched_ec);
	WRITE_INT_FIELD(nmatched_rcols);
	WRITE_INT_FIELD(nmatched_ri);
	/* for compactness, just print the number of matches per column: */
	appendStringInfoString(str, " :eclass");
	for (i = 0; i < node->nkeys; i++)
		appendStringInfo(str, " %d", (node->eclass[i] != NULL));
	appendStringInfoString(str, " :rinfos");
	for (i = 0; i < node->nkeys; i++)
		appendStringInfo(str, " %d", list_length(node->rinfos[i]));
}

static void
_outStatisticExtInfo(StringInfo str, const StatisticExtInfo *node)
{
	WRITE_NODE_TYPE("STATISTICEXTINFO");

	/* NB: this isn't a complete set of fields */
	WRITE_OID_FIELD(statOid);
	/* don't write rel, leads to infinite recursion in plan tree dump */
	WRITE_CHAR_FIELD(kind);
	WRITE_BITMAPSET_FIELD(keys);
}

static void
_outEquivalenceClass(StringInfo str, const EquivalenceClass *node)
{
	/*
	 * To simplify reading, we just chase up to the topmost merged EC and
	 * print that, without bothering to show the merge-ees separately.
	 */
	while (node->ec_merged)
		node = node->ec_merged;

	WRITE_NODE_TYPE("EQUIVALENCECLASS");

	WRITE_NODE_FIELD(ec_opfamilies);
#ifdef XCP
	if (portable_output)
		WRITE_COLLID_FIELD(ec_collation);
	else
#endif
	WRITE_OID_FIELD(ec_collation);
	WRITE_NODE_FIELD(ec_members);
	WRITE_NODE_FIELD(ec_sources);
	WRITE_NODE_FIELD(ec_derives);
	WRITE_BITMAPSET_FIELD(ec_relids);
	WRITE_BOOL_FIELD(ec_has_const);
	WRITE_BOOL_FIELD(ec_has_volatile);
	WRITE_BOOL_FIELD(ec_below_outer_join);
	WRITE_BOOL_FIELD(ec_broken);
	WRITE_UINT_FIELD(ec_sortref);
	WRITE_UINT_FIELD(ec_min_security);
	WRITE_UINT_FIELD(ec_max_security);
}

static void
_outEquivalenceMember(StringInfo str, const EquivalenceMember *node)
{
	WRITE_NODE_TYPE("EQUIVALENCEMEMBER");

	WRITE_NODE_FIELD(em_expr);
	WRITE_BITMAPSET_FIELD(em_relids);
	WRITE_BITMAPSET_FIELD(em_nullable_relids);
	WRITE_BOOL_FIELD(em_is_const);
	WRITE_BOOL_FIELD(em_is_child);
#ifdef __OPENTENBASE__
	if (portable_output)
	{
		WRITE_TYPID_FIELD(em_datatype);
	}
	else
#endif
	WRITE_OID_FIELD(em_datatype);
}

static void
_outPathKey(StringInfo str, const PathKey *node)
{
	WRITE_NODE_TYPE("PATHKEY");

	WRITE_NODE_FIELD(pk_eclass);
	WRITE_OID_FIELD(pk_opfamily);
	WRITE_INT_FIELD(pk_strategy);
	WRITE_BOOL_FIELD(pk_nulls_first);
}

static void
_outPathTarget(StringInfo str, const PathTarget *node)
{
	WRITE_NODE_TYPE("PATHTARGET");

	WRITE_NODE_FIELD(exprs);
	if (node->sortgrouprefs)
	{
		int			i;

		appendStringInfoString(str, " :sortgrouprefs");
		for (i = 0; i < list_length(node->exprs); i++)
			appendStringInfo(str, " %u", node->sortgrouprefs[i]);
	}
	WRITE_FLOAT_FIELD(cost.startup, "%.2f");
	WRITE_FLOAT_FIELD(cost.per_tuple, "%.2f");
	WRITE_INT_FIELD(width);
}

static void
_outParamPathInfo(StringInfo str, const ParamPathInfo *node)
{
	WRITE_NODE_TYPE("PARAMPATHINFO");

	WRITE_BITMAPSET_FIELD(ppi_req_outer);
	WRITE_FLOAT_FIELD(ppi_rows, "%.0f");
	WRITE_NODE_FIELD(ppi_clauses);
}

static void
_outRestrictInfo(StringInfo str, const RestrictInfo *node)
{
	WRITE_NODE_TYPE("RESTRICTINFO");

	/* NB: this isn't a complete set of fields */
	WRITE_NODE_FIELD(clause);
	WRITE_BOOL_FIELD(is_pushed_down);
	WRITE_BOOL_FIELD(outerjoin_delayed);
	WRITE_BOOL_FIELD(can_join);
	WRITE_BOOL_FIELD(pseudoconstant);
	WRITE_BOOL_FIELD(leakproof);
	WRITE_UINT_FIELD(security_level);
	WRITE_BITMAPSET_FIELD(clause_relids);
	WRITE_BITMAPSET_FIELD(required_relids);
	WRITE_BITMAPSET_FIELD(outer_relids);
	WRITE_BITMAPSET_FIELD(nullable_relids);
	WRITE_BITMAPSET_FIELD(left_relids);
	WRITE_BITMAPSET_FIELD(right_relids);
	WRITE_NODE_FIELD(orclause);
	/* don't write parent_ec, leads to infinite recursion in plan tree dump */
	WRITE_FLOAT_FIELD(norm_selec, "%.4f");
	WRITE_FLOAT_FIELD(outer_selec, "%.4f");
	WRITE_NODE_FIELD(mergeopfamilies);
	/* don't write left_ec, leads to infinite recursion in plan tree dump */
	/* don't write right_ec, leads to infinite recursion in plan tree dump */
	WRITE_NODE_FIELD(left_em);
	WRITE_NODE_FIELD(right_em);
	WRITE_BOOL_FIELD(outer_is_left);
	WRITE_OID_FIELD(hashjoinoperator);
	WRITE_NODE_FIELD(retain_value_left);
	WRITE_NODE_FIELD(retain_value_right);
}

static void
_outPlaceHolderVar(StringInfo str, const PlaceHolderVar *node)
{
	WRITE_NODE_TYPE("PLACEHOLDERVAR");

	WRITE_NODE_FIELD(phexpr);
	WRITE_BITMAPSET_FIELD(phrels);
	WRITE_UINT_FIELD(phid);
	WRITE_UINT_FIELD(phlevelsup);
}

static void
_outSpecialJoinInfo(StringInfo str, const SpecialJoinInfo *node)
{
	WRITE_NODE_TYPE("SPECIALJOININFO");

	WRITE_BITMAPSET_FIELD(min_lefthand);
	WRITE_BITMAPSET_FIELD(min_righthand);
	WRITE_BITMAPSET_FIELD(syn_lefthand);
	WRITE_BITMAPSET_FIELD(syn_righthand);
	WRITE_ENUM_FIELD(jointype, JoinType);
	WRITE_BOOL_FIELD(lhs_strict);
	WRITE_BOOL_FIELD(delay_upper_joins);
	WRITE_BOOL_FIELD(semi_can_btree);
	WRITE_BOOL_FIELD(semi_can_hash);
	WRITE_NODE_FIELD(semi_operators);
	WRITE_NODE_FIELD(semi_rhs_exprs);
}

static void
_outAppendRelInfo(StringInfo str, const AppendRelInfo *node)
{
	WRITE_NODE_TYPE("APPENDRELINFO");

	WRITE_UINT_FIELD(parent_relid);
	WRITE_UINT_FIELD(child_relid);
	WRITE_OID_FIELD(parent_reltype);
	WRITE_OID_FIELD(child_reltype);
	WRITE_NODE_FIELD(translated_vars);
#ifdef XCP
	if (portable_output)
		WRITE_RELID_FIELD(parent_reloid);
	else
#endif
	WRITE_OID_FIELD(parent_reloid);
}

static void
_outRowIdentityVarInfo(StringInfo str, const RowIdentityVarInfo *node)
{
	WRITE_NODE_TYPE("ROWIDENTITYVARINFO");

	WRITE_NODE_FIELD(rowidvar);
	WRITE_INT_FIELD(rowidwidth);
	WRITE_STRING_FIELD(rowidname);
	WRITE_BITMAPSET_FIELD(rowidrels);
}

static void
_outPlaceHolderInfo(StringInfo str, const PlaceHolderInfo *node)
{
	WRITE_NODE_TYPE("PLACEHOLDERINFO");

	WRITE_UINT_FIELD(phid);
	WRITE_NODE_FIELD(ph_var);
	WRITE_BITMAPSET_FIELD(ph_eval_at);
	WRITE_BITMAPSET_FIELD(ph_lateral);
	WRITE_BITMAPSET_FIELD(ph_needed);
	WRITE_INT_FIELD(ph_width);
}

static void
_outMinMaxAggInfo(StringInfo str, const MinMaxAggInfo *node)
{
	WRITE_NODE_TYPE("MINMAXAGGINFO");

#ifdef __OPENTENBASE__
	if (portable_output)
	{
		WRITE_FUNCID_FIELD(aggfnoid);
		WRITE_OPERID_FIELD(aggsortop);
	}
	else
	{
#endif
	WRITE_OID_FIELD(aggfnoid);
	WRITE_OID_FIELD(aggsortop);
#ifdef __OPENTENBASE__
	}
#endif
	WRITE_NODE_FIELD(target);
	/* We intentionally omit subroot --- too large, not interesting enough */
	WRITE_NODE_FIELD(path);
	WRITE_FLOAT_FIELD(pathcost, "%.2f");
	WRITE_NODE_FIELD(param);
}

static void
_outPlannerParamItem(StringInfo str, const PlannerParamItem *node)
{
	WRITE_NODE_TYPE("PLANNERPARAMITEM");

	WRITE_NODE_FIELD(item);
	WRITE_INT_FIELD(paramId);
}

/*****************************************************************************
 *
 *	Stuff from extensible.h
 *
 *****************************************************************************/

static void
_outExtensibleNode(StringInfo str, const ExtensibleNode *node)
{
	const ExtensibleNodeMethods *methods;

	methods = GetExtensibleNodeMethods(node->extnodename, false);

	WRITE_NODE_TYPE("EXTENSIBLENODE");

	WRITE_STRING_FIELD(extnodename);

	/* serialize the private fields */
	methods->nodeOut(str, node);
}

/*****************************************************************************
 *
 *	Stuff from parsenodes.h.
 *
 *****************************************************************************/

/*
 * print the basic stuff of all nodes that inherit from CreateStmt
 */
static void
_outCreateStmtInfo(StringInfo str, const CreateStmt *node)
{
	WRITE_NODE_FIELD(relation);
	WRITE_NODE_FIELD(tableElts);
	WRITE_NODE_FIELD(inhRelations);
	WRITE_NODE_FIELD(partspec);
	WRITE_NODE_FIELD(partbound);
	WRITE_NODE_FIELD(ofTypename);
	WRITE_NODE_FIELD(constraints);
	WRITE_NODE_FIELD(options);
	WRITE_ENUM_FIELD(oncommit, OnCommitAction);
	WRITE_STRING_FIELD(tablespacename);
	WRITE_BOOL_FIELD(if_not_exists);
}

static void
_outCreateStmt(StringInfo str, const CreateStmt *node)
{
	WRITE_NODE_TYPE("CREATESTMT");

	_outCreateStmtInfo(str, (const CreateStmt *) node);
}

static void
_outExtTableTypeDesc(StringInfo str, const ExtTableTypeDesc *node)
{
	WRITE_NODE_TYPE("EXTTABLETYPEDESC");

	WRITE_ENUM_FIELD(exttabletype, ExtTableType);
	WRITE_NODE_FIELD(location_list);
	WRITE_NODE_FIELD(on_clause);
	WRITE_STRING_FIELD(command_string);
}

static void
_outCreateExternalStmt(StringInfo str, const CreateExternalStmt *node)
{
	WRITE_NODE_TYPE("CREATEEXTERNALSTMT");

	WRITE_NODE_FIELD(relation);
	WRITE_NODE_FIELD(tableElts);
	WRITE_NODE_FIELD(exttypedesc);
	WRITE_STRING_FIELD(format);
	WRITE_NODE_FIELD(formatOpts);
	WRITE_BOOL_FIELD(isweb);
	WRITE_BOOL_FIELD(iswritable);
	WRITE_BOOL_FIELD(if_not_exists);
	WRITE_NODE_FIELD(sreh);
	WRITE_NODE_FIELD(extOptions);
	WRITE_NODE_FIELD(encoding);
	WRITE_NODE_FIELD(distributeby);
}

static void
_outCreateForeignTableStmt(StringInfo str, const CreateForeignTableStmt *node)
{
	WRITE_NODE_TYPE("CREATEFOREIGNTABLESTMT");

	_outCreateStmtInfo(str, (const CreateStmt *) node);

	WRITE_STRING_FIELD(servername);
	WRITE_NODE_FIELD(options);
	WRITE_BOOL_FIELD(write_only);
}

static void
_outImportForeignSchemaStmt(StringInfo str, const ImportForeignSchemaStmt *node)
{
	WRITE_NODE_TYPE("IMPORTFOREIGNSCHEMASTMT");

	WRITE_STRING_FIELD(server_name);
	WRITE_STRING_FIELD(remote_schema);
	WRITE_STRING_FIELD(local_schema);
	WRITE_ENUM_FIELD(list_type, ImportForeignSchemaType);
	WRITE_NODE_FIELD(table_list);
	WRITE_NODE_FIELD(options);
}

static void
_outIndexStmt(StringInfo str, const IndexStmt *node)
{
	WRITE_NODE_TYPE("INDEXSTMT");

	WRITE_STRING_FIELD(idxname);
	WRITE_NODE_FIELD(relation);
	WRITE_OID_FIELD(relationId);
	WRITE_STRING_FIELD(accessMethod);
	WRITE_STRING_FIELD(tableSpace);
	WRITE_NODE_FIELD(indexParams);
	WRITE_NODE_FIELD(options);
	WRITE_NODE_FIELD(whereClause);
	WRITE_NODE_FIELD(excludeOpNames);
	WRITE_STRING_FIELD(idxcomment);
	WRITE_OID_FIELD(indexOid);
	WRITE_OID_FIELD(oldNode);
	WRITE_BOOL_FIELD(unique);
	WRITE_BOOL_FIELD(cross_node);
	WRITE_BOOL_FIELD(primary);
	WRITE_BOOL_FIELD(isconstraint);
	WRITE_BOOL_FIELD(deferrable);
	WRITE_BOOL_FIELD(initdeferred);
	WRITE_BOOL_FIELD(transformed);
	WRITE_BOOL_FIELD(concurrent);
	WRITE_BOOL_FIELD(if_not_exists);
	WRITE_BOOL_FIELD(reset_default_tblspc);
}

static void
_outCreateStatsStmt(StringInfo str, const CreateStatsStmt *node)
{
	WRITE_NODE_TYPE("CREATESTATSSTMT");

	WRITE_NODE_FIELD(defnames);
	WRITE_NODE_FIELD(stat_types);
	WRITE_NODE_FIELD(exprs);
	WRITE_NODE_FIELD(relations);
	WRITE_BOOL_FIELD(if_not_exists);
}

static void
_outNotifyStmt(StringInfo str, const NotifyStmt *node)
{
	WRITE_NODE_TYPE("NOTIFY");

	WRITE_STRING_FIELD(conditionname);
	WRITE_STRING_FIELD(payload);
}

static void
_outDeclareCursorStmt(StringInfo str, const DeclareCursorStmt *node)
{
	WRITE_NODE_TYPE("DECLARECURSOR");

	WRITE_STRING_FIELD(portalname);
	WRITE_INT_FIELD(options);
	WRITE_NODE_FIELD(query);
}

static void
_outSelectStmt(StringInfo str, const SelectStmt *node)
{
	WRITE_NODE_TYPE("SELECT");

	WRITE_NODE_FIELD(distinctClause);
	WRITE_NODE_FIELD(intoClause);
	WRITE_NODE_FIELD(targetList);
	WRITE_NODE_FIELD(fromClause);
	WRITE_NODE_FIELD(whereClause);
	WRITE_NODE_FIELD(groupClause);
	WRITE_NODE_FIELD(havingClause);
	WRITE_NODE_FIELD(windowClause);
	WRITE_NODE_FIELD(valuesLists);
	WRITE_NODE_FIELD(sortClause);
	WRITE_NODE_FIELD(limitOffset);
	WRITE_NODE_FIELD(limitCount);
	WRITE_ENUM_FIELD(limitOption, LimitOption);
	WRITE_NODE_FIELD(lockingClause);
	WRITE_NODE_FIELD(withClause);
	WRITE_NODE_FIELD(connectClause);
	WRITE_ENUM_FIELD(op, SetOperation);
	WRITE_BOOL_FIELD(all);
	WRITE_NODE_FIELD(larg);
	WRITE_NODE_FIELD(rarg);
	WRITE_NODE_FIELD(withFuncClause);
}

static void
_outFuncCall(StringInfo str, const FuncCall *node)
{
	WRITE_NODE_TYPE("FUNCCALL");

	WRITE_NODE_FIELD(funcname);
	WRITE_NODE_FIELD(args);
	WRITE_NODE_FIELD(agg_order);
	WRITE_NODE_FIELD(agg_filter);
	WRITE_BOOL_FIELD(agg_within_group);
	WRITE_BOOL_FIELD(agg_star);
	WRITE_BOOL_FIELD(agg_distinct);
	WRITE_BOOL_FIELD(func_variadic);
	WRITE_NODE_FIELD(over);
	WRITE_LOCATION_FIELD(location);
}


static void
_outInsertStmt(StringInfo str, const InsertStmt *node)
{
	WRITE_NODE_TYPE("INSERTSTMT");

	WRITE_NODE_FIELD(relation);
	WRITE_NODE_FIELD(cols);
	WRITE_NODE_FIELD(selectStmt);
	WRITE_NODE_FIELD(onConflictClause);
	WRITE_NODE_FIELD(returningList);
	WRITE_NODE_FIELD(withClause);
	WRITE_ENUM_FIELD(override, OverridingKind);

}
static void
_outCreateFunctionStmt(StringInfo str, const CreateFunctionStmt *node)
{
	WRITE_NODE_TYPE("CREATEFUNCTIONSTMT");

	WRITE_BOOL_FIELD(replace);
	WRITE_BOOL_FIELD(pkginner);
	WRITE_NODE_FIELD(funcname);
	WRITE_NODE_FIELD(parameters);
	WRITE_NODE_FIELD(returnType);
	WRITE_BOOL_FIELD(is_procedure);
	WRITE_NODE_FIELD(options);
	WRITE_NODE_FIELD(withClause);
}

static void
_outCreatePackageStmt(StringInfo str, const CreatePackageStmt *node)
{
	WRITE_NODE_TYPE("CREATEPACKAGESTMT");

	WRITE_BOOL_FIELD(replace);
	WRITE_BOOL_FIELD(edition);
	WRITE_BOOL_FIELD(body);
	WRITE_NODE_FIELD(pkgname);
	WRITE_STRING_FIELD(pkgheadersrc);
	WRITE_NODE_FIELD(options);
	WRITE_NODE_FIELD(pkgfunclist);
	WRITE_NODE_FIELD(pkgtypelist);
	WRITE_NODE_FIELD(pkgvarlist);
	WRITE_STRING_FIELD(func_spec);
	WRITE_BOOL_FIELD(reparse_spec);
}

static void
_outCreatePackageBodyStmt(StringInfo str, const CreatePackageBodyStmt *node)
{
	WRITE_NODE_TYPE("CREATEPACKAGEBODYSTMT");

	WRITE_BOOL_FIELD(replace);
	WRITE_BOOL_FIELD(edition);
	WRITE_NODE_FIELD(pkgname);
	WRITE_STRING_FIELD(pkgbodysrc);
	WRITE_NODE_FIELD(options);
	WRITE_NODE_FIELD(pkgfunclist);
	WRITE_NODE_FIELD(pkgtypelist);
	WRITE_NODE_FIELD(pkgvarlist);
	WRITE_STRING_FIELD(priv_decl);
	WRITE_STRING_FIELD(func_def);
	WRITE_STRING_FIELD(initial_part);
	WRITE_BOOL_FIELD(reparse_body);
}


/* end ora_compatible */

static void
_outDefElem(StringInfo str, const DefElem *node)
{
	WRITE_NODE_TYPE("DEFELEM");

	WRITE_STRING_FIELD(defnamespace);
	WRITE_STRING_FIELD(defname);
	WRITE_NODE_FIELD(arg);
	WRITE_ENUM_FIELD(defaction, DefElemAction);
	WRITE_LOCATION_FIELD(location);
}

static void
_outCopyFormatter(StringInfo str, const CopyFormatter *node)
{
    WRITE_NODE_TYPE("COPYFORMATTER");

    WRITE_NODE_FIELD(fixed_attrs);
    WRITE_BOOL_FIELD(with_cut);
}

static void
_outCopyAttrFixed(StringInfo str, const CopyAttrFixed *node)
{
    WRITE_NODE_TYPE("COPYATTRFIXED");

    WRITE_STRING_FIELD(attname);
    WRITE_INT_FIELD(fix_len);
    WRITE_INT_FIELD(attnum);
    WRITE_ENUM_FIELD(align, ColAlign);
    WRITE_BOOL_FIELD(with_cut);
}

static void
_outTableLikeClause(StringInfo str, const TableLikeClause *node)
{
	WRITE_NODE_TYPE("TABLELIKECLAUSE");

	WRITE_NODE_FIELD(relation);
	WRITE_UINT_FIELD(options);
}

static void
_outLockingClause(StringInfo str, const LockingClause *node)
{
	WRITE_NODE_TYPE("LOCKINGCLAUSE");

	WRITE_NODE_FIELD(lockedRels);
	WRITE_ENUM_FIELD(strength, LockClauseStrength);
	WRITE_ENUM_FIELD(waitPolicy, LockWaitPolicy);
	WRITE_INT_FIELD(waitTimeout);
}

static void
_outXmlSerialize(StringInfo str, const XmlSerialize *node)
{
	WRITE_NODE_TYPE("XMLSERIALIZE");

	WRITE_ENUM_FIELD(xmloption, XmlOptionType);
	WRITE_NODE_FIELD(expr);
	WRITE_NODE_FIELD(typeName);
	WRITE_LOCATION_FIELD(location);
}

static void
_outTriggerTransition(StringInfo str, const TriggerTransition *node)
{
	WRITE_NODE_TYPE("TRIGGERTRANSITION");

	WRITE_STRING_FIELD(name);
	WRITE_BOOL_FIELD(isNew);
	WRITE_BOOL_FIELD(isTable);
}

static void
_outColumnDef(StringInfo str, const ColumnDef *node)
{
	WRITE_NODE_TYPE("COLUMNDEF");

	WRITE_STRING_FIELD(colname);
	WRITE_NODE_FIELD(typeName);
	WRITE_INT_FIELD(inhcount);
	WRITE_BOOL_FIELD(is_local);
	WRITE_BOOL_FIELD(is_not_null);
	WRITE_BOOL_FIELD(is_from_type);
	WRITE_BOOL_FIELD(is_from_parent);
	WRITE_CHAR_FIELD(storage);
	WRITE_NODE_FIELD(raw_default);
	WRITE_NODE_FIELD(cooked_default);
	WRITE_CHAR_FIELD(identity);
	WRITE_NODE_FIELD(collClause);
#ifdef __OPENTENBASE__
	if (portable_output)
	{
		WRITE_COLLID_FIELD(collOid);
	}
	else
#endif
	WRITE_OID_FIELD(collOid);
	WRITE_NODE_FIELD(constraints);
	WRITE_NODE_FIELD(fdwoptions);
	WRITE_LOCATION_FIELD(location);
#ifdef __OPENTENBASE_C__
	WRITE_NODE_FIELD(encoding);
#endif
	WRITE_NODE_FIELD(attinfo);
}

static void
_outTypeName(StringInfo str, const TypeName *node)
{
	WRITE_NODE_TYPE("TYPENAME");

	WRITE_NODE_FIELD(names);
#ifdef __OPENTENBASE__
	if (portable_output)
	{
		WRITE_TYPID_FIELD(typeOid);
	}
	else
#endif
	WRITE_OID_FIELD(typeOid);
	WRITE_BOOL_FIELD(setof);
	WRITE_BOOL_FIELD(pct_type);
	WRITE_BOOL_FIELD(pct_rowtype);
	WRITE_NODE_FIELD(typmods);
	WRITE_INT_FIELD(typemod);
	WRITE_NODE_FIELD(arrayBounds);
	WRITE_LOCATION_FIELD(location);
}

static void
_outTypeCast(StringInfo str, const TypeCast *node)
{
	WRITE_NODE_TYPE("TYPECAST");

	WRITE_NODE_FIELD(arg);
	WRITE_NODE_FIELD(typeName);
	WRITE_LOCATION_FIELD(location);
}

static void
_outCollateClause(StringInfo str, const CollateClause *node)
{
	WRITE_NODE_TYPE("COLLATECLAUSE");

	WRITE_NODE_FIELD(arg);
	WRITE_NODE_FIELD(collname);
	WRITE_LOCATION_FIELD(location);
}

static void
_outIndexElem(StringInfo str, const IndexElem *node)
{
	WRITE_NODE_TYPE("INDEXELEM");

	WRITE_STRING_FIELD(name);
	WRITE_NODE_FIELD(expr);
	WRITE_STRING_FIELD(indexcolname);
	WRITE_NODE_FIELD(collation);
	WRITE_NODE_FIELD(opclass);
	WRITE_ENUM_FIELD(ordering, SortByDir);
	WRITE_ENUM_FIELD(nulls_ordering, SortByNulls);
}

static void
_outQuery(StringInfo str, const Query *node)
{
	WRITE_NODE_TYPE("QUERY");

	WRITE_ENUM_FIELD(commandType, CmdType);
	WRITE_ENUM_FIELD(querySource, QuerySource);
	/* we intentionally do not print the queryId field */
	WRITE_BOOL_FIELD(canSetTag);

	/*
	 * Hack to work around missing outfuncs routines for a lot of the
	 * utility-statement node types.  (The only one we actually *need* for
	 * rules support is NotifyStmt.)  Someday we ought to support 'em all, but
	 * for the meantime do this to avoid getting lots of warnings when running
	 * with debug_print_parse on.
	 */
	if (node->utilityStmt)
	{
		switch (nodeTag(node->utilityStmt))
		{
			case T_CreateStmt:
			case T_CreateExternalStmt:
			case T_IndexStmt:
			case T_NotifyStmt:
			case T_DeclareCursorStmt:
				WRITE_NODE_FIELD(utilityStmt);
				break;
			default:
				appendStringInfoString(str, " :utilityStmt ?");
				break;
		}
	}
	else
		appendStringInfoString(str, " :utilityStmt <>");

	WRITE_INT_FIELD(resultRelation);
	WRITE_BOOL_FIELD(hasAggs);
	WRITE_BOOL_FIELD(hasWindowFuncs);
	WRITE_BOOL_FIELD(hasTargetSRFs);
	WRITE_BOOL_FIELD(hasSubLinks);
	WRITE_BOOL_FIELD(hasDistinctOn);
	WRITE_BOOL_FIELD(hasRecursive);
	WRITE_BOOL_FIELD(hasModifyingCTE);
	WRITE_BOOL_FIELD(hasForUpdate);
	WRITE_BOOL_FIELD(hasRowSecurity);
	WRITE_NODE_FIELD(cteList);
	WRITE_NODE_FIELD(rtable);
	WRITE_NODE_FIELD(jointree);
	WRITE_NODE_FIELD(targetList);
	WRITE_ENUM_FIELD(override, OverridingKind);
	WRITE_NODE_FIELD(onConflict);
	WRITE_NODE_FIELD(returningList);
	WRITE_NODE_FIELD(groupClause);
	WRITE_NODE_FIELD(groupingSets);
	WRITE_NODE_FIELD(havingQual);
	WRITE_NODE_FIELD(windowClause);
	WRITE_NODE_FIELD(distinctClause);
	WRITE_NODE_FIELD(sortClause);
	WRITE_NODE_FIELD(limitOffset);
	WRITE_NODE_FIELD(limitCount);
	WRITE_ENUM_FIELD(limitOption, LimitOption);
	WRITE_NODE_FIELD(rowMarks);
	WRITE_NODE_FIELD(setOperations);
	WRITE_NODE_FIELD(constraintDeps);
	WRITE_NODE_FIELD(connectByExpr);
	WRITE_BOOL_FIELD(hasRowNumExpr);
	/* withCheckOptions intentionally omitted, see comment in parsenodes.h */
	WRITE_LOCATION_FIELD(stmt_location);
	WRITE_LOCATION_FIELD(stmt_len);
	/* begin ora_compatible */
	WRITE_BOOL_FIELD(isall);
	WRITE_NODE_FIELD(multi_inserts);
	/* end ora_compatible */
	WRITE_NODE_FIELD(with_funcs);
}

static void
_outWithCheckOption(StringInfo str, const WithCheckOption *node)
{
	WRITE_NODE_TYPE("WITHCHECKOPTION");

	WRITE_ENUM_FIELD(kind, WCOKind);
	WRITE_STRING_FIELD(relname);
	WRITE_STRING_FIELD(polname);
	WRITE_NODE_FIELD(qual);
	WRITE_BOOL_FIELD(cascaded);
}

static void
_outSortGroupClause(StringInfo str, const SortGroupClause *node)
{
	WRITE_NODE_TYPE("SORTGROUPCLAUSE");

	WRITE_UINT_FIELD(tleSortGroupRef);
#ifdef XCP
	if (portable_output)
		WRITE_OPERID_FIELD(eqop);
	else
#endif
	WRITE_OID_FIELD(eqop);
#ifdef XCP
	if (portable_output)
		WRITE_OPERID_FIELD(sortop);
	else
#endif
	WRITE_OID_FIELD(sortop);
	WRITE_BOOL_FIELD(nulls_first);
	WRITE_BOOL_FIELD(hashable);
}

static void
_outGroupingSet(StringInfo str, const GroupingSet *node)
{
	WRITE_NODE_TYPE("GROUPINGSET");

	WRITE_ENUM_FIELD(kind, GroupingSetKind);
	WRITE_NODE_FIELD(content);
	WRITE_LOCATION_FIELD(location);
}

static void
_outWindowClause(StringInfo str, const WindowClause *node)
{
	WRITE_NODE_TYPE("WINDOWCLAUSE");

	WRITE_STRING_FIELD(name);
	WRITE_STRING_FIELD(refname);
	WRITE_NODE_FIELD(partitionClause);
	WRITE_NODE_FIELD(orderClause);
	WRITE_INT_FIELD(frameOptions);
	WRITE_NODE_FIELD(startOffset);
	WRITE_NODE_FIELD(endOffset);
	if (portable_output)
	{
		WRITE_FUNCID_FIELD(startInRangeFunc);
		WRITE_FUNCID_FIELD(endInRangeFunc);
		WRITE_COLLID_FIELD(inRangeColl);
	}
	else
	{
		WRITE_OID_FIELD(startInRangeFunc);
		WRITE_OID_FIELD(endInRangeFunc);
		WRITE_OID_FIELD(inRangeColl);
	}
	WRITE_BOOL_FIELD(inRangeAsc);
	WRITE_BOOL_FIELD(inRangeNullsFirst);
	WRITE_UINT_FIELD(winref);
	WRITE_BOOL_FIELD(copiedOrder);
	WRITE_BOOL_FIELD(start_contain_vars);
	WRITE_BOOL_FIELD(end_contain_vars);
}

static void
_outRowMarkClause(StringInfo str, const RowMarkClause *node)
{
	WRITE_NODE_TYPE("ROWMARKCLAUSE");

	WRITE_UINT_FIELD(rti);
	WRITE_ENUM_FIELD(strength, LockClauseStrength);
	WRITE_ENUM_FIELD(waitPolicy, LockWaitPolicy);
	WRITE_INT_FIELD(waitTimeout);
	WRITE_BOOL_FIELD(pushedDown);
}

static void
_outWithClause(StringInfo str, const WithClause *node)
{
	WRITE_NODE_TYPE("WITHCLAUSE");

	WRITE_NODE_FIELD(ctes);
	WRITE_BOOL_FIELD(recursive);
	WRITE_LOCATION_FIELD(location);
}

static void
_outCommonTableExpr(StringInfo str, const CommonTableExpr *node)
{
	WRITE_NODE_TYPE("COMMONTABLEEXPR");

	WRITE_STRING_FIELD(ctename);
	WRITE_NODE_FIELD(aliascolnames);
	WRITE_ENUM_FIELD(ctematerialized, CTEMaterialize);
	WRITE_NODE_FIELD(ctequery);
	WRITE_LOCATION_FIELD(location);
	WRITE_BOOL_FIELD(cterecursive);
	WRITE_INT_FIELD(cterefcount);
	WRITE_NODE_FIELD(ctecolnames);
	WRITE_NODE_FIELD(ctecoltypes);
	WRITE_NODE_FIELD(ctecoltypmods);
	WRITE_NODE_FIELD(ctecolcollations);
}

static void
_outSetOperationStmt(StringInfo str, const SetOperationStmt *node)
{
	WRITE_NODE_TYPE("SETOPERATIONSTMT");

	WRITE_ENUM_FIELD(op, SetOperation);
	WRITE_BOOL_FIELD(all);
	WRITE_NODE_FIELD(larg);
	WRITE_NODE_FIELD(rarg);
	WRITE_NODE_FIELD(colTypes);
	WRITE_NODE_FIELD(colTypmods);
	WRITE_NODE_FIELD(colCollations);
	WRITE_NODE_FIELD(groupClauses);
}

static void
_outRangeTblEntry(StringInfo str, const RangeTblEntry *node)
{
	WRITE_NODE_TYPE("RTE");

	/* put alias + eref first to make dump more legible */
	WRITE_NODE_FIELD(alias);
	WRITE_NODE_FIELD(eref);
	WRITE_ENUM_FIELD(rtekind, RTEKind);

	switch (node->rtekind)
	{
		case RTE_RELATION:
			WRITE_CHAR_FIELD(relkind);
#ifdef XCP
			if (portable_output)
				WRITE_RELID_FIELD(relid);
			else
#endif
			WRITE_OID_FIELD(relid);
			WRITE_NODE_FIELD(tablesample);
			break;
		case RTE_SUBQUERY:
			WRITE_NODE_FIELD(subquery);
			WRITE_BOOL_FIELD(security_barrier);
			break;
		case RTE_JOIN:
			WRITE_ENUM_FIELD(jointype, JoinType);
			WRITE_NODE_FIELD(joinaliasvars);
			break;
		case RTE_FUNCTION:
			WRITE_NODE_FIELD(functions);
			WRITE_BOOL_FIELD(funcordinality);
			break;
		case RTE_TABLEFUNC:
			WRITE_NODE_FIELD(tablefunc);
			break;
		case RTE_VALUES:
			WRITE_NODE_FIELD(values_lists);
			WRITE_NODE_FIELD(coltypes);
			WRITE_NODE_FIELD(coltypmods);
			WRITE_NODE_FIELD(colcollations);
			break;
		case RTE_CTE:
			WRITE_STRING_FIELD(ctename);
			WRITE_UINT_FIELD(ctelevelsup);
			WRITE_BOOL_FIELD(self_reference);
			WRITE_NODE_FIELD(coltypes);
			WRITE_NODE_FIELD(coltypmods);
			WRITE_NODE_FIELD(colcollations);
			break;
		case RTE_NAMEDTUPLESTORE:
			WRITE_STRING_FIELD(enrname);
			WRITE_FLOAT_FIELD(enrtuples, "%.0f");
#ifdef __OPENTENBASE__
			if (portable_output)
			{
				WRITE_RELID_FIELD(relid);
			}
			else
#endif
			WRITE_OID_FIELD(relid);
			WRITE_NODE_FIELD(coltypes);
			WRITE_NODE_FIELD(coltypmods);
			WRITE_NODE_FIELD(colcollations);
			break;
#ifdef PGXC
		case RTE_REMOTE_DUMMY:
			/* Everything relevant already copied */
			break;
#endif /* PGXC */
		default:
			elog(ERROR, "unrecognized RTE kind: %d", (int) node->rtekind);
			break;
	}

	WRITE_BOOL_FIELD(lateral);
	WRITE_BOOL_FIELD(inh);
	WRITE_BOOL_FIELD(inFromCl);
	WRITE_UINT_FIELD(requiredPerms);
#ifdef XCP
	/* no check on data node, consider it is trusted */
	if (portable_output)
		appendStringInfo(str, " :checkAsUser %u", InvalidOid);
	else
#endif
	WRITE_OID_FIELD(checkAsUser);
	WRITE_BITMAPSET_FIELD(selectedCols);
	WRITE_BITMAPSET_FIELD(insertedCols);
	WRITE_BITMAPSET_FIELD(updatedCols);
	WRITE_NODE_FIELD(securityQuals);
}

static void
_outRangeTblFunction(StringInfo str, const RangeTblFunction *node)
{
	WRITE_NODE_TYPE("RANGETBLFUNCTION");

	WRITE_NODE_FIELD(funcexpr);
	WRITE_INT_FIELD(funccolcount);
	WRITE_NODE_FIELD(funccolnames);
    if (portable_output)
    {
        WRITE_TYPID_LIST_FIELD(funccoltypes);
    }
    else
    {
	WRITE_NODE_FIELD(funccoltypes);
    }
	WRITE_NODE_FIELD(funccoltypmods);
	WRITE_NODE_FIELD(funccolcollations);
	WRITE_BITMAPSET_FIELD(funcparams);
}

static void
_outTableSampleClause(StringInfo str, const TableSampleClause *node)
{
	WRITE_NODE_TYPE("TABLESAMPLECLAUSE");

#ifdef XCP
	if (portable_output)
	{
		WRITE_FUNCID_FIELD(tsmhandler);
	}
	else
	{
#endif
	WRITE_OID_FIELD(tsmhandler);
#ifdef XCP
	}
#endif
	WRITE_NODE_FIELD(args);
	WRITE_NODE_FIELD(repeatable);
}

static void
_outAExpr(StringInfo str, const A_Expr *node)
{
	WRITE_NODE_TYPE("AEXPR");

	switch (node->kind)
	{
		case AEXPR_OP:
			appendStringInfoChar(str, ' ');
			WRITE_NODE_FIELD(name);
			break;
		case AEXPR_OP_ANY:
			appendStringInfoChar(str, ' ');
			WRITE_NODE_FIELD(name);
			appendStringInfoString(str, " ANY ");
			break;
		case AEXPR_OP_ALL:
			appendStringInfoChar(str, ' ');
			WRITE_NODE_FIELD(name);
			appendStringInfoString(str, " ALL ");
			break;
		case AEXPR_DISTINCT:
			appendStringInfoString(str, " DISTINCT ");
			WRITE_NODE_FIELD(name);
			break;
		case AEXPR_NOT_DISTINCT:
			appendStringInfoString(str, " NOT_DISTINCT ");
			WRITE_NODE_FIELD(name);
			break;
		case AEXPR_NULLIF:
			appendStringInfoString(str, " NULLIF ");
			WRITE_NODE_FIELD(name);
			break;
		case AEXPR_OF:
			appendStringInfoString(str, " OF ");
			WRITE_NODE_FIELD(name);
			break;
		case AEXPR_IN:
			appendStringInfoString(str, " IN ");
			WRITE_NODE_FIELD(name);
			break;
		case AEXPR_LIKE:
			appendStringInfoString(str, " LIKE ");
			WRITE_NODE_FIELD(name);
			break;
		case AEXPR_ILIKE:
			appendStringInfoString(str, " ILIKE ");
			WRITE_NODE_FIELD(name);
			break;
		case AEXPR_SIMILAR:
			appendStringInfoString(str, " SIMILAR ");
			WRITE_NODE_FIELD(name);
			break;
		case AEXPR_BETWEEN:
			appendStringInfoString(str, " BETWEEN ");
			WRITE_NODE_FIELD(name);
			break;
		case AEXPR_NOT_BETWEEN:
			appendStringInfoString(str, " NOT_BETWEEN ");
			WRITE_NODE_FIELD(name);
			break;
		case AEXPR_BETWEEN_SYM:
			appendStringInfoString(str, " BETWEEN_SYM ");
			WRITE_NODE_FIELD(name);
			break;
		case AEXPR_NOT_BETWEEN_SYM:
			appendStringInfoString(str, " NOT_BETWEEN_SYM ");
			WRITE_NODE_FIELD(name);
			break;
		case AEXPR_PAREN:
			appendStringInfoString(str, " PAREN");
			break;
		default:
			appendStringInfoString(str, " ??");
			break;
	}

	WRITE_NODE_FIELD(lexpr);
	WRITE_NODE_FIELD(rexpr);
	WRITE_LOCATION_FIELD(location);
}

static void
_outValue(StringInfo str, const Value *value)
{
	switch (value->type)
	{
		case T_Integer:
			appendStringInfo(str, "%d", value->val.ival);
			break;
		case T_Float:

			/*
			 * We assume the value is a valid numeric literal and so does not
			 * need quoting.
			 */
			appendStringInfoString(str, value->val.str);
			break;
		case T_String:

			/*
			 * We use outToken to provide escaping of the string's content,
			 * but we don't want it to do anything with an empty string.
			 */
			appendStringInfoChar(str, '"');
			if (value->val.str[0] != '\0')
				outToken(str, value->val.str);
			appendStringInfoChar(str, '"');
			break;
		case T_BitString:
			/* internal representation already has leading 'b' */
			appendStringInfoString(str, value->val.str);
			break;
		case T_Null:
			/* this is seen only within A_Const, not in transformed trees */
			appendStringInfoString(str, "NULL");
			/*
			 * Out string of raw node is not recorded in system catalog, then
			 * need not consider meta-data upgrade.
			 */
			if (value->val.str)
			{
				appendStringInfoChar(str, ' ');
				appendStringInfoChar(str, '"');
				if (value->val.str[0] != '\0')
					outToken(str, value->val.str);
				appendStringInfoChar(str, '"');
			}
			break;
		default:
			elog(ERROR, "unrecognized node type: %d", (int) value->type);
			break;
	}
}

static void
_outColumnRef(StringInfo str, const ColumnRef *node)
{
	WRITE_NODE_TYPE("COLUMNREF");

	WRITE_NODE_FIELD(fields);
	WRITE_LOCATION_FIELD(location);
}

static void
_outParamRef(StringInfo str, const ParamRef *node)
{
	WRITE_NODE_TYPE("PARAMREF");

	WRITE_INT_FIELD(number);
	WRITE_LOCATION_FIELD(location);
}

static void
_outAConst(StringInfo str, const A_Const *node)
{
	WRITE_NODE_TYPE("A_CONST");

	appendStringInfoString(str, " :val ");
	_outValue(str, &(node->val));
	WRITE_LOCATION_FIELD(location);
}

static void
_outA_Star(StringInfo str, const A_Star *node)
{
	WRITE_NODE_TYPE("A_STAR");
}

static void
_outA_Indices(StringInfo str, const A_Indices *node)
{
	WRITE_NODE_TYPE("A_INDICES");

	WRITE_BOOL_FIELD(is_slice);
	WRITE_NODE_FIELD(lidx);
	WRITE_NODE_FIELD(uidx);
}

static void
_outA_Indirection(StringInfo str, const A_Indirection *node)
{
	WRITE_NODE_TYPE("A_INDIRECTION");

	WRITE_NODE_FIELD(arg);
	WRITE_NODE_FIELD(indirection);
}

static void
_outA_ArrayExpr(StringInfo str, const A_ArrayExpr *node)
{
	WRITE_NODE_TYPE("A_ARRAYEXPR");

	WRITE_NODE_FIELD(elements);
	WRITE_LOCATION_FIELD(location);
}

static void
_outResTarget(StringInfo str, const ResTarget *node)
{
	WRITE_NODE_TYPE("RESTARGET");

	WRITE_STRING_FIELD(name);
	WRITE_NODE_FIELD(indirection);
	WRITE_NODE_FIELD(val);
	WRITE_LOCATION_FIELD(location);
}

static void
_outMultiAssignRef(StringInfo str, const MultiAssignRef *node)
{
	WRITE_NODE_TYPE("MULTIASSIGNREF");

	WRITE_NODE_FIELD(source);
	WRITE_INT_FIELD(colno);
	WRITE_INT_FIELD(ncolumns);
}

static void
_outSortBy(StringInfo str, const SortBy *node)
{
	WRITE_NODE_TYPE("SORTBY");

	WRITE_NODE_FIELD(node);
	WRITE_ENUM_FIELD(sortby_dir, SortByDir);
	WRITE_ENUM_FIELD(sortby_nulls, SortByNulls);
	WRITE_NODE_FIELD(useOp);
	WRITE_LOCATION_FIELD(location);
}

static void
_outWindowDef(StringInfo str, const WindowDef *node)
{
	WRITE_NODE_TYPE("WINDOWDEF");

	WRITE_STRING_FIELD(name);
	WRITE_STRING_FIELD(refname);
	WRITE_NODE_FIELD(partitionClause);
	WRITE_NODE_FIELD(orderClause);
	WRITE_INT_FIELD(frameOptions);
	WRITE_NODE_FIELD(startOffset);
	WRITE_NODE_FIELD(endOffset);
	WRITE_LOCATION_FIELD(location);
}

static void
_outRangeSubselect(StringInfo str, const RangeSubselect *node)
{
	WRITE_NODE_TYPE("RANGESUBSELECT");

	WRITE_BOOL_FIELD(lateral);
	WRITE_NODE_FIELD(subquery);
	WRITE_NODE_FIELD(alias);
}

static void
_outRangeFunction(StringInfo str, const RangeFunction *node)
{
	WRITE_NODE_TYPE("RANGEFUNCTION");

	WRITE_BOOL_FIELD(lateral);
	WRITE_BOOL_FIELD(ordinality);
	WRITE_BOOL_FIELD(is_rowsfrom);
	WRITE_NODE_FIELD(functions);
	WRITE_NODE_FIELD(alias);
	WRITE_NODE_FIELD(coldeflist);
}

static void
_outRangeTableSample(StringInfo str, const RangeTableSample *node)
{
	WRITE_NODE_TYPE("RANGETABLESAMPLE");

	WRITE_NODE_FIELD(relation);
	WRITE_NODE_FIELD(method);
	WRITE_NODE_FIELD(args);
	WRITE_NODE_FIELD(repeatable);
	WRITE_LOCATION_FIELD(location);
}

static void
_outRangeTableFunc(StringInfo str, const RangeTableFunc *node)
{
	WRITE_NODE_TYPE("RANGETABLEFUNC");

	WRITE_BOOL_FIELD(lateral);
	WRITE_NODE_FIELD(docexpr);
	WRITE_NODE_FIELD(rowexpr);
	WRITE_NODE_FIELD(namespaces);
	WRITE_NODE_FIELD(columns);
	WRITE_NODE_FIELD(alias);
	WRITE_LOCATION_FIELD(location);
}

static void
_outRangeTableFuncCol(StringInfo str, const RangeTableFuncCol *node)
{
	WRITE_NODE_TYPE("RANGETABLEFUNCCOL");

	WRITE_STRING_FIELD(colname);
	WRITE_NODE_FIELD(typeName);
	WRITE_BOOL_FIELD(for_ordinality);
	WRITE_BOOL_FIELD(is_not_null);
	WRITE_NODE_FIELD(colexpr);
	WRITE_NODE_FIELD(coldefexpr);
	WRITE_LOCATION_FIELD(location);
}

static void
_outConstraint(StringInfo str, const Constraint *node)
{
	WRITE_NODE_TYPE("CONSTRAINT");

	WRITE_STRING_FIELD(conname);
	WRITE_BOOL_FIELD(deferrable);
	WRITE_BOOL_FIELD(initdeferred);
	WRITE_LOCATION_FIELD(location);

	appendStringInfoString(str, " :contype ");
	switch (node->contype)
	{
		case CONSTR_NULL:
			appendStringInfoString(str, "NULL");
			break;

		case CONSTR_NOTNULL:
			appendStringInfoString(str, "NOT_NULL");
			break;

		case CONSTR_DEFAULT:
			appendStringInfoString(str, "DEFAULT");
			WRITE_NODE_FIELD(raw_expr);
			WRITE_STRING_FIELD(cooked_expr);
			break;

		case CONSTR_IDENTITY:
			appendStringInfoString(str, "IDENTITY");
			WRITE_NODE_FIELD(raw_expr);
			WRITE_STRING_FIELD(cooked_expr);
			WRITE_CHAR_FIELD(generated_when);
			break;

		case CONSTR_CHECK:
			appendStringInfoString(str, "CHECK");
			WRITE_BOOL_FIELD(is_no_inherit);
			WRITE_NODE_FIELD(raw_expr);
			WRITE_STRING_FIELD(cooked_expr);
			break;

		case CONSTR_PRIMARY:
			appendStringInfoString(str, "PRIMARY_KEY");
			WRITE_NODE_FIELD(keys);
			WRITE_NODE_FIELD(options);
			WRITE_STRING_FIELD(indexname);
			WRITE_STRING_FIELD(indexspace);
			WRITE_BOOL_FIELD(reset_default_tblspc);
			/* access_method and where_clause not currently used */
			break;

		case CONSTR_UNIQUE:
			appendStringInfoString(str, "UNIQUE");
			WRITE_NODE_FIELD(keys);
			WRITE_NODE_FIELD(options);
			WRITE_STRING_FIELD(indexname);
			WRITE_STRING_FIELD(indexspace);
			WRITE_BOOL_FIELD(reset_default_tblspc);
			/* access_method and where_clause not currently used */
			break;

		case CONSTR_EXCLUSION:
			appendStringInfoString(str, "EXCLUSION");
			WRITE_NODE_FIELD(exclusions);
			WRITE_NODE_FIELD(options);
			WRITE_STRING_FIELD(indexname);
			WRITE_STRING_FIELD(indexspace);
			WRITE_BOOL_FIELD(reset_default_tblspc);
			WRITE_STRING_FIELD(access_method);
			WRITE_NODE_FIELD(where_clause);
			break;

		case CONSTR_FOREIGN:
			appendStringInfoString(str, "FOREIGN_KEY");
			WRITE_NODE_FIELD(pktable);
			WRITE_NODE_FIELD(fk_attrs);
			WRITE_NODE_FIELD(pk_attrs);
			WRITE_CHAR_FIELD(fk_matchtype);
			WRITE_CHAR_FIELD(fk_upd_action);
			WRITE_CHAR_FIELD(fk_del_action);
			WRITE_NODE_FIELD(old_conpfeqop);
			WRITE_OID_FIELD(old_pktable_oid);
			WRITE_BOOL_FIELD(skip_validation);
			WRITE_BOOL_FIELD(initially_valid);
			break;

		case CONSTR_ATTR_DEFERRABLE:
			appendStringInfoString(str, "ATTR_DEFERRABLE");
			break;

		case CONSTR_ATTR_NOT_DEFERRABLE:
			appendStringInfoString(str, "ATTR_NOT_DEFERRABLE");
			break;

		case CONSTR_ATTR_DEFERRED:
			appendStringInfoString(str, "ATTR_DEFERRED");
			break;

		case CONSTR_ATTR_IMMEDIATE:
			appendStringInfoString(str, "ATTR_IMMEDIATE");
			break;

		default:
			appendStringInfo(str, "<unrecognized_constraint %d>",
							 (int) node->contype);
			break;
	}
}

static void
_outForeignKeyCacheInfo(StringInfo str, const ForeignKeyCacheInfo *node)
{
	WRITE_NODE_TYPE("FOREIGNKEYCACHEINFO");

#ifdef __OPENTENBASE__
	if (portable_output)
	{
		WRITE_RELID_FIELD(conrelid);
		WRITE_RELID_FIELD(confrelid);
	}
	else
	{
#endif
	WRITE_OID_FIELD(conrelid);
	WRITE_OID_FIELD(confrelid);
#ifdef __OPENTENBASE__
	}
#endif
	WRITE_INT_FIELD(nkeys);
	WRITE_ATTRNUMBER_ARRAY(conkey, node->nkeys);
	WRITE_ATTRNUMBER_ARRAY(confkey, node->nkeys);
	WRITE_OID_ARRAY(conpfeqop, node->nkeys);
}

static void
_outPartitionElem(StringInfo str, const PartitionElem *node)
{
	WRITE_NODE_TYPE("PARTITIONELEM");

	WRITE_STRING_FIELD(name);
	WRITE_NODE_FIELD(expr);
	WRITE_NODE_FIELD(collation);
	WRITE_NODE_FIELD(opclass);
	WRITE_LOCATION_FIELD(location);
}

static void
_outPartitionDef(StringInfo str, const PartitionDef *node)
{
	WRITE_NODE_TYPE("PARTITIONDEF");

	WRITE_NODE_FIELD(part_rv);
	WRITE_NODE_FIELD(bound_spec);
	WRITE_NODE_FIELD(subpart_defs);
}

static void
_outPartitionSpec(StringInfo str, const PartitionSpec *node)
{
	WRITE_NODE_TYPE("PARTITIONSPEC");

	WRITE_STRING_FIELD(strategy);
	WRITE_NODE_FIELD(partParams);
	WRITE_NODE_FIELD(subPartSpec);
	WRITE_NODE_FIELD(partDefs);
	WRITE_LOCATION_FIELD(location);
}

static void
_outPartitionBoundSpec(StringInfo str, const PartitionBoundSpec *node)
{
	WRITE_NODE_TYPE("PARTITIONBOUNDSPEC");

	WRITE_CHAR_FIELD(strategy);
	WRITE_BOOL_FIELD(is_default);
	WRITE_INT_FIELD(modulus);
	WRITE_INT_FIELD(remainder);
	WRITE_NODE_FIELD(listdatums);
	WRITE_NODE_FIELD(lowerdatums);
	WRITE_NODE_FIELD(upperdatums);
	WRITE_LOCATION_FIELD(location);
}

static void
_outPartitionRangeDatum(StringInfo str, const PartitionRangeDatum *node)
{
	WRITE_NODE_TYPE("PARTITIONRANGEDATUM");

	WRITE_ENUM_FIELD(kind, PartitionRangeDatumKind);
	WRITE_NODE_FIELD(value);
	WRITE_LOCATION_FIELD(location);
}

#ifdef _PG_ORCL_
static void
_outPartitionInto(StringInfo str, const PartitionInto *node)
{
	WRITE_NODE_TYPE("PARTITIONINTO");

	WRITE_NODE_FIELD(part_name);
	WRITE_STRING_FIELD(part_tablespace);
}

static void
_outPartitionMergeSplit(StringInfo str, const PartitionMergeSplit *node)
{
	WRITE_NODE_TYPE("PARTITIONMERGESPLIT");

	WRITE_NODE_FIELD(part_ext_names);
	WRITE_NODE_FIELD(into_parts);
	WRITE_ENUM_FIELD(stp, MergeSplitStep);
	WRITE_NODE_FIELD(from_rel);
	WRITE_BOOL_FIELD(is_merge);
	WRITE_BOOL_FIELD(is_partition);
	WRITE_NODE_FIELD(bound);
}

static void
_outPartitionMergeSpec(StringInfo str, const PartitionMergeSpec *node)
{
	WRITE_NODE_TYPE("PARTITIONMERGESPEC");

	WRITE_CHAR_FIELD(strategy);
	WRITE_INT_FIELD(partnatts);

	if (node->partattrs > 0)
	{
		int	i = 0;

		appendStringInfoString(str, " :partopclass");
		for (i = 0; i < node->partnatts; i++)
			appendStringInfo(str, " %u", node->partopclass[i]);

		appendStringInfoString(str, " :partcollation");
		for (i = 0; i < node->partnatts; i++)
			appendStringInfo(str, " %u", node->partcollation[i]);

		appendStringInfoString(str, " :partattrs");
		for (i = 0; i < node->partnatts; i++)
			appendStringInfo(str, " %u", node->partattrs[i]);
	}

	WRITE_NODE_FIELD(partexprs);
}

static void
_outPauseTransactionStmt(StringInfo str, const PauseTransactionStmt *node)
{
	WRITE_NODE_TYPE("PAUSETRANSACTIONSTMT");

	WRITE_STRING_FIELD(xact_name);
	WRITE_BOOL_FIELD(pause);
}


#endif

#ifdef __AUDIT__
static void
_outAuditStmt(StringInfo str, const AuditStmt *node)
{
	WRITE_NODE_TYPE("AUDIT");
	WRITE_BOOL_FIELD(audit_ison);
	WRITE_ENUM_FIELD(audit_type, AuditType);
	WRITE_ENUM_FIELD(audit_mode, AuditMode);
	WRITE_NODE_FIELD(action_list);
	WRITE_NODE_FIELD(user_list);
	WRITE_NODE_FIELD(object_name);
	WRITE_ENUM_FIELD(object_type, ObjectType);
}

static void
_outCleanAuditStmt(StringInfo str, const CleanAuditStmt *node)
{
	WRITE_NODE_TYPE("CLEAN_AUDIT");

	WRITE_ENUM_FIELD(clean_type, CleanAuditType);
	WRITE_NODE_FIELD(user_list);
	WRITE_NODE_FIELD(object_name);
	WRITE_ENUM_FIELD(object_type, ObjectType);
}
#endif

#ifdef __RESOURCE_QUEUE__
static void
_outCreateResourceQueueStmt(StringInfo str, const CreateResourceQueueStmt *node)
{
	WRITE_NODE_TYPE("CREATERESQUEUESTMT");

	WRITE_STRING_FIELD(queue);
	WRITE_NODE_FIELD(options); /* List of DefElem nodes */
}

static void
_outAlterResourceQueueStmt(StringInfo str, const AlterResourceQueueStmt *node)
{
	WRITE_NODE_TYPE("ALTERRESQUEUESTMT");

	WRITE_STRING_FIELD(queue);
	WRITE_NODE_FIELD(options); /* List of DefElem nodes */
}

static void
_outDropResourceQueueStmt(StringInfo str, const DropResourceQueueStmt *node)
{
	WRITE_NODE_TYPE("DROPRESQUEUESTMT");

	WRITE_STRING_FIELD(queue);
}

static void
_outCreateResourceGroupStmt(StringInfo str, const CreateResourceGroupStmt *node)
{
	WRITE_NODE_TYPE("CREATERESGROUPSTMT");

	WRITE_STRING_FIELD(name);
	WRITE_NODE_FIELD(options); /* List of DefElem nodes */
}

static void
_outAlterResourceGroupStmt(StringInfo str, const AlterResourceGroupStmt *node)
{
	WRITE_NODE_TYPE("ALTERRESGROUPSTMT");

	WRITE_STRING_FIELD(name);
	WRITE_NODE_FIELD(options); /* List of DefElem nodes */
}

static void
_outDropResourceGroupStmt(StringInfo str, const DropResourceGroupStmt *node)
{
	WRITE_NODE_TYPE("DROPRESGROUPSTMT");

	WRITE_STRING_FIELD(name);
}
#endif

static void
_outMergeQualProj(StringInfo str, const MergeQualProj *node)
{
	WRITE_NODE_TYPE("MERGEQUALPROJ");

	_outPlanInfo(str, (const Plan *) node);
	WRITE_NODE_FIELD(mergeActionList);
}

static void
_outMergeQualProjPath(StringInfo str, const MergeQualProjPath *node)
{
	WRITE_NODE_TYPE("MERGEQUALPROJPATH");

	_outPathInfo(str, (const Path *) node);
	WRITE_NODE_FIELD(subpath);
	WRITE_NODE_FIELD(mergeActionLists);
}

static void 
_outFdwDataNodeTask(StringInfo str, const FdwDataNodeTask* node)
{
    WRITE_NODE_TYPE("FDWDATANODETASK");

    WRITE_STRING_FIELD(dnName);
	WRITE_STRING_FIELD(instant_time);
    WRITE_NODE_FIELD(task);
}

static void 
_outFdwFileSegment(StringInfo str, const FdwFileSegment* node)
{
    WRITE_NODE_TYPE("FDWFILESEGMENT");

    WRITE_STRING_FIELD(filename);
    WRITE_LONG_FIELD(begin);
    WRITE_LONG_FIELD(end);
    WRITE_LONG_FIELD(fdwfileSize);
	WRITE_NODE_FIELD(hudi_delta_log);
}

static void
_outFragmentDest(StringInfo str, const FragmentDest *node)
{
	WRITE_NODE_TYPE("FRAGMENTDEST");

	WRITE_INT_FIELD(fid);
	WRITE_INT_FIELD(recvfid);
	WRITE_INT_FIELD(targetNodeType);
	WRITE_NODE_FIELD(targetNodes);
}

static void
_outPriorExpr(StringInfo str, const PriorExpr *node)
{
	WRITE_NODE_TYPE("PRIOREXPR");
	WRITE_NODE_FIELD(expr);
	WRITE_BOOL_FIELD(for_path);
	WRITE_INT_FIELD(location);
}

static void
_outLevelExpr(StringInfo str, const LevelExpr *node)
{
	WRITE_NODE_TYPE("LEVELEXPR");
	WRITE_INT_FIELD(location);
}

static void
_outCBIsCycleExpr(StringInfo str, const CBIsCycleExpr *node)
{
	WRITE_NODE_TYPE("CBISCYCLEEXPR");
	WRITE_INT_FIELD(location);
}

static void
_outCBIsLeafExpr(StringInfo str, const CBIsLeafExpr *node)
{
	WRITE_NODE_TYPE("CBISLEAFEXPR");
	WRITE_INT_FIELD(location);
}

static void
_outCBRootExpr(StringInfo str, const CBRootExpr *node)
{
	WRITE_NODE_TYPE("CBROOTEXPR");
	WRITE_NODE_FIELD(expr);
	WRITE_INT_FIELD(location);
}

static void
_outCBPathExpr(StringInfo str, const CBPathExpr *node)
{
	WRITE_NODE_TYPE("CBPATHEXPR");
	WRITE_NODE_FIELD(expr);
	WRITE_INT_FIELD(location);
}

static void
_outMultiSetExpr(StringInfo str, const MultiSetExpr *node)
{
	WRITE_NODE_TYPE("MULTISETEXPR");
	WRITE_NODE_FIELD(lexpr);
	WRITE_NODE_FIELD(rexpr);
	WRITE_ENUM_FIELD(setop, SetOperation);
	WRITE_BOOL_FIELD(all);
	WRITE_INT_FIELD(location);
}

/*
 * outNode -
 *	  converts a Node into ascii string and append it to 'str'
 */
void
outNode(StringInfo str, const void *obj)
{
	if (obj == NULL)
		appendStringInfoString(str, "<>");
	else if (IsA(obj, List) ||IsA(obj, IntList) || IsA(obj, OidList))
		_outList(str, obj);
	else if (IsA(obj, Integer) ||
			 IsA(obj, Float) ||
			 IsA(obj, String) ||
			 IsA(obj, BitString))
	{
		/* nodeRead does not want to see { } around these! */
		_outValue(str, obj);
	}
	else
	{
		appendStringInfoChar(str, '{');
		switch (nodeTag(obj))
		{
			case T_PlannedStmt:
				_outPlannedStmt(str, obj);
				break;
			case T_Plan:
				_outPlan(str, obj);
				break;
			case T_Result:
				_outResult(str, obj);
				break;
			case T_ProjectSet:
				_outProjectSet(str, obj);
				break;
			case T_ModifyTable:
				_outModifyTable(str, obj);
				break;
			case T_PartIterator:
				_outPartIterator(str, obj);
				break;
			case T_Append:
				_outAppend(str, (void *) obj);
				break;
			case T_MergeAppend:
				_outMergeAppend(str, (void *) obj);
				break;
			case T_RecursiveUnion:
				_outRecursiveUnion(str, obj);
				break;
			case T_BitmapAnd:
				_outBitmapAnd(str, obj);
				break;
			case T_BitmapOr:
				_outBitmapOr(str, obj);
				break;
			case T_Gather:
				_outGather(str, obj);
				break;
			case T_GatherMerge:
				_outGatherMerge(str, obj);
				break;
			case T_Scan:
				_outScan(str, obj);
				break;
			case T_SeqScan:
				_outSeqScan(str, obj);
				break;
			case T_ExternalScanInfo:
				_outExternalScanInfo(str, obj);
				break;
#ifdef PGXC
			case T_RemoteQuery:
				_outRemoteQuery(str, obj);
				break;
#endif
			case T_SampleScan:
				_outSampleScan(str, obj);
				break;
			case T_IndexScan:
				_outIndexScan(str, obj);
				break;
			case T_IndexOnlyScan:
				_outIndexOnlyScan(str, obj);
				break;
			case T_BitmapIndexScan:
				_outBitmapIndexScan(str, obj);
				break;
			case T_BitmapHeapScan:
				_outBitmapHeapScan(str, obj);
				break;
			case T_TidScan:
				_outTidScan(str, obj);
				break;
			case T_SubqueryScan:
				_outSubqueryScan(str, obj);
				break;
			case T_FunctionScan:
				_outFunctionScan(str, obj);
				break;
			case T_TableFuncScan:
				_outTableFuncScan(str, obj);
				break;
			case T_ValuesScan:
				_outValuesScan(str, obj);
				break;
			case T_CteScan:
				_outCteScan(str, obj);
				break;
			case T_NamedTuplestoreScan:
				_outNamedTuplestoreScan(str, obj);
				break;
			case T_WorkTableScan:
				_outWorkTableScan(str, obj);
				break;
			case T_ForeignScan:
				_outForeignScan(str, obj);
				break;
			case T_CustomScan:
				_outCustomScan(str, obj);
				break;
			case T_Join:
				_outJoin(str, obj);
				break;
			case T_NestLoop:
				_outNestLoop(str, obj);
				break;
			case T_MergeJoin:
				_outMergeJoin(str, obj);
				break;
			case T_HashJoin:
				_outHashJoin(str, obj);
				break;
			case T_Agg:
				_outAgg(str, obj);
				break;
			case T_WindowAgg:
				_outWindowAgg(str, obj);
				break;
			case T_Group:
				_outGroup(str, obj);
				break;
			case T_Material:
				_outMaterial(str, obj);
				break;
			case T_Sort:
				_outSort(str, obj);
				break;
			case T_Unique:
				_outUnique(str, obj);
				break;
			case T_Hash:
				_outHash(str, obj);
				break;
			case T_SetOp:
				_outSetOp(str, obj);
				break;
			case T_LockRows:
				_outLockRows(str, obj);
				break;
			case T_Limit:
				_outLimit(str, obj);
				break;
			case T_NestLoopParam:
				_outNestLoopParam(str, obj);
				break;
#ifdef XCP
			case T_RemoteSubplan:
				_outRemoteSubplan(str, obj);
				break;
			case T_RemoteStmt:
				_outRemoteStmt(str, obj);
				break;
			case T_SimpleSort:
				_outSimpleSort(str, obj);
				break;
#endif
			case T_PlanRowMark:
				_outPlanRowMark(str, obj);
				break;
            case T_PartitionPruneStepOp:
                _outPartitionPruneStepOp(str, obj);
                break;
            case T_PartitionPruneStepCombine:
                _outPartitionPruneStepCombine(str, obj);
                break;
            case T_PartitionPruneInfo:
                _outPartitionPruneInfo(str, obj);
                break;
            case T_PartitionedRelPruneInfo:
                _outPartitionedRelPruneInfo(str, obj);
                break;
			case T_PlanInvalItem:
				_outPlanInvalItem(str, obj);
				break;
			case T_Alias:
				_outAlias(str, obj);
				break;
			case T_RangeVar:
				_outRangeVar(str, obj);
				break;
			case T_TableFunc:
				_outTableFunc(str, obj);
				break;
			case T_IntoClause:
				_outIntoClause(str, obj);
				break;
			case T_Var:
				_outVar(str, obj);
				break;
			case T_Const:
				_outConst(str, obj);
				break;
			case T_Param:
				_outParam(str, obj);
				break;
			case T_Aggref:
				_outAggref(str, obj);
				break;
			case T_GroupingFunc:
				_outGroupingFunc(str, obj);
				break;
			case T_WindowFunc:
				_outWindowFunc(str, obj);
				break;
			case T_ArrayRef:
				_outArrayRef(str, obj);
				break;
			case T_FuncExpr:
				_outFuncExpr(str, obj);
				break;
			case T_NamedArgExpr:
				_outNamedArgExpr(str, obj);
				break;
			case T_OpExpr:
				_outOpExpr(str, obj);
				break;
			case T_DistinctExpr:
				_outDistinctExpr(str, obj);
				break;
			case T_NullIfExpr:
				_outNullIfExpr(str, obj);
				break;
			case T_ScalarArrayOpExpr:
				_outScalarArrayOpExpr(str, obj);
				break;
			case T_BoolExpr:
				_outBoolExpr(str, obj);
				break;
			case T_SubLink:
				_outSubLink(str, obj);
				break;
			case T_SubPlan:
				_outSubPlan(str, obj);
				break;
			case T_AlternativeSubPlan:
				_outAlternativeSubPlan(str, obj);
				break;
			case T_FieldSelect:
				_outFieldSelect(str, obj);
				break;
			case T_FieldStore:
				_outFieldStore(str, obj);
				break;
			case T_RelabelType:
				_outRelabelType(str, obj);
				break;
			case T_CoerceViaIO:
				_outCoerceViaIO(str, obj);
				break;
			case T_ArrayCoerceExpr:
				_outArrayCoerceExpr(str, obj);
				break;
			case T_ConvertRowtypeExpr:
				_outConvertRowtypeExpr(str, obj);
				break;
			case T_CollateExpr:
				_outCollateExpr(str, obj);
				break;
			case T_CaseExpr:
				_outCaseExpr(str, obj);
				break;
			case T_CaseWhen:
				_outCaseWhen(str, obj);
				break;
			case T_CaseTestExpr:
				_outCaseTestExpr(str, obj);
				break;
			case T_ArrayExpr:
				_outArrayExpr(str, obj);
				break;
			case T_RowExpr:
				_outRowExpr(str, obj);
				break;
			case T_RowCompareExpr:
				_outRowCompareExpr(str, obj);
				break;
			case T_CoalesceExpr:
				_outCoalesceExpr(str, obj);
				break;
			case T_MinMaxExpr:
				_outMinMaxExpr(str, obj);
				break;
			case T_SQLValueFunction:
				_outSQLValueFunction(str, obj);
				break;
			case T_NextValueExpr:
				_outNextValueExpr(str, obj);
				break;
			case T_XmlExpr:
				_outXmlExpr(str, obj);
				break;
			case T_NullTest:
				_outNullTest(str, obj);
				break;
			case T_BooleanTest:
				_outBooleanTest(str, obj);
				break;
			case T_CoerceToDomain:
				_outCoerceToDomain(str, obj);
				break;
			case T_CoerceToDomainValue:
				_outCoerceToDomainValue(str, obj);
				break;
			case T_SetToDefault:
				_outSetToDefault(str, obj);
				break;
			case T_CurrentOfExpr:
				_outCurrentOfExpr(str, obj);
				break;
			case T_InferenceElem:
				_outInferenceElem(str, obj);
				break;
			case T_TargetEntry:
				_outTargetEntry(str, obj);
				break;
			case T_RangeTblRef:
				_outRangeTblRef(str, obj);
				break;
			case T_JoinExpr:
				_outJoinExpr(str, obj);
				break;
			case T_FromExpr:
				_outFromExpr(str, obj);
				break;
			case T_OnConflictExpr:
				_outOnConflictExpr(str, obj);
				break;
            case T_MergeAction:
                _outMergeAction(str, (MergeAction*)obj);
                break;
			case T_Path:
				_outPath(str, obj);
				break;
			case T_IndexPath:
				_outIndexPath(str, obj);
				break;
			case T_BitmapHeapPath:
				_outBitmapHeapPath(str, obj);
				break;
			case T_BitmapAndPath:
				_outBitmapAndPath(str, obj);
				break;
			case T_BitmapOrPath:
				_outBitmapOrPath(str, obj);
				break;
			case T_TidPath:
				_outTidPath(str, obj);
				break;
			case T_SubqueryScanPath:
				_outSubqueryScanPath(str, obj);
				break;
			case T_ForeignPath:
				_outForeignPath(str, obj);
				break;
			case T_CustomPath:
				_outCustomPath(str, obj);
				break;
			case T_AppendPath:
				_outAppendPath(str, obj);
				break;
			case T_PartIteratorPath:
				_outPartIteratorPath(str, obj);
				break;
			case T_MergeAppendPath:
				_outMergeAppendPath(str, obj);
				break;
			case T_ResultPath:
				_outResultPath(str, obj);
				break;
			case T_MaterialPath:
				_outMaterialPath(str, obj);
				break;
			case T_UniquePath:
				_outUniquePath(str, obj);
				break;
			case T_GatherPath:
				_outGatherPath(str, obj);
				break;
			case T_ProjectionPath:
				_outProjectionPath(str, obj);
				break;
			case T_ProjectSetPath:
				_outProjectSetPath(str, obj);
				break;
			case T_QualPath:
				_outQualPath(str, obj);
				break;
			case T_SortPath:
				_outSortPath(str, obj);
				break;
			case T_GroupPath:
				_outGroupPath(str, obj);
				break;
			case T_UpperUniquePath:
				_outUpperUniquePath(str, obj);
				break;
			case T_AggPath:
				_outAggPath(str, obj);
				break;
			case T_GroupingSetsPath:
				_outGroupingSetsPath(str, obj);
				break;
			case T_MinMaxAggPath:
				_outMinMaxAggPath(str, obj);
				break;
			case T_WindowAggPath:
				_outWindowAggPath(str, obj);
				break;
			case T_SetOpPath:
				_outSetOpPath(str, obj);
				break;
			case T_RecursiveUnionPath:
				_outRecursiveUnionPath(str, obj);
				break;
			case T_LockRowsPath:
				_outLockRowsPath(str, obj);
				break;
			case T_ModifyTablePath:
				_outModifyTablePath(str, obj);
				break;
			case T_LimitPath:
				_outLimitPath(str, obj);
				break;
			case T_GatherMergePath:
				_outGatherMergePath(str, obj);
				break;
			case T_NestPath:
				_outNestPath(str, obj);
				break;
			case T_MergePath:
				_outMergePath(str, obj);
				break;
			case T_HashPath:
				_outHashPath(str, obj);
				break;
			case T_PlannerGlobal:
				_outPlannerGlobal(str, obj);
				break;
			case T_PlannerInfo:
				_outPlannerInfo(str, obj);
				break;
			case T_RelOptInfo:
				_outRelOptInfo(str, obj);
				break;
			case T_IndexOptInfo:
				_outIndexOptInfo(str, obj);
				break;
			case T_ForeignKeyOptInfo:
				_outForeignKeyOptInfo(str, obj);
				break;
			case T_EquivalenceClass:
				_outEquivalenceClass(str, obj);
				break;
			case T_EquivalenceMember:
				_outEquivalenceMember(str, obj);
				break;
			case T_PathKey:
				_outPathKey(str, obj);
				break;
			case T_PathTarget:
				_outPathTarget(str, obj);
				break;
			case T_ParamPathInfo:
				_outParamPathInfo(str, obj);
				break;
			case T_RestrictInfo:
				_outRestrictInfo(str, obj);
				break;
			case T_PlaceHolderVar:
				_outPlaceHolderVar(str, obj);
				break;
			case T_SpecialJoinInfo:
				_outSpecialJoinInfo(str, obj);
				break;
			case T_AppendRelInfo:
				_outAppendRelInfo(str, obj);
				break;
			case T_RowIdentityVarInfo:
				_outRowIdentityVarInfo(str, obj);
				break;
			case T_PlaceHolderInfo:
				_outPlaceHolderInfo(str, obj);
				break;
			case T_MinMaxAggInfo:
				_outMinMaxAggInfo(str, obj);
				break;
			case T_PlannerParamItem:
				_outPlannerParamItem(str, obj);
				break;
			case T_RollupData:
				_outRollupData(str, obj);
				break;
			case T_GroupingSetData:
				_outGroupingSetData(str, obj);
				break;
			case T_StatisticExtInfo:
				_outStatisticExtInfo(str, obj);
				break;
			case T_ExtensibleNode:
				_outExtensibleNode(str, obj);
				break;
			case T_CreateStmt:
				_outCreateStmt(str, obj);
				break;
			case T_ExtTableTypeDesc:
				_outExtTableTypeDesc(str, obj);
				break;
			case T_CreateExternalStmt:
				_outCreateExternalStmt(str, obj);
				break;
			case T_CreateForeignTableStmt:
				_outCreateForeignTableStmt(str, obj);
				break;
			case T_ImportForeignSchemaStmt:
				_outImportForeignSchemaStmt(str, obj);
				break;
			case T_IndexStmt:
				_outIndexStmt(str, obj);
				break;
			case T_CreateStatsStmt:
				_outCreateStatsStmt(str, obj);
				break;
			case T_NotifyStmt:
				_outNotifyStmt(str, obj);
				break;
			case T_DeclareCursorStmt:
				_outDeclareCursorStmt(str, obj);
				break;
			case T_SelectStmt:
				_outSelectStmt(str, obj);
				break;
			case T_ColumnDef:
				_outColumnDef(str, obj);
				break;
			case T_TypeName:
				_outTypeName(str, obj);
				break;
			case T_TypeCast:
				_outTypeCast(str, obj);
				break;
			case T_CollateClause:
				_outCollateClause(str, obj);
				break;
			case T_IndexElem:
				_outIndexElem(str, obj);
				break;
			case T_Query:
				_outQuery(str, obj);
				break;
			case T_WithCheckOption:
				_outWithCheckOption(str, obj);
				break;
			case T_SortGroupClause:
				_outSortGroupClause(str, obj);
				break;
			case T_GroupingSet:
				_outGroupingSet(str, obj);
				break;
			case T_WindowClause:
				_outWindowClause(str, obj);
				break;
			case T_RowMarkClause:
				_outRowMarkClause(str, obj);
				break;
			case T_WithClause:
				_outWithClause(str, obj);
				break;
			case T_CommonTableExpr:
				_outCommonTableExpr(str, obj);
				break;
			case T_SetOperationStmt:
				_outSetOperationStmt(str, obj);
				break;
			case T_RangeTblEntry:
				_outRangeTblEntry(str, obj);
				break;
			case T_RangeTblFunction:
				_outRangeTblFunction(str, obj);
				break;
			case T_TableSampleClause:
				_outTableSampleClause(str, obj);
				break;
			case T_A_Expr:
				_outAExpr(str, obj);
				break;
			case T_ColumnRef:
				_outColumnRef(str, obj);
				break;
			case T_ParamRef:
				_outParamRef(str, obj);
				break;
			case T_A_Const:
				_outAConst(str, obj);
				break;
			case T_A_Star:
				_outA_Star(str, obj);
				break;
			case T_A_Indices:
				_outA_Indices(str, obj);
				break;
			case T_A_Indirection:
				_outA_Indirection(str, obj);
				break;
			case T_A_ArrayExpr:
				_outA_ArrayExpr(str, obj);
				break;
			case T_ResTarget:
				_outResTarget(str, obj);
				break;
			case T_MultiAssignRef:
				_outMultiAssignRef(str, obj);
				break;
			case T_SortBy:
				_outSortBy(str, obj);
				break;
			case T_WindowDef:
				_outWindowDef(str, obj);
				break;
			case T_RangeSubselect:
				_outRangeSubselect(str, obj);
				break;
			case T_RangeFunction:
				_outRangeFunction(str, obj);
				break;
			case T_RangeTableSample:
				_outRangeTableSample(str, obj);
				break;
			case T_RangeTableFunc:
				_outRangeTableFunc(str, obj);
				break;
			case T_RangeTableFuncCol:
				_outRangeTableFuncCol(str, obj);
				break;
			case T_Constraint:
				_outConstraint(str, obj);
				break;
			case T_FuncCall:
				_outFuncCall(str, obj);
				break;
			case T_InsertStmt:
				_outInsertStmt(str, obj);
				break;
			case T_CreateFunctionStmt:
				_outCreateFunctionStmt(str, obj);
				break;
			case T_CreatePackageStmt:
				_outCreatePackageStmt(str, obj);
				break;
			case T_CreatePackageBodyStmt:
				_outCreatePackageBodyStmt(str, obj);
				break;
			/* end ora_compatible */
			case T_DefElem:
				_outDefElem(str, obj);
				break;
			case T_TableLikeClause:
				_outTableLikeClause(str, obj);
				break;
			case T_LockingClause:
				_outLockingClause(str, obj);
				break;
			case T_XmlSerialize:
				_outXmlSerialize(str, obj);
				break;
			case T_ForeignKeyCacheInfo:
				_outForeignKeyCacheInfo(str, obj);
				break;
#ifdef PGXC
			case T_ExecNodes:
				_outExecNodes(str, obj);
				break;
#endif
#ifdef __AUDIT_FGA__
            case T_AuditFgaPolicy:
                _outAuditFgaStmt(str, obj);
                break;
#endif

			case T_TriggerTransition:
				_outTriggerTransition(str, obj);
				break;
			case T_PartitionElem:
				_outPartitionElem(str, obj);
				break;
			case T_PartitionSpec:
				_outPartitionSpec(str, obj);
				break;
			case T_PartitionBoundSpec:
				_outPartitionBoundSpec(str, obj);
				break;
			case T_PartitionRangeDatum:
				_outPartitionRangeDatum(str, obj);
				break;
#ifdef _PG_ORCL_
			case T_PartitionInto:
				_outPartitionInto(str, obj);
				break;
			case T_PartitionMergeSplit:
				_outPartitionMergeSplit(str, obj);
				break;
			case T_PartitionMergeSpec:
				_outPartitionMergeSpec(str, obj);
				break;
			case T_PauseTransactionStmt:
				_outPauseTransactionStmt(str, obj);
				break;
#endif
#ifdef __AUDIT__
			case T_AuditStmt:
				_outAuditStmt(str, obj);
				break;
			case T_CleanAuditStmt:
				_outCleanAuditStmt(str, obj);
				break;
#endif
#ifdef __RESOURCE_QUEUE__
			case T_CreateResourceQueueStmt:
				_outCreateResourceQueueStmt(str, obj);
				break;
			case T_AlterResourceQueueStmt:
				_outAlterResourceQueueStmt(str, obj);
				break;
			case T_DropResourceQueueStmt:
				_outDropResourceQueueStmt(str, obj);
				break;
#endif
			case T_CreateResourceGroupStmt:
				_outCreateResourceGroupStmt(str, obj);
				break;
			case T_AlterResourceGroupStmt:
				_outAlterResourceGroupStmt(str, obj);
				break;
			case T_DropResourceGroupStmt:
				_outDropResourceGroupStmt(str, obj);
				break;
			case T_MergeQualProj:
				_outMergeQualProj(str, obj);
				break;
			case T_MergeQualProjPath:
				_outMergeQualProjPath(str, obj);
				break;
			case T_FdwDataNodeTask:
				_outFdwDataNodeTask(str, obj);
				break;
			case T_FdwFileSegment:
				_outFdwFileSegment(str, obj);
				break;
			case T_FragmentDest:
				_outFragmentDest(str, obj);
				break;
			case T_CopyFormatter:
				_outCopyFormatter(str, obj);
				break;
			case T_CopyAttrFixed:
				_outCopyAttrFixed(str, obj);
			case T_LevelExpr:
				_outLevelExpr(str, obj);
				break;
			case T_PriorExpr:
				_outPriorExpr(str, obj);
				break;
			case T_CBIsCycleExpr:
				_outCBIsCycleExpr(str, obj);
				break;
			case T_CBIsLeafExpr:
				_outCBIsLeafExpr(str, obj);
				break;
			case T_CBRootExpr:
				_outCBRootExpr(str, obj);
				break;
			case T_CBPathExpr:
				_outCBPathExpr(str, obj);
				break;
			case T_PartitionDef:
				_outPartitionDef(str, obj);
				break;
			case T_MultiSetExpr:
				_outMultiSetExpr(str, obj);
				break;
			default:

				/*
				 * This should be an ERROR, but it's too useful to be able to
				 * dump structures that outNode only understands part of.
				 */
				elog(WARNING, "could not dump unrecognized node type: %d",
					 (int) nodeTag(obj));
				break;
		}
		appendStringInfoChar(str, '}');
	}
}

/*
 * nodeToString -
 *	   returns the ascii representation of the Node as a palloc'd string
 */
char *
nodeToString(const void *obj)
{
	StringInfoData str;

	/* see stringinfo.h for an explanation of this maneuver */
	initStringInfo(&str);
	outNode(&str, obj);
	return str.data;
}

/*
 * bmsToString -
 *	   returns the ascii representation of the Bitmapset as a palloc'd string
 */
char *
bmsToString(const Bitmapset *bms)
{
	StringInfoData str;

	/* see stringinfo.h for an explanation of this maneuver */
	initStringInfo(&str);
	outBitmapset(&str, bms);
	return str.data;
}
