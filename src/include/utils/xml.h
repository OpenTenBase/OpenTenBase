/*-------------------------------------------------------------------------
 *
 * xml.h
 *	  Declarations for XML data type support.
 *
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/utils/xml.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef XML_H
#define XML_H

#include "fmgr.h"
#include "nodes/execnodes.h"
#include "nodes/primnodes.h"
#include "executor/tablefunc.h"

typedef struct varlena xmltype;

typedef enum
{
	XML_STANDALONE_YES,
	XML_STANDALONE_NO,
	XML_STANDALONE_NO_VALUE,
	XML_STANDALONE_OMITTED
}			XmlStandaloneType;

typedef enum
{
	XMLBINARY_BASE64,
	XMLBINARY_HEX
}			XmlBinaryType;

typedef enum
{
	PG_XML_STRICTNESS_LEGACY,	/* ignore errors unless function result
								 * indicates error condition */
	PG_XML_STRICTNESS_WELLFORMED,	/* ignore non-parser messages */
	PG_XML_STRICTNESS_ALL		/* report all notices/warnings/errors */
} PgXmlStrictness;

/* struct PgXmlErrorContext is private to xml.c */
typedef struct PgXmlErrorContext PgXmlErrorContext;

#define DatumGetXmlP(X)		((xmltype *) PG_DETOAST_DATUM(X))
#define XmlPGetDatum(X)		PointerGetDatum(X)

#define PG_GETARG_XML_P(n)	DatumGetXmlP(PG_GETARG_DATUM(n))
#define PG_RETURN_XML_P(x)	PG_RETURN_POINTER(x)

extern void pg_xml_init_library(void);
extern PgXmlErrorContext *pg_xml_init(PgXmlStrictness strictness);
extern void pg_xml_done(PgXmlErrorContext *errcxt, bool isError);
extern bool pg_xml_error_occurred(PgXmlErrorContext *errcxt);
extern void xml_ereport(PgXmlErrorContext *errcxt, int level, int sqlcode,
			const char *msg);

extern xmltype *xmlconcat(List *args);
extern xmltype *xmlelement(XmlExpr *xexpr,
		   Datum *named_argvalue, bool *named_argnull,
		   Datum *argvalue, bool *argnull);
extern xmltype *xmlparse(text *data, XmlOptionType xmloption, int preserve_whitespace_or_wellformed);
extern xmltype *xmlpi(char *target, text *arg, bool arg_is_null, bool *result_is_null);
extern xmltype *xmlroot(xmltype *data, text *version, int standalone);
extern bool xml_is_document(xmltype *arg);
extern text *xmltotext_with_xmloption(xmltype *data, XmlOptionType xmloption_arg);
extern char *escape_xml(const char *str);

extern char *map_sql_identifier_to_xml_name(char *ident, bool fully_escaped, bool escape_period);
extern char *map_xml_name_to_sql_identifier(char *name);
extern char *map_sql_value_to_xml_value(Datum value, Oid type, bool xml_escape_strings);

extern bool is_opentenbase_ora_xmltype(Oid typoid);

extern int	xmlbinary;			/* XmlBinaryType, but int for guc enum */

extern int	xmloption;			/* XmlOptionType, but int for guc enum */

extern const TableFuncRoutine XmlTableRoutine;

#ifdef USE_LIBXML
#include <libxml/chvalid.h>
#include <libxml/parser.h>
#include <libxml/parserInternals.h>
#include <libxml/tree.h>
#include <libxml/uri.h>
#include <libxml/xmlerror.h>
#include <libxml/xmlversion.h>
#include <libxml/xmlwriter.h>
#include <libxml/xpath.h>
#include <libxml/xpathInternals.h>
#include <libxml/xmlreader.h>

/*
 * We used to check for xmlStructuredErrorContext via a configure test; but
 * that doesn't work on Windows, so instead use this grottier method of
 * testing the library version number.
 */
#if LIBXML_VERSION >= 20704
#define HAVE_XMLSTRUCTUREDERRORCONTEXT 1
#endif

extern xmlDocPtr xml_parse(text *data, XmlOptionType xmloption_arg,
						   bool preserve_whitespace, int encoding, bool need_check);
extern ArrayType * trans_nspstring_to_array(char *nspstring);
extern void xpath_internal(text *xpath_expr_text, xmltype *data, ArrayType *namespaces,
			   int *res_nitems, ArrayBuildState *astate);
extern xmlChar *pg_xmlCharStrndup(char *str, size_t len);
extern int parse_xml_decl(const xmlChar *str, size_t *lenp,
						  xmlChar **version, xmlChar **encoding, int *standalone);
extern int xml_xpathobjtoxmlarray(xmlXPathObjectPtr xpathobj,
								  ArrayBuildState *astate,
								  PgXmlErrorContext *xmlerrcxt);
void opentenbaseOraXmlRemoveBlankNodes(xmlNodePtr node);
#endif

#endif							/* XML_H */
