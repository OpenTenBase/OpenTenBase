/*-------------------------------------------------------------------------
 *
 * varlena.h
 *	  Functions for the variable-length built-in types.
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/utils/varlena.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef VARLENA_H
#define VARLENA_H

#include "nodes/pg_list.h"
#include "utils/sortsupport.h"
#include "fmgr.h"
#include "utils/array.h"

extern int	varstr_cmp(char *arg1, int len1, char *arg2, int len2, Oid collid);
extern void varstr_sortsupport(SortSupport ssup, Oid collid, bool bpchar);
extern int varstr_levenshtein(const char *source, int slen,
				   const char *target, int tlen,
				   int ins_c, int del_c, int sub_c,
				   bool trusted);
extern int varstr_levenshtein_less_equal(const char *source, int slen,
							  const char *target, int tlen,
							  int ins_c, int del_c, int sub_c,
							  int max_d, bool trusted);
extern List *textToQualifiedNameList(text *textval);
extern bool SplitIdentifierStringInternal(char *rawstring, char separator,
					  List **namelist, bool upper);
extern bool SplitDirectoriesString(char *rawstring, char separator,
					   List **namelist);
extern text *replace_text_regexp(text *src_text, void *regexp,
					text *replace_text, bool glob);
extern text *text_catenate(text *t1, text *t2);
extern int	text_cmp(text *arg1, text *arg2, Oid collid);
extern int32 text_length(Datum str);
extern text *text_substring(Datum str,
			   int32 start,
			   int32 length,
			   bool length_not_specified);
extern int internal_text_pattern_compare(text *arg1, text *arg2);
extern text *array_to_text_internal(FunctionCallInfo fcinfo, ArrayType *v,
					   const char *fldsep, const char *null_string);

extern text *replace_text_regexp_opentenbase_ora(text *src_text, void *regexp,
					text *replace_text,
					int start_position,
					int match_occurence,
					bool glob);
extern void assign_serveroutput(bool newval, void *extra);
extern char* ByteaToHex(bytea *vlena, bool need_prefix);
extern char* ByteaToEscape(bytea *vlena, int *dst_len);

/* Import the macro to avoid more code change */
#define SplitIdentifierString(rawstring, separator, namelist) SplitIdentifierStringInternal(rawstring, separator, namelist, false)
extern List *textToQualifiedNameListAsPG(text *textval);

#define MAX_RAW_SIZE (2000)
#endif
