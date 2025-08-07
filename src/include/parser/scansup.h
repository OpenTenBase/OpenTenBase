/*-------------------------------------------------------------------------
 *
 * scansup.h
 *	  scanner support routines.  used by both the bootstrap lexer
 * as well as the normal lexer
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/parser/scansup.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef SCANSUP_H
#define SCANSUP_H

extern char *scanstr(const char *s);

extern char *downcase_truncate_identifier(const char *ident, int len,
							 bool warn);

extern char *downcase_identifier(const char *ident, int len,
					bool warn, bool truncate);

extern void truncate_identifier(char *ident, int len, bool warn);

extern bool scanner_isspace(char ch);

extern char *upcase_identifier(const char *ident, int len);
extern char *upcase_truncate_identifier(const char *ident, int len, bool warn);

extern int downcase_ora_ident_strcmp(const char *ora_ident, const char *const_str);
extern char *get_lowercase_ora_ident_str(char *s);
#endif							/* SCANSUP_H */
