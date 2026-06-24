/*
 *	string.h
 *		string handling helpers
 *
 *	Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 *	Portions Copyright (c) 1994, Regents of the University of California
 *
 *	src/include/common/string.h
 */
#ifndef COMMON_STRING_H
#define COMMON_STRING_H

extern bool pg_str_endswith(const char *str, const char *end);
extern void	replaceAll(char *buf, const char *before, const char *after);
extern int count_substring(const char *str1, const char *str2);
extern char* replace_eol_char(const char *str_raw);
extern bool pg_str_startwith(const char *str, const char *start);

extern char *pg_str_strrstr(const char *str1, const char *str2);
#endif							/* COMMON_STRING_H */
