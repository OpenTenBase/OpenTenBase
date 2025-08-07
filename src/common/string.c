/*-------------------------------------------------------------------------
 *
 * string.c
 *		string handling helpers
 *
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/common/string.c
 *
 *-------------------------------------------------------------------------
 */


#ifndef FRONTEND
#include "postgres.h"
#else
#include "postgres_fe.h"
#endif

#include "common/string.h"

static int count_common_eol_char(const char *str);

/*
 * Returns whether the string `str' has the postfix `end'.
 */
bool
pg_str_endswith(const char *str, const char *end)
{
	size_t		slen = strlen(str);
	size_t		elen = strlen(end);

	/* can't be a postfix if longer */
	if (elen > slen)
		return false;

	/* compare the end of the strings */
	str += slen - elen;
	return strcmp(str, end) == 0;
}

/*
 * Replace all before strings to after strings in buf strings.
 * NOTE: Assumes there is enough room in the buf buffer!
 */
void
replaceAll(char *buf, const char *before, const char *after)
{
	char	   *dup = pstrdup(buf);

	//copy buf, and malloc
	char		*ptr1,
				*ptr2;

	int			dup_len = strlen(dup);
	int			before_len = strlen(before);
	int			after_len = strlen(after);

	ptr1 = dup;
	while ((ptr2 = strstr(ptr1, before)) != NULL)
	{
		strncpy(buf, ptr1, ptr2 - ptr1);
		buf += (ptr2 - ptr1);

		strncpy(buf, after, after_len);
		buf += after_len;

		ptr1 = ptr2 + before_len;
	}
	strncpy(buf, ptr1, dup + dup_len - ptr1);
	buf += (dup + dup_len - ptr1);
	strcpy(buf, "\0");

	pfree(dup);
}

/*
 * Count how many times str2 appear in str1.
 * Notice that count_substring("ppppp", "pp") = 2.
 */
int
count_substring(const char *str1, const char *str2)
{
	int count = 0;
	int str2_len = strlen(str2);
	const char *ptr = str1;
	const char *end = str1 + strlen(str1);

	if ((str1 != NULL) && (str2 != NULL))
	{
		while (ptr != end)
		{
			ptr = strstr(ptr, str2);

			if (ptr == NULL)
				break;
			else
			{
				count += 1;
				ptr += str2_len;
			}
		}
	}

	return count;
}

/*
 * Replace the strings of escape characters like "\r" in str_raw by strings
 * like "\\r". By the way, do remember to free memory after invoking this
 * function.
 */
char*
replace_eol_char(const char *str_raw)
{
	int eol_char_count = 0;
	int str_raw_len = strlen(str_raw);
	char *str_buf = NULL;

	eol_char_count += count_common_eol_char(str_raw);
	str_buf = (char*) palloc((str_raw_len + eol_char_count + 1) * sizeof(char));
	memcpy(str_buf, str_raw, str_raw_len + 1);

	if (eol_char_count > 0)
	{
		replaceAll(str_buf, "\b", "\\b");
		replaceAll(str_buf, "\f", "\\f");
		replaceAll(str_buf, "\n", "\\n");
		replaceAll(str_buf, "\r", "\\r");
		replaceAll(str_buf, "\t", "\\t");
	}

	return str_buf;
}

/* Count the strings of the common end-of-line characters in str. */
static int
count_common_eol_char(const char *str)
{
	int count = 0;

	count += count_substring(str, "\b");
	count += count_substring(str, "\f");
	count += count_substring(str, "\n");
	count += count_substring(str, "\r");
	count += count_substring(str, "\t");

	return count;
}

/*
 * Returns whether the string `str' has the prefix `end'.
 */
bool
pg_str_startwith(const char *str, const char *start)
{
	size_t		slen = strlen(str);
	size_t		elen = strlen(start);

	/* can't be a prefix if longer */
	if (elen > slen)
		return false;

	return strncmp(str, start, elen) == 0;
}

/* 
 * Locate the last occurrence of substring str2 in string str1
 */ 
char * 
pg_str_strrstr(const char *str1, const char *str2) 
{
	const char *pstr, *plast = NULL;
	pstr = str1;
	while((pstr = strstr(pstr, str2))) 
    {
       	plast = pstr;
        pstr += strlen(str2);
    }
	return (char *)plast;
}
