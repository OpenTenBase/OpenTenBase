#ifndef COMMON_H
#define COMMON_H

#include <errno.h>
#include "license_c.h"
#include "license_timestamp.h"

#define MaxAllocSize	((Size) 0x3fffffff)		/* 1 gigabyte - 1 */

#define palloc(sz) malloc(sz)
#define repalloc(pt,sz) realloc(pt,sz)
#define pfree(pt) free(pt)

#define elog(a, ...) do { 	if(is_print())	printf(__VA_ARGS__); } while(0)

#define ereport(a, b) 
#define errmsg(a, ...) 

int is_print(void);
void switch_elog(int ison);


#endif
