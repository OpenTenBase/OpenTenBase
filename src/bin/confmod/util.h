/*
 * Copyright (c) 2023 THL A29 Limited, a Tencent company.
 *
 * This source code file is licensed under the BSD 3-Clause License,
 * you may obtain a copy of the License at http://opensource.org/license/bsd-3-clause/
 */
#ifndef __UTIL_H__
#define __UTIL_H__

#include <stdlib.h>
#include <stdio.h>
/* related function declaration */
extern void *Malloc(size_t size);
extern void *Malloc0(size_t size);
extern void *Realloc(void *ptr, size_t size);
extern void Free(void *ptr);
extern int Chdir(char *path, int flag);
extern FILE *Fopen(char *path, char *mode);
extern char *Strdup(const char *s);
extern void TrimNl(char *s);
extern void MyUsleep(long microsec);

#define freeAndReset(x) do{Free(x);(x)=NULL;}while(0)
#define myWEXITSTATUS(rc) ((rc) & 0x000000FF)

#define MAXPATH (1024-1)
#define MAXLINE (8192-1)
#define MAXTOKEN (128-1)
/* common macro define*/
#define true 1
#define false 0
#define TRUE 1
#define FALSE 0

#endif /* __UTIL_H__ */
