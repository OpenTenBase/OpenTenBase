#ifndef __UTIL_H__
#define __UTIL_H__

#include <stdlib.h>
#include <stdio.h>

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

#define true 1
#define false 0
#define TRUE 1
#define FALSE 0

#endif

