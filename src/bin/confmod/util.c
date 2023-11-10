/*
 * Copyright (c) 2023 THL A29 Limited, a Tencent company.
 *
 * This source code file is licensed under the BSD 3-Clause License,
 * you may obtain a copy of the License at http://opensource.org/license/bsd-3-clause
 * 
 */
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <unistd.h>
#include <errno.h>
#include <stdio.h>
#include <time.h>
#include <stdio.h>

#include "util.h"
#include "log.h"

static int Malloc_ed = 0;
static int Strdup_ed = 0;
static int Freed = 0;

void
*Malloc(size_t size)
{
    void *rv = malloc(size);

    Malloc_ed++;
    if (rv == NULL)
    {
        elog(PANIC, "No more memory.  See core file for details.\n");
        abort();
        exit(1);
    }
    return(rv);
}

void *
Malloc0(size_t size)
{
    void *rv = malloc(size);

    Malloc_ed++;
    if (rv == NULL)
    {
        elog(PANIC, "No more memory.  See core file for details. \n");
        abort();
        exit(1);
    }
    memset(rv, 0, size);
    return(rv);
}

void *
Realloc(void *ptr, size_t size)
{
    void *rv = realloc(ptr, size);

    if (rv == NULL)
    {
        elog(PANIC, "No more memory.  See core file for details. \n");
        abort();
        exit(1);
    }
    return(rv);
}

void
Free(void *ptr)
{
    Freed++;
    if (ptr)
        free(ptr);
}

/*
 * If flag is TRUE and chdir fails, then exit(1)
 */
int
Chdir(char *path, int flag)
{
    if (chdir(path))
    {
        elog(ERROR, "Could not change work directory to \"%s\". %s%s \n", 
             path, flag == TRUE ? "Exiting. " : "", strerror(errno));
        if (flag == TRUE)
            exit(1);
        else
            return -1;
    }

    return 0;
}

FILE *
Fopen(char *path, char *mode)
{
    FILE *rv;

    if ((rv = fopen(path, mode)) == NULL)
        elog(ERROR, "Could not open the file \"%s\" in \"%s\", %s\n", path, mode, strerror(errno));
    return(rv);
}

char *
Strdup(const char *s)
{
    char *rv;

    Strdup_ed++;
    rv = strdup(s);
    if (rv == NULL)
    {
        elog(PANIC, "No more memory.  See core file for details.\n");
        abort();
        exit(1);
    }
    return(rv);
}

void
TrimNl(char *s)
{
    for (;*s && *s != '\n'; s++);
    *s = 0;
}

void
MyUsleep(long microsec)
{
    struct timeval delay;

    if (microsec <= 0)
        return;

    delay.tv_sec = microsec / 1000000L;
    delay.tv_usec = microsec % 1000000L;
    (void) select(0, NULL, NULL, NULL, &delay);
}
