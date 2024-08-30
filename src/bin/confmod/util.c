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

// Strdup 函数：复制一个字符串并返回其指针  
char *  
Strdup(const char *s)  
{  
    char *rv;  // 用于存储复制后的字符串的指针  
  
    // 递增全局变量 Strdup_ed 的值（这个变量在这段代码中未定义，可能在其他地方）  
    Strdup_ed++;  
      
    // 使用系统提供的 strdup 函数复制字符串 s，并将结果赋值给 rv  
    rv = strdup(s);  
      
    // 如果 rv 为 NULL，表示内存分配失败  
    if (rv == NULL)  
    {  
        // 使用 elog 函数记录一条 PANIC 级别的日志，表示没有更多内存可用  
        elog(PANIC, "No more memory.  See core file for details.\n");  
          
        // 终止程序执行  
        abort();  
          
        // 如果上面的 abort() 没有起作用，则退出程序并返回状态码 1  
        exit(1);  
    }  
      
    // 返回复制后的字符串的指针  
    return(rv);  
}  
  
// TrimNl 函数：去除字符串末尾的换行符  
void  
TrimNl(char *s)  
{  
    // 循环遍历字符串 s 直到其末尾或者遇到换行符 '\n'  
    for (;*s && *s != '\n'; s++);  
      
    // 将当前位置（可能是换行符或者字符串末尾）的字符设置为 '\0'，即结束字符串  
    *s = 0;  
}  
  
// MyUsleep 函数：使程序暂停指定的微秒数  
void  
MyUsleep(long microsec)  
{  
    struct timeval delay;  // 用于设置延迟时间的 timeval 结构体  
  
    // 如果指定的微秒数小于等于 0，则直接返回，不执行任何操作  
    if (microsec <= 0)  
        return;  
  
    // 计算需要暂停的秒数和微秒数  
    delay.tv_sec = microsec / 1000000L;  // 秒数部分  
    delay.tv_usec = microsec % 1000000L;  // 微秒数部分  
      
    // 使用 select 函数使程序暂停指定的时间  
    // 这里使用 select 是因为它可以在不改变文件描述符集的情况下引入延迟  
    (void) select(0, NULL, NULL, NULL, &delay);  
}