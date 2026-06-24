#include <stdio.h>
#include <stdlib.h>
#include <stdarg.h>
#include <sys/types.h>
#include <errno.h>
#include <string.h>
#include <unistd.h>
#include <time.h>
#include <fcntl.h>

#include "log.h"
#include "util.h"

static FILE *logFile = NULL;
static int logMsgLevel = INFO;

static char *fname = NULL;
static char *funcname = NULL;
static int lineno = -1;

static char timebuf[MAXTOKEN+1] = { 0 };
static char *elog_time(void)
{
    struct tm *tm_s;
    time_t now;

    now = time(NULL);
    tm_s = localtime(&now);

    snprintf(timebuf, MAXTOKEN, "%02d-%02d-%02d %02d:%02d:%02d", 
             ((tm_s->tm_year+1900) >= 2000) ? (tm_s->tm_year + (1900 - 2000)) : tm_s->tm_year, 
             tm_s->tm_mon+1, tm_s->tm_mday, tm_s->tm_hour, tm_s->tm_min, tm_s->tm_sec);
    return timebuf;
}

void
elog_start(const char *file, const char *func, int line)
{
    fname = Strdup(file);
    funcname = Strdup(func);
    lineno = line;

    if (!logFile)
        logFile = stderr;
}

static void
clean_location(void)
{
    freeAndReset(fname);
    freeAndReset(funcname);
    lineno = -1;
}

static const char * 
elog_level(int level)
{
    const char * ret = NULL;
    switch(level)
    {
        case DEBUG3:
            ret = "DEBUG3";
            break;
        case DEBUG2:
            ret = "DEBUG2";
            break;
        case DEBUG1:
            ret = "DEBUG1";
            break;
        case INFO:
            ret = "INFO";
            break;
        case NOTICE2:
            ret = "NOTICE2";
            break;
        case NOTICE:
            ret = "NOTICE";
            break;
        case WARNING:
            ret = "WARNING";
            break;
        case ERROR:
            ret = "ERROR";
            break;
        case PANIC:
            ret = "PANIC";
            break;
        case MANDATORY:
            ret = "MANDATORY";
            break;
        default:
            ret = "UNKNOWN";
            break;
    }

    return ret;
}

static void
elogMsgRaw(int level, const char *msg)
{
    if (logFile && level >= logMsgLevel)
    {
        fprintf(logFile, "[%s] [%s:%s:%d] [%s]: %s", 
            elog_time(), fname, funcname, lineno, elog_level(level), msg);
        fflush(logFile);
    }
    clean_location();
}

void
elogFinish(int level, const char *fmt, ...)
{
    char msg[MAXLINE+1];
    va_list arg;

    va_start(arg, fmt);
    vsnprintf(msg, MAXLINE, fmt, arg);
    va_end(arg);
    elogMsgRaw(level, msg);
}
