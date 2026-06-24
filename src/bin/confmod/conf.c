#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>
#include <errno.h>

#include "conf.h"
#include "var.h"
#include "util.h"
#include "log.h"

static char *
get_name(char *line, char **token)
{
    *token = NULL;

    if (!line)
        return NULL;

    for(;*line == ' ' || *line == '\t'; line++);

    if (!*line)
    {
        *token = NULL;
        return NULL;
    }

    if (*line == '#')
    {
        *line = 0;
        *token = NULL;
        return NULL;
    }

    *token = line;
    for (; *line && *line != ' ' && *line != '\t' && *line != '='; line++);

    if (!*line)
    {
        *token = NULL;
        return NULL;
    }
    else
    {
        *line = 0;
        return (line + 1);
    }
}

static char *
get_value(char *line, char **token)
{
    *token = NULL;

    if (!line)
        return NULL;

    for(;*line == ' ' || *line == '\t' || *line == '='; line++);

    if (!*line || *line == '#')
    {
        *token = NULL;
        return NULL;
    }

    *token = line;
    return line;
}

static int
parse_line(char *line, const char * del, int lineno)
{
    char *name;
    char *value;
    pg_conf_var *newv;

    line = get_name(line, &name);
    if (!name)
        return -1;

    if (del && strcmp(del, name) == 0)
        return 0;

    line = get_value(line, &value);
    if (!value)
        return -2;

    if (!(newv = confirm_var(name)))
    {
        elog(ERROR, "Failed to confirm guc '%s' in hash table, exit \n", name);
        exit(1);
    }

    set_value(newv, value);
    set_line(newv, lineno);

    return 0;
}

int
read_vars(FILE *conf, const char * del)
{
    char line[MAXLINE+1];
    int lineno = 0;

    while (fgets(line, MAXLINE, conf))
        parse_line(line, del, lineno++);

    return lineno;
}


