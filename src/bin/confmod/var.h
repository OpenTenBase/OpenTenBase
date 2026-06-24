#ifndef __VAR_H__
#define __VAR_H__

#include <stdio.h>
#include <stdlib.h>

#include "stree.h"

typedef struct pg_conf_var {
    struct pg_conf_var *next;
    char    *name;
    char    *value;
    int     line;
} pg_conf_var;

typedef struct pg_conf_var_hash {
    pg_conf_var *head;
} pg_conf_var_hash;

extern void init_var_hash(void);
extern void print_var_hash(void);

extern pg_conf_var *confirm_var(char *name);
extern void set_value(pg_conf_var *var, char *value);
extern void set_line(pg_conf_var *var, int line);
extern stree *var_hash_2_stree(void);

#endif
