#include <stdlib.h>
#include <string.h>
#include <stdarg.h>

#include "var.h"
#include "util.h"

#define NUM_HASH_BUCKET 256
static pg_conf_var_hash var_hash[NUM_HASH_BUCKET];

static int
hash_value(char *name)
{
    unsigned char *name_u = (unsigned char *)name;
    unsigned int v;

    for(v = 0; *name_u; name_u++)
        v += *name_u;
    return (v%NUM_HASH_BUCKET);
}

void
init_var_hash()
{
    int i;

    for (i = 0; i < NUM_HASH_BUCKET; i++)
    {
        var_hash[i].head = NULL;
    }
}

void 
print_var_hash()
{
    int i;

    for (i = 0; i < NUM_HASH_BUCKET; i++)
    {
        pg_conf_var *head = var_hash[i].head;

        while (head)
        {
            if (head->name && head->value)
            {
                printf("%04d: %s = %s", head->line, head->name, head->value);
            }

            head = head->next;
        }
    }
}

static void
add_var_hash(pg_conf_var *var)
{
    int hash_v = hash_value(var->name);
    var->next = var_hash[hash_v].head;
    var_hash[hash_v].head = var;
}

static pg_conf_var *
find_var(char *name)
{
    pg_conf_var_hash *hash = &var_hash[hash_value(name)];
    pg_conf_var *head = hash->head;

    while (head)
    {
        if (strcmp(head->name, name) == 0)
            return head;
        head = head->next;
    }

    return NULL;
}

static pg_conf_var *
new_var(char *name)
{
    pg_conf_var *newv;

    newv = (pg_conf_var *)Malloc(sizeof(pg_conf_var));
    newv->next = NULL;
    newv->name = Strdup(name);
    newv->value = NULL;
    newv->line = 0;

    add_var_hash(newv);

    return(newv);
}

pg_conf_var *
confirm_var(char *name)
{
    pg_conf_var *rc;
    if ((rc = find_var(name)))
        return rc;
    return new_var(name);
}

void
set_value(pg_conf_var *var, char *value)
{
    if (var->value)
    {
        freeAndReset(var->value);
    }

    var->value = Strdup(value);
}

void 
set_line(pg_conf_var *var, int line)
{
    var->line = line;
}

static int
var_compare(void *data0, void *data1)
{
    pg_conf_var * var0 = (pg_conf_var *)data0;
    pg_conf_var * var1 = (pg_conf_var *)data1;

    return var0->line - var1->line;
}

stree *
var_hash_2_stree()
{
    stree * root = NULL;
    int i;

    for (i = 0; i < NUM_HASH_BUCKET; i++)
    {
        pg_conf_var *head = var_hash[i].head;

        while (head)
        {
            if (head->name && head->value)
            {
                root = stree_insert(root, (void *) head, var_compare);
            }

            head = head->next;
        }
    }

    return root;
}

