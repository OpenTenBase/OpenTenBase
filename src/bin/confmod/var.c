/*
 * Copyright (c) 2023 THL A29 Limited, a Tencent company.
 *
 * This source code file is licensed under the BSD 3-Clause License,
 * you may obtain a copy of the License at http://opensource.org/license/bsd-3-clause/
 * 
 * Terms of the BSD 3-Clause License:
 * --------------------------------------------------------------------
 * Redistribution and use in source and binary forms, with or without modification, are permitted provided that the following conditions are met:
 * 
 * 1. Redistributions of source code must retain the above copyright notice, this list of conditions and the following disclaimer.
 * 
 * 2. Redistributions in binary form must reproduce the above copyright notice, this list of conditions and the following disclaimer in the documentation 
 * and/or other materials provided with the distribution.
 * 
 * 3. Neither the name of THL A29 Limited nor the names of its contributors may be used to endorse or promote products derived from this software without 
 * specific prior written permission.
 * 
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, 
 * THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS 
 * BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE 
 * GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT 
 * LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH 
 * DAMAGE.
 * 
 */
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

