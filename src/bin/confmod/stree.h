/*
 * Copyright (c) 2023 THL A29 Limited, a Tencent company.
 *
 * This source code file is licensed under the BSD 3-Clause License,
 * you may obtain a copy of the License at http://opensource.org/license/bsd-3-clause/
 */
#ifndef __STREE_H__
#define __STREE_H__

typedef struct stree {
    struct stree * left;
    struct stree * right;
    void * data;
} stree;
/* the numbers of attribute  */
extern stree *stree_insert(stree * root, void * data, int (*compare)(void *, void *));
extern void stree_pre_traverse(stree * root, void (*traverse)(void *));

#endif /* __STREE_H__ */
