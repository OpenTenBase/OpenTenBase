#ifndef __STREE_H__
#define __STREE_H__

typedef struct stree {
    struct stree * left;
    struct stree * right;
    void * data;
} stree;

extern stree *stree_insert(stree * root, void * data, int (*compare)(void *, void *));
extern void stree_pre_traverse(stree * root, void (*traverse)(void *));

#endif
