/*-------------------------------------------------------------------------
 *
 * rbtree.h
 *      interface for PostgreSQL generic Red-Black binary tree package
 *
 * Copyright (c) 2009-2017, PostgreSQL Global Development Group
 *
 * This source code file contains modifications made by THL A29 Limited ("Tencent Modifications").
 * All Tencent Modifications are Copyright (C) 2023 THL A29 Limited.
 *
 * IDENTIFICATION
 *        src/include/lib/rbtree.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef RBTREE_H
#define RBTREE_H

/*
 * RBNode is intended to be used as the first field of a larger struct,
 * whose additional fields carry whatever payload data the caller needs
 * for a tree entry.  (The total size of that larger struct is passed to
 * rb_create.)    RBNode is declared here to support this usage, but
 * callers must treat it as an opaque struct.
 */
typedef struct RBNode
{
    char color;                    /* node's current color, red or black */
    struct RBNode *left;        /* left child, or RBNIL if none */
    struct RBNode *right;        /* right child, or RBNIL if none */
    struct RBNode *parent;        /* parent, or NULL (not RBNIL!) if none */
} RBNode;

/* Opaque struct representing a whole tree */
typedef struct RBTree RBTree;

/* Available tree iteration orderings */
typedef enum RBOrderControl
{
    LeftRightWalk,                /* inorder: left child, node, right child */
    RightLeftWalk,                /* reverse inorder: right, node, left */
    DirectWalk,                    /* preorder: node, left child, right child */
    InvertedWalk                /* postorder: left child, right child, node */
} RBOrderControl;

/*
 * RBTreeIterator holds state while traversing a tree.  This is declared
 * here so that callers can stack-allocate this, but must otherwise be
 * treated as an opaque struct.
 */
typedef struct RBTreeIterator RBTreeIterator;

struct RBTreeIterator
{
    RBTree       *rb;
    RBNode       *(*iterate) (RBTreeIterator *iter);
    RBNode       *last_visited;
    char        next_step;
    bool        is_over;
};

/* Support functions to be provided by caller */
typedef int (*rb_comparator) (const RBNode *a, const RBNode *b, void *arg);
typedef void (*rb_combiner) (RBNode *existing, const RBNode *newdata, void *arg);
typedef RBNode *(*rb_allocfunc) (void *arg);
typedef void (*rb_freefunc) (RBNode *x, void *arg);

extern RBTree *rb_create(Size node_size,
          rb_comparator comparator,
          rb_combiner combiner,
          rb_allocfunc allocfunc,
          rb_freefunc freefunc,
          void *arg);

extern RBNode *rb_find(RBTree *rb, const RBNode *data);
extern RBNode *rb_leftmost(RBTree *rb);

extern RBNode *rb_insert(RBTree *rb, const RBNode *data, bool *isNew);
extern void rb_delete(RBTree *rb, RBNode *node);

extern void rb_begin_iterate(RBTree *rb, RBOrderControl ctrl,
                 RBTreeIterator *iter);
extern RBNode *rb_iterate(RBTreeIterator *iter);

#endif                            /* RBTREE_H */
