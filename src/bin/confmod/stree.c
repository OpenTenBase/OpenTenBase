#include "stree.h"
#include "util.h"

static stree *
stree_insert_internal(stree * root, stree * node, int (*compare)(void *, void *))
{
    stree * left;
    stree * right;
    int ret;

    if (!root)
        return node;

    left = root->left;
    right = root->right;
    ret = compare(node->data, root->data);

    if (ret > 0)
    {
        if (!right)
        {
            root->right = node;
        }
        else
        {
            stree_insert_internal(right, node, compare);
        }
    }
    else
    {
        if (!left)
        {
            root->left = node;
        }
        else
        {
            stree_insert_internal(left, node, compare);
        }
    }

    return root;
}

stree * 
stree_insert(stree * root, void * data, int (*compare)(void *, void *))
{
    stree * node;

    node = (stree *)Malloc(sizeof(stree));
    node->left = NULL;
    node->right = NULL;
    node->data = data;

    return stree_insert_internal(root, node, compare);
}

void 
stree_pre_traverse(stree * root, void (*traverse)(void *))
{
    if (root)
    {
        stree_pre_traverse(root->left, traverse);
        traverse(root->data);
        stree_pre_traverse(root->right, traverse);
    }
}


