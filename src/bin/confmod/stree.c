/*
 * Copyright (c) 2023 THL A29 Limited, a Tencent company.
 *
 * This source code file is licensed under the BSD 3-Clause License,
 * you may obtain a copy of the License at http://opensource.org/license/bsd-3-clause/
 */
#include "stree.h"
#include "util.h"


static stree *
// stree_insert_internal 函数：递归地将节点插入到二叉搜索树中 
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
	//   compare: 比较两个数据大小的函数指针  

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
	//   root: 当前子树的根节点  
	//   node: 要插入的节点  
    }
	//   更新后的子树根节点  
    return root;
}

stree * 
stree_insert(stree * root, void * data, int (*compare)(void *, void *))
{
    stree * node;  // 新节点指针  
  
    // 分配新节点的内存空间  
    node = (stree *)Malloc(sizeof(stree));  
    node->left = NULL;  // 初始化左子节点为空  
    node->right = NULL; // 初始化右子节点为空  
    node->data = data;  // 设置新节点的数据  
  
    // 调用内部插入函数，将新节点插入到二叉搜索树中  

    return stree_insert_internal(root, node, compare);
}
// stree_pre_traverse 函数：对二叉搜索树进行先序遍历  
// 参数：  
//   root: 二叉搜索树的根节点  
//   traverse: 遍历时调用的函数指针

void 
stree_pre_traverse(stree * root, void (*traverse)(void *))
{
    // 如果根节点不为空，则进行遍历  
    if (root)  
    {  
        // 先遍历左子树  
        stree_pre_traverse(root->left, traverse);  
          
        // 访问当前节点  
        traverse(root->data);  
          
        // 再遍历右子树  
        stree_pre_traverse(root->right, traverse);  
    } 
}


