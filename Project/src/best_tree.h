#ifndef BEST_TREE
#define BEST_TREE
#include "join.h"

typedef struct tree {
    int all_rel;
    int type;
    int rel;
    int col;
    predicate *preds;
    int pred_size;
    int *rels;
    int rel_size;
    statistics **stats;
    struct tree *right;
    struct tree *left;
} tree;


typedef struct q_node { 
    struct q_node *next; 
    char *name; 
    tree *tree; 
} q_node;

tree *BestTree(char *s, q_node *hashtab[]);
q_node *BestTree_set(char *name, tree *tree, q_node *hashtab[]);
void BestTree_delete();
#endif