#include "join.h"
#include "best_tree.h"



#define HASHSIZE 101


unsigned hash(char *s) {
    unsigned hashval;
    for (hashval = 0; *s != '\0'; s++)
      hashval = *s + 31 * hashval;
    return hashval % HASHSIZE;
}



q_node *find(char *s, q_node *hashtab[HASHSIZE]) {
    q_node *np;
    for (np = hashtab[hash(s)]; np != NULL; np = np->next)
        if (strcmp(s, np->name) == 0) return np;
    return NULL; 
}

tree *BestTree(char *s, q_node *hashtab[HASHSIZE]) {
    if (find(s, hashtab)) return find(s, hashtab)->tree;
    else return NULL;
}

tree *create(tree *t) {
    tree *p;
    p = (tree *) malloc(sizeof(tree));
    p = t;
    return p;
}


q_node *BestTree_set(char *name, tree *tree, q_node *hashtab[HASHSIZE]) {
    q_node *np;
    unsigned hashval;

    if ((np = find(name, hashtab)) == NULL) { 

        np = (q_node *) malloc(sizeof(q_node));
        if (np == NULL) return NULL;
        
        hashval = hash(name);
        np->next = hashtab[hashval];
        np->name = name;
        hashtab[hashval] = np;

    } else {
        free(name);
        free(np->tree->preds);
        for (size_t i = 0; i < np->tree->all_rel; i++)
        {               
            free(np->tree->stats[i]);
        }
        free(np->tree->rels);
        free(np->tree->stats);
        free(np->tree); /*free previous tree */

    }

    np->tree = tree;

    return np;
}

static void free_tree(tree *t){
    //if (t->left != NULL) free_tree(t->left);
    //if (t->right != NULL) free_tree(t->right);
    free(t->preds);
    free(t->rels);
    for (size_t i = 0; i < t->all_rel; i++)
    {
        free(t->stats[i]);
    }
    free(t->stats);
    free(t);
}

void BestTree_delete(q_node *hashtab[HASHSIZE]){
    q_node *np;
    q_node *temp;
    for (size_t i = 0; i < HASHSIZE; i++) {
        for (np = hashtab[i]; np != NULL;){
            temp = np;
            np = np->next;
            free_tree(temp->tree);
            free(temp->name);
            free(temp);
        }
        hashtab[i] = NULL;
    }
    
}