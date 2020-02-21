#include "join_enumeration.h"
#include "stretchy_buffer.h"

#pragma GCC diagnostic ignored "-Wunused-function"


//works for numbers smaller than 10
char *to_string(int rel) {
    char *s = (char *) malloc(sizeof(char)*2);
    sprintf(s, "%d", rel);
    return s;
}

char *copy_string(char* str) {
    char *s = (char *) malloc(strlen(str)+1);
    strcpy(s, str);
    return s;
}

int in_string(char* str, int rel) {
    char *rel_str = to_string(rel);
    int size = strlen(str);
    for (size_t i = 0; i < size; i++) {
        if (str[i] ==  rel_str[0]) {
            free(rel_str);
            return 1;
        }
    }
    
    free(rel_str);
    return 0;
}

char *append_string(int rel, char* str) {
    int new_size = strlen(str) + 1;
    char* new = (char *) malloc(sizeof(char)*(new_size + 1));
    char* rel_str = to_string(rel);

    int str_index = 0, new_index = 0;
    
    while (str_index < strlen(str) && str[str_index] < rel_str[0])
    {
        new[new_index++] = str[str_index++];
    }
    new[new_index++] = rel_str[0];
    while (str_index < strlen(str))
    {
        new[new_index++] = str[str_index++];    
    }
    new[new_index] = '\0';
    

    free(rel_str);
    return new;
    
}

void get_columns(int rel_a, int rel_b, predicate *predicates, int pred_size, predicate ***preds, int **inverted) {

    for (size_t i = 0; i < pred_size; i++) {
        if (predicates[i].first.relation == rel_a && ((relation_column*)predicates[i].second)->relation == rel_b){
            buf_push((*preds), &predicates[i]);
            buf_push((*inverted), 0);
        } else if (predicates[i].first.relation == rel_b && ((relation_column*)predicates[i].second)->relation == rel_a) {
            buf_push((*preds), &predicates[i]);
            buf_push((*inverted), 1);
        }
    }
    
}

 void calculate_statistics(int rel_a, int rel_b, int col_a, int col_b, statistics **stats, statistics *stat) {

    if (stats[rel_a][col_a].min_value > stats[rel_b][col_b].min_value) stat->min_value = stats[rel_a][col_a].min_value;
    else stat->min_value = stats[rel_b][col_b].min_value;

    if (stats[rel_a][col_a].max_value < stats[rel_b][col_b].max_value) stat->max_value = stats[rel_a][col_a].max_value;
    else stat->max_value = stats[rel_b][col_b].max_value;

    uint64_t f_a, f_b;
    f_a = ((stat->max_value - stat->min_value) * stats[rel_a][col_a].approx_elements)/(stats[rel_a][col_a].max_value - stats[rel_a][col_a].min_value + 1);
    f_b = ((stat->max_value - stat->min_value) * stats[rel_b][col_b].approx_elements)/(stats[rel_b][col_b].max_value - stats[rel_b][col_b].min_value + 1);

    uint64_t n = stat->max_value - stat->min_value + 1;
    stat->approx_elements = (f_a*f_b)/n;
}

void free_tree(tree *t){
    free(t->preds);
    free(t->rels);
    for (size_t i = 0; i < t->all_rel; i++)
    {
        free(t->stats[i]);
    }
    free(t->stats);
    free(t);
}

tree *to_tree(int rel,const metadata *meta, uint32_t *relations, int relation_size){
    tree *new;
    new = (tree*) malloc(sizeof(tree));
    new->left = NULL;
    new->right = NULL;
    new->type = 1;
    new->rel = rel;
    new->all_rel = relation_size;

    new->pred_size = 0;
    new->preds = NULL;
    

    new->rel_size = 1;
    new->rels = (int*) malloc(sizeof(int)*relation_size);
    for (size_t i = 0; i < relation_size; i++) {
        new->rels[i] = -1;
    }
    
    new->rels[rel] = rel;
    

    new->stats = (statistics**) malloc(sizeof(statistics*)*relation_size);
    for (size_t i = 0; i < relation_size; i++){
        new->stats[i] = NULL;
    }


    metadata current_stat = meta[relations[rel]];
    new->stats[rel] = (statistics*) malloc(sizeof(statistics)*current_stat.columns);
    for (size_t i = 0; i < current_stat.columns; i++) {
        //printf("%d %d\n", rel, i);
        new->stats[rel][i] = *(current_stat.data[i]->stats);
    }
    

    return new;
}

int predicate_compare(predicate p1, predicate p2) {
    if (p1.first.column != p2.first.column) return 0;
    if (p1.first.relation != p2.first.relation) return 0;
    if (((relation_column*) p1.second)->column != ((relation_column*) p2.second)->column) return 0;
    if (((relation_column*) p1.second)->relation != ((relation_column*) p2.second)->relation) return 0;
    //if (p1.operator != p2.operator) return 0;
    //if (p1.type != p2.type) return 0;
    return 1;

}

tree *create_join_tree(tree *t, int rel, const metadata *meta, int *flag, uint32_t *relations, int relation_size, predicate* all_predicates, int predicate_size, int** graph) {
    //printf("new relation -> %d\n", rel);
    tree *n_tree = (tree*) malloc(sizeof(tree));
    n_tree->type = 0;
    n_tree->left = t;
    n_tree->right = to_tree(rel, meta, relations, relation_size);
    n_tree->all_rel = relation_size;

    //RELATIONS
    n_tree->rel_size = n_tree->left->rel_size + n_tree->right->rel_size;
    n_tree->rels = (int*) malloc(sizeof(int) * relation_size);
    for (size_t i = 0; i < relation_size; i++) {
        n_tree->rels[i] = t->rels[i];
    }
    
    //PREDICATES
    n_tree->pred_size = 0;
    for (size_t i = 0; i < relation_size; i++) {
        if (t->rels[i] != -1) n_tree->pred_size += graph[i][rel];
    } 

    n_tree->pred_size += t->pred_size;
    n_tree->preds = (predicate*) malloc(sizeof(predicate)*n_tree->pred_size);

    for (size_t i = 0; i < t->pred_size; i++) {
        n_tree->preds[i] = t->preds[i];
    }
    
    //STATISTICS
    n_tree->stats = (statistics**) malloc(sizeof(statistics*)*relation_size);
    for (size_t i = 0; i < relation_size; i++){
        if (t->stats[i] != NULL){

            metadata current_stat = meta[relations[i]];
            n_tree->stats[i] = (statistics*) malloc(sizeof(statistics)*current_stat.columns);
            for (size_t j = 0; j < current_stat.columns; j++) 
                n_tree->stats[i][j] = t->stats[i][j];
            
        } else {

            n_tree->stats[i] = NULL;

        }
    }
    
    metadata current_stat = meta[relations[rel]];
    n_tree->stats[rel] = (statistics*) malloc(sizeof(statistics)*current_stat.columns);
    for (size_t i = 0; i < current_stat.columns; i++) {
        n_tree->stats[rel][i] = *(current_stat.data[i]->stats);
    }

    
    //FIND THE BEST PREDICATE
    int best_rel_a = -1, best_rel_b;
    int best_col_a, best_col_b;
    statistics best_stat_all;
    best_stat_all.approx_elements = -1;
    predicate *best_pred_all = NULL;
    
    for (int i = 0; i < relation_size; i++) {
        //relation doesnt exist
        if (n_tree->rels[i] == -1) continue;

        int rel_a = i;
        int rel_b = rel;

        //find the predicates in which rel_a and rel_b participate
        predicate **preds = NULL;
        int *inverted = NULL;
        get_columns(rel_a, rel_b, all_predicates, predicate_size, &preds, &inverted);

        //relations doesnt participate in any predicate
        if (buf_len(preds) == 0) continue;
        
        
        int t_best_col_a = -1, t_best_col_b;
        statistics stat, best_stat;
        predicate *best_pred;

        for (size_t j = 0; j < buf_len(preds); j++) {
            int col_a, col_b;

            predicate *pred = preds[j];
            if (inverted[j]){
                col_b = pred->first.column;
                col_a = ((relation_column*) pred->second)->column;
            } else {
                col_a = pred->first.column;
                col_b = ((relation_column*) pred->second)->column;
            }
            
            calculate_statistics(rel_a, rel_b, col_a, col_b, n_tree->stats, &stat);
            
            //min value of one relation is greater from the max value of the other
            if (stat.approx_elements == -1) {
                *flag = 1;
                return NULL;
            }

            if (t_best_col_a == -1){
                t_best_col_a = col_a;
                t_best_col_b = col_b;
                best_stat = stat;
                best_pred = pred;
            } else if (best_stat.approx_elements > stat.approx_elements) {
                t_best_col_a = col_a;
                t_best_col_b = col_b;
                best_stat = stat;
                best_pred = pred;
            }
        }
        
        if (best_rel_a == -1) {
            best_rel_a = rel_a;
            best_rel_b = rel_b;
            best_col_a = t_best_col_a;
            best_col_b = t_best_col_b;
            best_stat_all = best_stat;
            best_pred_all = best_pred;
        } else if (best_stat_all.approx_elements > best_stat.approx_elements) {
            best_rel_a = rel_a;
            best_rel_b = rel_b;
            best_col_a = t_best_col_a;
            best_col_b = t_best_col_b;
            best_stat_all = best_stat;
            best_pred_all = best_pred;
        }
        buf_free(preds);
        buf_free(inverted);  
    } 

    
    //SOMETHING WENT REALLY WRONG
    if (best_rel_a == -1) return NULL;
    
    n_tree->rels[rel] = rel;
    n_tree->rel = best_rel_a;
    n_tree->col = best_col_a;
    
    n_tree->stats[best_rel_a][best_col_a] = best_stat_all;
    n_tree->stats[best_rel_b][best_col_b] = best_stat_all;

    //FIX OTHER COLUMNS
    for (size_t i = 0; i < n_tree->rel_size; i++) {

        if (n_tree->stats[i] == NULL) continue;
        
        //metadata md_a = meta[relations[i]];
        for (size_t j = 0; j < meta[relations[i]].columns; j++) {
            if ((i == best_rel_a && j == best_col_a) ||
                (i == best_rel_b && j == best_col_b)) {
                    continue;
            }
            n_tree->stats[i][j].approx_elements = best_stat_all.approx_elements;
        }

    }

    //ADD THE BEST PREDICATE
    n_tree->preds[t->pred_size] = *best_pred_all;
    
    //IF THERE ARE ANY OTHER PREDICATES BETWEEN REL_A AND REL_B ADD THEM HERE
    size_t i = t->pred_size + 1;
    for (size_t j = 0; j < predicate_size && i < n_tree->pred_size; j++) {
        if(all_predicates[j].first.relation == rel || ((relation_column*)all_predicates[j].second)->relation == rel){
            if (predicate_compare(all_predicates[j], n_tree->preds[t->pred_size])) continue;
            else n_tree->preds[i++] = all_predicates[j];
        }
    }
    
    free_tree(n_tree->right);
    
    return n_tree;
}

int **predicate_graph(predicate *preds, int pred_size, int rel_size) {
    int **graph = (int**) malloc(rel_size*sizeof(int*));
    for (size_t i = 0; i < rel_size; i++) {
        graph[i] = (int*) malloc(rel_size*sizeof(int));

        for (size_t j = 0; j < rel_size; j++) graph[i][j] = 0;
    }

    for (size_t i = 0; i < pred_size; i++) {
        int index_1 = preds[i].first.relation;
        int index_2 = (*(relation_column *) preds[i].second).relation;

        graph[index_1][index_2]++;
        graph[index_2][index_1]++;
    }
    return graph;
    
}

int connected(char *str, int rel, int** graph) {
    for (size_t i = 0; i < strlen(str); i++) {
        if (graph[str[i] - '0'][rel]) return 1;
    }
    return 0;
}


int cost(tree *t){
    return t->stats[t->rel][t->col].approx_elements;
}


static predicate *dp_linear(uint32_t *relations, int rel_size, predicate *predicates, int pred_size, const metadata *meta, int *null_join, ssize_t index){
    q_node *hashtab[101];
    for (size_t i = 0; i < 101; i++)
    {
        hashtab[i] = NULL;
    }
    
    queue *set = new_queue();
    int **graph = predicate_graph(predicates, pred_size, rel_size);
    
    int flag = 0;

    for (size_t i = 0; i < rel_size; i++) {
        char* n_set = to_string(i);
        BestTree_set(n_set, to_tree(i, meta, relations, rel_size), hashtab);
        push(set, copy_string(n_set));
    }

    for (size_t i = 0; i < rel_size - 1; i++) {
        
        int set_size = q_size(set);
        
        for (size_t j = 0; j < set_size; j++) {
            char* old_set = pop(set);
            

            for (size_t k = 0; k < rel_size; k++) {
                
                if (!in_string(old_set, k) && connected(old_set, k, graph)) {
                    
                    tree *curr_tree = create_join_tree(BestTree(old_set, hashtab), k, meta, &flag, relations, rel_size, predicates, pred_size, graph);
                    if (flag) {
                        printf("    nulll");
                        *null_join = 1;
                        return NULL;
                    }
                    char *new_set = append_string(k, old_set);
                    
                    if (BestTree(new_set, hashtab) == NULL || cost(BestTree(new_set, hashtab)) > cost(curr_tree)){
                        
                        if (BestTree(new_set, hashtab) == NULL) push(set, copy_string(new_set));
                        BestTree_set(new_set, curr_tree, hashtab);
                    } else {
                        free(new_set);
                        free_tree(curr_tree);
                    }
                
                }
            }
            free(old_set);
            
        }
        
    }
    char* res = pop(set);
    
    delete_queue(set);
    for (size_t i = 0; i < rel_size; i++) {
        free(graph[i]);
    }
    free(graph);
    
    predicate *tmp = BestTree(res, hashtab)->preds;
    predicate *result = (predicate*) malloc(sizeof(predicate)*rel_size);
    
    for (size_t i = 0; i < pred_size; i++) {
        result[i] = tmp[i];
        result[i].second = (relation_column*) malloc(sizeof(relation_column));
        ((relation_column*)result[i].second)->column = ((relation_column*)tmp[i].second)->column;
        ((relation_column*)result[i].second)->relation = ((relation_column*)tmp[i].second)->relation;
    }
    for (size_t i = 0; i < index; i++)
    {
        FREE(tmp[i].second);
    }
    
    BestTree_delete(hashtab);
    free(res);

    return result;
    
}