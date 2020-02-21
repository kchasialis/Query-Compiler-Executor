#include "filter.h"
#include "join.h"
#include "structs.h"
#include "stretchy_buffer.h"

static void exec_filter_rel_exists(predicate *pred , relation* rel , uint64_t number , mid_result *mid_res) {  

    for(size_t i = 0 ; i <  buf_len(mid_res->payloads) ; i++) {   
    
        uint64_t payload = mid_res->payloads[i];
        //every payload that does not satisfy the new filter remove it from the dynamic array 
        if (pred->operator == EQ) {
            if (rel->tuples[payload].key != number) {
                buf_remove(mid_res->payloads, i);
                i--;
            }
        }
        else if (pred->operator == G) {
            if (rel->tuples[payload].key <= number ) { 
                buf_remove(mid_res->payloads, i);
                i--;
            } 
        }
        else if (pred->operator == L) { 
            if (rel->tuples[payload].key >= number ) {   
                buf_remove(mid_res->payloads, i);
                i--;
            }               
        }
    }
}

static void exec_filter_rel_no_exists(predicate *pred,relation* rel , uint64_t *number ,uint64_t **payloads) {

    for (size_t i = 0 ; i < rel->num_tuples; i++) {
        
        //if the tuple satisfies the filter push it in the dynamic array of the payloads
        if ( pred->operator == EQ){
            if (rel->tuples[i].key == *number) {
                buf_push((*payloads), rel->tuples[i].payload);
            }
        }
        else if (pred->operator == G) {
            if (rel->tuples[i].key > *number) {
                buf_push((*payloads), rel->tuples[i].payload);
            }
        }
        else if (pred->operator == L) {
            if (rel->tuples[i].key < *number ) {
                buf_push((*payloads), rel->tuples[i].payload);  
            }
        }
    }
}

mid_result** execute_filter(predicate *pred, uint32_t *relations, metadata *metadata_arr, mid_result **mid_results_array) {

    metadata tmp_data = metadata_arr[relations[pred->first.relation]];
    relation *rel = tmp_data.data[pred->first.column];
    uint64_t *number = (uint64_t*) pred->second; 

    exists_info exists = relation_exists(mid_results_array, relations[pred->first.relation], pred->first.relation);
    if (exists.index != -1)  {

        mid_result *current = &(mid_results_array[exists.mid_result][exists.index]);
        
        if (pred->operator != SELF_JOIN) {
            exec_filter_rel_exists(pred, rel, *number, current);
        }
        else {
            relation_column rel_col = *(relation_column *) pred->second;
            metadata *md = &(metadata_arr[relations[rel_col.relation]]);
            relation *rel_s = md->data[rel_col.column];
            
            join_result join_res = scan_join(rel, rel_s);
            uint64_t *to_destroy = current->payloads;
            current->payloads = join_res.results[0];
            buf_free(to_destroy);
        }
    } else {
        mid_result res;
        res.relation = relations[pred->first.relation];
        res.predicate_id = pred->first.relation;
        res.last_column_sorted = -1;

        buf_push(mid_results_array, NULL);
        
        if (pred->operator != SELF_JOIN) {
            
            res.payloads = NULL;
            exec_filter_rel_no_exists(pred, rel, number, &res.payloads);

            buf_push(mid_results_array[buf_len(mid_results_array) - 1], res);
        }
        else {
            relation_column rel_col = *(relation_column *) pred->second;
            metadata *md = &(metadata_arr[relations[rel_col.relation]]);
            relation *rel_s = md->data[rel_col.column];
            
            join_result join_res = scan_join(rel, rel_s);
            res.payloads = join_res.results[0];
            
            buf_push(mid_results_array[buf_len(mid_results_array) - 1], res);
        }
    }

    return mid_results_array;
}