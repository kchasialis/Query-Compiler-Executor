#include "join_utilities.h"
#include "quicksort.h"
#include "stretchy_buffer.h"

static void allocate_relation_mid_results(rel_info *rel_arr[], mid_result mid_res, tuple *tuples, ssize_t rel_arr_index) {

    relation *temp_rel = MALLOC(relation, 1);

    temp_rel->num_tuples = buf_len(mid_res.payloads);
    temp_rel->tuples = MALLOC(tuple, temp_rel->num_tuples);
    for (size_t i = 0 ; i < buf_len(mid_res.payloads) ; i++) {
        temp_rel->tuples[i].key = tuples[mid_res.payloads[i]].key;
        temp_rel->tuples[i].payload = i;
    }

    rel_arr[rel_arr_index]->tuples = temp_rel;
}

static void allocate_relation(rel_info *rel_arr[], relation *rel, ssize_t rel_arr_index) {

    relation *temp_rel = MALLOC(relation, 1);

    temp_rel->num_tuples = rel->num_tuples;
    temp_rel->tuples = MALLOC(tuple, temp_rel->num_tuples);
    for (size_t i = 0 ; i < temp_rel->num_tuples ; i++) {
        temp_rel->tuples[i].key = rel->tuples[i].key;
        temp_rel->tuples[i].payload = rel->tuples[i].payload;
    }

    rel_arr[rel_arr_index]->tuples = temp_rel;

}


int build_relations(predicate *pred, uint32_t *relations, metadata *metadata_arr, mid_result ***mid_results_array_ptr, rel_info *rel[]) {

    uint64_t lhs_rel = relations[pred->first.relation];
    uint64_t lhs_col = pred->first.column;

    relation_column *second = (relation_column *) pred->second;
    uint64_t rhs_rel = relations[second->relation];
    uint64_t rhs_col = second->column;

    if (lhs_rel == rhs_rel && lhs_col == rhs_col) {
        return DO_NOTHING;
    }

    mid_result **mid_results_array = *mid_results_array_ptr;

    exists_info exists_r = relation_exists(mid_results_array, lhs_rel, pred->first.relation);
    exists_info exists_s = relation_exists(mid_results_array, rhs_rel, second->relation);

    if (exists_r.index != -1 && exists_s.index == -1) {

        allocate_relation_mid_results(rel, mid_results_array[exists_r.mid_result][exists_r.index], metadata_arr[lhs_rel].data[lhs_col]->tuples, 0);

        allocate_relation(rel, metadata_arr[rhs_rel].data[rhs_col], 1);

        if (mid_results_array[exists_r.mid_result][exists_r.index].last_column_sorted == (int32_t) lhs_col) {
            return JOIN_SORT_RHS;
        }
        else {
            mid_results_array[exists_r.mid_result][exists_r.index].last_column_sorted = lhs_col;
            return CLASSIC_JOIN;
        }
    }
    else if (exists_r.index == -1 && exists_s.index != -1) {

        allocate_relation_mid_results(rel, mid_results_array[exists_s.mid_result][exists_s.index], metadata_arr[rhs_rel].data[rhs_col]->tuples, 1);

        allocate_relation(rel, metadata_arr[lhs_rel].data[lhs_col], 0);

        if (mid_results_array[exists_s.mid_result][exists_s.index].last_column_sorted == (int32_t) rhs_col) {
            return JOIN_SORT_LHS;
        }
        else {
            return CLASSIC_JOIN;
        }
    }
    else if (exists_r.index != -1 && exists_s.index != -1) {
        
        allocate_relation_mid_results(rel, mid_results_array[exists_s.mid_result][exists_s.index], metadata_arr[rhs_rel].data[rhs_col]->tuples, 1);
        allocate_relation_mid_results(rel, mid_results_array[exists_r.mid_result][exists_r.index], metadata_arr[lhs_rel].data[lhs_col]->tuples, 0);
        
        if (exists_r.mid_result == exists_s.mid_result) {    
            return SCAN_JOIN;
        }
        else {
            if (mid_results_array[exists_r.mid_result][exists_r.index].last_column_sorted == lhs_col && mid_results_array[exists_s.mid_result][exists_s.index].last_column_sorted == rhs_col) {
                return SCAN_JOIN;
            }
            else if (mid_results_array[exists_r.mid_result][exists_r.index].last_column_sorted == lhs_col) {
                return JOIN_SORT_RHS;
            }
            else if (mid_results_array[exists_s.mid_result][exists_s.index].last_column_sorted == rhs_col) {
                return JOIN_SORT_LHS;
            }
            else {
                return CLASSIC_JOIN;
            }
        }
    }
    else {

        allocate_relation(rel, metadata_arr[rhs_rel].data[rhs_col], 1);
        allocate_relation(rel, metadata_arr[lhs_rel].data[lhs_col], 0);
        
        mid_result *mid_res = NULL;
        buf_push((*mid_results_array_ptr), mid_res);

        return CLASSIC_JOIN;
    }
}

static void fix_all_mid_results(mid_result **mid_results_array, ssize_t mid_result_index, ssize_t index, mid_result *current_mid_res, uint64_t *join_payloads, bool is_scan) {

    for (size_t i = 0 ; i < buf_len(mid_results_array[mid_result_index]) ; i++) {
        uint64_t *old_payloads = mid_results_array[mid_result_index][i].payloads;
        mid_results_array[mid_result_index][i].payloads = NULL;
        for (size_t j = 0 ; j < buf_len(join_payloads) ; j++) {
            buf_push(mid_results_array[mid_result_index][i].payloads, old_payloads[join_payloads[j]]);
        }
            
        buf_free(old_payloads);
    }
    if (!is_scan) {
        mid_results_array[mid_result_index][index].last_column_sorted = current_mid_res->last_column_sorted;
        mid_results_array[mid_result_index][index].predicate_id = current_mid_res->predicate_id;
        mid_results_array[mid_result_index][index].relation = current_mid_res->relation;
    }
}

void update_mid_results(mid_result **mid_results_array, metadata *metadata_arr, join_info info) {

    mid_result tmp_R;
    tmp_R.last_column_sorted = info.colR;
    tmp_R.relation = info.relR;
    tmp_R.payloads = info.join_res.results[0];
    tmp_R.predicate_id = info.predR_id;

    mid_result tmp_S;
    tmp_S.last_column_sorted = info.colS;
    tmp_S.relation = info.relS;
    tmp_S.payloads = info.join_res.results[1];
    tmp_S.predicate_id = info.predS_id;

    int join_id = info.join_id;

    /*Debug that in work_tmp*/

    if (join_id == CLASSIC_JOIN) {
        exists_info exists_r = relation_exists(mid_results_array, info.relR, info.predR_id);
        if (exists_r.index != -1) {
            fix_all_mid_results(mid_results_array, exists_r.mid_result, exists_r.index, &tmp_R ,info.join_res.results[0], 0);
            buf_free(tmp_R.payloads);
        }

        exists_info exists_s = relation_exists(mid_results_array, info.relS, info.predS_id);
        if (exists_s.index != -1) {
            fix_all_mid_results(mid_results_array, exists_s.mid_result, exists_s.index, &tmp_S ,info.join_res.results[1], 0);
            buf_free(tmp_S.payloads);
        }

        if (exists_r.index == -1) {
            if (exists_s.index != -1) {
                buf_push(mid_results_array[exists_s.mid_result], tmp_R); 
            }
            else {
                buf_push(mid_results_array[buf_len(mid_results_array) - 1], tmp_R);
            }
        }
        if (exists_s.index == -1) {
            if (exists_r.index != -1) {
                buf_push(mid_results_array[exists_r.mid_result], tmp_S); 
            }
            else {
                buf_push(mid_results_array[buf_len(mid_results_array) - 1], tmp_S);
            }
        }
    }
    else if (join_id == JOIN_SORT_LHS) {

        exists_info exists_r = relation_exists(mid_results_array, info.relR, info.predR_id);
        if (exists_r.index != -1) {
            fix_all_mid_results(mid_results_array, exists_r.mid_result, exists_r.index, &tmp_R, info.join_res.results[0], 0);
            buf_free(tmp_R.payloads);
        }

        exists_info exists_s = relation_exists(mid_results_array, info.relS, info.predS_id);
        if (exists_s.index == -1) {
            log_err("Something went really wrong");
            exit(EXIT_FAILURE);
        } 

        fix_all_mid_results(mid_results_array, exists_s.mid_result, exists_s.index, &tmp_S, info.join_res.results[1], 0);

        buf_free(tmp_S.payloads);

        if (exists_r.index == -1) {
            buf_push(mid_results_array[exists_s.mid_result], tmp_R);
        }
    }
    else if (join_id == JOIN_SORT_RHS) {

        exists_info exists_s = relation_exists(mid_results_array, info.relS, info.predS_id);
        if (exists_s.index != -1) {
            fix_all_mid_results(mid_results_array, exists_s.mid_result, exists_s.index, &tmp_S, info.join_res.results[1], 0);
            buf_free(tmp_S.payloads);
        }

        exists_info exists_r = relation_exists(mid_results_array, info.relR, info.predR_id);
        if (exists_r.index == -1) {
            log_err("Something went really wrong");
            exit(EXIT_FAILURE);
        }

        fix_all_mid_results(mid_results_array, exists_r.mid_result, exists_r.index, &tmp_R, info.join_res.results[0], 0);
        buf_free(tmp_R.payloads);

        if (exists_s.index == -1) {
            buf_push(mid_results_array[exists_r.mid_result], tmp_S);
        }
    }
    else if (join_id == SCAN_JOIN) {
        exists_info exists = relation_exists(mid_results_array, info.relR, info.predR_id);

        if (exists.index == -1) {
            log_err("Something went really wrong");
            exit(EXIT_FAILURE);
        }

        fix_all_mid_results(mid_results_array, exists.mid_result, -1, NULL, info.join_res.results[0], 1);
        buf_free(info.join_res.results[0]);
    }
}
