#include "join.h"
#include "join_utilities.h"
#include "stretchy_buffer.h"

join_result scan_join(relation *relR, relation *relS) {

    join_result res;

    uint64_t *results_r = NULL;
    uint64_t *results_s = NULL;

    size_t iterations = relR->num_tuples < relS->num_tuples ? relR->num_tuples : relS->num_tuples;
    for (size_t i = 0 ; i < iterations ; i++) {
        if (relR->tuples[i].key == relS->tuples[i].key) {
            buf_push(results_r, relR->tuples[i].payload);
            buf_push(results_s, relS->tuples[i].payload);
        }
    }

    res.results[0] = results_r;
    res.results[1] = results_s;

    return res;

}

join_result join_relations_single_threaded(relation *relR, relation *relS) {

    join_result join_res;

    uint64_t *results_r = NULL;
    uint64_t *results_s = NULL;

    size_t pr = 0; 
    size_t s_start = 0; 

    while (pr < relR->num_tuples && s_start < relS->num_tuples) {

        size_t ps = s_start;
        int flag = 0;
        while (ps < relS->num_tuples) {
            if (relR->tuples[pr].key < relS->tuples[ps].key) {
                break;
            }

            if (relR->tuples[pr].key > relS->tuples[ps].key) {
                ps++;
                if (flag == 0) {
                    s_start = ps;
                }
            }
            else {
                
                buf_push(results_r, relR->tuples[pr].payload);
                buf_push(results_s, relS->tuples[ps].payload);

                flag = 1;
                ps++;
            }
        }
        pr++;
    }

    join_res.results[0] = results_r;
    join_res.results[1] = results_s;
    
    return join_res;
}

void *join_job(void *args) {

    join_arguments *argm = (join_arguments *) args;
    relation *relR = argm->rel_R;
    relation *relS = argm->rel_S;

    join_result join_res;

    uint64_t *results_r = NULL;
    uint64_t *results_s = NULL;

    size_t found = 0;
    for (ssize_t i = argm->start ; i < argm->end ; i++) {
        queue_node qnode_R = argm->queue_R[argm->indices_to_check[0][i]];
        queue_node qnode_S = argm->queue_S[argm->indices_to_check[1][i]];
        
        size_t pr = qnode_R.base;
        size_t r_end = qnode_R.base + qnode_R.size;
        size_t s_start = qnode_S.base;
        size_t s_end = qnode_S.base + qnode_S.size;

        while (pr < r_end && s_start < s_end) {
            
            size_t ps = s_start;
            int flag = 0;

            while (ps < s_end) {
                if (relR->tuples[pr].key < relS->tuples[ps].key) {
                    break;
                }

                if (relR->tuples[pr].key > relS->tuples[ps].key) {
                    ps++;
                    if (flag == 0) {
                        s_start = ps;
                    }
                }
                else {
                    found++;
                    buf_push(results_r, relR->tuples[pr].payload);
                    buf_push(results_s, relS->tuples[ps].payload);
                    
                    flag = 1;
                    ps++;
                }
            }
            pr++;
        }
    }

    join_res.results[0] = results_r;
    join_res.results[1] = results_s;

    argm->join_res = join_res;
    
    return NULL;
}


join_result join_relations(relation *relR, relation *relS, queue_node *queue_R, queue_node *queue_S, uint32_t jobs_to_create, thr_pool_t *pool) {

	uint32_t common_bytes = 0;
    uint16_t **indices_to_check = MALLOC(uint16_t *, 2);
    indices_to_check[0] = MALLOC(uint16_t, 256);
    indices_to_check[1] = MALLOC(uint16_t, 256);

    for (size_t i = 0 ; i < buf_len(queue_R) ; i++) {
        queue_node qnode_R = queue_R[i];
        for (size_t j = 0 ; j < buf_len(queue_S) ; j++) {
            queue_node qnode_S = queue_S[j];
            if (qnode_S.byte == qnode_R.byte) {
                indices_to_check[0][common_bytes] = i; 
                indices_to_check[1][common_bytes] = j;
                common_bytes++;
            }
        }
    }   

    join_arguments *params = MALLOC(join_arguments, jobs_to_create);
    for (ssize_t i = 0 ; i < jobs_to_create ; i++) {
        
        uint32_t start = i * (common_bytes / jobs_to_create);
        uint32_t end = (i + 1) * (common_bytes / jobs_to_create);
        if (i == jobs_to_create - 1) {
            end += common_bytes % jobs_to_create;
        }
        params[i].end = end;
        params[i].indices_to_check = indices_to_check;
        params[i].start = start;
        params[i].rel_R = relR;
        params[i].rel_S = relS;
        params[i].queue_R = queue_R;
        params[i].queue_S = queue_S;
        
        thr_pool_queue(pool, join_job, (void *) &params[i]);
    }
    thr_pool_barrier(pool);

    join_result res;

    uint64_t *concat_r = params[0].join_res.results[0];
    uint64_t *concat_s = params[0].join_res.results[1];

    for (ssize_t i = 1 ; i < jobs_to_create ; i++) {
        /*Concatenate all results from jobs*/
        uint64_t *results_r = params[i].join_res.results[0];
        uint64_t *results_s = params[i].join_res.results[1];
        
        for (size_t j = 0 ; j < buf_len(results_r) ; j++) {
            buf_push(concat_r, results_r[j]);
            buf_push(concat_s, results_s[j]);
        }

        buf_free(results_r);
        buf_free(results_s);
    }

    FREE(indices_to_check[0]);
    FREE(indices_to_check[1]);
    FREE(indices_to_check);
    FREE(params);

    res.results[0] = concat_r;
    res.results[1] = concat_s;
    
    return res;
}

bool is_sorted(relation *rel) {
    for (ssize_t i = 0 ; i < rel->num_tuples - 1 ; i++) {
        if (rel->tuples[i].key > rel->tuples[i + 1].key) {
            return false;
        }
    }
    return true;
}

typedef struct sort_call_arguments {
    relation *rel;
    queue_node **queue;
    uint32_t *jobs_to_create;
    thr_pool_t *pool;
} sort_call_arguments;

void* iterative_sort_job(void *argm) {
    
    sort_call_arguments *arg = (sort_call_arguments *) argm;

    relation *rel = arg->rel;
    queue_node **queue = arg->queue;
    uint32_t *jobs_to_create = arg->jobs_to_create;
    thr_pool_t *pool = arg->pool;

    iterative_sort(rel, queue, jobs_to_create, pool);
    return NULL;
}

mid_result** execute_join(predicate *pred, uint32_t *relations, metadata *metadata_arr, mid_result **mid_results_array, thr_pool_t *pool) {

    rel_info *rel[2];
    rel[0] = MALLOC(rel_info, 1);
    rel[1] = MALLOC(rel_info, 1);

    /*Possible methods :
    1) {parallel queries}
    2) {parallel queries, sort (buckets)}
    3) {parallel queries, sort (buckets), join}
    4) {parallel queries, sort (2 sort parallel), join}
    5) {parallel sort (buckets), join}
*/

    int retval = build_relations(pred, relations, metadata_arr, &mid_results_array, rel);
    
    join_result join_res;

    uint32_t first_relation = relations[pred->first.relation];
    uint32_t second_relation = relations[((relation_column *) pred->second)->relation];
    uint32_t first_column = pred->first.column;
    uint32_t second_column =  ((relation_column *) pred->second)->column;

    #ifndef MULTITHREAD_SORT_CALL

        if (retval != SCAN_JOIN) {
            iterative_sort(rel[0]->tuples, &(rel[0]->queue), &(rel[0]->jobs_to_create), pool);
            iterative_sort(rel[1]->tuples, &(rel[1]->queue), &(rel[1]->jobs_to_create), pool);
        }
        else if (retval == SCAN_JOIN) {
            rel[0]->queue = NULL;
            rel[1]->queue = NULL;
            
            join_res = scan_join(rel[0]->tuples, rel[1]->tuples);
            buf_free(join_res.results[1]);
            goto scan_joined;
        }
        else {
            return NULL;
        }
    #else
        
        if (retval != SCAN_JOIN) {
            thr_pool_t *temp_pool = thr_pool_create(2);
            sort_call_arguments *args = MALLOC(sort_call_arguments, 2);

            args[0].rel = rel[0]->tuples;
            args[0].queue = &(rel[0]->queue);
            args[0].jobs_to_create = &(rel[0]->jobs_to_create);
            args[0].pool = pool;

            args[1].rel = rel[1]->tuples;
            args[1].queue = &(rel[1]->queue);
            args[1].jobs_to_create = &(rel[1]->jobs_to_create);
            args[1].pool = pool;

            thr_pool_queue(temp_pool, iterative_sort_job, &(args[0]));
            thr_pool_queue(temp_pool, iterative_sort_job, &(args[1]));

            thr_pool_barrier(temp_pool);

            thr_pool_destroy(temp_pool);
            FREE(args);
        }
        else if (retval == SCAN_JOIN) {
            rel[0]->queue = NULL;
            rel[1]->queue = NULL;
            
            join_res = scan_join(rel[0]->tuples, rel[1]->tuples);
            buf_free(join_res.results[1]);
            goto scan_joined;
        }
        else {
            return NULL;
        }
    #endif

    uint32_t jobs_to_create = rel[0]->jobs_to_create < rel[1]->jobs_to_create ? rel[0]->jobs_to_create : rel[1]->jobs_to_create;

    if (jobs_to_create != 0) {
        join_res = join_relations(rel[0]->tuples, rel[1]->tuples, rel[0]->queue, rel[1]->queue, jobs_to_create, pool);
    }
    else {
        join_res = join_relations_single_threaded(rel[0]->tuples, rel[1]->tuples);
    }

    join_info info;

    scan_joined:

    info.relR = first_relation;
    info.relS = second_relation;
    info.colR = first_column;
    info.colS = second_column;
    info.predR_id = pred->first.relation;
    info.predS_id = ((relation_column *) pred->second)->relation;
    info.join_id = retval;
    info.join_res = join_res;

    update_mid_results(mid_results_array, metadata_arr, info);

    FREE(rel[0]->tuples->tuples);
    FREE(rel[1]->tuples->tuples);
    FREE(rel[0]->tuples);
    FREE(rel[1]->tuples);
    if (rel[0]->queue) {
        buf_free(rel[0]->queue);
    }
    if (rel[1]->queue) {
       buf_free(rel[1]->queue);
    }
    FREE(rel[0]);
    FREE(rel[1]);

    return mid_results_array;
}