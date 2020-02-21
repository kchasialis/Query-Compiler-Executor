#include "queries.h"
#include "filter.h"
#include "join.h"
#include "quicksort.h"
#include "stretchy_buffer.h"
#include "utilities.h"
#include "join_enumeration.h"

typedef struct exec_query_args {
    query qry;
    metadata *metadata_arr;
    thr_pool_t *pool;
    mid_result ***mid_results_arrays;
    uint32_t job_id;
} exec_query_args;

static void free_mid_result_arrays(mid_result ***mid_results_arrays, ssize_t num_arrays) {

    for (ssize_t curr = 0 ; curr < num_arrays ; curr++) {
        mid_result **mid_results_array = mid_results_arrays[curr];
        for (size_t j = 0 ; j < buf_len(mid_results_array) ; j++) {
            for (size_t i = 0 ; i < buf_len(mid_results_array[j]) ; i++) {
                buf_free(mid_results_array[j][i].payloads);
            }
            buf_free(mid_results_array[j]);
        }
        buf_free(mid_results_array);
    }
    FREE(mid_results_arrays);
}

static inline void swap_predicates(query *qry, ssize_t i, ssize_t j) {

    if (i == j) {
        return;
    }

    predicate temp = qry->predicates[i];
    qry->predicates[i] = qry->predicates[j];
    qry->predicates[j] = temp;
}

static inline bool is_match(predicate *lhs, predicate *rhs) {
   

    relation_column *lhs_second = (relation_column *) lhs->second;
    relation_column *rhs_second = (relation_column *) rhs->second;
    if (lhs->first.column == rhs->first.column && lhs->first.relation == rhs->first.relation) {
        return true;
    }
    else if (lhs->first.column == rhs_second->column && lhs->first.relation == rhs_second->relation) {
        return true;
    }
    else if (lhs_second->column == rhs->first.column && lhs_second->relation == rhs->first.relation) {
        return true;
    }
    else if (lhs_second->column == rhs_second->column && lhs_second->relation == rhs_second->relation) {
        return true;
    }

    return false;
}

static inline void group_matches(query *qry, ssize_t index) {

    for (ssize_t i = index ; i < (ssize_t) qry->predicates_size - 1 ; ) {
        predicate *current = &(qry->predicates[i]);
        bool swapped = false;
        for (ssize_t j = i + 1 ; j < (ssize_t) qry->predicates_size ; j++) {
            predicate *next = &(qry->predicates[j]);
            if (is_match(current, next)) {
                swap_predicates(qry, ++index, j);
                swapped = true;
            }
        }
        if (swapped) {
            i += index - i;
        } else {
            i++;
        }
    }
}

static ssize_t group_filters(query * qry) {

    ssize_t index = 0 ;
    for (ssize_t i = 1 ; i < (ssize_t) qry->predicates_size  ; i++){
        
        if( qry->predicates[i].type == 1) {
            ssize_t swaps = i; 
            for(ssize_t j=0 ; j < i-index ; j++){
                
                swap_predicates(qry, swaps , swaps-1); 
                swaps--;
            }
            index++;
        } 
    }
    return index;         
}

void print_predicates(predicate* predicates, size_t size);
void arrange_predicates(query *qry, metadata *meta, int *null_join) {

    ssize_t index = group_filters(qry);
    
}

static void print_sums(mid_result **mid_results_array, uint32_t *relations, metadata *metadata_arr, relation_column *selects, size_t select_size) {

    for (size_t i = 0 ; i < select_size ; i++) {
        uint32_t rel = relations[selects[i].relation];
        uint32_t col = selects[i].column;

        exists_info exists = relation_exists(mid_results_array, rel, selects[i].relation);
        if (exists.index == -1) {
            log_err("Something went really wrong...");
            exit(EXIT_FAILURE);
        }

        mid_result res = mid_results_array[exists.mid_result][exists.index];

        if (buf_len(res.payloads) == 0) {
            printf("%s", "NULL ");
        } else {
            uint64_t sum = 0;
            tuple *tuples = metadata_arr[rel].data[col]->tuples;
            for (ssize_t j = 0 ; j < buf_len(res.payloads) ; j++) {
                sum += tuples[res.payloads[j]].key;
            }
            printf("%lu ", sum);
        }
    }
    printf("\n");
}


#ifndef MULTITHREAD_QUERIES 
    static int execute_query(query q , metadata* metadata_arr, thr_pool_t *pool) {

        mid_result **mid_results_array = NULL;
    
        for (size_t i = 0 ; i < (size_t)q.predicates_size ; i++) {

            if( q.predicates[i].type == 1) { //filter predicate
                mid_results_array = execute_filter(&q.predicates[i], q.relations, metadata_arr, mid_results_array);
            } else {    //join predicate
                mid_results_array = execute_join(&q.predicates[i], q.relations, metadata_arr, mid_results_array, pool);
            }
        }

        print_sums(mid_results_array, q.relations, metadata_arr, q.selects, q.select_size);

        for (size_t j = 0 ; j < buf_len(mid_results_array) ; j++) {
            for (size_t i = 0 ; i < buf_len(mid_results_array[j]) ; i++) {
                buf_free(mid_results_array[j][i].payloads);
            }
            buf_free(mid_results_array[j]);
        }
        buf_free(mid_results_array);

        return 0;
    }


    void execute_queries(query *q_list, metadata *metadata_arr, thr_pool_t *pool) {

        #ifdef MULTITHREAD_SORT
            thr_pool_t *inner_pool = thr_pool_create(2);
        #else   
            thr_pool_t *inner_pool = NULL;
        #endif

        for (size_t i = 0; i < buf_len(q_list); i++) {

            query qry = q_list[i];

            int null_join = 0;
            arrange_predicates(&qry, metadata_arr, &null_join);

            if (null_join) {
                for (size_t i = 0; i < qry.predicates_size; i++)
                {
                    printf("NULL ");
                }
                printf("\n");
                continue;
            }


            execute_query(qry , metadata_arr, inner_pool);
        }

        #ifdef MULTITHREAD_SORT
            thr_pool_destroy(inner_pool);
        #endif
    }
#else

    static void* execute_query(void *arg) {
    
        exec_query_args *args = (exec_query_args *) arg;

        query qry = args->qry;
        metadata *meta = args->metadata_arr;
        int null_join = 0;
       
        arrange_predicates(&qry, meta, &null_join);

        mid_result **mid_results_array = NULL;

        for (size_t i = 0 ; i < (size_t) qry.predicates_size ; i++) {
            if (qry.predicates[i].type == 0) {
                mid_results_array = execute_join(&(qry.predicates[i]), qry.relations, args->metadata_arr, mid_results_array, args->pool);
            }
            else {
                mid_results_array = execute_filter(&(qry.predicates[i]), qry.relations, args->metadata_arr, mid_results_array);
            }
        }
        args->mid_results_arrays[args->job_id] = mid_results_array;

        return NULL;
    }

    void execute_queries(query *q_list, metadata *metadata_arr, thr_pool_t *pool) {

        exec_query_args **args_array = MALLOC(exec_query_args *, buf_len(q_list));

        #ifdef MULTITHREAD_SORT
            thr_pool_t *inner_pool = thr_pool_create(2);
        #else  
            thr_pool_t *inner_pool = NULL;
        #endif

        mid_result ***mid_results_arrays = MALLOC(mid_result **, buf_len(q_list));

        for (size_t i = 0; i < buf_len(q_list); i++) {

            query current = q_list[i];

            exec_query_args *args = MALLOC(exec_query_args, 1);
            args->metadata_arr = metadata_arr;
            args->mid_results_arrays = mid_results_arrays;
            args->qry = current;
            args->pool = inner_pool;
            args->job_id = i;

            args_array[i] = args;
    
            thr_pool_queue(pool, execute_query, args_array[i]);
        }       
        thr_pool_barrier(pool);

        #ifdef MULTITHREAD_SORT
            thr_pool_destroy(inner_pool);
        #endif

        for (size_t i = 0 ; i < buf_len(q_list) ; i++) {
            exec_query_args *args = args_array[i];
            query current = q_list[i];

            print_sums(args->mid_results_arrays[i], current.relations, args->metadata_arr, current.selects, current.select_size);
        }

        free_mid_result_arrays(mid_results_arrays, buf_len(q_list));

        for (ssize_t i = 0 ; i < buf_len(q_list) ; i++) {
            FREE(args_array[i]);
        }
        FREE(args_array);
    }

#endif
