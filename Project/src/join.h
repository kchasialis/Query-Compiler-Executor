#ifndef JOIN_H
#define JOIN_H

#include <stdio.h>
#include <stdint.h>
#include <stdarg.h>
#include "structs.h"
#include "alloc_free.h"
#include "dbg.h"
#include "utilities.h"
#include "quicksort.h"
#include "queries.h"

typedef struct join_result {
    uint64_t *results[2];
} join_result;

typedef struct join_info {
    uint32_t relR;
    uint32_t relS;
    uint32_t colR;
    uint32_t colS;
    uint32_t predR_id;
    uint32_t predS_id;
    int join_id;
    join_result join_res;
} join_info;

typedef struct join_arguments {
    uint32_t start;
    uint32_t end;
    uint16_t **indices_to_check;
    relation *rel_R;
    relation *rel_S;
    queue_node *queue_R;
    queue_node *queue_S;
    join_result join_res;
} join_arguments;

typedef struct rel_info {
    relation *tuples;
    queue_node *queue;
    uint32_t jobs_to_create;
} rel_info; 

join_result scan_join(relation *relR, relation *relS);

join_result join_relations(relation *relR, relation *relS, queue_node *queue_R, queue_node *queue_S, uint32_t jobs_to_create, thr_pool_t *pool);

mid_result** execute_join(predicate *pred, uint32_t *relations, metadata *metadata_arr, mid_result **mid_results_array, thr_pool_t *pool);

#endif