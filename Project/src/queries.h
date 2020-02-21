#ifndef QUERIES_H
#define QUERIES_H

#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include "alloc_free.h"
#include "dbg.h"
#include "thr_pool.h"
#include "structs.h"

#define EQ 1
#define LEQ 2 
#define GEQ 3 
#define L 4
#define G 5
#define SELF_JOIN 6

typedef struct relation_column {
    uint64_t relation;
    uint64_t column;
} relation_column;

typedef struct predicate {
    int8_t type;
    relation_column first;
    void *second;
    int operator;
} predicate;

typedef struct query {
    uint32_t* relations;
    size_t relations_size;
    predicate* predicates;
    size_t predicates_size;
    relation_column* selects;
    size_t select_size;
} query;

typedef struct mid_result {
    uint64_t relation;
    uint64_t predicate_id;
    int32_t last_column_sorted;
    uint64_t  *payloads;
} mid_result;

typedef struct exists_info {
    ssize_t mid_result;
    ssize_t index;
} exists_info;

void execute_queries(query *q_list, metadata *metadata_arr, thr_pool_t *pool);

#endif