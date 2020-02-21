#ifndef STRUCTS_H
#define STRUCTS_H

#include <stdio.h>
#include <stdint.h>
#include <stdbool.h>

#define MAX_JOBS 4

typedef struct tuple {
    uint64_t key;
    uint64_t payload;
} tuple;

typedef struct statistics {
    uint64_t max_value;
    uint64_t min_value;
    uint64_t approx_elements;
    size_t distinct_values;
    size_t array_size;
    bool *array;
} statistics;

typedef struct relation {
    tuple *tuples;
    uint64_t num_tuples;
    statistics *stats;
} relation;

typedef struct metadata {
    uint64_t tuples;
    uint64_t columns;
    relation **data;
} metadata;

typedef struct histogram {
    int32_t array[256];
} histogram;

#endif