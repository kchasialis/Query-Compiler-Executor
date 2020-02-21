#ifndef UTILITIES_H
#define UTILITIES_H

#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <math.h>
#include "alloc_free.h"
#include "dbg.h"
#include "structs.h"
#include "queries.h"

#define get_byte(num, byte) ( num >> ( (sizeof(num) << 3) - (byte << 3) ) & 0xFF)

//function to build a histogram
void build_histogram(relation *, histogram *, uint8_t, int, int);

void build_psum(histogram *, histogram *);

relation* build_reordered_array(relation* , relation *, histogram *, histogram *, uint8_t, int, int);

relation* allocate_reordered_array(relation *);

void free_reordered_array(relation *);

metadata* read_relations();

exists_info relation_exists(mid_result **mid_results_array, uint64_t relation, uint64_t predicate_id);

ssize_t relation_exists_current(mid_result *mid_results, uint64_t relation, uint64_t predicate_id);

void update_statistics(query *qry, metadata *metadata_arr);

#endif