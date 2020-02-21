#ifndef JOIN_UTILITIES_H
#define JOIN_UTILITIES_H

#define CLASSIC_JOIN 1
#define JOIN_SORT_LHS 2
#define JOIN_SORT_RHS 3
#define SCAN_JOIN 4
#define DO_NOTHING 5

#include "join.h"
#include "structs.h"
#include "queries.h"

void update_mid_results(mid_result **mid_results_array, metadata *metadata_arr, join_info info);

int build_relations(predicate *pred, uint32_t *relations, metadata *metadata_arr, mid_result ***mid_results_array, rel_info *rel[2]);

#endif
